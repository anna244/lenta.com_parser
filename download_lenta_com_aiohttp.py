"""
Before start, run in the console
    pip install aiolimiter httpx pytz

List of public APIs:
    https://lenta.com/api/v1/stores/
    https://lenta.com/api/v1/stores/types
    https://lenta.com/api/v1/stores/{storeId}/crazy
    https://lenta.com/api/v1/stores/{storeId}/crazy/{id}
    https://lenta.com/api/v1/stores/{storeId}/mobilepromo
    https://lenta.com/api/v1/stores/{storeId}/nodes/filters
    https://lenta.com/api/v1/stores/{storeId}/promotionfilters
    https://lenta.com/api/v1/stores/{storeId}/skuslist
    https://lenta.com/api/v1/stores/{cityId}/skus/{skuCode}
    https://lenta.com/api/v1/stores/{storeId}/catalog/search
    https://lenta.com/api/v1/stories
    https://lenta.com/api/v1/stores/{storeId}/skus
    https://lenta.com/api/v1/stores/{storeId}/skus/{skuCode}

    https://lenta.com/api/v1/check/comment/allow/{skuCode}
    https://lenta.com/api/v1/complaints
    https://lenta.com/api/v1/complaints/{complaintGuid}

    https://lenta.com/api/v2/stories
    https://lenta.com/api/v2/stores
    https://lenta.com/api/v2/stores/{storeId}/catalog

    https://lenta.com/api/v1/cities
    https://lenta.com/api/v1/cities/{cityId}/stores
    https://lenta.com/api/v1/comments/{skuCode}
    https://lenta.com/api/v1/devices
    https://lenta.com/api/v1/labels/sku

Example POST request:
    POST https://lenta.com/api/v1/stores/0124/skus
    Accept: application/json

    { "offset": 0, "limit": 24, nodeCode: "c6e2412e28ef3b1f86a0cc8d9eb68b4a7" }
"""
import asyncio
import csv
import datetime
import logging
import os
import re

import pytz
import aiohttp

STORE_IDS = [
    '0124',  # Москва и МО, ул. 7-я Кожуховская, д. 9, hypermarket
    '0091',  # Тюмень, ул. Мельникайте, д. 139, hypermarket
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s @ %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
)
logger = logging.getLogger(name='LentaComParser')


class LentaComParser:
    API_URL_LIST_OF_STORES = 'https://lenta.com/api/v2/stores'
    API_URL_LIST_OF_CATEGORIES = 'https://lenta.com/api/v2/stores/{storeId}/catalog'
    API_URL_LIST_OF_GOODS = 'https://lenta.com/api/v1/stores/{storeId}/skus'
    CSV_FIELDS = (
        'timestamp', 'storeId',
        'code', 'title', 'brand', 'subTitle', 'description',
        'regularPrice', 'discountPrice', 'imageUrl', 'webUrl',
        'groupName', 'categoryName', 'subcategoryName'
    )

    API_SKUS_PER_PAGE = 24
    API_SKUS_BATCH_SIZE = 5
    API_RETRIES = 5
    API_PARALLEL_CONNECTIONS = 2

    REGEXP_REMOVE_NEWLINES = re.compile(r'[\s\r\n ]+')

    def __init__(self, *, outfile_name: str, store_ids: list[str]):
        self.outfile_name = outfile_name
        self.store_ids = store_ids

        self._csv_writer = None
        self._outfile = None
        self._tz = pytz.timezone('Europe/Moscow')

        self._client = None

    @property
    def csv_writer(self):
        if self._csv_writer is None:
            is_outfile_exists = os.path.exists(self.outfile_name)
            mode = 'a' if is_outfile_exists else 'w'
            self._outfile = open(self.outfile_name, mode=mode, buffering=1, encoding='utf-8')
            self._csv_writer = csv.DictWriter(
                self._outfile,
                fieldnames=self.CSV_FIELDS,
                extrasaction='ignore'
            )
            if not is_outfile_exists:
                self._csv_writer.writeheader()

        return self._csv_writer

    @property
    def client(self):
        # https://www.python-httpx.org/async/
        if self._client is None:
            conn = aiohttp.TCPConnector(limit=self.API_PARALLEL_CONNECTIONS)
            timeout = aiohttp.ClientTimeout(total=30)
            self._client = aiohttp.ClientSession(timeout=timeout, raise_for_status=True)
        return self._client

    async def shutdown(self):
        if self._outfile is not None:
            self._outfile.close()

        if self._client:
            await self._client.close()

    async def request(self, *, url: str, method: str = 'GET', **kwargs):
        resp = await self.client.request(
            method, 
            url, 
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
            },
            **kwargs
        )
        return await resp.json()

    async def get_list_of_goods(
            self,
            *,
            store_id: str,
            offset: int = 0,
            limit: int = 24,
            category_code: str = '',
            retries: int = 5
    ):
        data = {
            'limit': limit
        }
        if offset:
            data['offset'] = offset
        if category_code:
            data['nodeCode'] = category_code

        current_try = 0
        while current_try < retries:
            try:
                return await self.request(
                    method='POST',
                    url=self.API_URL_LIST_OF_GOODS.format(storeId=store_id),
                    json=data
                )
            except Exception as e:
                current_try += 1
                logger.error(
                    f'Network error while getting list of goods for store_id: {store_id}, data: {str(data)[:1000]}, error: {str(e)[:1000]}. '
                    f'Retrying ({current_try}/{retries}).'
                )
                await asyncio.sleep(current_try)

    async def download_list_of_goods(self, store_id: str):
        total_skus = 0
        total_skus_in_store = 0
        current_skus = None
        good_urls = set()

        while current_skus or current_skus is None:
            tasks = [
                self.get_list_of_goods(
                    store_id=store_id,
                    offset=total_skus + i * self.API_SKUS_PER_PAGE,
                    limit=self.API_SKUS_PER_PAGE,
                    retries=self.API_RETRIES
                )
                for i in range(self.API_SKUS_BATCH_SIZE)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            list_of_goods = []

            for items in results:
                if isinstance(items, Exception):
                    logger.exception('Cannot fetch', exc_info=items)
                    continue

                if not total_skus_in_store:
                    total_skus_in_store = items['total']

                items = items['skus']
                if not items:
                    continue

                for item in items:
                    if item['webUrl'] in good_urls:
                        continue

                    item['timestamp'] = datetime.datetime.now(tz=self._tz).isoformat()
                    item['storeId'] = store_id
                    item['imageUrl'] = item['image']['fullSize'] if item['image'] else ''
                    item['groupName'] = item['categories'].get('group', {}).get('name', '')
                    item['categoryName'] = item['categories'].get('category', {}).get('name', '')
                    item['subcategoryName'] = item['categories'].get('subcategory', {}).get('name', '')
                    item['title'] = re.sub(self.REGEXP_REMOVE_NEWLINES, ' ', item['title'])
                    item['description'] = re.sub(self.REGEXP_REMOVE_NEWLINES, ' ', item['description'])

                    good_urls.add(item['webUrl'])
                    list_of_goods.append(item)

            current_skus = len(list_of_goods)
            total_skus += current_skus
            self.csv_writer.writerows(list_of_goods)

            logger.info(f'Store {store_id}, downloaded {current_skus} items ({total_skus} of {total_skus_in_store}).')

    async def _producer(self):
        tasks = [
            self.download_list_of_goods(store_id)
            for store_id in self.store_ids
        ]

        await asyncio.gather(*tasks)

    async def run(self):
        try:
            await self._producer()
        finally:
            await self.shutdown()


def main():
    parser = LentaComParser(
        outfile_name='lenta-com-goods.csv',
        store_ids=STORE_IDS
    )

    try:
        asyncio.run(parser.run())
    except KeyboardInterrupt:
        asyncio.run(parser.shutdown())
        logger.info('KeyboardInterrupt, exiting...')


if __name__ == '__main__':
    main()
