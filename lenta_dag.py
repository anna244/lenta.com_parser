import asyncio
import csv
import datetime
import logging
import os
import re

import aiohttp
import pytz

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, max_by, col, replace, unix_timestamp, avg
import pyspark.sql.functions as F
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg,  floor as spark_floor, regexp_extract
from pyspark.sql.functions import lit

import re
import numpy as np

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
import pendulum

import requests


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

    REGEXP_REMOVE_NEWLINES = re.compile(r'[\s\r\n ]+')

    def __init__(self, *, store_ids: list[str]):
        self.store_ids = store_ids

        self._tz = pytz.timezone('Europe/Moscow')

        self._result = []
        self._client = None

    @property
    def client(self):
        # https://www.python-httpx.org/async/
        if self._client is None:
            conn = aiohttp.TCPConnector(limit=self.API_PARALLEL_CONNECTIONS)
            timeout = aiohttp.ClientTimeout(total=30)
            self._client = aiohttp.ClientSession(timeout=timeout, raise_for_status=True)
        return self._client

    @property
    def result(self):
        return self._result

    async def shutdown(self):
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

                    # оставляем только поля перечисленные в self.CSV_FIELDS
                    new_item = {}
                    for key in self.CSV_FIELDS:
                        new_item[key] = item[key]

                    list_of_goods.append(new_item)

            current_skus = len(list_of_goods)
            total_skus += current_skus
            self._result += list_of_goods

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


with DAG(
    dag_id='lenta_parsing_dag',
    start_date=pendulum.datetime(2024, 1, 11, tz='UTC'),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    @task(task_id='get_data')
    def get_data(**kwargs):
        STORE_IDS = [
        '0124',  # Москва и МО, ул. 7-я Кожуховская, д. 9, hypermarket
        '0091',  # Тюмень, ул. Мельникайте, д. 139, hypermarket
        '0210',
        '0203',
        '0179',
        '0238'
        ]

        parser = LentaComParser(
            store_ids=STORE_IDS
            )

        try:
            asyncio.run(parser.run())
        except KeyboardInterrupt:
            asyncio.run(parser.shutdown())
            logger.info('KeyboardInterrupt, exiting...')

        pd_df = pd.DataFrame(parser.result)

        spark = SparkSession.builder\
            .master('local[*]')\
            .appName('spark_dataframe')\
            .getOrCreate()

        current_date = kwargs['ds']
        df = spark.createDataFrame(pd_df)
        df\
            .repartition(1)\
            .write\
            .mode('overwrite')\
            .parquet("/user/kurtis/lenta_com_parser/data/DT=" + current_date)

    
    @task(task_id='agg_data')
    def agg_data(**kwargs):
        spark = SparkSession.builder\
            .master('local[*]')\
            .appName('spark_dataframe')\
            .getOrCreate()

        current_date = kwargs['ds']
        df_1 = spark.read.parquet("/user/kurtis/lenta_com_parser/data")
        sc = spark.sparkContext

        df_1 = df_1.withColumn("timestamp", F.to_timestamp("timestamp"))

        df_2 = df_1\
            .filter(df_1.categoryName.isin('Сыр','Курица', 'Мясо охлажденное', 'Овощи', 'Фрукты', 'Ягоды', 'Молочная продукция', 'Яйца', 'Вино'))\
            .select(col('timestamp'),
                col('storeId'),
                col('categoryName'),
                col('subcategoryName'),
                col('title'),
                col('subTitle'),
                col('discountPrice'),
                col('regularPrice')
                )

        df_3 = df_2\
            .filter((df_2.subTitle.contains('г')) | (df_2.subTitle.contains('мл')) | (df_2.subTitle.contains('L')))\
            .select(col('storeId'),
                col('categoryName'),
                col("subcategoryName"),
                col('subTitle'),
                col('discountPrice'),
                col('regularPrice'))\
            .withColumn("weight", regexp_extract(df_2["subTitle"], r'\d+ | \d.\d+', 0))\
            .withColumn("price_discount_new", F.when(col("categoryName") == 'Вино', spark_floor(lit(1000) * col("discountPrice") / col("weight")) / 1000)\
                .when(col("categoryName") != 'Вино', spark_floor(lit(1000) * col("discountPrice") / col("weight"))))\
            .withColumn("price_regular_new", F.when(col("categoryName") == 'Вино', spark_floor(lit(1000) * col("regularPrice") / col("weight")) / 1000)\
                .when(col("categoryName") != 'Вино', spark_floor(lit(1000) * col("regularPrice") / col("weight"))))\
            .withColumn("city", F.when(col("storeId") == '0124', 'Moscow')\
                .when(col("storeId") == '0210', 'St.Petersburg')\
                .when(col("storeId") == '0203', 'Ekaterinburg')\
                .when(col("storeId") == '0179', 'Krasnodar')\
                .when(col("storeId") == '0238', 'Kazan')\
                .when(col("storeId") == '0091', 'Tumen')\
                .when(col("storeId") == '0209', 'Tumen'))

        df_disc_price = df_3\
            .groupBy(col("storeId"),col ('categoryName'),col("subcategoryName"))\
            .agg(spark_avg(col("price_discount_new")).alias("avg_price_discount_new"))\
            .withColumn("city", F.when(col("storeId") == '0124', 'Moscow')\
                .when(col("storeId") == '0210', 'St.Petersburg')\
                .when(col("storeId") == '0203', 'Ekaterinburg')\
                .when(col("storeId") == '0179', 'Krasnodar')\
                .when(col("storeId") == '0238', 'Kazan')\
                .when(col("storeId") == '0091', 'Tumen')\
                .when(col("storeId") == '0209', 'Tumen'))

        df_reg_price = df_3\
            .groupBy(col("storeId"),col ('categoryName'),col("subcategoryName"))\
            .agg(spark_avg(col("price_regular_new")).alias("avg_price_regular_new"))\
            .withColumn("city", F.when(col("storeId") == '0124', 'Moscow')\
                .when(col("storeId") == '0210', 'St.Petersburg')\
                .when(col("storeId") == '0203', 'Ekaterinburg')\
                .when(col("storeId") == '0179', 'Krasnodar')\
                .when(col("storeId") == '0238', 'Kazan')\
                .when(col("storeId") == '0091', 'Tumen')\
                .when(col("storeId") == '0209', 'Tumen'))

        df_disc_price\
            .repartition(1)\
            .write\
            .mode('overwrite')\
            .parquet("/user/kurtis/lenta_com_parser/agg/discount_price/DT=" + current_date)

        df_reg_price\
            .repartition(1)\
            .write\
            .mode('overwrite')\
            .parquet("/user/kurtis/lenta_com_parser/agg/regular_price/DT=" + current_date)


    @task(task_id='upload_data')
    def upload_data(**kwargs):
        spark = SparkSession.builder\
            .master('local[*]')\
            .appName('dataframe_exercise')\
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
            .getOrCreate()

        current_date = kwargs['ds']

        df_reg = spark.read.parquet("/user/kurtis/lenta_com_parser/agg/regular_price")
        df_disc = spark.read.parquet("/user/kurtis/lenta_com_parser/agg/discount_price")
        df_salary = spark.read.parquet("/user/kurtis/lenta_com_parser/salary") 

        df_reg\
            .write\
            .mode('overwrite')\
            .format('jdbc')\
            .option('driver','com.mysql.cj.jdbc.Driver')\
            .option('url','jdbc:mysql://localhost:3306/hse')\
            .option('dbtable','avg_regular_price')\
            .option('user','')\
            .option('password','')\
            .save()

        df_disc\
            .write\
            .mode('overwrite')\
            .format('jdbc')\
            .option('driver','com.mysql.cj.jdbc.Driver')\
            .option('url','jdbc:mysql://localhost:3306/hse')\
            .option('dbtable','avg_discount_price')\
            .option('user','')\
            .option('password','')\
            .save()
       
        df_salary\
            .write\
            .mode('overwrite')\
            .format('jdbc')\
            .option('driver','com.mysql.cj.jdbc.Driver')\
            .option('url','jdbc:mysql://localhost:3306/hse')\
            .option('dbtable','avg_salary')\
            .option('user','')\
            .option('password','')\
            .save()


    @task(task_id='get_salary')
    def get_salary(**kwargs):
        url = 'https://gogov.ru/articles/average-salary'
        headers = requests.utils.default_headers()

        headers.update(
            {
            'User-Agent': 'My User Agent 1.0',
            }
            )
        response = requests.get(url, headers=headers)

        df_list = pd.read_html(response.text)
        pd_df = df_list[0]

        spark = SparkSession.builder\
            .master('local[*]')\
            .appName('spark_dataframe')\
            .getOrCreate()

        current_date = kwargs['ds']
        df = spark.createDataFrame(pd_df)
        df\
            .repartition(1)\
            .write\
            .mode('overwrite')\
            .parquet("/user/kurtis/lenta_com_parser/salary/DT=" + current_date)

    
    get_data() >> get_salary() >> agg_data() >> upload_data()