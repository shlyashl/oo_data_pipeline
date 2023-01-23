# -*- coding: utf-8 -*-
from io import StringIO, BytesIO
import os
from time import sleep
import logging

import requests
import pandas as pd
import asyncio
import aiohttp


logger = logging
logger.basicConfig(
    format=f'%(asctime)s [%(levelname)s] %(name)s:\t\t%(message)s',
    level=logging.DEBUG
)


def try_again(func, tries=20, delay=5):
    def wraper(*args, **kwargs):
        for t in range(tries):
            try:
                result = func(*args, **kwargs)
                if t != 0:
                    logger.info(f'Attempt {t + 1}/{tries} succeeded')
                return result
            except Exception as e:
                if tries - t != 1:
                    logger.warning(f'Attempt {t + 1}/{tries} failed, sleep {delay} seconds. Error: \n{e}')
                    sleep(delay)
                else:
                    raise
    return wraper


def _get_sql_dict():
    import importlib
    from glob import glob
    sql_path = importlib.resources.files('src.sql')
    queries = {
        f.split('.')[0]: open(f'{sql_path}/{f}', mode='r', encoding='utf-8').read()
        for f in [f for f in os.listdir(sql_path) if '.sql' in f]}
    return queries


class ClickHouse:
    def __init__(self, host, user, password, table):
        self._host = host
        self._user = user
        self._password = password
        self._table = table
        self.queries = _get_sql_dict()

    async def insert(self, df_data, table=None):
        csv_buffer = StringIO()

        table = table if table else self._table
        df_data.to_csv(csv_buffer, sep='\t', line_terminator='\n', index=False)
        query_dict = {'query': f'insert into {table} format TSVWithNames'}

        async with aiohttp.ClientSession() as session:
            async with session.post(self._host, params=query_dict,
                                    auth=aiohttp.BasicAuth(self._user, self._password),
                                    data=csv_buffer.getvalue().encode('utf-8')) as response:
                response_txt = await response.text()
                await asyncio.sleep(0.1)

        assert response_txt == '', f'Insertion error: {response_txt}'

    def truncate(self, table=None):
        table = table if table else self._table
        query_dict = {'query': f'truncate {table}'}

        r = requests.post(self._host, params=query_dict, auth=(self._user, self._password), verify=False)
        if r.status_code != 200:
            raise Exception(r.text)

        logger.info(f'{table} truncated')

    @try_again
    def select(self, query) -> pd.DataFrame:
        query_dict = {'query': query + f'\nFORMAT TSVWithNames'}
        r = requests.post(self._host, params=query_dict, auth=(self._user, self._password), verify=False)
        if r.status_code == 200 and r.text != '':
            return pd.read_csv(BytesIO(r.content), sep='\t')
        else:
            raise Exception(f'\nStatus: {r.status_code}\n{r.text}')

