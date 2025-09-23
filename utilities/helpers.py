import random

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.connect.column import Column
from pyspark.sql import SparkSession

from pyspark.sql.window import Window

from pyspark.sql.connect.dataframe import DataFrame
from dataclasses import dataclass
from functools import *
from typing import *
from datetime import date

import itertools as I

### config
CATALOG = 'uat_catalog'
SCHEMA = 'lakehouse'

REPORT_CATALOG = 'leg_uat'
REPORT_SCHEMA = 'lakehouse'

GENERAL_DATE_FORMAT = 'yyyy-MM-dd'
MONTH_DATE_FORMAT = 'yyyy-MM'

TABLE = lambda x: f'{CATALOG}.{SCHEMA}.{x}'
REPORT_TABLE = lambda x: f'{REPORT_CATALOG}.{REPORT_SCHEMA}.{x}'

### CONST
CONST_SOURCE_GROUP_MAPS = {
    'Marketing Campaign': ['57', 'xw', 'marketing campaign'],
    'TI': ['50', 'ti'],
    'WI': ['53', '55', '59'],
    'Corporate': ['58', 'corporate_event', 'corporate_lead', 'corporate_partnership', 'corporate'],
    'BR': ['60', '51', '54', 'br-360app', 'br-wechat', 'marketing_br', 'br-pos', 'br-360'],
    'Cold case': ['52', 'cold case'],
    'Others-Info': ['56']
}
###

### simple fetching
def fetch_table(spark: SparkSession, table_name: str):
    return spark.read.table(TABLE(table_name))

def rename_cols(df: DataFrame, rename_dict: dict):
    for old_col, new_col in rename_dict.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

def add_empty_cols(df: DataFrame, cols: list):
    for col in cols:
        df = df.withColumn(col, F.lit(None))
    return df

### dynamic filtering
def value_when(col, maps:dict, method:str='__eq__', condition:F.Column=None, otherwise=False):
    # _ws = F.when(F.lit(False), None)
    _ws = None
    _c = col

    for _new, _ori in maps.items():
        # dict item -> when
        _cond = getattr(_c, method)(_ori)

        # and global cond
        if condition is not None:
            _cond = _cond & condition

        # concat when
        _ws = F.when(_cond, _new) if _ws is None else _ws.when(_cond, _new)

    if otherwise:
        _ws = _ws.otherwise(_c)

    return _ws


### Change aferward
# market_lead (support multiple col filtering)
MARKET_LEAD_WHEN = lambda s=F.col('final_lead_source'), c=F.col('final_channel_code'): \
    F.when(
        s.isin(['57', 'xw']) &
        c.isin(['AI', ' CN', 'EP', 'FY', 'IZ', 'JF', 'KQ', 'NC', 'NH', 'NR', 'PZ', 'QJ']),
        'Webform'
    ).when(
        s.isin(['57', 'xw']) &
        c.isin(['JI', 'KH', 'NI']),
        'SEM'
    ).otherwise('Others')