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
    'WI': ['53', '55', '59', 'walk in'],
    'Corporate': ['58', 'corporate_event', 'corporate_lead', 'corporate_partnership', 'corporate'],
    'BR': ['60', '51', '54', 'br-360app', 'br-wechat', 'marketing_br', 'br-pos', 'br-360'],
    'Cold case': ['52', 'cold case'],
    'Others-Info': ['56']
}

def get_layer_hierachy_maps(layer_level: int, table_name: str):
    """
        1. Returns a dictionary of layer hierarchies based on the provided layer level and table name.
        2. The dictionary is constructed based on the provided layer level and table name.
        3. The dictionary contains the following keys:
            (a) 'layer_level': The provided layer level.
            (b) 'table_name': The provided table name.
    """
    layer_level = str(layer_level) # convert to string

    layer_hierachy = {
        '4' : {
            'rpt_kpi_guest': {
                'KPI_GT_GT_WI': 'WI'
            }
        }
    }
    return layer_hierachy.get(layer_level, {}).get(table_name, {})

def fetch_table(spark: SparkSession, table_name: str):
    """
        1. Fetches a table from the specified catalog and schema.
        2. Returns a DataFrame containing the table's data.
    """
    return spark.read.table(TABLE(table_name))

def concat_cols(df: DataFrame, cols: list):
    """
        1. Concatenates multiple columns in a DataFrame into a single column.
            (a) If a column's data type is decimal, it is cast to bigint before concatenation.
            (b) Otherwise, the column is used as is.
            (c) Returns a Column expression suitable for use in df.withColumn.
    """
    _map = dict(df.dtypes)

    _cl = []

    for _ in cols:
        _c = F.col(_).cast('bigint') if _map[_].startswith('decimal') else F.col(_)
        _cl.append(_c)

    return F.concat(*_cl)


def add_prefix(prefix: str, original_text: str, delimiter: str = '_'):
    """
        1. Returns a new column name by adding a prefix to 'original_text', avoiding duplicate segments.
        2. For example, prefix_diff('foo_bar', 'bar_baz') returns 'foo_bar_baz' (not 'foo_bar_bar_baz').
    """
    prefixes = prefix.split(delimiter)

    for _ in range(len(prefixes), 0, -1):
        _check = delimiter.join(prefixes[-_:])
        _prefix = delimiter.join(prefixes[:-_])

        if original_text.startswith(_check):
            prefix = _prefix
            break

    return delimiter.join([prefix, original_text]) if prefix else original_text


def col_name_add_prefix(df: DataFrame, prefix: str, delimiter='_'):
    """
        1. Adds a prefix to all column names in the DataFrame, avoiding duplicate segments.
        2. For example, if a column is 'foo_bar' and prefix is 'foo', it will not become 'foo_foo_bar' but just 'foo_bar'.
        3. Usage example:
            (a) prefix = 'foo'
            (b) original column names: ['bar', 'foo_baz', 'x_bar']
            (c) new column names: ['foo_bar', 'foo_baz', 'foo_x_bar'] 
    """
    for _ in df.columns:
        _new = add_prefix(prefix, _, delimiter)
        df = df.withColumnRenamed(_, _new)
    return df



def fetch_table_with_resource(spark: SparkSession, table_name: str, concat_col_dict: dict, prefix: str):
    """
        1. Fetches a table from the specified catalog and schema using the provided table name.
        2. If concat_col_dict is provided, for each key-value pair:
            (a) Key is the new column name.
            (b) Value is a list of column names to concatenate.
            (c) Adds a new column to the DataFrame by concatenating the specified columns.
        3. If prefix is provided, adds the prefix to all column names in the DataFrame, avoiding duplicate segments.
        4. Returns the resulting DataFrame.
    """
    df = spark.read.table(TABLE(table_name))
    if concat_col_dict is not None:
        for _col_name, _col_list in concat_col_dict.items():
            df = df.withColumn(
                _col_name, concat_cols(df, _col_list)
            )
    if prefix is not None:
        df = col_name_add_prefix(df, prefix)

    return df

def rename_cols(df: DataFrame, rename_dict: dict):
    """
        1. Renames columns in a DataFrame based on a provided dictionary.
        2. rename_dict, for each key-value pair:
            (a) Key is the original column name.
            (b) Value is the new column name.
        3. Returns the DataFrame with modified column names.
    """
    for old_col, new_col in rename_dict.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

def upsert_empty_cols(df: DataFrame, cols: list):
    """
        1. Add/Replace columns to the DataFrame with the specified names in 'cols'.
        2. Each new column is filled with null (None) values.
        3. Returns the DataFrame with the added empty columns.
    """
    for col in cols:
        df = df.withColumn(col, F.lit(None))
    return df


def value_when(col, maps:dict, method:str = '__eq__', condition:F.Column = None, otherwise = False):
    """
        1. Builds a chained Spark SQL 'when' expression for conditional value mapping.
        2. For each key-value pair in 'maps':
            (a) Compares 'col' to the value using the specified 'method' (default: equality).
            (b) If 'condition' is provided, combines it with the comparison using AND.
            (c) If the condition is met, assigns the key as the new value.
        3. If 'otherwise' is True, returns the original column value when no conditions match.
        4. Returns the constructed Column expression.
    """
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