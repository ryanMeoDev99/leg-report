#====== [START] CELL 1 ======

import random

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.connect.column import Column

from pyspark.sql.window import Window

from pyspark.sql.connect.dataframe import DataFrame
from dataclasses import dataclass
from functools import *
from typing import *

from datetime import datetime
import pytz



#====== [END] CELL 1 ======




#====== [MARKDOWN] CELL 2 ======

#====== [START] CELL 3 ======

tz = pytz.timezone('Asia/Singapore')  # or 'Asia/Shanghai', etc.
now_utc8 = datetime.now(tz)

# Get last year
last_year = now_utc8.year - 1

print(last_year)

#====== [END] CELL 3 ======


#====== [START] CELL 4 ======

PURE_CATALOG = 'prod_catalog'
PURE_SCHEMA = 'lakehouse'

TABLE = lambda x: f'{PURE_CATALOG}.{PURE_SCHEMA}.{x}'

#====== [END] CELL 4 ======


#====== [START] CELL 5 ======

LEG_CATALOG = 'leg_uat'
LEG_SCHEMA = 'lakehouse'

LEG_TABLE = lambda x: f'{LEG_CATALOG}.{LEG_SCHEMA}.{x}'

#====== [END] CELL 5 ======


#====== [START] CELL 6 ======

def value_when(col:str, maps:dict, method:str='__eq__', otherwise=False):
    _ws = F.when(F.lit(False), None)
    _c = F.col(col)

    for _new, _ori in maps.items():
        _cond = getattr(_c, method)(_ori)
        _ws = _ws.when(_cond, _new)

    _ws = _ws.otherwise(_c) if otherwise else _ws
    return _ws

def create_nested(df, whens:list, layer_col:str='layer'):
    # nested layer
    tmp_cols = [f'_tmp_{layer_col}_{_}' for _, _w in enumerate(whens)]

    for _i, _when in enumerate(whens):
        # create when(s)
        df = df.withColumn(tmp_cols[_i], _when)

    # create concat array
    df = df.withColumn(
        layer_col, F.concat(tmp_cols)
    )

    # drop tmp cols
    df = df.drop(*tmp_cols)

    # filter none
    df = df.filter(F.size(layer_col) > 0)
    return df

#====== [END] CELL 6 ======


#====== [START] CELL 7 ======

# location
location_maps = {
    'RP': 'RPY',
    'HKK11': 'K11',
    'head': 'HEAD',
    'MOKO': 'Moko',
    'ASTHK': 'AST',
    'NACF': 'NAF',
    'NACY': 'NAC',
    'ASTSG2': 'SGASTY',
    'ASTSG': 'SGASTF',
    'STS': 'SSP',
    'HH': 'HUT'
}

LOCATION_WHEN = value_when('location', maps=location_maps, otherwise=True)


#====== [END] CELL 7 ======




#====== [MARKDOWN] CELL 8 ======

#====== [START] CELL 9 ======

silver_accounts = spark.read.table(TABLE('silver_accounts'))
silver_contact = spark.read.table(TABLE('silver_contact'))


silver_agreement = spark.read.table(TABLE('360_silver_agreement'))

silver_package = spark.read.table(TABLE('silver_package'))
silver_invoice_detail = spark.read.table(TABLE('silver_invoice_detail'))
silver_invoice_header = spark.read.table(TABLE('silver_invoice_header'))

#====== [END] CELL 9 ======


#====== [START] CELL 10 ======

silver_accounts = silver_accounts.select(
    'ref_id',
    'id',
    'region',
    'guest_status',
    'success_date',
    ).filter(F.col('success_date').isNotNull() & (F.col('guest_status') == 'Success'))

silver_accounts = (
    silver_accounts
    .withColumn('min_success_date', F.min('success_date').over(Window.partitionBy('ref_id').orderBy('success_date')))
    .filter(F.col('success_date') == F.col('min_success_date'))
    .drop('min_success_date')
)

silver_contact = silver_contact.select(
    'bk', 
    'source',
    'id',
    'ext_ref_contact_id',
    )

#====== [END] CELL 10 ======


#====== [START] CELL 11 ======

accounts_nmu_ym = (
    silver_contact.alias('contact')
    .join(
        silver_accounts.alias('accounts'), 
        F.concat(F.col('accounts.region'), F.col('accounts.ref_id')) == F.concat(F.col('contact.source'), F.col('contact.ext_ref_contact_id')), 
        'left'
    )
    .select(
        F.concat(F.col('contact.source'), F.col('contact.id').cast('bigint')).alias('dim_contact_key'),
        # F.col('accounts.id').alias('dim_accounts_id'),
        F.col('contact.source').alias('region'),
        F.col('accounts.success_date').alias('success_date'),
        F.date_format(F.col('success_date'), 'yyyyMM').alias('NMU_ym'),
    )
    .dropDuplicates()
)

#====== [END] CELL 11 ======


#====== [START] CELL 12 ======

# # Check for duplicate dim_contact_key
# duplicate_dim_contact_key = (
#     accounts_nmu_ym
#     .groupBy('dim_contact_key')
#     .count()
#     .filter(F.col('count') > 1)
# )

# display(duplicate_dim_contact_key)

#====== [END] CELL 12 ======




#====== [MARKDOWN] CELL 13 ======

#====== [START] CELL 14 ======

# Read tables
silver_agreement = spark.read.table(TABLE('360_silver_agreement'))
silver_package = spark.read.table(TABLE('silver_package'))
silver_package_type = spark.read.table(TABLE('silver_package_type'))
silver_package_term = spark.read.table(TABLE('silver_package_term'))
silver_agreement_status = spark.read.table(TABLE('silver_agreement_status'))
silver_contact = spark.read.table(TABLE('silver_contact'))
silver_contact_status = spark.read.table(TABLE('silver_contact_status'))
silver_360_package = spark.read.table(TABLE('360_silver_package'))
silver_service = spark.read.table(TABLE('silver_service'))

#====== [END] CELL 14 ======


#====== [START] CELL 15 ======

# 1. Create membership_agreement 
membership_agreement = (
    silver_agreement.alias('a')
    .filter(F.col('a.deleted_at').isNull() & F.col('a.parent_agreement_id').isNull())
    .join(
        silver_package.alias('p'),
        F.concat(F.col('a.source'), F.col('a.package_id').cast('bigint')) == F.concat(F.col('p.source'), F.col('p.id').cast('bigint')),
        'left'
    )
    .select(
        'a.*',
        F.col('p.source').alias('p_source'),
        F.when(
            (F.col('a.source') == 'HK') & 
            (F.col('a.package_type_id') == 1) & 
            (F.col('p.package_sub_type_id').isin(8, 9)), 1
        ).when(
            (F.col('a.source') == 'SG') & 
            (F.col('a.package_type_id') == 1) & 
            (F.col('p.package_sub_type_id').isin(1, 2)), 1
        ).when(
            (F.col('a.source') == 'CN') & 
            (F.col('a.package_type_id') == 1), 1
        ).otherwise(0).alias('is_membership')
    )
)

#====== [END] CELL 15 ======


#====== [START] CELL 16 ======

# 2. Create previous_agreement 
previous_agreement = (
    membership_agreement
    .filter(F.col('deleted_at').isNull() & (F.col('is_membership') == 1))
    .select(
        'source',
        'bk',
        'id',
        'contact_id',
        F.concat('source', 'contact_id').alias('dim_client_key'),
        'signed_date',
        'start_date',
        'end_date',
        'last_workout_date',
        'package_id',
        F.coalesce('last_workout_date', 'end_date').alias('prev_gap_reference')
    )
)

#====== [END] CELL 16 ======


#====== [START] CELL 17 ======

# 3. Create first_agreement  
# first_agreement_window = Window.partitionBy(F.concat(F.col('source'), F.col('contact_id'))).orderBy(F.coalesce('signed_date', 'start_date'))
# first_agreement_window = Window.partitionBy('source', 'contact_id').orderBy(F.coalesce('signed_date', 'start_date'))
first_agreement_window = Window.partitionBy('source', 'contact_id').orderBy('start_date', F.col('id').cast('bigint'))

first_agreement_df = (
    membership_agreement
    .filter(
        F.col('deleted_at').isNull() & 
        (F.col('package_type_id') == 1) & 
        F.col('parent_agreement_id').isNull() & 
        (F.col('is_membership') == 1)
    )
    .withColumn('row_num', F.row_number().over(first_agreement_window))
    .filter(F.col('row_num') == 1)
    .drop('row_num')
)

#====== [END] CELL 17 ======


#====== [START] CELL 18 ======

# membership_agreement.filter(
#                         F.col('deleted_at').isNull() & 
#                         (F.col('package_type_id') == 1) & 
#                         F.col('parent_agreement_id').isNull() & 
#                         (F.col('is_membership') == 1)
#                     ).withColumn('row_num', F.row_number().over(first_agreement_window)
#                     ).filter(F.col("contact_id").isin(["262987"])).display()

#====== [END] CELL 18 ======


#====== [START] CELL 19 ======

# 4. Create prev_agreement
# prev_agreement_window = Window.partitionBy('ma.bk', 'ma.source', 'ma.contact_id').orderBy(F.coalesce('pa.signed_date', 'pa.start_date').desc())
# prev_agreement_window = Window.partitionBy('ma.bk').orderBy(F.coalesce('pa.signed_date', 'pa.start_date').desc())
prev_agreement_window = Window.partitionBy('ma.bk').orderBy(F.col('pa.start_date').desc(), F.col('pa.id').cast('bigint').desc())

prev_agreement_df = (
    membership_agreement.alias('ma')
    .join(
        previous_agreement.alias('pa'),
        (F.concat(F.col('ma.source'), F.col('ma.contact_id').cast('bigint')) == F.concat(F.col('pa.source'), F.col('pa.contact_id').cast('bigint'))) &
        (F.col('pa.prev_gap_reference') < F.col('ma.start_date')), 
        'left'
    )
    .withColumn('row_num', F.row_number().over(prev_agreement_window))
    .filter(F.col('row_num') == 1)
    .select(
        F.col('ma.bk').alias('fct_agreement_bk'),
        F.col('pa.source').alias('source'),
        F.col('pa.bk').alias('bk'),
        F.col('pa.contact_id').alias('contact_id'),
        F.col('pa.dim_client_key').alias('dim_client_key'),
        F.col('pa.signed_date').alias('signed_date'),
        F.col('pa.start_date').alias('start_date'),
        F.col('pa.end_date').alias('end_date'),
        F.col('pa.last_workout_date').alias('last_workout_date'),
        F.col('pa.package_id').alias('package_id'),
        F.col('pa.prev_gap_reference').alias('prev_gap_reference'),
    )
)

# prev_agreement_df.filter(F.col('fct_agreement_bk') == "SG625702").filter(F.col('contact_id') == "139293").display()


#====== [END] CELL 19 ======


#====== [START] CELL 20 ======

# 5. Joining 
join_agreement = (
    membership_agreement.alias('agreement')
    .join(
        silver_360_package.alias('package'),
        F.concat(F.col('agreement.source'), F.col('agreement.package_id')) == F.col('package.bk'),
        'left'
    )
    .join(
        silver_package_type.alias('package_type'),
        F.concat(F.col('package.source'), F.col('package.package_type_id')) == F.col('package_type.bk'),
        'left'
    )
    .join(
        silver_package_term.alias('package_term'),
        F.concat(F.col('package.source'), F.col('package.package_term_id')) == F.col('package_term.bk'),
        'left'
    )
    .join(
        first_agreement_df.alias('first_agreement'),
        F.col('agreement.bk') == F.col('first_agreement.bk'),
        'left'
    )
    .join(
        prev_agreement_df.alias('prev_agreement'),
        F.col('agreement.bk') == F.col('prev_agreement.fct_agreement_bk'),
        'left'
    )
    .join(
        silver_agreement_status.alias('agreement_status'),
        (F.col('agreement.source') == F.col('agreement_status.source')) & 
        (F.col('agreement.agreement_status_id') == F.col('agreement_status.id')),
        'left'
    )
    .join(
        silver_contact.alias('contact'),
        F.concat(F.col('agreement.source'), F.col('agreement.contact_id')) == F.col('contact.bk'),
        'left'
    )
    .join(
        silver_contact_status.alias('contact_status'),
        F.concat(F.col('contact.source'), F.col('contact.contact_status_id')) == F.col('contact_status.bk'),
        'left'
    )
    .join(
        membership_agreement.alias('old_agreement'),
        F.concat(F.col('agreement.source'), F.col('agreement.from_agreement_id').cast('bigint')) == F.col('old_agreement.bk'),
        'left'
    )
    .join(
        silver_360_package.alias('prev_package'),
        F.concat(F.col('prev_agreement.source'), F.col('prev_agreement.package_id')) == F.col('prev_package.bk'),
        'left'
    )
    .join(
        silver_360_package.alias('old_package'),
        F.concat(F.col('old_agreement.source'), F.col('old_agreement.package_id')) == F.col('old_package.bk'),
        'left'
    )
    .join(
        silver_package_term.alias('old_package_term'),
        F.concat(F.col('old_package.source'), F.col('old_package.package_term_id')) == F.col('old_package_term.bk'),
        'left'
    )
    .join(
        silver_service.alias('service'),
        F.concat(F.col('package.source'), F.col('package.ext_ref_package_id')) == F.concat(F.col('service.source'), F.col('service.service_id')),
        'left'
    )
    .filter(
        F.col('agreement.ext_ref_agreement_id').isNotNull()
    )
)

#====== [END] CELL 20 ======


#====== [START] CELL 21 ======

# Final transformations and calculations for all the required fields
fct_agreement = join_agreement.select(
    F.col("agreement.bk").cast("string").alias("fct_agreement_key"),
    F.col("agreement.source").cast("string").alias("source"),
    F.concat(F.col("agreement.source"), F.col("agreement.from_agreement_id").cast("bigint")).cast("string").alias("from_agreement_key"),
    F.col("prev_agreement.bk").cast("string").alias("prev_agreement_key"),
    F.concat(F.col("agreement.source"), F.col("agreement.package_id")).cast("string").alias("dim_package_key"),
    F.concat(F.col("agreement.source"), F.col("agreement.contact_id")).cast("string").alias("dim_contact_key"),
    F.concat(F.col("agreement.source"), F.col("agreement.revenue_location_id")).cast("string").alias("dim_revenue_location_key"),
    F.from_utc_timestamp(
        F.from_unixtime(F.col("agreement.created_at") / 1000), 
        "Asia/Shanghai"
    ).cast("timestamp").alias("agreement_start_date"),
    F.date_format(
        F.from_utc_timestamp(F.from_unixtime(F.col("agreement.created_at") / 1000), "Asia/Shanghai"), 
        "yyyyMMdd"
    ).cast("string").alias("agreement_start_date_key"),
    F.col("agreement.agreement_no").cast("string").alias("agreement_no"),
    F.col("agreement.min_committed_month").cast("int").alias("min_committed_month"),
    F.col("agreement.first_month_fee").cast("decimal(18,4)").alias("first_month_fee"),
    F.col("agreement.last_month_fee").cast("decimal(18,4)").alias("last_month_fee"),
    F.col("agreement.prorata_fee").cast("decimal(18,4)").alias("prorata_fee"),
    F.col("agreement.prepaid_fee").cast("decimal(18,4)").alias("prepaid_fee"),
    F.col("agreement.joining_fee").cast("decimal(18,4)").alias("joining_fee"),
    F.col("agreement.monthly_fee").cast("decimal(18,4)").alias("monthly_fee"),
    F.col("agreement.start_date").cast("timestamp").alias("start_date"),
    F.col("agreement.end_date").cast("timestamp").alias("end_date"),
    F.col("agreement.signed_date").cast("timestamp").alias("signed_date"),
    F.col("agreement.last_workout_date").cast("timestamp").alias("last_workout_date"),
    F.col("package.service_category_id").cast("bigint").alias("service_category_id"),
    F.col("package.package_sub_type_id").cast("decimal(38,18)").alias("package_sub_type_id"),
    F.col("service.service_name").cast("string").alias("service_name"),
    F.col("agreement.from_agreement_id").cast("decimal(38,18)").alias("from_agreement_id"),
    F.col("agreement.end_date").cast("timestamp").alias("agreement_end_date"),
    F.date_format(F.col("agreement.end_date"), "yyyyMMdd").cast("string").alias("agreement_end_date_key"),
    F.concat(F.col("agreement.source"), F.col("agreement.ext_ref_agreement_id")).cast("string").alias("ext_ref_agreement_id"),
    F.col("agreement.package_id").cast("bigint").alias("package_id"),
    F.col("package_type.name").cast("string").alias("package_type_name"),
    F.col("package_term.name").cast("string").alias("package_term_name"),
    F.col("agreement_status.bk").cast("string").alias("dim_agreement_status_key"),
    F.col("contact_status.status_360").cast("string").alias("contact_status"),
    F.when(F.col("agreement.total_session") >= 99999, 1).otherwise(0).cast("int").alias("is_unlimited"),
    F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")).cast("int").alias("gap_days"),
    F.when(F.col("package.package_type_id") != 1, None)
        .when(F.col("first_agreement.bk") == F.col("agreement.bk"), "New")
        .when(F.col("agreement.from_agreement_id").isNotNull(), "Upgrade")
        .when(
            (F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")) > 1) &
            (F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")) <= 90)       # updated on 2025.08.21. Added
            , "Rejoin"
        ).when(F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")) <= 1, "Renew")
        .otherwise("New").cast("string").alias("agreement_status_type"),                                        # updated on 2025.08.21. Renew -> New
    F.col("prev_agreement.prev_gap_reference").cast("timestamp").alias("prev_gap_reference"),
    F.when(
        (F.col("agreement.from_agreement_id").isNotNull()) &
        (F.col("package_term.id") == 1) &
        (F.col("old_package_term.id") == 2),
        "Upgrade prepaid to due"
    ).when(
        (F.col("agreement.from_agreement_id").isNotNull()) &
        (F.col("package_term.id") == 2) &
        (F.col("old_package_term.id") == 2),
        "Upgrade prepaid to prepaid"
    ).when(
        (F.col("agreement.from_agreement_id").isNotNull()) &
        (F.col("package_term.id") == 2) &
        (F.col("old_package_term.id") == 1),
        "Upgrade due to prepaid"
    ).when(
        (F.col("agreement.from_agreement_id").isNotNull()) &
        (F.col("package_term.id") == 1) &
        (F.col("old_package_term.id") == 1),
        "Upgrade due to due"
    ).when(
        (F.col("agreement.from_agreement_id").isNull()) &
        (F.col("package.package_type_id") == 1) &
        (F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")) <= 1) &
        (F.col("prev_package.package_term_id") == 2) &
        (F.col("package.package_term_id") == 2),
        # (F.col("package.is_renew") == 1),                                                                     # updated on 2025.08.21. Removed
        "Renew prepaid to prepaid renew"
    ).when(
        (F.col("agreement.from_agreement_id").isNull()) &
        (F.col("package.package_type_id") == 1) &
        (F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")) <= 1) &
        (F.col("package.package_term_id") == 2),
        # (F.col("package.is_renew") == 1),                                                                     # updated on 2025.08.21. Removed
        "Renew prepaid"
    ).when(
        (F.col("agreement.from_agreement_id").isNull()) &
        (F.col("package.package_type_id") == 1) &
        (F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")) > 1) &
        (F.datediff(F.col("agreement.start_date"), F.col("prev_agreement.prev_gap_reference")) <=90) &          # updated on 2025.08.21. Added
        (F.col("prev_package.package_term_id") == 2) &
        (F.col("package.package_term_id") == 2),
        # (F.col("package.is_renew") == 1),                                                                     # updated on 2025.08.21. Removed
        "Rejoin prepaid to prepaid renew"
    ).otherwise("Unknown").cast("string").alias("agreement_status_group")
).orderBy(
    'dim_contact_key', 
    'agreement.start_date', 
    'agreement.signed_date'
)



#====== [END] CELL 21 ======




#====== [MARKDOWN] CELL 22 ======

#====== [START] CELL 23 ======

# fct_agreement = spark.read.table(TABLE('360_fct_agreement'))
dim_location_360f = spark.read.table(TABLE('dim_location_360f'))


silver_invoice_detail = spark.read.table(TABLE('silver_invoice_detail'))
silver_invoice_header = spark.read.table(TABLE('silver_invoice_header'))
silver_invoice_category = spark.read.table(TABLE('silver_invoice_category'))
silver_package = spark.read.table(TABLE('360_silver_package'))
silver_package_sub_type = spark.read.table(TABLE('silver_package_sub_type'))
silver_package_book_limit_line = spark.read.table(TABLE('silver_package_book_limit_line'))
silver_location = spark.read.table(TABLE('silver_location'))



#====== [END] CELL 23 ======


#====== [START] CELL 24 ======

prev_agt_invoice_df = (
    silver_invoice_detail
    .filter(F.col('item_type') == 'agreement')
    .withColumn("row_num", F.row_number().over(Window.partitionBy("item_id").orderBy(F.desc("created_at"))))
    .filter("row_num = 1")
)

upgrade_agt_invoice_df = (
    silver_invoice_detail
    .filter(F.col('item_type') == 'agreement')
    .withColumn("row_num", F.row_number().over(Window.partitionBy("item_id").orderBy(F.desc("created_at"))))
    .filter("row_num = 1")
)

# updated on 2025.08.21. Added
pos_discount_rounding_df = (
    silver_invoice_detail.alias("id")
    .filter(F.col("active") == 1)
    .filter(F.col("item_type") == "product")
    .join(
        silver_invoice_category.alias("ic").filter(F.col("active") == 1).filter(F.col("ic.name").isin(["Others", "Rounding"])),
        F.concat(F.col("id.source"), F.col("id.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "inner"
    )
    .groupBy("id.invoice_header_id", "id.source")
    .agg(F.sum("id.amount").cast(DecimalType(18, 4)).alias("amount"))
    .withColumn("invoice_header_bk", F.concat(F.col("source"), F.col("invoice_header_id").cast("bigint")))
    .filter(F.col("amount") < F.lit(0).cast(DecimalType(18, 4)))
    .select("invoice_header_bk", "amount")
)

raw_invoice_df = (
    silver_invoice_detail.alias("id")
    .filter(F.col("active") == 1)
    .join(
        silver_invoice_category.alias("ic").filter(~F.col("ic.name").isin(["Others", "Rounding"])),
        F.concat(F.col("id.source"), F.col("id.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "inner"
    )
    .groupBy("id.invoice_header_id", "id.source")
    .agg(F.sum("id.amount").cast(DecimalType(18, 4)).alias("raw_amount"))
    .withColumn("invoice_header_bk", F.concat(F.col("source"), F.col("invoice_header_id").cast("bigint")))
    .select("invoice_header_bk", "raw_amount")
)

reverse_invoice_df = (
    silver_invoice_detail
    .filter(F.col("item_type") == "invoice_header")
    .filter(F.col("active") == 1)
    .groupBy("source", "item_id")
    .agg(F.sum("amount").alias("reverse_amount"))
    .withColumn("reverse_invoice_header_bk", F.concat(F.col("source"), F.col("item_id").cast("bigint")))
    .select("reverse_invoice_header_bk", "reverse_amount")
)


#====== [END] CELL 24 ======


#====== [START] CELL 25 ======

join_membership_sales = (
    fct_agreement.alias("a") 
    .join(
        silver_invoice_detail.alias("e"),
        (F.concat(F.col("e.source"), F.col("e.item_id").cast("bigint")) == F.col("a.fct_agreement_key")) &
        (F.col("e.item_type") == "agreement") &
        (F.col("e.active") == 1),              # updated on 2025.08.21. Added
        "inner"
    ) 
    .join(
        silver_invoice_header.alias("ih"),
        (F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")) == F.concat(F.col("ih.source"), F.col("ih.id").cast("bigint"))) &
        (F.col("e.active") == 1),              # updated on 2025.08.21. Added
        "left"
    ) 
    .join(
        silver_invoice_category.alias("ic"),
        F.concat(F.col("e.source"), F.col("e.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "left"
    ) 
    .join(
        silver_package.alias("b"),
        F.col("a.dim_package_key") == F.col("b.bk"),
        "left"
    ) 
    .join(
        silver_package_sub_type.alias("pst"),
        F.concat(F.col("b.source"), F.col("b.package_sub_type_id").cast("bigint")) == F.col("pst.bk"),
        "left"
    ) 
    .join(
        silver_package_book_limit_line.alias("c"),
        F.col("a.package_id") == F.col("c.package_id"),
        "left"
    ) 
    .join(
        silver_location.alias("f"),
        F.col("a.dim_revenue_location_key") == F.col("f.bk"),
        "left"
    ) 
    .join(
        dim_location_360f.alias("loc_f"),
        F.col("loc_f.dim_location_360f_key") == F.col("f.bk"),
        "left"
    ) 
    .join(
        fct_agreement.alias("upgrade_agt"),
        F.col("upgrade_agt.fct_agreement_key") == F.col("a.from_agreement_key"),
        "left"
    ) 
    .join(
        fct_agreement.alias("prev_agt"),
        F.col("prev_agt.fct_agreement_key") == F.col("a.prev_agreement_key"),
        "left"
    ) 
    .join(
        silver_package_sub_type.alias("prev_agt_pst"),
        F.concat(F.col("prev_agt.source"), F.col("prev_agt.package_sub_type_id").cast("bigint")) == F.col("prev_agt_pst.bk"),
        "left"
    ) 
    .join(
        prev_agt_invoice_df.alias("prev_agt_invoice"),
        F.concat(F.col("prev_agt_invoice.source"), F.col("prev_agt_invoice.item_id").cast("bigint")) == F.col("a.prev_agreement_key"),
        "left"
    ) 
    .join(
        upgrade_agt_invoice_df.alias("upgrade_agt_invoice"),
        F.concat(F.col("upgrade_agt_invoice.source"), F.col("upgrade_agt_invoice.item_id").cast("bigint")) == F.col("a.prev_agreement_key"),
        "left"
    )

    # updated on 2025.08.21. Added
    .join(
        pos_discount_rounding_df.alias("pos_dis"),
        F.col("pos_dis.invoice_header_bk") == F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")),
        "left"
    )
    .join(
        raw_invoice_df.alias("ri"),
        F.col("ri.invoice_header_bk") == F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")),
        "left"
    )
    .join(
        reverse_invoice_df.alias("reverse_invoice"),
        F.col("reverse_invoice.reverse_invoice_header_bk") == F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")),
        "left"
    )
    .filter(
        F.col('b.package_type_id') == 1
    )
    .filter(
        ~(
            (F.col("e.invoice_category_id") == 2) &
            (F.col("e.amount") < 0) &
            (F.col("e.desc").like("%Corporate Subsidy%"))
        )
    )
)

#====== [END] CELL 25 ======


#====== [START] CELL 26 ======

window_spec = Window.partitionBy("a.dim_contact_key").orderBy("a.signed_date", "a.end_date", "b.service_category_id")

fct_membership_sales = join_membership_sales.select(
    F.col("a.fct_agreement_key").cast("string").alias("fct_agreement_key"),
    F.col("a.from_agreement_key").cast("string").alias("from_agreement_key"),
    F.col("a.prev_agreement_key").cast("string").alias("prev_agreement_key"),
    F.col("a.dim_contact_key").cast("string").alias("dim_contact_key"),
    F.col("a.dim_revenue_location_key").cast("string").alias("dim_revenue_location_key"),
    F.col("loc_f.dim_location_key").cast("string").alias("dim_location_key"),
    F.col("f.source").cast("string").alias("source"),
    F.lag("a.end_date").over(window_spec).cast("timestamp").alias("lagged_end_date"),
    F.date_format(F.col("a.signed_date"), "yyyy-MM-dd").cast("date").alias("signed_date"),
    F.date_format(F.col("ih.post_date"), "yyyy-MM-dd").cast("date").alias("post_date"),
    F.col("a.agreement_status_type").cast("string").alias("agreement_status_type"),
    F.col("a.agreement_status_group").cast("string").alias("agreement_status_group"),
    F.col("a.package_id").cast("long").alias("package_id"),
    F.col("a.monthly_fee").cast("decimal(18,4)").alias("monthly_fee"),
    F.col("b.package_type_id").cast("long").alias("package_type_id"),
    F.col("b.package_sub_type_id").cast("long").alias("package_sub_type_id"),
    F.col("b.package_term_id").cast("long").alias("package_term_id"),
    F.col("b.service_category_id").cast("long").alias("service_category_id"),
    F.col("b.name").cast("string").alias("name"),
    F.col("pst.code").cast("string").alias("package_sub_type_code"),
    F.col("e.amount").cast("decimal(18,4)").alias("invoice_detail_amount"),
    F.col("e.invoice_category_id").cast("long").alias("invoice_category_id"),
    F.col("ic.name").cast("string").alias("invoice_category_name"),
    
    # Complex CASE for renew_upgrade_revenue - cast to DECIMAL(38, 17)
    F.when(
        (F.col("a.agreement_status_group") == "Upgrade due to prepaid") & 
        (F.col("ic.name").isin("Prepaid")), 
        F.col("a.prepaid_fee") - F.col("upgrade_agt.monthly_fee")
    ).when(
        (F.col("a.agreement_status_group") == "Upgrade prepaid to prepaid") & 
        (F.coalesce(F.col("a.prepaid_fee"), F.lit(0)) == 0), 
        F.lit(0)
    ).when(
        (F.col("a.agreement_status_group") == "Upgrade prepaid to prepaid") & 
        (F.coalesce(F.col("a.prepaid_fee"), F.lit(0)) != 0) & 
        (F.col("ic.name").isin("Prepaid")), 
        F.coalesce(F.col("a.prepaid_fee"), F.lit(0)) - F.coalesce(F.col("upgrade_agt.prepaid_fee"), F.lit(0))
    ).when(
        (F.col("a.agreement_status_group") == "Renew prepaid to prepaid renew") & 
        (F.col("ic.name").isin("Prepaid")), 
        F.col("e.amount").cast("decimal(18,4)") - F.coalesce(F.col("prev_agt_invoice.amount"), F.lit(0))
    ).when(
        (F.col("a.agreement_status_group") == "Rejoin prepaid to prepaid renew") & 
        (F.col("ic.name").isin("Prepaid")), 
        F.col("e.amount").cast("decimal(18,4)") - F.coalesce(F.col("prev_agt_invoice.amount"), F.lit(0))
    ).when(
        (F.col("a.agreement_status_type").isin("Upgrade")) & 
        (F.col("ic.name").isin("Prepaid")) & 
        (F.col("b.service_category_id").isin(82, 17)), 
        F.col("e.amount").cast("decimal(18,4)") - F.coalesce(F.col("upgrade_agt_invoice.amount"), F.lit(0))
    ).when(
        (F.col("a.agreement_status_type").isin("Upgrade")) & 
        (F.col("ic.name").isin("First Month")) & 
        (F.col("b.service_category_id").isin(82, 17)), 
        F.coalesce(F.col("a.monthly_fee"), F.lit(0)) - F.coalesce(F.col("upgrade_agt.monthly_fee"), F.lit(0))
    ).when(
        (F.col("a.agreement_status_type").isin("Rejoin", "Renew")) & 
        (F.col("ic.name").isin("Prepaid")) & 
        (F.col("b.service_category_id").isin(82, 17)), 
        F.col("e.amount").cast("decimal(18,4)") - F.coalesce(F.col("prev_agt_invoice.amount"), F.lit(0))
    ).when(
        (F.col("a.agreement_status_type").isin("Rejoin", "Renew")) & 
        (F.col("ic.name").isin("First Month")) & 
        (F.col("b.service_category_id").isin(82, 17)), 
        F.coalesce(F.col("a.monthly_fee"), F.lit(0)) - F.coalesce(F.col("prev_agt.monthly_fee"), F.lit(0))
    ).cast("decimal(38,17)").alias("renew_upgrade_revenue"),
    
    # Complex CASE for first_last_month_revenue - cast to DECIMAL(19, 4)
    F.when(
        (F.col("a.agreement_status_type") == "New") & 
        (F.col("ic.name").isin("First Month")), 
        F.col("a.first_month_fee")
    ).when(
        (F.col("a.agreement_status_type") == "New") & 
        (F.col("ic.name").isin("Last Month")), 
        F.col("a.last_month_fee")
    ).when(
        (F.col("a.agreement_status_type") == "Upgrade") & 
        (F.col("ic.name").isin("First Month")), 
        F.col("a.first_month_fee") - F.col("upgrade_agt.first_month_fee")
    ).when(
        (F.col("a.agreement_status_type") == "Upgrade") & 
        (F.col("ic.name").isin("Last Month")), 
        F.col("a.last_month_fee") - F.col("upgrade_agt.last_month_fee")
    ).when(
        (F.col("f.source") == "CN") & 
        (F.col("a.agreement_status_type").isin("Rejoin", "Renew")) & 
        (F.col("ic.name").isin("First Month")), 
        F.col("a.first_month_fee") - F.col("prev_agt.first_month_fee")
    ).when(
        (F.col("f.source") == "CN") & 
        (F.col("a.agreement_status_type").isin("Rejoin", "Renew")) & 
        (F.col("ic.name").isin("Last Month")), 
        F.col("a.last_month_fee") - F.col("prev_agt.last_month_fee")
    ).when(
        (F.col("a.agreement_status_type").isin("Rejoin", "Renew")) & 
        (F.col("prev_agt_pst.name").isin("Short Term", "Membership")) & 
        (F.col("ic.name").isin("First Month")), 
        F.col("a.first_month_fee") - F.col("prev_agt.first_month_fee")
    ).when(
        (F.col("a.agreement_status_type").isin("Rejoin", "Renew")) & 
        (F.col("prev_agt_pst.name").isin("Short Term", "Membership")) & 
        (F.col("ic.name").isin("Last Month")), 
        F.col("a.last_month_fee") - F.col("prev_agt.last_month_fee")
    ).cast("decimal(19,4)").alias("first_last_month_revenue"),

    # updated on 2025.08.21. Added
    F.when(
        (F.col("ic.name").isin("Others", "Rounding")), 
        F.lit(0)
    ).when(
        (F.col("e.source") == "CN") &
        (F.col("e.amount") <= F.lit(0).cast("decimal(18,4)")), 
        F.lit(0)
    ).otherwise(F.lit(1)).alias("entitle_to_pos_discount"),
    F.coalesce(F.col("ri.raw_amount"), F.lit(0)).alias("invoice_raw_total_amount"),
    F.coalesce(F.col("reverse_invoice.reverse_amount"), F.lit(0)).alias("invoice_reversed_amount"),
    F.coalesce(F.col("pos_dis.amount"), F.lit(0)).alias("pos_discount_amount"),
    F.coalesce(F.col("ih.total_amount"), F.lit(0)).alias("invoice_header_total_amount"),
    F.when(
        (F.col("invoice_detail_amount") <= 0) | 
        (F.coalesce(F.col("ri.raw_amount"), F.lit(0)) == 0), 
        F.lit(0)
    ).otherwise(F.col("invoice_detail_amount") / F.col("ri.raw_amount")).alias("invoice_amount_ratio"),

    F.concat(F.col("e.source"), F.col("e.id").cast("bigint")).cast("string").alias("fct_invoice_detail_key"),


).select(
    F.col("fct_agreement_key").cast("string"),
    F.col("from_agreement_key").cast("string"),
    F.col("prev_agreement_key").cast("string"),
    F.col("dim_contact_key").cast("string"),
    F.col("dim_revenue_location_key").cast("string"),
    F.col("dim_location_key").cast("string"),
    F.col("source").cast("string"),
    F.col("lagged_end_date").cast("timestamp"),
    F.col("signed_date").cast("date"),
    F.col("post_date").cast("date"),
    F.col("agreement_status_type").cast("string"),
    F.col("agreement_status_group").cast("string"),
    F.col("package_id").cast("bigint"),
    F.col("monthly_fee").cast("decimal(18,4)"),
    F.col("package_type_id").cast("bigint"),
    F.col("package_sub_type_id").cast("bigint"),
    F.col("package_term_id").cast("bigint"),
    F.col("service_category_id").cast("bigint"),
    F.col("name").cast("string"),
    F.col("package_sub_type_code").cast("string"),
    F.col("invoice_category_id").cast("bigint"),
    F.col("invoice_category_name").cast("string"),
    F.col("renew_upgrade_revenue").cast("decimal(38,17)"),
    F.col("first_last_month_revenue").cast("decimal(19,4)"),
    F.col("invoice_detail_amount").alias("bk_invoice_detail_amount").cast("decimal(18,4)"),
    F.col("fct_invoice_detail_key").cast("string"),
    
    # Complex CASE for adjusted invoice_detail_amount -- account for pos discount and reverse invoice by ratio
    F.when(
        F.col("invoice_amount_ratio") == 0, 
        F.col("invoice_detail_amount")
    )
    .when(
        (F.col("pos_discount_amount") == 0) | (F.col("entitle_to_pos_discount") == 0),
        (F.col("invoice_detail_amount") + F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2)).cast(DecimalType(18, 4))
    )
    .otherwise(
        (
            F.col("invoice_detail_amount") - 
            F.abs(F.round(F.col("invoice_amount_ratio") * F.col("pos_discount_amount"), 2)) + 
            F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2)
        ).cast(DecimalType(18, 4))
    ).cast("decimal(18,4)").alias("invoice_detail_amount"),
    
    # Adjusted pos_discount_amount
    F.when(
        (F.col("pos_discount_amount") == 0) | (F.col("entitle_to_pos_discount") == 0), 
        F.lit(0)
    ).otherwise(
        F.round(F.col("invoice_amount_ratio") * F.col("pos_discount_amount"), 2).cast(DecimalType(18, 4))
    ).cast("decimal(18,4)").alias("pos_discount_amount"),
    
    # voided_amount calculation -- voided or reversed invoice amount
    F.when(
        F.col("invoice_reversed_amount") == 0, 
        F.lit(0)
    ).otherwise(
        F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2).cast(DecimalType(18, 4))
    ).cast("decimal(18,4)").alias("voided_amount")
).orderBy(
    'a.dim_contact_key',
    'signed_date'
).withColumn(
    "dim_location_key", 
    F.when(F.col("dim_location_key").isNull(), F.concat(F.col("source"), F.lit("9999")))
        .otherwise(F.col("dim_location_key"))
)



#====== [END] CELL 26 ======


#====== [START] CELL 27 ======

# display(fct_membership_sales)

#====== [END] CELL 27 ======




#====== [MARKDOWN] CELL 28 ======

#====== [START] CELL 29 ======

silver_agreement = spark.read.table(TABLE('360_silver_agreement'))
silver_invoice_detail = spark.read.table(TABLE('silver_invoice_detail'))
silver_invoice_header = spark.read.table(TABLE('silver_invoice_header'))
silver_invoice_category = spark.read.table(TABLE('silver_invoice_category'))
silver_package = spark.read.table(TABLE('360_silver_package'))
silver_package_type = spark.read.table(TABLE('silver_package_type'))
silver_package_book_limit_line = spark.read.table(TABLE('silver_package_book_limit_line'))
silver_service = spark.read.table(TABLE('silver_service'))
silver_mbo_service_category = spark.read.table(TABLE('silver_mbo_service_category'))
silver_location = spark.read.table(TABLE('silver_location'))

dim_location_360f = spark.read.table(TABLE('dim_location_360f'))


#====== [END] CELL 29 ======


#====== [START] CELL 30 ======

first_valid_agreement = (
    silver_agreement
    .filter((F.col('agreement_status_id') != 2) & (F.col('active') == 1))
    .withColumn("row_num", F.row_number().over(Window.partitionBy("source", "contact_id", F.col("package_type_id").cast("bigint")).orderBy("start_date")))
    .filter(F.col('row_num') == 1)
)


# updated on 2025.08.21. Added
pos_discount_rounding_df = (
    silver_invoice_detail.alias("id")
    .filter(F.col("active") == 1)
    .filter(F.col("item_type") == "product")
    .join(
        silver_invoice_category.alias("ic").filter(F.col("active") == 1).filter(F.col("ic.name").isin(["Others", "Rounding"])),
        F.concat(F.col("id.source"), F.col("id.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "inner"
    )
    .groupBy("id.invoice_header_id", "id.source")
    .agg(F.sum("id.amount").cast(DecimalType(18, 4)).alias("amount"))
    .withColumn("invoice_header_bk", F.concat(F.col("source"), F.col("invoice_header_id").cast("bigint")))
    .filter(F.col("amount") < F.lit(0).cast(DecimalType(18, 4)))
    .select("invoice_header_bk", "amount")
)

raw_invoice_df = (
    silver_invoice_detail.alias("id")
    .filter(F.col("active") == 1)
    .join(
        silver_invoice_category.alias("ic").filter(~F.col("ic.name").isin(["Others", "Rounding"])),
        F.concat(F.col("id.source"), F.col("id.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "inner"
    )
    .groupBy("id.invoice_header_id", "id.source")
    .agg(F.sum("id.amount").cast(DecimalType(18, 4)).alias("raw_amount"))
    .withColumn("invoice_header_bk", F.concat(F.col("source"), F.col("invoice_header_id").cast("bigint")))
    .select("invoice_header_bk", "raw_amount")
)

reverse_invoice_df = (
    silver_invoice_detail
    .filter(F.col("item_type") == "invoice_header")
    .filter(F.col("active") == 1)
    .groupBy("source", "item_id")
    .agg(F.sum("amount").alias("reverse_amount"))
    .withColumn("reverse_invoice_header_bk", F.concat(F.col("source"), F.col("item_id").cast("bigint")))
    .select("reverse_invoice_header_bk", "reverse_amount")
)


#====== [END] CELL 30 ======


#====== [START] CELL 31 ======

join_agreement_sales = (
    silver_agreement.alias("a")
    .join(
        silver_invoice_detail.alias("e"),
        (F.concat(F.col("e.source"), F.col("e.item_id").cast("bigint")) == F.col("a.bk")) & 
        (F.col("e.item_type") == "agreement") & 
        (F.col("e.active") == 1),       # updated on 2025.08.21. Added
        "inner"
    )
    .join(
        silver_invoice_header.alias("ih"),
        (F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")) == F.concat(F.col("ih.source"), F.col("ih.id").cast("bigint"))) &
        (F.col("ih.active") == 1),      # updated on 2025.08.21. Added
        "inner"
    )
    .join(
        silver_invoice_category.alias("ic").filter(~F.col("ic.name").isin(["Others", "Rounding"])),
        F.concat(F.col("e.source"), F.col("e.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "left"
    )
    .join(
        silver_package.alias("b"),
        F.concat(F.col("a.source"), F.col("a.package_id")) == F.col("b.bk"),
        "left"
    )
    .join(
        silver_package_type.alias("pt"),
        (F.concat(F.col("b.source"), F.col("b.package_type_id")) == F.col("pt.bk")) &
        (F.col("pt.active") == 1),      # updated on 2025.08.21. Added
        "left"
    )
    .join(
        silver_package_book_limit_line.alias("c"),
        F.col("a.package_id") == F.col("c.package_id"),
        "left"
    )
    .join(
        silver_service.alias("s"),
        F.concat(F.col("b.source"), F.col("b.ext_ref_package_id")) == F.concat(F.col("s.source"), F.col("s.service_id")),
        "left"
    )
    .join(
        silver_mbo_service_category.alias("mbo_service_cat"),
        F.concat(F.col("b.source"), F.col("b.service_category_id")) == F.concat(F.col("mbo_service_cat.source"), F.col("mbo_service_cat.service_category_id")),
        "left"
    )
    .join(
        silver_location.alias("f"),
        F.concat(F.col("a.source"), F.col("a.revenue_location_id")) == F.col("f.bk"),
        "left"
    )
    .join(
        dim_location_360f.alias("loc_f"),
        F.col("loc_f.dim_location_360f_key") == F.col("f.bk"),
        "left"
    )
    .join(
        first_valid_agreement.alias("first_agmt"),
        (F.concat(F.col("first_agmt.source"), F.col("first_agmt.contact_id"), F.col("first_agmt.package_type_id").cast("bigint")) == F.concat(F.col("a.source"), F.col("a.contact_id"), F.col("a.package_type_id").cast("bigint"))) &
        (F.col("a.start_date") > F.col("first_agmt.start_date")) & 
        (F.col("a.bk") != F.col("first_agmt.bk")),
        "left"
    )

    # updated on 2025.08.21. Added
    .join(
        pos_discount_rounding_df.alias("pos_dis"),
        F.col("pos_dis.invoice_header_bk") == F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")),
        "left"
    )
    .join(
        raw_invoice_df.alias("ri"),
        F.col("ri.invoice_header_bk") == F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")),
        "left"
    )
    .join(
        reverse_invoice_df.alias("reverse_invoice"),
        F.col("reverse_invoice.reverse_invoice_header_bk") == F.concat(F.col("e.source"), F.col("e.invoice_header_id").cast("bigint")),
        "left"
    )
    # updated end

    .select(
        F.col("a.bk").cast("string").alias("fct_agreement_key"),
        F.concat(F.col("a.source"), F.col("a.contact_id")).cast("string").alias("dim_contact_key"),
        F.col("a.contact_id").cast("bigint").alias("contact_id"),
        F.col("f.source").cast("string").alias("source"),
        F.col("f.bk").cast("string").alias("dim_revenue_location_key"),
        F.col("loc_f.dim_location_key").cast("string").alias("dim_location_key"),
        F.col("a.start_date").alias("start_date"),
        F.col("a.end_date").alias("end_date"),
        F.to_date(F.col("a.signed_date")).alias("signed_date"),
        F.to_date(F.col("ih.post_date")).alias("post_date"),
        F.col("a.package_id").cast("bigint").alias("package_id"),
        F.col("b.package_type_id").cast("bigint").alias("package_type_id"),
        F.col("pt.name").cast("string").alias("package_type_name"),
        F.col("pt.ext_ref_package_type_id").cast("string").alias("ext_ref_package_type_id"),
        F.col("b.package_sub_type_id").cast("decimal(38,18)").alias("package_sub_type_id"),
        F.col("b.package_term_id").cast("bigint").alias("package_term_id"),
        F.col("b.service_category_id").cast("bigint").alias("service_category_id"),
        F.col("mbo_service_cat.service_category_name").cast("string").alias("service_category_name"),
        F.col("s.service_name").cast("string").alias("service_name"),
        F.col("b.is_pos").cast("int").alias("is_pos"),
        F.when(
            (F.col("a.source") == "SG") &
            (F.col("first_agmt.bk").isNull()) &
            (F.col("a.package_type_id").isin(2, 3, 10)) &           # updated on 2025.08.21. Added #10
            (F.col("b.is_pos").cast("int") == 0), 
            1
        ).when(
            (F.col("a.source") == "SG") &
            (F.col("a.package_type_id").isin(2, 3, 10)),            # updated on 2025.08.21. Added #10
            0
        ).otherwise(F.col("b.is_new").cast("int")).cast("int").alias("is_new"),
        F.when(
            (F.col("a.source") == "SG") &
            (F.col("first_agmt.bk").isNotNull()) &
            (F.col("a.package_type_id").isin(2, 3, 10)) &           # updated on 2025.08.21. Added #10
            (F.col("is_pos").cast("int") == 0), 
            1
        ).when(
            (F.col("a.source") == "SG") &
            (F.col("a.package_type_id").isin(2, 3, 10)),            # updated on 2025.08.21. Added #10
            0
        ).otherwise(F.col("b.is_renew").cast("int")).cast("int").alias("is_renew"),
        # F.col("b.is_new").cast("int").alias("is_new"),
        # F.col("b.is_renew").cast("int").alias("is_renew"),
        F.col("b.name").cast("string").alias("name"),
        F.col("b.min_committed_month").alias("min_committed_month"),
        F.col("e.amount").cast("decimal(38,18)").alias("invoice_detail_amount"),
        F.col("e.invoice_category_id").cast("decimal(38,18)").alias("invoice_category_id"),
        F.col("ic.ext_ref_invoice_category_id").cast("string").alias("ext_ref_invoice_category_id"),

        # updated on 2025.08.21. Added
        F.col("pos_dis.amount").cast("decimal(38,18)").alias("pos_discount_amount"),
        F.col("ri.raw_amount").alias("invoice_raw_total_amount"),
        F.when(
            F.col("ic.name").isin(["Others", "Rounding"]), 
            0
        )
        .when(
            (F.col("e.source") == "CN") & 
            (F.col("e.amount") <= F.lit(0).cast("decimal(18, 4)")), 
            0
        ).otherwise(1).alias("entitle_to_pos_discount"),
        F.coalesce(F.col("reverse_invoice.reverse_amount"), F.lit(0)).alias("reversed_amount"),
        F.col("ih.total_amount").alias("invoice_header_total_amount"),
        F.concat(F.col("e.source"), F.col("e.id").cast("bigint")).cast("string").alias("fct_invoice_detail_key"),
    )
)

#====== [END] CELL 31 ======


#====== [START] CELL 32 ======

agg_agreement_sales = (
    join_agreement_sales.groupBy(
        "fct_agreement_key",
        "dim_contact_key",
        "contact_id",
        "dim_revenue_location_key",
        "dim_location_key",
        "source",
        "signed_date",
        "post_date",
        "package_id",
        "package_type_id",
        "package_type_name",
        "ext_ref_package_type_id",
        "package_sub_type_id",
        "package_term_id",
        F.col("is_pos").cast("int"),
        F.col("is_new").cast("int"),
        F.col("is_renew").cast("int"),
        "service_category_id",
        "service_category_name",
        "service_name",
        "name",
        "invoice_category_id",
        "ext_ref_invoice_category_id",
        "entitle_to_pos_discount",
        "fct_invoice_detail_key"
    )
    .agg(
        F.count("fct_agreement_key").cast("bigint").alias("n_agreement"),
        F.sum("invoice_detail_amount").cast("decimal(38,18)").alias("total_revenue_agreement"),

        # updated on 2025.08.21. Added
        F.coalesce(F.sum("invoice_raw_total_amount"), F.lit(0)).alias("invoice_raw_total_amount"),
        F.coalesce(F.sum("reversed_amount"), F.lit(0)).alias("invoice_reversed_amount"),
        F.coalesce(F.sum("pos_discount_amount"), F.lit(0)).alias("pos_discount_amount"),
        F.when(
            (F.sum("invoice_detail_amount") <= 0) | 
            (F.sum("invoice_raw_total_amount") == 0), 
            0
        ).otherwise(
            F.sum("invoice_detail_amount") / F.sum("invoice_raw_total_amount")
        ).alias("invoice_amount_ratio"),
    )
)

fct_agreement_sales = (
    agg_agreement_sales.select(
        F.col("fct_agreement_key").cast("string"),
        F.col("dim_contact_key").cast("string"),
        F.col("contact_id").cast("bigint"),
        F.col("dim_revenue_location_key").cast("string"),
        F.col("dim_location_key").cast("string"),
        F.col("source").cast("string"),
        F.col("signed_date").cast("date"),
        F.col("post_date").cast("date"),
        F.col("package_id").cast("bigint"),
        F.col("package_type_id").cast("bigint"),
        F.col("package_type_name").cast("string"),
        F.col("ext_ref_package_type_id").cast("string"),
        F.col("package_sub_type_id").cast("decimal(38, 18)"),
        F.col("package_term_id").cast("bigint"),
        F.col("is_pos").cast("int"),
        F.col("is_new").cast("int"),
        F.col("is_renew").cast("int"),
        F.col("service_category_id").cast("bigint"),
        F.col("service_category_name").cast("string"),
        F.col("service_name").cast("string"),
        F.col("name").cast("string"),
        F.col("invoice_category_id").cast("decimal(38, 18)"),
        F.col("ext_ref_invoice_category_id").cast("string"),
        F.col("n_agreement").cast("bigint"),
        F.col("invoice_amount_ratio").cast("decimal(38, 10)"),
        F.col("total_revenue_agreement").cast("decimal(28, 4)").alias("bk_total_revenue_agreement"),
        F.col("fct_invoice_detail_key").cast("string"),

        # Complex CASE for adjusted total_revenue_agreement
        F.when(
            F.col("invoice_amount_ratio") == 0, 
            F.col("total_revenue_agreement")
        ).when(
            (F.col("pos_discount_amount") == 0) | (F.col("entitle_to_pos_discount") == 0),
            (F.col("total_revenue_agreement") + F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2)).cast("decimal(38,18)")
        ).otherwise(
            (
                F.col("total_revenue_agreement") - 
                F.abs(F.round(F.col("invoice_amount_ratio") * F.col("pos_discount_amount"), 2)) + 
                F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2)
            ).cast("decimal(38,18)")
        ).cast("decimal(28, 4)").alias("total_revenue_agreement"),
        
        # Adjusted pos_discount_amount
        F.when(
            (F.col("pos_discount_amount") == 0) | (F.col("entitle_to_pos_discount") == 0), 
            0
        ).otherwise(
            F.round(F.col("invoice_amount_ratio") * F.col("pos_discount_amount"), 2).cast("decimal(38,18)")
        ).cast("decimal(18, 4)").alias("pos_discount_amount"),
        
        # voided_amount calculation
        F.when(
            F.col("invoice_reversed_amount") == 0, 
            0
        ).otherwise(
            F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2).cast("decimal(38,18)")
        ).cast("decimal(18, 4)").alias("voided_amount")
    )
    .withColumn(
        "dim_location_key", 
        F.when(F.col("dim_location_key").isNull(), F.concat(F.col("source"), F.lit("9999")))
         .otherwise(F.col("dim_location_key"))
    )
)

#====== [END] CELL 32 ======


#====== [START] CELL 33 ======

# join_agreement_sales.limit(5).display()

#====== [END] CELL 33 ======




#====== [MARKDOWN] CELL 34 ======

#====== [START] CELL 35 ======

silver_invoice_detail = spark.read.table(TABLE('silver_invoice_detail'))
silver_invoice_header = spark.read.table(TABLE('silver_invoice_header'))
silver_product = spark.read.table(TABLE('silver_product'))
silver_product_category = spark.read.table(TABLE('silver_product_category'))
silver_location = spark.read.table(TABLE('silver_location'))
dim_location_360f = spark.read.table(TABLE('dim_location_360f'))

silver_agreement = spark.read.table(TABLE('360_silver_agreement'))

# 360_silver_agreement.bk < silver_invoice_detail.source & silver_invoice_detail.item_id


#====== [END] CELL 35 ======


#====== [START] CELL 36 ======

# updated on 2025.08.22. Added
pos_discount_rounding_df = (
    silver_invoice_detail.alias("id")
    .filter(F.col("active") == 1)
    .filter(F.col("item_type") == "product")
    .join(
        silver_invoice_category.alias("ic").filter(F.col("active") == 1).filter(F.col("ic.name").isin(["Others", "Rounding"])),
        F.concat(F.col("id.source"), F.col("id.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "inner"
    )
    .groupBy("id.invoice_header_id", "id.source")
    .agg(F.sum("id.amount").cast(DecimalType(18, 4)).alias("amount"))
    .withColumn("invoice_header_bk", F.concat(F.col("source"), F.col("invoice_header_id").cast("bigint")))
    .filter(F.col("amount") < F.lit(0).cast(DecimalType(18, 4)))
    .select("invoice_header_bk", "amount")
)

raw_invoice_df = (
    silver_invoice_detail.alias("id")
    .filter(F.col("active") == 1)
    .join(
        silver_invoice_category.alias("ic").filter(~F.col("ic.name").isin(["Others", "Rounding"])),
        F.concat(F.col("id.source"), F.col("id.invoice_category_id").cast("bigint")) == F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")),
        "inner"
    )
    .groupBy("id.invoice_header_id", "id.source")
    .agg(F.sum("id.amount").cast(DecimalType(18, 4)).alias("raw_amount"))
    .withColumn("invoice_header_bk", F.concat(F.col("source"), F.col("invoice_header_id").cast("bigint")))
    .select("invoice_header_bk", "raw_amount")
)

reverse_invoice_df = (
    silver_invoice_detail
    .filter(F.col("item_type") == "invoice_header")
    .filter(F.col("active") == 1)
    .groupBy("source", "item_id")
    .agg(F.sum("amount").alias("reverse_amount"))
    .withColumn("reverse_invoice_header_bk", F.concat(F.col("source"), F.col("item_id").cast("bigint")))
    .select("reverse_invoice_header_bk", "reverse_amount")
)


#====== [END] CELL 36 ======


#====== [START] CELL 37 ======

join_product_sales = (
    silver_invoice_detail.alias("id")
    .filter(F.col("id.active") == 1)
    .filter(F.col("id.item_type") == "product")
    .join(
        silver_agreement.alias("a"),
        F.concat(F.col("id.source"), F.col("id.item_id").cast("bigint")) == F.col("a.bk"),
        "left"
    )
    .join(
        silver_invoice_header.alias("ih"),
        (F.concat(F.col("id.source"), F.col("id.invoice_header_id")) == F.concat(F.col("ih.source"), F.col("ih.id"))) & 
        (F.col("ih.active") == 1) &                                # updated on 2025.08.22. Added
        (F.col("ih.total_amount") >= 0),                            # updated on 2025.08.22. Added
        "inner"
    )
    .join(
        silver_product.alias("p"),
        (F.concat(F.col("id.source"), F.col("id.item_id").cast("bigint")) == F.concat(F.col("p.source"), F.col("p.id").cast("bigint"))) & 
        (F.col("p.active") == 1),                                   # updated on 2025.08.22. Added
        "inner"
    )
    .join(                                                          # updated on 2025.08.22. Added
        silver_invoice_category.alias("ic"),
        (F.concat(F.col("ic.source"), F.col("ic.id").cast("bigint")) == F.concat(F.col("id.source"), F.col("id.invoice_category_id").cast("bigint"))) &
        (F.col("ic.active") == 1) & 
        ~(F.col("ic.name").isin(["Others", "Rounding"])), 
        "inner"
    )
    .join(
        silver_product_category.alias("pc"),
        (F.concat(F.col("p.source"), F.col("p.product_category_id").cast("bigint")) == F.concat(F.col("pc.source"), F.col("pc.id").cast("bigint"))) &
        (F.col("pc.active") == 1),                                 # updated on 2025.08.22. Added
        "left"
    )
    .join(
        silver_location.alias("f"),
        (F.concat(F.col("id.source"), F.col("id.location_id").cast("bigint")) == F.col("f.bk")) &
        (F.col("f.active") == 1),                                  # updated on 2025.08.22. Added
        "left"
    )
    .join(
        dim_location_360f.alias("loc_f"),
        F.col("loc_f.dim_location_360f_key") == F.col("f.bk"),
        "left"
    )

    # updated on 2025.08.22. Added
    .join(
        pos_discount_rounding_df.alias("pos_dis"),
        F.col("pos_dis.invoice_header_bk") == F.concat(F.col("id.source"), F.col("id.invoice_header_id").cast("bigint")),
        "left"
    )
    .join(
        raw_invoice_df.alias("ri"),
        F.col("ri.invoice_header_bk") == F.concat(F.col("id.source"), F.col("id.invoice_header_id").cast("bigint")),
        "left"
    )
    .join(
        reverse_invoice_df.alias("reverse_invoice"),
        F.col("reverse_invoice.reverse_invoice_header_bk") == F.concat(F.col("id.source"), F.col("id.invoice_header_id").cast("bigint")),
        "left"
    )
    # update end 

    .select(
        F.col("a.bk").cast("string").alias("fct_agreement_key"),
        F.concat(F.col("a.source"), F.col("a.contact_id")).cast("string").alias("dim_contact_key"),

        F.col("p.id").cast("bigint").alias("product_id"),
        F.col("p.name").cast("string").alias("product_name"),
        F.col("p.product_category_id").cast("bigint").alias("product_category_id"),
        F.col("pc.name").cast("string").alias("product_category_name"),
        F.col("loc_f.dim_location_key").cast("string").alias("dim_location_key"),
        F.col("id.invoice_category_id").cast("bigint").alias("invoice_category_id"),
        F.col("id.amount").cast("decimal(18,4)").alias("invoice_detail_amount"),
        F.col("id.source").cast("string").alias("source"),
        F.col("ih.post_date").cast("date").alias("post_date"),

        # updated on 2025.08.22. Added
        F.concat(F.col("id.source"), F.col("id.id").cast("bigint")).cast("string").alias("fct_invoice_detail_key"),
        F.col("ih.total_amount").cast("decimal(18,4)").alias("invoice_total_amount"),
        F.col("pos_dis.amount").cast(DecimalType(18, 4)).alias("pos_discount_amount"),
        F.col("ri.raw_amount").cast(DecimalType(18, 4)).alias("invoice_raw_total_amount"),

        F.when(
            F.col("ic.name").isin(["Others", "Rounding"]), 
            0
        ).when(
            (F.col("id.source") == "CN") & 
            (F.col("id.amount") <= F.lit(0).cast(DecimalType(18, 4))), 
            0
        ).otherwise(1).cast("int").alias("entitle_to_pos_discount"),
        
        F.coalesce(F.col("reverse_invoice.reverse_amount"), F.lit(0)).cast(DecimalType(18, 4)).alias("reversed_amount"),
        
        F.col("ih.total_amount").cast(DecimalType(18, 4)).alias("invoice_header_total_amount")
        # update end 

    )
)

#====== [END] CELL 37 ======


#====== [START] CELL 38 ======

fct_product_sales = (
    join_product_sales.groupBy(
        "fct_agreement_key",
        "dim_contact_key",
        "product_id",
        "product_name",
        "product_category_id",
        "product_category_name",
        "dim_location_key",
        "invoice_category_id",
        "source",
        "post_date",
        "fct_invoice_detail_key",               # updated on 2025.08.22. Added
        "entitle_to_pos_discount",              # updated on 2025.08.22. Added
    )
    .agg(
        F.sum("invoice_detail_amount").cast("decimal(28,4)").alias("total_revenue_product"),

        # updated on 2025.08.22. Added
        F.coalesce(F.sum("invoice_raw_total_amount"), F.lit(0)).cast(DecimalType(18, 4)).alias("invoice_raw_total_amount"),
        F.coalesce(F.sum("reversed_amount"), F.lit(0)).cast(DecimalType(18, 4)).alias("invoice_reversed_amount"),
        F.coalesce(F.sum("pos_discount_amount"), F.lit(0)).cast(DecimalType(18, 4)).alias("pos_discount_amount"),
        
        F.when(
            (F.sum("invoice_detail_amount") <= 0) | 
            (F.sum("invoice_raw_total_amount") == 0), 
            F.lit(0)
        ).otherwise(
            (F.sum("invoice_detail_amount") / F.sum("invoice_raw_total_amount")).cast(DecimalType(18, 10))
        ).cast(DecimalType(38, 10)).alias("invoice_amount_ratio"),
    )

    # updated on 2025.08.22. Added
    .select(
        F.col("fct_agreement_key").cast("string"),
        F.col("dim_contact_key").cast("string"),
        F.col("fct_invoice_detail_key").cast("string"),
        F.col("product_id").cast("bigint"),
        F.col("product_name").cast("string"),
        F.col("product_category_id").cast("bigint"),
        F.col("product_category_name").cast("string"),
        F.col("dim_location_key").cast("string"),
        F.col("invoice_category_id").cast("bigint"),
        F.col("source").cast("string"),
        F.col("post_date").cast("timestamp"),
        F.col("total_revenue_product").cast(DecimalType(28, 4)).alias("bk_total_revenue_product"),
        
        # Complex CASE for adjusted total_revenue_product
        F.when(
            F.col("invoice_amount_ratio") == 0, 
            F.col("total_revenue_product")
        ).when(
            (F.col("pos_discount_amount") == 0) | 
            (F.col("entitle_to_pos_discount") == 0),
            (F.col("total_revenue_product") + F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2)).cast(DecimalType(18, 4))
        ).otherwise(
            (
                F.col("total_revenue_product") - 
                F.abs(F.round(F.col("invoice_amount_ratio") * F.col("pos_discount_amount"), 2)) + 
                F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2)
            ).cast(DecimalType(18, 4))
        ).cast(DecimalType(28, 4)).alias("total_revenue_product"),
        
        F.when(
            (F.col("pos_discount_amount") == 0) | 
            (F.col("entitle_to_pos_discount") == 0), 
            0
        ).otherwise(
            F.round(F.col("invoice_amount_ratio") * F.col("pos_discount_amount"), 2).cast(DecimalType(18, 4))
        ).cast(DecimalType(18, 4)).alias("pos_discount_amount"),
        
        # voided_amount calculation
        F.when(
            F.col("invoice_reversed_amount") == 0, 
            0
        ).otherwise(
            F.round(F.col("invoice_amount_ratio") * F.col("invoice_reversed_amount"), 2).cast(DecimalType(18, 4))
        ).cast(DecimalType(18, 4)).alias("voided_amount")
    )
    .withColumn(
        "dim_location_key", 
        F.when(F.col("dim_location_key").isNull(), F.concat(F.col("source"), F.lit("9999")))
         .otherwise(F.col("dim_location_key"))
    )
)

#====== [END] CELL 38 ======


#====== [START] CELL 39 ======

# fct_product_sales.limit(5).display()

#====== [END] CELL 39 ======




#====== [MARKDOWN] CELL 40 ======

#====== [START] CELL 41 ======

silver_payment_detail = spark.read.table(TABLE('silver_payment_detail'))
silver_payment_header = spark.read.table(TABLE('silver_payment_header'))
silver_invoice_header = spark.read.table(TABLE('silver_invoice_header'))
silver_location = spark.read.table(TABLE('silver_location'))
dim_location_360f = spark.read.table(TABLE('dim_location_360f'))


#====== [END] CELL 41 ======


#====== [START] CELL 42 ======

join_outstanding = (
    silver_payment_detail.alias("pd")
    .join(
        silver_payment_header.alias("ph"),
        F.concat(F.col("pd.source"), F.col("pd.payment_header_id").cast("bigint")) == F.concat(F.col("ph.source"), F.col("ph.id").cast("bigint")),
    )
    .join(
        silver_invoice_header.alias("ih"),
        F.concat(F.col("pd.source"), F.col("pd.invoice_header_id").cast("bigint")) == F.concat(F.col("ih.source"), F.col("ih.id").cast("bigint")),
    )
    .join(
        silver_location.alias("f"),
        F.concat(F.col("ph.source"), F.col("ph.location_id").cast("bigint")) == F.col("f.bk"),
        "left"
    )
    .join(
        dim_location_360f.alias("loc_f"),
        F.col("loc_f.dim_location_360f_key") == F.col("f.bk"),
        "left"
    )
    .filter(F.col("pd.payment_category_id") == 6)
    .select(
        F.col("pd.bk").alias("bk"),
        F.col("pd.source").cast("string").alias("source"),
        F.concat(F.col("pd.source"), F.col("pd.payment_header_id").cast("bigint")).alias("fct_payment_detail_key"),
        F.concat(F.col("pd.source"), F.col("pd.invoice_header_id").cast("bigint")).alias("fct_invoice_detail_key"),
        F.col("ph.post_date").cast("timestamp").alias("post_date"),
        F.col("pd.amount").cast("decimal(18,4)").alias("amount"),
        F.col("loc_f.dim_location_key").cast("string").alias("dim_location_key"),
        F.col("ph.location_id").cast("bigint").alias("location_id"),
    )
    .withColumn(
        "dim_location_key", 
        F.when(F.col("dim_location_key").isNull(), F.concat(F.col("source"), F.lit("9999")))
         .otherwise(F.col("dim_location_key"))
    )
)

#====== [END] CELL 42 ======


#====== [START] CELL 43 ======

fct_outstanding = join_outstanding.groupBy(
                        "source",
                        "fct_payment_detail_key",
                        "fct_invoice_detail_key",
                        # "amount",
                        "post_date",
                        "dim_location_key",
                    ).agg(
                        F.sum("amount").cast("decimal(28,4)").alias("total_amount")
                    )
# fct_outstanding.display()

#====== [END] CELL 43 ======




#====== [MARKDOWN] CELL 44 ======

#====== [START] CELL 45 ======

# fct_agreement_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_fpa_revenue_agreement_sales")


#====== [END] CELL 45 ======


#====== [START] CELL 46 ======

# fct_agreement.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_360_agreement")

# fct_membership_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_fpa_revenue_membership_sales")
# fct_agreement_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_fpa_revenue_agreement_sales")
# fct_product_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_fpa_revenue_product_sales")
# fct_outstanding.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_fpa_outstanding")


#====== [END] CELL 46 ======




#====== [MARKDOWN] CELL 47 ======

#====== [START] CELL 48 ======

# membership_sales = spark.read.table('leg_uat.lakehouse.fct_fpa_revenue_membership_sales')
# agreement_sales = spark.read.table('leg_uat.lakehouse.fct_fpa_revenue_agreement_sales')
# product_sales = spark.read.table('leg_uat.lakehouse.fct_fpa_revenue_product_sales')
# outstanding = spark.read.table('leg_uat.lakehouse.fct_fpa_outstanding')

membership_sales = fct_membership_sales
agreement_sales = fct_agreement_sales
product_sales = fct_product_sales
outstanding = fct_outstanding

rate = spark.read.table('leg_uat.lakehouse.rpt_exchange_rate')


#====== [END] CELL 48 ======




#====== [MARKDOWN] CELL 49 ======

#====== [START] CELL 50 ======

exchange_rate = (
    rate
    # .filter(F.col("date") >= f'{last_year}-01-01')
    .groupBy("date", "basesymbol", "region")
    .pivot("transactionsymbol", ["USD", "HKD"])
    .agg(F.first("rate"))  # Or F.max("rate"), depending on your needs
    .withColumnRenamed("USD", "transaction_USD")
    .withColumnRenamed("HKD", "transaction_HKD")
    .withColumn(
        "transaction_HKD", 
        F.when(
            F.col("basesymbol") == "HKD", 
            F.lit(1)
        ).otherwise(F.col("transaction_HKD"))
    )
    .orderBy("date", "basesymbol")
)



#====== [END] CELL 50 ======


#====== [START] CELL 51 ======

# exchange_rate.filter(F.col("date") >= f'{last_year}-01-01').display()

#====== [END] CELL 51 ======


#====== [START] CELL 52 ======

# display(membership_sales.filter(F.col("dim_contact_key") == "HK231452"))

#====== [END] CELL 52 ======




#====== [MARKDOWN] CELL 53 ======

#====== [START] CELL 54 ======

nmu_membership_sales =(
    membership_sales.alias('sales')
    .join(
        accounts_nmu_ym.alias('nmu'),
        F.col('sales.dim_contact_key') == F.col('nmu.dim_contact_key'), 
        'left'
    )
    .select(
        "sales.*",
        # "nmu.dim_accounts_id",
        "nmu.success_date",
        "nmu.nmu_ym",
    )
    # .filter(F.col("post_date") >= f"{last_year}-01-01")
    .withColumn(
        "post_ym",
        F.date_format(F.col('post_date'), 'yyyyMM')
    )
    .withColumn(
        "nmu_ind",
        F.when(
            (F.col("nmu_ym").isNull()), 
            99
        )
        .when(
            (F.col("nmu_ym") == F.date_format(F.col('post_date'), 'yyyyMM')), 
            1
        )
        .otherwise(0)
    )
)


#====== [END] CELL 54 ======


#====== [START] CELL 55 ======

flag_membership_sales = (
    nmu_membership_sales
    .withColumn("137_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("invoice_category_name") == "Joining Fee") &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("agreement_status_group").isin(["Unknown"]))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("invoice_category_name") == "Joining Fee") &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("agreement_status_group").isin(["Unknown"]))
            , 1
        ).otherwise(0)
    )
    .withColumn("139_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("invoice_category_name").isin(["Prepaid", "Others", "Rounding"])) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("package_term_id") == 2) &
            (F.col("agreement_status_group").isin(["Unknown"])) &
            (~F.col("service_category_id").isin(82))
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("invoice_category_name").isin(["Prepaid", "Others", "Rounding"])) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("agreement_status_group").isin(["Unknown"])) &
            (~F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("package_term_id") == 2) &
            (F.col("agreement_status_group").isin(["Unknown"])) &
            (~F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("140_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("invoice_category_name").isin(["Prepaid", "Others", "Rounding"])) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("package_term_id") == 2) &
            (F.col("agreement_status_group").isin(["Unknown"])) &
            (F.col("service_category_id").isin(82))
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("invoice_category_name").isin(["Prepaid", "Others", "Rounding"])) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("agreement_status_group").isin(["Unknown"])) &
            (F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("package_term_id") == 2) &
            (F.col("agreement_status_group").isin(["Unknown"])) &
            (F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("141_flag", 
        F.lit(0)
    )
    .withColumn("142_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("package_sub_type_code").isin(["ST"])) &
            (F.col("agreement_status_type").isin(["New"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","First Month","Last Month","Pro-rata","Others","Rounding"]))
            , 1
        ).when(
            (F.col("source").isin(["CN"]))
            , 0
        ).otherwise(0)
    )
    .withColumn("144_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type") == "New") &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (~F.col("service_category_id").isin(82))
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type") == "New") &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"]))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type") == "New") &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["Membership Sales"]))
            , 1
        ).otherwise(0)
    )
    .withColumn("145_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["Rejoin", "Renew", "Upgrade"])) &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (~F.col("service_category_id").isin(82))
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["Rejoin", "Renew", "Upgrade"])) &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (~F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["Rejoin","Renew","Upgrade"])) &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (~F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("146_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type") == "New") &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (F.col("service_category_id").isin(82))
            , 1
        ).when(
            (F.col("source").isin(["CN", "SG"]))
            , 0
        ).otherwise(0)
    )
    .withColumn("147_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (F.col("service_category_id").isin(82))
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("148_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (F.col("service_category_id").isin(82))
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["First Month", "Last Month", "Rounding", "Others"])) &
            (F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("package_term_id") == 1) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("agreement_status_group") == "Unknown") &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("149_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("invoice_category_name") == "Pro-rata")
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("invoice_category_name") == "Pro-rata")
            , 1
        ).otherwise(0)
        ) 
    .withColumn("151_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","First Month", "Last Month","Pro-rata" ,"Others", "Rounding"])) &
            (F.col("service_category_id").isin(82))
            , 0                     # Fields not ready yet
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","First Month", "Last Month","Pro-rata" ,"Others", "Rounding"])) &
            (F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("152_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","First Month", "Last Month","Pro-rata" ,"Others", "Rounding"])) &
            (F.col("agreement_status_group").isin(["Renew prepaid to prepaid renew", "Rejoin prepaid to prepaid renew","Renew prepaid"])) &
            (~F.col("service_category_id").isin(82))
            , 0                     # Fields not ready yet
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","First Month", "Last Month","Pro-rata" ,"Others", "Rounding"])) &
            (F.col("agreement_status_group").isin(["Renew prepaid to prepaid renew", "Rejoin prepaid to prepaid renew","Renew prepaid"])) &
            (~F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("agreement_status_type").isin(["Renew", "Rejoin"])) &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (F.col("agreement_status_group").isin(["Renew prepaid to prepaid renew", "Rejoin prepaid to prepaid renew","Renew prepaid"])) &
            (~F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("154_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group").isin(["Upgrade due to prepaid"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","Others", "Rounding"])) &
            (~F.col("service_category_id").isin(82))
            , 0                     # Fields not ready yet
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group").isin(["Upgrade due to prepaid"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","Others", "Rounding"])) &
            (~F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group").isin(["Upgrade due to prepaid"])) &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (~F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("155_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (~F.col("agreement_status_group").isin(["Unknown"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","First Month", "Last Month","Pro-rata" ,"Others", "Rounding"])) &
            (F.col("service_category_id").isin(82))
            , 0                     # Fields not ready yet
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (~F.col("agreement_status_group").isin(["Unknown"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","First Month", "Last Month","Pro-rata" ,"Others", "Rounding"])) &
            (F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("agreement_status_type") == "Upgrade") &
            (~F.col("agreement_status_group").isin(["Unknown"])) &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("156_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group") == "Upgrade prepaid to prepaid") &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","Others", "Rounding"])) &
            (~F.col("service_category_id").isin(82))
            , 0                     # Fields not ready yet
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group") == "Upgrade prepaid to prepaid") &
            (F.col("invoice_category_name").isin(["Joining Fee","Prepaid","Others", "Rounding"])) &
            (~F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group") == "Upgrade prepaid to prepaid") &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (~F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("157_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group").isin(["Upgrade due to due", "Upgrade prepaid to due"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","First Month", "Last Month","Pro-rata","Others","Rounding"])) &
            (~F.col("service_category_id").isin(82))
            , 0                     # Fields not ready yet
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("package_sub_type_code").isin(["MEM", "ST"])) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group").isin(["Upgrade due to due", "Upgrade prepaid to due"])) &
            (F.col("invoice_category_name").isin(["Joining Fee","First Month", "Last Month","Pro-rata","Others","Rounding"])) &
            (~F.col("service_category_id").isin(77))
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("agreement_status_type") == "Upgrade") &
            (F.col("agreement_status_group").isin(["Upgrade due to due", "Upgrade prepaid to due"])) &
            (F.col("invoice_category_name").isin(["Membership Sales"])) &
            (~F.col("service_category_id").isin(21))
            , 1
        ).otherwise(0)
    )
    .withColumn("201_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("package_sub_type_code").isin(["MEM"])) &
            (F.col("invoice_category_name") == "Autopay")
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("package_type_id") == 1) &
            (F.col("invoice_category_name") == "Autopay")
            , 1
        ).otherwise(0)
    ) 
)


#====== [END] CELL 55 ======


#====== [START] CELL 56 ======

flagchk_membership_sales = flag_membership_sales.withColumn(
    "multiple_flags_count",
    (F.col("137_flag").cast("int") + F.col("139_flag").cast("int") + F.col("140_flag").cast("int") + F.col("141_flag").cast("int") + 
     F.col("142_flag").cast("int") + F.col("144_flag").cast("int") + F.col("145_flag").cast("int") + F.col("146_flag").cast("int") + 
     F.col("147_flag").cast("int") + F.col("148_flag").cast("int") + F.col("149_flag").cast("int") + F.col("151_flag").cast("int") + 
     F.col("152_flag").cast("int") + F.col("154_flag").cast("int") + F.col("155_flag").cast("int") + F.col("156_flag").cast("int") + 
     F.col("157_flag").cast("int") + F.col("201_flag").cast("int"))
).withColumn(
    "has_multiple_flags",
    F.when(F.col("multiple_flags_count") > 1, True).otherwise(False)
)
# ).filter(F.col("has_multiple_flags") == True).display()

#====== [END] CELL 56 ======


#====== [START] CELL 57 ======

# # Display rows where each flag equals 1
# flag_membership_sales.filter(F.col("137_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("139_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("140_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("141_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("142_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("144_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("145_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("146_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("147_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("148_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("149_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("151_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("152_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("154_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("155_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("156_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("157_flag") == 1).limit(5).display()
# flag_membership_sales.filter(F.col("201_flag") == 1).limit(5).display()

#====== [END] CELL 57 ======


#====== [START] CELL 58 ======

# flagchk_membership_sales = flagchk_membership_sales.filter(F.col('multiple_flags_count') > 1).display()
# flag_membership_sales_nmu.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.test_flag_membership_sales_nmu")


#====== [END] CELL 58 ======


#====== [START] CELL 59 ======

# display(flag_membership_sales)


#====== [END] CELL 59 ======




#====== [MARKDOWN] CELL 60 ======

#====== [START] CELL 61 ======

nmu_agreement_sales =(
    agreement_sales.alias('sales')
    .join(
        accounts_nmu_ym.alias('nmu'),
        F.col('sales.dim_contact_key') == F.col('nmu.dim_contact_key'), 
        'left'
    )
    .select(
        "sales.*",
        "nmu.success_date",
        "nmu.nmu_ym",
    )
    # .filter(F.col("post_date") >= f"{last_year}-01-01")
    .withColumn(
        "post_ym",
        F.date_format(F.col('post_date'), 'yyyyMM')
    )
    .withColumn(
        "nmu_ind",
        F.when(
            (F.col("nmu_ym").isNull()), 
            99
        )
        .when(
            (F.col("nmu_ym") == F.date_format(F.col('post_date'), 'yyyyMM')), 
            1
        )
        .otherwise(0)
    )
)


#====== [END] CELL 61 ======


#====== [START] CELL 62 ======

flag_agreement_sales = (
    nmu_agreement_sales
    .withColumn("159_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id") == "2") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (~F.upper(F.col("name")).contains("OUTDOOR")) &
            (F.col("is_pos") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id") == "2") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (~F.upper(F.col("name")).contains("OUTDOOR")) &
            (F.upper(F.col("name")).contains("POS"))
            , 1
        ).otherwise(0)
    )
    .withColumn("160_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id") == "2") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (~F.upper(F.col("name")).contains("OUTDOOR")) &
            (F.col("is_new") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id") == "2") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (~F.upper(F.col("name")).contains("OUTDOOR")) &
            (F.upper(F.col("name")).contains("NEW")) &
            (~F.upper(F.col("name")).contains("RENEW"))
            , 1
        ).otherwise(0)
    )
    .withColumn("161_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id") == "2") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (~F.upper(F.col("name")).contains("OUTDOOR")) &
            (F.col("is_renew") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id") == "2") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (~F.upper(F.col("name")).contains("OUTDOOR")) &
            (F.upper(F.col("name")).contains("RENEW"))
            , 1
        ).otherwise(0)
    )
    .withColumn("162_flag", 
        F.when(
            (F.col("source").isin(["HK", "CN", "SG"])) &
            (F.col("ext_ref_package_type_id") == "2") &
            (F.upper(F.col("name")).contains("ONLINE") | F.upper(F.col("name")).contains("OUTDOOR"))
            , 1
        ).otherwise(0)
    )
    .withColumn("163_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id") == "3") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.col("is_pos") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id") == "3") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.upper(F.col("name")).contains("POS")) 
            , 1
        ).otherwise(0)
    )
    .withColumn("164_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id") == "3") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.col("is_new") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id") == "3") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.upper(F.col("name")).contains("NEW")) &
            (~F.upper(F.col("name")).contains("RENEW"))
            , 1
        ).otherwise(0)
    )
    .withColumn("165_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id") == "3") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.col("is_renew") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id") == "3") &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.upper(F.col("name")).contains("RENEW"))
            , 1
        ).otherwise(0)
    )
    .withColumn("166_flag", 
        F.when(
            (F.col("source").isin(["HK", "CN", "SG"])) &
            (F.col("ext_ref_package_type_id") == "3") &
            (F.upper(F.col("name")).contains("ONLINE")) 
            , 1
        ).otherwise(0)
    )
    .withColumn("167_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.col("service_category_id") == 28) 
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            (F.col("service_category_id") == 69) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("service_category_id") == 66) 
            , 1
        ).otherwise(0)
    )
    .withColumn("168_flag", 
        F.when(
            (F.col("source").isin(["HK", "CN", "SG"])) &
            (F.col("service_category_id").isin([6]))
            , 1
        ).otherwise(0)
    )
    .withColumn("169_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id").isin(["103", "104", "116"])) &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.col("is_pos") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id").isin(["116", "360_27", "103"])) &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.upper(F.col("name")).contains("POS"))
            , 1
        ).otherwise(0)
    )
    .withColumn("170_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id").isin(["103", "104", "116"])) &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.col("is_new") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id").isin(["116", "360_27", "103"])) &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.upper(F.col("name")).contains("NEW"))&
            (~F.upper(F.col("name")).contains("RENEW"))
            , 1
        ).otherwise(0)
    )
    .withColumn("171_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id").isin(["103", "104", "116"])) &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.col("is_renew") == 1) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id").isin(["116", "360_27", "103"])) &
            (~F.upper(F.col("name")).contains("ONLINE")) &
            (F.upper(F.col("name")).contains("RENEW"))
            , 1
        ).otherwise(0)
    )
    .withColumn("172_flag", 
        F.when(
            (F.col("source").isin(["HK", "SG"])) &
            (F.col("ext_ref_package_type_id").isin(["103", "104", "116"])) &
            (F.upper(F.col("name")).contains("ONLINE")) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            (F.col("ext_ref_package_type_id").isin(["116", "360_27", "103"])) &
            (F.upper(F.col("name")).contains("ONLINE")) 
            , 1
        ).otherwise(0)
    )
    .withColumn("173_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("174_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("175_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("176_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("177_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("178_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("179_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("180_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("181_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("185_flag", 
        F.when(
            (F.col("source").isin(["HK", "CN", "SG"])) &
            (F.col("ext_ref_package_type_id") == "4") &
            (~F.col("ext_ref_invoice_category_id").isin(["2", "31", "42"]))
            , 1
        ).otherwise(0)
    )
    .withColumn("186_flag", 
        F.lit(0)  # BLANK - no calculation logic applied
    )
    .withColumn("187_flag", 
        F.when(
            (F.col("source").isin(["HK", "CN", "SG"])) &
            (F.col("service_category_id") == 15) &
            (F.col("ext_ref_package_type_id") == "106") 
            , 1
        ).otherwise(0)
    )
    .withColumn("188_flag", 
        F.when(
            (F.col("source").isin(["HK", "CN", "SG"])) &
            (F.col("service_category_id") == 15) &
            (F.col("ext_ref_package_type_id") == "107") 
            , 1
        ).otherwise(0)
    )
    .withColumn("189b_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            ((F.col("service_category_id").isin(70)) | 
             (F.col("ext_ref_package_type_id").isin(["360_28", "104", "120"]))) 
            , 1
        ).when(
            (F.col("source").isin(["SG"])) &
            ((F.col("service_category_id").isin(70)) | 
             (F.col("ext_ref_package_type_id").isin(["110", "115", "120"]))) 
            , 1
        ).when(
            (F.col("source").isin(["CN"])) &
            ((F.col("service_category_id").isin(28)) | 
             (F.col("ext_ref_package_type_id").isin(["360_28", "104", "120"]))) 
            , 1
        ).otherwise(0)
    )
    .withColumn("189c_flag", 
        F.when(
            (F.col("source").isin(["HK"])) &
            (F.upper(F.col("service_name")).contains("DANCE")) 
            , 1
        ).when(
            (F.col("source").isin(["SG", "CN"])) &
            (~F.col("ext_ref_package_type_id").isin(["106", "107"])) &
            (F.upper(F.col("service_name")).contains("DANCE")) 
            , 1
        ).otherwise(0)
    )
)


#====== [END] CELL 62 ======


#====== [START] CELL 63 ======

flagchk_agreement_sales = flag_agreement_sales.withColumn(
    "multiple_flags_count",
    (F.col("159_flag") + F.col("160_flag") + F.col("161_flag") + F.col("162_flag") + 
     F.col("163_flag") + F.col("164_flag") + F.col("165_flag") + F.col("166_flag") + 
     F.col("167_flag") + F.col("168_flag") + F.col("169_flag") + F.col("170_flag") + 
     F.col("171_flag") + F.col("172_flag") + F.col("173_flag") + F.col("174_flag") + 
     F.col("175_flag") + F.col("176_flag") + F.col("177_flag") + F.col("178_flag") + 
     F.col("179_flag") + F.col("180_flag") + F.col("181_flag") + F.col("185_flag") + 
     F.col("186_flag") + F.col("187_flag") + F.col("188_flag") + F.col("189b_flag") + 
     F.col("189c_flag"))
).withColumn(
    "has_multiple_flags",
    F.when(F.col("multiple_flags_count") > 1, True).otherwise(False)
)
# ).filter(F.col("has_multiple_flags") == True).display()

#====== [END] CELL 63 ======


#====== [START] CELL 64 ======

# # Display rows where each flag equals 1
# flag_agreement_sales.filter(F.col("159_flag") == 1).display()
# flag_agreement_sales.filter(F.col("160_flag") == 1).display()
# flag_agreement_sales.filter(F.col("161_flag") == 1).display()
# flag_agreement_sales.filter(F.col("162_flag") == 1).display()
# flag_agreement_sales.filter(F.col("163_flag") == 1).display()
# flag_agreement_sales.filter(F.col("164_flag") == 1).display()
# flag_agreement_sales.filter(F.col("165_flag") == 1).display()
# flag_agreement_sales.filter(F.col("166_flag") == 1).display()
# flag_agreement_sales.filter(F.col("167_flag") == 1).display()
# flag_agreement_sales.filter(F.col("168_flag") == 1).display()
# flag_agreement_sales.filter(F.col("169_flag") == 1).display()
# flag_agreement_sales.filter(F.col("170_flag") == 1).display()
# flag_agreement_sales.filter(F.col("171_flag") == 1).display()
# flag_agreement_sales.filter(F.col("172_flag") == 1).display()
# flag_agreement_sales.filter(F.col("173_flag") == 1).display()
# flag_agreement_sales.filter(F.col("174_flag") == 1).display()
# flag_agreement_sales.filter(F.col("175_flag") == 1).display()
# flag_agreement_sales.filter(F.col("176_flag") == 1).display()
# flag_agreement_sales.filter(F.col("177_flag") == 1).display()
# flag_agreement_sales.filter(F.col("178_flag") == 1).display()
# flag_agreement_sales.filter(F.col("179_flag") == 1).display()
# flag_agreement_sales.filter(F.col("180_flag") == 1).display()
# flag_agreement_sales.filter(F.col("181_flag") == 1).display()
# flag_agreement_sales.filter(F.col("185_flag") == 1).display()
# flag_agreement_sales.filter(F.col("186_flag") == 1).display()
# flag_agreement_sales.filter(F.col("187_flag") == 1).display()
# flag_agreement_sales.filter(F.col("188_flag") == 1).display()
# flag_agreement_sales.filter(F.col("189b_flag") == 1).display()
# flag_agreement_sales.filter(F.col("189c_flag") == 1).display()

#====== [END] CELL 64 ======




#====== [MARKDOWN] CELL 65 ======

#====== [START] CELL 66 ======

nmu_product_sales =(
    product_sales.alias('sales')
    .join(
        accounts_nmu_ym.alias('nmu'),
        F.col('sales.dim_contact_key') == F.col('nmu.dim_contact_key'), 
        'left'
    )
    .select(
        "sales.*",
        "nmu.success_date",
        "nmu.nmu_ym",
    )
    # .filter(F.col("post_date") >= f"{last_year}-01-01")
    .withColumn(
        "post_ym",
        F.date_format(F.col('post_date'), 'yyyyMM')
    )
    .withColumn(
        "nmu_ind",
        F.when(
            (F.col("nmu_ym").isNull()), 
            99
        )
        .when(
            (F.col("nmu_ym") == F.date_format(F.col('post_date'), 'yyyyMM')), 
            1
        )
        .otherwise(0)
    )
)


#====== [END] CELL 66 ======


#====== [START] CELL 67 ======

flag_product_sales = (
    nmu_product_sales
    .withColumn("182_flag", 
        F.when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["HK", "CN"])) &
            (F.col("invoice_category_id") == 21) 
            , 1
        ).when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["SG"])) &
            (F.col("invoice_category_id") == 21) 
            , 0                     # Fields not ready yet
        ).otherwise(0)
    ) 
    .withColumn("183_flag", 
        F.when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["HK"])) &
            (F.col("product_id").isin([26, 27])) &
            (F.col("product_category_name") == "Corporate Item") 
            , 1
        ).when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["SG"])) &
            (F.col("product_id").isin([15])) &
            (F.col("product_category_name") == "Corporate Item") 
            , 1
        ).when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["CN"])) &
            (F.col("product_id").isin([16])) 
            , 1
        ).otherwise(0)
    ) 
    .withColumn("189_flag", 
        F.when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["HK"])) &
            (F.col("product_category_id").isin([441])) 
            , 1
        ).when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["SG"])) &
            (F.col("product_category_id").isin([3])) 
            , 1
        ).when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["CN"])) &
            (F.col("product_name").isin([
                "Drop In", 
                "Suspension fee", 
                "Locker overnight charge", 
                "Lost Daily Locker Key", 
                "Lost Shoe Locker Key"
            ])) 
            , 1
        ).otherwise(0)
    ) 
    .withColumn("190_flag", 
        F.when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["HK", "CN", "SG"])) &
            (F.col("product_category_id").isin([434, 435])) 
            , 1
        ).otherwise(0)
    ) 
    .withColumn("191_flag", 
        F.when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["HK", "CN", "SG"])) &
            (F.col("product_category_id").isin([430, 123])) 
            , 1
        ).otherwise(0)
    )
)


#====== [END] CELL 67 ======


#====== [START] CELL 68 ======

flagchk_product_sales = flag_product_sales.withColumn(
    "multiple_flags_count",
    (F.col("182_flag") + F.col("183_flag") + F.col("189_flag") + F.col("190_flag") + F.col("191_flag"))
).withColumn(
    "has_multiple_flags",
    F.when(F.col("multiple_flags_count") > 1, True).otherwise(False)
)
# ).filter(F.col("has_multiple_flags") == True).display()

#====== [END] CELL 68 ======


#====== [START] CELL 69 ======

# # Display rows where each flag equals 1
# flag_product_sales.filter(F.col("182_flag") == 1).display()
# flag_product_sales.filter(F.col("183_flag") == 1).display()
# flag_product_sales.filter(F.col("189_flag") == 1).display()
# flag_product_sales.filter(F.col("190_flag") == 1).display()
# flag_product_sales.filter(F.col("191_flag") == 1).display()

#====== [END] CELL 69 ======


#====== [START] CELL 70 ======

# display(flag_product_sales.sort(F.col('post_date').desc()).filter(F.col('_chk') > 1))
# display(flag_product_sales)


#====== [END] CELL 70 ======




#====== [MARKDOWN] CELL 71 ======

#====== [START] CELL 72 ======

flag_outstanding =(
    outstanding
    .withColumn(
        "nmu_ind",
        F.lit(99)
    )
    .withColumn(
        "post_ym",
        F.date_format(F.col('post_date'), 'yyyyMM')
    )
    .withColumn("193_flag", 
        F.when(
            (F.substring(F.col("dim_location_key"), 1, 2).isin(["HK", "CN", "SG"]))
            , 1
        ).otherwise(0)
    ) 
)


#====== [END] CELL 72 ======


#====== [START] CELL 73 ======

# display(flag_outstanding)


#====== [END] CELL 73 ======


#====== [START] CELL 74 ======

flag_membership_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LEG_TABLE('stage_fct_membership'))
flag_agreement_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LEG_TABLE('stage_fct_agreement'))
flag_product_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LEG_TABLE('stage_fct_product'))
flag_outstanding.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LEG_TABLE('stage_fct_outstanding'))


#====== [END] CELL 74 ======




#====== [MARKDOWN] CELL 75 ======

#====== [START] CELL 76 ======

# def cal_total_revenue(df, col):
#     base_cols = [col for col in df.columns if not col.endswith("_flag")]
#     flag_cols = [col for col in df.columns if col.endswith("_flag")]

#     schema = StructType([
#         StructField("source", StringType(), True),
#         StructField("dim_location_key", StringType(), True),
#         StructField("post_date", TimestampType(), True),
#         StructField("index", StringType(), True),
#         StructField("nmu_ind", IntegerType(), True),
#         StructField("total_revenue", DecimalType(38, 4), True)
#     ])

#     fct_df = spark.createDataFrame([], schema)

#     for _i in flag_cols:
#         _tdf = (
#             df
#             .select(*base_cols, _i)
#             .filter(F.col(_i) == 1)
#             .groupBy("source", 'dim_location_key', 'post_date', 'nmu_ind')
#             .agg(
#                 F.sum(f"{col}").alias("total_revenue"),
#             )
#             .withColumn('index', F.lit(f"{_i}"))
#         ) 
#         fct_df = fct_df.unionByName(_tdf)

#     return fct_df


#====== [END] CELL 76 ======


#====== [START] CELL 77 ======

# flag_membership_revenue = cal_total_revenue(flag_membership_sales, "invoice_detail_amount")
# fct_agreement_revenue = cal_total_revenue(flag_agreement_sales, "total_revenue_agreement")
# fct_product_revenue = cal_total_revenue(flag_product_sales, "total_revenue_product")
# fct_outstanding_revenue = cal_total_revenue(flag_outstanding, "total_amount")


#====== [END] CELL 77 ======


#====== [START] CELL 78 ======

# index_to_code = {
#     "137": "SALES_MEMS_NMUS_JOIN",
#     "139": "SALES_MEMS_NMUS_PRE",
#     "140": "SALES_MEMS_NMUS_PRE",
#     "141": "SALES_MEMS_NMUS_OPT",
#     "142": "SALES_MEMS_NMUS_SP",
#     "144": "SALES_MEMS_NMUS_ADJ",
#     "145": "SALES_MEMS_NMUS_ADJ",
#     "146": "SALES_MEMS_NMUS_ADJ",
#     "147": "SALES_MEMS_NMUS_ADJ",
#     "148": "SALES_MEMS_NMUS_ADJ",
#     "149": "SALES_MEMS_NMUS_ADJ",
#     "151": "SALES_MEMS_EMEMS_RES",
#     "152": "SALES_MEMS_EMEMS_RES",
#     "154": "SALES_MEMS_EMEMS_UPS",
#     "155": "SALES_MEMS_EMEMS_UPS",
#     "156": "SALES_MEMS_EMEMS_UPS",
#     "157": "SALES_MEMS_EMEMS_UPS",
#     "159": "SALES_ARPU_PT_PT",
#     "160": "SALES_ARPU_PT_PT",
#     "161": "SALES_ARPU_PT_PT",
#     "162": "SALES_ARPU_PT_PT",
#     "163": "SALES_ARPU_PY_PY",
#     "164": "SALES_ARPU_PY_PY",
#     "165": "SALES_ARPU_PY_PY",
#     "166": "SALES_ARPU_PY_PY",
#     "167": "SALES_ARPU_PY_PY",
#     "168": "SALES_ARPU_PY_PY",
#     "169": "SALES_ARPU_RP_RP",
#     "170": "SALES_ARPU_RP_RP",
#     "171": "SALES_ARPU_RP_RP",
#     "172": "SALES_ARPU_RP_RP",
#     "173": "SALES_ARPU_RP_RP",
#     "174": "SALES_ARPU_OPT_OPT",
#     "175": "SALES_ARPU_OPT_OPT",
#     "176": "SALES_ARPU_OPT_OPT",
#     "178": "SALES_ARPU_O_O",
#     "179": "SALES_ARPU_O_O",
#     "180": "SALES_ARPU_O_O",
#     "181": "SALES_ARPU_O_O",
#     "182": "SALES_ARPU_ND_ND",
#     "183": "SALES_ARPU_O_O",
#     "185": "SALES_ARPU_O_O",
#     "186": "SALES_ARPU_O_O",
#     "187": "SALES_ARPU_O_O",
#     "188": "SALES_ARPU_O_O",
#     "189": "SALES_ARPU_O_O",
#     "190": "SALES_ARPU_O_O",
#     "191": "SALES_ARPU_O_O",
#     "193": "SALES_OUT_OUT_OUT",
#     "201": "SALES_MEMS_EMEMS_ADP"
# }

# # Broadcast the mapping dictionary for performance
# mapping_expr = F.create_map([F.lit(x) for x in sum(index_to_code.items(), ())])

#====== [END] CELL 78 ======


#====== [START] CELL 79 ======

# union_total_revenue = (
#     flag_membership_revenue
#     .union(fct_agreement_revenue)
#     .union(fct_product_revenue)
#     .union(fct_outstanding_revenue)
# )


#====== [END] CELL 79 ======


#====== [START] CELL 80 ======

# group_total_revenue = (
#     union_total_revenue
#     .filter(F.col("total_revenue") > 0)
#     .withColumn(
#         "index",
#         F.regexp_replace(F.col("index"), "_flag|a|b|c", "")
#     )
#     .withColumn("layer_code", mapping_expr[F.col("index")])
#     .withColumn("year", F.date_format(F.col('post_date'), 'yyyy').cast('int'))
#     .withColumn("yearmonth", F.date_format(F.col('post_date'), 'yyyyMM').cast('int'))
#     .withColumn(
#         "dim_location_key", 
#         F.when(F.col("dim_location_key").isNull(), F.concat(F.col("source"), F.lit("9999")))
#          .otherwise(F.col("dim_location_key"))
#     )
#     .groupBy("source", 'dim_location_key', 'post_date', 'layer_code', 'nmu_ind', 'year', 'yearmonth', )
#     .agg(
#         F.sum('total_revenue').alias('total_revenue')
#     )
# )

#====== [END] CELL 80 ======


#====== [START] CELL 81 ======

# group_total_revenue.filter(F.col("dim_location_key").isNull()).display()

# (
#     group_total_revenue
#     .filter(F.col("yearmonth") == "202506")
#     .filter(F.col("layer_code") == "SALES_ARPU_PT_PT")
#     .filter(F.col("source") == "HK")
# ).display()

#====== [END] CELL 81 ======


#====== [START] CELL 82 ======

# fct_total_revenue = (
#     group_total_revenue.alias("fct")
#     .join(
#         exchange_rate.alias("r"),
#         # (F.substring(F.col("fct.dim_location_key"), 1, 2) == F.col("r.region")) & 
#         (F.col("fct.source") == F.col("r.region")) & 
#         (F.col("fct.post_date") == F.col("r.date")), 
#         "left"
#     )
#     .withColumn("total_revenue_HKD", (F.col("total_revenue") / F.col("r.transaction_HKD")).cast('decimal(38,18)'))
#     .withColumn("total_revenue_USD", (F.col("total_revenue") / F.col("r.transaction_USD")).cast('decimal(38,18)'))
#     .drop(*[col for col in exchange_rate.columns])
# )

#====== [END] CELL 82 ======


#====== [START] CELL 83 ======

# fct_total_revenue.display()

#====== [END] CELL 83 ======


#====== [START] CELL 84 ======

# fct_total_revenue.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_total_revenue")


#====== [END] CELL 84 ======


#====== [START] CELL 85 ======

# fct_kpi_revenue.limit(5).display()

#====== [END] CELL 85 ======


#====== [START] CELL 86 ======

# fct_kpi_revenue.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("leg_uat.lakehouse.fct_kpi_revenue")


#====== [END] CELL 86 ======


#====== [START] CELL 87 ======

# fct_total_revenue.groupBy("source", "dim_location_key").agg(F.sum("total_revenue")).filter(F.col("source") == "HK").display()

# fct_kpi_revenue.groupBy("source", "dim_location_key").agg(F.sum("kpi_revenue")).filter(F.col("source") == "HK").display()


#====== [END] CELL 87 ======


#====== [START] CELL 88 ======



#====== [END] CELL 88 ======


