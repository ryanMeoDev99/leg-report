#====== [START] CELL 1 ======


import sys
sys.path.append('./')

import kpi_utils

import importlib
importlib.reload(kpi_utils)

import kpi_utils as K

#
import random

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.connect.column import Column

from pyspark.sql.window import Window

from pyspark.sql.connect.dataframe import DataFrame
from dataclasses import dataclass
from functools import *
from typing import *
from datetime import date

import itertools as I

#====== [END] CELL 1 ======


#====== [START] CELL 2 ======

K.CATALOG = dbutils.widgets.get("PURE_CATALOG")
K.SCHEMA = dbutils.widgets.get("PURE_SCHEMA")

K.REPORT_CATALOG = dbutils.widgets.get("LEG_CATALOG")
K.REPORT_SCHEMA = dbutils.widgets.get("LEG_SCHEMA")

#====== [END] CELL 2 ======




#====== [MARKDOWN] CELL 3 ======



#====== [MARKDOWN] CELL 4 ======

#====== [START] CELL 5 ======

### dim location
df_dim_location = spark.read.table(K.TABLE('dim_location'))

# region
df_dim_location = df_dim_location.withColumn(
    'left_region', F.left('dim_location_key', F.lit(2))
).withColumn(
    'region', F.left('dim_region_key', F.lit(2)) # for join
)

# window
_window = Window.partitionBy('location_code', 'left_region').orderBy('dim_location_key')
df_dim_location = df_dim_location.withColumn(
    '_row', F.row_number().over(_window)
)

df_dim_location = df_dim_location.filter(F.col('_row')==1)

# select
df_dim_location = df_dim_location.select(
    'region',
    'dim_region_key', 'dim_location_key',
    'location_code', 'location_name',
)

# rename
df_dim_location = K.prefix(df_dim_location, 'dim_location')

### silver location
df_location = spark.read.table(K.TABLE('silver_location'))

df_location = df_location.withColumns({
    'source_id': K.concat_id(df_location, cols=['source', 'id']),
})

df_location = K.prefix(df_location, 'location')

### dim location 360f
df_dim_location_360f = spark.read.table(K.TABLE('dim_location_360f'))
df_dim_location_360f = K.prefix(df_dim_location_360f, 'dim_location')

### join
df_location_full = df_location.join(
    df_dim_location_360f,
    df_location.location_bk == df_dim_location_360f.dim_location_360f_key,
    'left'
)

#====== [END] CELL 5 ======




#====== [MARKDOWN] CELL 6 ======

#====== [START] CELL 7 ======

# 360 package
df_360_package = spark.read.table(K.TABLE('360_silver_package'))
df_360_package = df_360_package.withColumns({
    'source_package_sub_type_id': K.concat_id(df_360_package, cols=['source', 'package_sub_type_id']),
    'package_source_id': K.concat_id(df_360_package, cols=['source', 'id']),
})

# package
df_package = spark.read.table(K.TABLE('silver_package'))

df_package = df_package.withColumns({
    'source_id': K.concat_id(df_package, cols=['source', 'id']),

    'package_type_bk': K.concat_id(df_package, cols=['source', 'package_type_id']),
    'package_sub_type_bk': K.concat_id(df_package, cols=['source', 'package_sub_type_id']),
    'package_term_bk': K.concat_id(df_package, cols=['source', 'package_term_id']),
})

df_package = K.prefix(df_package, 'package')

# type
df_package_type = spark.read.table(K.TABLE('silver_package_type'))
df_package_type = K.prefix(df_package_type, 'package_type')

# sub_type
df_package_sub_type = spark.read.table(K.TABLE('silver_package_sub_type'))
df_package_sub_type = K.prefix(df_package_sub_type, 'package_sub_type')

# term
df_package_term = spark.read.table(K.TABLE('silver_package_term'))
df_package_term = K.prefix(df_package_term, 'package_term')

#====== [END] CELL 7 ======


#====== [START] CELL 8 ======

# join packages
def join_packages(df, _type=False, _sub=False, _term=False):
    if _type:
        df = df.join(
            df_package_type,
            df_package.package_type_bk == df_package_type.package_type_bk,
            'left'
        )

    if _sub:
        df = df.join(
            df_package_sub_type,
            df_package.package_sub_type_bk == df_package_sub_type.package_sub_type_bk,
            'left'
        )

    if _term:
        df = df.join(
            df_package_term,
            df_package.package_term_bk == df_package_term.package_term_bk,
            'left'
        )

    return df

#====== [END] CELL 8 ======




#====== [MARKDOWN] CELL 9 ======

#====== [START] CELL 10 ======

df_account = spark.read.table(K.TABLE('silver_accounts'))

# fct_sf_accounts

df_account = df_account.withColumns({
    'region': K.SH_CN_WHEN(),
    'location': K.ACCOUNT_LOCATION_WHEN(),
    'date': F.to_date('success_date'),
}).withColumn(
    'region_ref_id', K.concat_id(df_account, cols=['region', 'ref_id'])
)

# join location
df_account_location = df_account.join(
    df_dim_location,
    (df_account.region == df_dim_location.dim_location_region) & (df_account.location == F.trim(df_dim_location.dim_location_code)),
    'left'
)

# success account
## filter
df_account_success = df_account.filter(
    F.col('success_date').isNotNull() & 
    (F.col('guest_status') == 'Success')
)

## window
### get first account 
_window = Window.partitionBy('ref_id').orderBy('success_date')
df_account_success = df_account_success.withColumn(
    '_row', F.row_number().over(_window)
)\
    .filter(F.col('_row')==1)\
    .select(
        'region_ref_id', F.date_format('success_date', K.MONTH_DATE_FORMAT).alias('success_month')
    )

#====== [END] CELL 10 ======


#====== [START] CELL 11 ======

df_fct_account = spark.read.table(K.TABLE('fct_sf_accounts'))
df_fct_account = df_fct_account.withColumns({
    'date': F.to_date('success_date'),
    'region': F.col('region_2'),
})

#====== [END] CELL 11 ======




#====== [MARKDOWN] CELL 12 ======

#====== [START] CELL 13 ======

df_agreement_status_history = spark.read.table(K.TABLE('silver_agreement_status_history'))

# create current month and next month
df_agreement_status_history = df_agreement_status_history.withColumns({
    'source_agreement_id': K.concat_id(df_agreement_status_history, cols=['source', 'agreement_id']),
    
    'max_ending_date': F.ifnull('ending_date', F.lit('2099-12-31')),
})

#====== [END] CELL 13 ======




#====== [MARKDOWN] CELL 14 ======

#====== [START] CELL 15 ======

df_membership = spark.read.table(K.TABLE('fct_fpa_nmu_ending_balance'))
df_membership = df_membership.withColumn('source_contact_id', K.concat_id(df_membership, cols=['source', 'contact_id']))

#
# df_package_bll = spark.read.table(K.TABLE('silver_package_book_limit_line'))

df_book_limit = spark.read.table(K.TABLE('silver_package_book_limit_line'))
df_book_limit = K.prefix(df_book_limit, 'book_limit')

#====== [END] CELL 15 ======




#====== [MARKDOWN] CELL 16 ======

#====== [START] CELL 17 ======

#
df_campaign = spark.read.table(K.TABLE('silver_campaign'))

df_campaign = df_campaign.withColumn(
    'source_id', K.concat_id(df_campaign, cols=['source', 'id'])
)

df_campaign = K.prefix(df_campaign, 'campaign')

#
df_channel = spark.read.table(K.TABLE('silver_channel_code'))

df_channel = df_channel.withColumn(
    'source_id', K.concat_id(df_channel, cols=['source', 'id'])
)

df_channel = K.prefix(df_channel, 'channel')

# df_lookup = spark.read.table(K.TABLE('silver_z_lookup'))

# #
# df_campaign = df_lookup.filter(
#     (F.col('category') == 'Marketing Campaign') &
#     (F.col('deleted') == 0)
# )

# df_campaign = K.prefix(df_campaign, 'campaign')

# #
# df_channel = df_lookup.filter(
#     (F.col('category') == 'Channel Code') &
#     (F.col('deleted') == 0)
# )

# df_channel = K.prefix(df_channel, 'channel')

#====== [END] CELL 17 ======




#====== [MARKDOWN] CELL 18 ======

#====== [START] CELL 19 ======

df_budget = spark.read.table(K.TABLE('silver_uat_marketing_budget'))

# join campaign
df_budget_campaign = df_budget.join(
    df_campaign,
    (df_budget.campaign_id == df_campaign.campaign_id) & (df_budget.source == df_campaign.campaign_source),
    'left'
)

# join channel
df_budget_channel = df_budget_campaign.join(
    df_channel,
    (df_budget.channel_code_id == df_channel.channel_id) & (df_budget.source == df_channel.channel_source),
    'left'
)

# select
df_budget_select = df_budget_channel.select(
    df_campaign.campaign_code.alias('budget_campaign_code'),
    df_channel.channel_code.alias('budget_channel_code'),

    df_budget.source.alias('budget_source'),
    df_budget.budget_amount
)

#====== [END] CELL 19 ======




#====== [MARKDOWN] CELL 20 ======

#====== [START] CELL 21 ======

df_360_agreement = spark.read.table(K.TABLE('360_silver_agreement'))

# create join key
df_360_agreement = df_360_agreement.withColumns({
    'prev_gap_reference': F.coalesce('last_workout_date', 'end_date'),
    'order_date': F.coalesce('signed_date', 'start_date'),
    'signed_month': F.date_format('signed_date', K.MONTH_DATE_FORMAT),

    'source_id': K.concat_id(df_360_agreement, cols=['source', 'id']),
    'source_contact_id': K.concat_id(df_360_agreement, cols=['source', 'contact_id']),
    'source_package_id': K.concat_id(df_360_agreement, cols=['source', 'package_id']),
    'source_ext_ref_agreement_id': K.concat_id(df_360_agreement, cols=['source', 'ext_ref_agreement_id']),
    'source_revenue_location_id': K.concat_id(df_360_agreement, cols=['source', 'revenue_location_id']),
})

# join
def agreement_join_package(df=df_360_agreement, df_join=df_package):
    _df = df.join(
        df_join,
        df.source_package_id == df_join.package_source_id,
        'left'
    )

    # create membership
    _df = _df.withColumn(
        'is_membership', K.IS_MEMBER_WHEN(
            s=df.source,
            t=df.package_type_id,
            st=df_join.package_sub_type_id,
        )
    )

    _df_filter = _df.filter(
        df.deleted_at.isNull() & df.parent_agreement_id.isNull()
    )

    return _df, _df_filter

df_360_agreement_package, df_membership_agreement = agreement_join_package()
df_360_agreement_360_package, df_membership_360_agreement = agreement_join_package(df_join=df_360_package)

#====== [END] CELL 21 ======




#====== [MARKDOWN] CELL 22 ======

#====== [START] CELL 23 ======

df_leads = spark.read.table(K.TABLE('silver_leads'))

# create
df_leads = df_leads.withColumns({
    'date': F.to_date('lead_create_date'),
    'region_2': K.SH_CN_WHEN(),
    'location_2': K.LEAD_LOCATION_WHEN(),
    'is_hk': K.IS_HK_WHEN(),
}).withColumns({
    'market_lead': K.MARKET_LEAD_WHEN(),
    'lead_source_group': K.SOURCE_GROUP_WHEN(),
    'region_is_hk': K.REGION_IS_HK_WHEN(),
}).withColumn(
    'group', K.ML_GROUP_WHEN()
)

# join user
df_user = spark.read.table(K.TABLE('silver_users')).select(
    F.col('id').alias('user_id'),
    'user_name',
)

df_leads_user = df_leads.join(
    df_user,
    df_leads.assigned_user_id == df_user.user_id,
    'left'
)

# join location
df_leads_location = df_leads_user.join(
    df_dim_location,
    (df_leads.region_2 == df_dim_location.dim_location_region) & (df_leads.location_2 == df_dim_location.dim_location_code),
    'left'
)

# join budget
df_leads_budget = df_leads_location.join(
    df_budget_select,
    (df_leads.marketing_campaign == df_budget_select.budget_campaign_code) &
    (df_leads.channel_code == df_budget_select.budget_channel_code) &
    (df_leads.region == df_budget_select.budget_source),
    'left'
)

# create per budget by window
_window = Window.partitionBy('marketing_campaign', 'channel_code', 'region', 'budget_amount')
df_leads_budget = df_leads_budget.withColumn(
    'no_leads_by_campaign', F.count('*').over(_window)
)

df_leads_budget = df_leads_budget.withColumn(
    'per_budget_amount', F.col('budget_amount') / F.col('no_leads_by_campaign')
)

#====== [END] CELL 23 ======




#====== [MARKDOWN] CELL 24 ======

#====== [START] CELL 25 ======

df_meetings = spark.read.table(K.TABLE('silver_meetings'))

# create appointment_date
df_meetings = df_meetings.withColumns({
    'date': F.to_date('date_start'),
    'location': K.MEET_LOCATION_WHEN(),
})

# join lead
df_meetings_lead = df_meetings.join(
    df_leads_user,
    df_meetings.parent_id == df_leads.id,
    'left'
)

#
df_meetings_lead = df_meetings_lead.withColumns({
    'lead_source_group': K.SOURCE_GROUP_WHEN(c=df_meetings.lead_source),
    'market_lead': K.MARKET_LEAD_WHEN(s=df_meetings.lead_source),
    'meet_region': K.MEET_REGION_WHEN(),
}).withColumn(
    'group', K.ML_GROUP_WHEN()
).withColumn(
    'layer', K.LEAD_LAYER_WHEN()
)

# join location
df_meetings_location = df_meetings_lead.join(
    df_dim_location,
    (df_meetings_lead.meet_region == df_dim_location.dim_location_region) & (df_meetings.location == df_dim_location.dim_location_code),
    'left'
)

#====== [END] CELL 25 ======




#====== [MARKDOWN] CELL 26 ======

#====== [START] CELL 27 ======

df_contact = spark.read.table(K.TABLE('silver_contact'))
df_contact = df_contact.withColumns({
    'source_id': K.concat_id(df_contact, cols=['source', 'id']),
    'source_member_id': K.concat_id(df_contact, cols=['source', 'member_id']),
    'source_ext_ref_contact_id': K.concat_id(df_contact, cols=['source', 'ext_ref_contact_id']),
    'source_home_location_id': K.concat_id(df_contact, cols=['source', 'home_location_id']),
})

# join location
df_contact_location = df_contact.join(
    df_location_full,
    (df_contact.source_home_location_id == df_location_full.location_bk),
    'left'
)


# join - create first account flag
df_contact_success = df_contact_location.join(
    df_account_success,
    df_contact.source_ext_ref_contact_id == df_account.region_ref_id,
    'left'
)

## contact + membership_agreement

# df_contact_membership_agreement = df_contact_success.join(
#     df_membership_agreement,
#     df_contact.source_id == df_360_agreement.source_contact_id,
#     'inner'
# )

df_contact_membership_360_agreement = df_contact_success.join(
    df_membership_360_agreement,
    df_contact.source_id == df_360_agreement.source_contact_id,
    'inner'
)

#====== [END] CELL 27 ======


#====== [START] CELL 28 ======

# join membership
df_contact_membership = df_contact.join(
    df_membership,
    (df_contact.id == df_membership.contact_id) & (df_contact.source == df_membership.source),
    'inner'
)

# join 360 package
df_contact_package = df_contact_membership.join(
    df_360_package,
    (df_membership.package_id == df_360_package.id) & (df_membership.source == df_360_package.source),
    'left'
)

# join package_sub_type
df_contact_package_pst = df_contact_package.join(
    df_package_sub_type,
    df_360_package.source_package_sub_type_id == df_package_sub_type.package_sub_type_bk,
    'left'
)

# join package book limit
df_contact_package_bll = df_contact_package_pst.join(
    df_book_limit,
    (df_membership.package_id == df_book_limit.book_limit_package_id) & (df_membership.source == df_book_limit.book_limit_source),
    'left'
)

# LEFT JOIN LATERAL previous_agreement 

## create id for per row join
df_cpb = df_contact_package_bll.withColumn(
    '_partition_id', F.monotonically_increasing_id()
)

df_contact_agreement_prev = df_cpb.join(
    df_membership_agreement,
    (df_membership.source_contact_id == df_360_agreement.source_contact_id)
    & (df_360_agreement.prev_gap_reference <= df_membership.start_date) 
    & (df_360_agreement_package.is_membership == 1),
    'left'
)

## window
pa_window = Window.partitionBy('_partition_id').orderBy(df_360_agreement.order_date.desc())

df_contact_agreement_prev_window = df_contact_agreement_prev.withColumn(
    '_row', 
    F.row_number().over(pa_window)
).filter(F.col('_row')==1).drop('_row')

# outer - create day
df_contact_outer = df_contact_agreement_prev_window.withColumn(
    'day_gap', 
    F.datediff(df_membership.signed_date, df_360_agreement.prev_gap_reference)
)

# create
df_contact_outer = df_contact_outer.withColumn(
    'nmu_cat', K.NUM_CAT_WEHN(p=df_360_package.package_term_id, m=df_360_package.min_committed_month)
).withColumn(
    'package_gp', K.PACKAGE_GP_WHEN(
        p=df_360_package.package_term_id, m=df_360_package.min_committed_month, 
        t=df_membership.total_session, s=df_360_package.service_category_id,
        u=df_book_limit.book_limit_unit_code_id, a=df_book_limit.book_limit_amount,
    )
).withColumn(
    'is_all_clubs', K.IS_CLUB_WHEN(a=df_360_package.is_all_locations.cast('int'), s=df_360_package.is_single_location.cast('int'))
)

# groupby count
df_contact_ct = df_contact_outer.groupBy(
    F.to_date(df_membership.signed_date).alias('date'),
    df_membership.source.alias('region'), 
    df_360_package.package_term_id, 
    df_package_sub_type.package_sub_type_code, 
    df_membership.dim_location_key,
    'nmu_cat', 'package_gp', 'is_all_clubs',
).agg(
    F.countDistinct(df_membership.contact_id).alias('count')
)

#====== [END] CELL 28 ======




#====== [MARKDOWN] CELL 29 ======

#====== [START] CELL 30 ======

def prepare_item(df, id_cols, df_join, join_col):
    _df = df.withColumn(
        'source_item_id', K.concat_id(df, cols=['source', 'item_id'])
    )

    _df = _df.filter(
        (F.col('item_type') == 'contact')
        # & (F.col('active') == 1)
    )

    # create id col
    _id = '_'.join(id_cols)
    _df = _df.withColumn(
        _id, K.concat_id(df, cols=id_cols)
    )

    # join to get dim code
    _df = _df.join(
        df_join,
        _df[_id] == df_join[join_col],
        'left'
    )

    return _df

df_channel_code_item = spark.read.table(K.TABLE('silver_channel_code_item'))
df_channel_code_item = prepare_item(
    df_channel_code_item,
    id_cols=['source', 'channel_code_id'],
    df_join=df_channel,
    join_col='channel_source_id'
)

##
df_campaign_item = spark.read.table(K.TABLE('silver_campaign_item'))
df_campaign_item = prepare_item(
    df_campaign_item,
    id_cols=['source', 'campaign_id'],
    df_join=df_campaign,
    join_col='campaign_source_id'
)

#====== [END] CELL 30 ======




#====== [MARKDOWN] CELL 31 ======

#====== [START] CELL 32 ======

df_contact_status_history = spark.read.table(K.TABLE('silver_contact_status_history'))

df_contact_status_history = df_contact_status_history.withColumn(
    'source_contact_id', K.concat_id(df_contact_status_history, cols=['source', 'contact_id'])
)

#
df_csh_filter = df_contact_status_history.filter(
    F.col('contact_status_id').isin([1, 21, 25, 28, 33]) & 
    (F.col('source') == 'CN') & 
    (F.col('active') == 1)
)

# join contact + channel code item
df_cl_cci = df_contact_location.join(
    df_channel_code_item,
    df_channel_code_item.source_item_id == df_contact.bk,
    'left'
)

# join contact + channel camp item
df_clcci_camp = df_cl_cci.join(
    df_campaign_item,
    df_campaign_item.source_item_id == df_contact.bk,
    'left'
)

# join back contact status history
df_csh_clcci = df_csh_filter.join(
    df_clcci_camp,
    df_contact_status_history.source_contact_id == df_contact.bk,
    'left'
)

# create 
df_csh_clcci = df_csh_clcci.withColumns({
    'lead_source_group': K.CN_SOURCE_GROUP_WHEN(),
    'market_lead': K.CN_MARKET_LEAD_WHEN(),
}).withColumn(
    'group', K.ML_GROUP_WHEN()
).withColumn(
    'layer', K.LEAD_LAYER_WHEN()
)

# join budget
df_csh_clcci_budget = df_csh_clcci.join(
    df_budget_select,
    (df_campaign_item.campaign_code == df_budget_select.budget_campaign_code) &
    (df_channel_code_item.channel_code == df_budget_select.budget_channel_code) &
    (df_contact_status_history.source == df_budget_select.budget_source),
    'left'
)

# create per budget by window
_window = Window.partitionBy('campaign_code', 'channel_code', df_contact_status_history.source, 'budget_amount')
df_csh_clcci_budget = df_csh_clcci_budget.withColumn(
    'no_leads_by_campaign', F.count('*').over(_window)
).withColumn(
    'per_budget_amount', F.col('budget_amount') / F.col('no_leads_by_campaign')
)

#====== [END] CELL 32 ======




#====== [MARKDOWN] CELL 33 ======

#====== [START] CELL 34 ======

df_invoice_detail = spark.read.table(K.TABLE('silver_invoice_detail'))

df_invoice_detail = df_invoice_detail.withColumn(
    'source_item_id', K.concat_id(df_invoice_detail, cols=['source', 'item_id'])
)

# filter
df_invoice_detail = df_invoice_detail.filter(
    F.col('item_type') == 'agreement'
)

# prefix
df_invoice_detail = K.prefix(df_invoice_detail, 'invoice')

#====== [END] CELL 34 ======




#====== [MARKDOWN] CELL 35 ======

#====== [START] CELL 36 ======

df_termination = spark.read.table(K.TABLE('termination'))
df_termination = df_termination.withColumns({
    'region_rssid': F.concat('region', F.upper('rssid'))
})
df_termination = K.prefix(df_termination, 'termination')

#====== [END] CELL 36 ======




#====== [MARKDOWN] CELL 37 ======

#====== [START] CELL 38 ======

def get_months(times:int=None):
    if times is None:
        times = 12 + date.today().month
    
    return [
        F.add_months(
            F.current_date(), 
            -_
        )
        for _ in range(times)
    ]

def set_current(df, current=None):
    if current is None:
        current = F.current_date()

    df = df\
        .withColumn('current_date', current)\
        .withColumn(
            'current_month', 
            F.date_trunc('month', 'current_date')
        )\
        .withColumn('current_month_last_day', F.last_day('current_month'))\
            .withColumn(
                'is_effective_month', 
                F.col('current_month_last_day').between(F.col('effective_date'), F.col('max_ending_date'))
            )\
        .withColumn('next_month', F.add_months('current_month', 1))

    return df

#
def current_ash(df=df_agreement_status_history, current=None):
    # if not specific, set today
    df = set_current(df, current)

    # filter before window
    df = df.filter(
        (F.col('active') == 1) &
        F.col('is_effective_month')
    )

    # window
    _window = Window.partitionBy('agreement_id').orderBy(F.col('request_date').desc())
    df = df.withColumn(
        'agreement_id_row', F.row_number().over(_window)
    )

    return df

def current_cmal_ash(df, current=None):
    df_filter = df.filter(
        F.col('agreement_id_row') == 1
    )

    # join ash
    df =  df_contact_membership_360_agreement.join(
        df_filter,
        df_360_agreement.source_id == df_agreement_status_history.source_agreement_id,
        'left'
    )

    # reset the current values for the missing join value
    df = set_current(df, current)

    return df

def current_cmal_ash_window(df):
    _adjusted_date = lambda id: \
        F.when(
            df_agreement_status_history.agreement_status_id.isin(id),
            F.date_add(df_agreement_status_history.effective_date, -1)
        ).otherwise(
            F.ifnull(df_360_agreement.end_date, F.lit('2099-12-31'))
        )

    # 
    expr1 = F.when(
        _adjusted_date(id=[2, 3, 7, 8, 9, 10]) >= df.current_month_last_day,  # <---
        0
    ).otherwise(1)

    expr2 = F.when(
        df_360_agreement.start_date <= df.current_month_last_day,  # <---
        0
    ).otherwise(1)

    expr3 = _adjusted_date(id=[2, 3, 4, 7, 8, 9, 10])

    #
    _window = Window.partitionBy(df_contact.id, df_contact.source).orderBy(
        expr1,
        expr2,
        expr3.desc(),
    )

    return df.withColumn(
        'adjusted_date_row', F.row_number().over(_window)
    )

# create agreement status history
## contact + 360_agreement + package + location + agreement_status_history
def current_cmalash(df=df_agreement_status_history, current=None):
    df = current_ash(df, current)
    df = current_cmal_ash(df, current)

    df = df.filter(
        df_360_agreement_360_package.is_membership == 1
    )

    df = current_cmal_ash_window(df)

    return df

#====== [END] CELL 38 ======




#====== [MARKDOWN] CELL 39 ======

#====== [START] CELL 40 ======

MEMBERSHIP_SCHEMA = {
    'contact_id': 'string',
    'client_id': 'string',
    'activity_date': 'timestamp',
    'activity_type': 'string',
    'activity': 'string',
    'activitydetail1': 'string',
    'activitydetail2': 'string',
    'activitydetail3': 'string',
    'value': 'long',
    'region': 'string',
    'location': 'string',
    'activity_id': 'string',
}

def dict_to_when(mapping:dict, layer_col:str='layer'):
    when_expr = None
    
    for key, value in mapping.items():
        if when_expr is None:
            when_expr = F.when(F.col(layer_col) == key, F.lit(value))
        else:
            when_expr = when_expr.when(F.col(layer_col) == key, F.lit(value))
    
    return when_expr

def layer_to_activity(df, layer_col:str='layer'):
    df = df.withColumns({
        f'activitydetail{_+1}':F.col(layer_col)[_].cast('string') for _ in range(3)
    })

    return df

def write_table_mem(df, activity:str, activity_type:str='KPI', mode:str='append'):
    table_path = K.REPORT_TABLE('rpt_membership')

    if mode == 'overwrite':
        spark.sql(f'DROP TABLE IF EXISTS {table_path}')

    # explode layer to 3 activity
    df = layer_to_activity(df)

    #
    df = df.withColumns({
        'activity': F.lit(activity),
        'activity_type': F.lit(activity_type),
    })

    #
    _lost = [_ for _ in MEMBERSHIP_SCHEMA if _ not in df.columns]

    for _ in _lost:
        df = df.withColumn(
            _, 
            F.lit(None).cast(MEMBERSHIP_SCHEMA[_])
        )

    # filter null
    df = df.filter(F.col('activitydetail1').isNotNull())

    # select + cast
    df = df.select(
        *[F.col(_c).cast(_t) for _c, _t in MEMBERSHIP_SCHEMA.items()]
    )

    df.write.mode(mode).saveAsTable(table_path)

#====== [END] CELL 40 ======


#====== [START] CELL 41 ======

# ts = ['mem_kpi_leads', 'mem_kpi_booking', 'mem_kpi_show', 'mem_kpi_guest', 'mem_kpi_nmu', 'mem_kpi_renew_upgrade', 'mem_kpi_termination_workout', 'mem_kpi_suspension', 'mem_kpi_checkin']

# for _ in ts:
#     _c = spark.read.table(K.REPORT_TABLE(_)).columns
#     _sc = MEMBERSHIP_SCHEMA.keys()

#     print(
#         _,
#         len(_c), len(_sc),
#         list(set(_sc) - set(_c)),
#         list(set(_c) - set(_sc))
#     )

#====== [END] CELL 41 ======




#====== [MARKDOWN] CELL 42 ======

#====== [START] CELL 43 ======

leads_layer_map = {
    'KPI_XX_XX_BR': ['Buddy Referral', None, None],
    'KPI_XX_XX_TI': ['Telephone in', None, None],
    'KPI_XX_XX_CORP': ['Corporate leads', None, None],
    #
    'KPI_XX_XX_MLWEB': ['Market leads', 'Webform', None],
    'KPI_XX_XX_MLSEM': ['Market leads', 'Webform', None],
    'KPI_XX_XX_MLRB': ['Market leads', 'Webform', None],
    'KPI_XX_XX_MLO': ['Market leads', 'Webform', None],

    'KPI_XX_XX_O': ['Others', None, None],
}

#====== [END] CELL 43 ======


#====== [START] CELL 44 ======

df_leads_budget.withColumn('layer', K.LEAD_LAYER_WHEN()).filter(
    F.col('layer').isNull()
).count()

#====== [END] CELL 44 ======


#====== [START] CELL 45 ======

# create layer
df_leads_layer = df_leads_budget.withColumn('layer', K.LEAD_LAYER_WHEN())

# filter
df_leads_layer = df_leads_layer.filter(
    (~F.col('status').isin('Duplicate Lead', 'Existing Member', 'Duplicate', 'Invalid', 'Unreachable') | F.col('status').isNull())
)

# groupby
df_leads_rpt = df_leads_layer.groupBy(
    df_leads.date,
    df_leads.region_is_hk.alias('region'), 
    df_dim_location.dim_location_key,

    df_leads.marketing_campaign.alias('campaign'),
    df_leads.channel_code.alias('channel'),

    df_leads_layer.layer,
    df_leads.lead_source,

    df_user.user_name,
    
).agg(
    F.count('*').alias('count'),
    F.sum('per_budget_amount').alias('budget')
)

df_leads_rpt = df_leads_rpt.withColumn('layer', K.NO_LAYER_RENAME('LD'))

# HK 
df_leads_rpt = df_leads_rpt.filter(
    F.col('region').isin(['HK', 'SG'])
)

# K.write_table(df_leads_rpt, table='rpt_kpi_leads')

#====== [END] CELL 45 ======


#====== [START] CELL 46 ======

# create layer
df_leads_layer = df_leads_budget.withColumn('layer', K.LEAD_LAYER_WHEN()).filter(F.col('layer').isNotNull())

# filter
df_leads_layer = df_leads_layer.filter(
    (~F.col('status').isin('Duplicate Lead', 'Existing Member', 'Duplicate', 'Invalid', 'Unreachable') | F.col('status').isNull())
)

# 
_dfc = df_contact.select('id', 'ext_ref_contact_id').distinct()
df_leads_mem = df_leads_layer.join(
    _dfc,
    df_leads.member_num == df_contact.ext_ref_contact_id,
    'left'
)

df_leads_mem = df_leads_mem.groupBy(
    df_leads.date.alias('activity_date'),
    df_leads.region_is_hk.alias('region'), 
    df_dim_location.dim_location_key.alias('location'),

    df_leads.id.alias('activity_id'),
    df_leads.member_num.alias('client_id'),
    df_contact.id.cast('int').alias('contact_id'),

    df_leads_layer.layer,
).agg(
    F.count('*').alias('value'),
)

# HK 
df_leads_mem = df_leads_mem.filter(
    F.col('region').isin(['HK', 'SG'])
)

df_leads_mem.select('layer').distinct().display()

#
df_leads_mem = df_leads_mem.withColumn(
    'layer', dict_to_when(leads_layer_map)
)

write_table_mem(df_leads_mem, activity='Market Lead', mode='overwrite')

#====== [END] CELL 46 ======




#====== [MARKDOWN] CELL 47 ======

#====== [START] CELL 48 ======

# filter for booking
df_meeting_booking = df_meetings_location.filter(F.col('name').like('%1st time%'))

#
df_meeting_booking = df_meeting_booking.join(
    _dfc,
    df_leads.member_num == df_contact.ext_ref_contact_id,
    'left'
)

# groupby
df_meeting_booking = df_meeting_booking.groupBy(
    df_meetings_lead.meet_region.alias('region'),
    df_dim_location.dim_location_key.alias('location'),

    df_meetings.parent_id.alias('activity_id'),
    df_leads.member_num.alias('client_id'),
    df_contact.id.cast('int').alias('contact_id'),

    df_meetings.date_start.alias('activity_date'),

    df_meetings_lead.layer,
).agg(
    F.count('*').alias('value'),
)

# HK 
df_meeting_booking = df_meeting_booking.filter(
    F.col('region').isin(['HK', 'SG'])
)

#
df_meeting_booking = df_meeting_booking.withColumn(
    'layer', dict_to_when(leads_layer_map)
).filter(F.col('layer').isNotNull())

write_table_mem(df_meeting_booking, activity='Booking')

#====== [END] CELL 48 ======




#====== [MARKDOWN] CELL 49 ======

#====== [START] CELL 50 ======

# filter for booking
df_meeting_show = df_meetings_location.filter(
    (F.col('name').like('%1st time%')) & (df_meetings.status == 'Held')
)

#
df_meeting_show = df_meeting_show.join(
    _dfc,
    df_leads.member_num == df_contact.ext_ref_contact_id,
    'left'
)

# groupby
df_meeting_show = df_meeting_show.groupBy(
    df_meetings_lead.meet_region.alias('region'),
    df_dim_location.dim_location_key.alias('location'),

    df_meetings.parent_id.alias('activity_id'),
    df_leads.member_num.alias('client_id'),
    df_contact.id.cast('int').alias('contact_id'),

    df_meetings.date_start.alias('activity_date'),

    df_meetings_lead.layer,
).agg(
    F.count('*').alias('value'),
)

# HK 
df_meeting_show = df_meeting_show.filter(
    F.col('region').isin(['HK', 'SG'])
)

df_meeting_show = df_meeting_show.withColumn(
    'layer', dict_to_when(leads_layer_map)
).filter(F.col('layer').isNotNull())

write_table_mem(df_meeting_show, activity='Show')

#====== [END] CELL 50 ======




#====== [MARKDOWN] CELL 51 ======

#====== [START] CELL 52 ======

# create new layer
df_meeting_guest = df_meetings_location.withColumn('layer', K.GUEST_LAYER_WHEN())

# filter for booking
df_meeting_guest = df_meeting_guest.filter(
    (F.col('name').like('%1st time%')) & (df_meetings.status == 'Held')
)

#
df_meeting_guest = df_meeting_guest.join(
    _dfc,
    df_leads.member_num == df_contact.ext_ref_contact_id,
    'left'
)

# groupby
df_meeting_guest = df_meeting_guest.groupBy(
    df_meetings_lead.meet_region.alias('region'),
    df_dim_location.dim_location_key.alias('location'),

    df_meetings.parent_id.alias('activity_id'),
    df_leads.member_num.alias('client_id'),
    df_contact.id.cast('int').alias('contact_id'),

    df_meetings.date_start.alias('activity_date'),

    df_meetings_lead.layer,
).agg(
    F.count('*').alias('value'),
)

# HK 
df_meeting_guest = df_meeting_guest.filter(
    F.col('region').isin(['HK', 'SG'])
)

df_meeting_guest = df_meeting_guest.withColumn(
    'layer', dict_to_when({
        'KPI_GT_GT_WI': ['Total Guest', 'Show', None],
        'KPI_GT_GT_SW': ['Total Guest', 'Walk in + ASK-FOR', None],
    })
).filter(F.col('layer').isNotNull())

write_table_mem(df_meeting_guest, activity='Guest')

#====== [END] CELL 52 ======




#====== [MARKDOWN] CELL 53 ======

#====== [START] CELL 54 ======

# groupby count
df_contact_ct = df_contact_outer.select(
    df_membership.source.alias('region'), 
    df_membership.dim_location_key.alias('location'),

    df_360_agreement.agreement_no.alias('activity_id'),
    df_contact.ext_ref_contact_id.alias('client_id'),
    df_360_agreement.contact_id.cast('string'),

    F.to_date(df_membership.signed_date).alias('activity_date'),

    df_360_package.package_term_id, 
    df_package_sub_type.package_sub_type_code, 
    'nmu_cat', 'package_gp', 'is_all_clubs',
)

# create layer
df_nmuc = K.create_nested(
    df_contact_ct,
    [
        K.NMUC_LAYER_WHEN(),
        K.NMUC_LAYER_PACKAGE_WHEN(),
    ]
)

# group
df_nmuc = df_nmuc.groupBy(
    'region',
    'location',

    'activity_id',
    'client_id',
    'contact_id',
    'activity_date',

    'layer',
).agg(
    F.count('*').alias('value')
)

df_nmuc = df_nmuc.withColumn(
    'layer', dict_to_when({
        'due_3_months_unlimited': ['Due', '3 months', '3 months Unlimited / SG Lifestyle'],
        'due_3_months_unlimited_w': ['Due', '3 months', '3 months Unlimited w/Reformer Pilates / SG Transformation'],
        'due_3_months_6_class': ['Due', '3 months', '3 months 6 Class/6Visit Per Month / SG Starters'],
        'due_3_months_others': ['Due', '3 months', '3 months Others'],

        'due_6_months_unlimited': ['Due', '6 months', '6 months Unlimited / SG Lifestyle'],
        'due_6_months_unlimited_w': ['Due', '6 months', '6 months Unlimited w/Reformer Pilates / SG transformation'],
        'due_6_months_limited': ['Due', '6 months', '6 months limited'],
        'due_6_months_limited_w': ['Due', '6 months', '6 months Reformer Pilates (NMU)'],
        'due_6_months_6_class': ['Due', '6 months', '6 monhts 6 Class/6 Visit Per Month / SG Starters'],
        'due_6_months_others': ['Due', '6 months', '6 months Others'],

        'due_12_months_limited': ['Due', '12 months', '12 months Unlimited / SG Lifestyle'],
        'due_12_months_unlimited_w': ['Due', '12 months', '12 months Unlimited w/Reformer Pilates / SG transformation'],
        'due_12_months_6_class': ['Due', '12 months', '12 monhts 6 Class/6 Visit Per Month / SG starters'],
        'due_12_months_others': ['Due', '12 months', '12 months Others'],

        'due_others': ['Due', 'Due - Others', None],

        'prepaid_3_months': ['Prepaid', '3 months', None],
        'prepaid-6-months': ['Prepaid', '6 months', 'Prepaid 6 months'],
        'prepaid-6-months-wRP': ['Prepaid', '6 months', 'Prepaid 6 months w/Reformer Pilates'],
        'prepaid-12-months': ['Prepaid', '12 months', 'Prepaid 12 months'],
        'prepaid-12-months-wRP': ['Prepaid', '12 months', 'Prepaid 12 months w/Reformer Pilates'],
        'prepaid-18-months': ['Prepaid', '18 months', 'Prepaid 18 months'],
        'prepaid-18-months-wRP': ['Prepaid', '18 months', 'Prepaid 18 months w/Reformer Pilates'],
        'prepaid-24-months': ['Prepaid', '24 months', 'Prepaid 24 months'],
        'prepaid-24-months-wRP': ['Prepaid', '24 months', 'Prepaid 18 months w/Reformer Pilates'],
        'prepaid_others': ['Prepaid', 'Prepaid - Others', None],

        'package_1_months_all_clubs': ['Packages', '1 Month - All Clubs', None],
        'package_2_months_1_clubs': ['Packages', '2 Months - 1 Club', None], 
        'package_3_months_1_clubs': ['Packages', '3 Months - 1 Club', None], 
        'package_4_months_1_clubs': ['Packages', '4 Months - 1 Club', None], 
        'package_5_months_1_clubs': ['Packages', '5 Months - 1 Club', None], 
    })
)

write_table_mem(df_nmuc, activity='NMU')

#====== [END] CELL 54 ======




#====== [MARKDOWN] CELL 55 ======

#====== [START] CELL 56 ======

# inner join 360 agreement
df_membership_agreement_inner = df_membership_agreement.filter(
    F.col('package_source_id').isNotNull()
)

df_contact_360_agreement = df_contact.join(
    df_membership_agreement_inner,
    df_contact.source_id == df_360_agreement.source_contact_id,
    'inner'
)

# loop for snapshot
for _i, _m in enumerate(get_months(1)):

    # join silver_agreement_status_history
    df_ash_base = current_ash(current=_m).filter(
        (F.col('active') == 1) &
        (F.col('effective_date') >= F.col('current_month')) &
        (F.col('effective_date') < F.col('next_month'))
    )

    df_c_ash = df_contact_360_agreement.join(
        df_ash_base,
        df_360_agreement.source_id == df_agreement_status_history.source_agreement_id,
        'left'
    )

    # join package + package_sub_type
    df_c_pst = df_c_ash.join(
        df_package_sub_type,
        'package_sub_type_bk',
        'left'
    )

    # join locatoin + 360f_location
    df_c_location_360f = df_c_pst.join(
        df_location_full,
        (df_contact.home_location_id == df_location_full.location_id) 
        & (df_contact.source == df_location_full.location_source),
        'left'
    )

    # filter
    df_c_location_360f = df_c_location_360f.filter(
       df_360_agreement_package.is_membership == 1
    )

    # create
    df_c_location_360f = df_c_location_360f.withColumns({
        'is_terminated': K.IS_TERMINATED(id=df_agreement_status_history.agreement_status_id),
        'is_effective_date': K.IS_EFFECTIVE_DATE(id=df_agreement_status_history.agreement_status_id),

        'region': F.substring('dim_location_key', 0, 2),
    })

    # window
    date_window = Window.partitionBy(
        df_contact.id, df_contact.source
    ).orderBy(
        F.col('is_effective_date').desc()
    )

    df_c_window = df_c_location_360f.withColumn(
        'rn', F.row_number().over(date_window)
    )

    # filter
    df_c_window = df_c_window.filter(
        (F.col('is_terminated') == 1) & 
        (F.col('rn') == 1) &

        df_agreement_status_history.effective_date.isNotNull()
    )

    df_c_layer = K.create_nested(
        df_c_window,
        [
            K.TERMINATION_ACTUAL_WHEN(),
            K.TERMINATION_ACTUAL_SHORT_WHEN(pstid=df_package.package_sub_type_id),
        ]
    )

    # group by
    df_c_ct = df_c_layer.groupBy(
        'region', 
        df_location_full.dim_location_key.alias('location'),

        df_contact.ext_ref_contact_id.alias('client_id'),
        df_360_agreement.contact_id,

        F.to_date(df_agreement_status_history.effective_date).alias('activity_date'),

        'layer',
    ).agg(
        F.count('*').alias('value')
    )

    df_c_ct = df_c_ct.withColumn(
        'layer', dict_to_when({
            'dues': ['Dues termination', None, None],
            'prepaid': ['Prepaid Termination', None, None],
            'short term': ['Short Term Termination / 10/20 class package members', None, None],
        })
    )

    write_table_mem(df_c_ct, activity='Termination - Actual per last workout date')

#====== [END] CELL 56 ======




#====== [MARKDOWN] CELL 57 ======

#====== [START] CELL 58 ======

df_agreement = spark.read.table(K.TABLE('silver_agreement'))

# create join key
df_agreement = df_agreement.withColumns({
    'package_bk': K.concat_id(df_agreement, cols=['source', 'package_id']),
    'contact_bk': K.concat_id(df_agreement, cols=['source', 'contact_id']),
    'location_bk': K.concat_id(df_agreement, cols=['source', 'revenue_location_id']),
    'agreement_bk': K.concat_id(df_agreement, cols=['source', 'from_agreement_id']),
})

# join package
df_agreement_package = df_agreement.join(
    df_package,
    'package_bk',
    'left'
)

# join package type
df_agreement_package_type = df_agreement_package.join(
    df_package_type,
    'package_type_bk',
    'left'
)

# join package term
df_agreement_package_term = df_agreement_package_type.join(
    df_package_term,
    'package_term_bk',
    'left'
)

# join contact
df_agreement_contact = df_agreement_package_term.join(
    df_contact,
    df_agreement.contact_bk ==  df_contact.bk,
    'left'
)

# join invoice_detail
df_agreement_invoice = df_agreement_contact.join(
    df_invoice_detail,
    df_agreement.bk ==  df_invoice_detail.invoice_source_item_id,
    'left'
)

###

# join silver_package_sub_type
df_agreement_package_sub_type = df_agreement_invoice.join(
    df_package_sub_type,
    'package_sub_type_bk',
    'left'
)

# join silver_package_book_limit_line
df_agreement_book_limit = df_agreement_package_sub_type.join(
    df_book_limit,
    df_agreement.package_bk ==  df_book_limit.book_limit_bk,
    'left'
)

# join silver_location + 360f
df_agreement_location = df_agreement_book_limit.join(
    df_location_full,
    'location_bk',
    'left'
)

# self join last agreement
df_agreement_last = df_agreement_location.select(
    df_agreement.bk,
    df_agreement.id,
    df_agreement.agreement_no,
    df_package_term.package_term_name,
    df_package.package_name,
    df_invoice_detail.invoice_category_id
)

df_agreement_last = K.prefix(df_agreement_last, 'last')

df_agreement_join = df_agreement_location.join(
    df_agreement_last,
    df_agreement.agreement_bk == df_agreement_last.last_bk,
    'left'
)

# filter
df_agreement_join = df_agreement_join.filter(
    df_package.package_type_id == 1
)

# window 
contact_window = Window.partitionBy(df_agreement.contact_id).orderBy(
    'signed_date', F.to_date('end_date'), df_agreement.agreement_no
)

lag_map = {
    'last_id': df_agreement.id,
    'last_agreement_no': df_agreement.agreement_no,
    'last_package_term_name': df_package_term.package_term_name,
    'last_package_name': df_package.package_name,
    'last_invoice_category_id': df_invoice_detail.invoice_category_id,
}

for _c, _k in lag_map.items():
    df_agreement_join = df_agreement_join.withColumn(
        _c, 
        F.coalesce(
            _c,
            F.lag(_k).over(contact_window)
        )
    )

# select
df_agreement_select = df_agreement_join.select(

    F.to_date(df_agreement.signed_date).alias('activity_date'),

    df_agreement.agreement_no.alias('activity_id'),
    df_contact.ext_ref_contact_id.alias('client_id'),

    # 
    df_agreement.contact_id,
    df_agreement.signed_date,
    df_agreement.from_agreement_id,

    F.substring(df_location_full.dim_location_key, 0, 2).alias('region'),
    df_location_full.dim_location_key.alias('location'), 
    df_location_full.location_code,

    df_package.package_name,
    df_package.package_sub_type_id, 

    df_package_sub_type.package_sub_type_code,

    df_package_term.package_term_name,

    'last_package_term_name',
)


# create renew_upgrade
df_agreement_select = df_agreement_select.withColumn(
    'layer',
    K.RENEWAL_UPGRADE_WHEN()
)

df_agreement_ct = df_agreement_select.groupBy(
    'region',
    'location',

    'activity_id',
    'client_id',
    'contact_id',
    'activity_date',
    
    'layer',
).agg(
    F.count('*').alias('value')
)

df_agreement_ct = df_agreement_ct.withColumn(
    'layer', dict_to_when({
        'drop-in': ['Drop-in\'s', None, None], 
        'up_reform_pilates': ['Updrade', 'Upgrade Reformer Pilates', None], 
        'up_dp': ['Updrade', 'Upgrade (Dues to Prepaid)', None], 
        'up_pp': ['Updrade', 'Upgrade (Prepaid to Prepaid)', None],
        'up_dd': ['Updrade', 'Upgrade (Dues to Dues)', None],
        'renew_pp': ['Renewal', 'Renewal (Prepaid to Prepaid Renew)', None], 
        'renew_pd': ['Renewal', 'Renewal (Prepaid Renewal to Dues)', None], 
        'renew_reform_pilates': ['Renewal', 'Renewal Reformer Pilates', None], 
    })
)

write_table_mem(df_agreement_ct, activity='Renewal / Upgrade')

#====== [END] CELL 58 ======




#====== [MARKDOWN] CELL 59 ======

#====== [START] CELL 60 ======

# loop for snapshot
for _i, _m in enumerate(get_months(1)):

    df_tr = current_cmalash(current=_m)

    # filter
    df_tr_filter = df_tr.filter(
        (df_tr.adjusted_date_row == 1) &
        (
            df_agreement_status_history.agreement_status_id.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]) |
            (
                df_agreement_status_history.agreement_status_id.isNull() &
                (F.ifnull(df_360_agreement.prev_gap_reference, F.lit('2099-12-31')) >= df_tr.current_month_last_day) & # <---
                (F.least(df_360_agreement.start_date, df_360_agreement.signed_date) < df_tr.current_month_last_day) # <---
            )
        )
    )


    df_tr_select = df_tr_filter.select(
        df_360_agreement.contact_id,
        df_contact.ext_ref_contact_id,

        df_contact.source,
        #
        df_contact.source_id,

        df_360_agreement.source_contact_id,
        df_360_agreement.prev_gap_reference, #end_date
        df_360_agreement.signed_date,

        df_360_package.package_term_id,
        df_360_package.package_sub_type_id,
        df_360_package.min_committed_month,

        df_agreement_status_history.agreement_status_id,
        
        df_location_full.dim_location_key,
    )

    # self inner join contact
    df_base_contact = df_contact.select('source_id', 'source_member_id')

    df_tr_contact = df_tr_select.join(
        df_base_contact,
        df_base_contact.source_id == df_360_agreement.source_contact_id,
        'inner'
    )

    # 
    df_tr_termination = df_termination.join(
        df_tr_contact,
        df_contact.source_member_id == df_termination.termination_region_rssid,
        'left'
    )

    df_tr_layer = df_tr_termination.withColumn(
        'layer', K.TERMINATION_REQUEST_WHEN(),
    )

    # groupby
    df_tr_ct = df_tr_layer.groupBy(
        F.col('termination_date').alias('activity_date'), 
        F.col('source').alias('region'),

        df_360_agreement.contact_id,
        df_contact.ext_ref_contact_id.alias('client_id'),

        df_location_full.dim_location_key.alias('location'),
        'layer',
    ).agg(
        F.count('*').alias('value')
    )

    # # create snapshot date -- only latest version
    # df_tr_ct = df_tr_ct.withColumn('snapshot_date', _m)

    df_tr_ct = df_tr_ct.withColumn(
        'layer', dict_to_when({
            'dues_3': ['Dues termination', '3-month', 'autopay members'],
            'dues_6': ['Dues termination', '6-month', 'autopay members'],
            'dues_12': ['Dues termination', '12-month', 'autopay members'],
            'prepaid_3': ['Prepaid Termination', '3-month', 'prepaid members'],
            'prepaid_6': ['Prepaid Termination', '6-month', 'prepaid members'],
            'prepaid_12': ['Prepaid Termination', '12-month', 'prepaid members'],
            'prepaid_18': ['Prepaid Termination', '18-month', 'prepaid members'],
            'prepaid_24': ['Prepaid Termination', '24-month', 'prepaid members'],
            'shortterm': ['Short Term Termination', 'Short Term Termination / 10/20 class package members', None],
        })
    )

    write_table_mem(df_tr_ct, activity='Termination - Per request received date')

#====== [END] CELL 60 ======




#====== [MARKDOWN] CELL 61 ======

#====== [START] CELL 62 ======

df_checkin = spark.read.table(K.TABLE('silver_check_in'))

time_scale = 1000
df_checkin = df_checkin.withColumns({
    'dim_location_key': K.concat_id(df_checkin, cols=['source', 'location_id']),
    'source_client_service_id': K.concat_id(df_checkin, cols=['source', 'client_service_id']),

    'check_in_utc': F.from_unixtime(F.col('check_in_time') / time_scale),
}).withColumn(
    'check_in_datetime', F.from_utc_timestamp('check_in_utc', 'Asia/Hong_Kong')
).withColumn(
    'check_in_date', F.to_date('check_in_datetime'),
).withColumns({
    'check_in_year': F.year('check_in_date'),
    'check_in_month': F.month('check_in_date'),
    'check_in_day': F.day('check_in_date'),
})

# join agreement + package
df_checkin_3ap = df_checkin.join(
    df_360_agreement_package,
    df_checkin.source_client_service_id == df_360_agreement.source_ext_ref_agreement_id,
    'left'
)

# join package type
df_checkin_3ap_package = join_packages(df_checkin_3ap, _type=True)

#
df_checkin_layer = df_checkin_3ap_package.groupBy(
    df_360_agreement.contact_id,
    df_checkin.id.alias('activity_id'),
    F.col('dim_location_key').alias('location'),
    F.substring('dim_location_key', 0, 2).alias('region'), 

    F.col('check_in_date').alias('activity_date'),

    F.array(df_package_type.package_type_name, F.lit(None).cast('string'), F.lit(None).cast('string')).alias('layer')
).agg(
    F.count('*').alias('value')
)

write_table_mem(df_checkin_layer, activity='Check-in')

#====== [END] CELL 62 ======




#====== [MARKDOWN] CELL 63 ======

#====== [START] CELL 64 ======

def current_suspension(df=df_agreement_status_history, current=None):
    df_ash_current = set_current(df, current=current)

    # inner join package (filter)
    df_suspension = df_360_agreement_package.filter(
        df_package.package_source_id.isNotNull()
    )

    # join contact
    df_suspension_contact = df_suspension.join(
        df_contact,
        df_360_agreement.source_contact_id == df_contact.source_id,
        'inner'
    )

    # join agreement status history
    df_suspension_ash = df_suspension_contact.join(
        df_ash_current,
        df_360_agreement.source_id == df_agreement_status_history.source_agreement_id,
        'inner'
    ).filter(
        df_agreement_status_history.active == 1
    )


    # join package sub type
    df_suspension_pst = join_packages(df_suspension_ash, _sub=True)

    # join location
    df_suspension_loc = df_suspension_pst.join(
        df_location_full,
        df_location.location_source_id == df_360_agreement.source_revenue_location_id,
        'left'
    )

    # filter
    region_col = F.substring('dim_location_key', 0, 2)

    df_suspension_filter = df_suspension_loc.filter(
        (df_360_agreement.package_type_id == 1) &
        df_360_agreement.parent_agreement_id.isNull() &
        (df_agreement_status_history.agreement_status_id == 6) &
        K.SUSPENSION_FILTER(reg=region_col) &

        # new filter
        (df_agreement_status_history.effective_date <= df_ash_current.current_month_last_day) &
        (df_agreement_status_history.max_ending_date >= df_ash_current.current_month)
    )

    # groupby count
    df_suspension_ct = df_suspension_filter.groupBy(
        df_360_agreement.contact_id,
        F.to_date(df_agreement_status_history.effective_date).alias('activity_date'),
        df_location_full.dim_location_key.alias('location'),
        region_col.alias('region'),
        F.lit(['Suspension', None, None]).alias('layer'),
    ).agg(
        F.count('*').alias('value')
    )

    write_table_mem(df_suspension_ct, activity='Suspension')

for _i, _m in enumerate(get_months(1)):
    current_suspension(current=_m)

#====== [END] CELL 64 ======




#====== [MARKDOWN] CELL 65 ======

#====== [START] CELL 66 ======

sales_d={
    'stage_fct_membership': {'id': 'dim_contact_key', 'sales': 'invoice_detail_amount'},
    'stage_fct_agreement': {'id': 'dim_contact_key', 'sales': 'total_revenue_agreement'},
    'stage_fct_product': {'id': 'dim_contact_key', 'sales': 'total_revenue_product'},
    'stage_fct_outstanding': {'id': 'fct_payment_detail_key', 'sales': 'total_amount'},
}

layer_d={
    137: ('Joining fee (dues)', None),
    138: ('Prepaid', None),
    139: ('Prepaid Fee', None),
    140: ('Prepaid Fee', 'Reformer Pilates'),
    141: ('Optimose360 (SG) - autopay / due', None),
    142: ('Short Package', None),
    143: ('Adjustment 1st & Last Month Dues', None),
    144: ('Adjustment 1st & Last Month Dues', '1st & last month - Dues'),
    145: ('Adjustment 1st & Last Month Dues', '1st & Last Month Dues (Rejoin/Renewal/Upgrade)'),
    146: ('Adjustment 1st & Last Month Dues', '1st & Last Month Dues (HK Group Reformer Pilates) --- NMU'),
    147: ('Adjustment 1st & Last Month Dues', '1st & Last Month Dues Upgrade (Group Reformer Pilates)'),
    148: ('Adjustment 1st & Last Month Dues', '1st & Last Month Dues Renew (Group Reformer Pilates)'),
    149: ('Adjustment 1st & Last Month Dues', 'Pro-rata Fee'),

    150: ('Renewal sales - Prepaid', None),
    151: ('Renewal sales - Prepaid', 'Renew Fee - Reformer Pilates'),
    152: ('Renewal sales - Prepaid', 'Renewal (Prepaid/Due to prepaid renewal)'),
    153: ('Upgrade sales - Due to Prepaid', None),
    154: ('Upgrade sales - Due to Prepaid', 'Upgrade (Dues to Prepaid)'),
    155: ('Upgrade sales - Due to Prepaid', 'Upgrade Fee - Reformer Pilates'),
    156: ('Upgrade sales - Due to Prepaid', 'Upgrade (Prepaid to Prepaid)'),
    157: ('Upgrade sales - Due to Prepaid', 'Upgrade (Prepaid/Due to Due)'),
    201: ('Auto Pay / Due', None),

    159: ('Personal Training (POS)', None),
    160: ('Personal Training (New)', None),
    161: ('Personal Training (Renew)', None),
    162: ('Personal Training / Nutrition (Online) / Outdoor', None),

    163: ('Private Yoga (POS)', None),
    164: ('Private Yoga (New)', None),
    165: ('Private Yoga (Renew)', None),
    166: ('Private Yoga (Online)', None),
    167: ('Small Group Privates', None),
    168: ('Trial Yoga Class (HK)', None),

    169: ('HK Group Reformer Pilates (POS) / SG Personal Pilates (POS)', None),
    170: ('HK Group Reformer Pilates (New) / SG Personal Pilates (New)', None),
    171: ('HK Group Reformer Pilates (Renew) / SG Personal Pilates (Renew)', None),
    172: ('HK Group Reformer Pilates (Online) / SG Personal Pilates (Online)', None),
    173: ('Consolidated package (SG)', None),

    174: ('Optimise360 - packages (POS)', None),
    175: ('Optimise360 - packages (New)', None),
    176: ('Optimise360 - packages (Renew)', None),
    
    178: ('Academy (Yoga) - SGP', None),
    179: ('Academy (Pilates) - SGP', None),
    180: ('Academy (GX) - SGP', None),
    181: ('Academy (PT) - SGP', None),
    182: ('Nood Food / Pure Kitchen / Lincoln Café', None),
    183: ('Corporate Sales / events', None),
    184: ('Gross', None),
    185: ('Lockers', None),
    186: ('PURE+ packages (SGP)', None),
    187: ('Yoga Workshop (HK/China)', None),
    188: ('GX Workshop', None),
    189: ('Others (SGT, Lost Card, Suspension, Overnight, Private Swim/Dance/Pilates, Pad Lock, Drop In etc)', None),
    190: ('Others (Dues / Service Agreement)', None),
    191: ('Others (Joining Fee / Prepayment / Service Agreement)', None),
    192: ('Total ARPU Sales (PT, PY, Pilates, etc.)', None),
    193: ('Outstanding Collections (Clubs & Head Office)', None)
}

#====== [END] CELL 66 ======


#====== [START] CELL 67 ======

for _t, _c in sales_d.items():
    print(
        _t,
        [_ for _ in spark.read.table(K.REPORT_TABLE(_t)).columns if 'date' in _]
    )

#====== [END] CELL 67 ======


#====== [START] CELL 68 ======

def get_activity(layer:int):
    if 159 <= layer <= 162:
        return 'Personal Training'
    elif 163 <= layer <= 168:
        return 'Private Yoga'
    elif 169 <= layer <= 173:
        return 'Reformer Pilates / Personal Pilates'
    elif 174 <= layer <= 176:
        return 'Optimise360 - packages (SG)'
    elif 178 <= layer <= 193:
        return 'Others'
    else:
        return 'New members sales >> NMU'
    
def get_activity_id(layer:int):
    if layer == 193: # outstanding no col
        return F.lit(None).cast('string')
    elif (layer == 201) | (layer <= 157):
        return F.col('fct_agreement_key')
    else:
        return F.col('fct_invoice_detail_key')


for _no, _name in layer_d.items():
    for _table, _value in sales_d.items():
        df = spark.read.table(K.REPORT_TABLE(_table))

        _cols = [_ for _ in df.columns if str(_no) in _]
        for _col in _cols:

            _df = df.filter(
                F.col(_col) == 1
            ).select(
                F.col(_value['id']).alias('contact_id'),
                get_activity_id(_no).alias('activity_id'),
                F.col(_value['sales']).alias('value'),
                
                F.array(
                    F.lit(_name[0]),
                    F.lit(_name[1]),
                    F.lit(None).cast('string')
                ).alias('layer'),

                F.col('post_date').alias('activity_date'),
                F.col('source').alias('region'),
                F.col('dim_location_key').alias('location'),
            )

            write_table_mem(_df, activity=get_activity(_no), activity_type='Revenue')

#====== [END] CELL 68 ======


