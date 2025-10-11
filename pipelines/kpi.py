#====== [START] CELL 1 ======

#
import sys
sys.path.append('./')

import kpi_utils

import importlib
importlib.reload(kpi_utils)
import helpers
importlib.reload(helpers)

import kpi_utils as K
import helpers as H

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

# vw_lead_account_modal
df_vw_lead_account_modal = H.fetch_table(spark, 'silver_vw_lead_account_modal_2023')
df_vw_lead_account_modal = df_vw_lead_account_modal.withColumn(
    'creation_date', F.to_date(F.col('creation_date'))
)

# silver_users
df_users = H.fetch_table(spark, 'silver_users').select(
    F.col('id').alias('user_id'),
    'user_name',
)

# signed dim_location for vw_lead_account_modal
df_signed_dim_location_360f = K.prefix(df_dim_location_360f, 'signed_dim_location')

# suitecrm campaign
df_crm_campaign = H.fetch_table(spark, 'silver_campaign')
df_crm_campaign = df_crm_campaign.withColumn(
    'source_id', K.concat_id(df_crm_campaign, cols=['source', 'id'])
)
df_crm_campaign = K.prefix(df_crm_campaign, 'campaign')

# suitecrm channel
df_crm_channel = H.fetch_table(spark, 'silver_channel_code')
df_crm_channel = df_crm_channel.withColumn(
    'source_id', K.concat_id(df_crm_channel, cols=['source', 'id'])
)
df_crm_channel = K.prefix(df_crm_channel, 'channel')

# suitecrm budget
df_crm_budget = H.fetch_table(spark, 'silver_uat_marketing_budget')

# join budget to campaign, channel
df_crm_budget = df_crm_budget.join(
    df_crm_campaign,
    (df_crm_budget.campaign_id == df_crm_campaign.campaign_id) & (df_crm_budget.source == df_crm_campaign.campaign_source),
    'left'
).join(
    df_crm_channel,
    (df_crm_budget.channel_code_id == df_crm_channel.channel_id) & (df_crm_budget.source == df_crm_channel.channel_source),
    'left'
).select(
    df_crm_campaign.campaign_code.alias('budget_campaign_code'),
    df_crm_channel.channel_code.alias('budget_channel_code'),

    df_crm_budget.source.alias('budget_source'),
    df_crm_budget.budget_amount
)

# suitecrm lead to get lead's campaign for campaign budget
df_crm_lead = H.fetch_table(spark, 'silver_leads').select(
    F.col('id').alias('lead_id'),
    F.col('marketing_campaign').alias('lead_marketing_campaign_code')
)


# vw_lead_account_modal join dim_location and signed dim_location
df_fct_leads = df_vw_lead_account_modal.join(
    df_dim_location_360f,
    (df_dim_location_360f.dim_location_code == df_vw_lead_account_modal.final_location)
    & (df_dim_location_360f.dim_location_code.isNotNull())
    & (df_dim_location_360f.dim_location_source == df_vw_lead_account_modal.final_region),
    'left'
).join(
    df_signed_dim_location_360f,
    (df_signed_dim_location_360f.signed_dim_location_code == df_vw_lead_account_modal.signed_location)
    & (df_signed_dim_location_360f.signed_dim_location_code.isNotNull())
    & (df_signed_dim_location_360f.signed_dim_location_source == df_vw_lead_account_modal.final_region),
    'left'
).join(
    df_users,
    df_vw_lead_account_modal.assigned_user_id == df_users.user_id,
    'left'
).join(
    df_crm_lead,
    df_vw_lead_account_modal.id == df_crm_lead.lead_id,
    'left'
).join(
    df_crm_budget,
    (df_crm_lead.lead_marketing_campaign_code == df_crm_budget.budget_campaign_code) &
    (df_vw_lead_account_modal.final_channel_code == df_crm_budget.budget_channel_code) &
    (df_vw_lead_account_modal.final_region == df_crm_budget.budget_source),
    'left'
).filter(
    ~F.col('final_status').isin('Duplicate', 'Duplicate Lead', 'Existing Member', 'Blacklisted', 'Invalid', 'Unreachable')
)

# create per budget by window
_window = Window.partitionBy('final_marketing_campaign', 'final_channel_code', 'final_region', 'budget_amount')
df_fct_leads = df_fct_leads.withColumn(
    'no_leads_by_campaign', F.count('*').over(_window)
)

df_fct_leads = df_fct_leads.withColumn(
    'per_budget_amount', F.col('budget_amount') / F.col('no_leads_by_campaign')
)


# categorise for dashboard
df_fct_leads = df_fct_leads.withColumn(
    'final_lead_source', F.lower(df_fct_leads['final_lead_source']) # lowercase, for easier matching
).withColumns({
    'market_lead': H.MARKET_LEAD_WHEN(),
    'lead_source_group': H.value_when(F.col('final_lead_source'), H.CONST_SOURCE_GROUP_MAPS, 'isin').otherwise(''),
}).withColumn(
    'group', K.ML_GROUP_WHEN()
)

# NMU based on signed_location
df_fct_nmu = df_fct_leads.filter(
    (F.col('signed_location') != '0')
    & (F.col('signed_location').isNotNull())
)

# leads based on final_location
df_fct_leads = df_fct_leads.filter(
    (F.col('final_location') != '0')
    & (F.col('final_location').isNotNull())
)

# add layer column
df_fct_leads = df_fct_leads.withColumn(
    'layer', K.LEAD_LAYER_WHEN(),
)

# distinct lead (df_fct_leads -> df_fct_no_leads)
df_fct_no_leads = df_fct_leads.groupBy(
    df_fct_leads.creation_date,
    df_fct_leads.final_region,
    df_fct_leads.dim_location_key,
    df_fct_leads.final_channel_code,
    df_fct_leads.layer,
    df_fct_leads.final_lead_source,
    df_users.user_name,
    df_crm_lead.lead_marketing_campaign_code
).agg(
    F.countDistinct('id').alias('count'),
    F.sum('per_budget_amount').alias('budget')
)

# rename columns for dashboard
col_rename_dict = {
    'creation_date': 'date',
    'final_region': 'region',
    'final_channel_code': 'channel',
    'final_lead_source': 'lead_source',
    'lead_marketing_campaign_code': 'campaign'
}
df_fct_no_leads = H.rename_cols(df_fct_no_leads, col_rename_dict)

# rename layer
df_fct_no_leads = df_fct_no_leads.withColumn('layer', K.NO_LAYER_RENAME('LD')).withColumns({
    'date': F.to_date(F.col('date')),
    'month': (F.year('date') * 100 + F.month('date')),
    'year': F.year('date')
})

# SG and HK only
df_fct_no_leads = df_fct_no_leads.filter(
    F.col('region').isin(['HK', 'SG'])
)

# write table,  rpt_kpi_leads
K.write_table(df_fct_no_leads, table='rpt_kpi_leads')

#### NMU
# add layer column
df_fct_nmu = df_fct_nmu.withColumn(
    'layer', K.NMU_LAYER_WHEN(),
)

# distinct nmu/success
df_fct_no_nmu = df_fct_nmu.groupBy(
    df_fct_nmu.success_date,
    df_fct_nmu.final_region,
    df_fct_nmu.dim_location_key,
    df_fct_nmu.final_channel_code,
    df_fct_nmu.layer,
    df_fct_nmu.final_lead_source
).agg(
    F.countDistinct('id').alias('count')
)

# rename columns for dashboard
col_rename_dict = {
    'success_date': 'date',
    'final_region': 'region',
    'final_channel_code': 'channel',
    'final_lead_source': 'lead_source'
}
df_fct_no_nmu = H.rename_cols(df_fct_no_nmu, col_rename_dict)

# rename layer
df_fct_no_nmu = df_fct_no_nmu.withColumn('layer', K.NO_LAYER_RENAME('NMUCH'))

# add month & year
df_fct_no_nmu = df_fct_no_nmu.withColumns({
    'date': F.to_date(F.col('date')),
    'month': (F.year('date') * 100 + F.month('date')),
    'year': F.year('date')
})['date', 'region', 'dim_location_key', 'layer', 'count', 'month', 'year'] #rearrange columns

# SG and HK only
df_fct_no_nmu = df_fct_no_nmu.filter(
    F.col('region').isin(['HK', 'SG'])
)

# Write to table
K.write_table(df_fct_no_nmu, table='rpt_kpi_nmu_channel')

### booking & show
# vw_meeting
df_vw_meeting = H.fetch_table(spark, 'silver_vw_meetings')
# filter deleted & only 1st time
df_vw_meeting = df_vw_meeting.withColumn(
    'appointment_entered_date', F.to_date(
        df_vw_meeting.date_entered + F.expr('INTERVAL 8 HOUR')
    )  # +8 UTC
)

# renaming columns
df_vw_meeting = H.rename_cols(df_vw_meeting, {'date_start_tz': 'appointment_date'})

# join
df_fct_booking = df_vw_meeting.join(
    df_vw_lead_account_modal,
    (df_vw_lead_account_modal.id == df_vw_meeting.parent_id),
    'inner'
).join(
    df_dim_location_360f,
    (df_dim_location_360f.dim_location_code == df_vw_meeting.location)
    & (df_dim_location_360f.dim_location_code.isNotNull())
    & (df_dim_location_360f.dim_location_source == df_vw_lead_account_modal.final_region),
    'left'
).join(
    df_users,
    df_vw_lead_account_modal.assigned_user_id == df_users.user_id,
    'left'
).join(
    df_crm_lead,
    df_vw_lead_account_modal.id == df_crm_lead.lead_id,
    'left'
).withColumn(
    'final_lead_source', F.lower(df_vw_lead_account_modal['final_lead_source'])
)

# categorise lead_source_group and market lead
df_fct_booking = df_fct_booking.withColumns({
    'market_lead': H.MARKET_LEAD_WHEN(),
    'lead_source_group': H.value_when(F.col('final_lead_source'), H.CONST_SOURCE_GROUP_MAPS, 'isin').otherwise(''),
}).withColumn(
    'group', K.ML_GROUP_WHEN()
).filter(
    (df_fct_booking.deleted == False)
    & (df_fct_booking.name == '1st time')
    & (F.col('lead_source_group') != 'WI') # Walk-in is excluded
)

# add layer column
df_fct_booking = df_fct_booking.withColumn(
    'layer', K.LEAD_LAYER_WHEN()
)

# distinct appointment
df_fct_no_booking = df_fct_booking.groupBy(
    df_fct_booking.appointment_date,
    df_fct_booking.final_region,
    df_fct_booking.dim_location_key,
    df_crm_lead.lead_marketing_campaign_code,
    df_fct_booking.final_channel_code,
    df_fct_booking.final_lead_source,
    df_fct_booking.layer,
    df_users.user_name
).agg(
    F.countDistinct('silver_vw_meetings.id').alias('count')
)

# rename columns for dashboard
col_rename_dict = {
    'appointment_date': 'date',
    'final_region': 'region',
    'final_channel_code': 'channel',
    'final_lead_source': 'lead_source',
    'lead_marketing_campaign_code': 'campaign'
}
df_fct_no_booking = H.rename_cols(df_fct_no_booking, col_rename_dict)

# rename layer
df_fct_no_booking = df_fct_no_booking.withColumn('layer', K.NO_LAYER_RENAME('BK'))

#
df_fct_no_booking = df_fct_no_booking.withColumns({
    'date': F.to_date(F.col('date')),
    'month': (F.year('date') * 100 + F.month('date')),
    'year': F.year('date')
}) #rearrange columns

#only SG and HK
f_fct_no_booking = df_fct_no_booking.filter(
    F.col('region').isin(['HK', 'SG'])
)

# write table, rpt_kpi_booking
K.write_table(f_fct_no_booking, table='rpt_kpi_booking')


# booking/appointment show
df_fct_show = df_fct_booking.filter(
    (F.col('status') == 'Held')
)

# assign layer
df_fct_guest = df_fct_show.withColumn(
    'layer', H.value_when(F.col('group'), H.get_layer_hierachy_maps(4, 'rpt_kpi_guest'), 'isin').otherwise('KPI_GT_GT_SW')
)

# distinct guest
df_fct_no_guest = df_fct_guest.groupBy(
    df_fct_guest.appointment_date,
    df_fct_guest.final_region,
    df_fct_guest.dim_location_key,
    df_crm_lead.lead_marketing_campaign_code,
    df_fct_guest.final_channel_code,
    df_fct_guest.final_lead_source,
    df_fct_guest.layer,
    df_users.user_name
).agg(
    F.countDistinct('silver_vw_meetings.id').alias('count')
)

# rename columns for dashboard
col_rename_dict = {
    'appointment_date': 'date',
    'final_region': 'region',
    'final_channel_code': 'channel',
    'final_lead_source': 'lead_source',
    'lead_marketing_campaign_code': 'campaign'
}
df_fct_no_guest = H.rename_cols(df_fct_no_guest, col_rename_dict)

# add temp columns
df_fct_no_guest = df_fct_no_guest.withColumns({
    'date': F.to_date(F.col('date')),
    'month': (F.year('date') * 100 + F.month('date')),
    'year': F.year('date')
})

# Get from HK and SG only
df_fct_no_guest = df_fct_no_guest.filter(
    F.col('region').isin(['HK', 'SG'])
)

# write table, rpt_kpi_guest
K.write_table(df_fct_no_guest, table='rpt_kpi_guest') # guest and show has the same definition in SuiteCRM

# distinct appointment
df_fct_no_show = df_fct_show.groupBy(
    df_fct_show.appointment_date,
    df_fct_show.final_region,
    df_fct_show.dim_location_key,
    df_crm_lead.lead_marketing_campaign_code,
    df_fct_show.final_channel_code,
    df_fct_show.final_lead_source,
    df_fct_show.layer,
    df_users.user_name
).agg(
    F.countDistinct('silver_vw_meetings.id').alias('count')
)

# rename columns for dashboard
col_rename_dict = {
    'appointment_date': 'date',
    'final_region': 'region',
    'final_channel_code': 'channel',
    'final_lead_source': 'lead_source',
    'lead_marketing_campaign_code': 'campaign'
}
df_fct_no_show = H.rename_cols(df_fct_no_show, col_rename_dict)

# rename layer
df_fct_no_show = df_fct_no_show.withColumn('layer', K.NO_LAYER_RENAME('SW'))

# add temp columns
df_fct_no_show = df_fct_no_show.withColumns({
    'date': F.to_date(F.col('date')),
    'month': (F.year('date') * 100 + F.month('date')),
    'year': F.year('date')
})

# Get from HK and SG only
df_fct_no_show = df_fct_no_show.filter(
    F.col('region').isin(['HK', 'SG'])
)

# write table, rpt_kpi_show
K.write_table(df_fct_no_show, table='rpt_kpi_show')

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
            t=df_join.package_type_id,
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
    'is_all_clubs',
    K.IS_CLUB_WHEN(
        a=F.col('360_silver_package.is_all_locations').cast('int'),
        s=F.col('360_silver_package.is_single_location').cast('int')
    )
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



#====== [MARKDOWN] CELL 40 ======

#====== [START] CELL 41 ======

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

#====== [END] CELL 41 ======




#====== [MARKDOWN] CELL 42 ======

#====== [START] CELL 43 ======

account_cols = [
    df_fct_account.date,
    df_fct_account.region,
    df_fct_account.dim_location_key,
]

# filter
df_leads_nmu = df_leads_user.filter(
    ~F.col('status').isin('Duplicate Lead', 'Existing Member', 'Duplicate', 'Invalid', 'Unreachable')
)

# join
df_leads_nmu = df_leads_nmu.join(
    df_fct_account, # df_account_location,
    df_leads_nmu.account_id == df_fct_account.id,
    'left'
)

# window drop duplicate with different campaign/channel
account_window = Window.partitionBy(
    *account_cols,
    'account_id',
    'group',
).orderBy(
    df_leads.convert_date.desc(),
    df_leads.marketing_campaign.asc_nulls_last(),
    df_leads.channel_code.asc_nulls_last(),
)

df_leads_nmu_window = df_leads_nmu.withColumn(
    '_row', F.row_number().over(account_window)
).filter(F.col('_row') == 1).drop('_row')

#====== [END] CELL 43 ======




#====== [MARKDOWN] CELL 44 ======

#====== [START] CELL 45 ======

# group by
df_leads_nmu_rpt = df_leads_nmu_window.groupBy(
    *account_cols,
    'group'
).agg(
    F.countDistinct('account_id').alias('count'),
)

# count layer after count distinct by group
df_leads_nmu_rpt = df_leads_nmu_rpt\
    .withColumn('layer', K.NMU_LAYER_WHEN())\
    .groupBy(
        'date',
        'region',
        'dim_location_key',
        'layer',
    ).agg(
        F.sum('count').alias('count'),
    )

df_leads_nmu_rpt = df_leads_nmu_rpt.withColumn('layer', K.NO_LAYER_RENAME('NMUCH'))

# HK 
df_leads_nmu_rpt = df_leads_nmu_rpt.filter(
    F.col('region').isin(['HK', 'SG'])
)

# K.write_table(df_leads_nmu_rpt, table='rpt_kpi_nmu_channel')

#====== [END] CELL 45 ======




#====== [MARKDOWN] CELL 46 ======

#====== [START] CELL 47 ======

# group by
df_leads_nmu_rpt = df_leads_nmu_window.groupBy(
    *account_cols,

    df_leads.marketing_campaign.alias('campaign'),
    df_leads.channel_code.alias('channel'),

    df_leads.lead_source,
    'group',
    
    df_user.user_name,
).agg(
    F.countDistinct('account_id').alias('count'),
)

# count layer after count distinct by group
df_leads_nmu_rpt = df_leads_nmu_rpt\
    .withColumn('layer', K.NMU_LAYER_WHEN())\
    .groupBy(
        'date',
        'region',
        'dim_location_key',
        'campaign',
        'channel',
        'layer',
        'lead_source',
        'user_name',
    ).agg(
        F.sum('count').alias('count'),
    )

df_leads_nmu_rpt = df_leads_nmu_rpt.withColumn('layer', K.NO_LAYER_RENAME('NMUCH'))

# HK 
df_leads_nmu_rpt = df_leads_nmu_rpt.filter(
    F.col('region').isin(['HK', 'SG'])
)

K.write_table(df_leads_nmu_rpt, table='rpt_kpi_nmu_channel_by')

#====== [END] CELL 47 ======




#====== [MARKDOWN] CELL 48 ======

#====== [START] CELL 49 ======

# group by
df_leads_nmu_rpt = df_leads_nmu_window.groupBy(
    *account_cols,

    df_leads.marketing_campaign.alias('campaign'),
    df_leads.channel_code.alias('channel'),

    df_leads.lead_source,
    'group',

    # df_leads.member_num,
    df_leads.ref_id,
        
    df_user.user_name,
).agg(
    F.countDistinct('account_id').alias('count'),
)

# count layer after count distinct by group
df_leads_nmu_rpt = df_leads_nmu_rpt\
    .withColumn('layer', K.NMU_LAYER_WHEN())\
    .groupBy(
        'date',
        'region',
        'dim_location_key',
        'campaign',
        'channel',
        'layer',
        'lead_source',
        # 'member_num',
        'ref_id',
        'user_name',
    ).agg(
        F.sum('count').alias('count'),
    )

df_leads_nmu_rpt = df_leads_nmu_rpt.withColumn('layer', K.NO_LAYER_RENAME('NMUCH'))

# HK 
df_leads_nmu_rpt = df_leads_nmu_rpt.filter(
    F.col('region').isin(['HK', 'SG'])
)

K.write_table(df_leads_nmu_rpt, table='rpt_kpi_nmu_channel_mem')

#====== [END] CELL 49 ======




#====== [MARKDOWN] CELL 50 ======

#====== [START] CELL 51 ======

# filter for booking
df_meeting_booking = df_meetings_location.filter(F.col('name').like('%1st time%'))

# groupby
df_meeting_booking = df_meeting_booking.groupBy(
    df_meetings.date,

    df_meetings_lead.meet_region.alias('region'),
    df_dim_location.dim_location_key,

    df_leads.marketing_campaign.alias('campaign'),
    df_leads.channel_code.alias('channel'),

    df_leads.lead_source,
    df_meetings_lead.layer,

    df_user.user_name,
).agg(
    F.count('*').alias('count'),
)

df_meeting_booking = df_meeting_booking.withColumn('layer', K.NO_LAYER_RENAME('BK'))

# HK 
df_meeting_booking = df_meeting_booking.filter(
    F.col('region').isin(['HK', 'SG'])
)

# K.write_table(df_meeting_booking, table='rpt_kpi_booking')

#====== [END] CELL 51 ======




#====== [MARKDOWN] CELL 52 ======

#====== [START] CELL 53 ======

# filter for booking
df_meeting_show = df_meetings_location.filter(
    (F.col('name').like('%1st time%')) & (df_meetings.status == 'Held')
)

# groupby
df_meeting_show = df_meeting_show.groupBy(
    df_meetings.date,

    df_meetings_lead.meet_region.alias('region'),
    df_dim_location.dim_location_key,

    df_leads.marketing_campaign.alias('campaign'),
    df_leads.channel_code.alias('channel'),

    df_leads.lead_source,
    df_meetings_lead.layer,

    df_user.user_name,
).agg(
    F.count('*').alias('count'),
)

df_meeting_show = df_meeting_show.withColumn('layer', K.NO_LAYER_RENAME('SW'))

# HK 
df_meeting_show = df_meeting_show.filter(
    F.col('region').isin(['HK', 'SG'])
)

# K.write_table(df_meeting_show, table='rpt_kpi_show')

#====== [END] CELL 53 ======




#====== [MARKDOWN] CELL 54 ======

#====== [START] CELL 55 ======

# create new layer
df_meeting_guest = df_meetings_location.withColumn('layer', K.GUEST_LAYER_WHEN())

# filter for booking
df_meeting_guest = df_meeting_guest.filter(
    (F.col('name').like('%1st time%')) & (df_meetings.status == 'Held')
)

# groupby
df_meeting_guest = df_meeting_guest.groupBy(
    df_meetings.date,

    df_meetings_lead.meet_region.alias('region'),
    df_dim_location.dim_location_key,

    df_leads.marketing_campaign.alias('campaign'),
    df_leads.channel_code.alias('channel'),

    df_leads.lead_source,
    df_meeting_guest.layer,

    df_user.user_name,
).agg(
    F.count('*').alias('count'),
)

# HK 
df_meeting_guest = df_meeting_guest.filter(
    F.col('region').isin(['HK', 'SG'])
)

# K.write_table(df_meeting_guest, table='rpt_kpi_guest')

#====== [END] CELL 55 ======




#====== [MARKDOWN] CELL 56 ======

#====== [START] CELL 57 ======

def cn_lead_groupby(
    id:int, rename:str=None, df=df_csh_clcci_budget,
    keys=[
        df_contact_status_history.source.alias('region'),
        F.to_date(df_contact_status_history.effective_date).alias('date'),
        df_location_full.dim_location_key,

        df_contact.lead_source_type_id.cast('string').alias('lead_source'),
        df_campaign.campaign_code.alias('campaign'),
        df_channel.channel_code.alias('channel'),

        'layer'
    ],
    values=[
        F.count('*').alias('count')
    ]
):
    df = df.filter(df_contact_status_history.contact_status_id == id)

    df = df.groupBy(keys).agg(
        *values
    )

    df = df.withColumn('user_name', F.lit(None).cast('string'))

    # rename layer
    if rename:
        df = df.withColumn('layer', K.NO_LAYER_RENAME(rename))

    return df

#====== [END] CELL 57 ======


#====== [START] CELL 58 ======

# leads
df_leads_cn = cn_lead_groupby(
    id=28, rename='LD',
    values=[
        F.count('*').alias('count'),
        F.sum('per_budget_amount').alias('budget'),
    ]
)

K.write_table(df_leads_cn, table='rpt_kpi_leads', mode='append')

# booking
df_book_cn = cn_lead_groupby(id=25, rename='BK')
K.write_table(df_book_cn, table='rpt_kpi_booking', mode='append')

# show
df_show_cn = cn_lead_groupby(id=33, rename='SW')
K.write_table(df_show_cn, table='rpt_kpi_show', mode='append')

# nmu channel
df_nmu_cn = cn_lead_groupby(id=1, rename='NMUCH')

K.write_table(
    df_nmu_cn.select('region', 'date', 'dim_location_key', 'layer', 'count'), # no campaign/channel
    table='rpt_kpi_nmu_channel', mode='append'
)
K.write_table(df_nmu_cn, table='rpt_kpi_nmu_channel_by', mode='append')

# guest
df_guest_cn = df_csh_clcci.withColumn(
    'layer', K.GUEST_LAYER_WHEN()
)

df_guest_cn = cn_lead_groupby(df=df_guest_cn, id=33)
K.write_table(df_guest_cn, table='rpt_kpi_guest', mode='append')

#====== [END] CELL 58 ======




#====== [MARKDOWN] CELL 59 ======

#====== [START] CELL 60 ======

# create layer
df_nmuc = K.create_nested(
    df_contact_ct,
    [
        K.NMUC_LAYER_WHEN(),
        K.NMUC_LAYER_PACKAGE_WHEN(),
        # NMUC_LAYER_OTHER_WHEN(),
    ]
)

df_nmuc = K.GENERAL_LAYER_FILTER(df_nmuc, 'KPI_NMUCAT_NMUCAT_NMUCAT')

# group
df_nmuc = df_nmuc.groupBy(
    'date', 'region', 'dim_location_key', 'layer'
).agg(
    F.sum('count').alias('count')
)

df_nmuc = df_nmuc.withColumn(
    'count',
    F.col('count').cast('decimal(38,18)')
)

K.write_table(df_nmuc, table='rpt_kpi_nmu')

#====== [END] CELL 60 ======




#====== [MARKDOWN] CELL 61 ======

#====== [START] CELL 62 ======

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
for _i, _m in enumerate(get_months()):

    # join silver_agreement_status_history
    df_ash_base = current_ash(current=_m).filter(
        (F.col('active') == 1) &
        (F.col('effective_date') < F.coalesce(F.col('ending_date'), F.lit('2099-12-31'))) &
        (F.col('current_month_last_day').between(
            F.col('effective_date'),
            F.coalesce(F.col('ending_date'), F.lit('2099-12-31'))
        ))
    )

    # partition by source_agreement_id, get agmt latest status
    _window = Window.partitionBy('source_agreement_id').orderBy(
        F.col('effective_date').desc(),
        F.col('request_date').desc(),
        F.col('updated_at').desc(),
    )
    df_ash_base = df_ash_base.withColumn(
        'status_history_rn', F.row_number().over(_window)
    ).filter(
        F.col('status_history_rn') == 1
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
        (F.col('effective_date').between(
            (F.date_add(F.add_months(F.last_day(F.col('current_month_last_day')), -1), 1)),
            (F.last_day(F.col('current_month_last_day')))
            )
        ) &
        df_agreement_status_history.effective_date.isNotNull()
    )

    df_c_layer = K.create_nested(
        df_c_window,
        [
            K.TERMINATION_ACTUAL_WHEN(),
            K.TERMINATION_ACTUAL_SHORT_WHEN(pstid=df_package.package_sub_type_id),
        ]
    )

    df_c_layer = K.GENERAL_LAYER_FILTER(df_c_layer, 'KPI_TLWD_TLWD_TLWD')

    # SG use agmt ED/LWD as termination effective date
    df_c_layer = df_c_layer.withColumn(
        'real_effective_date',
        F.when(
            F.col('region') == 'SG',
            F.greatest(
                F.coalesce(F.col('end_date'), F.lit('2099-12-31')),
                F.coalesce(F.col('last_workout_date'), F.lit('2000-01-01')),
            )
        ).otherwise(
            F.col('effective_date')
        )
    )

    # group by
    df_c_ct = df_c_layer.groupBy(
        'region',

        # F.to_date(df_agreement_status_history.effective_date).alias('date'),
        F.to_date(F.col('real_effective_date')).alias('date'),

        # df_package.package_term_id,
        # df_package.package_sub_type_id,
        # df_package_sub_type.package_sub_type_code,

        df_location_full.dim_location_key,
        'layer',
    ).agg(
        F.count('*').alias('count')
    )

    _mode = 'overwrite' if _i == 0 else 'append'
    K.write_table(df_c_ct, table='rpt_kpi_termination_workout', mode=_mode)

#====== [END] CELL 62 ======




#====== [MARKDOWN] CELL 63 ======

#====== [START] CELL 64 ======
df_renew_upgrade = spark.read.table(K.TABLE('fct_fpa_renew_upgrade'))

df_renew_upgrade = df_renew_upgrade.withColumn(
    'date', F.to_date('signed_date')
)
df_renew_upgrade = df_renew_upgrade.withColumn(
    'region', F.left('dim_location_key', F.lit(2))
)
df_renew_upgrade = df_renew_upgrade.withColumnRenamed(
    'renewal_upgrade','layer'
)
df_renew_upgrade = df_renew_upgrade.drop('signed_date')

# general filter + last group
df_agreement_ct = K.GENERAL_LAYER_FILTER(df_renew_upgrade, 'KPI_REU_REU_REU')

df_agreement_ct = df_agreement_ct.groupBy(
    'date',
    'dim_location_key',
    'region',
    'layer',
).agg(
    F.sum('count').alias('count')
)

K.write_table(df_agreement_ct, table='rpt_kpi_renew_upgrade')

#====== [END] CELL 64 ======




#====== [MARKDOWN] CELL 65 ======

#====== [START] CELL 66 ======

df_termination = spark.read.table(K.TABLE('termination'))
df_silver_contact = spark.read.table(K.TABLE('silver_contact'))
df_360f_location = spark.read.table(K.TABLE('dim_location_360f'))
df_termination = df_termination.withColumns({
    "month": F.date_format(F.col("date"), "yyyy-MM"),
    "region_rssid": K.concat_id(df_termination, cols=['region', 'rssid']),
})
df_silver_contact = df_silver_contact.withColumns({
    "source_member": K.concat_id(df_silver_contact, cols=['source', 'member_id']),
    "source_location": K.concat_id(df_silver_contact, cols=['source', 'home_location_id']),
})
df_silver_contact = df_silver_contact.withColumnRenamed("source", "sector")
df_silver_contact = df_silver_contact.dropDuplicates(["source_member"])

df_termination_location = (
    df_termination.join(
        df_silver_contact,
        df_termination.region_rssid == df_silver_contact.source_member,
        'left'
    )
    .join(
        df_360f_location,
        df_silver_contact.source_location == df_360f_location.dim_location_360f_key,
        'left'
    )
)
# display(df_termination_location)

for _i, _m in enumerate(get_months(1)):
    window = Window.partitionBy("rssid", "region", "month").orderBy(F.col("date").asc())
    df_ranked = df_termination_location.withColumn("row_num", F.row_number().over(window))
    # Split by row_num condition
    df_valid = df_ranked.filter(
        (F.col("row_num") == 1) & (F.col("member_id").isNotNull())
    )
    df_invalid = df_ranked.filter(
        ~((F.col("row_num") == 1) & (F.col("member_id").isNotNull()))
    )

    # Apply your custom layer filters
    df_valid = df_valid.withColumn('layer', F.lit('KPI_TRD_TRD_VALID'))
    df_invalid = df_invalid.withColumn('layer', F.lit('KPI_TRD_TRD_INVALID'))

    # Combine both results back (optional)
    df_tr_layer = df_valid.unionByName(df_invalid)

    # groupby
    df_tr_ct = df_tr_layer.groupBy(
        F.col('date'), 
        F.col('region'),
        F.col('dim_location_key'),
        'layer',
    ).agg(
        F.count('*').alias('count')
    )
    display(df_tr_ct)
    

    _mode = 'overwrite' if _i == 0 else 'append'
    K.write_table(df_tr_ct, table='rpt_kpi_termination_requests', mode=_mode)

#====== [END] CELL 66 ======


#====== [START] CELL 67 ======

# # loop for snapshot
# for _i, _m in enumerate(get_months(1)):

#     df_tr = current_cmalash(current=_m)

#     # filter
#     df_tr_filter = df_tr.filter(
#         (df_tr.adjusted_date_row == 1) &
#         (
#             df_agreement_status_history.agreement_status_id.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]) |
#             (
#                 df_agreement_status_history.agreement_status_id.isNull() &
#                 (F.ifnull(df_360_agreement.prev_gap_reference, F.lit('2099-12-31')) >= df_tr.current_month_last_day) & # <---
#                 (F.least(df_360_agreement.start_date, df_360_agreement.signed_date) < df_tr.current_month_last_day) # <---
#             )
#         )
#     )

#     #
#     df_tr_select = df_tr_filter.select(
#         df_contact.id, #contact_id
#         df_contact.source,

#         df_contact.source_id,

#         df_360_agreement.source_contact_id,
#         df_360_agreement.prev_gap_reference, #end_date
#         df_360_agreement.signed_date,

#         df_360_package.package_term_id,
#         df_360_package.package_sub_type_id,
#         df_360_package.min_committed_month,

#         df_agreement_status_history.agreement_status_id,
        
#         df_location_full.dim_location_key,
#     )

#     # self inner join contact
#     df_base_contact = df_contact.select('source_id', 'source_member_id')

#     df_tr_contact = df_tr_select.join(
#         df_base_contact,
#         df_base_contact.source_id == df_360_agreement.source_contact_id,
#         'inner'
#     )

#     # 
#     df_tr_termination = df_termination.join(
#         df_tr_contact,
#         df_contact.source_member_id == df_termination.termination_region_rssid,
#         'left'
#     )

#     df_tr_layer = df_tr_termination.withColumn(
#         'layer', K.TERMINATION_REQUEST_WHEN(),
#     )

#     df_tr_layer = K.GENERAL_LAYER_FILTER(df_tr_layer, 'KPI_TRD_TRD_TRD')

#     # groupby
#     df_tr_ct = df_tr_layer.groupBy(
#         F.col('termination_date').alias('date'), 
#         F.col('source').alias('region'),
#         df_location_full.dim_location_key,
#         'layer',
#         # 'package_sub_type_id'
#     ).agg(
#         F.count('*').alias('count')
#     )

#     # # create snapshot date -- only latest version
#     # df_tr_ct = df_tr_ct.withColumn('snapshot_date', _m)

#     _mode = 'overwrite' if _i == 0 else 'append'
#     K.write_table(df_tr_ct, table='rpt_kpi_termination_requests', mode=_mode)

#====== [END] CELL 67 ======




#====== [MARKDOWN] CELL 68 ======

#====== [START] CELL 69 ======

df_lead_account = spark.read.table(K.TABLE('silver_vw_lead_account_modal_2023'))

# fct_sf_accounts
df_lead_account = df_lead_account.withColumns({
    'region': K.SH_CN_WHEN_ALT(),
    'location': K.ACCOUNT_LOCATION_WHEN(),
    'date': F.to_date('success_date'),
}).withColumn(
    'region_account_id', K.concat_id(df_lead_account, cols=['final_region', 'account_id'])
)

# success account
## filter
df_lead_account_success = df_lead_account.filter(
    F.col('success_date').isNotNull() & 
    (F.col('guest_status') == 'Success')
)

## window
### get first account 
_window = Window.partitionBy('account_id').orderBy('success_date')
df_lead_account_success = df_lead_account_success.withColumn(
    '_row', F.row_number().over(_window)
)\
    .filter(F.col('_row')==1)\
    .select(
        'region_account_id', F.date_format('success_date', K.MONTH_DATE_FORMAT).alias('success_month')
    )

#====== [END] CELL 69 ======


#====== [START] CELL 70 ======

NMU_ENDING_BALANCE_GROUP = [
    'date',
    'region',
    'dim_location_key',
    'layer',
    'is_success_month',
    'is_current_date',
]

def nmu_eb_group_count(df, whens):
    df = df.withColumn(
        'layer', whens
    )

    df = df.groupBy(
        *NMU_ENDING_BALANCE_GROUP
    ).agg(
        F.count_distinct('contact_id').alias('count')
    )

    df = K.GENERAL_LAYER_FILTER(df, 'KPI_MEB_MEB_MEB')

    return df

for _i, _m in enumerate(get_months()):
    df_nmu_endbal = spark.read.table(K.TABLE('fct_fpa_nmu_ending_balance'))
    df_nmu_endbal = df_nmu_endbal.withColumn('source_ext_ref_contact_id', K.concat_id(df_nmu_endbal, cols=['source', 'ext_ref_contact_id']))
    df_nmu_endbal = df_nmu_endbal.withColumn('max_ending_date', F.ifnull('status_ending_date', F.lit('2099-12-31')))

    # Join df_lead_account_success to df_nmu_eb
    df_nmu_eb = df_nmu_endbal.join(
        df_lead_account_success.select('region_account_id', 'success_month'),
        df_nmu_endbal.source_ext_ref_contact_id == df_lead_account_success.region_account_id,
        'left'
    )


    # create is_success_month
    df_nmu_eb = df_nmu_eb.withColumn(
        'is_success_month', 
        df_lead_account_success.success_month.isNotNull() &
        (df_lead_account_success.success_month == F.date_format(_m, K.MONTH_DATE_FORMAT))
    )

    # append
    def _append(df, is_current:bool=False):
        # reset current date to set month
        df = set_current(df, _m)

        # filter
        filter_col = F.col('current_date') if is_current else F.col('current_month_last_day') 
        df = df.filter(
            df_nmu_eb.signed_date <= filter_col
        )
        

        # select
        df = df.select(
            df.contact_id,

            # for groupby
            df.package_term_id,
            df.package_sub_type_id,
            df.min_committed_month,

            df.dim_location_key,

            F.to_date('current_month').alias('date'),

            df.source.alias('region'),

            'is_success_month',
        )

        # create flag
        df = df.withColumn('is_current_date', F.lit(is_current))

        # report
        df_layer1 = nmu_eb_group_count(df, whens=K.ENDING_BALANCE_MEMBER_WHEN())
        df_layer2 = nmu_eb_group_count(df, whens=K.ENDING_BALANCE_MEMBER_SHORT_WHEN())
        df_layer3 = K.ENDING_BALANCE_MEMBER_OTHER(df, group=NMU_ENDING_BALANCE_GROUP)

        #
        _table = 'rpt_kpi_nmu_ending_balance'

        _layer1_mode = 'overwrite' if _i == 0 and not is_current else 'append'
        K.write_table(df_layer1, table=_table, mode=_layer1_mode, is_full_load=True)
        K.write_table(df_layer2, table=_table, mode='append')
        K.write_table(df_layer3, table=_table, mode='append')

    # normal
    _append(df_nmu_eb)

    # special for MoM (1) and YoY (12)
    if _i in [0, 1, 12]: # MoM
        _append(df_nmu_eb, is_current=True)

#====== [END] CELL 70 ======


#====== [START] CELL 71 ======

# NMU_ENDING_BALANCE_GROUP = [
#     'date',
#     'region',
#     'dim_location_key',
#     'layer',
#     # 'is_success_month',
#     'is_current_date',
# ]

# def nmu_eb_group_count(df, whens):
#     df = df.withColumn(
#         'layer', whens
#     )

#     df = df.groupBy(
#         *NMU_ENDING_BALANCE_GROUP
#     ).agg(
#         F.count_distinct('contact_id').alias('count')
#     )

#     df = K.GENERAL_LAYER_FILTER(df, 'KPI_MEB_MEB_MEB')

#     return df

# for _i, _m in enumerate(get_months(2)):
#     # always use current month
#     df_nmu_eb = current_cmalash(current=get_months(1)[0])

#     df_nmu_eb = df_nmu_eb.filter(
#         # (df_nmu_eb.adjusted_date_row == 1) &
#         (
#             ~df_agreement_status_history.agreement_status_id.isin([2, 3, 4, 7]) | 
#             df_agreement_status_history.remark.like('Suspend - Frozen%') |
#             (
#                 df_agreement_status_history.agreement_status_id.isNull() &
#                 (F.ifnull(df_360_agreement.prev_gap_reference, F.lit('2099-12-31')) >= df_nmu_eb.current_month_last_day) & # <---
#                 (F.least(df_360_agreement.start_date, df_360_agreement.signed_date) < df_nmu_eb.current_month_last_day) # <---
#             )
#         )
#     )

#     # create is_success_month
#     df_nmu_eb = df_nmu_eb.withColumn(
#         'is_success_month', 
#         df_account_success.success_month.isNotNull() &
#         # df_360_agreement.signed_month.isNotNull() &

#         #(df_account_success.success_month == df_360_agreement.signed_month)
#         (df_account_success.success_month == F.date_format(_m, K.MONTH_DATE_FORMAT))
#     )

#     # append
#     def _append(df, is_current:bool=False):
#         # reset current date to set month
#         df = set_current(df, _m)

#         # filter
#         filter_col = F.col('current_date') if is_current else F.col('current_month_last_day')

#         df = df.filter(
#             df_360_agreement.signed_date <= filter_col
#         )

#         # select
#         df = df.select(
#             df_contact.id.alias('contact_id'),

#             # for groupby
#             df_360_package.package_term_id,
#             df_360_package.package_sub_type_id,
#             df_360_package.min_committed_month,

#             df_location_full.dim_location_key,

#             # F.to_date(df_360_agreement.signed_date).alias('signed_date'),
#             F.to_date('current_month').alias('date'),

#             df_contact.source.alias('region'),

#             # 'is_success_month',

#             # new 
#             df_nmu_eb.adjusted_date_row,
#         ) 

#         # create flag
#         df = df.withColumn('is_current_date', F.lit(is_current))

#         # report
#         df_layer1 = nmu_eb_group_count(df, whens=K.ENDING_BALANCE_MEMBER_WHEN())
#         df_layer2 = nmu_eb_group_count(df, whens=K.ENDING_BALANCE_MEMBER_SHORT_WHEN())
#         df_layer3 = K.ENDING_BALANCE_MEMBER_OTHER(df, group=NMU_ENDING_BALANCE_GROUP)

#         # df_layer1 = df.withColumn('layer', K.ENDING_BALANCE_MEMBER_WHEN())
#         # df_layer2 = df.withColumn('layer', K.ENDING_BALANCE_MEMBER_SHORT_WHEN())

#         #
#         _table = 'test_kpi_nmu_ending_balance'

#         _layer1_mode = 'overwrite' if _i == 0 and not is_current else 'append'
#         K.write_table(df_layer1, table=f'{_table}_1', mode=_layer1_mode, is_full_load=True)
#         K.write_table(df_layer2, table=f'{_table}_2', mode=_layer1_mode, is_full_load=True)
#         K.write_table(df_layer3, table=f'{_table}_3', mode=_layer1_mode, is_full_load=True)

#     # normal
#     _append(df_nmu_eb)

#     # # special for MoM (1) and YoY (12)
#     # if _i in [0, 1, 12]: # MoM
#     #     _append(df_nmu_eb, is_current=True)

#====== [END] CELL 71 ======




#====== [MARKDOWN] CELL 72 ======

#====== [START] CELL 73 ======

# Direct Gold Table Call
df_checkin = spark.read.table(K.TABLE('fct_fpa_check_in'))
df_checkin = df_checkin.withColumn("date", F.to_date("check_in_date", "yyyy-MM-dd"))
df_checkin = df_checkin.withColumnRenamed("source", "region")

# layer
df_checkin_when = df_checkin.withColumn(
    'layer', K.CHECK_IN_WHEN()
)

df_checkin_layer = df_checkin_when.groupBy(
    'dim_location_key',
    'region',
    'date',
    'layer',
).agg(
    F.sum('n').alias('count')
)

K.write_table(df_checkin_layer, table='rpt_kpi_checkin')

#====== [END] CELL 73 ======


#====== [START] CELL 74 ======

# df_checkin = spark.read.table(K.TABLE('silver_check_in'))

# time_scale = 1000
# df_checkin = df_checkin.withColumns({
#     'dim_location_key': K.concat_id(df_checkin, cols=['source', 'location_id']),
#     'source_client_service_id': K.concat_id(df_checkin, cols=['source', 'client_service_id']),

#     'check_in_utc': F.from_unixtime(F.col('check_in_time') / time_scale),
# }).withColumn(
#     'check_in_datetime', F.from_utc_timestamp('check_in_utc', 'Asia/Hong_Kong')
# ).withColumn(
#     'check_in_date', F.to_date('check_in_datetime'),
# ).withColumns({
#     'check_in_year': F.year('check_in_date'),
#     'check_in_month': F.month('check_in_date'),
#     'check_in_day': F.day('check_in_date'),
# })

# df_checkin_3a = df_checkin.join(
#     df_360_agreement,
#     df_checkin.source_client_service_id == df_360_agreement.source_ext_ref_agreement_id,
#     'left'
# )

# # group by daily
# df_checkin_ct = df_checkin_3a.groupby(
#     'dim_location_key',
#     F.substring('dim_location_key', 0, 2).alias('region'), 

#     F.col('check_in_date').alias('date'),
#     df_360_agreement.package_type_id
# ).agg(
#     F.count_distinct('client_id').alias('count')
# )

# # # window
# # _order_window = Window.partitionBy('dim_location_key', 'check_in_year', 'check_in_month').orderBy('check_in_date')
# # df_checkin_sum = df_checkin_ct.withColumn(
# #     '_order', F.row_number().over(_order_window)
# # )

# # _window = Window.partitionBy('dim_location_key', 'check_in_year', 'check_in_month').orderBy('_order')
# # df_checkin_sum = df_checkin_sum.withColumn(
# #     'avg_mtd', F.sum('n').over(_window) / F.col('check_in_day')
# # )

# # layer
# df_checkin_when = df_checkin_ct.withColumn(
#     'layer', K.CHECK_IN_WHEN()
# )

# df_checkin_layer = df_checkin_when.groupBy(
#     'dim_location_key',
#     'region', 
#     'date',
#     'layer',
# ).agg(
#     F.sum('count').alias('count')
# )

# K.write_table(df_checkin_layer, table='rpt_kpi_checkin')

#====== [END] CELL 74 ======




#====== [MARKDOWN] CELL 75 ======

#====== [START] CELL 76 ======

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
        F.to_date(df_agreement_status_history.effective_date).alias('date'),
        df_location_full.dim_location_key,
        region_col.alias('region'), 
        F.lit('KPI_SUS_SUS_SUS').alias('layer'),
        # df_package.package_term_id,
        # df_package.package_sub_type_id,
        # df_package_sub_type.package_sub_type_code,
    ).agg(
        F.count('*').alias('count')
    )


    _mode = 'overwrite' if _i == 0 else 'append'
    K.write_table(df_suspension_ct, table='rpt_kpi_suspension', mode=_mode)


for _i, _m in enumerate(get_months(1)):
    current_suspension(current=_m)

#====== [END] CELL 76 ======




#====== [MARKDOWN] CELL 77 ======

#====== [START] CELL 78 ======

import datetime
import calendar

basesymbol_list = ['HKD', 'SGD', 'CNY',]
transactionsymbol_list = ['HKD', 'USD']

def date_list():
    start_date = datetime.date(1970, 1, 1)

    # last day of current month
    today = datetime.date.today()
    _, last_day = calendar.monthrange(today.year, today.month)
    end_date = datetime.date(today.year, today.month, last_day)
    
    # Generate list of dates
    date_list = []
    current = start_date
    while current <= end_date:
        date_list.append((current,))
        current += datetime.timedelta(days=1)
        
    return date_list

df_exchange_base = spark.createDataFrame(date_list(), schema=['date'])

df_exchange_base = df_exchange_base.withColumn(
    'basesymbol', F.explode(F.lit(basesymbol_list))
).withColumn(
    'transactionsymbol', F.explode(F.lit(transactionsymbol_list))
)

# filter the same combination
df_exchange_base = df_exchange_base.filter(
    F.col('basesymbol') != F.col('transactionsymbol')
)

#====== [END] CELL 78 ======


#====== [START] CELL 79 ======

df_exchange = spark.read.table(K.TABLE('silver_exchange_rate'))

# filter
df_exchange = df_exchange.filter(
    F.col('basesymbol').isin(basesymbol_list) & 
    F.col('transactionsymbol').isin(transactionsymbol_list)
)

# date
df_exchange = df_exchange.withColumn(
    'date', F.to_date('effectivedate')
)

# join
df_exchange_join = df_exchange_base.join(
    df_exchange,
    ['date', 'basesymbol', 'transactionsymbol'],
    'left'
)

# window
_window = Window\
    .partitionBy('basesymbol', 'transactionsymbol')\
    .orderBy('date')\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_exchange_lag = df_exchange_join.withColumn(
    'rate', F.last('exchangerate', ignorenulls=True).over(_window)
)

df_exchange_lag = df_exchange_lag.filter(
    F.col('rate').isNotNull()
)

df_exchange_select = df_exchange_lag.select(
    'date', 'basesymbol', 'transactionsymbol', 'rate', 
    F.substring('basesymbol', 0, 2).alias('region')
)

df_exchange_select.write.mode('overwrite').saveAsTable(K.REPORT_TABLE('rpt_exchange_rate'))

#====== [END] CELL 79 ======




#====== [MARKDOWN] CELL 80 ======

#====== [START] CELL 81 ======

df_campaign.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(K.REPORT_TABLE('rpt_campaign'))
df_channel.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(K.REPORT_TABLE('rpt_channel'))

#====== [END] CELL 81 ======




#====== [MARKDOWN] CELL 82 ======

#====== [START] CELL 83 ======

spark.read.table(K.TABLE('dim_location')).withColumn(
    'is_hk', K.IS_HK_WHEN(
        c=F.col('location_code'), r=F.substring('dim_location_key', 0, 2)
    )
).write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(K.REPORT_TABLE('rpt_dim_location'))

#====== [END] CELL 83 ======




#====== [MARKDOWN] CELL 84 ======

#====== [START] CELL 85 ======

spark.read.table(K.TABLE('silver_lead_source_type'))\
    .write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(K.REPORT_TABLE('rpt_lead_source_type'))

#====== [END] CELL 85 ======


#====== [START] CELL 86 ======

spark.read.table(K.TABLE('silver_users')).select(
    # F.col('id').alias('user_id'),
    'user_name',
).dropDuplicates().write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(K.REPORT_TABLE('rpt_users'))

#====== [END] CELL 86 ======


