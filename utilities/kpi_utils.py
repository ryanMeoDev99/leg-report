from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import itertools as I

### config
CATALOG = 'prod_catalog'
SCHEMA = 'lakehouse'

REPORT_CATALOG = 'leg_uat'
REPORT_SCHEMA = 'lakehouse'

GENERAL_DATE_FORMAT = 'yyyy-MM-dd'
MONTH_DATE_FORMAT = 'yyyy-MM'

TABLE = lambda x: f'{CATALOG}.{SCHEMA}.{x}'
REPORT_TABLE = lambda x: f'{REPORT_CATALOG}.{REPORT_SCHEMA}.{x}'

### function
def write_table(df, table:str, mode:str='overwrite', is_full_load:bool=True):
    table_path = REPORT_TABLE(table)

    if is_full_load and (mode == 'overwrite'):
        SparkSession.getActiveSession().sql(
            f'DROP TABLE IF EXISTS {table_path}'
        )

    # filter out None
    df = df.filter(F.col('layer').isNotNull())

    # create year & month
    df = df.withColumns({
        'month': F.date_format('date', 'yyyyMM').cast('int'),
        'year': F.date_format('date', 'yyyy').cast('int'),
        'dim_location_key': F.coalesce(
            F.col('dim_location_key'), F.concat('region', F.lit('9999'))
        )
    })

    df.write.mode(mode).saveAsTable(table_path)


def prefix_diff(left:str, right:str, delimiter:str='_'):
    lefts = left.split(delimiter)

    for _ in range(len(lefts), 0, -1):
        _check = delimiter.join(lefts[-_:])
        _prefix = delimiter.join(lefts[:-_])

        if right.startswith(_check):
            left = _prefix
            break
        
    return delimiter.join([left, right]) if left else right

def prefix(df, text:str, delimiter='_'):
    for _ in df.columns:
        _new = prefix_diff(text, _)
        df = df.withColumnRenamed(_, _new)

    return df

def concat_id(df, cols):
    _map = dict(df.dtypes)

    _cl = []

    for _ in cols:
        _c = F.col(_).cast('bigint') if _map[_].startswith('decimal') else F.col(_)
        _cl.append(_c) 

    return F.concat(*_cl)

def concat_when(whens:dict):
    _whens = None
    for k, cond in whens.items():
        _whens = F.when(cond, k) if _whens is None else _whens.when(cond, k)

    return _whens

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

def create_nested(df, whens:list, layer_col:str='layer'):
    # nested layer
    tmp_cols = [f'_tmp_{layer_col}_{_}' for _, _w in enumerate(whens)]

    for _i, _when in enumerate(whens):
        # create when(s)
        df = df.withColumn(tmp_cols[_i], _when)

    # concat all when array + filter none
    df = df.withColumn(
        layer_col, 
        F.array_compact(
            F.array(tmp_cols)
        )
    )

    # explode
    df = df.withColumn(
        layer_col, F.explode(layer_col)
    )

    # drop tmp cols
    df = df.drop(*tmp_cols)
    return df

### filter
GENERAL_LAYER_FILTER = lambda df, text, c='layer': \
    df.filter(F.col(c).isNotNull())\
        .withColumn(c, F.lit(text))

### when
# location
base_location_maps = {
    # general
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

    # leads
    # 'HH': 'HUT',

    # account & meeting
    # 'apm': 'APM',

    # meeting
    # 'STS': 'PCAST',
}

# leads location
lead_location_maps = {
    **base_location_maps, 
    'HH': 'HUT',
}

LEAD_LOCATION_WHEN = lambda c=F.col('location'): value_when(c, maps=lead_location_maps, otherwise=True)

# account location
account_location_maps = {
    **base_location_maps, 
    'apm': 'APM',
}

ACCOUNT_LOCATION_WHEN = lambda c=F.col('signed_location'): value_when(c, maps=account_location_maps, otherwise=True)

# ishk
is_hk_maps = {
    1: ['ASTHK', 'MOKO', 'WTC', 'STS', 'SOU', 'PULSE', 'PUL', 'PP', 'PEN', 'PCCW', 'OTP', 'OHR', 'MP', 'APM', 'MC5', 'LTP', 'LPY', 'LPF', 'LKF', 'LIN', 'KIN', 'IFC', 'ICBC', 'K11', 'HEAD']
}

IS_HK_WHEN = lambda c=F.col('location'), r=F.col('region'): \
    value_when(c, maps=is_hk_maps, method='isin', condition=r == 'HK').otherwise(0)

REGION_IS_HK_WHEN = lambda r=F.col('region_2'), f=F.col('is_hk'):\
    F.when(
        (r == 'HK') & (f == 1),
        'HK'
    ).when(
        (r == 'SG') & (f == 0),
        'SG'
    ).when(
        (r == 'CN'),
        'CN'
    )


# SH_CN_WHEN
sh_cn_maps = {
    'CN': 'SH'
}
SH_CN_WHEN_ALT = lambda c=F.col('final_region'): value_when(c, maps=sh_cn_maps, otherwise=True)
SH_CN_WHEN = lambda c=F.col('region'): value_when(c, maps=sh_cn_maps, otherwise=True)

# lead_source_group
source_group_maps = {
    'Marketing Campaign': ['57', 'XW'],
    'TI': ['50'],
    'WI': ['53', '55', '59'],
    'Corporate': ['58'],
    'BR': ['60', '51', '54', 'BR-360app', 'BR-WeChat'],
    'Cold case': ['52'],
    'Others-Info': ['56']
}

SOURCE_GROUP_WHEN = lambda c=F.col('lead_source'): value_when(c, maps=source_group_maps, method='isin').otherwise('')

# lead_source_group ver.CN
cn_source_group_maps = {
    'Marketing Campaign': [73, 914],
    'TI': [69, 915],
    'WI': [1, 67, 68, 907],
    'Corporate': [59, 60, 911, 932, 933],
    'BR': [34, 35, 36, 37, 38, 39, 908, 909],
    'Others-Info': [34, 35, 36, 37, 38, 39, 908, 909, 69, 915, 59, 60, 911, 932, 933, 73, 914, 1, 67, 68, 907],
}

CN_SOURCE_GROUP_WHEN = lambda c=F.col('lead_source_type_id'): value_when(c, maps=cn_source_group_maps, method='isin').otherwise('')

# market_lead
MARKET_LEAD_WHEN = lambda s=F.col('lead_source'), c=F.col('channel_code'): \
    F.when(
        s.isin(['57', 'XW']) & 
        c.isin(['AI', ' CN', 'EP', 'FY', 'IZ', 'JF', 'KQ', 'NC', 'NH', 'NR', 'PZ', 'QJ']),
        'Webform'
    ).when(
        s.isin(['57', 'XW']) & 
        c.isin(['JI', 'KH', 'NI']),
        'SEM'
    ).otherwise('Others')

# cn market_lead
CN_MARKET_LEAD_WHEN = lambda lstid=F.col('lead_source_type_id'), ccid=F.col('channel_code_id'): \
    F.when(
        lstid.isin([73, 914]) & 
        ccid.isin([25, 26, 33]),
        'Webform'
    ).when(
        lstid.isin([73, 914]) & 
        ccid.isin([23, 24, 34]),
        'SEM'
    ).when(
        lstid.isin([73, 914]) & 
        ccid.isin([27, 50, 51]),
        'Redbook'
    ).when(
        lstid.isin([73, 914]) & 
        ccid.isin([31, 32, 36, 29, 35, None]),
        'Others'
    )

# lead layer
NO_LAYER_RENAME = lambda text, c=F.col('layer'): F.regexp_replace(c, 'XX', text)

no_layer_maps = {
    'KPI_XX_XX_BR': 'BR',
    'KPI_XX_XX_TI': 'TI',
    'KPI_XX_XX_CORP': 'Corporate',
    #
    'KPI_XX_XX_MLWEB': 'ML_Webform',
    'KPI_XX_XX_MLSEM': 'ML_SEM',
    'KPI_XX_XX_MLRB': 'ML_Redbook',
    'KPI_XX_XX_MLO': 'ML_Others',
}

NO_LAYER_WHEN = lambda c: value_when(c, maps=no_layer_maps, method='like')

LEAD_LAYER_WHEN = lambda c=F.col('group'): NO_LAYER_WHEN(c)\
        .when(
            c.isin('Others-Info', 'Cold case', ''), 'KPI_XX_XX_O'
        ) 

NMU_LAYER_WHEN = lambda c=F.col('group'): NO_LAYER_WHEN(c)\
        .when(
            c == 'WI', 'KPI_XX_XX_WI'
        ).otherwise('KPI_XX_XX_O')


# ml group when
ML_GROUP_WHEN = lambda g=F.col('lead_source_group'), m=F.col('market_lead'): \
    F.when(
        g == 'Marketing Campaign', 
        F.concat(F.lit('ML_'), m)
    ).otherwise(g)

# location
meet_location_maps = {
    'HKK11': 'K11',
}

MEET_LOCATION_WHEN = lambda c=F.col('location'): value_when(c, maps=meet_location_maps, otherwise=True)

# region
meet_region_maps = {
    'SG': 'SG',
}

MEET_REGION_WHEN = lambda c=F.col('region_2'): value_when(c, maps=meet_region_maps).otherwise('HK')

# WI layer
guest_layer_maps = {
    'KPI_GT_GT_WI': 'WI',
}

GUEST_LAYER_WHEN = lambda c=F.col('group'): value_when(c, maps=guest_layer_maps).otherwise('KPI_GT_GT_SW')

# NUM_CAT_WEHN
NUM_CAT_WEHN = lambda p=F.col('package_term_id'), m=F.col('min_committed_month'), d=F.col('day_gap'):\
    F.when(
       (p == 2) & m.isin([18, 24]) & d.between(1, 90),
        'renew'
    )\
    .when(
        (p == 1) & d.between(1, 90),
        's_nmu'
    )\
    .when(
        (p == 2) & (m < 18) & d.between(1, 90),
        's_nmu'
    )\
    .otherwise(m.cast('string'))

# PACKAGE GP WHEN
PACKAGE_GP_WHEN = lambda p=F.col('package_term_id'), m=F.col('min_committed_month'), t=F.col('total_session'), s=F.col('service_category_id'), \
    u=F.col('unit_code_id'), a=F.col('amount'):\
    F.when(
       (p == 1) & (t >= 99999) & (s == 82),
        'unlimited wRP'
    )\
    .when(
        (p == 1) & (t >= 99999),
        'unlimited'
    )\
    .when(
        (p == 1) & (u == 3) & (a == 6),
        'limited 6 Class'
    )\
    .when(
        (p == 1) & (t <= 99999) & (s == 82),
        'limited wRP'
    )\
    .when(
        (p == 1) & (t <= 99999),
        'limited'
    )\
    .when(
        (p == 2) & (m == 3),
        'pre-3'
    )\
    .when(
        (p == 2) & (m == 6) & (s == 82),
        'pre-6-wRP'
    )\
    .when(
        (p == 2) & (m == 6),
        'pre-6'
    )\
    .when(
        (p == 2) & (m == 12) & (s == 82),
        'pre-12-wRP'
    )\
    .when(
        (p == 2) & (m == 12),
        'pre-12'
    )\
    .when(
        (p == 2) & (m == 18) & (s == 82),
        'pre-18-wRP'
    )\
    .when(
        (p == 2) & (m == 18),
        'pre-18'
    )\
    .when(
        (p == 2) & (m == 24) & (s == 82),
        'pre-24-wRP'
    )\
    .when(
        (p == 2) & (m == 24),
        'pre-24'
    )\
    .otherwise('')

# IS_CLUB_WHEN
IS_CLUB_WHEN = lambda a=F.col('is_all_locations'), s=F.col('is_single_location'):\
    F.when(
       a.cast("int") == 1, F.lit(1)
    )\
    .when(
       s.cast("int") == 1, F.lit(0)
    )\
    .otherwise(F.lit(1))

# NMUC
def NMUC_LAYER_WHEN(
    pgp=F.col('package_gp'), ptid=F.col('package_term_id'), pstc=F.col('package_sub_type_code'), nc=F.col('nmu_cat'), iac=F.col('is_all_clubs'), reg=F.col('region')
):
    
    region_when = (
        ((pstc == 'MEM') & (reg == 'HK')) | 
        reg.isin('CN', 'SG')
    )

    due_when = lambda n: (nc == n) & (ptid == 1) & region_when

    _pre = (ptid == 2) & region_when
    pre_when = lambda n: (nc == n) & _pre

    _map = {
        # due
        'due_3_months_unlimited': due_when('3') & (pgp == 'unlimited'),
        'due_3_months_unlimited_w': due_when('3') & (pgp == 'unlimited wRP'),
        'due_3_months_6_class': due_when('3') & (pgp == 'limited 6 Class'),
        'due_3_months_others': due_when('3') & ~pgp.isin('unlimited', 'unlimited wRP', 'limited 6 Class'),

        'due_6_months_unlimited': due_when('6') & (pgp == 'unlimited'),
        'due_6_months_unlimited_w': due_when('6') & (pgp == 'unlimited wRP'),
        'due_6_months_limited': due_when('6') & (pgp == 'limited'),
        'due_6_months_limited_w': due_when('6') & (pgp == 'limited wRP'),
        'due_6_months_6_class': due_when('6') & (pgp == 'limited 6 Class'),
        'due_6_months_others': due_when('6') & ~pgp.isin('unlimited', 'unlimited wRP', 'limited', 'limited wRP', 'limited 6 Class'),

        'due_12_months_unlimited_w': due_when('12') & (pgp == 'unlimited wRP'),
        'due_12_months_limited': due_when('12') & (pgp == 'limited'),
        'due_12_months_6_class': due_when('12') & (pgp == 'limited 6 Class'),
        'due_12_months_others': due_when('12') & ~pgp.isin('unlimited wRP', 'limited', 'limited 6 Class'),

        # 'service_nmu': nc == 's_nmu',
        'due_others': due_when('s_nmu'),

        # pre
        'prepaid_3_months': pre_when('3') & (pgp == 'pre-3'),

        **{
            f'prepaid-{m}-months{t}': pre_when(m) & (pgp == f'pre-{m}{t}') 
            for m, t in I.product(
                ['6', '12', '18', '24'], 
                ['', '-wRP']
            )
        },

        'prepaid_others': _pre & nc.isin(['renew', 's_nmu']) & pgp.isNotNull() & (pgp != ''),
    }

    return concat_when(_map)

# NMUC package
def NMUC_LAYER_PACKAGE_WHEN(
    pstc=F.col('package_sub_type_code'), nc=F.col('nmu_cat'), iac=F.col('is_all_clubs')
):
    
    package_when = lambda n, i=0: (iac == i) & (nc == n) & (pstc == 'ST')

    _map = {
        # package
        'package_1_months_all_clubs': package_when(n='1', i=1),

        **{
            f'package_{_}_months_1_clubs': package_when(_) for _ in ['2', '3', '4', '5']
        }
    }

    return concat_when(_map)

# NMUC other
def NMUC_LAYER_OTHER_WHEN(
    pstc=F.col('package_sub_type_code'), nc=F.col('nmu_cat')
):
    return F.when(
        (pstc == 'ST') & ~nc.isin('renew', 's_nmu'),
        'short_term_others'
    )

def RENEWAL_UPGRADE_WHEN(
    pn=F.upper('package_name'), fai=F.col('from_agreement_id'), 
    lptn=F.col('last_package_term_name'), ptn=F.col('package_term_name'), pstc=F.col('package_sub_type_code'),
    reg=F.col('region')
):
    
    region_when = (
        ((pstc == 'MEM') & reg.isin(['HK', 'SG'])) |
        reg.isin(['CN'])
    )

    _map = {
        'drop-in': pn.like('%DROP%'),
        'up_reform_pilates': pn.like('%REFORM%PILATE%') & fai.isNotNull(),
        'up_dp': (ptn == 'Prepaid') & (lptn == 'Dues') & fai.isNotNull(),
        'up_pp': (ptn == 'Prepaid') & (lptn == 'Prepaid') & fai.isNotNull(),
        'up_dd': (ptn == 'Dues') & (lptn == 'Dues') & fai.isNotNull(),

        'renew_pp': pn.like('%RENEW%') & (ptn == 'Prepaid'),
        'renew_pd': pn.like('%RENEW%') & (ptn == 'Dues'),
        'renew_reform_pilates': pn.like('%RENEW%') & pn.like('%REFORM%PILATE%'),
    }

    # add region_when by loop
    _region_map = {k: v & region_when for k, v in _map.items()}

    return concat_when(_region_map)

# Termination Workout
def TERMINATION_ACTUAL_WHEN(
    pstc=F.col('package_sub_type_code'), ptid=F.col('package_term_id'), reg=F.col('region')
):
    region_when = (
        ((pstc == 'MEM') & reg.isin(['HK', 'SG'])) |
        reg.isin(['CN'])
    )

    return F.when(
       region_when & (ptid == 1),
       'dues'
    ).when(
        region_when & (ptid == 2),
       'prepaid'
    )

def TERMINATION_ACTUAL_SHORT_WHEN(
    pstc=F.col('package_sub_type_code'), pstid=F.col('package_sub_type_id'),
    ptid=F.col('package_term_id'), reg=F.col('region')
):
    region_when = (
        ((pstc == 'ST') & reg.isin(['HK', 'SG'])) |
        ((pstid == 8) & reg.isin(['CN']))
    )

    return F.when(
       region_when, 'short term'
    )


# IS MEMBER WHEN
IS_MEMBER_WHEN = lambda s=F.col('source'), t=F.col('package_type_id'), st=F.col('package_sub_type_id'):\
    F.when(
       (s == 'HK') & (t == 1) & st.isin(8, 9),
       1
    )\
    .when(
        (s == 'SG') & (t == 1) & st.isin(1, 2),
        1
    )\
    .when(
        (s == 'CN') & (t == 1),
        1
    )\
    .otherwise(0)

IS_TERMINATED = lambda id=F.col('agreement_status_id'):\
    F.when(
        id.isin(2, 3, 4, 7), 
        1
    ).otherwise(0)

IS_EFFECTIVE_DATE = lambda id=F.col('agreement_status_id'), eff=F.col('effective_date'), end=F.col('end_date'):\
    F.when(
        id.isin(2, 3, 4, 7, 8, 9, 10),
        eff
    ).otherwise(
        F.ifnull(end, F.lit('2099-12-31'))
    )

TERMINATION_REQUEST_WHEN = lambda tid=F.col('package_term_id'), mcm=F.col('min_committed_month'), r=F.col('termination_region'), stid=F.col('package_sub_type_id'):\
    F.when(
        (tid == 1) & (mcm == 3),
        'dues_3'
    ).when(
        (tid == 1) & (mcm == 6),
        'dues_6'
    ).when(
        (tid == 1) & (mcm == 12),
        'dues_12'
    ).when(
        (tid == 2) & (mcm == 3),
        'prepaid_3'
    ).when(
        (tid == 2) & (mcm == 6),
        'prepaid_6'
    ).when(
        (tid == 2) & (mcm == 12),
        'prepaid_12'
    ).when(
        (tid == 2) & (mcm == 18),
        'prepaid_18'
    ).when(
        (tid == 2) & (mcm == 24),
        'prepaid_24'
    ).when(
        (r == 'HK') & (stid == 8),
        'shortterm'
    ).when(
        (r == 'SG') & (stid == 1),
        'shortterm'
    ).when(
        (r == 'CN') & (stid == 3),
        'shortterm'
    )

def ENDING_BALANCE_MEMBER_WHEN(
    stid=F.col('package_sub_type_id'), tid=F.col('package_term_id'), mcm=F.col('min_committed_month'), reg=F.col('region')
):
    region_when_2 = (
        (stid.isin(8,9) & (reg == 'HK')) | 
        (stid.isin(1,2) & (reg == 'SG')) | 
        (reg == 'CN')
    )
    region_when_1 = (
        (stid.isin(9) & (reg == 'HK')) | 
        (stid.isin(2) & (reg == 'SG')) | 
        (reg == 'CN')
    )

    autopay_when_1 = lambda _: region_when_1 & (tid == 1) & (mcm == _)
    autopay_when_2 = lambda _: region_when_2 & (tid == 1) & (mcm == _) 
    prepaid_when_1 = lambda _: region_when_1 & (tid == 2) & (mcm == _)
    prepaid_when_2 = lambda _: region_when_2 & (tid == 2) & (mcm == _) 

    _map = {
        # autopay
        **{
            f'{_}_month_autopay': autopay_when_1(_) for _ in [3]
        },

        **{
            f'{_}_month_autopay': autopay_when_2(_) for _ in [6, 12]
        },

        # prepaid
        **{
            f'{_}_month_prepaid': prepaid_when_1(_) for _ in [3]
        },

        **{
            f'{_}_month_prepaid': prepaid_when_2(_) for _ in [6, 12, 18, 24]
        }
    }

    return concat_when(_map)


def ENDING_BALANCE_MEMBER_SHORT_WHEN (
    stid=F.col('package_sub_type_id'), mcm=F.col('min_committed_month'), reg=F.col('region')
):
    region_when = (
        (stid.isin([8]) & (reg == 'HK')) | 
        (stid.isin([1]) & reg.isin(['SG','CN']))
    )

    return F.when(
        region_when & mcm.between(1, 5),
        'short_term'
    )

def ENDING_BALANCE_MEMBER_OTHER(
    df, group, 
    tid=F.col('package_term_id'), stid=F.col('package_sub_type_id'), 
    mcm=F.col('min_committed_month'), cid=F.col('contact_id'), reg=F.col('region')
):
    region_cond1_when = (
        (stid.isin([8]) & (reg == 'HK')) | 
        (stid.isin([1]) & reg.isin(['SG', 'CN']))
    )

    region_cond2_when = (
        (~(stid.isin([9])) & (reg == 'HK')) | 
        (~(stid.isin([2])) & (reg == 'SG')) | 
        (reg == 'CN') 
    )

    region_cond3_when = (
        (~(stid.isin([8,9])) & (reg == 'HK')) | 
        (~(stid.isin([1,2])) & (reg == 'SG')) | 
        (reg == 'CN') 
    )

    region_combine_when = (
        (region_cond1_when & ~mcm.between(1, 5))
        | (tid.isin([1,2]) & region_cond2_when & ~mcm.isin([3])) 
        | (tid.isin([1]) & region_cond3_when & ~mcm.isin([6, 12])) 
        | (tid.isin([2]) & region_cond3_when & ~mcm.isin([6, 12, 18, 24]))
    )

    df = df.withColumns({
        '_cond1': F.when(region_combine_when, cid),
        'layer': F.lit('KPI_MEB_MEB_MEB'),
    })

    # 
    df = df.groupBy(
        *group
    ).agg(
        F.count_distinct('_cond1').alias('_cond1'),
    )

    df = df.withColumn(
        'count', 
        F.col('_cond1')
    ).drop(
        '_cond1',
    )

    return df
    

## check in
CHECK_IN_WHEN = lambda id=F.col('contact_type_id'):\
    F.when(
        id == 3, 'KPI_CIN_MEM_MEM'
    ).when(
        id == 2, 'KPI_CIN_GT_GT'
    ).otherwise(
        'KPI_CIN_LD_LD'
    )

## suspension
SUSPENSION_FILTER = lambda pstc=F.col('package_sub_type_code'), reg=F.col('region'): \
    (
        ((pstc == 'MEM') & reg.isin(['HK', 'SG'])) |
        reg.isin(['CN'])
    )