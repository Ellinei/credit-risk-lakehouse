{#
    One row per client application. application_train (has TARGET) and application_test
    (no TARGET) share every other column, so they are stacked with an is_train flag.
    Silver keeps all ~120 columns; staging picks the ones the gold layer uses.
#}
with train as (
    select *, cast(target as int) as target_value, true as is_train
    from {{ source('silver', 'application_train') }}
),

test as (
    select *, cast(null as int) as target_value, false as is_train
    from {{ source('silver', 'application_test') }}
),

{% set columns = [
    'sk_id_curr', 'name_contract_type', 'code_gender', 'flag_own_car', 'flag_own_realty',
    'cnt_children', 'amt_income_total', 'amt_credit', 'amt_annuity', 'amt_goods_price',
    'name_income_type', 'name_education_type', 'name_family_status', 'name_housing_type',
    'occupation_type', 'organization_type', 'cnt_fam_members', 'days_birth',
    'days_employed', 'days_employed_is_sentinel', 'ext_source_2',
    'record_hash', 'ingest_date', '_loaded_at',
] %}

stacked as (
    select {{ columns | join(', ') }}, target_value as target, is_train from train
    union all
    select {{ columns | join(', ') }}, target_value as target, is_train from test
)

select
    sk_id_curr,
    target,
    is_train,
    name_contract_type,
    code_gender,
    flag_own_car = 'Y' as owns_car,
    flag_own_realty = 'Y' as owns_realty,
    cnt_children,
    amt_income_total,
    amt_credit,
    amt_annuity,
    amt_goods_price,
    name_income_type,
    name_education_type,
    name_family_status,
    name_housing_type,
    occupation_type,
    organization_type,
    cnt_fam_members,
    days_birth,
    {{ age_years('days_birth') }} as age_years,
    days_employed,
    days_employed_is_sentinel,
    ext_source_2,
    record_hash,
    ingest_date,
    _loaded_at
from stacked
{{ latest_per_key(['sk_id_curr']) }}
