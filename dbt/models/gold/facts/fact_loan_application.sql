{#
    Grain: one row per current loan application (sk_id_curr).

    Incremental: each run only reads staging rows loaded after the newest _loaded_at
    already in this table, then MERGEs on sk_id_curr. A re-sent application updates its
    row instead of adding a second one.
#}
{{ config(unique_key='sk_id_curr') }}

with applications as (
    select * from {{ ref('stg_application') }}
    {{ loaded_since_last_run() }}
),

current_client as (
    select client_sk, sk_id_curr from {{ ref('dim_client') }} where is_current
)

select
    a.sk_id_curr,
    coalesce(c.client_sk, {{ unknown_key() }}) as client_sk,
    {{ dim_key('a.name_contract_type') }} as contract_type_sk,
    a.target,
    a.is_train,
    a.amt_credit,
    a.amt_annuity,
    a.amt_goods_price,
    a.amt_income_total,
    a.amt_credit / nullif(a.amt_income_total, 0) as credit_to_income_ratio,
    a.amt_annuity / nullif(a.amt_income_total, 0) as annuity_to_income_ratio,
    a.days_employed,
    a.days_employed_is_sentinel,
    a.ext_source_2,
    a.ingest_date,
    a._loaded_at
from applications as a
left join current_client as c on a.sk_id_curr = c.sk_id_curr
