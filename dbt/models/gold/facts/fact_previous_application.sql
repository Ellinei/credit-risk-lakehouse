-- Grain: one row per previous Home Credit application (sk_id_prev).
{{ config(unique_key='sk_id_prev') }}

with previous as (
    select * from {{ ref('stg_previous_application') }}
    {{ loaded_since_last_run() }}
),

current_client as (
    select client_sk, sk_id_curr from {{ ref('dim_client') }} where is_current
)

select
    p.sk_id_prev,
    p.sk_id_curr,
    coalesce(c.client_sk, {{ unknown_key() }}) as client_sk,
    {{ dim_key('p.name_contract_type') }} as contract_type_sk,
    {{ dim_key('p.name_contract_status') }} as application_status_sk,
    p.amt_application,
    p.amt_credit,
    p.amt_annuity,
    p.amt_goods_price,
    p.cnt_payment,
    p.days_decision,
    p.days_first_due,
    p.days_last_due,
    p.days_termination,
    p.days_termination_is_sentinel,
    p.ingest_date,
    p._loaded_at
from previous as p
left join current_client as c on p.sk_id_curr = c.sk_id_curr
