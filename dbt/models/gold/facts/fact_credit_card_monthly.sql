-- Grain: one row per credit card account per month.
{{ config(unique_key=['sk_id_prev', 'months_balance']) }}

with cards as (
    select * from {{ ref('stg_credit_card_balance') }}
    {{ loaded_since_last_run() }}
),

current_client as (
    select client_sk, sk_id_curr from {{ ref('dim_client') }} where is_current
)

select
    cc.sk_id_prev,
    cc.months_balance,
    cc.sk_id_curr,
    coalesce(c.client_sk, {{ unknown_key() }}) as client_sk,
    cc.name_contract_status,
    cc.amt_balance,
    cc.amt_credit_limit_actual,
    cc.amt_balance / nullif(cc.amt_credit_limit_actual, 0) as utilization,
    cc.amt_drawings_current,
    cc.amt_payment_current,
    cc.amt_total_receivable,
    cc.cnt_drawings_current,
    cc.sk_dpd,
    cc.sk_dpd_def,
    {{ dpd_bucket('cc.sk_dpd') }} as dpd_bucket,
    cc.ingest_date,
    cc._loaded_at
from cards as cc
left join current_client as c on cc.sk_id_curr = c.sk_id_curr
