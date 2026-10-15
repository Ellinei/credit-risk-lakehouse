-- Grain: one row per POS or cash loan per month.
{{ config(unique_key=['sk_id_prev', 'months_balance']) }}

with pos as (
    select * from {{ ref('stg_pos_cash_balance') }}
    {{ loaded_since_last_run() }}
),

current_client as (
    select client_sk, sk_id_curr from {{ ref('dim_client') }} where is_current
)

select
    p.sk_id_prev,
    p.months_balance,
    p.sk_id_curr,
    coalesce(c.client_sk, {{ unknown_key() }}) as client_sk,
    p.name_contract_status,
    p.cnt_instalment,
    p.cnt_instalment_future,
    p.sk_dpd,
    p.sk_dpd_def,
    {{ dpd_bucket('p.sk_dpd') }} as dpd_bucket,
    p.ingest_date,
    p._loaded_at
from pos as p
left join current_client as c on p.sk_id_curr = c.sk_id_curr
