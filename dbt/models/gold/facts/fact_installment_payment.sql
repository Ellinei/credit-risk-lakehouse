-- Grain: one row per installment payment event (record_hash; installments have no natural key).
{{ config(unique_key='record_hash') }}

with payments as (
    select * from {{ ref('stg_installments_payments') }}
    {{ loaded_since_last_run() }}
),

current_client as (
    select client_sk, sk_id_curr from {{ ref('dim_client') }} where is_current
)

select
    p.record_hash,
    p.sk_id_prev,
    p.sk_id_curr,
    coalesce(c.client_sk, {{ unknown_key() }}) as client_sk,
    p.num_instalment_version,
    p.num_instalment_number,
    p.days_instalment,
    p.days_entry_payment,
    p.amt_instalment,
    p.amt_payment,
    p.amt_instalment - coalesce(p.amt_payment, 0) as amt_shortfall,
    p.days_past_due,
    {{ dpd_bucket('p.days_past_due') }} as dpd_bucket,
    p.is_unpaid,
    p.ingest_date,
    p._loaded_at
from payments as p
left join current_client as c on p.sk_id_curr = c.sk_id_curr
