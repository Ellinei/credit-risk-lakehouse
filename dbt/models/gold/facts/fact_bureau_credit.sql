-- Grain: one row per Credit Bureau credit (sk_id_bureau).
{{ config(unique_key='sk_id_bureau') }}

with bureau as (
    select * from {{ ref('stg_bureau') }}
    {{ loaded_since_last_run() }}
),

current_client as (
    select client_sk, sk_id_curr from {{ ref('dim_client') }} where is_current
)

select
    b.sk_id_bureau,
    b.sk_id_curr,
    coalesce(c.client_sk, {{ unknown_key() }}) as client_sk,
    {{ dim_key('b.credit_type') }} as credit_type_sk,
    b.credit_active,
    b.credit_active = 'Active' as is_active,
    b.credit_currency,
    b.days_credit,
    b.credit_day_overdue,
    {{ dpd_bucket('b.credit_day_overdue') }} as dpd_bucket,
    b.days_credit_enddate,
    b.amt_credit_sum,
    b.amt_credit_sum_debt,
    b.amt_credit_sum_overdue,
    b.amt_annuity,
    b.ingest_date,
    b._loaded_at
from bureau as b
left join current_client as c on b.sk_id_curr = c.sk_id_curr
