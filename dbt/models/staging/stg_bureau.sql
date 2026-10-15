-- One row per credit reported by the Credit Bureau (other lenders).
select
    sk_id_bureau,
    sk_id_curr,
    credit_active,
    credit_currency,
    credit_type,
    days_credit,
    credit_day_overdue,
    days_credit_enddate,
    days_credit_update,
    amt_credit_sum,
    amt_credit_sum_debt,
    amt_credit_sum_overdue,
    amt_annuity,
    record_hash,
    ingest_date,
    _loaded_at
from {{ source('silver', 'bureau') }}
{{ latest_per_key(['sk_id_bureau']) }}
