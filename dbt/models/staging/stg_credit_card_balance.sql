-- Monthly snapshots of credit card accounts.
select
    sk_id_prev,
    sk_id_curr,
    months_balance,
    name_contract_status,
    amt_balance,
    amt_credit_limit_actual,
    amt_drawings_current,
    amt_payment_current,
    amt_total_receivable,
    cnt_drawings_current,
    sk_dpd,
    sk_dpd_def,
    record_hash,
    ingest_date,
    _loaded_at
from {{ source('silver', 'credit_card_balance') }}
{{ latest_per_key(['sk_id_prev', 'months_balance']) }}
