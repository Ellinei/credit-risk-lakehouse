-- Monthly snapshots of point-of-sale and cash loans.
select
    sk_id_prev,
    sk_id_curr,
    months_balance,
    name_contract_status,
    cnt_instalment,
    cnt_instalment_future,
    sk_dpd,
    sk_dpd_def,
    record_hash,
    ingest_date,
    _loaded_at
from {{ source('silver', 'pos_cash_balance') }}
{{ latest_per_key(['sk_id_prev', 'months_balance']) }}
