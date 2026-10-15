-- One row per bureau credit per month (months_balance is relative: 0 is the latest month).
select
    sk_id_bureau,
    months_balance,
    status,
    record_hash,
    ingest_date,
    _loaded_at
from {{ source('silver', 'bureau_balance') }}
{{ latest_per_key(['sk_id_bureau', 'months_balance']) }}
