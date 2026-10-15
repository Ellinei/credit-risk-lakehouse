{#
    One row per installment payment event. A single installment can have several partial
    payments, so there is no natural key; record_hash (all business columns) is the key.
#}
select
    record_hash,
    sk_id_prev,
    sk_id_curr,
    num_instalment_version,
    num_instalment_number,
    days_instalment,
    days_entry_payment,
    amt_instalment,
    amt_payment,
    -- Days paid after the due day, 0 when on time. Null when no payment was recorded
    -- (Spark's greatest() skips nulls, so the null case has to be explicit).
    case
        when days_entry_payment is null then null
        else greatest(days_entry_payment - days_instalment, 0)
    end as days_past_due,
    days_entry_payment is null as is_unpaid,
    ingest_date,
    _loaded_at
from {{ source('silver', 'installments_payments') }}
{{ latest_per_key(['record_hash']) }}
