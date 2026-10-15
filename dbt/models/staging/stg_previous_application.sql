-- One row per earlier Home Credit application by the same client.
select
    sk_id_prev,
    sk_id_curr,
    name_contract_type,
    name_contract_status,
    amt_annuity,
    amt_application,
    amt_credit,
    amt_goods_price,
    cnt_payment,
    days_decision,
    -- 365243 was nulled by Glue; the flags keep the "not applicable" information.
    days_first_drawing,
    days_first_drawing_is_sentinel,
    days_first_due,
    days_first_due_is_sentinel,
    days_last_due_1st_version,
    days_last_due_1st_version_is_sentinel,
    days_last_due,
    days_last_due_is_sentinel,
    days_termination,
    days_termination_is_sentinel,
    record_hash,
    ingest_date,
    _loaded_at
from {{ source('silver', 'previous_application') }}
{{ latest_per_key(['sk_id_prev']) }}
