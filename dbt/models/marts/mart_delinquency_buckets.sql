{#
    How overdue are repayments? Counts per DPD bucket for two sources:
    installment payments (days paid after the due day) and POS or cash loan months (SK_DPD).
    Unpaid installments have no days past due and get their own 'Unpaid' row.
#}
with installments as (
    select
        'installments' as source,
        coalesce(dpd_bucket, 'Unpaid') as dpd_bucket,
        sk_id_curr,
        amt_instalment as amount
    from {{ ref('fact_installment_payment') }}
),

pos as (
    select
        'pos_cash' as source,
        dpd_bucket,
        sk_id_curr,
        cast(null as double) as amount
    from {{ ref('fact_pos_cash_monthly') }}
    where dpd_bucket is not null
),

combined as (
    select * from installments
    union all
    select * from pos
)

select
    source,
    dpd_bucket,
    coalesce({{ dpd_bucket_rank('dpd_bucket') }}, 99) as bucket_order,
    count(*) as record_count,
    count(distinct sk_id_curr) as client_count,
    sum(amount) as amount_sum,
    count(*) / sum(count(*)) over (partition by source) as share_of_records
from combined
group by source, dpd_bucket
