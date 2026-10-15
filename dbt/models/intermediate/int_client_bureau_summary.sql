-- Per client: what other lenders report (Credit Bureau credits and their monthly status).
with credits as (
    select
        sk_id_curr,
        count(*) as bureau_credit_count,
        count_if(is_active) as bureau_active_credit_count,
        sum(amt_credit_sum) as bureau_credit_sum,
        sum(amt_credit_sum_debt) as bureau_debt_sum,
        sum(amt_credit_sum_overdue) as bureau_overdue_sum,
        max(credit_day_overdue) as bureau_max_days_overdue
    from {{ ref('fact_bureau_credit') }}
    where sk_id_curr is not null
    group by sk_id_curr
),

monthly as (
    select
        sk_id_curr,
        max({{ dpd_bucket_rank('dpd_bucket') }}) as bureau_worst_bucket_rank
    from {{ ref('fact_bureau_balance_monthly') }}
    where sk_id_curr is not null
    group by sk_id_curr
)

select
    credits.*,
    monthly.bureau_worst_bucket_rank
from credits
left join monthly on credits.sk_id_curr = monthly.sk_id_curr
