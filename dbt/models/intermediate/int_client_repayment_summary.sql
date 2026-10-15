-- Per client: repayment behaviour on earlier Home Credit loans.
with installments as (
    select
        sk_id_curr,
        count(*) as installment_payment_count,
        count_if(days_past_due > 0) as late_payment_count,
        count_if(days_past_due > 0) / count(*) as late_payment_ratio,
        max(days_past_due) as installment_max_days_past_due,
        count_if(is_unpaid) as unpaid_installment_count,
        sum(amt_shortfall) as installment_shortfall_sum
    from {{ ref('fact_installment_payment') }}
    group by sk_id_curr
),

pos as (
    select sk_id_curr, max(sk_dpd) as pos_max_dpd
    from {{ ref('fact_pos_cash_monthly') }}
    group by sk_id_curr
),

cards as (
    select
        sk_id_curr,
        max(sk_dpd) as card_max_dpd,
        avg(utilization) as card_avg_utilization
    from {{ ref('fact_credit_card_monthly') }}
    group by sk_id_curr
),

clients as (
    select sk_id_curr from installments
    union
    select sk_id_curr from pos
    union
    select sk_id_curr from cards
)

select
    clients.sk_id_curr,
    installments.installment_payment_count,
    installments.late_payment_count,
    installments.late_payment_ratio,
    installments.installment_max_days_past_due,
    installments.unpaid_installment_count,
    installments.installment_shortfall_sum,
    pos.pos_max_dpd,
    cards.card_max_dpd,
    cards.card_avg_utilization
from clients
left join installments on clients.sk_id_curr = installments.sk_id_curr
left join pos on clients.sk_id_curr = pos.sk_id_curr
left join cards on clients.sk_id_curr = cards.sk_id_curr
where clients.sk_id_curr is not null
