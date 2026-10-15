{#
    One row per applicant (sk_id_curr): application attributes plus aggregates from the
    Credit Bureau, previous applications and repayment history. This is the table a
    credit scoring model would train on.
#}
select
    f.sk_id_curr,
    f.target,
    f.is_train,
    ct.contract_type,
    c.gender,
    c.age_years,
    c.age_band,
    c.income_total,
    c.income_band,
    c.income_type,
    c.education_type,
    c.family_status,
    c.occupation_type,
    c.cnt_children,
    f.amt_credit,
    f.amt_annuity,
    f.credit_to_income_ratio,
    f.annuity_to_income_ratio,
    f.days_employed,
    f.days_employed_is_sentinel,
    f.ext_source_2,
    coalesce(b.bureau_credit_count, 0) as bureau_credit_count,
    coalesce(b.bureau_active_credit_count, 0) as bureau_active_credit_count,
    b.bureau_debt_sum,
    b.bureau_overdue_sum,
    b.bureau_max_days_overdue,
    b.bureau_worst_bucket_rank,
    coalesce(p.previous_application_count, 0) as previous_application_count,
    p.previous_approval_rate,
    p.previous_refused_count,
    r.installment_payment_count,
    r.late_payment_ratio,
    r.installment_max_days_past_due,
    {{ dpd_bucket('r.installment_max_days_past_due') }} as installment_worst_dpd_bucket,
    r.unpaid_installment_count,
    r.pos_max_dpd,
    r.card_max_dpd,
    r.card_avg_utilization
from {{ ref('fact_loan_application') }} as f
join {{ ref('dim_client') }} as c on f.client_sk = c.client_sk
join {{ ref('dim_contract_type') }} as ct on f.contract_type_sk = ct.contract_type_sk
left join {{ ref('int_client_bureau_summary') }} as b on f.sk_id_curr = b.sk_id_curr
left join {{ ref('int_client_previous_summary') }} as p on f.sk_id_curr = p.sk_id_curr
left join {{ ref('int_client_repayment_summary') }} as r on f.sk_id_curr = r.sk_id_curr
