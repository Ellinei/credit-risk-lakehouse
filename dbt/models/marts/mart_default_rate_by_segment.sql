{#
    Default rate (share of TARGET = 1) by contract type, income band and age band.
    Only application_train rows have a TARGET, so test rows are left out.
#}
select
    ct.contract_type,
    c.income_band,
    c.age_band,
    count(*) as application_count,
    sum(f.target) as default_count,
    avg(f.target) as default_rate,
    avg(f.amt_credit) as avg_credit_amount
from {{ ref('fact_loan_application') }} as f
join {{ ref('dim_client') }} as c on f.client_sk = c.client_sk
join {{ ref('dim_contract_type') }} as ct on f.contract_type_sk = ct.contract_type_sk
where f.is_train
group by ct.contract_type, c.income_band, c.age_band
