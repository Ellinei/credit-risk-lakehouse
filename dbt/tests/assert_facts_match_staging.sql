-- Incremental loads must not lose or duplicate rows: every fact holds exactly as many
-- rows as its deduplicated staging model.
{% set pairs = [
    ('fact_loan_application', 'stg_application'),
    ('fact_bureau_credit', 'stg_bureau'),
    ('fact_bureau_balance_monthly', 'stg_bureau_balance'),
    ('fact_previous_application', 'stg_previous_application'),
    ('fact_installment_payment', 'stg_installments_payments'),
    ('fact_pos_cash_monthly', 'stg_pos_cash_balance'),
    ('fact_credit_card_monthly', 'stg_credit_card_balance'),
] %}

{% for fact, staging in pairs %}
select
    '{{ fact }}' as fact_model,
    (select count(*) from {{ ref(fact) }}) as fact_rows,
    (select count(*) from {{ ref(staging) }}) as staging_rows
where (select count(*) from {{ ref(fact) }}) != (select count(*) from {{ ref(staging) }})
{% if not loop.last %}union all{% endif %}
{% endfor %}
