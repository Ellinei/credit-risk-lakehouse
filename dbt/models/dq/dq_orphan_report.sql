{#
    Orphaned foreign keys: child rows whose parent does not exist.

    Policy (also in the README):
    - Orphans are kept, not dropped. Their facts point at the unknown client member
      (client_sk = '-1'), so totals still add up and nothing disappears silently.
    - This report counts them per relationship on every run.
    - assert_orphan_rate_below_threshold warns when a relationship goes above
      var('orphan_warn_pct') percent.
#}
{% set checks = [
    ('bureau', 'application', 'stg_bureau', 'sk_id_curr', 'stg_application', 'sk_id_curr'),
    ('bureau_balance', 'bureau', 'stg_bureau_balance', 'sk_id_bureau', 'stg_bureau', 'sk_id_bureau'),
    ('previous_application', 'application', 'stg_previous_application', 'sk_id_curr', 'stg_application', 'sk_id_curr'),
    ('pos_cash_balance', 'application', 'stg_pos_cash_balance', 'sk_id_curr', 'stg_application', 'sk_id_curr'),
    ('credit_card_balance', 'application', 'stg_credit_card_balance', 'sk_id_curr', 'stg_application', 'sk_id_curr'),
    ('installments_payments', 'application', 'stg_installments_payments', 'sk_id_curr', 'stg_application', 'sk_id_curr'),
    ('installments_payments', 'previous_application', 'stg_installments_payments', 'sk_id_prev', 'stg_previous_application', 'sk_id_prev'),
] %}

{% for child, parent, child_model, child_key, parent_model, parent_key in checks %}
select
    '{{ child }}' as child_table,
    '{{ parent }}' as parent_table,
    '{{ child_key }}' as foreign_key,
    count(*) as total_rows,
    count_if(p.{{ parent_key }} is null) as orphan_rows,
    round(100.0 * count_if(p.{{ parent_key }} is null) / nullif(count(*), 0), 3) as orphan_pct,
    current_timestamp() as checked_at
from {{ ref(child_model) }} as c
left join (select distinct {{ parent_key }} from {{ ref(parent_model) }}) as p
    on c.{{ child_key }} = p.{{ parent_key }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
