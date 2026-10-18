{#
    Orphaned foreign keys: child rows whose parent does not exist.

    Policy (also in the README):
    - Orphans are kept, not dropped. Their facts point at the unknown client member
      (client_sk = '-1'), so totals still add up and nothing disappears silently.
    - This report counts them per relationship on every run, over all loaded rows and
      for the latest load (the rows with the newest _loaded_at: the day just delivered,
      or the day just backfilled).
    - Some orphans are normal for this dataset, so each relationship has an expected
      rate. assert_orphan_rate_near_baseline warns when the latest load is more than
      var('orphan_warn_margin_pct') points away from it.

    Expected rates are measured on the full Kaggle CSVs (DuckDB, 2026-10-19):
    11.43% of bureau_balance rows have no bureau credit, 9.19% of installments have no
    previous application, and no child row lacks its application.
#}
{% set checks = [
    ('bureau', 'application', 'stg_bureau', 'sk_id_curr', 'stg_application', 'sk_id_curr', 0.0),
    ('bureau_balance', 'bureau', 'stg_bureau_balance', 'sk_id_bureau', 'stg_bureau', 'sk_id_bureau', 11.43),
    ('previous_application', 'application', 'stg_previous_application', 'sk_id_curr', 'stg_application', 'sk_id_curr', 0.0),
    ('pos_cash_balance', 'application', 'stg_pos_cash_balance', 'sk_id_curr', 'stg_application', 'sk_id_curr', 0.0),
    ('credit_card_balance', 'application', 'stg_credit_card_balance', 'sk_id_curr', 'stg_application', 'sk_id_curr', 0.0),
    ('installments_payments', 'application', 'stg_installments_payments', 'sk_id_curr', 'stg_application', 'sk_id_curr', 0.0),
    ('installments_payments', 'previous_application', 'stg_installments_payments', 'sk_id_prev', 'stg_previous_application', 'sk_id_prev', 9.19),
] %}

{% for child, parent, child_model, child_key, parent_model, parent_key, expected_pct in checks %}
select
    '{{ child }}' as child_table,
    '{{ parent }}' as parent_table,
    '{{ child_key }}' as foreign_key,
    count(*) as total_rows,
    count_if(p.{{ parent_key }} is null) as orphan_rows,
    round(100.0 * count_if(p.{{ parent_key }} is null) / nullif(count(*), 0), 3) as orphan_pct,
    {{ expected_pct }} as expected_orphan_pct,
    max(case when c._loaded_at = latest.loaded_at then c.ingest_date end) as latest_ingest_date,
    count_if(c._loaded_at = latest.loaded_at) as latest_load_rows,
    round(
        100.0 * count_if(c._loaded_at = latest.loaded_at and p.{{ parent_key }} is null)
        / nullif(count_if(c._loaded_at = latest.loaded_at), 0),
        3
    ) as latest_load_orphan_pct,
    current_timestamp() as checked_at
from {{ ref(child_model) }} as c
cross join (select max(_loaded_at) as loaded_at from {{ ref(child_model) }}) as latest
left join (select distinct {{ parent_key }} from {{ ref(parent_model) }}) as p
    on c.{{ child_key }} = p.{{ parent_key }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
