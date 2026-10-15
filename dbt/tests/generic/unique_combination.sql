{#
    Rows are unique on a combination of columns (a small stand-in for
    dbt_utils.unique_combination_of_columns; the project uses no packages because
    Free Edition serverless compute has restricted outbound internet for `dbt deps`).
#}
{% test unique_combination(model, combination) %}
    select {{ combination | join(', ') }}, count(*) as row_count
    from {{ model }}
    group by {{ combination | join(', ') }}
    having count(*) > 1
{% endtest %}
