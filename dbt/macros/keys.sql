{#
    Surrogate keys are hashes of the business key instead of row_number() values.

    A row_number() key depends on which rows happen to be in the table when the model
    runs, so the same customer can get a different key after a rebuild or an
    incremental run. A hash of the business key is the same on every run, which is what
    lets facts be loaded incrementally while dimensions are rebuilt in full.
#}

{% macro unknown_key() -%}
    '-1'
{%- endmacro %}


{# Key of a single-column dimension. A missing value maps to the unknown member. #}
{% macro dim_key(column) -%}
    case when {{ column }} is null then {{ unknown_key() }} else md5(cast({{ column }} as string)) end
{%- endmacro %}


{# Hash of several columns, nulls kept distinct from empty strings. #}
{% macro surrogate_key(columns) -%}
    md5(concat_ws('||'
    {%- for column in columns -%}
        , coalesce(cast({{ column }} as string), '\\N')
    {%- endfor -%}
    ))
{%- endmacro %}
