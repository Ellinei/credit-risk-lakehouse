{# Days past due to the standard delinquency buckets. #}
{% macro dpd_bucket(days_past_due) -%}
    case
        when {{ days_past_due }} is null then null
        when {{ days_past_due }} <= 0 then 'DPD 0'
        when {{ days_past_due }} <= 30 then 'DPD 1-30'
        when {{ days_past_due }} <= 60 then 'DPD 31-60'
        when {{ days_past_due }} <= 90 then 'DPD 61-90'
        else 'DPD 90+'
    end
{%- endmacro %}


{#
    bureau_balance.STATUS: 0 no DPD, 1 = 1-30, 2 = 31-60, 3 = 61-90, 4 = 91-120,
    5 = 120+ or written off, C closed, X unknown.
#}
{% macro bureau_status_bucket(status) -%}
    case {{ status }}
        when '0' then 'DPD 0'
        when '1' then 'DPD 1-30'
        when '2' then 'DPD 31-60'
        when '3' then 'DPD 61-90'
        when '4' then 'DPD 90+'
        when '5' then 'DPD 90+'
        when 'C' then 'Closed'
        when 'X' then 'Unknown'
    end
{%- endmacro %}


{# Sort order for buckets, so "worst bucket" is a max(). #}
{% macro dpd_bucket_rank(bucket) -%}
    case {{ bucket }}
        when 'DPD 0' then 0
        when 'DPD 1-30' then 1
        when 'DPD 31-60' then 2
        when 'DPD 61-90' then 3
        when 'DPD 90+' then 4
    end
{%- endmacro %}


{# Age in whole years from DAYS_BIRTH (negative days before the application). #}
{% macro age_years(days_birth) -%}
    cast(floor(-{{ days_birth }} / 365.25) as int)
{%- endmacro %}


{% macro age_band(age_years) -%}
    case
        when {{ age_years }} is null then null
        when {{ age_years }} < 30 then '20-29'
        when {{ age_years }} < 40 then '30-39'
        when {{ age_years }} < 50 then '40-49'
        when {{ age_years }} < 60 then '50-59'
        else '60+'
    end
{%- endmacro %}


{% macro income_band(income) -%}
    case
        when {{ income }} is null then null
        when {{ income }} < 100000 then 'under 100k'
        when {{ income }} < 150000 then '100k-150k'
        when {{ income }} < 200000 then '150k-200k'
        when {{ income }} < 300000 then '200k-300k'
        else '300k+'
    end
{%- endmacro %}


{# Keep only the newest version of each key: latest ingest_date wins, then latest load. #}
{% macro latest_per_key(keys) -%}
    qualify row_number() over (
        partition by {{ keys | join(', ') }}
        order by ingest_date desc, _loaded_at desc
    ) = 1
{%- endmacro %}


{# Incremental facts only pick up rows loaded after the newest row already in the table. #}
{% macro loaded_since_last_run(column='_loaded_at') -%}
    {%- if is_incremental() -%}
        where {{ column }} > (
            select coalesce(max({{ column }}), timestamp'1900-01-01') from {{ this }}
        )
    {%- endif -%}
{%- endmacro %}
