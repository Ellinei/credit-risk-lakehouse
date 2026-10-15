{#
    Conformed client dimension, one row per version of a client (SCD Type 2 from
    snap_client). client_sk is dbt's per-version hash, so a fact row keeps pointing at
    the version that was current when it was loaded.
#}
with versions as (
    select
        dbt_scd_id as client_sk,
        sk_id_curr,
        code_gender as gender,
        age_years,
        {{ age_band('age_years') }} as age_band,
        amt_income_total as income_total,
        {{ income_band('amt_income_total') }} as income_band,
        name_income_type as income_type,
        name_education_type as education_type,
        name_family_status as family_status,
        name_housing_type as housing_type,
        occupation_type,
        cnt_children,
        owns_car,
        owns_realty,
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        dbt_valid_to is null as is_current
    from {{ ref('snap_client') }}
),

unknown_member as (
    -- Facts whose client has no application (orphans) point here instead of nowhere.
    select
        {{ unknown_key() }} as client_sk,
        cast(null as bigint) as sk_id_curr,
        cast(null as string) as gender,
        cast(null as int) as age_years,
        cast(null as string) as age_band,
        cast(null as double) as income_total,
        cast(null as string) as income_band,
        cast(null as string) as income_type,
        cast(null as string) as education_type,
        cast(null as string) as family_status,
        cast(null as string) as housing_type,
        cast(null as string) as occupation_type,
        cast(null as int) as cnt_children,
        cast(null as boolean) as owns_car,
        cast(null as boolean) as owns_realty,
        timestamp'1900-01-01' as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
)

select * from versions
union all
select * from unknown_member
