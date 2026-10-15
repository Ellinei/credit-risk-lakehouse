{#
    Conformed across the current application (Cash loans, Revolving loans) and previous
    applications (which add Consumer loans and XNA), so both facts can be sliced by the
    same contract type.
#}
with all_types as (
    select name_contract_type, 'application' as seen_in from {{ ref('stg_application') }}
    union all
    select name_contract_type, 'previous_application' from {{ ref('stg_previous_application') }}
),

types as (
    select
        name_contract_type as contract_type,
        max(seen_in = 'application') as seen_in_applications,
        max(seen_in = 'previous_application') as seen_in_previous_applications
    from all_types
    where name_contract_type is not null
    group by name_contract_type
)

select
    {{ dim_key('contract_type') }} as contract_type_sk,
    contract_type,
    seen_in_applications,
    seen_in_previous_applications
from types
union all
select {{ unknown_key() }}, 'Unknown', false, false
