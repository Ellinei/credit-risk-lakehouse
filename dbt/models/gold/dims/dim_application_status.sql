-- Outcome of a previous application (Approved, Refused, Canceled, Unused offer).
with statuses as (
    select distinct name_contract_status
    from {{ ref('stg_previous_application') }}
    where name_contract_status is not null
)

select
    {{ dim_key('name_contract_status') }} as application_status_sk,
    name_contract_status as application_status,
    name_contract_status = 'Approved' as is_approved
from statuses
union all
select {{ unknown_key() }}, 'Unknown', false
