-- Type of credit reported by the Credit Bureau (Consumer credit, Credit card, Mortgage, ...).
with types as (
    select distinct credit_type
    from {{ ref('stg_bureau') }}
    where credit_type is not null
)

select
    {{ dim_key('credit_type') }} as credit_type_sk,
    credit_type
from types
union all
select {{ unknown_key() }}, 'Unknown'
