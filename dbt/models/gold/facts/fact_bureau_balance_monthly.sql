{#
    Grain: one row per bureau credit per month. The client comes through bureau;
    rows whose bureau credit is missing (orphans in the source) get the unknown client.
#}
{{ config(unique_key=['sk_id_bureau', 'months_balance']) }}

with balance as (
    select * from {{ ref('stg_bureau_balance') }}
    {{ loaded_since_last_run() }}
),

bureau as (
    select sk_id_bureau, sk_id_curr from {{ ref('stg_bureau') }}
),

current_client as (
    select client_sk, sk_id_curr from {{ ref('dim_client') }} where is_current
)

select
    bb.sk_id_bureau,
    bb.months_balance,
    b.sk_id_curr,
    coalesce(c.client_sk, {{ unknown_key() }}) as client_sk,
    bb.status,
    {{ bureau_status_bucket('bb.status') }} as dpd_bucket,
    b.sk_id_bureau is null as is_orphan_bureau,
    bb.ingest_date,
    bb._loaded_at
from balance as bb
left join bureau as b on bb.sk_id_bureau = b.sk_id_bureau
left join current_client as c on b.sk_id_curr = c.sk_id_curr
