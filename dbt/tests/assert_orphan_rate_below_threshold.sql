{{ config(severity='warn') }}

-- Warn when any foreign key relationship has more orphans than var('orphan_warn_pct') percent.
select *
from {{ ref('dq_orphan_report') }}
where orphan_pct > {{ var('orphan_warn_pct') }}
