{{ config(severity='warn') }}

-- Warn when the latest load's orphan rate for a relationship is more than
-- var('orphan_warn_margin_pct') points away from its expected rate in dq_orphan_report.
-- Some orphans are normal in this dataset (about 11% and 9% for two relationships),
-- so a flat threshold would fire on every run and stop meaning anything.
select *
from {{ ref('dq_orphan_report') }}
where abs(latest_load_orphan_pct - expected_orphan_pct) > {{ var('orphan_warn_margin_pct') }}
