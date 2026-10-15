-- DAYS_EMPLOYED = 365243 means "pensioner or unemployed", not 1000 years of tenure.
-- Glue nulls it and sets days_employed_is_sentinel. Fail if a sentinel slipped through,
-- or if a flagged row still carries a value.
select sk_id_curr, days_employed, days_employed_is_sentinel
from {{ ref('stg_application') }}
where days_employed = 365243
   or days_employed > 0
   or (days_employed_is_sentinel and days_employed is not null)
