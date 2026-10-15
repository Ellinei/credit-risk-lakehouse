-- Free Edition S3 access test. Run each block in the SQL editor and note the result
-- in README.md ("Record the result"). Replace <BUCKET> before running.

-- Step 0: can Free Edition create its own catalog?
-- If this fails, use the built-in `workspace` catalog everywhere (dbt var `catalog`,
-- DATABRICKS_CATALOG in .env).
CREATE CATALOG IF NOT EXISTS credit_risk;
SHOW SCHEMAS IN credit_risk;

-- Step 4: after the storage credential and external location exist, list the probe file.
LIST 's3://<BUCKET>/silver/_probe/';

-- Step 5: read it. Success here means Databricks can read Glue's silver output directly.
SELECT *
FROM read_files('s3://<BUCKET>/silver/_probe/', format => 'csv', header => true)
LIMIT 10;

-- Fallback check: a managed Volume works on every edition. Upload the same probe file to
-- this Volume in Catalog Explorer, then run the two statements below.
CREATE SCHEMA IF NOT EXISTS credit_risk.landing;
CREATE VOLUME IF NOT EXISTS credit_risk.landing.silver;
LIST '/Volumes/credit_risk/landing/silver/';
SELECT * FROM read_files('/Volumes/credit_risk/landing/silver/', format => 'csv', header => true)
LIMIT 10;
