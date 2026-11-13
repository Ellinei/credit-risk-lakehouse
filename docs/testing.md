# Testing

> Part of the [documentation index](README.md). What is tested, how, and what has actually been run against real services.

| What | How | Status |
|---|---|---|
| Replayer | `pytest replayer/tests` | 14 tests pass |
| Glue transforms (Spark) | pytest in the `public.ecr.aws/glue/aws-glue-libs:5` container, see `glue/README.md` | 49 tests pass |
| Glue entrypoint | `glue/tests/smoke/run_smoke.sh`: real `spark-submit` on replayer output, then a reprocess run of a day with empty tables | passes |
| Airflow run plan and Volume sync | `pytest airflow/tests` (fakes for S3 and the Files API) | pass |
| DAG integrity | pytest inside the Airflow image; `plan_run` also run with `airflow tasks test` | pass |
| dbt | `dbt parse` (no connection) | passes: 25 models, 1 snapshot, 8 sources, 93 tests |
| Glue job on the real data | Local `spark-submit` in the Glue 5 container on 3 replayed days (about 980k rows), default 5% quarantine limit | passes in 1 min 40 s: 0 rows quarantined in all 8 tables, no column fell back to string, `DAYS_EMPLOYED` sentinel on about 18% of applicants |
| dbt against Databricks | Local dbt against the Free Edition SQL warehouse, 3 days through the Volume path, one `load_silver` plus `dbt build` per day | passes every day: 118 PASS, 1 WARN (the old flat 1% orphan check; its replacement passes on day 3 and warns as intended when the margin is tightened to 0.5), 0 errors, 2.5 to 3 min per build. After day 3: 6,000 clients in `fact_loan_application`, the 50 re-sent clients updated in place by MERGE, 50 closed versions in `snap_client`, every other fact equal to its silver row total |
| Glue job against S3 (local container) | `spark-submit` of `bronze_to_silver.py` in the Glue 5 container with `s3://` raw, silver and quarantine roots, one `--reprocess_ingest_date` run per day (bookmarks off), days landed by `replayer --target s3` | passes for 3 days: 5 min 20 s to 6 min 30 s per run, about 980k rows, 0 rows quarantined, silver row counts equal to the earlier local run for all 8 tables and 3 days |
| External location on real silver | `read_files('s3://<bucket>/silver/<table>/', format => 'parquet')` through the SQL warehouse | reads all 8 tables, `ingest_date` comes back as a partition column, counts match the local silver |
| Databricks job (bundle) | `databricks bundle validate`, `deploy --var silver_root=s3://<bucket>/silver`, then `bundle run credit_risk_dbt --params ingest_date=2026-10-03` (serverless dbt task, silver read from S3 through the external location) | passes: 2 min 47 s, PASS=119 WARN=0 ERROR=0 SKIP=0 (93 tests, 25 models, 1 snapshot). The 8 source freshness checks warn, as they do locally, because the data is older than the freshness window. The day was already loaded, so it doubles as an idempotency test: every fact, dim and mart count, `snap_client` rows (6,050, 50 closed) and `dq_orphan_report` rows were identical before and after |
| Cloud end to end | Managed Glue on AWS, the Airflow DAG against real services | **not run**: managed Glue is blocked by the AWS Free account plan, and the DAG has not run |
