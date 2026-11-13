# Design decisions

> Part of the [documentation index](README.md). The [project README](../README.md) has the architecture diagram; this page explains why each layer works the way it does.

## Replayer: one client range per day

`replayer/` cuts slice *k* as a contiguous `SK_ID_CURR` range (default 2,000 applications), with `ingest_date = start_date + k days`. Every child table is cut with the same range, so a client's bureau, previous application and payment rows arrive on the same day as the application.

A range is used instead of an exact client list on purpose: child rows whose client has no application (orphans) still fall inside some range and reach the pipeline, where dbt counts them. An exact-list filter would have dropped them silently. `bureau_balance` rows with no parent bureau credit are spread across days by `SK_ID_BUREAU` modulo the number of slices.

The source data never changes a client, so there is nothing for an SCD2 snapshot to record. `--resend-updates N` re-sends N earlier clients with a 10% higher income and a different family status. These changes are synthetic and exist only to exercise the snapshot.

## Glue: bookmarks plus dynamic partition overwrite

- **Job bookmarks** (one `transformation_ctx` per table) make a normal run process only the files that arrived since the last successful run. `job.commit()` is the last line, so a failed run does not advance the bookmark.
- **Dynamic partition overwrite** replaces only the `ingest_date` partitions present in the batch. Writing the same day twice gives the same result.
- **Reprocessing a day** (`--reprocess_ingest_date D` with bookmarks disabled) reads only `raw/<table>/ingest_date=D/`, so it rebuilds exactly that day. A table with no files that day comes back as an empty frame and is skipped (the smoke test covers this).
- **Quarantine instead of drop:** rows with a missing key, an unparseable value or no `ingest_date` go to `quarantine/<table>/` with the reasons and the original strings. If more than 5% of a table's rows are bad, the job fails before writing silver (`--max_quarantine_ratio`).
- **Rule-based typing** (`glue/silver_lib/specs.py`) instead of a 300-column hand-written schema: `SK_ID_*` bigint, `DAYS_*` and 0/1 flags int, `AMT_*` and `CNT_*` double, and so on. Unknown columns fall back to string and are logged, so a new column never causes a quarantine. Integers parse through double, so `"2.0"` is fine and `"2.5"` is a bad cast. `try_cast` keeps this safe under ANSI mode too.
- The transformations live in `silver_lib`, plain PySpark with no `awsglue` imports, so they are unit tested in a local SparkSession.

## Getting silver into Databricks

Open question from the start: can a Unity Catalog external location on Free Edition read my own S3 bucket? `databricks/s3_access_test/` is the runbook to find out. The pipeline supports both answers through one setting, `SILVER_ACCESS_MODE`:

- `external_location`: dbt reads `s3://<bucket>/silver/` directly.
- `volume`: Airflow copies the day's silver Parquet into the Volume `<catalog>.landing.silver` with the Databricks SDK, clearing that day's folder first.

Either way, the dbt macro `load_silver` runs `INSERT INTO silver.<table> REPLACE WHERE ingest_date = <day> SELECT ... FROM read_files(...)`. This atomically swaps one day's rows in a Delta table, mirroring Glue's partition overwrite on the Databricks side. It adds `_loaded_at` and changes nothing else.

## dbt: staging, SCD2, galaxy schema

- **Staging** keeps the newest version of each business key, ordered by `ingest_date` then `_loaded_at`. Ordering by load time alone would let an out-of-order backfill of an old day win over newer data.
- **Hashed surrogate keys** (md5 of the business key) instead of `row_number()`. The old Glue draft used window functions for keys. Those depend on which rows happen to be in the table at build time, so the same client could get a different key after a rebuild, which breaks incremental facts. A hash is the same on every run, so dimensions can be rebuilt in full while facts load incrementally.
- **Incremental facts** read only staging rows with a `_loaded_at` newer than the newest row already loaded, then `MERGE` on the business key. Staging's dedupe guarantees one row per key per batch, which Databricks MERGE requires.
- **`snap_client`** (dbt snapshot, check strategy) records SCD Type 2 history of client attributes. `dim_client` has one row per client version, and a fact row keeps the version that was current when it was loaded.
- **Galaxy schema:** 7 facts (applications, bureau credits, bureau months, previous applications, installment payments, POS cash months, credit card months) share conformed dimensions. `dim_client` and `dim_contract_type` are shared across facts; contract type is conformed between current applications (Cash loans, Revolving loans) and previous applications (which add Consumer loans and XNA).
- **No dbt packages:** Free Edition serverless has restricted outbound internet, so `dbt deps` might not reach dbt Hub. The one helper needed (`unique_combination`) is a 10-line generic test.

## Data quality

| Issue | Where | Handling |
|---|---|---|
| `DAYS_EMPLOYED = 365243` means pensioner or unemployed, not 1,000 years of tenure | Glue | Nulled, `days_employed_is_sentinel = true`. Same for five `DAYS_*` columns in previous_application. Singular dbt test plus unit test |
| `CODE_GENDER = 'XNA'` | Glue | Nulled; dbt `accepted_values` is just M and F |
| Unparseable values, missing keys | Glue | Quarantined with reasons; 5% circuit breaker |
| Orphaned foreign keys (for example a bureau row whose client has no application) | dbt | **Kept, not dropped.** Facts point at the unknown client member (`client_sk = '-1'`) so totals still add up. `dq_orphan_report` counts orphans per relationship every run; a warn-level test fires when the latest load is more than 2 points away from the relationship's expected rate |
| Installments have no natural key | Glue + dbt | `record_hash` (SHA-256 of all business columns) is the key. Rows identical in every column collapse into one; this is a deliberate policy |
| Duplicate keys within one delivery | dbt | Warn-level `unique_combination` tests on every silver source |
| Freshness | dbt | `dbt source freshness` warns when the newest `ingest_date` is over 2 days old (warn only, because backfills load old days) |

Plus `unique`, `not_null`, `relationships` (every fact's `client_sk` to `dim_client`, and the other dimension keys) and `accepted_values` tests: 93 data tests in total.

**Orphan rates in the real data.** Two relationships are orphaned in the source itself:

| Relationship | Full dataset (DuckDB over the CSVs) | First 3 replayed days (`dq_orphan_report`) |
|---|---|---|
| bureau_balance to bureau (`sk_id_bureau`) | 3,120,184 of 27.3M rows (11.4%) | 51,529 of 464,126 (11.1%) |
| installments_payments to previous_application (`sk_id_prev`) | 1,250,826 of 13.6M rows (9.2%) | 21,076 of 224,373 (9.4%) |

The other five relationships (every child table to its application) had no orphans in the replayed days. For bureau_balance the replayed rate matching the full dataset also shows that the `SK_ID_BUREAU` modulo rule spreads parentless rows evenly across days instead of piling them into one. Installments are sliced by client range like every other table, so their orphans simply arrive with their client.

The first version of the check warned on any relationship above a flat 1%, so it fired on every run and stopped meaning anything. Now each relationship has an expected rate (the full dataset figure, 0% for the other five), and `assert_orphan_rate_near_baseline` warns only when the latest load (the rows with the newest `_loaded_at`, which also covers a backfilled day) is more than `orphan_warn_margin_pct` (2) points away from it. On days 1 and 3 the daily rates were within 1 point of the expected rates.

## Orchestration

`credit_risk_daily` runs one `ingest_date` per DAG run: plan the run, land the raw slice, run the Glue job, optionally sync to the Volume, then trigger the Databricks job. `max_active_runs=1` because bookmarks and watermarks expect days in order. Backfill a range with:

```bash
airflow backfill create --dag-id credit_risk_daily --from-date 2026-10-01 --to-date 2026-10-07 --max-active-runs 1
```

Trigger with `reprocess=true` to rebuild one day, or `land_slice=false` when the raw files are already in S3.

## Decision log

| Date | Decision | Why |
|---|---|---|
| 2026-10-15 | Glue stops at silver; modeling moved to dbt | One owner per responsibility; no duplicated Spark work |
| 2026-10-15 | Glue 5.1 (Spark 3.5.6) | AWS default version at the time; Glue 6.0 was too new |
| 2026-10-15 | Airflow 3 | Scheduler-managed backfills; the ecommerce repo already shows Airflow 2 |
| 2026-10-19 | Lake bucket in `us-east-2` | `SELECT current_metastore()` shows the Free Edition workspace runs in AWS `us-east-2`. Keeping S3 in the same region avoids cross-region transfer, whichever way silver reaches Databricks |
| 2026-10-19 | Test dbt on Databricks before any AWS run | The Volume path needs no AWS: a local `spark-submit` of the Glue job writes silver, `scripts/push_local_silver.py` uploads it with the same code the DAG uses, and dbt runs against the SQL warehouse. This tested the riskiest untested parts first |
| 2026-10-20 | `SILVER_ACCESS_MODE=external_location` | The Free Edition S3 test passed: a storage credential and external location on `s3://<bucket>/silver/` were created through the SDK, and `LIST` and `read_files` returned the probe rows. The Volume copy stays as the fallback |
| 2026-10-20 | Run the Glue job in the local Glue 5 container against S3 | Managed Glue is blocked: the account is on the AWS Free account plan and `CreateJob` returns "Account ... is denied access". The same entrypoint runs with `spark-submit` in `public.ecr.aws/glue/aws-glue-libs:5` with `s3://` roots until the plan is upgraded. The Glue role and the job code are already deployed for that day |
