# Glue: bronze to silver

One Glue 5.1 PySpark job, `credit_risk_bronze_to_silver`, turns the raw CSV slices into typed, partitioned Parquet. It does not build any dimensional model; that is dbt's job on Databricks.

```
glue/
  jobs/bronze_to_silver.py   Glue entrypoint: GlueContext reads, bookmarks, writes
  silver_lib/specs.py        table contracts: keys, required columns, sentinels, typing rules
  silver_lib/transforms.py   plain PySpark: standardize, cast, clean, quarantine split, hash
  iam/                       trust and permissions policy for the job role
  tests/                     unit tests (pytest) and the container smoke test
```

## What the job does per table

1. Reads new CSV files under `raw/<table>/` (job bookmarks, one `transformation_ctx` per table). No new files: the table is skipped.
2. Renames columns to snake_case and fails fast if a required column is missing.
3. Casts each column by rule (see `specs.py`). A value that is present but cannot be parsed is a bad cast.
4. Cleans known data issues: `DAYS_EMPLOYED = 365243` (and five `DAYS_*` columns in previous_application) becomes null with a `<column>_is_sentinel` flag; `CODE_GENDER = 'XNA'` becomes null.
5. Adds `ingest_date` (from the file path), `_source_file`, `_processed_at` and `record_hash`.
6. Sends rows with a missing key, a bad cast or no `ingest_date` to `quarantine/<table>/` with the reasons and the original strings.
7. Fails the table if more than `--max_quarantine_ratio` (default 5%) of rows are bad, before writing silver.
8. Writes `silver/<table>/ingest_date=YYYY-MM-DD/` with dynamic partition overwrite.

## Running tests

The unit tests in `test_specs.py` need only Python. The Spark tests need PySpark 3.5 and Java 17, which is easiest in the Glue 5 image (matches the cloud runtime):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W):/home/hadoop/workspace" \
  -w /home/hadoop/workspace public.ecr.aws/glue/aws-glue-libs:5 \
  -c 'PYTHONPATH=glue:$PYTHONPATH python3 -m pytest glue/tests -q -p no:cacheprovider'
```

(On Linux or macOS use `$(pwd)` and drop `MSYS_NO_PATHCONV`. Prepend to `PYTHONPATH` rather than replacing it, or the image loses its `awsglue` module.)

Smoke test of the real entrypoint (replayer output from the fixtures, spark-submit in the container, then a reprocess run of one day):

```bash
glue/tests/smoke/run_smoke.sh
```

## Deploying

```bash
python scripts/deploy_glue_job.py --dry-run          # show the job definition
python scripts/deploy_glue_job.py --bucket <bucket> --role-arn <glue-role-arn>
```

The role needs `iam/glue_trust_policy.json` as trust policy and `iam/glue_job_policy.json` (with your bucket name) as permissions.

## Reprocessing a day

```bash
aws glue start-job-run --job-name credit_risk_bronze_to_silver --arguments \
  '{"--reprocess_ingest_date":"2026-10-02","--job-bookmark-option":"job-bookmark-disable"}'
```

Only `raw/<table>/ingest_date=2026-10-02/` is read, and only that silver partition is replaced. The Airflow DAG does this when triggered with `reprocess=true`.
