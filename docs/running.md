# Running it

> Part of the [documentation index](README.md). Commands run from the repo root unless a step says otherwise.

## Full setup

Prerequisites: Python 3.11+, Docker, an AWS account with the CLI configured, a Databricks Free Edition workspace, and the Kaggle CSVs in `data/raw/` (`kaggle competitions download -c home-credit-default-risk`).

1. `cp .env.example .env` and fill it in.
2. Create the bucket, then the Glue role from `glue/iam/` (trust policy plus the permissions policy with your bucket name).
3. `python scripts/deploy_glue_job.py` uploads the code and creates the Glue job.
4. Run the S3 test in [`databricks/s3_access_test/`](../databricks/s3_access_test/README.md) and set `SILVER_ACCESS_MODE`.
5. From `dbt/`: `databricks bundle deploy --var silver_root=s3://<bucket>/silver` (in `external_location` mode; the default is the Volume path; add `--var catalog=workspace` if needed). Any recent Databricks CLI works; it can be unzipped anywhere and called by full path.
6. From `airflow/`: `docker compose --env-file ../.env up -d --build`, open http://localhost:8080, and unpause `credit_risk_daily`, or trigger it with an `ingest_date`.

## Glue job against S3 without managed Glue

To run the Glue job against S3 without managed Glue, land a day with `python -m replayer.replay --target s3 --bucket <bucket>`, then run the same `spark-submit` as the smoke test in the Glue 5 container with `--raw_root`, `--silver_root` and `--quarantine_root` set to `s3://<bucket>/...`, `--job-bookmark-option job-bookmark-disable` and `--reprocess_ingest_date <day>`. Pass `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and `AWS_REGION` to `docker run` with `-e NAME` (the container cannot use an `aws login` profile). The `s3://` scheme worked in the image as it is.

## Try the replayer locally (no cloud)

Try the replayer locally without any cloud:

```bash
pip install -r requirements-dev.txt
python -m replayer.replay --data-dir tests/fixtures/home_credit --ingest-date 2026-10-01 \
    --clients-per-slice 3 --target local --out-dir out/landing
```

## Real data through Glue and dbt, without AWS

Run the real data through Glue and dbt on Databricks without AWS (the Volume path). This needs the Kaggle CSVs in `data/raw/`, the `DATABRICKS_*` values in `.env` exported to the shell, and `dbt/profiles.yml` copied from the example:

```bash
# 1. Land one day and run the Glue job locally (same docker run as glue/tests/smoke/run_smoke.sh,
#    with --raw_root, --silver_root and --quarantine_root under out/real/)
python -m replayer.replay --ingest-date 2026-10-01 --cache-dir data/cache --out-dir out/real/landing

# 2. Upload that day's silver into the Volume /Volumes/<catalog>/landing/silver
python scripts/push_local_silver.py --silver-dir out/real/silver --ingest-date 2026-10-01

# 3. Load it into Delta and build, as the Databricks job does (from dbt/)
dbt run-operation load_silver --args '{"ingest_date": "2026-10-01"}'
dbt source freshness
dbt build
```

## Cost notes

Estimates use us-east-1 list prices at the time of writing; check your region.

- **Glue:** 2 G.1X workers = 2 DPU. With the Flex execution class ($0.29 per DPU-hour, against $0.44 standard) and a run of about 3 minutes on a 2,000-client slice: 2 × 0.05 h × $0.29, roughly **$0.03 per run**. Glue bills a 1-minute minimum per run.
- **S3:** the full dataset is about 2.5 GB raw plus less in Parquet: a few cents per month. Requests are negligible at one slice per day.
- **Data transfer:** the Volume fallback downloads each day's silver Parquet out of AWS (a few MB per slice), well inside AWS's monthly free data transfer allowance.
- **Databricks Free Edition:** $0, under a fair usage quota. Going over it shuts compute down for the day, which is why slices default to 2,000 clients.
- **Airflow:** local Docker, $0.

A daily run therefore costs a few cents, almost all of it Glue.
