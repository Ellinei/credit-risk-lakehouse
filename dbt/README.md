# dbt: silver to gold on Databricks

dbt-databricks project `credit_risk`. Glue already typed and cleaned the data, so this project only models it: staging, an SCD2 snapshot, a galaxy schema (several facts sharing conformed dimensions), and credit risk marts. Everything lives in one Unity Catalog catalog (`credit_risk`, or `workspace` if Free Edition does not allow new catalogs).

| Schema | What is in it | Materialization |
|---|---|---|
| `silver` | Glue's Parquet loaded by `load_silver` (one table per source file) | Delta tables, `REPLACE WHERE ingest_date` |
| `staging` | newest version of each record, curated columns | views |
| `snapshots` | `snap_client`, SCD Type 2 client history | dbt snapshot |
| `gold` | 4 dimensions (tables) and 7 facts (incremental MERGE) | tables, incremental |
| `intermediate` | per-client summaries feeding the marts | views |
| `marts` | delinquency buckets, default rate by segment, applicant features | tables |
| `dq` | orphan report | table |

## Gold layer (galaxy schema)

- Dimensions: `dim_client` (conformed, SCD2), `dim_contract_type` (conformed across current and previous applications), `dim_credit_type`, `dim_application_status`.
- Facts: `fact_loan_application`, `fact_bureau_credit`, `fact_bureau_balance_monthly`, `fact_previous_application`, `fact_installment_payment`, `fact_pos_cash_monthly`, `fact_credit_card_monthly`.

Surrogate keys are md5 hashes of business keys (`macros/keys.sql`), so dimensions can be rebuilt in full while facts load incrementally without keys drifting.

## How a run works

The Databricks job (`databricks.yml`, `resources/credit_risk_job.yml`) runs three commands on serverless compute:

1. `dbt run-operation load_silver --args '{"ingest_date": "...", "silver_root": "..."}'` swaps that day's rows into `silver.*`.
2. `dbt source freshness` warns if the newest `ingest_date` is more than 2 days old.
3. `dbt build` runs staging, the snapshot, gold, marts and every test in dependency order.

Facts only read staging rows with a `_loaded_at` newer than what they already hold, then MERGE on the business key.

## Running locally

```bash
pip install dbt-databricks==1.12.7
cp profiles.yml.example profiles.yml        # fill env vars from ../.env
dbt run-operation load_silver --args '{"ingest_date": "2026-10-01"}'
dbt build
```

`dbt parse --profiles-dir ci` checks the project without a connection (this is what CI runs).

## Deploying the job

```bash
databricks bundle validate
databricks bundle deploy                  # --var catalog=workspace, --var silver_root=s3://... as needed
databricks bundle run credit_risk_dbt --params ingest_date=2026-10-01
```

The project uses no dbt packages, so the job never calls `dbt deps`. Free Edition limits outbound internet from serverless compute, and dbt Hub might not be reachable.
