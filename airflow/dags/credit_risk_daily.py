"""
### credit_risk_daily

One simulated day of the Home Credit lakehouse, end to end:

1. **plan_run**: settle the ingest_date, the Glue arguments and where Databricks reads silver.
2. **land_raw_slice**: the replayer drops that day's slice into `s3://<bucket>/raw/` (turn off
   with `land_slice=false` when files are already there).
3. **bronze_to_silver**: Glue job, CSV to typed Parquet with quarantine. Bookmarks on, or
   `reprocess=true` to rebuild exactly that day.
4. **choose_silver_path**: with `SILVER_ACCESS_MODE=volume`, copy the day's silver Parquet
   into a Unity Catalog Volume first (Free Edition fallback); otherwise read S3 in place.
5. **run_dbt_on_databricks**: the `credit_risk_dbt` job loads the day into Delta and runs
   `dbt build`.

Backfill a range (one run per day, oldest first):

    airflow backfill create --dag-id credit_risk_daily \\
        --from-date 2026-10-01 --to-date 2026-10-07 --max-active-runs 1
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, task
from airflow.sdk.exceptions import AirflowSkipException

from credit_risk_lib import run_plan, volume_sync

LAKE_BUCKET = os.environ.get("LAKE_BUCKET", "")
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "credit_risk_bronze_to_silver")
DATABRICKS_JOB_NAME = os.environ.get("DATABRICKS_JOB_NAME", "credit_risk_dbt")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "credit_risk")
SILVER_ACCESS_MODE = os.environ.get("SILVER_ACCESS_MODE", "volume")
REPLAY_START_DATE = os.environ.get("REPLAY_START_DATE", "2026-10-01")
REPLAY_CLIENTS_PER_SLICE = int(os.environ.get("REPLAY_CLIENTS_PER_SLICE", "2000"))
DATA_DIR = Path("/opt/airflow/data")

SILVER_TABLES = [
    "application_train", "application_test", "bureau", "bureau_balance",
    "previous_application", "pos_cash_balance", "credit_card_balance",
    "installments_payments",
]

with DAG(
    dag_id="credit_risk_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    # Glue bookmarks and the dbt watermarks expect days to be processed one at a time.
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=5)},
    params={
        "ingest_date": Param(
            "", type="string",
            description="YYYY-MM-DD. Empty means the run's logical date.",
        ),
        "land_slice": Param(
            True, type="boolean",
            description="Run the replayer to drop this day's raw slice into S3.",
        ),
        "reprocess": Param(
            False, type="boolean",
            description="Rebuild exactly this day in Glue (bookmarks off, one partition read).",
        ),
    },
    tags=["credit-risk", "glue", "databricks"],
    doc_md=__doc__,
) as dag:

    @task(multiple_outputs=True)
    def plan_run(params=None, logical_date=None) -> dict:
        ingest_date = run_plan.resolve_ingest_date(params["ingest_date"], logical_date)
        return {
            "ingest_date": ingest_date,
            "glue_args": run_plan.glue_script_args(ingest_date, params["reprocess"]),
            "silver_root": run_plan.silver_root(SILVER_ACCESS_MODE, LAKE_BUCKET, CATALOG),
            "access_mode": SILVER_ACCESS_MODE,
        }

    @task
    def land_raw_slice(ingest_date: str, params=None) -> dict:
        if not params["land_slice"]:
            return {"skipped": "land_slice is false, using files already in raw/"}
        from replayer.replay import replay
        from replayer.slicing import SliceOutOfRange

        try:
            result = replay(
                data_dir=DATA_DIR / "raw",
                cache_dir=DATA_DIR / "cache",
                ingest_date=dt.date.fromisoformat(ingest_date),
                start_date=dt.date.fromisoformat(REPLAY_START_DATE),
                clients_per_slice=REPLAY_CLIENTS_PER_SLICE,
                target="s3",
                bucket=LAKE_BUCKET,
            )
        except SliceOutOfRange as exc:
            # Every client has been replayed: nothing new today, skip the rest of the run.
            raise AirflowSkipException(str(exc)) from exc
        return result.rows_per_table

    plan = plan_run()
    landed = land_raw_slice(plan["ingest_date"])

    bronze_to_silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name=GLUE_JOB_NAME,
        script_args=plan["glue_args"],
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        stop_job_run_on_kill=True,
        verbose=False,
    )

    @task.branch
    def choose_silver_path(access_mode: str) -> str:
        return "sync_silver_to_volume" if access_mode == "volume" else "silver_read_in_place"

    @task
    def sync_silver_to_volume(ingest_date: str) -> list[dict]:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from databricks.sdk import WorkspaceClient

        s3 = S3Hook(aws_conn_id="aws_default", region_name=AWS_REGION).get_conn()
        workspace = WorkspaceClient()  # DATABRICKS_HOST and DATABRICKS_TOKEN from the env
        volume_sync.ensure_volume(workspace, CATALOG)
        synced = volume_sync.sync_partition(
            s3, workspace.files,
            bucket=LAKE_BUCKET, catalog=CATALOG, ingest_date=ingest_date, tables=SILVER_TABLES,
        )
        return [vars(s) for s in synced]

    silver_read_in_place = EmptyOperator(task_id="silver_read_in_place")

    run_dbt_on_databricks = DatabricksRunNowOperator(
        task_id="run_dbt_on_databricks",
        databricks_conn_id="databricks_default",
        job_name=DATABRICKS_JOB_NAME,
        job_parameters={
            "ingest_date": "{{ ti.xcom_pull(task_ids='plan_run', key='ingest_date') }}",
            "silver_root": "{{ ti.xcom_pull(task_ids='plan_run', key='silver_root') }}",
        },
        wait_for_termination=True,
        # Runs after whichever branch ran; skipped when the whole day was skipped.
        trigger_rule="none_failed_min_one_success",
    )

    choice = choose_silver_path(plan["access_mode"])
    synced = sync_silver_to_volume(plan["ingest_date"])

    landed >> bronze_to_silver >> choice
    choice >> [synced, silver_read_in_place] >> run_dbt_on_databricks
