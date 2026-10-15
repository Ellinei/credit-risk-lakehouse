"""DAG integrity checks. They need Airflow, so they run inside the Airflow image:

    cd airflow
    docker compose run --rm --no-deps -v "$(pwd)/tests:/opt/airflow/tests" \
        -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db --entrypoint "" \
        airflow-scheduler bash -c "pip install -q pytest && airflow db migrate && \
        cd /opt/airflow && PYTHONPATH=/opt/airflow:/opt/airflow/dags python -m pytest tests -q"

(DagBag in Airflow 3.3 needs a metadata DB; a throwaway SQLite file is enough.)
"""

from pathlib import Path

import pytest

# The repo's own airflow/ folder is importable as a namespace package, so check for a
# real Airflow module rather than just "airflow". DagBag lives here since Airflow 3.1.
dagbag_module = pytest.importorskip("airflow.dag_processing.dagbag")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = dagbag_module.DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    return bag.get_dag("credit_risk_daily")


def test_task_order(dag):
    def downstream(task_id):
        return set(dag.get_task(task_id).downstream_task_ids)

    assert downstream("plan_run") >= {"land_raw_slice", "bronze_to_silver", "choose_silver_path"}
    assert downstream("land_raw_slice") == {"bronze_to_silver"}
    assert downstream("bronze_to_silver") == {"choose_silver_path"}
    assert downstream("choose_silver_path") == {"sync_silver_to_volume", "silver_read_in_place"}
    assert downstream("sync_silver_to_volume") == {"run_dbt_on_databricks"}
    assert downstream("silver_read_in_place") == {"run_dbt_on_databricks"}


def test_dbt_runs_after_either_branch(dag):
    assert dag.get_task("run_dbt_on_databricks").trigger_rule == "none_failed_min_one_success"


def test_one_day_at_a_time_and_params(dag):
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert set(dag.params.keys()) == {"ingest_date", "land_slice", "reprocess"}
