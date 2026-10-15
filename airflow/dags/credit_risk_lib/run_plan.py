"""Decisions made once at the start of a DAG run. Plain Python, unit tested without Airflow."""

from __future__ import annotations

import datetime as dt

ACCESS_MODES = ("external_location", "volume")


def resolve_ingest_date(param_value: str | None, logical_date: dt.datetime | None) -> str:
    """The run's ingest_date: an explicit `ingest_date` param wins, else the logical date.

    Airflow 3 manual runs can have no logical date, so a manual run without the param fails
    loudly instead of guessing.
    """
    if param_value:
        return dt.date.fromisoformat(param_value).isoformat()
    if logical_date is None:
        raise ValueError("no logical date on this run: trigger it with params.ingest_date")
    return logical_date.date().isoformat()


def glue_script_args(ingest_date: str, reprocess: bool) -> dict[str, str]:
    """Arguments for this Glue run.

    Normal run: bookmarks on, Glue reads whatever arrived since the last successful run.
    Reprocess: bookmarks off and only raw/<table>/ingest_date=<day>/ is read, so exactly
    that day is rebuilt (silver uses dynamic partition overwrite).
    """
    if reprocess:
        return {
            "--reprocess_ingest_date": ingest_date,
            "--job-bookmark-option": "job-bookmark-disable",
        }
    return {"--job-bookmark-option": "job-bookmark-enable"}


def silver_root(access_mode: str, bucket: str, catalog: str) -> str:
    """Where Databricks reads Glue's silver output for the configured access mode."""
    if access_mode == "external_location":
        return f"s3://{bucket}/silver"
    if access_mode == "volume":
        return f"/Volumes/{catalog}/landing/silver"
    raise ValueError(f"SILVER_ACCESS_MODE must be one of {ACCESS_MODES}, got {access_mode!r}")
