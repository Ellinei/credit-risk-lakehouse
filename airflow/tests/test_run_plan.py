import datetime as dt

import pytest

from credit_risk_lib.run_plan import glue_script_args, resolve_ingest_date, silver_root


def test_param_wins_over_logical_date():
    logical = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)
    assert resolve_ingest_date("2026-10-02", logical) == "2026-10-02"


def test_logical_date_is_the_default():
    logical = dt.datetime(2026, 10, 5, 0, 0, tzinfo=dt.timezone.utc)
    assert resolve_ingest_date("", logical) == "2026-10-05"


def test_manual_run_without_date_fails_loudly():
    with pytest.raises(ValueError, match="params.ingest_date"):
        resolve_ingest_date("", None)


def test_bad_param_is_rejected():
    with pytest.raises(ValueError):
        resolve_ingest_date("2026/10/02", None)


def test_normal_run_uses_bookmarks():
    assert glue_script_args("2026-10-02", reprocess=False) == {
        "--job-bookmark-option": "job-bookmark-enable"
    }


def test_reprocess_reads_one_day_without_bookmarks():
    assert glue_script_args("2026-10-02", reprocess=True) == {
        "--reprocess_ingest_date": "2026-10-02",
        "--job-bookmark-option": "job-bookmark-disable",
    }


def test_silver_root_per_access_mode():
    assert silver_root("external_location", "lake", "credit_risk") == "s3://lake/silver"
    assert silver_root("volume", "lake", "workspace") == "/Volumes/workspace/landing/silver"
    with pytest.raises(ValueError):
        silver_root("ftp", "lake", "credit_risk")
