import csv
import datetime as dt
from pathlib import Path

import pytest

from replayer.replay import main, replay
from replayer.slicing import SliceOutOfRange, SourceData, plan_slice
from replayer.tables import SOURCE_TABLES

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "home_credit"
START = dt.date(2026, 10, 1)
CLIENTS = [100001, 100002, 100003, 100004, 100005, 100006, 100007, 100008, 100009]


def read_rows(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def land(tmp_path: Path, day: int, **kwargs):
    return replay(
        data_dir=FIXTURES,
        ingest_date=START + dt.timedelta(days=day),
        start_date=START,
        clients_per_slice=3,
        target="local",
        out_dir=tmp_path,
        **kwargs,
    )


def partition(tmp_path: Path, table: str, day: int) -> Path:
    return tmp_path / "raw" / table / f"ingest_date={START + dt.timedelta(days=day)}"


def test_client_ids_come_from_train_and_test():
    assert SourceData(FIXTURES).client_ids() == CLIENTS


def test_plan_slice_boundaries():
    first, middle, last = (plan_slice(CLIENTS, i, 3) for i in range(3))
    assert (first.lower, first.upper) == (None, 100004)
    assert (middle.lower, middle.upper) == (100004, 100007)
    assert (last.lower, last.upper) == (100007, None)
    assert {p.total_slices for p in (first, middle, last)} == {3}


def test_plan_slice_past_the_end():
    with pytest.raises(SliceOutOfRange):
        plan_slice(CLIENTS, 3, 3)


def test_every_source_row_lands_exactly_once(tmp_path):
    totals = {t.name: 0 for t in SOURCE_TABLES}
    for day in range(3):
        for table, rows in land(tmp_path, day).rows_per_table.items():
            totals[table] += rows
    for table in SOURCE_TABLES:
        assert totals[table.name] == len(read_rows(FIXTURES / table.file_name)), table.name


def test_child_rows_follow_their_client(tmp_path):
    land(tmp_path, 1)
    bureau = read_rows(partition(tmp_path, "bureau", 1) / "part-0000.csv")
    assert {r["SK_ID_CURR"] for r in bureau} == {"100004"}
    balance = read_rows(partition(tmp_path, "bureau_balance", 1) / "part-0000.csv")
    assert {r["SK_ID_BUREAU"] for r in balance} == {"5714462"}


def test_orphans_still_land(tmp_path):
    land(tmp_path, 0)
    land(tmp_path, 2)
    # bureau row for client 100010 (no application) falls in the open-ended last slice
    last_bureau = read_rows(partition(tmp_path, "bureau", 2) / "part-0000.csv")
    assert "100010" in {r["SK_ID_CURR"] for r in last_bureau}
    # bureau_balance row with no parent bureau goes to slice 9999999 % 3 == 0
    first_balance = read_rows(partition(tmp_path, "bureau_balance", 0) / "part-0000.csv")
    assert "9999999" in {r["SK_ID_BUREAU"] for r in first_balance}


def test_values_are_copied_verbatim(tmp_path):
    land(tmp_path, 2)
    rows = read_rows(partition(tmp_path, "application_train", 2) / "part-0000.csv")
    by_id = {r["SK_ID_CURR"]: r for r in rows}
    assert by_id["100008"]["AMT_INCOME_TOTAL"] == "abc"  # bad value kept for Glue to quarantine
    assert by_id["100007"]["CODE_GENDER"] == "XNA"
    assert list(rows[0].keys())[:2] == ["SK_ID_CURR", "TARGET"]


def test_empty_tables_get_no_partition(tmp_path):
    result = land(tmp_path, 0)
    assert result.rows_per_table["credit_card_balance"] == 0
    assert not partition(tmp_path, "credit_card_balance", 0).exists()


def test_resend_changes_attributes_of_earlier_clients(tmp_path):
    result = land(tmp_path, 2, resend_updates=2)
    assert len(result.resent_clients) == 2
    assert all(c < 100007 for c in result.resent_clients)

    resent = []
    for table in ("application_train", "application_test"):
        extra = partition(tmp_path, table, 2) / "part-0001.csv"
        if extra.exists():
            resent += read_rows(extra)
    assert sorted(int(r["SK_ID_CURR"]) for r in resent) == result.resent_clients

    originals = {r["SK_ID_CURR"]: r for r in read_rows(FIXTURES / "application_train.csv")}
    originals |= {r["SK_ID_CURR"]: r for r in read_rows(FIXTURES / "application_test.csv")}
    for row in resent:
        before = originals[row["SK_ID_CURR"]]
        assert row["NAME_FAMILY_STATUS"] != before["NAME_FAMILY_STATUS"]
        assert float(row["AMT_INCOME_TOTAL"]) == pytest.approx(
            round(float(before["AMT_INCOME_TOTAL"]) * 1.1, 1)
        )


def test_first_slice_has_nothing_to_resend(tmp_path):
    assert land(tmp_path, 0, resend_updates=5).resent_clients == []


def test_rerun_replaces_the_partition(tmp_path):
    land(tmp_path, 2, resend_updates=2)
    land(tmp_path, 2)
    files = {p.name for p in partition(tmp_path, "application_train", 2).iterdir()}
    assert files == {"part-0000.csv"}


def test_parquet_cache_gives_same_slice(tmp_path):
    plain = land(tmp_path / "plain", 1)
    cached = land(tmp_path / "cached", 1, cache_dir=tmp_path / "cache")
    assert plain.rows_per_table == cached.rows_per_table
    assert (tmp_path / "cache" / "bureau.parquet").exists()


def test_cli_local_run(tmp_path, capsys):
    code = main([
        "--data-dir", str(FIXTURES), "--ingest-date", "2026-10-02", "--start-date", "2026-10-01",
        "--clients-per-slice", "3", "--target", "local", "--out-dir", str(tmp_path),
    ])
    assert code == 0
    assert "Landed slice 2/3" in capsys.readouterr().out


def test_cli_past_the_end_is_not_an_error(tmp_path, capsys):
    code = main([
        "--data-dir", str(FIXTURES), "--ingest-date", "2026-12-01", "--start-date", "2026-10-01",
        "--clients-per-slice", "3", "--out-dir", str(tmp_path),
    ])
    assert code == 0
    assert "Nothing to land" in capsys.readouterr().out
