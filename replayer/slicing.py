"""Cut one simulated daily arrival out of the Home Credit CSVs.

A slice is a contiguous SK_ID_CURR range. The range boundaries come from the
sorted client ids in application_train + application_test, so every slice holds
`clients_per_slice` applications. Child tables are cut with the same range, which
keeps a slice consistent: a client's bureau, previous application and payment
rows land on the same day as the application.

Every source row lands in exactly one slice, including rows that would be
dropped by an exact client-list filter:
- child rows whose SK_ID_CURR has no application (orphans) still fall inside
  some range, so dbt can count them later;
- rows with a missing or non-numeric key go to slice 0, so Glue can quarantine them;
- bureau_balance rows with no parent bureau row are spread over the slices by
  SK_ID_BUREAU modulo the number of slices.
"""

from __future__ import annotations

import math
import random
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb

from replayer.tables import APPLICATION_TABLES, SOURCE_TABLES, SourceTable


class SliceOutOfRange(Exception):
    """The requested slice is past the last client, so the replay is finished."""


@dataclass(frozen=True)
class SlicePlan:
    index: int
    total_slices: int
    lower: int | None  # inclusive; None means no lower bound
    upper: int | None  # exclusive; None means no upper bound
    client_count: int


def plan_slice(client_ids: list[int], index: int, clients_per_slice: int) -> SlicePlan:
    """Work out the SK_ID_CURR range of slice `index` from the sorted client ids."""
    if clients_per_slice <= 0:
        raise ValueError("clients_per_slice must be positive")
    total = math.ceil(len(client_ids) / clients_per_slice)
    if index < 0 or index >= total:
        raise SliceOutOfRange(f"slice {index} requested, only {total} slices exist")
    start = index * clients_per_slice
    end = start + clients_per_slice
    return SlicePlan(
        index=index,
        total_slices=total,
        # The first and last slices are open ended so that keys below the first
        # client or above the last client are still delivered somewhere.
        lower=None if index == 0 else client_ids[start],
        upper=None if index == total - 1 else client_ids[end],
        client_count=len(client_ids[start:end]),
    )


def pick_resend_clients(
    client_ids: list[int], plan: SlicePlan, clients_per_slice: int, n: int, seed: int
) -> list[int]:
    """Choose `n` clients from earlier slices to re-send with changed attributes."""
    already_landed = client_ids[: plan.index * clients_per_slice]
    if n <= 0 or not already_landed:
        return []
    return sorted(random.Random(seed).sample(already_landed, min(n, len(already_landed))))


def _sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def _key_in_range(column: str, plan: SlicePlan) -> str:
    key = f"TRY_CAST({column} AS BIGINT)"
    conditions = []
    if plan.lower is not None:
        conditions.append(f"{key} >= {plan.lower}")
    if plan.upper is not None:
        conditions.append(f"{key} < {plan.upper}")
    in_range = " AND ".join(conditions) or "TRUE"
    if plan.index == 0:
        return f"(({in_range}) OR {key} IS NULL)"
    return f"({in_range})"


class SourceData:
    """DuckDB views over the source CSVs, with every column read as text.

    Reading as text keeps values exactly as they are in the source files, so the
    raw zone receives the same strings an upstream system would send. Typing is
    Glue's job.
    """

    def __init__(self, data_dir: Path, cache_dir: Path | None = None):
        self.data_dir = Path(data_dir)
        self.con = duckdb.connect()
        for table in SOURCE_TABLES:
            csv_path = self.data_dir / table.file_name
            if not csv_path.exists():
                raise FileNotFoundError(f"missing source file: {csv_path}")
            source = (
                f"read_csv('{_sql_path(csv_path)}', header = true, all_varchar = true)"
            )
            if cache_dir is not None:
                source = self._cached(source, Path(cache_dir) / f"{table.name}.parquet")
            self.con.execute(f"CREATE OR REPLACE VIEW src_{table.name} AS SELECT * FROM {source}")

    def _cached(self, csv_source: str, parquet_path: Path) -> str:
        # Scanning a 700 MB CSV for every slice is slow. Convert each file to
        # Parquet once (still all text) and scan that instead.
        if not parquet_path.exists():
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            self.con.execute(
                f"COPY (SELECT * FROM {csv_source}) TO '{_sql_path(parquet_path)}' (FORMAT parquet)"
            )
        return f"read_parquet('{_sql_path(parquet_path)}')"

    def client_ids(self) -> list[int]:
        rows = self.con.execute(
            "SELECT DISTINCT TRY_CAST(SK_ID_CURR AS BIGINT) AS id FROM ("
            " SELECT SK_ID_CURR FROM src_application_train"
            " UNION ALL SELECT SK_ID_CURR FROM src_application_test"
            ") WHERE id IS NOT NULL ORDER BY id"
        ).fetchall()
        return [row[0] for row in rows]

    def slice_query(self, table: SourceTable, plan: SlicePlan) -> str:
        if table.slice_by == "client":
            return f"SELECT * FROM src_{table.name} WHERE {_key_in_range('SK_ID_CURR', plan)}"
        bureau_key = "TRY_CAST(bb.SK_ID_BUREAU AS BIGINT)"
        orphan_rule = f"{bureau_key} % {plan.total_slices} = {plan.index}"
        if plan.index == 0:
            orphan_rule = f"({orphan_rule} OR {bureau_key} IS NULL)"
        return (
            "SELECT bb.* FROM src_bureau_balance AS bb "
            "LEFT JOIN (SELECT SK_ID_BUREAU, min(SK_ID_CURR) AS SK_ID_CURR "
            "           FROM src_bureau GROUP BY SK_ID_BUREAU) AS b "
            "  ON bb.SK_ID_BUREAU = b.SK_ID_BUREAU "
            f"WHERE (b.SK_ID_BUREAU IS NOT NULL AND {_key_in_range('b.SK_ID_CURR', plan)}) "
            f"   OR (b.SK_ID_BUREAU IS NULL AND {orphan_rule})"
        )

    def resend_query(self, table_name: str, client_ids: list[int]) -> str:
        # Synthetic attribute changes so `dbt snapshot` has SCD2 history to record.
        ids = ", ".join(str(i) for i in client_ids)
        return (
            f"SELECT * REPLACE ("
            "  CAST(ROUND(TRY_CAST(AMT_INCOME_TOTAL AS DOUBLE) * 1.1, 1) AS VARCHAR)"
            "    AS AMT_INCOME_TOTAL,"
            "  CASE WHEN NAME_FAMILY_STATUS = 'Married' THEN 'Separated' ELSE 'Married' END"
            "    AS NAME_FAMILY_STATUS"
            f") FROM src_{table_name} WHERE TRY_CAST(SK_ID_CURR AS BIGINT) IN ({ids})"
        )

    def export_csv(self, query: str, path: Path) -> int:
        """Write the query result to `path` as CSV. Writes nothing for zero rows."""
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE _export AS {query}")
        rows = self.con.execute("SELECT count(*) FROM _export").fetchone()[0]
        if rows:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.con.execute(f"COPY _export TO '{_sql_path(path)}' (HEADER, DELIMITER ',')")
        return rows


def write_slice(
    source: SourceData,
    plan: SlicePlan,
    partition: str,
    raw_root: Path,
    resend_ids: list[int],
) -> dict[str, int]:
    """Write one slice under raw_root/<table>/<partition>/ and return rows per table.

    Each partition directory is cleared first, so replaying the same date twice
    gives the same files. Tables with no rows in this slice get no directory,
    which matches how Glue skips a table when bookmarks find no new files.
    """
    counts: dict[str, int] = {}
    for table in SOURCE_TABLES:
        part_dir = raw_root / table.name / partition
        if part_dir.exists():
            shutil.rmtree(part_dir)
        rows = source.export_csv(source.slice_query(table, plan), part_dir / "part-0000.csv")
        if resend_ids and table.name in APPLICATION_TABLES:
            rows += source.export_csv(
                source.resend_query(table.name, resend_ids), part_dir / "part-0001.csv"
            )
        counts[table.name] = rows
    return counts
