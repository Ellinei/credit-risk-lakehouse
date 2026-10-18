"""Upload a local Glue run's silver Parquet into the Unity Catalog Volume.

    python scripts/push_local_silver.py --silver-dir out/real/silver --ingest-date 2026-10-01

For testing dbt against Databricks before anything runs on AWS: spark-submit the Glue
job locally, push its output with this script, then run load_silver and dbt build.
It calls the same volume_sync.sync_partition as the Airflow DAG, with the local
directory standing in for the bucket, so the Volume layout is exactly what the
DAG writes.

DATABRICKS_HOST and DATABRICKS_TOKEN come from the environment (export them from .env).
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "airflow" / "dags"))

from credit_risk_lib import volume_sync  # noqa: E402


class LocalS3:
    """The two S3 client calls sync_partition makes, served from a local directory.

    The bucket name is the local directory that holds the silver prefix.
    """

    def get_paginator(self, name: str) -> LocalS3:
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket: str, Prefix: str):
        root = Path(Bucket)
        keys = sorted(
            p.relative_to(root).as_posix() for p in root.joinpath(Prefix).rglob("*") if p.is_file()
        )
        yield {"Contents": [{"Key": k} for k in keys]} if keys else {}

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": io.BytesIO((Path(Bucket) / Key).read_bytes())}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--silver-dir", type=Path, required=True,
                   help="Local silver root, with <table>/ingest_date=<day>/ under it")
    p.add_argument("--ingest-date", required=True)
    p.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "credit_risk"))
    p.add_argument("--tables", nargs="*",
                   help="Default: every table directory under --silver-dir")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    silver_dir = args.silver_dir.resolve()
    if not silver_dir.is_dir():
        print(f"No such directory: {silver_dir}", file=sys.stderr)
        return 2
    tables = args.tables or sorted(p.name for p in silver_dir.iterdir() if p.is_dir())

    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    volume_sync.ensure_volume(workspace, args.catalog)
    synced = volume_sync.sync_partition(
        LocalS3(), workspace.files,
        bucket=str(silver_dir.parent), catalog=args.catalog, ingest_date=args.ingest_date,
        tables=tables, silver_prefix=silver_dir.name,
    )
    for s in synced:
        print(f"{s.table:<24} {s.files:>3} files {s.bytes / 1e6:>8.1f} MB")
    root = volume_sync.volume_root(args.catalog)
    print(f"Uploaded to {root}/<table>/ingest_date={args.ingest_date}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
