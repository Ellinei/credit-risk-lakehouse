"""Drop one simulated daily arrival into the raw landing zone.

Examples:
    python -m replayer.replay --ingest-date 2026-10-01 --target local
    python -m replayer.replay --ingest-date 2026-10-02 --target s3 --bucket my-lake
    python -m replayer.replay --ingest-date 2026-10-05 --resend-updates 25 --target s3

The slice index is the number of days between --start-date and --ingest-date, so
the same date always produces the same slice (safe to rerun and to backfill).
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from replayer.slicing import (
    SliceOutOfRange,
    SourceData,
    pick_resend_clients,
    plan_slice,
    write_slice,
)


@dataclass
class ReplayResult:
    ingest_date: dt.date
    slice_index: int
    total_slices: int
    rows_per_table: dict[str, int]
    resent_clients: list[int] = field(default_factory=list)
    destination: str = ""


def replay(
    *,
    data_dir: Path,
    ingest_date: dt.date,
    start_date: dt.date,
    clients_per_slice: int,
    target: str,
    out_dir: Path | None = None,
    bucket: str | None = None,
    prefix: str = "raw",
    resend_updates: int = 0,
    cache_dir: Path | None = None,
) -> ReplayResult:
    """Write the slice for `ingest_date`. Raises SliceOutOfRange when replay is done."""
    index = (ingest_date - start_date).days
    source = SourceData(data_dir, cache_dir=cache_dir)
    client_ids = source.client_ids()
    plan = plan_slice(client_ids, index, clients_per_slice)
    resend_ids = pick_resend_clients(
        client_ids, plan, clients_per_slice, resend_updates, seed=ingest_date.toordinal()
    )
    partition = f"ingest_date={ingest_date.isoformat()}"

    if target == "local":
        if out_dir is None:
            raise ValueError("--out-dir is required for --target local")
        raw_root = Path(out_dir) / prefix
        counts = write_slice(source, plan, partition, raw_root, resend_ids)
        destination = str(raw_root)
    elif target == "s3":
        if not bucket:
            raise ValueError("--bucket (or LAKE_BUCKET) is required for --target s3")
        with tempfile.TemporaryDirectory() as tmp:
            counts = write_slice(source, plan, partition, Path(tmp), resend_ids)
            upload_partition(Path(tmp), bucket, prefix, partition)
        destination = f"s3://{bucket}/{prefix}"
    else:
        raise ValueError(f"unknown target: {target}")

    return ReplayResult(
        ingest_date=ingest_date,
        slice_index=plan.index,
        total_slices=plan.total_slices,
        rows_per_table=counts,
        resent_clients=resend_ids,
        destination=destination,
    )


def upload_partition(local_root: Path, bucket: str, prefix: str, partition: str) -> None:
    """Replace s3://bucket/prefix/<table>/<partition>/ with the local files."""
    import boto3

    # AWS_REGION from .env, like deploy_glue_job.py. Without it boto3 takes the profile's
    # region, which may not be a real S3 region (for example aws-global).
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
    table_dirs = sorted(p for p in local_root.iterdir() if p.is_dir())
    for table_dir in table_dirs:
        key_prefix = f"{prefix}/{table_dir.name}/{partition}/"
        # Clear the partition first so a rerun never leaves stale files behind.
        listing = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
        stale = [{"Key": obj["Key"]} for obj in listing.get("Contents", [])]
        if stale:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": stale})
        for file in sorted((table_dir / partition).glob("*.csv")):
            s3.upload_file(str(file), bucket, key_prefix + file.name)


def _date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--data-dir", type=Path, default=Path(os.getenv("REPLAY_DATA_DIR", "data/raw")),
                   help="folder with the Kaggle CSVs (default: data/raw)")
    p.add_argument("--cache-dir", type=Path, default=None,
                   help="optional folder for a one-off Parquet copy of the CSVs (faster slices)")
    p.add_argument("--ingest-date", type=_date, required=True)
    p.add_argument("--start-date", type=_date,
                   default=_date(os.getenv("REPLAY_START_DATE", "2026-10-01")),
                   help="date of slice 0 (default: REPLAY_START_DATE or 2026-10-01)")
    p.add_argument("--clients-per-slice", type=int,
                   default=int(os.getenv("REPLAY_CLIENTS_PER_SLICE", "2000")))
    p.add_argument("--resend-updates", type=int, default=0,
                   help="re-send N earlier clients with changed income and family status")
    p.add_argument("--target", choices=["local", "s3"], default="local")
    p.add_argument("--out-dir", type=Path, default=Path("out/landing"),
                   help="local target root (default: out/landing)")
    p.add_argument("--bucket", default=os.getenv("LAKE_BUCKET"))
    p.add_argument("--prefix", default="raw")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = replay(
            data_dir=args.data_dir,
            ingest_date=args.ingest_date,
            start_date=args.start_date,
            clients_per_slice=args.clients_per_slice,
            target=args.target,
            out_dir=args.out_dir,
            bucket=args.bucket,
            prefix=args.prefix,
            resend_updates=args.resend_updates,
            cache_dir=args.cache_dir,
        )
    except SliceOutOfRange as exc:
        print(f"Nothing to land: {exc}. The replay has covered every client.")
        return 0

    print(
        f"Landed slice {result.slice_index + 1}/{result.total_slices} "
        f"for ingest_date={result.ingest_date} in {result.destination}"
    )
    for table, rows in result.rows_per_table.items():
        print(f"  {table:<24} {rows:>9,} rows")
    if result.resent_clients:
        print(f"  re-sent {len(result.resent_clients)} clients with changed attributes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
