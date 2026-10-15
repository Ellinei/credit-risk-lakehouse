"""Copy one ingest_date of Glue's silver Parquet from S3 into a Unity Catalog Volume.

This is the fallback for when Databricks Free Edition cannot read our bucket through a
Unity Catalog external location. Each table's Volume partition is cleared before the copy,
so a rerun (or a reprocess of the day) leaves exactly what is in S3.
"""

from __future__ import annotations

import io
from dataclasses import dataclass

try:
    from databricks.sdk.errors import NotFound
except ImportError:  # unit tests can run without the SDK installed

    class NotFound(Exception):
        pass


LANDING_SCHEMA = "landing"
LANDING_VOLUME = "silver"


@dataclass
class TableSync:
    table: str
    files: int
    bytes: int


def volume_root(catalog: str) -> str:
    return f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}"


def ensure_volume(workspace, catalog: str) -> None:
    """Create <catalog>.landing and the managed Volume <catalog>.landing.silver if missing."""
    from databricks.sdk.service.catalog import VolumeType

    try:
        workspace.schemas.get(f"{catalog}.{LANDING_SCHEMA}")
    except NotFound:
        workspace.schemas.create(name=LANDING_SCHEMA, catalog_name=catalog)
    try:
        workspace.volumes.read(f"{catalog}.{LANDING_SCHEMA}.{LANDING_VOLUME}")
    except NotFound:
        workspace.volumes.create(
            catalog_name=catalog,
            schema_name=LANDING_SCHEMA,
            name=LANDING_VOLUME,
            volume_type=VolumeType.MANAGED,
        )


def list_partition_files(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", []) if obj["Key"].endswith(".parquet")]
    return sorted(keys)


def clear_directory(files_api, directory: str) -> int:
    """Delete the files in a Volume directory. A missing directory is fine."""
    try:
        entries = list(files_api.list_directory_contents(directory))
    except NotFound:
        return 0
    removed = 0
    for entry in entries:
        if not entry.is_directory:
            files_api.delete(entry.path)
            removed += 1
    return removed


def sync_partition(
    s3,
    files_api,
    *,
    bucket: str,
    catalog: str,
    ingest_date: str,
    tables: list[str],
    silver_prefix: str = "silver",
) -> list[TableSync]:
    """Mirror s3://bucket/silver/<table>/ingest_date=<day>/ into the Volume for each table."""
    root = volume_root(catalog)
    results = []
    for table in tables:
        partition = f"{table}/ingest_date={ingest_date}"
        target_dir = f"{root}/{partition}"
        clear_directory(files_api, target_dir)
        copied_bytes = 0
        keys = list_partition_files(s3, bucket, f"{silver_prefix}/{partition}/")
        for key in keys:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            file_name = key.rsplit("/", 1)[-1]
            files_api.upload(f"{target_dir}/{file_name}", io.BytesIO(body), overwrite=True)
            copied_bytes += len(body)
        results.append(TableSync(table=table, files=len(keys), bytes=copied_bytes))
    return results
