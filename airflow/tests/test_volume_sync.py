import io
from dataclasses import dataclass

from credit_risk_lib import volume_sync


class FakeS3:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        keys = [k for k in self.objects if k.startswith(Prefix)]
        yield {"Contents": [{"Key": k} for k in keys]} if keys else {}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


@dataclass
class Entry:
    path: str
    is_directory: bool = False


class FakeFiles:
    def __init__(self, existing: dict[str, bytes] | None = None):
        self.files = dict(existing or {})

    def list_directory_contents(self, directory):
        found = [Entry(p) for p in self.files if p.startswith(directory + "/")]
        if not found:
            raise volume_sync.NotFound(directory)
        return found

    def delete(self, path):
        del self.files[path]

    def upload(self, path, contents, overwrite=False):
        self.files[path] = contents.read()


DAY = "2026-10-02"
ROOT = "/Volumes/credit_risk/landing/silver"


def test_copies_only_parquet_files_of_the_day():
    s3 = FakeS3({
        f"silver/bureau/ingest_date={DAY}/part-0000.snappy.parquet": b"aaa",
        f"silver/bureau/ingest_date={DAY}/_SUCCESS": b"",
        "silver/bureau/ingest_date=2026-10-01/part-0000.snappy.parquet": b"old",
    })
    files = FakeFiles()
    result = volume_sync.sync_partition(
        s3, files, bucket="lake", catalog="credit_risk", ingest_date=DAY, tables=["bureau"]
    )
    assert files.files == {f"{ROOT}/bureau/ingest_date={DAY}/part-0000.snappy.parquet": b"aaa"}
    assert (result[0].files, result[0].bytes) == (1, 3)


def test_rerun_replaces_stale_files():
    stale = f"{ROOT}/bureau/ingest_date={DAY}/part-old.parquet"
    other_day = f"{ROOT}/bureau/ingest_date=2026-10-01/part-0000.parquet"
    files = FakeFiles({stale: b"stale", other_day: b"keep"})
    s3 = FakeS3({f"silver/bureau/ingest_date={DAY}/part-new.parquet": b"new"})
    volume_sync.sync_partition(
        s3, files, bucket="lake", catalog="credit_risk", ingest_date=DAY, tables=["bureau"]
    )
    assert stale not in files.files
    assert files.files[other_day] == b"keep"
    assert files.files[f"{ROOT}/bureau/ingest_date={DAY}/part-new.parquet"] == b"new"


def test_table_without_files_that_day_is_empty_not_an_error():
    result = volume_sync.sync_partition(
        FakeS3({}), FakeFiles(), bucket="lake", catalog="credit_risk",
        ingest_date=DAY, tables=["credit_card_balance"],
    )
    assert (result[0].table, result[0].files) == ("credit_card_balance", 0)
