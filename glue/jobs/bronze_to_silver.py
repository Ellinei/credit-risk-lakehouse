"""AWS Glue job: raw CSV (bronze) to typed, partitioned Parquet (silver).

Job parameters:
  --raw_root               s3://<bucket>/raw
  --silver_root            s3://<bucket>/silver
  --quarantine_root        s3://<bucket>/quarantine
  --tables                 optional comma separated subset (default: all eight tables)
  --reprocess_ingest_date  optional YYYY-MM-DD; read only that partition. Pair it with
                           --job-bookmark-option job-bookmark-disable.
  --max_quarantine_ratio   optional, default 0.05; fail a table above this share of bad rows

How reruns stay safe:
- Normal runs read each table root with job bookmarks on, so only files that arrived
  since the last successful run are processed. Bookmarks are keyed by
  transformation_ctx, one per table.
- Silver is written with dynamic partition overwrite: only the ingest_date
  partitions present in this batch are replaced. Reprocessing a date (bookmarks
  off, one partition read) therefore replaces that day and touches nothing else.
- job.commit() runs last. If any table fails, bookmarks do not advance and the
  next run picks the same files up again.
"""

import json
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from silver_lib.specs import TABLE_SPECS, matched_by_rule
from silver_lib.transforms import SOURCE_FILE_COLUMN, standardize_column_name, to_silver

REQUIRED_ARGS = ["JOB_NAME", "raw_root", "silver_root", "quarantine_root"]
OPTIONAL_ARGS = {"tables": "", "reprocess_ingest_date": "", "max_quarantine_ratio": "0.05"}


class QuarantineThresholdExceeded(RuntimeError):
    pass


def resolve_args(argv: list[str]) -> dict[str, str]:
    # getResolvedOptions fails on names that were not passed, so only ask for the
    # optional ones that are present.
    present = [name for name in OPTIONAL_ARGS if f"--{name}" in argv]
    return {**OPTIONAL_ARGS, **getResolvedOptions(argv, REQUIRED_ARGS + present)}


def selected_tables(tables_arg: str) -> list[str]:
    if not tables_arg:
        return list(TABLE_SPECS)
    tables = [t.strip() for t in tables_arg.split(",") if t.strip()]
    unknown = sorted(set(tables) - set(TABLE_SPECS))
    if unknown:
        raise ValueError(f"unknown tables: {unknown}")
    return tables


def read_raw(glue_context: GlueContext, table: str, raw_root: str, reprocess_date: str):
    """Read one table's raw CSV files as an all-string DataFrame (None when nothing is new)."""
    path = f"{raw_root.rstrip('/')}/{table}/"
    if reprocess_date:
        path += f"ingest_date={reprocess_date}/"
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path], "recurse": True},
        format="csv",
        format_options={"withHeader": True, "attachFilename": SOURCE_FILE_COLUMN},
        transformation_ctx=f"read_{table}",
    )
    df = dyf.resolveChoice(choice="cast:string").toDF()
    if not df.columns:
        # Bookmarks found no new files: the frame comes back with no columns at all.
        return None
    # The CSV reader may infer numbers. Silver typing is silver_lib's job, so hand it strings.
    return df.select([F.col(f"`{c}`").cast("string").alias(c) for c in df.columns])


def write_partitioned(df, path: str) -> None:
    df.write.mode("overwrite").partitionBy("ingest_date").parquet(path)


def process_table(glue_context, args, table: str, processed_at: str) -> dict:
    spec = TABLE_SPECS[table]
    raw = read_raw(glue_context, table, args["raw_root"], args["reprocess_ingest_date"])
    if raw is None:
        print(f"[{table}] no new files, skipped")
        return {"table": table, "status": "skipped"}

    fallback = [
        standardize_column_name(c)
        for c in raw.columns
        if not c.startswith("_") and not matched_by_rule(standardize_column_name(c))
    ]
    if fallback:
        print(f"[{table}] columns typed as string by fallback: {fallback}")

    raw = raw.persist()
    result = to_silver(raw, spec, processed_at)
    good_rows = result.good.count()
    bad_rows = result.quarantine.count()
    total = good_rows + bad_rows
    ratio = bad_rows / total if total else 0.0
    summary = {
        "table": table, "status": "ok", "rows_in": total,
        "silver_rows": good_rows, "quarantined_rows": bad_rows,
        "quarantine_ratio": round(ratio, 4),
        "ingest_dates": sorted(
            str(r[0]) for r in result.good.select("ingest_date").distinct().collect()
        ),
    }
    print(f"[{table}] {json.dumps(summary)}")

    if bad_rows:
        write_partitioned(result.quarantine, f"{args['quarantine_root'].rstrip('/')}/{table}")
    if ratio > float(args["max_quarantine_ratio"]):
        raise QuarantineThresholdExceeded(
            f"{table}: {bad_rows}/{total} rows quarantined, above "
            f"max_quarantine_ratio={args['max_quarantine_ratio']}. Silver not written."
        )
    write_partitioned(result.good, f"{args['silver_root'].rstrip('/')}/{table}")
    raw.unpersist()
    return summary


def main() -> None:
    args = resolve_args(sys.argv)
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    summaries = [
        process_table(glue_context, args, table, processed_at)
        for table in selected_tables(args["tables"])
    ]
    print("RUN SUMMARY " + json.dumps(summaries))
    job.commit()


if __name__ == "__main__":
    main()
