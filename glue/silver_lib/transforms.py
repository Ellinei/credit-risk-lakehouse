"""Bronze to silver transformations on plain Spark DataFrames.

The Glue entrypoint reads raw CSV into a DataFrame where every column is a
string, then calls `to_silver`. Everything in here is ordinary PySpark, so it
runs the same in Glue, in a local SparkSession, and in the unit tests.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from silver_lib.specs import SENTINEL_DAYS, TableSpec, column_type

SOURCE_FILE_COLUMN = "_source_file"
_INGEST_DATE_PATTERN = r"ingest_date=(\d{4}-\d{2}-\d{2})"


class SchemaError(ValueError):
    """The incoming file does not match the table contract. The job fails fast."""


@dataclass
class SilverResult:
    good: DataFrame
    quarantine: DataFrame


def standardize_column_name(name: str) -> str:
    """'SK_ID_CURR' becomes 'sk_id_curr'; ' Odd-Name (x) ' becomes 'odd_name_x'."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()


def standardize_columns(df: DataFrame) -> DataFrame:
    """Rename business columns to snake_case. Metadata columns (leading '_') are left alone."""
    renamed = [c if c.startswith("_") else standardize_column_name(c) for c in df.columns]
    duplicates = sorted({c for c in renamed if renamed.count(c) > 1})
    if duplicates:
        raise SchemaError(f"columns collide after standardizing: {duplicates}")
    return df.toDF(*renamed)


def business_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if not c.startswith("_") and c != "ingest_date"]


def validate_required_columns(df: DataFrame, spec: TableSpec) -> None:
    missing = [c for c in spec.required if c not in df.columns]
    if missing:
        raise SchemaError(f"{spec.name}: required columns missing: {missing}")


def _q(column: str) -> str:
    return f"`{column}`"


def cast_columns(df: DataFrame) -> DataFrame:
    """Cast every business column to its rule-based type.

    Adds `_cast_errors`, an array with one `bad_cast:<column>` entry for each value
    that was present in the file but could not be parsed. Empty strings become null
    and are not errors (the source uses empty fields for missing values).

    `try_cast` returns null instead of raising, whatever the ANSI mode setting is.
    """
    selected: list[Column] = []
    errors: list[Column] = []
    to_cast = set(business_columns(df))
    for column in df.columns:
        if column not in to_cast:
            selected.append(F.col(_q(column)))
            continue
        target = column_type(column)
        raw = f"nullif(trim({_q(column)}), '')"
        if target == "string":
            selected.append(F.expr(raw).alias(column))
            continue
        as_double = f"try_cast({raw} AS double)"
        if target == "double":
            value = F.expr(as_double)
            bad = F.expr(f"{raw} IS NOT NULL AND {as_double} IS NULL")
        else:
            value = F.expr(f"CAST({as_double} AS {target})")
            # "2.0" is a fine integer, "2.5" is not.
            bad = F.expr(
                f"{raw} IS NOT NULL AND ({as_double} IS NULL OR {as_double} != floor({as_double}))"
            )
        selected.append(value.alias(column))
        errors.append(F.when(bad, F.lit(f"bad_cast:{column}")))

    error_array = F.array(*errors) if errors else F.array().cast("array<string>")
    cast_errors = F.filter(error_array, lambda e: e.isNotNull()).alias("_cast_errors")
    return df.select(*selected, cast_errors)


def apply_sentinels(df: DataFrame, spec: TableSpec) -> DataFrame:
    """Null out DAYS_* = 365243 and keep a `<column>_is_sentinel` flag so nothing is lost."""
    for column in spec.sentinel_columns:
        if column not in df.columns:
            continue
        is_sentinel = F.coalesce(F.col(column) == SENTINEL_DAYS, F.lit(False))
        df = df.withColumn(f"{column}_is_sentinel", is_sentinel).withColumn(
            column, F.when(is_sentinel, F.lit(None)).otherwise(F.col(column))
        )
    return df


def clean_gender(df: DataFrame) -> DataFrame:
    """CODE_GENDER uses 'XNA' for unknown. Store it as null so accepted values are just M and F."""
    if "code_gender" not in df.columns:
        return df
    gender = F.col("code_gender")
    return df.withColumn("code_gender", F.when(gender == "XNA", F.lit(None)).otherwise(gender))


def add_ingest_date(df: DataFrame) -> DataFrame:
    """Take ingest_date from the Hive-style path, e.g. .../ingest_date=2026-10-01/part-0000.csv."""
    extracted = F.regexp_extract(F.col(SOURCE_FILE_COLUMN), _INGEST_DATE_PATTERN, 1)
    return df.withColumn(
        "ingest_date", F.to_date(F.when(extracted != "", extracted), "yyyy-MM-dd")
    )


def record_hash(columns: list[str]) -> Column:
    """SHA-256 over the business columns. Same content, same hash, on every run."""
    parts = [F.coalesce(F.col(_q(c)).cast("string"), F.lit("\\N")) for c in columns]
    return F.sha2(F.concat_ws("||", *parts), 256)


def to_silver(raw: DataFrame, spec: TableSpec, processed_at: str) -> SilverResult:
    """Turn one table's raw string DataFrame into silver rows and quarantined rows.

    `raw` must hold the CSV header columns as strings plus `_source_file`.
    `processed_at` is an ISO timestamp shared by every table in one job run.
    """
    if SOURCE_FILE_COLUMN not in raw.columns:
        raise SchemaError(f"{spec.name}: {SOURCE_FILE_COLUMN} column missing")
    df = standardize_columns(raw)
    validate_required_columns(df, spec)
    source_columns = business_columns(df)

    # Keep the original strings for quarantined rows, so a bad value can be inspected later.
    df = df.withColumn("_raw_json", F.to_json(F.struct(*[F.col(_q(c)) for c in source_columns])))
    df = cast_columns(df)
    df = clean_gender(df)
    df = apply_sentinels(df, spec)
    df = add_ingest_date(df)

    reasons = [F.when(F.col(k).isNull(), F.lit(f"missing_key:{k}")) for k in spec.not_null]
    reasons.append(F.when(F.col("ingest_date").isNull(), F.lit("missing_ingest_date")))
    df = df.withColumn(
        "_dq_reasons",
        F.concat(
            F.filter(F.array(*reasons), lambda r: r.isNotNull()),
            F.col("_cast_errors"),
        ),
    ).withColumn("_processed_at", F.to_timestamp(F.lit(processed_at)))

    flag_columns = [c for c in df.columns if c.endswith("_is_sentinel")]
    silver_columns = source_columns + flag_columns
    good = (
        df.where(F.size("_dq_reasons") == 0)
        .withColumn("record_hash", record_hash(silver_columns))
        .select(*silver_columns, "record_hash", SOURCE_FILE_COLUMN, "_processed_at", "ingest_date")
    )
    quarantine = df.where(F.size("_dq_reasons") > 0).select(
        F.lit(spec.name).alias("table_name"),
        F.col("_dq_reasons").alias("dq_reasons"),
        F.col("_raw_json").alias("raw_record"),
        SOURCE_FILE_COLUMN,
        "_processed_at",
        "ingest_date",
    )
    return SilverResult(good=good, quarantine=quarantine)
