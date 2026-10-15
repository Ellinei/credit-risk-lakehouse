import json

import pytest

pytest.importorskip("pyspark")

from silver_lib.specs import TABLE_SPECS  # noqa: E402
from silver_lib.transforms import (  # noqa: E402
    SchemaError,
    standardize_column_name,
    standardize_columns,
    to_silver,
)

PROCESSED_AT = "2026-10-15 01:00:00"
FILE_DAY1 = "s3://lake/raw/application_train/ingest_date=2026-10-01/part-0000.csv"


def raw_df(spark, header, rows, source_file=FILE_DAY1):
    data = [tuple(r) + (source_file,) for r in rows]
    return spark.createDataFrame(data, [*header, "_source_file"])


APP_HEADER = [
    "SK_ID_CURR", "TARGET", "NAME_CONTRACT_TYPE", "CODE_GENDER", "AMT_INCOME_TOTAL",
    "AMT_CREDIT", "DAYS_BIRTH", "DAYS_EMPLOYED", "CNT_FAM_MEMBERS",
]


def app_row(sk_id="100002", gender="M", income="202500.0", days_employed="-637", fam="1.0"):
    return [sk_id, "1", "Cash loans", gender, income, "406597.5", "-9461", days_employed, fam]


def silver_app(spark, rows, source_file=FILE_DAY1):
    raw = raw_df(spark, APP_HEADER, rows, source_file)
    return to_silver(raw, TABLE_SPECS["application_train"], PROCESSED_AT)


def test_standardize_column_name():
    assert standardize_column_name("SK_ID_CURR") == "sk_id_curr"
    assert standardize_column_name(" Odd-Name (x) ") == "odd_name_x"


def test_standardize_columns_rejects_collisions(spark):
    df = spark.createDataFrame([("1", "2")], ["A-B", "a_b"])
    with pytest.raises(SchemaError, match="collide"):
        standardize_columns(df)


def test_missing_required_column_fails_fast(spark):
    raw = raw_df(spark, ["SK_ID_CURR", "TARGET"], [["100002", "1"]])
    with pytest.raises(SchemaError, match="required columns missing"):
        to_silver(raw, TABLE_SPECS["application_train"], PROCESSED_AT)


def test_types_and_metadata(spark):
    result = silver_app(spark, [app_row()])
    row = result.good.collect()[0]
    assert row["sk_id_curr"] == 100002
    assert row["target"] == 1
    assert row["amt_income_total"] == pytest.approx(202500.0)
    assert row["cnt_fam_members"] == pytest.approx(1.0)
    assert row["days_employed"] == -637
    assert str(row["ingest_date"]) == "2026-10-01"
    assert row["_source_file"] == FILE_DAY1
    assert len(row["record_hash"]) == 64
    types = dict(result.good.dtypes)
    assert types["sk_id_curr"] == "bigint"
    assert types["days_birth"] == "int"
    assert types["ingest_date"] == "date"
    assert result.good.columns[-1] == "ingest_date"


def test_days_employed_sentinel_is_nulled_and_flagged(spark):
    result = silver_app(spark, [app_row("100006", days_employed="365243"), app_row("100002")])
    rows = {r["sk_id_curr"]: r for r in result.good.collect()}
    assert rows[100006]["days_employed"] is None
    assert rows[100006]["days_employed_is_sentinel"] is True
    assert rows[100002]["days_employed"] == -637
    assert rows[100002]["days_employed_is_sentinel"] is False


def test_xna_gender_becomes_null(spark):
    result = silver_app(spark, [app_row("100007", gender="XNA")])
    assert result.good.collect()[0]["code_gender"] is None


def test_empty_string_is_null_not_an_error(spark):
    result = silver_app(spark, [app_row(fam="")])
    assert result.quarantine.count() == 0
    assert result.good.collect()[0]["cnt_fam_members"] is None


def test_bad_cast_goes_to_quarantine_with_raw_value(spark):
    result = silver_app(spark, [app_row("100008", income="abc"), app_row("100002")])
    assert [r["sk_id_curr"] for r in result.good.collect()] == [100002]
    bad = result.quarantine.collect()
    assert len(bad) == 1
    assert bad[0]["dq_reasons"] == ["bad_cast:amt_income_total"]
    assert bad[0]["table_name"] == "application_train"
    assert json.loads(bad[0]["raw_record"])["amt_income_total"] == "abc"


def test_fractional_integer_is_a_bad_cast(spark):
    row = app_row()
    row[6] = "-9461.5"  # DAYS_BIRTH
    result = silver_app(spark, [row])
    assert result.quarantine.collect()[0]["dq_reasons"] == ["bad_cast:days_birth"]


def test_null_key_goes_to_quarantine(spark):
    result = silver_app(spark, [app_row(sk_id="")])
    assert result.good.count() == 0
    assert result.quarantine.collect()[0]["dq_reasons"] == ["missing_key:sk_id_curr"]


def test_path_without_ingest_date_is_quarantined(spark):
    result = silver_app(spark, [app_row()], source_file="s3://lake/raw/application_train/x.csv")
    assert result.quarantine.collect()[0]["dq_reasons"] == ["missing_ingest_date"]


def test_record_hash_is_stable_and_content_based(spark):
    result = silver_app(spark, [app_row("100002"), app_row("100002"), app_row("100003")])
    hashes = [r["record_hash"] for r in result.good.orderBy("sk_id_curr").collect()]
    assert hashes[0] == hashes[1]
    assert hashes[0] != hashes[2]
    other_day = silver_app(
        spark,
        [app_row("100002")],
        source_file="s3://lake/raw/application_train/ingest_date=2026-10-02/part-0000.csv",
    )
    assert other_day.good.collect()[0]["record_hash"] == hashes[0]


def test_application_test_has_no_target(spark):
    header = [c for c in APP_HEADER if c != "TARGET"]
    row = app_row()
    del row[1]
    result = to_silver(raw_df(spark, header, [row]), TABLE_SPECS["application_test"], PROCESSED_AT)
    assert "target" not in result.good.columns
    assert result.good.count() == 1


def test_previous_application_sentinels(spark):
    header = [
        "SK_ID_PREV", "SK_ID_CURR", "NAME_CONTRACT_TYPE", "NAME_CONTRACT_STATUS", "AMT_CREDIT",
        "DAYS_DECISION", "DAYS_FIRST_DRAWING", "DAYS_FIRST_DUE", "DAYS_LAST_DUE_1ST_VERSION",
        "DAYS_LAST_DUE", "DAYS_TERMINATION",
    ]
    rows = [["1038818", "100002", "Consumer loans", "Approved", "179055.0", "-606",
             "365243.0", "-565.0", "125.0", "-25.0", "-17.0"]]
    result = to_silver(
        raw_df(spark, header, rows), TABLE_SPECS["previous_application"], PROCESSED_AT
    )
    row = result.good.collect()[0]
    assert row["days_first_drawing"] is None
    assert row["days_first_drawing_is_sentinel"] is True
    assert row["days_first_due"] == -565
    assert row["days_first_due_is_sentinel"] is False


def test_installments_without_natural_key(spark):
    header = [
        "SK_ID_PREV", "SK_ID_CURR", "NUM_INSTALMENT_VERSION", "NUM_INSTALMENT_NUMBER",
        "DAYS_INSTALMENT", "DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT",
    ]
    rows = [["2396755", "100006", "1.0", "1", "-150.0", "", "10000.0", ""]]
    result = to_silver(
        raw_df(spark, header, rows), TABLE_SPECS["installments_payments"], PROCESSED_AT
    )
    row = result.good.collect()[0]
    assert row["days_entry_payment"] is None
    assert row["num_instalment_version"] == pytest.approx(1.0)
