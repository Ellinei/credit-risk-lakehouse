"""Assertions on the silver and quarantine output of the smoke run (runs inside the Glue container).

    python3 check_silver.py /path/to/out/smoke
"""

import sys

from pyspark.sql import SparkSession

root = sys.argv[1].rstrip("/")
spark = SparkSession.builder.master("local[1]").appName("smoke-check").getOrCreate()
failures = []


def check(condition, message):
    print(("ok    " if condition else "FAIL  ") + message)
    if not condition:
        failures.append(message)


def silver(table):
    return spark.read.parquet(f"{root}/silver/{table}")


def dates(df):
    return sorted(str(r[0]) for r in df.select("ingest_date").distinct().collect())


app = silver("application_train")
check(app.count() == 6, "application_train: 7 rows in, 1 bad cast out, 6 in silver")
check(dates(app) == ["2026-10-01", "2026-10-02", "2026-10-03"], "application_train: 3 partitions")
pensioner = app.where("sk_id_curr = 100006").first()
check(
    pensioner["days_employed"] is None and pensioner["days_employed_is_sentinel"],
    "days_employed 365243 nulled and flagged",
)
check(app.where("sk_id_curr = 100007").first()["code_gender"] is None, "CODE_GENDER XNA is null")
check(dict(app.dtypes)["sk_id_curr"] == "bigint", "sk_id_curr typed bigint")

quarantine = spark.read.parquet(f"{root}/quarantine/application_train").collect()
check(
    len(quarantine) == 1 and quarantine[0]["dq_reasons"] == ["bad_cast:amt_income_total"],
    "quarantine holds the 'abc' income row with its reason",
)
check('"abc"' in quarantine[0]["raw_record"], "quarantine keeps the raw value")

check(silver("application_test").count() == 2, "application_test: 2 rows")
bureau = silver("bureau")
check(bureau.count() == 7, "bureau: all 7 rows including the orphan client 100010")
check(silver("bureau_balance").count() == 7, "bureau_balance: all 7 rows including the orphan")

cards = silver("credit_card_balance")
check(dates(cards) == ["2026-10-02", "2026-10-03"], "credit_card_balance: no partition for day 1")

inst = silver("installments_payments")
dupes = inst.groupBy("record_hash").count().where("count > 1").count()
check(inst.count() == 7 and dupes == 1, "installments: exact duplicate rows share a record_hash")

prev = silver("previous_application")
check(
    prev.where("days_first_drawing_is_sentinel").count() == 5,
    "previous_application: five 365243 days_first_drawing values flagged",
)

if failures:
    print(f"\n{len(failures)} check(s) failed")
    sys.exit(1)
print("\nall smoke checks passed")
