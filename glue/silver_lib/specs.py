"""Silver layer contracts: keys, required columns, sentinels and column typing rules.

Column names here are the standardized snake_case names (SK_ID_CURR becomes sk_id_curr).

Typing is rule based instead of a 300-column hand-written schema. The rules are
checked in order and the first match wins:

1. string: `name_*`, `code_*`, and the explicit STRING_COLUMNS set
2. bigint: `sk_id_*`
3. int:    `days_*`, `flag_*`, `nflag_*`, `reg_*`, `live_*`, `hour_*`, and INT_COLUMNS
4. double: `amt_*`, `cnt_*`, `ext_source_*`, `rate_*`, `obs_*`, `def_*`,
           `*_avg`, `*_mode`, `*_medi`, and DOUBLE_COLUMNS
5. string: anything else, so an unexpected column is never quarantined for its type

Integers are parsed through double, so "2.0" is accepted and "2.5" is a bad cast.
"""

from __future__ import annotations

from dataclasses import dataclass

# DAYS_* columns use 365243 (about 1000 years) to mean "not applicable",
# for example DAYS_EMPLOYED for pensioners and unemployed applicants.
SENTINEL_DAYS = 365243

STRING_COLUMNS = frozenset({
    "flag_own_car", "flag_own_realty", "flag_last_appl_per_contract",
    "occupation_type", "organization_type", "weekday_appr_process_start",
    "fondkapremont_mode", "housetype_mode", "wallsmaterial_mode", "emergencystate_mode",
    "credit_active", "credit_currency", "credit_type", "status",
    "channel_type", "product_combination",
})

INT_COLUMNS = frozenset({
    "target", "cnt_children", "months_balance", "sk_dpd", "sk_dpd_def",
    "credit_day_overdue", "cnt_credit_prolong", "num_instalment_number",
    "region_rating_client", "region_rating_client_w_city", "sellerplace_area",
})

DOUBLE_COLUMNS = frozenset({
    "region_population_relative", "own_car_age", "num_instalment_version",
})

STRING_PREFIXES = ("name_", "code_")
INT_PREFIXES = ("days_", "flag_", "nflag_", "reg_", "live_", "hour_")
DOUBLE_PREFIXES = ("amt_", "cnt_", "ext_source_", "rate_", "obs_", "def_")
DOUBLE_SUFFIXES = ("_avg", "_mode", "_medi")


def column_type(column: str) -> str:
    """Spark SQL type for a standardized column name."""
    if column in STRING_COLUMNS or column.startswith(STRING_PREFIXES):
        return "string"
    if column.startswith("sk_id_"):
        return "bigint"
    if column in INT_COLUMNS or column.startswith(INT_PREFIXES):
        return "int"
    if (
        column in DOUBLE_COLUMNS
        or column.startswith(DOUBLE_PREFIXES)
        or column.endswith(DOUBLE_SUFFIXES)
    ):
        return "double"
    return "string"


def matched_by_rule(column: str) -> bool:
    """False when a column only got the fallback string type (worth a log line)."""
    return (
        column in STRING_COLUMNS
        or column.startswith(STRING_PREFIXES)
        or column_type(column) != "string"
    )


@dataclass(frozen=True)
class TableSpec:
    name: str
    # Natural key. Empty for installments_payments, which has none (silver adds record_hash).
    keys: tuple[str, ...]
    # Columns that must exist in every file, or the job fails before writing anything.
    required: tuple[str, ...]
    # Columns where SENTINEL_DAYS is nulled and flagged.
    sentinel_columns: tuple[str, ...] = ()

    @property
    def not_null(self) -> tuple[str, ...]:
        """Rows missing any of these go to quarantine: the key plus the client id."""
        extra = ("sk_id_curr",) if "sk_id_curr" in self.required else ()
        return tuple(dict.fromkeys(self.keys + extra + self._parent_ids()))

    def _parent_ids(self) -> tuple[str, ...]:
        return tuple(c for c in ("sk_id_prev", "sk_id_bureau") if c in self.required)


_APPLICATION_COLUMNS = (
    "sk_id_curr", "name_contract_type", "code_gender", "amt_income_total",
    "amt_credit", "days_birth", "days_employed",
)

TABLE_SPECS: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        TableSpec(
            "application_train",
            keys=("sk_id_curr",),
            required=_APPLICATION_COLUMNS + ("target",),
            sentinel_columns=("days_employed",),
        ),
        TableSpec(
            "application_test",
            keys=("sk_id_curr",),
            required=_APPLICATION_COLUMNS,
            sentinel_columns=("days_employed",),
        ),
        TableSpec(
            "bureau",
            keys=("sk_id_bureau",),
            required=("sk_id_curr", "sk_id_bureau", "credit_active", "credit_type",
                      "days_credit", "amt_credit_sum"),
        ),
        TableSpec(
            "bureau_balance",
            keys=("sk_id_bureau", "months_balance"),
            required=("sk_id_bureau", "months_balance", "status"),
        ),
        TableSpec(
            "previous_application",
            keys=("sk_id_prev",),
            required=("sk_id_prev", "sk_id_curr", "name_contract_type",
                      "name_contract_status", "amt_credit", "days_decision"),
            sentinel_columns=("days_first_drawing", "days_first_due",
                              "days_last_due_1st_version", "days_last_due", "days_termination"),
        ),
        TableSpec(
            "pos_cash_balance",
            keys=("sk_id_prev", "months_balance"),
            required=("sk_id_prev", "sk_id_curr", "months_balance", "name_contract_status",
                      "sk_dpd", "sk_dpd_def"),
        ),
        TableSpec(
            "credit_card_balance",
            keys=("sk_id_prev", "months_balance"),
            required=("sk_id_prev", "sk_id_curr", "months_balance", "amt_balance",
                      "sk_dpd", "sk_dpd_def"),
        ),
        TableSpec(
            "installments_payments",
            keys=(),
            required=("sk_id_prev", "sk_id_curr", "num_instalment_number", "days_instalment",
                      "days_entry_payment", "amt_instalment", "amt_payment"),
        ),
    )
}
