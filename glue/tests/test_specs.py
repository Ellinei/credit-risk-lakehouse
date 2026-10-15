import pytest

from silver_lib.specs import TABLE_SPECS, column_type, matched_by_rule


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        ("sk_id_curr", "bigint"),
        ("sk_id_bureau", "bigint"),
        ("target", "int"),
        ("days_employed", "int"),
        ("days_entry_payment", "int"),
        ("flag_document_3", "int"),
        ("flag_own_car", "string"),
        ("flag_last_appl_per_contract", "string"),
        ("nflag_insured_on_approval", "int"),
        ("reg_city_not_work_city", "int"),
        ("live_region_not_work_region", "int"),
        ("region_population_relative", "double"),
        ("region_rating_client_w_city", "int"),
        ("cnt_children", "int"),
        ("cnt_fam_members", "double"),
        ("cnt_payment", "double"),
        ("months_balance", "int"),
        ("sk_dpd_def", "int"),
        ("amt_income_total", "double"),
        ("ext_source_2", "double"),
        ("apartments_avg", "double"),
        ("totalarea_mode", "double"),
        ("housetype_mode", "string"),
        ("obs_30_cnt_social_circle", "double"),
        ("rate_down_payment", "double"),
        ("name_contract_type", "string"),
        ("code_gender", "string"),
        ("status", "string"),
        ("credit_type", "string"),
        ("weekday_appr_process_start", "string"),
        ("some_new_column", "string"),
    ],
)
def test_column_type_rules(column, expected):
    assert column_type(column) == expected


def test_fallback_columns_are_reported():
    assert matched_by_rule("amt_credit")
    assert matched_by_rule("name_type_suite")
    assert not matched_by_rule("some_new_column")


def test_specs_cover_all_eight_tables():
    assert set(TABLE_SPECS) == {
        "application_train", "application_test", "bureau", "bureau_balance",
        "previous_application", "pos_cash_balance", "credit_card_balance",
        "installments_payments",
    }


def test_not_null_includes_key_client_and_parent_ids():
    assert TABLE_SPECS["application_train"].not_null == ("sk_id_curr",)
    assert TABLE_SPECS["bureau_balance"].not_null == ("sk_id_bureau", "months_balance")
    assert TABLE_SPECS["installments_payments"].not_null == ("sk_id_curr", "sk_id_prev")
    assert set(TABLE_SPECS["pos_cash_balance"].not_null) == {
        "sk_id_prev", "months_balance", "sk_id_curr"
    }
