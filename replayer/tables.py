"""Source tables of the Home Credit dataset and how each one is sliced."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceTable:
    # Lowercase name used for the raw/<name>/ prefix and the silver table.
    name: str
    # File name in the Kaggle download.
    file_name: str
    # "client": the file has SK_ID_CURR, slice on it directly.
    # "bureau": no SK_ID_CURR, reach the client through bureau.SK_ID_BUREAU.
    slice_by: str


SOURCE_TABLES = (
    SourceTable("application_train", "application_train.csv", "client"),
    SourceTable("application_test", "application_test.csv", "client"),
    SourceTable("bureau", "bureau.csv", "client"),
    SourceTable("bureau_balance", "bureau_balance.csv", "bureau"),
    SourceTable("previous_application", "previous_application.csv", "client"),
    SourceTable("pos_cash_balance", "POS_CASH_balance.csv", "client"),
    SourceTable("credit_card_balance", "credit_card_balance.csv", "client"),
    SourceTable("installments_payments", "installments_payments.csv", "client"),
)

APPLICATION_TABLES = ("application_train", "application_test")
