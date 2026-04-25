import pandas as pd
from core.constants import COLUMN_ALIASES
from core.utils import find_matching_column, parse_money_value


def map_dataframe_to_standard_schema(df: pd.DataFrame):
    df = df.copy()

    date_col = find_matching_column(df.columns, COLUMN_ALIASES["date"])
    merchant_col = find_matching_column(df.columns, COLUMN_ALIASES["merchant"])
    amount_col = find_matching_column(df.columns, COLUMN_ALIASES["amount"])
    debit_col = find_matching_column(df.columns, COLUMN_ALIASES["debit"])
    credit_col = find_matching_column(df.columns, COLUMN_ALIASES["credit"])
    currency_col = find_matching_column(df.columns, COLUMN_ALIASES["currency"])

    out = pd.DataFrame()

    if date_col:
        out["date"] = df[date_col]
    if merchant_col:
        out["merchant"] = df[merchant_col]
    if currency_col:
        out["currency"] = df[currency_col]

    warnings = []

    if amount_col:
        out["amount"] = df[amount_col].apply(parse_money_value)
    elif debit_col or credit_col:
        debit_vals = df[debit_col].apply(parse_money_value) if debit_col else 0
        credit_vals = df[credit_col].apply(parse_money_value) if credit_col else 0

        if debit_col is not None and credit_col is not None:
            out["amount"] = pd.Series(debit_vals).fillna(0) - pd.Series(credit_vals).fillna(0)
        elif debit_col is not None:
            out["amount"] = pd.Series(debit_vals)
        else:
            out["amount"] = pd.Series(credit_vals)

        out["amount"] = out["amount"].abs()
        warnings.append("Used debit/credit columns to derive amount.")

    detected_schema = {
        "date": date_col,
        "merchant": merchant_col,
        "amount": amount_col,
        "debit": debit_col,
        "credit": credit_col,
        "currency": currency_col,
    }

    return out, detected_schema, warnings
