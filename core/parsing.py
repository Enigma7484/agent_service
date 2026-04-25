import io
import re
import pandas as pd
import pdfplumber
from core.utils import try_parse_date


def extract_text_from_pdf(raw_bytes: bytes) -> str:
    text_parts = []
    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text(x_tolerance=2, y_tolerance=2)
            if txt:
                text_parts.append(txt)
    return "\n".join(text_parts)


def extract_tables_from_pdf(raw_bytes: bytes) -> list:
    all_tables = []
    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if tables:
                all_tables.extend(tables)
    return all_tables


def looks_like_amount(val: str):
    if val is None:
        return False
    val = str(val).strip().replace(",", "")
    return bool(re.fullmatch(r"-?\d+\.\d{2}", val))


def parse_pdf_transactions_from_tables(tables: list) -> pd.DataFrame:
    rows = []

    for table in tables:
        if not table:
            continue

        for row in table:
            if not row or len(row) < 3:
                continue

            cleaned = [str(cell).strip() if cell is not None else "" for cell in row]
            date_idx = None
            amount_idx = None

            for i, cell in enumerate(cleaned):
                if date_idx is None and try_parse_date(cell):
                    date_idx = i
                if amount_idx is None and looks_like_amount(cell):
                    amount_idx = i

            if date_idx is None or amount_idx is None or date_idx == amount_idx:
                continue

            parsed_date = try_parse_date(cleaned[date_idx])
            amount = float(cleaned[amount_idx].replace(",", ""))
            merchant_parts = [cell for i, cell in enumerate(cleaned) if i not in [date_idx, amount_idx] and cell]
            merchant = " ".join(merchant_parts).strip()

            if parsed_date and merchant:
                rows.append({
                    "date": parsed_date.strftime("%Y-%m-%d"),
                    "merchant": merchant,
                    "amount": amount,
                })

    return pd.DataFrame(rows)


def parse_pdf_transactions_from_text(text: str) -> pd.DataFrame:
    rows = []
    lines = text.splitlines()

    patterns = [
        re.compile(
            r"^\s*(?P<date>\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})\s+"
            r"(?P<merchant>.+?)\s+(?P<amount>-?\d+\.\d{2})(?:\s+(?P<currency>[A-Z]{3}))?\s*$"
        ),
        re.compile(
            r"^\s*(?P<merchant>.+?)\s+"
            r"(?P<date>\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})\s+"
            r"(?P<amount>-?\d+\.\d{2})(?:\s+(?P<currency>[A-Z]{3}))?\s*$"
        ),
    ]

    for line in lines:
        line = line.strip()
        if not line:
            continue

        for pattern in patterns:
            m = pattern.match(line)
            if not m:
                continue

            parsed_date = try_parse_date(m.group("date"))
            if not parsed_date:
                continue

            row = {
                "date": parsed_date.strftime("%Y-%m-%d"),
                "merchant": m.group("merchant").strip(),
                "amount": float(m.group("amount")),
            }

            currency = m.groupdict().get("currency")
            if currency:
                row["currency"] = currency

            rows.append(row)
            break

    return pd.DataFrame(rows)
