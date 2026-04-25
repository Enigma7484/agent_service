import re
from datetime import datetime
import pandas as pd
from core.constants import NOISE, BAD_ROW_PATTERNS, EXCLUDE_PATTERNS


def normalize_column_name(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[^a-z0-9\s_]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def find_matching_column(columns, aliases):
    normalized_map = {normalize_column_name(col): col for col in columns}
    for alias in aliases:
        alias_norm = normalize_column_name(alias)
        if alias_norm in normalized_map:
            return normalized_map[alias_norm]
    return None


def parse_money_value(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    s = s.replace(",", "")
    s = s.replace("$", "")
    s = re.sub(r"\b(CAD|USD|EUR|BDT|GBP)\b", "", s, flags=re.IGNORECASE).strip()
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return None


def try_parse_date(val: str):
    if not val:
        return None
    val = str(val).strip()
    formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y",
        "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y",
        "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            pass
    return None


def canonicalize_merchant(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("amazonprime", "amazon prime")

    if "netflix" in s:
        return "netflix"
    if "spotify" in s:
        return "spotify"
    if "google" in s and "storage" in s:
        return "google storage"
    if ("amazon" in s or "amzn" in s) and "prime" in s:
        return "amazon prime"
    if "rogers" in s:
        return "rogers"
    if "uber" in s and "trip" in s:
        return "uber trip"
    if "hydro" in s:
        return "hydro one"
    return s


def clean_merchant(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\d+", " ", s)
    s = re.sub(r"[^a-z\s]", " ", s)
    parts = [p for p in s.split() if p not in NOISE]
    s = " ".join(parts).strip()
    return canonicalize_merchant(s)


def is_noise_row(merchant: str) -> bool:
    s = (merchant or "").lower()
    return any(p in s for p in BAD_ROW_PATTERNS)


def is_excluded_row(merchant: str) -> bool:
    s = (merchant or "").lower()
    return any(p in s for p in EXCLUDE_PATTERNS)
