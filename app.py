from fastapi import FastAPI, UploadFile, File
import pandas as pd
import io
import re
import pdfplumber
from datetime import datetime, timedelta

app = FastAPI()

NOISE = {
    "pos", "visa", "debit", "credit", "purchase",
    "canada", "ca", "inc", "ltd", "store", "payment", "preauth"
}

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

def try_parse_date(val: str):
    if not val:
        return None
    val = str(val).strip()
    formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y",
        "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(val, fmt)
        except:
            pass
    return None

def extract_text_from_pdf(raw_bytes: bytes) -> str:
    text_parts = []
    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text(x_tolerance=2, y_tolerance=2)
            if txt:
                text_parts.append(txt)
    return "\n".join(text_parts)

def parse_pdf_transactions_from_text(text: str) -> pd.DataFrame:
    rows = []
    lines = text.splitlines()

    pattern = re.compile(
        r"^\s*"
        r"(?P<date>\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})"
        r"\s+"
        r"(?P<merchant>.+?)"
        r"\s+"
        r"(?P<amount>-?\d+\.\d{2})"
        r"(?:\s+(?P<currency>[A-Z]{3}))?"
        r"\s*$"
    )

    for line in lines:
        line = line.strip()
        if not line:
            continue

        m = pattern.match(line)
        if not m:
            continue

        parsed_date = try_parse_date(m.group("date"))
        if not parsed_date:
            continue

        row = {
            "date": parsed_date.strftime("%Y-%m-%d"),
            "merchant": m.group("merchant").strip(),
            "amount": float(m.group("amount"))
        }

        currency = m.groupdict().get("currency")
        if currency:
            row["currency"] = currency

        rows.append(row)

    return pd.DataFrame(rows)

def detect_frequency(day_diffs):
    if len(day_diffs) < 2:
        return None
    avg = sum(day_diffs) / len(day_diffs)

    if 6 <= avg <= 8:
        return "weekly"
    if 25 <= avg <= 35:
        return "monthly"
    if 350 <= avg <= 380:
        return "yearly"
    return None

def score_confidence(n, amount_cv, cadence_ok):
    score = 0.0
    score += min(n / 6, 1.0) * 0.4
    score += max(0.0, 1.0 - min(amount_cv / 0.10, 1.0)) * 0.4
    score += 0.2 if cadence_ok else 0.0
    return round(min(score, 1.0), 2)

def run_detection(df: pd.DataFrame):
    subscriptions = []
    needs_review = []

    if df.empty:
        return subscriptions, needs_review

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["merchant"] = df["merchant"].astype(str)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["date", "merchant", "amount"])
    df["merchant_normalized"] = df["merchant"].apply(clean_merchant)

    for merchant, g in df.groupby("merchant_normalized"):
        g = g.sort_values("date")

        if len(g) < 3:
            continue

        amounts = g["amount"].astype(float).tolist()
        avg_amount = sum(amounts) / len(amounts)

        mean = avg_amount
        var = sum((x - mean) ** 2 for x in amounts) / max(len(amounts) - 1, 1)
        std = var ** 0.5
        amount_cv = (std / mean) if mean != 0 else 1.0

        dates = g["date"].dt.date.tolist()
        day_diffs = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates))]
        freq = detect_frequency(day_diffs)
        cadence_ok = freq is not None
        conf = score_confidence(len(g), amount_cv, cadence_ok)

        last_paid = dates[-1]
        if freq == "weekly":
            next_expected = last_paid + timedelta(days=7)
        elif freq == "monthly":
            next_expected = last_paid + timedelta(days=30)
        elif freq == "yearly":
            next_expected = last_paid + timedelta(days=365)
        else:
            next_expected = None

        item = {
            "merchant": g["merchant"].iloc[-1],
            "merchant_normalized": merchant,
            "frequency": freq or "unknown",
            "avg_amount": round(avg_amount, 2),
            "last_paid": str(last_paid),
            "next_expected": str(next_expected) if next_expected else None,
            "confidence": conf,
            "evidence": f"{len(g)} charges; avg cadence ≈ {round(sum(day_diffs)/len(day_diffs), 1)} days; amount variation ~ {round(amount_cv * 100, 1)}%"
        }

        if conf >= 0.7 and freq != "unknown":
            subscriptions.append(item)
        else:
            needs_review.append(item)

    df["date"] = df["date"].astype(str)
    return subscriptions, needs_review, df

@app.get("/")
def health():
    return {"ok": True}

@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    raw = await file.read()
    filename = (file.filename or "").lower()

    if filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw))
    elif filename.endswith(".pdf"):
        text = extract_text_from_pdf(raw)
        df = parse_pdf_transactions_from_text(text)
    else:
        return {"error": "Unsupported file type", "subscriptions": [], "needs_review": [], "parsed_rows": [], "row_count": 0}

    subscriptions, needs_review, parsed_df = run_detection(df)

    return {
        "subscriptions": subscriptions,
        "needs_review": needs_review,
        "parsed_rows": parsed_df.to_dict(orient="records"),
        "row_count": len(parsed_df),
        "error": ""
    }