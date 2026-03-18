from fastapi import FastAPI, UploadFile, File
import pandas as pd
import io
import re
import pdfplumber
from datetime import datetime

app = FastAPI()

NOISE = {
    "pos", "visa", "debit", "credit", "purchase",
    "canada", "ca", "inc", "ltd", "store", "payment", "preauth"
}

def clean_merchant(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\d+", " ", s)
    s = re.sub(r"[^a-z\s]", " ", s)
    parts = [p for p in s.split() if p not in NOISE]
    return " ".join(parts).strip()

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

@app.get("/")
def health():
    return {"ok": True}

@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    raw = await file.read()
    filename = (file.filename or "").lower()

    if filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw))
        if "merchant" in df.columns:
            df["merchant_normalized"] = df["merchant"].astype(str).apply(clean_merchant)
        return {
            "file_type": "csv",
            "row_count": len(df),
            "parsed_rows": df.head(20).to_dict(orient="records")
        }

    if filename.endswith(".pdf"):
        text = extract_text_from_pdf(raw)
        df = parse_pdf_transactions_from_text(text)

        if not df.empty:
            df["merchant_normalized"] = df["merchant"].astype(str).apply(clean_merchant)

        return {
            "file_type": "pdf",
            "text_preview": text[:1000],
            "row_count": len(df),
            "parsed_rows": df.head(20).to_dict(orient="records")
        }

    return {"error": "Unsupported file type"}