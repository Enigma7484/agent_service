from fastapi import FastAPI, UploadFile, File, Body
from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Dict, Any
import pandas as pd
import os
import io
import re
import pdfplumber
from datetime import timedelta, datetime

app = FastAPI()

# ----------------------------
# Agent State
# ----------------------------
class AgentState(TypedDict):
    filename: str
    raw_bytes: bytes
    file_type: str
    df: Any
    extracted_text: str
    pdf_tables: Any
    subscriptions: List[Dict[str, Any]]
    needs_review: List[Dict[str, Any]]
    error: str

# ----------------------------
# Helpers
# ----------------------------
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

def looks_like_date(s: str):
    formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y",
        "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y",
        "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s.strip(), fmt)
        except:
            continue
    return None

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

            # Look for any row that has at least:
            # one date-like cell and one amount-like cell
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

            merchant_parts = []
            for i, cell in enumerate(cleaned):
                if i not in [date_idx, amount_idx] and cell:
                    merchant_parts.append(cell)

            merchant = " ".join(merchant_parts).strip()

            if merchant:
                rows.append({
                    "date": parsed_date.strftime("%Y-%m-%d"),
                    "merchant": merchant,
                    "amount": amount
                })

    return pd.DataFrame(rows)

def parse_pdf_transactions_from_text(text: str) -> pd.DataFrame:
    rows = []
    lines = text.splitlines()

    patterns = [
        re.compile(
            r"^\s*"
            r"(?P<date>\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})"
            r"\s+"
            r"(?P<merchant>.+?)"
            r"\s+"
            r"(?P<amount>-?\d+\.\d{2})"
            r"(?:\s+(?P<currency>[A-Z]{3}))?"
            r"\s*$"
        ),
        re.compile(
            r"^\s*"
            r"(?P<merchant>.+?)"
            r"\s+"
            r"(?P<date>\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})"
            r"\s+"
            r"(?P<amount>-?\d+\.\d{2})"
            r"(?:\s+(?P<currency>[A-Z]{3}))?"
            r"\s*$"
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

            date_raw = m.group("date").strip()
            merchant = m.group("merchant").strip()
            amount = float(m.group("amount"))
            currency = m.groupdict().get("currency")

            parsed_date = try_parse_date(date_raw)
            if not parsed_date:
                continue

            row = {
                "date": parsed_date.strftime("%Y-%m-%d"),
                "merchant": merchant,
                "amount": amount
            }

            if currency:
                row["currency"] = currency

            rows.append(row)
            break

    return pd.DataFrame(rows)

# ----------------------------
# LangGraph nodes
# ----------------------------
def detect_file_type(state: AgentState) -> AgentState:
    filename = state["filename"].lower()

    if filename.endswith(".csv"):
        state["file_type"] = "csv"
    elif filename.endswith(".pdf"):
        state["file_type"] = "pdf"
    else:
        state["error"] = "Unsupported file type. Please upload a CSV or PDF file."

    return state

def parse_csv(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    try:
        df = pd.read_csv(io.BytesIO(state["raw_bytes"]))
        state["df"] = df
    except Exception as e:
        state["error"] = f"Failed to parse CSV: {str(e)}"

    return state

def extract_pdf(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    try:
        text = extract_text_from_pdf(state["raw_bytes"])
        tables = extract_tables_from_pdf(state["raw_bytes"])

        state["extracted_text"] = text
        state["pdf_tables"] = tables

        if not text.strip() and not tables:
            state["error"] = "No extractable text or tables found in PDF. It may be scanned/image-based."
    except Exception as e:
        state["error"] = f"Failed to read PDF: {str(e)}"

    return state


def parse_pdf(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    try:
        df_tables = parse_pdf_transactions_from_tables(state.get("pdf_tables", []))
        df_text = parse_pdf_transactions_from_text(state["extracted_text"])

        if not df_tables.empty:
            df = df_tables
        elif not df_text.empty:
            df = df_text
        else:
            state["error"] = (
                "PDF was read, but no transaction rows could be parsed. "
                "The statement likely needs a custom parser or OCR."
            )
            return state

        state["df"] = df.drop_duplicates().reset_index(drop=True)
    except Exception as e:
        state["error"] = f"Failed to parse PDF transactions: {str(e)}"

    print(df.to_dict(orient="records"))
    print("PARSED PDF ROW COUNT:", len(df))

    return state

def standardize_schema(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    df = state["df"].copy()

    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str)

    required = ["date", "merchant", "amount"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        state["error"] = f"Missing required columns: {', '.join(missing)}"
        return state

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["merchant"] = df["merchant"].astype(str)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    df = df.dropna(subset=["date", "merchant", "amount"])
    state["df"] = df
    return state

def normalize_merchants(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    df = state["df"].copy()
    df["merchant_normalized"] = df["merchant"].apply(clean_merchant)
    state["df"] = df
    return state

def detect_recurring(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    df = state["df"].copy()
    subscriptions = []
    needs_review = []

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
            "evidence": f"{len(g)} charges; avg cadence ≈ {round(sum(day_diffs)/len(day_diffs), 1)} days; amount variation ~ {round(amount_cv*100, 1)}%"
        }

        if conf >= 0.7 and freq != "unknown":
            subscriptions.append(item)
        else:
            needs_review.append(item)

    state["subscriptions"] = subscriptions
    state["needs_review"] = needs_review
    return state

def finalize(state: AgentState) -> AgentState:
    return state

# ----------------------------
# Routing
# ----------------------------
def route_after_type(state: AgentState):
    if state["error"]:
        return "finalize"
    if state["file_type"] == "csv":
        return "parse_csv"
    if state["file_type"] == "pdf":
        return "extract_pdf"
    return "finalize"

def route_after_csv(state: AgentState):
    if state["error"]:
        return "finalize"
    return "standardize_schema"

def route_after_extract_pdf(state: AgentState):
    if state["error"]:
        return "finalize"
    return "parse_pdf"

def route_after_parse_pdf(state: AgentState):
    if state["error"]:
        return "finalize"
    return "standardize_schema"

def route_after_standardize(state: AgentState):
    if state["error"]:
        return "finalize"
    return "normalize_merchants"

# ----------------------------
# Build graph
# ----------------------------
graph = StateGraph(AgentState)

graph.add_node("detect_file_type", detect_file_type)
graph.add_node("parse_csv", parse_csv)
graph.add_node("extract_pdf", extract_pdf)
graph.add_node("parse_pdf", parse_pdf)
graph.add_node("standardize_schema", standardize_schema)
graph.add_node("normalize_merchants", normalize_merchants)
graph.add_node("detect_recurring", detect_recurring)
graph.add_node("finalize", finalize)

graph.set_entry_point("detect_file_type")

graph.add_conditional_edges("detect_file_type", route_after_type, {
    "parse_csv": "parse_csv",
    "extract_pdf": "extract_pdf",
    "finalize": "finalize"
})

graph.add_conditional_edges("parse_csv", route_after_csv, {
    "standardize_schema": "standardize_schema",
    "finalize": "finalize"
})

graph.add_conditional_edges("extract_pdf", route_after_extract_pdf, {
    "parse_pdf": "parse_pdf",
    "finalize": "finalize"
})

graph.add_conditional_edges("parse_pdf", route_after_parse_pdf, {
    "standardize_schema": "standardize_schema",
    "finalize": "finalize"
})

graph.add_conditional_edges("standardize_schema", route_after_standardize, {
    "normalize_merchants": "normalize_merchants",
    "finalize": "finalize"
})

graph.add_edge("normalize_merchants", "detect_recurring")
graph.add_edge("detect_recurring", "finalize")
graph.add_edge("finalize", END)

compiled_graph = graph.compile()

def simple_category_rule(name: str) -> str:
    s = (name or "").lower()

    if "netflix" in s or "spotify" in s or "prime" in s:
        return "Streaming"
    if "google" in s and "storage" in s:
        return "Cloud Storage"
    if "rogers" in s:
        return "Telecom"
    if "hydro" in s:
        return "Utilities"
    if "uber" in s:
        return "Transport"
    return "Other"

# ----------------------------
# FastAPI endpoint
# ----------------------------
@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    raw = await file.read()

    initial_state: AgentState = {
        "filename": file.filename or "",
        "raw_bytes": raw,
        "file_type": "",
        "df": None,
        "extracted_text": "",
        "pdf_tables": [],
        "subscriptions": [],
        "needs_review": [],
        "error": ""
    }

    result = compiled_graph.invoke(initial_state)

    if result["error"]:
        return {
            "error": result["error"],
            "subscriptions": [],
            "needs_review": [],
            "debug_text_preview": result.get("extracted_text", "")[:2000]
        }

    return {
        "subscriptions": result["subscriptions"],
        "needs_review": result["needs_review"],
        "parsed_rows": result["df"].to_dict(orient="records") if result["df"] is not None else [],
        "row_count": len(result["df"]) if result["df"] is not None else 0
    }

@app.post("/recalculate")
async def recalculate(payload: dict = Body(...)):
    parsed_rows = payload.get("parsed_rows", [])

    if not parsed_rows:
        return {
            "error": "No parsed rows provided.",
            "subscriptions": [],
            "needs_review": [],
            "parsed_rows": [],
            "row_count": 0
        }

    try:
        df = pd.DataFrame(parsed_rows)

        required = ["date", "merchant", "amount", "merchant_normalized"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            return {
                "error": f"Missing required columns: {', '.join(missing)}",
                "subscriptions": [],
                "needs_review": [],
                "parsed_rows": parsed_rows,
                "row_count": len(parsed_rows)
            }

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["merchant"] = df["merchant"].astype(str)
        df["merchant_normalized"] = df["merchant_normalized"].astype(str)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        if "currency" in df.columns:
            df["currency"] = df["currency"].astype(str)

        df = df.dropna(subset=["date", "merchant", "amount", "merchant_normalized"])

        subscriptions = []
        needs_review = []

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
                "evidence": f"{len(g)} charges; avg cadence ≈ {round(sum(day_diffs)/len(day_diffs), 1)} days; amount variation ~ {round(amount_cv*100, 1)}%"
            }

            if conf >= 0.7 and freq != "unknown":
                subscriptions.append(item)
            else:
                needs_review.append(item)

        parsed_rows_out = df.copy()
        parsed_rows_out["date"] = parsed_rows_out["date"].astype(str)

        return {
            "subscriptions": subscriptions,
            "needs_review": needs_review,
            "parsed_rows": parsed_rows_out.to_dict(orient="records"),
            "row_count": len(parsed_rows_out),
            "error": ""
        }

    except Exception as e:
        return {
            "error": f"Failed to recalculate: {str(e)}",
            "subscriptions": [],
            "needs_review": [],
            "parsed_rows": parsed_rows,
            "row_count": len(parsed_rows)
        }
    
@app.post("/enrich")
async def enrich(payload: dict = Body(...)):
    subscriptions = payload.get("subscriptions", [])

    if not subscriptions:
        return {"subscriptions": [], "error": ""}

    api_key = os.getenv("OPENAI_API_KEY")

    # fallback without LLM
    if not api_key:
        enriched = []
        for sub in subscriptions:
            item = dict(sub)
            item["category"] = simple_category_rule(sub.get("merchant_normalized", ""))
            item["description"] = f"Likely recurring {item['category'].lower()} charge."
            enriched.append(item)

        return {"subscriptions": enriched, "error": ""}

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        enriched = []

        for sub in subscriptions:
            merchant_name = sub.get("merchant_normalized") or sub.get("merchant") or ""

            prompt = f"""
You are classifying a subscription merchant.
Merchant: {merchant_name}

Return JSON with exactly these keys:
category
description

Rules:
- category should be short, like Streaming, Utilities, Telecom, Cloud Storage, Transport, Other
- description should be one short sentence
"""

            response = client.responses.create(
                model="gpt-5-mini",
                input=prompt
            )

            text = response.output_text.strip()

            category = simple_category_rule(merchant_name)
            description = f"Likely recurring {category.lower()} charge."

            try:
                import json
                parsed = json.loads(text)
                category = parsed.get("category", category)
                description = parsed.get("description", description)
            except:
                pass

            item = dict(sub)
            item["category"] = category
            item["description"] = description
            enriched.append(item)

        return {"subscriptions": enriched, "error": ""}

    except Exception as e:
        enriched = []
        for sub in subscriptions:
            item = dict(sub)
            item["category"] = simple_category_rule(sub.get("merchant_normalized", ""))
            item["description"] = f"Likely recurring {item['category'].lower()} charge."
            enriched.append(item)

        return {"subscriptions": enriched, "error": f"LLM enrichment failed, fallback rules used: {str(e)}"}