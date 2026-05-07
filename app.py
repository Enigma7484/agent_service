from fastapi import FastAPI, UploadFile, File, Body
from core.models import build_initial_state
from core.graph import get_compiled_graph
from core.detection import recalculate_from_rows
from core.enrichment import enrich_subscriptions

app = FastAPI()


@app.get("/")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    import os
    return {
        "ok": True,
        "openai_configured": bool(os.getenv("OPENAI_API_KEY"))
    }


@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    raw = await file.read()
    state = build_initial_state(file.filename or "", raw)
    result = get_compiled_graph().invoke(state)

    if result["error"]:
        return {
            "error": result["error"],
            "subscriptions": [],
            "needs_review": [],
            "parsed_rows": [],
            "row_count": 0,
            "detected_schema": result.get("detected_schema", {}),
            "file_type": result.get("file_type"),
            "warnings": result.get("warnings", []),
            "debug_text_preview": result.get("extracted_text", "")[:2000]
        }

    df = result["df"]
    return {
        "subscriptions": result["subscriptions"],
        "needs_review": result["needs_review"],
        "parsed_rows": df.to_dict(orient="records") if df is not None else [],
        "row_count": len(df) if df is not None else 0,
        "detected_schema": result.get("detected_schema", {}),
        "file_type": result.get("file_type"),
        "warnings": result.get("warnings", []),
        "error": ""
    }


@app.post("/analyze-rows")
async def analyze_rows(payload: dict = Body(...)):
    import pandas as pd
    from core.detection import run_detection

    rows = payload.get("rows", [])

    if not rows:
        return {
            "subscriptions": [],
            "needs_review": [],
            "parsed_rows": [],
            "row_count": 0,
            "detected_schema": {},
            "file_type": "manual",
            "warnings": [],
            "error": "No rows provided."
        }

    try:
        df = pd.DataFrame(rows)

        required = ["date", "merchant", "amount"]
        missing = [col for col in required if col not in df.columns]

        if missing:
            return {
                "subscriptions": [],
                "needs_review": [],
                "parsed_rows": rows,
                "row_count": len(rows),
                "detected_schema": {},
                "file_type": "manual",
                "warnings": [],
                "error": f"Missing required fields: {', '.join(missing)}"
            }

        subscriptions, needs_review, parsed_df, warnings = run_detection(df)

        return {
            "subscriptions": subscriptions,
            "needs_review": needs_review,
            "parsed_rows": parsed_df.to_dict(orient="records"),
            "row_count": len(parsed_df),
            "detected_schema": {
                "date": "manual",
                "merchant": "manual",
                "amount": "manual"
            },
            "file_type": "manual",
            "warnings": warnings,
            "error": ""
        }
    except Exception as e:
        return {
            "subscriptions": [],
            "needs_review": [],
            "parsed_rows": rows,
            "row_count": len(rows),
            "detected_schema": {},
            "file_type": "manual",
            "warnings": [],
            "error": f"Failed to analyze rows: {str(e)}"
        }


@app.post("/recalculate")
async def recalculate(payload: dict = Body(...)):
    return recalculate_from_rows(payload.get("parsed_rows", []))


@app.post("/enrich")
async def enrich(payload: dict = Body(...)):
    return enrich_subscriptions(payload.get("subscriptions", []))
