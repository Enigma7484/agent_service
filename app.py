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


@app.post("/recalculate")
async def recalculate(payload: dict = Body(...)):
    return recalculate_from_rows(payload.get("parsed_rows", []))


@app.post("/enrich")
async def enrich(payload: dict = Body(...)):
    return enrich_subscriptions(payload.get("subscriptions", []))
