from functools import lru_cache
import io
import pandas as pd
from langgraph.graph import StateGraph, END
from core.models import AgentState
from core.schema import map_dataframe_to_standard_schema
from core.parsing import (
    extract_text_from_pdf,
    extract_tables_from_pdf,
    parse_pdf_transactions_from_tables,
    parse_pdf_transactions_from_text,
)
from core.detection import run_detection


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
        raw_df = pd.read_csv(io.BytesIO(state["raw_bytes"]))
        df, detected_schema, warnings = map_dataframe_to_standard_schema(raw_df)

        required = ["date", "merchant", "amount"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            state["error"] = f"Could not map required columns: {', '.join(missing)}"
            state["detected_schema"] = detected_schema
            state["warnings"].extend(warnings)
            return state

        state["df"] = df
        state["detected_schema"] = detected_schema
        state["warnings"].extend(warnings)
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

        if not df_tables.empty and len(df_tables) >= len(df_text):
            df = df_tables
            state["warnings"].append("Parsed PDF using table extraction.")
        elif not df_text.empty:
            df = df_text
            state["warnings"].append("Parsed PDF using text extraction.")
        else:
            state["error"] = (
                "PDF was read, but no transaction rows could be parsed. "
                "The statement likely needs a custom parser or OCR."
            )
            return state

        state["df"] = df.drop_duplicates().reset_index(drop=True)
    except Exception as e:
        state["error"] = f"Failed to parse PDF transactions: {str(e)}"
    return state


def standardize_schema(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    df = state["df"].copy()
    required = ["date", "merchant", "amount"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        state["error"] = f"Missing required columns: {', '.join(missing)}"
        return state

    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str)

    state["df"] = df
    return state


def detect_recurring_node(state: AgentState) -> AgentState:
    if state["error"]:
        return state

    subscriptions, needs_review, df, warnings = run_detection(state["df"])
    state["subscriptions"] = subscriptions
    state["needs_review"] = needs_review
    state["df"] = df
    state["warnings"].extend(warnings)
    return state


def finalize(state: AgentState) -> AgentState:
    return state


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
    return "detect_recurring"


@lru_cache(maxsize=1)
def get_compiled_graph():
    graph = StateGraph(AgentState)

    graph.add_node("detect_file_type", detect_file_type)
    graph.add_node("parse_csv", parse_csv)
    graph.add_node("extract_pdf", extract_pdf)
    graph.add_node("parse_pdf", parse_pdf)
    graph.add_node("standardize_schema", standardize_schema)
    graph.add_node("detect_recurring", detect_recurring_node)
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
        "detect_recurring": "detect_recurring",
        "finalize": "finalize"
    })

    graph.add_edge("detect_recurring", "finalize")
    graph.add_edge("finalize", END)

    return graph.compile()
