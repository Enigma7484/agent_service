from typing import TypedDict, List, Dict, Any


class AgentState(TypedDict):
    filename: str
    raw_bytes: bytes
    file_type: str
    df: Any
    extracted_text: str
    pdf_tables: Any
    subscriptions: List[Dict[str, Any]]
    needs_review: List[Dict[str, Any]]
    detected_schema: Dict[str, Any]
    warnings: List[str]
    error: str


def build_initial_state(filename: str, raw_bytes: bytes) -> AgentState:
    return {
        "filename": filename,
        "raw_bytes": raw_bytes,
        "file_type": "",
        "df": None,
        "extracted_text": "",
        "pdf_tables": [],
        "subscriptions": [],
        "needs_review": [],
        "detected_schema": {},
        "warnings": [],
        "error": "",
    }
