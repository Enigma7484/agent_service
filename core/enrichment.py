import os
import json


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


def simple_bill_type(category: str) -> str:
    c = (category or "").lower()
    if c in {"streaming", "cloud storage"}:
        return "subscription"
    if c in {"utilities", "telecom"}:
        return "utility"
    if c == "transport":
        return "transport"
    return "other_recurring"


def enrich_subscriptions(subscriptions):
    if not subscriptions:
        return {"subscriptions": [], "warnings": [], "error": ""}

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        enriched = []
        for sub in subscriptions:
            item = dict(sub)
            category = simple_category_rule(sub.get("merchant_normalized", ""))
            item["category"] = category
            item["bill_type"] = simple_bill_type(category)
            item["description"] = f"Likely recurring {category.lower()} charge."
            enriched.append(item)
        return {"subscriptions": enriched, "warnings": [], "error": ""}

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        enriched = []
        warnings = []

        for sub in subscriptions:
            merchant_name = sub.get("merchant_normalized") or sub.get("merchant") or ""
            prompt = f"""
You are classifying a recurring charge merchant.
Merchant: {merchant_name}

Return strict JSON with exactly these keys:
category
description

category should be short, like:
Streaming, Utilities, Telecom, Cloud Storage, Transport, Other
description should be one short sentence.
"""

            response = client.responses.create(model="gpt-5-mini", input=prompt)
            text = response.output_text.strip()

            category = simple_category_rule(merchant_name)
            description = f"Likely recurring {category.lower()} charge."

            try:
                parsed = json.loads(text)
                category = parsed.get("category", category)
                description = parsed.get("description", description)
            except Exception:
                warnings.append(f"Fallback parsing used for merchant: {merchant_name}")

            item = dict(sub)
            item["category"] = category
            item["bill_type"] = simple_bill_type(category)
            item["description"] = description
            enriched.append(item)

        return {"subscriptions": enriched, "warnings": warnings, "error": ""}

    except Exception as e:
        enriched = []
        for sub in subscriptions:
            item = dict(sub)
            category = simple_category_rule(sub.get("merchant_normalized", ""))
            item["category"] = category
            item["bill_type"] = simple_bill_type(category)
            item["description"] = f"Likely recurring {category.lower()} charge."
            enriched.append(item)

        return {
            "subscriptions": enriched,
            "warnings": ["LLM enrichment failed; rule-based fallback used."],
            "error": f"LLM enrichment failed, fallback rules used: {str(e)}",
        }
