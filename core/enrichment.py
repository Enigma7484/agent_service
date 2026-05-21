import os
import json
import urllib.error
import urllib.request


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


def fallback_enrich(subscriptions):
    enriched = []
    for sub in subscriptions:
        item = dict(sub)
        category = simple_category_rule(sub.get("merchant_normalized", ""))
        item["category"] = category
        item["bill_type"] = simple_bill_type(category)
        item["description"] = f"Likely recurring {category.lower()} charge."
        enriched.append(item)
    return enriched


def parse_json_object(text: str):
    text = (text or "").strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start:end + 1])
    return {}


def gemini_key():
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if key:
        return key

    # Some hosts have a single AI key variable configured. Gemini API keys
    # commonly start with AIza, while OpenAI keys commonly start with sk-.
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if openai_key and not openai_key.startswith("sk-"):
        return openai_key
    return ""


def openai_key():
    key = os.getenv("OPENAI_API_KEY", "")
    return key if key.startswith("sk-") else ""


def gemini_models():
    configured = [
        model.strip()
        for model in os.getenv("GEMINI_MODEL", "").split(",")
        if model.strip()
    ]
    defaults = ["gemini-3.5-flash", "gemini-2.5-flash", "gemini-2.0-flash"]
    return list(dict.fromkeys(configured + defaults))


def enrich_with_gemini(subscriptions, api_key):
    enriched = []
    warnings = []
    used_model = None

    for sub in subscriptions:
        merchant_name = sub.get("merchant_normalized") or sub.get("merchant") or ""
        prompt = f"""
Classify this recurring charge merchant.
Merchant: {merchant_name}

Return only strict JSON with exactly these keys:
category
description

category should be short, like:
Streaming, Utilities, Telecom, Cloud Storage, Transport, Other
description should be one short sentence.
"""
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }
        category = simple_category_rule(merchant_name)
        description = f"Likely recurring {category.lower()} charge."
        last_error = None

        for model in gemini_models():
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            request = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key,
                },
                method="POST",
            )

            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    response_body = json.loads(response.read().decode("utf-8"))
                text = (
                    response_body.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                )
                parsed = parse_json_object(text)
                category = parsed.get("category", category)
                description = parsed.get("description", description)
                last_error = None
                used_model = model
                break
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", errors="replace")
                last_error = f"HTTP {e.code}: {detail[:240]}"
            except (urllib.error.URLError, KeyError, IndexError, json.JSONDecodeError) as e:
                last_error = str(e)

        if last_error:
            warnings.append(f"Gemini fallback used for merchant: {merchant_name} ({last_error})")

        item = dict(sub)
        item["category"] = category
        item["bill_type"] = simple_bill_type(category)
        item["description"] = description
        enriched.append(item)

    return enriched, warnings, used_model


def enrich_with_openai(subscriptions, api_key):
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

        response = client.responses.create(model=os.getenv("OPENAI_MODEL", "gpt-5-mini"), input=prompt)
        text = response.output_text.strip()

        category = simple_category_rule(merchant_name)
        description = f"Likely recurring {category.lower()} charge."

        try:
            parsed = parse_json_object(text)
            category = parsed.get("category", category)
            description = parsed.get("description", description)
        except Exception:
            warnings.append(f"Fallback parsing used for merchant: {merchant_name}")

        item = dict(sub)
        item["category"] = category
        item["bill_type"] = simple_bill_type(category)
        item["description"] = description
        enriched.append(item)

    return enriched, warnings, os.getenv("OPENAI_MODEL", "gpt-5-mini")


def enrich_subscriptions(subscriptions):
    if not subscriptions:
        return {"subscriptions": [], "warnings": [], "error": ""}

    gemini_api_key = gemini_key()
    openai_api_key = openai_key()

    if not gemini_api_key and not openai_api_key:
        return {
            "subscriptions": fallback_enrich(subscriptions),
            "warnings": [],
            "error": "",
            "enrichment_provider": "rules_fallback",
            "enrichment_model": "",
        }

    try:
        if gemini_api_key:
            enriched, warnings, model = enrich_with_gemini(subscriptions, gemini_api_key)
            provider = "gemini" if not warnings else "rules_fallback"
        else:
            enriched, warnings, model = enrich_with_openai(subscriptions, openai_api_key)
            provider = "openai" if not warnings else "rules_fallback"

        return {
            "subscriptions": enriched,
            "warnings": warnings,
            "error": "",
            "enrichment_provider": provider,
            "enrichment_model": model or "",
        }

    except Exception as e:
        return {
            "subscriptions": fallback_enrich(subscriptions),
            "warnings": ["LLM enrichment failed; rule-based fallback used."],
            "error": "",
            "enrichment_provider": "rules_fallback",
            "enrichment_model": "",
        }
