import pandas as pd
from datetime import timedelta
from core.utils import clean_merchant, is_noise_row, is_excluded_row


def detect_frequency(day_diffs):
    if len(day_diffs) < 2:
        return None
    avg = sum(day_diffs) / len(day_diffs)

    if 6 <= avg <= 8:
        return "weekly"
    if 13 <= avg <= 16:
        return "biweekly"
    if 25 <= avg <= 35:
        return "monthly"
    if 80 <= avg <= 100:
        return "quarterly"
    if 350 <= avg <= 380:
        return "yearly"
    return None


def score_confidence(n, amount_cv, cadence_ok):
    score = 0.0
    score += min(n / 6, 1.0) * 0.4
    score += max(0.0, 1.0 - min(amount_cv / 0.10, 1.0)) * 0.4
    score += 0.2 if cadence_ok else 0.0
    return round(min(score, 1.0), 2)


def next_expected_date(last_paid, freq):
    if freq == "weekly":
        return last_paid + timedelta(days=7)
    if freq == "biweekly":
        return last_paid + timedelta(days=14)
    if freq == "monthly":
        return last_paid + timedelta(days=30)
    if freq == "quarterly":
        return last_paid + timedelta(days=90)
    if freq == "yearly":
        return last_paid + timedelta(days=365)
    return None


def run_detection(df: pd.DataFrame):
    subscriptions = []
    needs_review = []
    warnings = []

    if df.empty:
        return subscriptions, needs_review, df, warnings

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["merchant"] = df["merchant"].astype(str)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str)

    df = df.dropna(subset=["date", "merchant", "amount"])
    df = df[~df["merchant"].apply(is_noise_row)]
    df["excluded_from_detection"] = df["merchant"].apply(is_excluded_row)
    df["merchant_normalized"] = df["merchant"].apply(clean_merchant)

    detection_df = df[~df["excluded_from_detection"]].copy()

    for merchant, g in detection_df.groupby("merchant_normalized"):
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
        next_expected = next_expected_date(last_paid, freq)

        evidence = {
            "occurrences": len(g),
            "avg_cadence_days": round(sum(day_diffs) / len(day_diffs), 1),
            "amount_variation_pct": round(amount_cv * 100, 1),
        }

        item = {
            "merchant": g["merchant"].iloc[-1],
            "merchant_normalized": merchant,
            "frequency": freq or "unknown",
            "avg_amount": round(avg_amount, 2),
            "last_paid": str(last_paid),
            "next_expected": str(next_expected) if next_expected else None,
            "confidence": conf,
            "evidence": evidence,
            "evidence_summary": f"{len(g)} charges; avg cadence ≈ {evidence['avg_cadence_days']} days; amount variation ~ {evidence['amount_variation_pct']}%",
        }

        if conf >= 0.7 and freq != "unknown":
            subscriptions.append(item)
        else:
            needs_review.append(item)

    df["date"] = df["date"].astype(str)
    return subscriptions, needs_review, df, warnings


def recalculate_from_rows(parsed_rows):
    if not parsed_rows:
        return {
            "error": "No parsed rows provided.",
            "subscriptions": [],
            "needs_review": [],
            "parsed_rows": [],
            "row_count": 0,
            "warnings": [],
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
                "row_count": len(parsed_rows),
                "warnings": [],
            }

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["merchant"] = df["merchant"].astype(str)
        df["merchant_normalized"] = df["merchant_normalized"].astype(str)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        if "currency" in df.columns:
            df["currency"] = df["currency"].astype(str)
        if "excluded_from_detection" not in df.columns:
            df["excluded_from_detection"] = False

        df = df.dropna(subset=["date", "merchant", "amount", "merchant_normalized"])

        subscriptions = []
        needs_review = []
        detection_df = df[~df["excluded_from_detection"]].copy()

        for merchant, g in detection_df.groupby("merchant_normalized"):
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
            next_expected = next_expected_date(last_paid, freq)

            evidence = {
                "occurrences": len(g),
                "avg_cadence_days": round(sum(day_diffs) / len(day_diffs), 1),
                "amount_variation_pct": round(amount_cv * 100, 1),
            }

            item = {
                "merchant": g["merchant"].iloc[-1],
                "merchant_normalized": merchant,
                "frequency": freq or "unknown",
                "avg_amount": round(avg_amount, 2),
                "last_paid": str(last_paid),
                "next_expected": str(next_expected) if next_expected else None,
                "confidence": conf,
                "evidence": evidence,
                "evidence_summary": f"{len(g)} charges; avg cadence ≈ {evidence['avg_cadence_days']} days; amount variation ~ {evidence['amount_variation_pct']}%",
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
            "warnings": [],
            "error": "",
        }

    except Exception as e:
        return {
            "error": f"Failed to recalculate: {str(e)}",
            "subscriptions": [],
            "needs_review": [],
            "parsed_rows": parsed_rows,
            "row_count": len(parsed_rows),
            "warnings": [],
        }
