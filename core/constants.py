COLUMN_ALIASES = {
    "date": ["date", "transaction_date", "transaction date", "posted_date", "posted date", "trans date"],
    "merchant": ["merchant", "description", "details", "payee", "transaction", "narration", "memo"],
    "amount": ["amount", "transaction_amount", "transaction amount", "value", "amt"],
    "debit": ["debit", "withdrawal", "money out", "expense"],
    "credit": ["credit", "deposit", "money in", "refund"],
    "currency": ["currency", "curr", "ccy"],
    "account": ["account", "account_name", "account name"],
    "account_type": ["account_type", "account type", "card type"],
    "transaction_type": ["type", "transaction_type", "transaction type"],
}

NOISE = {
    "pos", "visa", "debit", "credit", "purchase",
    "canada", "ca", "inc", "ltd", "store", "payment", "preauth"
}

BAD_ROW_PATTERNS = [
    "opening balance", "closing balance", "statement balance",
    "total", "payment received", "available credit",
    "previous balance", "new balance"
]

EXCLUDE_PATTERNS = [
    "transfer", "refund", "reversal", "payment thank you",
    "autopay payment", "cash advance", "interest charge"
]
