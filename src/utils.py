import re, hashlib
from datetime import datetime

def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def iso_date(s: str) -> str:
    if not s: return datetime.utcnow().strftime("%Y-%m-%d")
    if "/" in s:
        d,m,y = s.split("/")
        return f"{y}-{m}-{d}"
    return s[:10]

def hash_signature(d: dict, keys: list[str]) -> str:
    blob = "|".join(str(d.get(k, "")) for k in keys)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]
