# src/db.py
import os
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
import certifi
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB  = os.getenv("MONGO_DB", "pt_harvester")

_client = MongoClient(
    MONGO_URI,
    tlsCAFile=certifi.where(),       # <- CA bundle moderno para Atlas
    serverSelectionTimeoutMS=30000,  # timeout claro
)
_db = _client[MONGO_DB]
_events = _db["events"]

# Índices (idempotentes)
_events.create_index([("tenant.cnpj", ASCENDING), ("familia", ASCENDING), ("occurred_at", DESCENDING)])
_events.create_index([("tenant.cnpj", ASCENDING), ("event_key", ASCENDING)], unique=False)

# Mantiene tu patrón with SessionLocal() as sess: ... sess.commit()
class _DummySession:
    def __init__(self): self.tenant_cnpj = None
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): pass
    def commit(self): pass

SessionLocal = _DummySession

def _to_dt(x):
    if isinstance(x, datetime): return x
    if not x: return None
    if isinstance(x, str) and " " in x: x = x.replace(" ", "T")
    return datetime.fromisoformat(x)

def insert_event(sess, familia, event_type, event_key, occurred_at, payload: dict):
    # CNPJ del tenant: usa sess.tenant_cnpj si existe; si no, trata de deducir
    cnpj = getattr(sess, "tenant_cnpj", None) or \
           payload.get("cnpj") or payload.get("cpfCnpj") or \
           (payload.get("fornecedor") or {}).get("cnpjCpf") or \
           (payload.get("favorecido") or {}).get("cpfCnpj") or \
           (payload.get("estabelecimento") or {}).get("cnpjCpf") or ""
    cnpj = "".join(ch for ch in str(cnpj) if ch.isdigit())[:14]

    doc = {
        "tenant": {"cnpj": cnpj},
        "familia": familia,
        "event_type": event_type,
        "event_key": str(event_key),
        "occurred_at": _to_dt(occurred_at),
        "payload": payload,
    }
    _events.update_one(
        {"tenant.cnpj": cnpj, "event_key": doc["event_key"]},
        {"$set": doc},
        upsert=True
    )

# Si prefieres seguir usando tu nombre nuevo:
def insert_event_mongo(tenant_cnpj, familia, event_type, event_key, occurred_at, payload):
    doc = {
        "tenant": {"cnpj": tenant_cnpj},
        "familia": familia,
        "event_type": event_type,
        "event_key": str(event_key),
        "occurred_at": _to_dt(occurred_at),
        "payload": payload,
    }
    _events.update_one(
        {"tenant.cnpj": tenant_cnpj, "event_key": str(event_key)},
        {"$set": doc},
        upsert=True
    )
