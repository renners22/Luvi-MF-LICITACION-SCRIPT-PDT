# src/db.py
import os
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
import certifi
import json, hashlib, copy
from dotenv import load_dotenv


def _hash_payload(p):
    norm = json.dumps(p, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def insert_event(sess, familia, event_type, event_key, occurred_at, payload):
    user_id = getattr(sess, "user_id", None)
    
    # ✅ Lógica correcta para obtener el CNPJ
    cnpj = (
        getattr(sess, "tenant_cnpj", None)
        or payload.get("cnpj")
        or payload.get("cpfCnpj")
        or (payload.get("fornecedor") or {}).get("cnpjCpf")
        or (payload.get("favorecido") or {}).get("cpfCnpj")
        or (payload.get("estabelecimento") or {}).get("cnpjCpf")
        or ""
    )
    cnpj = "".join(ch for ch in str(cnpj) if ch.isdigit())[:14]
    
    h = _hash_payload(payload)

    prev = _events.find_one(
        {"tenant.cnpj": cnpj, "event_key": str(event_key)}, {"payload_hash": 1}
    )

    change = (
        "new" if not prev else ("update" if prev.get("payload_hash") != h else "same")
    )

    doc = {
        "tenant": {"cnpj": cnpj, "user_id": user_id}, 
        "familia": familia,
        "event_type": event_type,
        "event_key": str(event_key),
        "occurred_at": _to_dt(occurred_at),
        "payload": payload,
        "payload_hash": h,
        "updated_at": datetime.utcnow(),
    }

    _events.update_one(
        {"tenant.cnpj": cnpj, "event_key": doc["event_key"]}, {"$set": doc}, upsert=True
    )

    if change != "same":
        _db["events_log"].insert_one(
            {
                **doc,
                "change_type": change,
                "logged_at": datetime.utcnow(),
            }
        )

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.getenv("MONGO_DB", "pt_harvester")

_client = MongoClient(
    MONGO_URI,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=30000,
)
_db = _client[MONGO_DB]
_events = _db["events"]

_events.create_index(
    [("tenant.cnpj", ASCENDING), ("familia", ASCENDING), ("occurred_at", DESCENDING)]
)
_events.create_index(
    [("tenant.cnpj", ASCENDING), ("event_key", ASCENDING)], unique=False
)


class _DummySession:
    def __init__(self):
        self.tenant_cnpj = None
        self.user_id = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def commit(self):
        pass


SessionLocal = _DummySession


def _to_dt(x):
    if isinstance(x, datetime):
        return x
    if not x:
        return None
    if isinstance(x, str) and " " in x:
        x = x.replace(" ", "T")
    return datetime.fromisoformat(x)