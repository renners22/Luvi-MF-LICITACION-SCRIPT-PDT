# src/db.py  (REEMPLAZA el contenido actual)
import os
from datetime import datetime
from pymongo import MongoClient

from .config import DB_URL  # lo dejo por compatibilidad si lo usas en otro lado

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.getenv("MONGO_DB", "pt_harvester")

_client = MongoClient(MONGO_URI)
_db = _client[MONGO_DB]
_events = _db["events"]


# Session dummy para mantener "with SessionLocal() as sess: ... sess.commit()"
class _DummySession:
    def __init__(self):
        self.tenant_cnpj = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def commit(self):
        pass


SessionLocal = _DummySession  # 👈 deja tu patrón tal cual


def _to_dt(s: str) -> datetime:
    # esperas "YYYY-MM-DD HH:MM:SS"
    if isinstance(s, datetime):
        return s
    s = (s or "1970-01-01 00:00:00").replace("T", " ").strip()
    return datetime.fromisoformat(s)


def insert_event(sess, familia, event_type, event_key, occurred_at, payload: dict):
    """
    Inserta/upserta en pt_harvester.events con el schema:
      tenant.cnpj, familia, event_type, event_key, occurred_at(Date), payload(Object)
    Para saber el CNPJ del tenant:
      - usa sess.tenant_cnpj si está seteado en el collector
      - o intenta deducir de payload (fallback)
    """
    # 1) CNPJ del tenant
    cnpj = None
    if hasattr(sess, "tenant_cnpj") and sess.tenant_cnpj:
        cnpj = sess.tenant_cnpj
    else:
        # Fallbacks comunes – ajusta si necesitas
        cnpj = (
            payload.get("cnpj")
            or payload.get("cpfCnpj")
            or payload.get("fornecedor", {}).get("cnpjCpf")
            or payload.get("favorecido", {}).get("cpfCnpj")
            or payload.get("estabelecimento", {}).get("cnpjCpf")
            or ""
        )

    cnpj = "".join([ch for ch in str(cnpj) if ch.isdigit()])[:14]  # normaliza

    doc = {
        "tenant": {"cnpj": cnpj},
        "familia": familia,
        "event_type": event_type,
        "event_key": str(event_key),
        "occurred_at": _to_dt(occurred_at),
        "payload": payload,
    }

    # upsert por (tenant.cnpj + event_key)
    _events.update_one(
        {"tenant.cnpj": cnpj, "event_key": doc["event_key"]}, {"$set": doc}, upsert=True
    )
