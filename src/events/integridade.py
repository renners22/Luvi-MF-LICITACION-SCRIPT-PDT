# src/events/integridade.py
from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date

FAMILIA = "sancoes"

# Ambos usan el mismo nombre de parámetro p/ CNPJ/CPF
ENDPOINTS = {
    "ceis": {
        "param": "codigoSancionado",
        "event_type": "ceis_sancao",
        "date_fields": ["dataInicioSancao", "dataPublicacao", "data"],
        "id_fields": ["id", "numeroProcesso", "processo"],
    },
    "cnep": {
        "param": "codigoSancionado",
        "event_type": "cnep_sancao",
        "date_fields": ["dataInicioSancao", "dataPublicacao", "data"],
        "id_fields": ["id", "numeroProcesso", "processo"],
    },
}

def _pick(d: dict, keys: list[str]) -> str:
    for k in keys:
        v = d.get(k)
        if v:
            return str(v)
    return ""

def _pick_date(d: dict, keys: list[str]) -> str:
    for k in keys:
        v = d.get(k)
        if v:
            return iso_date(str(v))
    return "1970-01-01"

def run(cnpj_cpf: str, which: list[str] | None = None):
    codigo = only_digits(cnpj_cpf)
    client = PTClient()
    which = which or list(ENDPOINTS.keys())

    with SessionLocal() as sess:
        sess.tenant_cnpj = codigo  # guarda como tenant.cnpj (válido también si es CPF)

        for path in which:
            cfg = ENDPOINTS[path]
            params = {cfg["param"]: codigo}  # ✅ filtro correcto por CNPJ/CPF

            for item in client.get_pages(path, params):
                d = _pick_date(item, cfg["date_fields"])
                occurred = f"{d} 00:00:00"

                rid = _pick(item, cfg["id_fields"])
                tipo = str(item.get("tipoSancao") or item.get("situacao") or "").strip()
                key = rid if rid else f"{path}|{codigo}|{tipo}|{d}"

                insert_event(sess, FAMILIA, cfg["event_type"], key, occurred, item)

        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True, help="CNPJ o CPF (solo dígitos)")
    ap.add_argument("--only", nargs="*", default=None, help="ej: --only ceis cnep")
    args = ap.parse_args()
    run(args.cnpj, args.only)

# CEIS + CNEP para ese CNPJ/CPF
# python -m src.events.integridade --cnpj 39435028000197

# Solo CEIS
# python -m src.events.integridade --cnpj 39435028000197 --only ceis