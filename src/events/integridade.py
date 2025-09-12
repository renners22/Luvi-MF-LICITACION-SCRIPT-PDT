# src/events/integridade.py
from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date

FAMILIA = "sancoes"

ENDPOINTS = {
    "ceis": {
        "param": "codigoSancionado",                 # <- ESTE es el correcto
        "event_type": "ceis_sancao",
        "date_fields": ["dataInicioSancao", "dataPublicacao", "data"],
        "id_fields": ["id", "numeroProcesso", "processo"],
    },
    "cnep": {
        "param": "codigoSancionado",                 # <- idem
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

def _codigo_do_item(item: dict) -> str:
    # intenta varias rutas; normaliza a solo dígitos
    val = (
        _pick(item, ["codigoSancionado"]) or
        _pick(item.get("sancionado", {}), ["codigoFormatado"]) or
        _pick(item.get("pessoa", {}), ["cnpjFormatado", "cpfFormatado"])
    )
    return only_digits(val or "")

def run(cnpj_cpf: str, which: list[str] | None = None):
    codigo = only_digits(cnpj_cpf)
    client = PTClient()
    which = which or list(ENDPOINTS.keys())

    with SessionLocal() as sess:
        sess.tenant_cnpj = codigo

        for path in which:
            cfg = ENDPOINTS[path]
            params = {cfg["param"]: codigo}  # ✅ filtro correcto

            for item in client.get_pages(path, params):
                # seguridad extra: si el item no corresponde al CNPJ/CPF pedido, lo saltamos
                item_code = _codigo_do_item(item)
                if item_code and item_code != codigo:
                    continue

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
