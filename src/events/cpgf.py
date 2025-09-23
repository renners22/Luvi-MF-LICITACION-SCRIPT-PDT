# src/events/cpgf.py
from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date
from ..config import DEFAULT_START_DATE

FAMILIA = "cpgf"
PATH = "cartoes"

def _pt_date(d: str) -> str:
    """YYYY-MM-DD -> DD/MM/YYYY (si ya viene DD/MM/YYYY, lo deja)."""
    if not d:
        return d
    d = d.strip().replace("-", "/")
    p = d.split("/")
    if len(p) == 3 and len(p[0]) == 4:
        return f"{p[2]}/{p[1]}/{p[0]}"
    return d

# ⬅️ Nuevo: Se agrega el `user_id` como parámetro a la función `run`
def run(cnpj: str, user_id: str, start_date: str | None = None, end_date: str | None = None):
    cnpj = only_digits(cnpj)
    client = PTClient()

    # ✅ Mínimo requerido: favorecido específico por CNPJ
    params = {"cpfCnpjFavorecido": cnpj}

    # (Opcional) limitar por periodo: DD/MM/AAAA
    if start_date:
        params["dataTransacaoInicio"] = _pt_date(start_date)
    if end_date:
        params["dataTransacaoFim"] = _pt_date(end_date)

    with SessionLocal() as sess:
        sess.tenant_cnpj = cnpj
        sess.user_id = user_id  # ⬅️ Nuevo: Se asigna el `user_id` a la sesión

        for tx in client.get_pages(PATH, params):
            # Fecha/hora defensiva
            data_raw = tx.get("dataTransacao") or tx.get("data") or (tx.get("dataHora") or "").split(" ")[0]
            hora_raw = tx.get("horaTransacao") or tx.get("hora") or (tx.get("dataHora", "").split(" ")[1] if " " in str(tx.get("dataHora", "")) else "00:00:00")
            data_iso = iso_date(data_raw or "")
            occurred = f"{data_iso} {hora_raw}" if data_iso else None

            # Clave de transacción
            valor = str(tx.get("valor") or tx.get("valorTransacao") or "")
            aut   = str(tx.get("autorizacao") or tx.get("codigoAutorizacao") or "")
            txid  = tx.get("idTransacao") or tx.get("id") or f"{data_iso}|{hora_raw}|{valor}|{aut}"

            insert_event(sess, FAMILIA, "cpgf_transacao", txid, occurred, tx)

        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--user-id", required=True) # ⬅️ Nuevo: Se agrega el argumento `user-id`
    ap.add_argument("--start", default=None, help="(opcional) YYYY-MM-DD o DD/MM/YYYY")
    ap.add_argument("--end", default=None, help="(opcional) YYYY-MM-DD o DD/MM/YYYY")
    args = ap.parse_args()
    # ⬅️ Nuevo: Se llama a la función `run` con el `user_id`
    run(args.cnpj, args.user_id, args.start, args.end)
# python -m src.events.cpgf --cnpj 39435028000197