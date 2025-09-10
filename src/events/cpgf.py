from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date
from ..config import DEFAULT_START_DATE

FAMILIA = "cpgf"
PATH = "cartoes"

def run(cnpj: str, start_date: str = DEFAULT_START_DATE):
    cnpj = only_digits(cnpj)
    client = PTClient()
    params = {
        "estabelecimento": cnpj,   # <-- confirma nombre del parámetro
        "dataInicial": start_date
    }
    with SessionLocal() as sess:
        for tx in client.get_pages(PATH, params):
            txid = tx.get("idTransacao")
            if not txid:
                data = iso_date(tx.get("dataHora") or tx.get("data"))
                val  = str(tx.get("valor") or "")
                aut  = str(tx.get("autorizacao") or "")
                txid = f"{data}|{val}|{aut}"
            occurred = iso_date(tx.get("dataHora") or tx.get("data")) + " 00:00:00"
            insert_event(sess, FAMILIA, "cpgf_tx", txid, occurred, tx)
        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--start", default=DEFAULT_START_DATE)
    args = ap.parse_args()
    run(args.cnpj, args.start)
