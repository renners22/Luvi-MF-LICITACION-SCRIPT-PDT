from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date
from ..config import DEFAULT_START_DATE

FAMILIA = "despesas"
PATH = "despesas/documentos-por-favorecido"  # endpoint restrito

def run(cnpj: str, start_date: str = DEFAULT_START_DATE):
    cnpj = only_digits(cnpj)
    client = PTClient()
    params = {
        "favorecido": cnpj,        # <-- confirma nombre del parámetro
        "dataInicial": start_date  # DD/MM/AAAA o AAAA-MM-DD según API
    }
    with SessionLocal() as sess:
        for item in client.get_pages(PATH, params):
            fase = (item.get("fase") or "").lower()   # "empenho","liquidacao","pagamento"
            data = iso_date(item.get("data") or item.get("dataDocumento") or "")
            ug   = item.get("ug", {}).get("codigo", "")
            gest = item.get("gestao", {}).get("codigo", "")
            num  = item.get("numeroDocumento") or ""
            key = f"{ug}|{gest}|{num}|{fase}|{data}"
            insert_event(sess, FAMILIA, fase or "despesa", key, f"{data} 00:00:00", item)
        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--start", default=DEFAULT_START_DATE)
    args = ap.parse_args()
    run(args.cnpj, args.start)
