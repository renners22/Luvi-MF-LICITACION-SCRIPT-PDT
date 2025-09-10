from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date
from ..config import DEFAULT_START_DATE

FAMILIA = "nfe"
PATH = "notas-fiscais"

def run(cnpj: str, start_date: str = DEFAULT_START_DATE):
    cnpj = only_digits(cnpj)
    client = PTClient()
    params = {
        "fornecedor": cnpj,        # <-- confirma nombre del parámetro
        "dataInicial": start_date
    }
    with SessionLocal() as sess:
        for nfe in client.get_pages(PATH, params):
            chave = nfe.get("chaveAcesso") or nfe.get("chave")
            if not chave: continue
            data = iso_date(nfe.get("dataEmissao") or nfe.get("data"))
            insert_event(sess, FAMILIA, "nfe_nova", chave, f"{data} 00:00:00", nfe)
        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--start", default=DEFAULT_START_DATE)
    args = ap.parse_args()
    run(args.cnpj, args.start)
