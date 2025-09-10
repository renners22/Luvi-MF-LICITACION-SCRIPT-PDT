from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits

FAMILIA = "sancoes"
ENDPOINTS = {"CEIS": "ceis", "CNEP": "cnep"}  # agrega "CEAF" si usas PF

def run(cnpj: str):
    cnpj = only_digits(cnpj)
    client = PTClient()
    with SessionLocal() as sess:
        for nome, path in ENDPOINTS.items():
            for item in client.get_pages(path, {"cnpj": cnpj}):  # <-- confirma 'cnpj' param
                data = (item.get("dataPublicacao") or item.get("data") or "1970-01-01")[:10] + " 00:00:00"
                key = f"{nome}|{cnpj}|{data}"
                insert_event(sess, FAMILIA, f"{nome.lower()}_evento", key, data, item)
        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    args = ap.parse_args()
    run(args.cnpj)
