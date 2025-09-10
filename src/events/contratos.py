from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, hash_signature

FAMILIA = "contratos"
LIST_PATH = "contratos/cpf-cnpj"
FIRMA_KEYS = ["situacao","valorInicial","valorAtual","dataInicioVigencia","dataFimVigencia"]

def run(cnpj: str):
    cnpj = only_digits(cnpj)
    client = PTClient()
    with SessionLocal() as sess:
        for c in client.get_pages(LIST_PATH, {"cpfCnpj": cnpj}):
            idc = str(c.get("idContrato") or c.get("id") or "")
            if not idc: continue
            occurred = (c.get("dataPublicacao") or c.get("dataAssinatura") or "1970-01-01")[:10] + " 00:00:00"
            insert_event(sess, FAMILIA, "contrato_novo", idc, occurred, c)

            sig = hash_signature(c, FIRMA_KEYS)
            insert_event(sess, FAMILIA, "contrato_update", f"{idc}|{sig}", occurred, c)
        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    args = ap.parse_args()
    run(args.cnpj)
