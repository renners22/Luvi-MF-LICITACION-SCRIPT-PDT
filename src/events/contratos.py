from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, hash_signature

FAMILIA = "contratos"
LIST_PATH = "contratos/cpf-cnpj"
FIRMA_KEYS = ["situacao", "valorInicial", "valorAtual", "dataInicioVigencia", "dataFimVigencia"]

# ⬅️ Actualizado: Ahora la función `run` recibe el `user_id`
def run(cnpj: str, user_id: str):
    cnpj = only_digits(cnpj)
    client = PTClient()
    with SessionLocal() as sess:
        sess.tenant_cnpj = cnpj
        sess.user_id = user_id # ⬅️ Nuevo: Se asigna el user_id a la sesión
        
        for c in client.get_pages(LIST_PATH, {"cpfCnpj": cnpj}):
            idc = str(c.get("idContrato") or c.get("id") or "")
            if not idc:
                continue
            occurred = (c.get("dataPublicacao") or c.get("dataAssinatura") or "1970-01-01")[:10] + " 00:00:00"
            insert_event(sess, FAMILIA, "contrato_novo", idc, occurred, c)

            sig = hash_signature(c, FIRMA_KEYS)
            insert_event(sess, FAMILIA, "contrato_update", f"{idc}|{sig}", occurred, c)
        sess.commit()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--user-id", required=True)
    args = ap.parse_args()
    run(args.cnpj, args.user_id)
