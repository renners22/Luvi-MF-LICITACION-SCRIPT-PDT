# src/events/notas_fiscais.py
from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date
from ..config import DEFAULT_START_DATE

FAMILIA = "nfe"
PATH = "notas-fiscais"

# ⬅️ Nuevo: La función `run` ahora recibe el `user_id`
def run(cnpj: str, user_id: str, codigo_orgao: str | None = None, nome_produto: str | None = None):
    """
    Requisitos mínimos do endpoint:
      - pagina (a maneja get_pages) y
      - uno de: cnpjEmitente | codigoOrgao | nomeProduto
    Usamos cnpjEmitente=cnpj por defecto; orgao/nome son opcionales.
    """
    cnpj = only_digits(cnpj)
    client = PTClient()

    params = {
        "cnpjEmitente": cnpj,
    }
    if codigo_orgao:
        params["codigoOrgao"] = str(codigo_orgao)
    if nome_produto:
        params["nomeProduto"] = nome_produto

    with SessionLocal() as sess:
        sess.tenant_cnpj = cnpj
        sess.user_id = user_id  # ⬅️ Nuevo: Asigna el `user_id` a la sesión

        for nfe in client.get_pages(PATH, params):
            # Campos típicos (defensivo)
            chave = nfe.get("chaveAcesso") or nfe.get("chave") or ""
            numero = str(nfe.get("numero") or nfe.get("numeroDocumento") or "")
            serie  = str(nfe.get("serie") or "")
            data   = iso_date(nfe.get("dataEmissao") or nfe.get("data") or "")

            # event_key: usa la chave si existe; si no, arma una con numero|serie|data
            event_key = chave if chave else f"{numero}|{serie}|{data}"
            occurred  = f"{data} 00:00:00" if data else None

            insert_event(sess, FAMILIA, "nfe_emitida", event_key, occurred, nfe)

        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--user-id", required=True) # ⬅️ Nuevo: Agrega el argumento `user-id`
    ap.add_argument("--orgao", dest="codigo_orgao", default=None)
    ap.add_argument("--produto", dest="nome_produto", default=None)
    args = ap.parse_args()
    run(args.cnpj, args.user_id, args.codigo_orgao, args.nome_produto)


# python -m src.events.notas_fiscais --cnpj 39435028000197