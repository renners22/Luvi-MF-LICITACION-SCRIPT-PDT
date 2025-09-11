# src/events/notas_fiscais.py
from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date
from ..config import DEFAULT_START_DATE  # no se usa, pero dejo la firma compatible

FAMILIA = "nfe"
PATH = "notas-fiscais"

def run(cnpj: str, codigo_orgao: str | None = None, nome_produto: str | None = None):
    """
    Requisitos mínimos del endpoint:
      - pagina (la maneja get_pages) y
      - uno de: cnpjEmitente | codigoOrgao | nomeProduto
    Usamos cnpjEmitente=cnpj por defecto; orgao/nome son opcionales.
    """
    cnpj = only_digits(cnpj)
    client = PTClient()

    params = {
        "cnpjEmitente": cnpj,   # ✅ filtro mínimo
        # get_pages añadirá 'pagina' y 'tamanho'
    }
    if codigo_orgao:
        params["codigoOrgao"] = str(codigo_orgao)
    if nome_produto:
        params["nomeProduto"] = nome_produto

    with SessionLocal() as sess:
        sess.tenant_cnpj = cnpj

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
    ap.add_argument("--orgao", dest="codigo_orgao", default=None, help="(Opcional) Código SIAFI do órgão")
    ap.add_argument("--produto", dest="nome_produto", default=None, help="(Opcional) Nome do produto")
    args = ap.parse_args()
    run(args.cnpj, args.codigo_orgao, args.nome_produto)

# python -m src.events.notas_fiscais --cnpj 39435028000197