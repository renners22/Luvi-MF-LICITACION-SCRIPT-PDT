from datetime import date
from ..client import PTClient
from ..db import SessionLocal, insert_event
from ..utils import only_digits, iso_date
from ..config import DEFAULT_START_DATE

FAMILIA = "despesas"
PATH = "despesas/documentos-por-favorecido"

FASES = {1: "empenho", 2: "liquidacao", 3: "pagamento"}

def _years_range(start_date: str, end_date: str | None) -> list[int]:
    def _y(d: str) -> int:
        d = d.strip()
        if "/" in d:      # DD/MM/YYYY
            return int(d.split("/")[-1])
        return int(d.split("-")[0])  # YYYY-MM-DD
    y0 = _y(start_date)
    y1 = _y(end_date) if end_date else date.today().year
    return list(range(y0, y1 + 1))

def _get_code(obj, *keys) -> str:
    for k in keys:
        v = obj.get(k)
        if v is None:
            continue
        if isinstance(v, dict):
            return str(v.get("codigo") or v.get("code") or v.get("id") or "")
        return str(v)
    return ""

def run(cnpj: str, user_id: str, start_date: str | None = None, end_date: str | None = None):
    cnpj = only_digits(cnpj)
    client = PTClient()
    
    with SessionLocal() as sess:
        sess.tenant_cnpj = cnpj
        sess.user_id = user_id
        
        years = _years_range(start_date or DEFAULT_START_DATE, end_date)
        for year in years:
            for fase_code, fase_name in FASES.items():
                # Parámetros obligatorios para la API
                params = {
                    "ano": year,
                    "fase": fase_code,
                    "cpfCnpjFavorecido": cnpj,
                    "codigoPessoa": cnpj, # ⬅️ Corregido: sin espacio al final
                }

                # (Opcional) limitar por periodo
                if start_date:
                    params["dataTransacaoInicio"] = iso_date(start_date)
                if end_date:
                    params["dataTransacaoFim"] = iso_date(end_date)

                for item in client.get_pages(PATH, params):
                    data_raw = item.get("data") or item.get("dataDocumento")
                    if not data_raw and item.get("ano"):
                        mes = str(item.get("mes", "01")).zfill(2)
                        data_raw = f"{item['ano']}-{mes}-01"
                    data = iso_date(data_raw or "")

                    ug   = _get_code(item, "ug", "codigoUG", "unidadeGestora")
                    gest = _get_code(item, "gestao", "codigoGestao")
                    num  = str(item.get("numeroDocumento") or item.get("documento") or item.get("empenho") or item.get("numero") or "")

                    key = f"{ug}|{gest}|{num}|{fase_name}|{data}"
                    occurred = f"{data} 00:00:00" if data else None

                    insert_event(sess, FAMILIA, f"despesa_{fase_name}", key, occurred, item)

        sess.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--user-id", required=True)
    ap.add_argument("--start", default=DEFAULT_START_DATE, help="YYYY-MM-DD o DD/MM/YYYY")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD o DD/MM/YYYY")
    args = ap.parse_args()
    run(args.cnpj, args.user_id, args.start, args.end)
