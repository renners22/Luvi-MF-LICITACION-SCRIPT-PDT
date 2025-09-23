# src/events/despesas.py
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

# ⬅️ Nuevo: Se agrega el `user_id` como parámetro a la función `run`
def run(cnpj: str, user_id: str, start_date: str = DEFAULT_START_DATE, end_date: str | None = None, fases: str = "1,2,3"):
    cnpj = only_digits(cnpj)
    client = PTClient()

    years = _years_range(start_date, end_date)
    fase_codes = [int(x) for x in str(fases).split(",") if str(x).strip()]
    fase_map = {code: FASES[code] for code in fase_codes if code in FASES}

    with SessionLocal() as sess:
        sess.tenant_cnpj = cnpj
        sess.user_id = user_id # ⬅️ Nuevo: Se asigna el `user_id` a la sesión

        for ano in years:
            for fase_code, fase_name in fase_map.items():
                # ✅ SOLO parámetros requeridos
                params = {
                    "codigoPessoa": cnpj,  # requerido
                    "fase": fase_code,     # requerido (1/2/3)
                    "ano": ano,            # requerido
                    # (pagina la agrega get_pages; no es filtro)
                }

                for item in client.get_pages(PATH, params):
                    # fecha robusta
                    data_raw = item.get("data") or item.get("dataDocumento")
                    if not data_raw and item.get("ano"):
                        mes = str(item.get("mes", "01")).zfill(2)
                        data_raw = f"{item['ano']}-{mes}-01"
                    data = iso_date(data_raw or "")

                    # ug/gestao se usan SOLO para componer la clave, no como filtro
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
    ap.add_argument("--user-id", required=True) # ⬅️ Nuevo: Se agrega el argumento `user-id`
    ap.add_argument("--start", default=DEFAULT_START_DATE, help="YYYY-MM-DD o DD/MM/YYYY")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD o DD/MM/YYYY")
    ap.add_argument("--fases", default="1,2,3", help="1=empenho,2=liquidacao,3=pagamento")
    args = ap.parse_args()
    # ⬅️ Nuevo: Se llama a la función `run` con el `user_id`
    run(args.cnpj, args.user_id, args.start, args.end, args.fases)

# python -m src.events.despesas --cnpj 39435028000197 --start 2024-01-01 --end 2025-12-31
