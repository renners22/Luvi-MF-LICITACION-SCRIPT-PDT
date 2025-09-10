import os
from dotenv import load_dotenv
load_dotenv()

PT_TOKEN = os.getenv("PT_TOKEN", "")
DB_URL   = os.getenv("DB_URL", "mysql+pymysql://root:root@127.0.0.1:3306/tu_base")
BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"
DEFAULT_START_DATE = os.getenv("DEFAULT_START_DATE", "2024-01-01")

assert PT_TOKEN, "Falta PT_TOKEN en .env"
