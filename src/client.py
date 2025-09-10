# src/client.py
import requests
from .config import BASE_URL, PT_TOKEN

class PTClient:
    def __init__(self, base_url: str = BASE_URL, token: str = PT_TOKEN):
        token = (token or "").strip()
        assert token, "PT_TOKEN vacío (revisa tu .env)"
        self.base_url = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({
            "Accept": "application/json",
            "chave-api-dados": token,                # 👈 obligatorio
            "User-Agent": "luvi-mf-harvester/1.0"    # ayuda en soporte/logs
        })

    def _get(self, path: str, params: dict):
        url = f"{self.base_url}/{path.lstrip('/')}"
        r = self.s.get(url, params=params, timeout=60)
        if r.status_code != 200:  # debug útil
            print("DEBUG:", r.status_code, r.url)
            try:
                print("DEBUG body:", r.json())
            except Exception:
                print("DEBUG body:", r.text[:500])
        r.raise_for_status()
        return r

    def get_pages(self, path: str, params: dict, page_size: int = 100):
        page = 1
        while True:
            qp = dict(params)
            qp.setdefault("pagina", page)
            qp.setdefault("tamanho", page_size)
            data = self._get(path, qp).json()
            if not data:
                break
            for item in data:
                yield item
            page += 1
