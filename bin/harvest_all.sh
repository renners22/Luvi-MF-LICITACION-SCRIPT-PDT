#!/usr/bin/env bash
set -Eeuo pipefail

DOC_RAW="${1:-}"
if [[ -z "$DOC_RAW" ]]; then
  echo "[ERR] uso: $0 <cpf_ou_cnpj>"
  exit 2
fi

# normaliza a dígitos
DOC="$(echo "$DOC_RAW" | tr -cd '0-9')"

BASE="/var/www/scripts/portal_transparencia"
source "$BASE/.venv/bin/activate"
export PYTHONPATH="$BASE"

# Evitar solapes por documento (si ya se está procesando ese doc, no lo duplica)
LOCK="/tmp/harvest_${DOC}.lock"
exec {lockfd}>$LOCK
flock -n "$lockfd" || { echo "[INFO] Ya hay un harvest en curso para $DOC"; exit 0; }

cd "$BASE"

run() {
  echo "[RUN] $*"
  bash -lc "$*"
}

# Lanza los que se necesiten
run "python -m src.events.notas_fiscais --cnpj $DOC"
run "python -m src.events.contratos --cnpj $DOC"
run "python -m src.events.despesas  --cnpj $DOC"
run "python -m src.events.cpgf      --cnpj $DOC"
run "python -m src.events.integridade --cnpj $DOC"

echo "[OK] Harvest finalizado para $DOC"
