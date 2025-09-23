#!/usr/bin/env bash
set -Eeuo pipefail

DOC_RAW="${1:-}"
USER_ID_RAW="${2:-}" # ⬅️ Nuevo: Lee el segundo argumento
if [[ -z "$DOC_RAW" ]]; then
  echo "[ERR] uso: $0 <cpf_ou_cnpj> <user_id>"
  exit 2
fi

# normaliza a dígitos
DOC="$(echo "$DOC_RAW" | tr -cd '0-9')"
# ⬅️ Nuevo: Normaliza a dígitos el user_id también, por seguridad
USER_ID="$(echo "$USER_ID_RAW" | tr -cd '0-9')"


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

# ⬅️ Actualizado: Pasa el user_id a cada script de Python
run "python -m src.events.notas_fiscais --cnpj $DOC --user-id $USER_ID"
run "python -m src.events.contratos --cnpj $DOC --user-id $USER_ID"
run "python -m src.events.despesas  --cnpj $DOC --user-id $USER_ID"
run "python -m src.events.cpgf      --cnpj $DOC --user-id $USER_ID"
run "python -m src.events.integridade --cnpj $DOC --user-id $USER_ID"

echo "[OK] Harvest finalizado para $DOC y User ID: $USER_ID"