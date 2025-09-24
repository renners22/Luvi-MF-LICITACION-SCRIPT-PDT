#!/usr/bin/env bash
set -Eeuo pipefail

DOC_RAW="${1:-}"
USER_ID_RAW="${2:-}"
if [[ -z "$DOC_RAW" ]]; then
  echo "[ERR] uso: $0 <cpf_ou_cnpj> <user_id>"
  exit 2
fi

# normaliza a dígitos
DOC="$(echo "$DOC_RAW" | tr -cd '0-9')"
USER_ID="$(echo "$USER_ID_RAW" | tr -cd '0-9')"

# --- Configuración del entorno de producción ---
BASE="/var/www/scripts/portal_transparencia"
PYTHON_BIN="$BASE/venv/bin/python"

# ✅ Se elimina el `source` que causaba problemas de entorno
# ✅ Se elimina el bloqueo (`flock`) que causaba el error de permisos
export PYTHONPATH="$BASE"

# Verificación para asegurar que la ruta de Python existe
if [ ! -f "$PYTHON_BIN" ]; then
    echo "[ERR] No se encontró el intérprete de Python en: $PYTHON_BIN"
    echo "[INFO] Por favor, verifica que la ruta de tu entorno virtual sea correcta."
    exit 1
fi

echo "Usando la ruta de produccion: $BASE"
echo "Ejecutando con intérprete Python: $PYTHON_BIN"

# --- Ejecuta los scripts de Python directamente ---
run_python() {
  echo "[RUN] $PYTHON_BIN -m src.events.$1 --cnpj $DOC --user-id $USER_ID"
  "$PYTHON_BIN" -m "src.events.$1" --cnpj "$DOC" --user-id "$USER_ID"
}

run_python "notas_fiscais"
run_python "contratos"
run_python "despesas"
run_python "cpgf"
run_python "integridade"

echo "[OK] Harvest finalizado para $DOC y User ID: $USER_ID"
