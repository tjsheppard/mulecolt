#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# setup-homepage.sh — Generate Homepage config files from templates
#
# Reads DOMAIN from .env and substitutes it into the templates along with
# any API key / credential variables.
#
# Usage:
#   ./setup-homepage.sh              # reads from .env
#   ./setup-homepage.sh example.com  # override domain
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE_DIR="${ROOT_DIR}/apps/homepage/config"
OUTPUT_DIR="${ROOT_DIR}/apps/homepage/data"
ENV_FILE="${ROOT_DIR}/.env"

# --- Load .env ---
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
fi

# Allow passing domain as argument
if [[ $# -ge 1 ]]; then
  DOMAIN="$1"
fi

if [[ -z "${DOMAIN:-}" ]]; then
  echo "Usage: ./setup-homepage.sh <domain>"
  echo ""
  echo "Or set DOMAIN in .env"
  exit 1
fi

# --- Detect local IP (if not set in .env) ---
if [[ -z "${LOCAL_IP:-}" ]]; then
  if command -v ip &> /dev/null; then
    LOCAL_IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src") print $(i+1); exit}')
  elif command -v ipconfig &> /dev/null; then
    LOCAL_IP=$(ipconfig getifaddr en0 2>/dev/null || true)
  fi
  # Fallback
  if [[ -z "${LOCAL_IP:-}" ]]; then
    LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
  fi
fi

if [[ -n "${LOCAL_IP:-}" ]]; then
  echo "Local IP detected: ${LOCAL_IP}"
else
  echo "Warning: Could not detect local IP. Set LOCAL_IP in .env"
  echo "  Local Access bookmarks will show placeholder values."
fi

# --- Check templates exist ---
for tmpl in services.yaml.template settings.yaml.template bookmarks.yaml.template; do
  if [[ ! -f "${TEMPLATE_DIR}/${tmpl}" ]]; then
    echo "Error: Template not found at ${TEMPLATE_DIR}/${tmpl}"
    exit 1
  fi
done

# --- Ensure output directory exists ---
mkdir -p "${OUTPUT_DIR}"

# --- Generate config files from templates ---
for tmpl in services.yaml.template settings.yaml.template bookmarks.yaml.template; do
  output="${OUTPUT_DIR}/${tmpl%.template}"
  cp "${TEMPLATE_DIR}/${tmpl}" "$output"

  # Replace domain placeholder
  sed -i '' "s/<DOMAIN>/${DOMAIN}/g" "$output"

  # Replace local IP placeholder
  if [[ -n "${LOCAL_IP:-}" ]]; then
    sed -i '' "s/<LOCAL_IP>/${LOCAL_IP}/g" "$output"
  fi

  # Replace API key and credential placeholders
  for VAR in JELLYFIN_API_KEY PORTAINER_API_KEY; do
    VAL="${!VAR:-}"
    if [[ -n "$VAL" ]]; then
      sed -i '' "s/<${VAR}>/${VAL}/g" "$output"
    fi
  done
done

# --- Copy static config files to data directory ---
for static in docker.yaml widgets.yaml; do
  if [[ -f "${TEMPLATE_DIR}/${static}" ]]; then
    cp "${TEMPLATE_DIR}/${static}" "${OUTPUT_DIR}/${static}"
  fi
done

echo "Homepage config generated in: ${OUTPUT_DIR}/"
echo "  Domain: *.${DOMAIN}"
if [[ -n "${LOCAL_IP:-}" ]]; then
  echo "  Local:  http://${LOCAL_IP}:3000"
fi
echo ""
echo "Files created:"
echo "  - services.yaml"
echo "  - settings.yaml"
echo "  - bookmarks.yaml"
echo ""
echo "Note: widgets.yaml and docker.yaml are static and don't need templating."
echo ""
echo "If you haven't set API keys yet, update these in .env and re-run:"
echo "  JELLYFIN_API_KEY, PORTAINER_API_KEY"
