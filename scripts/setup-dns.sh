#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# setup-dns.sh — Create or update DNS A records in Cloudflare
#
# Reads DOMAIN, CF_API_TOKEN, and CF_ZONE_ID from .env, finds the "mulecolt"
# Tailscale node's IPv4 address, then ensures both DOMAIN and *.DOMAIN
# point to it.
#
# NOTE: DNS records are normally created automatically when the Caddy
# container starts. This script is provided as a manual fallback for
# updating records after the container is already running.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

# --- Load .env ---
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: .env file not found at ${ENV_FILE}"
  echo "Copy .env.example to .env and fill in your values first."
  exit 1
fi

set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

# --- Validate required variables ---
missing=()
[[ -z "${DOMAIN:-}" ]] && missing+=("DOMAIN")
[[ -z "${CF_API_TOKEN:-}" ]] && missing+=("CF_API_TOKEN")
[[ -z "${CF_ZONE_ID:-}" ]] && missing+=("CF_ZONE_ID")

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Error: The following variables are missing from .env:"
  printf '  - %s\n' "${missing[@]}"
  echo ""
  echo "Set them in ${ENV_FILE} and try again."
  echo "CF_ZONE_ID can be found on your domain's overview page in the Cloudflare dashboard (right sidebar)."
  exit 1
fi

# --- Check dependencies ---
for cmd in curl jq tailscale; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: '$cmd' is required but not installed."
    exit 1
  fi
done

# --- Get Tailscale IP of the "mulecolt" node ---
echo "Looking for Tailscale node \"mulecolt\"..."
TAILSCALE_IP="$(tailscale status --json 2>/dev/null \
  | jq -r '
      .Peer[]
      | select(.HostName == "mulecolt")
      | .TailscaleIPs[]
      | select(test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$"))
    ' 2>/dev/null \
  | head -n1)" || true

if [[ -z "${TAILSCALE_IP}" ]]; then
  echo "Error: Could not find a Tailscale peer named \"mulecolt\"."
  echo "Make sure the Caddy container is running (docker compose up -d --build)"
  echo "and that it has registered itself on the tailnet."
  exit 1
fi
echo "  mulecolt Tailscale IP: ${TAILSCALE_IP}"

# --- Cloudflare API ---
CF_API="https://api.cloudflare.com/client/v4"

auth_header="Authorization: Bearer ${CF_API_TOKEN}"
content_type="Content-Type: application/json"

echo "Using Zone ID: ${CF_ZONE_ID}"

# --- Upsert a single A record ---
# Usage: upsert_record <display_name> <dns_name>
#   display_name: shown in output (e.g. "*.example.com")
#   dns_name:     the "name" field sent to Cloudflare API (e.g. "*" or "@")
upsert_record() {
  local display_name="$1"
  local dns_name="$2"

  echo ""
  echo "Checking for existing DNS record: ${display_name}..."
  local response
  response="$(curl -s -X GET \
    "${CF_API}/zones/${CF_ZONE_ID}/dns_records?type=A&name=${display_name}" \
    -H "$auth_header" \
    -H "$content_type")"

  local success
  success="$(echo "$response" | jq -r '.success')"
  if [[ "$success" != "true" ]]; then
    echo "Error: Cloudflare API request failed."
    echo "$response" | jq '.errors' 2>/dev/null || echo "$response"
    return 1
  fi

  local record_count
  record_count="$(echo "$response" | jq '.result | length')"

  if [[ "$record_count" -gt 0 ]]; then
    # Record exists — check if update is needed
    local record_id current_ip
    record_id="$(echo "$response" | jq -r '.result[0].id')"
    current_ip="$(echo "$response" | jq -r '.result[0].content')"

    if [[ "$current_ip" == "$TAILSCALE_IP" ]]; then
      echo "  Already up to date: ${display_name} → ${TAILSCALE_IP}"
      return 0
    fi

    # Update existing record
    echo "  Updating (${current_ip} → ${TAILSCALE_IP})..."
    local update_response
    update_response="$(curl -s -X PUT \
      "${CF_API}/zones/${CF_ZONE_ID}/dns_records/${record_id}" \
      -H "$auth_header" \
      -H "$content_type" \
      -d "{\"type\":\"A\",\"name\":\"${dns_name}\",\"content\":\"${TAILSCALE_IP}\",\"ttl\":1,\"proxied\":false}")"

    if [[ "$(echo "$update_response" | jq -r '.success')" == "true" ]]; then
      echo "  Updated: ${display_name} → ${TAILSCALE_IP}"
    else
      echo "Error: Failed to update DNS record."
      echo "$update_response" | jq '.errors' 2>/dev/null || echo "$update_response"
      return 1
    fi
  else
    # Create new record
    echo "  Creating A record..."
    local create_response
    create_response="$(curl -s -X POST \
      "${CF_API}/zones/${CF_ZONE_ID}/dns_records" \
      -H "$auth_header" \
      -H "$content_type" \
      -d "{\"type\":\"A\",\"name\":\"${dns_name}\",\"content\":\"${TAILSCALE_IP}\",\"ttl\":1,\"proxied\":false}")"

    if [[ "$(echo "$create_response" | jq -r '.success')" == "true" ]]; then
      echo "  Created: ${display_name} → ${TAILSCALE_IP}"
    else
      echo "Error: Failed to create DNS record."
      echo "$create_response" | jq '.errors' 2>/dev/null || echo "$create_response"
      return 1
    fi
  fi
}

# --- Create/update both records ---
upsert_record "${DOMAIN}" "@"
upsert_record "*.${DOMAIN}" "*"

echo ""
echo "Done."
