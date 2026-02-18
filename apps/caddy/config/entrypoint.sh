#!/bin/sh
set -eu

# ---------------------------------------------------------------------------
# entrypoint.sh — Start Caddy, then auto-create Cloudflare DNS records
#
# When CF_ZONE_ID and TS_API_KEY are set (Option B), this script starts Caddy
# in the background, waits for the "mulecolt" Tailscale node to appear on the
# tailnet, retrieves its IP, and upserts Cloudflare A records for DOMAIN and
# *.DOMAIN. Then it waits on the Caddy process for the container's lifetime.
#
# When those variables are absent (Option A / local-only), Caddy starts
# directly with no DNS setup.
# ---------------------------------------------------------------------------

CADDY_CMD="caddy run --config /etc/caddy/Caddyfile --adapter caddyfile"

# --- Skip DNS setup if credentials are missing ---
if [ -z "${CF_ZONE_ID:-}" ] || [ -z "${TS_API_KEY:-}" ] || [ -z "${DOMAIN:-}" ] || [ -z "${CF_API_TOKEN:-}" ]; then
  echo "[entrypoint] DNS auto-setup skipped (CF_ZONE_ID, TS_API_KEY, DOMAIN, or CF_API_TOKEN not set)"
  exec $CADDY_CMD
fi

# --- Start Caddy in the background so it registers the "mulecolt" Tailscale node ---
echo "[entrypoint] Starting Caddy in background..."
$CADDY_CMD &
CADDY_PID=$!

# --- Wait for the "mulecolt" node to appear on the tailnet ---
CF_API="https://api.cloudflare.com/client/v4"
TS_API="https://api.tailscale.com/api/v2"
MAX_ATTEMPTS=30
SLEEP_SECONDS=10

echo "[entrypoint] Waiting for Tailscale node \"mulecolt\" to appear on the tailnet..."

TAILSCALE_IP=""
attempt=1
while [ $attempt -le $MAX_ATTEMPTS ]; do
  # Query Tailscale API for devices in the tailnet
  ts_response="$(curl -sf -H "Authorization: Bearer ${TS_API_KEY}" \
    "${TS_API}/tailnet/-/devices?fields=default" 2>/dev/null)" || true

  if [ -n "$ts_response" ]; then
    # Look for a device with hostname "mulecolt" and extract its IPv4 address
    TAILSCALE_IP="$(echo "$ts_response" | jq -r '
      .devices[]
      | select(.hostname == "mulecolt")
      | .addresses[]
      | select(test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$"))
    ' 2>/dev/null | head -n1)" || true
  fi

  if [ -n "$TAILSCALE_IP" ]; then
    echo "[entrypoint] Found mulecolt node: ${TAILSCALE_IP}"
    break
  fi

  echo "[entrypoint] Attempt ${attempt}/${MAX_ATTEMPTS} — mulecolt not found yet, retrying in ${SLEEP_SECONDS}s..."
  sleep $SLEEP_SECONDS
  attempt=$((attempt + 1))
done

if [ -z "$TAILSCALE_IP" ]; then
  echo "[entrypoint] Warning: Could not find \"mulecolt\" node after ${MAX_ATTEMPTS} attempts."
  echo "[entrypoint] DNS records were NOT created. Caddy is still running."
  echo "[entrypoint] You can create them manually later with ./setup-dns.sh"
  wait $CADDY_PID
  exit $?
fi

# --- Upsert Cloudflare DNS records ---
upsert_record() {
  display_name="$1"
  dns_name="$2"

  echo "[entrypoint] Checking DNS record: ${display_name}..."
  response="$(curl -s -X GET \
    "${CF_API}/zones/${CF_ZONE_ID}/dns_records?type=A&name=${display_name}" \
    -H "Authorization: Bearer ${CF_API_TOKEN}" \
    -H "Content-Type: application/json")"

  success="$(echo "$response" | jq -r '.success')"
  if [ "$success" != "true" ]; then
    echo "[entrypoint] Error: Cloudflare API request failed for ${display_name}."
    echo "$response" | jq '.errors' 2>/dev/null || echo "$response"
    return 1
  fi

  record_count="$(echo "$response" | jq '.result | length')"

  if [ "$record_count" -gt 0 ]; then
    record_id="$(echo "$response" | jq -r '.result[0].id')"
    current_ip="$(echo "$response" | jq -r '.result[0].content')"

    if [ "$current_ip" = "$TAILSCALE_IP" ]; then
      echo "[entrypoint] Already up to date: ${display_name} → ${TAILSCALE_IP}"
      return 0
    fi

    echo "[entrypoint] Updating ${display_name} (${current_ip} → ${TAILSCALE_IP})..."
    update_response="$(curl -s -X PUT \
      "${CF_API}/zones/${CF_ZONE_ID}/dns_records/${record_id}" \
      -H "Authorization: Bearer ${CF_API_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{\"type\":\"A\",\"name\":\"${dns_name}\",\"content\":\"${TAILSCALE_IP}\",\"ttl\":1,\"proxied\":false}")"

    if [ "$(echo "$update_response" | jq -r '.success')" = "true" ]; then
      echo "[entrypoint] Updated: ${display_name} → ${TAILSCALE_IP}"
    else
      echo "[entrypoint] Error: Failed to update ${display_name}."
      echo "$update_response" | jq '.errors' 2>/dev/null || echo "$update_response"
      return 1
    fi
  else
    echo "[entrypoint] Creating A record for ${display_name}..."
    create_response="$(curl -s -X POST \
      "${CF_API}/zones/${CF_ZONE_ID}/dns_records" \
      -H "Authorization: Bearer ${CF_API_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{\"type\":\"A\",\"name\":\"${dns_name}\",\"content\":\"${TAILSCALE_IP}\",\"ttl\":1,\"proxied\":false}")"

    if [ "$(echo "$create_response" | jq -r '.success')" = "true" ]; then
      echo "[entrypoint] Created: ${display_name} → ${TAILSCALE_IP}"
    else
      echo "[entrypoint] Error: Failed to create ${display_name}."
      echo "$create_response" | jq '.errors' 2>/dev/null || echo "$create_response"
      return 1
    fi
  fi
}

upsert_record "${DOMAIN}" "@"
upsert_record "*.${DOMAIN}" "*"

echo "[entrypoint] DNS setup complete."

# --- Wait for Caddy to keep the container alive ---
wait $CADDY_PID
