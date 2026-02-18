#!/bin/sh
set -eu

# Substitute the Real Debrid token into the config template
if [ -z "${REAL_DEBRID_API_KEY:-}" ]; then
  echo "ERROR: REAL_DEBRID_API_KEY is not set. Get your token from https://real-debrid.com/apitoken"
  exit 1
fi

sed "s|__REAL_DEBRID_API_KEY__|${REAL_DEBRID_API_KEY}|g" /app/config.yml > /app/config.runtime.yml

exec /app/zurg --config /app/config.runtime.yml
