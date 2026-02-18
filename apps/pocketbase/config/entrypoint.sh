#!/bin/sh
set -e

# Create superuser on first boot (idempotent — upsert won't fail if it exists)
if [ -n "${PB_ADMIN_EMAIL:-}" ] && [ -n "${PB_ADMIN_PASSWORD:-}" ]; then
    echo "Ensuring superuser account exists..."
    pocketbase superuser upsert "$PB_ADMIN_EMAIL" "$PB_ADMIN_PASSWORD" \
        --dir=/pb/pb_data \
        --migrationsDir=/pb/pb_migrations \
        2>&1 || echo "Warning: superuser upsert failed (may already exist)"
fi

exec pocketbase serve \
    --http=0.0.0.0:8090 \
    --dir=/pb/pb_data \
    --migrationsDir=/pb/pb_migrations \
    &

# Wait for PocketBase to become ready
PB_PID=$!
echo "Waiting for PocketBase to be ready..."
for i in $(seq 1 30); do
    if wget -qO- http://localhost:8090/api/health >/dev/null 2>&1; then
        echo "PocketBase is ready"
        break
    fi
    sleep 1
done

# Belt-and-braces: delete the users collection if it still exists.
# The migration handles this on a clean slate; this catches the case where
# PocketBase was already initialised before the migration was introduced.
USER_COL=$(wget -qO- "http://localhost:8090/api/collections/users" 2>/dev/null || true)
if echo "$USER_COL" | grep -q '"name":"users"'; then
    echo "Deleting users collection..."
    # Requires admin auth — build a token first
    TOKEN=$(wget -qO- \
        --header='Content-Type: application/json' \
        --post-data="{\"identity\":\"${PB_ADMIN_EMAIL}\",\"password\":\"${PB_ADMIN_PASSWORD}\"}" \
        "http://localhost:8090/api/admins/auth-with-password" 2>/dev/null \
        | grep -o '"token":"[^"]*"' | cut -d'"' -f4 || true)
    if [ -n "$TOKEN" ]; then
        wget -qO- \
            --header="Authorization: Bearer $TOKEN" \
            --method=DELETE \
            "http://localhost:8090/api/collections/users" >/dev/null 2>&1 || true
        echo "users collection deleted"
    fi
fi

wait $PB_PID
