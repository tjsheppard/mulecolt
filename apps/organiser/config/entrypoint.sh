#!/bin/bash
set -e

echo "Starting rclone mount..."
rclone mount zurg: /zurg \
    --config /rclone/rclone.conf \
    --allow-other \
    --allow-non-empty \
    --dir-cache-time 10s \
    --vfs-cache-mode off \
    --daemon

echo "Waiting for rclone mount at /zurg..."
for i in $(seq 1 30); do
    if mountpoint -q /zurg 2>/dev/null; then
        echo "rclone mount ready"
        break
    fi
    sleep 2
done

if ! mountpoint -q /zurg 2>/dev/null; then
    echo "WARNING: rclone mount not detected after 60s, starting organiser anyway..."
fi

exec python -u organiser.py
