#!/bin/bash
set -e

echo "Starting rclone mount..."
rclone mount zurg:__all__/ /zurg \
    --config /rclone/rclone.conf \
    --allow-other \
    --allow-non-empty \
    --dir-cache-time 10s \
    --vfs-cache-mode off \
    --daemon

# Brief wait for the mount to settle — the organiser has its own
# robust wait loop (up to 5 minutes), so we just need it started.
sleep 3

exec python -u organiser.py
