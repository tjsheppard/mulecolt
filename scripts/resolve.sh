#!/bin/bash
# resolve.sh — Manually resolve a torrent that the organiser couldn't identify.
#
# Usage:
#   ./scripts/resolve.sh <torrent_pb_id> <tmdb_id> [film|show]
#
# Examples:
#   ./scripts/resolve.sh abc123def456 155
#   # Resolves torrent as TMDB ID 155 (tries movie first, then TV)
#
#   ./scripts/resolve.sh abc123def456 1429 show
#   # Resolves torrent as TMDB TV ID 1429 (Attack on Titan)
#
# To find torrent IDs that need resolving, check the manual_torrents view
# in PocketBase at http://localhost:8090/_/

set -euo pipefail

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <torrent_pb_id> <tmdb_id> [film|show]"
    exit 1
fi

docker exec organiser python resolve.py "$@"
