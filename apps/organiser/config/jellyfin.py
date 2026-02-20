"""
jellyfin.py — Jellyfin library refresh triggers.

Sends per-library refresh requests to Jellyfin when the organiser
detects that symlinks have changed in the films or shows directories.
"""

import logging

import requests

from constants import JELLYFIN_API_KEY, JELLYFIN_URL

log = logging.getLogger("organiser")


def trigger_refresh(films_changed: bool, shows_changed: bool) -> None:
    """Trigger a per-library refresh in Jellyfin for libraries that changed."""
    if not JELLYFIN_API_KEY:
        log.debug("Jellyfin refresh skipped — JELLYFIN_API_KEY not set")
        return
    if not films_changed and not shows_changed:
        return

    headers = {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"'}

    try:
        resp = requests.get(
            f"{JELLYFIN_URL}/Library/VirtualFolders",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        libraries = resp.json()
    except Exception as e:
        log.warning(f"Failed to query Jellyfin libraries: {e}")
        return

    # Map collection types to whether they changed
    changed_types = set()
    if films_changed:
        changed_types.add("movies")
    if shows_changed:
        changed_types.add("tvshows")

    refreshed = []
    for lib in libraries:
        collection_type = (lib.get("CollectionType") or "").lower()
        item_id = lib.get("ItemId")
        name = lib.get("Name", "?")

        if not item_id or collection_type not in changed_types:
            continue

        try:
            r = requests.post(
                f"{JELLYFIN_URL}/Items/{item_id}/Refresh",
                headers=headers,
                params={
                    "Recursive": "true",
                    "MetadataRefreshMode": "Default",
                    "ImageRefreshMode": "Default",
                    "ReplaceAllMetadata": "false",
                    "ReplaceAllImages": "false",
                },
                timeout=10,
            )
            r.raise_for_status()
            refreshed.append(name)
        except Exception as e:
            log.warning(f"Failed to refresh Jellyfin library '{name}': {e}")

    if refreshed:
        log.info(f"  Jellyfin refresh triggered: {', '.join(refreshed)}")
