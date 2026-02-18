#!/usr/bin/env python3
"""
resolve.py — Manually resolve an unidentified torrent.

Usage (inside the organiser container):
    python resolve.py <torrent_pb_id> <tmdb_id>

This script:
  1. Looks up the TMDB ID via the TMDB API to determine type (movie/TV),
     title, and year.
  2. Fetches the torrent record from PocketBase to get its path.
  3. If movie: creates a film record (with duplicate handling by score).
  4. If TV show: scans video files in the torrent path, parses S/E numbers,
     creates show records for each episode.
  5. Sets manual=false on the torrent.

The next organiser scan will pick up the changes and build symlinks.
"""

import os
import re
import sys
from pathlib import Path

import requests
from guessit import guessit

# Re-use organiser config
ZURG_MOUNT = Path("/zurg")
POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "http://pocketbase:8090")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".vob", ".iso", ".m2ts",
}


# ---------------------------------------------------------------------------
# Minimal PocketBase client (subset of organiser's client)
# ---------------------------------------------------------------------------

class PBClient:
    def __init__(self, base_url: str):
        self.api = f"{base_url.rstrip('/')}/api"
        self._s = requests.Session()

    def _url(self, col: str, rid: str = "") -> str:
        url = f"{self.api}/collections/{col}/records"
        return f"{url}/{rid}" if rid else url

    def get_torrent(self, record_id: str) -> dict | None:
        try:
            r = self._s.get(self._url("torrents", record_id), timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def update_torrent(self, record_id: str, **fields) -> dict | None:
        try:
            r = self._s.patch(self._url("torrents", record_id), json=fields, timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def get_film_by_tmdb(self, tmdb_id: int) -> dict | None:
        try:
            r = self._s.get(
                self._url("films"),
                params={"filter": f"tmdb_id = {tmdb_id}", "perPage": 1, "expand": "torrent"},
                timeout=5,
            )
            r.raise_for_status()
            items = r.json().get("items", [])
            return items[0] if items else None
        except Exception:
            return None

    def create_film(self, torrent_id: str, tmdb_id: int,
                    title: str, year: int | None) -> dict | None:
        try:
            r = self._s.post(self._url("films"), json={
                "torrent": torrent_id, "tmdb_id": tmdb_id,
                "title": title, "year": year or 0,
            }, timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def update_film(self, record_id: str, **fields) -> dict | None:
        try:
            r = self._s.patch(self._url("films", record_id), json=fields, timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def get_show_episode(self, tmdb_id: int, season: int, episode: int) -> dict | None:
        try:
            filt = f"tmdb_id = {tmdb_id} && season = {season} && episode = {episode}"
            r = self._s.get(
                self._url("shows"),
                params={"filter": filt, "perPage": 1, "expand": "torrent"},
                timeout=5,
            )
            r.raise_for_status()
            items = r.json().get("items", [])
            return items[0] if items else None
        except Exception:
            return None

    def create_show(self, torrent_id: str, tmdb_id: int, title: str,
                    year: int | None, season: int, episode: int) -> dict | None:
        try:
            r = self._s.post(self._url("shows"), json={
                "torrent": torrent_id, "tmdb_id": tmdb_id,
                "title": title, "year": year or 0,
                "season": season, "episode": episode,
            }, timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def update_show(self, record_id: str, **fields) -> dict | None:
        try:
            r = self._s.patch(self._url("shows", record_id), json=fields, timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def get_torrent_by_id(self, record_id: str) -> dict | None:
        return self.get_torrent(record_id)

    def list_films_by_torrent(self, torrent_id: str) -> list[dict]:
        try:
            r = self._s.get(
                self._url("films"),
                params={"filter": f'torrent = "{torrent_id}"', "perPage": 200},
                timeout=5,
            )
            r.raise_for_status()
            return r.json().get("items", [])
        except Exception:
            return []

    def list_shows_by_torrent(self, torrent_id: str) -> list[dict]:
        try:
            r = self._s.get(
                self._url("shows"),
                params={"filter": f'torrent = "{torrent_id}"', "perPage": 200},
                timeout=5,
            )
            r.raise_for_status()
            return r.json().get("items", [])
        except Exception:
            return []

    def delete_film(self, record_id: str) -> bool:
        try:
            r = self._s.delete(self._url("films", record_id), timeout=5)
            r.raise_for_status()
            return True
        except Exception:
            return False

    def delete_show(self, record_id: str) -> bool:
        try:
            r = self._s.delete(self._url("shows", record_id), timeout=5)
            r.raise_for_status()
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# TMDB lookup
# ---------------------------------------------------------------------------

def tmdb_lookup(tmdb_id: int, media_type: str | None = None) -> dict | None:
    """Look up a TMDB ID and return {type, title, year, tmdb_id}.

    If media_type is given ('film' or 'show'), only tries that endpoint.
    Otherwise tries movie first, then TV show.
    """
    if not TMDB_API_KEY:
        print("ERROR: TMDB_API_KEY is not set")
        return None

    try_movie = media_type in (None, "film")
    try_tv = media_type in (None, "show")

    # Try movie
    if try_movie:
        try:
            r = requests.get(
                f"{TMDB_BASE}/movie/{tmdb_id}",
                params={"api_key": TMDB_API_KEY},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                release = data.get("release_date", "")
                return {
                    "type": "film",
                    "title": data["title"],
                    "year": int(release[:4]) if release and len(release) >= 4 else None,
                    "tmdb_id": data["id"],
                }
        except Exception:
            pass

    # Try TV show
    if try_tv:
        try:
            r = requests.get(
                f"{TMDB_BASE}/tv/{tmdb_id}",
                params={"api_key": TMDB_API_KEY},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                air_date = data.get("first_air_date", "")
                return {
                    "type": "show",
                    "title": data["name"],
                    "year": int(air_date[:4]) if air_date and len(air_date) >= 4 else None,
                    "tmdb_id": data["id"],
                }
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# Video file helpers
# ---------------------------------------------------------------------------

def get_video_files(torrent_path: str) -> list[Path]:
    """Get all video files from a torrent path."""
    path = Path(torrent_path)
    if not path.exists():
        return []
    if path.is_file():
        return [path] if path.suffix.lower() in VIDEO_EXTENSIONS else []
    files = []
    try:
        for item in path.rglob("*"):
            if item.is_file() and item.suffix.lower() in VIDEO_EXTENSIONS:
                files.append(item)
    except OSError:
        pass
    return files


# ---------------------------------------------------------------------------
# Resolve logic
# ---------------------------------------------------------------------------

def resolve_as_film(pb: PBClient, torrent: dict, tmdb_info: dict):
    """Resolve a torrent as a film."""
    torrent_id = torrent["id"]
    torrent_score = torrent.get("score", 0)
    tmdb_id = tmdb_info["tmdb_id"]
    title = tmdb_info["title"]
    year = tmdb_info["year"]

    existing = pb.get_film_by_tmdb(tmdb_id)

    if existing:
        existing_torrent_id = existing.get("torrent")
        if not existing_torrent_id:
            # Film exists but has no torrent — take it over
            pb.update_film(existing["id"], torrent=torrent_id)
            print(f"  Re-linked film: {title} ({year})")
        else:
            existing_torrent = (existing.get("expand") or {}).get("torrent")
            if not existing_torrent:
                existing_torrent = pb.get_torrent_by_id(existing_torrent_id)
            existing_score = existing_torrent.get("score", 0) if existing_torrent else 0

            if torrent_score > existing_score:
                pb.update_film(existing["id"], torrent=torrent_id)
                pb.update_torrent(existing_torrent_id, archived=True)
                print(f"  Film: {title} ({year}) — new torrent wins ({torrent_score} > {existing_score})")
            else:
                pb.update_torrent(torrent_id, archived=True)
                print(f"  Film: {title} ({year}) — existing wins ({existing_score} >= {torrent_score}), archiving")
    else:
        pb.create_film(torrent_id=torrent_id, tmdb_id=tmdb_id, title=title, year=year)
        print(f"  Created film: {title} ({year}) [tmdbid={tmdb_id}]")


def resolve_as_show(pb: PBClient, torrent: dict, tmdb_info: dict):
    """Resolve a torrent as a TV show, creating episode records."""
    torrent_id = torrent["id"]
    torrent_score = torrent.get("score", 0)
    tmdb_id = tmdb_info["tmdb_id"]
    title = tmdb_info["title"]
    year = tmdb_info["year"]

    video_files = get_video_files(torrent.get("path", ""))
    if not video_files:
        print(f"  ERROR: No video files found at {torrent.get('path', '?')}")
        return

    episodes_found = 0
    for vf in video_files:
        fg = guessit(vf.name, {"type": "episode"})
        season = fg.get("season", 1)
        episode = fg.get("episode")

        if episode is None:
            print(f"  Skipping (no episode detected): {vf.name}")
            continue

        eps = episode if isinstance(episode, list) else [episode]
        for ep_num in eps:
            episodes_found += 1
            existing = pb.get_show_episode(tmdb_id, season, ep_num)

            if existing:
                existing_torrent_id = existing.get("torrent")
                if not existing_torrent_id:
                    pb.update_show(existing["id"], torrent=torrent_id)
                    print(f"  Re-linked: {title} S{season:02d}E{ep_num:02d}")
                    continue

                existing_torrent = (existing.get("expand") or {}).get("torrent")
                if not existing_torrent:
                    existing_torrent = pb.get_torrent_by_id(existing_torrent_id)
                existing_score = existing_torrent.get("score", 0) if existing_torrent else 0

                if torrent_score > existing_score:
                    pb.update_show(existing["id"], torrent=torrent_id)
                    print(f"  {title} S{season:02d}E{ep_num:02d} — new torrent wins ({torrent_score} > {existing_score})")
                else:
                    print(f"  {title} S{season:02d}E{ep_num:02d} — existing wins ({existing_score} >= {torrent_score})")
            else:
                pb.create_show(
                    torrent_id=torrent_id, tmdb_id=tmdb_id,
                    title=title, year=year,
                    season=season, episode=ep_num,
                )
                print(f"  Created: {title} S{season:02d}E{ep_num:02d}")

    if episodes_found == 0:
        print("  WARNING: No episodes could be parsed from the video files")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python resolve.py <torrent_pb_id> <tmdb_id> [film|show]")
        print()
        print("  torrent_pb_id  PocketBase record ID of the torrent")
        print("  tmdb_id        TMDB ID to assign (movie or TV show)")
        print("  film|show      Optional: force type when TMDB IDs collide")
        sys.exit(1)

    torrent_pb_id = sys.argv[1]
    try:
        tmdb_id = int(sys.argv[2])
    except ValueError:
        print(f"ERROR: tmdb_id must be a number, got '{sys.argv[2]}'")
        sys.exit(1)

    media_type = None
    if len(sys.argv) == 4:
        media_type = sys.argv[3].lower()
        if media_type not in ("film", "show"):
            print(f"ERROR: type must be 'film' or 'show', got '{sys.argv[3]}'")
            sys.exit(1)

    pb = PBClient(POCKETBASE_URL)

    # Get the torrent
    torrent = pb.get_torrent(torrent_pb_id)
    if not torrent:
        print(f"ERROR: Torrent '{torrent_pb_id}' not found in PocketBase")
        sys.exit(1)

    print(f"Torrent: {torrent.get('name', '?')} (score: {torrent.get('score', 0)})")
    print(f"Path:    {torrent.get('path', '?')}")

    # Look up the TMDB ID
    type_hint = f" as {media_type}" if media_type else ""
    print(f"Looking up TMDB ID {tmdb_id}{type_hint}...")
    tmdb_info = tmdb_lookup(tmdb_id, media_type)
    if not tmdb_info:
        print(f"ERROR: Could not find TMDB ID {tmdb_id}")
        sys.exit(1)

    print(f"Found:   {tmdb_info['title']} ({tmdb_info['year']}) [{tmdb_info['type']}]")
    print()

    # Clean up any existing media records for this torrent
    existing_films = pb.list_films_by_torrent(torrent_pb_id)
    existing_shows = pb.list_shows_by_torrent(torrent_pb_id)
    if existing_films or existing_shows:
        print(f"Removing {len(existing_films)} existing film(s) and {len(existing_shows)} existing show episode(s)...")
        for f in existing_films:
            pb.delete_film(f["id"])
        for s in existing_shows:
            pb.delete_show(s["id"])
        print()

    # Resolve based on type
    if tmdb_info["type"] == "film":
        resolve_as_film(pb, torrent, tmdb_info)
    else:
        resolve_as_show(pb, torrent, tmdb_info)

    # Clear the manual flag
    pb.update_torrent(torrent_pb_id, manual=False)
    print()
    print("Done. The next organiser scan will build symlinks.")


if __name__ == "__main__":
    main()
