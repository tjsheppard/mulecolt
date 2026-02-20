#!/usr/bin/env python3
"""
resolve.py — Manually resolve an unidentified torrent.

Usage (inside the organiser container):
    python resolve.py <torrent_pb_id> <tmdb_id> [film|show]

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

import logging
import sys

import requests
from guessit import guessit

from constants import POCKETBASE_URL, TMDB_API_KEY, TMDB_BASE
from media_resolver import (
    extract_season_from_path,
    get_video_files,
    resolve_episode_duplicate,
    resolve_film_duplicate,
)
from pb_client import PocketBaseClient
from tmdb_utils import match_file_to_tmdb_episode, tmdb_get_show_structure

# Set up logging (same format as organiser)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("organiser")


# ---------------------------------------------------------------------------
# TMDB lookup (by known ID, not search)
# ---------------------------------------------------------------------------

def tmdb_lookup(tmdb_id: int, media_type: str | None = None) -> dict | None:
    """Look up a TMDB ID and return {type, title, year, tmdb_id}.

    If media_type is given ('film' or 'show'), only tries that endpoint.
    Otherwise tries movie first, then TV show.
    """
    if not TMDB_API_KEY:
        log.error("TMDB_API_KEY is not set")
        return None

    endpoints = []
    if media_type in (None, "film"):
        endpoints.append(("film", f"{TMDB_BASE}/movie/{tmdb_id}", "title", "release_date"))
    if media_type in (None, "show"):
        endpoints.append(("show", f"{TMDB_BASE}/tv/{tmdb_id}", "name", "first_air_date"))

    for mtype, url, name_key, date_key in endpoints:
        try:
            r = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                date_str = data.get(date_key, "")
                return {
                    "type": mtype,
                    "title": data[name_key],
                    "year": int(date_str[:4]) if date_str and len(date_str) >= 4 else None,
                    "tmdb_id": data["id"],
                }
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# Resolve logic
# ---------------------------------------------------------------------------

def resolve_as_film(pb: PocketBaseClient, torrent: dict, tmdb_info: dict):
    """Resolve a torrent as a film."""
    result = resolve_film_duplicate(
        pb,
        torrent_id=torrent["id"],
        torrent_score=torrent.get("score", 0),
        tmdb_id=tmdb_info["tmdb_id"],
        title=tmdb_info["title"],
        year=tmdb_info["year"],
    )
    log.info(f"  Film resolution: {result}")


def resolve_as_show(pb: PocketBaseClient, torrent: dict, tmdb_info: dict):
    """Resolve a torrent as a TV show, creating episode records."""
    torrent_id = torrent["id"]
    torrent_score = torrent.get("score", 0)
    tmdb_id = tmdb_info["tmdb_id"]
    title = tmdb_info["title"]
    year = tmdb_info["year"]

    from pathlib import Path

    video_files = get_video_files(torrent.get("path", ""))
    if not video_files:
        log.error(f"No video files found at {torrent.get('path', '?')}")
        return

    torrent_root = Path(torrent.get("path", ""))

    tmdb_structure = tmdb_get_show_structure(tmdb_id, TMDB_API_KEY, TMDB_BASE)
    if tmdb_structure:
        season_summary = ", ".join(
            f"S{s:02d}×{tmdb_structure.episodes_in_season(s)}"
            for s in tmdb_structure.season_numbers
        )
        log.info(f"  TMDB structure: {tmdb_structure.total_episodes} episodes ({season_summary})")

    episodes_found = 0
    for vf in video_files:
        fg = guessit(vf.name, {"type": "episode"})
        guessit_season = fg.get("season")
        guessit_episode = fg.get("episode")

        if guessit_season is None:
            guessit_season = extract_season_from_path(vf, torrent_root)

        # Primary: TMDB structure matching
        matched_episodes = None
        if tmdb_structure:
            matched_episodes = match_file_to_tmdb_episode(
                vf.name, guessit_season, guessit_episode, tmdb_structure,
            )

        if matched_episodes:
            for season, ep_num in matched_episodes:
                episodes_found += 1
                resolve_episode_duplicate(
                    pb, torrent_id, torrent_score,
                    tmdb_id, title, year, season, ep_num,
                )
            continue

        # Fallback: guessit-only
        season = guessit_season if guessit_season is not None else 1
        episode = guessit_episode

        if episode is None:
            log.warning(f"  Skipping (no episode detected): {vf.name}")
            continue

        eps = episode if isinstance(episode, list) else [episode]
        for ep_num in eps:
            episodes_found += 1
            resolve_episode_duplicate(
                pb, torrent_id, torrent_score,
                tmdb_id, title, year, season, ep_num,
                label=" (fallback)",
            )

    if episodes_found == 0:
        log.warning("  No episodes could be parsed from the video files")


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
        log.error(f"tmdb_id must be a number, got '{sys.argv[2]}'")
        sys.exit(1)

    media_type = None
    if len(sys.argv) == 4:
        media_type = sys.argv[3].lower()
        if media_type not in ("film", "show"):
            log.error(f"type must be 'film' or 'show', got '{sys.argv[3]}'")
            sys.exit(1)

    pb = PocketBaseClient(POCKETBASE_URL)

    torrent = pb.get_torrent_by_id(torrent_pb_id)
    if not torrent:
        log.error(f"Torrent '{torrent_pb_id}' not found in PocketBase")
        sys.exit(1)

    log.info(f"Torrent: {torrent.get('name', '?')} (score: {torrent.get('score', 0)})")
    log.info(f"Path:    {torrent.get('path', '?')}")

    type_hint = f" as {media_type}" if media_type else ""
    log.info(f"Looking up TMDB ID {tmdb_id}{type_hint}...")
    tmdb_info = tmdb_lookup(tmdb_id, media_type)
    if not tmdb_info:
        log.error(f"Could not find TMDB ID {tmdb_id}")
        sys.exit(1)

    log.info(f"Found:   {tmdb_info['title']} ({tmdb_info['year']}) [{tmdb_info['type']}]")

    # Clean up existing media records for this torrent
    existing_films = pb.list_films_by_torrent(torrent_pb_id)
    existing_shows = pb.list_shows_by_torrent(torrent_pb_id)
    if existing_films or existing_shows:
        log.info(f"Removing {len(existing_films)} existing film(s) and "
                 f"{len(existing_shows)} existing show episode(s)...")
        for f in existing_films:
            pb.delete_film(f["id"])
        for s in existing_shows:
            pb.delete_show(s["id"])

    if tmdb_info["type"] == "film":
        resolve_as_film(pb, torrent, tmdb_info)
    else:
        resolve_as_show(pb, torrent, tmdb_info)

    pb.update_torrent(torrent_pb_id, manual=False)
    log.info("Done. The next organiser scan will build symlinks.")


if __name__ == "__main__":
    main()
