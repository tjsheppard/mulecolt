"""
media_resolver.py — Duplicate resolution and media record management.

Consolidates the "compare scores → swap or archive" pattern used when
a new torrent provides media that already exists in PocketBase. This
pattern was previously duplicated across organiser.py and resolve.py.

Also provides shared helpers for scanning video files and extracting
season numbers from directory paths.
"""

import logging
from pathlib import Path
from typing import Literal

from constants import SEASON_DIR_PATTERN, VIDEO_EXTENSIONS
from pb_client import PocketBaseClient
from scoring import format_score

log = logging.getLogger("organiser")

# Type alias for the result of a duplicate resolution
ResolveResult = Literal["created", "won", "lost", "relinked"]


# ---------------------------------------------------------------------------
# Video file helpers
# ---------------------------------------------------------------------------

def get_video_files(torrent_path: str | Path) -> list[Path]:
    """Get all video files from a torrent path (file or directory)."""
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


def extract_season_from_path(video_path: Path, torrent_root: Path) -> int | None:
    """Extract season number from a file's parent directory structure.

    Looks for directory names like 'Season 1', 'Season 02', 'S3', etc.
    between the torrent root and the file.
    """
    try:
        rel = video_path.relative_to(torrent_root)
    except ValueError:
        return None
    for part in rel.parts[:-1]:  # skip the filename itself
        m = SEASON_DIR_PATTERN.search(part)
        if m:
            return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Duplicate resolution — the core pattern
# ---------------------------------------------------------------------------

def resolve_film_duplicate(
    pb: PocketBaseClient,
    torrent_id: str,
    torrent_score: int,
    tmdb_id: int,
    title: str,
    year: int | None,
    label: str = "",
) -> ResolveResult:
    """Resolve a film against an existing record, or create a new one.

    Handles the "compare scores → swap or archive" pattern for films.
    Returns the outcome so the caller can log or act accordingly.
    """
    existing = pb.get_film_by_tmdb(tmdb_id)

    if not existing:
        pb.create_film(torrent_id=torrent_id, tmdb_id=tmdb_id,
                       title=title, year=year)
        log.info(f"  Film: {title} ({year}) [tmdbid={tmdb_id}]  "
                 f"{format_score(torrent_score)}{label}")
        return "created"

    existing_torrent_id = existing.get("torrent")
    if not existing_torrent_id:
        # Film exists but has no torrent — take it over
        pb.update_film(existing["id"], torrent=torrent_id)
        log.info(f"  Film: {title} ({year}) — re-linked{label}")
        return "relinked"

    # Compare scores
    existing_torrent = (existing.get("expand") or {}).get("torrent")
    if not existing_torrent:
        existing_torrent = pb.get_torrent_by_id(existing_torrent_id)
    existing_score = existing_torrent.get("score", 0) if existing_torrent else 0

    if torrent_score > existing_score:
        pb.update_film(existing["id"], torrent=torrent_id)
        pb.update_torrent(existing_torrent_id, archived=True)
        log.info(f"  Film: {title} ({year}) — new torrent wins "
                 f"({format_score(torrent_score)} > {format_score(existing_score)}){label}")
        return "won"
    else:
        pb.update_torrent(torrent_id, archived=True)
        log.info(f"  Film: {title} ({year}) — existing torrent wins "
                 f"({format_score(existing_score)} >= {format_score(torrent_score)}), "
                 f"archiving new{label}")
        return "lost"


def resolve_episode_duplicate(
    pb: PocketBaseClient,
    torrent_id: str,
    torrent_score: int,
    tmdb_id: int,
    title: str,
    year: int | None,
    season: int,
    episode: int,
    label: str = "",
) -> ResolveResult:
    """Resolve a single episode against an existing record, or create new.

    Same pattern as resolve_film_duplicate but for show episodes.
    Returns the outcome and handles archiving of the losing torrent's
    media if it has no remaining episodes.
    """
    existing = pb.get_show_episode(tmdb_id, season, episode)

    if not existing:
        pb.create_show(
            torrent_id=torrent_id, tmdb_id=tmdb_id,
            title=title, year=year,
            season=season, episode=episode,
        )
        log.info(f"  Show: {title} S{season:02d}E{episode:02d} "
                 f"[tmdbid={tmdb_id}]{label}")
        return "created"

    existing_torrent_id = existing.get("torrent")
    if not existing_torrent_id:
        pb.update_show(existing["id"], torrent=torrent_id)
        log.info(f"  Show: {title} S{season:02d}E{episode:02d} — re-linked{label}")
        return "relinked"

    # Compare scores
    existing_torrent = (existing.get("expand") or {}).get("torrent")
    if not existing_torrent:
        existing_torrent = pb.get_torrent_by_id(existing_torrent_id)
    existing_score = existing_torrent.get("score", 0) if existing_torrent else 0

    if torrent_score > existing_score:
        pb.update_show(existing["id"], torrent=torrent_id)
        log.info(f"  Show: {title} S{season:02d}E{episode:02d} — new torrent wins "
                 f"({format_score(torrent_score)} > {format_score(existing_score)}){label}")
        # Check if the old torrent still has any media
        maybe_archive_orphan(pb, existing_torrent_id)
        return "won"
    else:
        log.info(f"  Show: {title} S{season:02d}E{episode:02d} — existing wins "
                 f"({format_score(existing_score)} >= {format_score(torrent_score)}){label}")
        return "lost"


def maybe_archive_orphan(pb: PocketBaseClient, torrent_id: str) -> None:
    """Archive a torrent if it no longer provides any films or episodes."""
    if pb.list_films_by_torrent(torrent_id):
        return
    if pb.list_shows_by_torrent(torrent_id):
        return
    pb.update_torrent(torrent_id, archived=True)
    torrent = pb.get_torrent_by_id(torrent_id)
    name = torrent.get("name", torrent_id) if torrent else torrent_id
    log.info(f"  Torrent archived (no media remaining): {name}")
