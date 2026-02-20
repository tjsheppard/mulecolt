#!/usr/bin/env python3
"""
Media Organiser — torrent lifecycle manager.

Manages the full lifecycle of torrents from Real-Debrid: scanning, identifying,
deduplicating by quality score, and creating Jellyfin-compatible symlinks.

PocketBase is the single source of truth. No local state files.

Schema (PocketBase):
  torrents:  id, name, path, score, archived, manual, hash, rd_id, rd_filename, repair_attempts
  films:     id, torrent (relation), tmdb_id, title, year
  shows:     id, torrent (relation), tmdb_id, title, year, season, episode

Workflow per scan:
  Phase A — Sync torrents with zurg mount
  Phase B — Identify unidentified torrents (guessit + TMDB)
  Phase C — Detect removed torrents (path no longer on zurg)
  Phase D — Build symlinks from PocketBase state
  Phase E — Clean up archived torrents (delete from RD + PocketBase)

Unidentifiable torrents are flagged manual=true. Use resolve.sh to fix them.

Environment variables:
  TMDB_API_KEY           — TMDb API key (required for identification)
  SCAN_INTERVAL_SECS     — seconds between scans (default: 300)
  POCKETBASE_URL         — PocketBase API URL (default: http://pocketbase:8090)
  REAL_DEBRID_API_KEY    — Real-Debrid API token (required for auto-repair)
  REPAIR_ENABLED         — enable dead-torrent auto-repair (default: true)
  MAX_REPAIR_ATTEMPTS    — max repair retries per torrent (default: 3)
  MIN_VIDEO_FILE_SIZE_MB — minimum file size for repair file selection (default: 100)
  JELLYFIN_API_KEY       — Jellyfin API key for triggering library refreshes
  JELLYFIN_URL           — Jellyfin base URL (default: http://jellyfin:8096)
  JELLYFIN_ZURG_PATH     — Zurg mount path inside Jellyfin container (default: /zurg)
  WEBHOOK_PORT           — port for the trigger webhook server (default: 8080)
  CLEANUP_ARCHIVED       — delete archived torrents from RD + PocketBase (default: true)
"""

import datetime
import logging
import os
import re
import threading
import time
from pathlib import Path

import requests
from guessit import guessit

from constants import (
    CLEANUP_ARCHIVED,
    FILMS_DIR,
    JELLYFIN_API_KEY,
    JELLYFIN_ZURG_PATH,
    MAX_REPAIR_ATTEMPTS,
    MEDIA_DIR,
    MIN_VIDEO_FILE_SIZE_MB,
    POCKETBASE_URL,
    REAL_DEBRID_API_KEY,
    REPAIR_ENABLED,
    SCAN_INTERVAL,
    SHOW_PATTERNS,
    SHOWS_DIR,
    TMDB_API_KEY,
    TMDB_BASE,
    VIDEO_EXTENSIONS,
    WEBHOOK_PORT,
    ZURG_MOUNT,
    is_meaningless_title,
    validate_year,
)
from formatting import format_episode, format_media_name
from media_resolver import (
    extract_season_from_path,
    get_video_files,
    maybe_archive_orphan,
    resolve_episode_duplicate,
    resolve_film_duplicate,
)
from pb_client import PocketBaseClient
from rd_api import RealDebridClient
from scoring import format_score, score_quality
from tmdb_utils import (
    ShowStructure,
    clear_structure_cache,
    match_file_to_tmdb_episode,
    tmdb_get_show_structure,
)

import jellyfin as jellyfin_mod
import webhook as webhook_mod

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("organiser")

# Global PocketBase client
pb = PocketBaseClient(POCKETBASE_URL)


# ---------------------------------------------------------------------------
# Media classification — show vs film
# ---------------------------------------------------------------------------

def _classify_torrent(folder_name: str, files: list[Path]) -> str:
    """Classify a torrent entry as 'show' or 'film'.

    Strategy:
      1. Check folder name for show patterns
      2. Check individual filenames for episode patterns
      3. Multiple video files → likely a show (season pack)
      4. Fall back to 'film'
    """
    for pattern in SHOW_PATTERNS:
        if pattern.search(folder_name):
            return "show"

    if files:
        filenames = [f.name for f in files[:20]]
        ep_count = sum(
            1 for fn in filenames
            if any(p.search(fn) for p in SHOW_PATTERNS)
        )
        if ep_count > len(filenames) / 2:
            return "show"

        video_count = sum(1 for f in files if f.suffix.lower() in VIDEO_EXTENSIONS)
        if video_count > 3:
            return "show"

    return "film"


def _scan_zurg_mount() -> dict[str, list[Path]]:
    """Scan the flat Zurg mount and return {folder_name: [video_file_paths]}."""
    results: dict[str, list[Path]] = {}

    if not ZURG_MOUNT.exists():
        log.warning(f"Zurg mount not found at {ZURG_MOUNT}")
        return results

    try:
        entries = sorted(ZURG_MOUNT.iterdir())
    except OSError as e:
        log.error(f"Failed to list Zurg mount: {e}")
        return results

    for entry in entries:
        if entry.is_dir():
            video_files = []
            try:
                for item in entry.rglob("*"):
                    if item.is_file() and item.suffix.lower() in VIDEO_EXTENSIONS:
                        video_files.append(item)
            except OSError as e:
                log.warning(f"Error scanning {entry.name}: {e}")
            if video_files:
                results[entry.name] = video_files
        elif entry.is_file() and entry.suffix.lower() in VIDEO_EXTENSIONS:
            results[entry.name] = [entry]

    return results


# ---------------------------------------------------------------------------
# TMDb search (with scoring and caching)
# ---------------------------------------------------------------------------

def _score_tmdb_result(query_title: str, result: dict,
                       query_year: int | None,
                       name_key: str = "name",
                       date_key: str = "first_air_date",
                       search_rank: int = 0) -> float:
    """Score a single TMDB result against the query title/year.

    Uses word overlap (Jaccard) between the query and the result's
    name/original_name, plus year proximity and recency bonuses.
    """
    def _words(text: str) -> set[str]:
        return set(re.findall(r'[a-z0-9]+', text.lower()))

    query_words = _words(query_title)
    if not query_words:
        return 0.0

    name = result.get(name_key, "")
    original_name = result.get("original_name", result.get("original_title", ""))

    name_words = _words(name)
    orig_words = _words(original_name)

    overlap_name = len(query_words & name_words) / len(query_words | name_words) if query_words else 0
    overlap_orig = len(query_words & orig_words) / len(query_words | orig_words) if query_words else 0
    title_score = max(overlap_name, overlap_orig)

    # Year proximity
    year_score = 0.0
    if query_year:
        air_date = result.get(date_key, "")
        if air_date and len(air_date) >= 4:
            try:
                result_year = int(air_date[:4])
                diff = abs(result_year - query_year)
                if diff == 0:
                    year_score = 0.3
                elif diff <= 1:
                    year_score = 0.15
                elif diff <= 2:
                    year_score = 0.1
                else:
                    year_score = -0.5
            except ValueError:
                pass

    # Recency bias (when no year provided)
    recency_bonus = 0.0
    if not query_year:
        air_date = result.get(date_key, "")
        if air_date and len(air_date) >= 4:
            try:
                result_year = int(air_date[:4])
                years_ago = datetime.date.today().year - result_year
                if years_ago <= 2:
                    recency_bonus = 0.06
                elif years_ago <= 5:
                    recency_bonus = 0.04
                elif years_ago <= 10:
                    recency_bonus = 0.02
            except ValueError:
                pass

    pop_score = min(result.get("popularity", 0) / 500, 0.10)
    rank_bonus = max(0.0, 0.04 - search_rank * 0.002)

    return title_score + year_score + recency_bonus + pop_score + rank_bonus


def _tmdb_search(title: str, year: int | None, media_type: str,
                 _cache: dict | None = None) -> dict | None:
    """Search TMDb for a film or TV show.

    Args:
        media_type: "film" or "show"

    Returns {title, year, tmdb_id} or None.
    """
    cache_key = (title.lower(), year)
    if _cache is not None and cache_key in _cache:
        return _cache[cache_key]

    if not TMDB_API_KEY:
        return None

    if media_type == "film":
        endpoint = f"{TMDB_BASE}/search/movie"
        name_key, date_key = "title", "release_date"
        year_param = "year"
    else:
        endpoint = f"{TMDB_BASE}/search/tv"
        name_key, date_key = "name", "first_air_date"
        year_param = "first_air_date_year"

    params: dict = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params[year_param] = year

    try:
        resp = requests.get(endpoint, params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])

        # Retry without year filter if no results
        if not results and year:
            params_retry = {"api_key": TMDB_API_KEY, "query": title}
            resp = requests.get(endpoint, params=params_retry, timeout=10)
            resp.raise_for_status()
            results = resp.json().get("results", [])

        if results:
            scored = [
                (r, _score_tmdb_result(title, r, year,
                                       name_key=name_key,
                                       date_key=date_key,
                                       search_rank=i))
                for i, r in enumerate(results)
            ]
            scored.sort(key=lambda x: x[1], reverse=True)
            r = scored[0][0]

            date_str = r.get(date_key, "")
            result = {
                "title": r[name_key],
                "year": int(date_str[:4]) if date_str and len(date_str) >= 4 else year,
                "tmdb_id": r["id"],
            }
            if _cache is not None:
                _cache[cache_key] = result
            log.info(f"  TMDb API → {title} = {result['title']} "
                     f"({result['year']}) [tmdbid={result['tmdb_id']}]")
            return result
    except Exception as e:
        log.debug(f"TMDb {media_type} search failed for '{title}': {e}")
    return None


def tmdb_search_film(title: str, year: int | None = None,
                     _cache: dict | None = None) -> dict | None:
    """Search TMDb for a film. Returns {title, year, tmdb_id} or None."""
    return _tmdb_search(title, year, "film", _cache)


def tmdb_search_tv(title: str, year: int | None = None,
                   _cache: dict | None = None) -> dict | None:
    """Search TMDb for a TV show. Returns {title, year, tmdb_id} or None."""
    return _tmdb_search(title, year, "show", _cache)


# ---------------------------------------------------------------------------
# Phase A — Sync torrent records with zurg mount
# ---------------------------------------------------------------------------

def _build_rd_lookup(
    rd_client: RealDebridClient | None,
    torrent_entries: dict[str, list[Path]] | None = None,
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Build RD lookup dicts from the RD API.

    Returns (primary, reverse) where:
      primary  = {rd_filename → {hash, rd_id, rd_filename}}
      reverse  = {internal_basename → {hash, rd_id, rd_filename}}
    """
    empty: tuple[dict[str, dict], dict[str, dict]] = ({}, {})
    if rd_client is None:
        return empty

    primary: dict[str, dict] = {}
    reverse: dict[str, dict] = {}
    needs_info: list[dict] = []

    try:
        rd_torrents = rd_client.list_all_torrents()
        for t in rd_torrents:
            filename = t.get("filename", "")
            if not filename:
                continue

            meta = {
                "hash": t.get("hash", ""),
                "rd_id": t.get("id", ""),
                "rd_filename": filename,
            }
            primary[filename] = meta

            title_stem = Path(filename).stem
            is_meaningless = is_meaningless_title(title_stem)
            is_unmatched = torrent_entries is not None and filename not in torrent_entries
            if is_meaningless or is_unmatched:
                needs_info.append(meta)

        log.info(f"  RD API: fetched {len(primary)} torrent(s) for hash hydration")
    except Exception as e:
        log.warning(f"  RD API: failed to list torrents: {e}")
        return empty

    if needs_info:
        log.info(f"  RD API: enriching {len(needs_info)} torrent(s) via /torrents/info...")
        for meta in needs_info:
            try:
                info = rd_client.get_torrent_info(meta["rd_id"])
                if not info:
                    continue
                orig = info.get("original_filename", "")
                if orig and orig != meta["rd_filename"]:
                    log.info(f"  RD API: original_filename for "
                             f"{meta['rd_filename']!r} → {orig!r}")
                    meta["rd_filename"] = orig
                for f in info.get("files", []):
                    fpath = f.get("path", "")
                    if fpath:
                        basename = Path(fpath).name
                        if basename and basename not in reverse:
                            reverse[basename] = meta
            except Exception as e:
                log.debug(f"  RD API: failed to get info for {meta['rd_id']}: {e}")

        if reverse:
            log.info(f"  RD API: reverse index has {len(reverse)} file-name mapping(s)")

    return primary, reverse


def phase_a_sync_torrents(torrent_entries: dict[str, list[Path]],
                          rd_client: RealDebridClient | None = None) -> list[tuple[str, dict]]:
    """Sync zurg mount entries with PocketBase torrents.

    Returns a list of (folder_name, torrent_record) pairs needing identification.
    """
    needs_identification: list[tuple[str, dict]] = []
    rd_primary, rd_reverse = _build_rd_lookup(rd_client, torrent_entries)

    for folder_name, video_files in torrent_entries.items():
        torrent_path = str(ZURG_MOUNT / folder_name)
        existing = pb.get_torrent_by_path(torrent_path)

        # Resolve RD metadata
        rd_meta = rd_primary.get(folder_name) or rd_reverse.get(folder_name, {})
        if not rd_primary.get(folder_name) and rd_meta:
            log.info(f"  RD reverse match: {folder_name} → {rd_meta.get('rd_filename', '?')}")

        rd_hash = rd_meta.get("hash", "")
        rd_id = rd_meta.get("rd_id", "")
        rd_filename = rd_meta.get("rd_filename", "")

        if existing is None:
            score_name = rd_filename if rd_filename else folder_name
            score = score_quality(score_name)
            torrent = pb.create_torrent(
                name=folder_name, path=torrent_path, score=score,
                hash=rd_hash, rd_id=rd_id, rd_filename=rd_filename,
            )
            if torrent:
                log.info(f"  New torrent: {folder_name}  {format_score(score)}")
                needs_identification.append((folder_name, torrent))
            continue

        # Hydrate hash/rd_id/rd_filename if changed
        updates: dict = {}
        if rd_hash and existing.get("hash") != rd_hash:
            updates["hash"] = rd_hash
        if rd_id and existing.get("rd_id") != rd_id:
            updates["rd_id"] = rd_id
        if rd_filename and existing.get("rd_filename") != rd_filename:
            updates["rd_filename"] = rd_filename
            new_score = score_quality(rd_filename)
            if new_score > existing.get("score", 0):
                updates["score"] = new_score
        if existing.get("repair_attempts", 0) > 0:
            updates["repair_attempts"] = 0
        if updates:
            pb.update_torrent(existing["id"], **updates)
            existing.update(updates)

        if existing.get("archived") or existing.get("manual"):
            continue

        # Check if already has media
        if pb.list_films_by_torrent(existing["id"]) or pb.list_shows_by_torrent(existing["id"]):
            continue

        needs_identification.append((folder_name, existing))

    return needs_identification


# ---------------------------------------------------------------------------
# Phase B — Identify unidentified torrents
# ---------------------------------------------------------------------------

def _identify_film(folder_name: str, torrent: dict,
                   tmdb_cache: dict) -> bool:
    """Try to identify a torrent as a film. Returns True if identified."""
    guess = guessit(folder_name, {"type": "movie"})
    title = guess.get("title", folder_name)
    year = validate_year(guess.get("year"), folder_name)

    tmdb = tmdb_search_film(title, year, _cache=tmdb_cache)

    # Fallback: try rd_filename if primary name is meaningless
    if not tmdb and is_meaningless_title(title):
        rd_fn = torrent.get("rd_filename", "")
        if rd_fn:
            log.info(f"  Fallback to RD filename for film: {rd_fn}")
            guess = guessit(rd_fn, {"type": "movie"})
            title = guess.get("title", rd_fn)
            year = validate_year(guess.get("year"), rd_fn)
            tmdb = tmdb_search_film(title, year, _cache=tmdb_cache)

    if not tmdb:
        return False

    resolve_film_duplicate(
        pb,
        torrent_id=torrent["id"],
        torrent_score=torrent.get("score", 0),
        tmdb_id=tmdb["tmdb_id"],
        title=tmdb["title"],
        year=tmdb.get("year", year),
    )
    return True


def _identify_show(folder_name: str, video_files: list[Path],
                   torrent: dict, tmdb_cache: dict,
                   folder_cache: dict) -> bool:
    """Try to identify a torrent as a show. Returns True if identified."""
    torrent_id = torrent["id"]
    torrent_score = torrent.get("score", 0)

    # Resolve show identity via folder cache
    if folder_name in folder_cache:
        cached = folder_cache[folder_name]
        title, year, tmdb_id = cached["title"], cached.get("year"), cached.get("tmdb_id")
    else:
        folder_guess = guessit(folder_name, {"type": "episode"})
        title = folder_guess.get("title", folder_name)
        year = validate_year(folder_guess.get("year"), folder_name)

        tmdb = tmdb_search_tv(title, year, _cache=tmdb_cache)

        if not tmdb and is_meaningless_title(title):
            rd_fn = torrent.get("rd_filename", "")
            if rd_fn:
                log.info(f"  Fallback to RD filename for show: {rd_fn}")
                folder_guess = guessit(rd_fn, {"type": "episode"})
                title = folder_guess.get("title", rd_fn)
                year = validate_year(folder_guess.get("year"), rd_fn)
                tmdb = tmdb_search_tv(title, year, _cache=tmdb_cache)

        if tmdb:
            title, year, tmdb_id = tmdb["title"], tmdb.get("year", year), tmdb["tmdb_id"]
        else:
            tmdb_id = None

        folder_cache[folder_name] = {"title": title, "year": year, "tmdb_id": tmdb_id}

    if tmdb_id is None:
        return False

    tmdb_structure = tmdb_get_show_structure(tmdb_id, TMDB_API_KEY, TMDB_BASE)

    any_found = False
    all_lost = True

    for video_path in video_files:
        file_guess = guessit(video_path.name, {"type": "episode"})
        guessit_season = file_guess.get("season")
        guessit_episode = file_guess.get("episode")

        if guessit_season is None:
            torrent_root = Path(torrent.get("path", ""))
            guessit_season = extract_season_from_path(video_path, torrent_root)

        # Primary: TMDB structure matching
        matched_episodes = None
        if tmdb_structure:
            matched_episodes = match_file_to_tmdb_episode(
                video_path.name, guessit_season, guessit_episode, tmdb_structure,
            )

        if matched_episodes:
            for season, ep_num in matched_episodes:
                any_found = True
                result = resolve_episode_duplicate(
                    pb, torrent_id, torrent_score,
                    tmdb_id, title, year, season, ep_num,
                )
                if result != "lost":
                    all_lost = False
            continue

        # Fallback: guessit-only
        season = guessit_season
        episode = guessit_episode

        if season is None:
            fg_folder = guessit(folder_name, {"type": "episode"})
            season = fg_folder.get("season", 1)

        if episode is None:
            log.warning(f"  Skipping (no episode detected): {video_path.name}")
            continue

        episodes = episode if isinstance(episode, list) else [episode]
        for ep_num in episodes:
            any_found = True
            result = resolve_episode_duplicate(
                pb, torrent_id, torrent_score,
                tmdb_id, title, year, season, ep_num,
                label=" (fallback)",
            )
            if result != "lost":
                all_lost = False

    if not any_found:
        return False

    if all_lost:
        pb.update_torrent(torrent_id, archived=True)
        log.info(f"  Torrent archived (all episodes superseded): {folder_name}")

    return True


def phase_b_identify(needs_identification: list[tuple[str, dict]],
                     torrent_entries: dict[str, list[Path]]):
    """Identify unidentified torrents using guessit + TMDB."""
    if not needs_identification:
        log.info("  No torrents need identification")
        return

    log.info(f"Identifying {len(needs_identification)} torrent(s)...")

    tmdb_film_cache: dict = {}
    tmdb_tv_cache: dict = {}
    folder_cache: dict = {}

    for folder_name, torrent in needs_identification:
        video_files = torrent_entries.get(folder_name, [])
        media_type = _classify_torrent(folder_name, video_files)

        if media_type == "film":
            identified = _identify_film(folder_name, torrent, tmdb_film_cache)
        else:
            identified = _identify_show(
                folder_name, video_files, torrent, tmdb_tv_cache, folder_cache,
            )

        if not identified:
            # Try the other type as fallback
            if media_type == "film":
                identified = _identify_show(
                    folder_name, video_files, torrent, tmdb_tv_cache, folder_cache,
                )
            else:
                identified = _identify_film(folder_name, torrent, tmdb_film_cache)

        if not identified:
            pb.update_torrent(torrent["id"], manual=True)
            log.warning(f"  ✗ Could not identify: {folder_name} — marked for manual resolution")


# ---------------------------------------------------------------------------
# Phase C — Detect removed torrents
# ---------------------------------------------------------------------------

def _attempt_repair(torrent: dict, rd_client: RealDebridClient) -> bool:
    """Try to re-add a dead torrent to Real-Debrid via its cached hash."""
    torrent_hash = torrent.get("hash", "")
    name = torrent.get("name", torrent.get("id", "?"))
    old_rd_id = torrent.get("rd_id", "")

    if not torrent_hash:
        log.info(f"  Repair skipped (no hash cached): {name}")
        return False

    try:
        new_id = rd_client.add_magnet(torrent_hash)
        if not new_id:
            log.warning(f"  Repair failed (addMagnet returned nothing): {name}")
            return False

        rd_client.select_video_files(new_id)

        if old_rd_id and old_rd_id != new_id:
            rd_client.delete_torrent(old_rd_id)

        return True
    except Exception as e:
        log.warning(f"  Repair failed for {name}: {e}")
        return False


def phase_c_detect_removed(rd_client: RealDebridClient | None = None):
    """Detect torrents removed from zurg and attempt repair or clean up."""
    all_torrents = pb.list_all_torrents()
    removed_count = 0
    repaired_count = 0

    for torrent in all_torrents:
        torrent_path = torrent.get("path", "")
        if not torrent_path or Path(torrent_path).exists():
            continue

        torrent_id = torrent["id"]
        name = torrent.get("name", torrent_id)
        repair_attempts = torrent.get("repair_attempts", 0)

        # Try repair first
        if (REPAIR_ENABLED and rd_client is not None
                and torrent.get("hash")
                and repair_attempts < MAX_REPAIR_ATTEMPTS):
            log.info(f"  Attempting repair ({repair_attempts + 1}/{MAX_REPAIR_ATTEMPTS}): {name}")
            success = _attempt_repair(torrent, rd_client)
            pb.update_torrent(torrent_id, repair_attempts=repair_attempts + 1)

            if success:
                log.info(f"  ✓ Repaired torrent: {name} — waiting for Zurg to pick it up")
                repaired_count += 1
                continue
            elif repair_attempts + 1 < MAX_REPAIR_ATTEMPTS:
                continue

        # Orphan and delete
        for film in pb.list_films_by_torrent(torrent_id):
            pb.update_film(film["id"], torrent="")
            log.info(f"  Film orphaned (torrent removed): {film.get('title', '?')}")

        for show in pb.list_shows_by_torrent(torrent_id):
            pb.update_show(show["id"], torrent="")
            log.info(f"  Show orphaned (torrent removed): "
                     f"{show.get('title', '?')} S{show.get('season', 0):02d}E{show.get('episode', 0):02d}")

        pb.delete_torrent(torrent_id)
        log.info(f"  ✗ Torrent removed from RD: {name}")
        removed_count += 1

    if repaired_count:
        log.info(f"  Repaired {repaired_count} torrent(s) — they will reappear on next scan")
    if removed_count:
        log.info(f"  Cleaned up {removed_count} removed torrent(s)")


# ---------------------------------------------------------------------------
# Phase D — Build symlinks from PocketBase state
# ---------------------------------------------------------------------------

def _find_best_video_file(video_files: list[Path]) -> Path | None:
    """Pick the main video file from a list (largest file = main feature)."""
    if not video_files:
        return None
    if len(video_files) == 1:
        return video_files[0]
    return max(video_files, key=lambda f: f.stat().st_size)


def _match_episode_file(video_files: list[Path], season: int,
                        episode: int, torrent_path: str = "",
                        tmdb_structure: ShowStructure | None = None) -> Path | None:
    """Find the video file matching a specific season/episode."""
    torrent_root = Path(torrent_path) if torrent_path else None

    for vf in video_files:
        fg = guessit(vf.name, {"type": "episode"})
        file_season = fg.get("season")
        file_episode = fg.get("episode")

        if file_season is None and torrent_root:
            file_season = extract_season_from_path(vf, torrent_root)

        if tmdb_structure:
            matched = match_file_to_tmdb_episode(
                vf.name, file_season, file_episode, tmdb_structure,
            )
            if matched and (season, episode) in matched:
                return vf
            if matched:
                continue

        if file_episode is None:
            continue
        file_episodes = file_episode if isinstance(file_episode, list) else [file_episode]
        if episode in file_episodes and (file_season is None or file_season == season):
            return vf

    return None


def _resolve_symlink_target(source: Path) -> str:
    """Compute the symlink target string for a source file."""
    try:
        relative_to_zurg = source.relative_to(ZURG_MOUNT)
        return str(JELLYFIN_ZURG_PATH / relative_to_zurg)
    except ValueError:
        return str(source)


def _collect_existing_symlinks(directory: Path) -> dict[Path, str]:
    """Scan a media directory and return {symlink_path: link_target}."""
    existing: dict[Path, str] = {}
    if not directory.exists():
        return existing
    for item in directory.rglob("*"):
        if item.is_symlink():
            existing[item] = os.readlink(item)
    return existing


def _prune_empty_dirs(directory: Path):
    """Remove empty directories bottom-up."""
    if not directory.exists():
        return
    for dirpath in sorted(directory.rglob("*"), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()


def _compute_desired_state() -> dict[Path, str]:
    """Build the desired symlink map from PocketBase state."""
    desired: dict[Path, str] = {}

    # Films
    for film in pb.list_all_films():
        torrent_id = film.get("torrent")
        if not torrent_id:
            continue
        torrent = (film.get("expand") or {}).get("torrent")
        if not torrent:
            torrent = pb.get_torrent_by_id(torrent_id)
        if not torrent:
            continue

        video_files = get_video_files(torrent.get("path", ""))
        main_file = _find_best_video_file(video_files)
        if not main_file:
            continue

        title = film.get("title", "Unknown")
        year = film.get("year") or None
        tmdb_id = film.get("tmdb_id")

        name = format_media_name(title, year, tmdb_id)
        target_path = FILMS_DIR / name / f"{name}{main_file.suffix}"
        desired[target_path] = _resolve_symlink_target(main_file)

    # Shows
    torrent_files_cache: dict[str, list[Path]] = {}
    tmdb_structure_cache: dict[int, ShowStructure | None] = {}

    for show in pb.list_all_shows():
        torrent_id = show.get("torrent")
        if not torrent_id:
            continue
        torrent = (show.get("expand") or {}).get("torrent")
        if not torrent:
            torrent = pb.get_torrent_by_id(torrent_id)
        if not torrent:
            continue

        if torrent_id not in torrent_files_cache:
            torrent_files_cache[torrent_id] = get_video_files(torrent.get("path", ""))
        video_files = torrent_files_cache[torrent_id]

        season = show.get("season", 1)
        episode = show.get("episode", 1)

        show_tmdb_id = show.get("tmdb_id")
        tmdb_struct = None
        if show_tmdb_id:
            if show_tmdb_id not in tmdb_structure_cache:
                tmdb_structure_cache[show_tmdb_id] = tmdb_get_show_structure(
                    show_tmdb_id, TMDB_API_KEY, TMDB_BASE,
                )
            tmdb_struct = tmdb_structure_cache[show_tmdb_id]

        matched_file = _match_episode_file(
            video_files, season, episode,
            torrent.get("path", ""), tmdb_structure=tmdb_struct,
        )
        if not matched_file:
            continue

        title = show.get("title", "Unknown")
        year = show.get("year") or None
        tmdb_id = show.get("tmdb_id")

        show_name = format_media_name(title, year, tmdb_id)
        season_dir = SHOWS_DIR / show_name / f"Season {season:02d}"
        episode_name = format_episode(title, year, season, episode)
        target_path = season_dir / f"{episode_name}{matched_file.suffix}"
        desired[target_path] = _resolve_symlink_target(matched_file)

    return desired


def phase_d_build_symlinks() -> dict:
    """Incrementally sync symlinks from PocketBase state.

    Returns {"films_changed": bool, "shows_changed": bool}.
    """
    log.info("Syncing symlinks...")

    desired = _compute_desired_state()

    on_disk = {
        **_collect_existing_symlinks(FILMS_DIR),
        **_collect_existing_symlinks(SHOWS_DIR),
    }

    created = updated = removed = 0
    films_changed = shows_changed = False

    # Create or update
    for target_path, link_target in desired.items():
        if target_path in on_disk:
            if on_disk[target_path] == link_target:
                continue
            target_path.unlink()
            target_path.symlink_to(link_target)
            log.info(f"  ↻ {target_path.relative_to(MEDIA_DIR)} → {link_target}")
            updated += 1
        else:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.symlink_to(link_target)
            log.info(f"  ✓ {target_path.relative_to(MEDIA_DIR)} → {link_target}")
            created += 1

        # Track which libraries changed
        if str(target_path).startswith(str(FILMS_DIR)):
            films_changed = True
        elif str(target_path).startswith(str(SHOWS_DIR)):
            shows_changed = True

    # Remove stale
    for target_path in on_disk:
        if target_path not in desired:
            target_path.unlink()
            log.info(f"  ✗ {target_path.relative_to(MEDIA_DIR)}")
            removed += 1
            if str(target_path).startswith(str(FILMS_DIR)):
                films_changed = True
            elif str(target_path).startswith(str(SHOWS_DIR)):
                shows_changed = True

    _prune_empty_dirs(FILMS_DIR)
    _prune_empty_dirs(SHOWS_DIR)

    if created or updated or removed:
        log.info(f"  Symlinks: {created} created, {updated} updated, {removed} removed")
    else:
        log.info("  Symlinks up to date — no changes")

    return {"films_changed": films_changed, "shows_changed": shows_changed}


# ---------------------------------------------------------------------------
# Phase E — Clean up archived torrents
# ---------------------------------------------------------------------------

def phase_e_cleanup_archived(rd: RealDebridClient | None):
    """Delete archived torrents from Real-Debrid and PocketBase."""
    archived = pb.list_archived_torrents()
    if not archived:
        log.info("  No archived torrents to clean up")
        return

    log.info(f"  Found {len(archived)} archived torrent(s) to clean up")
    deleted = 0

    for torrent in archived:
        name = torrent.get("name", torrent["id"])
        rd_id = torrent.get("rd_id", "")

        if rd and rd_id:
            rd.delete_torrent(rd_id)

        pb.delete_torrent(torrent["id"])
        log.info(f"  🗑 {name}")
        deleted += 1

    log.info(f"  Cleaned up {deleted} archived torrent(s)")


# ---------------------------------------------------------------------------
# Main scan cycle
# ---------------------------------------------------------------------------

# Global RD client — initialised in main() if API key is available
rd_client: RealDebridClient | None = None

# Webhook event
_scan_event = threading.Event()


def run_scan():
    """Run a single scan cycle through all phases."""
    log.info("Starting scan...")

    torrent_entries = _scan_zurg_mount()
    log.info(f"Found {len(torrent_entries)} torrent(s) on Zurg mount")

    log.info("Phase A: Syncing torrent records...")
    needs_identification = phase_a_sync_torrents(torrent_entries, rd_client)

    log.info("Phase B: Identifying torrents...")
    phase_b_identify(needs_identification, torrent_entries)

    log.info("Phase C: Detecting removed torrents...")
    phase_c_detect_removed(rd_client)

    log.info("Phase D: Building symlinks...")
    changes = phase_d_build_symlinks()

    if changes:
        jellyfin_mod.trigger_refresh(changes["films_changed"], changes["shows_changed"])

    if CLEANUP_ARCHIVED:
        log.info("Phase E: Cleaning up archived torrents...")
        phase_e_cleanup_archived(rd_client)

    # Summary counts
    all_t = pb.list_all_torrents()
    active = sum(1 for t in all_t if not t.get("archived") and not t.get("manual"))
    archived = sum(1 for t in all_t if t.get("archived"))
    manual = sum(1 for t in all_t if t.get("manual"))
    films = len(pb.list_all_films())
    shows = len(pb.list_all_shows())
    log.info(f"Scan complete. {len(all_t)} torrent(s) "
             f"({active} active, {archived} archived, {manual} manual), "
             f"{films} film(s), {shows} episode(s)")


def wait_for_pocketbase():
    """Wait for PocketBase to become available."""
    log.info(f"Waiting for PocketBase at {POCKETBASE_URL}...")
    for _ in range(60):
        if pb.health_check():
            log.info("PocketBase is ready")
            return True
        time.sleep(2)
    log.warning("PocketBase not available after 2 minutes")
    return False


def main():
    """Entry point — run scan loop."""
    global rd_client

    log.info("=" * 60)
    log.info("Media Organiser starting")
    log.info(f"  Zurg mount:     {ZURG_MOUNT}")
    log.info(f"  Jellyfin path:  {JELLYFIN_ZURG_PATH}")
    log.info(f"  Media output:   {MEDIA_DIR}")
    log.info(f"  TMDb API:       {'enabled' if TMDB_API_KEY else 'disabled (set TMDB_API_KEY)'}")
    log.info(f"  PocketBase:     {POCKETBASE_URL}")
    log.info(f"  Scan interval:  {SCAN_INTERVAL}s")
    log.info(f"  Jellyfin:       {'enabled' if JELLYFIN_API_KEY else 'disabled (set JELLYFIN_API_KEY)'}")
    log.info(f"  Webhook port:   {WEBHOOK_PORT}")

    if REAL_DEBRID_API_KEY and REPAIR_ENABLED:
        rd_client = RealDebridClient(
            api_key=REAL_DEBRID_API_KEY,
            min_file_size_mb=MIN_VIDEO_FILE_SIZE_MB,
        )
        log.info(f"  Auto-repair:    enabled (max {MAX_REPAIR_ATTEMPTS} attempts, "
                 f"min file size {MIN_VIDEO_FILE_SIZE_MB}MB)")
    elif not REAL_DEBRID_API_KEY:
        log.info("  Auto-repair:    disabled (set REAL_DEBRID_API_KEY to enable)")
    else:
        log.info("  Auto-repair:    disabled (REPAIR_ENABLED=false)")

    log.info("=" * 60)

    FILMS_DIR.mkdir(parents=True, exist_ok=True)
    SHOWS_DIR.mkdir(parents=True, exist_ok=True)

    wait_for_pocketbase()

    # Wait for Zurg mount
    log.info("Waiting for Zurg mount...")
    for _ in range(60):
        if ZURG_MOUNT.exists() and any(ZURG_MOUNT.iterdir()):
            log.info("Zurg mount detected")
            break
        time.sleep(5)
    else:
        log.warning("Zurg mount not detected after 5 minutes, starting anyway")

    webhook_mod.start_server(_scan_event, WEBHOOK_PORT)

    run_scan()

    while True:
        log.info(f"Next scan in {SCAN_INTERVAL}s (or on webhook trigger)...")
        triggered = _scan_event.wait(timeout=SCAN_INTERVAL)
        _scan_event.clear()
        if triggered:
            log.info("Scan triggered by webhook")
        try:
            run_scan()
        except Exception as e:
            log.error(f"Scan failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
