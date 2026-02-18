#!/usr/bin/env python3
"""
Media Organiser — creates Jellyfin-compatible symlinks from a Zurg/rclone mount.

Reads the raw torrent-named files from /zurg/films/ and /zurg/shows/, parses
them with guessit, verifies against TMDb (with PocketBase caching), and creates
a clean symlink tree at /media/films/ and /media/shows/ following Jellyfin
naming conventions.

Jellyfin naming conventions:
  Films: /media/films/Film Name (Year) [tmdbid=XXXXX]/Film Name (Year) [tmdbid=XXXXX].ext
  Shows: /media/shows/Show Name (Year) [tmdbid=XXXXX]/Season XX/Show Name (Year) SXXEXX.ext

TMDB lookups are cached in PocketBase so each title is only queried once.
All source→target mappings are stored in PocketBase for recovery/rebuild.

Environment variables:
  TMDB_API_KEY        — TMDb API key for name verification (optional but recommended)
  SCAN_INTERVAL_SECS  — seconds between scans (default: 300)
  POCKETBASE_URL      — PocketBase API URL (default: http://pocketbase:8090)
  REBUILD_MODE        — set to "true" to rebuild symlinks from DB and exit
  PUID / PGID         — not used directly (symlinks don't have ownership issues)
"""

import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests
from guessit import guessit

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ZURG_MOUNT = Path("/zurg")
MEDIA_DIR = Path("/media")
STATE_FILE = Path("/app/data/state.json")

# The path where the Zurg mount appears inside Jellyfin's container.
JELLYFIN_ZURG_PATH = Path(os.environ.get("JELLYFIN_ZURG_PATH", "/zurg"))

FILMS_DIR = MEDIA_DIR / "films"
SHOWS_DIR = MEDIA_DIR / "shows"

ZURG_FILMS = ZURG_MOUNT / "films"
ZURG_SHOWS = ZURG_MOUNT / "shows"

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL_SECS", "300"))

POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "http://pocketbase:8090")
REBUILD_MODE = os.environ.get("REBUILD_MODE", "").lower() == "true"

TMDB_BASE = "https://api.themoviedb.org/3"

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".vob", ".iso", ".m2ts",
}

# Characters that are not allowed in filenames (Jellyfin restriction)
UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*]')

# ---------------------------------------------------------------------------
# Quality scoring — when duplicates exist, the highest score wins
# ---------------------------------------------------------------------------

RESOLUTION_SCORES = {
    "4320p": 100,  # 8K
    "2160p": 90,   # 4K
    "1080p": 70,
    "1080i": 65,
    "720p":  50,
    "576p":  30,
    "480p":  20,
    "360p":  10,
}

SOURCE_SCORES = {
    "Blu-ray":    60,
    "Ultra HD Blu-ray": 65,
    "HD-DVD":     55,
    "Web":        40,
    "HDTV":       35,
    "PDTV":       25,
    "SDTV":       20,
    "DVD":        30,
    "VHS":        5,
    "Telecine":   10,
    "Telesync":   8,
    "Workprint":  3,
    "Camera":     1,
}

CODEC_SCORES = {
    "H.265": 30,
    "HEVC":  30,
    "H.264": 20,
    "AVC":   20,
    "VP9":   18,
    "AV1":   35,
    "MPEG-2": 5,
    "XviD":   3,
    "DivX":   3,
}

# Bonus points for various quality markers
REMUX_BONUS = 25       # Remux = untouched disc stream
HDR_BONUS = 15         # Any HDR (HDR10, HDR10+, Dolby Vision, HLG)
ATMOS_BONUS = 10       # Dolby Atmos / DTS:X
LOSSLESS_AUDIO_BONUS = 8  # DTS-HD MA, TrueHD, FLAC, PCM


def score_quality(name: str) -> int:
    """Score a torrent/file name by quality. Higher = better."""
    guess = guessit(name)
    score = 0

    # Resolution
    res = guess.get("screen_size", "")
    score += RESOLUTION_SCORES.get(res, 0)

    # Source
    source = guess.get("source", "")
    if isinstance(source, list):
        score += max(SOURCE_SCORES.get(s, 0) for s in source)
    else:
        score += SOURCE_SCORES.get(source, 0)

    # Video codec
    codec = guess.get("video_codec", "")
    score += CODEC_SCORES.get(codec, 0)

    # Remux bonus
    name_upper = name.upper()
    if "REMUX" in name_upper:
        score += REMUX_BONUS

    # HDR bonus
    other = guess.get("other", [])
    if not isinstance(other, list):
        other = [other]
    hdr_terms = {"HDR10", "HDR10+", "HDR", "Dolby Vision", "DV", "HLG", "HDR10Plus"}
    if any(o in hdr_terms for o in other) or any(t in name_upper for t in ("HDR", "DV", "DOLBY.VISION")):
        score += HDR_BONUS

    # Lossless audio bonus
    audio = guess.get("audio_codec", "")
    if isinstance(audio, list):
        audio = " ".join(audio)
    audio_str = f"{audio} {name_upper}"
    if any(t in audio_str for t in ("DTS-HD", "DTS-HD MA", "TRUEHD", "TRUE HD", "FLAC", "PCM", "LPCM")):
        score += LOSSLESS_AUDIO_BONUS

    # Atmos / DTS:X bonus
    if "ATMOS" in name_upper or "DTS:X" in name_upper or "DTS-X" in name_upper:
        score += ATMOS_BONUS

    return score


def format_score(score: int) -> str:
    """Human-readable quality score label."""
    if score >= 200:
        return f"★★★★★ ({score})"
    if score >= 150:
        return f"★★★★ ({score})"
    if score >= 100:
        return f"★★★ ({score})"
    if score >= 50:
        return f"★★ ({score})"
    return f"★ ({score})"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("organiser")


# ---------------------------------------------------------------------------
# PocketBase client
# ---------------------------------------------------------------------------

class PocketBaseClient:
    """Lightweight PocketBase REST API client for the organiser."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api"
        self._session = requests.Session()

    def _url(self, collection: str, record_id: str = "") -> str:
        url = f"{self.api}/collections/{collection}/records"
        if record_id:
            url += f"/{record_id}"
        return url

    # --- TMDB collection ---

    def get_tmdb(self, tmdb_id: int, media_type: str) -> dict | None:
        """Look up a canonical TMDB record by tmdb_id and type."""
        try:
            filt = f'tmdb_id = {tmdb_id} && type = "{media_type}"'
            resp = self._session.get(
                self._url("tmdb"),
                params={"filter": filt, "perPage": 1},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                return items[0]
        except Exception as e:
            log.debug(f"PocketBase tmdb query failed: {e}")
        return None

    def upsert_tmdb(self, tmdb_id: int, media_type: str,
                    title: str, year: int | None) -> dict | None:
        """Create or update a tmdb record; return the record (with its PocketBase id)."""
        data = {
            "tmdb_id": tmdb_id,
            "type": media_type,
            "title": title,
            "year": year or 0,
        }
        try:
            existing = self.get_tmdb(tmdb_id, media_type)
            if existing:
                resp = self._session.patch(
                    self._url("tmdb", existing["id"]),
                    json=data,
                    timeout=5,
                )
            else:
                resp = self._session.post(self._url("tmdb"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase upsert tmdb failed: {e}")
        return None

    # --- Films collection ---

    def get_film(self, source_path: str) -> dict | None:
        """Look up a film record by source path."""
        try:
            filt = f'source_path = "{self._escape(source_path)}"'
            resp = self._session.get(
                self._url("films"),
                params={"filter": filt, "perPage": 1},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                return items[0]
        except Exception as e:
            log.debug(f"PocketBase films query failed: {e}")
        return None

    def upsert_film(self, source_path: str, target_path: str,
                    tmdb_row_id: str, score: int = 0) -> dict | None:
        """Create or update a film record."""
        data = {
            "source_path": source_path,
            "target_path": target_path,
            "tmdb": tmdb_row_id,
            "score": score,
        }
        try:
            existing = self.get_film(source_path)
            if existing:
                resp = self._session.patch(
                    self._url("films", existing["id"]), json=data, timeout=5,
                )
            else:
                resp = self._session.post(self._url("films"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase upsert film failed: {e}")
        return None

    def delete_film(self, record_id: str):
        """Delete a film record by PocketBase row ID."""
        try:
            self._session.delete(self._url("films", record_id), timeout=5).raise_for_status()
        except Exception as e:
            log.debug(f"PocketBase delete film failed: {e}")

    def list_all_films(self) -> list[dict]:
        """Fetch all film records (paginated), expanding the tmdb relation."""
        return self._paginate("films", expand="tmdb")

    # --- Shows collection ---

    def get_show(self, source_path: str) -> dict | None:
        """Look up a show record by source path."""
        try:
            filt = f'source_path = "{self._escape(source_path)}"'
            resp = self._session.get(
                self._url("shows"),
                params={"filter": filt, "perPage": 1},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                return items[0]
        except Exception as e:
            log.debug(f"PocketBase shows query failed: {e}")
        return None

    def upsert_show(self, source_path: str, target_path: str,
                    tmdb_row_id: str, season: int | None = None,
                    episode: int | None = None) -> dict | None:
        """Create or update a show record."""
        data = {
            "source_path": source_path,
            "target_path": target_path,
            "tmdb": tmdb_row_id,
            "season": season or 0,
            "episode": episode or 0,
        }
        try:
            existing = self.get_show(source_path)
            if existing:
                resp = self._session.patch(
                    self._url("shows", existing["id"]), json=data, timeout=5,
                )
            else:
                resp = self._session.post(self._url("shows"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase upsert show failed: {e}")
        return None

    def delete_show(self, record_id: str):
        """Delete a show record by PocketBase row ID."""
        try:
            self._session.delete(self._url("shows", record_id), timeout=5).raise_for_status()
        except Exception as e:
            log.debug(f"PocketBase delete show failed: {e}")

    def list_all_shows(self) -> list[dict]:
        """Fetch all show records (paginated), expanding the tmdb relation."""
        return self._paginate("shows", expand="tmdb")

    # --- Helpers ---

    def _paginate(self, collection: str, expand: str = "") -> list[dict]:
        items = []
        page = 1
        params: dict = {"perPage": 200, "page": page}
        if expand:
            params["expand"] = expand
        while True:
            try:
                params["page"] = page
                resp = self._session.get(self._url(collection), params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                items.extend(data.get("items", []))
                if page >= data.get("totalPages", 1):
                    break
                page += 1
            except Exception as e:
                log.warning(f"PocketBase list {collection} failed: {e}")
                break
        return items

    def health_check(self) -> bool:
        """Check if PocketBase is reachable."""
        try:
            resp = self._session.get(f"{self.base_url}/api/health", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    @staticmethod
    def _escape(value: str) -> str:
        """Escape a string for PocketBase filter syntax."""
        return value.replace("\\", "\\\\").replace('"', '\\"')


# Global PocketBase client
pb = PocketBaseClient(POCKETBASE_URL)


# ---------------------------------------------------------------------------
# TMDb lookup (with PocketBase caching)
# ---------------------------------------------------------------------------

def tmdb_search_film(title: str, year: int | None = None,
                     _cache: dict | None = None) -> dict | None:
    """Search TMDb for a film, return {title, year, tmdb_id} or None.

    Checks the in-memory cache first (keyed on parsed title, per scan cycle),
    then queries the TMDb API. On a hit the result is persisted to PocketBase
    (upserted by tmdb_id + type so it is never duplicated).
    """
    if _cache is not None and title.lower() in _cache:
        return _cache[title.lower()]

    # No persistent pre-lookup by title — go straight to TMDb API
    if not TMDB_API_KEY:
        return None

    params = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params["year"] = year
    try:
        resp = requests.get(f"{TMDB_BASE}/search/movie", params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            r = results[0]
            release = r.get("release_date", "")
            result = {
                "title": r["title"],
                "year": int(release[:4]) if release and len(release) >= 4 else year,
                "tmdb_id": r["id"],
            }
            # Persist / refresh in PocketBase (keyed on tmdb_id + type)
            pb.upsert_tmdb(
                tmdb_id=r["id"],
                media_type="film",
                title=r["title"],
                year=result["year"],
            )
            if _cache is not None:
                _cache[title.lower()] = result
            log.info(f"  TMDb API → {title} = {result['title']} ({result['year']}) [tmdbid={result['tmdb_id']}]")
            return result
    except Exception as e:
        log.debug(f"TMDb film search failed for '{title}': {e}")
    return None


def tmdb_search_tv(title: str, year: int | None = None,
                   _cache: dict | None = None) -> dict | None:
    """Search TMDb for a TV show, return {title, year, tmdb_id} or None.

    The in-memory _cache dict deduplicates lookups within a single scan cycle
    (e.g. 20 episodes of the same show share one cached result).
    """
    if _cache is not None and title.lower() in _cache:
        return _cache[title.lower()]

    if not TMDB_API_KEY:
        return None

    params = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params["first_air_date_year"] = year
    try:
        resp = requests.get(f"{TMDB_BASE}/search/tv", params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            r = results[0]
            air_date = r.get("first_air_date", "")
            result = {
                "title": r["name"],
                "year": int(air_date[:4]) if air_date and len(air_date) >= 4 else year,
                "tmdb_id": r["id"],
            }
            # Persist / refresh in PocketBase (keyed on tmdb_id + type)
            pb.upsert_tmdb(
                tmdb_id=r["id"],
                media_type="show",
                title=r["name"],
                year=result["year"],
            )
            if _cache is not None:
                _cache[title.lower()] = result
            log.info(f"  TMDb API → {title} = {result['title']} ({result['year']}) [tmdbid={result['tmdb_id']}]")
            return result
    except Exception as e:
        log.debug(f"TMDb TV search failed for '{title}': {e}")
    return None


# ---------------------------------------------------------------------------
# Name sanitisation
# ---------------------------------------------------------------------------

def sanitise(name: str) -> str:
    """Remove characters that Jellyfin doesn't allow in filenames."""
    name = UNSAFE_CHARS.sub("", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.rstrip(". ")
    return name


def format_film_name(title: str, year: int | None, tmdb_id: int | None = None) -> str:
    """Format: Film Name (Year) [tmdbid=XXXXX]"""
    title = sanitise(title)
    parts = [title]
    if year:
        parts.append(f"({year})")
    if tmdb_id:
        parts.append(f"[tmdbid={tmdb_id}]")
    return " ".join(parts)


def format_show_name(title: str, year: int | None, tmdb_id: int | None = None) -> str:
    """Format: Show Name (Year) [tmdbid=XXXXX]"""
    title = sanitise(title)
    parts = [title]
    if year:
        parts.append(f"({year})")
    if tmdb_id:
        parts.append(f"[tmdbid={tmdb_id}]")
    return " ".join(parts)


def format_episode(title: str, year: int | None, season: int, episode: int | list) -> str:
    """Format: Show Name (Year) SXXEXX  (no tmdbid in episode filename)"""
    base = sanitise(title)
    if year:
        base = f"{base} ({year})"
    if isinstance(episode, list):
        ep_str = "".join(f"E{e:02d}" for e in episode)
    else:
        ep_str = f"E{episode:02d}"
    return f"{base} S{season:02d}{ep_str}"


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def find_video_files(directory: Path) -> list[Path]:
    """Recursively find all video files in a directory."""
    if not directory.exists():
        return []
    files = []
    try:
        for item in directory.rglob("*"):
            if item.is_file() and item.suffix.lower() in VIDEO_EXTENSIONS:
                files.append(item)
    except OSError as e:
        log.warning(f"Error scanning {directory}: {e}")
    return files


# ---------------------------------------------------------------------------
# Symlink management
# ---------------------------------------------------------------------------

def create_symlink(source: Path, target: Path):
    """Create a symlink at target pointing to source, creating parent dirs."""
    try:
        relative_to_zurg = source.relative_to(ZURG_MOUNT)
        symlink_target = JELLYFIN_ZURG_PATH / relative_to_zurg
    except ValueError:
        symlink_target = source

    if target.exists() or target.is_symlink():
        if target.is_symlink() and os.readlink(target) == str(symlink_target):
            return
        target.unlink()

    target.parent.mkdir(parents=True, exist_ok=True)
    target.symlink_to(symlink_target)
    log.info(f"  ✓ {target.relative_to(MEDIA_DIR)} → {symlink_target}")


def cleanup_broken_symlinks(directory: Path):
    """Remove symlinks whose target no longer exists, then prune empty dirs."""
    if not directory.exists():
        return

    removed = 0
    for item in directory.rglob("*"):
        if item.is_symlink() and not item.resolve().exists():
            log.info(f"  ✗ Removing broken symlink: {item.relative_to(MEDIA_DIR)}")
            item.unlink()
            removed += 1

    if removed:
        log.info(f"  Cleaned up {removed} broken symlink(s)")

    # Prune empty directories (bottom-up)
    for dirpath in sorted(directory.rglob("*"), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()
            log.debug(f"  Removed empty dir: {dirpath}")


# ---------------------------------------------------------------------------
# Processing logic
# ---------------------------------------------------------------------------

def process_films(state: dict) -> dict:
    """Process the Zurg films directory and create film symlinks.

    TMDB lookups are cached in PocketBase. Each unique film title is only
    looked up once, ever (across reboots).
    """
    processed = state.get("films", {})
    video_files = find_video_files(ZURG_FILMS)

    if not video_files:
        log.info("  No video files found in films directory")
        return processed

    # In-memory cache for this scan cycle (avoids repeated PocketBase queries)
    tmdb_cache: dict[str, dict] = {}

    # Collect candidates grouped by target path
    candidates: dict[str, list] = {}

    for video_path in video_files:
        relative = video_path.relative_to(ZURG_FILMS)
        if len(relative.parts) > 1:
            guess_name = relative.parts[0]
        else:
            guess_name = video_path.stem

        guess = guessit(guess_name, {"type": "movie"})
        title = guess.get("title", guess_name)
        year = guess.get("year")
        tmdb_id = None

        # Check if already fully processed with same target
        source_key = str(video_path)
        if source_key in processed:
            existing = processed[source_key]
            # Fast path: if source is tracked and nothing changed, reuse it
            title = existing.get("title", title)
            year = existing.get("year", year)
            tmdb_id = existing.get("tmdb_id")

        # TMDB lookup (cached via PocketBase + in-memory per scan)
        if tmdb_id is None:
            tmdb = tmdb_search_film(title, year, _cache=tmdb_cache)
            if tmdb:
                title = tmdb["title"]
                year = tmdb.get("year", year)
                tmdb_id = tmdb.get("tmdb_id")

        film_name = format_film_name(title, year, tmdb_id)
        target_file = FILMS_DIR / film_name / f"{film_name}{video_path.suffix}"
        target_str = str(target_file)

        score = score_quality(guess_name)

        if target_str not in candidates:
            candidates[target_str] = []
        candidates[target_str].append((video_path, score, guess_name, title, year, tmdb_id))

    # For each target, pick the best candidate
    new_processed = {}
    for target_str, options in candidates.items():
        target_file = Path(target_str)

        if len(options) > 1:
            options.sort(key=lambda x: x[1], reverse=True)
            best = options[0]
            log.info(f"  Film: {best[3]} — {len(options)} versions found, picking best:")
            for src, sc, gn, *_ in options:
                marker = "→" if src == best[0] else " "
                log.info(f"    {marker} {format_score(sc)}  {gn}")
        else:
            best = options[0]

        video_path, score, guess_name, title, year, tmdb_id = best
        source_key = str(video_path)

        if len(options) == 1:
            log.info(f"  Film: {guess_name}  {format_score(score)}")

        # create_symlink is idempotent — no-ops if the symlink already exists and is correct
        create_symlink(video_path, target_file)

        entry = {
            "title": title,
            "year": year,
            "tmdb_id": tmdb_id,
            "target": target_str,
            "score": score,
        }
        new_processed[source_key] = entry

        # Always upsert to PocketBase so it stays in sync even after a data wipe
        if tmdb_id is not None:
            tmdb_record = pb.upsert_tmdb(tmdb_id, "film", title, year)
            if tmdb_record:
                pb.upsert_film(
                    source_path=source_key,
                    target_path=target_str,
                    tmdb_row_id=tmdb_record["id"],
                    score=score,
                )

    return new_processed


def process_shows(state: dict) -> dict:
    """Process the Zurg shows directory and create TV show symlinks.

    TMDB lookups are cached in PocketBase. All episodes of the same show
    share one cached TMDB lookup (both in-memory per scan and in PocketBase
    across scans).
    """
    processed = state.get("shows", {})
    video_files = find_video_files(ZURG_SHOWS)

    if not video_files:
        log.info("  No video files found in shows directory")
        return processed

    # In-memory cache for this scan cycle
    tmdb_cache: dict[str, dict] = {}

    # Collect candidates grouped by target path
    candidates: dict[str, list] = {}

    for video_path in video_files:
        relative = video_path.relative_to(ZURG_SHOWS)
        if len(relative.parts) > 1:
            guess_name = relative.parts[0]
            full_guess = f"{relative.parts[0]} {video_path.name}"
        else:
            guess_name = video_path.stem
            full_guess = video_path.name

        guess = guessit(full_guess, {"type": "episode"})
        title = guess.get("title", guess_name)
        year = guess.get("year")
        season = guess.get("season", 1)
        episode = guess.get("episode")

        if episode is None:
            guess2 = guessit(video_path.name, {"type": "episode"})
            episode = guess2.get("episode")
            if not title or title == guess_name:
                title = guess2.get("title", title)
            if not year:
                year = guess2.get("year")
            season = guess2.get("season", season)

        if episode is None:
            log.warning(f"  Skipping (no episode detected): {video_path.name}")
            continue

        tmdb_id = None

        # Check if already fully processed
        source_key = str(video_path)
        if source_key in processed:
            existing = processed[source_key]
            title = existing.get("title", title)
            year = existing.get("year", year)
            tmdb_id = existing.get("tmdb_id")

        # TMDB lookup (cached — one lookup per show title, shared by all episodes)
        if tmdb_id is None:
            tmdb = tmdb_search_tv(title, year, _cache=tmdb_cache)
            if tmdb:
                title = tmdb["title"]
                year = tmdb.get("year", year)
                tmdb_id = tmdb.get("tmdb_id")

        show_name = format_show_name(title, year, tmdb_id)
        season_dir = SHOWS_DIR / show_name / f"Season {season:02d}"
        episode_name = format_episode(title, year, season, episode)
        target_file = season_dir / f"{episode_name}{video_path.suffix}"
        target_str = str(target_file)

        score = score_quality(video_path.name)

        if target_str not in candidates:
            candidates[target_str] = []
        candidates[target_str].append((video_path, score, title, year, season, episode, tmdb_id))

    # For each target, pick the best candidate
    new_processed = {}
    for target_str, options in candidates.items():
        target_file = Path(target_str)

        if len(options) > 1:
            options.sort(key=lambda x: x[1], reverse=True)
            best = options[0]
            log.info(f"  Show: {best[2]} S{best[4]:02d} — {len(options)} versions, picking best:")
            for src, sc, *_ in options:
                marker = "→" if src == best[0] else " "
                log.info(f"    {marker} {format_score(sc)}  {src.name}")
        else:
            best = options[0]

        video_path, score, title, year, season, episode, tmdb_id = best
        source_key = str(video_path)

        if len(options) == 1:
            log.info(f"  Show: {video_path.name}  {format_score(score)}")

        # create_symlink is idempotent — no-ops if the symlink already exists and is correct
        create_symlink(video_path, target_file)

        ep_value = episode if isinstance(episode, int) else list(episode)
        ep_for_db = episode if isinstance(episode, int) else episode[0]

        entry = {
            "title": title,
            "year": year,
            "tmdb_id": tmdb_id,
            "season": season,
            "episode": ep_value,
            "target": target_str,
            "score": score,
        }
        new_processed[source_key] = entry

        # Always upsert to PocketBase so it stays in sync even after a data wipe
        if tmdb_id is not None:
            tmdb_record = pb.upsert_tmdb(tmdb_id, "show", title, year)
            if tmdb_record:
                pb.upsert_show(
                    source_path=source_key,
                    target_path=target_str,
                    tmdb_row_id=tmdb_record["id"],
                    season=season,
                    episode=ep_for_db,
                )

    return new_processed


# ---------------------------------------------------------------------------
# State persistence (kept as fallback alongside PocketBase)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    """Load the processing state from disk."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            log.warning("Corrupt state file, starting fresh")
    return {"films": {}, "shows": {}}


def save_state(state: dict):
    """Persist the processing state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Rebuild mode — recreate all symlinks from PocketBase without TMDB calls
# ---------------------------------------------------------------------------

def run_rebuild():
    """Rebuild all symlinks from PocketBase films/shows records.

    This mode does NOT query TMDB at all. It reads every record from
    PocketBase, verifies that the source file still exists on the Zurg
    mount, and recreates the symlink.
    """
    log.info("=" * 60)
    log.info("REBUILD MODE — recreating symlinks from PocketBase")
    log.info("=" * 60)

    all_items = pb.list_all_films() + pb.list_all_shows()
    if not all_items:
        log.warning("No media items found in PocketBase. Nothing to rebuild.")
        return

    log.info(f"Found {len(all_items)} media item(s) in PocketBase")

    rebuilt = 0
    skipped = 0
    missing = 0

    for item in all_items:
        source = Path(item["source_path"])
        target = Path(item["target_path"])

        if not source.exists():
            log.warning(f"  ✗ Source missing: {source}")
            missing += 1
            continue

        if target.exists() or target.is_symlink():
            if target.is_symlink():
                # Already linked — skip
                skipped += 1
                continue
            target.unlink()

        create_symlink(source, target)
        rebuilt += 1

    log.info("=" * 60)
    log.info(f"Rebuild complete: {rebuilt} created, {skipped} already linked, {missing} source(s) missing")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Sync state from PocketBase (bootstrap local state from DB)
# ---------------------------------------------------------------------------

def sync_state_from_pocketbase() -> dict:
    """Build local state dict from PocketBase films/shows.

    This allows the organiser to bootstrap its in-memory state from PocketBase
    if state.json is lost or empty. The tmdb relation is expanded so we can
    read title/year without a separate lookup.
    """
    state = {"films": {}, "shows": {}}

    for item in pb.list_all_films():
        tmdb_exp = (item.get("expand") or {}).get("tmdb", {})
        state["films"][item["source_path"]] = {
            "title": tmdb_exp.get("title", ""),
            "year": tmdb_exp.get("year"),
            "tmdb_id": tmdb_exp.get("tmdb_id"),
            "target": item.get("target_path", ""),
            "score": item.get("score", 0),
        }

    for item in pb.list_all_shows():
        tmdb_exp = (item.get("expand") or {}).get("tmdb", {})
        state["shows"][item["source_path"]] = {
            "title": tmdb_exp.get("title", ""),
            "year": tmdb_exp.get("year"),
            "tmdb_id": tmdb_exp.get("tmdb_id"),
            "season": item.get("season"),
            "episode": item.get("episode"),
            "target": item.get("target_path", ""),
        }

    return state


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_scan():
    """Run a single scan cycle."""
    log.info("Starting scan...")

    state = load_state()

    # If local state is empty but PocketBase has data, sync from PocketBase
    if not state.get("films") and not state.get("shows"):
        pb_state = sync_state_from_pocketbase()
        if pb_state.get("films") or pb_state.get("shows"):
            log.info(f"Bootstrapped state from PocketBase: "
                     f"{len(pb_state.get('films', {}))} films, "
                     f"{len(pb_state.get('shows', {}))} shows")
            state = pb_state

    # Clean up broken symlinks first
    log.info("Checking for broken symlinks...")
    cleanup_broken_symlinks(FILMS_DIR)
    cleanup_broken_symlinks(SHOWS_DIR)

    # Purge state entries whose sources no longer exist
    stale_films = [k for k in state.get("films", {}) if not Path(k).exists()]
    for k in stale_films:
        pb_item = pb.get_film(k)
        if pb_item:
            pb.delete_film(pb_item["id"])
        del state["films"][k]

    stale_shows = [k for k in state.get("shows", {}) if not Path(k).exists()]
    for k in stale_shows:
        pb_item = pb.get_show(k)
        if pb_item:
            pb.delete_show(pb_item["id"])
        del state["shows"][k]

    # Process new content
    log.info("Processing films...")
    state["films"] = process_films(state)

    log.info("Processing shows...")
    state["shows"] = process_shows(state)

    save_state(state)

    total = len(state.get("films", {})) + len(state.get("shows", {}))
    log.info(f"Scan complete. Tracking {total} item(s) "
             f"({len(state.get('films', {}))} films, {len(state.get('shows', {}))} shows)")


def wait_for_pocketbase():
    """Wait for PocketBase to become available."""
    log.info(f"Waiting for PocketBase at {POCKETBASE_URL}...")
    for attempt in range(60):
        if pb.health_check():
            log.info("PocketBase is ready")
            return True
        time.sleep(2)
    log.warning("PocketBase not available after 2 minutes, continuing without caching")
    return False


def main():
    """Entry point — run scan loop."""
    log.info("=" * 60)
    log.info("Media Organiser starting")
    log.info(f"  Zurg mount:     {ZURG_MOUNT}")
    log.info(f"  Jellyfin path:  {JELLYFIN_ZURG_PATH}")
    log.info(f"  Media output:   {MEDIA_DIR}")
    log.info(f"  TMDb API:       {'enabled' if TMDB_API_KEY else 'disabled (set TMDB_API_KEY for better naming)'}")
    log.info(f"  PocketBase:     {POCKETBASE_URL}")
    log.info(f"  Rebuild mode:   {REBUILD_MODE}")
    log.info(f"  Scan interval:  {SCAN_INTERVAL}s")
    log.info("=" * 60)

    # Ensure output directories exist
    FILMS_DIR.mkdir(parents=True, exist_ok=True)
    SHOWS_DIR.mkdir(parents=True, exist_ok=True)

    # Wait for PocketBase
    wait_for_pocketbase()

    # Rebuild mode: recreate symlinks from PocketBase and exit
    if REBUILD_MODE:
        # Wait for Zurg mount (needed to verify source files)
        log.info("Waiting for Zurg mount...")
        for attempt in range(60):
            if ZURG_MOUNT.exists() and any(ZURG_MOUNT.iterdir()):
                log.info("Zurg mount detected")
                break
            time.sleep(5)
        else:
            log.warning("Zurg mount not detected after 5 minutes, starting rebuild anyway")

        run_rebuild()
        log.info("Rebuild mode complete — exiting.")
        return

    # Wait for Zurg mount to become available
    log.info("Waiting for Zurg mount...")
    for attempt in range(60):
        if ZURG_MOUNT.exists() and any(ZURG_MOUNT.iterdir()):
            log.info("Zurg mount detected")
            break
        time.sleep(5)
    else:
        log.warning("Zurg mount not detected after 5 minutes, starting anyway")

    # Initial scan
    run_scan()

    # Continuous loop
    while True:
        log.info(f"Next scan in {SCAN_INTERVAL}s...")
        time.sleep(SCAN_INTERVAL)
        try:
            run_scan()
        except Exception as e:
            log.error(f"Scan failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
