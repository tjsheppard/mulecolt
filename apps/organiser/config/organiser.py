#!/usr/bin/env python3
"""
Media Organiser — torrent lifecycle manager.

Manages the full lifecycle of torrents from Real-Debrid: scanning, identifying,
deduplicating by quality score, and creating Jellyfin-compatible symlinks.

PocketBase is the single source of truth. No local state files.

Schema (PocketBase):
  torrents:  id, name, path, score, archived, manual
  films:     id, torrent (relation), tmdb_id, title, year
  shows:     id, torrent (relation), tmdb_id, title, year, season, episode

Workflow per scan:
  Phase A — Sync torrents with zurg mount
  Phase B — Identify unidentified torrents (guessit + TMDB)
  Phase C — Detect removed torrents (path no longer on zurg)
  Phase D — Build symlinks from PocketBase state

Unidentifiable torrents are flagged manual=true. Use resolve.sh to fix them.

Environment variables:
  TMDB_API_KEY        — TMDb API key (required for identification)
  SCAN_INTERVAL_SECS  — seconds between scans (default: 300)
  POCKETBASE_URL      — PocketBase API URL (default: http://pocketbase:8090)
"""

import datetime
import logging
import os
import re
import time
from pathlib import Path

import requests
from guessit import guessit

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ZURG_MOUNT = Path("/zurg")
MEDIA_DIR = Path("/media")

# The path where the Zurg mount appears inside Jellyfin's container.
JELLYFIN_ZURG_PATH = Path(os.environ.get("JELLYFIN_ZURG_PATH", "/zurg"))

FILMS_DIR = MEDIA_DIR / "films"
SHOWS_DIR = MEDIA_DIR / "shows"

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL_SECS", "300"))

POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "http://pocketbase:8090")

TMDB_BASE = "https://api.themoviedb.org/3"

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".vob", ".iso", ".m2ts",
}

# Characters that are not allowed in filenames (Jellyfin restriction)
UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*]')

# Year plausibility range for validation
_CURRENT_YEAR = datetime.date.today().year
_MIN_YEAR = 1920
_MAX_YEAR = _CURRENT_YEAR + 1

# Patterns that indicate a TV show (season/episode markers)
_SHOW_PATTERNS = [
    re.compile(r'[Ss]\d{1,2}[Ee]\d{1,3}'),            # S01E01
    re.compile(r'[Ss]\d{1,2}'),                         # S01
    re.compile(r'[Ss]eason[\s._-]?\d', re.IGNORECASE),  # Season 1
    re.compile(r'[Ee]\d{2,3}'),                          # E01, E001
    re.compile(r'Episode[\s._-]?\d', re.IGNORECASE),     # Episode 1
    re.compile(r'\bComplete[\s._-]?Series\b', re.IGNORECASE),
    re.compile(r'\bBatch\b', re.IGNORECASE),
    re.compile(r'\b\d{1,2}x\d{2}\b'),                   # 1x01 format
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("organiser")


# ---------------------------------------------------------------------------
# Year validation
# ---------------------------------------------------------------------------

def validate_year(year: int | None, reference_text: str | None = None) -> int | None:
    """Validate an extracted year for plausibility.

    Returns the year if it passes validation, otherwise None.

    Rules:
      - Must be within [1920, current_year + 1]
      - If reference_text is provided (e.g. the folder name), the year must
        appear literally in that text.  This prevents guessit from extracting
        years embedded in episode titles (e.g. "To You, in 2000 Years").
    """
    if year is None:
        return None
    if not (_MIN_YEAR <= year <= _MAX_YEAR):
        return None
    if reference_text is not None and str(year) not in reference_text:
        return None
    return year


# ---------------------------------------------------------------------------
# Season-from-path helper
# ---------------------------------------------------------------------------

_SEASON_DIR_PATTERN = re.compile(r'(?:Season|S)\s*(\d+)', re.IGNORECASE)


def _extract_season_from_path(video_path: Path, torrent_root: Path) -> int | None:
    """Extract season number from a file's parent directory structure.

    Looks at the relative path between the torrent root and the file for
    directory names like 'Season 1', 'Season 02', 'S3', etc.
    """
    try:
        rel = video_path.relative_to(torrent_root)
    except ValueError:
        return None
    for part in rel.parts[:-1]:  # skip the filename itself
        m = _SEASON_DIR_PATTERN.search(part)
        if m:
            return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Media classification — show vs film
# ---------------------------------------------------------------------------

def _classify_torrent(folder_name: str, files: list[Path]) -> str:
    """Classify a torrent entry as 'show' or 'film'.

    Strategy:
      1. Check folder name for show patterns (S01E01, Season, etc.)
      2. Check individual filenames for episode patterns
      3. Multiple video files → likely a show (season pack)
      4. Fall back to 'film'

    Returns 'show' or 'film'.
    """
    # Check folder name for show patterns
    for pattern in _SHOW_PATTERNS:
        if pattern.search(folder_name):
            return "show"

    # Check filenames — if most files have episode markers, it's a show
    if files:
        filenames = [f.name for f in files[:20]]  # sample first 20
        ep_count = 0
        for fn in filenames:
            for pattern in _SHOW_PATTERNS:
                if pattern.search(fn):
                    ep_count += 1
                    break
        if ep_count > len(filenames) / 2:
            return "show"

        # Multiple video files usually means a season pack
        video_count = sum(1 for f in files if f.suffix.lower() in VIDEO_EXTENSIONS)
        if video_count > 3:
            return "show"

    return "film"


def _scan_zurg_mount() -> dict[str, list[Path]]:
    """Scan the flat Zurg mount and return {folder_name: [file_paths]}.

    Handles both:
      - Torrent folders: /zurg/Some.Show.S01/episode1.mkv
      - Loose files:     /zurg/Some.Movie.2023.mkv
    """
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
            # Torrent folder — collect all video files recursively
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
            # Loose file — the filename is the "folder name"
            results[entry.name] = [entry]

    return results


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


# ---------------------------------------------------------------------------
# PocketBase client — new torrent-centric schema
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

    # --- Torrents collection ---

    def get_torrent_by_path(self, path: str) -> dict | None:
        """Look up a torrent record by its zurg path."""
        try:
            filt = f'path = "{self._escape(path)}"'
            resp = self._session.get(
                self._url("torrents"),
                params={"filter": filt, "perPage": 1},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                return items[0]
        except Exception as e:
            log.debug(f"PocketBase torrent query failed: {e}")
        return None

    def get_torrent_by_id(self, record_id: str) -> dict | None:
        """Look up a torrent record by PocketBase ID."""
        try:
            resp = self._session.get(self._url("torrents", record_id), timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase torrent get failed: {e}")
        return None

    def create_torrent(self, name: str, path: str, score: int = 0) -> dict | None:
        """Create a new torrent record."""
        data = {
            "name": name,
            "path": path,
            "score": score,
            "archived": False,
            "manual": False,
        }
        try:
            resp = self._session.post(self._url("torrents"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase create torrent failed: {e}")
        return None

    def update_torrent(self, record_id: str, **fields) -> dict | None:
        """Update fields on a torrent record."""
        try:
            resp = self._session.patch(
                self._url("torrents", record_id), json=fields, timeout=5,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase update torrent failed: {e}")
        return None

    def delete_torrent(self, record_id: str):
        """Delete a torrent record."""
        try:
            self._session.delete(self._url("torrents", record_id), timeout=5).raise_for_status()
        except Exception as e:
            log.debug(f"PocketBase delete torrent failed: {e}")

    def list_all_torrents(self) -> list[dict]:
        """Fetch all torrent records."""
        return self._paginate("torrents")

    # --- Films collection ---

    def get_film_by_tmdb(self, tmdb_id: int) -> dict | None:
        """Look up a film record by TMDB ID."""
        try:
            filt = f'tmdb_id = {tmdb_id}'
            resp = self._session.get(
                self._url("films"),
                params={"filter": filt, "perPage": 1, "expand": "torrent"},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                return items[0]
        except Exception as e:
            log.debug(f"PocketBase film query failed: {e}")
        return None

    def create_film(self, torrent_id: str, tmdb_id: int,
                    title: str, year: int | None) -> dict | None:
        """Create a new film record."""
        data = {
            "torrent": torrent_id,
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year or 0,
        }
        try:
            resp = self._session.post(self._url("films"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase create film failed: {e}")
        return None

    def update_film(self, record_id: str, **fields) -> dict | None:
        """Update fields on a film record."""
        try:
            resp = self._session.patch(
                self._url("films", record_id), json=fields, timeout=5,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase update film failed: {e}")
        return None

    def list_all_films(self) -> list[dict]:
        """Fetch all film records, expanding the torrent relation."""
        return self._paginate("films", expand="torrent")

    def list_films_by_torrent(self, torrent_id: str) -> list[dict]:
        """Find all film records linked to a specific torrent."""
        try:
            filt = f'torrent = "{self._escape(torrent_id)}"'
            resp = self._session.get(
                self._url("films"),
                params={"filter": filt, "perPage": 200},
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json().get("items", [])
        except Exception as e:
            log.debug(f"PocketBase films by torrent query failed: {e}")
        return []

    # --- Shows collection ---

    def get_show_episode(self, tmdb_id: int, season: int,
                         episode: int) -> dict | None:
        """Look up a show episode by (tmdb_id, season, episode)."""
        try:
            filt = f'tmdb_id = {tmdb_id} && season = {season} && episode = {episode}'
            resp = self._session.get(
                self._url("shows"),
                params={"filter": filt, "perPage": 1, "expand": "torrent"},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                return items[0]
        except Exception as e:
            log.debug(f"PocketBase show episode query failed: {e}")
        return None

    def create_show(self, torrent_id: str, tmdb_id: int, title: str,
                    year: int | None, season: int,
                    episode: int) -> dict | None:
        """Create a new show episode record."""
        data = {
            "torrent": torrent_id,
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year or 0,
            "season": season,
            "episode": episode,
        }
        try:
            resp = self._session.post(self._url("shows"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase create show failed: {e}")
        return None

    def update_show(self, record_id: str, **fields) -> dict | None:
        """Update fields on a show record."""
        try:
            resp = self._session.patch(
                self._url("shows", record_id), json=fields, timeout=5,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug(f"PocketBase update show failed: {e}")
        return None

    def list_all_shows(self) -> list[dict]:
        """Fetch all show records, expanding the torrent relation."""
        return self._paginate("shows", expand="torrent")

    def list_shows_by_torrent(self, torrent_id: str) -> list[dict]:
        """Find all show records linked to a specific torrent."""
        try:
            filt = f'torrent = "{self._escape(torrent_id)}"'
            resp = self._session.get(
                self._url("shows"),
                params={"filter": filt, "perPage": 200},
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json().get("items", [])
        except Exception as e:
            log.debug(f"PocketBase shows by torrent query failed: {e}")
        return []

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
# TMDb lookup
# ---------------------------------------------------------------------------

def tmdb_search_film(title: str, year: int | None = None,
                     _cache: dict | None = None) -> dict | None:
    """Search TMDb for a film, return {title, year, tmdb_id} or None.

    Uses an in-memory cache per scan cycle to avoid duplicate API calls.
    """
    if _cache is not None and title.lower() in _cache:
        return _cache[title.lower()]

    if not TMDB_API_KEY:
        return None

    params = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params["year"] = year
    try:
        resp = requests.get(f"{TMDB_BASE}/search/movie", params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])

        # If year-filtered search returned nothing, retry without year filter
        if not results and year:
            params_no_year = {"api_key": TMDB_API_KEY, "query": title}
            resp = requests.get(f"{TMDB_BASE}/search/movie", params=params_no_year, timeout=10)
            resp.raise_for_status()
            results = resp.json().get("results", [])

        if results:
            scored = [
                (r, _score_tmdb_result(title, r, year,
                                       name_key="title",
                                       date_key="release_date"))
                for r in results
            ]
            scored.sort(key=lambda x: x[1], reverse=True)
            r = scored[0][0]

            release = r.get("release_date", "")
            result = {
                "title": r["title"],
                "year": int(release[:4]) if release and len(release) >= 4 else year,
                "tmdb_id": r["id"],
            }
            if _cache is not None:
                _cache[title.lower()] = result
            log.info(f"  TMDb API → {title} = {result['title']} ({result['year']}) [tmdbid={result['tmdb_id']}]")
            return result
    except Exception as e:
        log.debug(f"TMDb film search failed for '{title}': {e}")
    return None


def _score_tmdb_result(query_title: str, result: dict,
                       query_year: int | None,
                       name_key: str = "name",
                       date_key: str = "first_air_date") -> float:
    """Score a single TMDB result against the query title/year.

    Higher score = better match.  Uses word overlap between the query and
    the result's name/original_name, plus year proximity if available.
    """
    def _words(text: str) -> set[str]:
        return set(re.findall(r'[a-z0-9]+', text.lower()))

    query_words = _words(query_title)
    if not query_words:
        return 0.0

    # Check both the localised name and the original name
    name = result.get(name_key, "")
    original_name = result.get("original_name", result.get("original_title", ""))

    name_words = _words(name)
    orig_words = _words(original_name)

    # Best word overlap ratio (Jaccard-ish: intersection / query size)
    overlap_name = len(query_words & name_words) / len(query_words) if query_words else 0
    overlap_orig = len(query_words & orig_words) / len(query_words) if query_words else 0
    title_score = max(overlap_name, overlap_orig)

    # Year proximity bonus (0 to 0.2)
    year_score = 0.0
    if query_year:
        air_date = result.get(date_key, "")
        if air_date and len(air_date) >= 4:
            try:
                result_year = int(air_date[:4])
                diff = abs(result_year - query_year)
                if diff == 0:
                    year_score = 0.2
                elif diff <= 2:
                    year_score = 0.1
            except ValueError:
                pass

    # Slight popularity tiebreaker (normalised, max 0.05)
    popularity = result.get("popularity", 0)
    pop_score = min(popularity / 1000, 0.05)

    return title_score + year_score + pop_score


def tmdb_search_tv(title: str, year: int | None = None,
                   _cache: dict | None = None) -> dict | None:
    """Search TMDb for a TV show, return {title, year, tmdb_id} or None.

    Uses an in-memory cache per scan cycle to avoid duplicate API calls.
    All results are scored by title similarity, year proximity, and popularity.
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

        # If year-filtered search returned nothing, retry without year filter
        if not results and year:
            params_no_year = {"api_key": TMDB_API_KEY, "query": title}
            resp = requests.get(f"{TMDB_BASE}/search/tv", params=params_no_year, timeout=10)
            resp.raise_for_status()
            results = resp.json().get("results", [])

        if results:
            scored = [
                (r, _score_tmdb_result(title, r, year,
                                       name_key="name",
                                       date_key="first_air_date"))
                for r in results
            ]
            scored.sort(key=lambda x: x[1], reverse=True)
            r = scored[0][0]

            air_date = r.get("first_air_date", "")
            result = {
                "title": r["name"],
                "year": int(air_date[:4]) if air_date and len(air_date) >= 4 else year,
                "tmdb_id": r["id"],
            }
            if _cache is not None:
                _cache[title.lower()] = result
            log.info(f"  TMDb API → {title} = {result['title']} ({result['year']}) [tmdbid={result['tmdb_id']}]")
            return result
    except Exception as e:
        log.debug(f"TMDb TV search failed for '{title}': {e}")
    return None


# ---------------------------------------------------------------------------
# Name sanitisation & formatting
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
# Phase A — Sync torrent records with zurg mount
# ---------------------------------------------------------------------------

def _get_torrent_path(folder_name: str) -> str:
    """Compute the canonical torrent path from a folder name."""
    return str(ZURG_MOUNT / folder_name)


def _torrent_has_media(torrent_id: str) -> bool:
    """Check if a torrent has any associated films or shows in PocketBase."""
    films = pb.list_films_by_torrent(torrent_id)
    if films:
        return True
    shows = pb.list_shows_by_torrent(torrent_id)
    if shows:
        return True
    return False


def phase_a_sync_torrents(torrent_entries: dict[str, list[Path]]) -> list[tuple[str, dict]]:
    """Sync zurg mount entries with PocketBase torrents.

    Returns a list of (folder_name, torrent_record) pairs that need identification.
    """
    needs_identification: list[tuple[str, dict]] = []

    for folder_name, video_files in torrent_entries.items():
        torrent_path = _get_torrent_path(folder_name)
        existing = pb.get_torrent_by_path(torrent_path)

        if existing is None:
            # New torrent — create record and queue for identification
            score = score_quality(folder_name)
            torrent = pb.create_torrent(name=folder_name, path=torrent_path, score=score)
            if torrent:
                log.info(f"  New torrent: {folder_name}  {format_score(score)}")
                needs_identification.append((folder_name, torrent))
            continue

        # Existing torrent
        if existing.get("archived"):
            # Archived — skip
            continue

        if existing.get("manual"):
            # Awaiting manual resolution — skip
            continue

        # Active torrent — check if it already has media linked
        if _torrent_has_media(existing["id"]):
            # Already indexed — skip
            continue

        # Has a record but no media — needs identification
        needs_identification.append((folder_name, existing))

    return needs_identification


# ---------------------------------------------------------------------------
# Phase B — Identify unidentified torrents
# ---------------------------------------------------------------------------

def _identify_film(folder_name: str, torrent: dict,
                   tmdb_cache: dict) -> bool:
    """Try to identify a torrent as a film and handle duplicate resolution.

    Returns True if identified, False otherwise.
    """
    guess = guessit(folder_name, {"type": "movie"})
    title = guess.get("title", folder_name)
    year = validate_year(guess.get("year"), folder_name)

    tmdb = tmdb_search_film(title, year, _cache=tmdb_cache)
    if not tmdb:
        return False

    title = tmdb["title"]
    year = tmdb.get("year", year)
    tmdb_id = tmdb["tmdb_id"]
    torrent_id = torrent["id"]
    torrent_score = torrent.get("score", 0)

    # Check if this film already exists
    existing_film = pb.get_film_by_tmdb(tmdb_id)

    if existing_film:
        # Film exists — compare scores
        existing_torrent_id = existing_film.get("torrent")
        if not existing_torrent_id:
            # Existing film has no torrent (was removed) — take it over
            pb.update_film(existing_film["id"], torrent=torrent_id)
            log.info(f"  Film: {title} ({year}) — re-linked to torrent {folder_name}")
            return True

        # Get the existing torrent's score
        existing_torrent = (existing_film.get("expand") or {}).get("torrent")
        if not existing_torrent:
            existing_torrent = pb.get_torrent_by_id(existing_torrent_id)
        existing_score = existing_torrent.get("score", 0) if existing_torrent else 0

        if torrent_score > existing_score:
            # New torrent wins — swap
            pb.update_film(existing_film["id"], torrent=torrent_id)
            pb.update_torrent(existing_torrent_id, archived=True)
            log.info(f"  Film: {title} ({year}) — new torrent wins "
                     f"({format_score(torrent_score)} > {format_score(existing_score)})")
        else:
            # Existing torrent wins — archive the new one
            pb.update_torrent(torrent_id, archived=True)
            log.info(f"  Film: {title} ({year}) — existing torrent wins "
                     f"({format_score(existing_score)} >= {format_score(torrent_score)}), archiving new")
    else:
        # New film — create it
        pb.create_film(torrent_id=torrent_id, tmdb_id=tmdb_id, title=title, year=year)
        log.info(f"  Film: {title} ({year}) [tmdbid={tmdb_id}]  {format_score(torrent_score)}")

    return True


def _identify_show(folder_name: str, video_files: list[Path],
                   torrent: dict, tmdb_cache: dict,
                   folder_cache: dict) -> bool:
    """Try to identify a torrent as a show and handle duplicate resolution.

    Returns True if identified, False otherwise.
    """
    torrent_id = torrent["id"]
    torrent_score = torrent.get("score", 0)

    # Resolve show identity — use folder cache to ensure consistency
    if folder_name in folder_cache:
        cached = folder_cache[folder_name]
        title = cached["title"]
        year = cached.get("year")
        tmdb_id = cached.get("tmdb_id")
    else:
        folder_guess = guessit(folder_name, {"type": "episode"})
        title = folder_guess.get("title", folder_name)
        year = validate_year(folder_guess.get("year"), folder_name)

        tmdb = tmdb_search_tv(title, year, _cache=tmdb_cache)
        if tmdb:
            title = tmdb["title"]
            year = tmdb.get("year", year)
            tmdb_id = tmdb["tmdb_id"]
        else:
            tmdb_id = None

        folder_cache[folder_name] = {
            "title": title,
            "year": year,
            "tmdb_id": tmdb_id,
        }

    if tmdb_id is None:
        return False

    # Process each video file as a potential episode
    any_episode_found = False
    all_episodes_lost = True  # track if this torrent loses every episode

    for video_path in video_files:
        file_guess = guessit(video_path.name, {"type": "episode"})
        season = file_guess.get("season")
        episode = file_guess.get("episode")

        # Try parent directory for season (e.g. /Season 2/03) Foo.mkv)
        if season is None:
            torrent_root = Path(torrent.get("path", ""))
            season = _extract_season_from_path(video_path, torrent_root)

        # Fall back to folder name, then default to 1
        if season is None:
            folder_guess_ep = guessit(folder_name, {"type": "episode"})
            season = folder_guess_ep.get("season", 1)

        if episode is None:
            log.warning(f"  Skipping (no episode detected): {video_path.name}")
            continue

        # Handle multi-episode files (e.g. S01E01E02)
        episodes = episode if isinstance(episode, list) else [episode]

        for ep_num in episodes:
            any_episode_found = True
            existing = pb.get_show_episode(tmdb_id, season, ep_num)

            if existing:
                # Episode exists — compare scores
                existing_torrent_id = existing.get("torrent")
                if not existing_torrent_id:
                    # No torrent linked — take it over
                    pb.update_show(existing["id"], torrent=torrent_id)
                    all_episodes_lost = False
                    log.info(f"  Show: {title} S{season:02d}E{ep_num:02d} — re-linked")
                    continue

                existing_torrent = (existing.get("expand") or {}).get("torrent")
                if not existing_torrent:
                    existing_torrent = pb.get_torrent_by_id(existing_torrent_id)
                existing_score = existing_torrent.get("score", 0) if existing_torrent else 0

                if torrent_score > existing_score:
                    # New torrent wins this episode
                    pb.update_show(existing["id"], torrent=torrent_id)
                    all_episodes_lost = False
                    log.info(f"  Show: {title} S{season:02d}E{ep_num:02d} — new torrent wins "
                             f"({format_score(torrent_score)} > {format_score(existing_score)})")
                    # Check if the old torrent still has any episodes
                    _maybe_archive_torrent(existing_torrent_id)
                else:
                    # Existing wins — this episode stays with old torrent
                    log.info(f"  Show: {title} S{season:02d}E{ep_num:02d} — existing wins "
                             f"({format_score(existing_score)} >= {format_score(torrent_score)})")
            else:
                # New episode — create it
                pb.create_show(
                    torrent_id=torrent_id, tmdb_id=tmdb_id,
                    title=title, year=year,
                    season=season, episode=ep_num,
                )
                all_episodes_lost = False
                log.info(f"  Show: {title} S{season:02d}E{ep_num:02d} [tmdbid={tmdb_id}]")

    if not any_episode_found:
        return False

    # If this torrent lost every episode contest, archive it
    if all_episodes_lost:
        pb.update_torrent(torrent_id, archived=True)
        log.info(f"  Torrent archived (all episodes superseded): {folder_name}")

    return True


def _maybe_archive_torrent(torrent_id: str):
    """Archive a torrent if it no longer provides any episodes or films."""
    films = pb.list_films_by_torrent(torrent_id)
    if films:
        return
    shows = pb.list_shows_by_torrent(torrent_id)
    if shows:
        return
    # No media linked — archive it
    pb.update_torrent(torrent_id, archived=True)
    torrent = pb.get_torrent_by_id(torrent_id)
    name = torrent.get("name", torrent_id) if torrent else torrent_id
    log.info(f"  Torrent archived (no media remaining): {name}")


def phase_b_identify(needs_identification: list[tuple[str, dict]],
                     torrent_entries: dict[str, list[Path]]):
    """Identify unidentified torrents using guessit + TMDB."""
    if not needs_identification:
        log.info("  No torrents need identification")
        return

    log.info(f"Identifying {len(needs_identification)} torrent(s)...")

    # In-memory caches for this scan cycle
    tmdb_film_cache: dict[str, dict] = {}
    tmdb_tv_cache: dict[str, dict] = {}
    folder_cache: dict[str, dict] = {}

    for folder_name, torrent in needs_identification:
        video_files = torrent_entries.get(folder_name, [])
        media_type = _classify_torrent(folder_name, video_files)

        if media_type == "film":
            identified = _identify_film(folder_name, torrent, tmdb_film_cache)
        else:
            identified = _identify_show(
                folder_name, video_files, torrent,
                tmdb_tv_cache, folder_cache,
            )

        if not identified:
            # Could not identify — try the other type as fallback
            if media_type == "film":
                identified = _identify_show(
                    folder_name, video_files, torrent,
                    tmdb_tv_cache, folder_cache,
                )
            else:
                identified = _identify_film(folder_name, torrent, tmdb_film_cache)

        if not identified:
            # Still can't identify — mark for manual resolution
            pb.update_torrent(torrent["id"], manual=True)
            log.warning(f"  ✗ Could not identify: {folder_name} — marked for manual resolution")


# ---------------------------------------------------------------------------
# Phase C — Detect removed torrents
# ---------------------------------------------------------------------------

def phase_c_detect_removed():
    """Detect torrents removed from zurg and clean up PocketBase."""
    all_torrents = pb.list_all_torrents()
    removed_count = 0

    for torrent in all_torrents:
        torrent_path = torrent.get("path", "")
        if not torrent_path:
            continue

        path = Path(torrent_path)
        if path.exists():
            continue

        # Torrent no longer on zurg — null out relations and delete
        torrent_id = torrent["id"]
        name = torrent.get("name", torrent_id)

        # Null out torrent relation on films
        for film in pb.list_films_by_torrent(torrent_id):
            pb.update_film(film["id"], torrent="")
            log.info(f"  Film orphaned (torrent removed): {film.get('title', '?')}")

        # Null out torrent relation on shows
        for show in pb.list_shows_by_torrent(torrent_id):
            pb.update_show(show["id"], torrent="")
            log.info(f"  Show orphaned (torrent removed): "
                     f"{show.get('title', '?')} S{show.get('season', 0):02d}E{show.get('episode', 0):02d}")

        # Delete the torrent record
        pb.delete_torrent(torrent_id)
        log.info(f"  ✗ Torrent removed from RD: {name}")
        removed_count += 1

    if removed_count:
        log.info(f"  Cleaned up {removed_count} removed torrent(s)")


# ---------------------------------------------------------------------------
# Phase D — Build symlinks from PocketBase state
# ---------------------------------------------------------------------------

def _get_video_files_for_torrent(torrent: dict) -> list[Path]:
    """Get video files from a torrent's path on the zurg mount."""
    torrent_path = Path(torrent.get("path", ""))
    if not torrent_path.exists():
        return []

    if torrent_path.is_file():
        # Loose file
        if torrent_path.suffix.lower() in VIDEO_EXTENSIONS:
            return [torrent_path]
        return []

    # Directory — find all video files
    video_files = []
    try:
        for item in torrent_path.rglob("*"):
            if item.is_file() and item.suffix.lower() in VIDEO_EXTENSIONS:
                video_files.append(item)
    except OSError as e:
        log.warning(f"Error scanning {torrent_path}: {e}")

    return video_files


def _find_best_video_file(video_files: list[Path]) -> Path | None:
    """Pick the main video file from a list (largest file = main feature)."""
    if not video_files:
        return None
    if len(video_files) == 1:
        return video_files[0]
    # Pick the largest file
    return max(video_files, key=lambda f: f.stat().st_size)


def _match_episode_file(video_files: list[Path], season: int,
                        episode: int, torrent_path: str = "") -> Path | None:
    """Find the video file matching a specific season/episode."""
    torrent_root = Path(torrent_path) if torrent_path else None
    for vf in video_files:
        fg = guessit(vf.name, {"type": "episode"})
        file_season = fg.get("season")
        file_episode = fg.get("episode")

        # Try parent directory for season if not in filename
        if file_season is None and torrent_root:
            file_season = _extract_season_from_path(vf, torrent_root)

        if file_episode is None:
            continue

        # Handle multi-episode files
        file_episodes = file_episode if isinstance(file_episode, list) else [file_episode]

        if episode in file_episodes:
            # Season must match (or be absent — default to match)
            if file_season is None or file_season == season:
                return vf

    return None


def _collect_existing_symlinks(directory: Path) -> dict[Path, str]:
    """Scan a media directory and return {symlink_path: link_target} for all symlinks."""
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


def _resolve_symlink_target(source: Path) -> str:
    """Compute what the symlink target string should be for a given source file."""
    try:
        relative_to_zurg = source.relative_to(ZURG_MOUNT)
        return str(JELLYFIN_ZURG_PATH / relative_to_zurg)
    except ValueError:
        return str(source)


def phase_d_build_symlinks():
    """Incrementally sync symlinks from PocketBase state.

    Computes the desired symlink map, diffs against what's on disk,
    and only creates, updates, or removes what has changed.
    This avoids disrupting active streams.
    """
    log.info("Syncing symlinks...")

    # --- Build the desired state: {target_path: link_target_string} ---
    desired: dict[Path, str] = {}

    # Films
    all_films = pb.list_all_films()
    for film in all_films:
        torrent_id = film.get("torrent")
        if not torrent_id:
            continue

        torrent = (film.get("expand") or {}).get("torrent")
        if not torrent:
            torrent = pb.get_torrent_by_id(torrent_id)
        if not torrent:
            continue

        video_files = _get_video_files_for_torrent(torrent)
        main_file = _find_best_video_file(video_files)
        if not main_file:
            continue

        title = film.get("title", "Unknown")
        year = film.get("year") or None
        tmdb_id = film.get("tmdb_id")

        film_name = format_film_name(title, year, tmdb_id)
        target_path = FILMS_DIR / film_name / f"{film_name}{main_file.suffix}"
        desired[target_path] = _resolve_symlink_target(main_file)

    # Shows
    all_shows = pb.list_all_shows()
    torrent_files_cache: dict[str, list[Path]] = {}

    for show in all_shows:
        torrent_id = show.get("torrent")
        if not torrent_id:
            continue

        torrent = (show.get("expand") or {}).get("torrent")
        if not torrent:
            torrent = pb.get_torrent_by_id(torrent_id)
        if not torrent:
            continue

        if torrent_id not in torrent_files_cache:
            torrent_files_cache[torrent_id] = _get_video_files_for_torrent(torrent)
        video_files = torrent_files_cache[torrent_id]

        season = show.get("season", 1)
        episode = show.get("episode", 1)

        matched_file = _match_episode_file(video_files, season, episode,
                                             torrent.get("path", ""))
        if not matched_file:
            continue

        title = show.get("title", "Unknown")
        year = show.get("year") or None
        tmdb_id = show.get("tmdb_id")

        show_name = format_show_name(title, year, tmdb_id)
        season_dir = SHOWS_DIR / show_name / f"Season {season:02d}"
        episode_name = format_episode(title, year, season, episode)
        target_path = season_dir / f"{episode_name}{matched_file.suffix}"
        desired[target_path] = _resolve_symlink_target(matched_file)

    # --- Diff against what's on disk ---
    existing_films = _collect_existing_symlinks(FILMS_DIR)
    existing_shows = _collect_existing_symlinks(SHOWS_DIR)
    on_disk = {**existing_films, **existing_shows}

    created = 0
    updated = 0
    removed = 0

    # Create or update symlinks that should exist
    for target_path, link_target in desired.items():
        if target_path in on_disk:
            if on_disk[target_path] == link_target:
                continue  # Already correct — leave it alone
            # Wrong target — update
            target_path.unlink()
            target_path.symlink_to(link_target)
            log.info(f"  ↻ {target_path.relative_to(MEDIA_DIR)} → {link_target}")
            updated += 1
        else:
            # New symlink
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.symlink_to(link_target)
            log.info(f"  ✓ {target_path.relative_to(MEDIA_DIR)} → {link_target}")
            created += 1

    # Remove symlinks that shouldn't exist
    for target_path in on_disk:
        if target_path not in desired:
            target_path.unlink()
            log.info(f"  ✗ {target_path.relative_to(MEDIA_DIR)}")
            removed += 1

    # Clean up empty directories
    _prune_empty_dirs(FILMS_DIR)
    _prune_empty_dirs(SHOWS_DIR)

    if created or updated or removed:
        log.info(f"  Symlinks: {created} created, {updated} updated, {removed} removed")
    else:
        log.info("  Symlinks up to date — no changes")


# ---------------------------------------------------------------------------
# Main scan cycle
# ---------------------------------------------------------------------------

def run_scan():
    """Run a single scan cycle through all phases."""
    log.info("Starting scan...")

    # Scan the flat Zurg mount
    torrent_entries = _scan_zurg_mount()
    log.info(f"Found {len(torrent_entries)} torrent(s) on Zurg mount")

    # Phase A — Sync torrent records
    log.info("Phase A: Syncing torrent records...")
    needs_identification = phase_a_sync_torrents(torrent_entries)

    # Phase B — Identify unidentified torrents
    log.info("Phase B: Identifying torrents...")
    phase_b_identify(needs_identification, torrent_entries)

    # Phase C — Detect removed torrents
    log.info("Phase C: Detecting removed torrents...")
    phase_c_detect_removed()

    # Phase D — Build symlinks
    log.info("Phase D: Building symlinks...")
    phase_d_build_symlinks()

    # Summary
    all_torrents = pb.list_all_torrents()
    all_films = pb.list_all_films()
    all_shows = pb.list_all_shows()
    active = sum(1 for t in all_torrents if not t.get("archived") and not t.get("manual"))
    archived = sum(1 for t in all_torrents if t.get("archived"))
    manual = sum(1 for t in all_torrents if t.get("manual"))
    log.info(f"Scan complete. {len(all_torrents)} torrent(s) "
             f"({active} active, {archived} archived, {manual} manual), "
             f"{len(all_films)} film(s), {len(all_shows)} episode(s)")


def wait_for_pocketbase():
    """Wait for PocketBase to become available."""
    log.info(f"Waiting for PocketBase at {POCKETBASE_URL}...")
    for attempt in range(60):
        if pb.health_check():
            log.info("PocketBase is ready")
            return True
        time.sleep(2)
    log.warning("PocketBase not available after 2 minutes")
    return False


def main():
    """Entry point — run scan loop."""
    log.info("=" * 60)
    log.info("Media Organiser starting")
    log.info(f"  Zurg mount:     {ZURG_MOUNT}")
    log.info(f"  Jellyfin path:  {JELLYFIN_ZURG_PATH}")
    log.info(f"  Media output:   {MEDIA_DIR}")
    log.info(f"  TMDb API:       {'enabled' if TMDB_API_KEY else 'disabled (set TMDB_API_KEY)'}")
    log.info(f"  PocketBase:     {POCKETBASE_URL}")
    log.info(f"  Scan interval:  {SCAN_INTERVAL}s")
    log.info("=" * 60)

    # Ensure output directories exist
    FILMS_DIR.mkdir(parents=True, exist_ok=True)
    SHOWS_DIR.mkdir(parents=True, exist_ok=True)

    # Wait for PocketBase
    wait_for_pocketbase()

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

    # Continuous monitoring loop
    while True:
        log.info(f"Next scan in {SCAN_INTERVAL}s...")
        time.sleep(SCAN_INTERVAL)
        try:
            run_scan()
        except Exception as e:
            log.error(f"Scan failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
