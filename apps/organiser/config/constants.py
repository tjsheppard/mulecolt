"""
constants.py — Shared configuration and constants for the organiser.

All environment variables, paths, regex patterns, and constants that are
used across multiple modules are centralised here.
"""

import datetime
import os
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ZURG_MOUNT = Path("/zurg")
MEDIA_DIR = Path("/media")
FILMS_DIR = MEDIA_DIR / "films"
SHOWS_DIR = MEDIA_DIR / "shows"

# The path where the Zurg mount appears inside Jellyfin's container.
JELLYFIN_ZURG_PATH = Path(os.environ.get("JELLYFIN_ZURG_PATH", "/zurg"))

# ---------------------------------------------------------------------------
# Environment-variable configuration
# ---------------------------------------------------------------------------

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"

SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL_SECS", "300"))
POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "http://pocketbase:8090")

# Real-Debrid
REAL_DEBRID_API_KEY = os.environ.get("REAL_DEBRID_API_KEY", "")
REPAIR_ENABLED = os.environ.get("REPAIR_ENABLED", "true").lower() in ("true", "1", "yes")
MAX_REPAIR_ATTEMPTS = int(os.environ.get("MAX_REPAIR_ATTEMPTS", "3"))
MIN_VIDEO_FILE_SIZE_MB = int(os.environ.get("MIN_VIDEO_FILE_SIZE_MB", "100"))

# Jellyfin
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "http://jellyfin:8096")
JELLYFIN_API_KEY = os.environ.get("JELLYFIN_API_KEY", "")

# Webhook
WEBHOOK_PORT = int(os.environ.get("WEBHOOK_PORT", "8080"))

# Archived torrent cleanup
CLEANUP_ARCHIVED = os.environ.get("CLEANUP_ARCHIVED", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Video extensions (used for file scanning and RD file selection)
# ---------------------------------------------------------------------------

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".vob", ".m2ts",
}

# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

# Characters that Jellyfin doesn't allow in filenames
UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*]')

# Season directory detection (e.g. "Season 1", "S02")
SEASON_DIR_PATTERN = re.compile(r'(?:Season|S)\s*(\d+)', re.IGNORECASE)

# Meaningless titles that need RD filename fallback
_MEANINGLESS_TITLE = re.compile(
    r'^(?:\d+|[\W_]+|.{0,2})$'  # all digits, all punctuation, or <=2 chars
)

# Patterns that indicate a TV show (season/episode markers)
SHOW_PATTERNS = [
    re.compile(r'[Ss]\d{1,2}[Ee]\d{1,3}'),            # S01E01
    re.compile(r'[Ss]\d{1,2}'),                         # S01
    re.compile(r'[Ss]eason[\s._-]?\d', re.IGNORECASE),  # Season 1
    re.compile(r'[Ee]\d{2,3}'),                          # E01, E001
    re.compile(r'Episode[\s._-]?\d', re.IGNORECASE),     # Episode 1
    re.compile(r'\bComplete[\s._-]?Series\b', re.IGNORECASE),
    re.compile(r'\bBatch\b', re.IGNORECASE),
    re.compile(r'\b\d{1,2}x\d{2}\b'),                   # 1x01 format
]

# ---------------------------------------------------------------------------
# Year validation
# ---------------------------------------------------------------------------

_CURRENT_YEAR = datetime.date.today().year
_MIN_YEAR = 1920
_MAX_YEAR = _CURRENT_YEAR + 1


def is_meaningless_title(title: str) -> bool:
    """Return True if a guessit-extracted title is too generic to identify."""
    return bool(_MEANINGLESS_TITLE.match(title.strip()))


def validate_year(year: int | None, reference_text: str | None = None) -> int | None:
    """Validate an extracted year for plausibility.

    Returns the year if valid, otherwise None.

    Rules:
      - Must be within [1920, current_year + 1]
      - If reference_text is provided, the year must appear literally in
        that text (prevents guessit extracting years from episode titles).
    """
    if year is None:
        return None
    if not (_MIN_YEAR <= year <= _MAX_YEAR):
        return None
    if reference_text is not None and str(year) not in reference_text:
        return None
    return year
