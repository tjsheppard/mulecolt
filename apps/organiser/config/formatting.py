"""
formatting.py — Media name formatting and sanitisation.

Produces Jellyfin-compatible filenames and directory names for films and
TV show episodes.
"""

import re

from constants import UNSAFE_CHARS


def sanitise(name: str) -> str:
    """Remove characters that Jellyfin doesn't allow in filenames."""
    name = UNSAFE_CHARS.sub("", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.rstrip(". ")
    return name


def format_media_name(title: str, year: int | None,
                      tmdb_id: int | None = None) -> str:
    """Format a media folder name: Title (Year) [tmdbid=XXXXX].

    Works for both films and shows — the format is identical.
    """
    title = sanitise(title)
    parts = [title]
    if year:
        parts.append(f"({year})")
    if tmdb_id:
        parts.append(f"[tmdbid={tmdb_id}]")
    return " ".join(parts)


def format_episode(title: str, year: int | None,
                   season: int, episode: int | list) -> str:
    """Format an episode filename: Show Name (Year) SXXEXX.

    No tmdbid in episode filenames — only in the parent show folder.
    """
    base = sanitise(title)
    if year:
        base = f"{base} ({year})"
    if isinstance(episode, list):
        ep_str = "".join(f"E{e:02d}" for e in episode)
    else:
        ep_str = f"E{episode:02d}"
    return f"{base} S{season:02d}{ep_str}"
