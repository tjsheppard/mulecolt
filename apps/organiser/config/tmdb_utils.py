"""
tmdb_utils.py — TMDB episode-structure helpers.

Fetches the full season/episode structure for a TV show from TMDB and provides
matching functions that map torrent video files to (season, episode) tuples.

Two matching strategies are used (tried in order):

  1. Absolute numbering — The extracted episode number is treated as an
     absolute offset across all seasons.  E.g. if S01 has 12 episodes and
     S02 has 13, then E13 → S02E01, E25 → S02E13, E26 → S03E01, etc.

  2. Title matching — The filename is fuzzy-matched against TMDB episode
     titles using Jaccard word overlap.  Useful when the filename contains
     the episode title but no reliable numbering.
"""

import logging
import re
from dataclasses import dataclass, field

import requests

log = logging.getLogger("organiser")

# Minimum confidence for title-based matching (0–1 scale).
_TITLE_MATCH_THRESHOLD = 0.45


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TMDBEpisode:
    season: int
    episode: int
    title: str


@dataclass
class ShowStructure:
    """Full season/episode layout for a single TMDB show."""
    tmdb_id: int
    episodes: list[TMDBEpisode] = field(default_factory=list)

    # Computed after fetch — maps absolute episode number → (season, ep)
    _abs_map: dict[int, tuple[int, int]] = field(default_factory=dict, repr=False)

    # --------------- absolute-numbering helpers ---------------

    def build_absolute_map(self) -> None:
        """Build a mapping from 1-based absolute episode number to (season, episode)."""
        self._abs_map.clear()
        abs_num = 1
        # Group by season, keeping episode order
        seasons: dict[int, list[TMDBEpisode]] = {}
        for ep in self.episodes:
            seasons.setdefault(ep.season, []).append(ep)
        for season_num in sorted(seasons):
            eps = sorted(seasons[season_num], key=lambda e: e.episode)
            for ep in eps:
                self._abs_map[abs_num] = (ep.season, ep.episode)
                abs_num += 1

    @property
    def total_episodes(self) -> int:
        return len(self.episodes)

    @property
    def season_numbers(self) -> list[int]:
        return sorted({ep.season for ep in self.episodes})

    def episodes_in_season(self, season: int) -> int:
        return sum(1 for ep in self.episodes if ep.season == season)

    def lookup_absolute(self, abs_ep: int) -> tuple[int, int] | None:
        """Map an absolute episode number to (season, episode), or None."""
        return self._abs_map.get(abs_ep)


# ---------------------------------------------------------------------------
# TMDB API — fetch show structure
# ---------------------------------------------------------------------------

_structure_cache: dict[int, ShowStructure | None] = {}


def tmdb_get_show_structure(tmdb_id: int, api_key: str,
                            base_url: str = "https://api.themoviedb.org/3",
                            ) -> ShowStructure | None:
    """Fetch the full season/episode structure for a TMDB show.

    Results are cached in-memory for the lifetime of the process so repeated
    calls for the same show (e.g. multiple torrents providing different
    seasons) are free.
    """
    if tmdb_id in _structure_cache:
        return _structure_cache[tmdb_id]

    if not api_key:
        return None

    try:
        # 1. Fetch the show to get the list of seasons
        resp = requests.get(
            f"{base_url}/tv/{tmdb_id}",
            params={"api_key": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        show_data = resp.json()
    except Exception as e:
        log.warning(f"TMDB: failed to fetch show {tmdb_id}: {e}")
        _structure_cache[tmdb_id] = None
        return None

    structure = ShowStructure(tmdb_id=tmdb_id)
    seasons_meta = show_data.get("seasons", [])

    for season_meta in seasons_meta:
        season_num = season_meta.get("season_number")
        if season_num is None or season_num == 0:
            # Skip specials (season 0)
            continue

        try:
            resp = requests.get(
                f"{base_url}/tv/{tmdb_id}/season/{season_num}",
                params={"api_key": api_key},
                timeout=10,
            )
            resp.raise_for_status()
            season_data = resp.json()
        except Exception as e:
            log.warning(f"TMDB: failed to fetch season {season_num} for show {tmdb_id}: {e}")
            continue

        for ep_data in season_data.get("episodes", []):
            ep_num = ep_data.get("episode_number")
            ep_title = ep_data.get("name", "")
            if ep_num is not None:
                structure.episodes.append(TMDBEpisode(
                    season=season_num,
                    episode=ep_num,
                    title=ep_title,
                ))

    if not structure.episodes:
        log.warning(f"TMDB: show {tmdb_id} has no episodes")
        _structure_cache[tmdb_id] = None
        return None

    structure.build_absolute_map()
    _structure_cache[tmdb_id] = structure

    season_summary = ", ".join(
        f"S{s:02d}×{structure.episodes_in_season(s)}"
        for s in structure.season_numbers
    )
    log.info(f"  TMDB structure for {tmdb_id}: {structure.total_episodes} episodes "
             f"({season_summary})")
    return structure


def clear_structure_cache() -> None:
    """Clear the in-memory structure cache (call between scan cycles if desired)."""
    _structure_cache.clear()


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

def _words(text: str) -> set[str]:
    """Extract lowercase alphanumeric tokens from text."""
    return set(re.findall(r'[a-z0-9]+', text.lower()))


def _jaccard(a: set[str], b: set[str]) -> float:
    """Jaccard similarity between two word sets."""
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _extract_title_from_filename(filename: str) -> str:
    """Try to extract the episode-title portion from a filename.

    Strips common prefixes (show name, SXXEXX markers, episode numbers,
    quality tags) and returns the remaining text which hopefully is the
    episode title.
    """
    name = filename.rsplit(".", 1)[0]  # drop extension

    # Remove leading show name + episode markers
    # e.g. "Show.Name.S01E01.Episode.Title.720p..." → "Episode.Title.720p..."
    name = re.sub(r'^.*?[Ss]\d{1,2}[Ee]\d{1,3}\s*[-._]*\s*', '', name)

    # Remove standalone episode marker at start: "E01 - Title" / "01 - Title"
    name = re.sub(r'^[Ee]?\d{1,4}\s*[-._]+\s*', '', name)

    # Remove "- XX -" or "- Episode XX -" patterns
    name = re.sub(r'[-._]\s*(?:Episode\s*)?\d{1,4}\s*[-._]', ' ', name, flags=re.IGNORECASE)

    # Remove quality/codec tags from the end
    name = re.sub(
        r'[\[(]?\b(?:720p|1080p|2160p|4K|BluRay|BDRip|WEB[-.]?DL|WEB[-.]?Rip|'
        r'HDTV|x264|x265|H\.?264|H\.?265|HEVC|AAC|DTS|FLAC|10bit|'
        r'REMUX|HDR|DV|Atmos)\b.*$',
        '', name, flags=re.IGNORECASE,
    )

    # Replace separators with spaces
    name = re.sub(r'[._-]+', ' ', name).strip()
    return name


def match_file_to_tmdb_episode(
    filename: str,
    guessit_season: int | None,
    guessit_episode: int | list[int] | None,
    structure: ShowStructure,
) -> list[tuple[int, int]] | None:
    """Match a video file to TMDB (season, episode) tuples.

    Returns a list of (season, episode) tuples (to handle multi-episode files),
    or None if no confident match could be made.

    Strategy order:
      1. If guessit already extracted both season and episode, AND the episode
         exists in the TMDB structure for that season → trust it.
      2. Absolute numbering — treat the guessit episode number as a global
         offset across seasons.
      3. Title matching — fuzzy-match the filename against TMDB episode titles.
    """
    episodes: list[int] = []
    if guessit_episode is not None:
        episodes = guessit_episode if isinstance(guessit_episode, list) else [guessit_episode]

    # --- Strategy 0: guessit already has season+episode, validate it ---
    if guessit_season is not None and episodes:
        all_valid = all(
            any(e.season == guessit_season and e.episode == ep for e in structure.episodes)
            for ep in episodes
        )
        if all_valid:
            return [(guessit_season, ep) for ep in episodes]

    # --- Strategy 1: Absolute numbering ---
    if episodes and len(structure.season_numbers) > 1:
        results = []
        all_found = True
        for ep_num in episodes:
            mapped = structure.lookup_absolute(ep_num)
            if mapped:
                results.append(mapped)
            else:
                all_found = False
                break
        if all_found and results:
            return results

    # --- Strategy 2: Title matching ---
    title_text = _extract_title_from_filename(filename)
    title_words = _words(title_text)

    if title_words and len(title_words) >= 2:
        best_score = 0.0
        best_ep: TMDBEpisode | None = None

        for ep in structure.episodes:
            ep_words = _words(ep.title)
            if not ep_words:
                continue
            score = _jaccard(title_words, ep_words)
            if score > best_score:
                best_score = score
                best_ep = ep

        if best_ep and best_score >= _TITLE_MATCH_THRESHOLD:
            log.debug(f"  Title match: '{title_text}' → {best_ep.title} "
                      f"(S{best_ep.season:02d}E{best_ep.episode:02d}, "
                      f"score={best_score:.2f})")
            return [(best_ep.season, best_ep.episode)]

    # --- Strategy 3: If guessit has episode but no season, try within-season match ---
    # Check if the episode number fits within any single season's range
    if guessit_season is None and episodes:
        for ep_num in episodes:
            candidates = [
                e for e in structure.episodes if e.episode == ep_num
            ]
            if len(candidates) == 1:
                # Unambiguous — only one season has this episode number
                return [(candidates[0].season, candidates[0].episode)]

    return None
