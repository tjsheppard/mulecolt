"""
scoring.py — Quality scoring for torrent/file names.

When duplicate torrents exist for the same media, the highest score wins.
Score is computed from resolution, source, codec, and bonus markers
extracted via guessit.
"""

from guessit import guessit

# ---------------------------------------------------------------------------
# Score tables
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
    "Blu-ray":          60,
    "Ultra HD Blu-ray": 65,
    "HD-DVD":           55,
    "Web":              40,
    "HDTV":             35,
    "PDTV":             25,
    "SDTV":             20,
    "DVD":              30,
    "VHS":              5,
    "Telecine":         10,
    "Telesync":         8,
    "Workprint":        3,
    "Camera":           1,
}

CODEC_SCORES = {
    "H.265":   30,
    "HEVC":    30,
    "H.264":   20,
    "AVC":     20,
    "VP9":     18,
    "AV1":     35,
    "MPEG-2":  5,
    "XviD":    3,
    "DivX":    3,
}

# Bonus points for various quality markers
REMUX_BONUS = 25
HDR_BONUS = 15
ATMOS_BONUS = 10
LOSSLESS_AUDIO_BONUS = 8


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

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

    name_upper = name.upper()

    # Remux bonus
    if "REMUX" in name_upper:
        score += REMUX_BONUS

    # HDR bonus
    other = guess.get("other", [])
    if not isinstance(other, list):
        other = [other]
    hdr_terms = {"HDR10", "HDR10+", "HDR", "Dolby Vision", "DV", "HLG", "HDR10Plus"}
    if any(o in hdr_terms for o in other) or any(
        t in name_upper for t in ("HDR", "DV", "DOLBY.VISION")
    ):
        score += HDR_BONUS

    # Lossless audio bonus
    audio = guess.get("audio_codec", "")
    if isinstance(audio, list):
        audio = " ".join(audio)
    audio_str = f"{audio} {name_upper}"
    if any(t in audio_str for t in (
        "DTS-HD", "DTS-HD MA", "TRUEHD", "TRUE HD", "FLAC", "PCM", "LPCM",
    )):
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
