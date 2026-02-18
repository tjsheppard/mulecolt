"""
Real-Debrid API client for torrent repair.

Provides the minimum surface needed by the organiser to:
  1. List all user torrents (with hash + status)
  2. Re-add a dead torrent via its info-hash
  3. Select video files on the new torrent
  4. Delete the old dead torrent entry

Rate limits (as of 2025):
  - General endpoints:  250 req/min
  - /torrents/*:         60 req/min

We respect 429 / HTTP-503 with exponential back-off.
"""

import logging
import time
from typing import Any

import requests

log = logging.getLogger("organiser.rd_api")

RD_BASE = "https://api.real-debrid.com/rest/1.0"

# Video extensions considered when selecting files after magnet add
_VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm",
               ".m4v", ".mpg", ".mpeg", ".ts", ".vob", ".m2ts", ".iso"}


class RealDebridError(Exception):
    """Raised on non-retryable RD API errors."""


class RealDebridClient:
    """Lightweight Real-Debrid REST client for torrent repair."""

    def __init__(self, api_key: str, min_file_size_mb: int = 100):
        self.api_key = api_key
        self.min_file_size_bytes = min_file_size_mb * 1024 * 1024
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {api_key}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_torrents(self, page: int = 1, limit: int = 100) -> list[dict]:
        """Fetch a page of the user's torrents.

        Limit must be <= 100 to get the ``links`` field in the response.
        Returns a list of torrent dicts with at least:
          id, filename, hash, status, bytes, links
        """
        resp = self._get("/torrents", params={"page": page, "limit": min(limit, 100)})
        return resp if isinstance(resp, list) else []

    def list_all_torrents(self) -> list[dict]:
        """Paginate through all user torrents and return a flat list."""
        all_torrents: list[dict] = []
        page = 1
        while True:
            batch = self.list_torrents(page=page, limit=100)
            if not batch:
                break
            all_torrents.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return all_torrents

    def add_magnet(self, info_hash: str) -> str | None:
        """Add a magnet link constructed from an info-hash.

        Returns the new RD torrent id on success, or None on failure.
        """
        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        try:
            resp = self._post("/torrents/addMagnet", data={"magnet": magnet})
            new_id = resp.get("id") if isinstance(resp, dict) else None
            if new_id:
                log.info(f"  RD: added magnet for hash {info_hash[:12]}… → id={new_id}")
            return new_id
        except RealDebridError as e:
            # Error 33 = "torrent already active" — treat as success
            if "already_active" in str(e).lower() or "33" in str(e):
                log.info(f"  RD: magnet for hash {info_hash[:12]}… already active")
            else:
                raise
        return None

    def get_torrent_info(self, torrent_id: str) -> dict | None:
        """Get detailed torrent info including its file list.

        Returns a dict with at least: id, filename, hash, status, files[]
        Each file has: id, path, bytes, selected
        """
        try:
            return self._get(f"/torrents/info/{torrent_id}")
        except RealDebridError:
            return None

    def select_video_files(self, torrent_id: str) -> bool:
        """Select video files above the minimum size threshold on a torrent.

        Returns True if at least one file was selected.
        """
        info = self.get_torrent_info(torrent_id)
        if not info:
            return False

        files = info.get("files", [])
        video_ids: list[int] = []

        for f in files:
            path = f.get("path", "")
            size = f.get("bytes", 0)
            ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""

            if f".{ext}" in _VIDEO_EXTS and size >= self.min_file_size_bytes:
                video_ids.append(f["id"])

        if not video_ids:
            log.warning(f"  RD: no qualifying video files on torrent {torrent_id}")
            return False

        file_str = ",".join(str(fid) for fid in video_ids)
        try:
            self._post(f"/torrents/selectFiles/{torrent_id}", data={"files": file_str})
            log.info(f"  RD: selected {len(video_ids)} file(s) on torrent {torrent_id}")
            return True
        except RealDebridError as e:
            log.warning(f"  RD: file selection failed for {torrent_id}: {e}")
            return False

    def delete_torrent(self, torrent_id: str) -> bool:
        """Delete a torrent from the user's account.

        Returns True on success. Silently succeeds if already gone.
        """
        try:
            self._delete(f"/torrents/delete/{torrent_id}")
            log.info(f"  RD: deleted torrent {torrent_id}")
            return True
        except RealDebridError as e:
            log.warning(f"  RD: failed to delete torrent {torrent_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> Any:
        return self._request("GET", path, params=params)

    def _post(self, path: str, data: dict | None = None) -> Any:
        return self._request("POST", path, data=data)

    def _delete(self, path: str) -> Any:
        return self._request("DELETE", path)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        """Make an API request with retry + exponential back-off on 429/503."""
        url = f"{RD_BASE}{path}"
        max_retries = 3
        backoff = 2.0

        for attempt in range(max_retries + 1):
            try:
                resp = self._session.request(method, url, timeout=15, **kwargs)

                # Rate-limited or server overload — back off and retry
                if resp.status_code in (429, 503) and attempt < max_retries:
                    wait = backoff * (2 ** attempt)
                    log.warning(f"  RD: {resp.status_code} on {method} {path}, "
                                f"retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait)
                    continue

                # 204 No Content (success for DELETE / selectFiles)
                if resp.status_code == 204:
                    return {}

                # 201 Created (success for addMagnet)
                if resp.status_code == 201:
                    return resp.json()

                resp.raise_for_status()
                return resp.json()

            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait = backoff * (2 ** attempt)
                    log.warning(f"  RD: request error on {method} {path}: {e}, "
                                f"retrying in {wait:.0f}s")
                    time.sleep(wait)
                    continue
                raise RealDebridError(f"{method} {path} failed after {max_retries} retries: {e}") from e

        raise RealDebridError(f"{method} {path} failed after {max_retries} retries")
