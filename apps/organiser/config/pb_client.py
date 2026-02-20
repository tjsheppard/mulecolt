"""
pb_client.py — PocketBase REST API client.

Single source of truth for all PocketBase interactions. Used by both
the main organiser scan loop and the manual resolve.py CLI tool.
"""

import logging

import requests

log = logging.getLogger("organiser")


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

    # ------------------------------------------------------------------
    # Torrents
    # ------------------------------------------------------------------

    def get_torrent_by_path(self, path: str) -> dict | None:
        """Look up a torrent record by its zurg path."""
        try:
            resp = self._session.get(
                self._url("torrents"),
                params={"filter": f'path = "{self._escape(path)}"', "perPage": 1},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            return items[0] if items else None
        except Exception as e:
            log.warning(f"PB: torrent query failed: {e}")
        return None

    def get_torrent_by_id(self, record_id: str) -> dict | None:
        """Look up a torrent record by PocketBase ID."""
        try:
            resp = self._session.get(self._url("torrents", record_id), timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"PB: torrent get failed: {e}")
        return None

    def create_torrent(self, name: str, path: str, score: int = 0,
                       hash: str = "", rd_id: str = "",
                       rd_filename: str = "") -> dict | None:
        """Create a new torrent record."""
        data = {
            "name": name, "path": path, "score": score,
            "archived": False, "manual": False,
            "hash": hash, "rd_id": rd_id, "rd_filename": rd_filename,
            "repair_attempts": 0,
        }
        try:
            resp = self._session.post(self._url("torrents"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"PB: create torrent failed: {e}")
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
            log.warning(f"PB: update torrent failed: {e}")
        return None

    def delete_torrent(self, record_id: str) -> bool:
        """Delete a torrent record."""
        try:
            self._session.delete(
                self._url("torrents", record_id), timeout=5,
            ).raise_for_status()
            return True
        except Exception as e:
            log.warning(f"PB: delete torrent failed: {e}")
        return False

    def list_all_torrents(self) -> list[dict]:
        """Fetch all torrent records."""
        return self._paginate("torrents")

    def list_archived_torrents(self) -> list[dict]:
        """Fetch all archived torrent records."""
        return self._paginate("torrents", filter_str="archived = true")

    # ------------------------------------------------------------------
    # Films
    # ------------------------------------------------------------------

    def get_film_by_tmdb(self, tmdb_id: int) -> dict | None:
        """Look up a film record by TMDB ID."""
        try:
            resp = self._session.get(
                self._url("films"),
                params={"filter": f"tmdb_id = {tmdb_id}", "perPage": 1, "expand": "torrent"},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            return items[0] if items else None
        except Exception as e:
            log.warning(f"PB: film query failed: {e}")
        return None

    def create_film(self, torrent_id: str, tmdb_id: int,
                    title: str, year: int | None) -> dict | None:
        """Create a new film record."""
        data = {
            "torrent": torrent_id, "tmdb_id": tmdb_id,
            "title": title, "year": year or 0,
        }
        try:
            resp = self._session.post(self._url("films"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"PB: create film failed: {e}")
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
            log.warning(f"PB: update film failed: {e}")
        return None

    def delete_film(self, record_id: str) -> bool:
        """Delete a film record."""
        try:
            self._session.delete(
                self._url("films", record_id), timeout=5,
            ).raise_for_status()
            return True
        except Exception as e:
            log.warning(f"PB: delete film failed: {e}")
        return False

    def list_all_films(self) -> list[dict]:
        """Fetch all film records, expanding the torrent relation."""
        return self._paginate("films", expand="torrent")

    def list_films_by_torrent(self, torrent_id: str) -> list[dict]:
        """Find all film records linked to a specific torrent."""
        return self._paginate(
            "films",
            filter_str=f'torrent = "{self._escape(torrent_id)}"',
        )

    # ------------------------------------------------------------------
    # Shows
    # ------------------------------------------------------------------

    def get_show_episode(self, tmdb_id: int, season: int,
                         episode: int) -> dict | None:
        """Look up a show episode by (tmdb_id, season, episode)."""
        try:
            filt = f"tmdb_id = {tmdb_id} && season = {season} && episode = {episode}"
            resp = self._session.get(
                self._url("shows"),
                params={"filter": filt, "perPage": 1, "expand": "torrent"},
                timeout=5,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            return items[0] if items else None
        except Exception as e:
            log.warning(f"PB: show episode query failed: {e}")
        return None

    def create_show(self, torrent_id: str, tmdb_id: int, title: str,
                    year: int | None, season: int,
                    episode: int) -> dict | None:
        """Create a new show episode record."""
        data = {
            "torrent": torrent_id, "tmdb_id": tmdb_id,
            "title": title, "year": year or 0,
            "season": season, "episode": episode,
        }
        try:
            resp = self._session.post(self._url("shows"), json=data, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"PB: create show failed: {e}")
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
            log.warning(f"PB: update show failed: {e}")
        return None

    def delete_show(self, record_id: str) -> bool:
        """Delete a show record."""
        try:
            self._session.delete(
                self._url("shows", record_id), timeout=5,
            ).raise_for_status()
            return True
        except Exception as e:
            log.warning(f"PB: delete show failed: {e}")
        return False

    def list_all_shows(self) -> list[dict]:
        """Fetch all show records, expanding the torrent relation."""
        return self._paginate("shows", expand="torrent")

    def list_shows_by_torrent(self, torrent_id: str) -> list[dict]:
        """Find all show records linked to a specific torrent."""
        return self._paginate(
            "shows",
            filter_str=f'torrent = "{self._escape(torrent_id)}"',
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _paginate(self, collection: str, expand: str = "",
                  filter_str: str = "") -> list[dict]:
        """Generic paginated fetch for any collection."""
        items: list[dict] = []
        page = 1
        while True:
            try:
                params: dict = {"perPage": 200, "page": page}
                if expand:
                    params["expand"] = expand
                if filter_str:
                    params["filter"] = filter_str
                resp = self._session.get(
                    self._url(collection), params=params, timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                items.extend(data.get("items", []))
                if page >= data.get("totalPages", 1):
                    break
                page += 1
            except Exception as e:
                log.warning(f"PB: list {collection} failed (page {page}): {e}")
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
