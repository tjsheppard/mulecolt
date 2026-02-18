# Mulecolt

Stream films and shows from Real Debrid via Jellyfin. That's it.

Uses [Debrid Media Manager](https://debridmediamanager.com) to add content to your Real Debrid library, [Zurg](https://github.com/debridmediamanager/zurg-testing) to expose it as WebDAV, and a custom organiser to create clean Jellyfin-compatible symlinks.

```
Debrid Media Manager → Real Debrid → Zurg (WebDAV) → rclone (FUSE) → Organiser (symlinks) → Jellyfin
                                                                          ↕
                                                                      PocketBase (TMDB cache + media DB)
```

Each container that needs the Zurg filesystem (organiser, Jellyfin) runs its own embedded [rclone](https://rclone.org/) FUSE mount. This avoids FUSE mount propagation issues on macOS Docker Desktop.

| Service                                                           | Description                               |
| ----------------------------------------------------------------- | ----------------------------------------- |
| [Jellyfin](https://github.com/jellyfin/jellyfin)                  | Media server (+ embedded rclone mount)    |
| [Zurg](https://github.com/debridmediamanager/zurg-testing)        | Real Debrid WebDAV server                 |
| Media Organiser                                                   | Symlink creator (+ embedded rclone mount) |
| [PocketBase](https://pocketbase.io)                               | Media database and TMDB cache             |
| [File Browser](https://github.com/filebrowser/filebrowser)        | File management                           |
| [Portainer](https://github.com/portainer/portainer)               | Container management                      |
| [Homepage](https://github.com/gethomepage/homepage)               | Dashboard                                 |
| [Caddy + Tailscale](https://github.com/tailscale/caddy-tailscale) | HTTPS reverse proxy over Tailscale        |

## How it works

1. **Add content** — use [Debrid Media Manager](https://debridmediamanager.com) to add films and shows to your Real Debrid library
2. **Zurg** exposes your Real Debrid library as a WebDAV server, automatically categorising torrents into `films/` and `shows/` directories
3. **Media Organiser** mounts Zurg via its own embedded rclone instance, scans every 5 minutes, parses torrent names using `guessit`, verifies against TMDb, and creates clean symlinks:
   - `media/films/The Dark Knight (2008) [tmdbid=155]/The Dark Knight (2008) [tmdbid=155].mkv`
   - `media/shows/Breaking Bad (2008) [tmdbid=1396]/Season 01/Breaking Bad (2008) S01E01.mkv`
4. **PocketBase** stores every TMDB lookup and media mapping, so no duplicate API calls are ever made (see [PocketBase](#pocketbase) below)
5. **Jellyfin** runs its own embedded rclone mount and reads the organised `media/` directory — symlinks resolve because both containers mount at `/zurg`. The `[tmdbid=XXXXX]` in folder names lets Jellyfin auto-match metadata without scraping.

## PocketBase

PocketBase is a lightweight database that serves two purposes:

### TMDB lookup cache

Every time the organiser encounters a new title, it searches TMDB for the canonical name, year, and ID. That result is cached in PocketBase's `tmdb_lookups` table — **one row per unique title**. All episodes of the same show share a single cached lookup. Subsequent scans (every 5 minutes) hit the cache instead of the TMDB API, reducing API calls from hundreds per hour to essentially zero for a stable library.

| query_title     | media_type | tmdb_id | canonical_title | canonical_year |
| --------------- | ---------- | ------- | --------------- | -------------- |
| Children of Men | film       | 1267    | Children of Men | 2006           |
| Doctor Who      | show       | 57243   | Doctor Who      | 2005           |

### Media item mappings

Every source file (Real Debrid torrent on the Zurg mount) is mapped to its organised symlink path in PocketBase's `media_items` table. This gives you a browsable record of your entire library — what's on Real Debrid, where the symlink points, and the TMDB metadata.

| source_path                              | target_path                                           | title           | tmdb_id | season | episode | score |
| ---------------------------------------- | ----------------------------------------------------- | --------------- | ------- | ------ | ------- | ----- |
| /zurg/films/Children.of.Men.../movie.mkv | /media/films/Children of Men (2006) [tmdbid=1267]/... | Children of Men | 1267    |        |         | 183   |
| /zurg/shows/Doctor.Who.S01E01.../ep.mkv  | /media/shows/Doctor Who (2005) [tmdbid=57243]/...     | Doctor Who      | 57243   | 1      | 1       | 183   |

### Rebuild mode

If all symlinks are deleted or lost, set `REBUILD_MODE=true` and the organiser will recreate every symlink from PocketBase's stored mappings — **zero TMDB API calls**. On normal startup, if `state.json` is lost, the organiser automatically syncs its state from PocketBase.

```bash
# Rebuild all symlinks from the database (no TMDB calls)
REBUILD_MODE=true docker compose up organiser
```

### Admin UI

Browse and manage the database at `https://pocketbase.yourdomain.com/_/` (or `localhost:8090/_/`). The superuser account is created automatically from `EMAIL` and `PASSWORD` in `.env`.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) (included with Docker Desktop)
- A [Real Debrid](https://real-debrid.com/) account
- [GitHub Desktop](https://desktop.github.com/) is recommended for cloning and managing the repository

## Step 1 — Basic setup

1. Copy `.env.example` to `.env`:
   ```
   cp .env.example .env
   ```
2. Set your **Real Debrid API token** (`REAL_DEBRID_API_KEY`) — get it from https://real-debrid.com/apitoken
3. Set your **TMDb API key** (`TMDB_API_KEY`) — free key from https://www.themoviedb.org/settings/api (recommended for accurate naming)
4. Set `EMAIL` and `PASSWORD` for the PocketBase admin account
5. Check your timezone (`TZ`) and `MEDIA` path are correct
6. Check `PUID` and `PGID` match your user (find with `id $USER`)

## Step 2 — Choose your access method

### Option A: Local only

Keep it simple. Services are available at `localhost` ports on the machine running Mulecolt. No Tailscale or Cloudflare needed.

No additional configuration needed — just build and start:

```
docker compose up -d --build
```

### Option B: Remote access (Tailscale + Cloudflare)

Access all services remotely via your own domain (e.g. `jellyfin.example.com`) over Tailscale. Requires a Tailscale account and a domain managed by Cloudflare.

1. [Create a Tailscale account](https://login.tailscale.com/start) (free tier works fine)
2. Go to [Settings → Keys](https://login.tailscale.com/admin/settings/keys) and generate a **reusable auth key**
3. Set `TS_AUTHKEY` in `.env`
4. Generate a Tailscale **API access token** at [Settings → Keys](https://login.tailscale.com/admin/settings/keys) and set `TS_API_KEY` in `.env`
5. Set `DOMAIN` in `.env` (e.g. `example.com`)
6. Create a [Cloudflare API token](https://dash.cloudflare.com/profile/api-tokens) with **Zone → DNS → Edit** permissions
7. Set `CF_API_TOKEN` in `.env`
8. Set `CF_ZONE_ID` in `.env` — found on your domain's overview page in the [Cloudflare dashboard](https://dash.cloudflare.com) (right sidebar, under **API**)
9. Copy the Caddyfile:
   ```
   cp apps/caddy/config/Caddyfile.cloudflare apps/caddy/data/Caddyfile
   ```
10. Generate the Homepage dashboard config:
    ```
    ./scripts/setup-homepage.sh
    ```
11. Build and start:
    ```
    docker compose up -d --build
    ```

> **Note:** DNS records are created automatically when the Caddy container starts. It registers itself as a Tailscale node called "mulecolt", then uses the Tailscale API to discover its own IP and upserts Cloudflare A records for `DOMAIN` and `*.DOMAIN`. Check progress with `docker compose logs caddy`. If you need to manually update DNS records after the container is running, you can still use `./scripts/setup-dns.sh`.

## Adding content

1. Go to [Debrid Media Manager](https://debridmediamanager.com) and sign in with your Real Debrid account
2. Search for a film or show and add it to your library
3. Within ~5 minutes, the organiser will detect the new content, look up TMDB (cached in PocketBase), create properly named symlinks, and Jellyfin will pick it up on its next library scan

> **Tip:** You can trigger a Jellyfin library scan manually from the Jellyfin admin dashboard, or wait for the scheduled scan.

## Accessing your services

| Service      | Local            | Remote (Option B)                      |
| ------------ | ---------------- | -------------------------------------- |
| Jellyfin     | `localhost:8096` | `https://jellyfin.yourdomain.com`      |
| Homepage     | `localhost:3000` | `https://yourdomain.com`               |
| PocketBase   | `localhost:8090` | `https://pocketbase.yourdomain.com/_/` |
| Portainer    | `localhost:9000` | `https://portainer.yourdomain.com`     |
| File Browser | `localhost:8080` | `https://files.yourdomain.com`         |
| Zurg         | `localhost:9999` | `https://zurg.yourdomain.com`          |

## Transcoding

Jellyfin supports transcoding for clients that can't direct-play the source format. On macOS with Apple Silicon, hardware transcoding (VideoToolmulecolt) is not available inside Docker containers — software transcoding is used instead, which is fast enough on Apple Silicon for most use cases.

If you migrate to a Linux host with an Intel iGPU or NVIDIA GPU, uncomment the device passthrough lines in `docker-compose.yml` to enable hardware transcoding.

## Jellyfin library cache

The Jellyfin data directory (`apps/jellyfin/data/`) is gitignored by default, so your library metadata is not committed. If you'd like to commit it for portability, remove the `apps/*/data/` rule from `.gitignore` and add back specific ignores for the other apps.

## Sharing with friends

1. In Tailscale, [share your device](https://tailscale.com/kb/1084/sharing) with friends
2. They install Tailscale, accept the share, then open the Jellyfin URL
3. Create Jellyfin accounts for them via the admin dashboard