/// <reference path="../pb_data/types.d.ts" />

// PocketBase migration: create torrents, films, and shows collections.
// This runs automatically on first boot (clean slate — no upgrade path).

migrate(
    (app) => {
        // ---------------------------------------------------------------
        // Delete the default users collection if it exists
        // ---------------------------------------------------------------
        try {
            const users = app.findCollectionByNameOrId("users");
            app.delete(users);
        } catch (_) {
            // Already gone — fine
        }

        // ---------------------------------------------------------------
        // Collection: torrents — one record per torrent from Real-Debrid
        // ---------------------------------------------------------------
        const torrents = new Collection({
            name: "torrents",
            type: "base",
            system: false,
            listRule: "",
            viewRule: "",
            createRule: "",
            updateRule: "",
            deleteRule: "",
            fields: [
                {
                    name: "name",
                    type: "text",
                    required: true,
                },
                {
                    name: "path",
                    type: "text",
                    required: true,
                },
                {
                    name: "score",
                    type: "number",
                    required: false,
                },
                {
                    name: "archived",
                    type: "bool",
                    required: false,
                },
                {
                    name: "manual",
                    type: "bool",
                    required: false,
                },
                {
                    name: "hash",
                    type: "text",
                    required: false,
                },
                {
                    name: "rd_id",
                    type: "text",
                    required: false,
                },
                {
                    name: "repair_attempts",
                    type: "number",
                    required: false,
                },
            ],
            indexes: [
                "CREATE UNIQUE INDEX idx_torrents_path ON torrents (path)",
            ],
        });
        app.save(torrents);

        // ---------------------------------------------------------------
        // Collection: films — one record per unique TMDB film identity
        // ---------------------------------------------------------------
        const films = new Collection({
            name: "films",
            type: "base",
            system: false,
            listRule: "",
            viewRule: "",
            createRule: "",
            updateRule: "",
            deleteRule: "",
            fields: [
                {
                    name: "torrent",
                    type: "relation",
                    required: false,
                    collectionId: torrents.id,
                    cascadeDelete: false,
                    maxSelect: 1,
                },
                {
                    name: "tmdb_id",
                    type: "number",
                    required: true,
                },
                {
                    name: "title",
                    type: "text",
                    required: true,
                },
                {
                    name: "year",
                    type: "number",
                    required: false,
                },
            ],
            indexes: [
                "CREATE UNIQUE INDEX idx_films_tmdb ON films (tmdb_id)",
            ],
        });
        app.save(films);

        // ---------------------------------------------------------------
        // Collection: shows — one record per unique episode identity
        // ---------------------------------------------------------------
        const shows = new Collection({
            name: "shows",
            type: "base",
            system: false,
            listRule: "",
            viewRule: "",
            createRule: "",
            updateRule: "",
            deleteRule: "",
            fields: [
                {
                    name: "torrent",
                    type: "relation",
                    required: false,
                    collectionId: torrents.id,
                    cascadeDelete: false,
                    maxSelect: 1,
                },
                {
                    name: "tmdb_id",
                    type: "number",
                    required: true,
                },
                {
                    name: "title",
                    type: "text",
                    required: true,
                },
                {
                    name: "year",
                    type: "number",
                    required: false,
                },
                {
                    name: "season",
                    type: "number",
                    required: true,
                },
                {
                    name: "episode",
                    type: "number",
                    required: true,
                },
            ],
            indexes: [
                "CREATE UNIQUE INDEX idx_shows_episode ON shows (tmdb_id, season, episode)",
            ],
        });
        app.save(shows);

        // ---------------------------------------------------------------
        // View: archived_torrents — torrents with archived = true
        // ---------------------------------------------------------------
        const archivedView = new Collection({
            name: "archived_torrents",
            type: "view",
            system: false,
            listRule: "",
            viewRule: "",
            viewQuery: "SELECT id, name, path, score, archived, manual, hash, rd_id, repair_attempts FROM torrents WHERE archived = 1",
        });
        app.save(archivedView);

        // ---------------------------------------------------------------
        // View: manual_torrents — torrents with manual = true
        // ---------------------------------------------------------------
        const manualView = new Collection({
            name: "manual_torrents",
            type: "view",
            system: false,
            listRule: "",
            viewRule: "",
            viewQuery: "SELECT id, name, path, score, archived, manual, hash, rd_id, repair_attempts FROM torrents WHERE manual = 1",
        });
        app.save(manualView);

        // ---------------------------------------------------------------
        // View: unique_shows — one row per distinct show (by tmdb_id)
        // ---------------------------------------------------------------
        const uniqueShowsView = new Collection({
            name: "unique_shows",
            type: "view",
            system: false,
            listRule: "",
            viewRule: "",
            viewQuery: "SELECT MIN(id) as id, tmdb_id, title, year, COUNT(DISTINCT season) as season_count, COUNT(*) as episode_count FROM shows GROUP BY tmdb_id, title, year",
        });
        app.save(uniqueShowsView);

        // ---------------------------------------------------------------
        // View: show_seasons — one row per show + season combination
        // ---------------------------------------------------------------
        const showSeasonsView = new Collection({
            name: "show_seasons",
            type: "view",
            system: false,
            listRule: "",
            viewRule: "",
            viewQuery: "SELECT MIN(id) as id, tmdb_id, title, year, season, COUNT(*) as episode_count FROM shows GROUP BY tmdb_id, title, year, season",
        });
        app.save(showSeasonsView);
    },
    (app) => {
        // Rollback
        for (const name of ["show_seasons", "unique_shows", "manual_torrents", "archived_torrents", "shows", "films", "torrents"]) {
            try {
                const col = app.findCollectionByNameOrId(name);
                app.delete(col);
            } catch (_) { }
        }
    }
);
