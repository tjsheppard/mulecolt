/// <reference path="../pb_data/types.d.ts" />

// PocketBase migration: create tmdb, films, and shows collections.
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
        // Collection: tmdb — canonical TMDB records (deduped by tmdb_id + type)
        // ---------------------------------------------------------------
        const tmdb = new Collection({
            name: "tmdb",
            type: "base",
            system: false,
            listRule: "",
            viewRule: "",
            createRule: "",
            updateRule: "",
            deleteRule: "",
            fields: [
                {
                    name: "tmdb_id",
                    type: "number",
                    required: true,
                },
                {
                    name: "type",
                    type: "select",
                    required: true,
                    values: ["film", "show"],
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
                "CREATE UNIQUE INDEX idx_tmdb_key ON tmdb (tmdb_id, type)",
            ],
        });
        app.save(tmdb);

        // ---------------------------------------------------------------
        // Collection: films — source→target mappings for films
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
                    name: "source_path",
                    type: "text",
                    required: true,
                },
                {
                    name: "target_path",
                    type: "text",
                    required: true,
                },
                {
                    name: "tmdb",
                    type: "relation",
                    required: true,
                    collectionId: tmdb.id,
                    cascadeDelete: false,
                    maxSelect: 1,
                },
                {
                    name: "score",
                    type: "number",
                    required: false,
                },
            ],
            indexes: [
                "CREATE UNIQUE INDEX idx_films_source ON films (source_path)",
            ],
        });
        app.save(films);

        // ---------------------------------------------------------------
        // Collection: shows — source→target mappings for TV episodes
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
                    name: "source_path",
                    type: "text",
                    required: true,
                },
                {
                    name: "target_path",
                    type: "text",
                    required: true,
                },
                {
                    name: "tmdb",
                    type: "relation",
                    required: true,
                    collectionId: tmdb.id,
                    cascadeDelete: false,
                    maxSelect: 1,
                },
                {
                    name: "season",
                    type: "number",
                    required: false,
                },
                {
                    name: "episode",
                    type: "number",
                    required: false,
                },
            ],
            indexes: [
                "CREATE UNIQUE INDEX idx_shows_source ON shows (source_path)",
            ],
        });
        app.save(shows);
    },
    (app) => {
        // Rollback
        for (const name of ["shows", "films", "tmdb"]) {
            try {
                const col = app.findCollectionByNameOrId(name);
                app.delete(col);
            } catch (_) { }
        }
    }
);
