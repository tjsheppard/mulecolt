/// <reference path="../pb_data/types.d.ts" />

// Add rd_filename field to the torrents collection.
// Stores the original RD torrent name for fallback identification when the
// Zurg mount entry has a generic filename (e.g. 00000.m2ts).

migrate(
    (app) => {
        const torrents = app.findCollectionByNameOrId("torrents");

        torrents.fields.add(
            new Field({
                name: "rd_filename",
                type: "text",
                required: false,
            })
        );

        app.save(torrents);
    },
    (app) => {
        const torrents = app.findCollectionByNameOrId("torrents");

        torrents.fields.removeByName("rd_filename");

        app.save(torrents);
    }
);
