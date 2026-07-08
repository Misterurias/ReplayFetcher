import pg from "pg";
import { config } from "../config.js";

// ─── Pool ─────────────────────────────────────────────────────────────────────

const pool = new pg.Pool({
    connectionString: config.databaseUrl,
    keepAlive: true,
    max: 5,
});

pool.on("error", (err) => {
    console.error("[DB] Unexpected pool error:", err.message);
});

// ─── Schema ───────────────────────────────────────────────────────────────────

/**
 * Idempotent migration — safe to call on every startup.
 *
 * Key design decisions
 * ────────────────────
 * replays PK = (cycle, id)
 *   Replay IDs reset to 1 at the start of every cycle, so id alone is not
 *   globally unique. The composite key lets us keep every historical cycle
 *   in the same table without collisions.
 *
 * replaydata BYTEA
 *   Stored as raw PSON binary, gzip-compressed (see codec.js).
 *   ~40-60% smaller than the original base64/LZString-encoded API string.
 *
 * replay_players FK → (cycle, replay_id)
 *   Matches the composite PK on replays.
 *
 * maps PK = (mapid, version)
 *   A map's geometry can be edited by its author after publishing — bonk.io's
 *   own `dbv` field (found in every replay's startingState.mm) tells us which
 *   version any given replay was actually played on. Keying on (mapid, version)
 *   and never overwriting an existing row means we capture map history
 *   correctly for free, instead of clobbering older geometry with "latest
 *   wins" semantics.
 *
 * scraper_state
 *   Single row (id = 1). Stores both position and cycle so restarts and
 *   redeploys always resume exactly where they left off.
 */
export async function migrate() {

    // Separate + non-fatal: some managed Postgres users lack CREATE EXTENSION
    // privileges. If it fails, we just skip the trigram index below.
    try {
        await pool.query(`CREATE EXTENSION IF NOT EXISTS pg_trgm;`);
    } catch (err) {
        console.warn("[DB] Could not create pg_trgm extension (non-fatal):", err.message);
    }

    await pool.query(`
        CREATE TABLE IF NOT EXISTS scraper_state (
            id       INT     PRIMARY KEY DEFAULT 1,
            cycle    INT     NOT NULL,
            position BIGINT  NOT NULL
        );

        CREATE TABLE IF NOT EXISTS replays (
            cycle        INT         NOT NULL,
            id           BIGINT      NOT NULL,
            mapid        BIGINT,
            replaydata   BYTEA       NOT NULL,
            fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (cycle, id)
        );

        -- Query replays by cycle efficiently
        CREATE INDEX IF NOT EXISTS idx_replays_cycle
            ON replays (cycle);

        CREATE TABLE IF NOT EXISTS players (
            username   TEXT        PRIMARY KEY,
            level      INT         NOT NULL DEFAULT 0,
            avatars    JSONB       NOT NULL DEFAULT '[]'::jsonb,
            seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS replay_players (
            cycle      INT    NOT NULL,
            replay_id  BIGINT NOT NULL,
            username   TEXT   NOT NULL REFERENCES players(username) ON DELETE CASCADE,
            PRIMARY KEY (cycle, replay_id, username),
            FOREIGN KEY (cycle, replay_id) REFERENCES replays(cycle, id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_replay_players_username
            ON replay_players (username);

        CREATE INDEX IF NOT EXISTS idx_replay_players_cycle
            ON replay_players (cycle);

        -- Versioned map archive. See migrate() doc comment above for why
        -- (mapid, version) is the PK.
        CREATE TABLE IF NOT EXISTS maps (
            mapid       BIGINT      NOT NULL,
            version     INT         NOT NULL,
            name        TEXT,
            author      TEXT,
            author_id   BIGINT,
            published   BOOLEAN,
            votes_up    INT,
            votes_down  INT,
            remix_of    BIGINT,
            mapdata     BYTEA,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (mapid, version)
        );

        CREATE INDEX IF NOT EXISTS idx_maps_mapid_version
            ON maps (mapid, version DESC);
    `);

    // Separate call: only works if pg_trgm actually installed above.
    try {
        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_maps_name_trgm
                ON maps USING gin (name gin_trgm_ops);
        `);
    } catch (err) {
        console.warn("[DB] Could not create trigram index on maps.name (non-fatal):", err.message);
    }

    console.log("[DB] Schema up-to-date.");
}

// ─── State ────────────────────────────────────────────────────────────────────

/**
 * Returns { cycle, position }, creating the state row if this is the first run.
 * The seed values come from config (env vars CURRENT_CYCLE / START_POSITION).
 */
export async function getState() {
    const res = await pool.query(
        `INSERT INTO scraper_state (id, cycle, position)
         VALUES (1, $1, $2)
         ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id
         RETURNING cycle, position`,
        [config.currentCycle, config.defaultStartPosition]
    );
    const row = res.rows[0];
    return { cycle: row.cycle, position: Number(row.position) };
}

/**
 * Atomically saves the current cycle + position.
 * Called after every successful batch so the scraper can always resume cleanly.
 */
export async function saveState({ cycle, position }) {
    await pool.query(
        `UPDATE scraper_state SET cycle = $1, position = $2 WHERE id = 1`,
        [cycle, position]
    );
}

/**
 * Transitions to a new cycle.
 * - Increments the cycle number.
 * - Resets position to 1 (new cycle IDs start from the beginning).
 * - Returns the new { cycle, position } state.
 *
 * Existing replay/player data is intentionally preserved — historical cycles
 * are valuable. Only the scraper's cursor resets.
 */
export async function beginNewCycle(currentCycle) {
    const newCycle = currentCycle + 1;
    await pool.query(
        `UPDATE scraper_state SET cycle = $1, position = 1 WHERE id = 1`,
        [newCycle]
    );
    console.log(`[DB] Cycle transition: ${currentCycle} → ${newCycle}`);
    return { cycle: newCycle, position: 1 };
}

// ─── Replays ─────────────────────────────────────────────────────────────────

/**
 * Bulk-inserts a batch of replays and upserts related players and map
 * metadata in one transaction. If the transaction fails the entire batch is
 * rolled back — the scraper retries from the same position on the next
 * iteration.
 *
 * @param {number} cycle  The current cycle number.
 * @param {Array<{
 *   id: number,
 *   mapid: number|null,
 *   replayBytes: Buffer,
 *   players: Array<{username, level, avatar}>,
 *   map: {
 *     mapid: number,
 *     version: number,
 *     name: string|null,
 *     author: string|null,
 *     authorId: number|null,
 *     published: boolean|null,
 *     votesUp: number|null,
 *     votesDown: number|null,
 *     remixOf: number|null,
 *     mapBytes: Buffer|null,
 *   } | null
 * }>} docs
 */
export async function insertBatch(cycle, docs) {
    if (docs.length === 0) return;

    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        for (const doc of docs) {
            const inserted = await client.query(
                `INSERT INTO replays (cycle, id, mapid, replaydata)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (cycle, id) DO NOTHING
                 RETURNING id`,
                [cycle, doc.id, doc.mapid ?? null, doc.replayBytes]
            );

            // Versioned map upsert — DO NOTHING on conflict since a captured
            // (mapid, version) pair is immutable (see migrate() doc comment).
            if (doc.map?.mapid != null && doc.map?.version != null) {
                const m = doc.map;
                await client.query(
                    `INSERT INTO maps
                        (mapid, version, name, author, author_id, published,
                         votes_up, votes_down, remix_of, mapdata, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
                     ON CONFLICT (mapid, version) DO NOTHING`,
                    [
                        m.mapid,
                        m.version,
                        m.name ?? null,
                        m.author ?? null,
                        m.authorId ?? null,
                        m.published ?? null,
                        m.votesUp ?? null,
                        m.votesDown ?? null,
                        m.remixOf ?? null,
                        m.mapBytes ?? null,
                    ]
                );
            }

            for (const p of doc.players) {
                if (!p.username) continue;

                await client.query(
                    `INSERT INTO players (username, level, avatars, seen_at)
                     VALUES ($1, $2, $3::jsonb, NOW())
                     ON CONFLICT (username) DO UPDATE SET
                         level   = GREATEST(players.level, EXCLUDED.level),
                         avatars = (
                             SELECT jsonb_agg(DISTINCT elem)
                             FROM jsonb_array_elements(
                                 players.avatars || EXCLUDED.avatars
                             ) AS elem
                         ),
                         seen_at = NOW()`,
                    [p.username, p.level, JSON.stringify([p.avatar])]
                );

                if (inserted.rowCount > 0) {
                    await client.query(
                        `INSERT INTO replay_players (cycle, replay_id, username)
                         VALUES ($1, $2, $3)
                         ON CONFLICT DO NOTHING`,
                        [cycle, doc.id, p.username]
                    );
                }
            }
        }

        await client.query("COMMIT");
    } catch (err) {
        await client.query("ROLLBACK");
        throw err;
    } finally {
        client.release();
    }
}

// ─── Teardown ─────────────────────────────────────────────────────────────────

export async function closeDb() {
    await pool.end();
}