#!/usr/bin/env node
/**
 * Usage:
 *   node --env-file=.env src/read-replay.js <replay_id> [cycle]
 *
 * Examples:
 *   node --env-file=.env src/read-replay.js 12105
 *   node --env-file=.env src/read-replay.js 12105 6
 *
 * Prints the decoded replay object as formatted JSON to stdout.
 * If you want to pipe it: add  2>/dev/null  to suppress the log lines.
 */

import pg            from "pg";
import { gunzip }    from "node:zlib";
import { promisify } from "node:util";
import PSON          from "pson";

const gunzipAsync = promisify(gunzip);

// ── Args ──────────────────────────────────────────────────────────────────────

const [,, replayIdArg, cycleArg] = process.argv;

if (!replayIdArg) {
    console.error("Usage: node src/read-replay.js <replay_id> [cycle]");
    process.exit(1);
}

const replayId = parseInt(replayIdArg, 10);
const cycle    = cycleArg ? parseInt(cycleArg, 10) : null;

// ── PSON dictionary (must match bonk.io exactly) ──────────────────────────────

const pairs = new PSON.StaticPair([
    "physics", "shapes", "fixtures", "bodies", "bro", "joints", "ppm",
    "lights", "spawns", "lasers", "capZones", "type", "w", "h", "c", "a",
    "v", "l", "s", "sh", "fr", "re", "de", "sn", "fc", "fm", "f", "d", "n",
    "bg", "lv", "av", "ld", "ad", "fr", "bu", "cf", "rv", "p", "d", "bf",
    "ba", "bb", "aa", "ab", "axa", "dr", "em", "mmt", "mms", "ms", "ut",
    "lt", "New body", "Box Shape", "Circle Shape", "Polygon Shape",
    "EdgeChain Shape", "priority", "Light", "Laser", "Cap Zone", "BG Shape",
    "Background Layer", "Rotate Joint", "Slider Joint", "Rod Joint",
    "Gear Joint", 65535, 16777215,
]);

// ── DB ────────────────────────────────────────────────────────────────────────

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

async function main() {
    // If no cycle specified, grab the most recent one with this replay ID
    const query = cycle != null
        ? `SELECT cycle, id, mapid, fetched_at, replaydata
           FROM replays WHERE cycle = $1 AND id = $2`
        : `SELECT cycle, id, mapid, fetched_at, replaydata
           FROM replays WHERE id = $1
           ORDER BY cycle DESC LIMIT 1`;

    const params = cycle != null ? [cycle, replayId] : [replayId];
    const res    = await pool.query(query, params);

    if (res.rows.length === 0) {
        console.error(
            cycle != null
                ? `No replay found: cycle=${cycle}, id=${replayId}`
                : `No replay found with id=${replayId}`
        );
        process.exit(1);
    }

    const row = res.rows[0];
    console.error(`Found: cycle=${row.cycle}, id=${row.id}, mapid=${row.mapid}, fetched_at=${row.fetched_at}`);

    // ── Decode: gunzip → PSON binary → JS object ──────────────────────────────
    const psonBytes = await gunzipAsync(row.replaydata);
    const decoded   = pairs.decode(psonBytes.buffer);

    console.log(JSON.stringify(decoded, null, 2));
}

main()
    .catch((err) => { console.error(err); process.exit(1); })
    .finally(() => pool.end());