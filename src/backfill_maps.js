#!/usr/bin/env node
/**
 * One-time backfill: decode every replay currently in `replays` and populate
 * the `maps` table with map metadata + geometry, so historical replays
 * become searchable by map name (not just replays scraped after the maps
 * feature shipped).
 *
 * Safe to re-run — every write is `ON CONFLICT (mapid, version) DO NOTHING`,
 * so already-captured map versions are never overwritten. Progress is
 * checkpointed to a local file (--checkpoint, default .backfill-checkpoint)
 * so an interrupted run can resume without rescanning from the start.
 *
 * Usage:
 *   node --env-file=.env src/backfill_maps.js
 *   node --env-file=.env src/backfill_maps.js --batch-size=250
 *   node --env-file=.env src/backfill_maps.js --fresh        (ignore checkpoint)
 *   node --env-file=.env src/backfill_maps.js --dry-run       (estimate only, writes nothing)
 *   node --env-file=.env src/backfill_maps.js --dry-run --sample-size=1000
 */

import pg   from "pg";
import PSON from "pson";
import { readFile, writeFile, unlink } from "node:fs/promises";
import { compressJsonForStorage, decompressFromStorage } from "./codec.js";

// ── PSON dictionary (must match bonk.io exactly — same as codec.js/read_replay.js) ──

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

// ── Args ──────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const flag = (name, def) => {
    const hit = args.find((a) => a.startsWith(`--${name}=`));
    return hit ? hit.split("=")[1] : def;
};

const BATCH_SIZE      = parseInt(flag("batch-size", "200"), 10);
const CHECKPOINT_FILE = flag("checkpoint", ".backfill-checkpoint.json");
const FRESH           = args.includes("--fresh");
const DRY_RUN         = args.includes("--dry-run");
const SAMPLE_SIZE     = parseInt(flag("sample-size", "500"), 10);

// ── DB ────────────────────────────────────────────────────────────────────────

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 5 });

async function loadCheckpoint() {
    if (FRESH) return null;
    try {
        const raw = await readFile(CHECKPOINT_FILE, "utf8");
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

async function saveCheckpoint(cursor, stats) {
    await writeFile(CHECKPOINT_FILE, JSON.stringify({ cursor, stats }, null, 2));
}

async function clearCheckpoint() {
    await unlink(CHECKPOINT_FILE).catch(() => {});
}

// ── Small helper — normalize NaN/non-finite values to null before they
// hit an INT/BIGINT column (Postgres rejects NaN with a cryptic 22P02).
function toInt(v) {
    return typeof v === "number" && Number.isFinite(v) ? v : null;
}

// ── Per-replay decode → map metadata ───────────────────────────────────────────
// Mirrors the extraction logic in scraper.js, but reads from the stored gzip'd
// PSON blob (decompressFromStorage) instead of the raw API string
// (decodeReplayData), since these rows are already in the DB.

async function extractMap(row) {
    const psonBytes = await decompressFromStorage(row.replaydata);
    const decoded    = pairs.decode(psonBytes.buffer);

    const ss = decoded?.startingState ?? {};
    const mm = ss.mm ?? {};

    const mapid   = toInt(mm.dbid);
    const version = toInt(mm.dbv);

    if (mapid == null || version == null) return null;

    const mapBytes = await compressJsonForStorage({
        physics:  ss.physics  ?? null,
        capZones: ss.capZones ?? null,
    });

    const remixOf = toInt(mm.rxid);

    return {
        mapid,
        version,
        name:      mm.n ?? decoded.mn ?? null,
        author:    mm.a ?? decoded.ma ?? null,
        authorId:  toInt(mm.authid),
        published: mm.pub ?? null,
        votesUp:   toInt(mm.vu),
        votesDown: toInt(mm.vd),
        remixOf:   remixOf != null && remixOf > 0 ? remixOf : null,
        mapBytes,
    };
}

async function upsertMaps(client, mapDocs) {
    let stored = 0;
    for (const m of mapDocs) {
        if (!m) continue;
        const res = await client.query(
            `INSERT INTO maps
                (mapid, version, name, author, author_id, published,
                 votes_up, votes_down, remix_of, mapdata, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
             ON CONFLICT (mapid, version) DO NOTHING`,
            [
                m.mapid, m.version, m.name, m.author, m.authorId,
                m.published, m.votesUp, m.votesDown, m.remixOf, m.mapBytes,
            ]
        );
        if (res.rowCount > 0) stored++;
    }
    return stored;
}

// ── Dry run ─────────────────────────────────────────────────────────────────────
// Decodes a sample (default 500 rows, spread across the start/middle/end of the
// table so the estimate isn't skewed by one cycle's data shape) and extrapolates
// timing + new-row counts from it. Writes nothing — no transaction, no
// checkpoint, `maps` and `replays` both untouched.

async function sampleRows(offset, limit) {
    const { rows } = await pool.query(
        `SELECT cycle, id, mapid, replaydata FROM replays
         ORDER BY cycle, id
         OFFSET $1 LIMIT $2`,
        [offset, limit]
    );
    return rows;
}

async function runDryRun() {
    const { rows: [{ count }] } = await pool.query(`SELECT COUNT(*)::bigint AS count FROM replays`);
    const total = Number(count);

    if (total === 0) {
        console.log(`[Dry run] \`replays\` is empty — nothing to backfill.`);
        return;
    }

    const perChunk = Math.max(1, Math.floor(SAMPLE_SIZE / 3));
    const startOffset  = 0;
    const middleOffset = Math.max(0, Math.floor(total / 2) - Math.floor(perChunk / 2));
    const endOffset    = Math.max(0, total - perChunk);

    console.log(`[Dry run] Sampling ~${perChunk * 3} rows from the start, middle, and end of \`replays\` (${total.toLocaleString()} total)...`);

    const [start, middle, end] = await Promise.all([
        sampleRows(startOffset, perChunk),
        sampleRows(middleOffset, perChunk),
        sampleRows(endOffset, perChunk),
    ]);

    const sampleRowsAll = [...start, ...middle, ...end];

    if (sampleRowsAll.length === 0) {
        console.log(`[Dry run] \`replays\` is empty — nothing to backfill.`);
        return;
    }

    const decodeStarted = Date.now();
    let fails = 0;
    const mapDocs = await Promise.all(
        sampleRowsAll.map(async (row) => {
            try {
                return await extractMap(row);
            } catch (e) {
                fails++;
                return null;
            }
        })
    );
    const decodeElapsedMs = Date.now() - decodeStarted;

    const decodedOk   = mapDocs.filter(Boolean).length;
    const noMapId      = sampleRowsAll.length - decodedOk - fails;
    const distinctPairs = new Set(mapDocs.filter(Boolean).map((m) => `${m.mapid}:${m.version}`));

    // Check how many of the sampled (mapid, version) pairs are already in `maps`
    // (relevant if you're re-running after a partial backfill).
    let alreadyStored = 0;
    if (distinctPairs.size > 0) {
        const pairsArr = [...distinctPairs].map((p) => p.split(":").map(Number));
        const { rows } = await pool.query(
            `SELECT COUNT(*)::bigint AS n FROM maps m
             WHERE (m.mapid, m.version) IN (
                 SELECT * FROM UNNEST($1::bigint[], $2::int[])
             )`,
            [pairsArr.map((p) => p[0]), pairsArr.map((p) => p[1])]
        );
        alreadyStored = Number(rows[0].n);
    }

    const rowsPerSec   = sampleRowsAll.length / (decodeElapsedMs / 1000);
    const estTotalSec  = total / rowsPerSec;
    const estMinutes   = (estTotalSec / 60).toFixed(1);

    console.log(``);
    console.log(`[Dry run] ── Results ─────────────────────────────────────`);
    console.log(`  Total replays in DB:        ${total.toLocaleString()}`);
    console.log(`  Sample size:                ${sampleRowsAll.length}`);
    console.log(`  Decoded OK:                 ${decodedOk}`);
    console.log(`  No mapid/version present:   ${noMapId}`);
    console.log(`  Decode failures:            ${fails}`);
    console.log(`  Distinct (mapid,version):   ${distinctPairs.size} in sample`);
    console.log(`  Already in \`maps\` table:    ${alreadyStored} of those`);
    console.log(`  Decode rate:                ${rowsPerSec.toFixed(1)} rows/sec`);
    console.log(`  Estimated full run time:    ~${estMinutes} min (${estTotalSec.toFixed(0)}s)`);
    console.log(`[Dry run] ─────────────────────────────────────────────────`);
    console.log(``);
    console.log(`No writes were made. Run without --dry-run to perform the real backfill.`);
}

// ── Main loop ───────────────────────────────────────────────────────────────────
// Keyset pagination over the existing (cycle, id) primary key — no new index
// needed, and unlike OFFSET this stays fast no matter how deep into the table
// we are.

async function main() {
    if (DRY_RUN) {
        await runDryRun();
        return;
    }

    const { rows: [{ count }] } = await pool.query(`SELECT COUNT(*)::bigint AS count FROM replays`);
    const total = Number(count);

    const checkpoint = await loadCheckpoint();
    let cursor  = checkpoint?.cursor ?? null;
    let scanned = checkpoint?.stats?.scanned ?? 0;
    let stored  = checkpoint?.stats?.stored ?? 0;
    let fails   = checkpoint?.stats?.fails ?? 0;

    if (checkpoint) {
        console.log(`[Backfill] Resuming from checkpoint: ${scanned}/${total} already scanned.`);
    } else {
        console.log(`[Backfill] Starting fresh — ${total} replays to scan.`);
    }

    const startedAt = Date.now();

    while (true) {
        const { rows: batch } = await pool.query(
            cursor
                ? `SELECT cycle, id, mapid, replaydata FROM replays
                   WHERE (cycle, id) > ($1, $2)
                   ORDER BY cycle, id
                   LIMIT $3`
                : `SELECT cycle, id, mapid, replaydata FROM replays
                   ORDER BY cycle, id
                   LIMIT $1`,
            cursor ? [cursor.cycle, cursor.id, BATCH_SIZE] : [BATCH_SIZE]
        );

        if (batch.length === 0) break;

        const mapDocs = await Promise.all(
            batch.map(async (row) => {
                try {
                    return await extractMap(row);
                } catch (e) {
                    fails++;
                    console.warn(`[Backfill] Decode failed cycle=${row.cycle} id=${row.id}: ${e.message}`);
                    return null;
                }
            })
        );

        const client = await pool.connect();
        try {
            await client.query("BEGIN");
            stored += await upsertMaps(client, mapDocs);
            await client.query("COMMIT");
        } catch (err) {
            await client.query("ROLLBACK");
            throw err;
        } finally {
            client.release();
        }

        scanned += batch.length;
        cursor = { cycle: batch[batch.length - 1].cycle, id: batch[batch.length - 1].id };
        await saveCheckpoint(cursor, { scanned, stored, fails });

        const pct     = total > 0 ? ((scanned / total) * 100).toFixed(1) : "?";
        const elapsed = ((Date.now() - startedAt) / 1000).toFixed(0);
        console.log(
            `[Backfill] ${scanned}/${total} (${pct}%) scanned, ` +
            `${stored} map versions stored, ${fails} decode fails — ${elapsed}s elapsed`
        );
    }

    console.log(`[Backfill] Done. ${scanned} replays scanned, ${stored} map versions stored, ${fails} decode failures.`);
    await clearCheckpoint();
}

main()
    .catch((err) => { console.error("[Backfill] FATAL:", err); process.exit(1); })
    .finally(() => pool.end());