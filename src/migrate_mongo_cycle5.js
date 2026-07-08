#!/usr/bin/env node
/**
 * One-time migration: pull the cycle-5 MongoDB archive (restored locally as
 * `bonk_cycle5`, collection `replays`) into the same Postgres schema the live
 * scraper writes to, tagged as cycle 5 — so cycle 5 becomes queryable exactly
 * like cycle 6 already is via the search API.
 *
 * IMPORTANT: unlike our own scraper's rows, the Mongo docs have NO top-level
 * mapid — the only place map info exists is inside the encoded `replaydata`
 * string. So every document has to be decoded (same codec.js pipeline the
 * scraper already uses) to recover mapid/map metadata/geometry, not just
 * copied across field-for-field.
 *
 * The Mongo doc's own `players` array is intentionally NOT used — it's
 * redundant with what decoding replaydata already yields (decoded.playerArray
 * has the same userName/level/avatar), and using the decoded version keeps a
 * single source of truth consistent with how scraper.js already works.
 *
 * Uses db.js's insertBatchBulk() — a handful of multi-row INSERT ... UNNEST
 * statements per batch instead of one query per player per replay — so this
 * doesn't inherit insertBatch()'s per-row round-trip cost (that pattern
 * measured ~3/sec here, which would have been 60+ hours for 729K docs).
 * insertBatchBulk() is a separate function from the live scraper's
 * insertBatch(); the scraper's own batches are always exactly 10 replays
 * (bonk.io's fixed page size), where round-trip count was never the issue,
 * so that code path is untouched.
 *
 * Requires the `mongodb` package: npm install mongodb
 *
 * Usage:
 *   node --env-file=.env src/migrate_mongo_cycle5.js
 *   node --env-file=.env src/migrate_mongo_cycle5.js --batch-size=200
 *   node --env-file=.env src/migrate_mongo_cycle5.js --fresh        (ignore checkpoint)
 *   node --env-file=.env src/migrate_mongo_cycle5.js --dry-run       (decode-only sample —
 *                                                                     see caveat below)
 *
 * Env vars:
 *   MONGO_URL     default mongodb://127.0.0.1:27017
 *   MONGO_DB      default bonk_cycle5
 *   TARGET_CYCLE  default 5
 */

import { MongoClient } from "mongodb";
import { readFile, writeFile, unlink } from "node:fs/promises";
import { migrate, insertBatchBulk, closeDb } from "./db.js";
import { encodeForStorage, compressJsonForStorage } from "./codec.js";

const MONGO_URL = process.env.MONGO_URL || "mongodb://127.0.0.1:27017";
const MONGO_DB = process.env.MONGO_DB || "bonk_cycle5";
const TARGET_CYCLE = parseInt(process.env.TARGET_CYCLE ?? "5", 10);

// ── Args ──────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const flag = (name, def) => {
  const hit = args.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.split("=")[1] : def;
};

const BATCH_SIZE = parseInt(flag("batch-size", "200"), 10);
const CHECKPOINT_FILE = flag("checkpoint", ".mongo-migration-checkpoint.json");
const FRESH = args.includes("--fresh");
const DRY_RUN = args.includes("--dry-run");

// ── Checkpoint ──────────────────────────────────────────────────────────────────

async function loadCheckpoint() {
  if (FRESH) return null;
  try {
    return JSON.parse(await readFile(CHECKPOINT_FILE, "utf8"));
  } catch {
    return null;
  }
}

async function saveCheckpoint(lastId, stats) {
  await writeFile(CHECKPOINT_FILE, JSON.stringify({ lastId, stats }, null, 2));
}

async function clearCheckpoint() {
  await unlink(CHECKPOINT_FILE).catch(() => {});
}

// ── Per-document processing ────────────────────────────────────────────────────
// Mirrors scraper.js's extraction logic exactly, just sourced from a Mongo doc's
// `replaydata` string instead of the live bonk.io API response.

async function processDoc(doc) {
  try {
    const { replayBytes, decoded } = await encodeForStorage(doc.replaydata);

    // MAX_USERNAME_LENGTH is generous relative to bonk.io's real client-side
    // limit (well under 24 chars) — it only needs to stay safely below
    // Postgres's btree index row-size ceiling (~2704 bytes on an 8KB page).
    // A corrupted decode can produce a "username" that's thousands of
    // characters of garbage without decodeReplayData ever throwing, and an
    // unguarded value like that crashes the whole batch's insert on
    // players_pkey rather than just dropping the one bad row.
    const MAX_USERNAME_LENGTH = 100;

    const players = (decoded.playerArray ?? [])
      .filter(Boolean)
      .map((p) => ({
        username: typeof p.userName === "string" ? p.userName : null,
        level: Number.isFinite(p.level) ? p.level : 0,
        avatar: p.avatar ?? null,
      }))
      .filter((p) => {
        if (!p.username) return false;
        if (p.username.length > MAX_USERNAME_LENGTH) {
          console.warn(
            `[Migrate] Dropping player with implausible username length ` +
              `(${p.username.length} chars) on doc id=${doc.id}: "${p.username.slice(0, 40)}…"`
          );
          return false;
        }
        return true;
      });

    const ss = decoded?.startingState ?? {};
    const mm = ss.mm ?? {};

    const mapid = Number.isFinite(mm.dbid) ? mm.dbid : null;
    const version = Number.isFinite(mm.dbv) ? mm.dbv : null;

    let map = null;
    if (mapid != null && version != null) {
      const mapBytes = await compressJsonForStorage({
        physics: ss.physics ?? null,
        capZones: ss.capZones ?? null,
      });

      const remixOf = Number.isFinite(mm.rxid) ? mm.rxid : null;

      map = {
        mapid,
        version,
        name: mm.n ?? decoded.mn ?? null,
        author: mm.a ?? decoded.ma ?? null,
        authorId: Number.isFinite(mm.authid) ? mm.authid : null,
        published: typeof mm.pub === "boolean" ? mm.pub : null,
        votesUp: Number.isFinite(mm.vu) ? mm.vu : null,
        votesDown: Number.isFinite(mm.vd) ? mm.vd : null,
        remixOf: remixOf != null && remixOf > 0 ? remixOf : null,
        mapBytes,
      };
    }

    return {
      ok: true,
      id: doc.id,
      mapid,
      replayBytes,
      players,
      map,
    };
  } catch (e) {
    return { ok: false, id: doc.id, error: e.message };
  }
}

// ── Dry run ─────────────────────────────────────────────────────────────────────
// Decode-only sample. Deliberately does NOT claim to estimate total runtime —
// the maps backfill taught us decode speed and DB-insert speed are very
// different bottlenecks. Use this to sanity-check decode success rate; use the
// real run's first few logged batches to judge actual throughput.

async function runDryRun(collection) {
  const total = await collection.countDocuments();
  const sample = await collection.find({}).sort({ id: 1 }).limit(300).toArray();

  let ok = 0,
    fails = 0,
    withMap = 0;

  const started = Date.now();
  for (const doc of sample) {
    const r = await processDoc(doc);
    if (r.ok) {
      ok++;
      if (r.map) withMap++;
    } else {
      fails++;
    }
  }
  const elapsedMs = Date.now() - started;
  const rate = sample.length / (elapsedMs / 1000);

  console.log(`[Dry run] ${total.toLocaleString()} total docs in Mongo \`${MONGO_DB}.replays\`.`);
  console.log(`[Dry run] Sample ${sample.length}: ${ok} decoded OK (${withMap} with map data), ${fails} failed.`);
  console.log(`[Dry run] Decode rate: ${rate.toFixed(1)} docs/sec (decode only — NOT the actual DB-insert rate).`);
  console.log(
    `[Dry run] Run the real migration and watch the first few "[Migrate] ..." lines for the true ` +
      `throughput, since insertBatch()'s DB round-trips are the real bottleneck, not decoding.`
  );
}

// ── Main loop ───────────────────────────────────────────────────────────────────

async function main() {
  const client = new MongoClient(MONGO_URL);
  await client.connect();
  const collection = client.db(MONGO_DB).collection("replays");

  if (DRY_RUN) {
    await runDryRun(collection);
    await client.close();
    return;
  }

  await migrate(); // idempotent — safe even though the schema already exists

  const total = await collection.countDocuments();
  const checkpoint = await loadCheckpoint();

  let lastId = checkpoint?.lastId ?? null;
  let scanned = checkpoint?.stats?.scanned ?? 0;
  let stored = checkpoint?.stats?.stored ?? 0;
  let fails = checkpoint?.stats?.fails ?? 0;

  console.log(
    checkpoint
      ? `[Migrate] Resuming — ${scanned}/${total} already scanned (last Mongo id ${lastId}).`
      : `[Migrate] Starting fresh — ${total.toLocaleString()} docs → Postgres cycle ${TARGET_CYCLE}.`
  );

  const startedAt = Date.now();

  while (true) {
    const query = lastId != null ? { id: { $gt: lastId } } : {};
    const batch = await collection.find(query).sort({ id: 1 }).limit(BATCH_SIZE).toArray();

    if (batch.length === 0) break;

    const results = await Promise.all(batch.map(processDoc));
    const docs = results.filter((r) => r.ok);
    const failed = results.filter((r) => !r.ok);

    for (const f of failed) {
      console.warn(`[Migrate] Decode failed for Mongo id=${f.id}: ${f.error}`);
    }
    fails += failed.length;

    if (docs.length > 0) {
      try {
        const result = await insertBatchBulk(TARGET_CYCLE, docs);
        stored += result.replaysInserted;
      } catch (err) {
        // Don't advance the checkpoint past this batch — bail so a rerun
        // retries from lastId rather than skipping past a failed batch.
        console.error("[Migrate] insertBatchBulk failed — stopping so this batch can be retried:", err.message);
        await client.close();
        process.exit(1);
      }
    }

    scanned += batch.length;
    lastId = batch[batch.length - 1].id;
    await saveCheckpoint(lastId, { scanned, stored, fails });

    const pct = ((scanned / total) * 100).toFixed(1);
    const elapsed = ((Date.now() - startedAt) / 1000).toFixed(0);
    const rate = (scanned / Math.max(1, (Date.now() - startedAt) / 1000)).toFixed(1);
    console.log(
      `[Migrate] ${scanned}/${total} (${pct}%) scanned, ${stored} stored, ${fails} failed — ` +
        `${elapsed}s elapsed, ${rate}/sec`
    );
  }

  console.log(`[Migrate] Done. ${scanned} scanned, ${stored} stored, ${fails} failed.`);
  await clearCheckpoint();
  await client.close();
  await closeDb();
}

main().catch((err) => {
  console.error("[Migrate] FATAL:", err);
  process.exit(1);
});