import { config }                                    from "../config.js";
import { migrate, getState, saveState, beginNewCycle,
         insertBatch, closeDb }                      from "./db.js";
import { fetchReplays }                              from "./api.js";
import { encodeForStorage, compressJsonForStorage } from "./codec.js";

// ─── Helpers ──────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Generous relative to bonk.io's real client-side username limit (well under
// 24 chars) — only needs to stay safely below Postgres's btree index row-size
// ceiling (~2704 bytes on an 8KB page). See the players extraction below.
const MAX_USERNAME_LENGTH = 100;

function clamp(val, min, max) {
    return Math.min(Math.max(val, min), max);
}

// ─── Cycle-reset detection ────────────────────────────────────────────────────

/**
 * Returns true if the returned replay IDs indicate a new cycle has started.
 *
 * How it works:
 *   After being caught up, the next successful batch should have IDs just above
 *   our current position. If instead the max returned ID is tiny relative to
 *   our position, Chaz wiped the DB and the IDs have reset.
 *
 * Example: position = 1 400 000, threshold = 0.01 (1%)
 *   → maxReturnedId must be < 14 000 to trigger a cycle reset.
 *
 * We only check this AFTER being caught up (wasCaughtUp = true) because during
 * normal scraping the returned IDs will always be near our position. The
 * caught-up gate prevents false positives early in a cycle when our position
 * is still low.
 *
 * @param {number[]} returnedIds   IDs in the API response batch.
 * @param {number}   position      Current scraper position before this batch.
 * @param {boolean}  wasCaughtUp   Whether we were in caught-up state last iteration.
 */
function isCycleReset(returnedIds, position, wasCaughtUp) {
    if (!wasCaughtUp) return false;
    if (position < 1000) return false; // Too early in a cycle to have meaningful signal.

    const maxId = Math.max(...returnedIds);
    const isReset = maxId < position * config.cycleResetThreshold;

    if (isReset) {
        console.log(
            `[Cycle] Reset detected: position was ${position}, ` +
            `but new batch max ID is ${maxId} ` +
            `(${((maxId / position) * 100).toFixed(3)}% of position, ` +
            `threshold: ${config.cycleResetThreshold * 100}%)`
        );
    }

    return isReset;
}

// ─── Scraper ──────────────────────────────────────────────────────────────────

export class Scraper {
    #running     = false;
    #stopPromise = null;
    #stopResolve = null;

    stop(reason = "unknown") {
        if (!this.#running) return;
        console.log(`\n[Scraper] Shutdown requested (${reason}) — finishing current batch…`);
        this.#running = false;
        this.#stopResolve?.();
    }

    async run() {
        this.#running = true;
        this.#stopPromise = new Promise((r) => (this.#stopResolve = r));

        await migrate();

        let { cycle, position } = await getState();
        let consecutiveFails    = 0;
        let wasCaughtUp         = false;

        console.log(`[Scraper] Resuming — cycle ${cycle}, position ${position}`);

        while (this.#running) {
            const result = await fetchReplays(position);

            // ── Network error ─────────────────────────────────────────────────
            if (result.status === "network_error") {
                consecutiveFails++;
                const wait = clamp(
                    config.delays.initialBackoff * Math.pow(2, consecutiveFails - 1),
                    config.delays.initialBackoff,
                    config.delays.maxBackoff
                );
                console.error(
                    `[Scraper] Network error (fail #${consecutiveFails}): ${result.error.message}` +
                    ` — retrying in ${wait / 1000}s`
                );
                await this.#sleepInterruptible(wait);
                continue;
            }

            // ── Rate-limited ──────────────────────────────────────────────────
            if (result.status === "ratelimit") {
                console.warn(
                    `[Scraper] Rate-limited — waiting ${config.delays.rateLimit / 60_000} min`
                );
                await this.#sleepInterruptible(config.delays.rateLimit);
                continue;
            }

            // ── Caught up ─────────────────────────────────────────────────────
            if (result.status === "caught_up") {
                if (!wasCaughtUp) {
                    console.log(
                        `[Scraper] Caught up at cycle ${cycle}, position ${position} — ` +
                        `polling every ${config.delays.caughtUp / 60_000} min`
                    );
                }
                wasCaughtUp = true;
                await this.#sleepInterruptible(config.delays.caughtUp);
                continue;
            }

            // ── OK — we have replays ──────────────────────────────────────────
            consecutiveFails = 0;
            const { replays } = result;
            const returnedIds = replays.map((r) => r.id);

            // ── Cycle-reset check ─────────────────────────────────────────────
            if (isCycleReset(returnedIds, position, wasCaughtUp)) {
                ({ cycle, position } = await beginNewCycle(cycle));
                wasCaughtUp = false;
                console.log(`[Scraper] Now on cycle ${cycle} — restarting from position ${position}`);
                // Don't process this batch under the old cycle — loop again
                // from the new position so the fetch is clean.
                continue;
            }

            wasCaughtUp = false;

            // ── Decode & compress ─────────────────────────────────────────────
            const docs      = [];
            const failedIds = [];

            await Promise.all(
                replays.map(async (r) => {
                    try {
                        const { replayBytes, decoded } = await encodeForStorage(r.replaydata);

                        const players = (decoded.playerArray ?? [])
                            .filter(Boolean)
                            .map((p) => ({
                                username: typeof p.userName === "string" ? p.userName : null,
                                // Number.isFinite (not `??`) so a malformed/NaN
                                // level never reaches the DB — `?? 0` alone
                                // doesn't catch NaN, and an unguarded NaN here
                                // throws a Postgres integer error that the
                                // scraper's retry loop can't get past, stalling
                                // it on the same batch indefinitely instead of
                                // skipping the one bad replay.
                                level: Number.isFinite(p.level) ? p.level : 0,
                                avatar:   p.avatar   ?? null,
                            }))
                            .filter((p) => {
                                if (!p.username) return false;
                                // Same stall-forever risk as the level guard
                                // above, different failure mode: a corrupted
                                // decode can produce a "username" thousands of
                                // characters long without decodeReplayData ever
                                // throwing, which exceeds Postgres's btree
                                // index row-size limit on players_pkey and
                                // crashes the whole batch's insert.
                                if (p.username.length > MAX_USERNAME_LENGTH) {
                                    console.warn(
                                        `[Scraper] Dropping player with implausible username length ` +
                                            `(${p.username.length} chars) on replay ${r.id}`
                                    );
                                    return false;
                                }
                                return true;
                            });

                        // Pull map metadata + geometry, matching output.json's confirmed shape
                        const ss = decoded?.startingState ?? {};
                        const mm = ss.mm ?? {};

                        let map = null;
                        if (Number.isFinite(mm.dbid) && Number.isFinite(mm.dbv)) {
                            const mapBytes = await compressJsonForStorage({
                                physics:  ss.physics ?? null,
                                capZones: ss.capZones ?? null,
                            });
                            map = {
                                mapid:     mm.dbid,
                                version:   mm.dbv,
                                name:      mm.n ?? decoded.mn ?? null,
                                author:    mm.a ?? decoded.ma ?? null,
                                authorId:  Number.isFinite(mm.authid) ? mm.authid : null,
                                published: typeof mm.pub === "boolean" ? mm.pub : null,
                                votesUp:   Number.isFinite(mm.vu) ? mm.vu : null,
                                votesDown: Number.isFinite(mm.vd) ? mm.vd : null,
                                remixOf:   Number.isFinite(mm.rxid) && mm.rxid > 0 ? mm.rxid : null,
                                mapBytes,
                            };
                        }

                        docs.push({
                            id: r.id,
                            mapid: r.mapid ?? null,
                            replayBytes,
                            players,
                            map,
                        });
                    } catch (e) {
                        failedIds.push(r.id);
                        console.warn(`[Scraper] Decode failed for replay ${r.id}: ${e.message}`);
                    }
                })
            );

            // ── Persist ───────────────────────────────────────────────────────
            try {
                await insertBatch(cycle, docs);
            } catch (err) {
                console.error("[Scraper] DB insert failed:", err.message);
                await this.#sleepInterruptible(config.delays.initialBackoff);
                continue;
            }

            // ── Advance position ──────────────────────────────────────────────
            // Always advance by the full batch size (including decode failures)
            // so a permanently corrupt replay never blocks progress.
            position += replays.length;
            await saveState({ cycle, position });

            const invalid = replays.length - docs.length;
            if (invalid > 0) {
                console.log(
                    `[Scraper] Cycle ${cycle} | Batch: ${docs.length}/${replays.length} stored, ` +
                    `${invalid} decode-failed (ids: ${failedIds.join(", ")}). ` +
                    `Position → ${position}`
                );
            } else {
                console.log(
                    `[Scraper] Cycle ${cycle} | ${docs.length} stored. Position → ${position}`
                );
            }

            await this.#sleepInterruptible(config.delays.between);
        }

        console.log("[Scraper] Shutting down — closing DB…");
        await closeDb();
        console.log("[Scraper] Done.");
    }

    #sleepInterruptible(ms) {
        return Promise.race([sleep(ms), this.#stopPromise]);
    }
}