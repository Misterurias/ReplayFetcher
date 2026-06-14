// ─── Bonkverse Replay Decoder — Web server ───────────────────────────────────
//
// A tiny zero-dependency HTTP server (Node's built-in `http`) that:
//
//   GET  /               → serves public/index.html
//   POST /api/decode     → { replaydata } → fully decoded replay + pipeline stats
//   GET  /api/latest     → newest 10 replays from bonk.io (auto-finds the frontier)
//   GET  /api/fetch?startingFrom=N
//                        → 10 replays starting at a specific id (manual override)
//
// Run:  node src/server.js     (PORT defaults to 3000; Railway injects PORT)
//
// It reuses the project's real codec (codec.js) and API client (api.js), so what
// you see here is exactly what the scraper sees.

import http from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { dirname, join } from "node:path";

import LZString from "lz-string";

import { config }                         from "../config.js";
import { decodeReplayData,
         compressForStorage }             from "./codec.js";
import { fetchReplays }                   from "./api.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = join(__dirname, "..", "public");
const INDEX_HTML = join(PUBLIC_DIR, "index.html");

// Static assets served from public/ (strict allowlist — no path traversal).
const STATIC_FILES = {
    "/skin-shapes.js": "application/javascript; charset=utf-8",
    "/skin-render.js": "application/javascript; charset=utf-8",
};

const PORT           = parseInt(process.env.PORT ?? "3000", 10);
const MAX_BODY_BYTES = 12 * 1024 * 1024; // 12 MB — replays can be chunky
const BATCH          = 10;               // bonk returns 10 replays per call

// ─── Helpers ──────────────────────────────────────────────────────────────────

const sendJson = (res, status, obj) => {
    const body = JSON.stringify(obj);
    res.writeHead(status, {
        "Content-Type":   "application/json; charset=utf-8",
        "Content-Length": Buffer.byteLength(body),
        "Cache-Control":  "no-store",
    });
    res.end(body);
};

function readBody(req) {
    return new Promise((resolve, reject) => {
        let size = 0;
        const chunks = [];
        req.on("data", (c) => {
            size += c.length;
            if (size > MAX_BODY_BYTES) { reject(new Error("Request body too large")); req.destroy(); return; }
            chunks.push(c);
        });
        req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
        req.on("error", reject);
    });
}

// Same first-100-char case flip codec.js uses — kept here only to measure the
// base64 intermediate size for the pipeline view. The authoritative decode still
// goes through decodeReplayData().
function flip100(str) {
    return str.split("").map((ch, i) =>
        i <= 100 ? (ch === ch.toLowerCase() ? ch.toUpperCase() : ch.toLowerCase()) : ch
    ).join("");
}

function extractPlayers(decoded) {
    const scores = decoded?.startingState?.scores;
    return (decoded?.playerArray ?? [])
        .map((p, i) => p && {
            slot:     i,
            username: p.userName ?? null,
            level:    p.level ?? 0,
            team:     p.team ?? null,
            ping:     p.ping ?? null,
            guest:    p.guest ?? false,
            score:    Array.isArray(scores) ? (scores[i] ?? null) : null,
            avatar:   p.avatar ?? null,
        })
        .filter((p) => p && p.username);
}

// Pull the human-meaningful header info out of a decoded replay. Bonk stores the
// same facts in a couple of places, so we fall back across them.
function extractMeta(decoded) {
    const ss = decoded?.startingState ?? {};
    const mm = ss.mm ?? {};
    const gs = decoded?.gameSettings ?? {};
    return {
        mapName:   decoded?.mn ?? mm.n ?? null,
        mapAuthor: decoded?.ma ?? mm.a ?? null,
        mapId:     mm.dbid ?? null,          // bonk map database id (== API mapid)
        mode:      gs.mo ?? null,            // "ar", "b", … (short code)
        teams:     typeof gs.tea === "boolean" ? gs.tea : null,
        winLimit:  gs.wl ?? null,
        quickplay: gs.q ?? null,
        seed:      ss.seed ?? null,
        rounds:    ss.rc ?? null,
        scores:    Array.isArray(ss.scores) ? ss.scores : null,
        frames:    decoded?.es ?? null,      // replay length (physics steps)
    };
}

const slim = (r) => ({ id: r.id, mapid: r.mapid ?? null, replaydata: r.replaydata });

// ─── Latest-replay discovery ──────────────────────────────────────────────────
//
// The bonk API is a forward cursor: fetchReplays(N) returns up to 10 replays with
// id ≥ N, or "caught_up" when N is past the newest replay. To show the *latest*
// replays we first locate the newest id, then fetch the window ending there.
//
// Cold discovery is an exponential bracket + binary search (~25 calls on a mature
// ~1.4M-id cycle), with an early exit: any batch under 10 replays is the tail, so
// its max id is the newest. The result is cached briefly; a warm call just walks
// forward a few batches from the known frontier (1–3 calls).

let maxCache = { id: null, at: 0 };
const MAX_TTL_MS = 30_000;

function probeError(result) {
    const e = new Error(
        result.status === "ratelimit"
            ? "bonk.io rate-limited the request. Wait a minute and try again."
            : `Couldn't reach bonk.io: ${result.error?.message ?? "network error"}`
    );
    e.kind = result.status === "ratelimit" ? "ratelimit" : "network";
    return e;
}

// Probe one cursor position. → { ok:true, count, maxId, replays } | { ok:false }
async function probe(id, fetcher) {
    const r = await fetcher(Math.max(1, id));
    if (r.status === "ok") {
        const ids = r.replays.map((x) => x.id);
        return { ok: true, count: r.replays.length, maxId: Math.max(...ids), replays: r.replays };
    }
    if (r.status === "caught_up") return { ok: false };
    throw probeError(r);
}

async function findMaxId(fetcher) {
    const first = await probe(1, fetcher);
    if (!first.ok) return null;                  // empty cycle
    if (first.count < BATCH) return first.maxId; // whole cycle fits in one batch

    // Exponential bracket: grow until we overshoot into empty space.
    let lo = 1, hi = 0, step = 10_000;
    while (true) {
        const p = await probe(step, fetcher);
        if (p.ok) {
            if (p.count < BATCH) return p.maxId;          // landed in the tail
            lo = step; step *= 2;
            if (step > 100_000_000) { hi = step; break; } // safety cap
        } else { hi = step; break; }
    }

    // Binary search between lo (non-empty) and hi (empty).
    while (hi - lo > 1) {
        const mid = Math.floor((lo + hi) / 2);
        const p = await probe(mid, fetcher);
        if (p.ok) { if (p.count < BATCH) return p.maxId; lo = mid; }
        else hi = mid;
    }
    const tail = await probe(lo, fetcher);
    return tail.ok ? tail.maxId : lo;
}

// Warm path: from a known frontier, walk forward to find any newer replays.
// Bounded — returns null (→ full rediscovery) if too far behind.
async function walkForward(known, fetcher) {
    let cur = known;
    for (let i = 0; i < 20; i++) {
        const p = await probe(cur + 1, fetcher);
        if (!p.ok) return cur;                  // nothing newer
        if (p.count < BATCH) return p.maxId;    // reached the new tail
        cur = p.maxId;
    }
    return null; // >200 new replays since last check → rediscover from scratch
}

async function discoverLatest(fetcher = fetchReplays) {
    const now = Date.now();
    let maxId = maxCache.id;

    if (maxId === null || now - maxCache.at > MAX_TTL_MS) {
        if (maxId !== null) {
            const walked = await walkForward(maxId, fetcher);
            maxId = walked ?? await findMaxId(fetcher);
        } else {
            maxId = await findMaxId(fetcher);
        }
        maxCache = { id: maxId, at: now };
    }

    if (maxId === null) return { empty: true, replays: [] };

    const from = Math.max(1, maxId - (BATCH - 1));
    const p = await probe(from, fetcher);
    return { empty: false, maxId, from, replays: p.ok ? p.replays : [] };
}

// ─── Route: POST /api/decode ────────────────────────────────────────────────

async function handleDecode(req, res) {
    let payload;
    try { payload = JSON.parse(await readBody(req)); }
    catch { return sendJson(res, 400, { ok: false, error: "Body must be valid JSON." }); }

    const raw = (payload?.replaydata ?? "").trim();
    if (!raw) return sendJson(res, 400, { ok: false, error: "Paste an encoded replaydata string first." });

    try {
        const base64 = LZString.decompressFromEncodedURIComponent(flip100(raw)); // size only
        const { decoded, rawBuffer } = decodeReplayData(raw);                     // authoritative
        const gz = await compressForStorage(rawBuffer);

        const encodedChars = raw.length;
        const stats = {
            encodedChars,
            base64Chars: base64 ? base64.length : 0,
            psonBytes:   rawBuffer.length,
            gzipBytes:   gz.length,
            savingsPct:  encodedChars > 0 ? Math.round((1 - gz.length / encodedChars) * 1000) / 10 : 0,
        };
        const topKeys = decoded && typeof decoded === "object" ? Object.keys(decoded) : [];

        return sendJson(res, 200, {
            ok: true, stats, meta: extractMeta(decoded),
            players: extractPlayers(decoded), topKeys, decoded,
        });
    } catch (err) {
        return sendJson(res, 422, {
            ok: false,
            error: err?.message ?? "Decode failed.",
            stage: classifyError(err?.message ?? ""),
        });
    }
}

function classifyError(msg) {
    const m = msg.toLowerCase();
    if (m.includes("lzstring") || m.includes("decompress")) return "lz";
    if (m.includes("base64"))                                return "base64";
    if (m.includes("pson") || m.includes("decode"))          return "pson";
    return "input";
}

// ─── Route: GET /api/latest ──────────────────────────────────────────────────

async function handleLatest(res) {
    try {
        const out = await discoverLatest();
        if (out.empty) {
            return sendJson(res, 200, { ok: true, count: 0, replays: [], note: "No replays available right now." });
        }
        return sendJson(res, 200, {
            ok: true, maxId: out.maxId, from: out.from,
            count: out.replays.length, replays: out.replays.map(slim),
        });
    } catch (err) {
        return sendJson(res, err.kind === "ratelimit" ? 429 : 502, { ok: false, error: err.message });
    }
}

// ─── Route: GET /api/fetch?startingFrom=N ────────────────────────────────────

async function handleFetch(res, url) {
    const startingFrom = parseInt(url.searchParams.get("startingFrom") ?? "1", 10);
    if (!Number.isFinite(startingFrom) || startingFrom < 0) {
        return sendJson(res, 400, { ok: false, error: "startingFrom must be a positive integer." });
    }

    const result = await fetchReplays(startingFrom);
    switch (result.status) {
        case "ok":
            return sendJson(res, 200, {
                ok: true, startingFrom, count: result.replays.length, replays: result.replays.map(slim),
            });
        case "caught_up":
            return sendJson(res, 200, {
                ok: true, startingFrom, count: 0, replays: [],
                note: `No replays at or after ${startingFrom} — that's past the newest replay. Try a lower id, or use Fetch latest.`,
            });
        case "ratelimit":
            return sendJson(res, 429, { ok: false, error: "bonk.io rate-limited the request. Wait a minute and try again." });
        default:
            return sendJson(res, 502, { ok: false, error: `Couldn't reach bonk.io: ${result.error?.message ?? "unknown error"}` });
    }
}

// ─── Route: GET / ────────────────────────────────────────────────────────────

async function handleIndex(res) {
    try {
        const html = await readFile(INDEX_HTML);
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
        res.end(html);
    } catch {
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("index.html not found. Expected at public/index.html");
    }
}

async function handleStatic(res, pathname) {
    const contentType = STATIC_FILES[pathname];
    if (!contentType) { sendJson(res, 404, { ok: false, error: "Not found." }); return; }
    try {
        const body = await readFile(join(PUBLIC_DIR, pathname.slice(1)));
        res.writeHead(200, {
            "Content-Type":  contentType,
            "Cache-Control": "public, max-age=86400",
        });
        res.end(body);
    } catch {
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end(`${pathname} not found. Expected at public${pathname}`);
    }
}

// ─── Server ──────────────────────────────────────────────────────────────────

export const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host ?? "localhost"}`);
    try {
        if (req.method === "GET"  && url.pathname === "/")            return handleIndex(res);
        if (req.method === "GET"  && url.pathname in STATIC_FILES)    return handleStatic(res, url.pathname);
        if (req.method === "POST" && url.pathname === "/api/decode")  return handleDecode(req, res);
        if (req.method === "GET"  && url.pathname === "/api/latest")  return handleLatest(res);
        if (req.method === "GET"  && url.pathname === "/api/fetch")   return handleFetch(res, url);
        sendJson(res, 404, { ok: false, error: "Not found." });
    } catch (err) {
        console.error("[Server] Unhandled error:", err);
        sendJson(res, 500, { ok: false, error: "Internal server error." });
    }
});

// Only start listening when run directly (so tests can import without binding a port).
const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
    server.listen(PORT, () => {
        console.log(`[Decoder] Listening on http://localhost:${PORT}`);
        console.log(`[Decoder] Bonk API: ${config.apiUrl} (version ${config.apiVersion})`);
    });
}

// Exported for testing.
export { findMaxId, discoverLatest };