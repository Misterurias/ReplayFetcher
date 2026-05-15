import { config } from "./config.js";

// ─── Types ────────────────────────────────────────────────────────────────────
//
// ApiResult discriminated union:
//   { status: "ok",        replays: RawReplay[] }
//   { status: "ratelimit"                        }
//   { status: "caught_up"                        }
//   { status: "network_error", error: Error      }

// ─── Fetch ────────────────────────────────────────────────────────────────────

/**
 * Calls the bonk.io replay API.
 *
 * Returns an ApiResult — never throws (all errors are captured as
 * { status: "network_error" } so the scraper loop can handle them uniformly).
 *
 * @param {number} startingFrom  The replay ID to start fetching from.
 * @returns {Promise<ApiResult>}
 */
export async function fetchReplays(startingFrom) {
    let res;
    try {
        res = await fetch(config.apiUrl, {
            method:  "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body:    `version=${config.apiVersion}&startingFrom=${startingFrom}`,
            // Give bonk.io 15 seconds to respond before we treat it as down.
            signal:  AbortSignal.timeout(15_000),
        });
    } catch (err) {
        // Network error, DNS failure, timeout, bonk.io is down, etc.
        return { status: "network_error", error: err };
    }

    if (!res.ok) {
        return {
            status: "network_error",
            error:  new Error(`HTTP ${res.status} ${res.statusText}`),
        };
    }

    let body;
    try {
        body = await res.text();
    } catch (err) {
        return { status: "network_error", error: err };
    }

    // ── Parse ────────────────────────────────────────────────────────────────

    let json;
    try {
        json = JSON.parse(body);
    } catch {
        // Bonk occasionally returns an HTML error page or empty body.
        return {
            status: "network_error",
            error:  new Error(`Non-JSON response: ${body.slice(0, 120)}`),
        };
    }

    if (json.r !== "success") {
        // The API signals rate-limiting (and possibly other errors) this way.
        console.warn("[API] Non-success response:", JSON.stringify(json).slice(0, 200));
        return { status: "ratelimit" };
    }

    const replays = json.replays ?? [];

    if (replays.length === 0) {
        return { status: "caught_up" };
    }

    if (replays.length !== 10) {
        console.warn(`[API] Expected 10 replays, got ${replays.length} — near end of data?`);
    }

    return { status: "ok", replays };
}