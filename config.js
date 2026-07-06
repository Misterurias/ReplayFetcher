// ─── Bonk.io Replay Scraper — Config ─────────────────────────────────────────

export const config = {
    // ── Postgres ──────────────────────────────────────────────────────────────
    databaseUrl: process.env.DATABASE_URL,

    // ── Bonk API ──────────────────────────────────────────────────────────────
    apiUrl:     "https://bonk2.io/scripts/replay_get.php",
    apiVersion: 49,

    // ── Cycle tracking ────────────────────────────────────────────────────────
    //
    // The current known cycle number. Only used to seed scraper_state on the
    // very first run. After that the DB value is authoritative.
    //
    // A "cycle" is the period between Chaz wiping all replays. Each wipe resets
    // replay IDs back to a small number, so IDs alone are not globally unique —
    // (cycle, id) is the true primary key.
    currentCycle: parseInt(process.env.CURRENT_CYCLE ?? "6", 10),

    // ── Cycle-reset detection ─────────────────────────────────────────────────
    //
    // After we're caught up, if the next successful batch's max ID is less than
    // (our current position × this threshold), we treat it as a new cycle.
    //
    // Example: position = 1 400 000, threshold = 0.01
    //   → any batch whose max ID < 14 000 is flagged as a reset.
    //
    // 0.01 (1%) is conservative: even if Chaz starts the new cycle at ID 50 000
    // it would still be far below 1% of a mature cycle's position.
    cycleResetThreshold: parseFloat(process.env.CYCLE_RESET_THRESHOLD ?? "0.01"),

    // ── Rate-limiting & delays (all in ms) ───────────────────────────────────
    delays: {
        between:        3_600,           // ~1 req / 3.6 s  →  ~1 000 req/hr  (limit: 10 000)
        rateLimit:      65 * 60_000,     // wait after a rate-limit response
        caughtUp:        2 * 60_000,     // poll interval when fully caught up
        initialBackoff: 30_000,          // first retry delay on network error
        maxBackoff:     30 * 60_000,     // cap for exponential back-off
    },

    // ── Scraper start position ────────────────────────────────────────────────
    // Only used the very first time (no row in scraper_state yet).
    defaultStartPosition: parseInt(process.env.START_POSITION ?? "1", 10),
};