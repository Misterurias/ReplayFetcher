# bonk-scraper

A 24/7 Bonk.io replay scraper that stores decoded, compressed replays in Postgres. Built for Railway deployment.

---

## Architecture

```
bonk2.io API  ──fetch──►  api.js        (HTTP, typed result union)
                              │
                              ▼
                          scraper.js    (main loop, backoff, shutdown)
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
                codec.js             db.js
          (decode + gzip)      (Postgres pool, schema, upserts)
```

### Storage design

| Field | Type | Notes |
|-------|------|-------|
| `replays.replaydata` | `BYTEA` | Raw PSON binary, gzip-compressed |
| `players.avatars` | `JSONB` | Append-only set, queryable |
| `replay_players` | join table | Many-to-many, indexed on `username` |
| `scraper_state.position` | `BIGINT` | Survives restarts/redeploys |

**Why BYTEA + gzip instead of storing the original encoded string?**

The original `replaydata` string from the API is:
  `raw PSON binary → base64 (+33%) → LZ-compressed → URI-encoded`

We reverse all of that to get the raw PSON bytes, then gzip them.
Result: **40–60% smaller** than the original string, with full round-trip fidelity.

---

## Resilience

| Scenario | Behaviour |
|----------|-----------|
| bonk.io down / timeout | Exponential back-off: 30s → 1min → 2min … up to 30min |
| Rate-limited (non-success response) | Wait 65 minutes, then retry |
| Caught up (no new replays) | Wait 5 minutes, then check again |
| DB insert failure | Don't advance position — retry same batch next iteration |
| Decode failure on a replay | Log it, skip it, still advance position to avoid infinite loop |
| SIGINT / SIGTERM | Finish current batch, close DB, exit cleanly |

---

## Railway deployment

### 1. Create a new Railway project

```bash
railway init
```

### 2. Add a Postgres plugin

In the Railway dashboard → **New** → **Database** → **Postgres**.

Railway will inject `DATABASE_URL` into your service automatically.

### 3. Set environment variables

```
START_POSITION=<first replay id of the current cycle, e.g. 12096>
```

(`DATABASE_URL` is injected automatically — you don't need to set it.)

### 4. Deploy

```bash
railway up
```

Or connect your GitHub repo in the Railway dashboard for automatic deploys on push.

### 5. Monitor

Railway's log viewer will show lines like:

```
[DB] Schema up-to-date.
[Scraper] Starting from position 12096
[Scraper] Batch: 10 stored. Position → 12106
[Scraper] Batch: 10 stored. Position → 12116
...
[Scraper] Caught up at position 1382000 — waiting 5 min for new replays
```

---

## Local development

```bash
cp .env.example .env
# edit .env with your local Postgres credentials

npm install
node --env-file=.env src/index.js
```

---

## Starting a new replay cycle

When Chaz wipes replays and a new cycle begins:

1. Find the first ID of the new cycle (visible in the API response or bonkverse).
2. In Railway → Variables, update `START_POSITION` to that ID.
3. In Railway → Postgres, run:
   ```sql
   UPDATE scraper_state SET position = <new_start_id> WHERE id = 1;
   ```
   (Or just truncate all tables and let the scraper recreate the state row.)
4. Redeploy.

---

## Querying the data

```sql
-- All replays a player appeared in
SELECT r.id, r.mapid, r.fetched_at
FROM replays r
JOIN replay_players rp ON rp.replay_id = r.id
WHERE rp.username = 'SomePlayer'
ORDER BY r.id DESC;

-- Top players by number of replays
SELECT rp.username, COUNT(*) AS replay_count
FROM replay_players rp
GROUP BY rp.username
ORDER BY replay_count DESC
LIMIT 20;

-- Current scrape position
SELECT position FROM scraper_state;
```

---

## File structure

```
src/
  index.js     Entry point, signal handlers
  scraper.js   Main loop — fetch → decode → store → advance
  api.js       bonk.io HTTP client, typed result union
  codec.js     LZString/PSON decode + gzip compress/decompress
  db.js        Postgres pool, migrations, batch upsert
config.js      All constants and env-var reads
railway.toml   Railway build/deploy config
.env.example   Required environment variables
```