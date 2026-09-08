# Heimdall

The SessionSeal gateway (Node/Express, port 8000). The **only** service the
Web app talks to. It authenticates requests, mints presigned S3 uploads,
orchestrates sealing, serves record reads/downloads, and proxies everything
else to Odin. It holds no audio and does no analysis — the heavy file bytes
go browser↔S3 directly.

## File layout

```
server.js               Wiring only — express, CORS, mount routes, listen.
config.js               All env / process.env constants. Import from here;
                        nothing else reads process.env.
utils/
  db.js                 The DB connector. The ONLY file that imports `pg` or
                        holds the pool. Exports query(text, params) and
                        tx(fn) (transaction helper). Everything else runs SQL
                        through these.
  auth.js               requireUser middleware — decodes the Auth.js session
                        cookie (shared AUTH_SECRET) and checks users.active
                        on every request. Its user lookup uses utils/db.js.
  storage.js            S3 (or local stand-in): presignPut / presignGet /
                        assetSize / localAssetPath. Owns the S3 client.
  queue.js              nudgeThor — the SQS wake-up. Owns the SQS client.
  records-queries.js    ALL record/asset/job SQL, as named functions built on
                        utils/db.js. Route handlers call these, never inline SQL.
routes/
  records.js            /records/draft, /records, /records/:id,
                        /records/:id/verifications, /records/:id/release,
                        plus local-mode /uploads/local/* and /manifests/*.
  seal.js               POST /records/:id/seal + waitForSeal (the sync facade).
  proxy.js              Catch-all → Odin. Mounted LAST.
```

## The layering rule

`routes/*` (HTTP: auth, validation, status codes, response shape)
  → `utils/records-queries.js` (the SQL)
  → `utils/db.js` (the pool + query/tx).

`pg` is imported in exactly one place: `utils/db.js`. If you need a new query,
add a named function to `utils/records-queries.js` — don't write SQL in a route.

## Request flow

- Web calls Heimdall same-origin via its `/backend` proxy, so the session
  cookie rides along automatically. `requireUser` decrypts it independently
  of Web (Web's middleware is UX-gating only).
- Seal: draft (presign) → browser PUTs to S3 → seal (verify uploads → queue
  job → nudge Thor → `waitForSeal`). The jobs table is the source of truth;
  SQS is only a nudge; Thor does the actual pipeline.
- Anything Heimdall doesn't own (verify, checkers, labs) falls through to Odin.

## Run

```sh
node server.js          # needs .env (DATABASE_URL, AUTH_SECRET, AWS creds,
                        # SQS_SEAL_QUEUE_URL, S3_ASSETS_BUCKET, ODIN_URL)
```

## Notes / history

- `waitForSeal` is the synchronous facade over the async jobs table. The plan
  to replace it with 202 + client polling is in `infra/SEAL-RESILIENCE.md`.
- The legacy multipart `POST /product/register` (and its multer/archiver/
  lib-storage deps and the POC seed user) were removed in the 2026-09-08
  refactor that split this file out of a single 679-line server.js. Real
  intake is always draft → S3 → seal.
