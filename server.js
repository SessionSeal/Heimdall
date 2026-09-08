/**
 * Heimdall — the SessionSeal gateway (Node/Express).
 *
 * The only service the Web app talks to. Responsibilities:
 *   - authenticate every /records* request against the Auth.js cookie
 *     (utils/auth.js), independently of Web;
 *   - mint presigned S3 uploads and orchestrate sealing (routes/records.js,
 *     routes/seal.js) — the heavy audio bytes go browser<->S3 direct, never
 *     through here;
 *   - proxy everything else (verify, checkers, labs) to Odin (routes/proxy.js).
 *
 * This file is wiring only. Logic lives in utils/ and routes/.
 */
const express = require("express");

const { PORT, ODIN_URL, SQS_URL, ASSETS_BUCKET, CORS_ORIGINS } = require("./config");
const recordRoutes = require("./routes/records");
const sealRoutes = require("./routes/seal");
const { proxyToOdin } = require("./routes/proxy");

const app = express();

// --- CORS (the Next app is the real caller) --------------------------------
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && CORS_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
  }
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// --- routes Heimdall owns --------------------------------------------------
app.use(recordRoutes);
app.use(sealRoutes);

// --- everything else -> Odin (must be last) --------------------------------
app.use(proxyToOdin);

const server = app.listen(PORT, "127.0.0.1", () => {
  console.log(`[heimdall] :${PORT} odin=${ODIN_URL} sqs=${SQS_URL ? "on" : "off"} ` +
              `store=${ASSETS_BUCKET ? `s3:${ASSETS_BUCKET}` : "local"}`);
});
server.requestTimeout = 0;
server.headersTimeout = 120000;
server.keepAliveTimeout = 75000;
