/**
 * Heimdall — the MotherTape gateway (Node).
 *
 * Responsibilities (auth arrives in a later step):
 *   - register intake: stage uploads in STORAGE_DIR/uploads (local S3
 *     stand-in), create the QUEUED record + SEAL job, nudge thor via SQS,
 *     then wait for the result — a synchronous facade over the async
 *     pipeline, so the web app needs no changes yet.
 *   - GET /records/:id            job status (the future polling endpoint)
 *   - GET /records/:id/release    release master staged by thor
 *   - everything else             streamed reverse-proxy to odin
 *
 * Contracts kept from the FastAPI version: error bodies are
 * {detail: string}; the jobs table is the truth and a failed SQS send
 * only costs latency.
 */

require("dotenv").config();

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const archiver = require("archiver");
const express = require("express");
const httpProxy = require("http-proxy");
const multer = require("multer");
const { Pool } = require("pg");
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

const PORT = parseInt(process.env.PORT || "8000", 10);
const ODIN_URL = process.env.ODIN_URL || "http://127.0.0.1:8001";
const STORAGE_DIR = process.env.STORAGE_DIR ||
  path.resolve(__dirname, "..", "_localstore");
const UPLOADS = path.join(STORAGE_DIR, "uploads");
const RELEASE = path.join(STORAGE_DIR, "release");
const SEAL_TIMEOUT_S = parseInt(process.env.SEAL_TIMEOUT_S || "900", 10);
const SQS_URL = process.env.SQS_SEAL_QUEUE_URL || null;
const POC_USER_EMAIL = "poc@mothertape.local";

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
const sqs = SQS_URL
  ? new SQSClient({ region: process.env.AWS_REGION || "ap-south-1" })
  : null;

const app = express();

// ---------------------------------------------------------------------------
// CORS (the web app may call directly as well as via its dev proxy)
// ---------------------------------------------------------------------------
const ORIGINS = new Set(["http://localhost:3000", "http://127.0.0.1:3000"]);
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
  }
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// ---------------------------------------------------------------------------
// db helpers (mirrors the shared python pg.py contracts)
// ---------------------------------------------------------------------------
let pocUserIdCache = null;
async function pocUserId() {
  if (pocUserIdCache) return pocUserIdCache;
  const { rows } = await pool.query(
    `insert into users (email, name, signup_source)
     values ($1, 'POC seed user', 'OTHER')
     on conflict (email) do update set updated_at = now()
     returning id`, [POC_USER_EMAIL]);
  pocUserIdCache = rows[0].id;
  return pocUserIdCache;
}

async function createRecordAndJob(recordId, artist, payload) {
  const userId = await pocUserId();
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(
      `insert into records (id, user_id, artist_name, status, watermark_payload)
       values ($1, $2, $3, 'QUEUED', $4)`,
      [recordId, userId, artist, recordId.replaceAll("-", "")]);
    await client.query(
      `insert into jobs (kind, status, record_id, user_id, payload)
       values ('SEAL', 'QUEUED', $1, $2, $3)`,
      [recordId, userId, payload]);
    await client.query("commit");
  } catch (e) {
    await client.query("rollback");
    throw e;
  } finally {
    client.release();
  }
}

async function jobForRecord(recordId) {
  const { rows } = await pool.query(
    `select id, status, attempts, error, result
       from jobs where record_id = $1 and kind = 'SEAL'
      order by queued_at desc limit 1`, [recordId]);
  return rows[0] || null;
}

async function nudgeThor(recordId) {
  if (!sqs) return;
  try {
    await sqs.send(new SendMessageCommand({
      QueueUrl: SQS_URL,
      MessageBody: JSON.stringify({ kind: "SEAL", record_id: recordId }),
    }));
  } catch (e) {
    console.log(`[heimdall] sqs nudge failed (thor will poll): ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// register intake
// ---------------------------------------------------------------------------
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, _file, cb) => {
      if (!req.stagingDir) {
        req.stagingDir = path.join(UPLOADS, `.tmp-${crypto.randomUUID()}`);
        fs.mkdirSync(req.stagingDir, { recursive: true });
      }
      cb(null, req.stagingDir);
    },
    filename: (_req, _file, cb) => cb(null, crypto.randomUUID()),
  }),
  limits: { fileSize: 2 * 1024 * 1024 * 1024, files: 1200, fieldSize: 1024 * 1024 },
});

const registerFields = upload.fields([
  { name: "master", maxCount: 1 },
  { name: "stems", maxCount: 64 },
  { name: "project", maxCount: 1 },
  { name: "project_files", maxCount: 1024 },
]);

function safeName(name, fallback) {
  return path.basename(name || fallback);
}

function buildProjectZip(files, relPaths, outPath) {
  return new Promise((resolve, reject) => {
    const out = fs.createWriteStream(outPath);
    const zip = archiver("zip", { zlib: { level: 6 } });
    out.on("close", resolve);
    zip.on("error", reject);
    zip.pipe(out);
    for (let i = 0; i < files.length; i++) {
      const rel = String(relPaths[i]).replaceAll("\\", "/").replace(/^\/+/, "");
      if (!rel || rel.split("/").includes("..")) {
        return reject(Object.assign(new Error(`unsafe path: ${rel}`), { status: 422 }));
      }
      zip.file(files[i].path, { name: rel });
    }
    zip.finalize();
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

app.post("/product/register", registerFields, async (req, res) => {
  const cleanup = () => {
    if (req.stagingDir) fs.rmSync(req.stagingDir, { recursive: true, force: true });
  };
  try {
    const artist = (req.body.artist || "").trim();
    const master = (req.files.master || [])[0];
    const stems = req.files.stems || [];
    const project = (req.files.project || [])[0];
    const projectFiles = req.files.project_files || [];
    let projectPaths = req.body.project_paths || [];
    if (typeof projectPaths === "string") projectPaths = [projectPaths];

    if (!artist) return res.status(422).json({ detail: "artist is required" });
    if (!master) return res.status(422).json({ detail: "master is required" });
    if (stems.length < 2) {
      return res.status(422).json({ detail: "upload at least two stems" });
    }

    const recordId = crypto.randomUUID();
    const updir = path.join(UPLOADS, recordId);
    fs.mkdirSync(updir, { recursive: true });

    const masterName = safeName(master.originalname, "master.wav");
    const masterPath = path.join(updir, `master_${masterName}`);
    fs.renameSync(master.path, masterPath);

    const stemPaths = [];
    stems.forEach((s, i) => {
      const p = path.join(updir, `stem_${i}_${safeName(s.originalname, "stem.wav")}`);
      fs.renameSync(s.path, p);
      stemPaths.push(p);
    });

    const projectPath = path.join(updir, "project.zip");
    if (project) {
      fs.renameSync(project.path, projectPath);
    } else if (projectFiles.length && projectFiles.length === projectPaths.length) {
      await buildProjectZip(projectFiles, projectPaths, projectPath);
    } else {
      cleanup();
      return res.status(422).json(
        { detail: "provide the .logicx as a zip or as folder files+paths" });
    }
    cleanup();

    await createRecordAndJob(recordId, artist, {
      artist,
      master_path: masterPath,
      master_name: masterName,
      stem_paths: stemPaths,
      project_zip_path: projectPath,
    });
    await nudgeThor(recordId);

    // Synchronous facade: wait for thor. The wizard's polling upgrade
    // (GET /records/:id) replaces this wait in a later step.
    const deadline = Date.now() + SEAL_TIMEOUT_S * 1000;
    while (Date.now() < deadline) {
      await sleep(1000);
      const job = await jobForRecord(recordId);
      if (!job) continue;
      if (job.status === "SUCCEEDED") return res.json(job.result);
      if (job.status === "FAILED") return res.status(422).json({ detail: job.error });
    }
    return res.status(504).json({ detail: "sealing timed out" });
  } catch (e) {
    cleanup();
    const status = e.status || 500;
    console.error("[heimdall] register error:", e);
    return res.status(status).json({ detail: e.message || "internal error" });
  }
});

// ---------------------------------------------------------------------------
// record status + release download
// ---------------------------------------------------------------------------
app.get("/records/:id", async (req, res) => {
  let job;
  try {
    job = await jobForRecord(req.params.id);
  } catch {
    return res.status(422).json({ detail: "bad record id" });
  }
  if (!job) return res.status(404).json({ detail: "no such record" });
  res.json({ record_id: req.params.id, status: job.status,
             error: job.error, result: job.result });
});

app.get("/records/:id/release", (req, res) => {
  const id = path.basename(req.params.id);
  const file = path.join(RELEASE, `${id}.wav`);
  if (!fs.existsSync(file)) {
    return res.status(404).json({ detail: "no release file" });
  }
  res.download(file, `${id}_watermarked_signed.wav`);
});

// ---------------------------------------------------------------------------
// everything else streams to odin (link, manifest, checkers, labs, simulate)
// ---------------------------------------------------------------------------
const proxy = httpProxy.createProxyServer({
  target: ODIN_URL,
  proxyTimeout: 600000,
  timeout: 600000,
});
proxy.on("error", (err, _req, res) => {
  if (res && !res.headersSent && res.writeHead) {
    res.writeHead(502, { "Content-Type": "application/json" });
  }
  if (res && res.end) res.end(JSON.stringify({ detail: "odin is unreachable" }));
});

app.use((req, res) => proxy.web(req, res));

const server = app.listen(PORT, "127.0.0.1", () => {
  console.log(`[heimdall] listening on :${PORT}, odin=${ODIN_URL}, ` +
              `sqs=${SQS_URL ? "on" : "off"}, storage=${STORAGE_DIR}`);
});
// The sync seal facade can hold a request for many minutes.
server.requestTimeout = 0;
server.headersTimeout = 120000;
server.keepAliveTimeout = 75000;
