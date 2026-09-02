/**
 * Heimdall — the MotherTape gateway (Node).
 *
 * Two intake flows:
 *   NEW (S3-shaped): POST /records/draft  -> DRAFT record + asset rows +
 *     presigned PUT urls; the browser uploads directly (real S3, or this
 *     service's /uploads/local/* stand-in); POST /records/:id/seal then
 *     verifies the uploads, queues the SEAL job, nudges thor, and waits.
 *   LEGACY (multipart): POST /product/register — stages the files itself
 *     and runs the same seal path (kept for the POC console).
 *
 * Storage backend: S3 when S3_ASSETS_BUCKET is set, otherwise _localstore
 * mirroring the exact S3 layout ({userId}/{recordId}/{assetId}.{ext};
 * manifests v1/{recordId}.json). Flipping to real buckets is an env
 * change only. The jobs table is the truth; SQS is the nudge.
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
const { S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand } =
  require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { decode } = require("@auth/core/jwt");

const PORT = parseInt(process.env.PORT || "8000", 10);
const PUBLIC_URL = process.env.HEIMDALL_PUBLIC_URL || `http://127.0.0.1:${PORT}`;
const ODIN_URL = process.env.ODIN_URL || "http://127.0.0.1:8001";
const STORAGE_DIR = process.env.STORAGE_DIR ||
  path.resolve(__dirname, "..", "_localstore");
const SEAL_TIMEOUT_S = parseInt(process.env.SEAL_TIMEOUT_S || "900", 10);
const SQS_URL = process.env.SQS_SEAL_QUEUE_URL || null;
const AWS_REGION = process.env.AWS_REGION || "ap-south-1";
const ASSETS_BUCKET = process.env.S3_ASSETS_BUCKET || null;
const LOCAL_BUCKET = "_localstore";
const POC_USER_EMAIL = "poc@mothertape.local";

const KIND_EXT = { MASTER: null, STEM: null, PROJECT: ".zip" }; // null = keep upload's ext

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
const sqs = SQS_URL ? new SQSClient({ region: AWS_REGION }) : null;
const s3 = ASSETS_BUCKET ? new S3Client({ region: AWS_REGION }) : null;

const app = express();

// ---------------------------------------------------------------------------
// CORS
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
// storage backend (S3 | local stand-in with identical layout)
// ---------------------------------------------------------------------------
function localAssetPath(key) {
  const p = path.normalize(path.join(STORAGE_DIR, "assets", key));
  if (!p.startsWith(path.join(STORAGE_DIR, "assets"))) {
    throw Object.assign(new Error("unsafe key"), { status: 422 });
  }
  return p;
}

async function presignPut(key, contentType) {
  if (s3) {
    return getSignedUrl(s3, new PutObjectCommand({
      Bucket: ASSETS_BUCKET, Key: key, ContentType: contentType,
    }), { expiresIn: 3600 });
  }
  return `${PUBLIC_URL}/uploads/local/${key.split("/").map(encodeURIComponent).join("/")}`;
}

async function presignGet(key, filename) {
  if (s3) {
    return getSignedUrl(s3, new GetObjectCommand({
      Bucket: ASSETS_BUCKET, Key: key,
      ResponseContentDisposition: `attachment; filename="${filename}"`,
    }), { expiresIn: 3600 });
  }
  return null; // local mode streams the file directly
}

async function assetSize(key) {
  if (s3) {
    try {
      const h = await s3.send(new HeadObjectCommand({ Bucket: ASSETS_BUCKET, Key: key }));
      return h.ContentLength ?? 0;
    } catch { return null; }
  }
  try { return fs.statSync(localAssetPath(key)).size; } catch { return null; }
}

function putLocalAssetFromFile(srcPath, key) {
  const dest = localAssetPath(key);
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.renameSync(srcPath, dest);
}

// ---------------------------------------------------------------------------
// db helpers
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

async function createDraftWithAssets(recordId, artist, userId, assets) {
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(
      `insert into records (id, user_id, artist_name, status, watermark_payload)
       values ($1, $2, $3, 'DRAFT', $4)`,
      [recordId, userId, artist, recordId.replaceAll("-", "")]);
    for (const a of assets) {
      await client.query(
        `insert into assets (id, owner_user_id, record_id, kind, position,
                             status, s3_bucket, s3_key, original_filename,
                             content_type, size_bytes)
         values ($1, $2, $3, $4, $5, 'PENDING_UPLOAD', $6, $7, $8, $9, $10)`,
        [a.id, userId, recordId, a.kind, a.position,
         ASSETS_BUCKET || LOCAL_BUCKET, a.key, a.filename,
         a.content_type, a.size_bytes ?? null]);
    }
    await client.query("commit");
  } catch (e) {
    await client.query("rollback");
    throw e;
  } finally {
    client.release();
  }
}

async function inputAssetsForRecord(recordId) {
  const { rows } = await pool.query(
    `select id, kind, position, status, s3_key, original_filename, content_type
       from assets
      where record_id = $1 and kind in ('MASTER', 'STEM', 'PROJECT')
        and deleted_at is null
      order by kind, position`, [recordId]);
  return rows;
}

async function queueSeal(recordId, userId, payload) {
  const client = await pool.connect();
  try {
    await client.query("begin");
    const r = await client.query(
      `update records set status = 'QUEUED'
        where id = $1 and status = 'DRAFT' returning id`, [recordId]);
    if (r.rowCount === 0) {
      throw Object.assign(new Error("record is not in DRAFT state"), { status: 409 });
    }
    await client.query(
      `update assets set status = 'UPLOADED'
        where record_id = $1 and status = 'PENDING_UPLOAD'`, [recordId]);
    await client.query(
      `insert into jobs (kind, status, record_id, user_id, payload)
       values ('SEAL', 'QUEUED', $1, $2, $3)`, [recordId, userId, payload]);
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

async function releaseAssetForRecord(recordId) {
  const { rows } = await pool.query(
    `select s3_key, original_filename from assets
      where record_id = $1 and kind = 'RELEASE_MASTER' and deleted_at is null
      order by created_at desc limit 1`, [recordId]);
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
// auth: verify the Auth.js session cookie independently of the web app.
// The cookie is a JWE keyed by the shared AUTH_SECRET; after decoding we
// still check users.active so suspension bites despite stateless tokens.
// ---------------------------------------------------------------------------
const AUTH_SECRET = process.env.AUTH_SECRET || null;
const SESSION_COOKIES = ["__Secure-authjs.session-token", "authjs.session-token"];

function readCookies(req) {
  const out = {};
  for (const part of (req.headers.cookie || "").split(";")) {
    const i = part.indexOf("=");
    if (i > 0) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}

async function requireUser(req, res, next) {
  try {
    if (!AUTH_SECRET) {
      return res.status(500).json({ detail: "auth is not configured" });
    }
    const cookies = readCookies(req);
    let token = null;
    for (const name of SESSION_COOKIES) {
      if (!cookies[name]) continue;
      token = await decode({ token: decodeURIComponent(cookies[name]),
                             secret: AUTH_SECRET, salt: name }).catch(() => null);
      if (token) break;
    }
    if (!token?.uid) {
      return res.status(401).json({ detail: "sign in required" });
    }
    const { rows } = await pool.query(
      "select id, active from users where id = $1 and deleted_at is null",
      [token.uid]);
    if (!rows.length || !rows[0].active) {
      return res.status(401).json({ detail: "account unavailable" });
    }
    req.user = { id: rows[0].id };
    next();
  } catch (e) {
    console.error("[heimdall] auth error:", e.message);
    res.status(401).json({ detail: "sign in required" });
  }
}

async function recordOwner(recordId) {
  const { rows } = await pool.query(
    "select user_id from records where id = $1", [recordId]);
  return rows.length ? rows[0].user_id : null;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitForSeal(recordId, res) {
  const deadline = Date.now() + SEAL_TIMEOUT_S * 1000;
  while (Date.now() < deadline) {
    await sleep(1000);
    const job = await jobForRecord(recordId);
    if (!job) continue;
    if (job.status === "SUCCEEDED") return res.json(job.result);
    if (job.status === "FAILED") return res.status(422).json({ detail: job.error });
  }
  return res.status(504).json({ detail: "sealing timed out" });
}

// ---------------------------------------------------------------------------
// NEW flow: draft + presigned uploads + seal
// ---------------------------------------------------------------------------
app.post("/records/draft", requireUser, express.json(), async (req, res) => {
  try {
    const artist = (req.body.artist || "").trim();
    const files = req.body.files || [];
    if (!artist) return res.status(422).json({ detail: "artist is required" });
    const kinds = files.map((f) => f.kind);
    if (kinds.filter((k) => k === "MASTER").length !== 1 ||
        kinds.filter((k) => k === "STEM").length < 2 ||
        kinds.filter((k) => k === "PROJECT").length !== 1) {
      return res.status(422).json(
        { detail: "need exactly one MASTER, one PROJECT, and two or more STEMs" });
    }

    const userId = req.user.id;
    const recordId = crypto.randomUUID();
    let stemPos = 0;
    const assets = files.map((f) => {
      const assetId = crypto.randomUUID();
      const origExt = path.extname(f.filename || "").toLowerCase() || ".bin";
      const ext = KIND_EXT[f.kind] ?? origExt;
      return {
        id: assetId,
        kind: f.kind,
        position: f.kind === "STEM" ? stemPos++ : 0,
        key: `${userId}/${recordId}/${assetId}${ext || origExt}`,
        filename: path.basename(f.filename || "file"),
        content_type: f.content_type || "application/octet-stream",
        size_bytes: f.size_bytes,
      };
    });

    await createDraftWithAssets(recordId, artist, userId, assets);
    const uploads = [];
    for (const a of assets) {
      uploads.push({
        asset_id: a.id, kind: a.kind, position: a.position,
        filename: a.filename, key: a.key,
        url: await presignPut(a.key, a.content_type),
      });
    }
    res.json({ record_id: recordId, uploads });
  } catch (e) {
    console.error("[heimdall] draft error:", e);
    res.status(e.status || 500).json({ detail: e.message || "internal error" });
  }
});

// Local-mode stand-in for S3 presigned PUTs: raw body streamed to disk.
app.put("/uploads/local/*", (req, res) => {
  if (s3) return res.status(404).json({ detail: "local uploads disabled in s3 mode" });
  let key;
  try {
    key = decodeURIComponent(req.params[0]);
    const dest = localAssetPath(key);
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    const out = fs.createWriteStream(dest);
    req.pipe(out);
    out.on("finish", () => res.sendStatus(200));
    out.on("error", (e) => res.status(500).json({ detail: e.message }));
  } catch (e) {
    res.status(e.status || 500).json({ detail: e.message });
  }
});

app.post("/records/:id/seal", requireUser, express.json(), async (req, res) => {
  try {
    const recordId = req.params.id;
    const userId = req.user.id;
    if (await recordOwner(recordId) !== userId) {
      return res.status(404).json({ detail: "no such draft" });
    }
    const inputs = await inputAssetsForRecord(recordId);
    if (!inputs.length) return res.status(404).json({ detail: "no such draft" });

    // verify every upload actually landed before queueing
    for (const a of inputs) {
      const size = await assetSize(a.s3_key);
      if (size === null || size === 0) {
        return res.status(422).json(
          { detail: `upload missing for ${a.kind} ${a.original_filename}` });
      }
      await pool.query("update assets set size_bytes = $1 where id = $2",
                       [size, a.id]);
    }

    const master = inputs.find((a) => a.kind === "MASTER");
    const project = inputs.find((a) => a.kind === "PROJECT");
    const stems = inputs.filter((a) => a.kind === "STEM")
      .sort((x, y) => x.position - y.position);
    const { rows } = await pool.query(
      "select artist_name from records where id = $1", [recordId]);
    if (!rows.length) return res.status(404).json({ detail: "no such record" });

    await queueSeal(recordId, userId, {
      version: 2,
      artist: rows[0].artist_name,
      user_id: userId,
      master_key: master.s3_key,
      master_name: master.original_filename,
      stem_keys: stems.map((s) => s.s3_key),
      stem_names: stems.map((s) => s.original_filename),
      project_key: project.s3_key,
    });
    await nudgeThor(recordId);
    return waitForSeal(recordId, res);
  } catch (e) {
    console.error("[heimdall] seal error:", e);
    res.status(e.status || 500).json({ detail: e.message || "internal error" });
  }
});

// ---------------------------------------------------------------------------
// LEGACY flow: multipart register (POC console) — same seal path underneath
// ---------------------------------------------------------------------------
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, _file, cb) => {
      if (!req.stagingDir) {
        req.stagingDir = path.join(STORAGE_DIR, "tmp", `.reg-${crypto.randomUUID()}`);
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

async function stageAsset(userId, recordId, kind, position, srcPath, filename,
                          contentType) {
  const assetId = crypto.randomUUID();
  const ext = KIND_EXT[kind] ?? (path.extname(filename || "") || ".bin");
  const key = `${userId}/${recordId}/${assetId}${ext}`;
  if (s3) {
    const { Upload } = require("@aws-sdk/lib-storage");
    await new Upload({
      client: s3,
      params: { Bucket: ASSETS_BUCKET, Key: key,
                Body: fs.createReadStream(srcPath),
                ContentType: contentType },
    }).done();
    fs.rmSync(srcPath, { force: true });
  } else {
    putLocalAssetFromFile(srcPath, key);
  }
  await pool.query(
    `insert into assets (id, owner_user_id, record_id, kind, position, status,
                         s3_bucket, s3_key, original_filename, content_type)
     values ($1, $2, $3, $4, $5, 'UPLOADED', $6, $7, $8, $9)`,
    [assetId, userId, recordId, kind, position, ASSETS_BUCKET || LOCAL_BUCKET,
     key, filename, contentType]);
  return key;
}

app.post("/product/register", requireUser, registerFields, async (req, res) => {
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

    let projectSrc;
    if (project) {
      projectSrc = project.path;
    } else if (projectFiles.length && projectFiles.length === projectPaths.length) {
      projectSrc = path.join(req.stagingDir, "project.zip");
      await buildProjectZip(projectFiles, projectPaths, projectSrc);
    } else {
      cleanup();
      return res.status(422).json(
        { detail: "provide the .logicx as a zip or as folder files+paths" });
    }

    const userId = req.user.id;
    const recordId = crypto.randomUUID();
    const masterName = path.basename(master.originalname || "master.wav");

    await pool.query(
      `insert into records (id, user_id, artist_name, status, watermark_payload)
       values ($1, $2, $3, 'DRAFT', $4)`,
      [recordId, userId, artist, recordId.replaceAll("-", "")]);

    const masterKey = await stageAsset(userId, recordId, "MASTER", 0,
      master.path, masterName, master.mimetype || "audio/wav");
    const stemKeys = [], stemNames = [];
    for (let i = 0; i < stems.length; i++) {
      const name = path.basename(stems[i].originalname || `stem_${i}.wav`);
      stemKeys.push(await stageAsset(userId, recordId, "STEM", i,
        stems[i].path, name, stems[i].mimetype || "audio/wav"));
      stemNames.push(name);
    }
    const projectKey = await stageAsset(userId, recordId, "PROJECT", 0,
      projectSrc, "project.zip", "application/zip");
    cleanup();

    await queueSeal(recordId, userId, {
      version: 2, artist, user_id: userId,
      master_key: masterKey, master_name: masterName,
      stem_keys: stemKeys, stem_names: stemNames,
      project_key: projectKey,
    });
    await nudgeThor(recordId);
    return waitForSeal(recordId, res);
  } catch (e) {
    cleanup();
    console.error("[heimdall] register error:", e);
    res.status(e.status || 500).json({ detail: e.message || "internal error" });
  }
});

// ---------------------------------------------------------------------------
// status, release, manifests
// ---------------------------------------------------------------------------
app.get("/records/:id", requireUser, async (req, res) => {
  let job;
  try {
    if (await recordOwner(req.params.id) !== req.user.id) {
      return res.status(404).json({ detail: "no such record" });
    }
    job = await jobForRecord(req.params.id);
  } catch {
    return res.status(422).json({ detail: "bad record id" });
  }
  if (!job) return res.status(404).json({ detail: "no such record" });
  res.json({ record_id: req.params.id, status: job.status,
             error: job.error, result: job.result });
});

app.get("/records/:id/release", requireUser, async (req, res) => {
  try {
    if (await recordOwner(req.params.id) !== req.user.id) {
      return res.status(404).json({ detail: "no such record" });
    }
    const asset = await releaseAssetForRecord(req.params.id);
    if (asset) {
      const filename = asset.original_filename ||
        `${req.params.id}_watermarked_signed.wav`;
      const signed = await presignGet(asset.s3_key, filename);
      if (signed) return res.redirect(302, signed);
      return res.download(localAssetPath(asset.s3_key), filename);
    }
    // legacy records sealed before the assets store
    const legacy = path.join(STORAGE_DIR, "release",
                             `${path.basename(req.params.id)}.wav`);
    if (fs.existsSync(legacy)) {
      return res.download(legacy, `${req.params.id}_watermarked_signed.wav`);
    }
    res.status(404).json({ detail: "no release file" });
  } catch (e) {
    res.status(e.status || 500).json({ detail: e.message });
  }
});

// local-mode public manifests (S3 mode serves these from the bucket URL)
app.get("/manifests/*", (req, res) => {
  const p = path.normalize(path.join(STORAGE_DIR, "manifests", req.params[0]));
  if (!p.startsWith(path.join(STORAGE_DIR, "manifests")) || !fs.existsSync(p)) {
    return res.status(404).json({ detail: "no such manifest" });
  }
  res.type("application/json").send(fs.readFileSync(p));
});

// ---------------------------------------------------------------------------
// everything else streams to odin
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
  console.log(`[heimdall] :${PORT} odin=${ODIN_URL} sqs=${SQS_URL ? "on" : "off"} ` +
              `store=${ASSETS_BUCKET ? `s3:${ASSETS_BUCKET}` : "local"}`);
});
server.requestTimeout = 0;
server.headersTimeout = 120000;
server.keepAliveTimeout = 75000;
