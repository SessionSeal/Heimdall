/**
 * Record routes: create a draft (with presigned uploads), list the user's
 * catalog, read one record, its verification history, and download the
 * signed release master. All owner-scoped via requireUser + ownership checks.
 */
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const express = require("express");

const { requireUser } = require("../utils/auth");
const records = require("../utils/records-queries");
const storage = require("../utils/storage");
const { KIND_EXT, STORAGE_DIR } = require("../config");

const router = express.Router();

// --- create a draft record + presigned upload URLs -------------------------
router.post("/records/draft", requireUser, express.json(), async (req, res) => {
  try {
    const artist = (req.body.artist || "").trim();
    const title = (req.body.title || "").trim().slice(0, 200);
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

    await records.createDraftWithAssets(recordId, artist, title, userId, assets);
    const uploads = [];
    for (const a of assets) {
      uploads.push({
        asset_id: a.id, kind: a.kind, position: a.position,
        filename: a.filename, key: a.key,
        url: await storage.presignPut(a.key, a.content_type),
      });
    }
    res.json({ record_id: recordId, uploads });
  } catch (e) {
    console.error("[heimdall] draft error:", e);
    res.status(e.status || 500).json({ detail: e.message || "internal error" });
  }
});

// --- local-mode stand-in for S3 presigned PUTs (dev only) ------------------
router.put("/uploads/local/*", (req, res) => {
  if (storage.usingS3()) {
    return res.status(404).json({ detail: "local uploads disabled in s3 mode" });
  }
  try {
    const key = decodeURIComponent(req.params[0]);
    const dest = storage.localAssetPath(key);
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    const out = fs.createWriteStream(dest);
    req.pipe(out);
    out.on("finish", () => res.sendStatus(200));
    out.on("error", (e) => res.status(500).json({ detail: e.message }));
  } catch (e) {
    res.status(e.status || 500).json({ detail: e.message });
  }
});

// --- the musician's catalog ------------------------------------------------
router.get("/records", requireUser, async (req, res) => {
  try {
    res.json({ records: await records.listForUser(req.user.id) });
  } catch (e) {
    res.status(500).json({ detail: e.message });
  }
});

// --- verification history for one record -----------------------------------
router.get("/records/:id/verifications", requireUser, async (req, res) => {
  try {
    if (await records.ownerOf(req.params.id) !== req.user.id) {
      return res.status(404).json({ detail: "no such record" });
    }
    const rows = await records.verificationsForRecord(req.params.id);
    res.json({ record_id: req.params.id, count: rows.length, verifications: rows });
  } catch {
    res.status(422).json({ detail: "bad record id" });
  }
});

// --- one record: status + summary row (job optional) -----------------------
router.get("/records/:id", requireUser, async (req, res) => {
  // The record row is the authority for ownership and existence; the job is
  // optional (a DRAFT whose upload never finished has no job yet — it should
  // still render its own page, not 404).
  let record;
  try {
    record = await records.detailById(req.params.id);
  } catch {
    return res.status(422).json({ detail: "bad record id" });
  }
  if (!record || record.user_id !== req.user.id) {
    return res.status(404).json({ detail: "no such record" });
  }
  delete record.user_id;
  const job = await records.latestSealJob(req.params.id).catch(() => null);
  res.json({
    record_id: req.params.id,
    status: job ? job.status : record.status,
    error: job ? job.error : null,
    result: job ? job.result : null,
    record,
  });
});

// --- download the signed release master ------------------------------------
router.get("/records/:id/release", requireUser, async (req, res) => {
  try {
    if (await records.ownerOf(req.params.id) !== req.user.id) {
      return res.status(404).json({ detail: "no such record" });
    }
    const asset = await records.releaseAsset(req.params.id);
    if (asset) {
      const filename = asset.original_filename ||
        `${req.params.id}_watermarked_signed.wav`;
      const signed = await storage.presignGet(asset.s3_key, filename);
      if (signed) return res.redirect(302, signed);
      return res.download(storage.localAssetPath(asset.s3_key), filename);
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

// --- local-mode public manifests (S3 mode serves from the bucket URL) ------
router.get("/manifests/*", (req, res) => {
  const p = path.normalize(path.join(STORAGE_DIR, "manifests", req.params[0]));
  if (!p.startsWith(path.join(STORAGE_DIR, "manifests")) || !fs.existsSync(p)) {
    return res.status(404).json({ detail: "no such manifest" });
  }
  res.type("application/json").send(fs.readFileSync(p));
});

module.exports = router;
