/**
 * Dispute share routes.
 *
 * Owner routes (requireUser): the musician mints/edits/revokes shares and sees
 * the access log.
 * Public routes (NO auth — the reviewer has no account): keyed by the share
 * token. The DB is the source of truth for access; status/expiry/tiers are
 * re-checked on EVERY reviewer call (mount and mint). Signed URLs are minted
 * only on an explicit action, with a short TTL, and only after re-confirming
 * the tier is currently allowed.
 */
const crypto = require("crypto");
const express = require("express");

const { requireUser } = require("../utils/auth");
const records = require("../utils/records-queries");
const storage = require("../utils/storage");
const { buildReport } = require("../utils/report");

const router = express.Router();

const VALID_TIERS = new Set(["REPORT", "STEM_PREVIEW", "SESSION"]);
const STREAM_TTL_S = 300; // short: revocation/tier-drop bites within 5 min

function reqMeta(req) {
  return {
    ip: (req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.ip,
    userAgent: req.headers["user-agent"] || null,
  };
}

// ---------------------------------------------------------------------------
// OWNER routes (musician)
// ---------------------------------------------------------------------------

// Create a share for a record you own.
router.post("/records/:id/shares", requireUser, express.json(), async (req, res) => {
  try {
    if (await records.ownerOf(req.params.id) !== req.user.id) {
      return res.status(404).json({ detail: "no such record" });
    }
    let tiers = Array.isArray(req.body.tiers) ? req.body.tiers : ["REPORT"];
    tiers = [...new Set(tiers.filter((t) => VALID_TIERS.has(t)))];
    if (!tiers.includes("REPORT")) tiers.unshift("REPORT"); // report is always on
    // default expiry 30 days; allow explicit null for "never" only if sent
    let expiresAt;
    if (req.body.expires_at === null) expiresAt = null;
    else if (req.body.expires_at) expiresAt = new Date(req.body.expires_at);
    else { expiresAt = new Date(Date.now() + 30 * 864e5); }
    const token = crypto.randomBytes(24).toString("base64url");
    const row = await records.createShare(
      req.params.id, req.user.id, token, tiers, expiresAt,
      (req.body.label || "").slice(0, 200));
    res.json({ id: row.id, token: row.token, tiers, expires_at: expiresAt });
  } catch (e) {
    console.error("[heimdall] create share error:", e);
    res.status(500).json({ detail: e.message });
  }
});

// List a record's shares + their access logs.
router.get("/records/:id/shares", requireUser, async (req, res) => {
  try {
    if (await records.ownerOf(req.params.id) !== req.user.id) {
      return res.status(404).json({ detail: "no such record" });
    }
    res.json({ shares: await records.sharesForRecord(req.params.id) });
  } catch (e) {
    res.status(500).json({ detail: e.message });
  }
});

// Edit tiers / expiry / label (musician changes access level anytime).
router.patch("/shares/:shareId", requireUser, express.json(), async (req, res) => {
  try {
    let tiers;
    if (Array.isArray(req.body.tiers)) {
      tiers = [...new Set(req.body.tiers.filter((t) => VALID_TIERS.has(t)))];
      if (!tiers.includes("REPORT")) tiers.unshift("REPORT");
    }
    const patch = { tiers };
    if ("expires_at" in req.body) {
      patch.expiresAt = req.body.expires_at ? new Date(req.body.expires_at) : null;
    }
    if ("label" in req.body) patch.label = String(req.body.label).slice(0, 200);
    const ok = await records.updateShare(req.params.shareId, req.user.id, patch);
    if (!ok) return res.status(404).json({ detail: "no such share" });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ detail: e.message });
  }
});

router.post("/shares/:shareId/revoke", requireUser, async (req, res) => {
  try {
    const ok = await records.revokeShare(req.params.shareId, req.user.id);
    if (!ok) return res.status(404).json({ detail: "no such share" });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ detail: e.message });
  }
});

// ---------------------------------------------------------------------------
// PUBLIC reviewer routes (token-keyed, no auth). DB re-checked every call.
// ---------------------------------------------------------------------------

async function loadActiveShare(token) {
  const share = await records.shareByToken(token);
  if (!share || !share.active) return null;
  return share;
}

// Mount: the reviewer opens the link. Returns the Tier A report + which tiers
// are currently enabled (so the UI shows/hides stem/session sections live).
router.get("/shares/:token", async (req, res) => {
  try {
    const share = await loadActiveShare(req.params.token);
    if (!share) return res.status(404).json({ detail: "This link is no longer active." });
    const rec = await records.reportSource(share.record_id);
    if (!rec) return res.status(404).json({ detail: "record unavailable" });

    const report = buildReport(rec);
    // list stem/session assets by name only (no URLs yet — minted on action)
    const assets = (await records.shareableAssets(share.record_id)).map((a) => ({
      asset_id: a.id, kind: a.kind, position: a.position,
      filename: a.original_filename,
    }));

    const m = reqMeta(req);
    await records.logShareAccess({
      shareId: share.id, tier: "REPORT", action: "VIEW_REPORT",
      reviewerEmail: req.query.email || null, ip: m.ip, userAgent: m.userAgent,
    });

    res.json({
      tiers: share.tiers,
      report,
      assets: share.tiers.includes("STEM_PREVIEW") || share.tiers.includes("SESSION")
        ? assets : [],
    });
  } catch (e) {
    console.error("[heimdall] share view error:", e);
    res.status(500).json({ detail: e.message });
  }
});

// Action: reviewer clicks "listen" / "download". Re-checks tier from DB, logs
// with the email, returns a SHORT-TTL signed URL for that one asset only.
router.post("/shares/:token/asset-url", express.json(), async (req, res) => {
  try {
    const share = await loadActiveShare(req.params.token);
    if (!share) return res.status(404).json({ detail: "This link is no longer active." });

    const { asset_id: assetId, email } = req.body || {};
    const asset = await records.shareableAssetById(share.record_id, assetId);
    if (!asset) return res.status(404).json({ detail: "asset unavailable" });

    // re-check the CURRENT tier grant at mint time (revocation/tier-drop bites)
    const isSession = asset.kind === "PROJECT";
    const neededTier = isSession ? "SESSION" : "STEM_PREVIEW";
    if (!share.tiers.includes(neededTier)) {
      return res.status(403).json({ detail: "This content isn't shared with you." });
    }

    const filename = asset.original_filename ||
      (isSession ? "session.zip" : "stem.wav");
    const url = await storage.presignGet(asset.s3_key, filename, STREAM_TTL_S);

    const m = reqMeta(req);
    await records.logShareAccess({
      shareId: share.id, tier: neededTier,
      action: isSession ? "DOWNLOAD_SESSION" : "STREAM_STEM",
      reviewerEmail: email || null, assetId: asset.id,
      ip: m.ip, userAgent: m.userAgent,
    });

    res.json({ url, expires_in: STREAM_TTL_S,
               download: isSession, kind: asset.kind });
  } catch (e) {
    console.error("[heimdall] share asset-url error:", e);
    res.status(500).json({ detail: e.message });
  }
});

module.exports = router;
