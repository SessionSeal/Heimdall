/**
 * Seal route: verify every upload landed, enqueue the seal job, nudge Thor,
 * then hold the request open until the job finishes (the sync facade).
 *
 * NOTE: waitForSeal is the synchronous facade over the async jobs table.
 * SEAL-RESILIENCE.md plans to replace it with a 202 + client polling.
 */
const express = require("express");

const { requireUser } = require("../utils/auth");
const records = require("../utils/records-queries");
const storage = require("../utils/storage");
const { nudgeThor } = require("../utils/queue");
const { SEAL_TIMEOUT_S } = require("../config");

const router = express.Router();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitForSeal(recordId, res) {
  const deadline = Date.now() + SEAL_TIMEOUT_S * 1000;
  while (Date.now() < deadline) {
    await sleep(1000);
    const job = await records.latestSealJob(recordId);
    if (!job) continue;
    if (job.status === "SUCCEEDED") return res.json(job.result);
    if (job.status === "FAILED") return res.status(422).json({ detail: job.error });
  }
  return res.status(504).json({ detail: "sealing timed out" });
}

router.post("/records/:id/seal", requireUser, express.json(), async (req, res) => {
  try {
    const recordId = req.params.id;
    const userId = req.user.id;
    if (await records.ownerOf(recordId) !== userId) {
      return res.status(404).json({ detail: "no such draft" });
    }
    const inputs = await records.inputAssets(recordId);
    if (!inputs.length) return res.status(404).json({ detail: "no such draft" });

    // verify every upload actually landed before queueing
    for (const a of inputs) {
      const size = await storage.assetSize(a.s3_key);
      if (size === null || size === 0) {
        return res.status(422).json(
          { detail: `upload missing for ${a.kind} ${a.original_filename}` });
      }
      await records.setAssetSize(a.id, size);
    }

    const master = inputs.find((a) => a.kind === "MASTER");
    const project = inputs.find((a) => a.kind === "PROJECT");
    const stems = inputs.filter((a) => a.kind === "STEM")
      .sort((x, y) => x.position - y.position);
    const artist = await records.artistNameOf(recordId);
    if (artist === null) return res.status(404).json({ detail: "no such record" });

    await records.queueSeal(recordId, userId, {
      version: 2,
      artist,
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

module.exports = router;
