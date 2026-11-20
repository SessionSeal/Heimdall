/**
 * All SQL for records, assets, and seal jobs. Route handlers call these
 * named functions instead of writing SQL inline. The only file besides
 * these helpers that talks to the DB is utils/auth.js (its user lookup).
 */
const { query, tx } = require("./db");
const { ASSETS_BUCKET, LOCAL_BUCKET } = require("../config");

/** Who owns a record (user_id), or null if it doesn't exist. */
async function ownerOf(recordId) {
  const { rows } = await query(
    "select user_id from records where id = $1", [recordId]);
  return rows.length ? rows[0].user_id : null;
}

/** Create a DRAFT record + its PENDING_UPLOAD asset rows in one transaction. */
async function createDraftWithAssets(recordId, artist, title, userId, assets) {
  return tx(async (client) => {
    await client.query(
      `insert into records (id, user_id, artist_name, title, status, watermark_payload)
       values ($1, $2, $3, $4, 'DRAFT', $5)`,
      [recordId, userId, artist, title || null, recordId.replaceAll("-", "")]);
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
  });
}

/** The MASTER/STEM/PROJECT input assets for a record. */
async function inputAssets(recordId) {
  const { rows } = await query(
    `select id, kind, position, status, s3_key, original_filename, content_type
       from assets
      where record_id = $1 and kind in ('MASTER', 'STEM', 'PROJECT')
        and deleted_at is null
      order by kind, position`, [recordId]);
  return rows;
}

async function setAssetSize(assetId, size) {
  await query("update assets set size_bytes = $1 where id = $2", [size, assetId]);
}

async function artistNameOf(recordId) {
  const { rows } = await query(
    "select artist_name from records where id = $1", [recordId]);
  return rows.length ? rows[0].artist_name : null;
}

/**
 * Flip DRAFT->QUEUED, mark uploads UPLOADED, and enqueue the seal job —
 * atomically. Throws {status:409} if the record isn't in DRAFT state.
 */
async function queueSeal(recordId, userId, payload) {
  return tx(async (client) => {
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
  });
}

/** The latest SEAL job for a record (status/result/error), or null. */
async function latestSealJob(recordId) {
  const { rows } = await query(
    `select id, status, attempts, error, result
       from jobs where record_id = $1 and kind = 'SEAL'
      order by queued_at desc limit 1`, [recordId]);
  return rows[0] || null;
}

/** The published RELEASE_MASTER asset for a record, or null. */
async function releaseAsset(recordId) {
  const { rows } = await query(
    `select s3_key, original_filename from assets
      where record_id = $1 and kind = 'RELEASE_MASTER' and deleted_at is null
      order by created_at desc limit 1`, [recordId]);
  return rows[0] || null;
}

/** The musician's catalog — every record they own, newest first. */
async function listForUser(userId) {
  const { rows } = await query(
    `select r.id, r.title, r.artist_name, r.status,
            r.created_at, r.sealed_at,
            r.coherence_verified, r.coherence_confidence,
            r.sameorigin_score, r.sameorigin_band,
            r.watermark_selfcheck, r.manifest_public_url,
            (select count(*)::int from verifications v
              where v.matched_record_id = r.id and v.linked) as times_verified
       from records r
      where r.user_id = $1 and r.deleted_at is null
      order by r.created_at desc
      limit 200`, [userId]);
  return rows;
}

/** Full record row for the detail page (includes user_id for the owner check). */
async function detailById(recordId) {
  const { rows } = await query(
    `select id, user_id, title, artist_name, status, created_at, sealed_at,
            coherence_verified, coherence_confidence, sameorigin_score,
            sameorigin_band, watermark_selfcheck, manifest_public_url,
            cert_subject, signer_self_attested
       from records where id = $1 and deleted_at is null`, [recordId]);
  return rows[0] || null;
}

/** Sanitized verification history for a record (no requester IP / UA). */
async function verificationsForRecord(recordId) {
  const { rows } = await query(
    `select created_at, linked_via, copy_attack_suspected
       from verifications
      where matched_record_id = $1 and linked
      order by created_at desc
      limit 50`, [recordId]);
  return rows;
}

// --- dispute shares --------------------------------------------------------

/** Full record row for building the reviewer report (includes same-origin
 *  report jsonb and the manifest/coherence fields). */
async function reportSource(recordId) {
  const { rows } = await query(
    `select id, title, artist_name, status, sealed_at,
            coherence_verified, coherence_confidence,
            sameorigin_score, sameorigin_band, sameorigin_report,
            manifest_public_url, cert_subject, signer_self_attested
       from records where id = $1 and deleted_at is null`, [recordId]);
  return rows[0] || null;
}

/** Stem + session assets available to preview/download for a record. */
async function shareableAssets(recordId) {
  const { rows } = await query(
    `select id, kind, position, s3_key, original_filename, content_type
       from assets
      where record_id = $1 and kind in ('STEM', 'PROJECT')
        and deleted_at is null
      order by kind, position`, [recordId]);
  return rows;
}

async function createShare(recordId, userId, token, tiers, expiresAt, label) {
  const { rows } = await query(
    `insert into record_shares (record_id, created_by, token, tiers, expires_at, label)
     values ($1, $2, $3, $4, $5, $6) returning id, token`,
    [recordId, userId, token, tiers, expiresAt, label || null]);
  return rows[0];
}

/** All shares for a record, with their recent access log rolled up. */
async function sharesForRecord(recordId) {
  const { rows } = await query(
    `select s.id, s.token, s.tiers, s.status, s.expires_at, s.label, s.created_at,
            coalesce((
              select json_agg(json_build_object(
                'tier', a.tier, 'action', a.action,
                'reviewer_email', a.reviewer_email,
                'created_at', a.created_at) order by a.created_at desc)
              from share_accesses a where a.share_id = s.id), '[]') as accesses
       from record_shares s
      where s.record_id = $1
      order by s.created_at desc`, [recordId]);
  return rows;
}

/** Update tiers / expiry / label of a share the caller owns. */
async function updateShare(shareId, userId, { tiers, expiresAt, label }) {
  const { rows } = await query(
    `update record_shares s
        set tiers = coalesce($3, s.tiers),
            expires_at = case when $4::boolean then $5 else s.expires_at end,
            label = coalesce($6, s.label),
            updated_at = now()
       from records r
      where s.id = $1 and s.record_id = r.id and r.user_id = $2
      returning s.id`,
    [shareId, userId, tiers ?? null, expiresAt !== undefined,
     expiresAt ?? null, label ?? null]);
  return rows.length > 0;
}

async function revokeShare(shareId, userId) {
  const { rows } = await query(
    `update record_shares s set status = 'REVOKED', updated_at = now()
       from records r
      where s.id = $1 and s.record_id = r.id and r.user_id = $2
      returning s.id`, [shareId, userId]);
  return rows.length > 0;
}

/** Look up a share by its public token. Returns the row incl. record_id +
 *  a computed `active` (ACTIVE and not expired). The reviewer path calls this
 *  on every mount and every mint — DB is the source of truth. */
async function shareByToken(token) {
  const { rows } = await query(
    `select id, record_id, tiers, status, expires_at,
            (status = 'ACTIVE' and (expires_at is null or expires_at > now())) as active
       from record_shares where token = $1`, [token]);
  return rows[0] || null;
}

async function logShareAccess({ shareId, tier, action, reviewerEmail,
                                assetId, ip, userAgent }) {
  await query(
    `insert into share_accesses
       (share_id, tier, action, reviewer_email, asset_id, ip, user_agent)
     values ($1, $2, $3, $4, $5, $6, $7)`,
    [shareId, tier, action, reviewerEmail || null, assetId || null,
     ip || null, userAgent || null]);
}

/** One shareable asset by id, scoped to the record (prevents cross-record ids). */
async function shareableAssetById(recordId, assetId) {
  const { rows } = await query(
    `select id, kind, s3_key, original_filename, content_type
       from assets
      where id = $1 and record_id = $2 and kind in ('STEM','PROJECT')
        and deleted_at is null`, [assetId, recordId]);
  return rows[0] || null;
}

module.exports = {
  ownerOf, createDraftWithAssets, inputAssets, setAssetSize, artistNameOf,
  queueSeal, latestSealJob, releaseAsset, listForUser, detailById,
  verificationsForRecord,
  // shares
  reportSource, shareableAssets, createShare, sharesForRecord, updateShare,
  revokeShare, shareByToken, logShareAccess, shareableAssetById,
};
