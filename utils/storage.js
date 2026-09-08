/**
 * Storage backend: real S3 when S3_ASSETS_BUCKET is set, otherwise a local
 * stand-in under STORAGE_DIR mirroring the exact S3 key layout. Presigned
 * URLs let the browser upload/download directly, off our servers.
 */
const fs = require("fs");
const path = require("path");
const { S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand } =
  require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { ASSETS_BUCKET, AWS_REGION, STORAGE_DIR, PUBLIC_URL } = require("../config");

const s3 = ASSETS_BUCKET ? new S3Client({ region: AWS_REGION }) : null;
const usingS3 = () => !!s3;

/** Absolute local path for an asset key, guarded against path traversal. */
function localAssetPath(key) {
  const p = path.normalize(path.join(STORAGE_DIR, "assets", key));
  if (!p.startsWith(path.join(STORAGE_DIR, "assets"))) {
    throw Object.assign(new Error("unsafe key"), { status: 422 });
  }
  return p;
}

/** Presigned PUT (1h) for the browser to upload directly; local URL otherwise. */
async function presignPut(key, contentType) {
  if (s3) {
    return getSignedUrl(s3, new PutObjectCommand({
      Bucket: ASSETS_BUCKET, Key: key, ContentType: contentType,
    }), { expiresIn: 3600 });
  }
  return `${PUBLIC_URL}/uploads/local/${key.split("/").map(encodeURIComponent).join("/")}`;
}

/** Presigned GET (1h) with a download filename; null in local mode. */
async function presignGet(key, filename) {
  if (s3) {
    return getSignedUrl(s3, new GetObjectCommand({
      Bucket: ASSETS_BUCKET, Key: key,
      ResponseContentDisposition: `attachment; filename="${filename}"`,
    }), { expiresIn: 3600 });
  }
  return null; // local mode streams the file directly
}

/** Size of an uploaded asset (S3 HeadObject or local stat); null if missing. */
async function assetSize(key) {
  if (s3) {
    try {
      const h = await s3.send(new HeadObjectCommand({ Bucket: ASSETS_BUCKET, Key: key }));
      return h.ContentLength ?? 0;
    } catch { return null; }
  }
  try { return fs.statSync(localAssetPath(key)).size; } catch { return null; }
}

module.exports = { s3, usingS3, localAssetPath, presignPut, presignGet, assetSize };
