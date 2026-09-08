/**
 * All environment configuration in one place. Import from here; never read
 * process.env elsewhere.
 */
require("dotenv").config();

const path = require("path");

const PORT = parseInt(process.env.PORT || "8000", 10);
const PUBLIC_URL = process.env.HEIMDALL_PUBLIC_URL || `http://127.0.0.1:${PORT}`;
const ODIN_URL = process.env.ODIN_URL || "http://127.0.0.1:8001";
const STORAGE_DIR = process.env.STORAGE_DIR ||
  path.resolve(__dirname, "..", "_localstore");
const SEAL_TIMEOUT_S = parseInt(process.env.SEAL_TIMEOUT_S || "900", 10);

const SQS_URL = process.env.SQS_SEAL_QUEUE_URL || null;
const AWS_REGION = process.env.AWS_REGION || "ap-south-1";
const ASSETS_BUCKET = process.env.S3_ASSETS_BUCKET || null;
const LOCAL_BUCKET = "_localstore"; // recorded in assets.s3_bucket in local mode

const AUTH_SECRET = process.env.AUTH_SECRET || null;
const DATABASE_URL = process.env.DATABASE_URL;

// Browser origins allowed to call the API (the Next app; the Astro site is
// harmless since it makes no API calls, but is allow-listed for parity).
const CORS_ORIGINS = new Set([
  "http://localhost:3000", "http://127.0.0.1:3000",
  "http://localhost:3001", "http://127.0.0.1:3001",
]);

// null = keep the upload's own extension; PROJECT is always zipped.
const KIND_EXT = { MASTER: null, STEM: null, PROJECT: ".zip" };

module.exports = {
  PORT, PUBLIC_URL, ODIN_URL, STORAGE_DIR, SEAL_TIMEOUT_S,
  SQS_URL, AWS_REGION, ASSETS_BUCKET, LOCAL_BUCKET,
  AUTH_SECRET, DATABASE_URL, CORS_ORIGINS, KIND_EXT,
};
