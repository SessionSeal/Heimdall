/**
 * Auth: verify the Auth.js session cookie independently of the web app.
 * The cookie is a JWE keyed by the shared AUTH_SECRET; after decoding we
 * still check users.active on every request, so suspension bites despite
 * the token being stateless. Keep @auth/core in version lockstep with Web.
 */
const { decode } = require("@auth/core/jwt");
const { query } = require("./db");
const { AUTH_SECRET } = require("../config");

const SESSION_COOKIES = ["__Secure-authjs.session-token", "authjs.session-token"];

function readCookies(req) {
  const out = {};
  for (const part of (req.headers.cookie || "").split(";")) {
    const i = part.indexOf("=");
    if (i > 0) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}

/** Express middleware: 401 unless a valid cookie maps to an active user. */
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
    const { rows } = await query(
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

module.exports = { requireUser };
