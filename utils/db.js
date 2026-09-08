/**
 * The database connector. This is the ONLY file that imports `pg` or holds
 * the connection pool. Everything else runs SQL through query() / tx().
 */
const { Pool } = require("pg");
const { DATABASE_URL } = require("../config");

const pool = new Pool({ connectionString: DATABASE_URL, max: 5 });

/** Run a single query. Returns the pg result ({ rows, rowCount, ... }). */
function query(text, params) {
  return pool.query(text, params);
}

/**
 * Run `fn` inside a transaction. `fn` receives a dedicated client; the
 * transaction commits if it resolves and rolls back if it throws. The
 * client is always released.
 */
async function tx(fn) {
  const client = await pool.connect();
  try {
    await client.query("begin");
    const result = await fn(client);
    await client.query("commit");
    return result;
  } catch (e) {
    await client.query("rollback");
    throw e;
  } finally {
    client.release();
  }
}

module.exports = { query, tx };
