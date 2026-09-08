/**
 * Catch-all: everything Heimdall doesn't handle itself streams to Odin
 * (verify, checkers, labs, manifest lookups). Mount this LAST.
 */
const httpProxy = require("http-proxy");
const { ODIN_URL } = require("../config");

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

/** Express handler that forwards the request to Odin. */
function proxyToOdin(req, res) {
  proxy.web(req, res);
}

module.exports = { proxyToOdin };
