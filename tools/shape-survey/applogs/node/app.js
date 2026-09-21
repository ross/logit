// `pino` in two configurations at once, on a bare `node:http` server.
//
// Config source, copied rather than invented:
//   https://getpino.io/#/docs/api            (pino(), pino.destination() -- the production sink)
//   https://github.com/pinojs/pino-http      (the middleware, at its own defaults)
//   https://github.com/pinojs/pino-std-serializers  (the req/res serializers pino-http defaults to)
//
//   * PRODUCTION stream: `pino-http` with its default options. This is the one app in the survey
//     whose *library* decides the record's fields: pino-std-serializers turns the request into a
//     nested `req` object ({id, method, url, query, params, headers{...}, remoteAddress,
//     remotePort}) and the response into a nested `res` ({statusCode, headers{...}}), so the
//     record carries two nested maps, each containing a further nested header map -- depth 3.
//     That nesting is exactly what this leg exists to measure.
//   * DEFAULT stream: a bare `pino(destination)` logger. pino's out-of-the-box output IS JSON
//     (level as a number, epoch-millisecond `time`, `pid`, `hostname`, `msg`), so there is a real
//     default to contrast with -- and it is given the same eight flat access-log fields every
//     other app in the survey logs, so the difference between the two rows here is pino-http's
//     serializers and nothing else.

const http = require("node:http");
const { randomUUID } = require("node:crypto");
const pino = require("pino");
const pinoHttp = require("pino-http");

const [prodOut, defaultOut, port] = process.argv.slice(2);

// Every Nth request is logged a second time through the default-configuration logger (see
// app_pyjson.py's DEFAULT_EVERY -- the same sampling, for the same reason).
const DEFAULT_EVERY = 4;

// `pinoHttp([options], [stream])` -- the empty options object is load-bearing: passed a stream as
// its *first* argument, pino-http reads it as the options object, and the default req/res
// serializers are never installed (the record then carries the raw Node objects, thousands of
// bytes of socket internals). Defaults everywhere else.
const httpLogger = pinoHttp({}, pino.destination({ dest: prodOut, sync: false }));
const plain = pino(pino.destination({ dest: defaultOut, sync: false }));

let seq = 0;

// The shared route table (tools/shape-survey/applogs/python/appbase.py's `route`), so a width
// difference between two sources is the logging library and not a different application.
function route(path) {
  const segments = new URL(path, "http://x").pathname.split("/").filter(Boolean);
  if (segments.length === 0) return [200, "ok\n", false];
  if (segments[0] === "boom") return [500, "internal server error\n", true];
  if (segments[0] === "items") {
    if (segments[1] === "0") return [404, "not found\n", false];
    return [200, '{"items":[]}\n', false];
  }
  if (segments[0] === "search") return [200, '{"results":[]}\n', false];
  if (segments[0] === "healthz") return [200, "ok\n", false];
  return [404, "not found\n", false];
}

const server = http.createServer((req, res) => {
  const started = process.hrtime.bigint();
  httpLogger(req, res);
  const [status, body, boom] = route(req.url);
  let err = null;
  if (boom) {
    err = new Error(`synthetic failure serving ${req.url}`);
    // pino-http's documented way to get the error onto the completion log rather than emitting a
    // second line for it.
    res.err = err;
  }
  // `setHeader` rather than passing them to `writeHead`: pino-std-serializers' `res` serializer
  // reads `res.getHeaders()`, which only sees headers set this way -- the survey should measure
  // the nested `res.headers` map a real framework produces, not an empty one.
  res.setHeader("Content-Type", "application/json");
  res.setHeader("Content-Length", Buffer.byteLength(body));
  res.writeHead(status);
  res.end(body);

  seq += 1;
  if (seq % DEFAULT_EVERY === 0) {
    const fields = {
      method: req.method,
      path: req.url,
      status,
      duration_ms: Number(process.hrtime.bigint() - started) / 1e6,
      bytes: Buffer.byteLength(body),
      remote_addr: req.socket.remoteAddress,
      user_agent: req.headers["user-agent"] || "-",
      request_id: randomUUID().replace(/-/g, ""),
    };
    if (err) plain.error({ err, ...fields }, "request failed");
    else plain.info(fields, "request");
  }
});

plain.info({ port: Number(port), library: "pino" }, "starting");
server.listen(Number(port), "0.0.0.0");
