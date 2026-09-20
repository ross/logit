# `semantic_logger` in its documented JSON configuration, on a WEBrick server.
#
# Config source, copied rather than invented:
#   https://logger.rocketjob.io/started.html   (SemanticLogger.add_appender(..., formatter: :json))
#   https://logger.rocketjob.io/appenders.html (file_name: appender)
#
# WHY NOT LOGRAGE. lograge is a Rails railtie -- it replaces `ActionController`'s own request log
# line, and there is no supported way to run it outside Rails. Standing up Rails here would mean a
# `gem install rails` plus a `rails new` inside the image build, which is minutes of build for an
# app the survey only needs four routes out of. The brief's own fallback is taken instead:
# semantic_logger's JSON formatter, which is the same class of thing (a Ruby structured-logging
# library's documented JSON production output) without the framework. **No Rails, no lograge in
# this run**, and the summary says so rather than leaving Ruby's row looking like a Rails row.
#
# NO DEFAULT-CONFIGURATION STREAM, for structlog's reason: semantic_logger's default appender
# formatter is `:color` (human text), not JSON. `formatter: :json` is the opt-in this app makes,
# and it is the library's own documented production choice.
#
# Note the shape semantic_logger produces: the caller's fields do NOT go at the top level. They
# land in a nested `payload` map, alongside a flat envelope of host/application/environment/
# timestamp/level/level_index/pid/thread/name/message -- and an error adds a nested `exception`
# map whose `stack_trace` is an array of strings. One record, two nested maps.

require "securerandom"
require "semantic_logger"
require "webrick"

out_path, port = ARGV[0], ARGV[1].to_i

SemanticLogger.default_level = :info
SemanticLogger.add_appender(file_name: out_path, formatter: :json)
logger = SemanticLogger["app"]

# The shared route table (tools/shape-survey/applogs/python/appbase.py's `route`), so a width
# difference between two sources is the logging library and not a different application.
def route(path)
  segments = path.split("?").first.to_s.split("/").reject(&:empty?)
  return [200, "ok\n", false] if segments.empty?

  case segments[0]
  when "boom" then [500, "internal server error\n", true]
  when "items" then segments[1] == "0" ? [404, "not found\n", false] : [200, "{\"items\":[]}\n", false]
  when "search" then [200, "{\"results\":[]}\n", false]
  when "healthz" then [200, "ok\n", false]
  else [404, "not found\n", false]
  end
end

server = WEBrick::HTTPServer.new(
  Port: port,
  BindAddress: "0.0.0.0",
  AccessLog: [],
  Logger: WEBrick::Log.new(File::NULL)
)

server.mount_proc "/" do |req, res|
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  full_path = req.unparsed_uri
  status, body, boom = route(full_path)
  res.status = status
  res["Content-Type"] = "application/json"
  res.body = body

  # The same eight access-log fields every app in this survey logs (appbase.FIELDS).
  fields = {
    method: req.request_method,
    path: full_path,
    status: status,
    duration_ms: ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1000).round(3),
    bytes: body.bytesize,
    remote_addr: req.peeraddr[3],
    user_agent: req["user-agent"] || "-",
    request_id: SecureRandom.hex(16)
  }

  if boom
    begin
      raise "synthetic failure serving #{full_path}"
    rescue RuntimeError => e
      logger.error("request failed", fields, e)
    end
  else
    logger.info("request", fields)
  end
end

trap("INT") { server.shutdown }
trap("TERM") { server.shutdown }
logger.info("starting", port: port, library: "semantic_logger")
server.start
