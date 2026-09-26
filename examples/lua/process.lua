-- The Lua stage of logit.yaml, beside this file.
--
-- The shop API logs one logfmt line per request. `logfmt` has already split each line into
-- attributes, all of them strings, so
--
--   level=info msg=request method=GET path=/api/orders/42 status=200 dur=12ms user=alice
--
-- arrives with event.attributes.status == "200" and event.attributes.dur == "12ms".
--
-- What process() returns decides where an event goes:
--
--   an event, unmarked           -> this component's own consumers (archive_out)
--   an event:to("errors")        -> the `errors` target (alerts_out)
--   an event:to("stats")         -> the `stats` target (window, then stats_out)
--   a table of events            -> each one, by its own mark
--   nil                          -> dropped
--
-- The sections below follow a line through: computation, state, routing and fan-out in
-- process(), then emission in flush().

local SERVICE = "shop-api"

-- Everything this script remembers is capped, and reset at each flush, so a burst of traffic
-- can't grow the VM without bound.
local MAX_ROUTES = 50     -- distinct routes per interval; the rest count as "/{other}"
local MAX_SAMPLES = 500   -- durations kept per route per interval
local MAX_USERS = 1000    -- distinct users per interval
local SLOW_MS = 1000      -- a request taking this long or longer is marked slow

---------------------------------------------------------------------------------------------
-- Computation: numbers and bounded names from string fields
---------------------------------------------------------------------------------------------

local MS_PER_UNIT = {ns = 1e-6, us = 1e-3, ms = 1, s = 1000}

-- "12ms" -> 12, "1.5s" -> 1500. nil for a missing or unrecognized duration.
local function duration_ms(raw)
  if raw == nil then
    return nil
  end
  local number, unit = string.match(raw, "^(%d+%.?%d*)(%a+)$")
  if number == nil or MS_PER_UNIT[unit] == nil then
    return nil
  end
  return tonumber(number) * MS_PER_UNIT[unit]
end

-- "/api/orders/42?expand=items" -> "/api/orders/{id}". A numeric segment, or a long hex or UUID
-- one, is an id: collapsing them keeps the number of routes down to the number of endpoints.
local function route_of(path)
  local segments = {}
  for segment in string.gmatch(string.match(path, "^[^?#]*"), "[^/]+") do
    if string.match(segment, "^%d+$") or (#segment >= 16 and string.match(segment, "^[%x%-]+$")) then
      segment = "{id}"
    end
    segments[#segments + 1] = segment
  end
  return "/" .. table.concat(segments, "/")
end

---------------------------------------------------------------------------------------------
-- State: what this interval has seen, kept between process() calls until flush()
---------------------------------------------------------------------------------------------

local routes = {}        -- per-route counters, keyed by route
local route_count = 0
local users = {}         -- distinct users, as a set: users[name] = true
local user_count = 0
local alerted = {}       -- server errors already alerted, keyed by "<route> <status>"

local function stats_for(route)
  if routes[route] == nil and route_count >= MAX_ROUTES then
    route = "/{other}"
  end
  local stats = routes[route]
  if stats == nil then
    stats = {requests = 0, errors = 0, seen = 0, durations = {}}
    routes[route] = stats
    route_count = route_count + 1
  end
  return stats, route
end

-- Keeps up to MAX_SAMPLES durations. Past that, each new one replaces a random kept one with
-- probability MAX_SAMPLES / seen (reservoir sampling), so what's kept is a fair sample of the
-- whole interval rather than its first few seconds.
local function record_duration(stats, ms)
  stats.seen = stats.seen + 1
  if stats.seen <= MAX_SAMPLES then
    stats.durations[stats.seen] = ms
  else
    local slot = math.random(stats.seen)
    if slot <= MAX_SAMPLES then
      stats.durations[slot] = ms
    end
  end
end

local function remember_user(name)
  if name ~= nil and users[name] == nil and user_count < MAX_USERS then
    users[name] = true
    user_count = user_count + 1
  end
end

-- An alert is a new, short log line rather than a copy of the request line. The app sends every
-- line at one syslog priority and puts its level in the body, and a copy would keep that
-- priority: event.log.severity is read-only. A new event takes its own, which syslog_out turns
-- into the alert's PRI.
local function alert_for(event, route, status)
  local a = event.attributes
  local message = string.format("%s %s returned %d", a.method or "-", route, status)
  if a.err ~= nil then
    message = message .. ": " .. a.err
  end
  return Event.new{
    timestamp = event.timestamp,
    attributes = {
      ["syslog.hostname"] = a["syslog.hostname"],
      ["syslog.tag"] = a["syslog.tag"],
      ["http.route"] = route,
      ["http.request.method"] = a.method,
      ["http.response.status_code"] = status,
      user = a.user,
    },
    log = {message = message, severity = "error"},
  }
end

---------------------------------------------------------------------------------------------
-- process(event): once per line
---------------------------------------------------------------------------------------------

function process(event)
  local a = event.attributes
  local status = tonumber(a.status)

  -- Not a request line (a startup message, say): archive it as it came.
  if a.msg ~= "request" or a.path == nil or status == nil then
    return event
  end

  -- Computation: typed fields the archive and the alerts can be searched by.
  local stats, route = stats_for(route_of(a.path))
  local ms = duration_ms(a.dur)
  a["http.route"] = route
  a["http.response.status_code"] = status
  if ms ~= nil then
    a.duration_ms = ms
    a.slow = ms >= SLOW_MS
  end

  -- State: counted toward this interval, and emitted by flush().
  stats.requests = stats.requests + 1
  if ms ~= nil then
    record_duration(stats, ms)
  end
  remember_user(a.user)

  -- Load-balancer health checks count toward the stats but aren't worth archiving.
  if route == "/healthz" then
    return nil
  end

  -- Routing: anything under 500 goes out unmarked, to the archive.
  if status < 500 then
    return event
  end

  stats.errors = stats.errors + 1
  local key = route .. " " .. status
  local held = alerted[key]
  if held ~= nil then
    -- Alerted already this interval: archive the line, and count it for flush()'s summary.
    held.repeats = held.repeats + 1
    return event
  end
  alerted[key] = {route = route, status = status, host = a["syslog.hostname"], repeats = 0}

  -- Fan-out: the first server error per route and status becomes two events, an alert marked
  -- for `errors` and the original line, unmarked, for the archive.
  return {alert_for(event, route, status):to("errors"), event}
end

---------------------------------------------------------------------------------------------
-- flush(now): every `interval:`
---------------------------------------------------------------------------------------------

-- One metrics event per route and one for the interval's users, all marked for `stats`, and a
-- summary line for `errors` wherever repeats were held back. `now` is the tick time as a
-- decimal-nanosecond string, the form event.timestamp takes.
function flush(now)
  -- flush() starts with an empty resource, since no one incoming batch produced what it emits.
  -- Name the service here, or the stats arrive without one.
  resource["service.name"] = SERVICE

  local out = {}
  for route, stats in pairs(routes) do
    local metrics = {
      {name = "http.server.requests", kind = "sum", value = stats.requests},
      {name = "http.server.errors", kind = "sum", value = stats.errors},
    }
    if stats.seen > 0 then
      -- sample_rate tells `aggregate` how many requests each kept duration stands for.
      metrics[#metrics + 1] = {
        name = "http.server.request.duration", kind = "samples", unit = "ms",
        values = stats.durations, sample_rate = #stats.durations / stats.seen,
      }
    end
    out[#out + 1] = Event.new{
      timestamp = now,
      attributes = {["http.route"] = route},
      metrics = metrics,
    }:to("stats")
  end

  if user_count > 0 then
    local members = {}
    for name in pairs(users) do
      members[#members + 1] = name
    end
    out[#out + 1] = Event.new{
      timestamp = now,
      metrics = {{name = "shop.users", kind = "set_members", members = members}},
    }:to("stats")
  end

  for _, held in pairs(alerted) do
    if held.repeats > 0 then
      out[#out + 1] = Event.new{
        timestamp = now,
        attributes = {
          ["syslog.hostname"] = held.host,
          ["http.route"] = held.route,
          ["http.response.status_code"] = held.status,
          repeats = held.repeats,
        },
        log = {
          message = string.format("%s returned %d %d more times since the last alert",
            held.route, held.status, held.repeats),
          severity = "warn",
        },
      }:to("errors")
    end
  end

  routes, route_count = {}, 0
  users, user_count = {}, 0
  alerted = {}
  return out
end
