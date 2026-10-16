-- The Lua stage of logit.yaml, beside this file.
--
-- A Prusa printer on Buddy firmware sends one syslog datagram about every half second. With
-- `framing: datagram`, syslog_in hands this script the whole message as one event:
--
--   msg=920,tm=1730850936,v=4 ttemp_noz,n=0,a=1 value=0i -158
--   stp_stall v=1i 38950
--   heap free=61088i,total=79012i 128900
--   ...
--   esp_out sent=4171237i 676222
--
-- The first line opens with a header: a datagram sequence number, the time since boot, and the
-- format version. Every line is an InfluxDB line-protocol point whose timestamp is an offset from
-- the time since boot, not a real time. This script turns the message into plain line protocol
-- with absolute nanosecond timestamps, which the `line_protocol` stage after it parses into one
-- event per point.
--
-- An event that isn't a Buddy datagram passes through unchanged.

-- The InfluxDB bucket the points are written to. Edit this line to use another bucket.
local BUCKET = "prusa"

-- Nanoseconds per offset unit, by format version: microseconds in v4, milliseconds before it.
local UNIT_NS = {[2] = 1000000, [3] = 1000000, [4] = 1000}

-- The last `msg` sequence number seen from each printer, keyed by MAC address. One entry per
-- printer, kept for the life of the process.
local last_msg = {}

-- `receipt` minus `delta_ns`, as a decimal string. Event timestamps are decimal-nanosecond
-- strings because a Lua number is exact only to 2^53 and a nanosecond timestamp is near 2^61, so
-- the subtraction runs on the seconds and the nanoseconds separately.
local function shift(receipt, delta_ns)
  local len = #receipt
  local secs = 0
  if len > 9 then
    secs = tonumber(string.sub(receipt, 1, len - 9))
  end
  local nanos = tonumber(string.sub(receipt, math.max(1, len - 8))) - delta_ns
  local borrow = math.floor(nanos / 1000000000)
  return string.format("%d%09d", secs + borrow, nanos - borrow * 1000000000)
end

-- Whether `fields` is a single field named `v`. A string value can hold a comma, so a quoted
-- value is scanned to its closing quote.
local function lone_v(fields)
  if string.sub(fields, 1, 2) ~= "v=" then
    return false
  end
  if string.sub(fields, 3, 3) ~= '"' then
    return string.find(fields, ",", 3, true) == nil
  end
  local i = 4
  while i <= #fields do
    local c = string.sub(fields, i, i)
    if c == "\\" then
      i = i + 2
    elseif c == '"' then
      return i == #fields
    else
      i = i + 1
    end
  end
  return false
end

-- Counts gaps in a printer's `msg` sequence. A step of 2 to 9 is datagrams lost on the network.
-- A jump of 10 or more, or a step back, is a reboot or a firmware restart, and starts the count
-- over. A MAC address is a bounded tag: one value per printer.
local function track_sequence(mac, msg)
  local last = last_msg[mac]
  last_msg[mac] = msg
  if last == nil then
    return
  end
  local step = msg - last
  if step >= 2 and step <= 9 then
    telemetry.count("prusa.datagrams.missed", step - 1, {mac_address = mac})
  elseif step >= 10 or step < 0 then
    telemetry.count("prusa.datagrams.resync", 1, {mac_address = mac})
  end
end

function process(event)
  local log = event.log
  if log == nil or event.attributes["syslog.tag"] ~= "buddy" then
    return event
  end
  local mac = event.attributes["syslog.hostname"]
  local message = log.message
  if type(mac) ~= "string" or type(message) ~= "string" then
    return event
  end

  -- The header is the message's first word: `msg=N,tm=T,v=V`, then a space and the first point.
  local header, body = string.match(message, "^(%S+) (.*)$")
  if header == nil or string.find(header, "msg=", 1, true) == nil then
    return event
  end
  local fields = {}
  for key, value in string.gmatch(header, "([^,=]+)=([^,]*)") do
    fields[key] = value
  end
  local version = tonumber(fields.v)
  local msg = tonumber(fields.msg)
  if msg == nil then
    return event
  end
  local unit_ns = UNIT_NS[version]
  if unit_ns == nil then
    -- Without the version, the offsets have no unit and the header can't be told from a point,
    -- so line_protocol would write nonsense. The datagram's points are lost: count it as loss.
    telemetry.count("prusa.datagrams.dropped", 1, {reason = "version"})
    return nil
  end
  track_sequence(mac, msg)

  -- Split each line into its series (measurement and tags), its fields, and its offset. A v2
  -- offset is relative to the line before it, so it's summed into an offset from `tm`.
  local points = {}
  local running = 0
  local newest = nil
  for line in string.gmatch(body, "[^\n]+") do
    local series, point_fields, offset = string.match(line, "^(%S+) (.-) (%-?%d+)$")
    if series == nil then
      -- Not a point this script can anchor; `line_protocol` counts it if it doesn't parse.
      points[#points + 1] = {line = line}
    else
      offset = tonumber(offset)
      if version == 2 then
        running = running + offset
        offset = running
      end
      if newest == nil or offset > newest then
        newest = offset
      end
      points[#points + 1] = {series = series, fields = point_fields, offset = offset}
    end
  end

  -- Anchor the datagram: its newest point gets the receipt time, and every other point keeps its
  -- distance from the newest. The printer's clock then never needs to agree with this host's.
  local receipt = event.timestamp
  local lines = {}
  for _, point in ipairs(points) do
    if point.line ~= nil then
      lines[#lines + 1] = point.line
    else
      local point_fields = point.fields
      -- Prusa's collector stores a lone `v` field as `value`.
      if lone_v(point_fields) then
        point_fields = "value" .. string.sub(point_fields, 2)
      end
      if string.find("," .. point_fields .. ",", ",value=[nN][aA][nN],") then
        -- Line protocol has no NaN, so the point can't be written.
        telemetry.count("prusa.points.skipped", 1, {reason = "nan"})
      elseif version == 3 and string.find(point.series, ",_seq=", 1, true) then
        -- v3 firmware sent a per-record debug series tagged `_seq`; Prusa's collector skips it.
        telemetry.count("prusa.points.skipped", 1, {reason = "seq"})
      else
        local at = shift(receipt, (newest - point.offset) * unit_ns)
        lines[#lines + 1] = point.series .. " " .. point_fields .. " " .. at
      end
    end
  end
  if #lines == 0 then
    return nil
  end

  -- Every `syslog.*` attribute would become an InfluxDB tag on every point, so remove them and
  -- keep only the MAC address.
  for key in pairs(event.attributes) do
    if string.sub(key, 1, 7) == "syslog." then
      event.attributes[key] = nil
    end
  end
  event.attributes.mac_address = mac
  event.log.message = table.concat(lines, "\n")

  -- influxdb_out writes a batch to the bucket its resource names when the sink sets no `bucket:`.
  -- The resource is per batch, so this applies to every event in it.
  resource["_logit.influxdb.bucket"] = BUCKET
  return event
end
