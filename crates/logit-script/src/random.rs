//! The `random` global: a table of Rust functions that mint ids a script can put on an event, a
//! W3C trace or span id, a UUID, a ULID, or raw hex. [`install`] also seeds `math.random` per VM. See `docs/design/lua-api.md`'s "Minting ids"
//! section and `docs/adr/lua-random-ids.md`.
//!
//! The bytes come from `logit_core::ids`. `uuid7` and `ulid` take the time they encode as an
//! argument, a decimal-nanos string as `event.timestamp` reads, because a script has no clock.
//!
//! Installed before the script's source runs, as `crate::trace` is, so a top-level alias
//! (`local random = random`) captures the table. An argument error is an `mlua::Error` from the
//! callback, prefixed `random.<call>:`; none of these is a call a script loops on to probe for
//! success, so none takes `Event.new`'s Lua-shim path (`docs/adr/lua-refusals-raised-from-lua.md`).

use logit_core::ids;
use mlua::{Lua, String as LuaString, Value as LuaValue};

/// The largest byte count `random.hex(n)` takes.
const MAX_HEX_BYTES: usize = 256;

/// Creates the `random` global.
pub fn install(lua: &Lua) -> mlua::Result<()> {
    let table = lua.create_table()?;
    table.set("trace_id", lua.create_function(|lua, ()| lua_hex(lua, &ids::trace_id()))?)?;
    table.set("span_id", lua.create_function(|lua, ()| lua_hex(lua, &ids::span_id()))?)?;
    table.set(
        "uuid",
        lua.create_function(|lua, ()| lua.create_string(ids::format_uuid(&ids::uuid_v4())))?,
    )?;
    table.set(
        "uuid7",
        lua.create_function(|lua, ts: LuaValue| {
            let b = ids::uuid_v7(unix_ms("uuid7", ts)?).map_err(|e| error("uuid7", e))?;
            lua.create_string(ids::format_uuid(&b))
        })?,
    )?;
    table.set(
        "ulid",
        lua.create_function(|lua, ts: LuaValue| {
            let b = ids::ulid(unix_ms("ulid", ts)?).map_err(|e| error("ulid", e))?;
            lua.create_string(ids::format_ulid(&b))
        })?,
    )?;
    table.set(
        "hex",
        lua.create_function(|lua, n: LuaValue| {
            let mut bytes = [0u8; MAX_HEX_BYTES];
            let bytes = &mut bytes[..byte_count(n)?];
            ids::fill(bytes);
            lua_hex(lua, bytes)
        })?,
    )?;
    lua.globals().set("random", table)?;
    seed_math_random(lua)
}

/// Seeds LuaJIT's `math.random` from `logit_core::ids`, once per VM. LuaJIT seeds every new
/// state's generator with the same constant, so without this every worker VM would draw the same
/// `math.random()` sequence. `math.randomseed` takes one number; 53 bits keep it an exact double.
fn seed_math_random(lua: &Lua) -> mlua::Result<()> {
    let math: mlua::Table = lua.globals().get("math")?;
    let randomseed: mlua::Function = math.get("randomseed")?;
    let mut seed = [0u8; 8];
    ids::fill(&mut seed);
    randomseed.call((u64::from_le_bytes(seed) >> 11) as f64)
}

fn error(call: &str, message: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::RuntimeError(format!("random.{call}: {message}"))
}

/// `bytes` as a lowercase hex Lua string, rendered on the stack so a call allocates nothing on
/// the Rust heap.
fn lua_hex<'lua>(lua: &'lua Lua, bytes: &[u8]) -> mlua::Result<LuaString<'lua>> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; MAX_HEX_BYTES * 2];
    for (i, b) in bytes.iter().enumerate() {
        out[2 * i] = DIGITS[usize::from(b >> 4)];
        out[2 * i + 1] = DIGITS[usize::from(b & 0x0f)];
    }
    lua.create_string(&out[..bytes.len() * 2])
}

/// Reads `ts` by `event.timestamp`'s write rule, a decimal-digit string and never a Lua number,
/// and returns its unix milliseconds. Any `i64` nanosecond count fits the 48-bit millisecond
/// field, so `ids`'s range error can't follow a value this accepts.
fn unix_ms(call: &str, ts: LuaValue) -> mlua::Result<u64> {
    let LuaValue::String(s) = ts else {
        return Err(error(
            call,
            format!(
                "ts must be a string of decimal digits (a Lua number can't represent full \
                 nanosecond precision), got {}",
                ts.type_name()
            ),
        ));
    };
    let nanos: i64 = s
        .to_str()
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| error(call, "ts must be a string of decimal digits"))?;
    if nanos < 0 {
        return Err(error(call, format!("ts must not be negative, got {nanos}")));
    }
    Ok(nanos as u64 / 1_000_000)
}

/// Reads `random.hex`'s `n`: an integer from 1 to [`MAX_HEX_BYTES`].
fn byte_count(n: LuaValue) -> mlua::Result<usize> {
    let count = match n {
        LuaValue::Integer(i) => usize::try_from(i).ok(),
        LuaValue::Number(f) if f.fract() == 0.0 && (0.0..=MAX_HEX_BYTES as f64).contains(&f) => {
            Some(f as usize)
        }
        _ => None,
    };
    match count {
        Some(count @ 1..=MAX_HEX_BYTES) => Ok(count),
        _ => Err(error(
            "hex",
            format!("n must be an integer from 1 to {MAX_HEX_BYTES}, got {}", describe(&n)),
        )),
    }
}

fn describe(value: &LuaValue) -> String {
    match value {
        LuaValue::Integer(i) => i.to_string(),
        LuaValue::Number(f) => f.to_string(),
        other => other.type_name().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{ProcessOutcome, ScriptWorker};
    use logit_core::{AttrMap, BodyFormat, Event, LogRecord, Value};

    fn worker() -> ScriptWorker {
        ScriptWorker::new("function process(event) return event end").expect("script should load")
    }

    fn eval(code: &str) -> String {
        worker().lua.load(code).eval::<String>().expect("the call should succeed")
    }

    fn eval_err(code: &str) -> String {
        match worker().lua.load(code).eval::<String>() {
            Err(err) => err.to_string(),
            Ok(s) => panic!("expected an error, got {s}"),
        }
    }

    fn is_lower_hex(s: &str) -> bool {
        s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    #[test]
    fn trace_and_span_ids_are_lowercase_hex_of_their_widths() {
        let trace = eval("return random.trace_id()");
        assert_eq!(trace.len(), 32);
        assert!(is_lower_hex(&trace), "{trace}");
        assert_ne!(trace, "0".repeat(32));
        let span = eval("return random.span_id()");
        assert_eq!(span.len(), 16);
        assert!(is_lower_hex(&span), "{span}");
        assert_ne!(span, "0".repeat(16));
    }

    #[test]
    fn uuid_is_a_hyphenated_v4() {
        let u = eval("return random.uuid()");
        let groups: Vec<&str> = u.split('-').collect();
        assert_eq!(groups.iter().map(|g| g.len()).collect::<Vec<_>>(), [8, 4, 4, 4, 12], "{u}");
        assert!(groups.iter().all(|g| is_lower_hex(g)), "{u}");
        assert!(groups[2].starts_with('4'), "{u}");
        assert!("89ab".contains(&groups[3][..1]), "{u}");
    }

    #[test]
    fn uuid7_carries_the_milliseconds_of_a_nanos_string() {
        // 1469918176385 ms, with sub-millisecond nanos that the field truncates.
        let u = eval(r#"return random.uuid7("1469918176385999999")"#);
        let hex: String = u.split('-').collect();
        assert_eq!(u64::from_str_radix(&hex[..12], 16).unwrap(), 1_469_918_176_385);
        assert_eq!(&hex[12..13], "7", "{u}");
        assert!("89ab".contains(&hex[16..17]), "{u}");
    }

    #[test]
    fn ulid_carries_the_milliseconds_of_a_nanos_string() {
        let u = eval(r#"return random.ulid("1469918176385000000")"#);
        assert_eq!(u.len(), 26);
        assert_eq!(&u[..10], "01ARYZ6S41");
        assert!(u.bytes().all(|b| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&b)), "{u}");
    }

    #[test]
    fn hex_returns_n_bytes_as_lowercase_hex() {
        for n in [1, 7, 16, 256] {
            let h = eval(&format!("return random.hex({n})"));
            assert_eq!(h.len(), 2 * n);
            assert!(is_lower_hex(&h), "{h}");
        }
        assert_eq!(eval("return random.hex(2.0)").len(), 4);
    }

    #[test]
    fn a_numeric_ts_is_rejected_like_an_event_timestamp_write() {
        for call in ["uuid7", "ulid"] {
            let err = eval_err(&format!("return random.{call}(1700000000000000000)"));
            assert!(
                err.contains(&format!(
                    "random.{call}: ts must be a string of decimal digits (a Lua number can't \
                     represent full nanosecond precision), got "
                )),
                "{err}"
            );
        }
        let err = eval_err("return random.uuid7()");
        assert!(err.contains("random.uuid7: ts must be a string of decimal digits"), "{err}");
        assert!(err.contains("got nil"), "{err}");
    }

    #[test]
    fn a_non_digit_ts_is_rejected() {
        let err = eval_err(r#"return random.ulid("soon")"#);
        assert!(err.contains("random.ulid: ts must be a string of decimal digits"), "{err}");
    }

    #[test]
    fn a_negative_ts_is_rejected() {
        let err = eval_err(r#"return random.uuid7("-1000000")"#);
        assert!(err.contains("random.uuid7: ts must not be negative, got -1000000"), "{err}");
    }

    #[test]
    fn hex_rejects_a_count_outside_1_to_256() {
        for (arg, got) in
            [("0", "0"), ("257", "257"), ("-1", "-1"), ("1.5", "1.5"), (r#""8""#, "string")]
        {
            let err = eval_err(&format!("return random.hex({arg})"));
            assert!(
                err.contains(&format!("random.hex: n must be an integer from 1 to 256, got {got}")),
                "{arg}: {err}"
            );
        }
    }

    #[test]
    fn minted_ids_are_accepted_by_event_new_and_a_log_write() {
        let w = ScriptWorker::new(
            r#"
            function process(event)
                event.log.trace_id = random.trace_id()
                event.log.span_id = random.span_id()
                return {event, Event.new{timestamp = event.timestamp, span = {
                    trace_id = random.trace_id(), span_id = random.span_id(), name = "minted"}}}
            end
            "#,
        )
        .expect("script should load");
        let mut event = Event::empty(5, AttrMap::new());
        event.log = Some(LogRecord {
            message: Value::str("hello"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        let ProcessOutcome::EmitMany(out) = w.process(event).expect("process should succeed")
        else {
            panic!("expected EmitMany");
        };
        let trace = out[0].0.log.as_ref().and_then(|l| l.trace).expect("a trace ref");
        assert_ne!(trace.trace_id, [0; 16]);
        assert!(trace.span_id.is_some_and(|s| s != [0; 8]));
        let span = out[1].0.span.as_ref().expect("a span");
        assert_ne!(span.trace_id, [0; 16]);
        assert_ne!(span.span_id, [0; 8]);
    }

    /// Two VMs' first `math.random()` draws.
    fn first_draws() -> Vec<f64> {
        worker()
            .lua
            .load("return {math.random(), math.random(), math.random()}")
            .eval()
            .expect("math.random should run")
    }

    /// Fails without `seed_math_random`: LuaJIT's own seed is a constant.
    #[test]
    fn each_vm_draws_a_different_math_random_sequence() {
        assert_ne!(first_draws(), first_draws());
    }
}
