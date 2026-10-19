//! Throwaway instrumentation: longest gap between heartbeat updates, and the phase it fell in.
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

thread_local! { static MINE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub static PHASE: AtomicU8 = AtomicU8::new(0);
static ACTIVE: AtomicU8 = AtomicU8::new(0);
static LAST_NS: AtomicU64 = AtomicU64::new(0);
static LAST_PHASE: AtomicU8 = AtomicU8::new(0);
static MAX_GAP: AtomicU64 = AtomicU64::new(0);
static MAX_FROM: AtomicU8 = AtomicU8::new(0);
static MAX_TO: AtomicU8 = AtomicU8::new(0);
pub static SORT_NS: AtomicU64 = AtomicU64::new(0);
pub static REPORT: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn epoch() -> Instant {
    static E: OnceLock<Instant> = OnceLock::new();
    *E.get_or_init(Instant::now)
}
pub fn now_ns() -> u64 {
    epoch().elapsed().as_nanos() as u64
}
pub fn name(p: u8) -> &'static str {
    match p {
        1 => "lua-flush",
        2 => "pairs-walk",
        3 => "sort",
        4 => "convert",
        5 => "after",
        _ => "other",
    }
}
/// Called from every heartbeat update.
pub fn on_update() {
    if ACTIVE.load(Relaxed) == 0 || !MINE.with(|m| m.get()) {
        return;
    }
    let now = now_ns();
    let last = LAST_NS.swap(now, Relaxed);
    let from = LAST_PHASE.swap(PHASE.load(Relaxed), Relaxed);
    let gap = now.saturating_sub(last);
    if gap > MAX_GAP.load(Relaxed) {
        MAX_GAP.store(gap, Relaxed);
        MAX_FROM.store(from, Relaxed);
        MAX_TO.store(PHASE.load(Relaxed), Relaxed);
    }
}
pub fn begin() {
    MAX_GAP.store(0, Relaxed);
    SORT_NS.store(0, Relaxed);
    LAST_NS.store(now_ns(), Relaxed);
    LAST_PHASE.store(1, Relaxed);
    PHASE.store(1, Relaxed);
    ACTIVE.store(1, Relaxed);
    MINE.with(|m| m.set(true));
}
pub fn end(total_start_ns: u64) {
    on_update();
    ACTIVE.store(0, Relaxed);
    MINE.with(|m| m.set(false));
    let line = format!(
        "GAPREPORT longest_gap_ms={:.1} phase={}->{} sort_ms={:.1} flush_total_ms={:.1}",
        MAX_GAP.load(Relaxed) as f64 / 1e6,
        name(MAX_FROM.load(Relaxed)),
        name(MAX_TO.load(Relaxed)),
        SORT_NS.load(Relaxed) as f64 / 1e6,
        (now_ns() - total_start_ns) as f64 / 1e6
    );
    eprintln!("{line}");
    REPORT.lock().unwrap().push(line);
}
