"""VictoriaMetrics queries from the host, over the loopback port compose.yaml publishes.

`/api/v1/export` returns one JSON object per series per line: `{"metric": {...}, "values":
[...], "timestamps": [...]}`, timestamps in milliseconds. Newly ingested samples become
searchable after VictoriaMetrics flushes its in-memory buffers, which `/internal/force_flush`
forces.

The ledger's V is `totals_by_life()`: each series' reset-aware total, split at its resets and at
the SUT's life boundaries into the SUT's process lives.
"""

import json
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT_S = 30.0


class VmError(Exception):
    pass


def _get(base, path, params=None, timeout=TIMEOUT_S):
    url = f"{base}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.read().decode()
    except (urllib.error.URLError, OSError, ValueError) as err:
        raise VmError(f"GET {path}: {err}") from None


def healthy(base):
    try:
        return _get(base, "/health", timeout=5).strip() == "OK"
    except VmError:
        return False


def force_flush(base):
    _get(base, "/internal/force_flush")


def parse_export(text):
    """The series of an `/api/v1/export` body, skipping lines that aren't JSON."""
    series = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            series.append(json.loads(line))
        except ValueError:
            continue
    return series


def export_raw(base, selector, start, end=None, timeout=TIMEOUT_S):
    params = {"match[]": selector, "start": str(start)}
    if end is not None:
        params["end"] = str(end)
    return _get(base, "/api/v1/export", params, timeout=timeout)


def export(base, selector, start, end=None):
    return parse_export(export_raw(base, selector, start, end))


def newest_ms(series):
    """The newest sample timestamp across `series`, in ms, or None."""
    newest = None
    for one in series:
        stamps = one.get("timestamps") or []
        if stamps and (newest is None or stamps[-1] > newest):
            newest = stamps[-1]
    return newest


def freshness(base, selector, now, lookback_s):
    """How old the newest stored sample matching `selector` is at `now` (epoch seconds),
    looking `lookback_s` back: `{series, newest_ms, age_s}`, `age_s` None when nothing matched.
    Raises `VmError`."""
    series = export(base, selector, int(now - lookback_s))
    newest = newest_ms(series)
    return {
        "series": len(series),
        "newest_ms": newest,
        "age_s": None if newest is None else round(now - newest / 1000, 3),
    }


def sample_spacing_s(series):
    """The median gap between consecutive samples across `series`, in seconds, or None."""
    gaps = sorted(b - a for one in series
                  for a, b in zip(one.get("timestamps") or [], (one.get("timestamps") or [])[1:]))
    return gaps[len(gaps) // 2] / 1000 if gaps else None


def last_total(series):
    """The sum of each series' last value: what the stored counters read now."""
    return sum((one.get("values") or [0])[-1] for one in series if one.get("values"))



def series_name(one):
    metric = one.get("metric") or {}
    labels = ",".join(f"{k}={v}" for k, v in sorted(metric.items()) if k != "__name__")
    return metric.get("__name__", "?") + (f"{{{labels}}}" if labels else "")


def reset_aware(one, life_starts=()):
    """One exported series as `(total, segments)`: the reset-aware total (the first value, then
    every non-negative step, and on a decrease or a new life the new value), and the same total
    split at each into `[(first_ms, amount)]`, one segment per counter life.

    `life_starts` are the writer's life starts in epoch seconds. A sample's timestamp is the
    writer's own window time, so it places the sample in a life; a restart whose first value is
    at or above the last life's final value is a reset no decrease shows."""
    values = one.get("values") or []
    stamps = one.get("timestamps") or []
    segments = []
    previous = None
    previous_life = None
    for value, stamp in zip(values, stamps):
        life = life_at(life_starts, stamp / 1000)
        if previous is None or value < previous or life != previous_life:
            segments.append([stamp, value])
        else:
            segments[-1][1] += value - previous
        previous = value
        previous_life = life
    segments = [(stamp, amount) for stamp, amount in segments]
    return sum(amount for _, amount in segments), segments


def totals_by_life(series, life_starts):
    """The reset-aware total of `series` split per process life of the writer.

    `life_starts` holds each life's start in epoch seconds. A series with one segment per life
    (split at its resets and at each life start, so one reset fewer than there are lives) has its
    segment k credited to life k; any other series, one absent from a life or with a decrease
    inside one, has each segment credited to the life its first sample falls in, and is listed as
    a mismatch. Returns
    `(by_life, total, resets, mismatched)`: `{life: amount}`, the overall total, `{series name:
    resets}`, and the names whose resets weren't lives minus one."""
    lives = max(1, len(life_starts))
    by_life = {}
    resets = {}
    mismatched = []
    total = 0
    for one in series:
        amount, segments = reset_aware(one, life_starts)
        total += amount
        name = series_name(one)
        resets[name] = len(segments) - 1
        if len(segments) == lives:
            owners = range(lives)
        else:
            mismatched.append(name)
            owners = [life_at(life_starts, stamp / 1000) for stamp, _ in segments]
        for life, (_, part) in zip(owners, segments):
            by_life[life] = by_life.get(life, 0) + part
    return by_life, total, resets, mismatched


def life_at(life_starts, t):
    """The index of the newest life that started at or before `t`, or 0."""
    life = 0
    for index, start in enumerate(life_starts):
        if t >= start:
            life = index
    return life
