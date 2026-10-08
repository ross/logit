"""VictoriaMetrics queries from the host, over the loopback port compose.yaml publishes.

`/api/v1/export` returns one JSON object per series per line: `{"metric": {...}, "values":
[...], "timestamps": [...]}`, timestamps in milliseconds. Newly ingested samples become
searchable after VictoriaMetrics flushes its in-memory buffers, which `/internal/force_flush`
forces.
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

