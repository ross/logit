#!/usr/bin/env python3
"""Drives every `applogs` app with a small, realistic request mix for the capture window.

Stdlib-only. Each target gets a thread pool paced to a fixed rate, so line counts are comparable
and nothing saturates. Prints progress every 15s.

Usage:  drive.py <seconds> <profile>=<base-url>[,<rate>] ...
        profile is `app` (the logging-library apps) or `django` (trailing slashes, /fanout/).
"""

import random
import sys
import threading
import time
import urllib.error
import urllib.request

# Real user-agent strings of very different lengths: the long tail of an access log's value bytes.
AGENTS = [
    "curl/8.5.0",
    "python-requests/2.32.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
    "kube-probe/1.31",
]

TERMS = ["widget", "blue%20widget", "socket+wrench", "q1%20report", "ünïcode", "a", "very-long-search-term-nobody-would-type"]

#: (weight, path-template) pairs for a read-mostly web tier: mostly successful reads, a tail of
#: 404s, and a trickle of 500s.
MIX_APP = [
    (20, "/"),
    (30, "/items?limit={limit}&page={page}&sort=created"),
    (25, "/items/{id}"),
    (15, "/search?q={term}&lang=en&page={page}"),
    (7, "/nope/{id}"),
    (3, "/boom"),
]

MIX_DJANGO = [
    (18, "/"),
    (25, "/items/"),
    (25, "/items/{id}/"),
    (14, "/fanout/"),
    (12, "/items/{big}/"),
    (6, "/boom/"),
]

counts = {}
lock = threading.Lock()


def pick(mix):
    total = sum(w for w, _ in mix)
    n = random.randrange(total)
    for weight, template in mix:
        if n < weight:
            return template
        n -= weight
    return mix[-1][1]


def fill(template):
    return template.format(
        limit=random.choice([10, 20, 50, 100]),
        page=random.randrange(1, 40),
        id=random.choice([0, 1, 2, 3, 7, 11, 19, 25, 999]),
        big=random.randrange(100, 100000),
        term=random.choice(TERMS),
    )


def worker(name, base, mix, rate, threads, deadline):
    interval = threads / rate
    while time.monotonic() < deadline:
        started = time.monotonic()
        url = base + fill(pick(mix))
        request = urllib.request.Request(url, headers={"User-Agent": random.choice(AGENTS)})
        status = "err"
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status = response.status
                response.read()
        except urllib.error.HTTPError as e:  # a 404/500 is traffic, not a failure
            status = e.code
            e.read()
        except Exception:
            pass
        with lock:
            counts[name] = counts.get(name, 0) + 1
            counts[f"{name}:{status}"] = counts.get(f"{name}:{status}", 0) + 1
        slept = interval - (time.monotonic() - started)
        if slept > 0:
            time.sleep(slept)


def main():
    seconds = float(sys.argv[1])
    deadline = time.monotonic() + seconds
    random.seed(1)
    threads = []
    for spec in sys.argv[2:]:
        profile, _, rest = spec.partition("=")
        base, _, rate = rest.partition(",")
        rate = float(rate or 25)
        mix = MIX_DJANGO if profile == "django" else MIX_APP
        pool = 4 if profile == "django" else 3
        name = base.split("//")[-1].split(":")[0]
        for _ in range(pool):
            t = threading.Thread(target=worker, args=(name, base, mix, rate, pool, deadline), daemon=True)
            t.start()
            threads.append(t)
    print(f"drive: {len(threads)} threads for {seconds}s", flush=True)
    while time.monotonic() < deadline:
        time.sleep(15)
        with lock:
            print("drive: " + " ".join(f"{k}={v}" for k, v in sorted(counts.items()) if ":" not in k), flush=True)
    for t in threads:
        t.join(timeout=15)
    with lock:
        print("drive: final " + " ".join(f"{k}={v}" for k, v in sorted(counts.items())), flush=True)


if __name__ == "__main__":
    main()
