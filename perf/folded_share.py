#!/usr/bin/env python3
"""Share of samples under named frame groups, from `inferno-collapse-perf` output.

    script/perf flamegraph --scenario json-parse-app-log --folded /tmp/app.folded
    python3 perf/folded_share.py /tmp/app.folded

`logit-perf flamegraph --folded PATH` keeps the collapsed stacks the SVG is rendered from: one
`frame;frame;frame <count>` line per unique stack, the count being `perf record` samples. This
reads those and reports, for each group below, the share of samples whose stack *contains* a
matching frame -- "how much of this process's time is spent under the allocator", not "how much is
spent in `malloc` itself with nothing below it".

Three things about that denominator, because they decide what the numbers mean:

- **Containment, not leaf.** A sample is counted for a group if any frame anywhere in its stack
  matches. `memcpy` called from inside `realloc` is counted under both `alloc` and `memcpy`, so the
  groups deliberately do not sum to 100% and are not a partition. That is the right shape for the
  question `docs/plans/event-sizing.md`'s W2 asks -- "what share of the process is allocator work"
  -- and the wrong shape for "where does the time go", which is `script/perf attribute`'s job.
- **A group is matched at most once per stack**, so a recursive or repeated frame doesn't inflate
  it.
- **Every thread, and every process under the capture.** `perf record` was pointed at one `logit`
  run, so that is all there is, but the tokio worker threads and the generator thread are all in
  here together. Pass `--thread-regex` to restrict to stacks whose thread name matches.

Stdlib only, no repo imports, no dependency on a build: this is deliberately runnable against a
folded file captured on the perf VM and copied back, on any machine.
"""

import argparse
import re
import sys
from collections import defaultdict

# Frame-name patterns, by group. Substring match on the lowercased frame name unless the pattern
# is wrapped in `re:`, in which case it is a regex.
#
# The jemalloc names are what `tikv-jemalloc-sys` actually exports in this build: the `_rjem_`
# prefix (its `with_jemalloc_prefix` renaming), plus the unprefixed `je_`/`arena_`/`tcache_`
# internals that survive into the symbol table, plus the `*allocx` extended API Rust's
# `GlobalAlloc` impl calls into. `docs/adr/jemalloc-global-allocator.md`.
GROUPS = {
    "alloc": [
        "_rjem_",
        "je_",
        "jemalloc",
        "mallocx",
        "rallocx",
        "sdallocx",
        "xallocx",
        "nallocx",
        "tcache_",
        "arena_",
        "re:^(__)?(malloc|free|calloc|realloc|posix_memalign|aligned_alloc)$",
        "__rust_alloc",
        "__rust_dealloc",
        "__rust_realloc",
    ],
    "memmove": ["memcpy", "memmove", "memset", "__memcpy", "__memmove"],
    "attrmap": ["attrmap", "smallvec", "insert_sym", "spilled", "try_grow", "reserve_one"],
    "value_drop": [
        "re:value.*drop",
        "re:drop_in_place.*value",
        "re:drop_in_place.*event",
        "re:drop_in_place.*attrmap",
    ],
    "event_clone": [
        "re:clone.*event",
        "re:event.*clone",
        "re:eventbatch.*clone",
        "re:unwrap_batch",
        "re:attrmap.*clone",
    ],
    "interner": ["interner", "lasso", "rodeo"],
    "serde_json": ["serde_json", "simd_json"],
}


def compile_group(patterns):
    plain, regexes = [], []
    for pattern in patterns:
        if pattern.startswith("re:"):
            regexes.append(re.compile(pattern[3:]))
        else:
            plain.append(pattern)
    return plain, regexes


COMPILED = {name: compile_group(patterns) for name, patterns in GROUPS.items()}


def matches(frame, group):
    plain, regexes = COMPILED[group]
    return any(p in frame for p in plain) or any(r.search(frame) for r in regexes)


def read(path, thread_regex):
    """(total samples, {group: samples}, [(stack, samples)]) for one folded file."""
    total = 0
    per_group = defaultdict(int)
    stacks = []
    thread = re.compile(thread_regex) if thread_regex else None
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line:
                continue
            stack, _, count = line.rpartition(" ")
            try:
                count = int(count)
            except ValueError:
                continue
            if thread and not thread.search(stack):
                continue
            total += count
            stacks.append((stack, count))
            frames = [frame.lower() for frame in stack.split(";")]
            for group in GROUPS:
                if any(matches(frame, group) for frame in frames):
                    per_group[group] += count
    return total, per_group, stacks


def top_frames(stacks, group, limit):
    """The matching frames themselves, by samples, so a share can be explained rather than trusted."""
    counts = defaultdict(int)
    for stack, count in stacks:
        for frame in stack.split(";"):
            if matches(frame.lower(), group):
                counts[frame] += count
    return sorted(counts.items(), key=lambda kv: -kv[1])[:limit]


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("folded", nargs="+", help="one or more `--folded` outputs")
    parser.add_argument(
        "--thread-regex", help="only stacks whose folded line matches this (thread name is frame 0)"
    )
    parser.add_argument(
        "--explain",
        metavar="GROUP",
        help="also list the individual frames that matched GROUP, by samples",
    )
    parser.add_argument("--top", type=int, default=15, help="how many frames `--explain` lists")
    args = parser.parse_args()

    width = max(len(p.rsplit("/", 1)[-1]) for p in args.folded)
    header = f"{'capture':{width}} {'samples':>9} " + " ".join(f"{g:>12}" for g in GROUPS)
    print(header)
    print("-" * len(header))
    for path in args.folded:
        total, per_group, stacks = read(path, args.thread_regex)
        if total == 0:
            print(f"{path.rsplit('/', 1)[-1]:{width}} {'0':>9}  (no stacks matched)")
            continue
        shares = " ".join(f"{100.0 * per_group[g] / total:11.2f}%" for g in GROUPS)
        print(f"{path.rsplit('/', 1)[-1]:{width}} {total:9d} {shares}")
        if args.explain:
            if args.explain not in GROUPS:
                sys.exit(f"unknown group {args.explain!r}; known: {', '.join(GROUPS)}")
            print(f"\n  {args.explain} frames in {path}:")
            for frame, count in top_frames(stacks, args.explain, args.top):
                print(f"  {100.0 * count / total:7.2f}%  {frame}")
            print()


if __name__ == "__main__":
    main()
