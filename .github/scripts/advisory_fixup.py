#!/usr/bin/env python3
"""Bump crates that RustSec flags to a patched semver-compatible release, and describe the result.

Usage: advisory_fixup.py OUT_DIR

Run from the repository root by .github/workflows/advisories.yml's `fixup` job after `cargo deny
check advisories` fails on `main` (docs/adr/advisories-off-the-pr-gate.md). It runs `cargo update -p
NAME@VERSION` for each crate flagged as vulnerable, unsound, or yanked, edits nothing but
`Cargo.lock`, and reruns the check. Neither cargo command compiles anything or runs a build script,
and the job has to stay that way: it runs with a token that can push.

It writes OUT_DIR/pr-body.md and OUT_DIR/issue-body.md, and these step outputs to $GITHUB_OUTPUT
(printed to stdout when that's unset):

- `pr`: `true` when the bump cleared at least one advisory and a PR token is available.
- `title`: the PR title and commit subject.
- `pr_body`, `issue_body`: absolute paths of the two body files.
- `issue`: `true` when an advisory is still flagged after the bump, or when the bump cleared one
  but `HAS_PR_TOKEN` isn't `true`, so no PR can carry it.

Exits nonzero only when cargo-deny fails without reporting an advisory, such as when it can't fetch
the database.
"""

import json
import os
import subprocess
import sys
import tomllib

# Kinds a newer semver-compatible release can clear. `unmaintained` and informational notices
# stay flagged whatever the version, so bumping for them only adds unrelated lock churn.
BUMPABLE = {"vulnerability", "unsound", "yanked"}


def check():
    """Run the advisory check; return a finding for every error it reports."""
    proc = subprocess.run(
        ["cargo", "deny", "--format", "json", "check", "advisories"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    findings = []
    # cargo-deny writes one JSON object per line to stderr. Diagnostics are
    # {"type": "diagnostic", "fields": {"code", "severity", "message", "advisory"?, "graphs"}},
    # where graphs[0].Krate is the flagged {name, version}; a yanked crate has no `advisory`.
    for line in proc.stderr.splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        fields = obj.get("fields", {})
        if obj.get("type") != "diagnostic" or fields.get("severity") != "error":
            continue
        graphs = fields.get("graphs") or [{}]
        krate = graphs[0].get("Krate") or {}
        advisory = fields.get("advisory") or {}
        ident = advisory.get("id") or fields.get("code", "unknown")
        name = krate.get("name") or advisory.get("package") or "?"
        findings.append({
            "id": ident,
            "kind": fields.get("code", "unknown"),
            "crate": name,
            "version": krate.get("version", "?"),
            # The title lands in a Markdown table cell.
            "title": (advisory.get("title") or fields.get("message", "")).replace("|", "\\|"),
            "url": advisory.get("url"),
        })
    if proc.returncode != 0 and not findings:
        sys.stderr.write(proc.stderr)
        sys.exit(f"cargo deny exited {proc.returncode} without reporting an advisory")
    return findings


def locked_versions():
    with open("Cargo.lock", "rb") as f:
        lock = tomllib.load(f)
    versions = {}
    for pkg in lock.get("package", []):
        versions.setdefault(pkg["name"], set()).add(pkg["version"])
    return versions


def advisory_link(f):
    if f["id"].startswith("RUSTSEC-"):
        return f"[{f['id']}](https://rustsec.org/advisories/{f['id']}.html)"
    return f"`{f['id']}`"


def short_ids(ids):
    """RUSTSEC-2026-0346, RUSTSEC-2026-0347 -> RUSTSEC-2026-0346/0347, as the hand-written PRs do."""
    groups = {}
    for i in sorted(ids):
        prefix, _, num = i.rpartition("-")
        groups.setdefault(prefix if i.startswith("RUSTSEC-") else i, []).append(num)
    parts = []
    for prefix, nums in groups.items():
        parts.append(f"{prefix}-{'/'.join(nums)}" if prefix.startswith("RUSTSEC-") else prefix)
    return ", ".join(parts)


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: advisory_fixup.py OUT_DIR")
    out_dir = os.path.abspath(sys.argv[1])
    os.makedirs(out_dir, exist_ok=True)
    has_token = os.environ.get("HAS_PR_TOKEN") == "true"

    before = check()
    if not before:
        print("cargo deny check advisories passes; nothing to fix")
    old_lock = locked_versions()

    for name, version in sorted({(f["crate"], f["version"]) for f in before if f["kind"] in BUMPABLE}):
        print(f"cargo update -p {name}@{version}", flush=True)
        if subprocess.run(["cargo", "update", "-p", f"{name}@{version}"]).returncode != 0:
            print(f"  cargo update failed for {name}@{version}; leaving it flagged")

    new_lock = locked_versions()
    after = check() if new_lock != old_lock else before

    # A bump changes the version, so a finding is matched across the two runs by advisory and crate.
    still = {(f["id"], f["crate"]) for f in after}
    fixed = [f for f in before if (f["id"], f["crate"]) not in still]
    remaining = after
    for f in fixed:
        added = sorted(new_lock.get(f["crate"], set()) - old_lock.get(f["crate"], set()))
        f["new"] = ", ".join(added) if added else "removed"

    bumps = sorted({(f["crate"], f["new"]) for f in fixed})
    title = "fix(deps): bump {} for {}".format(
        ", ".join(f"{c} to {v}" for c, v in bumps), short_ids({f["id"] for f in fixed})
    )

    pr_lines = [
        "A semver-compatible `cargo update` clears these RustSec advisories. Only `Cargo.lock` changes.",
        "",
        "| Advisory | Crate | Version |",
        "|---|---|---|",
    ]
    pr_lines += [
        f"| {advisory_link(f)}: {f['title']} | `{f['crate']}` | {f['version']} → {f['new']} |" for f in fixed
    ]
    if remaining:
        pr_lines += ["", "Other advisories are still flagged after the bump; the `rustsec` issue lists them."]
    pr_lines += ["", "Opened by `.github/workflows/advisories.yml`."]

    issue_lines = []
    if remaining:
        issue_lines += [
            "`cargo deny check advisories` fails on `main`, and a semver-compatible `cargo update` doesn't clear these:",
            "",
            "| Advisory | Kind | Crate | Version | Title |",
            "|---|---|---|---|---|",
        ]
        issue_lines += [
            f"| {advisory_link(f)} | {f['kind']} | `{f['crate']}` | {f['version']} | {f['title']} |" for f in remaining
        ]
        issue_lines += [
            "",
            "Each needs a hand fix: a `Cargo.toml` bump across a major version, a replacement crate, "
            "or an `ignore` entry with its reason in `deny.toml`.",
        ]
    if fixed and not has_token:
        if issue_lines:
            issue_lines.append("")
        issue_lines += [
            "A semver-compatible bump clears these, but `ADVISORY_PR_TOKEN` isn't set in the "
            "`advisories` environment, so no PR was opened:",
            "",
        ]
        issue_lines += [f"- {advisory_link(f)}: `{f['crate']}` {f['version']} → {f['new']}" for f in fixed]
        issue_lines += ["", "To apply it: " + "; ".join(f"`cargo update -p {c}`" for c, _ in bumps) + "."]

    pr_body = os.path.join(out_dir, "pr-body.md")
    issue_body = os.path.join(out_dir, "issue-body.md")
    with open(pr_body, "w") as f:
        f.write("\n".join(pr_lines) + "\n")
    with open(issue_body, "w") as f:
        f.write("\n".join(issue_lines) + "\n")

    outputs = {
        "pr": str(bool(fixed) and has_token).lower(),
        "issue": str(bool(issue_lines)).lower(),
        "title": title,
        "pr_body": pr_body,
        "issue_body": issue_body,
    }
    print(f"fixed: {len(fixed)}, still flagged: {len(remaining)}")
    lines = "".join(f"{k}={v}\n" for k, v in outputs.items())
    target = os.environ.get("GITHUB_OUTPUT")
    if target:
        with open(target, "a") as out:
            out.write(lines)
    else:
        sys.stdout.write(lines)


if __name__ == "__main__":
    main()
