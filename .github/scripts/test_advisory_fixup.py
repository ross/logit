#!/usr/bin/env python3
"""Tests for advisory_fixup.py's matching. Run with `python3 .github/scripts/test_advisory_fixup.py`."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from advisory_fixup import cleared  # noqa: E402


def finding(ident, crate, version):
    return {"id": ident, "kind": "vulnerability", "crate": crate, "version": version, "title": "", "url": None}


class Cleared(unittest.TestCase):
    def test_bump_clears_the_only_flagged_version(self):
        before = [finding("RUSTSEC-1", "foo", "0.9.0")]
        fixed = cleared(before, [], {"foo": {"0.9.0"}}, {"foo": {"0.9.1"}})
        self.assertEqual([(f["crate"], f["version"], f["new"]) for f in fixed], [("foo", "0.9.0", "0.9.1")])

    def test_unbumped_finding_is_not_cleared(self):
        before = [finding("RUSTSEC-1", "foo", "0.9.0")]
        self.assertEqual(cleared(before, before, {"foo": {"0.9.0"}}, {"foo": {"0.9.0"}}), [])

    def test_one_advisory_two_lines_only_one_patched(self):
        # One advisory flags foo 0.1 and 0.2. Only 0.2 has a patched release, so 0.2.0 becomes
        # 0.2.1 and 0.1.0 stays flagged: the 0.2 fix counts and the 0.1 finding doesn't.
        before = [finding("RUSTSEC-1", "foo", "0.1.0"), finding("RUSTSEC-1", "foo", "0.2.0")]
        after = [finding("RUSTSEC-1", "foo", "0.1.0")]
        fixed = cleared(before, after, {"foo": {"0.1.0", "0.2.0"}}, {"foo": {"0.1.0", "0.2.1"}})
        self.assertEqual([(f["crate"], f["version"], f["new"]) for f in fixed], [("foo", "0.2.0", "0.2.1")])

    def test_bump_still_flagged_in_the_new_version_is_not_cleared(self):
        before = [finding("RUSTSEC-1", "foo", "1.0.0"), finding("RUSTSEC-1", "foo", "2.0.0")]
        after = [finding("RUSTSEC-1", "foo", "1.0.0"), finding("RUSTSEC-1", "foo", "2.0.1")]
        fixed = cleared(before, after, {"foo": {"1.0.0", "2.0.0"}}, {"foo": {"1.0.0", "2.0.1"}})
        self.assertEqual(fixed, [])

    def test_crate_dropped_from_the_lock_is_cleared(self):
        before = [finding("RUSTSEC-1", "foo", "0.9.0")]
        fixed = cleared(before, [], {"foo": {"0.9.0"}}, {})
        self.assertEqual([f["new"] for f in fixed], ["removed"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
