"""SafeName's printable test is pinned to Unicode 17.0.0 (c4 core PR #149).

Decided 2026-11-13 by the release lead; reversible. The table is the shared
tests/vectors/safename-vector/printable-17.0.0.txt (a byte-for-byte copy of
c4 core's), turned into src/c4py/_printable.py by scripts/gen_printable.py.
The runtime's unicodedata is never asked.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from c4py._printable import PRINTABLE_RANGES, PRINTABLE_UNICODE_VERSION
from c4py.safename import _is_printable, safe_name, unsafe_name

ROOT = Path(__file__).resolve().parent.parent
VECTOR = ROOT / "tests" / "vectors" / "safename-vector"


def _shared_ranges() -> list[tuple[int, int]]:
    ranges = []
    text = (VECTOR / f"printable-{PRINTABLE_UNICODE_VERSION}.txt").read_text(encoding="ascii")
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        lo, hi = line.split("..")
        ranges.append((int(lo, 16), int(hi, 16)))
    return ranges


def test_pinned_version_is_the_vectors():
    assert PRINTABLE_UNICODE_VERSION == "17.0.0"
    pinned = (VECTOR / "unicode-version.txt").read_text(encoding="ascii").strip()
    assert pinned == PRINTABLE_UNICODE_VERSION


def test_table_equals_shared_file():
    shared = _shared_ranges()
    assert len(shared) == 741
    assert list(PRINTABLE_RANGES) == shared


def test_generated_module_is_current():
    # The checked-in module is exactly what the generator writes.
    gen_path = ROOT / "scripts" / "gen_printable.py"
    spec = importlib.util.spec_from_file_location("gen_printable", gen_path)
    gen = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(gen)
    want = gen.render(PRINTABLE_UNICODE_VERSION, _shared_ranges())
    assert (ROOT / "src" / "c4py" / "_printable.py").read_text(encoding="utf-8") == want


def test_lookup_matches_table_at_every_boundary():
    prev_hi = -1
    for lo, hi in PRINTABLE_RANGES:
        if lo - 1 > prev_hi:
            assert not _is_printable(chr(lo - 1)), hex(lo - 1)
        assert _is_printable(chr(lo)), hex(lo)
        assert _is_printable(chr(hi)), hex(hi)
        prev_hi = hi
    assert not _is_printable(chr(prev_hi + 1))
    assert not _is_printable("\x00")
    assert not _is_printable("\x1f")


def test_safe_name_does_not_follow_the_runtime_unicode():
    # U+0870 was assigned in Unicode 14.0: Python 3.10's unicodedata (13.0)
    # called it unassigned, so SafeName escaped it there and wrote it
    # plainly on 3.11+, and a name holding it had two IDs.
    assert safe_name("aࡰb") == "aࡰb"
    # U+0378 is unassigned in 17.0.0 and stays encoded even on a runtime
    # that assigns it later.
    enc = safe_name("a͸b")
    assert enc != "a͸b"
    assert unsafe_name(enc) == "a͸b"
    # Go's IsPrint admits U+0020 and no other space separator.
    assert safe_name("a b") == "a b"
    for zs in (" ", " ", " ", "　"):
        enc = safe_name("a" + zs + "b")
        assert enc != "a" + zs + "b", hex(ord(zs))
        assert unsafe_name(enc) == "a" + zs + "b"
