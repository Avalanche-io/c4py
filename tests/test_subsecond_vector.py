"""Cross-implementation conformance: a scan's sub-second times
(vectors/subsecond-vector, byte-identical to the Go reference's
c4m/testdata/subsecond-vector; README there).

A c4m line carries whole seconds, so:

1. a scan records each mtime floored to its second (b.txt's
   ``…:40.9999997`` is ``:40``; the float ``st_mtime`` rounded it to
   ``:41`` and the tree's C4 IDs differed from Go's);
2. a diff of a scanned tree against its own saved c4m is an empty patch.
   Comparing below the second wrote every unchanged entry into the patch,
   and ``c4 patch`` read each encoded line, byte-identical to the base's,
   as a removal: the documented ``patch_diff(load(old), scan(dir))``
   workflow deleted unchanged files on 1.0.26.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.diff import diff, patch_diff
from c4py.encoder import dumps
from c4py.entry import Entry
from c4py.id import identify_bytes
from c4py.manifest import Manifest
from c4py.scanner import scan

VECTOR = Path(__file__).parent / "vectors" / "subsecond-vector"


def _build(dest: Path) -> None:
    """Make the tree tree.txt describes, as the README says: a file's
    bytes are its base name and a newline; times are set once every entry
    exists, files first, then directories deepest first."""
    rows = []
    for line in (VECTOR / "tree.txt").read_text(encoding="utf-8").splitlines():
        mode, ns, path = line.split(" ", 2)
        rows.append((int(mode, 8), int(ns), path))
    for _, _, path in rows:
        full = dest / path.rstrip("/")
        if path.endswith("/"):
            full.mkdir(parents=True, exist_ok=True)
        else:
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_bytes(full.name.encode() + b"\n")
    for mode, ns, path in sorted(
        rows, key=lambda r: (r[2].endswith("/"), -r[2].count("/"))
    ):
        full = dest / path.rstrip("/")
        os.chmod(full, mode)
        os.utime(full, ns=(ns, ns))


def _entry_lines(patch: str) -> list[str]:
    """A patch's entry lines: everything after its base ID line."""
    return [ln for ln in patch.splitlines()[1:] if ln]


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    root = tmp_path / "tree"
    root.mkdir()
    _build(root)
    return root


def test_saved_resolves_to_root_id():
    saved = loads((VECTOR / "saved.c4m").read_text(encoding="utf-8"))
    want = (VECTOR / "root-id.txt").read_text(encoding="utf-8").strip()
    assert str(saved.compute_c4id()) == want


def test_scan_floors_to_the_second(tree: Path):
    saved = loads((VECTOR / "saved.c4m").read_text(encoding="utf-8"))
    want = {e.name: e.timestamp for e in saved}
    got = {e.name: e.timestamp for e in scan(tree)}
    assert got == want
    assert all(t.microsecond == 0 for t in got.values())


def test_diff_against_own_saved_c4m_is_empty(tree: Path):
    scanned = scan(tree)
    own = loads(dumps(scanned))
    assert _entry_lines(patch_diff(own, scan(tree))) == []
    d = diff(own, scan(tree))
    assert not (d.added or d.removed or d.modified)


@pytest.mark.skipif(sys.platform == "win32", reason="saved.c4m carries POSIX modes")
def test_scan_encodes_to_saved_and_diffs_empty(tree: Path):
    text = (VECTOR / "saved.c4m").read_text(encoding="utf-8")
    assert dumps(scan(tree)) == text
    assert _entry_lines(patch_diff(loads(text), scan(tree))) == []


def test_patch_diff_compares_at_the_second():
    """patch_diff compares at the format's resolution whatever made the
    manifests: a sub-second time in memory equals the whole second a
    loaded line carries."""
    c4id = identify_bytes(b"a\n")
    t = datetime(2025, 6, 15, 15, 6, 40, tzinfo=timezone.utc)

    def one(ts: datetime, with_id: bool = True) -> Manifest:
        e = Entry(name="a.txt", mode=0o644, timestamp=ts, size=2)
        if with_id:
            e.c4id = c4id
        return Manifest(entries=[e])

    fine = one(t.replace(microsecond=999_999))
    assert _entry_lines(patch_diff(loads(dumps(fine)), fine)) == []
    assert _entry_lines(patch_diff(one(t), one(t.replace(microsecond=1)))) == []
    # A different second is a change.
    later = one(t.replace(second=41))
    assert _entry_lines(patch_diff(one(t), later)) == [later[0].format()]
    # diff without C4 IDs compares metadata, also to the second.
    d = diff(one(t, with_id=False), one(t.replace(microsecond=500_000), with_id=False))
    assert d.modified == [] and len(d.same) == 1
    d = diff(one(t, with_id=False), one(t.replace(second=41), with_id=False))
    assert len(d.modified) == 1


@pytest.mark.parametrize(
    "ns, want",
    [
        (1_750_000_000_999_999_700, "2025-06-15T15:06:40Z"),  # rounds up as a float
        (-1_500_000_000, "1969-12-31T23:59:58Z"),  # truncating toward zero gives :59
        (-1_000_000_300, "1969-12-31T23:59:58Z"),  # rounding to the µs gives :59
        (-999_999_700, "1969-12-31T23:59:59Z"),
    ],
)
def test_scan_time_is_the_floor_of_the_second(tmp_path: Path, ns: int, want: str):
    """The scan floors st_mtime_ns toward negative infinity, the second Go
    writes for the same instant (measured with Go release/1.0.27)."""
    (tmp_path / "f").write_bytes(b"f\n")
    os.utime(tmp_path / "f", ns=(ns, ns))
    (e,) = list(scan(tmp_path))
    assert e.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") == want
