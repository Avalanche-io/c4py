"""A size of 0 beside a C4 ID other than the empty content's is a null size.

Ruled 2026-11-08, landed in c4 core PR #68 on 2026-11-13 (decided by the
release lead; reversible). A filesystem surface has no "-", so a null size
materialises there as 0, and only the empty content truthfully has size 0.
A 0 with no C4 ID, a 0 with the empty content's ID, and a symlink's 0 are
known zeros.

The fixtures under ``tests/vectors/null-size-vector/`` are byte-identical
copies of the Go reference repo's ``c4m/testdata/null-size-vector/``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from c4py.decoder import load, loads
from c4py.encoder import dumps
from c4py.entry import EMPTY_CONTENT_ID, NULL_SIZE, Entry
from c4py.id import identify_bytes
from c4py.manifest import Manifest, _propagate_metadata

VECTOR = Path(__file__).parent / "vectors" / "null-size-vector"
TS = "2026-03-06T12:00:00Z"
HELLO = str(identify_bytes(b"hello"))
EMPTY = str(EMPTY_CONTENT_ID)

# The README's table: entry name -> (size is null, known size)
EXPECTED = {
    "null.txt": (True, None),
    "collapsed/": (True, None),
    "empty.txt": (False, 0),
    "emptydir/": (False, 0),
    "structure.txt": (False, 0),
    "link": (False, 0),
    "known.txt": (False, 5),
}


def test_empty_content_id_is_id_of_zero_bytes():
    assert EMPTY_CONTENT_ID == identify_bytes(b"")


@pytest.mark.parametrize("name", ["canonical.c4m", "zero-spelled.c4m"])
def test_vector(name):
    root = (VECTOR / "root-id.txt").read_text().strip()
    canonical = (VECTOR / "canonical.c4m").read_bytes()

    m = load(str(VECTOR / name))
    assert str(m.compute_c4id()) == root

    seen = {}
    for e in m.entries:
        is_null, known = EXPECTED[e.name]
        seen[e.name] = True
        assert e.has_null_size() is is_null, e.name
        if is_null:
            assert e.size == NULL_SIZE, f"{e.name}: decoder must hold a null size as -1"
        else:
            assert e.size == known, e.name
    assert set(seen) == set(EXPECTED)

    assert dumps(m).encode("utf-8") == canonical


@pytest.mark.parametrize(
    "line, want_null",
    [
        ("-rw-r--r-- " + TS + " 0 f " + HELLO, True),  # 0 beside real content's ID
        ("-rw-r--r-- " + TS + " 0 f " + EMPTY, False),  # 0 beside the empty ID
        ("-rw-r--r-- " + TS + " 0 f -", False),  # structure-level scan
        ("lrwxr-xr-x " + TS + " 0 l -> f " + HELLO, False),  # symlink exempt
        ("drwxr-xr-x " + TS + " 0 d/ " + EMPTY, False),  # empty listing
        ("drwxr-xr-x " + TS + " 0 d/ " + HELLO, True),  # 0 beside another ID
        ("-rw-r--r-- " + TS + " 5 f " + HELLO, False),  # non-zero
        ("-rw-r--r-- " + TS + " - f " + HELLO, True),  # dash
    ],
)
def test_null_size_zero(line, want_null):
    e = loads(line + "\n").entries[0]
    assert e.has_null_size() is want_null
    if want_null:
        assert e.size == NULL_SIZE
    size_field = e.canonical().split(" ")[2]
    assert (size_field == "-") is want_null


def test_zero_and_dash_are_one_description():
    from_zero = loads("-rw-r--r-- " + TS + " 0 f " + HELLO + "\n")
    from_dash = loads("-rw-r--r-- " + TS + " - f " + HELLO + "\n")
    assert from_zero.compute_c4id() == from_dash.compute_c4id()

    # Built through the API with size 0 (as a scan over a mount would build
    # it), the entry still renders and hashes as null.
    built = Manifest(entries=[Entry(
        name="f",
        mode=0o100644,
        timestamp=datetime(2026, 3, 6, 12, 0, 0, tzinfo=timezone.utc),
        size=0,
        c4id=identify_bytes(b"hello"),
    )])
    assert built.compute_c4id() == from_dash.compute_c4id()
    assert " - f " in dumps(built)
    assert " - f " in dumps(built, pretty=True)
    assert built.entries[0].has_null_size()
    assert built.entries[0].human_size() == "-"


def test_null_size_zero_propagates():
    m = loads(
        "drwxr-xr-x " + TS + " - d/ -\n"
        "  -rw-r--r-- " + TS + " 0 f " + HELLO + "\n"
    )
    _propagate_metadata(m.entries)
    for e in m.entries:
        if e.is_dir():
            assert e.has_null_size(), f"directory size must be null, got {e.size}"


def test_api_built_zero_propagates():
    # Not through the decoder: the predicate, not the decoder's rewrite to
    # -1, is what propagation asks.
    d = Entry(name="d/", mode=0o040755, size=NULL_SIZE)
    f = Entry(name="f", mode=0o100644, size=0, c4id=identify_bytes(b"hello"), depth=1)
    _propagate_metadata([d, f])
    assert d.size == NULL_SIZE
