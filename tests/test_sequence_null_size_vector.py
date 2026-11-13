"""A folded sequence's size is null when any frame's size is null.

Ruled 2026-11-13: "One cannot use size as an expectation if there are nulls
present, so we must say nothing." Landed in c4 core PR #155 (decided
2026-11-13 by the release lead; reversible). A range's size is the sum of
its frames' sizes, and null if any frame's size is null (``-``, or a ``0``
beside a C4 ID other than the empty content's).

c4py does not fold sequences (it has no sequence detector), so this tests
the decode half of the vector only: decoding ``folded.c4m`` to its root ID,
the plate range's size known 24 and the shot range's null, and re-encoding
it canonically byte for byte. The frames files are copied with the vector
for the day c4py folds.

The fixtures under ``tests/vectors/sequence-null-size-vector/`` are
byte-identical copies of the Go reference repo's
``c4m/testdata/sequence-null-size-vector/`` at c4 commit 7a6ba94 (#156).
"""

from __future__ import annotations

from pathlib import Path

from c4py.decoder import load
from c4py.encoder import dumps
from c4py.entry import NULL_SIZE

VECTOR = Path(__file__).parent / "vectors" / "sequence-null-size-vector"

# The README's table: entry name -> (size is null, known size)
EXPECTED = {
    "plate.[1001-1004].exr": (False, 24),
    "shot.[1001-1004].exr": (True, None),
}


def test_decode_folded():
    root = (VECTOR / "folded-root-id.txt").read_text().strip()
    m = load(str(VECTOR / "folded.c4m"))
    assert str(m.compute_c4id()) == root

    seen = set()
    for e in m.entries:
        is_null, known = EXPECTED[e.name]
        seen.add(e.name)
        assert e.is_sequence, e.name
        assert e.has_null_size() is is_null, e.name
        if is_null:
            assert e.size == NULL_SIZE, f"{e.name}: a null range size is held as -1"
        else:
            assert e.size == known, e.name
    assert seen == set(EXPECTED)


def test_reencode_folded_byte_for_byte():
    folded = (VECTOR / "folded.c4m").read_bytes()
    m = load(str(VECTOR / "folded.c4m"))
    assert dumps(m).encode("utf-8") == folded


def test_frames_zero_spelled_is_the_same_description():
    # The shot.1003.exr frame's size is "-" in one file and "0" beside its
    # (non-empty) C4 ID in the other: both are null, so one description.
    dash = load(str(VECTOR / "frames.c4m"))
    zero = load(str(VECTOR / "frames-zero-spelled.c4m"))
    assert dash.compute_c4id() == zero.compute_c4id()
    frame = next(e for e in zero.entries if e.name == "shot.1003.exr")
    assert frame.has_null_size()
