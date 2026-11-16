"""A folded range's size counts its ID list; a directory above it counts that.

The shared sequence-size vector (c4 core ``c4m/testdata/sequence-size-vector/``,
c4 #161), vendored byte for byte from c4 commit b82ec07. Ruled 2026-11-13 by
the maintainer: "The size is the size of every frame and the frame id list.
Note that the frame id list has a compleately deducable size based on the
item count alone since c4 id's have a fixed length."

A range's size is its frames' bytes plus count x 90 (the list's bytes), null
if any frame's size is null. A directory holding a range follows the existing
rule with the range as one child: its children's sizes plus its listing as
written, the folded one.

c4py does not fold sequences and has no expander, so it tests the decode
side: the sizes ``folded.c4m`` and ``expanded.c4m`` state, and the directory
size c4py's normaliser (``_propagate_metadata``, run by ``compute_c4id``)
derives over a range child. Through 2026-11-13 Go's normaliser derived 175
for ``sub/`` here, from a range of 33; c4py derives from the range's stated
size, so it needed no change and gives 446.
"""

from __future__ import annotations

from pathlib import Path

from c4py.decoder import load
from c4py.encoder import dumps
from c4py.entry import NULL_SIZE
from c4py.manifest import _propagate_metadata

VECTOR = Path(__file__).parent / "vectors" / "sequence-size-vector"
RANGE = "f.[001-003].exr"


def _folded():
    return load(str(VECTOR / "folded.c4m"))


def _sizes(m):
    return {e.name: e.size for e in m.entries}


def test_folded_sizes():
    m = _folded()
    assert _sizes(m) == {"sub/": 446, RANGE: 303}
    rng = next(e for e in m.entries if e.name == RANGE)
    assert rng.is_sequence
    # 33 bytes of frames plus a 3 x 90-byte list, and that list is inline.
    assert len(m._range_data[rng.c4id]) == 3 * 90
    assert rng.size - 3 * 90 == 33


def test_expanded_sizes():
    m = load(str(VECTOR / "expanded.c4m"))
    assert _sizes(m) == {
        "sub/": 446,
        "f.001.exr": NULL_SIZE,
        "f.002.exr": NULL_SIZE,
        "f.003.exr": NULL_SIZE,
        RANGE: 303,
    }


def test_normaliser_derives_the_directory_size_over_the_range():
    m = _folded()
    want = m.compute_c4id()
    m.entries[0].size = NULL_SIZE  # sub/
    _propagate_metadata(m.entries)
    assert m.entries[0].size == 446  # 303 + 143, its folded listing's bytes

    stated_null = _folded()
    stated_null.entries[0].size = NULL_SIZE
    assert stated_null.compute_c4id() == want


def test_normalising_folded_changes_nothing():
    m = _folded()
    _propagate_metadata(m.entries)
    assert dumps(m) == (VECTOR / "folded.c4m").read_text()


def test_a_null_range_size_makes_the_directory_null():
    m = _folded()
    m.entries[0].size = NULL_SIZE
    m.entries[1].size = NULL_SIZE
    _propagate_metadata(m.entries)
    assert m.entries[0].has_null_size()
