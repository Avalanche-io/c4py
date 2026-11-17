"""Cross-implementation conformance: a time inside the epoch second
(vectors/epoch-second-vector, byte-identical to the Go reference's
c4m/testdata/epoch-second-vector; README there).

A c4m line carries the second a time falls in, and the epoch is null, so a
time that floors to the epoch second is null and its line is ``-``. c4py's
scan already floored at capture; an entry a caller built with such a time
was written ``1970-01-01T00:00:00Z``, which reads back as null, so the
manifest's C4 ID differed from the ID of its own text.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.entry import NULL_TIMESTAMP, Entry, is_null_timestamp
from c4py.manifest import Manifest
from c4py.scanner import scan
from tests.vector_tree import build_tree

VECTOR = Path(__file__).parent / "vectors" / "epoch-second-vector"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _at(ns: int) -> datetime:
    """The instant ns after the epoch, to the microsecond, floored."""
    return EPOCH + timedelta(microseconds=ns // 1000)


def _field(text: str, name: str) -> str:
    for line in text.splitlines():
        f = line.split()
        if f[3] == name:
            return f[1]
    raise KeyError(name)


@pytest.mark.parametrize(
    "ns, null",
    [
        (0, True),
        (1_000, True),
        (500_000_000, True),
        (999_999_000, True),
        (1_000_000_000, False),
        (-1_000, False),
        (-500_000_000, False),
        (1_750_000_000_000_000_000, False),
    ],
)
def test_a_built_entry_is_null_at_the_second(ns: int, null: bool):
    """An entry a caller builds in memory is null exactly when its time
    floors to the epoch second: its line is "-" and the manifest has the
    C4 ID of its own text (README rule 1)."""
    e = Entry(name="f.txt", mode=0o100644, timestamp=_at(ns), size=1)
    assert is_null_timestamp(e.timestamp) is null
    assert e.has_null_timestamp() is null
    m = Manifest()
    m.entries.append(e)
    text = dumps(m)
    assert (_field(text, "f.txt") == "-") is null
    assert m.compute_c4id() == loads(text).compute_c4id()
    # The pretty form spells the same null and reads back to the same ID.
    pretty = dumps(m, pretty=True)
    assert (pretty.split()[1] == "-") is null
    assert loads(pretty).compute_c4id() == m.compute_c4id()


def test_the_scan_is_saved_c4m_and_one_description_id(tmp_path: Path):
    """Scanning the vector's tree writes half.txt and sub/in.txt as "-", the
    scan's ID is its own text's, and on POSIX the text is saved.c4m and the
    ID is root-id.txt (README rules 1 and 2)."""
    root = tmp_path / "tree"
    root.mkdir()
    build_tree(VECTOR, root)
    m = scan(root)
    text = dumps(m)
    assert _field(text, "half.txt") == "-"
    assert _field(text, "in.txt") == "-"
    assert m.compute_c4id() == loads(text).compute_c4id()
    if sys.platform == "win32":
        return  # saved.c4m carries POSIX modes
    assert text == (VECTOR / "saved.c4m").read_text(encoding="utf-8")
    assert str(m.compute_c4id()) == (VECTOR / "root-id.txt").read_text(encoding="utf-8").strip()


def test_saved_c4m_resolves_to_root_id_and_re_encodes_to_itself():
    saved = (VECTOR / "saved.c4m").read_text(encoding="utf-8")
    m = loads(saved)
    assert dumps(m) == saved
    assert str(m.compute_c4id()) == (VECTOR / "root-id.txt").read_text(encoding="utf-8").strip()


def test_the_epoch_itself_is_still_the_null_sentinel():
    assert is_null_timestamp(NULL_TIMESTAMP)


def test_a_directory_inside_the_epoch_second_is_filled_from_its_children():
    """A directory timed inside the epoch second is null, so its time is
    filled from its children exactly as a null directory's is."""

    def build(dir_time: datetime) -> Manifest:
        m = Manifest()
        m.entries.append(Entry(name="sub/", mode=0o40755, timestamp=dir_time, size=-1, depth=0))
        m.entries.append(Entry(name="a.txt", mode=0o100644, timestamp=_at(1_750_000_000_000_000_000), size=1, depth=1))
        return m

    inside, null_dir = build(_at(500_000_000)), build(NULL_TIMESTAMP)
    assert inside.compute_c4id() == null_dir.compute_c4id()
    assert loads(dumps(inside)).compute_c4id() == inside.compute_c4id()
