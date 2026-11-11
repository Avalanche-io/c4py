"""Cross-implementation name-escape vector (c4 core, c4m/testdata/name-escape-vector).

The files under vectors/name-escape-vector/ are byte-identical copies. In a
written name a space is "\\ ", a double quote is '\\"', and a control byte is
encoded by SafeName; a raw one is damage and is refused, naming the line.
Through 1.0.26 c4py read each into the name ("real.txt junk" became one
name). Any consistent indent unit reads.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.entry import Entry
from c4py.id import identify_bytes
from c4py.manifest import Manifest

VECTOR = Path(__file__).parent / "vectors" / "name-escape-vector"
DAMAGED = [
    "file-unescaped-space",
    "file-damaged-id-field",
    "file-raw-quote",
    "file-raw-tab",
    "file-raw-control",
    "dir-unescaped-space",
]


def _read(name: str) -> str:
    return (VECTOR / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("case", DAMAGED)
def test_each_damaged_name_is_refused_naming_the_line(case: str) -> None:
    with pytest.raises(ValueError, match="line 1"):
        loads(_read(case + ".c4m"))


def test_ok_files_decode() -> None:
    assert len(loads(_read("ok-file.c4m")).entries) == 1
    assert len(loads(_read("ok-dir.c4m")).entries) == 1
    assert [e.name for e in loads(_read("ok-escaped.c4m")).entries] == ['my file "q"\t.txt']
    tree = loads(_read("ok-indent-3.c4m"))
    assert [(e.name, e.depth) for e in tree.entries] == [("d/", 0), ("x.txt", 1)]


def test_sequence_flag_without_range_is_escaped() -> None:
    """The one writer path that could leave a space, quote or control byte raw."""
    name = 'bad seq "x"\x01[a]'
    e = Entry(name=name, mode=0o644, timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc), size=1,
              c4id=identify_bytes(b"x"), is_sequence=True)
    text = dumps(Manifest([e]))
    assert "bad seq" not in text and "\x01" not in text
    assert all(text[i - 1] == "\\" for i, ch in enumerate(text) if ch == '"')
    assert [x.name for x in loads(text).entries] == [name]
