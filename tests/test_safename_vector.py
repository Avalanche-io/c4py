"""Cross-implementation SafeName vector (c4 core, c4m/testdata/safename-vector).

The files under vectors/safename-vector/ are byte-identical copies. A name
holding bytes that are not UTF-8 reaches Python surrogate-escaped (each such
byte as U+DC80..U+DCFF, as os.fsdecode gives it). Through 1.0.26 c4py's
writer and compute_c4id raised UnicodeEncodeError on such a name, so a tree
holding one could be read but not written or identified. Tier 3 decides the
bytes: they are braille between ¤, exactly as Go writes them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.entry import format_name

VECTOR = Path(__file__).parent / "vectors" / "safename-vector"
INVALID_UTF8 = ["invalid-ff", "lone-continuation", "overlong-nul", "surrogate"]


def _cases() -> dict[str, tuple[bytes, str]]:
    out = {}
    for ln in (VECTOR / "cases.txt").read_text(encoding="utf-8").split("\n"):
        if ln:
            label, hx, field = ln.split(" ", 2)
            out[label] = (bytes.fromhex(hx), field)
    return out


@pytest.mark.parametrize("label", INVALID_UTF8)
def test_invalid_utf8_name_is_written_as_go_writes_it(label: str) -> None:
    raw, field = _cases()[label]
    assert format_name(raw.decode("utf-8", "surrogateescape")) == field


def test_a_tree_holding_invalid_utf8_names_is_identified_and_round_trips() -> None:
    m = loads((VECTOR / "names.c4m").read_text(encoding="utf-8"))
    names = {e.name.encode("utf-8", "surrogateescape") for e in m.entries}
    for label in INVALID_UTF8:
        assert _cases()[label][0] in names, label
    m.compute_c4id()  # raised UnicodeEncodeError before
    back = loads(dumps(m))
    assert [e.name for e in back.entries] == [e.name for e in m.entries]
    assert back.compute_c4id() == m.compute_c4id()


def test_vector_root_id_and_canonical_text() -> None:
    text = (VECTOR / "names.c4m").read_text(encoding="utf-8")
    m = loads(text)
    assert str(m.compute_c4id()) == (VECTOR / "root-id.txt").read_text(encoding="ascii").strip()
    assert dumps(m) == text


def test_every_name_is_written_as_go_writes_it() -> None:
    # Held until 2026-11-13: Tier 1 asked unicodedata, which passed the
    # non-ASCII Zs spaces raw. It is now the pinned Unicode 17.0.0 table.
    for label, (raw, field) in _cases().items():
        assert format_name(raw.decode("utf-8", "surrogateescape")) == field, label
