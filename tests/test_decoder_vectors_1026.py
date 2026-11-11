"""Cross-implementation conformance for the 1.0.26 decoder rules.

The fixtures under ``tests/vectors/link-target-vector/``,
``tests/vectors/backslash-name-vector/`` and
``tests/vectors/damaged-line-vector/`` are byte-identical copies of the Go
reference repo's ``c4m/testdata/`` vectors of the same names; each README
states what every implementation MUST do.

- After a link operator, one field that is ``-`` or a C4 ID is an ungrouped
  hard link's ID field; two fields are a target and the ID field, even when
  the target starts with ``c4`` (under a null mode, as views carry links).
- The pretty form's padding before the C4 ID field is not part of a target.
- A backslash always starts a two-byte escape, so a name or target ending
  in a backslash reads back as itself.
- A damaged field is an error naming the line, never a value: a size token
  ends at a space, and each mode position holds only its own letter or
  ``-`` (plus s/S, t/T at the execute positions).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from c4py.decoder import loads

VECTORS = Path(__file__).parent / "vectors"
ID_TEXT = "c43312d4NsKWQvybWGtgLk6iaWeuHUjz6PzjYgD6bNgqkqk4u6ECWjD2v88R1eRBPw3nNQnG6QMrmofSZcKfmMmJpy"
CONTENT = "c45WurN5j5WeFqxZMB8ZY9p4D2Ks9rNTEmbXFihjWmCJjDM66pNmRYvW41hGt1Bzj8iJ8WsqyXwNmH7VAJSjvwV91v"

LINK_TARGET = {
    "id-null": (ID_TEXT, 0, None),
    "id-set": (ID_TEXT, 0, CONTENT),
    "id-exr-null": (ID_TEXT + ".exr", 0, None),
    "id-exr-set": (ID_TEXT + ".exr", 0, CONTENT),
    "c4-prefix-null": ("c4studio", 0, None),
    "objects-null": ("../objects/" + ID_TEXT, 0, None),
    "hard-null-id": ("", -1, CONTENT),
    "hard-null-nil": ("", -1, None),
    "epoch-link-nil": ("missing.txt", 0, None),
    "epoch-link-id": ("real.txt", 0, CONTENT),
}

BACKSLASH = {
    "plain.txt": "",
    "trail\\": "",
    "end\\\\": "",
    "a\\ b": "",
    "link": "target\\",
    "dir\\x/": "",
    "in.txt": "",
}


def _root_id(name: str) -> str:
    return (VECTORS / name / "root-id.txt").read_text().strip()


@pytest.mark.parametrize("form", ["canonical.c4m", "pretty.c4m", "patch.c4m"])
def test_link_target_vector(form: str) -> None:
    m = loads((VECTORS / "link-target-vector" / form).read_text())
    got = {e.name: (e.target, e.hard_link, str(e.c4id) if e.c4id else None) for e in m.entries}
    assert got == LINK_TARGET
    if form != "patch.c4m":
        assert str(m.compute_c4id()) == _root_id("link-target-vector")


@pytest.mark.parametrize("form", ["canonical.c4m", "pretty.c4m", "patch.c4m"])
def test_backslash_name_vector(form: str) -> None:
    m = loads((VECTORS / "backslash-name-vector" / form).read_text())
    got = {e.name: e.target for e in m.entries}
    assert got == BACKSLASH
    if form != "patch.c4m":
        assert str(m.compute_c4id()) == _root_id("backslash-name-vector")


def test_damaged_line_vector_ok_decodes() -> None:
    loads((VECTORS / "damaged-line-vector" / "ok.c4m").read_text())


@pytest.mark.parametrize(
    "case",
    sorted(p.stem for p in (VECTORS / "damaged-line-vector").glob("*.c4m") if p.stem != "ok"),
)
def test_damaged_line_vector_refused(case: str) -> None:
    with pytest.raises(ValueError, match="line 1"):
        loads((VECTORS / "damaged-line-vector" / f"{case}.c4m").read_text())
