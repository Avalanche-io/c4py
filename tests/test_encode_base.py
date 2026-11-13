"""A description that names an external base keeps it through encode and decode.

Through 2026-11-13 the encoder dropped ``Manifest.base``, so ``loads`` then
``dumps`` turned a block on a base into a standalone description of the
block's own entries (c4 core PR #145).
"""

from __future__ import annotations

import os

from c4py.decoder import loads
from c4py.diff import patch_diff
from c4py.encoder import dumps
from c4py.id import identify_bytes
from c4py.scanner import scan


def _one_entry_line(tmp_path) -> str:
    # The entry line comes from a real scan through the port's own encoder,
    # never hand-typed field values.
    tree = tmp_path / "tree"
    tree.mkdir()
    f = tree / "a.txt"
    f.write_bytes(b"a")
    os.utime(f, (1767225600, 1767225600))  # 2026-01-01T00:00:00Z
    line = dumps(scan(str(tree)))
    assert line.count("\n") == 1, line
    return line


def test_encode_keeps_external_base(tmp_path):
    base = identify_bytes(b"prior block")
    entry = _one_entry_line(tmp_path)
    text = str(base) + "\n" + entry

    m = loads(text)
    assert m.base == base

    out = dumps(m)
    assert out == text, "encode must write the base first and change nothing else"

    back = loads(out)
    assert back.base == base
    assert len(back.entries) == 1


def test_pretty_encode_starts_with_base(tmp_path):
    base = identify_bytes(b"prior block")
    m = loads(str(base) + "\n" + _one_entry_line(tmp_path))
    assert dumps(m, pretty=True).startswith(str(base) + "\n")


def test_no_base_no_bare_line(tmp_path):
    base = identify_bytes(b"prior block")
    entry = _one_entry_line(tmp_path)
    m = loads(str(base) + "\n" + entry)
    m.base = None
    assert dumps(m) == entry
    assert not dumps(m, pretty=True).startswith("c4")


def test_encode_does_not_modify_callers_manifest(tmp_path):
    base = identify_bytes(b"prior block")
    m = loads(str(base) + "\n" + _one_entry_line(tmp_path))
    dumps(m)
    dumps(m, pretty=True)
    assert m.base == base


def test_patch_diff_writes_one_first_line(tmp_path):
    # patch_diff writes its own first line (the old state's ID) and builds
    # the entries itself; a base either input carries is never written too.
    base = identify_bytes(b"other")
    old = loads(str(base) + "\n" + _one_entry_line(tmp_path))
    new = old.copy()
    new.entries[0].name = "b.txt"
    out = patch_diff(old, new)
    lines = out.strip("\n").split("\n")
    assert lines[0] == str(old.compute_c4id())
    assert str(base) not in lines
    assert old.base == base
