"""A pretty (aligned) line whose C4 ID column is "-" must not read its
padding into the name.

Through 1.0.24 the decoder tested a single space for the field boundary,
so `a.txt` in a names-only pretty file decoded as `a.txt` plus 35 spaces,
and Workspace.checkout of such a file onto its own tree removed every
real file and left empty padded-name decoys (measured on the released
1.0.24 package, 2026-11-09). The fixture is a real file written by c4
1.0.21's pretty encoder.
"""

from pathlib import Path

import c4py

LEGACY = Path(__file__).parent / "vectors" / "legacy" / "ten-dash-null-1.0.21.c4m"


def test_pretty_null_id_names_carry_no_padding() -> None:
    m = c4py.load(str(LEGACY))
    names = [e.name for e in m.entries]
    assert names == ["a.txt", "d/", "b.txt"], names
    for name in names:
        assert not name.endswith(" ")


def test_checkout_of_names_only_pretty_file_removes_nothing(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    (tree / "d").mkdir(parents=True)
    (tree / "a.txt").write_text("A\n")
    (tree / "d" / "b.txt").write_text("B\n")

    ws = c4py.Workspace(tree)
    ws.checkout(c4py.load(str(LEGACY)))

    assert (tree / "a.txt").read_text() == "A\n"
    assert (tree / "d" / "b.txt").read_text() == "B\n"
    leftovers = sorted(p.name for p in tree.rglob("*") if p.name.endswith(" "))
    assert leftovers == [], leftovers
