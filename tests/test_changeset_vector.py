"""A changeset, as ``c4 diff`` writes it, and the chain ``c4 diff >>`` makes.

The shared changeset vector (c4 core ``c4m/testdata/changeset-vector/``,
c4 #175), vendored byte for byte from c4 commit 0b410a2. Its MUST rule:
every patch writer, given ``base.c4m`` and ``v2.c4m``, writes
``changeset.c4m`` byte for byte, and every chain reader resolves
``chain.c4m`` to ``v2.c4m``.

A patch section is canonical c4m text: "files before directories, then
natural sort within each group" (SPECIFICATION.md). Through 2026-11-16
c4py's ``patch_diff`` emitted its entries in sorted path order, so within
``sub/`` the directory ``deep/`` came before the files ``gone.txt`` and
``new.txt``. Core 1.0.27 and c4py both read that order back to the same
state, but the bytes, and so the changeset's C4 ID, were not core's.
"""

from __future__ import annotations

from pathlib import Path

from c4py.decoder import load, loads
from c4py.diff import apply_patch, patch_diff
from c4py.encoder import dumps

VECTOR = Path(__file__).parent / "vectors" / "changeset-vector"


def _text(name: str) -> str:
    return (VECTOR / name).read_bytes().decode("utf-8")


def test_patch_diff_writes_changeset_byte_for_byte() -> None:
    got = patch_diff(load(str(VECTOR / "base.c4m")), load(str(VECTOR / "v2.c4m")))
    assert got == _text("changeset.c4m")


def test_changeset_never_ends_with_a_bare_id() -> None:
    got = patch_diff(load(str(VECTOR / "base.c4m")), load(str(VECTOR / "v2.c4m")))
    last = got.rstrip("\n").split("\n")[-1]
    assert not (len(last) == 90 and last.startswith("c4"))


def test_chain_resolves_to_v2() -> None:
    m = load(str(VECTOR / "chain.c4m"))
    assert "sub/gone.txt" not in [p for p, _ in m.flat_entries()]
    assert dumps(m) == _text("v2.c4m")


def test_patch_diff_appended_to_base_resolves_to_v2() -> None:
    base, v2 = load(str(VECTOR / "base.c4m")), load(str(VECTOR / "v2.c4m"))
    assert dumps(loads(_text("base.c4m") + patch_diff(base, v2))) == _text("v2.c4m")


def test_apply_changeset_patch_to_base_is_v2() -> None:
    patch = loads(_text("changeset.c4m").split("\n", 1)[1])
    assert dumps(apply_patch(load(str(VECTOR / "base.c4m")), patch)) == _text("v2.c4m")
