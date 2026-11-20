"""A description holding a folded range is refused before anything is written.

c4py has no range expander, so it can't write a folded range (one line such
as ``frame.[0001-0003].exr`` standing for numbered frames) back to files.
Through this change ``reconcile`` wrote the range line as one file under the
range's name, holding the ID list's bytes, and reported it created. The
fixture (``vectors/folded-range-refusal``, made by the Go c4 CLI) holds a
range beside a file, one nested in a subdirectory, two in one folder, and a
stored listing whose range carries no inline list. The same trees unfolded
still round-trip.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from c4py import Workspace, load, parse, reconcile, scan
from c4py.store import FSStore

VECTOR = Path(__file__).parent / "vectors" / "folded-range-refusal"
FOLDED = ["A-folded.c4m", "B-folded.c4m", "C-folded.c4m", "B-s010.c4m"]
RANGES = {
    "A-folded.c4m": ["frame.[0001-0003].exr"],
    "B-folded.c4m": ["shots/s010/img.[0001-0003].dpx"],
    "C-folded.c4m": ["a.[0001-0003].exr", "b.[0001-0003].exr"],
    "B-s010.c4m": ["img.[0001-0003].dpx"],
}


def _plain_ids() -> dict[str, str]:
    ids = {}
    for line in (VECTOR / "plain-ids.txt").read_text().splitlines():
        name, cid = line.split()
        ids[name] = cid
    return ids


def _full_store(tmp_path: Path) -> FSStore:
    """A store holding every tree file and B's ID list: everything a range needs."""
    store = FSStore(tmp_path / "store")
    for f in sorted((VECTOR / "trees").rglob("*")):
        if f.is_file():
            store.put(io.BytesIO(f.read_bytes()))
    store.put(io.BytesIO((VECTOR / "B-s010.list").read_bytes()))
    return store


def _listing(path: Path) -> list[str]:
    return sorted(str(p.relative_to(path)) for p in path.rglob("*"))


@pytest.mark.parametrize("name", FOLDED)
@pytest.mark.parametrize("with_lists", [True, False], ids=["store-has-lists", "empty-store"])
def test_reconcile_refuses_a_folded_range_and_writes_nothing(tmp_path, name, with_lists):
    store = _full_store(tmp_path) if with_lists else FSStore(tmp_path / "empty-store")
    if name == "B-s010.c4m":
        # The stored listing carries no inline list; the list is in the store
        # under the range's C4 ID, or (empty store) nowhere.
        assert not load(str(VECTOR / name))._range_data
        assert store.has(parse(_plain_ids()["range"])) == with_lists
    target = tmp_path / "out"
    target.mkdir()
    with pytest.raises(ValueError, match="can't write a folded range") as err:
        reconcile(load(str(VECTOR / name)), target, store=store)
    for rng in RANGES[name]:
        assert rng in str(err.value)
    assert "nothing was written" in str(err.value)
    assert _listing(target) == []


@pytest.mark.parametrize("name", FOLDED)
def test_dry_run_refuses_too(tmp_path, name):
    target = tmp_path / "out"
    target.mkdir()
    with pytest.raises(ValueError, match="can't write a folded range"):
        reconcile(load(str(VECTOR / name)), target, store=_full_store(tmp_path), dry_run=True)
    assert _listing(target) == []


def test_reconcile_from_a_path_refuses_too(tmp_path):
    target = tmp_path / "out"
    target.mkdir()
    with pytest.raises(ValueError, match="can't write a folded range"):
        reconcile(str(VECTOR / "A-folded.c4m"), target, store=_full_store(tmp_path))
    assert _listing(target) == []


def test_refusal_leaves_an_existing_target_untouched(tmp_path):
    target = tmp_path / "out"
    target.mkdir()
    (target / "keep.txt").write_bytes(b"keep")
    with pytest.raises(ValueError, match="can't write a folded range"):
        reconcile(load(str(VECTOR / "A-folded.c4m")), target, store=_full_store(tmp_path))
    assert _listing(target) == ["keep.txt"]
    assert (target / "keep.txt").read_bytes() == b"keep"


def test_workspace_checkout_refuses_and_saves_no_state(tmp_path):
    target = tmp_path / "ws"
    target.mkdir()
    ws = Workspace(target, store=_full_store(tmp_path))
    before = _listing(target)
    with pytest.raises(ValueError, match="can't write a folded range"):
        ws.checkout(load(str(VECTOR / "B-folded.c4m")))
    assert _listing(target) == before
    assert ws.current is None


@pytest.mark.parametrize("tree", ["A", "B", "C"])
def test_the_same_trees_unfolded_still_round_trip(tmp_path, tree):
    target = tmp_path / "out"
    target.mkdir()
    plain = load(str(VECTOR / f"{tree}-plain.c4m"))
    result = reconcile(plain, target, store=_full_store(tmp_path))
    assert not result.errors
    want = _plain_ids()[tree]
    assert str(plain.compute_c4id()) == want
    assert str(scan(target).compute_c4id()) == want
