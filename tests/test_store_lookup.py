"""FSStore reads a store any TreeStore left, by core's lookup rule.

The deepest path first; on a miss, a search from the root down that looks
for the file at every level before descending, through each segment's case
variants (Go's ``locate`` and ``foldFind``, store/treestore.go; c4
design/store-split-race.md). Through 2026-11-16 c4py looked only at the
deepest path, so an object a split in another process, or a split cut
short by a crash, left a level up read as absent: measured against a store
Go had split, with one object moved a level up, ``has`` was False.
"""

from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path

import pytest

from c4py.id import identify_bytes
from c4py.store import ContentNotFound, FSStore, _fold_variants


def _split_store(root: Path, n: int = 40) -> tuple[FSStore, list]:
    """A store split twice (root, then c4/) by its own puts."""
    store = FSStore(root, split_threshold=8)
    ids = [store.put(BytesIO(b"obj-%d\n" % i)) for i in range(n)]
    assert any(d.is_dir() for d in (root / "c4").iterdir()), "c4/ split"
    return store, ids


def _deep_files(root: Path) -> list[Path]:
    return sorted(p for p in (root / "c4").glob("*/*") if p.is_file())


def test_object_a_level_too_high_is_found(tmp_path: Path):
    store, ids = _split_store(tmp_path)
    victim = _deep_files(tmp_path)[0]
    up = tmp_path / "c4" / victim.name
    os.replace(victim, up)

    c4id = next(i for i in ids if str(i) == victim.name)
    assert store.has(c4id)
    with store.get(c4id) as f:
        assert f.read().startswith(b"obj-")
    # Storing the same bytes again finds it and makes no second copy.
    data = up.read_bytes()
    assert store.put(BytesIO(data)) == c4id
    copies = [p for p in tmp_path.rglob(victim.name) if p.is_file()]
    assert copies == [up]


def test_split_cut_short_finds_every_object(tmp_path: Path):
    """A crash mid-split leaves some files in the old leaf and some below;
    every one is found."""
    store, ids = _split_store(tmp_path)
    for p in _deep_files(tmp_path)[::2]:
        os.replace(p, tmp_path / "c4" / p.name)
    for c4id in ids:
        assert store.has(c4id), str(c4id)


def test_object_at_root_after_split_is_found(tmp_path: Path):
    """A writer that resolved the flat root before the first split landed
    there for good."""
    store, ids = _split_store(tmp_path)
    victim = _deep_files(tmp_path)[-1]
    os.replace(victim, tmp_path / victim.name)
    assert store.has(next(i for i in ids if str(i) == victim.name))


def test_shard_named_in_other_case_is_found(tmp_path: Path):
    """A store built on a case-insensitive filesystem files an ID under a
    shard directory named in another case; a case-sensitive one finds it
    through the case variants."""
    store, _ = _split_store(tmp_path)
    data = next(
        b"case-%d\n" % i
        for i in range(10_000)
        if any(c.isalpha() for c in str(identify_bytes(b"case-%d\n" % i))[2:4])
        and not (tmp_path / "c4" / str(identify_bytes(b"case-%d\n" % i))[2:4]).exists()
    )
    c4id = identify_bytes(data)
    seg = str(c4id)[2:4]
    other = tmp_path / "c4" / seg.swapcase()
    other.mkdir(exist_ok=True)
    (other / str(c4id)).write_bytes(data)
    assert store.has(c4id)
    with store.get(c4id) as f:
        assert f.read() == data


def test_absent_object_is_absent(tmp_path: Path):
    store, _ = _split_store(tmp_path)
    missing = identify_bytes(b"never stored")
    assert not store.has(missing)
    with pytest.raises(ContentNotFound):
        store.get(missing)


def test_fold_variants():
    assert _fold_variants("12") == ["12"]
    assert _fold_variants("1j") == ["1j", "1J"]
    assert _fold_variants("aB") == ["aB", "AB", "ab", "Ab"]


# The shared fixture (vectors/split-race-store, byte-identical to the Go
# reference's store/testdata/split-race-store; README there): a store Go's
# TreeStore split at threshold 4, with one object a level too high.
SPLIT_RACE = Path(__file__).parent / "vectors" / "split-race-store"


def _fixture_objects() -> list[tuple[str, bytes]]:
    rows = []
    for line in (SPLIT_RACE / "objects.txt").read_text(encoding="utf-8").splitlines():
        c4id, data = line.split(" ", 1)
        rows.append((c4id, data.encode()))
    return rows


def _tree_state(root: Path) -> list[tuple[str, bytes]]:
    return sorted(
        (str(p.relative_to(root)), p.read_bytes()) for p in root.rglob("*") if p.is_file()
    )


def test_split_race_fixture_reads_every_object():
    from c4py.id import parse

    root = SPLIT_RACE / "store"
    before = _tree_state(root)
    store = FSStore(root)
    misplaced = (SPLIT_RACE / "misplaced.txt").read_text(encoding="utf-8").strip()
    rows = _fixture_objects()
    assert misplaced in {c for c, _ in rows}
    # The shape under test: the object is at c4/<ID> although c4/XX/ exists.
    assert (root / "c4" / misplaced).is_file()
    assert (root / "c4" / misplaced[2:4]).is_dir()
    for c4id, data in rows:
        assert identify_bytes(data) == parse(c4id)
        assert store.has(parse(c4id)), c4id
        with store.get(parse(c4id)) as f:
            assert f.read() == data
    assert _tree_state(root) == before, "a read wrote to the fixture"


def test_split_moves_only_objects(tmp_path: Path):
    """A split moves files whose names are C4 IDs and nothing else, as
    core's does (c4 #171): before, the root's split moved Go's journal to
    jo/journal, a config to co/config and a person's note to RE/."""
    others = {"journal": b"c4journal\n", "config": b"store=x\n", "README.txt": b"hi\n"}
    for name, data in others.items():
        (tmp_path / name).write_bytes(data)
    store, ids = _split_store(tmp_path)
    for name, data in others.items():
        assert (tmp_path / name).read_bytes() == data, name
    for seg in ("jo", "co", "RE"):
        assert not (tmp_path / seg).exists(), seg
    assert all(store.has(i) for i in ids)
    assert not [p for p in tmp_path.iterdir() if p.is_file() and p.name.startswith("c4")]


def test_get_follows_a_file_moved_down_after_it_was_found(tmp_path: Path, monkeypatch):
    """Another process's split can move the file between the lookup and the
    open; get searches again (Go's TreeStore.Open)."""
    store, ids = _split_store(tmp_path)
    c4id = ids[0]
    found = store._locate(c4id)
    assert found is not None
    up = tmp_path / "c4" / found.name
    os.replace(found, up)  # the file is one level up, where a lookup finds it
    real = FSStore._locate
    calls = {"n": 0}

    def locate_then_split(self, i):
        p = real(self, i)
        calls["n"] += 1
        if calls["n"] == 1:
            os.replace(up, found)  # the split moves it down before the open
        return p

    monkeypatch.setattr(FSStore, "_locate", locate_then_split)
    with store.get(c4id) as f:
        assert f.read().startswith(b"obj-")
    assert calls["n"] == 2
