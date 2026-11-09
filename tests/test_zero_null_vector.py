"""Cross-implementation zero-against-null conformance (2026-11-09).

The fixtures under ``tests/vectors/zero-null-vector/`` and
``tests/vectors/legacy/`` are byte-identical copies of the Go reference
repo's ``c4m/testdata/zero-null-vector/`` and ``c4m/testdata/legacy/``.
Every c4m implementation MUST pass (SPECIFICATION.md "Mode" and "Null and
Zero Values"; C4M-STANDARD §4.1):

- a null mode is a single ``-``; the known zero is ten dashes; both
  encoders write each as itself and never rewrite one as the other
- ``d---------`` is a known directory mode (zero permissions), not null
- ten dashes on a directory line is the one legacy null (pretty encoders
  through c4 1.0.21 wrote every null that way)
- a childless directory with no C4 ID keeps a null size
"""

from __future__ import annotations

import os
import stat
import time
from pathlib import Path

import pytest

from c4py.decoder import load, loads
from c4py.encoder import dumps
from c4py.entry import NULL_SIZE, ZERO_MODE, Entry
from c4py.id import identify_bytes
from c4py.manifest import Manifest
from c4py.reconcile import reconcile
from c4py.scanner import scan
from c4py.store import FSStore

VECTOR = Path(__file__).parent / "vectors" / "zero-null-vector"
LEGACY = Path(__file__).parent / "vectors" / "legacy" / "ten-dash-null-1.0.21.c4m"
EMPTY_ID = identify_bytes(b"")


def _root_id() -> str:
    return (VECTOR / "root-id.txt").read_text(encoding="utf-8").strip()


def _by_path(m: Manifest) -> dict[str, Entry]:
    return dict(m.flat_entries())


def _check_vector_manifest(m: Manifest) -> None:
    """The per-entry contract from the vector README."""
    e = _by_path(m)
    assert set(e) == {
        "zero.txt", "nullmode.txt", "empty.txt", "emptydir", "locked",
        "locked/inside.txt", "unread",
    }

    assert e["zero.txt"].mode == ZERO_MODE
    assert not e["zero.txt"].has_null_mode()
    assert e["zero.txt"].size == 4
    assert e["zero.txt"].c4id is not None

    assert e["nullmode.txt"].mode == 0
    assert e["nullmode.txt"].has_null_mode()
    assert e["nullmode.txt"].size == 4
    assert e["nullmode.txt"].c4id is not None

    assert e["empty.txt"].mode == 0o644
    assert e["empty.txt"].size == 0
    assert e["empty.txt"].c4id == EMPTY_ID

    assert e["emptydir"].mode == stat.S_IFDIR | 0o755
    assert e["emptydir"].size == 0
    assert e["emptydir"].c4id == EMPTY_ID

    assert e["locked"].mode == stat.S_IFDIR, "d--------- is known, not null"
    assert not e["locked"].has_null_mode()
    assert e["locked"].size == 142
    assert e["locked"].c4id is not None
    assert e["locked/inside.txt"].mode == 0o644
    assert e["locked/inside.txt"].size == 6
    assert e["locked/inside.txt"].c4id is not None

    assert e["unread"].mode == stat.S_IFDIR
    assert not e["unread"].has_null_mode()
    assert e["unread"].size == NULL_SIZE
    assert e["unread"].c4id is None

    assert str(m.compute_c4id()) == _root_id()


@pytest.fixture
def utc(monkeypatch):
    """Pretty form renders local time; the Go fixture was rendered in UTC."""
    monkeypatch.setenv("TZ", "UTC")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


class TestZeroNullVector:
    def test_canonical_decodes_to_contract(self):
        _check_vector_manifest(load(str(VECTOR / "canonical.c4m")))

    def test_pretty_decodes_to_contract(self):
        _check_vector_manifest(load(str(VECTOR / "pretty.c4m")))

    def test_both_forms_are_one_manifest(self):
        a = load(str(VECTOR / "canonical.c4m"))
        b = load(str(VECTOR / "pretty.c4m"))
        assert dumps(a) == dumps(b)
        assert a.compute_c4id() == b.compute_c4id()

    @pytest.mark.parametrize("source", ["canonical.c4m", "pretty.c4m"])
    def test_canonical_reencode_is_byte_identical(self, source):
        m = load(str(VECTOR / source))
        assert dumps(m) == (VECTOR / "canonical.c4m").read_text(encoding="utf-8")

    @pytest.mark.parametrize("source", ["canonical.c4m", "pretty.c4m"])
    def test_pretty_reencode_is_byte_identical(self, source, utc):
        m = load(str(VECTOR / source))
        assert dumps(m, pretty=True) == (VECTOR / "pretty.c4m").read_text(encoding="utf-8")

    def test_null_size_survives_compute(self):
        """compute_c4id must not fill unread/'s null size with 0."""
        m = load(str(VECTOR / "canonical.c4m"))
        m.compute_c4id()
        assert _by_path(m)["unread"].size == NULL_SIZE

    def test_round_trip_through_loads(self, utc):
        m = load(str(VECTOR / "canonical.c4m"))
        for pretty in (False, True):
            again = loads(dumps(m, pretty=pretty))
            _check_vector_manifest(again)


class TestPrettyTimestampZone:
    """The pretty fixture says UTC; the reader's zone must not move it."""

    @pytest.fixture
    def los_angeles(self, monkeypatch):
        monkeypatch.setenv("TZ", "America/Los_Angeles")
        time.tzset()
        yield
        monkeypatch.undo()
        time.tzset()

    def test_pretty_utc_fixture_decodes_in_another_zone(self, los_angeles):
        _check_vector_manifest(load(str(VECTOR / "pretty.c4m")))

    def test_local_zone_pretty_output_round_trips(self, los_angeles):
        m = load(str(VECTOR / "canonical.c4m"))
        pretty = dumps(m, pretty=True)
        assert " PST " in pretty or " PDT " in pretty
        again = loads(pretty)
        assert again.compute_c4id() == m.compute_c4id()
        assert dumps(again) == dumps(m)


class TestLegacyTenDash:
    """A real pretty-form file written by c4 1.0.21, whose encoder wrote
    every null mode as ten dashes."""

    def test_directories_decode_null_files_decode_zero(self):
        m = load(str(LEGACY))
        e = _by_path(m)
        assert set(e) == {"a.txt", "d", "d/b.txt"}, "padding is not part of a name"
        for path, entry in e.items():
            if entry.is_dir():
                assert entry.mode == 0, f"{path}: ten dashes on a directory is a legacy null"
                assert entry.has_null_mode()
            else:
                assert entry.mode == ZERO_MODE, f"{path}: ten dashes on a file is the known zero"
            assert entry.has_null_timestamp()
            assert entry.has_null_size()
            assert entry.c4id is None

    def test_reencode_never_writes_null_as_ten_dashes(self, utc):
        m = load(str(LEGACY))
        canonical = dumps(m)
        assert canonical == (
            "---------- - - a.txt -\n"
            "- - - d/ -\n"
            "  ---------- - - b.txt -\n"
        )
        pretty = dumps(m, pretty=True)
        dir_line = next(ln for ln in pretty.splitlines() if " d/" in ln)
        assert dir_line.startswith("- "), dir_line
        assert "---------- " not in dir_line
        assert loads(pretty).compute_c4id() == m.compute_c4id()


class TestScanRecordsKnownZero:
    """A scan reads real modes: chmod 000 is the known zero, never null."""

    @pytest.fixture
    def tree(self, tmp_path: Path):
        root = tmp_path / "tree"
        root.mkdir()
        (root / "zero.txt").write_bytes(b"zero")
        (root / "plain.txt").write_bytes(b"plain")
        locked = root / "locked"
        locked.mkdir()
        (locked / "inside.txt").write_bytes(b"inside")
        (root / "zero.txt").chmod(0)
        locked.chmod(0)
        try:
            yield root
        finally:
            locked.chmod(0o755)
            (root / "zero.txt").chmod(0o644)

    def test_chmod_000_file_is_zero_mode(self, tree: Path):
        m = scan(tree, compute_ids=False)
        e = _by_path(m)
        assert e["zero.txt"].mode == ZERO_MODE
        assert not e["zero.txt"].has_null_mode()
        assert e["zero.txt"].canonical().startswith("---------- ")
        assert e["plain.txt"].mode == 0o644

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can read a chmod 000 directory")
    def test_unreadable_directory_keeps_null_size_and_id(self, tree: Path):
        m = scan(tree)
        e = _by_path(m)
        assert "locked/inside.txt" not in e, "nobody could list locked/"
        locked = e["locked"]
        assert locked.mode == stat.S_IFDIR, "d--------- is known"
        assert locked.size == NULL_SIZE
        assert locked.c4id is None
        assert locked.canonical() == "d--------- - - locked/ -"
        # The unread directory makes the tree's size null, not wrong.
        text = dumps(m)
        assert "d--------- - - locked/ -" in text


class TestReconcileAppliesModes:
    """Known zero applies as 0000; a null mode takes the platform default."""

    @pytest.fixture
    def built(self, tmp_path: Path):
        src = tmp_path / "src"
        src.mkdir()
        (src / "zero.txt").write_bytes(b"zero")
        (src / "nullmode.txt").write_bytes(b"null")
        (src / "locked").mkdir()
        (src / "locked" / "inside.txt").write_bytes(b"inside")
        store = FSStore(tmp_path / "store")
        m = scan(src, store=store)
        e = _by_path(m)
        e["zero.txt"].mode = ZERO_MODE
        e["nullmode.txt"].mode = 0
        e["locked"].mode = stat.S_IFDIR  # d---------
        target = tmp_path / "target"
        target.mkdir()
        try:
            yield m, store, target
        finally:
            for p in (target / "locked", target / "zero.txt"):
                if p.exists():
                    p.chmod(0o755)

    def test_zero_mode_chmods_to_0000(self, built):
        m, store, target = built
        result = reconcile(m, target, store=store)
        assert result.errors == []
        assert stat.S_IMODE(os.stat(target / "zero.txt").st_mode) == 0

    def test_null_mode_takes_platform_default(self, built):
        m, store, target = built
        reconcile(m, target, store=store)
        perms = stat.S_IMODE(os.stat(target / "nullmode.txt").st_mode)
        assert perms != 0
        assert perms == 0o666 & ~_umask()

    def test_locked_directory_is_filled_then_chmodded(self, built):
        m, store, target = built
        result = reconcile(m, target, store=store)
        assert result.errors == []
        assert stat.S_IMODE(os.stat(target / "locked").st_mode) == 0
        (target / "locked").chmod(0o755)
        assert (target / "locked" / "inside.txt").read_bytes() == b"inside"


def _umask() -> int:
    current = os.umask(0)
    os.umask(current)
    return current
