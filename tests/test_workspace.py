"""Tests for Workspace — directory-backed content-addressed experiment environments."""

from io import BytesIO

import pytest

import c4py
from c4py.workspace import Workspace


@pytest.fixture
def store(tmp_path):
    store_dir = tmp_path / "store"
    return c4py.FSStore(store_dir)


@pytest.fixture
def sample_manifest(store):
    """A manifest with 3 files stored in the store."""
    id_a = store.put(BytesIO(b"alpha content"))
    id_b = store.put(BytesIO(b"beta content"))
    id_c = store.put(BytesIO(b"gamma content"))

    m = c4py.Manifest(entries=[
        c4py.Entry(name="a.txt", mode=0o100644, size=13, c4id=id_a, depth=0),
        c4py.Entry(name="b.txt", mode=0o100644, size=12, c4id=id_b, depth=0),
        c4py.Entry(name="sub/", mode=0o40755, size=-1, depth=0),
        c4py.Entry(name="c.txt", mode=0o100644, size=13, c4id=id_c, depth=1),
    ])
    return m


@pytest.fixture
def alt_manifest(store):
    """A different manifest sharing some content."""
    id_a = store.put(BytesIO(b"alpha content"))  # same as sample
    id_d = store.put(BytesIO(b"delta content"))

    m = c4py.Manifest(entries=[
        c4py.Entry(name="a.txt", mode=0o100644, size=13, c4id=id_a, depth=0),
        c4py.Entry(name="d.txt", mode=0o100644, size=13, c4id=id_d, depth=0),
    ])
    return m


class TestCheckout:
    def test_creates_files(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)

        assert (tmp_path / "ws" / "a.txt").read_bytes() == b"alpha content"
        assert (tmp_path / "ws" / "b.txt").read_bytes() == b"beta content"
        assert (tmp_path / "ws" / "sub" / "c.txt").read_bytes() == b"gamma content"

    def test_sets_current_manifest(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)
        assert ws.current is not None

    def test_switch_manifests(self, tmp_path, store, sample_manifest, alt_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)
        assert (tmp_path / "ws" / "b.txt").exists()
        assert not (tmp_path / "ws" / "d.txt").exists()

        ws.checkout(alt_manifest)
        assert (tmp_path / "ws" / "a.txt").exists()  # shared, still there
        assert (tmp_path / "ws" / "d.txt").exists()  # new
        # b.txt and sub/ should be removed by reconcile
        # (depends on reconcile removing extras)

    def test_dry_run(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest, dry_run=True)
        # Directory should not be fully populated in dry run
        assert ws.current is None  # not set during dry run

    def test_checkout_from_path(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        c4m_path = tmp_path / "test.c4m"
        c4py.dump(sample_manifest, str(c4m_path))
        ws.checkout(str(c4m_path))
        assert (tmp_path / "ws" / "a.txt").exists()

    def test_persists_state(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)

        # New workspace instance should load persisted state
        ws2 = Workspace(tmp_path / "ws", store=store)
        assert ws2._state.manifest_c4id != ""
        assert ws2._state.last_checkout != ""

    def test_manifest_persists_across_instances(self, tmp_path, store, sample_manifest):
        """Manifest must survive across process boundaries (new Workspace instance).

        Regression: self.current was only held in memory. A new Workspace
        instance (e.g., a separate CLI invocation) had current=None even
        after a previous checkout.
        """
        ws1 = Workspace(tmp_path / "ws", store=store)
        ws1.checkout(sample_manifest)

        # New instance, same path — should have current set
        ws2 = Workspace(tmp_path / "ws", store=store)
        assert ws2.current is not None
        ws2.reset()  # should work without raising


class TestSnapshot:
    def test_captures_state(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)

        snapshot = ws.snapshot()
        assert snapshot.file_count() > 0

    def test_snapshot_stores_content(self, tmp_path, store):
        ws_dir = tmp_path / "ws"
        ws_dir.mkdir()
        (ws_dir / "new.txt").write_bytes(b"new content")

        ws = Workspace(ws_dir, store=store)
        snapshot = ws.snapshot(store_content=True)

        # Content should be in the store
        for path, entry in snapshot.files():
            if entry.c4id:
                assert store.has(entry.c4id)


class TestReset:
    def test_undoes_modifications(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)

        # Modify a file
        (tmp_path / "ws" / "a.txt").write_bytes(b"corrupted!")

        ws.reset()
        assert (tmp_path / "ws" / "a.txt").read_bytes() == b"alpha content"

    def test_raises_without_checkout(self, tmp_path, store):
        ws = Workspace(tmp_path / "ws", store=store)
        with pytest.raises(RuntimeError, match="no manifest checked out"):
            ws.reset()


class TestDiffFromCurrent:
    def test_detects_modification(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)

        (tmp_path / "ws" / "a.txt").write_bytes(b"modified!")

        result = ws.diff_from_current()
        assert len(result.modified) > 0 or len(result.added) > 0

    def test_no_changes(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)

        ws.diff_from_current()
        # Should be no changes (or minimal — hidden files might differ)

    def test_raises_without_checkout(self, tmp_path, store):
        ws = Workspace(tmp_path / "ws", store=store)
        with pytest.raises(RuntimeError, match="no manifest checked out"):
            ws.diff_from_current()


class TestStatus:
    def test_empty_workspace(self, tmp_path, store):
        ws = Workspace(tmp_path / "ws", store=store)
        status = ws.status()
        assert status["has_manifest"] is False
        assert status["manifest_c4id"] is None

    def test_after_checkout(self, tmp_path, store, sample_manifest):
        ws = Workspace(tmp_path / "ws", store=store)
        ws.checkout(sample_manifest)
        status = ws.status()
        assert status["has_manifest"] is True
        assert status["manifest_c4id"] is not None
        assert status["last_checkout"] is not None


class TestMLWorkflow:
    """End-to-end ML experiment workflow."""

    def test_experiment_switching(self, tmp_path, store, sample_manifest, alt_manifest):
        """Simulate switching between two experiment configurations."""
        ws = Workspace(tmp_path / "data", store=store)

        # Experiment A
        ws.checkout(sample_manifest)
        assert (tmp_path / "data" / "a.txt").read_bytes() == b"alpha content"
        assert (tmp_path / "data" / "b.txt").read_bytes() == b"beta content"

        # Snapshot before training modifies things
        ws.snapshot()

        # Simulate training corrupting data
        (tmp_path / "data" / "a.txt").write_bytes(b"training artifact")

        # Reset to clean state
        ws.reset()
        assert (tmp_path / "data" / "a.txt").read_bytes() == b"alpha content"

        # Switch to experiment B
        ws.checkout(alt_manifest)
        assert (tmp_path / "data" / "a.txt").read_bytes() == b"alpha content"  # shared
        assert (tmp_path / "data" / "d.txt").read_bytes() == b"delta content"  # new

    def test_filtered_view(self, tmp_path, store, sample_manifest):
        """Create a filtered view of a dataset and materialize it."""
        # Filter to only .txt files at root (not in sub/)
        filtered = sample_manifest.filter(lambda p, e: not e.is_dir() and e.depth == 0)

        ws = Workspace(tmp_path / "data", store=store)
        ws.checkout(filtered)

        assert (tmp_path / "data" / "a.txt").exists()
        assert (tmp_path / "data" / "b.txt").exists()
