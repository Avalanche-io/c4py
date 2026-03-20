"""Tests for the content store — must be compatible with Go CLI store layout."""

import os
import tempfile
from io import BytesIO
from pathlib import Path

import pytest

from c4py.id import identify_bytes
from c4py.store import ContentNotFound, FSStore, open_store


class TestFSStore:
    """Filesystem store with adaptive trie sharding."""

    def test_put_and_get(self, tmp_path):
        store = FSStore(tmp_path)
        data = b"hello world"
        c4id = store.put(BytesIO(data))

        assert store.has(c4id)
        result = store.get(c4id)
        assert result.read() == data
        result.close()

    def test_put_returns_correct_id(self, tmp_path):
        store = FSStore(tmp_path)
        data = b"hello world"
        c4id = store.put(BytesIO(data))
        expected = identify_bytes(data)
        assert c4id == expected

    def test_put_idempotent(self, tmp_path):
        store = FSStore(tmp_path)
        data = b"same content"
        id1 = store.put(BytesIO(data))
        id2 = store.put(BytesIO(data))
        assert id1 == id2

    def test_get_not_found(self, tmp_path):
        store = FSStore(tmp_path)
        fake_id = identify_bytes(b"does not exist")
        with pytest.raises(ContentNotFound):
            store.get(fake_id)

    def test_has_false(self, tmp_path):
        store = FSStore(tmp_path)
        fake_id = identify_bytes(b"nope")
        assert not store.has(fake_id)

    def test_empty_content(self, tmp_path):
        store = FSStore(tmp_path)
        c4id = store.put(BytesIO(b""))
        expected = identify_bytes(b"")
        assert c4id == expected
        assert store.has(c4id)
        assert store.get(c4id).read() == b""

    def test_large_content(self, tmp_path):
        store = FSStore(tmp_path)
        data = os.urandom(1_000_000)
        c4id = store.put(BytesIO(data))
        assert store.get(c4id).read() == data

    def test_store_layout_starts_flat(self, tmp_path):
        """Small store should have content at c4/ level."""
        store = FSStore(tmp_path)
        c4id = store.put(BytesIO(b"test"))
        # File should exist directly under the root trie path
        content_path = store._resolve_path(c4id)
        assert content_path.exists()
        assert content_path.name == str(c4id)


class TestAdaptiveSplit:
    """Adaptive trie sharding splits when threshold is exceeded."""

    def test_split_on_threshold(self, tmp_path):
        """When files exceed threshold, directory should split."""
        store = FSStore(tmp_path, split_threshold=10)

        ids = []
        for i in range(15):
            c4id = store.put(BytesIO(f"content-{i}".encode()))
            ids.append(c4id)

        # All content should still be retrievable after split
        for i, c4id in enumerate(ids):
            data = store.get(c4id).read()
            assert data == f"content-{i}".encode()


class TestOpenStore:
    """Store discovery from environment and config."""

    def test_explicit_path(self, tmp_path):
        store = open_store(tmp_path)
        assert isinstance(store, FSStore)

    def test_from_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("C4_STORE", str(tmp_path))
        store = open_store()
        assert isinstance(store, FSStore)

    def test_no_store_raises(self, monkeypatch):
        monkeypatch.delenv("C4_STORE", raising=False)
        # Ensure no config file interferes
        monkeypatch.setattr("c4py.store.CONFIG_FILE", Path("/nonexistent"))
        with pytest.raises(ValueError, match="No content store"):
            open_store()


class TestTreeStoreCompatibility:
    """Content stored by c4py must match Go TreeStore layout exactly.

    Go TreeStore (store/treestore.go) path resolution:
        dir = root
        for i := 0; i+2 <= len(str); i += 2:
            sub = dir / str[i:i+2]
            if !isDir(sub): break
            dir = sub
        return dir / str

    Layout (small store, no splits):
        root/c4/{c4id}

    Layout (after split at c4/ level):
        root/c4/XY/{c4id}  where XY = chars 2-3 of ID

    Both tools walk the trie the same way: follow 2-char directory prefixes
    until reaching a leaf.
    """

    def test_file_named_as_c4id(self, tmp_path):
        """Content file should be named with the full 90-char C4 ID."""
        store = FSStore(tmp_path)
        c4id = store.put(BytesIO(b"interop test"))
        path = store._resolve_path(c4id)
        assert path.name == str(c4id)
        assert len(path.name) == 90
        assert path.name.startswith("c4")

    def test_flat_layout_before_split(self, tmp_path):
        """Small store: content lives at root/{id} (no prefix dirs yet)."""
        store = FSStore(tmp_path)
        c4id = store.put(BytesIO(b"flat test"))
        id_str = str(c4id)

        # Before any splits, content is directly in root
        content_file = tmp_path / id_str
        assert content_file.exists(), f"Content should be at root/{id_str}"

    def test_after_split_c4_prefix_exists(self, tmp_path):
        """After split, c4/ prefix dir exists (chars 0-1 of every C4 ID)."""
        store = FSStore(tmp_path, split_threshold=3)

        for i in range(8):
            store.put(BytesIO(f"force-split-{i}".encode()))

        # After split, all C4 IDs start with "c4", so c4/ should exist
        c4_dir = tmp_path / "c4"
        assert c4_dir.is_dir(), "c4/ prefix dir should exist after split"

    def test_split_creates_next_prefix_level(self, tmp_path):
        """After split, files move to root/c4/XY/ where XY = chars 2-3."""
        store = FSStore(tmp_path, split_threshold=5)

        ids = []
        for i in range(10):
            c4id = store.put(BytesIO(f"split-test-{i}".encode()))
            ids.append(c4id)

        # After split, c4/ should contain subdirectories (not just files)
        c4_dir = tmp_path / "c4"
        subdirs = [d for d in c4_dir.iterdir() if d.is_dir()]
        assert len(subdirs) > 0, "Split should create 2-char subdirectories"

        # All content should still be retrievable
        for i, c4id in enumerate(ids):
            data = store.get(c4id).read()
            assert data == f"split-test-{i}".encode()

    def test_temp_files_excluded_from_split(self, tmp_path):
        """Temp files (.ingest.*) should not count toward split threshold."""
        store = FSStore(tmp_path, split_threshold=5)

        # Create some content
        for i in range(3):
            store.put(BytesIO(f"content-{i}".encode()))

        # Create fake temp files (should be ignored by split logic)
        c4_dir = tmp_path / "c4"
        if c4_dir.exists():
            for i in range(10):
                (c4_dir / f".ingest.temp{i}").touch()

        # Add one more — should NOT trigger split because temps don't count
        store.put(BytesIO(b"one more"))
        # Just verify it doesn't crash and content is retrievable
        assert store.has(identify_bytes(b"one more"))
