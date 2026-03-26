"""Tests for c4m canonical identification.

A pretty-printed c4m and its canonical form must produce the same C4 ID.
Non-canonical c4m files are ephemeral views — the durable identity is always
computed from canonical bytes.
"""

from datetime import datetime, timezone
from io import BytesIO

from c4py.canonical import _looks_like_c4m, try_canonicalize
from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.entry import Entry
from c4py.id import identify_bytes, identify_file
from c4py.manifest import Manifest
from c4py.store import FSStore


def _build_test_manifest() -> Manifest:
    """Build a test manifest with real C4 IDs."""
    return Manifest(entries=[
        Entry(
            name="alpha.txt",
            mode=0o100644,
            timestamp=datetime(2025, 9, 1, 0, 36, 18, tzinfo=timezone.utc),
            size=1024,
            c4id=identify_bytes(b"alpha content"),
        ),
        Entry(
            name="beta.txt",
            mode=0o100644,
            timestamp=datetime(2025, 9, 1, 0, 36, 18, tzinfo=timezone.utc),
            size=2048,
            c4id=identify_bytes(b"beta content"),
        ),
        Entry(
            name="subdir/",
            mode=0o40755,
            timestamp=datetime(2025, 9, 1, 0, 36, 18, tzinfo=timezone.utc),
            size=3072,
            c4id=identify_bytes(b"subdir content"),
        ),
    ])


def _canonical_text() -> str:
    return dumps(_build_test_manifest(), pretty=False)


def _pretty_text() -> str:
    return dumps(_build_test_manifest(), pretty=True)


class TestLooksLikeC4m:
    """Heuristic detection of c4m content."""

    def test_canonical_entry(self):
        data = b"-rw-r--r-- 2025-09-01T00:36:18Z 1024 test.txt -\n"
        assert _looks_like_c4m(data)

    def test_directory_entry(self):
        data = b"drwxr-xr-x 2025-09-01T00:36:18Z 1024 testdir/ -\n"
        assert _looks_like_c4m(data)

    def test_symlink_entry(self):
        data = b"lrwxr-xr-x 2025-09-01T00:36:18Z 1024 link -> target -\n"
        assert _looks_like_c4m(data)

    def test_null_mode_entry(self):
        data = b"- - - test.txt -\n"
        assert _looks_like_c4m(data)

    def test_not_c4m(self):
        data = b"This is just a plain text file.\n"
        assert not _looks_like_c4m(data)

    def test_binary_content(self):
        data = b"\x00\x01\x02\xff\xfe"
        assert not _looks_like_c4m(data)

    def test_empty(self):
        data = b""
        assert not _looks_like_c4m(data)

    def test_blank_lines_before_entry(self):
        data = b"\n\n-rw-r--r-- 2025-09-01T00:36:18Z 1024 test.txt -\n"
        assert _looks_like_c4m(data)


class TestTryCanonicalize:
    """Canonicalization of c4m content."""

    def test_canonical_is_stable(self):
        """Canonical form canonicalizes to itself."""
        data = _canonical_text().encode("utf-8")
        result = try_canonicalize(data)
        assert result is not None
        assert result == data

    def test_pretty_canonicalizes(self):
        """Pretty-printed c4m canonicalizes to canonical form."""
        pretty_data = _pretty_text().encode("utf-8")
        result = try_canonicalize(pretty_data)
        assert result is not None
        canonical_data = _canonical_text().encode("utf-8")
        result_canon = try_canonicalize(canonical_data)
        assert result == result_canon

    def test_non_c4m_returns_none(self):
        """Non-c4m content returns None."""
        data = b"Just some regular text content.\n"
        assert try_canonicalize(data) is None

    def test_binary_returns_none(self):
        """Binary content returns None."""
        data = b"\x00\x01\x02\xff\xfe"
        assert try_canonicalize(data) is None

    def test_empty_returns_none(self):
        """Empty content returns None."""
        assert try_canonicalize(b"") is None


class TestIdentifyFileCanonical:
    """identify_file() canonicalizes c4m content before hashing."""

    def test_c4m_extension_canonicalizes(self, tmp_path):
        """File with .c4m extension is canonicalized."""
        canonical = tmp_path / "test.c4m"
        canonical.write_text(_canonical_text())
        pretty = tmp_path / "pretty.c4m"
        pretty.write_text(_pretty_text())

        id_canonical = identify_file(canonical)
        id_pretty = identify_file(pretty)
        assert id_canonical == id_pretty

    def test_pretty_and_canonical_same_id(self, tmp_path):
        """Core requirement: pretty and canonical c4m produce same C4 ID."""
        m = _build_test_manifest()

        canonical_text = dumps(m, pretty=False)
        pretty_text = dumps(m, pretty=True)

        # Sanity: they are different strings
        assert canonical_text != pretty_text

        # Write to files
        canonical_file = tmp_path / "project.c4m"
        canonical_file.write_text(canonical_text)
        pretty_file = tmp_path / "project_pretty.c4m"
        pretty_file.write_text(pretty_text)

        # Both must produce the same C4 ID
        assert identify_file(canonical_file) == identify_file(pretty_file)

    def test_non_c4m_file_unchanged(self, tmp_path):
        """Non-c4m files are identified by raw bytes as usual."""
        regular = tmp_path / "hello.txt"
        regular.write_bytes(b"hello world")
        assert identify_file(regular) == identify_bytes(b"hello world")

    def test_non_c4m_extension_with_c4m_content(self, tmp_path):
        """A .txt file containing c4m content is also canonicalized."""
        canonical = _canonical_text()
        txt_file = tmp_path / "manifest.txt"
        txt_file.write_text(canonical)
        c4m_file = tmp_path / "manifest.c4m"
        c4m_file.write_text(canonical)

        # Both should produce the same ID since content is valid c4m
        assert identify_file(txt_file) == identify_file(c4m_file)

    def test_sha512_divergence(self, tmp_path):
        """C4 ID of non-canonical c4m differs from raw SHA-512 hash."""
        pretty_file = tmp_path / "pretty.c4m"
        pretty_data = _pretty_text().encode("utf-8")
        pretty_file.write_bytes(pretty_data)

        c4id = identify_file(pretty_file)
        raw_id = identify_bytes(pretty_data)

        # Pretty form is NOT canonical, so identify_file (which canonicalizes)
        # must produce a different ID than identify_bytes (which does not)
        canonical = try_canonicalize(pretty_data)
        if canonical is not None and canonical != pretty_data:
            assert c4id != raw_id


class TestStoreCanonical:
    """FSStore.put() canonicalizes c4m content before storing."""

    def test_store_canonical_c4m(self, tmp_path):
        """Storing pretty c4m yields canonical content in store."""
        store = FSStore(tmp_path)

        pretty_data = _pretty_text().encode("utf-8")
        c4id = store.put(BytesIO(pretty_data))

        # Retrieve and verify it's canonical
        content = store.get(c4id).read()
        canonical_data = _canonical_text().encode("utf-8")
        assert content == canonical_data

    def test_store_id_matches_identify_file(self, tmp_path):
        """Store.put() and identify_file() produce the same ID for c4m content."""
        store_dir = tmp_path / "store"
        store = FSStore(store_dir)

        pretty = _pretty_text()
        file_path = tmp_path / "test.c4m"
        file_path.write_text(pretty)

        file_id = identify_file(file_path)
        store_id = store.put(BytesIO(pretty.encode("utf-8")))

        assert file_id == store_id

    def test_store_non_c4m_unchanged(self, tmp_path):
        """Non-c4m content is stored as-is."""
        store = FSStore(tmp_path)
        data = b"not a c4m file"
        c4id = store.put(BytesIO(data))
        assert store.get(c4id).read() == data


class TestCanonicalRoundTrip:
    """Round-trip: parse -> encode -> identify produces stable IDs."""

    def test_round_trip_stability(self):
        """Parsing and re-encoding canonical c4m produces identical bytes."""
        canonical = _canonical_text()
        data = canonical.encode("utf-8")
        manifest = loads(canonical)
        re_encoded = dumps(manifest, pretty=False).encode("utf-8")
        assert data == re_encoded
        assert identify_bytes(data) == identify_bytes(re_encoded)

    def test_unsorted_entries_canonicalize(self, tmp_path):
        """Entries in non-canonical order produce the same ID after sort."""
        canonical = _canonical_text()
        lines = canonical.strip().split("\n")
        reversed_text = "\n".join(reversed(lines)) + "\n"

        forward_file = tmp_path / "forward.c4m"
        forward_file.write_text(canonical)
        reversed_file = tmp_path / "reversed.c4m"
        reversed_file.write_text(reversed_text)

        assert identify_file(forward_file) == identify_file(reversed_file)
