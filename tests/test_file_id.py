"""A file's C4 ID is the ID of its bytes, c4m text included, and a
description's C4 ID is what its decoded text resolves to: two questions,
two answers. A pretty file and a canonical file holding the same description
have different file IDs and the same description ID.
"""

from datetime import datetime, timezone
from io import BytesIO

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.entry import Entry
from c4py.id import identify_bytes, identify_file
from c4py.manifest import Manifest
from c4py.scanner import scan
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


class TestFileIDIsItsBytes:
    """A file's C4 ID is the ID of its bytes, c4m text included: the ID must
    summon that exact file (the Go reference's design/no-self-id.md). Until
    2026-11-11 identify_file, the scanner and FSStore.put re-rendered any
    content that parsed as c4m, whatever the file's name, so a pretty c4m
    file's ID could not summon it."""

    def test_c4m_file_id_is_its_bytes(self, tmp_path):
        for name, text in [("canonical.c4m", _canonical_text()), ("pretty.c4m", _pretty_text())]:
            f = tmp_path / name
            f.write_text(text)
            assert identify_file(f) == identify_bytes(text.encode("utf-8")), name

    def test_pretty_and_canonical_files_have_different_ids(self, tmp_path):
        m = _build_test_manifest()
        canonical_text = dumps(m, pretty=False)
        pretty_text = dumps(m, pretty=True)
        assert canonical_text != pretty_text
        canonical_file = tmp_path / "project.c4m"
        canonical_file.write_text(canonical_text)
        pretty_file = tmp_path / "project_pretty.c4m"
        pretty_file.write_text(pretty_text)
        # Two files, two byte IDs. The description both hold has one ID,
        # which the decoder gives.
        assert identify_file(canonical_file) != identify_file(pretty_file)
        assert loads(canonical_text).compute_c4id() == loads(pretty_text).compute_c4id()

    def test_non_c4m_file_unchanged(self, tmp_path):
        regular = tmp_path / "hello.txt"
        regular.write_bytes(b"hello world")
        assert identify_file(regular) == identify_bytes(b"hello world")

    def test_c4m_text_under_another_name_is_its_bytes(self, tmp_path):
        text = _pretty_text()
        txt_file = tmp_path / "manifest.txt"
        txt_file.write_text(text)
        assert identify_file(txt_file) == identify_bytes(text.encode("utf-8"))

    def test_scan_records_each_files_byte_id(self, tmp_path):
        tree = tmp_path / "tree"
        tree.mkdir()
        pretty = _pretty_text().encode("utf-8")
        (tree / "pretty.c4m").write_bytes(pretty)
        (tree / "copy.dat").write_bytes(pretty)
        for store in (None, FSStore(tmp_path / "store")):
            m = scan(tree, store=store)
            for e in m.entries:
                assert e.c4id == identify_bytes(pretty), (e.name, store)


class TestStoreKeepsBytes:
    """FSStore.put() stores the bytes given, exactly."""

    def test_store_pretty_c4m_returns_it_exactly(self, tmp_path):
        store = FSStore(tmp_path)
        pretty_data = _pretty_text().encode("utf-8")
        c4id = store.put(BytesIO(pretty_data))
        assert c4id == identify_bytes(pretty_data)
        with store.get(c4id) as f:
            assert f.read() == pretty_data

    def test_store_id_matches_identify_file(self, tmp_path):
        store = FSStore(tmp_path / "store")
        pretty = _pretty_text()
        file_path = tmp_path / "test.c4m"
        file_path.write_text(pretty)
        assert identify_file(file_path) == store.put(BytesIO(pretty.encode("utf-8")))

    def test_store_non_c4m_unchanged(self, tmp_path):
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
        """Entries in non-canonical order describe the same tree: the
        description ID agrees, and each file keeps its own byte ID."""
        canonical = _canonical_text()
        lines = canonical.strip().split("\n")
        reversed_text = "\n".join(reversed(lines)) + "\n"

        forward_file = tmp_path / "forward.c4m"
        forward_file.write_text(canonical)
        reversed_file = tmp_path / "reversed.c4m"
        reversed_file.write_text(reversed_text)

        assert loads(canonical).compute_c4id() == loads(reversed_text).compute_c4id()
        assert identify_file(forward_file) == identify_bytes(canonical.encode("utf-8"))
        assert identify_file(reversed_file) == identify_bytes(reversed_text.encode("utf-8"))
