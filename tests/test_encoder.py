"""Tests for c4m encoder — write Manifest objects as c4m text."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dump, dumps
from c4py.entry import NULL_TIMESTAMP, NULL_SIZE, Entry, FlowDirection
from c4py.id import C4ID, parse
from c4py.manifest import Manifest

VECTORS_PATH = Path(__file__).parent / "vectors" / "known_ids.json"


def load_vectors():
    with open(VECTORS_PATH) as f:
        return json.load(f)


class TestCanonicalOutput:
    """Canonical (non-pretty) output."""

    def test_simple_file_entry(self):
        c4id = parse("c45xZeXwMSpqXjpDumcHMA6mhoAmGHkUo7r9WmN2UgSEQzj9KjgseaQdkEJ11fGb5S1WEENcV3q8RFWwEeVpC7Fjk2")
        e = Entry(
            name="README.md",
            mode=0o100644,
            timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
            size=3,
            c4id=c4id,
        )
        m = Manifest(entries=[e])
        text = dumps(m)
        assert text == (
            "-rw-r--r-- 2025-06-15T12:00:00Z 3 README.md "
            "c45xZeXwMSpqXjpDumcHMA6mhoAmGHkUo7r9WmN2UgSEQzj9KjgseaQdkEJ11fGb5S1WEENcV3q8RFWwEeVpC7Fjk2\n"
        )

    def test_null_values(self):
        e = Entry(
            name="file.txt",
            mode=0,
            timestamp=NULL_TIMESTAMP,
            size=NULL_SIZE,
            c4id=None,
        )
        m = Manifest(entries=[e])
        text = dumps(m)
        # Null mode renders as "-" in canonical/format output
        assert text == "- - - file.txt -\n"

    def test_directory_entry(self):
        e = Entry(
            name="src/",
            mode=0o40755,
            timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
            size=0,
            c4id=None,
        )
        m = Manifest(entries=[e])
        text = dumps(m)
        assert "src/" in text
        assert text.endswith("-\n")

    def test_indented_entries(self):
        entries = [
            Entry(
                name="src/",
                mode=0o40755,
                timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                size=3,
                c4id=None,
                depth=0,
            ),
            Entry(
                name="main.go",
                mode=0o100644,
                timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                size=3,
                c4id=None,
                depth=1,
            ),
        ]
        m = Manifest(entries=entries)
        text = dumps(m)
        lines = text.split("\n")
        # Second entry should be indented by 2 spaces
        assert lines[1].startswith("  ")

    def test_symlink_entry(self):
        import stat
        e = Entry(
            name="link",
            mode=stat.S_IFLNK | 0o777,
            timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
            size=0,
            target="/target/path",
            c4id=None,
        )
        m = Manifest(entries=[e])
        text = dumps(m)
        assert "-> /target/path" in text


class TestVectorRoundTrip:
    """Encode then decode must produce identical output."""

    def test_manifest_vector_round_trip(self):
        """Parse canonical text, re-encode, verify byte-identical."""
        vectors = load_vectors()
        for mv in vectors["manifest_vectors"]:
            manifest = loads(mv["canonical"])
            output = dumps(manifest)
            assert output == mv["canonical"], (
                f"Round-trip failed for: {mv['description']}\n"
                f"Expected:\n{mv['canonical']!r}\n"
                f"Got:\n{output!r}"
            )


class TestPrettyOutput:
    """Pretty (ergonomic) output."""

    def test_pretty_has_indentation(self):
        entries = [
            Entry(
                name="src/",
                mode=0o40755,
                timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                size=100,
                c4id=None,
                depth=0,
            ),
            Entry(
                name="main.go",
                mode=0o100644,
                timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                size=50,
                c4id=None,
                depth=1,
            ),
        ]
        m = Manifest(entries=entries)
        text = dumps(m, pretty=True)
        lines = text.strip().split("\n")
        assert len(lines) == 2
        # Child should be indented
        assert lines[1].startswith("  ")

    def test_pretty_has_commas_in_sizes(self):
        e = Entry(
            name="big.dat",
            mode=0o100644,
            timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
            size=1234567,
            c4id=None,
        )
        m = Manifest(entries=[e])
        text = dumps(m, pretty=True)
        assert "1,234,567" in text

    def test_pretty_round_trip(self):
        """Pretty-encoded manifest should be decodable."""
        c4id = parse("c45xZeXwMSpqXjpDumcHMA6mhoAmGHkUo7r9WmN2UgSEQzj9KjgseaQdkEJ11fGb5S1WEENcV3q8RFWwEeVpC7Fjk2")
        entries = [
            Entry(
                name="file.txt",
                mode=0o100644,
                timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                size=42,
                c4id=c4id,
            ),
        ]
        m = Manifest(entries=entries)
        pretty_text = dumps(m, pretty=True)
        decoded = loads(pretty_text)
        assert len(decoded) == 1
        assert decoded[0].name == "file.txt"
        assert decoded[0].c4id == c4id


class TestDump:
    """dump() function (file/stream output)."""

    def test_dump_to_stringio(self):
        e = Entry(name="f", mode=0, timestamp=NULL_TIMESTAMP, size=NULL_SIZE, c4id=None)
        m = Manifest(entries=[e])
        buf = StringIO()
        dump(m, buf)
        assert len(buf.getvalue()) > 0

    def test_dump_to_file(self, tmp_path):
        e = Entry(name="f", mode=0, timestamp=NULL_TIMESTAMP, size=NULL_SIZE, c4id=None)
        m = Manifest(entries=[e])
        path = tmp_path / "output.c4m"
        dump(m, str(path))
        assert path.exists()
        content = path.read_text()
        assert len(content) > 0


class TestSortOrder:
    """Encoder sorts entries before output."""

    def test_files_before_dirs(self):
        """Files should appear before directories at the same level."""
        entries = [
            Entry(
                name="src/",
                mode=0o40755,
                timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                size=0, c4id=None,
            ),
            Entry(
                name="README.md",
                mode=0o100644,
                timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                size=100, c4id=None,
            ),
        ]
        m = Manifest(entries=entries)
        text = dumps(m)
        lines = [l for l in text.split("\n") if l.strip()]
        # README.md (file) should come before src/ (directory)
        assert "README.md" in lines[0]
        assert "src/" in lines[1]

    def test_natural_sort_within_files(self):
        """Files should be naturally sorted."""
        entries = [
            Entry(name="file10.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None),
            Entry(name="file2.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None),
            Entry(name="file1.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None),
        ]
        m = Manifest(entries=entries)
        text = dumps(m)
        lines = [l for l in text.split("\n") if l.strip()]
        assert "file1.txt" in lines[0]
        assert "file2.txt" in lines[1]
        assert "file10.txt" in lines[2]
