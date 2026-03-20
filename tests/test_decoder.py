"""Tests for c4m decoder — parse c4m text into Manifest objects."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pytest

from c4py.decoder import load, loads
from c4py.entry import NULL_TIMESTAMP, NULL_SIZE, FlowDirection
from c4py.id import C4ID, parse

VECTORS_PATH = Path(__file__).parent / "vectors" / "known_ids.json"


def load_vectors():
    with open(VECTORS_PATH) as f:
        return json.load(f)


class TestBasicDecoding:
    """Basic entry parsing."""

    def test_single_file_entry(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 3 README.md c45xZeXwMSpqXjpDumcHMA6mhoAmGHkUo7r9WmN2UgSEQzj9KjgseaQdkEJ11fGb5S1WEENcV3q8RFWwEeVpC7Fjk2\n"
        m = loads(text)
        assert len(m) == 1
        e = m[0]
        assert e.name == "README.md"
        assert e.size == 3
        assert e.depth == 0
        assert str(e.c4id) == "c45xZeXwMSpqXjpDumcHMA6mhoAmGHkUo7r9WmN2UgSEQzj9KjgseaQdkEJ11fGb5S1WEENcV3q8RFWwEeVpC7Fjk2"

    def test_directory_entry(self):
        text = "-rwxr-xr-x 2025-06-15T12:00:00Z 3 src/ -\n"
        m = loads(text)
        assert len(m) == 1
        e = m[0]
        assert e.name == "src/"
        assert e.is_dir()
        assert e.c4id is None

    def test_empty_input(self):
        m = loads("")
        assert len(m) == 0

    def test_blank_lines_skipped(self):
        text = "\n\n-rw-r--r-- 2025-06-15T12:00:00Z 3 file.txt -\n\n\n"
        m = loads(text)
        assert len(m) == 1

    def test_cr_rejected(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 3 file.txt -\r\n"
        with pytest.raises(ValueError, match="CR"):
            loads(text)


class TestNullValues:
    """Parsing entries with null values."""

    def test_null_mode(self):
        text = "- - - file.txt -\n"
        m = loads(text)
        e = m[0]
        assert e.mode == 0
        assert e.has_null_mode()

    def test_null_timestamp(self):
        text = "- - - file.txt -\n"
        m = loads(text)
        e = m[0]
        assert e.timestamp == NULL_TIMESTAMP

    def test_null_size(self):
        text = "- - - file.txt -\n"
        m = loads(text)
        e = m[0]
        assert e.size == NULL_SIZE

    def test_null_c4id(self):
        text = "- - - file.txt -\n"
        m = loads(text)
        e = m[0]
        assert e.c4id is None

    def test_all_null_fields(self):
        text = "- - - everything-null -\n"
        m = loads(text)
        e = m[0]
        assert e.mode == 0
        assert e.timestamp == NULL_TIMESTAMP
        assert e.size == NULL_SIZE
        assert e.c4id is None

    def test_ten_dash_null_mode(self):
        """'----------' is also valid for null mode."""
        text = "---------- 2025-06-15T12:00:00Z 100 file.txt -\n"
        m = loads(text)
        e = m[0]
        assert e.mode == 0


class TestIndentation:
    """Indentation and depth tracking."""

    def test_simple_indented_entries(self):
        text = (
            "-rwxr-xr-x 2025-06-15T12:00:00Z 3 src/ -\n"
            "  -rw-r--r-- 2025-06-15T12:00:00Z 3 main.go -\n"
        )
        m = loads(text)
        assert len(m) == 2
        assert m[0].depth == 0
        assert m[0].name == "src/"
        assert m[1].depth == 1
        assert m[1].name == "main.go"

    def test_auto_detect_indent_width(self):
        text = (
            "-rwxr-xr-x 2025-06-15T12:00:00Z 0 root/ -\n"
            "    -rw-r--r-- 2025-06-15T12:00:00Z 0 child.txt -\n"
            "        -rw-r--r-- 2025-06-15T12:00:00Z 0 grandchild.txt -\n"
        )
        m = loads(text)
        assert m[0].depth == 0
        assert m[1].depth == 1
        assert m[2].depth == 2

    def test_nested_directories(self):
        text = (
            "drwxr-xr-x 2025-06-15T12:00:00Z 0 a/ -\n"
            "  drwxr-xr-x 2025-06-15T12:00:00Z 0 b/ -\n"
            "    -rw-r--r-- 2025-06-15T12:00:00Z 10 file.txt -\n"
        )
        m = loads(text)
        assert m[0].depth == 0
        assert m[1].depth == 1
        assert m[2].depth == 2


class TestSymlinks:
    """Symlink entry parsing."""

    def test_symlink_entry(self):
        text = "lrwxrwxrwx 2025-06-15T12:00:00Z 0 link -> /target/path -\n"
        m = loads(text)
        e = m[0]
        assert e.is_symlink()
        assert e.name == "link"
        assert e.target == "/target/path"


class TestHardLinks:
    """Hard link entry parsing."""

    def test_ungrouped_hard_link(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 100 file.txt -> -\n"
        m = loads(text)
        e = m[0]
        assert e.hard_link == -1

    def test_grouped_hard_link(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 100 file.txt ->1 -\n"
        m = loads(text)
        e = m[0]
        assert e.hard_link == 1


class TestFlowLinks:
    """Flow link entry parsing."""

    def test_outbound_flow(self):
        text = "drwxr-xr-x 2025-06-15T12:00:00Z 0 output/ -> studio:inbox/ -\n"
        m = loads(text)
        e = m[0]
        assert e.flow_direction == FlowDirection.OUTBOUND
        assert e.flow_target == "studio:inbox/"

    def test_inbound_flow(self):
        text = "drwxr-xr-x 2025-06-15T12:00:00Z 0 input/ <- studio:output/ -\n"
        m = loads(text)
        e = m[0]
        assert e.flow_direction == FlowDirection.INBOUND
        assert e.flow_target == "studio:output/"

    def test_bidirectional_flow(self):
        text = "drwxr-xr-x 2025-06-15T12:00:00Z 0 shared/ <> studio:shared/ -\n"
        m = loads(text)
        e = m[0]
        assert e.flow_direction == FlowDirection.BIDIRECTIONAL
        assert e.flow_target == "studio:shared/"


class TestTimestamps:
    """Timestamp format variations."""

    def test_canonical_utc(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 0 f -\n"
        m = loads(text)
        assert m[0].timestamp == datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

    def test_rfc3339_with_offset(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00-07:00 0 f -\n"
        m = loads(text)
        # Should be converted to UTC
        assert m[0].timestamp == datetime(2025, 6, 15, 19, 0, 0, tzinfo=timezone.utc)

    def test_null_timestamp_dash(self):
        text = "- - 0 f -\n"
        m = loads(text)
        assert m[0].timestamp == NULL_TIMESTAMP


class TestSizes:
    """Size field variations."""

    def test_plain_integer(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 12345 f -\n"
        m = loads(text)
        assert m[0].size == 12345

    def test_size_with_commas(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 1,234,567 f -\n"
        m = loads(text)
        assert m[0].size == 1234567

    def test_null_size(self):
        text = "- - - f -\n"
        m = loads(text)
        assert m[0].size == NULL_SIZE

    def test_zero_size(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 0 f -\n"
        m = loads(text)
        assert m[0].size == 0


class TestDirectives:
    """Directive rejection."""

    def test_at_directive_rejected(self):
        with pytest.raises(ValueError, match="directives not supported"):
            loads("@version 1.0\n")


class TestInlineIDLists:
    """Inline ID list (range data) handling."""

    def test_inline_id_list_skipped(self):
        """Lines >90 chars, multiple of 90, all valid C4 IDs are range data."""
        id1 = "c459dsjfscH38cYeXXYogktxf4Cd9ibshE3BHUo6a58hBXmRQdZrAkZzsWcbWtDg5oQstpDuni4Hirj75GEmTc1sFT"
        id2 = "c45xZeXwMSpqXjpDumcHMA6mhoAmGHkUo7r9WmN2UgSEQzj9KjgseaQdkEJ11fGb5S1WEENcV3q8RFWwEeVpC7Fjk2"
        inline = id1 + id2
        text = f"-rw-r--r-- 2025-06-15T12:00:00Z 3 file.txt -\n{inline}\n"
        m = loads(text)
        # The inline ID list should be skipped; only the entry counts
        assert len(m) == 1


class TestPatchChains:
    """Patch chain parsing."""

    def test_base_ref_on_first_line(self):
        """A bare C4 ID on the first line sets the base reference."""
        base_id = "c459dsjfscH38cYeXXYogktxf4Cd9ibshE3BHUo6a58hBXmRQdZrAkZzsWcbWtDg5oQstpDuni4Hirj75GEmTc1sFT"
        text = f"{base_id}\n-rw-r--r-- 2025-06-15T12:00:00Z 3 file.txt -\n"
        m = loads(text)
        assert m.base is not None
        assert str(m.base) == base_id
        assert len(m) == 1


class TestFileLoad:
    """Loading from file paths and streams."""

    def test_load_from_stringio(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 3 file.txt -\n"
        m = load(StringIO(text))
        assert len(m) == 1

    def test_load_from_file(self, tmp_path):
        path = tmp_path / "test.c4m"
        path.write_text(
            "-rw-r--r-- 2025-06-15T12:00:00Z 3 file.txt -\n",
            encoding="utf-8",
        )
        m = load(str(path))
        assert len(m) == 1


class TestVectorRoundTrip:
    """Test against known_ids.json manifest vectors."""

    def test_parse_manifest_vectors(self):
        vectors = load_vectors()
        for mv in vectors["manifest_vectors"]:
            m = loads(mv["canonical"])
            assert len(m) > 0, f"Empty manifest for: {mv['description']}"


class TestMultiEntry:
    """Multi-entry manifests."""

    def test_file_and_directory_with_child(self):
        text = (
            "-rw-r--r-- 2025-06-15T12:00:00Z 3 README.md "
            "c45xZeXwMSpqXjpDumcHMA6mhoAmGHkUo7r9WmN2UgSEQzj9KjgseaQdkEJ11fGb5S1WEENcV3q8RFWwEeVpC7Fjk2\n"
            "-rwxr-xr-x 2025-06-15T12:00:00Z 3 src/ -\n"
            "  -rw-r--r-- 2025-06-15T12:00:00Z 3 main.go "
            "c45KgBYEvEE7Yfv16JAgnUT29bon2WsYAiBFZvnKNJiQR8kya2tRtEdfD6vi8bjvmmDDrepEGmkNvk88M8NWdeV9ig\n"
        )
        m = loads(text)
        assert len(m) == 3
        assert m[0].name == "README.md"
        assert m[1].name == "src/"
        assert m[1].is_dir()
        assert m[2].name == "main.go"
        assert m[2].depth == 1


class TestModeFormats:
    """Various mode formats."""

    def test_regular_file_mode(self):
        text = "-rw-r--r-- 2025-06-15T12:00:00Z 0 f -\n"
        m = loads(text)
        # 0o644
        assert m[0].mode & 0o777 == 0o644

    def test_executable_mode(self):
        text = "-rwxr-xr-x 2025-06-15T12:00:00Z 0 f -\n"
        m = loads(text)
        assert m[0].mode & 0o777 == 0o755

    def test_directory_mode(self):
        text = "drwxr-xr-x 2025-06-15T12:00:00Z 0 d/ -\n"
        m = loads(text)
        assert m[0].is_dir()
        assert m[0].mode & 0o777 == 0o755

    def test_symlink_mode(self):
        text = "lrwxrwxrwx 2025-06-15T12:00:00Z 0 l -> target -\n"
        m = loads(text)
        assert m[0].is_symlink()
