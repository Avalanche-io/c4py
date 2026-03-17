"""Tests for C4M entry parsing and formatting."""

from datetime import datetime, timezone

from pyc4.entry import (
    Entry,
    FlowDirection,
    NULL_SIZE,
    NULL_TIMESTAMP,
    format_mode,
    format_size,
    format_timestamp,
)


class TestFormatTimestamp:
    def test_null(self):
        assert format_timestamp(NULL_TIMESTAMP) == "-"

    def test_utc(self):
        ts = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert format_timestamp(ts) == "2025-06-15T12:00:00Z"

    def test_midnight(self):
        ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert format_timestamp(ts) == "2025-01-01T00:00:00Z"


class TestFormatSize:
    def test_null(self):
        assert format_size(NULL_SIZE) == "-"

    def test_zero(self):
        assert format_size(0) == "0"

    def test_positive(self):
        assert format_size(1024) == "1024"

    def test_large(self):
        assert format_size(8_000_000_000_000) == "8000000000000"


class TestEntry:
    def test_is_dir(self):
        e = Entry(name="src/")
        assert e.is_dir()

    def test_is_not_dir(self):
        e = Entry(name="README.md")
        assert not e.is_dir()

    def test_null_fields(self):
        e = Entry(name="file.txt")
        assert e.has_null_mode()
        assert e.has_null_timestamp()
        assert e.has_null_size()
        assert e.has_null_c4id()

    def test_flow_link(self):
        e = Entry(
            name="outbox/",
            flow_direction=FlowDirection.OUTBOUND,
            flow_target="studio:inbox/",
        )
        assert e.is_flow_linked()
