"""Tests for C4M entry parsing and formatting."""

import stat
from datetime import datetime, timezone

from c4py.entry import (
    NULL_SIZE,
    NULL_TIMESTAMP,
    Entry,
    FlowDirection,
    format_mode,
    format_size,
    format_timestamp,
)
from c4py.id import C4ID


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


class TestFormatMode:
    """format_mode must match Go formatMode output for all file types."""

    def test_regular_file_644(self):
        assert format_mode(0o644) == "-rw-r--r--"

    def test_regular_file_755(self):
        assert format_mode(0o755) == "-rwxr-xr-x"

    def test_directory_755(self):
        assert format_mode(stat.S_IFDIR | 0o755) == "drwxr-xr-x"

    def test_symlink_777(self):
        assert format_mode(stat.S_IFLNK | 0o777) == "lrwxrwxrwx"

    def test_named_pipe(self):
        assert format_mode(stat.S_IFIFO | 0o666) == "prw-rw-rw-"

    def test_socket(self):
        assert format_mode(stat.S_IFSOCK | 0o666) == "srw-rw-rw-"

    def test_block_device(self):
        assert format_mode(stat.S_IFBLK | 0o666) == "brw-rw-rw-"

    def test_char_device(self):
        assert format_mode(stat.S_IFCHR | 0o666) == "crw-rw-rw-"

    def test_setuid(self):
        assert format_mode(stat.S_ISUID | 0o755) == "-rwsr-xr-x"

    def test_setgid(self):
        assert format_mode(stat.S_ISGID | 0o755) == "-rwxr-sr-x"

    def test_sticky(self):
        assert format_mode(stat.S_ISVTX | 0o755) == "-rwxr-xr-t"

    def test_setuid_no_exec(self):
        assert format_mode(stat.S_ISUID | 0o644) == "-rwSr--r--"

    def test_setgid_no_exec(self):
        assert format_mode(stat.S_ISGID | 0o644) == "-rw-r-Sr--"

    def test_sticky_no_exec(self):
        assert format_mode(stat.S_ISVTX | 0o644) == "-rw-r--r-T"

    def test_null_mode(self):
        """Null mode (0) renders as '----------' from format_mode."""
        assert format_mode(0) == "----------"


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


class TestCanonical:
    """canonical() must match Go Canonical() output."""

    TEST_TIME = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    TEST_ID = C4ID("c41j3C6Jqga95PL2zmZVBWixAUhoWDNmwamiWiNTDAMRL1UWqe4WdtYjSozRijRSokEsaTnYyxoCBt43u4sfqWG2uB")  # noqa: E501

    def test_file_with_c4id(self):
        e = Entry(
            name="test.txt",
            mode=0o644,
            timestamp=self.TEST_TIME,
            size=1234,
            c4id=self.TEST_ID,
            depth=1,  # Should be ignored in canonical
        )
        assert e.canonical() == (
            "-rw-r--r-- 2024-01-15T10:30:00Z 1234 test.txt "
            "c41j3C6Jqga95PL2zmZVBWixAUhoWDNmwamiWiNTDAMRL1UWqe4WdtYjSozRijRSokEsaTnYyxoCBt43u4sfqWG2uB"
        )

    def test_directory(self):
        e = Entry(
            name="mydir/",
            mode=stat.S_IFDIR | 0o755,
            timestamp=self.TEST_TIME,
            size=4096,
            c4id=self.TEST_ID,
            depth=2,
        )
        assert e.canonical() == (
            "drwxr-xr-x 2024-01-15T10:30:00Z 4096 mydir/ "
            "c41j3C6Jqga95PL2zmZVBWixAUhoWDNmwamiWiNTDAMRL1UWqe4WdtYjSozRijRSokEsaTnYyxoCBt43u4sfqWG2uB"
        )

    def test_symlink_with_target(self):
        e = Entry(
            name="link",
            mode=stat.S_IFLNK | 0o777,
            timestamp=self.TEST_TIME,
            size=0,
            target="target.txt",
        )
        assert e.canonical() == "lrwxrwxrwx 2024-01-15T10:30:00Z 0 link -> target.txt -"

    def test_file_with_spaces(self):
        e = Entry(
            name="my file.txt",
            mode=0o644,
            timestamp=self.TEST_TIME,
            size=100,
        )
        assert e.canonical() == "-rw-r--r-- 2024-01-15T10:30:00Z 100 my\\ file.txt -"

    def test_null_timestamp(self):
        e = Entry(
            name="test.txt",
            mode=0o644,
            timestamp=NULL_TIMESTAMP,
            size=1234,
            c4id=self.TEST_ID,
        )
        assert e.canonical() == (
            "-rw-r--r-- - 1234 test.txt "
            "c41j3C6Jqga95PL2zmZVBWixAUhoWDNmwamiWiNTDAMRL1UWqe4WdtYjSozRijRSokEsaTnYyxoCBt43u4sfqWG2uB"
        )

    def test_null_mode(self):
        """Null mode (0) renders as '-' (single char) in canonical."""
        e = Entry(
            name="unknown.bin",
            mode=0,
            timestamp=self.TEST_TIME,
            size=100,
            c4id=self.TEST_ID,
        )
        assert e.canonical() == (
            "- 2024-01-15T10:30:00Z 100 unknown.bin "
            "c41j3C6Jqga95PL2zmZVBWixAUhoWDNmwamiWiNTDAMRL1UWqe4WdtYjSozRijRSokEsaTnYyxoCBt43u4sfqWG2uB"
        )

    def test_null_mode_on_directory(self):
        """Null mode on directory entry still renders as '-' in canonical."""
        e = Entry(
            name="data/",
            mode=0,
            timestamp=self.TEST_TIME,
            size=NULL_SIZE,
        )
        assert e.is_dir(), "trailing slash makes it a directory"
        assert e.has_null_mode(), "mode 0 is null"
        assert e.canonical() == "- 2024-01-15T10:30:00Z - data/ -"

    def test_null_mode_on_directory_format(self):
        """Null mode on directory entry renders as '-' in format (canonical with indent)."""
        e = Entry(
            name="data/",
            mode=0,
            timestamp=self.TEST_TIME,
            size=NULL_SIZE,
        )
        assert e.format() == "- 2024-01-15T10:30:00Z - data/ -"

    def test_null_size(self):
        e = Entry(
            name="partial.txt",
            mode=0o644,
            timestamp=self.TEST_TIME,
            size=-1,
            c4id=self.TEST_ID,
        )
        assert e.canonical() == (
            "-rw-r--r-- 2024-01-15T10:30:00Z - partial.txt "
            "c41j3C6Jqga95PL2zmZVBWixAUhoWDNmwamiWiNTDAMRL1UWqe4WdtYjSozRijRSokEsaTnYyxoCBt43u4sfqWG2uB"
        )

    def test_all_null_values(self):
        e = Entry(
            name="mystery.dat",
            mode=0,
            timestamp=NULL_TIMESTAMP,
            size=-1,
            c4id=self.TEST_ID,
        )
        assert e.canonical() == (
            "- - - mystery.dat "
            "c41j3C6Jqga95PL2zmZVBWixAUhoWDNmwamiWiNTDAMRL1UWqe4WdtYjSozRijRSokEsaTnYyxoCBt43u4sfqWG2uB"
        )

    def test_null_c4id(self):
        e = Entry(
            name="test.txt",
            mode=0o644,
            timestamp=self.TEST_TIME,
            size=100,
        )
        assert e.canonical() == "-rw-r--r-- 2024-01-15T10:30:00Z 100 test.txt -"

    def test_hard_link_ungrouped(self):
        e = Entry(
            name="link.txt",
            mode=0o644,
            timestamp=self.TEST_TIME,
            size=100,
            hard_link=-1,
        )
        assert e.canonical() == "-rw-r--r-- 2024-01-15T10:30:00Z 100 link.txt -> -"

    def test_hard_link_grouped(self):
        e = Entry(
            name="link.txt",
            mode=0o644,
            timestamp=self.TEST_TIME,
            size=100,
            hard_link=3,
        )
        assert e.canonical() == "-rw-r--r-- 2024-01-15T10:30:00Z 100 link.txt ->3 -"

    def test_flow_link_outbound(self):
        e = Entry(
            name="outbox/",
            mode=stat.S_IFDIR | 0o755,
            timestamp=self.TEST_TIME,
            size=4096,
            flow_direction=FlowDirection.OUTBOUND,
            flow_target="studio:inbox/",
        )
        assert e.canonical() == (
            "drwxr-xr-x 2024-01-15T10:30:00Z 4096 outbox/ -> studio:inbox/ -"
        )
