"""C4M entry — one line in a c4m file representing a file, directory, or link.

Entry format: <mode> <timestamp> <size> <name> [link-operator target] <c4id>

Reference: github.com/Avalanche-io/c4/c4m/entry.go
           github.com/Avalanche-io/c4/c4m/SPECIFICATION.md
"""

from __future__ import annotations

import re
import stat
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from .id import C4ID


class FlowDirection(Enum):
    """Direction of a flow link."""
    NONE = ""
    OUTBOUND = "->"    # content here propagates there
    INBOUND = "<-"     # content there propagates here
    BIDIRECTIONAL = "<>"  # two-way sync


# Sentinel for null timestamp (Unix epoch)
NULL_TIMESTAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Sentinel for null size
NULL_SIZE = -1

# Sequence pattern: matches [0001-0100], [01-50,75-100], [001,005,010], etc.
SEQUENCE_PATTERN = re.compile(r"\[([0-9,\-:]+)\]")


@dataclass
class Entry:
    """A single entry in a c4m manifest.

    Represents a file, directory, symlink, or other filesystem entity
    with its metadata and C4 content identifier.
    """

    name: str
    mode: int = 0  # os.FileMode compatible (0 = null)
    timestamp: datetime = field(default_factory=lambda: NULL_TIMESTAMP)
    size: int = NULL_SIZE  # -1 = null
    c4id: Optional[C4ID] = None
    depth: int = 0

    # Link fields
    target: str = ""  # symlink target
    hard_link: int = 0  # 0=none, -1=ungrouped, >0=group
    flow_direction: FlowDirection = FlowDirection.NONE
    flow_target: str = ""  # "location:path"

    # Sequence fields
    is_sequence: bool = False
    pattern: str = ""  # original sequence pattern

    def is_dir(self) -> bool:
        """True if this entry is a directory."""
        return self.name.endswith("/") or (self.mode != 0 and stat.S_ISDIR(self.mode))

    def is_symlink(self) -> bool:
        """True if this entry is a symbolic link."""
        return self.mode != 0 and stat.S_ISLNK(self.mode)

    def is_flow_linked(self) -> bool:
        """True if this entry has a flow link."""
        return self.flow_direction != FlowDirection.NONE

    def has_null_mode(self) -> bool:
        return self.mode == 0

    def has_null_timestamp(self) -> bool:
        return self.timestamp == NULL_TIMESTAMP

    def has_null_size(self) -> bool:
        return self.size == NULL_SIZE

    def has_null_c4id(self) -> bool:
        return self.c4id is None

    def flow_operator(self) -> str:
        """Return the string representation of the flow direction."""
        return self.flow_direction.value

    def human_size(self) -> str:
        """Human-readable size string (e.g. '4.2 GB', '1.3 KB')."""
        if self.size == NULL_SIZE:
            return "-"
        return _human_size(self.size)

    def canonical(self) -> str:
        """Format this entry in canonical form (for C4 ID computation).

        Canonical form: no indentation, single space between fields,
        null values as '-', UTC timestamps with Z suffix.
        In canonical mode, null mode is '-' (single char), not '----------'.
        """
        # Mode: null mode is always "-" in canonical form, regardless of
        # whether the entry is a directory (trailing /) or anything else.
        if self.mode == 0:
            mode_str = "-"
        else:
            mode_str = format_mode(self.mode)

        # Timestamp
        ts_str = format_timestamp(self.timestamp)

        # Size: plain integer, no commas
        size_str = format_size(self.size)

        # Name with SafeName + field-boundary escaping
        name_str = format_name(self.name, self.is_sequence)

        parts = [mode_str, ts_str, size_str, name_str]

        # Link operators between name and C4 ID
        if self.target:
            parts.extend(["->", format_target(self.target)])
        elif self.hard_link != 0:
            if self.hard_link < 0:
                parts.append("->")
            else:
                parts.append(f"->{self.hard_link}")
        elif self.flow_direction != FlowDirection.NONE:
            parts.extend([self.flow_operator(), self.flow_target])

        # C4 ID (always last field)
        if self.c4id is not None and not self.c4id.is_nil():
            parts.append(str(self.c4id))
        else:
            parts.append("-")

        return " ".join(parts)

    def format(self, indent_width: int = 2, pretty: bool = False) -> str:
        """Format this entry for non-pretty output (canonical with indentation).

        Args:
            indent_width: spaces per indentation level
            pretty: if True, use ergonomic formatting (handled by encoder)
        """
        indent = " " * (self.depth * indent_width)

        # Mode: null mode is "-" in format (canonical with indentation)
        if self.mode == 0:
            mode_str = "-"
        else:
            mode_str = format_mode(self.mode)

        # Timestamp
        ts_str = format_timestamp(self.timestamp)

        # Size
        size_str = format_size(self.size)

        # Name
        name_str = format_name(self.name, self.is_sequence)

        parts = [indent + mode_str, ts_str, size_str, name_str]

        # Link operators
        if self.target:
            parts.extend(["->", format_target(self.target)])
        elif self.hard_link != 0:
            if self.hard_link < 0:
                parts.append("->")
            else:
                parts.append(f"->{self.hard_link}")
        elif self.flow_direction != FlowDirection.NONE:
            parts.extend([self.flow_operator(), self.flow_target])

        # C4 ID
        if self.c4id is not None and not self.c4id.is_nil():
            parts.append(str(self.c4id))
        else:
            parts.append("-")

        return " ".join(parts)


def format_mode(mode: int) -> str:
    """Format a file mode as a 10-character Unix mode string.

    Returns '-' for null mode (0) in canonical context.
    The caller decides whether to use '-' or '----------'.
    """
    if mode == 0:
        return "----------"

    buf = ["-"] * 10

    # File type (position 0)
    # Python stat module uses different constants than Go os.FileMode.
    # Go's os.ModeDir = 1<<31, os.ModeSymlink = 1<<27, etc.
    # Python's stat.S_IFDIR = 0o40000, stat.S_IFLNK = 0o120000, etc.
    fmt_bits = stat.S_IFMT(mode)
    if fmt_bits == stat.S_IFDIR:
        buf[0] = "d"
    elif fmt_bits == stat.S_IFLNK:
        buf[0] = "l"
    elif fmt_bits == stat.S_IFIFO:
        buf[0] = "p"
    elif fmt_bits == stat.S_IFSOCK:
        buf[0] = "s"
    elif fmt_bits == stat.S_IFBLK:
        buf[0] = "b"
    elif fmt_bits == stat.S_IFCHR:
        buf[0] = "c"
    # else regular file: '-'

    # Permission bits (positions 1-9)
    rwx = "rwxrwxrwx"
    for i in range(9):
        if mode & (1 << (8 - i)):
            buf[i + 1] = rwx[i]

    # Special bits
    if mode & stat.S_ISUID:
        buf[3] = "s" if buf[3] == "x" else "S"
    if mode & stat.S_ISGID:
        buf[6] = "s" if buf[6] == "x" else "S"
    if mode & stat.S_ISVTX:
        buf[9] = "t" if buf[9] == "x" else "T"

    return "".join(buf)


def parse_mode(s: str) -> int:
    """Parse a 10-character Unix mode string to a file mode integer.

    Accepts '-' or '----------' for null mode (returns 0).
    """
    if s == "-" or s == "----------":
        return 0

    if len(s) != 10:
        raise ValueError(f"mode must be 10 characters, got {len(s)}")

    mode = 0

    # File type
    type_char = s[0]
    if type_char == "-":
        pass  # regular file
    elif type_char == "d":
        mode |= stat.S_IFDIR
    elif type_char == "l":
        mode |= stat.S_IFLNK
    elif type_char == "p":
        mode |= stat.S_IFIFO
    elif type_char == "s":
        mode |= stat.S_IFSOCK
    elif type_char == "b":
        mode |= stat.S_IFBLK
    elif type_char == "c":
        mode |= stat.S_IFCHR
    else:
        raise ValueError(f"unknown file type: {type_char}")

    # Permission bits
    perm_chars = s[1:]

    # User
    if perm_chars[0] == "r":
        mode |= 0o400
    if perm_chars[1] == "w":
        mode |= 0o200
    if perm_chars[2] in ("x", "s"):
        mode |= 0o100

    # Group
    if perm_chars[3] == "r":
        mode |= 0o040
    if perm_chars[4] == "w":
        mode |= 0o020
    if perm_chars[5] in ("x", "s"):
        mode |= 0o010

    # Other
    if perm_chars[6] == "r":
        mode |= 0o004
    if perm_chars[7] == "w":
        mode |= 0o002
    if perm_chars[8] in ("x", "t"):
        mode |= 0o001

    # Special bits
    if perm_chars[2] in ("s", "S"):
        mode |= stat.S_ISUID
    if perm_chars[5] in ("s", "S"):
        mode |= stat.S_ISGID
    if perm_chars[8] in ("t", "T"):
        mode |= stat.S_ISVTX

    return mode


def format_timestamp(ts: datetime) -> str:
    """Format a timestamp in canonical form (YYYY-MM-DDTHH:MM:SSZ).

    Returns '-' for null timestamp (Unix epoch).
    """
    if ts == NULL_TIMESTAMP:
        return "-"
    utc = ts.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_size(size: int) -> str:
    """Format a file size as a decimal string.

    Returns '-' for null size (-1).
    """
    if size == NULL_SIZE:
        return "-"
    return str(size)


def format_name(name: str, is_sequence: bool = False) -> str:
    """Format a name with SafeName encoding and c4m field-boundary escaping.

    Reference: Go's formatName in entry.go
    """
    from .safename import safe_name

    safe = safe_name(name)

    if is_sequence:
        return _format_sequence_name(name)

    if safe.endswith("/"):
        base = safe[:-1]
        return _escape_c4m_name(base, is_sequence=False) + "/"

    return _escape_c4m_name(safe, is_sequence=False)


def _escape_c4m_name(s: str, is_sequence: bool = False) -> str:
    """Backslash-escape characters unsafe in c4m field-boundary context."""
    needs_escape = " " in s or '"' in s
    if not is_sequence and ("[" in s or "]" in s):
        needs_escape = True
    if not needs_escape:
        return s

    parts: list[str] = []
    for ch in s:
        if ch == " ":
            parts.append("\\ ")
        elif ch == '"':
            parts.append('\\"')
        elif ch == "[" and not is_sequence:
            parts.append("\\[")
        elif ch == "]" and not is_sequence:
            parts.append("\\]")
        else:
            parts.append(ch)
    return "".join(parts)


def _format_sequence_name(name: str) -> str:
    """Format a sequence name, escaping prefix/suffix but leaving range notation."""
    match = SEQUENCE_PATTERN.search(name)
    if match is None:
        return name
    prefix = name[:match.start()]
    range_part = name[match.start():match.end()]
    suffix = name[match.end():]
    return _escape_sequence_notation(prefix) + range_part + _escape_sequence_notation(suffix)


def _escape_sequence_notation(s: str) -> str:
    """Escape c4m-specific syntax characters in sequence prefix/suffix."""
    from .safename import safe_name

    safe = safe_name(s)
    parts: list[str] = []
    for ch in safe:
        if ch == " ":
            parts.append("\\ ")
        elif ch == '"':
            parts.append('\\"')
        elif ch == "[":
            parts.append("\\[")
        elif ch == "]":
            parts.append("\\]")
        else:
            parts.append(ch)
    return "".join(parts)


def _human_size(size: int) -> str:
    """Format a byte count as a human-readable string (e.g. '4.2 GB')."""
    s = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(s) < 1024:
            if unit == "B":
                return f"{int(s)} {unit}"
            return f"{s:.1f} {unit}"
        s /= 1024
    return f"{s:.1f} EB"


def format_target(target: str) -> str:
    """Backslash-escape a symlink target for c4m output.

    Unlike format_name, targets don't get bracket escaping.
    """
    from .safename import safe_name

    safe = safe_name(target)
    if " " not in safe and '"' not in safe:
        return safe

    parts: list[str] = []
    for ch in safe:
        if ch == " ":
            parts.append("\\ ")
        elif ch == '"':
            parts.append('\\"')
        else:
            parts.append(ch)
    return "".join(parts)
