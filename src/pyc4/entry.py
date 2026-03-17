"""C4M entry — one line in a c4m file representing a file, directory, or link.

Entry format: <mode> <timestamp> <size> <name> [link-operator target] <c4id>

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/entry.go
           /Users/joshua/ws/active/c4/oss/c4/c4m/SPECIFICATION.md
"""

from __future__ import annotations

import os
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
        return self.name.endswith("/") or bool(self.mode & stat.S_IFDIR)

    def is_symlink(self) -> bool:
        """True if this entry is a symbolic link."""
        return bool(self.mode & stat.S_IFLNK == stat.S_IFLNK)

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

    def canonical(self) -> str:
        """Format this entry in canonical form (for C4 ID computation).

        Canonical form: no indentation, single space between fields,
        null values as '-', UTC timestamps with Z suffix.
        """
        parts = [
            format_mode(self.mode),
            format_timestamp(self.timestamp),
            format_size(self.size),
            self.name,  # TODO: apply SafeName + field-boundary escaping
        ]

        # Link operators between name and C4 ID
        # TODO: implement link operator formatting

        # C4 ID (always last field)
        if self.c4id is not None:
            parts.append(str(self.c4id))
        else:
            parts.append("-")

        return " ".join(parts)

    def format(self, indent_width: int = 2, pretty: bool = False) -> str:
        """Format this entry for output.

        Args:
            indent_width: spaces per indentation level
            pretty: if True, use ergonomic formatting (local time, padding)
        """
        indent = " " * (self.depth * indent_width)
        if pretty:
            # TODO: implement pretty formatting
            return indent + self.canonical()
        return indent + self.canonical()


def format_mode(mode: int) -> str:
    """Format a file mode as a 10-character Unix mode string.

    Returns '-' for null mode (0).
    """
    if mode == 0:
        return "-"

    # TODO: implement full mode formatting
    # Position 0: file type (-, d, l, p, s, b, c)
    # Positions 1-9: rwx permissions with setuid/setgid/sticky
    raise NotImplementedError("mode formatting not yet implemented")


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
