"""C4M encoder — write Manifest objects as c4m text.

Two modes:
- Canonical: no padding, single spaces, UTC timestamps, for C4 ID computation
- Pretty: column-aligned, local timestamps, padded sizes, human-readable

Reference: github.com/Avalanche-io/c4/c4m/encoder.go
"""

from __future__ import annotations

from datetime import datetime
from typing import IO

from .entry import (
    NULL_TIMESTAMP,
    Entry,
    FlowDirection,
    format_mode,
    format_name,
    format_target,
)
from .manifest import Manifest


def dump(manifest: Manifest, dest: str | IO[str], *, pretty: bool = False) -> None:
    """Write a manifest to a file path or text stream.

    Args:
        manifest: the manifest to write
        dest: file path (str) or text-mode file object
        pretty: if True, use ergonomic formatting
    """
    text = dumps(manifest, pretty=pretty)
    if isinstance(dest, str):
        with open(dest, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        dest.write(text)


def dumps(manifest: Manifest, *, pretty: bool = False) -> str:
    """Encode a manifest as c4m text.

    Args:
        manifest: the manifest to encode
        pretty: if True, use ergonomic formatting (padded, local time)

    Returns:
        c4m text (UTF-8, LF line endings)

    Canonical format rules:
    - No indentation
    - Single space between fields
    - No padding
    - UTC timestamps with Z suffix
    - Null values as '-'
    - Natural sort order

    Pretty format:
    - Indented (2 spaces per level)
    - Sizes right-aligned and padded
    - C4 IDs column-aligned (col 80, or next 10-col boundary)
    - Local timestamps

    Reference: github.com/Avalanche-io/c4/c4m/encoder.go
    """
    # Sort a copy to avoid mutating the caller's manifest
    m = manifest.copy()
    m.sort_entries()

    indent_width = 2

    if not pretty:
        # Canonical mode
        lines: list[str] = []
        for entry in m.entries:
            lines.append(entry.format(indent_width, pretty=False))
            lines.append("\n")
        return "".join(lines)

    # Pretty mode
    # Calculate max size for padding
    max_size = 0
    for entry in m.entries:
        if entry.size > max_size:
            max_size = entry.size

    # Calculate C4 ID column
    c4id_column = _calculate_c4id_column(m, indent_width, max_size)

    lines = []
    for entry in m.entries:
        line = _format_entry_pretty(entry, indent_width, max_size, c4id_column)
        lines.append(line)
        lines.append("\n")
    return "".join(lines)


def _calculate_c4id_column(m: Manifest, indent_width: int, max_size: int) -> int:
    """Determine the appropriate column for C4 ID alignment."""
    max_size_width = len(_format_size_with_commas(max_size))

    max_len = 0
    for entry in m.entries:
        indent = " " * (entry.depth * indent_width)
        mode_str = format_mode(entry.mode)
        time_str = _format_timestamp_pretty(entry.timestamp)
        name_str = format_name(entry.name, entry.is_sequence)

        line_len = (
            len(indent) + len(mode_str) + 1 + len(time_str) + 1
            + max_size_width + 1 + len(name_str)
        )

        if entry.target:
            line_len += 4 + len(format_target(entry.target))  # " -> " + target
        elif entry.flow_direction != FlowDirection.NONE:
            line_len += 1 + len(entry.flow_operator()) + 1 + len(entry.flow_target)

        if line_len > max_len:
            max_len = line_len

    # Start at column 80, shift by 10 if needed
    min_spacing = 10
    column = 80
    while max_len + min_spacing > column:
        column += 10
    return column


def _format_entry_pretty(
    entry: Entry,
    indent_width: int,
    max_size: int,
    c4id_column: int,
) -> str:
    """Format an entry with ergonomic pretty-printing."""
    indent = " " * (entry.depth * indent_width)

    # Format mode (handle null value)
    if entry.mode == 0:
        mode_str = "----------"
    else:
        mode_str = format_mode(entry.mode)

    # Format timestamp
    if entry.timestamp == NULL_TIMESTAMP:
        # Null timestamp padded to match typical timestamp width
        time_str = "-                        "
    else:
        time_str = _format_timestamp_pretty(entry.timestamp)

    # Format size with padding and commas
    if entry.size < 0:
        max_size_str = _format_size_with_commas(max_size)
        padding = len(max_size_str) - 1
        size_str = " " * padding + "-"
    else:
        size_str = _format_size_pretty(entry.size, max_size)

    # Format name
    name_str = format_name(entry.name, entry.is_sequence)

    # Build base line
    parts = [indent + mode_str, time_str, size_str, name_str]

    # Add link fields
    if entry.target:
        parts.extend(["->", format_target(entry.target)])
    elif entry.hard_link != 0:
        if entry.hard_link < 0:
            parts.append("->")
        else:
            parts.append(f"->{entry.hard_link}")
    elif entry.flow_direction != FlowDirection.NONE:
        parts.extend([entry.flow_operator(), entry.flow_target])

    base_line = " ".join(parts)

    # C4 ID or "-" always last, aligned to column
    padding = c4id_column - len(base_line)
    if padding < 10:
        padding = 10

    if entry.c4id is not None and not entry.c4id.is_nil():
        return base_line + " " * padding + str(entry.c4id)
    return base_line + " " * padding + "-"


def _format_timestamp_pretty(ts: datetime) -> str:
    """Format timestamp in human-readable format with timezone.

    Format: 'Jan _2 15:04:05 2006 MST' (similar to ls -lT)
    """
    local_ts = ts.astimezone()
    # Python's %b gives abbreviated month, %Z gives timezone abbreviation
    # Use %d (portable) and strip leading zero to match `ls -lT` style
    day = local_ts.strftime("%d").lstrip("0").rjust(2)
    return local_ts.strftime(f"%b {day} %H:%M:%S %Y %Z")


def _format_size_pretty(size: int, max_size: int) -> str:
    """Format size with padding and thousand separators."""
    size_with_commas = _format_size_with_commas(size)
    max_size_str = _format_size_with_commas(max_size)
    padding = len(max_size_str) - len(size_with_commas)
    return " " * padding + size_with_commas


def _format_size_with_commas(size: int) -> str:
    """Add thousand separators to a number."""
    return f"{size:,}"
