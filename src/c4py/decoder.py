"""C4M decoder — parse c4m text into Manifest objects.

The parser is lenient: it accepts ergonomic format variations (padded sizes,
local timestamps, varying indentation) and normalizes internally. Only
canonical form is required for C4 ID computation.

The decoder must understand:
- **Patch boundaries**: A line containing exactly one 90-char C4 ID (starting
  with "c4") separates patch sections. First = external base ref, subsequent =
  checkpoints.
- **Inline ID lists**: Lines longer than 90 chars where length is a multiple of
  90 and every 90-char chunk is a valid C4 ID. These are range data (concatenated
  IDs for sequence entries) and must be skipped during patch chain parsing — they
  are NOT patch boundaries. Distinguishing rule: bare C4 ID = exactly 90 chars;
  inline ID list = >90 chars and divisible by 90.
- **No directives**: Lines starting with @ are rejected.

Reference: github.com/Avalanche-io/c4/c4m/decoder.go
           github.com/Avalanche-io/c4/c4m/chain.go
           github.com/Avalanche-io/c4/c4m/SPECIFICATION.md
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import IO

from .entry import (
    NULL_SIZE,
    NULL_TIMESTAMP,
    SEQUENCE_PATTERN,
    Entry,
    FlowDirection,
    parse_mode,
)
from .id import C4_ID_LENGTH, C4ID, parse
from .manifest import Manifest

# C4 ID pattern: c4 followed by 88 base58 characters
_C4ID_PATTERN = re.compile(r"^c4[1-9A-HJ-NP-Za-km-z]{88}$")


def load(source: str | IO[str]) -> Manifest:
    """Load a c4m manifest from a file path or text stream.

    Args:
        source: file path (str) or text-mode file object
    """
    if isinstance(source, str):
        with open(source, encoding="utf-8") as f:
            return loads(f.read())
    return loads(source.read())


def loads(text: str) -> Manifest:
    """Parse c4m text into a Manifest.

    Parser rules:
    - UTF-8 only, no BOM
    - LF line endings only (CR forbidden)
    - Blank lines ignored
    - Lines starting with @ rejected
    - Indentation: spaces only, auto-detected width
    - Fields: mode timestamp size name [link-op target] c4id
    - Null values: '-' for mode, timestamp, size, c4id

    Reference: github.com/Avalanche-io/c4/c4m/decoder.go
    """
    decoder = _Decoder()
    return decoder.decode(text)


class _Decoder:
    """Internal stateful decoder for c4m text."""

    def __init__(self) -> None:
        self.line_num = 0
        self.indent_width = -1  # auto-detected

    def decode(self, text: str) -> Manifest:
        """Decode c4m text into a Manifest."""
        m = Manifest()
        section: list[Entry] = []
        first_line = True
        patch_mode = False

        lines = text.split("\n")

        for line in lines:
            self.line_num += 1

            # Reject CR
            if "\r" in line:
                raise ValueError(
                    f"line {self.line_num}: CR (0x0D) not allowed — "
                    "c4m requires LF-only line endings"
                )

            trimmed = line.strip()

            # Skip blank lines
            if not trimmed:
                continue

            # Check for inline ID list (>90 chars, multiple of 90, all valid C4 IDs)
            if _is_inline_id_list(trimmed):
                # Range data — store keyed by C4 ID of the line itself
                from .id import identify_bytes
                line_id = identify_bytes(trimmed.encode("utf-8"))
                m._range_data[line_id] = trimmed
                continue

            # Check for bare C4 ID line (exactly 90 chars starting with "c4")
            if _is_bare_c4id(trimmed):
                try:
                    c4id = parse(trimmed)
                except ValueError as exc:
                    raise ValueError(
                        f"line {self.line_num}: invalid C4 ID: {exc}"
                    ) from exc

                if first_line and not section:
                    # First line of file: external base reference
                    m.base = c4id
                else:
                    # Reject empty patch sections
                    if patch_mode and not section:
                        raise ValueError(
                            f"empty patch section (line {self.line_num})"
                        )

                    # Flush current section
                    if not patch_mode:
                        m.entries.extend(section)
                    else:
                        patch = Manifest(entries=section)
                        m = _apply_patch(m, patch)
                    section = []

                    # The bare C4 ID is a block link (ID of previous block).
                    # Recorded as a boundary marker but not verified — O(1).
                    patch_mode = True

                first_line = False
                continue

            # Reject directive lines
            if trimmed.startswith("@"):
                raise ValueError(
                    f"directives not supported (line {self.line_num}): {line}"
                )

            # Parse as a normal entry
            entry = self._parse_entry_from_line(line)
            if entry is not None:
                section.append(entry)
            first_line = False

        # Flush remaining section
        if not patch_mode:
            m.entries.extend(section)
        elif section:
            patch = Manifest(entries=section)
            m = _apply_patch(m, patch)
        elif patch_mode:
            raise ValueError("empty patch section (at end of input)")

        return m

    def _parse_entry_from_line(self, line: str) -> Entry | None:
        """Parse a manifest entry from a line."""
        # Detect indentation
        indent = 0
        for ch in line:
            if ch != " ":
                break
            indent += 1

        # Detect indent width from first indented line
        if self.indent_width == -1 and indent > 0:
            self.indent_width = indent

        depth = 0
        if self.indent_width > 0:
            depth = indent // self.indent_width

        # Trim indentation
        line = line.lstrip(" ")

        # Parse mode
        if line.startswith("- "):
            # Single dash null mode
            mode_str = "-"
            line = line[2:]
        elif len(line) >= 11:
            mode_str = line[:10]
            line = line[11:]
        else:
            raise ValueError(
                f"line {self.line_num}: line too short"
            )

        mode = parse_mode(mode_str)

        entry = Entry(name="", mode=mode, depth=depth)

        # Parse timestamp
        ts_str, line = self._parse_timestamp_field(line)
        entry.timestamp = _parse_timestamp(ts_str)

        # Parse remaining fields: size, name, [link-op target], [c4id]
        size, name, raw_name, target, c4id, hard_link, flow_dir, flow_target = (
            self._parse_entry_fields(line, mode)
        )

        from .safename import unsafe_name

        entry.size = size
        entry.name = unsafe_name(name)
        entry.target = unsafe_name(target) if target else ""
        entry.c4id = c4id
        entry.hard_link = hard_link
        entry.flow_direction = flow_dir
        entry.flow_target = flow_target

        # Check for sequence notation in the RAW name (before unescaping)
        if _has_unescaped_sequence_notation(raw_name):
            entry.is_sequence = True
            entry.pattern = entry.name

        return entry

    def _parse_timestamp_field(self, line: str) -> tuple[str, str]:
        """Extract timestamp from the beginning of line.

        Returns (timestamp_string, remaining_line).
        """
        # Check for null timestamp
        if line.startswith("- ") or line.startswith("0 "):
            return line[0], line[2:]

        # Check for canonical/RFC3339 timestamp
        if len(line) >= 20 and line[4] == "-" and line[10] == "T":
            end_idx = 20
            if len(line) >= 25 and line[19] in ("-", "+"):
                end_idx = 25
            ts_str = line[:end_idx]
            remaining = line[end_idx + 1:] if len(line) > end_idx else ""
            return ts_str, remaining

        # Try pretty format (e.g., "Sep  1 00:36:18 2025 CDT")
        parts = line.split()
        if len(parts) >= 5:
            ts_str = " ".join(parts[:5])
            remaining = " ".join(parts[5:])
            return ts_str, remaining

        raise ValueError(
            f"line {self.line_num}: cannot parse timestamp from {line!r}"
        )

    def _parse_entry_fields(
        self,
        line: str,
        mode: int,
    ) -> tuple[int, str, str, str, C4ID | None, int, FlowDirection, str]:
        """Parse remaining fields after timestamp: SIZE NAME [LINK_OP TARGET] [C4ID].

        Returns (size, name, raw_name, target, c4id, hard_link, flow_dir, flow_target).
        """
        import stat as stat_mod

        pos = 0
        n = len(line)

        # Skip leading whitespace
        while pos < n and line[pos] == " ":
            pos += 1
        if pos >= n:
            raise ValueError(f"line {self.line_num}: insufficient fields after timestamp")

        # 1. Parse size token
        size: int
        if line[pos] == "-":
            size = NULL_SIZE
            pos += 1
        else:
            size_start = pos
            while pos < n and (line[pos].isdigit() or line[pos] == ","):
                pos += 1
            if pos == size_start:
                raise ValueError(f"line {self.line_num}: invalid size at position {pos}")
            size_str = line[size_start:pos].replace(",", "")
            size = int(size_str)

        # Skip whitespace
        while pos < n and line[pos] == " ":
            pos += 1
        if pos >= n:
            raise ValueError(f"line {self.line_num}: missing name after size")

        # 2. Parse name
        name_start = pos
        name, pos, has_unescaped_brackets = self._parse_name_or_target(line, pos)
        raw_name = line[name_start:pos]

        # Skip whitespace
        while pos < n and line[pos] == " ":
            pos += 1

        target = ""
        hard_link = 0
        flow_dir = FlowDirection.NONE
        flow_target = ""
        c4id: C4ID | None = None
        is_symlink = mode != 0 and stat_mod.S_ISLNK(mode)

        # 3. Check for link operator: ->, <-, or <>
        if pos + 1 < n and line[pos] == "-" and line[pos + 1] == ">":
            pos += 2

            if is_symlink:
                # Symlink mode: -> is always a symlink target
                while pos < n and line[pos] == " ":
                    pos += 1
                if pos < n:
                    target, pos, _err = self._parse_target(line, pos)
                    while pos < n and line[pos] == " ":
                        pos += 1
            elif pos < n and line[pos].isdigit() and line[pos] != "0":
                # Hard link group number: ->N
                group_start = pos
                while pos < n and line[pos].isdigit():
                    pos += 1
                hard_link = int(line[group_start:pos])
                while pos < n and line[pos] == " ":
                    pos += 1
            else:
                # Skip whitespace after ->
                while pos < n and line[pos] == " ":
                    pos += 1

                # Determine type by examining token after ->
                if pos < n and _is_flow_target(line[pos:]):
                    flow_dir = FlowDirection.OUTBOUND
                    flow_target, pos = self._parse_flow_target(line, pos)
                    while pos < n and line[pos] == " ":
                        pos += 1
                elif pos < n:
                    remaining = line[pos:].strip()
                    if remaining == "-" or remaining.startswith("c4"):
                        # Hard link (ungrouped)
                        hard_link = -1
                    else:
                        # Fallback: symlink target
                        target, pos, _err = self._parse_target(line, pos)
                        while pos < n and line[pos] == " ":
                            pos += 1

        elif pos + 1 < n and line[pos] == "<" and line[pos + 1] == "-":
            pos += 2
            while pos < n and line[pos] == " ":
                pos += 1
            flow_dir = FlowDirection.INBOUND
            flow_target, pos = self._parse_flow_target(line, pos)
            while pos < n and line[pos] == " ":
                pos += 1

        elif pos + 1 < n and line[pos] == "<" and line[pos + 1] == ">":
            pos += 2
            while pos < n and line[pos] == " ":
                pos += 1
            flow_dir = FlowDirection.BIDIRECTIONAL
            flow_target, pos = self._parse_flow_target(line, pos)
            while pos < n and line[pos] == " ":
                pos += 1

        # 4. Check for C4 ID or null ("-")
        if pos < n:
            remaining = line[pos:].strip()
            if remaining == "-":
                c4id = None
            elif remaining.startswith("c4"):
                try:
                    c4id = parse(remaining)
                except ValueError as exc:
                    raise ValueError(
                        f"line {self.line_num}: invalid C4 ID {remaining!r}: {exc}"
                    ) from exc

        return size, name, raw_name, target, c4id, hard_link, flow_dir, flow_target

    def _parse_name_or_target(
        self, line: str, pos: int
    ) -> tuple[str, int, bool]:
        """Parse a backslash-escaped name starting at pos.

        Returns (parsed_name, new_pos, has_unescaped_brackets).

        Boundary detection:
        - Directory names end at / (inclusive)
        - File names end at space followed by ->, <-, <>, c4 prefix, or -
        """
        n = len(line)
        if pos >= n:
            raise ValueError(f"line {self.line_num}: unexpected end of line")

        buf: list[str] = []
        has_unescaped_brackets = False

        while pos < n:
            ch = line[pos]

            # c4m field-boundary escapes
            if ch == "\\" and pos + 1 < n:
                nxt = line[pos + 1]
                if nxt in (" ", '"', "[", "]"):
                    buf.append(nxt)
                    pos += 2
                    continue

            if ch in ("[", "]"):
                has_unescaped_brackets = True

            # Directory name ends at / (inclusive)
            if ch == "/":
                buf.append("/")
                pos += 1
                return "".join(buf), pos, has_unescaped_brackets

            # Check for boundary: space followed by link operator, c4 prefix, or -
            if ch == " ":
                rest = line[pos:]
                if (rest.startswith(" -> ")
                        or rest.startswith(" <- ")
                        or rest.startswith(" <> ")):
                    return "".join(buf), pos, has_unescaped_brackets
                # Hard link group: " ->N" where N is digit 1-9
                if (len(rest) >= 4
                        and rest[1] == "-" and rest[2] == ">"
                        and rest[3].isdigit() and rest[3] != "0"):
                    return "".join(buf), pos, has_unescaped_brackets
                if len(rest) > 2 and rest[1] == "c" and rest[2] == "4":
                    return "".join(buf), pos, has_unescaped_brackets
                if len(rest) >= 2 and rest[1] == "-" and (len(rest) == 2 or rest[2] == " "):
                    return "".join(buf), pos, has_unescaped_brackets

            buf.append(ch)
            pos += 1

        return "".join(buf), pos, has_unescaped_brackets

    def _parse_target(self, line: str, pos: int) -> tuple[str, int, None]:
        """Parse a symlink target starting at pos.

        Unlike _parse_name_or_target, does NOT treat / as boundary.
        """
        n = len(line)
        if pos >= n:
            return "", pos, None

        buf: list[str] = []
        while pos < n:
            ch = line[pos]

            # Backslash escapes
            if ch == "\\" and pos + 1 < n:
                nxt = line[pos + 1]
                if nxt in (" ", '"'):
                    buf.append(nxt)
                    pos += 2
                    continue

            if ch == " ":
                rest = line[pos:]
                if len(rest) > 2 and rest[1] == "c" and rest[2] == "4":
                    return "".join(buf), pos, None
                if len(rest) >= 2 and rest[1] == "-" and (len(rest) == 2 or rest[2] == " "):
                    return "".join(buf), pos, None

            buf.append(ch)
            pos += 1

        return "".join(buf), pos, None

    def _parse_flow_target(self, line: str, pos: int) -> tuple[str, int]:
        """Parse a flow target (location:path) starting at pos."""
        n = len(line)
        if pos >= n:
            raise ValueError(f"line {self.line_num}: expected flow target")
        start = pos
        while pos < n:
            ch = line[pos]
            if ch == " ":
                rest = line[pos:]
                if len(rest) > 2 and rest[1] == "c" and rest[2] == "4":
                    return line[start:pos], pos
                if len(rest) >= 2 and rest[1] == "-" and (len(rest) == 2 or rest[2] == " "):
                    return line[start:pos], pos
            pos += 1
        return line[start:pos], pos


def _is_bare_c4id(s: str) -> bool:
    """True if the string is exactly a C4 ID (90 chars starting with 'c4')."""
    return len(s) == C4_ID_LENGTH and s[0] == "c" and s[1] == "4"


def _is_inline_id_list(s: str) -> bool:
    """True if the line is a bare-concatenated ID list (>90 chars, multiple of 90)."""
    n = len(s)
    if n <= 90 or n % 90 != 0 or s[0] != "c" or s[1] != "4":
        return False
    for i in range(0, n, 90):
        if not _C4ID_PATTERN.match(s[i:i + 90]):
            return False
    return True


def _is_flow_target(s: str) -> bool:
    """True if text matches flow target pattern: label followed by ':'."""
    if not s or not s[0].isalpha():
        return False
    for i in range(1, len(s)):
        c = s[i]
        if c == ":":
            return True
        if c == " ":
            return False
        if not (c.isalnum() or c in ("_", "-")):
            return False
    return False


def _has_unescaped_sequence_notation(raw: str) -> bool:
    """Check if raw text contains sequence notation with unescaped brackets."""
    # Replace escape sequences with neutral chars so escaped brackets don't match
    buf: list[str] = []
    i = 0
    while i < len(raw):
        if raw[i] == "\\" and i + 1 < len(raw):
            buf.append("__")
            i += 2
            continue
        buf.append(raw[i])
        i += 1
    return bool(SEQUENCE_PATTERN.search("".join(buf)))


def _parse_timestamp(s: str) -> datetime:
    """Parse a timestamp string, accepting multiple formats.

    The canonical format requires UTC (YYYY-MM-DDTHH:MM:SSZ).
    Ergonomic formats with timezones are accepted and converted to UTC.
    """
    if s in ("-", "0"):
        return NULL_TIMESTAMP

    # Try canonical format first (2006-01-02T15:04:05Z)
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        pass

    # Try RFC3339 with timezone offset
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except ValueError:
        pass

    # Try pretty format "Jan _2 15:04:05 2006 MST"
    # Python doesn't handle _2 directly, so try multiple patterns
    for fmt in (
        "%b %d %H:%M:%S %Y %Z",
        "%b  %d %H:%M:%S %Y %Z",
        "%a %b %d %H:%M:%S %Z %Y",
    ):
        try:
            return datetime.strptime(s, fmt).astimezone(timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"cannot parse timestamp {s!r}")


def _apply_patch(base: Manifest, patch: Manifest) -> Manifest:
    """Apply a patch manifest to a base. Simplified version for decoder use."""
    # Build path map for base
    base_by_path: dict[str, int] = {}
    path_stack: list[str] = []
    for i, entry in enumerate(base.entries):
        while len(path_stack) > entry.depth:
            path_stack.pop()
        full_path = "".join(path_stack) + entry.name
        base_by_path[full_path] = i
        if entry.is_dir():
            path_stack.append(entry.name)

    # Apply patch entries — collect indices to remove
    remove_indices: set[int] = set()
    replacements: dict[int, Entry] = {}
    added: list[Entry] = []

    path_stack = []
    for entry in patch.entries:
        while len(path_stack) > entry.depth:
            path_stack.pop()
        full_path = "".join(path_stack) + entry.name
        if entry.is_dir():
            path_stack.append(entry.name)

        if full_path in base_by_path:
            idx = base_by_path[full_path]
            base_entry = base.entries[idx]
            # Exact duplicate = removal
            if _entries_equal(base_entry, entry):
                remove_indices.add(idx)
                # Also remove children if it's a directory
                if entry.is_dir():
                    for j in range(idx + 1, len(base.entries)):
                        if j in remove_indices:
                            continue
                        if base.entries[j].depth <= entry.depth:
                            break
                        remove_indices.add(j)
            else:
                # Modification
                replacements[idx] = entry
        else:
            added.append(entry)

    # Build result — skip removed, apply replacements
    final: list[Entry] = []
    for i, entry in enumerate(base.entries):
        if i in remove_indices:
            continue
        if i in replacements:
            final.append(replacements[i])
        else:
            final.append(entry)
    final.extend(added)

    result = Manifest(entries=final)
    result.base = base.base
    return result


def _entries_equal(a: Entry, b: Entry) -> bool:
    """Check if two entries are identical (for patch removal detection)."""
    return (
        a.name == b.name
        and a.mode == b.mode
        and a.timestamp == b.timestamp
        and a.size == b.size
        and a.c4id == b.c4id
        and a.target == b.target
        and a.hard_link == b.hard_link
        and a.flow_direction == b.flow_direction
        and a.flow_target == b.flow_target
        and a.depth == b.depth
    )
