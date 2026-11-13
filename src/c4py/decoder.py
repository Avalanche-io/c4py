"""C4M decoder — parse c4m text into Manifest objects.

The parser is lenient: it accepts ergonomic format variations (padded sizes,
local timestamps, varying indentation) and normalizes internally. Only
canonical form is required for C4 ID computation.

The decoder must understand:
- **Patch boundaries**: A line containing exactly one 90-char C4 ID (starting
  with "c4") separates patch sections. First = external base ref, subsequent =
  checkpoints. A stream ends on an entry line: a bare C4 ID at end of input
  is TrailingIDError (ruled 2026-11-08).
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

from .diff import apply_patch
from .entry import (
    NULL_SIZE,
    NULL_TIMESTAMP,
    SEQUENCE_PATTERN,
    Entry,
    FlowDirection,
    is_path_name,
    os_mode,
    parse_mode,
)
from .id import C4_ID_LENGTH, C4ID, parse
from .manifest import Manifest

# C4 ID pattern: c4 followed by 88 base58 characters
_C4ID_PATTERN = re.compile(r"^c4[1-9A-HJ-NP-Za-km-z]{88}$")


class TrailingIDError(ValueError):
    """The c4m text ends with a bare C4 ID.

    A c4m file never ends with its own ID: a bare C4 ID is either the
    first-line external base reference or a checkpoint before a patch
    section, and one at end of input is a format error (ruled 2026-11-08).
    Files written by c4 1.0.13 through 1.0.26 with ``c4 diff >>`` have this
    shape; ``c4 id FILE.c4m`` (c4 1.1) rewrites them. A ``ValueError``
    subclass, so callers catching ``ValueError`` still catch it.

    Reference: c4m.ErrTrailingID in github.com/Avalanche-io/c4/c4m/errors.go
    """

    def __init__(self, line: int) -> None:
        self.line = line
        super().__init__(
            "c4m: a bare C4 ID ends the file; a c4m file never ends with its "
            f"own ID (line {line}): delete that line, or rewrite the file "
            "with `c4 id FILE.c4m > fixed.c4m`"
        )


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
        last_checkpoint_line = 0  # line of the latest checkpoint, 0 after an entry

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
                    # First line of file: external base reference. The
                    # accumulated state is unknowable here, so any
                    # checkpoints below defer verification to the resolver
                    # that fetches the base.
                    m.base = c4id
                else:
                    # Bare C4 ID = checkpoint: it names the accumulated
                    # manifest state (grammar erratum, 2026-07-13). Flush
                    # the pending section, then verify. A checkpoint
                    # directly following another (consecutive boundaries)
                    # verifies the same accumulated state. A checkpoint at
                    # end of input is a format error (see the flush below).
                    last_checkpoint_line = self.line_num
                    if not patch_mode:
                        m.entries.extend(section)
                    elif section:
                        patch = Manifest(entries=section)
                        m = _apply_patch(m, patch)
                    section = []
                    patch_mode = True

                    # A resolving decoder MUST verify checkpoints — except
                    # after an unresolved external base reference, where the
                    # accumulated state is unknowable here and verification
                    # defers to the resolver that fetches the base.
                    if m.base is None:
                        got = m.compute_c4id()
                        if got != c4id:
                            raise ValueError(
                                f"patch ID mismatch (line {self.line_num}): "
                                f"accumulated {got}, checkpoint {c4id}"
                            )

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
            last_checkpoint_line = 0

        # A bare C4 ID at end of input is a format error: a c4m file never
        # ends with its own ID (a bare ID is a boundary before a patch, and
        # a file cannot contain the ID that names it; ruled 2026-11-08).
        # The checkpoint was verified above, so a wrong trailing ID is
        # reported as a patch ID mismatch first, as in c4 core. A file that
        # is only a first-line base reference is not a trailing ID.
        if last_checkpoint_line:
            raise TrailingIDError(last_checkpoint_line)

        # Flush the remaining section: a chain ends on its last section's
        # entries, and the final patch is verified by what it names, not by
        # a line after it.
        if not patch_mode:
            m.entries.extend(section)
        elif section:
            patch = Manifest(entries=section)
            m = _apply_patch(m, patch)

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

        try:
            mode = parse_mode(mode_str)
        except ValueError as exc:
            raise ValueError(f"line {self.line_num}: invalid mode {mode_str!r}: {exc}") from exc

        entry = Entry(name="", mode=mode, depth=depth)

        # Parse timestamp
        ts_str, line = self._parse_timestamp_field(line)
        try:
            entry.timestamp = _parse_timestamp(ts_str)
        except ValueError as exc:
            raise ValueError(f"line {self.line_num}: {exc}") from exc

        # Parse remaining fields: size, name, [link-op target], [c4id]
        size, name, raw_name, target, c4id, hard_link, flow_dir, flow_target = (
            self._parse_entry_fields(line, mode)
        )

        from .safename import unsafe_name

        entry.size = size
        entry.name = unsafe_name(name)
        # A name is a bare entry name, checked once decoded, so an escaped
        # "/" is refused as surely as a raw one (name-rule vector).
        if is_path_name(entry.name):
            raise ValueError(
                f"line {self.line_num}: name {entry.name!r} is a path, not an entry "
                "name (only a directory's trailing / is allowed)"
            )
        # Ten dashes on a directory line can only be a legacy null: a
        # correct encoder writes a zero-permission directory as
        # "d---------", and pretty encoders through c4 1.0.21 wrote every
        # null mode as ten dashes. On a file line ten dashes is the known
        # zero (ZERO_MODE), as parse_mode already decided.
        if mode_str == "----------" and entry.name.endswith("/"):
            entry.mode = 0
        entry.target = unsafe_name(target) if target else ""
        entry.c4id = c4id
        entry.hard_link = hard_link
        entry.flow_direction = flow_dir
        entry.flow_target = flow_target
        # A 0 beside the C4 ID of non-empty content is a null size written
        # by a surface that has no "-" (a mount); hold it as null so every
        # consumer sees one value for one meaning (c4 core PR #68).
        if entry.has_null_size():
            entry.size = NULL_SIZE

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
        tok_start = pos
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
        # The size token ends at a space: "6x a.txt" read as size 6 and the
        # name "x a.txt", a different entry with no error (Go reference,
        # shared damaged-line vector).
        if pos < n and line[pos] != " ":
            end = pos
            while end < n and line[end] != " ":
                end += 1
            raise ValueError(f"line {self.line_num}: invalid size {line[tok_start:end]!r}")

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
        is_symlink = mode != 0 and stat_mod.S_ISLNK(os_mode(mode))

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
                    if _is_lone_id_field(remaining):
                        # Hard link (ungrouped): the C4 ID field (or its null)
                        # is the only field after the operator. A second
                        # field means the first is a target, even when it
                        # starts with "c4" (link-target vector).
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

            # A backslash always starts a two-byte escape: a c4m
            # field-boundary escape becomes its literal, any other (a
            # SafeName escape such as \\\\) passes through whole, so its
            # second byte never starts an escape of its own. A name ending
            # in a backslash read back with a space added before this
            # (backslash-name vector).
            if ch == "\\" and pos + 1 < n:
                nxt = line[pos + 1]
                if nxt in (" ", '"', "[", "]"):
                    buf.append(nxt)
                else:
                    buf.append(ch)
                    buf.append(nxt)
                pos += 2
                continue

            if ch in ("[", "]"):
                has_unescaped_brackets = True

            # A double quote is written \" in a name, and a control byte is
            # encoded by SafeName, so a raw one never comes from a
            # conforming writer: it is damage, refused rather than read
            # into the name (name-escape vector).
            if ch == '"':
                raise ValueError(f"line {self.line_num}: name holds a raw '\"' (written \\\" in a name)")
            if ord(ch) < 0x20:
                raise ValueError(
                    f"line {self.line_num}: name holds a raw control byte 0x{ord(ch):02X} (SafeName encodes it)"
                )

            # A "/" ends a directory's name, and only there: the field must
            # end with it. Anything after it but a space was part of the
            # name, so the name holds a "/" and is refused. Before, the
            # name was cut at its first "/" and the rest of the line, C4 ID
            # included, dropped (name-rule vector).
            if ch == "/":
                buf.append("/")
                pos += 1
                if pos < n and line[pos] != " ":
                    end = line.find(" ", pos)
                    held = "".join(buf) + line[pos:end if end >= 0 else n]
                    raise ValueError(
                        f"line {self.line_num}: name {held!r} holds '/' "
                        "(only a directory's trailing / is allowed)"
                    )
                return "".join(buf), pos, has_unescaped_brackets

            # Check for boundary: space(s) followed by link operator, c4
            # prefix, or -. Aligned (pretty) forms pad fields with runs of
            # spaces, so the lookahead skips the whole run before testing
            # (mirrors Go's parseNameOrTarget). Through 1.0.24 only one
            # space was tested, so a pretty line whose C4 ID column is "-"
            # read its padding into the name, and Workspace.checkout of a
            # names-only pretty file removed every real file (2026-11-09).
            if ch == " ":
                j = pos
                while j < n and line[j] == " ":
                    j += 1
                rest = line[j:]
                if rest == "":
                    # Trailing padding: the name ended at the first space.
                    return "".join(buf), pos, has_unescaped_brackets
                if (rest.startswith("-> ")
                        or rest.startswith("<- ")
                        or rest.startswith("<> ")):
                    return "".join(buf), pos, has_unescaped_brackets
                # Hard link group: "->N" where N is digit 1-9
                if (len(rest) >= 3
                        and rest[0] == "-" and rest[1] == ">"
                        and rest[2].isdigit() and rest[2] != "0"):
                    return "".join(buf), pos, has_unescaped_brackets
                if len(rest) >= 2 and rest[0] == "c" and rest[1] == "4":
                    return "".join(buf), pos, has_unescaped_brackets
                if rest[0] == "-" and (len(rest) == 1 or rest[1] == " "):
                    return "".join(buf), pos, has_unescaped_brackets
                # A space that is not a field boundary would be part of
                # the name, but a name's space is written "\\ ": an
                # unescaped one is damage, often a damaged C4 ID field that
                # would otherwise join the name (name-escape vector).
                end = rest.find(" ")
                held = "".join(buf) + " " + (rest if end < 0 else rest[:end])
                raise ValueError(
                    f"line {self.line_num}: name {held!r} holds an unescaped space "
                    "(written '\\ ' in a name), or its C4 ID field is damaged"
                )

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

            # A backslash always starts a two-byte escape (as in names).
            if ch == "\\" and pos + 1 < n:
                nxt = line[pos + 1]
                if nxt in (" ", '"'):
                    buf.append(nxt)
                else:
                    buf.append(ch)
                    buf.append(nxt)
                pos += 2
                continue

            # The pretty form pads before the C4 ID field; the whole run
            # of spaces is skipped before asking whether that field starts
            # (link-target vector).
            if ch == " " and _starts_id_field(line, pos):
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
            if line[pos] == " " and _starts_id_field(line, pos):
                return line[start:pos], pos
            pos += 1
        return line[start:pos], pos


def _starts_id_field(line: str, pos: int) -> bool:
    """Whether the run of spaces at pos is followed by the C4 ID field: a
    C4 ID, or the null "-" as the last field."""
    n = len(line)
    while pos < n and line[pos] == " ":
        pos += 1
    rest = line[pos:]
    return rest.startswith("c4") or rest == "-" or rest.startswith("- ")


def _is_lone_id_field(s: str) -> bool:
    """Whether s, the rest of a line after a link operator, is exactly one
    field that is a C4 ID or the null "-". A field ends at an unescaped
    space."""
    if s == "-":
        return True
    if not s.startswith("c4"):
        return False
    i = 0
    while i < len(s):
        if s[i] == "\\":
            i += 2
            continue
        if s[i] == " ":
            return False
        i += 1
    return _is_bare_c4id(s)


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

    # Pretty format "Jan _2 15:04:05 2006 MST". strptime's %Z accepts a
    # zone name but leaves the result naive, so the zone is attached here:
    # UTC/GMT is UTC, the process's local abbreviation is local time, and
    # any other name is a zero-offset zone, as Go's time.Parse fabricates.
    parts = s.split()
    if len(parts) == 5:
        try:
            naive = datetime.strptime(" ".join(parts[:4]), "%b %d %H:%M:%S %Y")
        except ValueError:
            naive = None
        if naive is not None:
            return _attach_zone(naive, parts[4])

    # Unix date format "Mon Jan _2 15:04:05 MST 2006"
    if len(parts) == 6:
        try:
            naive = datetime.strptime(
                " ".join(parts[:4] + parts[5:]), "%a %b %d %H:%M:%S %Y"
            )
        except ValueError:
            naive = None
        if naive is not None:
            return _attach_zone(naive, parts[4])

    raise ValueError(f"cannot parse timestamp {s!r}")


def _attach_zone(naive: datetime, zone: str) -> datetime:
    """Interpret a naive pretty-format time in the named zone, in UTC."""
    import time

    if zone in ("UTC", "GMT", "Z"):
        return naive.replace(tzinfo=timezone.utc)
    if zone in time.tzname:
        return naive.astimezone(timezone.utc)
    numeric = re.fullmatch(r"([+-])(\d{2})(\d{2})", zone)
    if numeric:
        sign = 1 if numeric.group(1) == "+" else -1
        offset = sign * (int(numeric.group(2)) * 60 + int(numeric.group(3)))
        from datetime import timedelta

        return naive.replace(tzinfo=timezone(timedelta(minutes=offset))).astimezone(timezone.utc)
    # Any other abbreviation names an offset this process cannot know.
    # Inventing zero (as Go's time.Parse does) reads the time hours wrong
    # while claiming success; refuse instead, as the Go decoder now does
    # (c4 #80) and as libc4 does. Write UTC or a numeric offset.
    raise ValueError(
        f"unknown time zone abbreviation {zone!r}: write UTC or a numeric offset"
    )


def _apply_patch(base: Manifest, patch: Manifest) -> Manifest:
    """Apply a patch section to the accumulated manifest.

    One implementation of the patch rule: ``c4py.diff.apply_patch``. The
    decoder used to carry its own copy, which still read a directory restated
    with children under it as a removal (erratum 2026-07-31).
    """
    result = apply_patch(base, patch)  # merges base's and patch's range data
    result.base = base.base
    return result
