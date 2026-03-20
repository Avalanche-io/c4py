"""C4M filename encoding -- Universal Filename Encoding.

Three-tier encoding system for representing any byte sequence as printable UTF-8:

Tier 1: Printable UTF-8 passes through unchanged (except currency sign and backslash)
Tier 2: Backslash escapes for common control characters:
    \\0 -> null (0x00), \\t -> tab (0x09), \\n -> newline (0x0A),
    \\r -> CR (0x0D), \\\\ -> backslash (0x5C)
Tier 3: Non-printable bytes as braille codepoints (U+2800-U+28FF) between currency signs

C4M field-boundary escaping (applied after SafeName):
    space -> '\\ ', double-quote -> '\\"', [ -> '\\[', ] -> '\\]'

Reference: github.com/Avalanche-io/c4/c4m/safename.go
"""

from __future__ import annotations

import unicodedata

# Currency sign used as Tier 3 delimiter
_CURRENCY = "\u00a4"  # ¤


def _is_printable(r: str) -> bool:
    """Check if a character is printable (matching Go's unicode.IsPrint).

    Go's unicode.IsPrint returns true for graphic characters and spaces
    (categories L, M, N, P, S, Zs), but false for control characters
    and other non-graphic categories.
    """
    cat = unicodedata.category(r)
    # L=letter, M=mark, N=number, P=punctuation, S=symbol, Zs=space separator
    return cat[0] in ("L", "M", "N", "P", "S") or cat == "Zs"


def _tier2_escape(ch: str) -> str | None:
    """Return the escape character for a Tier 2 character, or None."""
    if ch == "\x00":
        return "0"
    if ch == "\t":
        return "t"
    if ch == "\n":
        return "n"
    if ch == "\r":
        return "r"
    if ch == "\\":
        return "\\"
    return None


def _tier2_unescape(ch: str) -> tuple[int, bool]:
    """Return (byte_value, ok) for a Tier 2 escape character."""
    if ch == "0":
        return 0x00, True
    if ch == "t":
        return 0x09, True
    if ch == "n":
        return 0x0A, True
    if ch == "r":
        return 0x0D, True
    if ch == "\\":
        return 0x5C, True
    return 0, False


def safe_name(raw: str) -> str:
    """Encode a raw filename to printable UTF-8 (Tier 1-3).

    The Go implementation operates on raw bytes (string in Go is a byte
    sequence). In Python, we receive a str. For byte-identical behavior
    with the Go version, we process the UTF-8 byte representation and
    reconstruct characters, handling invalid sequences via Tier 3.

    Args:
        raw: raw filename (Python string -- may contain control chars)

    Returns:
        Encoded filename safe for c4m representation
    """
    # Fast path: check if encoding is needed
    needs_encoding = False
    for ch in raw:
        if ch == _CURRENCY or ch == "\\" or not _is_printable(ch):
            needs_encoding = True
            break
    if not needs_encoding:
        return raw

    # Encode byte-by-byte through UTF-8, matching Go's behavior
    raw_bytes = raw.encode("utf-8")
    result: list[str] = []
    pending: list[int] = []  # Tier 3 accumulator

    def flush_pending() -> None:
        if not pending:
            return
        result.append(_CURRENCY)
        for b in pending:
            result.append(chr(0x2800 + b))
        result.append(_CURRENCY)
        pending.clear()

    i = 0
    while i < len(raw_bytes):
        # Try to decode a full UTF-8 character
        b = raw_bytes[i]
        char_len = _utf8_char_len(b)

        if char_len == 0 or i + char_len > len(raw_bytes):
            # Invalid UTF-8 lead byte or truncated: Tier 3
            pending.append(b)
            i += 1
            continue

        try:
            char_bytes = raw_bytes[i : i + char_len]
            ch = char_bytes.decode("utf-8")
        except UnicodeDecodeError:
            # Invalid sequence: accumulate all bytes as Tier 3
            for j in range(char_len):
                if i + j < len(raw_bytes):
                    pending.append(raw_bytes[i + j])
            i += char_len
            continue

        # Tier 1: printable UTF-8, not ¤, not backslash
        if _is_printable(ch) and ch != _CURRENCY and ch != "\\":
            flush_pending()
            result.append(ch)
            i += char_len
            continue

        # Tier 2: backslash escapes for specific characters
        esc = _tier2_escape(ch)
        if esc is not None:
            flush_pending()
            result.append("\\")
            result.append(esc)
            i += char_len
            continue

        # Tier 3: accumulate bytes for braille encoding
        for j in range(char_len):
            pending.append(raw_bytes[i + j])
        i += char_len

    flush_pending()
    return "".join(result)


def unsafe_name(encoded: str) -> str:
    """Decode a SafeName-encoded filename back to the raw form.

    Reverses Tier 2 backslash escapes and Tier 3 braille patterns.

    Args:
        encoded: SafeName-encoded string

    Returns:
        Original raw filename
    """
    if _CURRENCY not in encoded and "\\" not in encoded:
        return encoded

    result_bytes = bytearray()
    i = 0
    encoded_len = len(encoded)

    while i < encoded_len:
        ch = encoded[i]

        # Tier 2: backslash escape
        if ch == "\\":
            if i + 1 < encoded_len:
                val, ok = _tier2_unescape(encoded[i + 1])
                if ok:
                    result_bytes.append(val)
                    i += 2
                    continue
            # Lone backslash or unknown escape -- pass through
            result_bytes.extend(ch.encode("utf-8"))
            i += 1
            continue

        # Tier 3: ¤...¤ braille range
        if ch == _CURRENCY:
            j = i + 1
            decoded = False
            while j < encoded_len:
                br = encoded[j]
                if br == _CURRENCY:
                    if decoded:
                        i = j + 1
                    else:
                        result_bytes.extend(_CURRENCY.encode("utf-8"))
                        i += 1
                    break
                if 0x2800 <= ord(br) <= 0x28FF:
                    result_bytes.append(ord(br) - 0x2800)
                    decoded = True
                    j += 1
                    continue
                break
            else:
                # Reached end without closing ¤
                result_bytes.extend(_CURRENCY.encode("utf-8"))
                i += 1
                continue
            if j < encoded_len and encoded[j] == _CURRENCY and decoded:
                continue
            if not decoded and j < encoded_len and encoded[j] != _CURRENCY:
                result_bytes.extend(_CURRENCY.encode("utf-8"))
                i += 1
            continue

        # Tier 1: passthrough
        result_bytes.extend(ch.encode("utf-8"))
        i += 1

    return result_bytes.decode("utf-8", errors="surrogateescape")


def escape_field(name: str, is_sequence: bool = False) -> str:
    """Apply c4m field-boundary escaping after SafeName encoding.

    Escapes space, double-quote, and (for non-sequences) square brackets.
    This matches the Go escapeC4MName function.
    """
    needs_escape = " " in name or '"' in name
    if not is_sequence and ("[" in name or "]" in name):
        needs_escape = True
    if not needs_escape:
        return name

    parts: list[str] = []
    for ch in name:
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


def unescape_field(name: str) -> str:
    """Reverse c4m field-boundary escaping.

    Unescapes \\ followed by space, double-quote, [, or ].
    """
    if "\\" not in name:
        return name

    result: list[str] = []
    i = 0
    while i < len(name):
        if name[i] == "\\" and i + 1 < len(name) and name[i + 1] in (' ', '"', "[", "]"):
            result.append(name[i + 1])
            i += 2
        else:
            result.append(name[i])
            i += 1
    return "".join(result)


def _utf8_char_len(lead_byte: int) -> int:
    """Return the expected character length from a UTF-8 lead byte.

    Returns 0 for continuation bytes (0x80-0xBF) or invalid lead bytes.
    """
    if lead_byte < 0x80:
        return 1
    if lead_byte < 0xC0:
        return 0  # continuation byte
    if lead_byte < 0xE0:
        return 2
    if lead_byte < 0xF0:
        return 3
    if lead_byte < 0xF8:
        return 4
    return 0  # invalid
