"""C4M canonical identification — canonicalize c4m content before hashing.

When C4 encounters content that parses as a valid c4m file, it canonicalizes
the content before computing the ID. The hash is always of the canonical bytes.
This ensures that two c4m files describing the same filesystem produce the
same C4 ID regardless of formatting.

Detection heuristic: file extension .c4m, or first non-blank line starts with
a valid mode character (-, d, l, or a 10-char Unix permission string).

Reference: design/c4m-canonical-storage.md
"""

from __future__ import annotations

# Mode line pattern: line starts with a valid mode character or 10-char perm string.
# Valid first characters: '-' (file/null), 'd' (dir), 'l' (link), 'p' (pipe),
# 's' (socket), 'b' (block), 'c' (char)
_MODE_CHARS = set("-dlpsbcrw")


def _looks_like_c4m(data: bytes) -> bool:
    """Quick heuristic: does the content look like it might be a c4m file?

    Checks if the first non-blank line starts with a valid mode character.
    This is a cheap pre-check before attempting a full parse.
    """
    try:
        text = data.decode("utf-8")
    except (UnicodeDecodeError, ValueError):
        return False

    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue
        # First non-blank line: check for mode characters
        first_char = stripped[0]
        if first_char in _MODE_CHARS:
            return True
        # Also accept bare C4 ID lines (patch format starts with a c4 ID)
        if first_char == "c" and len(stripped) == 90:
            return True
        return False

    return False


def try_canonicalize(data: bytes) -> bytes | None:
    """Attempt to parse data as c4m and return canonical form.

    Returns the canonical UTF-8 bytes if the data parses as a valid c4m file,
    or None if it does not parse.
    """
    if not _looks_like_c4m(data):
        return None

    try:
        from .decoder import loads
        from .encoder import dumps

        text = data.decode("utf-8")
        manifest = loads(text)

        # Empty manifest is not a c4m file
        if not manifest.entries:
            return None

        canonical_text = dumps(manifest, pretty=False)
        return canonical_text.encode("utf-8")
    except Exception:
        return None
