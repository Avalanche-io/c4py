"""C4 ID computation — SHA-512 content identification with base58 encoding.

Algorithm (SMPTE ST 2114:2017):
    1. Compute SHA-512 digest of content (64 bytes)
    2. Interpret digest as big-endian unsigned integer
    3. Encode as base58 (Bitcoin alphabet, no leading-zero compression)
    4. Pad to exactly 88 characters, prefix with "c4"
    Result: 90-character string like c45xZeXwMSpq...

Base58 alphabet: 123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz
(excludes 0, O, I, l to avoid visual ambiguity)

Tree IDs (set identity):
    1. Sort all IDs lexicographically (as 64-byte arrays)
    2. Deduplicate
    3. Build binary Merkle tree: hash pairs bottom-up
    4. For each pair, sort the two IDs before hashing (order independence)
    5. Hash: SHA-512(smaller_id_bytes || larger_id_bytes)
    6. Odd ID at end of level promotes unchanged
    7. Root of tree is the set's C4 ID

Reference: github.com/Avalanche-io/c4/id.go
           github.com/Avalanche-io/c4/tree.go
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import BinaryIO

# Bitcoin base58 alphabet (no 0, O, I, l)
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_MAP = {c: i for i, c in enumerate(BASE58_ALPHABET)}

C4_PREFIX = "c4"
C4_ID_LENGTH = 90  # 2 prefix + 88 encoded
DIGEST_SIZE = 64   # SHA-512 = 512 bits = 64 bytes


class C4ID:
    """A C4 content identifier — 64-byte SHA-512 digest with base58 string representation.

    Immutable value type. Two C4IDs are equal iff their digests are equal.
    """

    __slots__ = ("_digest",)

    def __init__(self, value: bytes | str) -> None:
        """Create a C4ID from a 64-byte digest or a 90-character C4 ID string.

        Both forms are accepted so that repr() output is copy-pasteable:
            >>> c4id = C4ID('c45xZeXwMSpq...')
            >>> C4ID(repr_string) == c4id  # True
        """
        if isinstance(value, str):
            self._digest = _parse_to_digest(value)
        elif isinstance(value, bytes):
            if len(value) != DIGEST_SIZE:
                raise ValueError(f"C4ID digest must be {DIGEST_SIZE} bytes, got {len(value)}")
            self._digest = value
        else:
            raise TypeError(f"C4ID requires bytes or str, got {type(value).__name__}")

    @property
    def digest(self) -> bytes:
        """Raw 64-byte SHA-512 digest."""
        return self._digest

    def hex(self) -> str:
        """Hex-encoded digest."""
        return self._digest.hex()

    def is_nil(self) -> bool:
        """True if all digest bytes are zero."""
        return self._digest == b"\x00" * DIGEST_SIZE

    def __bool__(self) -> bool:
        """False for nil ID (all zero bytes), True otherwise."""
        return not self.is_nil()

    def __str__(self) -> str:
        """90-character C4 ID string (c4 prefix + 88 base58 chars)."""
        # Interpret digest as big-endian unsigned integer
        num = int.from_bytes(self._digest, byteorder="big")

        # Encode to base58, filling from right
        chars = ["1"] * C4_ID_LENGTH  # '1' is base58 zero
        chars[0] = "c"
        chars[1] = "4"

        i = C4_ID_LENGTH - 1
        while i > 1 and num > 0:
            num, remainder = divmod(num, 58)
            chars[i] = BASE58_ALPHABET[remainder]
            i -= 1

        return "".join(chars)

    def __repr__(self) -> str:
        return f"C4ID('{self}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, C4ID):
            return self._digest == other._digest
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._digest)

    def __lt__(self, other: C4ID) -> bool:
        return self._digest < other._digest

    def __le__(self, other: C4ID) -> bool:
        return self._digest <= other._digest

    def __gt__(self, other: C4ID) -> bool:
        return self._digest > other._digest

    def __ge__(self, other: C4ID) -> bool:
        return self._digest >= other._digest

    def __bytes__(self) -> bytes:
        return self._digest


def identify(source: BinaryIO, *, buf_size: int = 65536) -> C4ID:
    """Compute the C4 ID of a binary stream.

    Reads the stream in chunks. Constant memory usage regardless of content size.
    """
    h = hashlib.sha512()
    while True:
        chunk = source.read(buf_size)
        if not chunk:
            break
        h.update(chunk)
    return C4ID(h.digest())


def identify_bytes(data: bytes) -> C4ID:
    """Compute the C4 ID of a byte string."""
    return C4ID(hashlib.sha512(data).digest())


def identify_file(path: str | os.PathLike[str], *, buf_size: int = 65536) -> C4ID:
    """Compute the C4 ID of a file on disk.

    If the file is a valid c4m file (detected by .c4m extension or content
    heuristic), the content is canonicalized before hashing. The C4 ID is
    always computed from the canonical form, not the raw bytes on disk.

    This is the one exception to the general rule that C4 identifies raw bytes.
    Two c4m files describing the same filesystem produce the same C4 ID
    regardless of formatting.
    """
    from .canonical import try_canonicalize

    p = Path(path)
    is_c4m_ext = p.suffix.lower() == ".c4m"

    with open(path, "rb") as f:
        data = f.read()

    # Try canonicalization: always for .c4m files, heuristic for others
    if is_c4m_ext or len(data) < 10 * 1024 * 1024:  # heuristic up to 10 MB
        canonical = try_canonicalize(data)
        if canonical is not None:
            return identify_bytes(canonical)

    return identify_bytes(data)


def identify_files(
    paths: list[str | os.PathLike[str]],
    *,
    workers: int = 4,
    progress: Callable[[str, int, int], None] | None = None,
) -> dict[Path, C4ID | None]:
    """Identify multiple files concurrently.

    Uses ThreadPoolExecutor for I/O-bound parallelism.
    Returns {Path: C4ID} dict. Files that cannot be read map to None.
    Progress callback: progress(path, completed, total)
    """
    resolved = [Path(p).resolve() for p in paths]
    total = len(resolved)
    results: dict[Path, C4ID | None] = {}

    if total == 0:
        return results

    def _do(p: Path) -> tuple[Path, C4ID | None]:
        try:
            return p, identify_file(p)
        except (OSError, ValueError):
            return p, None

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_do, p): p for p in resolved}
        for fut in as_completed(futures):
            p, c4id = fut.result()
            results[p] = c4id
            completed += 1
            if progress is not None:
                progress(str(p), completed, total)

    return results


def verify(path: str | os.PathLike[str], expected: C4ID) -> bool:
    """Check if a file's content matches an expected C4 ID.

    Returns True if the file's C4 ID matches, False otherwise.
    Raises FileNotFoundError if the file doesn't exist.
    """
    return identify_file(path) == expected


def parse(s: str) -> C4ID:
    """Parse a 90-character C4 ID string into a C4ID.

    Raises ValueError if the string is not a valid C4 ID.
    """
    return C4ID(_parse_to_digest(s))


def _parse_to_digest(s: str) -> bytes:
    """Parse a C4 ID string and return the raw 64-byte digest."""
    if len(s) != C4_ID_LENGTH:
        raise ValueError(f"C4 ID must be {C4_ID_LENGTH} characters, got {len(s)}")
    if s[:2] != C4_PREFIX:
        raise ValueError(f"C4 ID must start with '{C4_PREFIX}', got '{s[:2]}'")

    num = 0
    for i in range(2, C4_ID_LENGTH):
        c = s[i]
        val = BASE58_MAP.get(c)
        if val is None:
            raise ValueError(f"Invalid base58 character '{c}' at position {i}")
        num = num * 58 + val

    if num.bit_length() > 512:
        raise ValueError("C4 ID value exceeds 512-bit range")

    return num.to_bytes(DIGEST_SIZE, byteorder="big")


def tree_id(ids: list[C4ID]) -> C4ID:
    """Compute the C4 ID of a set of C4 IDs (order-independent Merkle tree).

    The result is the same regardless of input order — IDs are sorted and
    deduplicated before tree construction. Each pair is sorted before hashing
    to ensure commutativity.
    """
    if not ids:
        return C4ID(b"\x00" * DIGEST_SIZE)
    if len(ids) == 1:
        return ids[0]

    # Sort and deduplicate
    sorted_ids = sorted(set(ids))

    # Build Merkle tree bottom-up
    level = [sid.digest for sid in sorted_ids]
    while len(level) > 1:
        next_level: list[bytes] = []
        i = 0
        while i < len(level):
            if i + 1 < len(level):
                a, b = level[i], level[i + 1]
                # Sort pair for order independence
                if a > b:
                    a, b = b, a
                next_level.append(hashlib.sha512(a + b).digest())
                i += 2
            else:
                # Odd one promotes unchanged
                next_level.append(level[i])
                i += 1
        level = next_level

    return C4ID(level[0])
