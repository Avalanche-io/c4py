"""Content-addressed blob store — shared between c4py, c4 CLI, c4sh, and c4git.

The Store interface provides Has/Get/Put operations on content identified by
C4 ID. All C4 ecosystem tools share the same on-disk layout (TreeStore adaptive
trie), so content stored by any tool is immediately readable by any other.

Configuration is resolved in priority order:
    1. Explicit path passed to open_store()
    2. C4_STORE environment variable (path or s3:// URI)
    3. ~/.c4/config file (store setting)

Storage layout uses adaptive trie sharding (matching Go store.TreeStore):
    - All C4 IDs start with "c4", so the store root has one dir: c4/
    - Directories are either leaves (contain content files) or interior nodes
      (contain 2-char subdirectories). Never both.
    - When a leaf exceeds SPLIT_THRESHOLD files, it splits into 2-char subdirs
    - Path resolution walks the trie: follow 2-char prefixes until reaching a leaf
    - Small stores stay flat, large stores grow depth only where needed

Reference: github.com/Avalanche-io/c4/store/treestore.go
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

from .id import C4ID

# Default threshold before a leaf directory splits into 2-char subdirs
SPLIT_THRESHOLD = 4096

# Config file location
CONFIG_DIR = Path.home() / ".c4"
CONFIG_FILE = CONFIG_DIR / "config"


class Store(ABC):
    """Abstract content-addressed blob store.

    Matches the Go interface:
        Has(id) bool
        Get(id) (io.ReadCloser, error)
        Put(r io.Reader) (c4.ID, error)
    """

    @abstractmethod
    def has(self, c4id: C4ID) -> bool:
        """Check if content exists in the store."""
        ...

    @abstractmethod
    def get(self, c4id: C4ID) -> BinaryIO:
        """Retrieve content by C4 ID. Returns a readable binary stream.

        Raises ContentNotFound if the ID is not in the store.
        """
        ...

    @abstractmethod
    def put(self, source: BinaryIO) -> C4ID:
        """Store content and return its C4 ID.

        The content is hashed and stored atomically. If the content already
        exists (same C4 ID), this is a no-op.
        """
        ...


class ContentNotFound(Exception):  # noqa: N818
    """Raised when a C4 ID is not found in the store."""

    def __init__(self, c4id: C4ID) -> None:
        self.c4id = c4id
        super().__init__(f"content not found: {c4id}")


class FSStore(Store):
    """Filesystem-backed content store with adaptive trie sharding.

    Layout: root/c4/{id} at minimum depth, splitting into 2-char subdirectories
    when a leaf directory exceeds SPLIT_THRESHOLD files.

    Compatible with the Go implementation — content stored by `c4 id -s` is
    readable here, and vice versa.
    """

    def __init__(self, root: str | Path, *, split_threshold: int = SPLIT_THRESHOLD) -> None:
        self.root = Path(root)
        self.split_threshold = split_threshold

    def has(self, c4id: C4ID) -> bool:
        path = self._resolve_path(c4id)
        return path.exists()

    def get(self, c4id: C4ID) -> BinaryIO:
        """Retrieve content by C4 ID. The caller must close the returned stream."""
        path = self._resolve_path(c4id)
        if not path.exists():
            raise ContentNotFound(c4id)
        return open(path, "rb")

    def put(self, source: BinaryIO) -> C4ID:
        """Store content and return its C4 ID. Atomic: fsync + rename.

        If the content parses as a valid c4m file, the canonical form is
        stored and hashed instead of the raw bytes. This ensures that
        c4m content identity is always based on canonical form.
        """
        from .canonical import try_canonicalize

        data = source.read()

        # Canonicalize c4m content before storing
        canonical = try_canonicalize(data)
        if canonical is not None:
            data = canonical

        self.root.mkdir(parents=True, exist_ok=True)

        fd, tmp_path = tempfile.mkstemp(dir=self.root, prefix=".ingest.")
        try:
            h = hashlib.sha512()
            h.update(data)

            with os.fdopen(fd, "wb") as tmp:
                tmp.write(data)
                tmp.flush()
                os.fsync(tmp.fileno())

            c4id = C4ID(h.digest())

            # Atomic rename (same filesystem). Content-addressed = idempotent.
            dest = self._resolve_path(c4id)
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp_path, str(dest))
            self._maybe_split(dest.parent)

            return c4id
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def _resolve_path(self, c4id: C4ID) -> Path:
        """Walk the adaptive trie to find where this ID lives.

        Follows 2-char directory prefixes until reaching a leaf directory.
        Matches the Go implementation exactly:
            dir = root
            for i := 0; i+2 <= len(str); i += 2:
                sub = dir / str[i:i+2]
                if not isDir(sub): break
                dir = sub
            return dir / str
        """
        s = str(c4id)
        directory = self.root

        i = 0
        while i + 2 <= len(s):
            sub = directory / s[i:i + 2]
            if not sub.is_dir():
                break
            directory = sub
            i += 2

        return directory / s

    def _maybe_split(self, directory: Path) -> None:
        """Split a leaf directory if it exceeds the threshold.

        Moves files into 2-char subdirectories based on the next characters
        of their C4 ID after the current prefix depth.
        """
        # Count content files (not directories or temp files, matching Go isTemp())
        files = [f for f in directory.iterdir() if f.is_file() and not f.name.startswith(".")]
        if len(files) <= self.split_threshold:
            return

        # Determine current depth by counting path components from root
        try:
            rel = directory.relative_to(self.root)
        except ValueError:
            return
        depth = len(rel.parts) * 2  # each part is 2 chars of the ID

        # Redistribute content files into 2-char subdirectories
        for f in files:
            name = f.name
            if len(name) <= depth + 2:
                continue  # shouldn't happen, but be safe

            subdir_name = name[depth:depth + 2]
            subdir = directory / subdir_name
            subdir.mkdir(exist_ok=True)
            os.replace(str(f), str(subdir / name))


def open_store(path: str | Path | None = None) -> FSStore:
    """Open or discover a content store.

    Resolution order:
        1. Explicit path argument
        2. C4_STORE environment variable
        3. ~/.c4/config file (store setting)

    Raises ValueError if no store is configured.
    """
    if path is not None:
        return FSStore(Path(os.path.expanduser(str(path))))

    # Check environment
    env_store = os.environ.get("C4_STORE")
    if env_store:
        if env_store.startswith("s3://"):
            raise NotImplementedError("S3 store not yet implemented in c4py")
        return FSStore(Path(os.path.expanduser(env_store)))

    # Check config file
    if CONFIG_FILE.exists():
        store_path = _read_config_store()
        if store_path:
            return FSStore(Path(store_path))

    raise ValueError(
        "No content store configured. "
        "Set C4_STORE=/path/to/store or pass path to open_store()."
    )


def _read_config_store() -> str | None:
    """Read the store path from ~/.c4/config."""
    try:
        for line in CONFIG_FILE.read_text().splitlines():
            line = line.strip()
            if line.startswith("store") and len(line) > 5 and line[5] in ("=", " ", "\t"):
                _, _, value = line.partition("=")
                value = value.strip()
                if value:
                    return os.path.expanduser(value)
    except (OSError, ValueError):
        pass
    return None
