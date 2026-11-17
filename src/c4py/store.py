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
    - A lookup that misses there searches from the root down, looking for the
      file at every level before descending, so an object a split in another
      process (or a split cut short) left a level up is still found
    - Small stores stay flat, large stores grow depth only where needed

Reference: github.com/Avalanche-io/c4/store/treestore.go
"""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

from .id import C4ID, parse

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
        return self._locate(c4id) is not None

    def get(self, c4id: C4ID) -> BinaryIO:
        """Retrieve content by C4 ID. The caller must close the returned stream."""
        # Another process's split can move the file down between finding it
        # and opening it: search again. Each retry follows a file that moved
        # deeper, so the bound is reached only if the ID keeps being removed
        # and stored again (Go's TreeStore.Open, c4 #171).
        # On Windows an open can also meet the split's rename itself (a
        # sharing violation): wait and search again within the same bound,
        # and if it is still held at the end, raise that, never not-found
        # (Go's moving, c4 release/1.0.27).
        held: OSError | None = None
        for _ in range(len(str(c4id))):
            path = self._locate(c4id)
            if path is None:
                break
            try:
                return self._open(path)
            except FileNotFoundError:
                held = None
                continue
            except OSError as e:
                if not _moving(e):
                    raise
                held = e
                time.sleep(_MOVING_PAUSE)
        if held is not None:
            raise held
        raise ContentNotFound(c4id)

    def put(self, source: BinaryIO) -> C4ID:
        """Store content and return its C4 ID. Atomic: fsync + rename.

        The bytes given are stored exactly, and the C4 ID is theirs, so
        get(c4id) returns exactly what was put. c4m text is no exception.
        """
        data = source.read()

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

            # Already stored, at any level: skip the rename, as Go does, so
            # an object never gets a second copy at a deeper place.
            if self._locate(c4id) is not None:
                os.unlink(tmp_path)
                return c4id

            # Atomic rename (same filesystem). Content-addressed = idempotent.
            dest = self._resolve_path(c4id)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if self._publish(tmp_path, dest, c4id):
                self._maybe_split(dest.parent)

            return c4id
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def _publish(self, tmp_path: str, dest: Path, c4id: C4ID) -> bool:
        """Rename the complete temp file to the object's name, and report
        whether this rename published it. The same name means the same
        bytes, so a rename that fails while the object is now present lost
        a race to another writer of the same bytes and is not a failure (on
        Windows a rename onto a name another writer holds is refused; Go's
        publish, c4 #179)."""
        try:
            self._replace(tmp_path, str(dest))
            return True
        except OSError:
            if self._locate(c4id) is None:
                raise
            os.unlink(tmp_path)
            return False

    def _stat(self, path: Path) -> os.stat_result:
        """Every lookup's stat, a seam for tests."""
        return os.stat(path)

    def _open(self, path: Path) -> BinaryIO:
        """Every object open, a seam for tests."""
        return open(path, "rb")

    def _replace(self, src: str, dst: str) -> None:
        """The publishing rename, a seam for tests."""
        os.replace(src, dst)

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

    def _locate(self, c4id: C4ID) -> Path | None:
        """The path of the object file for *c4id*, or None if absent.

        Core's lookup rule (``locate`` and ``foldFind`` in Go's
        store/treestore.go; c4 design/store-split-race.md): first the
        deepest path, one stat; on a miss, a search from the root down
        that looks for the file at every level before descending.

        Every level, because a split moves a leaf's files one level down
        one rename at a time, after making the subdirectories, and another
        process may be splitting right now; a writer that resolved its
        target before another process's split lands one level up for good,
        as does every file a split cut short by a crash had not moved.
        Through 2026-11-16 c4py looked only at the deepest path, so those
        objects read as absent. A file only ever moves deeper and shard
        directories are never removed, so looking top down finds every
        file that exists, with no lock across processes.

        Each level also tries the segment's case variants: C4 IDs are
        case-sensitive base58, but a store built on a case-insensitive
        filesystem files IDs differing only in case under one shard
        directory named by whichever came first ("c4/1J" holding
        "c41j..."), which a case-sensitive filesystem then misses.
        """
        p = self._resolve_path(c4id)
        try:
            self._stat(p)
            return p
        except OSError:
            pass
        return self._fold_search(self.root, str(c4id), 0)

    def _fold_search(self, directory: Path, s: str, i: int) -> Path | None:
        # The file at this level first, then the levels below: the order
        # the files move in (see _locate).
        here = directory / s
        try:
            self._stat(here)
            return here
        except OSError:
            pass
        if i + 2 > len(s):
            return None
        seen: list[os.stat_result] = []
        for seg in _fold_variants(s[i:i + 2]):
            sub = directory / seg
            try:
                st = self._stat(sub)
            except OSError:
                continue
            if not stat.S_ISDIR(st.st_mode) or any(os.path.samestat(st, o) for o in seen):
                continue
            seen.append(st)
            found = self._fold_search(sub, s, i + 2)
            if found is not None:
                return found
        return None

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

        # Redistribute content files into 2-char subdirectories. Only
        # objects move: a file whose name is not a C4 ID (a tool's journal,
        # a config, a note a person left) stays where it is, as in core
        # (c4 #171, TestTreeStoreSplitMovesOnlyObjects). Through 2026-11-16
        # the root's split moved Go's journal to jo/journal, and its lock,
        # which is on the file, with it.
        for f in files:
            name = f.name
            try:
                parse(name)
            except ValueError:
                continue
            if len(name) <= depth + 2:
                continue  # shouldn't happen, but be safe

            subdir_name = name[depth:depth + 2]
            subdir = directory / subdir_name
            subdir.mkdir(exist_ok=True)
            os.replace(str(f), str(subdir / name))


# How long get waits before it looks again for a file whose open met a
# rename in progress (Go's movingPause).
_MOVING_PAUSE = 0.001


def _moving(err: OSError | None) -> bool:
    """An open that failed because another process's rename holds the file:
    ERROR_SHARING_VIOLATION, which only Windows produces (Go's moving)."""
    return err is not None and getattr(err, "winerror", None) == 32


def _fold_variants(seg: str) -> list[str]:
    """Every ASCII case variant of *seg*, *seg* itself first (Go's
    ``foldVariants``)."""
    out = [seg]
    for i, c in enumerate(seg):
        if not c.isascii() or not c.isalpha():
            continue
        out += [v[:i] + v[i].swapcase() + v[i + 1:] for v in out]
    return out


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
