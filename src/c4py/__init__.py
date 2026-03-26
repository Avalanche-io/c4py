"""c4py — Pure Python C4 universal content identification (SMPTE ST 2114:2017).

    import c4py

    # Identify content
    c4id = c4py.identify(open("file.mov", "rb"))
    c4id = c4py.identify_bytes(b"hello")
    c4id = c4py.parse("c45xZeXwMSpq...")

    # C4M manifests
    manifest = c4py.scan("/projects/HERO")
    manifest = c4py.load("project.c4m")
    diff = c4py.diff(old, new)

    # Content store (shared with c4 CLI and c4sh)
    store = c4py.open_store("~/.c4/store")
    c4id = store.put(open("render.exr", "rb"))
    content = store.get(c4id)
"""

from __future__ import annotations

from .decoder import load, loads
from .diff import (
    Conflict,
    DiffResult,
    PatchInfo,
    apply_patch,
    diff,
    log_chain,
    merge,
    patch_diff,
    resolve_chain,
)
from .encoder import dump, dumps
from .entry import Entry
from .id import (
    C4ID,
    identify,
    identify_bytes,
    identify_file,
    identify_files,
    parse,
    tree_id,
    verify,
)
from .manifest import Manifest
from .pool import IngestResult, PoolResult, ingest, pool
from .reconcile import ReconcileOp, ReconcilePlan, ReconcileResult, reconcile
from .scanner import scan
from .store import ContentNotFound, FSStore, Store, open_store
from .validator import ValidationResult, validate
from .verify import CorruptEntry, VerifyReport, verify_tree
from .workspace import Workspace

__version__ = "1.0.11"

__all__ = [
    # Identification
    "C4ID",
    "identify",
    "identify_bytes",
    "identify_file",
    "identify_files",
    "verify",
    "parse",
    "tree_id",
    # Manifest
    "Manifest",
    "Entry",
    # I/O
    "load",
    "loads",
    "dump",
    "dumps",
    "scan",
    # Content Store
    "Store",
    "FSStore",
    "ContentNotFound",
    "open_store",
    # Operations
    "Conflict",
    "DiffResult",
    "PatchInfo",
    "diff",
    "merge",
    "patch_diff",
    "apply_patch",
    "resolve_chain",
    "log_chain",
    # Reconcile
    "ReconcileOp",
    "ReconcilePlan",
    "ReconcileResult",
    "reconcile",
    # Pool / Ingest
    "PoolResult",
    "IngestResult",
    "pool",
    "ingest",
    # Validation
    "ValidationResult",
    "validate",
    # Verification
    "CorruptEntry",
    "VerifyReport",
    "verify_tree",
    # Workspace
    "Workspace",
]
