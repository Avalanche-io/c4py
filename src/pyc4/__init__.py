"""pyc4 — Pure Python C4 universal content identification (SMPTE ST 2114:2017).

    import pyc4

    # Identify content
    c4id = pyc4.identify(open("file.mov", "rb"))
    c4id = pyc4.identify_bytes(b"hello")
    c4id = pyc4.parse("c45xZeXwMSpq...")

    # C4M manifests
    manifest = pyc4.scan("/projects/HERO")
    manifest = pyc4.load("project.c4m")
    diff = pyc4.diff(old, new)
"""

from __future__ import annotations

from .id import C4ID, identify, identify_bytes, parse, tree_id
from .manifest import Manifest
from .entry import Entry
from .diff import DiffResult, diff
from .decoder import load, loads
from .encoder import dump, dumps
from .scanner import scan
from .validator import ValidationResult, validate

__version__ = "0.1.0"

__all__ = [
    # Identification
    "C4ID",
    "identify",
    "identify_bytes",
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
    # Operations
    "DiffResult",
    "diff",
    # Validation
    "ValidationResult",
    "validate",
]
