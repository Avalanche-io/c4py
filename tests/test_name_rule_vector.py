"""Cross-implementation name-rule vector (c4 core, c4m/testdata/name-rule-vector).

The files under vectors/name-rule-vector/ are byte-identical copies. A name
is a bare entry name, never a path: only "/" separates paths in c4m, and a
name may end in "/" only as a directory's marker; ".", "..", "./", "../"
and "/" are path components. Through 1.0.26 c4py cut a name at its first
"/" and dropped the rest of the line, C4 ID included ("sub/real.txt" read
as a directory "sub/"), read the path components as names, and wrote all
of them raw.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.entry import Entry
from c4py.id import identify_bytes
from c4py.manifest import Manifest

VECTOR = Path(__file__).parent / "vectors" / "name-rule-vector"

CASES = {
    "file-sub": "sub/real.txt",
    "file-leading-slash": "/real.txt",
    "file-trailing-path": "real.txt/x",
    "file-dotdot-inside": "a/../real.txt",
    "file-dot": ".",
    "file-dotdot": "..",
    "dir-nested": "a/empty/",
    "dir-dot": "./",
    "dir-dotdot": "../",
    "dir-root": "/",
    "dir-double-slash": "empty//",
}

TS = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def _read(name: str) -> str:
    return (VECTOR / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("ok", ["ok-file.c4m", "ok-dir.c4m"])
def test_ok_lines_decode(ok: str) -> None:
    assert len(loads(_read(ok)).entries) == 1


@pytest.mark.parametrize("case", sorted(CASES))
def test_decoder_refuses_each_case_naming_the_line(case: str) -> None:
    with pytest.raises(ValueError, match="line 1"):
        loads(_read(case + ".c4m"))


@pytest.mark.parametrize("case", sorted(CASES))
def test_encoder_refuses_each_name(case: str) -> None:
    name = CASES[case]
    mode = 0o040755 if name.endswith("/") else 0o644
    with pytest.raises(ValueError, match="is a path"):
        dumps(Manifest([Entry(name=name, mode=mode, timestamp=TS, size=1)]))


def test_encoder_writes_a_directory_marker() -> None:
    text = dumps(Manifest([Entry(name="empty/", mode=0o040755, timestamp=TS, size=0)]))
    assert " empty/ " in text


def test_forbidden_name_has_no_c4_id() -> None:
    """No conforming c4m text exists to hash: the nil ID and no text, as in Go."""
    hello = identify_bytes(b"hello\n")
    flat = Manifest([Entry(name="sub/real.txt", mode=0o644, timestamp=TS, size=6, c4id=hello)])
    nested = Manifest([
        Entry(name="d/", mode=0o040755, timestamp=TS, size=-1),
        Entry(name="../real.txt", mode=0o644, timestamp=TS, size=6, c4id=hello, depth=1),
    ])
    for m in (flat, nested):
        assert m.compute_c4id().is_nil()
        assert m._canonical_text() == ""
    ok = Manifest([Entry(name="real.txt", mode=0o644, timestamp=TS, size=6, c4id=hello)])
    assert not ok.compute_c4id().is_nil()
