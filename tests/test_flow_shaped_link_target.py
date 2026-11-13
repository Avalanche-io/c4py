"""A flow-shaped symlink target is a symlink only under a symlink mode.

A symlink whose target has the shape of a flow target (``host:path``,
``C:foo``) cannot be told from a flow link when the mode is null. Ruled
2026-11-13 (decided by the release lead; reversible): under a null mode the
line is a flow link, and a writer keeps a symlink's type by writing its
mode ``lrwxrwxrwx`` even when the rest of the mode is null. Pinned in c4
core by ``TestAFlowShapedTargetIsALinkOnlyUnderASymlinkMode`` (c4m/
link_null_mode_test.go, #154) and stated in
``tests/vectors/link-target-vector/README.md``. c4py already read both
cases this way when the ruling landed; this test pins it.

Two sources of lines. ``tests/fixtures/flow-shaped-link-go-b17c393.json``
holds the twelve lines the Go library writes at c4 b17c393 (``c4m.Marshal``
on the entries that core test builds: three targets, with and without a C4
ID, under each mode); c4py must read each as the ruling says and re-encode
it byte for byte. c4py's own encoder, given the same Entry, must write the
same line.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.entry import NULL_SIZE, NULL_TIMESTAMP, Entry, FlowDirection
from c4py.id import identify_bytes
from c4py.manifest import Manifest

CONTENT = identify_bytes(b"what the link points at")


def _round_trip(mode: int, target: str, with_id: bool) -> Entry:
    m = Manifest(entries=[Entry(
        name="link",
        mode=mode,
        size=NULL_SIZE,
        timestamp=NULL_TIMESTAMP,
        target=target,
        c4id=CONTENT if with_id else None,
    )])
    return loads(dumps(m)).entries[0]


@pytest.mark.parametrize("with_id", [False, True])
@pytest.mark.parametrize("target", ["host:path", "C:foo", "studio:inbox/"])
def test_under_a_symlink_mode_it_is_the_symlink(target, with_id):
    e = _round_trip(0o120777, target, with_id)  # lrwxrwxrwx
    assert e.is_symlink()
    assert e.target == target
    assert e.flow_direction == FlowDirection.NONE


@pytest.mark.parametrize("with_id", [False, True])
@pytest.mark.parametrize("target", ["host:path", "C:foo", "studio:inbox/"])
def test_under_a_null_mode_it_is_a_flow_link(target, with_id):
    e = _round_trip(0, target, with_id)  # null mode "-"
    assert e.flow_direction == FlowDirection.OUTBOUND
    assert e.flow_target == target
    assert e.target == ""


GO_LINES = json.loads(
    (Path(__file__).parent / "fixtures" / "flow-shaped-link-go-b17c393.json").read_text()
)


def _case_id(r):
    return f"{r['mode']}-{r['target']}-{'id' if r['with_id'] else 'noid'}"


def test_go_fixture_has_every_case():
    cases = {(r["target"], r["with_id"], r["mode"]) for r in GO_LINES}
    assert len(GO_LINES) == len(cases) == 12


@pytest.mark.parametrize("r", GO_LINES, ids=[_case_id(r) for r in GO_LINES])
def test_go_written_line_reads_as_ruled_and_reencodes_byte_for_byte(r):
    m = loads(r["text"])
    e = m.entries[0]
    if r["mode"] == "-":
        assert e.flow_direction == FlowDirection.OUTBOUND
        assert e.flow_target == r["target"]
        assert e.target == ""
    else:
        assert r["mode"] == "lrwxrwxrwx"
        assert e.is_symlink()
        assert e.target == r["target"]
        assert e.flow_direction == FlowDirection.NONE
    assert (e.c4id == CONTENT) if r["with_id"] else (e.c4id is None or e.c4id.is_nil())
    assert dumps(m).encode("utf-8") == r["text"].encode("utf-8")


@pytest.mark.parametrize("r", GO_LINES, ids=[_case_id(r) for r in GO_LINES])
def test_c4py_writes_the_line_go_writes(r):
    mode = 0 if r["mode"] == "-" else 0o120777
    m = Manifest(entries=[Entry(
        name="link",
        mode=mode,
        size=NULL_SIZE,
        timestamp=NULL_TIMESTAMP,
        target=r["target"],
        c4id=CONTENT if r["with_id"] else None,
    )])
    assert dumps(m) == r["text"]
