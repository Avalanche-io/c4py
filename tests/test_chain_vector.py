"""Cross-implementation chain-vector conformance (grammar erratum 2026-07-13,
amended 2026-11-08).

The shared fixtures under ``tests/vectors/chain-vector/`` are byte-identical
copies of ``c4m/testdata/chain-vector/`` in the Go reference repo
(SPECIFICATION.md "No Closing Validator" / C4M-STANDARD §10.5):

- ``vector.c4m``         resolves to exactly the ID in ``resolved-root-id.txt``
- ``bad-validator.c4m``  is rejected: its bare ID at EOF mismatches the
                         accumulated state, and ends the file. The resolving
                         decoder verifies the ID as a checkpoint before it
                         reaches EOF, so it reports the mismatch, as c4 core's
                         resolving decoder does.
- ``bad-checkpoint.c4m`` is rejected (interior checkpoint mismatch)

A c4m file never ends with a bare C4 ID (ruled 2026-11-08): a bare ID is the
first-line external base reference or a checkpoint before a patch section,
and one at end of input is ``TrailingIDError``.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest

import c4py
from c4py.decoder import TrailingIDError, load, loads

CHAIN_VECTOR = Path(__file__).parent / "vectors" / "chain-vector"

ENTRY_A = "-rw-r--r-- 2026-01-02T03:04:05Z 5 a.txt c44ixpugtZ3dNk5co1wpZbdZQnxMc7LKsbGiWZUGJgkP4MSPLNSSLkUAVt3A7TtLfWkE9119URkmsEe2Yo5jmoovsb"  # noqa: E501
ENTRY_B = "-rw-r--r-- 2026-01-02T03:04:05Z 3 b.txt c459dsjfscH38cYeXXYogktxf4Cd9ibshE3BHUo6a58hBXmRQdZrAkZzsWcbWtDg5oQstpDuni4Hirj75GEmTc1sFT"  # noqa: E501
BASE_REF = "c459dsjfscH38cYeXXYogktxf4Cd9ibshE3BHUo6a58hBXmRQdZrAkZzsWcbWtDg5oQstpDuni4Hirj75GEmTc1sFT"  # noqa: E501
WRONG_ID = "c416Xiouuba2KPjjATz6HsDdA9QJzxnuEoaE87wkfrhQ3ZenFGsLFFNvvM9tAVotBoBaKG2wxo54b7sHyLookBY25Y"  # noqa: E501


def _resolved_root_id() -> str:
    return (CHAIN_VECTOR / "resolved-root-id.txt").read_text(encoding="utf-8").strip()


def _vector_text() -> str:
    return (CHAIN_VECTOR / "vector.c4m").read_text(encoding="utf-8")


def _checkpoint(text: str) -> str:
    return str(loads(text).compute_c4id())


class TestChainVector:
    """The three-case cross-implementation conformance contract."""

    def test_vector_resolves_to_root_id(self):
        """vector.c4m: accept and resolve to exactly resolved-root-id.txt."""
        m = load(str(CHAIN_VECTOR / "vector.c4m"))
        # Base state (a.txt) plus the added b.txt.
        assert len(m) == 2
        assert str(m.compute_c4id()) == _resolved_root_id()

    def test_vector_ends_on_an_entry(self):
        """Since 2026-11-08 the vector's last line is an entry, not an ID."""
        last = _vector_text().rstrip("\n").split("\n")[-1]
        assert " " in last

    def test_bad_validator_rejected_with_mismatch(self):
        """bad-validator.c4m: the resolving decoder reports the mismatch first."""
        with pytest.raises(ValueError, match="patch ID mismatch") as exc:
            load(str(CHAIN_VECTOR / "bad-validator.c4m"))
        assert not isinstance(exc.value, TrailingIDError)

    def test_bad_checkpoint_rejected(self):
        """bad-checkpoint.c4m: interior checkpoint does not match accumulated state."""
        with pytest.raises(ValueError, match="patch ID mismatch"):
            load(str(CHAIN_VECTOR / "bad-checkpoint.c4m"))


class TestTrailingID:
    """A bare C4 ID at end of input is a format error (ruled 2026-11-08)."""

    @pytest.fixture
    def trailing_file(self, tmp_path):
        """vector.c4m plus its correct resolved ID: the shape `c4 diff >>` wrote."""
        text = _vector_text() + _resolved_root_id() + "\n"
        path = tmp_path / "trailing.c4m"
        path.write_text(text, encoding="utf-8")
        return path

    @pytest.mark.parametrize(
        "reader",
        [
            pytest.param(lambda p: loads(p.read_text(encoding="utf-8")), id="loads"),
            pytest.param(lambda p: load(str(p)), id="load-path"),
            pytest.param(
                lambda p: load(io.StringIO(p.read_text(encoding="utf-8"))),
                id="load-stream",
            ),
            pytest.param(lambda p: c4py.load(str(p)), id="c4py.load"),
            pytest.param(lambda p: c4py.loads(p.read_text(encoding="utf-8")), id="c4py.loads"),
        ],
    )
    def test_vector_plus_its_own_id_refused(self, trailing_file, reader):
        """A correct trailing ID is still refused, through every reader."""
        with pytest.raises(TrailingIDError) as exc:
            reader(trailing_file)
        assert exc.value.line == 4
        msg = str(exc.value)
        assert "a c4m file never ends with its own ID" in msg
        assert "(line 4)" in msg
        assert "`c4 id FILE.c4m > fixed.c4m`" in msg

    def test_is_a_value_error_exported_by_the_package(self):
        assert issubclass(TrailingIDError, ValueError)
        assert c4py.TrailingIDError is TrailingIDError

    def test_trailing_id_without_final_newline(self):
        with pytest.raises(TrailingIDError):
            loads(_vector_text() + _resolved_root_id())

    def test_trailing_id_followed_by_blank_lines(self):
        """Blank lines are not entries: the ID still ends the file."""
        with pytest.raises(TrailingIDError) as exc:
            loads(_vector_text() + _resolved_root_id() + "\n\n\n")
        assert exc.value.line == 4

    def test_single_entry_then_its_own_id(self):
        """One entry followed by its own state's ID is a trailing ID."""
        with pytest.raises(TrailingIDError) as exc:
            loads(f"{ENTRY_A}\n{_checkpoint(ENTRY_A + chr(10))}\n")
        assert exc.value.line == 2

    def test_consecutive_checkpoints_at_eof_refused(self):
        """Two checkpoints with nothing after them end the file with a bare ID."""
        cp = _checkpoint(ENTRY_A + "\n")
        with pytest.raises(TrailingIDError) as exc:
            loads(f"{ENTRY_A}\n{cp}\n{cp}\n")
        assert exc.value.line == 3

    def test_trailing_id_after_external_base_refused(self):
        """After a base reference checkpoints defer, but a trailing ID is refused."""
        with pytest.raises(TrailingIDError):
            loads(f"{BASE_REF}\n{ENTRY_A}\n{WRONG_ID}\n")


class TestCheckpointsBeforeAPatch:
    """Bare IDs that are followed by a patch section remain legal."""

    def test_consecutive_checkpoints_then_patch_accepted(self):
        """Repeated checkpoints naming the same state, then a patch, verify."""
        cp = _checkpoint(ENTRY_A + "\n")
        m = loads(f"{ENTRY_A}\n{cp}\n{cp}\n{ENTRY_B}\n")
        assert {e.name for e in m.entries} == {"a.txt", "b.txt"}
        assert m.compute_c4id() == loads(f"{ENTRY_A}\n{ENTRY_B}\n").compute_c4id()

    def test_checkpoint_verification_deferred_after_base_ref(self):
        """A first-line external base ref suppresses checkpoint verification."""
        # An intentionally wrong checkpoint is tolerated because the accumulated
        # state is unknowable without fetching the base.
        m = loads(f"{BASE_REF}\n{ENTRY_A}\n{WRONG_ID}\n{ENTRY_B}\n")
        assert str(m.base) == BASE_REF


class TestBaseReferenceOnly:
    """A file that is only a first-line base reference is not a trailing ID."""

    def test_base_ref_alone_accepted(self):
        """Unchanged behaviour: an empty manifest whose base is that ID."""
        m = loads(f"{BASE_REF}\n")
        assert str(m.base) == BASE_REF
        assert len(m) == 0

    def test_base_ref_then_second_id_refused(self):
        """A second bare ID after the base reference ends the file."""
        with pytest.raises(TrailingIDError) as exc:
            loads(f"{BASE_REF}\n{WRONG_ID}\n")
        assert exc.value.line == 2
