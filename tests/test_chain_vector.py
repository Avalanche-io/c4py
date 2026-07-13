"""Cross-implementation chain-vector conformance (grammar erratum, 2026-07-13).

The shared fixtures under ``tests/vectors/chain-vector/`` are byte-identical
copies of ``c4m/testdata/chain-vector/`` in the Go reference repo. Every c4m
implementation MUST pass all three cases before the coordinated release that
ships the erratum (SPECIFICATION.md "The Closing Validator" / C4M-STANDARD
§10.5):

- ``vector.c4m``         resolves to exactly the ID in ``resolved-root-id.txt``
- ``bad-validator.c4m``  is rejected (closing validator mismatch)
- ``bad-checkpoint.c4m`` is rejected (interior checkpoint mismatch)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from c4py.decoder import load, loads

CHAIN_VECTOR = Path(__file__).parent / "vectors" / "chain-vector"


def _resolved_root_id() -> str:
    return (CHAIN_VECTOR / "resolved-root-id.txt").read_text(encoding="utf-8").strip()


class TestChainVector:
    """The three-case cross-implementation conformance contract."""

    def test_vector_resolves_to_root_id(self):
        """vector.c4m: accept and resolve to exactly resolved-root-id.txt."""
        m = load(str(CHAIN_VECTOR / "vector.c4m"))
        # Base state (a.txt) plus the added b.txt.
        assert len(m) == 2
        assert str(m.compute_c4id()) == _resolved_root_id()

    def test_bad_validator_rejected(self):
        """bad-validator.c4m: closing validator does not match resolved state."""
        with pytest.raises(ValueError, match="patch ID mismatch"):
            load(str(CHAIN_VECTOR / "bad-validator.c4m"))

    def test_bad_checkpoint_rejected(self):
        """bad-checkpoint.c4m: interior checkpoint does not match accumulated state."""
        with pytest.raises(ValueError, match="patch ID mismatch"):
            load(str(CHAIN_VECTOR / "bad-checkpoint.c4m"))


class TestClosingValidatorErratum:
    """Erratum behaviors exercised directly (2026-07-13)."""

    def test_trailing_bare_id_is_closing_validator(self):
        """A verified bare C4 ID at EOF is the legal closing validator."""
        text = (CHAIN_VECTOR / "vector.c4m").read_text(encoding="utf-8")
        assert loads(text.rstrip("\n")).compute_c4id() == loads(text).compute_c4id()

    def test_single_entry_checkpoint_at_eof(self):
        """One base entry followed by its own accumulated-state ID at EOF."""
        entry = "-rw-r--r-- 2026-01-02T03:04:05Z 5 a.txt c44ixpugtZ3dNk5co1wpZbdZQnxMc7LKsbGiWZUGJgkP4MSPLNSSLkUAVt3A7TtLfWkE9119URkmsEe2Yo5jmoovsb"  # noqa: E501
        base = loads(entry + "\n")
        checkpoint = str(base.compute_c4id())
        m = loads(f"{entry}\n{checkpoint}\n")
        assert len(m) == 1
        assert str(m.compute_c4id()) == checkpoint

    def test_consecutive_equal_checkpoints_accepted(self):
        """Two consecutive checkpoints naming the same accumulated state verify."""
        entry = "-rw-r--r-- 2026-01-02T03:04:05Z 5 a.txt c44ixpugtZ3dNk5co1wpZbdZQnxMc7LKsbGiWZUGJgkP4MSPLNSSLkUAVt3A7TtLfWkE9119URkmsEe2Yo5jmoovsb"  # noqa: E501
        checkpoint = str(loads(entry + "\n").compute_c4id())
        m = loads(f"{entry}\n{checkpoint}\n{checkpoint}\n")
        assert len(m) == 1
        assert str(m.compute_c4id()) == checkpoint

    def test_checkpoint_verification_skipped_after_base_ref(self):
        """A first-line external base ref suppresses checkpoint verification."""
        base_ref = "c459dsjfscH38cYeXXYogktxf4Cd9ibshE3BHUo6a58hBXmRQdZrAkZzsWcbWtDg5oQstpDuni4Hirj75GEmTc1sFT"  # noqa: E501
        # An intentionally wrong checkpoint is tolerated because the accumulated
        # state is unknowable without fetching the base.
        wrong = "c416Xiouuba2KPjjATz6HsDdA9QJzxnuEoaE87wkfrhQ3ZenFGsLFFNvvM9tAVotBoBaKG2wxo54b7sHyLookBY25Y"  # noqa: E501
        text = (
            f"{base_ref}\n"
            "-rw-r--r-- 2026-01-02T03:04:05Z 5 a.txt c44ixpugtZ3dNk5co1wpZbdZQnxMc7LKsbGiWZUGJgkP4MSPLNSSLkUAVt3A7TtLfWkE9119URkmsEe2Yo5jmoovsb\n"  # noqa: E501
            f"{wrong}\n"
        )
        m = loads(text)
        assert str(m.base) == base_ref
