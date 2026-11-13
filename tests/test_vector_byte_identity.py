"""Every shared vector file round-trips through decode and canonical encode.

A c4m file's byte ID is a C4 ID, so a decode-and-encode round trip that
changes a canonical file's bytes loses data. Until 2026-11-13 nothing
compared bytes, and the encoder dropped a c4m's inline range data unseen
(sequence-null-size vector; decided 2026-11-13 by the release lead;
reversible). This test decodes and canonically re-encodes every ``.c4m``
file under ``tests/vectors/`` and compares SHA-512 digests from
``hashlib``, never c4py's own C4 ID code.

Every file is classified, as the Go reference measured on c4 7a6ba94 (#156):

- ``STABLE``: written canonically by the library; the bytes MUST match.
- ``NOT_CANONICAL``: decodes to a valid manifest, but the file is not
  canonical text by design, for the reason its vector gives. Its output
  must differ, and must itself be a canonical fixed point.
- ``REJECTED``: refused on decode by design; not part of the round trip.

A vendored file in none of the three fails the test, so a new vector
cannot go unclassified.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps

VECTORS = Path(__file__).parent / "vectors"

STABLE = {
    "backslash-name-vector/canonical.c4m",
    "backslash-name-vector/patch.c4m",
    "childless-dir-vector/normalized.c4m",
    "childless-dir-vector/stated.c4m",
    "damaged-line-vector/ok.c4m",
    "link-target-vector/canonical.c4m",
    "link-target-vector/patch.c4m",
    "name-escape-vector/ok-dir.c4m",
    "name-escape-vector/ok-escaped.c4m",
    "name-escape-vector/ok-file.c4m",
    "name-rule-vector/ok-dir.c4m",
    "name-rule-vector/ok-file.c4m",
    "null-size-vector/canonical.c4m",
    "safename-vector/names.c4m",
    "sequence-null-size-vector/folded.c4m",
    "sequence-null-size-vector/frames.c4m",
    "zero-null-vector/canonical.c4m",
}

# file -> the reason its vector gives for not being canonical text
NOT_CANONICAL = {
    "backslash-name-vector/pretty.c4m": "pretty form (README: the Go encoder, pretty form)",
    "link-target-vector/pretty.c4m": "pretty form (README: the Go encoder, pretty form)",
    "zero-null-vector/pretty.c4m": "pretty form (the pretty encoder's output)",
    "null-size-vector/zero-spelled.c4m": "a null size spelled 0 (README: canonical.c4m with every - size rewritten to 0)",
    "sequence-null-size-vector/frames-zero-spelled.c4m": "a null size spelled 0 (README: frames.c4m with the null size rewritten to 0)",
    "legacy/ten-dash-null-1.0.21.c4m": "legacy null mode: ten dashes on a directory line, as pretty encoders through c4 1.0.21 wrote a null",
    "chain-vector/vector.c4m": "a patch chain, which decodes to its resolved state (README: resolve to resolved-root-id.txt)",
    "deepen-vector/deepen.c4m": "a patch chain, which decodes to its resolved state (README: resolve to deepen-root-id.txt)",
    "deepen-vector/remove.c4m": "a patch chain, which decodes to its resolved state (README: resolve to remove-root-id.txt)",
    "name-escape-vector/ok-indent-3.c4m": "non-canonical indent (README: written with SetIndent(3))",
}

REJECTED = {
    "chain-vector/bad-checkpoint.c4m",
    "chain-vector/bad-validator.c4m",
    "damaged-line-vector/id-89.c4m",
    "damaged-line-vector/id-91.c4m",
    "damaged-line-vector/id-bad-char.c4m",
    "damaged-line-vector/mode-bad-char.c4m",
    "damaged-line-vector/mode-sticky-letter-misplaced.c4m",
    "damaged-line-vector/mode-wrong-letter.c4m",
    "damaged-line-vector/size-junk.c4m",
    "damaged-line-vector/size-null-junk.c4m",
    "damaged-line-vector/time-impossible.c4m",
    "name-escape-vector/dir-unescaped-space.c4m",
    "name-escape-vector/file-damaged-id-field.c4m",
    "name-escape-vector/file-raw-control.c4m",
    "name-escape-vector/file-raw-quote.c4m",
    "name-escape-vector/file-raw-tab.c4m",
    "name-escape-vector/file-unescaped-space.c4m",
    "name-rule-vector/dir-dot.c4m",
    "name-rule-vector/dir-dotdot.c4m",
    "name-rule-vector/dir-double-slash.c4m",
    "name-rule-vector/dir-nested.c4m",
    "name-rule-vector/dir-root.c4m",
    "name-rule-vector/file-dot.c4m",
    "name-rule-vector/file-dotdot-inside.c4m",
    "name-rule-vector/file-dotdot.c4m",
    "name-rule-vector/file-leading-slash.c4m",
    "name-rule-vector/file-sub.c4m",
    "name-rule-vector/file-trailing-path.c4m",
}


def _sha512(b: bytes) -> bytes:
    return hashlib.sha512(b).digest()


def _roundtrip(data: bytes) -> bytes:
    return dumps(loads(data.decode("utf-8"))).encode("utf-8")


def test_every_vendored_file_is_classified():
    vendored = {p.relative_to(VECTORS).as_posix() for p in VECTORS.glob("*/*.c4m")}
    classes = [STABLE, set(NOT_CANONICAL), REJECTED]
    assert not (STABLE & set(NOT_CANONICAL) or STABLE & REJECTED or set(NOT_CANONICAL) & REJECTED)
    named = set().union(*classes)
    assert vendored - named == set(), "vendored but unclassified"
    assert named - vendored == set(), "classified but not vendored"


@pytest.mark.parametrize("rel", sorted(STABLE))
def test_canonical_file_round_trips_byte_for_byte(rel):
    data = (VECTORS / rel).read_bytes()
    assert _sha512(_roundtrip(data)) == _sha512(data)


@pytest.mark.parametrize("rel", sorted(NOT_CANONICAL))
def test_non_canonical_file_decodes_to_a_canonical_fixed_point(rel):
    data = (VECTORS / rel).read_bytes()
    out = _roundtrip(data)
    assert _sha512(out) != _sha512(data), f"{rel} is canonical; move it to STABLE"
    assert _sha512(_roundtrip(out)) == _sha512(out)


@pytest.mark.parametrize("rel", sorted(REJECTED))
def test_rejected_file_is_refused(rel):
    with pytest.raises(ValueError):
        loads((VECTORS / rel).read_bytes().decode("utf-8"))
