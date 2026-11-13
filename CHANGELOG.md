# Changelog

## Unreleased (1.1)

### ⚠ A folded sequence's size is null when a frame's size is; the encoder keeps range data

Ruled 2026-11-13: "One cannot use size as an expectation if there are
nulls present, so we must say nothing." A folded sequence (a range like
`shot.[1001-1004].exr`) states the sum of its frames' sizes, and its size
is null if any frame's size is null (`-`, or a `0` beside a C4 ID other
than the empty content's), as a null child does a directory's (c4 core
PR #155; decided 2026-11-13 by the release lead; reversible). c4py does
not fold sequences, so no description ID it computes changes; it already
decoded a range's `-` size as null. The shared
`tests/vectors/sequence-null-size-vector/` (c4 core PR #156) is vendored
and its decode half tested. That test found that the encoder dropped a
c4m's inline range data (a folded sequence's member ID lists): it now
writes them after the entries, sorted by C4 ID, as the Go encoder does,
so `folded.c4m` re-encodes byte for byte. **The bytes, and so the byte
ID, of a c4m c4py re-encodes change** when it carries range data; its
description ID does not. CI's c4 CLI pin moves from 5ee54b6 to b17c393.

### CI fails on a test skip the allow-list does not name

A port can diverge silently from a shared vector when the comparison test
skips (c4py's NBSP case was one). CI now runs pytest with `--junitxml`,
and `scripts/check_skips.py` (stdlib only) prints every skipped or xfailed
test with its reason and fails on one that `tests/allowed-skips.txt` does
not name for that platform (`sys.platform`, or `*`). A listed skip that did
not happen is reported, not failed on. Measured on every CI leg first: the
only skip was `test_matches_go_cli_byte_for_byte`, which needs the Go c4
CLI. CI now installs it (`go install …/cmd/c4@5ee54b6`, pinned), the test
finds it through `C4_BIN` or PATH, and the allow-list is empty. The test
now compares c4py's output with the flattened form, `c4 id <dir> | c4 patch
-` (c4 master's `c4 id` streams a chain), and checks that `c4 id -q <dir>`
equals c4py's description ID. Both match on every leg.

### ⚠ SafeName's printable test is pinned to Unicode 17.0.0

SafeName writes a character as itself (Tier 1) when it is printable. c4py
decided that with `unicodedata`, which follows the interpreter's Unicode
(13.0 on Python 3.10, 14.0 on 3.11, 15.1 on 3.13), and it counted every
`Zs` space separator as printable where Go's `unicode.IsPrint` admits only
U+0020. So U+0870 (assigned in Unicode 14.0) was escaped on Python 3.10 and
written plainly on 3.11+, and U+00A0 and the other non-ASCII spaces were
written plainly where every other implementation escapes them: one name,
two encodings, two C4 IDs. The test is now one table pinned to Unicode
17.0.0, `tests/vectors/safename-vector/printable-17.0.0.txt` (shared with
c4 core, PR #149), embedded as `c4py/_printable.py` by
`scripts/gen_printable.py` and searched by bisection; `unicodedata` is no
longer used (decided 2026-11-13 by the release lead; reversible). **C4 IDs
change** for a name holding a code point whose class differed between the
interpreter's Unicode and 17.0.0, and for a name holding a non-ASCII space
separator, on every Python. The shared safename vector's last held case
now passes.

### Encoding a description keeps its external base

`dumps` and `dump` never wrote a manifest's `base`, the first-line bare
C4 ID naming the block a description refines, although `loads` keeps it.
So `loads` then `dumps` turned a block on a base into a standalone
description of the block's own entries, silently (found 2026-11-13 in c4
core, PR #145). Canonical and pretty encoding now write a set base as the
first line. A manifest with no base encodes exactly as before, and the
description ID (`compute_c4id`) is unchanged. `patch_diff` already writes
its own first line (the old state's ID) and never writes either input's
base.

### ⚠ A size of 0 beside a C4 ID other than the empty content's is null

Only the empty content has size 0. A `0` on a file or directory line
beside any other C4 ID is therefore a null size written by a surface that
has no `-` (a mount), and is read as one (ruled 2026-11-08; c4 core PR
#68, landed 2026-11-13, decided by the release lead; reversible).
`Entry.has_null_size()` is now the one test: the decoder holds such a `0`
as -1, canonical form and both encoders write `-`, and the parent
directory's size goes null over it, so `0 f <ID>` and `- f <ID>` are one
description with one ID. A `0` with no C4 ID, a `0` with the empty
content's ID, and a symlink's `0` stay known zeros.
`c4py.entry.EMPTY_CONTENT_ID` names the ID of zero bytes.

This changes the C4 ID of any description holding the combination,
including one built through the API with `size=0` beside a real content
ID. No shipped scanner writes it. The shared `null-size-vector` fixture
is copied from c4 core.

### ⚠ Breaking: a c4m file that ends with a bare C4 ID is refused

A c4m file never ends with its own ID (ruled 2026-11-08). A bare C4 ID is
the first-line external base reference or a checkpoint before a patch
section; one at end of input is a format error. `load` and `loads` now
raise `c4py.TrailingIDError` (a `ValueError`) for it, naming the line and
the fix. Until now c4py accepted a correct trailing ID as a "closing
validator".

Files of this shape were written by c4 1.0.13 through 1.0.26 with
`c4 diff FILE.c4m DIR >> FILE.c4m`. To read one again, delete its final
line, or rewrite it with `c4 id FILE.c4m > fixed.c4m` (c4 1.1), which
checks the ID before dropping it. c4py has no normaliser of its own and
does not migrate them.

A wrong trailing ID is still reported as a patch ID mismatch, because the
ID is checked as a checkpoint before the end of the file is reached, as
in c4 core. A file holding only a first-line base reference is unchanged:
it reads as an empty manifest with that base.

`patch_diff(old, new)` now returns `""` when nothing changed, as
`c4 diff` prints nothing. Before, it returned the old ID alone, which,
appended to `old.c4m`, ended the file with a bare ID.

The shared `chain-vector` and `deepen-vector` fixtures are updated from
c4 core: `vector.c4m`, `bad-checkpoint.c4m`, `deepen.c4m` and `remove.c4m`
lose their final bare-ID line, and their root IDs are unchanged.
