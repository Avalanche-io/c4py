# Changelog

## Unreleased (1.1)

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
