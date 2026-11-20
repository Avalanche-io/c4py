# Folded-range refusal fixture (2026-11-20)

The inputs for the folded-range refusal tests. A materialiser that can't
expand a folded range must refuse a description holding one, before it
writes anything, whether the range's ID list is inline, in the store, or
nowhere. The same trees unfolded must still round-trip.

Every `.c4m` here was written by the Go `c4` CLI built from core
release/1.0.27 2faac8d. None was written by hand. This fixture stands in
for core's shared folded-range vector until that lands; the tests then
switch to the vendored vector.

## The trees

`trees/` holds the files, all dated 2025-01-01T00:00:00Z when scanned (git
doesn't keep mtimes; the tests don't need them).

| tree | files | what it covers |
| --- | --- | --- |
| A | `frame.000{1,2,3}.exr` (`A frame N`), `notes.txt` (`readme`) | a range beside a file |
| B | `top.txt` (`top`), `shots/s010/img.000{1,2,3}.dpx` (`B frame N`) | a range nested in a subdirectory |
| C | `a.000{1,2,3}.exr` (`C a N`), `b.000{1,2,3}.exr` (`C b N`) | two ranges in one folder |

## The files

| file | made by |
| --- | --- |
| `X-folded.c4m` | `c4 id -S -s trees/X > X-folded.c4m`: each range's ID list is inline, after the entries |
| `X-plain.c4m` | `c4 id -s trees/X > X-plain.c4m`: the same tree, unfolded |
| `B-s010.c4m` | `c4 cat <s010/'s C4 ID>` from B's store: the stored listing of `s010/`, whose range line carries **no** inline list |
| `B-s010.list` | `c4 cat <the range's C4 ID>` from B's store: the 270-byte ID list (three C4 IDs, no newline), so a test can put the list in a store |
| `plain-ids.txt` | each tree's description ID (the C4 ID of its root's one-level listing), read from its line in `c4 id trees/`, where each tree is one directory; then the range's C4 ID in `B-s010.c4m`. For the nested tree B this differs from the `stored:` ID `c4 id -s trees/B` narrates on this line, which is the byte ID of the nested c4m it wrote |

## The contract

1. Each of `A-folded.c4m`, `B-folded.c4m`, `C-folded.c4m` and `B-s010.c4m`
   is refused with an error that names the folded range, with the list in
   the store and without it. The target folder is left exactly as it was.
2. Each `X-plain.c4m`, materialised into an empty folder from a store
   holding the tree's files, re-scans to the description ID in
   `plain-ids.txt`.

## Round trip

| File           | Re-encodes to |
| -------------- | ------------- |
| `*-folded.c4m` | itself        |
| `*-plain.c4m`  | itself        |
| `B-s010.c4m`   | itself        |
