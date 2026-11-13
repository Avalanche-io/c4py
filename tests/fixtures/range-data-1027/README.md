# Range-data fixture for release/1.0.27 (2026-11-13)

A folded range's ID list (its frame IDs) must survive decoding and
re-encoding, and every operation that keeps the range. Ruled 2026-11-13:
"it is an error to drop the frame IDs in any context". The decision to
take this data-loss fix into 1.0.27 was made 2026-11-13 by the release
lead and is reversible.

No line is composed by hand. Every file was written by the Go `c4m`
library at c4 release/1.0.27 (3ecd6ea). At c4 b17c393 the output is
byte-identical. Every frame size is known, so no 1.1 size rule is
involved.

| File            | Made by                                                                                              |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `folded.c4m`    | `Marshal` of `NewSequenceDetector(3).DetectSequences` over `plate.1001-1004.exr` (6 bytes each)      |
| `folded-id.txt` | the C4 ID of `folded.c4m`'s bytes                                                                    |
| `chain.c4m`     | `Marshal` of a state holding `notes.txt`, then `EncodePatch` of `PatchDiff` to that state plus the range, the patch carrying the range's `RangeData` |
| `chain-id.txt`  | the C4 ID of `chain.c4m`'s bytes                                                                     |
| `state2.c4m`    | `Marshal` of the chain's second state                                                                |

A conforming implementation does three things:
- It decodes `folded.c4m` and re-encodes it canonically to the same bytes, comparing SHA-512.
- It resolves `chain.c4m` to a state that keeps the patch section's list.
- It re-encodes that state to `state2.c4m` byte for byte.
