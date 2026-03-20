#!/usr/bin/env python3
"""Diff two versions of a dataset.

Benchmark datasets get updated. Training sets get curated. When someone
publishes v2 of a dataset, you want to know: what exactly changed?

    python ml_dataset_diff.py imagenet-v1.c4m imagenet-v2.c4m

This tells you precisely which files were added, removed, or modified —
not by filename (which can be misleading) but by content identity.
If a file was renamed but the content is the same, it shows up as a
remove + add with the same C4 ID. If a file was silently replaced with
a different version, it shows up as modified.
"""

import sys

import c4py


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <old.c4m> <new.c4m>")
        sys.exit(1)

    old = c4py.load(sys.argv[1])
    new = c4py.load(sys.argv[2])

    result = c4py.diff(old, new)

    print(f"Old: {old.summary()}")
    print(f"New: {new.summary()}")
    print()

    if result.is_empty:
        print("Datasets are identical.")
        return

    # Summary
    print(f"+{len(result.added)} added, -{len(result.removed)} removed, ~{len(result.modified)} modified, ={len(result.same)} unchanged")
    print()

    # Detect renames (same C4 ID, different path)
    removed_ids = {e.c4id: e for e in result.removed if e.c4id}
    renames = []
    pure_adds = []
    for e in result.added:
        if e.c4id and e.c4id in removed_ids:
            renames.append((removed_ids.pop(e.c4id), e))
        else:
            pure_adds.append(e)
    pure_removes = list(removed_ids.values())

    if renames:
        print(f"Renamed ({len(renames)}):")
        for old_e, new_e in renames[:20]:
            print(f"  {old_e.name} → {new_e.name}")
        if len(renames) > 20:
            print(f"  ... and {len(renames) - 20} more")
        print()

    if pure_adds:
        print(f"Added ({len(pure_adds)}):")
        for e in pure_adds[:20]:
            print(f"  + {e.name} ({e.human_size()})")
        if len(pure_adds) > 20:
            print(f"  ... and {len(pure_adds) - 20} more")
        print()

    if pure_removes:
        print(f"Removed ({len(pure_removes)}):")
        for e in pure_removes[:20]:
            print(f"  - {e.name}")
        if len(pure_removes) > 20:
            print(f"  ... and {len(pure_removes) - 20} more")
        print()

    if result.modified:
        print(f"Modified ({len(result.modified)}):")
        for e in result.modified[:20]:
            print(f"  ~ {e.name} ({e.human_size()})")
        if len(result.modified) > 20:
            print(f"  ... and {len(result.modified) - 20} more")

    # Size delta
    old_size = old.total_size()
    new_size = new.total_size()
    delta = new_size - old_size
    sign = "+" if delta >= 0 else ""
    from c4py.entry import _human_size
    print(f"\nSize: {old.human_total()} → {new.human_total()} ({sign}{_human_size(abs(delta))})")


if __name__ == "__main__":
    main()
