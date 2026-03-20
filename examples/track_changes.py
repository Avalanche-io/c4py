#!/usr/bin/env python3
"""Track what changed between two snapshots of a directory.

Take a snapshot, do some work, take another snapshot, see exactly what
changed. Useful for render wranglers tracking farm output or TDs
monitoring shot delivery updates.

    python track_changes.py /projects/HERO/shots/

First run creates a c4m snapshot. Subsequent runs show what changed.
"""

import os
import sys
from datetime import datetime

import c4py


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <directory>")
        sys.exit(1)

    directory = sys.argv[1]
    snapshot_file = os.path.basename(directory.rstrip("/")) + ".c4m"

    print(f"Scanning {directory}...")
    current = c4py.scan(directory, progress=lambda p, i, _: print(f"\r  {i} files", end="", flush=True))
    print()

    if not os.path.exists(snapshot_file):
        # First run — save snapshot
        c4py.dump(current, snapshot_file)
        print(f"Snapshot saved: {snapshot_file}")
        print(current.summary())
        return

    # Compare against previous snapshot
    previous = c4py.load(snapshot_file)
    result = c4py.diff(previous, current)

    if result.is_empty:
        print("No changes.")
        return

    if result.added:
        print(f"\n+ {len(result.added)} added:")
        for e in result.added[:20]:
            print(f"  + {e.name} ({e.human_size()})")
        if len(result.added) > 20:
            print(f"  ... and {len(result.added) - 20} more")

    if result.removed:
        print(f"\n- {len(result.removed)} removed:")
        for e in result.removed[:20]:
            print(f"  - {e.name}")
        if len(result.removed) > 20:
            print(f"  ... and {len(result.removed) - 20} more")

    if result.modified:
        print(f"\n~ {len(result.modified)} modified:")
        for e in result.modified[:20]:
            print(f"  ~ {e.name}")
        if len(result.modified) > 20:
            print(f"  ... and {len(result.modified) - 20} more")

    # Update snapshot
    c4py.dump(current, snapshot_file)
    print(f"\nSnapshot updated: {snapshot_file}")


if __name__ == "__main__":
    main()
