#!/usr/bin/env python3
"""ShotGrid delivery verification.

A production pipeline script that verifies vendor deliveries, identifies
which shots have new or changed frames, and reports status. This is the
kind of thing a pipeline TD writes once and runs on every delivery.

Assumes delivery structure:
    /deliveries/SHOW/v03/
        shots/
            shot_010/
                comp/
                    frame.[1001-1100].exr
            shot_020/
                comp/
                    frame.[1001-1050].exr

    python shotgrid_delivery.py /deliveries/SHOW/v02/ /deliveries/SHOW/v03/
"""

import sys
import c4py


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <old_delivery_dir> <new_delivery_dir>")
        sys.exit(1)

    old_dir, new_dir = sys.argv[1], sys.argv[2]

    print(f"Scanning {old_dir}...")
    old = c4py.scan(old_dir)
    print(f"Scanning {new_dir}...")
    new = c4py.scan(new_dir)

    result = c4py.diff(old, new)

    # diff() matches entries by full path, but the entries it returns carry
    # bare names ("frame.1002.exr", "comp/"). Map each back to its path in the
    # manifest it came from.
    old_paths = {id(e): p for p, e in old.flat_entries()}
    new_paths = {id(e): p for p, e in new.flat_entries()}

    def shot_of(path):
        parts = path.split("/")
        if len(parts) > 2 and parts[0] == "shots":
            return parts[1]  # shots/<shot>/...
        return "(outside shots/)"

    # Group changed files by shot. Folders are left out: a folder counts as
    # modified whenever anything under it changes.
    shots = {}
    totals = {"added": 0, "removed": 0, "modified": 0}
    for kind, entries, paths in (
        ("added", result.added, new_paths),
        ("removed", result.removed, old_paths),
        ("modified", result.modified, new_paths),
    ):
        for entry in entries:
            if entry.is_dir():
                continue
            shot = shot_of(paths[id(entry)])
            shots.setdefault(shot, {"added": 0, "removed": 0, "modified": 0})
            shots[shot][kind] += 1
            totals[kind] += 1

    if not shots:
        print("\nDeliveries are identical.")
        return

    print(f"\n{'Shot':<30} {'Added':>8} {'Removed':>8} {'Modified':>8}")
    print("-" * 58)
    for shot in sorted(shots):
        s = shots[shot]
        print(f"{shot:<30} {s['added']:>8} {s['removed']:>8} {s['modified']:>8}")

    print(f"\nTotal: +{totals['added']} -{totals['removed']} ~{totals['modified']} files")
    print(f"Unchanged: {sum(1 for e in result.same if not e.is_dir())} files")

    # Save the new manifest for next time
    c4m_file = new_dir.rstrip("/").split("/")[-1] + ".c4m"
    c4py.dump(new, c4m_file)
    print(f"\nManifest saved: {c4m_file} ({new.summary()})")


if __name__ == "__main__":
    main()
