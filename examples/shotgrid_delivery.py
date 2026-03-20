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

    # Group changes by shot directory
    shots = {}
    for entry in result.added:
        shot = entry.name.split("/")[0] if "/" in entry.name else "(root)"
        shots.setdefault(shot, {"added": 0, "removed": 0, "modified": 0})
        shots[shot]["added"] += 1

    for entry in result.removed:
        shot = entry.name.split("/")[0] if "/" in entry.name else "(root)"
        shots.setdefault(shot, {"added": 0, "removed": 0, "modified": 0})
        shots[shot]["removed"] += 1

    for entry in result.modified:
        shot = entry.name.split("/")[0] if "/" in entry.name else "(root)"
        shots.setdefault(shot, {"added": 0, "removed": 0, "modified": 0})
        shots[shot]["modified"] += 1

    if not shots:
        print("\nDeliveries are identical.")
        return

    print(f"\n{'Shot':<30} {'Added':>8} {'Removed':>8} {'Modified':>8}")
    print("-" * 58)
    for shot in sorted(shots):
        s = shots[shot]
        print(f"{shot:<30} {s['added']:>8} {s['removed']:>8} {s['modified']:>8}")

    print(f"\nTotal: +{len(result.added)} -{len(result.removed)} ~{len(result.modified)}")
    print(f"Unchanged: {len(result.same)} files")

    # Save the new manifest for next time
    c4m_file = new_dir.rstrip("/").split("/")[-1] + ".c4m"
    c4py.dump(new, c4m_file)
    print(f"\nManifest saved: {c4m_file} ({new.summary()})")


if __name__ == "__main__":
    main()
