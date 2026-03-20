#!/usr/bin/env python3
"""Monitor render farm output for completeness.

Given a c4m manifest describing expected output (from the lighting
department's publish), check which frames have been rendered and which
are still pending or failed.

    python render_farm_monitor.py expected.c4m /farm/output/HERO/shot_010/

This is the kind of script that runs in a cron job or gets called by
a render management system (Deadline, Tractor, OpenCue) to report
shot status.
"""

import sys
from pathlib import Path

import c4py


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <expected.c4m> <render_output_dir>")
        sys.exit(1)

    expected = c4py.load(sys.argv[1])
    render_dir = Path(sys.argv[2])

    # Count expected frames (non-directory entries)
    expected_files = dict(expected.files())
    rendered = 0
    missing = 0
    corrupt = 0

    for path, entry in expected_files.items():
        real_path = render_dir / path
        if not real_path.exists():
            missing += 1
            continue

        if entry.c4id:
            actual = c4py.identify_file(real_path)
            if actual == entry.c4id:
                rendered += 1
            else:
                corrupt += 1
                print(f"  CORRUPT: {path} (re-render needed)")
        else:
            # No expected C4 ID — just check existence
            rendered += 1

    total = len(expected_files)
    pct = (rendered / total * 100) if total else 0

    print(f"Shot status: {rendered}/{total} frames rendered ({pct:.0f}%)")
    if missing:
        print(f"  {missing} frames pending")
    if corrupt:
        print(f"  {corrupt} frames corrupt (need re-render)")

    # Exit code: 0 = complete, 1 = incomplete, 2 = corrupt
    if corrupt:
        sys.exit(2)
    elif missing:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
