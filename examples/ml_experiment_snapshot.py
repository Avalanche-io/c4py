#!/usr/bin/env python3
"""Snapshot an ML experiment for exact reproducibility.

Capture everything that went into a training run: the dataset, the
config, the code, and the resulting checkpoint. Store it all by
content ID so it can be reconstructed exactly on any machine.

    # Before training
    python ml_experiment_snapshot.py snapshot \
        --data ~/datasets/my_dataset/ \
        --code ./src/ \
        --config ./config/train.yaml \
        --output experiment_001.c4m

    # After training, add the checkpoint
    python ml_experiment_snapshot.py add-checkpoint \
        --experiment experiment_001.c4m \
        --checkpoint ./outputs/model_final.safetensors

    # On another machine, verify you have the same data
    python ml_experiment_snapshot.py verify \
        --experiment experiment_001.c4m \
        --data ~/datasets/my_dataset/

The c4m file is a text file you can read, diff, and version control.
Two researchers with the same c4m file trained on provably identical data.
"""

import argparse
from datetime import datetime, timezone

import c4py


def snapshot(args):
    entries = []
    store = None
    if args.store:
        store = c4py.open_store()

    # Scan each component
    components = []
    if args.data:
        components.append(("data", args.data))
    if args.code:
        components.append(("code", args.code))
    if args.config:
        components.append(("config", args.config))

    combined = c4py.Manifest()

    for label, path in components:
        print(f"Scanning {label}: {path}")
        m = c4py.scan(path, store=store)
        print(f"  {m.summary()}")

        # Add a directory entry for this component
        combined.entries.append(c4py.Entry(
            name=f"{label}/",
            mode=0o40755,
            timestamp=datetime.now(timezone.utc),
            size=-1,
            depth=0,
        ))
        # Add all entries under that directory
        for entry in m.entries:
            entry.depth += 1
            combined.entries.append(entry)

    combined.sort_entries()
    c4py.dump(combined, args.output)
    print(f"\nExperiment snapshot: {args.output}")
    print(f"  {combined.summary()}")
    print(f"  C4 ID: {combined.compute_c4id()}")


def add_checkpoint(args):
    manifest = c4py.load(args.experiment)
    c4id = c4py.identify_file(args.checkpoint)

    print(f"Checkpoint: {args.checkpoint}")
    print(f"  C4 ID: {c4id}")

    # Store if possible
    try:
        store = c4py.open_store()
        with open(args.checkpoint, "rb") as f:
            store.put(f)
        print("  Stored in content store")
    except (ValueError, Exception):
        print("  (no store configured, checkpoint not stored)")

    import os
    info = os.stat(args.checkpoint)
    manifest.entries.append(c4py.Entry(
        name="checkpoint/",
        mode=0o40755,
        timestamp=datetime.fromtimestamp(info.st_mtime, tz=timezone.utc),
        size=-1,
        depth=0,
    ))
    manifest.entries.append(c4py.Entry(
        name=os.path.basename(args.checkpoint),
        mode=info.st_mode,
        timestamp=datetime.fromtimestamp(info.st_mtime, tz=timezone.utc),
        size=info.st_size,
        c4id=c4id,
        depth=1,
    ))

    manifest.sort_entries()
    c4py.dump(manifest, args.experiment)
    print(f"\nUpdated: {args.experiment}")
    print(f"  {manifest.summary()}")


def verify(args):
    manifest = c4py.load(args.experiment)

    # Extract the data component
    data_entries = manifest.filter(lambda p, e: p.startswith("data/"))
    if not data_entries.entries:
        print("No 'data/' component in experiment manifest")
        return

    print(f"Verifying data component against {args.data}...")
    # Build a manifest from just the data entries (strip "data/" prefix)
    data_manifest = c4py.Manifest()
    for entry in data_entries.entries:
        entry.depth -= 1
        if entry.name == "data/":
            continue
        data_manifest.entries.append(entry)

    report = c4py.verify_tree(data_manifest, args.data)
    ok = len(report.ok)
    total = ok + len(report.corrupt) + len(report.missing)
    print(f"  {ok}/{total} files match")
    if report.corrupt:
        print(f"  {len(report.corrupt)} CORRUPT — your data differs from the experiment")
    if report.missing:
        print(f"  {len(report.missing)} MISSING — incomplete dataset")
    if not report.corrupt and not report.missing:
        print("  Dataset is identical to the experiment snapshot.")


def main():
    parser = argparse.ArgumentParser(description="ML experiment snapshots with C4")
    sub = parser.add_subparsers(dest="command")

    snap = sub.add_parser("snapshot", help="Create experiment snapshot")
    snap.add_argument("--data", help="Training data directory")
    snap.add_argument("--code", help="Source code directory")
    snap.add_argument("--config", help="Config file or directory")
    snap.add_argument("--output", required=True, help="Output c4m file")
    snap.add_argument("--store", action="store_true", help="Also store content")

    ckpt = sub.add_parser("add-checkpoint", help="Add checkpoint to experiment")
    ckpt.add_argument("--experiment", required=True, help="Experiment c4m file")
    ckpt.add_argument("--checkpoint", required=True, help="Checkpoint file")

    ver = sub.add_parser("verify", help="Verify data matches experiment")
    ver.add_argument("--experiment", required=True, help="Experiment c4m file")
    ver.add_argument("--data", required=True, help="Data directory to verify")

    args = parser.parse_args()
    if args.command == "snapshot":
        snapshot(args)
    elif args.command == "add-checkpoint":
        add_checkpoint(args)
    elif args.command == "verify":
        verify(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
