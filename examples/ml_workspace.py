#!/usr/bin/env python3
"""Workspace-based ML experiment management.

Run multiple experiments over the same dataset without copying data.
Each experiment is a filtered view — a c4m manifest pointing to content
in a shared store. Switch between experiments instantly. Reset after
training corrupts the working directory. All content stored once.

    export C4_STORE=~/.c4/store

    # First, scan your dataset into the store
    python ml_workspace.py init ~/datasets/imagenet-1k/

    # Create experiment views
    python ml_workspace.py create-experiment exp_cats_dogs \
        --filter "n02*" --filter "n02085*"

    # Materialize an experiment
    python ml_workspace.py checkout exp_cats_dogs ./data/

    # After training, reset to clean state
    python ml_workspace.py reset ./data/

    # Switch to a different experiment
    python ml_workspace.py checkout exp_all ./data/

The key insight: the ./data/ directory is a workspace backed by the
content store. Checkout morphs it to match any manifest. Content that's
shared between experiments (usually most of it) is never re-copied.
"""

import argparse
import os
from pathlib import Path

import c4py


def cmd_init(args):
    """Scan a dataset into the store and save its manifest."""
    store = c4py.open_store()
    print(f"Scanning {args.dataset}...")
    manifest = c4py.scan(
        args.dataset, store=store,
        progress=lambda p, i, _: print(f"\r  {i} files stored", end="", flush=True),
    )
    print()

    c4m_path = args.output or (Path(args.dataset).name + ".c4m")
    c4py.dump(manifest, c4m_path)
    print(f"Dataset manifest: {c4m_path}")
    print(f"  {manifest.summary()}")
    print(f"  C4 ID: {manifest.compute_c4id()}")


def cmd_create_experiment(args):
    """Create a filtered experiment view from a base manifest."""
    base = c4py.load(args.base)
    print(f"Base: {base.summary()}")

    if args.filter:
        import fnmatch
        patterns = args.filter

        def matches(path, entry):
            return any(fnmatch.fnmatch(path, p) for p in patterns)

        filtered = base.filter(matches)
    else:
        filtered = base

    output = args.name + ".c4m"
    c4py.dump(filtered, output)
    print(f"Experiment: {output}")
    print(f"  {filtered.summary()}")


def cmd_checkout(args):
    """Materialize an experiment in a workspace directory."""
    store = c4py.open_store()
    ws = c4py.Workspace(args.workspace, store=store)

    manifest = c4py.load(args.experiment)
    print(f"Checking out {args.experiment} → {args.workspace}")
    print(f"  {manifest.summary()}")

    result = ws.checkout(
        manifest,
        progress=lambda op, path, i, total: print(
            f"\r  {op}: {path[:60]:<60}", end="", flush=True
        ),
    )
    print()
    print(f"  Created: {result.created}, Skipped: {result.skipped}")


def cmd_reset(args):
    """Reset workspace to the last checked-out manifest."""
    store = c4py.open_store()
    ws = c4py.Workspace(args.workspace, store=store)

    print(f"Resetting {args.workspace}...")
    result = ws.reset()
    print(f"  Created: {result.created}, Removed: {result.removed}, Skipped: {result.skipped}")


def cmd_status(args):
    """Show workspace status and detect changes."""
    store = c4py.open_store()
    ws = c4py.Workspace(args.workspace, store=store)
    info = ws.status()

    print(f"Workspace: {info['path']}")
    if not info["has_manifest"]:
        print("  No experiment checked out")
        return

    print(f"  Manifest: {info['manifest_c4id'][:16]}...")
    print(f"  Last checkout: {info['last_checkout']}")

    print("  Scanning for changes...")
    diff = ws.diff_from_current()
    if diff.is_empty:
        print("  No changes since checkout")
    else:
        if diff.added:
            print(f"  +{len(diff.added)} added")
        if diff.removed:
            print(f"  -{len(diff.removed)} removed")
        if diff.modified:
            print(f"  ~{len(diff.modified)} modified")


def main():
    parser = argparse.ArgumentParser(
        description="Workspace-based ML experiment management",
    )
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("init", help="Scan dataset into store")
    p.add_argument("dataset", help="Dataset directory")
    p.add_argument("-o", "--output", help="Output c4m path")

    p = sub.add_parser("create-experiment", help="Create filtered experiment")
    p.add_argument("name", help="Experiment name")
    p.add_argument("--base", required=True, help="Base dataset c4m")
    p.add_argument("--filter", action="append", help="Glob patterns to include")

    p = sub.add_parser("checkout", help="Materialize experiment")
    p.add_argument("experiment", help="Experiment c4m file")
    p.add_argument("workspace", help="Workspace directory")

    p = sub.add_parser("reset", help="Reset workspace to clean state")
    p.add_argument("workspace", help="Workspace directory")

    p = sub.add_parser("status", help="Show workspace changes")
    p.add_argument("workspace", help="Workspace directory")

    args = parser.parse_args()
    commands = {
        "init": cmd_init,
        "create-experiment": cmd_create_experiment,
        "checkout": cmd_checkout,
        "reset": cmd_reset,
        "status": cmd_status,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
