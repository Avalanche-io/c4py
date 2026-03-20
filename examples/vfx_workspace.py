#!/usr/bin/env python3
"""Workspace-based VFX shot management.

A comp artist needs to work on a shot. The full show is 80 TB across
2,000 shots, but this shot only needs a few hundred GB of renders,
plates, and references. Pull just that subset into a local workspace,
work on it, publish the results, and move on to the next shot — all
without copying the full show.

    export C4_STORE=/mnt/show_store

    # Supervisor publishes the show manifest (once, from the file server)
    python vfx_workspace.py publish /mnt/nas/HERO/ HERO.c4m

    # Artist pulls their shot into a local workspace
    python vfx_workspace.py pull HERO.c4m shot_010 ~/workspace/

    # Work happens — artist modifies comps, adds versions, etc.

    # See what changed
    python vfx_workspace.py status ~/workspace/

    # Publish changes back (snapshot → new manifest)
    python vfx_workspace.py snapshot ~/workspace/ shot_010_v02.c4m

    # Move on to next shot (workspace morphs, shared content stays)
    python vfx_workspace.py pull HERO.c4m shot_020 ~/workspace/

The workspace directory is reusable. Switching from shot_010 to shot_020
only transfers the files that differ. Shared elements (show LUTs,
reference plates, common textures) are already on disk.
"""

import argparse
import sys
from pathlib import Path

import c4py


def cmd_publish(args):
    """Scan a show directory into the store and save the show manifest.

    Run this once on the file server. The manifest becomes the
    source of truth for what content exists and where it lives.
    """
    store = c4py.open_store()
    print(f"Publishing {args.show_dir} → {args.output}")
    print("This scans every file and stores content by C4 ID.")
    print("First run may take a while. Subsequent runs reuse stored content.\n")

    manifest = c4py.scan(
        args.show_dir, store=store,
        progress=lambda p, i, _: print(f"\r  {i} files", end="", flush=True),
    )
    print()

    c4py.dump(manifest, args.output, pretty=True)
    print(f"\nShow manifest: {args.output}")
    print(f"  {manifest.summary()}")


def cmd_pull(args):
    """Pull a shot (or any subtree) into a local workspace.

    Only the files for the requested path are materialized. If the
    workspace already has files from a previous pull, only the
    differences are transferred.
    """
    store = c4py.open_store()
    show = c4py.load(args.manifest)

    # Filter to the requested subtree
    prefix = args.path.rstrip("/") + "/"
    subset = show.filter(lambda p, e: p.startswith(prefix) or p == args.path)

    if subset.file_count() == 0:
        print(f"No files found under '{args.path}' in {args.manifest}")
        print(f"\nAvailable top-level directories:")
        for path, entry in show.directories():
            if entry.depth == 0:
                print(f"  {entry.name}")
        sys.exit(1)

    print(f"Pulling {args.path} → {args.workspace}")
    print(f"  {subset.file_count()} files, {subset.human_total()}")

    ws = c4py.Workspace(args.workspace, store=store)
    result = ws.checkout(
        subset,
        progress=lambda op, path, i, total: print(
            f"\r  {op}: {path[:60]:<60}", end="", flush=True
        ),
    )
    print()
    print(f"  {result.created} created, {result.skipped} unchanged")


def cmd_status(args):
    """Show what's changed in the workspace since the last pull."""
    store = c4py.open_store()
    ws = c4py.Workspace(args.workspace, store=store)
    info = ws.status()

    if not info["has_manifest"]:
        print(f"Workspace {args.workspace}: no shot checked out")
        return

    print(f"Workspace: {args.workspace}")
    print(f"  Last pull: {info['last_checkout']}")

    diff = ws.diff_from_current()
    if diff.is_empty:
        print("  Clean — no changes since last pull")
        return

    if diff.added:
        print(f"\n  New files ({len(diff.added)}):")
        for e in diff.added[:15]:
            print(f"    + {e.name} ({e.human_size()})")
        if len(diff.added) > 15:
            print(f"    ... and {len(diff.added) - 15} more")

    if diff.modified:
        print(f"\n  Modified ({len(diff.modified)}):")
        for e in diff.modified[:15]:
            print(f"    ~ {e.name}")
        if len(diff.modified) > 15:
            print(f"    ... and {len(diff.modified) - 15} more")

    if diff.removed:
        print(f"\n  Removed ({len(diff.removed)}):")
        for e in diff.removed[:10]:
            print(f"    - {e.name}")


def cmd_snapshot(args):
    """Snapshot the current workspace state for publishing or versioning."""
    store = c4py.open_store()
    ws = c4py.Workspace(args.workspace, store=store)

    print(f"Snapshotting {args.workspace}...")
    manifest = ws.snapshot(
        store_content=True,
        progress=lambda p, i, _: print(f"\r  {i} files", end="", flush=True),
    )
    print()

    c4py.dump(manifest, args.output)
    print(f"Snapshot: {args.output}")
    print(f"  {manifest.summary()}")
    print(f"  C4 ID: {manifest.compute_c4id()}")


def cmd_compare(args):
    """Compare two versions of a shot to see what the artist changed."""
    old = c4py.load(args.old)
    new = c4py.load(args.new)

    result = c4py.diff(old, new)

    print(f"Old: {old.summary()}")
    print(f"New: {new.summary()}")

    if result.is_empty:
        print("\nIdentical.")
        return

    print(f"\n+{len(result.added)} added, -{len(result.removed)} removed, ~{len(result.modified)} modified")

    # Detect renames (same C4 ID at different path)
    removed_ids = {e.c4id: e for e in result.removed if e.c4id}
    for e in result.added:
        if e.c4id and e.c4id in removed_ids:
            old_e = removed_ids[e.c4id]
            print(f"  renamed: {old_e.name} → {e.name}")

    if result.modified:
        print("\nModified:")
        for e in result.modified:
            print(f"  {e.name} ({e.human_size()})")

    if result.added:
        pure_adds = [e for e in result.added if not (e.c4id and e.c4id in removed_ids)]
        if pure_adds:
            print("\nNew:")
            for e in pure_adds:
                print(f"  {e.name} ({e.human_size()})")


def cmd_reset(args):
    """Reset workspace to the last pulled state, undoing all local changes."""
    store = c4py.open_store()
    ws = c4py.Workspace(args.workspace, store=store)

    print(f"Resetting {args.workspace} to last pull state...")
    result = ws.reset()
    print(f"  {result.created} restored, {result.removed} removed, {result.skipped} unchanged")


def main():
    parser = argparse.ArgumentParser(
        description="VFX shot workspace management",
        epilog="Set C4_STORE to your show's content store before use.",
    )
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("publish", help="Scan show into store + manifest")
    p.add_argument("show_dir", help="Show root directory")
    p.add_argument("output", help="Output c4m file")

    p = sub.add_parser("pull", help="Pull a shot into workspace")
    p.add_argument("manifest", help="Show manifest c4m")
    p.add_argument("path", help="Path within the show (e.g., shots/shot_010)")
    p.add_argument("workspace", help="Local workspace directory")

    p = sub.add_parser("status", help="Show workspace changes")
    p.add_argument("workspace", help="Workspace directory")

    p = sub.add_parser("snapshot", help="Snapshot workspace for publish")
    p.add_argument("workspace", help="Workspace directory")
    p.add_argument("output", help="Output c4m file")

    p = sub.add_parser("compare", help="Compare two shot versions")
    p.add_argument("old", help="Old version c4m")
    p.add_argument("new", help="New version c4m")

    p = sub.add_parser("reset", help="Reset workspace to last pull")
    p.add_argument("workspace", help="Workspace directory")

    args = parser.parse_args()
    commands = {
        "publish": cmd_publish,
        "pull": cmd_pull,
        "status": cmd_status,
        "snapshot": cmd_snapshot,
        "compare": cmd_compare,
        "reset": cmd_reset,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
