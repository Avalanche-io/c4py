"""CLI entry point for c4py — invoked via `python -m c4py`.

Commands:
    id <path>           Identify file/directory, output c4m
    id -i <path>        Bare C4 ID only (no c4m format)
    diff <old> <new>    Compare two c4m manifests
    verify <c4m> <dir>  Verify directory matches manifest
    cat <c4id>          Retrieve content from store
    version             Print version
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _cmd_id(args: argparse.Namespace) -> int:
    """Identify a file or directory."""
    from . import __version__  # noqa: F401
    from .encoder import dumps
    from .id import identify_file
    from .scanner import scan

    target = Path(args.path)

    if not target.exists():
        print(f"error: {args.path}: no such file or directory", file=sys.stderr)
        return 1

    if args.id_only:
        # Bare C4 ID mode
        if target.is_file():
            c4id = identify_file(target)
            print(str(c4id))
        elif target.is_dir():
            manifest = scan(target)
            c4id = manifest.compute_c4id()
            print(str(c4id))
        else:
            print(f"error: {args.path}: unsupported file type", file=sys.stderr)
            return 1
    else:
        # Full c4m output
        if target.is_file():
            c4id = identify_file(target)
            print(str(c4id))
        elif target.is_dir():
            manifest = scan(target)
            text = dumps(manifest)
            sys.stdout.write(text)
        else:
            print(f"error: {args.path}: unsupported file type", file=sys.stderr)
            return 1

    return 0


def _cmd_diff(args: argparse.Namespace) -> int:
    """Compare two c4m manifests."""
    from .decoder import load
    from .diff import diff

    old_path = args.old
    new_path = args.new

    for p in (old_path, new_path):
        if not Path(p).exists():
            print(f"error: {p}: no such file", file=sys.stderr)
            return 1

    old_manifest = load(old_path)
    new_manifest = load(new_path)
    result = diff(old_manifest, new_manifest)

    if result.is_empty:
        print("no differences")
        return 0

    if result.added:
        for entry in result.added:
            print(f"+ {entry.name}")
    if result.removed:
        for entry in result.removed:
            print(f"- {entry.name}")
    if result.modified:
        for entry in result.modified:
            print(f"~ {entry.name}")

    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    """Verify a directory matches a manifest."""
    from .verify import verify_tree

    manifest_path = args.manifest
    directory = args.directory

    if not Path(manifest_path).exists():
        print(f"error: {manifest_path}: no such file", file=sys.stderr)
        return 1
    if not Path(directory).is_dir():
        print(f"error: {directory}: not a directory", file=sys.stderr)
        return 1

    report = verify_tree(manifest_path, directory)

    if report.is_ok:
        print(f"OK: {len(report.ok)} files verified")
        return 0

    if report.ok:
        print(f"ok: {len(report.ok)} files match")
    if report.missing:
        print(f"missing: {len(report.missing)} files")
        for p in report.missing:
            print(f"  - {p}")
    if report.corrupt:
        print(f"corrupt: {len(report.corrupt)} files")
        for c in report.corrupt:
            print(f"  ! {c.path}")
            print(f"    expected: {c.expected}")
            print(f"    actual:   {c.actual}")
    if report.extra:
        print(f"extra: {len(report.extra)} files")
        for p in report.extra:
            print(f"  + {p}")

    return 1


def _cmd_cat(args: argparse.Namespace) -> int:
    """Retrieve content from store by C4 ID."""
    from .id import parse
    from .store import ContentNotFound, open_store

    try:
        c4id = parse(args.c4id)
    except ValueError as e:
        print(f"error: invalid C4 ID: {e}", file=sys.stderr)
        return 1

    try:
        store = open_store()
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    try:
        stream = store.get(c4id)
    except ContentNotFound:
        print(f"error: content not found: {args.c4id}", file=sys.stderr)
        return 1

    try:
        stdout_bin = sys.stdout.buffer
        while True:
            chunk = stream.read(65536)
            if not chunk:
                break
            stdout_bin.write(chunk)
    finally:
        stream.close()

    return 0


def _cmd_version(_args: argparse.Namespace) -> int:
    """Print version."""
    from . import __version__
    print(f"c4py {__version__}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate subcommand."""
    parser = argparse.ArgumentParser(
        prog="c4py",
        description="C4 universal content identification — SMPTE ST 2114:2017",
    )
    subparsers = parser.add_subparsers(dest="command")

    # id
    id_parser = subparsers.add_parser("id", help="identify file or directory")
    id_parser.add_argument("path", help="file or directory to identify")
    id_parser.add_argument(
        "-i", "--id-only", action="store_true",
        help="output bare C4 ID only (no c4m format)",
    )

    # diff
    diff_parser = subparsers.add_parser("diff", help="compare two c4m manifests")
    diff_parser.add_argument("old", help="old manifest path")
    diff_parser.add_argument("new", help="new manifest path")

    # verify
    verify_parser = subparsers.add_parser("verify", help="verify directory matches manifest")
    verify_parser.add_argument("manifest", help="c4m manifest path")
    verify_parser.add_argument("directory", help="directory to verify")

    # cat
    cat_parser = subparsers.add_parser("cat", help="retrieve content from store")
    cat_parser.add_argument("c4id", help="C4 ID to retrieve")

    # version
    subparsers.add_parser("version", help="print version")

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    dispatch = {
        "id": _cmd_id,
        "diff": _cmd_diff,
        "verify": _cmd_verify,
        "cat": _cmd_cat,
        "version": _cmd_version,
    }

    return dispatch[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
