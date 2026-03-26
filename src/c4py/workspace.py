"""Workspace — a directory backed by a content store that switches between manifests.

A workspace is like a Docker container for data: check out a manifest to
materialize it, switch to a different manifest to get different data, reset
to undo changes, snapshot to capture the current state. All backed by a
shared content store — content exists once, arrangements are free.

    ws = c4py.Workspace("./data", store=store)
    ws.checkout(training_data)           # materialize
    # ... train, modify files ...
    ws.reset()                           # undo all changes
    ws.checkout(different_experiment)    # switch to different data
    ws.snapshot()                        # capture current state

The key insight: checkout is declarative. "Make this directory look like
this manifest." It adds what's new, removes what's gone, skips what
matches. Switching between manifests that share 90% of their content
only touches the 10% that differs.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .diff import DiffResult, diff
from .manifest import Manifest
from .reconcile import ReconcilePlan, ReconcileResult, reconcile
from .scanner import scan
from .store import Store, open_store


@dataclass
class WorkspaceState:
    """Persisted workspace metadata."""
    manifest_c4id: str = ""
    manifest_path: str = ""
    created: str = ""
    last_checkout: str = ""


class Workspace:
    """A directory backed by a content store that switches between manifests.

    Like Docker but for data — each checkout gives you a different
    arrangement of content, materialized from a shared store. Creating
    views (filter, merge, subset) is instant because it's manifest
    manipulation. Materialization only transfers the bytes that differ
    from what's already on disk.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        store: Store | None = None,
    ) -> None:
        self.path = Path(path)
        self.store = store or open_store()
        self.current: Manifest | None = None
        self._state_file = self.path / ".c4-workspace.json"
        self._manifest_file = self.path / ".c4-workspace-manifest.c4m"
        self._load_state()

    def _load_state(self) -> None:
        """Load persisted workspace state if it exists."""
        if self._state_file.exists():
            try:
                data = json.loads(self._state_file.read_text())
                self._state = WorkspaceState(**data)
            except (json.JSONDecodeError, TypeError):
                self._state = WorkspaceState()
        else:
            self._state = WorkspaceState(
                created=datetime.now(timezone.utc).isoformat(),
            )

        # Reload the manifest from disk if it was previously saved.
        if self._manifest_file.exists() and self.current is None:
            try:
                from .decoder import load as load_manifest
                self.current = load_manifest(str(self._manifest_file))
            except Exception:
                self.current = None

    def _save_state(self) -> None:
        """Persist workspace state."""
        self.path.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(json.dumps(asdict(self._state), indent=2))

    def _save_manifest(self) -> None:
        """Persist the current manifest as a c4m file alongside the state file."""
        if self.current is None:
            if self._manifest_file.exists():
                self._manifest_file.unlink()
            return
        from .encoder import dump as dump_manifest
        self.path.mkdir(parents=True, exist_ok=True)
        dump_manifest(self.current, str(self._manifest_file))

    def checkout(
        self,
        manifest: Manifest | str | os.PathLike[str],
        *,
        progress: Callable[[str, str, int, int], None] | None = None,
        dry_run: bool = False,
    ) -> ReconcileResult | ReconcilePlan:
        """Make the directory match this manifest.

        Only transfers content that differs from what's already on disk.
        Switching between manifests that share 90% of their content
        only touches the 10% that differs.

        Args:
            manifest: Manifest object or path to c4m file
            progress: callback(op_type, path, index, total)
            dry_run: if True, return a plan without executing

        Returns:
            ReconcileResult (or ReconcilePlan if dry_run)
        """
        if isinstance(manifest, str | os.PathLike):
            from .decoder import load
            manifest = load(str(manifest))

        result = reconcile(
            manifest, self.path,
            store=self.store,
            progress=progress,
            dry_run=dry_run,
        )

        if not dry_run:
            self.current = manifest
            self._state.manifest_c4id = str(manifest.compute_c4id())
            self._state.last_checkout = datetime.now(timezone.utc).isoformat()
            self._save_state()
            self._save_manifest()

        return result

    def snapshot(
        self,
        *,
        store_content: bool = True,
        progress: Callable[[str, int, int], None] | None = None,
    ) -> Manifest:
        """Capture the current directory state as a manifest.

        If store_content is True (default), file content is stored in the
        content store during the scan — so you can check out this snapshot
        later and all content will be available.

        Args:
            store_content: store file content during scan
            progress: callback(path, index, total_estimate)

        Returns:
            Manifest describing the current directory state
        """
        return scan(
            self.path,
            store=self.store if store_content else None,
            progress=progress,
        )

    def reset(
        self, *, progress: Callable[[str, str, int, int], None] | None = None,
    ) -> ReconcileResult | ReconcilePlan:
        """Restore the directory to the last checked-out manifest.

        Undoes any modifications made since the last checkout.

        Raises:
            RuntimeError: if no manifest has been checked out
        """
        if self.current is None:
            raise RuntimeError("no manifest checked out — nothing to reset to")
        return self.checkout(self.current, progress=progress)

    def diff_from_current(
        self, *, progress: Callable[[str, int, int], None] | None = None,
    ) -> DiffResult:
        """Compare the current directory state against the checked-out manifest.

        Shows what has been added, removed, or modified since checkout.

        Raises:
            RuntimeError: if no manifest has been checked out
        """
        if self.current is None:
            raise RuntimeError("no manifest checked out — nothing to diff against")
        actual = scan(self.path, progress=progress)
        return diff(self.current, actual)

    def status(self) -> dict[str, object]:
        """Return workspace status information."""
        exists = self.path.exists()
        return {
            "path": str(self.path),
            "exists": exists,
            "has_manifest": self.current is not None,
            "manifest_c4id": self._state.manifest_c4id or None,
            "created": self._state.created or None,
            "last_checkout": self._state.last_checkout or None,
        }
