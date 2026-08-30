from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .config import Config
from .git_ops import GitError, GitRepository
from .models import CLEANUP_PUBLICATION_PHASES, Job, PublicationEntry, PublicationJobRecord, PublicationJournal, PublicationOperation, PublicationPhase, PublicationSnapshotEntry
from .preparation import PreparationError
from .state import StateStore


class PublicationError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class PublicationFaults(Protocol):
    def hit(self, event: str, **details: Any) -> None: ...


class NoopFaults:
    def hit(self, event: str, **details: Any) -> None:
        return None


class PublicationCrash(RuntimeError):
    """Raised by tests at a durable boundary and intentionally not recovered in-process."""


@dataclass(frozen=True)
class PublicationResult:
    batch_id: str
    phase: PublicationPhase
    action: str | None = None
    commit_id: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None


class BatchPublication:
    def __init__(self, config: Config, state: StateStore, git: GitRepository, *, faults: PublicationFaults | None = None, candidate_cleanup: Callable[[str], None] | None = None):
        self.config = config
        self.state = state
        self.git = git
        self.faults = faults or NoopFaults()
        self.candidate_cleanup = candidate_cleanup
        self.root = config.state_dir / "publications"

    def recover_oldest(self) -> PublicationResult | None:
        journal = self.state.oldest_unresolved_publication()
        if not journal:
            return None
        batch_id = journal.batch_id
        phase = journal.blocked_from_phase or journal.phase
        try:
            self._validate_journal(journal)
        except PublicationError as exc:
            return self._block(journal, exc.code, exc.message)
        if phase == PublicationPhase.COMMITTED_UNFINALIZED:
            try:
                commit_id = self._verified_commit(journal)
            except PublicationError as exc:
                return self._block(journal, exc.code, exc.message)
            if not commit_id:
                return self._block(journal, "publication_commit_missing_or_mismatched", "the recorded commit could not be proven to match the journal")
            return self._finalize_existing(journal, commit_id, "recovered_committed_batch")
        if phase == PublicationPhase.CANDIDATE_VALIDATED:
            self.state.update_publication(batch_id, PublicationPhase.ROLLED_BACK, action="discarded_unpublished_candidate")
            self.state.fail_batch(batch_id)
            self._compact(batch_id)
            return PublicationResult(batch_id, PublicationPhase.ROLLED_BACK, "discarded_unpublished_candidate")
        try:
            commit_id = self._verified_commit(journal)
        except PublicationError as exc:
            return self._block(journal, exc.code, exc.message)
        if commit_id:
            return self._finalize_existing(journal, commit_id, "recovered_existing_commit")
        if journal.base_commit != self.git.head():
            return self._block(journal, "publication_head_changed", "Git HEAD changed before the batch could be reconciled")
        try:
            self._verify_live_state(journal)
            self.git.restore_staged([entry.relative_path for entry in self.state.publication_entries(batch_id)])
            self._restore_snapshot(journal)
        except PublicationError as exc:
            return self._block(journal, exc.code, exc.message)
        self.state.update_publication(batch_id, PublicationPhase.ROLLED_BACK, action="rolled_back_uncommitted", failure_code=None, failure_message=None)
        self.state.fail_batch(batch_id)
        self._compact(batch_id)
        return PublicationResult(batch_id, PublicationPhase.ROLLED_BACK, "rolled_back_uncommitted")

    def publish(self, batch_id: str, files: Mapping[str, str | bytes], deletions: tuple[str, ...], *, queue_path: str | None, queue_job_ids: list[str], source_jobs: list[Job], raw_fingerprints: dict[str, tuple[str, str | None]] | None = None, synthesis_metadata: Mapping[str, Any] | None = None) -> PublicationResult:
        entries = self._prepare_entries(files, deletions, queue_path)
        if not entries:
            raise PublicationError("empty_publication", "validated candidate has no changes")
        workspace = self.root / batch_id
        payload = workspace / "candidate"
        self._assert_external_path_safe(workspace, self.root, "candidate workspace")
        self._assert_external_path_safe(payload, workspace, "candidate payload")
        payload.mkdir(parents=True, exist_ok=True)
        manifest = {"batch_id": batch_id, "entries": [self._entry_manifest(entry) for entry in entries]}
        if synthesis_metadata is not None:
            try:
                json.dumps(synthesis_metadata, ensure_ascii=False, sort_keys=True)
            except (TypeError, ValueError) as exc:
                raise PublicationError("synthesis_metadata_invalid", "validated synthesis metadata is not serializable") from exc
            manifest["synthesis"] = dict(synthesis_metadata)
        for entry in entries:
            self._materialize_entry(payload, entry, files)
        self._validate_candidate_manifest(batch_id, workspace, manifest, entries)
        manifest_path = workspace / "manifest.json"
        self._assert_external_path_safe(manifest_path, workspace, "candidate manifest")
        self._write_json(manifest_path, manifest)
        manifest_hash = self._sha256_json(manifest)
        self.state.write_publication_journal(batch_id, phase=PublicationPhase.CANDIDATE_VALIDATED, candidate_workspace=str(workspace), candidate_manifest_hash=manifest_hash, base_commit=self.git.head(), entries=entries, jobs=self._job_records(queue_job_ids, source_jobs, raw_fingerprints or {}))
        self._fault("journal_written", batch_id=batch_id)

        snapshot = self._capture_snapshot(workspace, entries)
        self.state.record_snapshot(batch_id, str(workspace / "rollback"), snapshot["hash"])
        self.state.update_publication(batch_id, PublicationPhase.ROLLBACK_SNAPSHOT_RECORDED)
        self._fault("snapshot_captured", batch_id=batch_id)

        self.state.update_publication(batch_id, PublicationPhase.PUBLISHING)
        for entry in entries:
            self._publish_entry(entry, payload)
            self._fault("file_published", batch_id=batch_id, path=entry.relative_path)
        self.state.update_publication(batch_id, PublicationPhase.PUBLISHED_UNCOMMITTED)
        self._fault("published_uncommitted", batch_id=batch_id)

        paths = [entry.relative_path for entry in entries]
        message = f"ingest: batch {batch_id} ({len(source_jobs)} complete)\n\nBatch-ID: {batch_id}"
        try:
            commit_id = self.git.commit_paths(paths, message)
        except GitError as exc:
            journal = self._journal(batch_id)
            try:
                commit_id = self._verified_commit(journal)
            except PublicationError as mismatch:
                return self._block(journal, mismatch.code, mismatch.message)
            if not commit_id:
                try:
                    self._verify_live_state(journal)
                    self._restore_snapshot(journal)
                    self.state.update_publication(batch_id, PublicationPhase.ROLLED_BACK, action="rolled_back_git_failure", failure_code="git_commit_failed", failure_message=str(exc))
                    self.state.fail_batch(batch_id)
                    self._compact(batch_id)
                    return PublicationResult(batch_id, PublicationPhase.ROLLED_BACK, "rolled_back_git_failure", failure_code="git_commit_failed", failure_message=str(exc))
                except PublicationError as rollback_error:
                    return self._block(journal, rollback_error.code, rollback_error.message)
            else:
                return self._finalize_existing(journal, commit_id, "recovered_commit_after_git_error")
        self._fault("git_commit", batch_id=batch_id, commit_id=commit_id)
        self.state.update_publication(batch_id, PublicationPhase.COMMITTED_UNFINALIZED, commit_id=commit_id)
        self._fault("commit_journaled", batch_id=batch_id, commit_id=commit_id)
        return self._finalize_existing(self._journal(batch_id), commit_id, "published_and_committed")

    def retry_cleanup(self) -> PublicationResult | None:
        for journal in reversed(self.state.list_publications()):
            if journal.phase not in CLEANUP_PUBLICATION_PHASES:
                continue
            self._cleanup_journal(journal)
            current = self.state.publication(journal.batch_id) or journal
            return PublicationResult(
                journal.batch_id,
                current.phase,
                current.recovery_action,
                current.commit_id,
                current.failure_code,
                current.failure_message,
            )
        return None

    def _finalize_existing(self, journal: PublicationJournal, commit_id: str, action: str) -> PublicationResult:
        batch_id = journal.batch_id
        entries = self.state.publication_entries(batch_id)
        hashes = {entry.relative_path: entry.candidate_hash for entry in entries}
        paths = [entry.relative_path for entry in entries]
        if not self.git.verify_commit(commit_id, paths, hashes, journal.base_commit, journal.batch_id):
            return self._block(journal, "publication_commit_mismatch", "the discovered commit does not match the journaled path set or content")
        if journal.phase != PublicationPhase.COMMITTED_UNFINALIZED:
            self.state.update_publication(batch_id, PublicationPhase.COMMITTED_UNFINALIZED, commit_id=commit_id)
            journal = self._journal(batch_id)
        try:
            for entry in entries:
                target = self._live_path(entry.relative_path)
                self._assert_parent_safe(target)
                current = self._sha256_file(target) if target.exists() else None
                if current != entry.candidate_hash:
                    raise PublicationError("live_path_diverged", f"live path no longer matches the committed batch: {entry.relative_path}")
        except PublicationError as exc:
            return self._block(journal, exc.code, exc.message)
        try:
            self.state.finalize_publication(batch_id, commit_id)
        except (KeyError, ValueError) as exc:
            return self._block(journal, "sqlite_finalization_mismatch", str(exc))
        self._fault("sqlite_finalized", batch_id=batch_id, commit_id=commit_id)
        self._cleanup_journal(self.state.publication(batch_id) or journal)
        final = self.state.publication(batch_id) or journal
        return PublicationResult(batch_id, final.phase, action, commit_id, final.failure_code, final.failure_message)

    def _cleanup_journal(self, journal: PublicationJournal) -> int:
        batch_id = journal.batch_id
        jobs = self.state.publication_jobs(batch_id)
        pending = []
        for item in jobs:
            job = self.state.get(item.job_id)
            if item.role == "source" and job is not None and job.cleanup_pending:
                pending.append(item)
        if pending:
            self.state.mark_publication_cleanup_pending(batch_id)
        cleaned = 0
        for item in pending:
            path = Path(item.raw_path) if item.raw_path else None
            try:
                if path is not None:
                    self._assert_external_path_safe(path, self.config.to_ingest, "raw payload")
                    if path.exists():
                        if not item.raw_hash:
                            raise PublicationError("cleanup_payload_unverified", f"raw payload fingerprint is unavailable: {path.name}")
                        if self._sha256_file(path) != item.raw_hash:
                            raise PublicationError("cleanup_payload_changed", f"raw payload changed before cleanup: {path.name}")
                        path.unlink()
                        self._fsync_dir(path.parent)
                        self._fault("raw_payload_removed", batch_id=batch_id, job_id=item.job_id)
                if self.candidate_cleanup is not None:
                    self.candidate_cleanup(item.job_id)
                self.state.mark_cleanup_done(item.job_id)
                self._fault("cleanup_marked", batch_id=batch_id, job_id=item.job_id)
                cleaned += 1
            except PublicationError as exc:
                self.state.update_publication(batch_id, PublicationPhase.CLEANUP_PENDING, action="cleanup_pending", failure_code=exc.code, failure_message=exc.message)
            except PreparationError as exc:
                self.state.update_publication(batch_id, PublicationPhase.CLEANUP_PENDING, action="cleanup_pending", failure_code=exc.code.value, failure_message=exc.message)
            except OSError as exc:
                self.state.update_publication(batch_id, PublicationPhase.CLEANUP_PENDING, action="cleanup_pending", failure_code="cleanup_failed", failure_message=str(exc))
        remaining = [
            item
            for item in jobs
            if item.role == "source"
            and (job := self.state.get(item.job_id)) is not None
            and job.cleanup_pending
        ]
        if not remaining:
            self.state.mark_publication_complete(batch_id)
            self._compact(batch_id)
        return cleaned

    def _prepare_entries(self, files: Mapping[str, str | bytes], deletions: tuple[str, ...], queue_path: str | None) -> list[PublicationEntry]:
        operations: dict[str, tuple[PublicationOperation, str | None]] = {}
        for path, content in files.items():
            self._validate_path(path, queue_path)
            if path in operations:
                raise PublicationError("duplicate_candidate_path", f"duplicate candidate path: {path}")
            operations[path] = ("write", self._content_hash(content))
        for path in deletions:
            self._validate_path(path, queue_path)
            if not (path.startswith("Concepts/") or path.startswith("Sources/")):
                raise PublicationError("invalid_deletion_scope", f"deletion is outside Sources/ and Concepts/: {path}")
            if path in operations:
                raise PublicationError("duplicate_candidate_path", f"path is both written and deleted: {path}")
            operations[path] = ("delete", None)
        ordered = []
        seen_casefold: set[str] = set()
        for path in sorted(operations):
            if path.casefold() in seen_casefold:
                raise PublicationError("duplicate_candidate_path", f"case-folded duplicate path: {path}")
            seen_casefold.add(path.casefold())
            operation, candidate_hash = operations[path]
            target = self._live_path(path)
            if operation == "write" and target.exists() and not target.is_symlink() and self._sha256_file(target) == candidate_hash:
                continue
            if operation == "delete" and not target.exists():
                continue
            ordered.append(PublicationEntry(relative_path=path, operation=operation, candidate_hash=candidate_hash))
        return ordered

    def _capture_snapshot(self, workspace: Path, entries: list[PublicationEntry]) -> dict:
        rollback = workspace / "rollback"
        payload = rollback / "payload"
        self._assert_external_path_safe(rollback, workspace, "rollback workspace")
        self._assert_external_path_safe(payload, rollback, "rollback payload")
        payload.mkdir(parents=True, exist_ok=True)
        snapshot_entries: list[PublicationSnapshotEntry] = []
        for entry in entries:
            target = self._live_path(entry.relative_path)
            if target.exists() or target.is_symlink():
                self._assert_regular(target, "live path")
                content = target.read_bytes()
                snapshot_entries.append(PublicationSnapshotEntry(entry.relative_path, True, hashlib.sha256(content).hexdigest()))
                destination = payload / entry.relative_path
                self._assert_external_path_safe(destination, payload, "rollback payload")
                destination.parent.mkdir(parents=True, exist_ok=True)
                self._atomic_replace_bytes(destination, content, f".{destination.name}.snapshot.")
            else:
                snapshot_entries.append(PublicationSnapshotEntry(entry.relative_path, False, None))
        manifest = {"entries": [self._snapshot_manifest(entry) for entry in snapshot_entries]}
        manifest_path = rollback / "manifest.json"
        self._assert_external_path_safe(manifest_path, rollback, "rollback manifest")
        self._write_json(manifest_path, manifest)
        return {"hash": self._sha256_json(manifest), "entries": snapshot_entries}

    def _materialize_entry(self, payload: Path, entry: PublicationEntry, files: Mapping[str, str | bytes]) -> None:
        if entry.operation != "write":
            return
        destination = payload / entry.relative_path
        self._assert_external_path_safe(destination, payload, "candidate payload")
        destination.parent.mkdir(parents=True, exist_ok=True)
        content = files[entry.relative_path]
        self._atomic_replace_bytes(destination, content if isinstance(content, bytes) else content.encode("utf-8"), f".{destination.name}.candidate.")

    def _publish_entry(self, entry: PublicationEntry, payload: Path) -> None:
        target = self._live_path(entry.relative_path)
        self._assert_parent_safe(target)
        if entry.operation == "delete":
            if target.exists():
                self._assert_regular(target, "live path")
                target.unlink()
                self._fsync_dir(target.parent)
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        candidate = payload / entry.relative_path
        self._assert_external_path_safe(candidate, payload, "candidate payload")
        self._atomic_replace_bytes(target, candidate.read_bytes(), f".{target.name}.")

    def _verify_live_state(self, journal: PublicationJournal) -> None:
        workspace = Path(journal.candidate_workspace)
        manifest_path = workspace / "manifest.json"
        snapshot_path = workspace / "rollback" / "manifest.json"
        self._assert_external_path_safe(manifest_path, workspace, "candidate manifest")
        self._assert_external_path_safe(snapshot_path, workspace / "rollback", "rollback manifest")
        manifest = self._read_json(manifest_path)
        snapshot = self._read_json(snapshot_path)
        by_path = {entry.relative_path: entry for entry in self._manifest_entries(manifest)}
        snap_by_path = {entry.relative_path: entry for entry in self._snapshot_entries(snapshot)}
        for path, candidate in by_path.items():
            target = self._live_path(path)
            snap = snap_by_path[path]
            if target.exists():
                self._assert_regular(target, "live path")
                current = self._sha256_file(target)
            else:
                current = None
            if current not in {snap.content_hash, candidate.candidate_hash}:
                raise PublicationError("live_path_diverged", f"live path changed outside the batch: {path}")

    def _validate_journal(self, journal: PublicationJournal) -> None:
        workspace = Path(journal.candidate_workspace)
        self._assert_external_path_safe(workspace, self.root, "candidate workspace")
        manifest_path = workspace / "manifest.json"
        self._assert_external_path_safe(manifest_path, workspace, "candidate manifest")
        manifest = self._read_json(manifest_path)
        if manifest.get("batch_id") != journal.batch_id or self._sha256_json(manifest) != journal.candidate_manifest_hash:
            raise PublicationError("manifest_invalid", "candidate manifest identity or contents do not match the journal")
        db_entries = self.state.publication_entries(journal.batch_id)
        manifest_entries = self._manifest_entries(manifest)
        if manifest_entries != db_entries:
            raise PublicationError("manifest_invalid", "candidate manifest path set does not match the journal")
        for entry in db_entries:
            if entry.operation == "write":
                candidate = workspace / "candidate" / entry.relative_path
                self._assert_external_path_safe(candidate, workspace / "candidate", "candidate payload")
                if not candidate.is_file() or self._sha256_file(candidate) != entry.candidate_hash:
                    raise PublicationError("manifest_invalid", f"candidate payload does not match its fingerprint: {entry.relative_path}")
        if (journal.blocked_from_phase or journal.phase) != PublicationPhase.CANDIDATE_VALIDATED:
            rollback = Path(journal.snapshot_workspace or workspace / "rollback")
            self._assert_external_path_safe(rollback, workspace, "rollback workspace")
            snapshot_path = rollback / "manifest.json"
            self._assert_external_path_safe(snapshot_path, rollback, "rollback manifest")
            snapshot = self._read_json(snapshot_path)
            if journal.snapshot_manifest_hash != self._sha256_json(snapshot):
                raise PublicationError("snapshot_invalid", "rollback snapshot identity does not match the journal")
            for entry in self._snapshot_entries(snapshot):
                if entry.existed:
                    payload = rollback / "payload" / entry.relative_path
                    self._assert_external_path_safe(payload, rollback / "payload", "rollback payload")
                    if not payload.is_file() or self._sha256_file(payload) != entry.content_hash:
                        raise PublicationError("snapshot_invalid", f"rollback payload does not match its fingerprint: {entry.relative_path}")

    def _restore_snapshot(self, journal: PublicationJournal) -> None:
        workspace = Path(journal.candidate_workspace)
        rollback = workspace / "rollback"
        snapshot_path = rollback / "manifest.json"
        self._assert_external_path_safe(snapshot_path, rollback, "rollback manifest")
        snapshot = self._read_json(snapshot_path)
        payload = workspace / "rollback" / "payload"
        for entry in self._snapshot_entries(snapshot):
            target = self._live_path(entry.relative_path)
            self._assert_parent_safe(target)
            if entry.existed:
                source = payload / entry.relative_path
                self._assert_external_path_safe(source, payload, "rollback payload")
                target.parent.mkdir(parents=True, exist_ok=True)
                self._atomic_replace_bytes(target, source.read_bytes(), f".{target.name}.rollback.")
            elif target.exists():
                target.unlink()
                self._fsync_dir(target.parent)

    def _verified_commit(self, journal: PublicationJournal) -> str | None:
        commit_id = journal.commit_id or self.git.find_batch_commit(journal.batch_id)
        if not commit_id:
            return None
        entries = self.state.publication_entries(journal.batch_id)
        if not self.git.verify_commit(commit_id, [entry.relative_path for entry in entries], {entry.relative_path: entry.candidate_hash for entry in entries}, journal.base_commit, journal.batch_id):
            raise PublicationError("publication_commit_mismatch", "a commit with the batch identifier does not match the journaled path set or content")
        return commit_id

    def _block(self, journal: PublicationJournal, code: str, message: str) -> PublicationResult:
        batch_id = journal.batch_id
        previous = journal.blocked_from_phase or journal.phase
        self.state.update_publication(batch_id, PublicationPhase.RECOVERY_BLOCKED, action="recovery_blocked", failure_code=code, failure_message=message, blocked_from_phase=previous)
        return PublicationResult(batch_id, PublicationPhase.RECOVERY_BLOCKED, "recovery_blocked", journal.commit_id, code, message)

    def _job_records(self, queue_job_ids: list[str], source_jobs: list[Job], raw_fingerprints: dict[str, tuple[str, str | None]]) -> list[PublicationJobRecord]:
        records = [PublicationJobRecord(job_id=job_id, role="queue_ack") for job_id in queue_job_ids]
        for job in source_jobs:
            raw_path, raw_hash = raw_fingerprints.get(job.id, (job.input_artifact, None))
            records.append(PublicationJobRecord(job_id=job.id, role="source", expected_content_hash=job.content_hash, raw_path=raw_path, raw_hash=raw_hash))
        return records

    def _validate_candidate_manifest(self, batch_id: str, workspace: Path, manifest: dict, entries: list[PublicationEntry]) -> None:
        if manifest.get("batch_id") != batch_id or manifest.get("entries") != [self._entry_manifest(entry) for entry in entries]:
            raise PublicationError("manifest_invalid", "candidate manifest does not match the validated candidate")
        for entry in entries:
            if entry.operation != "write":
                continue
            payload = workspace / "candidate" / entry.relative_path
            self._assert_external_path_safe(payload, workspace / "candidate", "candidate payload")
            if not payload.is_file() or self._sha256_file(payload) != entry.candidate_hash:
                raise PublicationError("manifest_invalid", f"candidate payload does not match its fingerprint: {entry.relative_path}")

    @staticmethod
    def _entry_manifest(entry: PublicationEntry) -> dict:
        return {"path": entry.relative_path, "operation": entry.operation, "candidate_hash": entry.candidate_hash}

    @staticmethod
    def _snapshot_manifest(entry: PublicationSnapshotEntry) -> dict:
        return {"path": entry.relative_path, "exists": entry.existed, "hash": entry.content_hash}

    @staticmethod
    def _manifest_entries(manifest: dict) -> list[PublicationEntry]:
        try:
            values = manifest["entries"]
            entries = [PublicationEntry(relative_path=item["path"], operation=item["operation"], candidate_hash=item.get("candidate_hash")) for item in values]
        except (KeyError, TypeError) as exc:
            raise PublicationError("manifest_invalid", "candidate manifest entries are malformed") from exc
        if any(entry.operation not in {"write", "delete"} for entry in entries):
            raise PublicationError("manifest_invalid", "candidate manifest contains an unknown operation")
        return entries

    @staticmethod
    def _snapshot_entries(manifest: dict) -> list[PublicationSnapshotEntry]:
        try:
            values = manifest["entries"]
            entries = [PublicationSnapshotEntry(relative_path=item["path"], existed=item["exists"], content_hash=item.get("hash")) for item in values]
        except (KeyError, TypeError) as exc:
            raise PublicationError("snapshot_invalid", "rollback snapshot entries are malformed") from exc
        if any(not isinstance(entry.existed, bool) for entry in entries):
            raise PublicationError("snapshot_invalid", "rollback snapshot contains an invalid existence marker")
        return entries

    def _journal(self, batch_id: str) -> PublicationJournal:
        journal = self.state.publication(batch_id)
        if journal is None:
            raise PublicationError("publication_journal_missing", f"publication journal disappeared: {batch_id}")
        return journal

    def _validate_path(self, relative: str, queue_path: str | None) -> None:
        if not relative or "\x00" in relative or "\\" in relative:
            raise PublicationError("invalid_candidate_path", f"invalid candidate path: {relative!r}")
        path = Path(relative)
        if path.is_absolute() or "." in path.parts or ".." in path.parts:
            raise PublicationError("invalid_candidate_path", f"candidate path escapes the vault: {relative}")
        if queue_path and relative == queue_path:
            self._assert_parent_safe(self._live_path(relative))
            return
        if not (relative.startswith("Sources/") or relative.startswith("Concepts/")):
            raise PublicationError("invalid_candidate_scope", f"candidate path is outside Sources/ and Concepts/: {relative}")
        self._assert_parent_safe(self._live_path(relative))

    def _live_path(self, relative: str) -> Path:
        target = self.config.vault / relative
        self._assert_under(target, self.config.vault, "live path")
        return target

    @staticmethod
    def _assert_under(path: Path, root: Path, label: str) -> None:
        try:
            path.resolve(strict=False).relative_to(root.resolve())
        except ValueError as exc:
            raise PublicationError("path_escape", f"{label} escapes its configured root: {path}") from exc

    @staticmethod
    def _assert_external_path_safe(path: Path, root: Path, label: str) -> None:
        if ".." in path.parts:
            raise PublicationError("path_escape", f"{label} escapes its configured root: {path}")
        root_absolute = Path(os.path.abspath(root))
        path_absolute = Path(os.path.abspath(path))
        try:
            relative = path_absolute.relative_to(root_absolute)
        except ValueError as exc:
            raise PublicationError("path_escape", f"{label} escapes its configured root: {path}") from exc
        current = root_absolute
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise PublicationError("symlink_escape", f"{label} traverses a symlink: {current}")

    def _assert_parent_safe(self, target: Path) -> None:
        root = self.config.vault.resolve()
        current = target.parent
        while current != root:
            if current.is_symlink():
                raise PublicationError("symlink_escape", f"path traverses a symlink: {current}")
            current = current.parent
            if root not in current.parents and current != root:
                raise PublicationError("path_escape", f"path escapes the vault: {target}")
        if target.is_symlink():
            raise PublicationError("symlink_escape", f"target is a symlink: {target}")

    @staticmethod
    def _assert_regular(path: Path, label: str) -> None:
        if path.is_symlink() or not path.is_file():
            raise PublicationError("unsafe_live_path", f"{label} is not a regular file: {path}")

    @staticmethod
    def _sha256_file(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    @staticmethod
    def _content_hash(content: str | bytes) -> str:
        return hashlib.sha256(content if isinstance(content, bytes) else content.encode("utf-8")).hexdigest()

    @staticmethod
    def _sha256_json(value: dict) -> str:
        return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()

    @staticmethod
    def _write_json(path: Path, value: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        BatchPublication._fsync_dir(path.parent)

    @staticmethod
    def _read_json(path: Path) -> dict:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise PublicationError("manifest_invalid", f"cannot read publication manifest: {path}") from exc

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @classmethod
    def _atomic_replace_bytes(cls, target: Path, content: bytes, prefix: str) -> None:
        fd, temporary = tempfile.mkstemp(prefix=prefix, dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            cls._fsync_dir(target.parent)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _compact(self, batch_id: str) -> None:
        workspace = self.root / batch_id
        if not workspace.exists():
            return
        candidate = workspace / "candidate"
        rollback_payload = workspace / "rollback" / "payload"
        self._assert_external_path_safe(workspace, self.root, "candidate workspace")
        self._assert_external_path_safe(candidate, workspace, "candidate payload")
        self._assert_external_path_safe(rollback_payload, workspace / "rollback", "rollback payload")
        shutil.rmtree(candidate, ignore_errors=True)
        shutil.rmtree(rollback_payload, ignore_errors=True)

    def _fault(self, event: str, **details: Any) -> None:
        self.faults.hit(event, **details)
