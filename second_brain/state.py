from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .models import Job, utc_now


PUBLICATION_PHASES = frozenset({
    "candidate_validated",
    "rollback_snapshot_recorded",
    "publishing",
    "published_uncommitted",
    "committed_unfinalized",
    "finalized",
    "cleanup_pending",
    "complete",
    "rolled_back",
    "recovery_blocked",
})


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS batches (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  commit_id TEXT
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('article', 'youtube')),
  source_key TEXT NOT NULL UNIQUE,
  original_locator TEXT NOT NULL,
  input_artifact TEXT,
  batch_id TEXT REFERENCES batches(id),
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  retryable INTEGER NOT NULL DEFAULT 1,
  failure_code TEXT,
  failure_message TEXT,
  content_hash TEXT,
  source_version INTEGER,
  captured_at TEXT,
  queue_acknowledged INTEGER NOT NULL DEFAULT 0,
  cleanup_pending INTEGER NOT NULL DEFAULT 0,
  commit_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_batch_idx ON jobs(batch_id);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status);
CREATE TABLE IF NOT EXISTS publication_journals (
  batch_id TEXT PRIMARY KEY REFERENCES batches(id),
  phase TEXT NOT NULL,
  candidate_workspace TEXT NOT NULL,
  candidate_manifest_hash TEXT NOT NULL,
  snapshot_workspace TEXT,
  snapshot_manifest_hash TEXT,
  base_commit TEXT NOT NULL,
  commit_id TEXT,
  blocked_from_phase TEXT,
  recovery_action TEXT,
  failure_code TEXT,
  failure_message TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS publication_entries (
  batch_id TEXT NOT NULL REFERENCES publication_journals(batch_id),
  ordinal INTEGER NOT NULL,
  relative_path TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('write', 'delete')),
  candidate_hash TEXT,
  PRIMARY KEY (batch_id, ordinal),
  UNIQUE (batch_id, relative_path)
);
CREATE TABLE IF NOT EXISTS publication_jobs (
  batch_id TEXT NOT NULL REFERENCES publication_journals(batch_id),
  job_id TEXT NOT NULL REFERENCES jobs(id),
  role TEXT NOT NULL CHECK (role IN ('source', 'queue_ack')),
  expected_content_hash TEXT,
  raw_path TEXT,
  raw_hash TEXT,
  PRIMARY KEY (batch_id, job_id, role)
);
CREATE INDEX IF NOT EXISTS publication_phase_idx ON publication_journals(phase, created_at);
"""


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def create_batch(self) -> str:
        batch_id = uuid.uuid4().hex
        now = utc_now()
        with self.transaction() as db:
            db.execute("INSERT INTO batches VALUES (?, 'running', ?, ?, NULL)", (batch_id, now, now))
        return batch_id

    def write_publication_journal(self, batch_id: str, *, phase: str, candidate_workspace: str, candidate_manifest_hash: str, base_commit: str, entries: list[dict], jobs: list[dict]) -> None:
        now = utc_now()
        with self.transaction() as db:
            db.execute("""INSERT INTO publication_journals
                (batch_id, phase, candidate_workspace, candidate_manifest_hash, base_commit, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""", (batch_id, phase, candidate_workspace, candidate_manifest_hash, base_commit, now, now))
            db.executemany("""INSERT INTO publication_entries
                (batch_id, ordinal, relative_path, operation, candidate_hash)
                VALUES (?, ?, ?, ?, ?)""", [(batch_id, index, entry["path"], entry["operation"], entry.get("candidate_hash")) for index, entry in enumerate(entries)])
            db.executemany("""INSERT INTO publication_jobs
                (batch_id, job_id, role, expected_content_hash, raw_path, raw_hash)
                VALUES (?, ?, ?, ?, ?, ?)""", [(batch_id, item["job_id"], item["role"], item.get("expected_content_hash"), item.get("raw_path"), item.get("raw_hash")) for item in jobs])

    def publication(self, batch_id: str) -> dict | None:
        row = self.connection.execute("SELECT * FROM publication_journals WHERE batch_id=?", (batch_id,)).fetchone()
        return dict(row) if row else None

    def publication_entries(self, batch_id: str) -> list[dict]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM publication_entries WHERE batch_id=? ORDER BY ordinal", (batch_id,))]

    def publication_jobs(self, batch_id: str) -> list[dict]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM publication_jobs WHERE batch_id=? ORDER BY job_id", (batch_id,))]

    def oldest_unresolved_publication(self) -> dict | None:
        row = self.connection.execute("""SELECT * FROM publication_journals
            WHERE phase IN ('candidate_validated', 'rollback_snapshot_recorded', 'publishing', 'published_uncommitted', 'committed_unfinalized', 'recovery_blocked')
            ORDER BY created_at, batch_id LIMIT 1""").fetchone()
        return dict(row) if row else None

    def list_publications(self) -> list[dict]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM publication_journals ORDER BY created_at DESC, batch_id DESC")]

    def update_publication(self, batch_id: str, phase: str, *, action: str | None = None, commit_id: str | None = None, failure_code: str | None = None, failure_message: str | None = None, blocked_from_phase: str | None = None) -> None:
        if phase not in PUBLICATION_PHASES:
            raise ValueError(f"unknown publication phase: {phase}")
        with self.transaction() as db:
            current = db.execute("SELECT phase FROM publication_journals WHERE batch_id=?", (batch_id,)).fetchone()
            if not current:
                raise KeyError(batch_id)
            db.execute("""UPDATE publication_journals
                SET phase=?, recovery_action=COALESCE(?, recovery_action), commit_id=COALESCE(?, commit_id),
                    failure_code=?, failure_message=?, blocked_from_phase=COALESCE(?, blocked_from_phase), updated_at=?
                WHERE batch_id=?""", (phase, action, commit_id, failure_code, failure_message, blocked_from_phase, utc_now(), batch_id))

    def record_snapshot(self, batch_id: str, workspace: str, manifest_hash: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE publication_journals SET snapshot_workspace=?, snapshot_manifest_hash=?, updated_at=? WHERE batch_id=?", (workspace, manifest_hash, utc_now(), batch_id))

    def finalize_publication(self, batch_id: str, commit_id: str) -> None:
        now = utc_now()
        with self.transaction() as db:
            journal = db.execute("SELECT * FROM publication_journals WHERE batch_id=?", (batch_id,)).fetchone()
            if not journal:
                raise KeyError(batch_id)
            for row in db.execute("SELECT * FROM publication_jobs WHERE batch_id=? AND role='source'", (batch_id,)):
                job = db.execute("SELECT * FROM jobs WHERE id=?", (row["job_id"],)).fetchone()
                if not job or job["batch_id"] != batch_id or job["status"] not in {"source_ready", "complete"} or job["content_hash"] != row["expected_content_hash"]:
                    raise ValueError(f"journaled source job {row['job_id']} no longer matches the publication")
                db.execute("UPDATE jobs SET status='complete', commit_id=?, updated_at=? WHERE id=?", (commit_id, now, row["job_id"]))
            db.execute("""UPDATE jobs SET queue_acknowledged=1, updated_at=?
                WHERE id IN (SELECT job_id FROM publication_jobs WHERE batch_id=? AND role='queue_ack')""", (now, batch_id))
            db.execute("UPDATE batches SET status='complete', commit_id=?, updated_at=? WHERE id=?", (commit_id, now, batch_id))
            db.execute("""UPDATE publication_journals SET phase='finalized', commit_id=?, recovery_action='finalized', failure_code=NULL, failure_message=NULL, updated_at=? WHERE batch_id=?""", (commit_id, now, batch_id))

    def mark_publication_cleanup_pending(self, batch_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE publication_journals SET phase='cleanup_pending', updated_at=? WHERE batch_id=? AND phase='finalized'", (utc_now(), batch_id))

    def mark_publication_complete(self, batch_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE publication_journals SET phase='complete', recovery_action='complete', updated_at=? WHERE batch_id=? AND phase IN ('finalized', 'cleanup_pending')", (utc_now(), batch_id))

    def recover(self) -> None:
        now = utc_now()
        with self.transaction() as db:
            db.execute("UPDATE jobs SET status='claimed', updated_at=? WHERE status='processing'", (now,))
            db.execute("UPDATE batches SET status='running', updated_at=? WHERE status='running'", (now,))

    def claim(self, kind: str, source_key: str, original_locator: str, *, input_artifact: str | None, batch_id: str, captured_at: str | None = None) -> Job:
        now = utc_now()
        with self.transaction() as db:
            row = db.execute("SELECT * FROM jobs WHERE source_key=?", (source_key,)).fetchone()
            if row:
                db.execute("UPDATE jobs SET batch_id=?, input_artifact=COALESCE(?, input_artifact), updated_at=? WHERE id=?", (batch_id, input_artifact, now, row["id"]))
                row = db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            else:
                job_id = uuid.uuid4().hex
                db.execute("""INSERT INTO jobs (id, kind, source_key, original_locator, input_artifact, batch_id, status, captured_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?, ?)""", (job_id, kind, source_key, original_locator, input_artifact, batch_id, captured_at or now, now, now))
                row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(row)

    def attach_artifact(self, job_id: str, path: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE jobs SET input_artifact=?, updated_at=? WHERE id=?", (path, utc_now(), job_id))

    def acknowledge(self, job_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE jobs SET queue_acknowledged=1, updated_at=? WHERE id=?", (utc_now(), job_id))

    def processing(self, job_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE jobs SET status='processing', attempts=attempts+1, updated_at=? WHERE id=?", (utc_now(), job_id))

    def complete(self, job_id: str, content_hash: str, source_version: int, *, cleanup_pending: bool = False) -> None:
        with self.transaction() as db:
            db.execute("UPDATE jobs SET status='source_ready', retryable=0, content_hash=?, source_version=?, cleanup_pending=?, updated_at=? WHERE id=?", (content_hash, source_version, int(cleanup_pending), utc_now(), job_id))

    def fail(self, job_id: str, code: str, message: str, *, retryable: bool = True) -> None:
        with self.transaction() as db:
            db.execute("UPDATE jobs SET status='failed', retryable=?, failure_code=?, failure_message=?, updated_at=? WHERE id=?", (int(retryable), code, message, utc_now(), job_id))

    def finalize(self, batch_id: str, commit_id: str) -> None:
        if not self.publication(batch_id):
            raise ValueError(f"batch {batch_id} has no publication journal")
        self.finalize_publication(batch_id, commit_id)

    def fail_batch(self, batch_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE batches SET status='failed', updated_at=? WHERE id=?", (utc_now(), batch_id))

    def mark_cleanup_done(self, job_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE jobs SET cleanup_pending=0, updated_at=? WHERE id=?", (utc_now(), job_id))

    def jobs_for_batch(self, batch_id: str) -> list[Job]:
        return [self._job(row) for row in self.connection.execute("SELECT * FROM jobs WHERE batch_id=? ORDER BY created_at, id", (batch_id,))]

    def get(self, job_id: str) -> Job | None:
        row = self.connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(row) if row else None

    def find(self, source_key: str) -> Job | None:
        row = self.connection.execute("SELECT * FROM jobs WHERE source_key=?", (source_key,)).fetchone()
        return self._job(row) if row else None

    def pending_cleanup(self) -> list[Job]:
        return [self._job(row) for row in self.connection.execute("SELECT * FROM jobs WHERE cleanup_pending=1")]

    def list_jobs(self) -> list[Job]:
        return [self._job(row) for row in self.connection.execute("SELECT * FROM jobs ORDER BY updated_at DESC, id")]

    def list_batches(self) -> list[dict[str, str | None]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM batches ORDER BY created_at DESC")]

    def retry(self, job_id: str, batch_id: str) -> Job:
        with self.transaction() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                raise KeyError(job_id)
            if not row["retryable"] and row["status"] != "failed":
                raise ValueError(f"job {job_id} is not retryable")
            db.execute("UPDATE jobs SET status='claimed', batch_id=?, failure_code=NULL, failure_message=NULL, updated_at=? WHERE id=?", (batch_id, utc_now(), job_id))
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(row)

    def _job(self, row: sqlite3.Row) -> Job:
        return Job(id=row["id"], kind=row["kind"], source_key=row["source_key"], original_locator=row["original_locator"], input_artifact=row["input_artifact"], batch_id=row["batch_id"], status=row["status"], attempts=row["attempts"], retryable=bool(row["retryable"]), failure_code=row["failure_code"], failure_message=row["failure_message"], content_hash=row["content_hash"], source_version=row["source_version"], captured_at=row["captured_at"], queue_acknowledged=bool(row["queue_acknowledged"]), cleanup_pending=bool(row["cleanup_pending"]), commit_id=row["commit_id"])
