from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal


SourceKind = Literal["article", "youtube"]
PublicationOperation = Literal["write", "delete"]
PublicationJobRole = Literal["source", "queue_ack"]


@dataclass(frozen=True)
class PublicationIntent:
    """Durable work that publication may finish after source preparation."""

    queue_path: str | None = None
    queue_locator: str | None = None
    youtube_playlist_item_id: str | None = None
    raw_path: str | None = None
    raw_hash: str | None = None


@dataclass(frozen=True)
class ArticleEvidenceBounds:
    first_line: int
    last_line: int


@dataclass(frozen=True)
class YouTubeEvidenceBounds:
    first_second: float
    last_second: float


EvidenceBounds = ArticleEvidenceBounds | YouTubeEvidenceBounds


class PreparationFailureCategory(StrEnum):
    INVALID_INPUT = "invalid_input"
    EXTRACTION_FAILED = "extraction_failed"
    TRANSCRIPT_MISSING = "transcript_missing"
    VERSION_COLLISION = "version_collision"
    PATH_COLLISION = "path_collision"
    PERSISTENCE_FAILED = "persistence_failed"
    CANDIDATE_MISSING = "candidate_missing"
    CANDIDATE_CORRUPT = "candidate_corrupt"


class SourceCandidateLifecycle(StrEnum):
    PREPARED = "prepared"
    PAYLOAD_REMOVED = "payload_removed"


class PublicationPhase(StrEnum):
    CANDIDATE_VALIDATED = "candidate_validated"
    ROLLBACK_SNAPSHOT_RECORDED = "rollback_snapshot_recorded"
    PUBLISHING = "publishing"
    PUBLISHED_UNCOMMITTED = "published_uncommitted"
    COMMITTED_UNFINALIZED = "committed_unfinalized"
    FINALIZED = "finalized"
    CLEANUP_PENDING = "cleanup_pending"
    COMPLETE = "complete"
    ROLLED_BACK = "rolled_back"
    RECOVERY_BLOCKED = "recovery_blocked"


UNRESOLVED_PUBLICATION_PHASES = frozenset({
    PublicationPhase.CANDIDATE_VALIDATED,
    PublicationPhase.ROLLBACK_SNAPSHOT_RECORDED,
    PublicationPhase.PUBLISHING,
    PublicationPhase.PUBLISHED_UNCOMMITTED,
    PublicationPhase.COMMITTED_UNFINALIZED,
    PublicationPhase.RECOVERY_BLOCKED,
})
UNCOMMITTED_PUBLICATION_PHASES = frozenset({
    PublicationPhase.ROLLBACK_SNAPSHOT_RECORDED,
    PublicationPhase.PUBLISHING,
    PublicationPhase.PUBLISHED_UNCOMMITTED,
})
CLEANUP_PUBLICATION_PHASES = frozenset({PublicationPhase.FINALIZED, PublicationPhase.CLEANUP_PENDING})
COMMITTED_PUBLICATION_PHASES = frozenset({PublicationPhase.FINALIZED, PublicationPhase.CLEANUP_PENDING, PublicationPhase.COMPLETE})
PUBLICATION_TRANSITIONS = {
    PublicationPhase.CANDIDATE_VALIDATED: frozenset({PublicationPhase.ROLLBACK_SNAPSHOT_RECORDED, PublicationPhase.ROLLED_BACK, PublicationPhase.RECOVERY_BLOCKED}),
    PublicationPhase.ROLLBACK_SNAPSHOT_RECORDED: frozenset({PublicationPhase.PUBLISHING, PublicationPhase.ROLLED_BACK, PublicationPhase.RECOVERY_BLOCKED}),
    PublicationPhase.PUBLISHING: frozenset({PublicationPhase.PUBLISHED_UNCOMMITTED, PublicationPhase.COMMITTED_UNFINALIZED, PublicationPhase.ROLLED_BACK, PublicationPhase.RECOVERY_BLOCKED}),
    PublicationPhase.PUBLISHED_UNCOMMITTED: frozenset({PublicationPhase.COMMITTED_UNFINALIZED, PublicationPhase.ROLLED_BACK, PublicationPhase.RECOVERY_BLOCKED}),
    PublicationPhase.COMMITTED_UNFINALIZED: frozenset({PublicationPhase.FINALIZED, PublicationPhase.RECOVERY_BLOCKED}),
    PublicationPhase.FINALIZED: frozenset({PublicationPhase.CLEANUP_PENDING, PublicationPhase.COMPLETE}),
    PublicationPhase.CLEANUP_PENDING: frozenset({PublicationPhase.COMPLETE}),
    PublicationPhase.COMPLETE: frozenset(),
    PublicationPhase.ROLLED_BACK: frozenset(),
    PublicationPhase.RECOVERY_BLOCKED: frozenset({PublicationPhase.COMMITTED_UNFINALIZED, PublicationPhase.ROLLED_BACK}),
}


def publication_transition_allowed(current: PublicationPhase, target: PublicationPhase) -> bool:
    return current == target or target in PUBLICATION_TRANSITIONS[current]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class Job:
    id: str
    kind: SourceKind
    source_key: str
    original_locator: str
    input_artifact: str | None
    batch_id: str | None
    status: str
    attempts: int
    retryable: bool
    failure_code: str | None = None
    failure_message: str | None = None
    content_hash: str | None = None
    source_version: int | None = None
    captured_at: str | None = None
    queue_path: str | None = None
    queue_locator: str | None = None
    youtube_playlist_item_id: str | None = None
    queue_acknowledged: bool = False
    cleanup_pending: bool = False
    commit_id: str | None = None


@dataclass(frozen=True)
class PublicationJournal:
    batch_id: str
    phase: PublicationPhase
    candidate_workspace: str
    candidate_manifest_hash: str
    snapshot_workspace: str | None
    snapshot_manifest_hash: str | None
    base_commit: str
    commit_id: str | None
    blocked_from_phase: PublicationPhase | None
    recovery_action: str | None
    failure_code: str | None
    failure_message: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class PublicationEntry:
    relative_path: str
    operation: PublicationOperation
    candidate_hash: str | None


@dataclass(frozen=True)
class PublicationJobRecord:
    job_id: str
    role: PublicationJobRole
    expected_content_hash: str | None = None
    raw_path: str | None = None
    raw_hash: str | None = None


@dataclass(frozen=True)
class PublicationSnapshotEntry:
    relative_path: str
    existed: bool
    content_hash: str | None


@dataclass(frozen=True)
class ArticleInput:
    url: str
    html_path: Path | None = None
    input_method: str = "url"


@dataclass(frozen=True)
class TranscriptSegment:
    start_seconds: float
    end_seconds: float
    text: str


@dataclass(frozen=True)
class VideoInput:
    video_id: str
    title: str
    channel: str | None = None
    published_at: str | None = None
    manual_transcript: tuple[TranscriptSegment, ...] = ()
    automatic_transcript: tuple[TranscriptSegment, ...] = ()
    transcript_language: str | None = None
    playlist_item_id: str | None = None


@dataclass(frozen=True)
class AcquiredArticle:
    html: str
    input_method: str
    publication_intent: PublicationIntent = field(default_factory=PublicationIntent)


@dataclass(frozen=True)
class AcquiredYouTube:
    video: VideoInput
    publication_intent: PublicationIntent = field(default_factory=PublicationIntent)


@dataclass(frozen=True)
class SourceDocument:
    source_id: str
    kind: SourceKind
    canonical_url: str
    title: str
    content: str
    metadata: dict[str, Any]
    relative_path: str
    content_hash: str
    source_version: int


@dataclass(frozen=True)
class SourceCandidate:
    """A validated source plus the exact bytes persisted for later consumers."""

    job_id: str
    source: SourceDocument
    source_identity: str
    artifact_path: str
    manifest_path: str
    manifest_hash: str
    input_fingerprint: str
    rendered_markdown: bytes
    rendered_hash: str
    evidence_bounds: EvidenceBounds
    publication_intent: PublicationIntent

    @property
    def source_id(self) -> str:
        return self.source.source_id

    @property
    def kind(self) -> SourceKind:
        return self.source.kind

    @property
    def canonical_url(self) -> str:
        return self.source.canonical_url

    @property
    def title(self) -> str:
        return self.source.title

    @property
    def content(self) -> str:
        return self.source.content

    @property
    def metadata(self) -> dict[str, Any]:
        return self.source.metadata

    @property
    def relative_path(self) -> str:
        return self.source.relative_path

    @property
    def content_hash(self) -> str:
        return self.source.content_hash

    @property
    def source_version(self) -> int:
        return self.source.source_version


@dataclass(frozen=True)
class PreparationFailure:
    job_id: str
    category: PreparationFailureCategory
    safe_message: str
    retryable: bool = True

    @property
    def message(self) -> str:
        return self.safe_message


@dataclass(frozen=True)
class SourceCandidateRecord:
    job_id: str
    source_identity: str
    source_version: int
    relative_path: str
    artifact_path: str
    manifest_path: str
    manifest_hash: str
    rendered_hash: str
    lifecycle: SourceCandidateLifecycle
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class CandidateFile:
    relative_path: str
    content: str


@dataclass(frozen=True)
class ChangeSet:
    files: tuple[CandidateFile, ...] = ()
    deletions: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BatchReport:
    batch_id: str
    claimed: int
    completed: int
    failed: int
    committed: bool
    commit_id: str | None
    failures: tuple[str, ...] = ()
    publication_phase: PublicationPhase | None = None
    recovery_action: str | None = None
    recovery_block_reason: str | None = None
    publication_failure_code: str | None = None
    publication_failure_message: str | None = None
    outstanding_cleanup: int = 0


@dataclass(frozen=True)
class RetryReport(BatchReport):
    selected_job_ids: tuple[str, ...] = ()
