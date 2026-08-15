from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal


SourceKind = Literal["article", "youtube"]


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
    queue_acknowledged: bool = False
    cleanup_pending: bool = False
    commit_id: str | None = None


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
