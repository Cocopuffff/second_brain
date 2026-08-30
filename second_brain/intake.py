from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Callable, Literal, Protocol, TypeAlias

from .canonical import source_key
from .config import Config
from .html_discovery import discover_html, html_hash
from .models import AcquiredArticle, AcquiredYouTube, Job, PublicationIntent, SourceKind
from .queue import read_article_queue
from .state import StateStore
from .youtube import YouTubeClient


AcquiredPayload = AcquiredArticle | AcquiredYouTube


class IntakeFailureCategory(StrEnum):
    DISCOVERY_FAILED = "discovery_failed"
    CONFLICTING_PAYLOAD = "conflicting_payload"
    ACQUISITION_FAILED = "acquisition_failed"
    INVALID_ACKNOWLEDGEMENT_INTENT = "invalid_acknowledgement_intent"


@dataclass(frozen=True)
class GlobalIntakeFailure:
    category: IntakeFailureCategory
    safe_message: str
    retryable: bool = True


@dataclass(frozen=True)
class IntakeSource:
    kind: SourceKind
    source_key: str
    original_locator: str
    input_artifact: str | None = None


@dataclass(frozen=True)
class SourceDiscoveryFailure:
    category: IntakeFailureCategory
    safe_message: str
    source: IntakeSource
    retryable: bool = True


@dataclass(frozen=True)
class SourceIntakeFailure:
    category: IntakeFailureCategory
    safe_message: str
    source: IntakeSource
    job_id: str
    retryable: bool = True


IntakeFailure: TypeAlias = GlobalIntakeFailure | SourceIntakeFailure
DiscoveryFailure: TypeAlias = GlobalIntakeFailure | SourceDiscoveryFailure


@dataclass(frozen=True)
class IntakeSuccess:
    job: Job
    payload: AcquiredPayload


@dataclass(frozen=True)
class IntakeBatch:
    successes: tuple[IntakeSuccess, ...]
    failures: tuple[IntakeFailure, ...]
    claimed_count: int
    queue_path: Path
    allowed_paths: frozenset[str]


@dataclass(frozen=True)
class ArticleWorkDescriptor:
    source_key: str
    original_locator: str
    publication_intent: PublicationIntent
    html_path: Path | None = None
    kind: Literal["article"] = field(default="article", init=False)


@dataclass(frozen=True)
class YouTubeWorkDescriptor:
    source_key: str
    original_locator: str
    publication_intent: PublicationIntent
    video_id: str
    kind: Literal["youtube"] = field(default="youtube", init=False)


WorkDescriptor = ArticleWorkDescriptor | YouTubeWorkDescriptor


class IntakeAdapter(Protocol):
    def discover(self) -> tuple[list[WorkDescriptor], list[DiscoveryFailure]]: ...

    def acquire(self, descriptor: WorkDescriptor, job: Job) -> AcquiredPayload: ...


class ArticleIntakeAdapter:
    def __init__(self, config: Config, fetcher: Callable[[str], str]):
        self.config = config
        self.fetcher = fetcher
        self.queue_path = queue_path_for(config)

    def discover(self) -> tuple[list[WorkDescriptor], list[DiscoveryFailure]]:
        queue_inputs, queue_errors = read_article_queue(self.queue_path)
        html_files, html_errors = discover_html(self.config.to_ingest)
        html_by_key: dict[str, list[Path]] = defaultdict(list)
        for path, url, _title in html_files:
            html_by_key[source_key("article", url)].append(path)

        descriptors: dict[str, ArticleWorkDescriptor] = {}
        queue_relative = str(self.queue_path.relative_to(self.config.vault))
        for item in queue_inputs:
            key = source_key("article", item.url)
            descriptors.setdefault(
                key,
                ArticleWorkDescriptor(
                    key,
                    item.url,
                    PublicationIntent(queue_path=queue_relative, queue_locator=item.url),
                ),
            )

        failures: list[DiscoveryFailure] = [
            GlobalIntakeFailure(IntakeFailureCategory.DISCOVERY_FAILED, message)
            for message in (*queue_errors, *html_errors)
        ]
        for key, paths in html_by_key.items():
            locator = key.split(":", 1)[1]
            hashes = {html_hash(path) for path in paths}
            if len(hashes) > 1:
                failures.append(
                    SourceDiscoveryFailure(
                        IntakeFailureCategory.CONFLICTING_PAYLOAD,
                        "multiple non-identical HTML files claim the same canonical URL",
                        IntakeSource("article", key, locator, str(paths[0])),
                    )
                )
                descriptors.pop(key, None)
                continue
            existing = descriptors.get(key)
            descriptors[key] = ArticleWorkDescriptor(
                key,
                locator,
                PublicationIntent(
                    queue_path=existing.publication_intent.queue_path if existing else None,
                    queue_locator=existing.publication_intent.queue_locator if existing else None,
                    raw_path=str(paths[0]),
                    raw_hash=next(iter(hashes)),
                ),
                html_path=paths[0],
            )
        return list(descriptors.values()), failures

    def acquire(self, descriptor: WorkDescriptor, job: Job) -> AcquiredArticle:
        if not isinstance(descriptor, ArticleWorkDescriptor):
            raise TypeError("article intake received a non-article descriptor")
        if descriptor.html_path:
            path = descriptor.html_path
            html = path.read_text(encoding="utf-8")
            input_method = "saved-html"
        else:
            html = self.fetcher(descriptor.original_locator)
            input_method = "http"
        return AcquiredArticle(html, input_method, descriptor.publication_intent)


class YouTubeIntakeAdapter:
    def __init__(self, playlist: str, client: YouTubeClient):
        self.playlist = playlist
        self.client = client

    def discover(self) -> tuple[list[WorkDescriptor], list[DiscoveryFailure]]:
        descriptors: dict[str, YouTubeWorkDescriptor] = {}
        failures: list[DiscoveryFailure] = []
        for item in self.client.list_playlist(self.playlist):
            locator = f"https://www.youtube.com/watch?v={item.video_id}"
            key = source_key("youtube", locator)
            if not item.playlist_item_id:
                failures.append(
                    SourceDiscoveryFailure(
                        IntakeFailureCategory.INVALID_ACKNOWLEDGEMENT_INTENT,
                        "YouTube playlist item is missing its acknowledgement identifier",
                        IntakeSource("youtube", key, locator),
                        retryable=False,
                    )
                )
                continue
            descriptors.setdefault(
                key,
                YouTubeWorkDescriptor(
                    key,
                    locator,
                    PublicationIntent(youtube_playlist_item_id=item.playlist_item_id),
                    item.video_id,
                ),
            )
        return list(descriptors.values()), failures

    def acquire(self, descriptor: WorkDescriptor, job: Job) -> AcquiredYouTube:
        if not isinstance(descriptor, YouTubeWorkDescriptor):
            raise TypeError("YouTube intake received a non-YouTube descriptor")
        video = self.client.acquire_video(descriptor.video_id)
        return AcquiredYouTube(video, descriptor.publication_intent)


class SourceIntake:
    def __init__(self, config: Config, state: StateStore, adapters: tuple[IntakeAdapter, ...]):
        self.config = config
        self.state = state
        self.adapters = adapters
        self.queue_path = queue_path_for(config)

    def collect(self, batch_id: str) -> IntakeBatch:
        successes: list[IntakeSuccess] = []
        failures: list[IntakeFailure] = []
        claimed_count = 0
        for adapter in self.adapters:
            try:
                descriptors, discovery_failures = adapter.discover()
            except Exception as exc:
                failures.append(GlobalIntakeFailure(IntakeFailureCategory.DISCOVERY_FAILED, str(exc)))
                continue
            for failure in discovery_failures:
                if isinstance(failure, GlobalIntakeFailure):
                    failures.append(failure)
                    continue
                source = failure.source
                job, owned = self.state.claim_exclusive(
                    source.kind,
                    source.source_key,
                    source.original_locator,
                    input_artifact=source.input_artifact,
                    batch_id=batch_id,
                )
                if owned:
                    claimed_count += 1
                    self.state.fail(job.id, failure.category.value, failure.safe_message, retryable=failure.retryable)
                failures.append(
                    SourceIntakeFailure(
                        category=failure.category,
                        safe_message=failure.safe_message,
                        source=source,
                        job_id=job.id,
                        retryable=failure.retryable,
                    )
                )
            for descriptor in descriptors:
                job, owned = self.state.claim_exclusive(
                    descriptor.kind,
                    descriptor.source_key,
                    descriptor.original_locator,
                    input_artifact=str(descriptor.html_path) if isinstance(descriptor, ArticleWorkDescriptor) and descriptor.html_path else None,
                    batch_id=batch_id,
                    publication_intent=descriptor.publication_intent,
                )
                if not owned or job.status != "claimed" or job.batch_id != batch_id:
                    continue
                claimed_count += 1
                try:
                    successes.append(IntakeSuccess(job, adapter.acquire(descriptor, job)))
                except Exception as exc:
                    self.state.fail(job.id, IntakeFailureCategory.ACQUISITION_FAILED.value, str(exc))
                    failures.append(
                        SourceIntakeFailure(
                            category=IntakeFailureCategory.ACQUISITION_FAILED,
                            safe_message=str(exc),
                            source=IntakeSource(
                                descriptor.kind,
                                descriptor.source_key,
                                descriptor.original_locator,
                                str(descriptor.html_path)
                                if isinstance(descriptor, ArticleWorkDescriptor) and descriptor.html_path
                                else None,
                            ),
                            job_id=job.id,
                        )
                    )
        return IntakeBatch(
            tuple(successes),
            tuple(failures),
            claimed_count,
            self.queue_path,
            frozenset(self.allowed_input_paths()),
        )

    def allowed_input_paths(self) -> set[str]:
        allowed = {str(self.queue_path.relative_to(self.config.vault))}
        pairing = self.config.to_ingest / "HTML Pairings.yaml"
        if pairing.exists():
            allowed.add(str(pairing.relative_to(self.config.vault)))
        if self.config.to_ingest.exists():
            allowed.update(
                str(path.relative_to(self.config.vault))
                for path in self.config.to_ingest.iterdir()
                if path.is_file() and path.suffix.lower() in {".html", ".htm"}
            )
        return allowed


def build_source_intake(
    config: Config,
    state: StateStore,
    *,
    fetcher: Callable[[str], str],
    youtube_client: YouTubeClient | None = None,
) -> SourceIntake:
    adapters: list[IntakeAdapter] = [ArticleIntakeAdapter(config, fetcher)]
    if youtube_client is not None:
        adapters.append(YouTubeIntakeAdapter(config.youtube_playlist, youtube_client))
    return SourceIntake(config, state, tuple(adapters))


def queue_path_for(config: Config) -> Path:
    nested = config.to_ingest / "To Ingest.md"
    return nested if nested.exists() else config.vault / "To Ingest.md"


__all__ = [
    "ArticleIntakeAdapter",
    "ArticleWorkDescriptor",
    "DiscoveryFailure",
    "GlobalIntakeFailure",
    "IntakeAdapter",
    "IntakeBatch",
    "IntakeFailure",
    "IntakeFailureCategory",
    "IntakeSource",
    "IntakeSuccess",
    "SourceIntakeFailure",
    "SourceDiscoveryFailure",
    "SourceIntake",
    "WorkDescriptor",
    "YouTubeIntakeAdapter",
    "YouTubeWorkDescriptor",
    "build_source_intake",
    "queue_path_for",
]
