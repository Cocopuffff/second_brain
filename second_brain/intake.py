from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Callable, Literal, Protocol, TypeAlias

from .canonical import source_key, youtube_video_id
from .config import Config
from .html_discovery import discover_html, html_hash
from .models import AcquiredArticle, AcquiredYouTube, Job, PublicationIntent, SourceKind
from .queue import read_article_queue
from .state import StateStore
from .youtube import YouTubeClient, YouTubePlaylistKind, YouTubePlaylistReference


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

    @property
    def input_artifact(self) -> str | None:
        return str(self.html_path) if self.html_path else None


@dataclass(frozen=True)
class YouTubeWorkDescriptor:
    source_key: str
    original_locator: str
    publication_intent: PublicationIntent
    video_id: str
    kind: Literal["youtube"] = field(default="youtube", init=False)

    @property
    def input_artifact(self) -> None:
        return None


WorkDescriptor = ArticleWorkDescriptor | YouTubeWorkDescriptor


class IntakeAdapter(Protocol):
    @property
    def kind(self) -> SourceKind: ...

    def discover(self) -> tuple[list[WorkDescriptor], list[DiscoveryFailure]]: ...

    def acquire(self, descriptor: WorkDescriptor, job: Job) -> AcquiredPayload: ...

    def retry_descriptor(self, job: Job) -> WorkDescriptor: ...


class ArticleIntakeAdapter:
    kind: Literal["article"] = "article"

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

    def retry_descriptor(self, job: Job) -> ArticleWorkDescriptor:
        if job.kind != self.kind:
            raise TypeError("article intake received a non-article job")
        html_path = self._retry_html_path(job)
        intent = PublicationIntent(
            queue_path=job.queue_path,
            queue_locator=job.queue_locator,
            raw_path=job.input_artifact,
            raw_hash=html_hash(html_path) if html_path else None,
        )
        return ArticleWorkDescriptor(job.source_key, job.original_locator, intent, html_path=html_path)

    def _retry_html_path(self, job: Job) -> Path | None:
        if not job.input_artifact:
            return None
        path = Path(job.input_artifact)
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(self.config.to_ingest.resolve())
        except (FileNotFoundError, OSError, ValueError) as exc:
            raise ValueError("saved HTML artifact is unavailable or outside ToIngest") from exc
        if not path.is_file():
            raise ValueError("saved HTML artifact is unavailable or outside ToIngest")
        return path


class YouTubeIntakeAdapter:
    kind: Literal["youtube"] = "youtube"

    def __init__(self, playlist: YouTubePlaylistReference, client: YouTubeClient):
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

    def retry_descriptor(self, job: Job) -> YouTubeWorkDescriptor:
        if job.kind != self.kind:
            raise TypeError("YouTube intake received a non-YouTube job")
        video_id = youtube_video_id(job.original_locator)
        if not video_id:
            raise ValueError(f"job {job.id} has an invalid YouTube locator")
        intent = PublicationIntent(
            queue_path=job.queue_path,
            queue_locator=job.queue_locator,
            youtube_playlist_item_id=job.youtube_playlist_item_id,
        )
        return YouTubeWorkDescriptor(job.source_key, job.original_locator, intent, video_id)


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
                    input_artifact=descriptor.input_artifact,
                    batch_id=batch_id,
                    publication_intent=descriptor.publication_intent,
                )
                if not owned or job.status != "claimed" or job.batch_id != batch_id:
                    continue
                claimed_count += 1
                result = self._acquire(adapter, descriptor, job)
                if isinstance(result, IntakeSuccess):
                    successes.append(result)
                else:
                    failures.append(result)
        return IntakeBatch(
            tuple(successes),
            tuple(failures),
            claimed_count,
            self.queue_path,
            frozenset(self.allowed_input_paths()),
        )

    def collect_retries(self, batch_id: str, jobs: list[Job]) -> IntakeBatch:
        successes: list[IntakeSuccess] = []
        failures: list[IntakeFailure] = []
        for job in jobs:
            if job.status != "claimed" or job.batch_id != batch_id:
                continue
            try:
                descriptor, adapter = self._retry_work(job)
            except Exception as exc:
                failures.append(self._acquisition_failure(job, exc))
                continue
            result = self._acquire(adapter, descriptor, job)
            if isinstance(result, IntakeSuccess):
                successes.append(result)
            else:
                failures.append(result)
        return IntakeBatch(
            tuple(successes),
            tuple(failures),
            len(jobs),
            self.queue_path,
            frozenset(self.allowed_retry_paths(jobs)),
        )

    def _retry_work(self, job: Job) -> tuple[WorkDescriptor, IntakeAdapter]:
        adapter = next((item for item in self.adapters if item.kind == job.kind), None)
        if adapter is None:
            raise ValueError(f"retry requires a configured {job.kind} intake client")
        return adapter.retry_descriptor(job), adapter

    def _acquire(self, adapter: IntakeAdapter, descriptor: WorkDescriptor, job: Job) -> IntakeSuccess | SourceIntakeFailure:
        try:
            return IntakeSuccess(job, adapter.acquire(descriptor, job))
        except Exception as exc:
            return self._acquisition_failure(
                job,
                exc,
                source=IntakeSource(
                    descriptor.kind,
                    descriptor.source_key,
                    descriptor.original_locator,
                    descriptor.input_artifact,
                ),
            )

    def _acquisition_failure(self, job: Job, exc: Exception, *, source: IntakeSource | None = None) -> SourceIntakeFailure:
        self.state.fail(job.id, IntakeFailureCategory.ACQUISITION_FAILED.value, str(exc))
        return SourceIntakeFailure(
            category=IntakeFailureCategory.ACQUISITION_FAILED,
            safe_message=str(exc),
            source=source or IntakeSource(job.kind, job.source_key, job.original_locator, job.input_artifact),
            job_id=job.id,
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

    def allowed_retry_paths(self, jobs: list[Job]) -> set[str]:
        allowed = {str(self.queue_path.relative_to(self.config.vault))}
        for job in jobs:
            for value in (job.queue_path, job.input_artifact):
                if not value:
                    continue
                allowed.add(self._retry_relative_path(value))
        return allowed

    def _retry_relative_path(self, value: str) -> str:
        path = Path(value)
        if not path.is_absolute():
            path = self.config.vault / path
        try:
            return str(path.resolve(strict=False).relative_to(self.config.vault.resolve()))
        except (OSError, ValueError) as exc:
            raise ValueError("retry input path is outside the vault") from exc


def build_source_intake(
    config: Config,
    state: StateStore,
    *,
    fetcher: Callable[[str], str],
    youtube_client: YouTubeClient | None = None,
) -> SourceIntake:
    adapters: list[IntakeAdapter] = [ArticleIntakeAdapter(config, fetcher)]
    if youtube_client is not None:
        if config.youtube_enabled:
            if not config.youtube_playlist_id:
                raise ValueError("youtube_playlist_id is required when YouTube is enabled")
            playlist = YouTubePlaylistReference(
                YouTubePlaylistKind.PRODUCTION,
                config.youtube_playlist_id,
            )
        else:
            playlist = YouTubePlaylistReference(
                YouTubePlaylistKind.FIXTURE,
                config.youtube_playlist,
            )
        adapters.append(YouTubeIntakeAdapter(playlist, youtube_client))
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
    "YouTubePlaylistKind",
    "YouTubePlaylistReference",
    "YouTubeWorkDescriptor",
    "build_source_intake",
    "queue_path_for",
]
