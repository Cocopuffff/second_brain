from __future__ import annotations

import fcntl
import os
import shutil
import tempfile
import urllib.request
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

from .canonical import source_key, stable_id
from .config import Config
from .extraction import ImageProcessor, extract_article
from .git_ops import GitError, GitRepository
from .html_discovery import discover_html, html_hash
from .models import BatchReport, ChangeSet, SourceDocument, utc_now
from .queue import claim_article_queue
from .render import render_markdown, render_source
from .state import StateStore
from .synthesis import NoopSynthesizer, Synthesizer
from .validation import ValidationError, validate_changes, validate_markdown
from .youtube import YouTubeClient, render_video_source


class BatchError(RuntimeError):
    pass


@contextmanager
def single_process_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise BatchError("another ingestion batch is already running") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class BatchRunner:
    def __init__(self, config: Config, *, synthesizer: Synthesizer | None = None, image_processor: ImageProcessor | None = None, youtube_client: YouTubeClient | None = None, fetcher=None):
        self.config = config
        self.synthesizer = synthesizer or NoopSynthesizer()
        self.image_processor = image_processor
        self.youtube_client = youtube_client
        self.fetcher = fetcher or self._fetch

    def initialize(self) -> None:
        self.config.to_ingest.mkdir(parents=True, exist_ok=True)
        self.config.sources.joinpath("Articles").mkdir(parents=True, exist_ok=True)
        self.config.sources.joinpath("YouTube").mkdir(parents=True, exist_ok=True)
        self.config.concepts.mkdir(parents=True, exist_ok=True)
        queue = self.config.to_ingest / "To Ingest.md"
        root_queue = self.config.vault / "To Ingest.md"
        if not queue.exists() and not root_queue.exists():
            queue.write_text("", encoding="utf-8")
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        StateStore(self.config.database).close()

    def dry_run(self) -> dict:
        queue = self._queue_path()
        inputs, queue_errors = self._read_queue_preview(queue)
        html_files, html_errors = discover_html(self.config.to_ingest)
        return {"queue_urls": len(inputs), "html_files": len(html_files), "errors": queue_errors + html_errors}

    def run(self, retry_job_id: str | None = None) -> BatchReport:
        errors = self.config.validate()
        if errors:
            raise BatchError("preflight failed: " + "; ".join(errors))
        self.initialize()
        state = StateStore(self.config.database)
        try:
            with single_process_lock(self.config.lockfile):
                try:
                    return self._run_locked(state, retry_job_id)
                except GitError as exc:
                    raise BatchError(str(exc)) from exc
        finally:
            state.close()

    def _run_locked(self, state: StateStore, retry_job_id: str | None = None) -> BatchReport:
        # This is the pre-consumption cleanliness checkpoint. Queue acknowledgement
        # and source publication below are this batch's own explicit changes.
        GitRepository(self.config.vault).ensure_clean(allowed_paths={str(self._queue_path().relative_to(self.config.vault))}, allowed_prefixes=("ToIngest/",))
        state.recover()
        batch_id = state.create_batch()
        claimed_count = 0
        failures: list[str] = []
        sources: list[SourceDocument] = []
        source_jobs: list[tuple[object, SourceDocument]] = []
        html_by_key: dict[str, list[Path]] = defaultdict(list)
        if retry_job_id:
            try:
                state.retry(retry_job_id, batch_id)
                claimed_count += 1
            except (KeyError, ValueError) as exc:
                state.fail_batch(batch_id)
                raise BatchError(str(exc)) from exc
        html_files, html_errors = discover_html(self.config.to_ingest)
        failures.extend(html_errors)
        for path, url, _title in html_files:
            html_by_key[source_key("article", url)].append(path)

        queue_inputs, queue_errors = claim_article_queue(self._queue_path(), state, batch_id)
        failures.extend(queue_errors)
        for item in queue_inputs:
            key = source_key("article", item.url)
            paths = html_by_key.get(key, [])
            if paths:
                hashes = {html_hash(path) for path in paths}
                if len(hashes) > 1:
                    job = state.find(key)
                    if job:
                        state.fail(job.id, "conflicting_html", "multiple non-identical HTML files claim the same canonical URL")
                    failures.append(f"{item.url}: conflicting HTML payloads")
                else:
                    state.attach_artifact(state.find(key).id, str(paths[0]))  # type: ignore[union-attr]
            state.acknowledge(state.find(key).id)  # type: ignore[union-attr]
        claimed_count += len(queue_inputs)

        for key, paths in html_by_key.items():
            if len({html_hash(path) for path in paths}) > 1:
                if not state.find(key):
                    job = state.claim("article", key, key.split(":", 1)[1], input_artifact=None, batch_id=batch_id)
                    state.fail(job.id, "conflicting_html", "multiple non-identical HTML files claim the same canonical URL")
                continue
            job = state.find(key)
            if job is None:
                job = state.claim("article", key, key.split(":", 1)[1], input_artifact=str(paths[0]), batch_id=batch_id)
                claimed_count += 1
            else:
                state.attach_artifact(job.id, str(paths[0]))

        if self.youtube_client:
            for video in self.youtube_client.list_playlist(self.config.youtube_playlist):
                url = f"https://www.youtube.com/watch?v={video.video_id}"
                key = source_key("youtube", url)
                job = state.claim("youtube", key, url, input_artifact=video.playlist_item_id, batch_id=batch_id)
                claimed_count += 1 if job.status != "complete" else 0
                state.acknowledge(job.id)
                try:
                    self.youtube_client.acknowledge(video)
                except Exception as exc:
                    failures.append(f"{video.video_id}: playlist acknowledgement failed: {exc}")

        for job in state.jobs_for_batch(batch_id):
            if job.status == "complete":
                continue
            state.processing(job.id)
            try:
                if job.kind == "article":
                    source = self._article_source(state.get(job.id))
                else:
                    video = next((item for item in (self.youtube_client.list_playlist(self.config.youtube_playlist) if self.youtube_client else []) if source_key("youtube", f"https://www.youtube.com/watch?v={item.video_id}") == job.source_key), None)
                    if video is None:
                        raise BatchError("YouTube video is not available from the configured client")
                    source = render_video_source(video, job.captured_at or utc_now(), job.source_version or 1)
                state.complete(job.id, source.content_hash, source.source_version, cleanup_pending=bool(job.input_artifact and job.kind == "article"))
                source_jobs.append((job, source))
                sources.append(source)
            except Exception as exc:
                code = "transcript_missing" if "transcript" in str(exc).lower() else "processing_failed"
                state.fail(job.id, code, str(exc))
                failures.append(f"{job.original_locator}: {exc}")

        if not sources:
            state.fail_batch(batch_id)
            return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))

        concept_catalog = self._concept_catalog()
        staging = self.config.state_dir / "staging" / batch_id
        try:
            changes = self.synthesizer.synthesize(sources, concept_catalog, staging)
            validate_changes(changes, sources, self.config.vault)
            source_files = {source.relative_path: render_markdown(source) for source in sources}
            for relative_path, content in source_files.items():
                validate_markdown(content)
            all_files = {**source_files, **{candidate.relative_path: candidate.content for candidate in changes.files}}
            if not all_files and not changes.deletions:
                state.fail_batch(batch_id)
                return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))
            GitRepository(self.config.vault).ensure_clean(allowed_paths={str(self._queue_path().relative_to(self.config.vault))}, allowed_prefixes=("ToIngest/",))
            self._publish_candidates(all_files, changes.deletions)
            git = GitRepository(self.config.vault)
            batch_paths = set(all_files) | set(changes.deletions)
            if queue_inputs:
                batch_paths.add(str(self._queue_path().relative_to(self.config.vault)))
            paths = sorted(batch_paths)
            commit_id = git.commit_paths(paths, f"ingest: batch {batch_id} ({len(sources)} complete, {len(failures)} failed)")
            state.finalize(batch_id, commit_id)
            self._cleanup_sources(state, source_jobs)
            return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=len(sources), failed=len(failures), committed=True, commit_id=commit_id, failures=tuple(failures))
        except Exception as exc:
            state.fail_batch(batch_id)
            if not isinstance(exc, ValidationError):
                failures.append(str(exc))
            return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))

    def _article_source(self, job) -> SourceDocument:
        if job is None:
            raise BatchError("job disappeared during processing")
        input_method = "saved-html"
        if job.input_artifact:
            html_text = Path(job.input_artifact).read_text(encoding="utf-8")
        else:
            input_method = "http"
            html_text = self.fetcher(job.original_locator)
        extracted = extract_article(html_text, self.image_processor)
        source_id = stable_id("article", job.source_key)
        return render_source(source_id=source_id, kind="article", canonical_url=job.original_locator, title=extracted["title"], body=extracted["body"], author=None, publication_date=None, captured_at=job.captured_at or utc_now(), input_method=input_method, source_version=job.source_version or 1)

    def _publish_candidates(self, files: dict[str, str], deletions: tuple[str, ...]) -> None:
        for relative_path, content in files.items():
            target = (self.config.vault / relative_path).resolve()
            target.relative_to(self.config.vault.resolve())
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.candidate")
            temporary.write_text(content, encoding="utf-8", newline="\n")
            os.replace(temporary, target)
        for relative_path in deletions:
            target = self.config.vault / relative_path
            if target.exists():
                target.unlink()

    def _cleanup_sources(self, state: StateStore, source_jobs: list[tuple[object, SourceDocument]]) -> None:
        for job, _source in source_jobs:
            if job.input_artifact:
                path = Path(job.input_artifact)
                try:
                    path.resolve().relative_to(self.config.to_ingest.resolve())
                except ValueError:
                    continue
                if path.exists():
                    path.unlink()
                state.mark_cleanup_done(job.id)

    def _concept_catalog(self) -> dict[str, str]:
        result: dict[str, str] = {}
        if not self.config.concepts.exists():
            return result
        for path in sorted(self.config.concepts.rglob("*.md")):
            result[str(path.relative_to(self.config.vault))] = path.read_text(encoding="utf-8")[:12000]
        return result

    def _queue_path(self) -> Path:
        nested = self.config.to_ingest / "To Ingest.md"
        return nested if nested.exists() else self.config.vault / "To Ingest.md"

    @staticmethod
    def _fetch(url: str) -> str:
        req = urllib.request.Request(url, headers={"User-Agent": "second-brain-ingestion/0.1 (+local batch)", "Accept": "text/html,application/xhtml+xml"})
        with urllib.request.urlopen(req, timeout=30) as response:
            if int(response.headers.get("Content-Length", "0") or "0") > 10_000_000:
                raise BatchError("HTTP response exceeds the configured size limit")
            content_type = response.headers.get_content_type()
            if content_type not in {"text/html", "application/xhtml+xml"}:
                raise BatchError(f"unsupported HTTP content type: {content_type}")
            return response.read(10_000_001).decode("utf-8", errors="strict")

    @staticmethod
    def _read_queue_preview(path: Path):
        from .queue import read_article_queue
        return read_article_queue(path)
