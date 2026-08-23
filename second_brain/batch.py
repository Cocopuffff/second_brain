from __future__ import annotations

import fcntl
import json
import urllib.request
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .canonical import source_key, stable_id
from .config import Config
from .extraction import ImageProcessor, extract_article
from .git_ops import GitError, GitRepository
from .html_discovery import discover_html, html_hash
from .models import BatchReport, COMMITTED_PUBLICATION_PHASES, ChangeSet, PublicationPhase, SourceDocument, utc_now
from .publication import BatchPublication, PublicationCrash, PublicationError, PublicationFaults, PublicationResult
from .queue import claim_article_queue, remove_claimed_urls_text
from .render import render_markdown, render_source
from .state import StateStore
from .synthesis import NoopSynthesizer
from .validation import ValidationError, validate_changes, validate_markdown
from .youtube import YouTubeClient, render_video_source
from .controlled_synthesis import ControlledSynthesis, SynthesisFailure, SynthesisOutcome


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
    def __init__(self, config: Config, *, synthesizer: Any | None = None, image_processor: ImageProcessor | None = None, youtube_client: YouTubeClient | None = None, fetcher=None, _publication_faults: PublicationFaults | None = None):
        self.config = config
        self.synthesizer = synthesizer or NoopSynthesizer()
        self.image_processor = image_processor
        self.youtube_client = youtube_client
        self.fetcher = fetcher or self._fetch
        self._publication_faults = _publication_faults

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
        git = GitRepository(self.config.vault)
        publication = BatchPublication(self.config, state, git, faults=self._publication_faults)
        recovery = publication.recover_oldest()
        if recovery:
            return self._publication_report(recovery, state)
        cleanup_recovery = publication.retry_cleanup()
        if cleanup_recovery:
            return self._publication_report(cleanup_recovery, state)

        # Queue and raw HTML are accepted inputs; every other existing staged,
        # modified, or untracked path is an operator-owned stop condition.
        git.ensure_clean(allowed_paths=self._allowed_input_paths())
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

        queue_path = self._queue_path()
        queue_inputs, queue_errors = claim_article_queue(queue_path, state, batch_id, acknowledge=False)
        queue_job_ids: list[str] = []
        failures.extend(queue_errors)
        for item in queue_inputs:
            key = source_key("article", item.url)
            job = state.find(key)
            if job:
                queue_job_ids.append(job.id)
            paths = html_by_key.get(key, [])
            if paths:
                hashes = {html_hash(path) for path in paths}
                if len(hashes) > 1:
                    if job:
                        state.fail(job.id, "conflicting_html", "multiple non-identical HTML files claim the same canonical URL")
                    failures.append(f"{item.url}: conflicting HTML payloads")
                else:
                    state.attach_artifact(job.id, str(paths[0]))  # type: ignore[union-attr]
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

        raw_fingerprints: dict[str, tuple[str, str | None]] = {}
        for job in state.jobs_for_batch(batch_id):
            if not job.input_artifact:
                continue
            artifact = Path(job.input_artifact)
            raw_fingerprints[job.id] = (str(artifact), html_hash(artifact) if artifact.exists() and artifact.is_file() else None)

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
                source_jobs.append((state.get(job.id), source))  # type: ignore[arg-type]
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
            changes, synthesis_metadata, synthesis_failure = self._synthesize(sources, concept_catalog, staging)
            if synthesis_failure:
                failures.append(f"{synthesis_failure.category}: {synthesis_failure.message}")
                state.fail_batch(batch_id)
                return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))
            source_files = {source.relative_path: render_markdown(source) for source in sources}
            for relative_path, content in source_files.items():
                validate_markdown(content)
            all_files = {**source_files, **{candidate.relative_path: candidate.content for candidate in changes.files}}
            if queue_inputs and queue_path.exists():
                latest_queue = queue_path.read_text(encoding="utf-8")
                claimed_urls = {item.url for item in queue_inputs}
                all_files[str(queue_path.relative_to(self.config.vault))] = remove_claimed_urls_text(latest_queue, claimed_urls)
            if not all_files and not changes.deletions:
                state.fail_batch(batch_id)
                return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))
            git.ensure_clean(allowed_paths=self._allowed_input_paths())
            result = publication.publish(batch_id, all_files, changes.deletions, queue_path=str(queue_path.relative_to(self.config.vault)), queue_job_ids=queue_job_ids, source_jobs=[job for job, _source in source_jobs], raw_fingerprints=raw_fingerprints, synthesis_metadata=synthesis_metadata)
            return self._publication_report(result, state, claimed=claimed_count, completed=len(sources), failed=len(failures), failures=tuple(failures))
        except PublicationCrash:
            raise
        except Exception as exc:
            publication_error = isinstance(exc, PublicationError)
            failure_code = exc.code if publication_error else "publication_failed"
            failure_message = exc.message if publication_error else str(exc)
            if publication_error or not isinstance(exc, ValidationError):
                failures.append(str(exc))
            journal = state.publication(batch_id)
            if journal:
                recovery = publication.recover_oldest()
                if recovery:
                    if recovery.phase != PublicationPhase.RECOVERY_BLOCKED:
                        if recovery.phase == PublicationPhase.ROLLED_BACK:
                            state.update_publication(recovery.batch_id, recovery.phase, action=recovery.action, commit_id=recovery.commit_id, failure_code=failure_code, failure_message=failure_message)
                        recovery = PublicationResult(recovery.batch_id, recovery.phase, recovery.action, recovery.commit_id, failure_code, failure_message)
                    return self._publication_report(recovery, state, claimed=claimed_count, completed=len(sources), failed=len(failures), failures=tuple(failures))
                state.update_publication(batch_id, journal.phase, action="publication_failed", failure_code=failure_code, failure_message=failure_message)
                phase = journal.phase
                commit_id = journal.commit_id
            else:
                state.fail_batch(batch_id)
                phase = PublicationPhase.ROLLED_BACK if publication_error else None
                commit_id = None
            report_failure = journal is not None or publication_error
            return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=commit_id, failures=tuple(failures), publication_phase=phase, recovery_action="publication_failed" if report_failure else None, publication_failure_code=failure_code if report_failure else None, publication_failure_message=failure_message if report_failure else None, outstanding_cleanup=len(state.pending_cleanup()))

    def _synthesize(self, sources: list[SourceDocument], concept_catalog: dict[str, str], staging: Path) -> tuple[ChangeSet, dict[str, Any], SynthesisFailure | None]:
        """Cross the controlled boundary once, retaining its validated handoff."""
        adapter = self.synthesizer
        if isinstance(adapter, ControlledSynthesis) or (hasattr(adapter, "run") and not hasattr(adapter, "synthesize")) or hasattr(adapter, "execute"):
            committed_sources = self._committed_sources()
            runner = adapter if isinstance(adapter, ControlledSynthesis) or (hasattr(adapter, "run") and not hasattr(adapter, "synthesize")) else ControlledSynthesis(adapter)
            result = runner.run(sources, concept_catalog, staging, vault=self.config.vault, committed_sources=committed_sources)
        else:
            # Compatibility for third-party legacy test doubles.  Production
            # adapters are wrapped above and never use this path.
            changes = adapter.synthesize(sources, concept_catalog, staging)
            validate_changes(changes, sources, self.config.vault)
            return changes, dict(changes.metadata), None
        if isinstance(result, SynthesisFailure):
            return ChangeSet(), {}, result
        if not isinstance(result, SynthesisOutcome):
            raise BatchError("controlled synthesis returned an invalid result")
        return result.change_set, dict(result.change_set.metadata), None

    def _committed_sources(self) -> list[SourceDocument]:
        """Read only the immutable, rendered source versions already in the vault."""
        committed: list[SourceDocument] = []
        if not self.config.sources.exists():
            return committed
        for path in sorted(self.config.sources.rglob("*.md")):
            if path.is_symlink() or not path.is_file():
                raise BatchError(f"committed source is not a regular file: {path}")
            text = path.read_text(encoding="utf-8")
            if not text.startswith("---\n") or "\n---\n" not in text:
                raise BatchError(f"committed source has invalid frontmatter: {path}")
            header, rendered_body = text[4:].split("\n---\n", 1)
            metadata: dict[str, object] = {}
            for line in header.splitlines():
                if ":" not in line:
                    raise BatchError(f"committed source has invalid metadata: {path}")
                key, raw = line.split(":", 1)
                try:
                    metadata[key] = None if raw.strip() == "null" else json.loads(raw.strip())
                except (TypeError, ValueError) as exc:
                    raise BatchError(f"committed source has invalid metadata: {path}") from exc
            title = str(metadata.get("title", ""))
            marker = f"# {title}\n\n"
            if not rendered_body.startswith(marker):
                raise BatchError(f"committed source body does not match its title: {path}")
            content = rendered_body[len(marker):]
            kind = str(metadata.get("source_type", ""))
            if kind not in {"article", "youtube"}:
                raise BatchError(f"committed source has invalid type: {path}")
            try:
                source_version = int(metadata["immutable_source_version"])
                source_id = str(metadata["source_id"])
                kind = str(metadata["source_type"])
                canonical_url = str(metadata["canonical_url"])
                content_hash = str(metadata["content_hash"])
            except (KeyError, TypeError, ValueError) as exc:
                raise BatchError(f"committed source is missing immutable identity: {path}") from exc
            try:
                candidate = render_source(source_id=source_id, kind=kind, canonical_url=canonical_url, title=title, body=content, author=metadata.get("author"), publication_date=metadata.get("publication_date"), captured_at=str(metadata["captured_at"]), input_method=str(metadata["input_method"]), source_version=source_version)
            except (KeyError, TypeError, ValueError) as exc:
                raise BatchError(f"committed source cannot be deterministically rendered: {path}") from exc
            if candidate.relative_path != str(path.relative_to(self.config.vault)) or candidate.content_hash != content_hash or render_markdown(candidate) != text:
                raise BatchError(f"committed source identity or rendering mismatch: {path}")
            committed.append(candidate)
        return committed

    @staticmethod
    def _publication_report(result, state: StateStore, *, claimed: int = 0, completed: int | None = None, failed: int | None = None, failures: tuple[str, ...] = ()) -> BatchReport:
        jobs = state.jobs_for_batch(result.batch_id)
        completed = sum(job.status == "complete" for job in jobs) if completed is None else completed
        failed = sum(job.status == "failed" for job in jobs) if failed is None else failed
        failures = tuple(job.failure_message for job in jobs if job.failure_message) if not failures else failures
        return BatchReport(batch_id=result.batch_id, claimed=claimed, completed=completed, failed=failed, committed=result.phase in COMMITTED_PUBLICATION_PHASES, commit_id=result.commit_id, failures=failures, publication_phase=result.phase, recovery_action=result.action, recovery_block_reason=(result.failure_code or result.failure_message) if result.phase == PublicationPhase.RECOVERY_BLOCKED else None, publication_failure_code=result.failure_code, publication_failure_message=result.failure_message, outstanding_cleanup=len(state.pending_cleanup()))

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

    def _allowed_input_paths(self) -> set[str]:
        allowed = {str(self._queue_path().relative_to(self.config.vault))}
        pairing = self.config.to_ingest / "HTML Pairings.yaml"
        if pairing.exists():
            allowed.add(str(pairing.relative_to(self.config.vault)))
        if self.config.to_ingest.exists():
            allowed.update(str(path.relative_to(self.config.vault)) for path in self.config.to_ingest.iterdir() if path.is_file() and path.suffix.lower() in {".html", ".htm"})
        return allowed

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
