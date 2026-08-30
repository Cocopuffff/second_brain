from __future__ import annotations

import fcntl
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .extraction import ImageProcessor
from .git_ops import GitError, GitRepository
from .html_discovery import discover_html
from .intake import build_source_intake, queue_path_for
from .models import BatchReport, COMMITTED_PUBLICATION_PHASES, ChangeSet, Job, PublicationPhase, SourceCandidate, PreparationFailure
from .preparation import PreparationFaults, SourcePreparation, build_source_preparation
from .publication import BatchPublication, PublicationCrash, PublicationError, PublicationFaults, PublicationResult
from .queue import remove_claimed_urls_text
from .state import StateStore
from .synthesis import ExecutorIdentity, FailureCategory, SynthesisFailure, SynthesisOutcome, SynthesisRunner, build_controlled_synthesis
from .validation import ValidationError, validate_markdown
from .youtube import YouTubeClient


class BatchError(RuntimeError):
    pass


@dataclass
class _PreparedBatch:
    state: StateStore
    git: GitRepository
    publication: BatchPublication
    batch_id: str
    sources: list[SourceCandidate]
    source_jobs: list[Job]
    queue_path: Path
    queue_job_ids: list[str]
    raw_fingerprints: dict[str, tuple[str, str | None]]
    claimed_count: int
    failures: list[str]
    allowed_paths: set[str]


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
    def __init__(self, config: Config, *, synthesis_runner: SynthesisRunner | None = None, image_processor: ImageProcessor | None = None, youtube_client: YouTubeClient | None = None, fetcher=None, _publication_faults: PublicationFaults | None = None, _preparation_faults: PreparationFaults | None = None):
        self.config = config
        self.synthesis_runner = synthesis_runner or build_controlled_synthesis(config)
        self.image_processor = image_processor
        self.youtube_client = youtube_client
        self.fetcher = fetcher or self._fetch
        self._publication_faults = _publication_faults
        self._preparation_faults = _preparation_faults

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
        preparation = build_source_preparation(self.config, state, self.image_processor, faults=self._preparation_faults)
        publication = BatchPublication(
            self.config,
            state,
            git,
            faults=self._publication_faults,
            candidate_cleanup=preparation.compact,
            youtube_acknowledger=self.youtube_client.acknowledge if self.youtube_client else None,
        )
        recovery = publication.recover_oldest()
        if recovery:
            return self._publication_report(recovery, state)
        cleanup_recovery = publication.retry_cleanup()
        if cleanup_recovery:
            return self._publication_report(cleanup_recovery, state)

        state.recover()
        source_ready = state.source_ready_jobs()
        if source_ready:
            batch_id = state.create_batch()
            state.rebind_source_ready([job.id for job in source_ready], batch_id)
            return self._run_source_ready_batch(state, preparation, publication, batch_id)
        intake = build_source_intake(
            self.config,
            state,
            fetcher=self.fetcher,
            youtube_client=self.youtube_client,
        )
        # Queue and raw HTML are accepted inputs; every other existing staged,
        # modified, or untracked path is an operator-owned stop condition.
        allowed_paths = intake.allowed_input_paths()
        git.ensure_clean(allowed_paths=allowed_paths)
        batch_id = state.create_batch()
        if retry_job_id:
            try:
                state.retry(retry_job_id, batch_id)
            except (KeyError, ValueError) as exc:
                state.fail_batch(batch_id)
                raise BatchError(str(exc)) from exc
        intake_batch = intake.collect(batch_id)
        claimed_count = intake_batch.claimed_count
        failures = [
            f"{failure.original_locator}: {failure.safe_message}"
            if failure.original_locator
            else failure.safe_message
            for failure in intake_batch.failures
        ]
        sources: list[SourceCandidate] = []
        source_jobs: list[Job] = []
        queue_path = intake_batch.queue_path
        queue_job_ids = [
            success.job.id
            for success in intake_batch.successes
            if success.payload.publication_intent.queue_locator
        ]
        raw_fingerprints: dict[str, tuple[str, str | None]] = {}
        for success in intake_batch.successes:
            job = success.job
            try:
                current_job = state.get(job.id)
                if current_job is None:
                    raise BatchError(f"claimed job disappeared: {job.id}")
                prepared = preparation.prepare(current_job, success.payload)
                if isinstance(prepared, PreparationFailure):
                    failures.append(f"{job.original_locator}: {prepared.safe_message}")
                    continue
                prepared_job = state.get(job.id) or job
                source_jobs.append(prepared_job)
                sources.append(prepared)
                if prepared.publication_intent.raw_path:
                    raw_fingerprints[job.id] = (prepared.publication_intent.raw_path, prepared.publication_intent.raw_hash)
            except Exception as exc:
                code = "transcript_missing" if "transcript" in str(exc).lower() else "processing_failed"
                state.fail(job.id, code, str(exc))
                failures.append(f"{job.original_locator}: {exc}")

        if not sources:
            state.fail_batch(batch_id)
            return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))

        return self._publish_prepared(_PreparedBatch(state, git, publication, batch_id, sources, source_jobs, queue_path, queue_job_ids, raw_fingerprints, claimed_count, failures, set(intake_batch.allowed_paths)))

    def _synthesize(self, batch_id: str, sources: list[SourceCandidate]) -> tuple[ChangeSet, dict, SynthesisFailure | None]:
        """Cross the one typed synthesis seam; no legacy fallback exists."""
        try:
            result = self.synthesis_runner.run(batch_id, sources)
        except Exception:
            return ChangeSet(), {}, SynthesisFailure(FailureCategory.ADAPTER_EXECUTION_FAILURE, "controlled synthesis failed", ExecutorIdentity("unknown"))
        if isinstance(result, SynthesisFailure):
            return ChangeSet(), {}, result
        if not isinstance(result, SynthesisOutcome):
            raise BatchError("controlled synthesis returned an invalid result")
        return result.change_set, result.metadata.as_dict(), None

    def _run_source_ready_batch(self, state: StateStore, preparation: SourcePreparation, publication: BatchPublication, batch_id: str) -> BatchReport:
        git = GitRepository(self.config.vault)
        sources: list[SourceCandidate] = []
        source_jobs: list[Job] = []
        queue_job_ids: list[str] = []
        raw_fingerprints: dict[str, tuple[str, str | None]] = {}
        failures: list[str] = []
        for job in state.jobs_for_batch(batch_id):
            if job.status != "source_ready":
                continue
            loaded = preparation.load(job)
            if isinstance(loaded, PreparationFailure):
                state.fail(job.id, loaded.category.value, loaded.safe_message, retryable=loaded.retryable)
                failures.append(f"{job.original_locator}: {loaded.safe_message}")
                continue
            sources.append(loaded)
            source_jobs.append(job)
            if loaded.publication_intent.queue_locator:
                queue_job_ids.append(job.id)
            if loaded.publication_intent.raw_path:
                raw_fingerprints[job.id] = (loaded.publication_intent.raw_path, loaded.publication_intent.raw_hash)
        queue_paths = {source.publication_intent.queue_path for source in sources if source.publication_intent.queue_path}
        if len(queue_paths) > 1:
            failures.append("source candidates contain conflicting durable queue paths")
            for job in source_jobs:
                state.fail(job.id, "path_collision", "source candidates contain conflicting durable queue paths")
            state.fail_batch(batch_id)
            return BatchReport(batch_id=batch_id, claimed=len(sources), completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))
        queue_path = self.config.vault / next(iter(queue_paths)) if queue_paths else self.config.vault / "To Ingest.md"
        allowed_paths = self._allowed_retry_paths(sources, queue_path)
        git.ensure_clean(allowed_paths=allowed_paths)
        return self._publish_prepared(_PreparedBatch(state, git, publication, batch_id, sources, source_jobs, queue_path, queue_job_ids, raw_fingerprints, len(sources), failures, allowed_paths))

    def _publish_prepared(self, context: _PreparedBatch) -> BatchReport:
        state = context.state
        git = context.git
        publication = context.publication
        batch_id = context.batch_id
        sources = context.sources
        source_jobs = context.source_jobs
        queue_path = context.queue_path
        queue_job_ids = context.queue_job_ids
        raw_fingerprints = context.raw_fingerprints
        claimed_count = context.claimed_count
        failures = context.failures
        allowed_paths = context.allowed_paths
        if not sources:
            state.fail_batch(batch_id)
            return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))
        try:
            changes, synthesis_metadata, synthesis_failure = self._synthesize(batch_id, sources)
            if synthesis_failure:
                failures.append(f"{synthesis_failure.category}: {synthesis_failure.safe_message}")
                state.fail_batch(batch_id)
                return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))
            source_files: dict[str, str | bytes] = {source.relative_path: source.rendered_markdown for source in sources}
            for content in source_files.values():
                validate_markdown(content.decode("utf-8") if isinstance(content, bytes) else content)
            all_files: dict[str, str | bytes] = {**source_files, **{candidate.relative_path: candidate.content for candidate in changes.files}}
            queue_intents = [source.publication_intent for source in sources if source.publication_intent.queue_locator]
            durable_queue_paths = {intent.queue_path for intent in queue_intents if intent.queue_path}
            if queue_intents and durable_queue_paths != {str(queue_path.relative_to(self.config.vault))}:
                raise PublicationError("queue_intent_mismatch", "prepared queue intent does not match the durable queue path")
            if queue_intents and not queue_path.exists():
                raise PublicationError("queue_payload_missing", f"durable queue path is missing: {queue_path}")
            if queue_intents:
                latest_queue = queue_path.read_text(encoding="utf-8")
                claimed_urls = {intent.queue_locator for intent in queue_intents if intent.queue_locator}
                all_files[str(queue_path.relative_to(self.config.vault))] = remove_claimed_urls_text(latest_queue, claimed_urls)
            if not all_files and not changes.deletions:
                state.fail_batch(batch_id)
                return BatchReport(batch_id=batch_id, claimed=claimed_count, completed=0, failed=len(failures), committed=False, commit_id=None, failures=tuple(failures))
            git.ensure_clean(allowed_paths=allowed_paths)
            youtube_acknowledgements = {
                source.job_id: source.publication_intent.youtube_playlist_item_id
                for source in sources
                if source.publication_intent.youtube_playlist_item_id
            }
            result = publication.publish(batch_id, all_files, changes.deletions, queue_path=str(queue_path.relative_to(self.config.vault)), queue_job_ids=queue_job_ids, source_jobs=source_jobs, youtube_acknowledgements=youtube_acknowledgements, raw_fingerprints=raw_fingerprints, synthesis_metadata=synthesis_metadata)
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

    @staticmethod
    def _publication_report(result, state: StateStore, *, claimed: int = 0, completed: int | None = None, failed: int | None = None, failures: tuple[str, ...] = ()) -> BatchReport:
        jobs = state.jobs_for_batch(result.batch_id)
        completed = sum(job.status == "complete" for job in jobs) if completed is None else completed
        failed = sum(job.status == "failed" for job in jobs) if failed is None else failed
        failures = tuple(job.failure_message for job in jobs if job.failure_message) if not failures else failures
        return BatchReport(batch_id=result.batch_id, claimed=claimed, completed=completed, failed=failed, committed=result.phase in COMMITTED_PUBLICATION_PHASES, commit_id=result.commit_id, failures=failures, publication_phase=result.phase, recovery_action=result.action, recovery_block_reason=(result.failure_code or result.failure_message) if result.phase == PublicationPhase.RECOVERY_BLOCKED else None, publication_failure_code=result.failure_code, publication_failure_message=result.failure_message, outstanding_cleanup=len(state.pending_cleanup()))

    def _queue_path(self) -> Path:
        return queue_path_for(self.config)

    def _allowed_retry_paths(self, sources: list[SourceCandidate], queue_path: Path) -> set[str]:
        allowed = {str(queue_path.relative_to(self.config.vault))}
        for source in sources:
            raw_path = source.publication_intent.raw_path
            if raw_path:
                try:
                    allowed.add(str(Path(raw_path).relative_to(self.config.vault)))
                except ValueError:
                    continue
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
