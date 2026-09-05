from __future__ import annotations

import json
import subprocess
from pathlib import Path

from second_brain.batch import BatchRunner
from second_brain.cli import build_parser, main
from second_brain.config import Config
from second_brain.intake import ArticleIntakeAdapter, SourceIntake
from second_brain.models import AcquiredArticle, Job, PublicationIntent
from second_brain.state import StateStore
from second_brain.synthesis import ExecutorIdentity, FailureCategory, SynthesisFailure


def _git_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


def _job(state: StateStore, job_id: str) -> Job:
    job = state.get(job_id)
    assert job is not None
    return job


def test_retry_parser_supports_explicit_all_eligible_mode():
    args = build_parser().parse_args(["retry", "--all-eligible"])

    assert args.command == "retry"
    assert args.all_eligible is True
    assert args.job_id is None


def test_state_lists_only_retryable_failed_jobs_in_stable_order(tmp_path: Path):
    state = StateStore(tmp_path / "state.sqlite3")
    try:
        batch_id = state.create_batch()
        older = state.claim("article", "article:https://example.com/older", "https://example.com/older", input_artifact=None, batch_id=batch_id)
        newer = state.claim("article", "article:https://example.com/newer", "https://example.com/newer", input_artifact=None, batch_id=batch_id)
        non_retryable = state.claim("article", "article:https://example.com/permanent", "https://example.com/permanent", input_artifact=None, batch_id=batch_id)
        active = state.claim("article", "article:https://example.com/active", "https://example.com/active", input_artifact=None, batch_id=batch_id)
        source_ready = state.claim("article", "article:https://example.com/ready", "https://example.com/ready", input_artifact=None, batch_id=batch_id)
        state.fail(older.id, "http_failed", "older", retryable=True)
        state.fail(newer.id, "http_failed", "newer", retryable=True)
        state.fail(non_retryable.id, "invalid_input", "permanent", retryable=False)
        state.processing(active.id)
        state.complete(source_ready.id, "hash", 1)
        state.connection.execute(
            "UPDATE jobs SET created_at=?, updated_at=? WHERE id=?",
            ("2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00", older.id),
        )
        state.connection.execute(
            "UPDATE jobs SET created_at=?, updated_at=? WHERE id=?",
            ("2026-01-02T00:00:00+00:00", "2026-01-02T00:00:00+00:00", newer.id),
        )

        selected = state.retryable_failed_jobs()

        assert [job.id for job in selected] == [older.id, newer.id]
        assert _job(state, non_retryable.id).status == "failed"
        assert _job(state, active.id).status == "processing"
        assert _job(state, source_ready.id).status == "source_ready"
    finally:
        state.close()


def test_state_retries_all_selected_jobs_atomically(tmp_path: Path):
    state = StateStore(tmp_path / "state.sqlite3")
    try:
        original_batch = state.create_batch()
        target = state.claim("article", "article:https://example.com/target", "https://example.com/target", input_artifact=None, batch_id=original_batch)
        excluded = state.claim("article", "article:https://example.com/excluded", "https://example.com/excluded", input_artifact=None, batch_id=original_batch)
        state.fail(target.id, "http_failed", "try again", retryable=True)
        state.fail(excluded.id, "invalid_input", "do not retry", retryable=False)
        retry_batch = state.create_batch()

        selected = state.retry_failed_jobs(retry_batch)

        assert [job.id for job in selected] == [target.id]
        retried = state.get(target.id)
        assert retried is not None
        assert retried.status == "claimed"
        assert retried.batch_id == retry_batch
        assert retried.failure_code is None
        assert retried.failure_message is None
        untouched = state.get(excluded.id)
        assert untouched is not None
        assert untouched.status == "failed"
        assert untouched.retryable is False
    finally:
        state.close()


def test_single_retry_rejects_an_active_job(tmp_path: Path):
    state = StateStore(tmp_path / "state.sqlite3")
    try:
        original_batch = state.create_batch()
        active = state.claim("article", "article:https://example.com/active", "https://example.com/active", input_artifact=None, batch_id=original_batch)
        retry_batch = state.create_batch()

        try:
            state.retry(active.id, retry_batch)
        except ValueError as exc:
            assert str(exc) == f"job {active.id} is not a failed job"
        else:
            raise AssertionError("active job was reopened")

        unchanged = state.get(active.id)
        assert unchanged is not None
        assert unchanged.status == "claimed"
        assert unchanged.batch_id == original_batch
    finally:
        state.close()


def test_retry_collection_reuses_saved_input_without_discovery(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    raw = to_ingest / "saved.html"
    raw.write_text("<article><p>Durable input.</p></article>", encoding="utf-8")
    config = Config.load(vault, tmp_path / "state")
    state = StateStore(config.database)
    try:
        original_batch = state.create_batch()
        job = state.claim(
            "article",
            "article:https://example.com/saved",
            "https://example.com/saved",
            input_artifact=str(raw),
            batch_id=original_batch,
            publication_intent=PublicationIntent(queue_path="To Ingest.md", queue_locator="https://example.com/saved", raw_path=str(raw)),
        )
        state.fail(job.id, "acquisition_failed", "temporary", retryable=True)
        retry_batch = state.create_batch()
        selected = state.retry_failed_jobs(retry_batch)
        adapter = ArticleIntakeAdapter(config, lambda _url: (_ for _ in ()).throw(AssertionError("retry fetched URL")))
        intake = SourceIntake(config, state, (adapter,))

        result = intake.collect_retries(retry_batch, selected)

        assert len(result.successes) == 1
        assert result.successes[0].job.id == job.id
        assert isinstance(result.successes[0].payload, AcquiredArticle)
        assert result.successes[0].payload.input_method == "saved-html"
    finally:
        state.close()


def test_retry_collection_rejects_saved_input_outside_to_ingest(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    external = tmp_path / "external.html"
    external.write_text("<article><p>Do not read this.</p></article>", encoding="utf-8")
    config = Config.load(vault, tmp_path / "state")
    state = StateStore(config.database)
    try:
        original_batch = state.create_batch()
        job = state.claim(
            "article",
            "article:https://example.com/external",
            "https://example.com/external",
            input_artifact=str(external),
            batch_id=original_batch,
        )
        state.fail(job.id, "acquisition_failed", "temporary", retryable=True)
        retry_batch = state.create_batch()
        selected = state.retry_failed_jobs(retry_batch)
        intake = SourceIntake(config, state, (ArticleIntakeAdapter(config, lambda _url: ""),))

        try:
            intake.collect_retries(retry_batch, selected)
        except ValueError as exc:
            assert str(exc) == "retry input path is outside the vault"
        else:
            raise AssertionError("external saved input was accepted")
    finally:
        state.close()


def test_cli_retries_all_eligible_jobs_with_a_concise_partial_report(tmp_path: Path, capsys):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    saved = to_ingest / "saved.html"
    saved.write_text(
        '<html><head><link rel="canonical" href="https://example.com/saved"></head><body><article><p>Retry evidence.</p></article></body></html>',
        encoding="utf-8",
    )
    _git_repo(vault)
    config = Config.load(vault, tmp_path / "state")
    state = StateStore(config.database)
    try:
        original_batch = state.create_batch()
        success = state.claim(
            "article",
            "article:https://example.com/saved",
            "https://example.com/saved",
            input_artifact=str(saved),
            batch_id=original_batch,
            publication_intent=PublicationIntent(queue_path="To Ingest.md", queue_locator="https://example.com/saved", raw_path=str(saved)),
        )
        repeated_failure = state.claim(
            "article",
            "article:https://example.com/missing",
            "https://example.com/missing",
            input_artifact=str(vault / "missing.html"),
            batch_id=original_batch,
            publication_intent=PublicationIntent(queue_path="To Ingest.md", queue_locator="https://example.com/missing"),
        )
        permanent = state.claim("article", "article:https://example.com/permanent", "https://example.com/permanent", input_artifact=None, batch_id=original_batch)
        active = state.claim("article", "article:https://example.com/active", "https://example.com/active", input_artifact=None, batch_id=original_batch)
        state.fail(success.id, "temporary", "retry me", retryable=True)
        state.fail(repeated_failure.id, "temporary", "still broken", retryable=True)
        state.fail(permanent.id, "invalid_input", "do not retry", retryable=False)
        state.connection.execute(
            "UPDATE jobs SET created_at=? WHERE id=?",
            ("2026-01-01T00:00:00+00:00", success.id),
        )
        state.connection.execute(
            "UPDATE jobs SET created_at=? WHERE id=?",
            ("2026-01-02T00:00:00+00:00", repeated_failure.id),
        )
    finally:
        state.close()

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "retry", "--all-eligible"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["mode"] == "all-eligible"
    assert report["selected_job_ids"] == [success.id, repeated_failure.id]
    assert report["completed"] == 1
    assert report["failed"] == 1
    assert report["committed"] is True
    state = StateStore(config.database)
    try:
        assert _job(state, success.id).status == "complete"
        assert _job(state, repeated_failure.id).status == "failed"
        assert _job(state, permanent.id).status == "failed"
        assert _job(state, permanent.id).retryable is False
        assert _job(state, active.id).status == "claimed"
    finally:
        state.close()


def test_cli_all_eligible_retry_with_no_candidates_is_successful_and_empty(tmp_path: Path, capsys):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    _git_repo(vault)
    config = Config.load(vault, tmp_path / "state")

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "retry", "--all-eligible"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report == {
        "mode": "all-eligible",
        "selected": 0,
        "selected_job_ids": [],
        "batch_id": None,
        "completed": 0,
        "failed": 0,
        "committed": False,
        "commit_id": None,
        "failures": [],
        "recovery_block_reason": None,
    }
    state = StateStore(config.database)
    try:
        assert state.list_batches() == []
    finally:
        state.close()


def test_status_exposes_actionable_retry_command_for_each_failure(tmp_path: Path, capsys):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    _git_repo(vault)
    config = Config.load(vault, tmp_path / "state")
    state = StateStore(config.database)
    try:
        batch_id = state.create_batch()
        retryable = state.claim("article", "article:https://example.com/retry", "https://example.com/retry", input_artifact=None, batch_id=batch_id)
        permanent = state.claim("article", "article:https://example.com/permanent", "https://example.com/permanent", input_artifact=None, batch_id=batch_id)
        state.fail(retryable.id, "temporary", "try again", retryable=True)
        state.fail(permanent.id, "invalid_input", "repair input", retryable=False)
    finally:
        state.close()

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    jobs = {job["id"]: job for job in status["jobs"]}

    assert jobs[retryable.id]["retry_command"] == f"second-brain retry {retryable.id}"
    assert jobs[permanent.id]["retry_command"] is None


def test_cli_retry_invalid_job_id_keeps_actionable_error(tmp_path: Path, capsys):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    _git_repo(vault)
    config = Config.load(vault, tmp_path / "state")

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "retry", "missing-job"]) == 2
    error = json.loads(capsys.readouterr().err)

    assert error == {"ok": False, "error": "'missing-job'"}


def test_cli_single_job_retry_keeps_existing_batch_report(tmp_path: Path, capsys):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    saved = to_ingest / "saved.html"
    saved.write_text(
        '<html><head><link rel="canonical" href="https://example.com/saved"></head><body><article><p>Single retry evidence.</p></article></body></html>',
        encoding="utf-8",
    )
    _git_repo(vault)
    config = Config.load(vault, tmp_path / "state")
    state = StateStore(config.database)
    try:
        original_batch = state.create_batch()
        job = state.claim(
            "article",
            "article:https://example.com/saved",
            "https://example.com/saved",
            input_artifact=str(saved),
            batch_id=original_batch,
            publication_intent=PublicationIntent(queue_path="To Ingest.md", queue_locator="https://example.com/saved", raw_path=str(saved)),
        )
        state.fail(job.id, "temporary", "retry me", retryable=True)
    finally:
        state.close()

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "retry", job.id]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["batch_id"]
    assert "mode" not in report
    assert report["committed"] is True


def test_cli_all_retry_resumes_source_ready_candidate_exactly(tmp_path: Path, capsys):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("https://example.com/article\n", encoding="utf-8")
    _git_repo(vault)
    config = Config.load(vault, tmp_path / "state")
    calls: list[str] = []

    class FailingSynthesis:
        def run(self, _batch_id, _sources):
            return SynthesisFailure(FailureCategory.ADAPTER_EXECUTION_FAILURE, "temporary synthesis failure", ExecutorIdentity("fixture"))

    first = BatchRunner(
        config,
        synthesis_runner=FailingSynthesis(),  # pyright: ignore[reportArgumentType]
        fetcher=lambda url: calls.append(url) or "<article><p>Exact durable evidence.</p></article>",
    ).run()
    assert first.committed is False
    state = StateStore(config.database)
    try:
        ready = state.source_ready_jobs()
        assert len(ready) == 1
        candidate = state.source_candidate(ready[0].id)
        assert candidate is not None
        prepared = Path(candidate.artifact_path, "source.md").read_bytes()
    finally:
        state.close()

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "retry", "--all-eligible"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["selected"] == 0
    assert report["completed"] == 1
    assert report["committed"] is True
    assert calls == ["https://example.com/article"]
    published = next((vault / "Sources").rglob("*.md"))
    assert published.read_bytes() == prepared
