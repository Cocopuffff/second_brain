from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from second_brain.config import Config
from second_brain.batch import BatchRunner
from second_brain.intake import IntakeFailureCategory, IntakeSuccess, build_source_intake
from second_brain.models import AcquiredArticle, AcquiredYouTube, VideoInput
from second_brain.state import StateStore
from second_brain.youtube import FixtureYouTubeClient
from second_brain.publication import PublicationCrash


def _git_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


def _fixture(tmp_path: Path) -> tuple[Path, Config, FixtureYouTubeClient]:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("https://example.com/article\n", encoding="utf-8")
    _git_repo(vault)
    config = Config.load(vault, tmp_path / "state")
    youtube_fixture = tmp_path / "youtube.json"
    youtube_fixture.write_text(
        '{"To Ingest": [{"video_id": "abcDEF12345", "title": "Video", "playlist_item_id": "playlist-item", "manual_transcript": [{"start": 12, "end": 20, "text": "Evidence"}]}]}',
        encoding="utf-8",
    )
    return vault, config, FixtureYouTubeClient(youtube_fixture)


def test_article_and_youtube_share_one_normalized_intake_contract(tmp_path: Path):
    vault, config, youtube = _fixture(tmp_path)
    queue_before = (vault / "To Ingest.md").read_bytes()
    state = StateStore(config.database)
    try:
        batch_id = state.create_batch()
        intake = build_source_intake(
            config,
            state,
            fetcher=lambda _url: "<article><p>Article evidence.</p></article>",
            youtube_client=youtube,
        )

        result = intake.collect(batch_id)

        assert result.claimed_count == 2
        assert result.failures == ()
        assert len(result.successes) == 2
        assert all(isinstance(item, IntakeSuccess) for item in result.successes)
        by_kind = {item.job.kind: item for item in result.successes}
        assert isinstance(by_kind["article"].payload, AcquiredArticle)
        assert isinstance(by_kind["youtube"].payload, AcquiredYouTube)
        assert by_kind["article"].payload.publication_intent.queue_locator == "https://example.com/article"
        assert by_kind["youtube"].payload.publication_intent.youtube_playlist_item_id == "playlist-item"
        assert all(item.job.status == "claimed" for item in result.successes)
        persisted_article = state.get(by_kind["article"].job.id)
        persisted_youtube = state.get(by_kind["youtube"].job.id)
        assert persisted_article is not None
        assert persisted_article.queue_path == "To Ingest.md"
        assert persisted_article.queue_locator == "https://example.com/article"
        assert persisted_youtube is not None
        assert persisted_youtube.youtube_playlist_item_id == "playlist-item"
    finally:
        state.close()

    assert (vault / "To Ingest.md").read_bytes() == queue_before
    assert youtube.acknowledged == []


def test_claim_is_exclusive_then_recoverable_after_restart(tmp_path: Path):
    vault, config, _youtube = _fixture(tmp_path)
    first_state = StateStore(config.database)
    second_state = StateStore(config.database)
    try:
        first_batch = first_state.create_batch()
        first = build_source_intake(
            config,
            first_state,
            fetcher=lambda _url: "<article><p>First acquisition.</p></article>",
        ).collect(first_batch)
        claimed_job = first.successes[0].job

        competing_batch = second_state.create_batch()
        competing = build_source_intake(
            config,
            second_state,
            fetcher=lambda _url: "<article><p>Must not run.</p></article>",
        ).collect(competing_batch)

        assert competing.claimed_count == 0
        assert competing.successes == ()
        assert second_state.get(claimed_job.id).batch_id == first_batch  # type: ignore[union-attr]

        second_state.recover()
        recovery_batch = second_state.create_batch()
        recovered = build_source_intake(
            config,
            second_state,
            fetcher=lambda _url: "<article><p>Recovered acquisition.</p></article>",
        ).collect(recovery_batch)

        assert recovered.claimed_count == 1
        assert recovered.successes[0].job.id == claimed_job.id
        assert recovered.successes[0].job.batch_id == recovery_batch
        assert len(second_state.list_jobs()) == 1
    finally:
        first_state.close()
        second_state.close()

    assert (vault / "To Ingest.md").read_text(encoding="utf-8") == "https://example.com/article\n"


def test_conflicting_article_payload_fails_through_the_intake_contract(tmp_path: Path):
    vault, config, _youtube = _fixture(tmp_path)
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    first = to_ingest / "first.html"
    second = to_ingest / "second.html"
    first.write_text(
        '<link rel="canonical" href="https://example.com/article"><article><p>First.</p></article>',
        encoding="utf-8",
    )
    second.write_text(
        '<link rel="canonical" href="https://example.com/article"><article><p>Second.</p></article>',
        encoding="utf-8",
    )
    queue_before = (vault / "To Ingest.md").read_bytes()

    report = BatchRunner(config).run()

    assert not report.committed
    assert any("multiple non-identical HTML files" in failure for failure in report.failures)
    state = StateStore(config.database)
    try:
        job = state.find("article:https://example.com/article")
        assert job is not None
        assert job.status == "failed"
        assert job.failure_code == "conflicting_payload"
    finally:
        state.close()
    assert (vault / "To Ingest.md").read_bytes() == queue_before
    assert first.exists()
    assert second.exists()


class _CrashAfterYouTubeAcknowledgement:
    def hit(self, event: str, **_details) -> None:
        if event == "youtube_acknowledged":
            raise PublicationCrash(event)


def test_youtube_acknowledgement_retries_after_commit_without_rediscovery(tmp_path: Path):
    vault, config, youtube = _fixture(tmp_path)
    (vault / "To Ingest.md").write_text("", encoding="utf-8")

    with pytest.raises(PublicationCrash):
        BatchRunner(
            config,
            youtube_client=youtube,
            _publication_faults=_CrashAfterYouTubeAcknowledgement(),
        ).run()

    assert youtube.acknowledged == ["playlist-item"]
    commits_after_crash = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=vault,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()

    recovered = BatchRunner(config, youtube_client=youtube).run()

    assert recovered.committed
    assert recovered.publication_phase == "complete"
    assert youtube.acknowledged == ["playlist-item"]
    assert youtube.acknowledgement_attempts == ["playlist-item", "playlist-item"]
    assert subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=vault,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip() == commits_after_crash
    state = StateStore(config.database)
    try:
        job = state.find("youtube:https://www.youtube.com/watch?v=abcDEF12345")
        assert job is not None
        assert job.status == "complete"
        assert job.queue_acknowledged
    finally:
        state.close()


def test_repeated_discovery_reuses_stable_job_identity(tmp_path: Path):
    vault, config, _youtube = _fixture(tmp_path)
    (vault / "To Ingest.md").write_text(
        "https://Example.com/article?utm_source=capture#fragment\nhttps://example.com/article\n",
        encoding="utf-8",
    )
    state = StateStore(config.database)
    try:
        batch_id = state.create_batch()
        intake = build_source_intake(
            config,
            state,
            fetcher=lambda _url: "<article><p>Evidence.</p></article>",
        )

        first = intake.collect(batch_id)
        repeated = intake.collect(batch_id)

        assert len(first.successes) == 1
        assert len(repeated.successes) == 1
        assert first.successes[0].job.id == repeated.successes[0].job.id
        assert first.successes[0].job.source_key == "article:https://example.com/article"
        assert len(state.list_jobs()) == 1
    finally:
        state.close()


def test_acquisition_failure_is_structured_and_preserves_inputs(tmp_path: Path):
    vault, config, _youtube = _fixture(tmp_path)
    queue_before = (vault / "To Ingest.md").read_bytes()
    head_before = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=vault, text=True, capture_output=True, check=True
    ).stdout.strip()
    state = StateStore(config.database)
    try:
        batch_id = state.create_batch()

        def failed_fetch(_url: str) -> str:
            raise RuntimeError("fixture HTTP failure")

        result = build_source_intake(config, state, fetcher=failed_fetch).collect(batch_id)

        assert result.successes == ()
        assert len(result.failures) == 1
        failure = result.failures[0]
        assert failure.category == "acquisition_failed"
        assert failure.safe_message == "fixture HTTP failure"
        job = state.get(failure.job_id or "")
        assert job is not None
        assert job.status == "failed"
        assert job.failure_code == "acquisition_failed"
    finally:
        state.close()

    assert (vault / "To Ingest.md").read_bytes() == queue_before
    assert not list((vault / "Sources").rglob("*.md"))
    assert subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=vault, text=True, capture_output=True, check=True
    ).stdout.strip() == head_before


def test_youtube_without_playlist_item_id_is_a_structured_intake_failure(tmp_path: Path):
    vault, config, _youtube = _fixture(tmp_path)
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    fixture = tmp_path / "youtube-missing-item.json"
    fixture.write_text(
        '{"To Ingest": [{"video_id": "abcDEF12345", "title": "Video", "manual_transcript": [{"start": 1, "end": 2, "text": "Evidence"}]}]}',
        encoding="utf-8",
    )
    state = StateStore(config.database)
    try:
        batch_id = state.create_batch()
        result = build_source_intake(
            config,
            state,
            fetcher=lambda _url: "",
            youtube_client=FixtureYouTubeClient(fixture),
        ).collect(batch_id)

        assert result.successes == ()
        assert len(result.failures) == 1
        assert result.failures[0].category == IntakeFailureCategory.INVALID_ACKNOWLEDGEMENT_INTENT
        job = state.find("youtube:https://www.youtube.com/watch?v=abcDEF12345")
        assert job is not None
        assert job.status == "failed"
        assert job.failure_code == "invalid_acknowledgement_intent"
        with pytest.raises(ValueError, match="not retryable"):
            state.retry(job.id, batch_id)
    finally:
        state.close()
