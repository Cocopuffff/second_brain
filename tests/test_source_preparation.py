from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from second_brain.config import Config
from second_brain.canonical import stable_id
from second_brain.models import AcquiredArticle, AcquiredYouTube, ArticleEvidenceBounds, PreparationFailure, PreparationFailureCategory, PublicationIntent, TranscriptSegment, VideoInput
from second_brain.preparation import PreparationCrash, build_source_preparation
from second_brain.render import render_source, render_markdown
from second_brain.state import StateStore


def _setup(tmp_path: Path):
    vault = tmp_path / "vault"
    (vault / "Sources" / "Articles").mkdir(parents=True)
    (vault / "Sources" / "YouTube").mkdir(parents=True)
    config = Config(vault=vault, state_dir=tmp_path / "state")
    state = StateStore(config.database)
    batch_id = state.create_batch()
    job = state.claim("article", "article:https://example.com/one", "https://example.com/one", input_artifact=str(vault / "ToIngest" / "one.html"), batch_id=batch_id, captured_at="2026-01-01T00:00:00+00:00")
    return config, state, job


def test_article_candidate_is_durable_and_rehydrates_exact_bytes(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    preparation = build_source_preparation(config, state)
    payload = AcquiredArticle(
        "<html><title>One</title><article><p>Evidence that survives restart.</p></article></html>",
        "saved-html",
        PublicationIntent(queue_path="To Ingest.md", queue_locator=job.original_locator, raw_path=job.input_artifact, raw_hash="raw-hash"),
    )

    candidate = preparation.prepare(job, payload)

    assert not isinstance(candidate, PreparationFailure)
    assert state.get(job.id).status == "source_ready"
    assert candidate.source_identity.endswith(":v1")
    assert candidate.rendered_markdown.endswith(b"\n")
    assert isinstance(candidate.evidence_bounds, ArticleEvidenceBounds)
    assert candidate.evidence_bounds == ArticleEvidenceBounds(1, len(candidate.rendered_markdown.splitlines()))

    loaded = preparation.load(state.get(job.id))
    assert not isinstance(loaded, PreparationFailure)
    assert loaded.rendered_markdown == candidate.rendered_markdown
    assert loaded.manifest_hash == candidate.manifest_hash
    assert loaded.publication_intent == payload.publication_intent

    again = preparation.prepare(job, payload)
    assert not isinstance(again, PreparationFailure)
    assert again.rendered_markdown == candidate.rendered_markdown


def test_youtube_bounds_come_from_final_rendered_bytes(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    youtube_job = state.claim("youtube", "youtube:https://www.youtube.com/watch?v=abcDEF12345", "https://www.youtube.com/watch?v=abcDEF12345", input_artifact="playlist-item", batch_id=job.batch_id or "", captured_at="2026-01-01T00:00:00+00:00")
    preparation = build_source_preparation(config, state)
    payload = AcquiredYouTube(
        VideoInput(
            video_id="abcDEF12345",
            title="Video",
            manual_transcript=(TranscriptSegment(12, 20, "Manual evidence"), TranscriptSegment(61, 73, "More evidence")),
        ),
        PublicationIntent(youtube_playlist_item_id="playlist-item"),
    )

    candidate = preparation.prepare(youtube_job, payload)

    assert not isinstance(candidate, PreparationFailure)
    assert candidate.source_identity.endswith(":v1")
    assert candidate.evidence_bounds.first_second == 12
    assert candidate.evidence_bounds.last_second == 73


def test_existing_source_path_is_an_explicit_collision(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    source_id = stable_id("article", job.source_key)
    (config.sources / "Articles" / f"{source_id}.md").write_text("occupied\n", encoding="utf-8")

    result = build_source_preparation(config, state).prepare(replace(job, status="processing", source_version=1), AcquiredArticle("<article><p>Evidence</p></article>", "http"))

    assert isinstance(result, PreparationFailure)
    assert result.category == PreparationFailureCategory.VERSION_COLLISION
    assert state.get(job.id).status == "failed"


def test_verified_v1_path_allocates_v2_without_overwrite(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    source_id = stable_id("article", job.source_key)
    existing = render_source(
        source_id=source_id,
        kind="article",
        canonical_url=job.original_locator,
        title="Existing",
        body="Previously committed evidence.",
        author=None,
        publication_date=None,
        captured_at=job.captured_at or "2026-01-01T00:00:00+00:00",
        input_method="http",
        source_version=1,
    )
    existing_path = config.sources / "Articles" / f"{source_id}.md"
    existing_path.write_text(render_markdown(existing), encoding="utf-8")

    candidate = build_source_preparation(config, state).prepare(job, AcquiredArticle("<article><p>New evidence</p></article>", "http"))

    assert not isinstance(candidate, PreparationFailure)
    assert candidate.source_version == 2
    assert candidate.relative_path == f"Sources/Articles/{source_id}-v2.md"
    assert existing_path.read_text(encoding="utf-8") == render_markdown(existing)


def test_corrupt_v1_path_does_not_get_skipped(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    source_id = stable_id("article", job.source_key)
    (config.sources / "Articles" / f"{source_id}.md").write_text("not source markdown\n", encoding="utf-8")

    result = build_source_preparation(config, state).prepare(job, AcquiredArticle("<article><p>Evidence</p></article>", "http"))

    assert isinstance(result, PreparationFailure)
    assert result.category == PreparationFailureCategory.VERSION_COLLISION


class _CrashAt:
    def __init__(self, event: str):
        self.event = event

    def hit(self, event: str, **_details):
        if event == self.event:
            raise PreparationCrash(event)


def test_restart_adopts_fully_installed_candidate_before_source_ready(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    payload = AcquiredArticle("<article><p>Evidence</p></article>", "http")
    crashing = build_source_preparation(config, state, faults=_CrashAt("candidate_installed"))

    try:
        crashing.prepare(job, payload)
    except PreparationCrash:
        pass
    else:
        raise AssertionError("expected preparation crash")

    assert state.get(job.id).status == "processing"
    state.recover()
    assert state.get(job.id).status == "claimed"
    recovered = build_source_preparation(config, state).prepare(state.get(job.id), payload)

    assert not isinstance(recovered, PreparationFailure)
    assert state.get(job.id).status == "source_ready"
    assert recovered.rendered_markdown == Path(recovered.artifact_path, "source.md").read_bytes()


def test_corrupt_candidate_is_reported_without_reacquisition(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    preparation = build_source_preparation(config, state)
    candidate = preparation.prepare(job, AcquiredArticle("<article><p>Evidence</p></article>", "http"))
    assert not isinstance(candidate, PreparationFailure)
    payload = Path(candidate.artifact_path, "source.md")
    payload.write_bytes(payload.read_bytes() + b"tampered")

    loaded = preparation.load(state.get(job.id))

    assert isinstance(loaded, PreparationFailure)
    assert loaded.category == PreparationFailureCategory.CANDIDATE_CORRUPT


def test_job_metadata_drift_invalidates_candidate(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    preparation = build_source_preparation(config, state)
    candidate = preparation.prepare(job, AcquiredArticle("<article><p>Evidence</p></article>", "http"))
    assert not isinstance(candidate, PreparationFailure)
    state.connection.execute("UPDATE jobs SET content_hash=? WHERE id=?", ("tampered", job.id))

    loaded = preparation.load(state.get(job.id))

    assert isinstance(loaded, PreparationFailure)
    assert loaded.category == PreparationFailureCategory.CANDIDATE_CORRUPT


def test_compaction_retains_manifest_and_identity_hash(tmp_path: Path):
    config, state, job = _setup(tmp_path)
    preparation = build_source_preparation(config, state)
    candidate = preparation.prepare(job, AcquiredArticle("<article><p>Evidence</p></article>", "http"))
    assert not isinstance(candidate, PreparationFailure)

    preparation.compact(job.id)

    assert Path(candidate.artifact_path, "manifest.json").is_file()
    assert not Path(candidate.artifact_path, "source.md").exists()
    assert state.source_candidate(job.id).lifecycle == "payload_removed"
