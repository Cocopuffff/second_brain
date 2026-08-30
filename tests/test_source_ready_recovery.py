from __future__ import annotations

import subprocess
from pathlib import Path

from second_brain.batch import BatchRunner
from second_brain.config import Config
from second_brain.models import ChangeSet
from second_brain.synthesis import ExecutorIdentity, FailureCategory, SynthesisFailure, SynthesisMetadata, SynthesisOutcome
from second_brain.state import StateStore
from second_brain.youtube import FixtureYouTubeClient


def _git_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


def _vault(tmp_path: Path, queue: str) -> tuple[Path, Config]:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text(queue, encoding="utf-8")
    _git_repo(vault)
    return vault, Config.load(vault, tmp_path / "state")


class _ToggleSynthesis:
    def __init__(self):
        self.fail = True
        self.calls = 0

    def run(self, _batch_id, _sources):
        self.calls += 1
        if self.fail:
            return SynthesisFailure(FailureCategory.ADAPTER_EXECUTION_FAILURE, "fixture synthesis failed", ExecutorIdentity("fixture"))
        return SynthesisOutcome(ChangeSet(), SynthesisMetadata((), (), (), ExecutorIdentity("fixture")))


def test_source_ready_retry_uses_exact_candidate_without_fetching_again(tmp_path: Path):
    vault, config = _vault(tmp_path, "https://example.com/article\n")
    calls: list[str] = []
    synthesis = _ToggleSynthesis()

    def fetch(url: str) -> str:
        calls.append(url)
        return "<article><p>Exact prepared evidence.</p></article>"

    first = BatchRunner(config, synthesis_runner=synthesis, fetcher=fetch).run()

    assert not first.committed
    assert calls == ["https://example.com/article"]
    state = StateStore(config.database)
    try:
        ready = state.source_ready_jobs()
        assert len(ready) == 1
        candidate = state.source_candidate(ready[0].id)
        assert candidate is not None
        prepared_bytes = Path(candidate.artifact_path, "source.md").read_bytes()
    finally:
        state.close()

    synthesis.fail = False
    second = BatchRunner(config, synthesis_runner=synthesis, fetcher=lambda _url: (_ for _ in ()).throw(AssertionError("source-ready retry fetched input"))).run()

    assert second.committed
    assert calls == ["https://example.com/article"]
    published = next((vault / "Sources").rglob("*.md"))
    assert published.read_bytes() == prepared_bytes
    assert synthesis.calls == 2


def test_source_ready_retry_does_not_recompute_input_allowlist(tmp_path: Path):
    vault, config = _vault(tmp_path, "https://example.com/article\n")
    synthesis = _ToggleSynthesis()
    first = BatchRunner(config, synthesis_runner=synthesis, fetcher=lambda _url: "<article><p>Evidence.</p></article>").run()
    assert not first.committed
    synthesis.fail = False

    class NoDiscoveryRunner(BatchRunner):
        def _allowed_input_paths(self):
            raise AssertionError("source-ready retry recomputed input discovery")

    second = NoDiscoveryRunner(config, synthesis_runner=synthesis, fetcher=lambda _url: (_ for _ in ()).throw(AssertionError("fetched on retry"))).run()

    assert second.committed


class _NoDiscoveryClient:
    def __init__(self, fixture: Path):
        self.fixture = fixture
        self.calls = 0

    def list_playlist(self, _playlist):
        self.calls += 1
        raise AssertionError("source-ready retry rediscovered YouTube input")

    def acknowledge(self, _item):
        raise AssertionError("SEC-7 must not acknowledge before publication finalization")


def test_source_ready_youtube_retry_does_not_list_playlist(tmp_path: Path):
    vault, config = _vault(tmp_path, "")
    fixture = tmp_path / "youtube.json"
    fixture.write_text('{"To Ingest": [{"video_id": "abcDEF12345", "title": "Video", "manual_transcript": [{"start": 12, "end": 20, "text": "Evidence"}]}]}', encoding="utf-8")
    synthesis = _ToggleSynthesis()
    first_client = FixtureYouTubeClient(fixture)

    first = BatchRunner(config, synthesis_runner=synthesis, youtube_client=first_client).run()

    assert not first.committed
    assert first_client.acknowledged == []
    synthesis.fail = False
    second_client = _NoDiscoveryClient(fixture)
    second = BatchRunner(config, synthesis_runner=synthesis, youtube_client=second_client).run()

    assert second.committed
    assert second_client.calls == 0


def test_preparation_failure_does_not_publish_or_mark_source_ready(tmp_path: Path):
    vault, config = _vault(tmp_path, "https://example.com/article\n")
    before = subprocess.run(["git", "rev-list", "--count", "HEAD"], cwd=vault, text=True, capture_output=True, check=True).stdout.strip()

    class UnreliableImages:
        def describe(self, **_kwargs):
            return None

    report = BatchRunner(
        config,
        image_processor=UnreliableImages(),
        fetcher=lambda _url: '<article><p>Evidence.</p><img src="chart.png" alt=""></article>',
    ).run()

    assert not report.committed
    assert subprocess.run(["git", "rev-list", "--count", "HEAD"], cwd=vault, text=True, capture_output=True, check=True).stdout.strip() == before
    assert not list((vault / "Sources").rglob("*.md"))
    state = StateStore(config.database)
    try:
        assert state.source_ready_jobs() == []
        assert state.list_jobs()[0].status == "failed"
    finally:
        state.close()


def test_legacy_source_ready_without_candidate_fails_closed(tmp_path: Path):
    vault, config = _vault(tmp_path, "https://example.com/article\n")
    state = StateStore(config.database)
    batch_id = state.create_batch()
    job = state.claim("article", "article:https://example.com/article", "https://example.com/article", input_artifact=None, batch_id=batch_id)
    state.complete(job.id, "legacy-hash", 1)
    state.close()

    report = BatchRunner(config, fetcher=lambda _url: (_ for _ in ()).throw(AssertionError("legacy source-ready job was reacquired"))).run()

    assert not report.committed
    state = StateStore(config.database)
    try:
        legacy = state.find("article:https://example.com/article")
        assert legacy is not None
        assert legacy.status == "failed"
        assert legacy.failure_code == "candidate_missing"
    finally:
        state.close()
