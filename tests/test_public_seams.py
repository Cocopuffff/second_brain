from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from second_brain.batch import BatchRunner
from second_brain.config import Config
from second_brain.models import CandidateFile, ChangeSet
from second_brain.provenance import article_citation
from second_brain.youtube import FixtureYouTubeClient


def git_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


def make_vault(tmp_path: Path) -> tuple[Path, Config]:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    git_repo(vault)
    config = Config.load(vault, tmp_path / "state")
    return vault, config


def test_queue_claim_is_canonical_and_preserves_blank_and_malformed_lines(tmp_path: Path):
    vault, config = make_vault(tmp_path)
    (vault / "To Ingest.md").write_text("\nhttps://Example.com/a?utm_source=x&keep=1#part\nnot a url\n", encoding="utf-8")
    runner = BatchRunner(config, fetcher=lambda _: "<html><title>A</title><article><p>Evidence.</p></article></html>")
    report = runner.run()
    assert report.committed
    queue = (vault / "To Ingest.md").read_text(encoding="utf-8")
    assert queue == "\nnot a url\n"
    source = next((vault / "Sources/Articles").glob("*.md"))
    assert "Evidence." in source.read_text(encoding="utf-8")


def test_saved_html_precedes_http_and_is_cleaned_after_commit(tmp_path: Path):
    vault, config = make_vault(tmp_path)
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    html = to_ingest / "saved.html"
    html.write_text('<html><head><link rel="canonical" href="https://example.com/article"></head><body><nav>Ad</nav><article><h1>Saved</h1><p>Saved evidence.</p><img src="chart.png" alt="A chart of evidence"></article></body></html>', encoding="utf-8")
    calls: list[str] = []
    runner = BatchRunner(config, fetcher=lambda url: calls.append(url) or "<article><p>Network evidence.</p></article>")
    report = runner.run()
    assert report.committed
    assert calls == []
    assert not html.exists()
    source = next((vault / "Sources/Articles").glob("*.md"))
    content = source.read_text(encoding="utf-8")
    assert "Saved evidence." in content
    assert "```image" in content
    assert "Ad" not in content


def test_unguarded_html_requires_declared_pairing(tmp_path: Path):
    vault, config = make_vault(tmp_path)
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    html = to_ingest / "article.html"
    html.write_text("<html><body><article><p>Evidence</p></article></body></html>", encoding="utf-8")
    report = BatchRunner(config).run()
    assert not report.committed
    assert html.exists()
    assert any("no canonical" in error for error in report.failures)


def test_untrusted_synthesis_write_is_rejected_without_commit(tmp_path: Path):
    vault, config = make_vault(tmp_path)
    (vault / "To Ingest.md").write_text("https://example.com/article\n", encoding="utf-8")

    class EscapeSynthesizer:
        def run(self, _batch_id, _sources):
            from second_brain.synthesis import ExecutorIdentity, SynthesisMetadata, SynthesisOutcome
            changes = ChangeSet(files=(CandidateFile("Sources/evil.md", "bad\n"),))
            return SynthesisOutcome(changes, SynthesisMetadata(("Sources/evil.md",), (), (), ExecutorIdentity("fixture")))

    report = BatchRunner(config, synthesis_runner=EscapeSynthesizer(), fetcher=lambda _: "<article><p>Evidence</p></article>").run()
    assert not report.committed
    assert not (vault / "Sources/evil.md").exists()
    assert not list((vault / "Sources/Articles").glob("*.md"))


def test_youtube_manual_transcript_is_preferred_and_cited(tmp_path: Path):
    vault, config = make_vault(tmp_path)
    fixture = tmp_path / "youtube.json"
    fixture.write_text('{"To Ingest": [{"video_id": "abcDEF12345", "title": "Video", "playlist_item_id": "playlist-item", "manual_transcript": [{"start": 12, "end": 20, "text": "Manual evidence"}], "automatic_transcript": [{"start": 12, "end": 20, "text": "Automatic evidence"}]}]}', encoding="utf-8")
    report = BatchRunner(config, youtube_client=FixtureYouTubeClient(fixture)).run()
    assert report.committed
    source = next((vault / "Sources/YouTube").glob("*.md"))
    content = source.read_text(encoding="utf-8")
    assert "Manual evidence" in content
    assert "Automatic evidence" not in content


def test_empty_batch_does_not_create_a_commit(tmp_path: Path):
    vault, config = make_vault(tmp_path)
    before = subprocess.run(["git", "rev-list", "--count", "HEAD"], cwd=vault, text=True, capture_output=True, check=True).stdout.strip()
    report = BatchRunner(config).run()
    after = subprocess.run(["git", "rev-list", "--count", "HEAD"], cwd=vault, text=True, capture_output=True, check=True).stdout.strip()
    assert not report.committed
    assert before == after


def test_dirty_unrelated_worktree_is_a_stop_condition(tmp_path: Path):
    vault, config = make_vault(tmp_path)
    (vault / "unrelated.md").write_text("do not stage me", encoding="utf-8")
    (vault / "To Ingest.md").write_text("https://example.com/article\n", encoding="utf-8")
    with pytest.raises(Exception, match="clean worktree"):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence</p></article>").run()
    assert not list((vault / "Sources/Articles").glob("*.md"))


def test_article_citation_uses_encoded_advanced_uri_and_one_indexed_line():
    citation = article_citation("A title", "Sources/Articles/article.md", 7, 11)
    assert citation == "[A title · L7-L11](obsidian://adv-uri?vault=Second%20Brain&filepath=Sources%2FArticles%2Farticle.md&line=7)"
