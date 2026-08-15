from __future__ import annotations

import subprocess
import json
from pathlib import Path

import pytest

from second_brain.batch import BatchError, BatchRunner
from second_brain.cli import main
from second_brain.config import Config
from second_brain.models import CandidateFile, ChangeSet
from second_brain.publication import PublicationCrash
from second_brain.state import StateStore


def _git_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


def _vault(tmp_path: Path, queue: str = "https://example.com/article\n") -> tuple[Path, Config]:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text(queue, encoding="utf-8")
    _git_repo(vault)
    return vault, Config.load(vault, tmp_path / "state")


def _commits(vault: Path) -> int:
    return int(subprocess.run(["git", "rev-list", "--count", "HEAD"], cwd=vault, text=True, capture_output=True, check=True).stdout)


class CrashAt:
    def __init__(self, event: str):
        self.event = event

    def hit(self, event: str, **_details):
        if event == self.event:
            raise PublicationCrash(event)


class ErrorAt:
    def __init__(self, event: str):
        self.event = event

    def hit(self, event: str, **_details):
        if event == self.event:
            raise RuntimeError(f"handled fault at {event}")


def test_successful_publication_reports_complete_and_one_batch_commit(tmp_path: Path, capsys):
    vault, config = _vault(tmp_path)
    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.committed
    assert report.publication_phase == "complete"
    assert report.commit_id
    assert _commits(vault) == 2
    assert f"Batch-ID: {report.batch_id}" in subprocess.run(["git", "show", "-s", "--format=%B", report.commit_id], cwd=vault, text=True, capture_output=True, check=True).stdout
    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["publications"][0]["phase"] == "complete"
    assert status["outstanding_cleanup"] == 0
    workspace = Path(status["publications"][0]["candidate_workspace"])
    assert (workspace / "manifest.json").is_file()
    assert (workspace / "rollback" / "manifest.json").is_file()
    assert not (workspace / "candidate").exists()
    assert not (workspace / "rollback" / "payload").exists()


def test_crash_during_publication_is_rolled_back_on_next_run(tmp_path: Path):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("file_published")).run()

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert not report.committed
    assert report.publication_phase == "rolled_back"
    assert _commits(vault) == 1
    assert (vault / "To Ingest.md").read_text(encoding="utf-8") == "https://example.com/article\n"
    assert not list((vault / "Sources").rglob("*.md"))


def test_handled_publication_failure_reconciles_before_returning(tmp_path: Path):
    vault, config = _vault(tmp_path)

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=ErrorAt("file_published")).run()

    assert report.publication_phase in {"rolled_back", "recovery_blocked"}
    assert report.publication_phase != "publishing"
    assert _commits(vault) == 1
    assert (vault / "To Ingest.md").read_text(encoding="utf-8") == "https://example.com/article\n"
    assert not list((vault / "Sources").rglob("*.md"))


@pytest.mark.parametrize("event", ["journal_written", "snapshot_captured", "published_uncommitted"])
def test_each_pre_commit_boundary_recovers_without_a_commit(tmp_path: Path, event: str):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt(event)).run()

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.publication_phase == "rolled_back"
    assert _commits(vault) == 1


@pytest.mark.parametrize("event", ["commit_journaled", "sqlite_finalized"])
def test_each_post_commit_boundary_is_idempotent(tmp_path: Path, event: str):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt(event)).run()

    BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert _commits(vault) == 2
    assert len(list((vault / "Sources").rglob("*.md"))) == 1


def test_crash_after_git_commit_adopts_existing_commit(tmp_path: Path):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("git_commit")).run()

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.committed
    assert report.recovery_action == "recovered_existing_commit"
    assert report.publication_phase == "complete"
    assert _commits(vault) == 2
    assert len(list((vault / "Sources").rglob("*.md"))) == 1


def test_status_recognizes_committed_but_unfinalized_publication(tmp_path: Path, capsys):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("git_commit")).run()

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    publication = status["publications"][0]
    assert publication["phase"] == "committed_unfinalized"
    assert publication["commit_id"] == subprocess.run(["git", "rev-parse", "HEAD"], cwd=vault, text=True, capture_output=True, check=True).stdout.strip()
    assert _commits(vault) == 2


def test_untracked_generated_input_is_a_stop_condition(tmp_path: Path):
    vault, config = _vault(tmp_path)
    (vault / "ToIngest").mkdir()
    (vault / "ToIngest/generated.txt").write_text("unexpected", encoding="utf-8")

    with pytest.raises(BatchError, match="clean worktree"):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()


@pytest.mark.parametrize("candidate_path", ["../escape.md", "/tmp/escape.md"])
def test_candidate_path_escape_is_rejected(tmp_path: Path, candidate_path: str):
    vault, config = _vault(tmp_path)

    class EscapeSynthesizer:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(files=(CandidateFile(candidate_path, "bad\n"),))

    report = BatchRunner(config, synthesizer=EscapeSynthesizer(), fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert not report.committed
    assert not (tmp_path / "escape.md").exists()


def test_duplicate_candidate_paths_are_rejected(tmp_path: Path):
    vault, config = _vault(tmp_path)

    class DuplicateSynthesizer:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(files=(CandidateFile("Concepts/a.md", "one\n"), CandidateFile("Concepts/a.md", "two\n")))

    report = BatchRunner(config, synthesizer=DuplicateSynthesizer(), fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert not report.committed


def test_changed_live_path_blocks_recovery_without_overwrite(tmp_path: Path):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("file_published")).run()
    source = next((vault / "Sources").rglob("*.md"))
    source.write_text("operator edit\n", encoding="utf-8")

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.publication_phase == "recovery_blocked"
    assert report.recovery_block_reason
    assert source.read_text(encoding="utf-8") == "operator edit\n"


def test_tampered_candidate_manifest_blocks_recovery(tmp_path: Path):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("file_published")).run()
    state = StateStore(config.database)
    try:
        journal = state.list_publications()[0]
    finally:
        state.close()
    manifest = Path(journal["candidate_workspace"]) / "manifest.json"
    contents = json.loads(manifest.read_text(encoding="utf-8"))
    contents["entries"].append({"path": "Concepts/tampered.md", "operation": "write", "candidate_hash": "bad"})
    manifest.write_text(json.dumps(contents) + "\n", encoding="utf-8")

    report = BatchRunner(config).run()

    assert report.publication_phase == "recovery_blocked"
    assert report.publication_failure_code == "manifest_invalid"


def test_deletion_is_committed_and_rollback_restores_deleted_file(tmp_path: Path):
    vault, config = _vault(tmp_path)
    (vault / "Concepts").mkdir()
    (vault / "Concepts/old.md").write_text("old\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=vault, check=True)
    subprocess.run(["git", "commit", "-qm", "concept fixture"], cwd=vault, check=True)

    class DeleteConcept:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(deletions=("Concepts/old.md",))

    with pytest.raises(PublicationCrash):
        BatchRunner(config, synthesizer=DeleteConcept(), fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("file_published")).run()
    recovery = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert recovery.publication_phase == "rolled_back"
    assert (vault / "Concepts/old.md").read_text(encoding="utf-8") == "old\n"


def test_source_deletion_requested_by_synthesis_is_rejected(tmp_path: Path):
    vault, config = _vault(tmp_path)
    (vault / "Sources/Articles").mkdir(parents=True)
    old_source = vault / "Sources/Articles/old.md"
    old_source.write_text("old source\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=vault, check=True)
    subprocess.run(["git", "commit", "-qm", "source fixture"], cwd=vault, check=True)

    class DeleteSource:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(deletions=("Sources/Articles/old.md",))

    report = BatchRunner(config, synthesizer=DeleteSource(), fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert not report.committed
    assert old_source.read_text(encoding="utf-8") == "old source\n"
    assert _commits(vault) == 2


@pytest.mark.parametrize("event", ["raw_payload_removed", "cleanup_marked"])
def test_cleanup_failures_retry_without_recommit(tmp_path: Path, event: str):
    vault, config = _vault(tmp_path, queue="")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    raw = to_ingest / "saved.html"
    raw.write_text('<html><head><link rel="canonical" href="https://example.com/saved"></head><body><article><p>Saved evidence.</p></article></body></html>', encoding="utf-8")
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "bad", _publication_faults=CrashAt(event)).run()
    before = _commits(vault)

    BatchRunner(config).run()

    assert _commits(vault) == before
    assert not raw.exists()


def test_cleanup_does_not_delete_payload_without_a_fingerprint(tmp_path: Path):
    vault, config = _vault(tmp_path, queue="")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    raw = to_ingest / "saved.html"
    raw.write_text('<html><head><link rel="canonical" href="https://example.com/saved"></head><body><article><p>Saved evidence.</p></article></body></html>', encoding="utf-8")
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "bad", _publication_faults=CrashAt("sqlite_finalized")).run()

    state = StateStore(config.database)
    try:
        batch_id = state.list_publications()[0]["batch_id"]
        state.connection.execute("UPDATE publication_jobs SET raw_hash=NULL WHERE batch_id=?", (batch_id,))
    finally:
        state.close()

    BatchRunner(config).run()

    assert raw.exists()
    state = StateStore(config.database)
    try:
        publication = state.publication(batch_id)
    finally:
        state.close()
    assert publication["phase"] == "cleanup_pending"
    assert publication["failure_code"] == "cleanup_payload_unverified"


def test_cleanup_recovery_is_reported_by_batch_run(tmp_path: Path):
    vault, config = _vault(tmp_path, queue="")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    raw = to_ingest / "saved.html"
    raw.write_text('<html><head><link rel="canonical" href="https://example.com/saved"></head><body><article><p>Saved evidence.</p></article></body></html>', encoding="utf-8")

    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "bad", _publication_faults=CrashAt("sqlite_finalized")).run()
    raw.write_text(raw.read_text(encoding="utf-8") + "changed", encoding="utf-8")

    report = BatchRunner(config).run()

    assert report.publication_phase == "cleanup_pending"
    assert report.recovery_action == "cleanup_pending"
    assert report.publication_failure_code == "cleanup_payload_changed"
    assert report.outstanding_cleanup == 1


def test_rebound_cleanup_stays_attached_to_original_publication(tmp_path: Path, capsys):
    vault, config = _vault(tmp_path, queue="")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    raw = to_ingest / "saved.html"
    raw.write_text('<html><head><link rel="canonical" href="https://example.com/saved"></head><body><article><p>Saved evidence.</p></article></body></html>', encoding="utf-8")

    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "bad", _publication_faults=CrashAt("sqlite_finalized")).run()

    raw.write_text(raw.read_text(encoding="utf-8") + "changed", encoding="utf-8")
    (vault / "To Ingest.md").write_text("https://example.com/saved\n", encoding="utf-8")

    BatchRunner(config).run()
    BatchRunner(config).run()

    assert _commits(vault) == 2
    assert raw.exists()
    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    saved_job = next(job for job in status["jobs"] if job["original_locator"] == "https://example.com/saved")
    publication = next(publication for publication in status["publications"] if publication["phase"] == "cleanup_pending")
    assert saved_job["cleanup_pending"] is True
    assert publication["phase"] == "cleanup_pending"
