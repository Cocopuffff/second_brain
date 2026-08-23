from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from second_brain.batch import BatchError, BatchRunner
from second_brain.cli import main
from second_brain.controlled_synthesis import ControlledSynthesis
from second_brain.config import Config
from second_brain.models import CandidateFile, ChangeSet
from second_brain.publication import PublicationCrash


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
    def __init__(self, event: str, occurrence: int = 1):
        self.event = event
        self.occurrence = occurrence
        self.seen = 0
        self.details: dict = {}

    def hit(self, event: str, **details):
        if event == self.event:
            self.seen += 1
            if self.seen == self.occurrence:
                self.details = details
                raise PublicationCrash(event)


class ErrorAt:
    def __init__(self, event: str):
        self.event = event

    def hit(self, event: str, **_details):
        if event == self.event:
            raise RuntimeError(f"handled fault at {event}")


def _publication_workspace(config: Config) -> Path:
    workspaces = list((config.state_dir / "publications").iterdir())
    assert len(workspaces) == 1
    return workspaces[0]


def _saved_html(vault: Path, name: str, url: str, body: str = "Saved evidence.") -> Path:
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir(exist_ok=True)
    raw = to_ingest / name
    raw.write_text(f'<html><head><link rel="canonical" href="{url}"></head><body><article><p>{body}</p></article></body></html>', encoding="utf-8")
    return raw


def _status(vault: Path, config: Config, capsys) -> dict:
    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "status"]) == 0
    return json.loads(capsys.readouterr().out)


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


def test_successful_publication_commits_exact_validated_paths(tmp_path: Path):
    vault, config = _vault(tmp_path, queue="https://example.com/saved\n")
    raw = _saved_html(vault, "saved.html", "https://example.com/saved")
    concepts = vault / "Concepts"
    concepts.mkdir()
    removed = concepts / "removed.md"
    removed.write_text("remove me\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=vault, check=True)
    subprocess.run(["git", "commit", "-qm", "concept fixture"], cwd=vault, check=True)

    class ConceptChanges:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(files=(CandidateFile("Concepts/new.md", "new concept\n"),), deletions=("Concepts/removed.md",))

    report = BatchRunner(config, synthesizer=ConceptChanges()).run()

    assert report.commit_id
    source = next((vault / "Sources").rglob("*.md"))
    committed_paths = set(subprocess.run(["git", "diff-tree", "--no-commit-id", "--name-only", "-r", report.commit_id], cwd=vault, text=True, capture_output=True, check=True).stdout.splitlines())
    assert committed_paths == {"Concepts/new.md", "Concepts/removed.md", str(source.relative_to(vault)), "To Ingest.md"}
    assert f"Batch-ID: {report.batch_id}" in subprocess.run(["git", "show", "-s", "--format=%B", report.commit_id], cwd=vault, text=True, capture_output=True, check=True).stdout
    assert not raw.exists()


def test_synthesis_failure_prevents_publication(tmp_path: Path):
    vault, config = _vault(tmp_path, queue="")
    raw = _saved_html(vault, "saved.html", "https://example.com/saved")

    class FailedSynthesis:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            raise RuntimeError("fixture synthesis failure")

    report = BatchRunner(config, synthesizer=FailedSynthesis()).run()

    assert not report.committed
    assert _commits(vault) == 1
    assert raw.exists()
    assert not list((vault / "Sources").rglob("*.md"))
    assert not list((vault / "Concepts").rglob("*.md"))


def test_controlled_outcome_reaches_publication_with_validated_metadata(tmp_path: Path):
    vault, config = _vault(tmp_path)

    controlled = ControlledSynthesis(
        lambda _capabilities: {"writes": [{"path": "Concepts/controlled.md", "content": "# New\n"}], "metadata": {"model": "fixture"}},
        executor={"kind": "fixture", "version": "1"},
    )
    report = BatchRunner(config, synthesizer=controlled, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.committed
    workspace = _publication_workspace(config)
    manifest = json.loads((workspace / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["synthesis"]["executor"]["kind"] == "fixture"
    assert manifest["synthesis"]["executor"]["model"] == "fixture"


def test_controlled_failure_returns_without_publication(tmp_path: Path):
    vault, config = _vault(tmp_path)
    controlled = ControlledSynthesis(
        lambda _capabilities: {"writes": [{"path": "Sources/escape.md", "content": "bad\n"}]},
        executor={"kind": "fixture", "version": "1"},
    )
    report = BatchRunner(config, synthesizer=controlled, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert not report.committed
    assert any("out_of_scope_change" in failure for failure in report.failures)
    assert not list((vault / "Sources").rglob("*.md"))


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


def test_rollback_then_retry_produces_one_eventual_commit(tmp_path: Path):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("file_published")).run()

    recovery = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()
    retry = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()
    repeated = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert recovery.publication_phase == "rolled_back"
    assert retry.committed
    assert not repeated.committed
    assert _commits(vault) == 2


def test_handled_publication_failure_reconciles_before_returning(tmp_path: Path, capsys):
    vault, config = _vault(tmp_path)

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=ErrorAt("file_published")).run()

    assert report.publication_phase in {"rolled_back", "recovery_blocked"}
    assert report.publication_phase != "publishing"
    assert report.publication_failure_code == "publication_failed"
    assert report.publication_failure_message == "handled fault at file_published"
    assert _commits(vault) == 1
    assert (vault / "To Ingest.md").read_text(encoding="utf-8") == "https://example.com/article\n"
    assert not list((vault / "Sources").rglob("*.md"))
    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    publication = next(item for item in status["publications"] if item["batch_id"] == report.batch_id)
    assert publication["failure_code"] == "publication_failed"
    assert publication["failure_message"] == "handled fault at file_published"


@pytest.mark.parametrize("event", ["journal_written", "snapshot_captured", "published_uncommitted"])
def test_each_pre_commit_boundary_recovers_without_a_commit(tmp_path: Path, event: str):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt(event)).run()

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.publication_phase == "rolled_back"
    assert _commits(vault) == 1


@pytest.mark.parametrize(
    ("event", "committed_after_recovery"),
    [
        ("journal_written", False),
        ("snapshot_captured", False),
        ("published_uncommitted", False),
        ("git_commit", True),
        ("commit_journaled", True),
    ],
)
def test_raw_html_is_retained_through_every_pre_finalization_boundary(tmp_path: Path, event: str, committed_after_recovery: bool):
    vault, config = _vault(tmp_path, queue="")
    raw = _saved_html(vault, "saved.html", "https://example.com/saved")

    with pytest.raises(PublicationCrash):
        BatchRunner(config, _publication_faults=CrashAt(event)).run()

    assert raw.exists()
    before_recovery = _commits(vault)
    report = BatchRunner(config).run()

    assert report.committed is committed_after_recovery
    assert _commits(vault) == before_recovery
    assert raw.exists() is (not committed_after_recovery)


@pytest.mark.parametrize("occurrence", range(1, 6))
def test_each_file_publication_boundary_restores_a_mixed_candidate(tmp_path: Path, capsys, occurrence: int):
    vault, config = _vault(tmp_path, queue="https://example.com/saved\n")
    raw = _saved_html(vault, "saved.html", "https://example.com/saved")
    concepts = vault / "Concepts"
    concepts.mkdir()
    deleted = concepts / "deleted.md"
    overwritten = concepts / "overwritten.md"
    deleted.write_text("delete before\n", encoding="utf-8")
    overwritten.write_text("overwrite before\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=vault, check=True)
    subprocess.run(["git", "commit", "-qm", "concept fixtures"], cwd=vault, check=True)

    class MixedConcepts:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(
                files=(CandidateFile("Concepts/new.md", "new\n"), CandidateFile("Concepts/overwritten.md", "overwrite after\n")),
                deletions=("Concepts/deleted.md",),
            )

    with pytest.raises(PublicationCrash):
        BatchRunner(config, synthesizer=MixedConcepts(), _publication_faults=CrashAt("file_published", occurrence)).run()

    assert raw.exists()
    report = BatchRunner(config).run()

    assert report.publication_phase == "rolled_back"
    assert report.recovery_action == "rolled_back_uncommitted"
    assert _commits(vault) == 2
    assert deleted.read_text(encoding="utf-8") == "delete before\n"
    assert overwritten.read_text(encoding="utf-8") == "overwrite before\n"
    assert not (concepts / "new.md").exists()
    assert not list((vault / "Sources").rglob("*.md"))
    assert (vault / "To Ingest.md").read_text(encoding="utf-8") == "https://example.com/saved\n"
    assert raw.exists()
    publication = next(item for item in _status(vault, config, capsys)["publications"] if item["batch_id"] == report.batch_id)
    assert publication["phase"] == "rolled_back"


@pytest.mark.parametrize("event", ["commit_journaled", "sqlite_finalized"])
def test_each_post_commit_boundary_is_idempotent(tmp_path: Path, event: str):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt(event)).run()

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()
    repeated = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.committed
    assert not repeated.committed
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


def test_status_recognizes_finalized_before_cleanup(tmp_path: Path, capsys):
    vault, config = _vault(tmp_path, queue="")
    raw = _saved_html(vault, "saved.html", "https://example.com/saved")

    with pytest.raises(PublicationCrash):
        BatchRunner(config, _publication_faults=CrashAt("sqlite_finalized")).run()

    publication = _status(vault, config, capsys)["publications"][0]
    assert publication["phase"] == "finalized"
    assert publication["commit_id"]
    assert raw.exists()


def test_fixture_dry_run_is_non_consuming_and_partial_batch_commits_once(tmp_path: Path, capsys):
    vault, config = _vault(tmp_path, queue="https://example.com/good\nhttps://example.com/bad\n")
    before_queue = (vault / "To Ingest.md").read_text(encoding="utf-8")
    before_commits = _commits(vault)

    assert main(["--vault", str(vault), "--state-dir", str(config.state_dir), "dry-run"]) == 0
    preview = json.loads(capsys.readouterr().out)
    assert (vault / "To Ingest.md").read_text(encoding="utf-8") == before_queue
    assert _commits(vault) == before_commits

    def fetch(url: str) -> str:
        if url.endswith("/bad"):
            raise RuntimeError("fixture fetch failure")
        return "<article><p>Good evidence.</p></article>"

    report = BatchRunner(config, fetcher=fetch).run()

    assert preview == {"queue_urls": 2, "html_files": 0, "errors": []}
    assert (vault / "To Ingest.md").read_text(encoding="utf-8") != before_queue
    assert report.committed
    assert report.completed == 1
    assert report.failed == 1
    assert _commits(vault) == before_commits + 1


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


def test_candidate_payload_symlink_blocks_recovery(tmp_path: Path):
    vault, config = _vault(tmp_path, queue="")
    to_ingest = vault / "ToIngest"
    to_ingest.mkdir()
    raw = to_ingest / "saved.html"
    raw.write_text('<html><head><link rel="canonical" href="https://example.com/saved"></head><body><article><p>Saved evidence.</p></article></body></html>', encoding="utf-8")
    fault = CrashAt("journal_written")

    with pytest.raises(PublicationCrash):
        BatchRunner(config, _publication_faults=fault).run()

    workspace = _publication_workspace(config)
    payload = workspace / "candidate"
    candidate = next(path for path in payload.rglob("*") if path.is_file())
    alias_target = payload / ".alias-target"
    alias_target.write_bytes(candidate.read_bytes())
    candidate.unlink()
    candidate.symlink_to(alias_target)

    report = BatchRunner(config).run()

    assert report.publication_phase == "recovery_blocked"
    assert report.publication_failure_code == "symlink_escape"
    assert raw.exists()
    assert not list((vault / "Sources").rglob("*.md"))


def test_live_vault_symlink_blocks_publication(tmp_path: Path):
    vault, config = _vault(tmp_path)
    elsewhere = vault / "Elsewhere"
    elsewhere.mkdir()
    concepts = vault / "Concepts"
    concepts.mkdir()
    (concepts / "linked").symlink_to(elsewhere, target_is_directory=True)
    subprocess.run(["git", "add", "."], cwd=vault, check=True)
    subprocess.run(["git", "commit", "-qm", "symlink fixture"], cwd=vault, check=True)

    class LinkedConcept:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(files=(CandidateFile("Concepts/linked/new.md", "unsafe\n"),))

    report = BatchRunner(config, synthesizer=LinkedConcept(), fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert not report.committed
    assert report.publication_failure_code == "symlink_escape"
    assert not (elsewhere / "new.md").exists()


def test_rollback_payload_symlink_blocks_recovery(tmp_path: Path):
    vault, config = _vault(tmp_path)
    concepts = vault / "Concepts"
    concepts.mkdir()
    existing = concepts / "existing.md"
    existing.write_text("before\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=vault, check=True)
    subprocess.run(["git", "commit", "-qm", "concept fixture"], cwd=vault, check=True)

    class OverwriteConcept:
        def synthesize(self, batch_sources, concept_catalog, workspace):
            return ChangeSet(files=(CandidateFile("Concepts/existing.md", "after\n"),))

    with pytest.raises(PublicationCrash):
        BatchRunner(config, synthesizer=OverwriteConcept(), fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("snapshot_captured")).run()

    workspace = _publication_workspace(config)
    payload = workspace / "rollback" / "payload"
    snapshot = payload / "Concepts" / "existing.md"
    alias_target = payload / ".alias-target"
    alias_target.write_bytes(snapshot.read_bytes())
    snapshot.unlink()
    snapshot.symlink_to(alias_target)

    report = BatchRunner(config).run()

    assert report.publication_phase == "recovery_blocked"
    assert report.publication_failure_code == "symlink_escape"
    assert existing.read_text(encoding="utf-8") == "before\n"


def test_batch_commit_with_mismatched_tree_blocks_recovery(tmp_path: Path):
    vault, config = _vault(tmp_path)
    fault = CrashAt("published_uncommitted")

    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=fault).run()

    source = next((vault / "Sources").rglob("*.md"))
    source.write_text("tampered committed content\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=vault, check=True)
    subprocess.run(["git", "commit", "-qm", f"wrong tree\n\nBatch-ID: {fault.details['batch_id']}"], cwd=vault, check=True)

    report = BatchRunner(config).run()

    assert report.publication_phase == "recovery_blocked"
    assert report.publication_failure_code == "publication_commit_mismatch"
    assert _commits(vault) == 2


def test_orphaned_journaled_commit_blocks_recovery(tmp_path: Path):
    vault, config = _vault(tmp_path)

    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("commit_journaled")).run()

    subprocess.run(["git", "commit", "--amend", "-qm", "operator amended commit"], cwd=vault, check=True)
    amended_head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=vault, text=True, capture_output=True, check=True).stdout.strip()

    report = BatchRunner(config).run()

    assert report.publication_phase == "recovery_blocked"
    assert report.publication_failure_code == "publication_commit_mismatch"
    assert report.commit_id != amended_head


def test_changed_live_path_blocks_recovery_without_overwrite(tmp_path: Path, capsys):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("file_published")).run()
    source = next((vault / "Sources").rglob("*.md"))
    source.write_text("operator edit\n", encoding="utf-8")

    report = BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>").run()

    assert report.publication_phase == "recovery_blocked"
    assert report.recovery_block_reason
    assert source.read_text(encoding="utf-8") == "operator edit\n"
    publication = next(item for item in _status(vault, config, capsys)["publications"] if item["batch_id"] == report.batch_id)
    assert publication["phase"] == "recovery_blocked"
    assert publication["recovery_block_reason"] == report.recovery_block_reason


def test_tampered_candidate_manifest_blocks_recovery(tmp_path: Path):
    vault, config = _vault(tmp_path)
    with pytest.raises(PublicationCrash):
        BatchRunner(config, fetcher=lambda _: "<article><p>Evidence.</p></article>", _publication_faults=CrashAt("file_published")).run()
    manifest = _publication_workspace(config) / "manifest.json"
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


def test_raw_cleanup_symlink_is_rejected_without_deleting_target(tmp_path: Path):
    vault, config = _vault(tmp_path, queue="")
    raw = _saved_html(vault, "saved.html", "https://example.com/saved")
    with pytest.raises(PublicationCrash):
        BatchRunner(config, _publication_faults=CrashAt("sqlite_finalized")).run()
    target = raw.with_name("target.html")
    target.write_bytes(raw.read_bytes())
    raw.unlink()
    raw.symlink_to(target)

    report = BatchRunner(config).run()

    assert report.publication_phase == "cleanup_pending"
    assert report.publication_failure_code == "symlink_escape"
    assert raw.is_symlink()
    assert target.exists()


def test_multiple_raw_payloads_recover_without_recommit_or_resynthesis(tmp_path: Path):
    vault, config = _vault(tmp_path, queue="")
    first_raw = _saved_html(vault, "first.html", "https://example.com/first", "First evidence.")
    second_raw = _saved_html(vault, "second.html", "https://example.com/second", "Second evidence.")

    class CountingSynthesis:
        calls = 0

        def synthesize(self, batch_sources, concept_catalog, workspace):
            type(self).calls += 1
            return ChangeSet()

    with pytest.raises(PublicationCrash):
        BatchRunner(config, synthesizer=CountingSynthesis(), _publication_faults=CrashAt("raw_payload_removed")).run()

    before = _commits(vault)
    report = BatchRunner(config, synthesizer=CountingSynthesis()).run()

    assert report.publication_phase == "complete"
    assert report.outstanding_cleanup == 0
    assert _commits(vault) == before
    assert CountingSynthesis.calls == 1
    assert not first_raw.exists()
    assert not second_raw.exists()


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
