from __future__ import annotations

import sys
from pathlib import Path

import pytest

from second_brain.controlled_synthesis import (
    AdapterResult,
    CodexWorkspaceAdapter,
    ControlledSynthesis,
    ProvenanceDeclaration,
    SynthesisFailure,
    codex_sandbox_available,
    synthesize_controlled,
)
from second_brain.models import CandidateFile
from second_brain.provenance import article_citation
from second_brain.render import render_source


def source():
    return render_source(
        source_id="article-one", kind="article", canonical_url="https://example.com/one",
        title="One", body="Evidence.", author=None, publication_date=None,
        captured_at="2026-01-01T00:00:00+00:00", input_method="http", source_version=1,
    )


def test_direct_adapter_only_gets_narrow_reads_and_returns_deterministic_result(tmp_path: Path):
    evidence = source()
    calls: list[str] = []

    def adapter(capabilities):
        assert capabilities.list_concepts() == ()
        assert capabilities.search_concepts("missing") == ()
        assert capabilities.read_source("article-one:v1") == evidence
        calls.append("read")
        return AdapterResult(
            writes=(CandidateFile("Concepts/one.md", f"Interpretation\n\n{article_citation('One', evidence.relative_path, 1, 2)}\n"),),
            provenance=(ProvenanceDeclaration("Concepts/one.md", 0, "article-one:v1"),),
            metadata={"api_key": "must-not-journal", "model": "fixture"},
        )

    result = synthesize_controlled(adapter, [evidence], {}, tmp_path / "candidate")
    assert not isinstance(result, SynthesisFailure)
    assert calls == ["read"]
    assert result.affected_paths == ("Concepts/one.md",)
    assert result.cited_source_versions == ("article-one:v1",)
    assert "api_key" not in result.executor


def test_path_collision_rejects_entire_result(tmp_path: Path):
    result = synthesize_controlled(
        lambda _capabilities: AdapterResult(
            writes=(CandidateFile("Concepts/Café.md", "a\n"), CandidateFile("Concepts/Café.md", "b\n")),
        ),
        [source()], {}, tmp_path / "candidate",
    )
    assert isinstance(result, SynthesisFailure)
    assert result.category == "path_collision"
    assert result.change_set is None


@pytest.mark.skipif(not codex_sandbox_available(), reason="host does not permit the Codex filesystem sandbox")
def test_codex_result_comes_from_actual_workspace_diff(tmp_path: Path):
    command = [sys.executable, "-c", "from pathlib import Path; Path('Concepts/new.md').write_text('# New\\n')"]
    result = ControlledSynthesis(CodexWorkspaceAdapter(command)).run([source()], {}, tmp_path / "candidate")
    assert not isinstance(result, SynthesisFailure)
    assert result.change_set.files == (CandidateFile("Concepts/new.md", "# New\n"),)


@pytest.mark.skipif(not codex_sandbox_available(), reason="host does not permit the Codex filesystem sandbox")
def test_codex_workspace_contamination_is_rejected(tmp_path: Path):
    command = [sys.executable, "-c", "from pathlib import Path; Path('outside.txt').write_text('nope')"]
    result = ControlledSynthesis(CodexWorkspaceAdapter(command)).run([source()], {}, tmp_path / "candidate")
    assert isinstance(result, SynthesisFailure)
    assert result.category == "workspace_contamination"


def test_catalog_seed_rejects_traversal_before_writing(tmp_path: Path):
    escaped = tmp_path / "escape.md"
    result = ControlledSynthesis(CodexWorkspaceAdapter([sys.executable, "-c", "pass"])).run(
        [source()], {"Concepts/../../escape.md": "bad\n"}, tmp_path / "candidate"
    )

    assert isinstance(result, SynthesisFailure)
    assert result.category == "invalid_operation"
    assert not escaped.exists()


@pytest.mark.skipif(not codex_sandbox_available(), reason="host does not permit the Codex filesystem sandbox")
def test_codex_baseline_seed_is_not_reported_as_a_write(tmp_path: Path):
    result = ControlledSynthesis(CodexWorkspaceAdapter([sys.executable, "-c", "pass"])).run(
        [source()], {"Concepts/existing.md": "existing\n"}, tmp_path / "candidate"
    )

    assert not isinstance(result, SynthesisFailure)
    assert result.change_set.files == ()


def test_raw_dot_and_repeated_separator_paths_are_rejected(tmp_path: Path):
    for path in ("Concepts/./one.md", "Concepts//one.md"):
        result = synthesize_controlled(
            lambda _capabilities, path=path: AdapterResult(writes=(CandidateFile(path, "one\n"),)),
            [source()], {}, tmp_path / path.replace("/", "_")
        )
        assert isinstance(result, SynthesisFailure)
        assert result.category == "invalid_operation"


def test_undeclared_citation_is_rejected(tmp_path: Path):
    evidence = source()
    result = synthesize_controlled(
        lambda _capabilities: AdapterResult(
            writes=(CandidateFile("Concepts/one.md", f"Interpretation\n\n{article_citation('One', evidence.relative_path, 1, 2)}\n"),),
        ),
        [evidence], {}, tmp_path / "candidate"
    )

    assert isinstance(result, SynthesisFailure)
    assert result.category == "invalid_provenance"


def test_versionless_provenance_is_rejected(tmp_path: Path):
    evidence = source()
    result = synthesize_controlled(
        lambda _capabilities: AdapterResult(
            writes=(CandidateFile("Concepts/one.md", f"Interpretation\n\n{article_citation('One', evidence.relative_path, 1, 2)}\n"),),
            provenance=(ProvenanceDeclaration("Concepts/one.md", 0, evidence.source_id),),
        ),
        [evidence], {}, tmp_path / "candidate"
    )

    assert isinstance(result, SynthesisFailure)
    assert result.category == "invalid_provenance"


@pytest.mark.skipif(not codex_sandbox_available(), reason="host does not permit the Codex filesystem sandbox")
def test_codex_cannot_write_outside_candidate_workspace(tmp_path: Path):
    escaped = tmp_path / "outside.txt"
    script = (
        "from pathlib import Path; "
        f"p=Path({str(escaped)!r}); "
        "\ntry: p.write_text('nope')\nexcept OSError: pass"
    )
    result = ControlledSynthesis(CodexWorkspaceAdapter([sys.executable, "-c", script])).run(
        [source()], {}, tmp_path / "candidate"
    )

    assert not isinstance(result, SynthesisFailure)
    assert not escaped.exists()
