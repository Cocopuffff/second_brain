from __future__ import annotations

import json
import stat
from pathlib import Path

from second_brain.config import Config
from second_brain.models import SourceDocument
from second_brain.synthesis import (
    ExecutorIdentity,
    FailureCategory,
    SynthesisFailure,
    SynthesisOutcome,
    SynthesisRunner,
    build_controlled_synthesis,
)
from second_brain.synthesis.adapters import DeepSeekAdapter


def _source() -> SourceDocument:
    return SourceDocument(
        source_id="article-one", kind="article", canonical_url="https://example.com/one",
        title="One", content="Evidence.\n", metadata={}, relative_path="Sources/Articles/article-one.md",
        content_hash="hash", source_version=1,
    )


def _config(vault: Path, state: Path, **values) -> Config:
    return Config(vault=vault, state_dir=state, **values)


def test_public_runner_returns_typed_noop_outcome_and_full_catalog(tmp_path: Path):
    vault = tmp_path / "vault"
    (vault / "Concepts").mkdir(parents=True)
    long_body = "# Existing\n\n" + ("x" * 20_000) + "\n"
    (vault / "Concepts" / "existing.md").write_text(long_body, encoding="utf-8")
    runner = build_controlled_synthesis(_config(vault, tmp_path / "state"))

    result = runner.run("batch-one", [_source()])

    assert isinstance(result, SynthesisOutcome)
    assert result.change_set.files == ()
    assert result.metadata.executor.kind == "noop"
    assert (tmp_path / "state" / "staging" / "batch-one").exists()


def test_config_rejects_removed_codex_command(tmp_path: Path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"executor": "codex", "codex_command": ["old"]}), encoding="utf-8")
    config = Config.load(tmp_path / "vault", tmp_path / "state", config_path)

    assert any("codex_command was removed" in error for error in config.validate())


def test_codex_native_cli_uses_baseline_diff_and_fixed_safety_flags(tmp_path: Path):
    vault = tmp_path / "vault"
    concepts = vault / "Concepts"
    concepts.mkdir(parents=True)
    baseline = "# Existing\n\n" + ("long " * 3_000) + "\n"
    (concepts / "existing.md").write_text(baseline, encoding="utf-8")
    executable = tmp_path / "fake-codex"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json, pathlib, sys\n"
        "if '--help' in sys.argv:\n"
        " print(' --sandbox --ephemeral --ignore-user-config --ignore-rules --strict-config --output-schema')\n"
        " raise SystemExit(0)\n"
        "pathlib.Path('Concepts/new.md').write_text('# New\\n\\n')\n"
        "print(json.dumps({'writes': [{'path': 'Concepts/new.md', 'content': '# New\\n\\n'}]}))\n",
        encoding="utf-8",
    )
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    config = _config(vault, tmp_path / "state", executor="codex", codex_executable=str(executable))

    result = build_controlled_synthesis(config).run("batch-codex", [_source()])

    assert isinstance(result, SynthesisOutcome)
    assert [item.relative_path for item in result.change_set.files] == ["Concepts/new.md"]
    assert result.change_set.files[0].content == "# New\n\n"


def test_failure_message_is_redacted(tmp_path: Path):
    class LeakingAdapter:
        def execute(self, _capabilities):
            raise RuntimeError("secret-token body=private evidence")

    config = _config(tmp_path / "vault", tmp_path / "state")
    runner = SynthesisRunner(config, LeakingAdapter(), ExecutorIdentity("fixture"))

    result = runner.run("batch-failure", [_source()])

    assert isinstance(result, SynthesisFailure)
    assert result.category == FailureCategory.ADAPTER_EXECUTION_FAILURE
    assert "secret-token" not in result.safe_message
    assert "private evidence" not in result.safe_message


def test_deepseek_tool_loop_starts_without_evidence_bodies(tmp_path: Path):
    adapter = DeepSeekAdapter("key", "https://example.invalid", "explicit-model", 5)
    requests: list[dict] = []

    def fake_request(messages, tools):
        requests.append({"messages": json.loads(json.dumps(messages)), "tools": tools})
        if len(requests) == 1:
            return {"choices": [{"message": {"role": "assistant", "tool_calls": [{"id": "call-1", "type": "function", "function": {"name": "read_source", "arguments": '{"source_version":"article-one:v1"}'}}]}}]}
        return {"choices": [{"message": {"role": "assistant", "content": '{"writes":[],"deletions":[],"provenance":[],"metadata":{}}'}}]}

    adapter._request = fake_request
    runner = SynthesisRunner(_config(tmp_path / "vault", tmp_path / "state"), adapter, ExecutorIdentity("deepseek", "explicit-model"))
    result = runner.run("deepseek-batch", [_source()])

    assert isinstance(result, SynthesisOutcome)
    assert len(requests) == 2
    assert "Evidence." not in json.dumps(requests[0]["messages"])
