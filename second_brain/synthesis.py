from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Protocol
from urllib import request

from .models import ChangeSet, SourceDocument


class SynthesisError(RuntimeError):
    pass


class Synthesizer(Protocol):
    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: dict[str, str], workspace: Path) -> ChangeSet: ...


def _payload(batch_sources: list[SourceDocument], concept_catalog: dict[str, str]) -> dict:
    return {"sources": [{"path": source.relative_path, "source_id": source.source_id, "title": source.title, "content": source.content} for source in batch_sources], "concept_catalog": concept_catalog, "write_scope": "Concepts/", "output": {"files": [{"path": "Concepts/example.md", "content": "..."}], "deletions": []}}


class NoopSynthesizer:
    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: dict[str, str], workspace: Path) -> ChangeSet:
        return ChangeSet()


class DeepSeekSynthesizer:
    def __init__(self, api_key: str | None = None, base_url: str = "https://api.deepseek.com", model: str = "deepseek-chat", timeout: int = 120):
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout

    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: dict[str, str], workspace: Path) -> ChangeSet:
        if not self.api_key:
            raise SynthesisError("DEEPSEEK_API_KEY is not configured")
        payload = _payload(batch_sources, concept_catalog)
        system = "Return JSON only. Reconcile evidence into Concepts/ Markdown files. Preserve disagreements and cite source paths. Never write outside Concepts/."
        body = json.dumps({"model": self.model, "messages": [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}], "response_format": {"type": "json_object"}, "stream": False}, ensure_ascii=False).encode("utf-8")
        req = request.Request(f"{self.base_url}/chat/completions", data=body, headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json", "User-Agent": "second-brain-ingestion/0.1"}, method="POST")
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                response_data = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            raise SynthesisError(f"DeepSeek request failed: {exc}") from exc
        try:
            content = response_data["choices"][0]["message"]["content"]
            result = json.loads(content)
            return ChangeSet(files=tuple(_candidate_files(result.get("files", []))), deletions=tuple(result.get("deletions", [])), metadata={"model": response_data.get("model", self.model)})
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise SynthesisError("DeepSeek returned an invalid synthesis JSON response") from exc


class CodexHarnessSynthesizer:
    def __init__(self, command: list[str], timeout: int = 600):
        self.command = command
        self.timeout = timeout

    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: dict[str, str], workspace: Path) -> ChangeSet:
        workspace.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(_payload(batch_sources, concept_catalog), ensure_ascii=False)
        try:
            result = subprocess.run(self.command, input=payload, text=True, capture_output=True, cwd=workspace, timeout=self.timeout, check=True)
            decoded = json.loads(result.stdout)
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
            raise SynthesisError(f"Codex harness failed: {exc}") from exc
        return ChangeSet(files=tuple(_candidate_files(decoded.get("files", []))), deletions=tuple(decoded.get("deletions", [])), metadata={"executor": "codex"})


def _candidate_files(values: list[dict]) -> list:
    from .models import CandidateFile
    return [CandidateFile(relative_path=str(item["path"]), content=str(item["content"])) for item in values]
