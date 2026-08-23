from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Mapping, Protocol
from urllib import request

from .models import CandidateFile, ChangeSet, SourceDocument

# The controlled interface is kept in a separate module so the legacy batch
# adapter protocol remains import-compatible while callers migrate.
from .controlled_synthesis import (
    AdapterResult,
    CodexWorkspaceAdapter,
    ConceptDescriptor,
    ControlledSynthesis,
    DirectAdapter,
    NarrowReadCapabilities,
    ProvenanceDeclaration,
    SynthesisFailure,
    SynthesisOutcome,
)


class SynthesisError(RuntimeError):
    pass


class Synthesizer(Protocol):
    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: dict[str, str], workspace: Path) -> ChangeSet: ...


def _payload(batch_sources: list[SourceDocument], concept_catalog: dict[str, str]) -> dict:
    return {"sources": [{"path": source.relative_path, "source_id": source.source_id, "title": source.title, "content": source.content} for source in batch_sources], "concept_catalog": concept_catalog, "write_scope": "Concepts/", "output": {"files": [{"path": "Concepts/example.md", "content": "..."}], "deletions": []}}


class NoopSynthesizer:
    def execute(self, _capabilities: NarrowReadCapabilities) -> AdapterResult:
        return AdapterResult()

    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: dict[str, str], workspace: Path) -> ChangeSet:
        return ChangeSet()


class DeepSeekSynthesizer:
    def __init__(self, api_key: str | None = None, base_url: str = "https://api.deepseek.com", model: str = "deepseek-chat", timeout: int = 120):
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._source_versions: tuple[str, ...] = ()

    def set_source_versions(self, source_versions: tuple[str, ...]) -> None:
        self._source_versions = tuple(source_versions)

    def execute(self, capabilities: NarrowReadCapabilities) -> AdapterResult:
        """Use the controlled adapter contract without sending evidence bodies."""
        descriptors = capabilities.list_concepts()
        payload = {
            "concepts": [{"identifier": item.identifier, "path": item.relative_path, "title": item.title, "aliases": item.aliases, "links": item.links} for item in descriptors],
            "source_versions": list(self._source_versions),
            "capabilities": ["list_concepts", "read_concept", "search_concepts", "read_source"],
            "tool_contract": {"read_source": {"argument": "source_version", "returns": "one bounded source document"}, "read_concept": {"argument": "catalog identifier", "returns": "one bounded concept body"}},
            "write_scope": "Concepts/",
            "output": {"writes": [{"path": "Concepts/example.md", "content": "..."}], "deletions": [], "provenance": [], "metadata": {}},
        }
        decoded = self._request(payload)
        if not isinstance(decoded, Mapping):
            raise SynthesisError("DeepSeek returned an invalid synthesis JSON response")
        if set(decoded) - {"writes", "deletions", "provenance", "metadata"}:
            raise SynthesisError("DeepSeek returned unknown synthesis fields")
        try:
            writes = tuple(CandidateFile(str(item["path"]), str(item["content"])) for item in decoded.get("writes", []))
            deletions = tuple(str(item) for item in decoded.get("deletions", []))
            provenance = tuple(ProvenanceDeclaration(str(item["concept_path"]), int(item["occurrence"]), str(item["source_version"])) for item in decoded.get("provenance", []))
            metadata = decoded.get("metadata", {})
            if not isinstance(metadata, Mapping):
                raise TypeError
        except (KeyError, TypeError, ValueError) as exc:
            raise SynthesisError("DeepSeek returned an invalid synthesis JSON response") from exc
        return AdapterResult(writes, deletions, provenance, {"kind": "deepseek", "model": self.model, **dict(metadata)})

    def _request(self, payload: dict[str, Any]) -> Any:
        if not self.api_key:
            raise SynthesisError("DEEPSEEK_API_KEY is not configured")
        system = "Return JSON only. Use the listed narrow capabilities to inspect evidence one item at a time. Never write outside Concepts/."
        body = json.dumps({"model": self.model, "messages": [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}], "response_format": {"type": "json_object"}, "stream": False}, ensure_ascii=False).encode("utf-8")
        req = request.Request(f"{self.base_url}/chat/completions", data=body, headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json", "User-Agent": "second-brain-ingestion/0.1"}, method="POST")
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                response_data = json.loads(response.read().decode("utf-8"))
            return json.loads(response_data["choices"][0]["message"]["content"])
        except Exception as exc:
            raise SynthesisError(f"DeepSeek request failed: {exc}") from exc

    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: dict[str, str], workspace: Path) -> ChangeSet:
        payload = _payload(batch_sources, concept_catalog)
        result = self._request(payload)
        try:
            return ChangeSet(files=tuple(_candidate_files(result.get("files", []))), deletions=tuple(result.get("deletions", [])), metadata={"model": self.model})
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
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
