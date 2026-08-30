"""The controlled synthesis boundary.

Only the objects exported in ``__all__`` are part of the application seam.
Adapter implementations and catalog/provenance plumbing intentionally remain
internal to this package.
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping, Sequence

from ._boundary import (
    AdapterResult,
    CodexWorkspaceAdapter,
    ConceptDescriptor,
    ControlledSynthesis,
    ProvenanceDeclaration,
    SynthesisFailure as _BoundaryFailure,
    SynthesisOutcome as _BoundaryOutcome,
    _source_version_key,
)
from .adapters import DeepSeekAdapter, NoopAdapter
from ..models import ChangeSet, SourceDocument
from ..render import render_markdown, render_source

__all__ = [
    "build_controlled_synthesis",
    "SynthesisRunner",
    "SynthesisOutcome",
    "SynthesisFailure",
    "SynthesisMetadata",
    "ExecutorIdentity",
    "FailureCategory",
]


class FailureCategory(StrEnum):
    MALFORMED_EXECUTOR_OUTPUT = "malformed_executor_output"
    ADAPTER_EXECUTION_FAILURE = "adapter_execution_failure"
    TIMEOUT = "timeout"
    NARROW_READ_UNAVAILABLE = "narrow_read_unavailable"
    INVALID_OPERATION = "invalid_operation"
    PATH_COLLISION = "path_collision"
    OUT_OF_SCOPE_CHANGE = "out_of_scope_change"
    SYMLINK_ESCAPE = "symlink_escape"
    WORKSPACE_CONTAMINATION = "workspace_contamination"
    INVALID_PROVENANCE = "invalid_provenance"
    INVALID_CONCEPT_MARKDOWN = "invalid_concept_markdown"


@dataclass(frozen=True)
class ExecutorIdentity:
    kind: str
    model: str | None = None
    version: str = "1"

    def as_dict(self) -> dict[str, str]:
        value = {"kind": self.kind, "version": self.version}
        if self.model:
            value["model"] = self.model
        return value


@dataclass(frozen=True)
class SynthesisMetadata:
    affected_paths: tuple[str, ...]
    deletions: tuple[str, ...]
    cited_source_versions: tuple[str, ...]
    executor: ExecutorIdentity
    validation: str = "validated"

    def as_dict(self) -> dict[str, Any]:
        return {
            "affected_paths": self.affected_paths,
            "deletions": self.deletions,
            "cited_source_versions": self.cited_source_versions,
            "executor": self.executor.as_dict(),
            "validation": self.validation,
        }


@dataclass(frozen=True)
class SynthesisOutcome:
    change_set: ChangeSet
    metadata: SynthesisMetadata


@dataclass(frozen=True)
class SynthesisFailure:
    category: FailureCategory
    safe_message: str
    executor: ExecutorIdentity

    @property
    def message(self) -> str:
        """Compatibility-readable alias; never contains executor evidence."""
        return self.safe_message


def _catalog(vault: Path) -> dict[str, dict[str, Any]]:
    """Build deterministic concept descriptors and complete baseline bodies."""
    root = vault / "Concepts"
    if not root.exists():
        return {}
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Concepts must be a regular directory")
    result: dict[str, dict[str, Any]] = {}
    folded: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"symlink in Concepts: {path.relative_to(vault)}")
        if not path.is_file():
            continue
        if path.suffix.casefold() != ".md":
            raise ValueError(f"non-Markdown file in Concepts: {path.relative_to(vault)}")
        relative = path.relative_to(vault).as_posix()
        normalized = "/".join(unicodedata.normalize("NFC", part) for part in relative.split("/"))
        key = normalized.casefold()
        if key in folded and folded[key] != normalized:
            raise ValueError(f"case-folded concept path collision: {folded[key]} / {normalized}")
        folded[key] = normalized
        content = path.read_text(encoding="utf-8")
        title = next((line[2:].strip() for line in content.splitlines() if line.startswith("# ")), path.stem)
        aliases = _frontmatter_aliases(content)
        links: list[str] = []
        seen_links: set[str] = set()
        for match in re.finditer(r"\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|[^\]]+)?\]\]", content):
            link = match.group(1).strip()
            if link and link not in seen_links:
                seen_links.add(link)
                links.append(link)
        result[normalized] = {
            "identifier": normalized,
            "title": title,
            "aliases": tuple(aliases),
            "links": tuple(links),
            "content": content,
        }
    return result


def _frontmatter_aliases(content: str) -> list[str]:
    if not content.startswith("---\n") or "\n---\n" not in content:
        return []
    header = content[4:].split("\n---\n", 1)[0]
    aliases: list[str] = []
    in_aliases = False
    for line in header.splitlines():
        stripped = line.strip()
        if stripped.startswith("aliases:"):
            raw = stripped.split(":", 1)[1].strip()
            if raw.startswith("[") and raw.endswith("]"):
                aliases.extend(item.strip().strip("'\"") for item in raw[1:-1].split(",") if item.strip())
            elif raw:
                aliases.append(raw.strip("'\""))
            in_aliases = True
            continue
        if in_aliases and stripped.startswith("-"):
            aliases.append(stripped[1:].strip().strip("'\""))
        elif in_aliases and stripped and not line.startswith((" ", "\t")):
            in_aliases = False
    return list(dict.fromkeys(item for item in aliases if item))


def _committed_sources(vault: Path) -> list[SourceDocument]:
    """Discover only rendered, immutable source versions from the vault."""
    root = vault / "Sources"
    if not root.exists():
        return []
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Sources must be a regular directory")
    committed: list[SourceDocument] = []
    for entry in sorted(root.rglob("*")):
        if entry.is_symlink():
            raise ValueError(f"symlink in Sources: {entry.relative_to(vault)}")
    for path in sorted(root.rglob("*.md")):
        if not path.is_file():
            raise ValueError(f"committed source is not a regular file: {path.relative_to(vault)}")
        text = path.read_text(encoding="utf-8")
        if not text.startswith("---\n") or "\n---\n" not in text:
            raise ValueError(f"committed source has invalid frontmatter: {path}")
        header, rendered_body = text[4:].split("\n---\n", 1)
        metadata: dict[str, object] = {}
        for line in header.splitlines():
            if ":" not in line:
                raise ValueError(f"committed source has invalid metadata: {path}")
            key, raw = line.split(":", 1)
            metadata[key] = None if raw.strip() == "null" else json.loads(raw.strip())
        title = str(metadata.get("title", ""))
        marker = f"# {title}\n\n"
        if not rendered_body.startswith(marker):
            raise ValueError(f"committed source body does not match title: {path}")
        content = rendered_body[len(marker):]
        kind = str(metadata.get("source_type", ""))
        source = render_source(
            source_id=str(metadata["source_id"]), kind=kind,
            canonical_url=str(metadata["canonical_url"]), title=title, body=content,
            author=metadata.get("author"), publication_date=metadata.get("publication_date"),
            captured_at=str(metadata["captured_at"]), input_method=str(metadata["input_method"]),
            source_version=int(metadata["immutable_source_version"]),
        )
        if source.relative_path != path.relative_to(vault).as_posix() or source.content_hash != str(metadata["content_hash"]) or render_markdown(source) != text:
            raise ValueError(f"committed source identity or rendering mismatch: {path}")
        committed.append(source)
    return committed


class SynthesisRunner:
    """Own catalog/provenance discovery and cross the validation boundary once."""

    def __init__(self, config, adapter: Any, executor: ExecutorIdentity):
        self.config = config
        self.adapter = adapter
        self.executor = executor
        self._boundary = ControlledSynthesis(adapter, executor=executor.as_dict())

    def run(self, batch_id: str, batch_sources: Sequence[SourceDocument]) -> SynthesisOutcome | SynthesisFailure:
        workspace = self.config.state_dir / "staging" / batch_id
        try:
            catalog = _catalog(self.config.vault)
            committed = _committed_sources(self.config.vault)
            result = self._boundary.run(list(batch_sources), catalog, workspace, vault=self.config.vault, committed_sources=committed)
        except Exception as exc:
            message = str(exc).casefold()
            category = FailureCategory.SYMLINK_ESCAPE if "symlink" in message else FailureCategory.INVALID_PROVENANCE if "source" in message or "frontmatter" in message else FailureCategory.ADAPTER_EXECUTION_FAILURE
            safe = "candidate contains an unsafe symlink" if category == FailureCategory.SYMLINK_ESCAPE else "immutable source catalog is invalid" if category == FailureCategory.INVALID_PROVENANCE else "controlled synthesis preflight failed"
            return SynthesisFailure(category, safe, self.executor)
        if isinstance(result, _BoundaryFailure):
            try:
                category = FailureCategory(result.category)
            except ValueError:
                category = FailureCategory.ADAPTER_EXECUTION_FAILURE
            # The boundary's failure text is already generated without bodies,
            # but do not expose provider/process details through this seam.
            safe = {
                FailureCategory.TIMEOUT: "synthesis executor timed out",
                FailureCategory.NARROW_READ_UNAVAILABLE: "requested evidence is unavailable within the read bound",
                FailureCategory.INVALID_PROVENANCE: "candidate provenance is invalid",
                FailureCategory.INVALID_OPERATION: "candidate operation is invalid",
                FailureCategory.PATH_COLLISION: "candidate paths collide",
                FailureCategory.OUT_OF_SCOPE_CHANGE: "candidate changes are outside Concepts/",
                FailureCategory.SYMLINK_ESCAPE: "candidate contains an unsafe symlink",
                FailureCategory.WORKSPACE_CONTAMINATION: "candidate workspace was contaminated",
                FailureCategory.MALFORMED_EXECUTOR_OUTPUT: "executor output is malformed",
                FailureCategory.INVALID_CONCEPT_MARKDOWN: "candidate concept Markdown is invalid",
            }.get(category, "controlled synthesis failed")
            return SynthesisFailure(category, safe, self.executor)
        if not isinstance(result, _BoundaryOutcome):
            return SynthesisFailure(FailureCategory.MALFORMED_EXECUTOR_OUTPUT, "controlled synthesis returned an invalid outcome", self.executor)
        metadata = SynthesisMetadata(
            affected_paths=result.affected_paths,
            deletions=result.deletions,
            cited_source_versions=result.cited_source_versions,
            executor=self.executor,
            validation=result.validation,
        )
        normalized_changes = ChangeSet(result.change_set.files, result.change_set.deletions)
        return SynthesisOutcome(normalized_changes, metadata)


def build_controlled_synthesis(config) -> SynthesisRunner:
    """Construct the only production synthesis runner from validated config."""
    if config.executor == "deepseek":
        if not config.deepseek_model:
            raise ValueError("deepseek_model is required; configure an explicit supported model")
        adapter = DeepSeekAdapter(api_key=None, base_url=config.deepseek_base_url, model=config.deepseek_model, timeout=config.request_timeout)
        identity = ExecutorIdentity("deepseek", config.deepseek_model)
    elif config.executor == "codex":
        adapter = CodexWorkspaceAdapter(config.codex_executable, model=config.codex_model, timeout=config.request_timeout)
        adapter.preflight()
        identity = ExecutorIdentity("codex", config.codex_model)
    else:
        adapter = NoopAdapter()
        identity = ExecutorIdentity("noop")
    return SynthesisRunner(config, adapter, identity)
