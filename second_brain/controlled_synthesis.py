"""The controlled boundary between untrusted synthesis and publication.

Adapters in this module are deliberately untrusted.  They can propose text, but
only :class:`ControlledSynthesis` can produce a publishable result.
"""
from __future__ import annotations

import json
import hashlib
import os
import re
import shutil
import stat
import subprocess
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import parse_qs, unquote, urlparse

from .models import CandidateFile, ChangeSet, SourceDocument
from .render import render_markdown


FAILURE_CATEGORIES = frozenset({
    "malformed_executor_output", "adapter_execution_failure", "timeout",
    "narrow_read_unavailable", "invalid_operation", "path_collision",
    "out_of_scope_change", "symlink_escape", "workspace_contamination",
    "invalid_provenance", "invalid_concept_markdown",
})


@dataclass(frozen=True)
class SynthesisFailure:
    category: str
    message: str
    executor: str
    change_set: None = None


@dataclass(frozen=True)
class ConceptDescriptor:
    identifier: str
    relative_path: str
    title: str
    aliases: tuple[str, ...] = ()
    links: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProvenanceDeclaration:
    concept_path: str
    occurrence: int
    source_version: str


@dataclass(frozen=True)
class AdapterResult:
    writes: tuple[CandidateFile, ...] = ()
    deletions: tuple[str, ...] = ()
    provenance: tuple[ProvenanceDeclaration, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    claims_present: bool = False


@dataclass(frozen=True)
class SynthesisOutcome:
    change_set: ChangeSet
    affected_paths: tuple[str, ...]
    deletions: tuple[str, ...]
    cited_source_versions: tuple[str, ...]
    executor: Mapping[str, str]
    validation: str = "validated"


@dataclass(frozen=True)
class NarrowReadCapabilities:
    """Exactly the four reads exposed to a direct adapter."""
    list_concepts: Callable[[], tuple[ConceptDescriptor, ...]]
    read_concept: Callable[[str], str]
    search_concepts: Callable[[str], tuple[ConceptDescriptor, ...]]
    read_source: Callable[[str], SourceDocument]


class DirectAdapter(Protocol):
    def execute(self, capabilities: NarrowReadCapabilities) -> AdapterResult | Mapping[str, Any]: ...


class SynthesisValidationError(ValueError):
    def __init__(self, category: str, message: str):
        self.category = category
        super().__init__(message)


def _failure(executor: str, exc: Exception) -> SynthesisFailure:
    category = getattr(exc, "category", "adapter_execution_failure")
    if isinstance(exc, json.JSONDecodeError):
        category = "malformed_executor_output"
    if category not in FAILURE_CATEGORIES:
        category = "adapter_execution_failure"
    return SynthesisFailure(category, str(exc), executor)


def _safe_path(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise SynthesisValidationError("invalid_operation", f"invalid concept path: {value!r}")
    # Inspect the raw spelling before PurePosixPath normalizes it.  Empty,
    # repeated, dot, and traversal components are all unsafe even when the
    # normalized path would appear to remain beneath Concepts/.
    raw_parts = value.split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise SynthesisValidationError("invalid_operation", f"invalid concept path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or value.startswith("/"):
        raise SynthesisValidationError("invalid_operation", f"invalid concept path: {value!r}")
    normalized = "/".join(unicodedata.normalize("NFC", part) for part in path.parts)
    if not normalized.startswith("Concepts/") or normalized == "Concepts/" or not normalized.lower().endswith(".md"):
        raise SynthesisValidationError("out_of_scope_change", f"path is outside Concepts/: {value!r}")
    return normalized


def _path_key(value: str) -> str:
    return unicodedata.normalize("NFC", value).casefold()


def _assert_resolved_under(path: Path, root: Path, category: str = "symlink_escape") -> None:
    try:
        path.resolve(strict=False).relative_to(root.resolve())
    except ValueError as exc:
        raise SynthesisValidationError(category, f"path escapes configured root: {path}") from exc


def _assert_no_symlink_ancestry(path: Path, root: Path) -> None:
    if root.is_symlink():
        raise SynthesisValidationError("symlink_escape", f"configured root is a symlink: {root}")
    _assert_resolved_under(path, root)
    current = root
    relative = path.absolute().relative_to(root.absolute())
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise SynthesisValidationError("symlink_escape", f"symlink in target ancestry: {path}")


def _candidate_path(relative: str, workspace: Path) -> tuple[str, Path]:
    normalized = _safe_path(relative)
    target = workspace.joinpath(*normalized.split("/"))
    _assert_no_symlink_ancestry(target, workspace)
    return normalized, target


def _source_version_key(source: SourceDocument) -> str:
    return f"{source.source_id}:v{source.source_version}"


def _requires_provenance(content: str) -> bool:
    """Identify ordinary interpretation text that cannot be an empty marker."""
    meaningful = []
    for line in content.splitlines():
        if line.lstrip().startswith("#"):
            continue
        stripped = re.sub(r"[>*_`-]", "", line).strip()
        if stripped:
            meaningful.append(stripped)
    text = " ".join(meaningful)
    return bool(text)


def _safe_metadata(value: Mapping[str, Any]) -> dict[str, str]:
    blocked = {"api_key", "token", "secret", "prompt", "content", "body", "sources", "concepts"}
    return {
        str(key): str(item)
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        if str(key).casefold() not in blocked and not any(part in str(key).casefold() for part in ("password", "credential"))
    }


def _regular_tree(root: Path) -> dict[str, bytes]:
    """Walk without following symlinks and reject non-regular entries."""
    result: dict[str, bytes] = {}
    if not root.exists():
        return result
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        try:
            mode = path.lstat().st_mode
        except OSError as exc:
            raise SynthesisValidationError("workspace_contamination", f"cannot inspect workspace entry {relative}") from exc
        if stat.S_ISLNK(mode):
            raise SynthesisValidationError("symlink_escape", f"symlink in candidate workspace: {relative}")
        if path.is_dir():
            if relative.startswith("Concepts/") and relative != "Concepts" and not any(path.iterdir()):
                raise SynthesisValidationError("workspace_contamination", f"empty directory in Concepts/: {relative}")
            continue
        if not stat.S_ISREG(mode):
            raise SynthesisValidationError("workspace_contamination", f"non-regular workspace entry: {relative}")
        if relative.startswith("Concepts/") and not relative.lower().endswith(".md"):
            raise SynthesisValidationError("workspace_contamination", f"non-Markdown concept entry: {relative}")
        result[relative] = path.read_bytes()
    return result


def _strict_adapter_result(value: AdapterResult | Mapping[str, Any]) -> AdapterResult:
    if isinstance(value, AdapterResult):
        return value
    if not isinstance(value, Mapping):
        raise SynthesisValidationError("malformed_executor_output", "adapter result must be an object")
    allowed = {"writes", "deletions", "provenance", "metadata"}
    unknown = set(value) - allowed
    if unknown:
        raise SynthesisValidationError("malformed_executor_output", f"unknown adapter fields: {sorted(unknown)}")
    writes = []
    for item in value.get("writes", []):
        if not isinstance(item, Mapping) or set(item) != {"path", "content"} or not isinstance(item["path"], str) or not isinstance(item["content"], str):
            raise SynthesisValidationError("invalid_operation", "malformed write operation")
        writes.append(CandidateFile(item["path"], item["content"]))
    deletions = tuple(value.get("deletions", []))
    if not all(isinstance(item, str) for item in deletions):
        raise SynthesisValidationError("invalid_operation", "deletions must contain paths")
    declarations = []
    for item in value.get("provenance", []):
        if not isinstance(item, Mapping) or set(item) != {"concept_path", "occurrence", "source_version"}:
            raise SynthesisValidationError("invalid_provenance", "malformed provenance declaration")
        if not isinstance(item["concept_path"], str) or not isinstance(item["source_version"], str) or not isinstance(item["occurrence"], int) or item["occurrence"] < 0:
            raise SynthesisValidationError("invalid_provenance", "malformed provenance declaration")
        declarations.append(ProvenanceDeclaration(item["concept_path"], item["occurrence"], item["source_version"]))
    metadata = value.get("metadata", {})
    if not isinstance(metadata, Mapping):
        raise SynthesisValidationError("malformed_executor_output", "adapter metadata must be an object")
    return AdapterResult(tuple(writes), deletions, tuple(declarations), dict(metadata), "writes" in value or "deletions" in value)


class ControlledSynthesis:
    """Run one adapter and return either one fully validated result or failure."""

    def __init__(self, adapter: Any, *, executor: Mapping[str, str] | None = None, max_search_results: int = 25, max_read_bytes: int = 100_000):
        self.adapter = adapter
        self.executor = dict(executor or {"kind": adapter.__class__.__name__.lower(), "version": "1"})
        self.max_search_results = max_search_results
        self.max_read_bytes = max_read_bytes

    def run(self, batch_sources: list[SourceDocument], concept_catalog: Mapping[str, Any], workspace: Path, *, vault: Path | None = None, committed_sources: list[SourceDocument] = ()) -> SynthesisOutcome | SynthesisFailure:
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            if isinstance(self.adapter, CodexWorkspaceAdapter):
                # The baseline is part of the candidate, not an executor
                # change.  Seed it before taking the comparison snapshot.
                self.adapter.seed_workspace(concept_catalog, workspace, batch_sources=batch_sources)
            workspace_before = _regular_tree(workspace)
            sources: dict[str, SourceDocument] = {}
            for source in [*batch_sources, *committed_sources]:
                # Provenance is immutable and version-specific.  Do not add
                # source-id or physical-path aliases that could silently pick
                # the wrong committed version.
                version_key = _source_version_key(source)
                previous = sources.get(version_key)
                if previous and previous != source:
                    raise SynthesisValidationError("invalid_provenance", f"ambiguous source identity: {version_key}")
                sources[version_key] = source
            concepts, bodies = self._catalog(concept_catalog, vault)
            capabilities = NarrowReadCapabilities(
                list_concepts=lambda: tuple(concepts),
                read_concept=lambda identifier: self._read_concept(identifier, concepts, bodies),
                search_concepts=lambda query: tuple(item for item in concepts if query.casefold() in (item.title + " " + " ".join(item.aliases)).casefold())[: self.max_search_results],
                read_source=lambda identifier: self._read_source(identifier, sources),
            )
            if hasattr(self.adapter, "set_source_versions"):
                self.adapter.set_source_versions(tuple(sorted(sources)))
            if hasattr(self.adapter, "execute"):
                if not getattr(self.adapter, "requires_context", False):
                    raw = self.adapter.execute(capabilities)
                elif isinstance(self.adapter, CodexWorkspaceAdapter):
                    raw = self.adapter.execute(batch_sources, concept_catalog, workspace, vault=vault)
                else:
                    raw = self.adapter.execute(batch_sources, concept_catalog, workspace)
            else:
                raw = self.adapter(capabilities)
            result = _strict_adapter_result(raw)
            if isinstance(self.adapter, CodexWorkspaceAdapter):
                result = self._codex_result(result, workspace, workspace_before, vault)
            return self._validate(result, sources, concepts, workspace, vault)
        except Exception as exc:
            return _failure(self.executor.get("kind", "unknown"), exc)

    execute = run

    def synthesize(self, batch_sources: list[SourceDocument], concept_catalog: Mapping[str, Any], workspace: Path) -> ChangeSet:
        """Compatibility adapter for the existing batch runner.

        New callers should use :meth:`run` so structured failures remain
        observable.  The legacy seam receives no publishable change set when
        validation fails.
        """
        result = self.run(batch_sources, concept_catalog, workspace)
        if isinstance(result, SynthesisFailure):
            raise SynthesisValidationError(result.category, result.message)
        return result.change_set

    def _catalog(self, catalog: Mapping[str, Any], vault: Path | None):
        concepts: list[ConceptDescriptor] = []
        bodies: dict[str, str] = {}
        for path, value in sorted(catalog.items()):
            relative = str(path)
            if isinstance(value, ConceptDescriptor):
                relative = _safe_path(value.relative_path)
                descriptor = ConceptDescriptor(value.identifier, relative, value.title, value.aliases, value.links)
            elif isinstance(value, Mapping):
                relative = _safe_path(relative)
                descriptor = ConceptDescriptor(str(value.get("identifier", relative)), relative, str(value.get("title", Path(relative).stem)), tuple(value.get("aliases", ())), tuple(value.get("links", ())))
                bodies[descriptor.identifier] = str(value.get("content", ""))
            else:
                relative = _safe_path(relative)
                body = str(value)
                descriptor = ConceptDescriptor(relative, relative, Path(relative).stem, ())
                bodies[relative] = body
            concepts.append(descriptor)
            if vault and not bodies.get(descriptor.identifier):
                target = vault.joinpath(*descriptor.relative_path.split("/"))
                _assert_no_symlink_ancestry(target, vault)
                if target.is_file() and not target.is_symlink():
                    bodies[descriptor.identifier] = target.read_text(encoding="utf-8")
        return concepts, bodies

    def _read_concept(self, identifier, concepts, bodies):
        descriptor = next((item for item in concepts if item.identifier == identifier), None)
        if not descriptor or identifier not in bodies or len(bodies[identifier].encode()) > self.max_read_bytes:
            raise SynthesisValidationError("narrow_read_unavailable", f"concept read unavailable: {identifier}")
        return bodies[identifier]

    def _read_source(self, identifier, sources):
        if identifier not in sources:
            raise SynthesisValidationError("narrow_read_unavailable", f"source read unavailable: {identifier}")
        source = sources[identifier]
        if len(source.content.encode("utf-8")) > self.max_read_bytes:
            raise SynthesisValidationError("narrow_read_unavailable", f"source read unavailable: {identifier}")
        return source

    def _codex_result(self, claimed: AdapterResult, workspace: Path, workspace_before: dict[str, bytes], vault: Path | None):
        after_workspace = _regular_tree(workspace)
        changed_workspace = {path for path in set(workspace_before) | set(after_workspace) if workspace_before.get(path) != after_workspace.get(path)}
        unexpected = sorted(path for path in changed_workspace if not path.startswith("Concepts/"))
        if unexpected:
            raise SynthesisValidationError("workspace_contamination", f"workspace changed outside Concepts/: {unexpected[0]}")
        before = {path.removeprefix("Concepts/"): value for path, value in workspace_before.items() if path.startswith("Concepts/")}
        after = {path.removeprefix("Concepts/"): value for path, value in after_workspace.items() if path.startswith("Concepts/")}
        changed = set(before) | set(after)
        writes = []
        for path in sorted(changed):
            if path not in after or before.get(path) == after[path]:
                continue
            try:
                content = after[path].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise SynthesisValidationError("invalid_concept_markdown", f"concept candidate is not UTF-8: {path}") from exc
            writes.append(CandidateFile(f"Concepts/{path}", content))
        writes = tuple(writes)
        deletions = tuple(f"Concepts/{path}" for path in sorted(changed) if path in before and path not in after)
        actual = {item.relative_path for item in writes} | set(deletions)
        claimed_paths = {item.relative_path for item in claimed.writes} | set(claimed.deletions)
        if claimed.claims_present and claimed_paths != actual:
            raise SynthesisValidationError("malformed_executor_output", "Codex output does not match candidate workspace diff")
        return AdapterResult(writes, deletions, claimed.provenance, claimed.metadata, claimed.claims_present)

    def _validate(self, result, sources, concepts, workspace, vault):
        writes: list[CandidateFile] = []
        deletes: list[str] = []
        seen: dict[str, str] = {}
        for item in result.writes:
            path = _safe_path(item.relative_path)
            key = _path_key(path)
            if key in seen:
                raise SynthesisValidationError("path_collision", f"path collision: {path}")
            seen[key] = path
            _validate_concept_markdown(item.content)
            self._check_ancestry(workspace, path, vault)
            writes.append(CandidateFile(path, item.content))
        for deletion in result.deletions:
            path = _safe_path(deletion)
            key = _path_key(path)
            if key in seen:
                raise SynthesisValidationError("path_collision", f"write/delete collision: {path}")
            seen[key] = path
            self._check_ancestry(workspace, path, vault)
            deletes.append(path)
        links_by_path: dict[str, list[tuple[str, str]]] = {item.relative_path: _citation_links(item.content) for item in writes}
        declarations: dict[tuple[str, int], ProvenanceDeclaration] = {}
        for declaration in result.provenance:
            path = _safe_path(declaration.concept_path)
            candidate = next((item.content for item in writes if item.relative_path == path), None)
            if candidate is None:
                raise SynthesisValidationError("invalid_provenance", f"provenance points to unwritten concept: {path}")
            declaration_key = (path, declaration.occurrence)
            if declaration_key in declarations:
                raise SynthesisValidationError("invalid_provenance", f"duplicate provenance declaration: {path}#{declaration.occurrence}")
            source = sources.get(declaration.source_version)
            if source is None:
                raise SynthesisValidationError("invalid_provenance", f"unknown source version: {declaration.source_version}")
            links = links_by_path[path]
            if declaration.occurrence >= len(links) or not _citation_matches(links[declaration.occurrence], source):
                raise SynthesisValidationError("invalid_provenance", f"citation {declaration.occurrence} does not resolve for {path}")
            declarations[declaration_key] = declaration
        cited: set[str] = set()
        for path, links in links_by_path.items():
            for occurrence, link in enumerate(links):
                declaration = declarations.get((path, occurrence))
                if declaration is None:
                    raise SynthesisValidationError("invalid_provenance", f"citation {occurrence} is undeclared for {path}")
                source = sources[declaration.source_version]
                if not _citation_matches(link, source):
                    raise SynthesisValidationError("invalid_provenance", f"citation {occurrence} does not resolve for {path}")
                cited.add(_source_version_key(source))
            candidate = next(item.content for item in writes if item.relative_path == path)
            if not links and _requires_provenance(candidate):
                raise SynthesisValidationError("invalid_provenance", f"substantive interpretation has no provenance: {path}")
        declared_keys = set(declarations)
        expected_keys = {(path, occurrence) for path, links in links_by_path.items() for occurrence in range(len(links))}
        if declared_keys != expected_keys:
            raise SynthesisValidationError("invalid_provenance", "provenance declarations do not match candidate citations")
        affected = tuple(sorted({item.relative_path for item in writes} | set(deletes)))
        ordered_deletes = tuple(sorted(deletes))
        normalized_executor = {str(key): str(value) for key, value in sorted(self.executor.items()) if key.casefold() not in {"api_key", "token", "secret", "prompt"}}
        normalized_executor.update({key: value for key, value in _safe_metadata(result.metadata).items() if key not in normalized_executor})
        changes = ChangeSet(tuple(sorted(writes, key=lambda item: item.relative_path)), ordered_deletes, {"executor": normalized_executor, "affected_paths": affected, "cited_source_versions": tuple(sorted(cited)), "validation": "validated"})
        return SynthesisOutcome(changes, affected, ordered_deletes, tuple(sorted(cited)), normalized_executor)

    @staticmethod
    def _check_ancestry(workspace, relative, vault):
        for root in (workspace, vault):
            if not root:
                continue
            target = root.joinpath(*relative.split("/"))
            _assert_no_symlink_ancestry(target, root)


class CodexWorkspaceAdapter:
    """Run a command in a candidate workspace; stdout is only optional metadata."""
    requires_context = True

    def __init__(self, command: list[str], *, timeout: int = 600, executor_version: str = "1"):
        self.command = list(command)
        self.timeout = timeout
        self.executor_version = executor_version

    def seed_workspace(self, concept_catalog, workspace: Path, *, batch_sources=()) -> None:
        workspace.mkdir(parents=True, exist_ok=True)
        if workspace.is_symlink():
            raise SynthesisValidationError("symlink_escape", "candidate workspace is a symlink")
        concepts_root = workspace / "Concepts"
        if concepts_root.is_symlink():
            raise SynthesisValidationError("symlink_escape", "candidate Concepts directory is a symlink")
        if concepts_root.exists() and not concepts_root.is_dir():
            raise SynthesisValidationError("workspace_contamination", "candidate Concepts entry is not a directory")
        concepts_root.mkdir(exist_ok=True)
        for relative, value in concept_catalog.items():
            if not isinstance(relative, str) or not isinstance(value, str):
                continue
            _normalized, target = _candidate_path(relative, workspace)
            if target.exists():
                if target.is_symlink() or not target.is_file():
                    raise SynthesisValidationError("symlink_escape", f"unsafe catalog target: {relative}")
                continue
            _assert_no_symlink_ancestry(target, workspace)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(value, encoding="utf-8")
        inputs = workspace / "Inputs"
        inputs.mkdir(exist_ok=True)
        source_manifest = {}
        for source in batch_sources:
            key = _source_version_key(source)
            filename = hashlib.sha256(key.encode("utf-8")).hexdigest() + ".md"
            target = inputs / filename
            target.write_text(source.content, encoding="utf-8")
            source_manifest[key] = filename
        (inputs / "sources.json").write_text(json.dumps(source_manifest, sort_keys=True) + "\n", encoding="utf-8")

    def execute(self, batch_sources, concept_catalog, workspace, *, vault: Path | None = None):
        self.seed_workspace(concept_catalog, workspace, batch_sources=batch_sources)
        payload = json.dumps({"sources": [{"source_id": item.source_id, "source_version": item.source_version, "title": item.title, "read_file": f"Inputs/{hashlib.sha256(_source_version_key(item).encode('utf-8')).hexdigest()}.md"} for item in batch_sources], "concepts": [{"path": path} for path in sorted(concept_catalog)], "write_scope": "Concepts/", "read_scope": "Inputs/"}, ensure_ascii=False)
        try:
            process = subprocess.run(_sandboxed_command(self.command, workspace, vault), input=payload, text=True, capture_output=True, cwd=workspace, timeout=self.timeout, check=True, env=_sandbox_environment())
        except subprocess.TimeoutExpired as exc:
            raise SynthesisValidationError("timeout", "Codex adapter timed out") from exc
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").casefold()
            category = "workspace_contamination" if "operation not permitted" in stderr or "sandbox" in stderr else "adapter_execution_failure"
            raise SynthesisValidationError(category, "Codex adapter process failed") from exc
        except (OSError, subprocess.SubprocessError) as exc:
            raise SynthesisValidationError("adapter_execution_failure", "Codex adapter execution failed") from exc
        metadata: dict[str, Any] = {}
        if process.stdout.strip():
            try:
                decoded = json.loads(process.stdout)
            except json.JSONDecodeError as exc:
                raise SynthesisValidationError("malformed_executor_output", "Codex stdout is not JSON") from exc
            parsed = _strict_adapter_result(decoded)
            metadata = dict(parsed.metadata)
            provenance = parsed.provenance
            claims_present = parsed.claims_present
        else:
            provenance = []
            claims_present = False
        return AdapterResult((), (), tuple(provenance), {"kind": "codex", "version": self.executor_version, **metadata}, claims_present)


def _sandbox_environment() -> dict[str, str]:
    return {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1"}


def codex_sandbox_available() -> bool:
    """Return whether the host permits the required filesystem sandbox."""
    sandbox = shutil.which("sandbox-exec")
    if sandbox is None:
        return False
    try:
        probe = subprocess.run([sandbox, "-p", "(version 1) (allow default)", "--", "/usr/bin/true"], capture_output=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return False
    return probe.returncode == 0


def _sandboxed_command(command: list[str], workspace: Path, vault: Path | None = None) -> list[str]:
    sandbox = shutil.which("sandbox-exec")
    if sandbox is None:
        raise SynthesisValidationError("adapter_execution_failure", "Codex execution sandbox is unavailable")
    escaped = str(workspace.resolve()).replace("\\", "\\\\").replace('"', '\\"')
    profile = f'(version 1) (deny default) (allow process-exec) (allow process-fork) (allow signal) (allow sysctl-read) (allow file-read*) (allow file-write* (subpath "{escaped}"))'
    if vault is not None:
        escaped_vault = str(vault.resolve()).replace("\\", "\\\\").replace('"', '\\"')
        profile += f' (deny file-read* (subpath "{escaped_vault}"))'
    return [sandbox, "-p", profile, "--", *command]


def _validate_concept_markdown(content: str) -> None:
    if not isinstance(content, str) or "\r" in content or not content.endswith("\n"):
        raise SynthesisValidationError("invalid_concept_markdown", "concept Markdown must use LF and end with a newline")
    try:
        content.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SynthesisValidationError("invalid_concept_markdown", "concept Markdown is not UTF-8") from exc
    if "\x00" in content:
        raise SynthesisValidationError("invalid_concept_markdown", "concept Markdown contains NUL")


def _citation_links(content: str) -> list[tuple[str, str]]:
    return re.findall(r"\[([^\]]+)\]\((obsidian://[^)]+|https?://(?:www\.)?youtube\.com/[^)]+)\)", content)


def _citation_matches(link: tuple[str, str], source: SourceDocument) -> bool:
    label, uri = link
    parsed = urlparse(uri)
    if source.kind == "article":
        query = parse_qs(parsed.query)
        filepath = unquote(query.get("filepath", [""])[0])
        line = query.get("line", [""])[0]
        match = re.search(r"·\s*L(\d+)-L(\d+)$", label)
        if not match or not line.isdigit():
            return False
        start, end = map(int, match.groups())
        return filepath == source.relative_path and start >= 1 and end >= start and end <= len(render_markdown(source).splitlines()) and int(line) == start
    query = parse_qs(parsed.query)
    video = query.get("v", [""])[0]
    start = query.get("t", [""])[0].rstrip("s")
    match = re.search(r"·\s*(\d+:\d{2}(?::\d{2})?)–(\d+:\d{2}(?::\d{2})?)$", label)
    if not match or not start.isdigit() or not video:
        return False
    def seconds(value: str) -> int:
        parts = [int(part) for part in value.split(":")]
        return parts[-1] + (parts[-2] * 60 if len(parts) > 1 else 0) + (parts[-3] * 3600 if len(parts) > 2 else 0)
    begin, end = (seconds(value) for value in match.groups())
    transcript_times = [seconds(item) for item in re.findall(r"^###\s+(\d+:\d{2}(?::\d{2})?)–", source.content, re.MULTILINE)]
    transcript_ends = [seconds(item) for item in re.findall(r"^###\s+\d+:\d{2}(?::\d{2})?–(\d+:\d{2}(?::\d{2})?)", source.content, re.MULTILINE)]
    upper = max(transcript_ends or transcript_times or [0])
    canonical_video = parse_qs(urlparse(source.canonical_url).query).get("v", [""])[0]
    return video == canonical_video and begin >= 0 and end >= begin and end <= upper and int(start) == begin


def validate_adapter_result(adapter_result: AdapterResult | Mapping[str, Any]) -> AdapterResult:
    """Public strict parser useful for deterministic adapter fixtures."""
    return _strict_adapter_result(adapter_result)


def synthesize_controlled(adapter: Any, batch_sources: list[SourceDocument], concept_catalog: Mapping[str, Any], workspace: Path, *, vault: Path | None = None, committed_sources: list[SourceDocument] = ()) -> SynthesisOutcome | SynthesisFailure:
    """Convenience entry point for callers that do not need a long-lived runner."""
    return ControlledSynthesis(adapter).run(batch_sources, concept_catalog, workspace, vault=vault, committed_sources=committed_sources)
