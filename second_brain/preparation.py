from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol, cast

from .canonical import stable_id
from .config import Config
from .extraction import ExtractionError, ImageProcessor, extract_article
from .models import (
    AcquiredArticle,
    AcquiredYouTube,
    ArticleEvidenceBounds,
    Job,
    PreparationFailure,
    PreparationFailureCategory,
    PublicationIntent,
    SourceCandidate,
    SourceCandidateRecord,
    SourceCandidateLifecycle,
    SourceDocument,
    SourceKind,
    YouTubeEvidenceBounds,
)
from .provenance import transcript_time_bounds
from .render import render_markdown, render_source
from .state import StateStore
from .validation import ValidationError, validate_markdown
from .youtube import TranscriptError, render_video_source


class PreparationError(RuntimeError):
    def __init__(self, code: PreparationFailureCategory, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


class PreparationCrash(RuntimeError):
    """Raised by deterministic test failpoints at candidate durability boundaries."""


class PreparationFaults(Protocol):
    def hit(self, event: str, **details: Any) -> None: ...


class NoopPreparationFaults:
    def hit(self, event: str, **details: Any) -> None:
        return None


class SourcePreparation:
    """Deep preparation module that turns acquired inputs into durable candidates."""

    def __init__(self, config: Config, state: StateStore, image_processor: ImageProcessor | None = None, *, faults: PreparationFaults | None = None):
        self.config = config
        self.state = state
        self.image_processor = image_processor
        self.faults = faults or NoopPreparationFaults()
        self.root = config.state_dir / "source_candidates"

    def prepare(self, job: Job, payload: AcquiredArticle | AcquiredYouTube) -> SourceCandidate | PreparationFailure:
        try:
            if job.status not in {"claimed", "processing"}:
                raise PreparationError(PreparationFailureCategory.INVALID_INPUT, f"job {job.id} is not claimed for preparation", retryable=False)
            if not isinstance(payload, (AcquiredArticle, AcquiredYouTube)):
                raise PreparationError(PreparationFailureCategory.INVALID_INPUT, "acquired payload has an unsupported type", retryable=False)

            existing = self.state.source_candidate(job.id)
            if existing:
                candidate = self._load_record(job, existing)
                if isinstance(candidate, SourceCandidate) and candidate.input_fingerprint == self._input_fingerprint(payload):
                    self._ensure_vault_path_available(candidate)
                    return candidate
                raise PreparationError(PreparationFailureCategory.PATH_COLLISION, f"persisted source candidate for job {job.id} does not match the acquired payload", retryable=False)

            if job.status == "claimed":
                self.state.processing(job.id)
                job = self.state.get(job.id) or job

            expected_input = self._input_fingerprint(payload)
            artifact = self.root / job.id
            if artifact.exists():
                candidate = self._load_artifact(job, artifact)
                if candidate.input_fingerprint != expected_input:
                    raise PreparationError(PreparationFailureCategory.PATH_COLLISION, f"candidate artifact already exists for job {job.id} with different input", retryable=False)
                self._ensure_vault_path_available(candidate)
                self._record(job, candidate)
                return candidate

            version = self._allocate_version(job)
            source = self._render(job, payload, version)
            rendered = render_markdown(source).encode("utf-8")
            try:
                validate_markdown(rendered.decode("utf-8"), source=source)
            except (UnicodeDecodeError, ValidationError) as exc:
                raise PreparationError(PreparationFailureCategory.EXTRACTION_FAILED, f"rendered source validation failed: {exc}") from exc
            bounds = self._evidence_bounds(source, rendered)
            candidate = self._candidate(
                job,
                source,
                rendered,
                bounds,
                payload.publication_intent,
                expected_input,
                artifact,
            )
            self._ensure_vault_path_available(candidate)
            self._persist(job, candidate)
            self._record(job, candidate)
            self._fault("candidate_recorded", job_id=job.id, source_identity=candidate.source_identity)
            return candidate
        except PreparationCrash:
            raise
        except PreparationError as exc:
            self._fail(job, exc)
            return PreparationFailure(job.id, exc.code, exc.message, exc.retryable)
        except (ExtractionError, TranscriptError) as exc:
            failure = PreparationError(
                PreparationFailureCategory.TRANSCRIPT_MISSING if isinstance(exc, TranscriptError) else PreparationFailureCategory.EXTRACTION_FAILED,
                str(exc),
            )
            self._fail(job, failure)
            return PreparationFailure(job.id, failure.code, failure.message, failure.retryable)
        except (OSError, UnicodeError, ValueError, TypeError) as exc:
            failure = PreparationError(PreparationFailureCategory.PERSISTENCE_FAILED, str(exc))
            self._fail(job, failure)
            return PreparationFailure(job.id, failure.code, failure.message, failure.retryable)

    def load(self, job: Job) -> SourceCandidate | PreparationFailure:
        try:
            if job.status != "source_ready":
                raise PreparationError(PreparationFailureCategory.INVALID_INPUT, f"job {job.id} is not source_ready", retryable=False)
            record = self.state.source_candidate(job.id)
            if not record:
                raise PreparationError(PreparationFailureCategory.CANDIDATE_MISSING, f"source candidate is missing for job {job.id}")
            candidate = self._load_record(job, record)
            if not isinstance(candidate, SourceCandidate):
                raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, "source candidate could not be rehydrated", retryable=False)
            return candidate
        except PreparationError as exc:
            return PreparationFailure(job.id, exc.code, exc.message, exc.retryable)
        except (OSError, UnicodeError, ValueError, TypeError) as exc:
            return PreparationFailure(job.id, PreparationFailureCategory.CANDIDATE_CORRUPT, str(exc), False)

    def compact(self, job_id: str) -> None:
        """Remove rendered bytes after publication while retaining the manifest."""
        record = self.state.source_candidate(job_id)
        if not record:
            return
        artifact = self._safe_artifact(Path(record.artifact_path))
        payload = artifact / "source.md"
        if payload.is_symlink():
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate payload is a symlink: {payload}", retryable=False)
        if payload.exists():
            if not payload.is_file():
                raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate payload is not a file: {payload}", retryable=False)
            payload.unlink()
            self._fsync_dir(payload.parent)
        self.state.mark_source_candidate_payload_removed(job_id)

    def _record(self, job: Job, candidate: SourceCandidate) -> None:
        try:
            self.state.write_source_candidate(
                job.id,
                source_identity=candidate.source_identity,
                source_version=candidate.source_version,
                relative_path=candidate.relative_path,
                artifact_path=candidate.artifact_path,
                manifest_path=candidate.manifest_path,
                manifest_hash=candidate.manifest_hash,
                rendered_hash=candidate.rendered_hash,
                content_hash=candidate.content_hash,
            )
        except ValueError as exc:
            raise PreparationError(PreparationFailureCategory.PATH_COLLISION, str(exc), retryable=False) from exc

    def _fail(self, job: Job, failure: PreparationError) -> None:
        try:
            current = self.state.get(job.id)
            if current and current.status != "complete":
                self.state.fail(job.id, failure.code.value, failure.message, retryable=failure.retryable)
        except (KeyError, ValueError):
            return None

    def _render(self, job: Job, payload: AcquiredArticle | AcquiredYouTube, version: int) -> SourceDocument:
        source_id = stable_id(job.kind, job.source_key)
        captured_at = job.captured_at or "1970-01-01T00:00:00+00:00"
        if isinstance(payload, AcquiredArticle):
            extracted = extract_article(payload.html, self.image_processor)
            return render_source(
                source_id=source_id,
                kind="article",
                canonical_url=job.original_locator,
                title=extracted["title"],
                body=extracted["body"],
                author=None,
                publication_date=None,
                captured_at=captured_at,
                input_method=payload.input_method,
                source_version=version,
            )
        return render_video_source(payload.video, captured_at, version)

    def _allocate_version(self, job: Job) -> int:
        if job.source_version:
            return job.source_version
        source_id = stable_id(job.kind, job.source_key)
        versions = set(self.state.source_versions(source_id))
        root = self.config.sources / ("Articles" if job.kind == "article" else "YouTube")
        if root.exists():
            if root.is_symlink() or not root.is_dir():
                raise PreparationError(PreparationFailureCategory.VERSION_COLLISION, f"source directory is not a regular directory: {root}", retryable=False)
            for path in root.iterdir():
                if path.is_symlink() or not path.is_file():
                    if path.name.startswith(source_id):
                        raise PreparationError(PreparationFailureCategory.VERSION_COLLISION, f"unsafe existing source path: {path}", retryable=False)
                    continue
                if path.name == f"{source_id}.md":
                    self._verify_existing_source(path, job, source_id, 1)
                    versions.add(1)
                else:
                    match = re.fullmatch(re.escape(source_id) + r"-v(\d+)\.md", path.name)
                    if match:
                        version = int(match.group(1))
                        if version < 2:
                            raise PreparationError(PreparationFailureCategory.VERSION_COLLISION, f"invalid immutable source version in path: {path.name}", retryable=False)
                        self._verify_existing_source(path, job, source_id, version)
                        versions.add(version)
                    elif path.name.startswith(source_id) and path.suffix == ".md":
                        raise PreparationError(PreparationFailureCategory.VERSION_COLLISION, f"ambiguous existing source path: {path.name}", retryable=False)
        return max(versions, default=0) + 1

    def _verify_existing_source(self, path: Path, job: Job, source_id: str, version: int) -> None:
        """Treat a live source version as allocated only when its bytes verify."""
        try:
            text = path.read_text(encoding="utf-8")
            validate_markdown(text)
            header, body = text[4:].split("\n---\n", 1)
            metadata: dict[str, Any] = {}
            for line in header.splitlines():
                key, raw = line.split(":", 1)
                metadata[key] = None if raw.strip() == "null" else json.loads(raw.strip())
            kind_value = metadata["source_type"]
            if kind_value not in {"article", "youtube"}:
                raise ValueError("invalid source type")
            kind = cast(SourceKind, kind_value)
            rendered_body = body.removeprefix("\n")
            marker = f"# {metadata['title']}\n\n"
            if not rendered_body.startswith(marker):
                raise ValueError("existing source body does not match title")
            source = render_source(
                source_id=str(metadata["source_id"]),
                kind=kind,
                canonical_url=str(metadata["canonical_url"]),
                title=str(metadata["title"]),
                body=rendered_body[len(marker):].removesuffix("\n"),
                author=metadata["author"] if isinstance(metadata["author"], str) else None,
                publication_date=metadata["publication_date"] if isinstance(metadata["publication_date"], str) else None,
                captured_at=str(metadata["captured_at"]),
                input_method=str(metadata["input_method"]),
                source_version=int(metadata["immutable_source_version"]),
            )
            if (
                source.source_id != source_id
                or source.kind != job.kind
                or source.canonical_url != job.original_locator
                or source.source_version != version
                or source.relative_path != path.relative_to(self.config.vault).as_posix()
                or render_markdown(source) != text
            ):
                raise ValueError("existing source bytes do not match immutable identity")
        except (OSError, UnicodeError, KeyError, TypeError, ValueError, ValidationError, json.JSONDecodeError) as exc:
            raise PreparationError(PreparationFailureCategory.VERSION_COLLISION, f"existing source version is corrupt: {path.name}", retryable=False) from exc

    def _ensure_vault_path_available(self, candidate: SourceCandidate) -> None:
        target = self.config.vault / candidate.relative_path
        if target.exists() or target.is_symlink():
            raise PreparationError(PreparationFailureCategory.VERSION_COLLISION, f"immutable source path already exists: {candidate.relative_path}", retryable=False)

    def _candidate(
        self,
        job: Job,
        source: SourceDocument,
        rendered: bytes,
        bounds: ArticleEvidenceBounds | YouTubeEvidenceBounds,
        intent: PublicationIntent,
        input_fingerprint: str,
        artifact: Path,
    ) -> SourceCandidate:
        rendered_hash = hashlib.sha256(rendered).hexdigest()
        manifest = self._manifest(job.id, source, rendered_hash, bounds, intent, input_fingerprint)
        manifest_bytes = self._manifest_bytes(manifest)
        return SourceCandidate(
            job_id=job.id,
            source=source,
            source_identity=f"{source.source_id}:v{source.source_version}",
            artifact_path=str(artifact),
            manifest_path=str(artifact / "manifest.json"),
            manifest_hash=hashlib.sha256(manifest_bytes).hexdigest(),
            input_fingerprint=input_fingerprint,
            rendered_markdown=rendered,
            rendered_hash=rendered_hash,
            evidence_bounds=bounds,
            publication_intent=intent,
        )

    def _persist(self, job: Job, candidate: SourceCandidate) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._safe_artifact(Path(candidate.artifact_path))
        artifact = Path(candidate.artifact_path)
        if artifact.exists():
            existing = self._load_artifact(job, artifact)
            if existing.rendered_hash != candidate.rendered_hash or existing.manifest_hash != candidate.manifest_hash:
                raise PreparationError(PreparationFailureCategory.PATH_COLLISION, f"candidate artifact already exists: {artifact}", retryable=False)
            return
        temporary = Path(tempfile.mkdtemp(prefix=f".{candidate.job_id}.", dir=self.root))
        try:
            self._write_bytes(temporary / "source.md", candidate.rendered_markdown)
            manifest = self._manifest_for_candidate(candidate)
            self._write_bytes(temporary / "manifest.json", self._manifest_bytes(manifest))
            self._fsync_dir(temporary)
            self._fault("candidate_files_persisted", job_id=candidate.job_id)
            os.replace(temporary, artifact)
            self._fsync_dir(self.root)
            self._fault("candidate_installed", job_id=candidate.job_id)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def _load_record(self, job: Job, record: SourceCandidateRecord) -> SourceCandidate | PreparationFailure:
        artifact = self._safe_artifact(Path(record.artifact_path))
        if record.lifecycle == SourceCandidateLifecycle.PAYLOAD_REMOVED:
            raise PreparationError(PreparationFailureCategory.CANDIDATE_MISSING, f"prepared bytes were already consumed for job {job.id}")
        candidate = self._load_artifact(job, artifact, record)
        self._ensure_vault_path_available(candidate)
        return candidate

    def _load_artifact(self, job: Job, artifact: Path, record: SourceCandidateRecord | None = None) -> SourceCandidate:
        artifact = self._safe_artifact(artifact)
        if artifact.is_symlink() or not artifact.is_dir():
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate artifact is not a regular directory: {artifact}", retryable=False)
        for child in artifact.iterdir():
            if child.is_symlink() or child.name not in {"manifest.json", "source.md"}:
                raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate contains an unexpected entry: {child.name}", retryable=False)
        manifest_path = artifact / "manifest.json"
        payload_path = artifact / "source.md"
        if not manifest_path.is_file() or manifest_path.is_symlink() or not payload_path.is_file() or payload_path.is_symlink():
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate is incomplete: {artifact}", retryable=False)
        manifest_bytes = manifest_path.read_bytes()
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        if record and manifest_hash != record.manifest_hash:
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate manifest hash changed: {job.id}", retryable=False)
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8"))
            if manifest.get("format_version") != 1:
                raise ValueError("unsupported candidate manifest format")
            source_data = manifest["source"]
            kind = source_data["kind"]
            if kind not in {"article", "youtube"}:
                raise ValueError("invalid source kind")
            source = SourceDocument(
                source_id=str(source_data["source_id"]),
                kind=cast(SourceKind, kind),
                canonical_url=str(source_data["canonical_url"]),
                title=str(source_data["title"]),
                content=str(source_data["content"]),
                metadata=dict(source_data["metadata"]),
                relative_path=str(source_data["relative_path"]),
                content_hash=str(source_data["content_hash"]),
                source_version=int(source_data["source_version"]),
            )
            rendered = payload_path.read_bytes()
            rendered_hash = hashlib.sha256(rendered).hexdigest()
            if rendered_hash != str(manifest["rendered_hash"]):
                raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate bytes changed: {job.id}", retryable=False)
            text = rendered.decode("utf-8")
            validate_markdown(text, source=source)
            author = source.metadata.get("author")
            publication_date = source.metadata.get("publication_date")
            captured_at = source.metadata.get("captured_at")
            input_method = source.metadata.get("input_method")
            if any(value is not None and not isinstance(value, str) for value in (author, publication_date, captured_at, input_method)):
                raise ValueError("source metadata has invalid scalar types")
            reconstructed = render_source(
                source_id=source.source_id,
                kind=source.kind,
                canonical_url=source.canonical_url,
                title=source.title,
                body=source.content,
                author=author if isinstance(author, str) else None,
                publication_date=publication_date if isinstance(publication_date, str) else None,
                captured_at=captured_at if isinstance(captured_at, str) else "",
                input_method=input_method if isinstance(input_method, str) else "",
                source_version=source.source_version,
            )
            if reconstructed != source:
                raise ValueError("source metadata does not match deterministic rendering")
            bounds = self._bounds_from_manifest(manifest["evidence_bounds"])
            try:
                expected_bounds = self._evidence_bounds(source, rendered)
            except PreparationError as exc:
                raise ValueError("source evidence bounds cannot be derived") from exc
            if bounds != expected_bounds:
                raise ValueError("source evidence bounds do not match rendered bytes")
            intent = PublicationIntent(**manifest.get("publication_intent", {}))
            candidate = SourceCandidate(
                job_id=str(manifest["job_id"]),
                source=source,
                source_identity=str(manifest["source_identity"]),
                artifact_path=str(artifact),
                manifest_path=str(manifest_path),
                manifest_hash=manifest_hash,
                input_fingerprint=str(manifest["input_fingerprint"]),
                rendered_markdown=rendered,
                rendered_hash=rendered_hash,
                evidence_bounds=bounds,
                publication_intent=intent,
            )
        except PreparationError:
            raise
        except (KeyError, TypeError, ValueError, UnicodeError, ValidationError) as exc:
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate manifest is invalid: {job.id}", retryable=False) from exc
        expected_source_id = stable_id(job.kind, job.source_key)
        if (
            candidate.job_id != job.id
            or source.source_id != expected_source_id
            or source.kind != job.kind
            or source.canonical_url != job.original_locator
            or (job.source_version is not None and source.source_version != job.source_version)
            or (job.content_hash is not None and source.content_hash != job.content_hash)
            or candidate.source_identity != f"{source.source_id}:v{source.source_version}"
            or candidate.relative_path != source.relative_path
        ):
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate identity is invalid: {job.id}", retryable=False)
        if source.source_version < 1:
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate version is invalid: {job.id}", retryable=False)
        if isinstance(candidate.evidence_bounds, ArticleEvidenceBounds):
            if candidate.evidence_bounds.first_line < 1 or candidate.evidence_bounds.last_line < candidate.evidence_bounds.first_line:
                raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate article bounds are invalid: {job.id}", retryable=False)
        elif candidate.evidence_bounds.first_second < 0 or candidate.evidence_bounds.last_second < candidate.evidence_bounds.first_second:
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate transcript bounds are invalid: {job.id}", retryable=False)
        if record and (
            candidate.source_identity != record.source_identity
            or candidate.source_version != record.source_version
            or candidate.relative_path != record.relative_path
            or candidate.artifact_path != record.artifact_path
            or candidate.manifest_path != record.manifest_path
            or candidate.manifest_hash != record.manifest_hash
            or candidate.rendered_hash != record.rendered_hash
        ):
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate record does not match its manifest: {job.id}", retryable=False)
        return candidate

    @staticmethod
    def _manifest(job_id: str, source: SourceDocument, rendered_hash: str, bounds: ArticleEvidenceBounds | YouTubeEvidenceBounds, intent: PublicationIntent, input_fingerprint: str) -> dict[str, Any]:
        return {
            "format_version": 1,
            "job_id": job_id,
            "source_identity": f"{source.source_id}:v{source.source_version}",
            "source": {
                "source_id": source.source_id,
                "kind": source.kind,
                "canonical_url": source.canonical_url,
                "title": source.title,
                "content": source.content,
                "metadata": source.metadata,
                "relative_path": source.relative_path,
                "content_hash": source.content_hash,
                "source_version": source.source_version,
            },
            "rendered_hash": rendered_hash,
            "input_fingerprint": input_fingerprint,
            "evidence_bounds": asdict(bounds),
            "publication_intent": asdict(intent),
        }

    @staticmethod
    def _manifest_for_candidate(candidate: SourceCandidate) -> dict[str, Any]:
        return SourcePreparation._manifest(candidate.job_id, candidate.source, candidate.rendered_hash, candidate.evidence_bounds, candidate.publication_intent, candidate.input_fingerprint)

    @staticmethod
    def _manifest_bytes(manifest: dict[str, Any]) -> bytes:
        return (json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    @staticmethod
    def _input_fingerprint(payload: AcquiredArticle | AcquiredYouTube) -> str:
        if isinstance(payload, AcquiredArticle):
            value = {"kind": "article", "html": payload.html, "input_method": payload.input_method, "intent": asdict(payload.publication_intent)}
        else:
            value = {"kind": "youtube", "video": asdict(payload.video), "intent": asdict(payload.publication_intent)}
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _evidence_bounds(source: SourceDocument, rendered: bytes) -> ArticleEvidenceBounds | YouTubeEvidenceBounds:
        if source.kind == "article":
            lines = rendered.splitlines()
            return ArticleEvidenceBounds(1, len(lines))
        bounds = transcript_time_bounds(rendered.decode("utf-8"))
        if bounds is None:
            raise PreparationError(PreparationFailureCategory.TRANSCRIPT_MISSING, "rendered transcript contains no timestamp bounds")
        return YouTubeEvidenceBounds(*bounds)

    @staticmethod
    def _bounds_from_manifest(value: Any) -> ArticleEvidenceBounds | YouTubeEvidenceBounds:
        if not isinstance(value, dict):
            raise ValueError("evidence bounds are malformed")
        if "first_line" in value and "last_line" in value:
            return ArticleEvidenceBounds(int(value["first_line"]), int(value["last_line"]))
        return YouTubeEvidenceBounds(float(value["first_second"]), float(value["last_second"]))

    def _safe_artifact(self, path: Path) -> Path:
        root = self.root.resolve()
        if self.root.exists() and self.root.is_symlink():
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, "source candidate root is a symlink", retryable=False)
        try:
            resolved = path.resolve(strict=False)
            resolved.relative_to(root)
        except ValueError as exc:
            raise PreparationError(PreparationFailureCategory.CANDIDATE_CORRUPT, f"source candidate escapes state directory: {path}", retryable=False) from exc
        return path

    @staticmethod
    def _write_bytes(path: Path, content: bytes) -> None:
        with path.open("wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _fault(self, event: str, **details: Any) -> None:
        self.faults.hit(event, **details)


def build_source_preparation(config: Config, state: StateStore, image_processor: ImageProcessor | None = None, *, faults: PreparationFaults | None = None) -> SourcePreparation:
    return SourcePreparation(config, state, image_processor, faults=faults)


__all__ = [
    "AcquiredArticle",
    "AcquiredYouTube",
    "ArticleEvidenceBounds",
    "PreparationFailure",
    "PreparationFailureCategory",
    "PublicationIntent",
    "SourceCandidate",
    "SourcePreparation",
    "YouTubeEvidenceBounds",
    "build_source_preparation",
]
