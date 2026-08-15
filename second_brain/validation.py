from __future__ import annotations

import re
from pathlib import Path

from .models import ChangeSet, SourceDocument
from .render import render_markdown


class ValidationError(ValueError):
    pass


REQUIRED_KEYS = ["source_id", "source_type", "canonical_url", "title", "author", "publication_date", "captured_at", "input_method", "content_hash", "source_format_version", "immutable_source_version"]


def validate_markdown(content: str, *, source: SourceDocument | None = None) -> None:
    if "\r" in content or not content.endswith("\n"):
        raise ValidationError("Markdown must use LF endings and a trailing newline")
    if not content.startswith("---\n"):
        if source:
            raise ValidationError("source Markdown is missing YAML frontmatter")
        return
    frontmatter = content.split("\n---\n", 1)[0].splitlines()[1:]
    keys = [line.split(":", 1)[0] for line in frontmatter if ":" in line]
    if keys != REQUIRED_KEYS:
        raise ValidationError(f"frontmatter keys are not in the fixed order: {keys}")
    if source and render_markdown(source) != content:
        raise ValidationError(f"rendered source does not match deterministic content for {source.source_id}")


def validate_changes(changes: ChangeSet, sources: list[SourceDocument], vault: Path) -> None:
    source_paths = {source.relative_path for source in sources}
    seen_paths: set[str] = set()
    for candidate in changes.files:
        path = Path(candidate.relative_path)
        if candidate.relative_path in seen_paths or candidate.relative_path.casefold() in {value.casefold() for value in seen_paths}:
            raise ValidationError(f"duplicate executor path: {candidate.relative_path}")
        seen_paths.add(candidate.relative_path)
        if path.is_absolute() or ".." in path.parts or not str(path).startswith("Concepts/"):
            raise ValidationError(f"executor write escapes Concepts/: {candidate.relative_path}")
        validate_markdown(candidate.content)
        if "Sources/" in candidate.content:
            unknown = [match for match in re.findall(r"Sources/[^\s)\]]+", candidate.content) if match.split("#", 1)[0] not in source_paths]
            if unknown:
                raise ValidationError(f"concept cites unknown source path: {unknown[0]}")
    for deletion in changes.deletions:
        path = Path(deletion)
        if deletion in seen_paths or deletion.casefold() in {value.casefold() for value in seen_paths}:
            raise ValidationError(f"duplicate executor path: {deletion}")
        seen_paths.add(deletion)
        if path.is_absolute() or ".." in path.parts or not (str(path).startswith("Concepts/") or str(path).startswith("Sources/")):
            raise ValidationError(f"executor deletion escapes the validated vault scopes: {deletion}")
    for source in sources:
        rendered = render_markdown(source)
        validate_markdown(rendered, source=source)
        if any(Path(candidate.relative_path).suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp"} for candidate in changes.files):
            raise ValidationError("image binaries are not valid candidate files")
