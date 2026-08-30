from __future__ import annotations

import hashlib
import re
import textwrap
from datetime import date
from typing import Any

from .models import SourceDocument, SourceKind


SOURCE_FORMAT_VERSION = "1"


def _wrap_line(line: str) -> list[str]:
    if not line or line.startswith(("```", "|", ">", "- ", "1. ", "# ", "## ", "### ", "#### ", "##### ", "###### ")) or "http://" in line or "https://" in line:
        return [line]
    return textwrap.wrap(line, width=120, break_long_words=False, break_on_hyphens=False) or [""]


def normalize_body(body: str) -> str:
    lines: list[str] = []
    in_fence = False
    for raw in body.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw.rstrip()
        if line.startswith("```"):
            in_fence = not in_fence
        lines.extend([line] if in_fence else _wrap_line(line))
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip() + "\n"


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    value = str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{value}"'


def render_source(*, source_id: str, kind: SourceKind, canonical_url: str, title: str, body: str, author: str | None, publication_date: str | None, captured_at: str, input_method: str, source_version: int = 1) -> SourceDocument:
    normalized = normalize_body(body)
    stable_metadata = "\n".join([source_id, kind, canonical_url, title, author or "", publication_date or "", captured_at, input_method, str(source_version)])
    content_hash = hashlib.sha256((stable_metadata + "\n" + normalized).encode("utf-8")).hexdigest()
    metadata = {"source_id": source_id, "source_type": kind, "canonical_url": canonical_url, "title": title, "author": author, "publication_date": publication_date, "captured_at": captured_at, "input_method": input_method, "content_hash": content_hash, "source_format_version": SOURCE_FORMAT_VERSION, "immutable_source_version": source_version}
    folder = "Articles" if kind == "article" else "YouTube"
    return SourceDocument(source_id=source_id, kind=kind, canonical_url=canonical_url, title=title, content=normalized, metadata=metadata, relative_path=f"Sources/{folder}/{source_filename_version(source_id, source_version)}", content_hash=content_hash, source_version=source_version)


def render_markdown(document: SourceDocument) -> str:
    order = ["source_id", "source_type", "canonical_url", "title", "author", "publication_date", "captured_at", "input_method", "content_hash", "source_format_version", "immutable_source_version"]
    lines = ["---"] + [f"{key}: {_yaml_scalar(document.metadata.get(key))}" for key in order] + ["---", "", f"# {document.title}", "", document.content.rstrip(), ""]
    return "\n".join(lines)


def source_filename_version(source_id: str, version: int) -> str:
    return f"{source_id}-v{version}.md" if version > 1 else f"{source_id}.md"
