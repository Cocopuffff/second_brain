from __future__ import annotations

import hashlib
import os
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

from .canonical import InvalidLocator, canonical_url


class _MetadataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.canonical: str | None = None
        self.og_url: str | None = None
        self.title: str | None = None
        self._title_parts: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        if tag.lower() == "link" and values.get("rel", "").lower() == "canonical":
            self.canonical = values.get("href") or self.canonical
        if tag.lower() == "meta":
            if values.get("property", "").lower() == "og:url":
                self.og_url = values.get("content") or self.og_url
            if values.get("name", "").lower() == "twitter:url":
                self.og_url = values.get("content") or self.og_url
        if tag.lower() == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False
            self.title = " ".join("".join(self._title_parts).split())

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title_parts.append(data)


def _metadata(path: Path) -> _MetadataParser:
    parser = _MetadataParser()
    parser.feed(path.read_text(encoding="utf-8", errors="strict"))
    return parser


def _manifest(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        value = value.strip().strip("'\"")
        if key.strip() and value:
            result[key.strip()] = value
    return result


def safe_relative_html_files(to_ingest: Path) -> tuple[list[Path], list[str]]:
    files: list[Path] = []
    errors: list[str] = []
    root = to_ingest.resolve()
    if not to_ingest.exists():
        return [], []
    for path in sorted(to_ingest.iterdir()):
        if path.suffix.lower() not in {".html", ".htm"}:
            continue
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(root)
        except (FileNotFoundError, ValueError):
            errors.append(f"{path.name}: path escapes ToIngest or is unavailable")
            continue
        files.append(path)
    return files, errors


def discover_html(to_ingest: Path) -> tuple[list[tuple[Path, str, str]], list[str]]:
    manifest = _manifest(to_ingest / "HTML Pairings.yaml")
    files, errors = safe_relative_html_files(to_ingest)
    discovered: list[tuple[Path, str, str]] = []
    for path in files:
        try:
            metadata = _metadata(path)
        except UnicodeDecodeError:
            errors.append(f"{path.name}: HTML is not valid UTF-8")
            continue
        raw_candidates = [metadata.canonical, metadata.og_url, manifest.get(path.name)]
        raw_url = next((candidate for candidate in raw_candidates if candidate), None)
        if not raw_url:
            errors.append(f"{path.name}: no canonical, og:url, or explicit pairing metadata")
            continue
        url = None
        for candidate in raw_candidates:
            if not candidate:
                continue
            try:
                url = canonical_url(urljoin("https://invalid.local", candidate)) if candidate.startswith("/") else canonical_url(candidate)
                break
            except InvalidLocator:
                continue
        if url is None:
            errors.append(f"{path.name}: declared URL metadata is invalid")
            continue
        discovered.append((path, url, metadata.title or path.stem))
    return discovered, errors


def html_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
