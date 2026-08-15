from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

from .canonical import InvalidLocator, canonical_url, source_key
from .models import ArticleInput
from .state import StateStore


URL_LINE = re.compile(r"^\s*(https?://\S+)\s*$", re.IGNORECASE)


def read_article_queue(path: Path) -> tuple[list[ArticleInput], list[str]]:
    if not path.exists():
        return [], []
    inputs: list[ArticleInput] = []
    errors: list[str] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = raw.strip()
        if not stripped:
            continue
        match = URL_LINE.match(raw)
        if not match:
            errors.append(f"line {line_number}: unsupported capture preserved")
            continue
        try:
            url = canonical_url(match.group(1))
        except InvalidLocator as exc:
            errors.append(f"line {line_number}: {exc}")
            continue
        inputs.append(ArticleInput(url=url))
    return inputs, errors


def claim_article_queue(path: Path, state: StateStore, batch_id: str, *, acknowledge: bool = True) -> tuple[list[ArticleInput], list[str]]:
    inputs, errors = read_article_queue(path)
    claimed: list[ArticleInput] = []
    seen: set[str] = set()
    for item in inputs:
        key = source_key("article", item.url)
        if key in seen:
            continue
        seen.add(key)
        state.claim("article", key, item.url, input_artifact=None, batch_id=batch_id)
        claimed.append(item)
    if claimed and acknowledge:
        _remove_claimed_urls(path, {canonical_url(item.url) for item in claimed})
    return claimed, errors


def _remove_claimed_urls(path: Path, claimed: set[str]) -> None:
    if not path.exists():
        return
    latest = path.read_text(encoding="utf-8")
    replacement = remove_claimed_urls_text(latest, claimed)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(replacement)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def remove_claimed_urls_text(latest: str, claimed: set[str]) -> str:
    retained: list[str] = []
    for line in latest.splitlines(keepends=True):
        match = URL_LINE.match(line.rstrip("\r\n"))
        if match:
            try:
                if canonical_url(match.group(1)) in claimed:
                    continue
            except InvalidLocator:
                pass
        retained.append(line)
    replacement = "".join(retained)
    if latest.endswith(("\n", "\r")) and replacement and not replacement.endswith("\n"):
        replacement += "\n"
    return replacement
