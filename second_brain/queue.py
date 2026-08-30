from __future__ import annotations

import re
from pathlib import Path

from .canonical import InvalidLocator, canonical_url
from .models import ArticleInput


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
