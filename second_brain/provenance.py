from __future__ import annotations

from urllib.parse import quote


def article_citation(title: str, source_path: str, start_line: int, end_line: int, *, vault: str = "Second Brain") -> str:
    if start_line < 1 or end_line < start_line:
        raise ValueError("citation line range must be 1-indexed and ordered")
    display = f"{title} · L{start_line}-L{end_line}"
    target = f"obsidian://adv-uri?vault={quote(vault)}&filepath={quote(source_path, safe='')}&line={start_line}"
    return f"[{display}]({target})"
