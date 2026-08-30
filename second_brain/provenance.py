from __future__ import annotations

import re
from urllib.parse import quote


def article_citation(title: str, source_path: str, start_line: int, end_line: int, *, vault: str = "Second Brain") -> str:
    if start_line < 1 or end_line < start_line:
        raise ValueError("citation line range must be 1-indexed and ordered")
    display = f"{title} · L{start_line}-L{end_line}"
    target = f"obsidian://adv-uri?vault={quote(vault)}&filepath={quote(source_path, safe='')}&line={start_line}"
    return f"[{display}]({target})"


def transcript_time_bounds(content: str) -> tuple[float, float] | None:
    """Return the first transcript start and final transcript end in seconds."""
    starts: list[float] = []
    ends: list[float] = []
    for start, end in re.findall(r"^###\s+(\d+:\d{2}(?::\d{2})?)–(\d+:\d{2}(?::\d{2})?)", content, re.MULTILINE):
        starts.append(timestamp_seconds(start))
        ends.append(timestamp_seconds(end))
    if not starts:
        return None
    return starts[0], ends[-1]


def timestamp_seconds(value: str) -> float:
    parts = [int(part) for part in value.split(":")]
    return float(parts[-1] + (parts[-2] * 60 if len(parts) > 1 else 0) + (parts[-3] * 3600 if len(parts) > 2 else 0))
