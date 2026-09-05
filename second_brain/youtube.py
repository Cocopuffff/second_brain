from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from .canonical import canonical_url, source_key, stable_id
from .models import SourceDocument, TranscriptSegment, VideoInput
from .render import render_source


class YouTubeAcknowledgement(StrEnum):
    REMOVED = "removed"
    ALREADY_ABSENT = "already_absent"


class YouTubePlaylistKind(StrEnum):
    FIXTURE = "fixture"
    PRODUCTION = "production"


@dataclass(frozen=True)
class YouTubePlaylistReference:
    kind: YouTubePlaylistKind
    value: str


@dataclass(frozen=True)
class YouTubePlaylistItem:
    video_id: str
    playlist_item_id: str | None


class YouTubeClient(Protocol):
    def list_playlist(self, playlist: YouTubePlaylistReference) -> list[YouTubePlaylistItem]: ...
    def acquire_video(self, video_id: str) -> VideoInput: ...
    def acknowledge(self, playlist_item_id: str) -> YouTubeAcknowledgement: ...


class TranscriptError(RuntimeError):
    pass


class FixtureYouTubeClient:
    def __init__(self, fixture: Path):
        self.fixture = fixture
        self.acknowledged: list[str] = []
        self.acknowledgement_attempts: list[str] = []
        self._removed: set[str] = set()
        self._discovered: dict[str, dict] = {}

    def list_playlist(self, playlist: YouTubePlaylistReference) -> list[YouTubePlaylistItem]:
        if playlist.kind is not YouTubePlaylistKind.FIXTURE:
            raise TranscriptError("fixture YouTube client requires a fixture playlist reference")
        data = json.loads(self.fixture.read_text(encoding="utf-8"))
        items = data.get(playlist.value, data if isinstance(data, list) else [])
        self._discovered.update((str(item["video_id"]), item) for item in items)
        return [
            YouTubePlaylistItem(
                video_id=str(item["video_id"]),
                playlist_item_id=item.get("playlist_item_id"),
            )
            for item in items
        ]

    def acquire_video(self, video_id: str) -> VideoInput:
        try:
            item = self._discovered[video_id]
        except KeyError as exc:
            raise TranscriptError(f"video was not discovered in the configured playlist: {video_id}") from exc
        return _video(item)

    def acknowledge(self, playlist_item_id: str) -> YouTubeAcknowledgement:
        self.acknowledgement_attempts.append(playlist_item_id)
        if playlist_item_id in self._removed:
            return YouTubeAcknowledgement.ALREADY_ABSENT
        self._removed.add(playlist_item_id)
        self.acknowledged.append(playlist_item_id)
        return YouTubeAcknowledgement.REMOVED


def _segments(values: list[dict]) -> tuple[TranscriptSegment, ...]:
    return tuple(TranscriptSegment(float(item["start"]), float(item.get("end", item["start"])), str(item["text"])) for item in values)


def _video(item: dict) -> VideoInput:
    return VideoInput(video_id=str(item["video_id"]), title=str(item.get("title", item["video_id"])), channel=item.get("channel"), published_at=item.get("published_at"), manual_transcript=_segments(item.get("manual_transcript", [])), automatic_transcript=_segments(item.get("automatic_transcript", [])), transcript_language=item.get("transcript_language"), playlist_item_id=item.get("playlist_item_id"))


def _stamp(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def render_video_source(video: VideoInput, captured_at: str, source_version: int = 1) -> SourceDocument:
    segments = video.manual_transcript or video.automatic_transcript
    if not segments:
        raise TranscriptError("no manual or automatic transcript is available")
    transcript_kind = "manual" if video.manual_transcript else "automatic"
    body = "\n\n".join(f"### {_stamp(segment.start_seconds)}–{_stamp(segment.end_seconds)}\n{segment.text.strip()}" for segment in segments if segment.text.strip())
    url = canonical_url(f"https://www.youtube.com/watch?v={video.video_id}")
    source = render_source(source_id=stable_id("youtube", source_key("youtube", url)), kind="youtube", canonical_url=url, title=video.title, body=body, author=video.channel, publication_date=video.published_at, captured_at=captured_at, input_method=f"youtube-playlist:{transcript_kind}", source_version=source_version)
    return source


def youtube_citation(title: str, video_id: str, start_seconds: float, end_seconds: float) -> str:
    return f"[{title} · {_stamp(start_seconds)}–{_stamp(end_seconds)}](https://www.youtube.com/watch?v={video_id}&t={int(start_seconds)}s)"
