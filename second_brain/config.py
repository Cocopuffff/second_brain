from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    vault: Path
    state_dir: Path
    youtube_playlist: str = "To Ingest"
    executor: str = "noop"
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-chat"
    request_timeout: int = 30
    codex_command: tuple[str, ...] = ()

    @property
    def to_ingest(self) -> Path:
        return self.vault / "ToIngest"

    @property
    def sources(self) -> Path:
        return self.vault / "Sources"

    @property
    def concepts(self) -> Path:
        return self.vault / "Concepts"

    @property
    def database(self) -> Path:
        return self.state_dir / "ingestion.sqlite3"

    @property
    def lockfile(self) -> Path:
        return self.state_dir / "batch.lock"

    @classmethod
    def load(cls, vault: Path, state_dir: Path | None = None, config_path: Path | None = None) -> "Config":
        values: dict = {}
        if config_path and config_path.exists():
            values = json.loads(config_path.read_text(encoding="utf-8"))
        resolved_vault = vault.resolve()
        default_state = Path(os.environ.get("SECOND_BRAIN_STATE_DIR", str(Path.home() / ".local" / "state" / "second-brain")))
        return cls(vault=resolved_vault, state_dir=Path(values.get("state_dir", state_dir or default_state)).expanduser().resolve(), youtube_playlist=values.get("youtube_playlist", "To Ingest"), executor=values.get("executor", "noop"), deepseek_base_url=values.get("deepseek_base_url", "https://api.deepseek.com"), deepseek_model=values.get("deepseek_model", "deepseek-chat"), request_timeout=int(values.get("request_timeout", 30)), codex_command=tuple(values.get("codex_command", [])))

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.vault.exists():
            errors.append(f"vault does not exist: {self.vault}")
        if not (self.vault / "To Ingest.md").exists() and not (self.to_ingest / "To Ingest.md").exists():
            errors.append("missing To Ingest.md or ToIngest/To Ingest.md")
        if self.executor not in {"noop", "deepseek", "codex"}:
            errors.append(f"unsupported executor: {self.executor}")
        if self.executor == "deepseek" and not os.environ.get("DEEPSEEK_API_KEY"):
            errors.append("DEEPSEEK_API_KEY is required for the deepseek executor")
        if self.executor == "codex" and not self.codex_command:
            errors.append("codex_command is required for the codex executor")
        return errors
