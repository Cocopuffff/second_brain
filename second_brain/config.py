from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    vault: Path
    state_dir: Path
    youtube_playlist: str = "To Ingest"
    executor: str = "noop"
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str | None = None
    request_timeout: int = 30
    codex_executable: str = "codex"
    codex_model: str | None = None
    legacy_codex_command: bool = False

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
        return cls(
            vault=resolved_vault,
            state_dir=Path(values.get("state_dir", state_dir or default_state)).expanduser().resolve(),
            youtube_playlist=values.get("youtube_playlist", "To Ingest"),
            executor=values.get("executor", "noop"),
            deepseek_base_url=values.get("deepseek_base_url", "https://api.deepseek.com"),
            deepseek_model=values.get("deepseek_model"),
            request_timeout=int(values.get("request_timeout", 30)),
            codex_executable=str(values.get("codex_executable", "codex")),
            codex_model=values.get("codex_model"),
            legacy_codex_command="codex_command" in values,
        )

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.vault.exists():
            errors.append(f"vault does not exist: {self.vault}")
        try:
            self.state_dir.relative_to(self.vault)
        except ValueError:
            pass
        else:
            errors.append("state directory must remain outside the vault")
        if self.vault.exists() and not (self.vault / ".git").exists():
            errors.append(f"vault is not a local Git worktree: {self.vault}")
        if not (self.vault / "To Ingest.md").exists() and not (self.to_ingest / "To Ingest.md").exists():
            errors.append("missing To Ingest.md or ToIngest/To Ingest.md")
        if self.executor not in {"noop", "deepseek", "codex"}:
            errors.append(f"unsupported executor: {self.executor}")
        if self.legacy_codex_command:
            errors.append("codex_command was removed; replace it with codex_executable and optional codex_model")
        if self.executor == "deepseek":
            if not self.deepseek_model:
                errors.append("deepseek_model is required for the deepseek executor")
            if not os.environ.get("DEEPSEEK_API_KEY"):
                errors.append("DEEPSEEK_API_KEY is required for the deepseek executor")
        if self.executor == "codex":
            if not self.codex_executable.strip():
                errors.append("codex_executable is required for the codex executor")
            elif shutil.which(self.codex_executable) is None and not Path(self.codex_executable).is_file():
                errors.append(f"codex executable is unavailable: {self.codex_executable}")
        return errors
