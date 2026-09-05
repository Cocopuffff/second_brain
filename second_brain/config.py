from __future__ import annotations

import json
import os
import shutil
from dataclasses import MISSING, dataclass, field, fields
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, cast
from urllib.parse import urlsplit


def _environment_value(name: str) -> str | None:
    return os.environ.get(f"SECOND_BRAIN_{name.upper()}")


def _integer_setting(name: str, value: object) -> int:
    try:
        if isinstance(value, bool):
            raise TypeError
        return int(str(value))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _boolean_setting(name: str, value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    raise ValueError(f"{name} must be a boolean")


def _optional_path(value: object) -> Path | None:
    if value is None or value == "":
        return None
    try:
        return Path(str(value)).expanduser().resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise ValueError("credential file path is invalid") from exc


class OAuthFileKind(Enum):
    CLIENT = "client"
    TOKEN = "token"


def _valid_oauth_structure(value: object, kind: OAuthFileKind) -> bool:
    if not isinstance(value, dict):
        return False
    if kind is OAuthFileKind.CLIENT:
        credentials = value.get("installed") or value.get("web")
        required = ("client_id", "client_secret", "auth_uri", "token_uri")
    else:
        credentials = value
        required = ("refresh_token", "token_uri", "client_id", "client_secret")
    return isinstance(credentials, dict) and all(isinstance(credentials.get(key), str) and credentials[key].strip() for key in required)


class SettingKind(Enum):
    STRING = "string"
    OPTIONAL_STRING = "optional_string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    PATH = "path"
    OPTIONAL_PATH = "optional_path"


@dataclass(frozen=True)
class ConfigSetting:
    name: str
    default: object
    kind: SettingKind

    @property
    def cli_option(self) -> str:
        return "--" + self.name.replace("_", "-")


def _convert_setting(setting: ConfigSetting, value: object) -> object:
    if setting.kind is SettingKind.STRING:
        return str(value)
    if setting.kind is SettingKind.OPTIONAL_STRING:
        return _optional_string(value)
    if setting.kind is SettingKind.INTEGER:
        return _integer_setting(setting.name, value)
    if setting.kind is SettingKind.BOOLEAN:
        return _boolean_setting(setting.name, value)
    if setting.kind is SettingKind.PATH:
        try:
            return Path(str(value)).expanduser().resolve()
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise ValueError(f"{setting.name} path is invalid") from exc
    if setting.kind is SettingKind.OPTIONAL_PATH:
        return _optional_path(value)
    raise AssertionError(f"unsupported configuration setting kind: {setting.kind}")


def _resolve_setting(setting: ConfigSetting, values: Mapping[str, object], cli_values: Mapping[str, object]) -> object:
    if setting.name in cli_values and cli_values[setting.name] is not None:
        value = cli_values[setting.name]
    elif setting.name in values:
        value = values[setting.name]
    else:
        environment_value = _environment_value(setting.name)
        value = setting.default if environment_value is None else environment_value
    return _convert_setting(setting, value)


@dataclass(frozen=True)
class Config:
    vault: Path = field(metadata={"config_kind": SettingKind.PATH, "config_default": "."})
    state_dir: Path = field(
        metadata={
            "config_kind": SettingKind.PATH,
            "config_default": Path.home() / ".local" / "state" / "second-brain",
        }
    )
    youtube_playlist: str = field(default="To Ingest", metadata={"config_kind": SettingKind.STRING})
    youtube_enabled: bool = field(default=False, metadata={"config_kind": SettingKind.BOOLEAN})
    executor: str = field(default="noop", metadata={"config_kind": SettingKind.STRING})
    deepseek_base_url: str = field(default="https://api.deepseek.com", metadata={"config_kind": SettingKind.STRING})
    deepseek_model: str | None = field(default=None, metadata={"config_kind": SettingKind.OPTIONAL_STRING})
    request_timeout: int = field(default=30, metadata={"config_kind": SettingKind.INTEGER})
    codex_executable: str = field(default="codex", metadata={"config_kind": SettingKind.STRING})
    codex_model: str | None = field(default=None, metadata={"config_kind": SettingKind.OPTIONAL_STRING})
    legacy_codex_command: bool = False
    youtube_playlist_id: str | None = field(default=None, metadata={"config_kind": SettingKind.OPTIONAL_STRING})
    youtube_client_file: Path | None = field(default=None, metadata={"config_kind": SettingKind.OPTIONAL_PATH})
    youtube_token_file: Path | None = field(default=None, metadata={"config_kind": SettingKind.OPTIONAL_PATH})
    image_processor: str = field(default="caption", metadata={"config_kind": SettingKind.STRING})
    deepseek_image_model: str | None = field(default=None, metadata={"config_kind": SettingKind.OPTIONAL_STRING})
    max_redirects: int = field(default=5, metadata={"config_kind": SettingKind.INTEGER})
    max_response_bytes: int = field(default=10_000_000, metadata={"config_kind": SettingKind.INTEGER})
    max_image_bytes: int = field(default=5_000_000, metadata={"config_kind": SettingKind.INTEGER})

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
    def load(
        cls,
        vault: Path | None = None,
        state_dir: Path | None = None,
        config_path: Path | None = None,
        cli_values: Mapping[str, object] | None = None,
    ) -> "Config":
        values: dict[str, object] = {}
        if config_path is not None:
            if not config_path.exists():
                raise ValueError("configuration file does not exist")
            try:
                loaded = json.loads(config_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise ValueError("configuration file is not valid UTF-8 JSON") from exc
            if not isinstance(loaded, dict):
                raise ValueError("configuration file must contain a JSON object")
            unknown = sorted(set(loaded) - set(_CONFIG_SETTINGS_BY_NAME) - {"codex_command"})
            if unknown:
                raise ValueError(f"unknown configuration setting: {unknown[0]}")
            values = loaded
        cli = dict(cli_values or {})
        unknown_cli = sorted(set(cli) - set(_CONFIG_SETTINGS_BY_NAME))
        if unknown_cli:
            raise ValueError(f"unknown CLI configuration setting: {unknown_cli[0]}")
        if vault is not None:
            cli["vault"] = vault
        if state_dir is not None:
            cli["state_dir"] = state_dir
        resolved = {
            setting.name: _resolve_setting(setting, values, cli)
            for setting in CONFIG_SETTINGS
        }
        return cls(
            **cast(dict[str, Any], resolved),
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
        if not isinstance(self.deepseek_base_url, str) or not self._valid_http_url(self.deepseek_base_url):
            errors.append("deepseek_base_url must be an absolute HTTP(S) URL")
        if not isinstance(self.request_timeout, int) or isinstance(self.request_timeout, bool) or self.request_timeout <= 0:
            errors.append("request_timeout must be greater than zero")
        if not isinstance(self.max_redirects, int) or isinstance(self.max_redirects, bool) or self.max_redirects < 0:
            errors.append("max_redirects must be zero or greater")
        if not isinstance(self.max_response_bytes, int) or isinstance(self.max_response_bytes, bool) or self.max_response_bytes <= 0:
            errors.append("max_response_bytes must be greater than zero")
        if not isinstance(self.max_image_bytes, int) or isinstance(self.max_image_bytes, bool) or self.max_image_bytes <= 0:
            errors.append("max_image_bytes must be greater than zero")
        if self.image_processor not in {"caption", "deepseek"}:
            errors.append("unsupported image_processor")
        if self.image_processor == "deepseek":
            if not self.deepseek_image_model:
                errors.append("deepseek_image_model is required for the deepseek image processor")
            if not os.environ.get("DEEPSEEK_API_KEY"):
                errors.append("DEEPSEEK_API_KEY is required for the deepseek image processor")
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
        if self.youtube_enabled:
            if not self.youtube_playlist_id:
                errors.append("youtube_playlist_id is required when YouTube is enabled")
            self._validate_credential_file(self.youtube_client_file, OAuthFileKind.CLIENT, errors)
            self._validate_credential_file(self.youtube_token_file, OAuthFileKind.TOKEN, errors)
        return errors

    def _validate_credential_file(self, path: Path | None, kind: OAuthFileKind, errors: list[str]) -> None:
        label = kind.value
        if path is None:
            errors.append(f"YouTube OAuth {label} file is required")
            return
        try:
            path.resolve().relative_to(self.vault.resolve())
        except ValueError:
            pass
        else:
            errors.append("YouTube OAuth files must remain outside the vault")
            return
        if not path.exists():
            errors.append(f"YouTube OAuth {label} file is missing")
        elif not path.is_file():
            errors.append(f"YouTube OAuth {label} path is not a file")
        else:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                errors.append(f"YouTube OAuth {label} file is not valid UTF-8 JSON")
            else:
                if not _valid_oauth_structure(value, kind):
                    errors.append(f"YouTube OAuth {label} file has invalid structure")

    @staticmethod
    def _valid_http_url(value: str) -> bool:
        try:
            parts = urlsplit(value.strip())
            hostname = parts.hostname
            parts.port
        except ValueError:
            return False
        return parts.scheme in {"http", "https"} and bool(hostname)


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _settings_from_config_fields() -> tuple[ConfigSetting, ...]:
    settings: list[ConfigSetting] = []
    for config_field in fields(Config):
        kind = config_field.metadata.get("config_kind")
        if not isinstance(kind, SettingKind):
            continue
        default = config_field.metadata.get("config_default", config_field.default)
        if default is MISSING:
            raise AssertionError(f"configuration setting has no default: {config_field.name}")
        settings.append(ConfigSetting(config_field.name, default, kind))
    return tuple(settings)


CONFIG_SETTINGS = _settings_from_config_fields()
_CONFIG_SETTINGS_BY_NAME = {setting.name: setting for setting in CONFIG_SETTINGS}
