from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from dataclasses import fields
from pathlib import Path

import pytest

from second_brain.batch import BatchError, BatchRunner
from second_brain.config import CONFIG_SETTINGS, Config
from second_brain.cli import _config, build_parser, main


def test_every_nonlegacy_config_field_is_registered():
    configurable_fields = tuple(
        config_field.name
        for config_field in fields(Config)
        if config_field.name != "legacy_codex_command"
    )

    assert tuple(setting.name for setting in CONFIG_SETTINGS) == configurable_fields


def test_config_precedence_is_cli_then_json_then_environment_then_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "state_dir": str(tmp_path / "json-state"),
                "youtube_playlist": "json-playlist",
                "youtube_playlist_id": "json-playlist-id",
                "youtube_client_file": str(tmp_path / "json-client.json"),
                "youtube_token_file": str(tmp_path / "json-token.json"),
                "image_processor": "deepseek",
                "deepseek_image_model": "json-vision",
                "request_timeout": 22,
                "max_redirects": 6,
                "max_response_bytes": 2_000_000,
                "max_image_bytes": 300_000,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_PLAYLIST", "env-playlist")
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_PLAYLIST_ID", "env-playlist-id")
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_CLIENT_FILE", str(tmp_path / "env-client.json"))
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_TOKEN_FILE", str(tmp_path / "env-token.json"))
    monkeypatch.setenv("SECOND_BRAIN_IMAGE_PROCESSOR", "caption")
    monkeypatch.setenv("SECOND_BRAIN_DEEPSEEK_IMAGE_MODEL", "env-vision")
    monkeypatch.setenv("SECOND_BRAIN_REQUEST_TIMEOUT", "11")
    monkeypatch.setenv("SECOND_BRAIN_MAX_REDIRECTS", "4")
    monkeypatch.setenv("SECOND_BRAIN_MAX_RESPONSE_BYTES", "1000000")
    monkeypatch.setenv("SECOND_BRAIN_MAX_IMAGE_BYTES", "200000")

    config = Config.load(tmp_path / "vault", tmp_path / "state", config_path)

    assert config.state_dir == (tmp_path / "state").resolve()
    assert config.youtube_playlist == "json-playlist"
    assert config.youtube_playlist_id == "json-playlist-id"
    assert config.image_processor == "deepseek"
    assert config.deepseek_image_model == "json-vision"
    assert config.request_timeout == 22
    assert config.max_redirects == 6
    assert config.max_response_bytes == 2_000_000
    assert config.max_image_bytes == 300_000
    assert config.youtube_client_file == (tmp_path / "json-client.json").resolve()

    cli = {
        "youtube_playlist": "cli-playlist",
        "youtube_playlist_id": "cli-playlist-id",
        "youtube_client_file": str(tmp_path / "cli-client.json"),
        "youtube_token_file": str(tmp_path / "cli-token.json"),
        "image_processor": "caption",
        "deepseek_image_model": "cli-vision",
        "request_timeout": 33,
        "max_redirects": 8,
        "max_response_bytes": 3_000_000,
        "max_image_bytes": 400_000,
    }
    config = Config.load(tmp_path / "vault", tmp_path / "state", config_path, cli_values=cli)

    assert config.youtube_playlist == "cli-playlist"
    assert config.youtube_playlist_id == "cli-playlist-id"
    assert config.youtube_client_file == (tmp_path / "cli-client.json").resolve()
    assert config.youtube_token_file == (tmp_path / "cli-token.json").resolve()
    assert config.image_processor == "caption"
    assert config.deepseek_image_model == "cli-vision"
    assert config.request_timeout == 33
    assert config.max_redirects == 8
    assert config.max_response_bytes == 3_000_000
    assert config.max_image_bytes == 400_000


@pytest.mark.parametrize(
    ("name", "environment_name", "json_value", "environment_value", "cli_value", "default_value"),
    [
        ("vault", "SECOND_BRAIN_VAULT", "json-vault", "env-vault", "cli-vault", Path(".").resolve()),
        ("state_dir", "SECOND_BRAIN_STATE_DIR", "json-state", "env-state", "cli-state", Path.home() / ".local" / "state" / "second-brain"),
        ("youtube_playlist", "SECOND_BRAIN_YOUTUBE_PLAYLIST", "json-playlist", "env-playlist", "cli-playlist", "To Ingest"),
        ("youtube_enabled", "SECOND_BRAIN_YOUTUBE_ENABLED", True, "true", False, False),
        ("youtube_playlist_id", "SECOND_BRAIN_YOUTUBE_PLAYLIST_ID", "json-playlist-id", "env-playlist-id", "cli-playlist-id", None),
        ("youtube_client_file", "SECOND_BRAIN_YOUTUBE_CLIENT_FILE", "json-client.json", "env-client.json", "cli-client.json", None),
        ("youtube_token_file", "SECOND_BRAIN_YOUTUBE_TOKEN_FILE", "json-token.json", "env-token.json", "cli-token.json", None),
        ("executor", "SECOND_BRAIN_EXECUTOR", "json-executor", "env-executor", "cli-executor", "noop"),
        ("deepseek_base_url", "SECOND_BRAIN_DEEPSEEK_BASE_URL", "https://json.example", "https://env.example", "https://cli.example", "https://api.deepseek.com"),
        ("deepseek_model", "SECOND_BRAIN_DEEPSEEK_MODEL", "json-text", "env-text", "cli-text", None),
        ("image_processor", "SECOND_BRAIN_IMAGE_PROCESSOR", "json-image", "env-image", "cli-image", "caption"),
        ("deepseek_image_model", "SECOND_BRAIN_DEEPSEEK_IMAGE_MODEL", "json-vision", "env-vision", "cli-vision", None),
        ("request_timeout", "SECOND_BRAIN_REQUEST_TIMEOUT", 22, "11", 33, 30),
        ("max_redirects", "SECOND_BRAIN_MAX_REDIRECTS", 6, "4", 8, 5),
        ("max_response_bytes", "SECOND_BRAIN_MAX_RESPONSE_BYTES", 2_000_000, "1000000", 3_000_000, 10_000_000),
        ("max_image_bytes", "SECOND_BRAIN_MAX_IMAGE_BYTES", 300_000, "200000", 400_000, 5_000_000),
        ("codex_executable", "SECOND_BRAIN_CODEX_EXECUTABLE", "json-codex", "env-codex", "cli-codex", "codex"),
        ("codex_model", "SECOND_BRAIN_CODEX_MODEL", "json-codex-model", "env-codex-model", "cli-codex-model", None),
    ],
)
def test_every_config_setting_uses_all_four_precedence_layers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    environment_name: str,
    json_value: object,
    environment_value: str,
    cli_value: object,
    default_value: object,
):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({name: json_value}), encoding="utf-8")
    monkeypatch.setenv(environment_name, environment_value)

    def value(config: Config) -> object:
        loaded = getattr(config, name)
        if name in {"vault", "state_dir", "youtube_client_file", "youtube_token_file"} and loaded is not None:
            return Path(loaded)
        return loaded

    def expected(raw: object) -> object:
        if name in {"vault", "state_dir", "youtube_client_file", "youtube_token_file"} and raw is not None:
            return Path(str(raw)).resolve()
        if name == "youtube_enabled":
            return raw is True or str(raw).lower() in {"true", "1", "yes", "on"}
        if name in {"request_timeout", "max_redirects", "max_response_bytes", "max_image_bytes"}:
            return int(str(raw))
        return raw

    assert value(Config.load(config_path=config_path, cli_values={name: cli_value})) == expected(cli_value)
    assert value(Config.load(config_path=config_path)) == expected(json_value)
    assert value(Config.load()) == expected(environment_value)
    monkeypatch.delenv(environment_name)
    assert value(Config.load()) == expected(default_value)


def test_config_uses_environment_when_json_and_cli_omit_a_setting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_PLAYLIST", "env-playlist")
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_PLAYLIST_ID", "env-playlist-id")
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_CLIENT_FILE", str(tmp_path / "env-client.json"))
    monkeypatch.setenv("SECOND_BRAIN_YOUTUBE_TOKEN_FILE", str(tmp_path / "env-token.json"))
    monkeypatch.setenv("SECOND_BRAIN_EXECUTOR", "noop")
    monkeypatch.setenv("SECOND_BRAIN_DEEPSEEK_BASE_URL", "https://env.example")
    monkeypatch.setenv("SECOND_BRAIN_DEEPSEEK_MODEL", "env-text-model")
    monkeypatch.setenv("SECOND_BRAIN_IMAGE_PROCESSOR", "caption")
    monkeypatch.setenv("SECOND_BRAIN_DEEPSEEK_IMAGE_MODEL", "env-vision-model")
    monkeypatch.setenv("SECOND_BRAIN_REQUEST_TIMEOUT", "19")
    monkeypatch.setenv("SECOND_BRAIN_MAX_REDIRECTS", "3")
    monkeypatch.setenv("SECOND_BRAIN_MAX_RESPONSE_BYTES", "800000")
    monkeypatch.setenv("SECOND_BRAIN_MAX_IMAGE_BYTES", "120000")
    monkeypatch.setenv("SECOND_BRAIN_CODEX_EXECUTABLE", "env-codex")
    monkeypatch.setenv("SECOND_BRAIN_CODEX_MODEL", "env-codex-model")
    monkeypatch.setenv("SECOND_BRAIN_STATE_DIR", str(tmp_path / "env-state"))

    config = Config.load(tmp_path / "vault")

    assert config.youtube_playlist == "env-playlist"
    assert config.youtube_playlist_id == "env-playlist-id"
    assert config.youtube_client_file == (tmp_path / "env-client.json").resolve()
    assert config.youtube_token_file == (tmp_path / "env-token.json").resolve()
    assert config.deepseek_base_url == "https://env.example"
    assert config.deepseek_model == "env-text-model"
    assert config.image_processor == "caption"
    assert config.deepseek_image_model == "env-vision-model"
    assert config.request_timeout == 19
    assert config.max_redirects == 3
    assert config.max_response_bytes == 800_000
    assert config.max_image_bytes == 120_000
    assert config.codex_executable == "env-codex"
    assert config.codex_model == "env-codex-model"
    assert config.state_dir == (tmp_path / "env-state").resolve()


def test_explicit_youtube_enablement_requires_production_configuration(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    (vault / ".git").mkdir()

    config = Config.load(vault, tmp_path / "state", cli_values={"youtube_enabled": True})

    errors = config.validate()

    assert "youtube_playlist_id is required when YouTube is enabled" in errors
    assert "YouTube OAuth client file is required" in errors
    assert "YouTube OAuth token file is required" in errors


def test_youtube_oauth_files_require_expected_json_structure(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    (vault / ".git").mkdir()
    client_file = tmp_path / "youtube-client.json"
    token_file = tmp_path / "youtube-token.json"
    client_file.write_text("{}", encoding="utf-8")
    token_file.write_text("[]", encoding="utf-8")

    config = Config(
        vault=vault,
        state_dir=tmp_path / "state",
        youtube_enabled=True,
        youtube_playlist_id="production-playlist",
        youtube_client_file=client_file,
        youtube_token_file=token_file,
    )

    errors = config.validate()

    assert "YouTube OAuth client file has invalid structure" in errors
    assert "YouTube OAuth token file has invalid structure" in errors


def test_valid_youtube_oauth_files_pass_production_preflight(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    (vault / ".git").mkdir()
    client_file = tmp_path / "youtube-client.json"
    token_file = tmp_path / "youtube-token.json"
    client_file.write_text(
        json.dumps(
            {
                "installed": {
                    "client_id": "client-id",
                    "client_secret": "client-secret",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            }
        ),
        encoding="utf-8",
    )
    token_file.write_text(
        json.dumps(
            {
                "refresh_token": "refresh-token",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
            }
        ),
        encoding="utf-8",
    )

    config = Config(
        vault=vault,
        state_dir=tmp_path / "state",
        youtube_enabled=True,
        youtube_playlist_id="production-playlist",
        youtube_client_file=client_file,
        youtube_token_file=token_file,
    )

    assert config.validate() == []


def test_article_only_preflight_does_not_require_youtube_oauth(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    (vault / ".git").mkdir()

    assert Config.load(vault, tmp_path / "state").validate() == []


def test_cli_can_disable_lower_precedence_youtube_configuration(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    (vault / ".git").mkdir()
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "youtube_enabled": True,
                "youtube_playlist_id": "production-playlist",
                "youtube_client_file": str(tmp_path / "missing-client.json"),
                "youtube_token_file": str(tmp_path / "missing-token.json"),
            }
        ),
        encoding="utf-8",
    )

    args = build_parser().parse_args(
        [
            "--vault",
            str(vault),
            "--state-dir",
            str(tmp_path / "state"),
            "--config",
            str(config_path),
            "--no-youtube-enabled",
            "preflight",
        ]
    )
    config = _config(args)

    assert config.youtube_enabled is False
    assert config.validate() == []


def test_cli_config_values_are_available_to_config_loader(tmp_path: Path):
    args = build_parser().parse_args(
        [
            "--vault",
            str(tmp_path / "vault"),
            "--state-dir",
            str(tmp_path / "state"),
            "--youtube-playlist",
            "cli-playlist",
            "--youtube-enabled",
            "--request-timeout",
            "41",
            "preflight",
        ]
    )

    config = _config(args)

    assert config.youtube_playlist == "cli-playlist"
    assert config.youtube_enabled is True
    assert config.request_timeout == 41


def test_omitting_cli_vault_allows_json_vault_to_win(tmp_path: Path):
    json_vault = tmp_path / "json-vault"
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"vault": str(json_vault)}), encoding="utf-8")

    args = build_parser().parse_args(["--config", str(config_path), "preflight"])

    assert _config(args).vault == json_vault.resolve()


def test_validate_reports_production_configuration_errors_without_secret_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("", encoding="utf-8")
    (vault / ".git").mkdir()
    client_secret = "oauth-client-secret-sentinel"
    refresh_token = "oauth-refresh-token-sentinel"
    deepseek_api_key = "deepseek-api-key-sentinel"
    client_file = tmp_path / "youtube-client.json"
    token_file = tmp_path / "youtube-token.json"
    client_file.write_text(json.dumps({"installed": {"client_secret": client_secret}}), encoding="utf-8")
    token_file.write_text(json.dumps({"refresh_token": refresh_token}), encoding="utf-8")
    monkeypatch.setenv("DEEPSEEK_API_KEY", deepseek_api_key)

    config = Config(
        vault=vault,
        state_dir=tmp_path / "state",
        youtube_enabled=True,
        youtube_playlist_id="production-playlist",
        youtube_client_file=client_file,
        youtube_token_file=token_file,
        executor="deepseek",
        deepseek_base_url="http://[",
        deepseek_model=None,
        image_processor="deepseek",
        deepseek_image_model=None,
        request_timeout=0,
        max_redirects=-1,
        max_response_bytes=0,
        max_image_bytes=-1,
    )
    errors = config.validate()

    rendered = " ".join(errors)
    assert "deepseek_model is required" in rendered
    assert "deepseek_image_model is required" in rendered
    assert "deepseek_base_url" in rendered
    assert "request_timeout" in rendered
    assert "max_redirects" in rendered
    assert "max_response_bytes" in rendered
    assert "max_image_bytes" in rendered
    assert "OAuth" in rendered
    assert client_secret not in rendered
    assert refresh_token not in rendered
    assert deepseek_api_key not in rendered

    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "youtube_enabled": True,
                "youtube_playlist_id": "production-playlist",
                "youtube_client_file": str(client_file),
                "youtube_token_file": str(token_file),
                "executor": "deepseek",
                "deepseek_base_url": "http://[",
                "image_processor": "deepseek",
            }
        ),
        encoding="utf-8",
    )
    preflight_args = ["--vault", str(vault), "--state-dir", str(tmp_path / "state"), "--config", str(config_path)]

    assert main([*preflight_args, "preflight"]) == 2
    preflight_output = capsys.readouterr()
    assert all(secret not in preflight_output.out for secret in (client_secret, refresh_token, deepseek_api_key))
    assert all(secret not in preflight_output.err for secret in (client_secret, refresh_token, deepseek_api_key))

    assert main([*preflight_args, "batch"]) == 2
    batch_output = capsys.readouterr()
    assert all(secret not in batch_output.out for secret in (client_secret, refresh_token, deepseek_api_key))
    assert all(secret not in batch_output.err for secret in (client_secret, refresh_token, deepseek_api_key))


def test_vault_env_file_is_not_loaded_as_configuration(tmp_path: Path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / ".env").write_text("SECOND_BRAIN_REQUEST_TIMEOUT=999\n", encoding="utf-8")

    config = Config.load(vault, tmp_path / "state")

    assert config.request_timeout == 30


def test_config_rejects_undocumented_setting_aliases(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"network_timeout": 99}), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown configuration setting: network_timeout"):
        Config.load(config_path=config_path)

    monkeypatch.setenv("YOUTUBE_PLAYLIST", "undocumented-playlist")

    assert Config.load().youtube_playlist == "To Ingest"


def test_local_state_and_secret_candidates_are_ignored_by_git():
    candidates = (
        ".codegraph/database.sqlite",
        ".codegraph/database.sqlite-wal",
        ".codegraph/database.sqlite-shm",
        ".codegraph/index.log",
        ".codegraph/index.pid",
        ".env",
        "credentials.json",
        "client.json",
        "client_secret.json",
        "oauth-token.json",
        "youtube-client.json",
        "youtube-token.json",
        "example.sqlite",
        "example.sqlite3-wal",
        "staging/image.png",
        "image-cache/image.png",
    )

    for candidate in candidates:
        result = subprocess.run(
            ["git", "check-ignore", "--no-index", "-q", candidate],
            check=False,
        )
        assert result.returncode == 0, candidate


def test_useful_repository_candidates_are_not_ignored_by_git():
    candidates = (
        ".env.example",
        "tokenizer.json",
        "docs/client.json",
        "docs/oauth-flow.json",
        "tests/fixtures/youtube-client.json",
        "tests/fixtures/youtube-token.json",
        "tests/fixtures/reference.sqlite",
        "tmp/README.md",
    )

    for candidate in candidates:
        result = subprocess.run(
            ["git", "check-ignore", "--no-index", "-q", candidate],
            check=False,
        )
        assert result.returncode == 1, candidate


def test_fetch_uses_configured_timeout_and_response_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = Config(vault=tmp_path / "vault", state_dir=tmp_path / "state", request_timeout=17, max_response_bytes=5)
    calls: dict[str, object] = {}

    class Headers:
        def get(self, _name: str, default: str = "") -> str:
            return default

        @staticmethod
        def get_content_type() -> str:
            return "text/html"

    class Response:
        headers = Headers()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def read(amount: int) -> bytes:
            calls["read_amount"] = amount
            return b"123456"

    class Opener:
        @staticmethod
        def open(_request, timeout: int) -> Response:
            calls["timeout"] = timeout
            return Response()

    def build_opener(handler):
        calls["max_redirections"] = handler.redirect_limit
        return Opener()

    monkeypatch.setattr(urllib.request, "build_opener", build_opener)

    with pytest.raises(BatchError, match="size limit"):
        BatchRunner(config)._fetch("https://example.com/article")

    assert calls == {"max_redirections": 5, "timeout": 17, "read_amount": 6}


def test_fetch_does_not_follow_a_redirect_when_limit_is_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    class Headers:
        @staticmethod
        def get(_name: str, default: str = "") -> str:
            return default

        @staticmethod
        def get_content_type() -> str:
            return "text/html"

    class Response:
        headers = Headers()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def read(_amount: int = -1) -> bytes:
            return b"<p>redirect followed</p>"

        @staticmethod
        def close() -> None:
            return None

    def build_opener(handler):
        class Opener:
            @staticmethod
            def open(request, timeout: int):
                redirected = handler.redirect_request(
                    request,
                    Response(),
                    302,
                    "Found",
                    Headers(),
                    "https://example.com/done",
                )
                assert redirected is not None
                return Response()

        return Opener()

    monkeypatch.setattr(urllib.request, "build_opener", build_opener)
    config = Config(vault=tmp_path / "vault", state_dir=tmp_path / "state", max_redirects=0)

    with pytest.raises(urllib.error.HTTPError):
        BatchRunner(config)._fetch("https://example.com/start")


@pytest.mark.parametrize(("redirects", "raises"), [(2, False), (3, True)])
def test_fetch_enforces_the_configured_redirect_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    redirects: int,
    raises: bool,
):
    class Headers:
        @staticmethod
        def get(_name: str, default: str = "") -> str:
            return default

        @staticmethod
        def get_content_type() -> str:
            return "text/html"

    class Response:
        headers = Headers()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def read(_amount: int = -1) -> bytes:
            return b"<p>done</p>"

        @staticmethod
        def close() -> None:
            return None

    def build_opener(handler):
        class Opener:
            @staticmethod
            def open(request, timeout: int):
                current = request
                for index in range(redirects):
                    current = handler.redirect_request(
                        current,
                        Response(),
                        302,
                        "Found",
                        Headers(),
                        f"https://example.com/redirect-{index}",
                    )
                    assert current is not None
                return Response()

        return Opener()

    monkeypatch.setattr(urllib.request, "build_opener", build_opener)
    config = Config(vault=tmp_path / "vault", state_dir=tmp_path / "state", max_redirects=2)

    if raises:
        with pytest.raises(urllib.error.HTTPError):
            BatchRunner(config)._fetch("https://example.com/start")
    else:
        assert BatchRunner(config)._fetch("https://example.com/start") == "<p>done</p>"


@pytest.mark.parametrize(("redirects", "raises"), [(12, False), (13, True)])
def test_fetch_uses_configured_redirect_limit_above_urllib_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    redirects: int,
    raises: bool,
):
    class Headers(dict[str, str]):
        @staticmethod
        def get_content_type() -> str:
            return "text/html"

    class Response:
        headers = Headers()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def read(_amount: int = -1) -> bytes:
            return b"<p>done</p>"

        @staticmethod
        def close() -> None:
            return None

    def build_opener(handler):
        class Opener:
            def __init__(self):
                self.followed = 0

            def open(self, request, timeout: int):
                request.timeout = timeout
                if self.followed >= redirects:
                    return Response()
                self.followed += 1
                return handler.http_error_302(
                    request,
                    Response(),
                    302,
                    "Found",
                    Headers(location=f"https://example.com/redirect-{self.followed}"),
                )

        opener = Opener()
        handler.add_parent(opener)
        return opener

    monkeypatch.setattr(urllib.request, "build_opener", build_opener)
    config = Config(vault=tmp_path / "vault", state_dir=tmp_path / "state", max_redirects=12)

    if raises:
        with pytest.raises(urllib.error.HTTPError):
            BatchRunner(config)._fetch("https://example.com/start")
    else:
        assert BatchRunner(config)._fetch("https://example.com/start") == "<p>done</p>"


def test_preflight_reports_invalid_config_before_constructing_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "To Ingest.md").write_text("https://example.com/should-not-be-read\n", encoding="utf-8")
    (vault / ".git").mkdir()
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    def runner_must_not_be_constructed(_config):
        raise AssertionError("preflight constructed a runner")

    monkeypatch.setattr("second_brain.cli._runner", runner_must_not_be_constructed)

    result = main(
        [
            "--vault",
            str(vault),
            "--state-dir",
            str(tmp_path / "state"),
            "--image-processor",
            "deepseek",
            "preflight",
        ]
    )

    assert result == 2
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is False
    assert any("deepseek_image_model" in error for error in output["errors"])
    assert any("DEEPSEEK_API_KEY" in error for error in output["errors"])
