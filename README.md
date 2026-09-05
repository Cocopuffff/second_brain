# Second Brain Ingestion

This repository contains a local, finite-batch ingestion tool for the `Second Brain` Obsidian vault. It keeps captured URLs, saved article HTML, and YouTube transcript evidence durable while allowing concept synthesis to change only `Concepts/`.

## Setup

Requires Python 3.11 or newer. The application uses only the standard library. From the ingestion project root, pass the path to the synced vault explicitly:

```text
python -m second_brain --vault "/path/to/Second Brain" init
python -m second_brain --vault "/path/to/Second Brain" preflight
python -m second_brain --vault "/path/to/Second Brain" dry-run
python -m second_brain --vault "/path/to/Second Brain" batch
python -m second_brain --vault "/path/to/Second Brain" retry <job_id>
python -m second_brain --vault "/path/to/Second Brain" retry --all-eligible
python -m second_brain --vault "/path/to/Second Brain" retry --youtube-fixture "/path/to/youtube.json" --all-eligible
```

The default SQLite database and lock live under `~/.local/state/second-brain`, outside the vault. Set `SECOND_BRAIN_STATE_DIR` or pass `--state-dir` to choose another platform-local directory. A JSON config can select the executor, YouTube settings, image processor, and network limits.

Configuration uses CLI values first, then JSON, then environment variables, then built-in defaults. The supported environment names use the `SECOND_BRAIN_` prefix, for example:

```sh
export SECOND_BRAIN_REQUEST_TIMEOUT=30
export SECOND_BRAIN_MAX_REDIRECTS=5
export SECOND_BRAIN_MAX_RESPONSE_BYTES=10000000
export SECOND_BRAIN_MAX_IMAGE_BYTES=5000000
```

When `--vault` is omitted, `vault` in JSON or `SECOND_BRAIN_VAULT` supplies the vault path through the same precedence chain.

The CLI exposes the same settings, including `--request-timeout`, `--max-redirects`, `--max-response-bytes`, and `--max-image-bytes`. Configuration paths may point to JSON files, but credential contents must stay in OS-managed files outside the vault. Export `DEEPSEEK_API_KEY` in the shell for DeepSeek. `.venv/` is the Python virtual environment directory, not an environment-variable file. The application does not load `.env` files and does not depend on `python-dotenv`.

Before a real batch, open Obsidian, wait for Obsidian Sync to finish, and avoid editing queue, source, or concept files during the run. The tool refuses automatic commits when the Git worktree is already dirty.

## Executors and evidence

The default `noop` executor prepares sources without concept changes. The DeepSeek adapter requires an explicit `deepseek_model`, starts with descriptors rather than evidence bodies, and uses a bounded four-tool loop (`list_concepts`, `read_concept`, `search_concepts`, and `read_source`). Set `DEEPSEEK_API_KEY` outside the vault and configure the model in JSON rather than storing credentials in the repository. Codex is CLI-only: configure `codex_executable` and optionally `codex_model`. The runner invokes `codex exec` with its native workspace-write sandbox, no approvals, ephemeral state, ignored user config/rules, strict config, a fixed candidate directory, and schema-validated output. The old `codex_command` setting is rejected with a migration error.

For example, a DeepSeek configuration must name the provider model explicitly:

```json
{"executor": "deepseek", "deepseek_model": "<supported-model>"}
```

The equivalent Codex configuration is:

```json
{"executor": "codex", "codex_executable": "codex", "codex_model": "<optional-model>"}
```

Image interpretation is a separate adapter. The safe default retains a meaningful image only when its caption or alt text is evidence; a configured processor can describe an image in place. Set `image_processor` to `deepseek` only with an explicit `deepseek_image_model`. Image bytes are never copied into `Sources/`.

YouTube version 1 accepts a client adapter or fixture and prefers manual source-language transcripts, then automatic transcripts. It does not download audio or use Whisper fallback. Intake persists playlist-acknowledgement intent with each prepared candidate. Publication removes the playlist item only after the Git commit and SQLite finalization succeed; a failed or interrupted acknowledgement remains retryable without rediscovery or another commit. Fixture-only and article-only runs do not need OAuth. Production YouTube is opt-in: set `youtube_enabled` to `true` in JSON, `SECOND_BRAIN_YOUTUBE_ENABLED=true` in the environment, or pass `--youtube-enabled`. When enabled, configure `youtube_playlist_id`, `youtube_client_file`, and `youtube_token_file`; preflight checks that the client and token files contain the expected OAuth JSON structure and remain outside the vault. Keep them outside both repositories. As a defense-in-depth measure, the repository root ignores the exact candidate names `client.json`, `youtube-client.json`, and `youtube-token.json`, plus the root `credentials/` and `tokens/` directories. Similar names elsewhere remain available for fixtures and documentation.

Install and enable the Obsidian Advanced URI community plugin yourself. Article citations use encoded `obsidian://adv-uri` links to stable physical source lines; YouTube citations link to the starting timestamp.

## Recovery and operations

The SQLite lifecycle records claims, attempts, failures, acknowledgements, source hashes, cleanup, and Git commit IDs. Each non-empty publication also has a durable journal and an external rollback snapshot. Re-running a batch reconciles the oldest interrupted publication before accepting new input; that recovery invocation reports its action and stops. A blocked recovery is never guessed through: inspect `status`, repair only the reported condition, and rerun.

Use `status` to find each failed job's `retry_command`. `retry <job_id>` preserves the single-job workflow. `retry --all-eligible` selects only retryable failed jobs in stable creation order, reuses their durable queue or saved-input records without requiring new capture input, and prints a concise selected/completed/failed report. Pass `--youtube-fixture <path>` to either retry form for fixture-backed YouTube work; all-eligible retry reacquires a failed video by its durable video ID without listing the playlist, and pending acknowledgement cleanup uses the same client. Completed, active, and non-retryable jobs are left unchanged. If no eligible jobs exist, the command succeeds without creating a batch or Git commit. A pending source-ready recovery is handled first so its exact prepared bytes remain authoritative, and a failed recovery returns a nonzero exit status.

Article and YouTube adapters share one intake contract. Discovery canonicalizes and deduplicates work, claims it in SQLite, persists queue or playlist finalization intent, and returns a typed raw payload or structured failure. Discovery and acquisition do not edit the tracked article queue, delete saved HTML, or acknowledge YouTube. YouTube acknowledgement clients report either `removed` or `already_absent`, so a retry after a crash remains safe.

Preparation persists exact rendered source bytes plus a manifest under the external state directory before setting `source_ready`. The oldest valid source-ready batch is resumed before new acquisition, loading those bytes without rediscovery, network access, transcript lookup, extraction, or rendering. Raw HTML is removed only after the corresponding source is included in a confirmed commit and finalized in SQLite. Cleanup and external acknowledgement are independently retryable, so a failure never re-renders or recommits a source. After finalization, rendered candidate payloads are removed while manifests and identity hashes remain. Failed synthesis leaves candidate work and raw HTML uncommitted. Re-submit a failed job through the retry command; do not edit the vault source evidence.

An explicit refresh should create a new immutable source version and file rather than overwrite an existing source. The implementation keeps source formatting deterministic: UTF-8, LF endings, fixed frontmatter order, stable hashes, and a trailing newline.
