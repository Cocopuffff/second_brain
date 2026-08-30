# Second Brain Ingestion

This repository contains a local, finite-batch ingestion tool for the `Second Brain` Obsidian vault. It keeps captured URLs, saved article HTML, and YouTube transcript evidence durable while allowing concept synthesis to change only `Concepts/`.

## Setup

Requires Python 3.11 or newer. The application uses only the standard library. From the ingestion project root, pass the path to the synced vault explicitly:

```text
python -m second_brain --vault "/path/to/Second Brain" init
python -m second_brain --vault "/path/to/Second Brain" preflight
python -m second_brain --vault "/path/to/Second Brain" dry-run
python -m second_brain --vault "/path/to/Second Brain" batch
```

The default SQLite database and lock live under `~/.local/state/second-brain`, outside the vault. Set `SECOND_BRAIN_STATE_DIR` or pass `--state-dir` to choose another platform-local directory. A JSON config can select `executor` (`noop`, `deepseek`, or `codex`), the YouTube playlist name, and network limits.

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

Image interpretation is a separate adapter. The safe default retains a meaningful image only when its caption or alt text is evidence; a configured processor can describe an image in place. Image bytes are never copied into `Sources/`.

YouTube version 1 accepts a client adapter or fixture and prefers manual source-language transcripts, then automatic transcripts. It does not download audio or use Whisper fallback. The batch persists playlist-acknowledgement intent with each prepared candidate but does not execute external acknowledgement in SEC-7; that behavior is reserved for SEC-8. Initial OAuth setup is deliberately external: store client material and refresh tokens outside the vault, then connect a `YouTubeClient` implementation.

Install and enable the Obsidian Advanced URI community plugin yourself. Article citations use encoded `obsidian://adv-uri` links to stable physical source lines; YouTube citations link to the starting timestamp.

## Recovery and operations

The SQLite lifecycle records claims, attempts, failures, acknowledgements, source hashes, cleanup, and Git commit IDs. Each non-empty publication also has a durable journal and an external rollback snapshot. Re-running a batch reconciles the oldest interrupted publication before accepting new input; that recovery invocation reports its action and stops. A blocked recovery is never guessed through: inspect `status`, repair only the reported condition, and rerun.

Preparation persists exact rendered source bytes plus a manifest under the external state directory before setting `source_ready`. The oldest valid source-ready batch is resumed before new acquisition, loading those bytes without rediscovery, network access, transcript lookup, extraction, or rendering. Raw HTML is removed only after the corresponding source is included in a confirmed commit and finalized in SQLite. Cleanup is independently retryable, so a cleanup failure never re-renders or recommits a source. After finalization, rendered candidate payloads are removed while manifests and identity hashes remain. Failed synthesis leaves candidate work and raw HTML uncommitted. Re-submit a failed job through the retry command; do not edit the vault source evidence.

An explicit refresh should create a new immutable source version and file rather than overwrite an existing source. The implementation keeps source formatting deterministic: UTF-8, LF endings, fixed frontmatter order, stable hashes, and a trailing newline.
