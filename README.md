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

The default `noop` executor prepares sources without concept changes. The DeepSeek adapter uses the configurable OpenAI-compatible `/chat/completions` endpoint with JSON output. Set `DEEPSEEK_API_KEY` outside the vault and configure the model in JSON rather than storing credentials in the repository. The Codex adapter accepts a command that reads the synthesis request as JSON on stdin and returns a JSON change set on stdout; its validated writes are still restricted to `Concepts/`.

Image interpretation is a separate adapter. The safe default retains a meaningful image only when its caption or alt text is evidence; a configured processor can describe an image in place. Image bytes are never copied into `Sources/`.

YouTube version 1 accepts a client adapter or fixture and prefers manual source-language transcripts, then automatic transcripts. It does not download audio or use Whisper fallback. Initial OAuth setup is deliberately external: store client material and refresh tokens outside the vault, then connect a `YouTubeClient` implementation.

Install and enable the Obsidian Advanced URI community plugin yourself. Article citations use encoded `obsidian://adv-uri` links to stable physical source lines; YouTube citations link to the starting timestamp.

## Recovery and operations

The SQLite lifecycle records claims, attempts, failures, acknowledgements, source hashes, cleanup, and Git commit IDs. Re-running a batch recovers jobs left in `processing`. Raw HTML is removed only after the corresponding source is included in a confirmed commit. Failed synthesis leaves candidate work and raw HTML uncommitted. Re-submit a failed job through a small adapter/CLI extension or delete only its failed state in a deliberate maintenance operation; do not edit the vault source evidence.

An explicit refresh should create a new immutable source version and file rather than overwrite an existing source. The implementation keeps source formatting deterministic: UTF-8, LF endings, fixed frontmatter order, stable hashes, and a trailing newline.
