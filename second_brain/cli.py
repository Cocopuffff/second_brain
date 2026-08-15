from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .batch import BatchError, BatchRunner
from .config import Config
from .git_ops import GitError, GitRepository
from .state import StateStore
from .synthesis import CodexHarnessSynthesizer, DeepSeekSynthesizer
from .youtube import FixtureYouTubeClient


def _config(args) -> Config:
    return Config.load(Path(args.vault), Path(args.state_dir) if args.state_dir else None, Path(args.config) if args.config else None)


def _runner(config: Config) -> BatchRunner:
    if config.executor == "deepseek":
        synthesizer = DeepSeekSynthesizer(api_key=None, base_url=config.deepseek_base_url, model=config.deepseek_model, timeout=config.request_timeout)
    elif config.executor == "codex":
        synthesizer = CodexHarnessSynthesizer(list(config.codex_command))
    else:
        synthesizer = None
    return BatchRunner(config, synthesizer=synthesizer)


def _publication_statuses(config: Config, state: StateStore) -> list[dict]:
    git = GitRepository(config.vault)
    publications: list[dict] = []
    try:
        for journal in state.list_publications():
            effective = dict(journal)
            if journal["phase"] in {"publishing", "published_uncommitted"}:
                commit_id = journal.get("commit_id") or git.find_batch_commit(journal["batch_id"])
                if commit_id:
                    entries = state.publication_entries(journal["batch_id"])
                    paths = [entry["relative_path"] for entry in entries]
                    hashes = {entry["relative_path"]: entry["candidate_hash"] for entry in entries}
                    if git.verify_commit(commit_id, paths, hashes, journal["base_commit"]):
                        effective["phase"] = "committed_unfinalized"
                        effective["commit_id"] = commit_id
            publications.append(effective)
    except GitError as exc:
        raise BatchError(str(exc)) from exc
    return publications


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="second-brain", description="Batch ingestion for an Obsidian second brain vault")
    parser.add_argument("--vault", default=".")
    parser.add_argument("--state-dir")
    parser.add_argument("--config")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="create expected vault directories and local state")
    sub.add_parser("preflight", help="validate configuration without consuming inputs")
    sub.add_parser("dry-run", help="discover inputs without consuming inputs")
    sub.add_parser("status", help="show durable batches, jobs, and cleanup state")
    retry = sub.add_parser("retry", help="retry one durable failed job")
    retry.add_argument("job_id")
    batch = sub.add_parser("batch", help="run one finite ingestion batch")
    batch.add_argument("--youtube-fixture")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _config(args)
    runner = _runner(config)
    try:
        if args.command == "init":
            runner.initialize()
            print(json.dumps({"initialized": True, "vault": str(config.vault), "state_dir": str(config.state_dir)}))
            return 0
        if args.command == "preflight":
            errors = config.validate()
            print(json.dumps({"ok": not errors, "errors": errors}))
            return 0 if not errors else 2
        if args.command == "dry-run":
            print(json.dumps(runner.dry_run(), ensure_ascii=False))
            return 0
        if args.command == "status":
            state = StateStore(config.database)
            try:
                pending_cleanup = [job.id for job in state.pending_cleanup()]
                print(json.dumps({"batches": state.list_batches(), "jobs": [job.__dict__ for job in state.list_jobs()], "publications": _publication_statuses(config, state), "pending_cleanup": pending_cleanup, "outstanding_cleanup": len(pending_cleanup)}, ensure_ascii=False))
            finally:
                state.close()
            return 0
        if args.command == "retry":
            report = runner.run(retry_job_id=args.job_id)
            print(json.dumps(report.__dict__, ensure_ascii=False))
            return 2 if report.recovery_block_reason else (0 if report.committed else 1)
        if args.command == "batch":
            if args.youtube_fixture:
                runner.youtube_client = FixtureYouTubeClient(Path(args.youtube_fixture))
            report = runner.run()
            print(json.dumps(report.__dict__, ensure_ascii=False))
            return 2 if report.recovery_block_reason else (0 if report.committed or report.claimed == 0 else 1)
    except BatchError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    return 2
