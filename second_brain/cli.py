from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .batch import BatchError, BatchRunner
from .config import CONFIG_SETTINGS, Config, SettingKind
from .git_ops import GitError, GitRepository
from .models import PublicationPhase, UNCOMMITTED_PUBLICATION_PHASES
from .state import StateStore
from .synthesis import build_controlled_synthesis
from .youtube import FixtureYouTubeClient


def _config(args) -> Config:
    cli_values = {
        setting.name: getattr(args, setting.name)
        for setting in CONFIG_SETTINGS
        if getattr(args, setting.name, None) is not None
    }
    return Config.load(
        config_path=Path(args.config) if args.config else None,
        cli_values=cli_values,
    )


def _runner(config: Config) -> BatchRunner:
    return BatchRunner(config, synthesis_runner=build_controlled_synthesis(config))


def _publication_statuses(config: Config, state: StateStore) -> list[dict]:
    git = GitRepository(config.vault)
    publications: list[dict] = []
    try:
        for journal in state.list_publications():
            effective = asdict(journal)
            effective["recovery_block_reason"] = (journal.failure_code or journal.failure_message) if journal.phase == PublicationPhase.RECOVERY_BLOCKED else None
            if journal.phase in UNCOMMITTED_PUBLICATION_PHASES:
                commit_id = journal.commit_id or git.find_batch_commit(journal.batch_id)
                if commit_id:
                    entries = state.publication_entries(journal.batch_id)
                    paths = [entry.relative_path for entry in entries]
                    hashes = {entry.relative_path: entry.candidate_hash for entry in entries}
                    if git.verify_commit(commit_id, paths, hashes, journal.base_commit, journal.batch_id):
                        effective["phase"] = PublicationPhase.COMMITTED_UNFINALIZED
                        effective["commit_id"] = commit_id
            publications.append(effective)
    except GitError as exc:
        raise BatchError(str(exc)) from exc
    return publications


def _job_status(job) -> dict:
    effective = job.__dict__.copy()
    if job.status == "failed":
        effective["retry_command"] = f"second-brain retry {job.id}" if job.retryable else None
    return effective


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="second-brain", description="Batch ingestion for an Obsidian second brain vault")
    parser.add_argument("--config")
    for setting in CONFIG_SETTINGS:
        if setting.kind is SettingKind.INTEGER:
            parser.add_argument(setting.cli_option, dest=setting.name, type=int)
        elif setting.kind is SettingKind.BOOLEAN:
            parser.add_argument(
                setting.cli_option,
                dest=setting.name,
                action=argparse.BooleanOptionalAction,
                default=None,
            )
        else:
            parser.add_argument(setting.cli_option, dest=setting.name)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="create expected vault directories and local state")
    sub.add_parser("preflight", help="validate configuration without consuming inputs")
    sub.add_parser("dry-run", help="discover inputs without consuming inputs")
    sub.add_parser("status", help="show durable batches, jobs, and cleanup state")
    retry = sub.add_parser("retry", help="retry one durable failed job or all eligible failed jobs")
    retry.add_argument("job_id", nargs="?")
    retry.add_argument("--all-eligible", "--all", dest="all_eligible", action="store_true", help="retry every currently eligible failed job")
    batch = sub.add_parser("batch", help="run one finite ingestion batch")
    batch.add_argument("--youtube-fixture")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        config = _config(args)
        if args.command == "preflight":
            errors = config.validate()
            print(json.dumps({"ok": not errors, "errors": errors}))
            return 0 if not errors else 2
        if args.command != "init":
            errors = config.validate()
            if errors:
                raise BatchError("preflight failed: " + "; ".join(errors))
        runner = _runner(config)
        if args.command == "init":
            runner.initialize()
            print(json.dumps({"initialized": True, "vault": str(config.vault), "state_dir": str(config.state_dir)}))
            return 0
        if args.command == "dry-run":
            preview = runner.dry_run()
            print(json.dumps(preview, ensure_ascii=False))
            return 0
        if args.command == "status":
            state = StateStore(config.database)
            try:
                pending_cleanup = [job.id for job in state.pending_cleanup()]
                print(json.dumps({"batches": state.list_batches(), "jobs": [_job_status(job) for job in state.list_jobs()], "publications": _publication_statuses(config, state), "pending_cleanup": pending_cleanup, "outstanding_cleanup": len(pending_cleanup)}, ensure_ascii=False))
            finally:
                state.close()
            return 0
        if args.command == "retry":
            if args.all_eligible and args.job_id:
                raise ValueError("retry accepts a job ID or --all-eligible, not both")
            if not args.all_eligible and not args.job_id:
                raise ValueError("retry requires a job ID or --all-eligible")
            report = runner.run(retry_job_id=args.job_id, retry_all=args.all_eligible)
            selected_job_ids: list[str] = []
            if args.all_eligible:
                selected_job_ids = list(getattr(report, "selected_job_ids", ()))
                print(
                    json.dumps(
                        {
                            "mode": "all-eligible",
                            "selected": len(selected_job_ids),
                            "selected_job_ids": selected_job_ids,
                            "batch_id": report.batch_id or None,
                            "completed": report.completed,
                            "failed": report.failed,
                            "committed": report.committed,
                            "commit_id": report.commit_id,
                            "failures": list(report.failures),
                            "recovery_block_reason": report.recovery_block_reason,
                        },
                        ensure_ascii=False,
                    )
                )
            else:
                print(json.dumps(report.__dict__, ensure_ascii=False))
            if args.all_eligible:
                return 2 if report.recovery_block_reason else (0 if report.committed or not selected_job_ids else 1)
            return 2 if report.recovery_block_reason else (0 if report.committed else 1)
        if args.command == "batch":
            if args.youtube_fixture:
                runner.youtube_client = FixtureYouTubeClient(Path(args.youtube_fixture))
            report = runner.run()
            print(json.dumps(report.__dict__, ensure_ascii=False))
            return 2 if report.recovery_block_reason else (0 if report.committed or report.claimed == 0 else 1)
    except (BatchError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    return 2
