from __future__ import annotations

import json
import hashlib
import subprocess
from pathlib import Path


class GitError(RuntimeError):
    pass


class GitRepository:
    def __init__(self, root: Path):
        self.root = root

    def status(self) -> list[str]:
        result = self._run("status", "--porcelain=v1", "--untracked-files=all")
        return [line for line in result.stdout.splitlines() if line]

    def ensure_clean(self, *, allowed_paths: set[str] | None = None, allowed_prefixes: tuple[str, ...] = ()) -> None:
        allowed_paths = allowed_paths or set()
        unexpected: list[str] = []
        for line in self.status():
            if len(line) < 3:
                unexpected.append(line)
                continue
            index_state, worktree_state = line[0], line[1]
            path = self._status_path(line[3:])
            allowed = path in allowed_paths or any(path.startswith(prefix) for prefix in allowed_prefixes)
            if not allowed or (index_state not in {" ", "?"}):
                unexpected.append(line)
            elif worktree_state not in {" ", "M", "D", "?"}:
                unexpected.append(line)
        if unexpected:
            raise GitError("automatic batch commit requires a clean worktree")

    def head(self) -> str:
        return self._run("rev-parse", "HEAD").stdout.strip()

    def commit_paths(self, paths: list[str], message: str) -> str:
        if not paths:
            raise GitError("cannot commit an empty path set")
        normalized = sorted(set(paths))
        try:
            self._run("add", "-A", "--", *normalized)
            staged = self._run("diff", "--cached", "--name-only", "-z").stdout.split("\x00")
            staged = sorted(path for path in staged if path)
            if staged != normalized:
                self.restore_staged(normalized)
                raise GitError("staged paths differ from the validated path set")
            self._run("commit", "-m", message)
        except GitError:
            self.restore_staged(normalized)
            raise
        return self.head()

    def restore_staged(self, paths: list[str]) -> None:
        if paths:
            self._run("restore", "--staged", "--", *sorted(set(paths)), check=False)

    def find_batch_commit(self, batch_id: str) -> str | None:
        result = self._run("log", "--all", "--format=%H%x00%B%x00", "--grep", f"Batch-ID: {batch_id}", check=False)
        matches: list[str] = []
        fields = result.stdout.split("\x00")
        for index in range(0, len(fields) - 1, 2):
            commit_id, body = fields[index], fields[index + 1]
            if commit_id and self._has_batch_trailer(body, batch_id):
                matches.append(commit_id)
        if len(set(matches)) > 1:
            raise GitError(f"multiple commits claim batch {batch_id}")
        return matches[0] if matches else None

    def verify_commit(self, commit_id: str, paths: list[str], candidate_hashes: dict[str, str | None], base_commit: str, batch_id: str) -> bool:
        if self.head() != commit_id:
            return False
        body = self._run("show", "-s", "--format=%B", commit_id).stdout
        if not self._has_batch_trailer(body, batch_id):
            return False
        parents = self._run("rev-list", "--parents", "-n", "1", commit_id).stdout.strip().split()
        if len(parents) != 2 or parents[1] != base_commit:
            return False
        changed = self._run("diff-tree", "--no-commit-id", "--name-only", "-r", "-z", commit_id).stdout.split("\x00")
        if {path for path in changed if path} != set(paths):
            return False
        for path, expected_hash in candidate_hashes.items():
            exists = self._run("cat-file", "-e", f"{commit_id}:{path}", check=False).returncode == 0
            if expected_hash is None:
                if exists:
                    return False
                continue
            if not exists or hashlib.sha256(self._run_bytes("show", f"{commit_id}:{path}")).hexdigest() != expected_hash:
                return False
        return True

    @staticmethod
    def _has_batch_trailer(body: str, batch_id: str) -> bool:
        return f"Batch-ID: {batch_id}" in {line.strip() for line in body.splitlines()}

    @staticmethod
    def _status_path(value: str) -> str:
        path = value.strip()
        if path.startswith('"'):
            try:
                return json.loads(path)
            except json.JSONDecodeError:
                return path
        return path.split(" -> ", 1)[-1]

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(["git", *args], cwd=self.root, text=True, capture_output=True)
        if check and result.returncode:
            raise GitError(result.stderr.strip() or f"git {' '.join(args)} failed")
        return result

    def _run_bytes(self, *args: str) -> bytes:
        result = subprocess.run(["git", *args], cwd=self.root, capture_output=True)
        if result.returncode:
            raise GitError(result.stderr.decode(errors="replace").strip() or f"git {' '.join(args)} failed")
        return result.stdout
