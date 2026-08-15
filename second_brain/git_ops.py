from __future__ import annotations

import subprocess
import json
from pathlib import Path


class GitError(RuntimeError):
    pass


class GitRepository:
    def __init__(self, root: Path):
        self.root = root

    def status(self) -> list[str]:
        result = self._run("status", "--porcelain")
        return [line for line in result.stdout.splitlines() if line]

    def ensure_clean(self, *, allowed_paths: set[str] | None = None, allowed_prefixes: tuple[str, ...] = ()) -> None:
        allowed_paths = allowed_paths or set()
        unexpected: list[str] = []
        for line in self.status():
            path = line[3:].strip()
            if path.startswith('"'):
                try:
                    path = json.loads(path)
                except json.JSONDecodeError:
                    pass
            if path not in allowed_paths and not any(path.startswith(prefix) for prefix in allowed_prefixes):
                unexpected.append(line)
        if unexpected:
            raise GitError("automatic batch commit requires a clean worktree")

    def commit_paths(self, paths: list[str], message: str) -> str:
        if not paths:
            raise GitError("cannot commit an empty path set")
        existing = [path for path in paths if (self.root / path).exists()]
        if existing:
            self._run("add", "--", *existing)
        missing = [path for path in paths if path not in existing]
        if missing:
            self._run("rm", "--cached", "--ignore-unmatch", *missing, check=False)
        staged = self._run("diff", "--cached", "--name-only").stdout.splitlines()
        if sorted(staged) != sorted(set(paths)):
            self._run("reset", "--", *paths, check=False)
            raise GitError("staged paths differ from the validated path set")
        self._run("commit", "-m", message)
        return self._run("rev-parse", "HEAD").stdout.strip()

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(["git", *args], cwd=self.root, text=True, capture_output=True)
        if check and result.returncode:
            raise GitError(result.stderr.strip() or f"git {' '.join(args)} failed")
        return result
