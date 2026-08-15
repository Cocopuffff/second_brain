# Deepen recoverable batch publication

**Status:** Ready for implementation  
**Triage:** `ready-for-agent`  
**Tracker:** [SEC-5 — Deepen recoverable batch publication](https://linear.app/kenneth-second-brain/issue/SEC-5/deepen-recoverable-batch-publication)

## Problem Statement

As a vault owner, I need each ingestion batch to either become one durable local Git commit and be finalized in SQLite, or remain safely recoverable without exposing an uncommitted or partially published vault state. Today, the batch orchestration publishes candidate files into the live vault before committing and treats any later failure as a failed batch. A crash or Git failure can therefore leave evidence or concepts visible but uncommitted, with no durable record that identifies how to complete or roll back that state. This undermines the source-evidence boundary and makes reruns unsafe.

## Solution

Deepen batch publication into one recoverable ingestion-batch module behind the existing batch CLI seam. The module owns all transitions from validated candidate workspace through publication, one narrow local commit, SQLite finalization, and raw-payload cleanup. It records a durable publication journal keyed by the stable batch ID, detects and reconciles interrupted work before accepting new input, and never starts a new batch while recovery is unresolved.

The implementation keeps source preparation and synthesis outside this module as already-validated inputs. The module hides filesystem, Git, and SQLite sequencing behind its interface, increasing leverage for callers and locality for recovery logic. The interface is the test surface: the batch CLI and `BatchRunner.run` remain the only public behavior tests need to cross.

## User Stories

1. As a vault owner, I want an ingestion batch to have one stable identifier, so that I can locate and understand an interrupted batch.
2. As a vault owner, I want a batch to prepare changes outside tracked vault directories, so that incomplete candidate work is never mistaken for committed knowledge.
3. As a vault owner, I want validated candidate files to be published only through the recoverable publication module, so that the live vault has one owner for this critical transition.
4. As a vault owner, I want each non-empty successful or partial batch to create exactly one local Git commit, so that history and recovery stay understandable.
5. As a vault owner, I want the commit message to carry the batch identifier, so that SQLite can reconcile a commit after a crash.
6. As a vault owner, I want the publication module to stage only the prevalidated path set, so that unrelated working-tree changes cannot enter the commit.
7. As a vault owner, I want a dirty worktree to stop publication before candidate visibility changes, so that personal edits remain untouched.
8. As a vault owner, I want a crash before publication to leave only external candidate state, so that rerunning can resume or discard it without modifying the vault.
9. As a vault owner, I want a crash while publishing to be detected on the next run, so that the same batch is reconciled rather than replaced by a new batch.
10. As a vault owner, I want a crash after publication but before Git commit to restore the pre-batch vault state, so that uncommitted evidence and concepts do not persist.
11. As a vault owner, I want a crash after Git commit but before SQLite finalization to be finalized from the existing commit, so that the system never creates a duplicate commit.
12. As a vault owner, I want recovery to verify that the commit’s paths and batch identity match the journal, so that an unrelated commit is never adopted.
13. As a vault owner, I want an incomplete publication that cannot be proved safe to stop with an actionable error, so that the system does not guess about vault state.
14. As a vault owner, I want source and concept deletions to be recoverable just like writes, so that a crash cannot make a validated deletion ambiguous.
15. As a vault owner, I want the pre-publication contents of every affected live path captured outside the vault, so that rollback can restore modified files and remove newly created files exactly.
16. As a vault owner, I want candidate publication to reject path traversal, symlink escapes, duplicate path entries, and paths outside the validated scope, so that recovery metadata cannot be used to mutate unrelated files.
17. As a vault owner, I want raw saved HTML removed only after the matching batch is both committed and finalized, so that raw evidence remains available during recovery.
18. As a vault owner, I want interrupted raw-payload cleanup retried independently, so that cleanup failures do not reprocess or recommit a source.
19. As an operator, I want the status command to distinguish pending publication recovery, committed-but-unfinalized batches, and pending cleanup, so that I know which action is safe.
20. As an operator, I want every recovery result included in the batch report, so that an automatic repair is observable.
21. As an operator, I want a failed publication to retain a structured failure code and explanatory message, so that retries do not require reading SQLite manually.
22. As an operator, I want a retry to resume the same incomplete batch before processing new jobs, so that candidate contents and job state remain coherent.
23. As an operator, I want an empty batch to avoid creating publication journal work or a commit, so that Git history remains meaningful.
24. As an operator, I want a synthesis failure to prevent publication entirely, so that no sources or concepts from an invalid synthesis appear in the vault.
25. As a concept reader, I want concept changes to appear together with the successfully prepared batch sources, so that citations do not temporarily point to absent evidence.
26. As a source reader, I want committed source evidence never to be overwritten by publication recovery, so that immutable source versions remain trustworthy.
27. As a test author, I want deterministic failpoints at every durable publication boundary, so that crash windows can be proven without killing the process.
28. As a future maintainer, I want callers to use one small batch-publication interface rather than coordinate Git, filesystem, and SQLite state themselves, so that changes have locality.

## Implementation Decisions

1. The existing ingestion-batch module is deepened rather than split into pass-through helpers. Its external interface remains the batch CLI and the existing `BatchRunner.run` behavior; callers submit an already validated candidate and receive a batch report. This is the highest existing seam and is the sole public test surface for this feature.

2. Introduce a durable publication journal in SQLite, linked one-to-one with an ingestion batch. It records the stable batch ID, the validated ordered path set, content fingerprints for each candidate, the candidate-workspace identity, publication phase, local commit identifier when known, rollback snapshot identity, and recovery/failure metadata. Journal records are written before the first live-vault mutation.

3. The publication phase is a decision-rich state machine:

   ```text
   candidate_validated
     → rollback_snapshot_recorded
     → publishing
     → published_uncommitted
     → committed_unfinalized
     → finalized
     → cleanup_pending
     → complete

   publishing or published_uncommitted → rolled_back | recovery_blocked
   committed_unfinalized → finalized | recovery_blocked
   ```

   `finalized` records the commit against the batch and completes source-ready jobs. `cleanup_pending` is deliberately outside publication success so cleanup can be retried without changing the commit.

4. Candidate workspace remains outside the vault and becomes the source of truth for proposed content until finalization. The publication module materializes a manifest there with stable relative paths, operation type (write or deletion), and SHA-256 fingerprints. The manifest is validated before journaling and again during recovery.

5. Before publishing, the module captures a rollback snapshot outside the vault for every affected live path, including the distinction between an absent path and an existing file. Snapshot and candidate paths must resolve beneath their configured roots and may not traverse symlinks. The module publishes atomically per file, but treats the multi-file set as incomplete until Git commits it.

6. A failed Git commit or recovery of `publishing`/`published_uncommitted` restores the exact snapshot. It must first verify that each live path is still either the recorded pre-batch fingerprint or the candidate fingerprint. Any third state is a recovery-blocked condition: do not overwrite user changes, leave the journal intact, and return a precise report.

7. The Git adapter gains operations for: confirming a clean worktree, narrowly staging a prevalidated set, committing with the batch identifier, locating a commit by batch identifier, verifying that commit’s path set and tree content match the journal, and restoring only the explicit staged set after a failed staging attempt. No broad stage-all operation is introduced.

8. On startup under the existing single-process lock, recovery runs before claiming inputs or creating a new batch. Recovery processes at most the oldest unresolved publication journal first. A committed journal is finalized only after Git verification; an uncommitted journal is rolled back only after fingerprint verification. A blocked journal prevents new ingestion work.

9. SQLite finalization is idempotent. It changes only the source-ready jobs associated with the journaled batch and only when their prepared source hashes match the journal. Repeated recovery after successful finalization reports the existing result rather than committing again.

10. Raw-payload cleanup starts only after journal finalization. Its durable cleanup state remains attached to the job and is retried during recovery and normal batch startup. Cleanup cannot move a batch back into a publication phase or cause synthesis, rendering, or Git work to repeat.

11. Batch reports and status output gain explicit publication and recovery fields: current publication phase, recovery action, recovery block reason, commit identifier, and outstanding cleanup count. Existing report fields remain compatible where practical.

12. A narrowly scoped fault-injection adapter is accepted as an internal test seam, not a second public interface. It can raise immediately after each durable boundary: journal write, snapshot capture, each file publication, Git commit, SQLite finalization, and each cleanup change. Production supplies a no-op adapter.

13. This feature does not redesign source extraction, source-version allocation, synthesis adapters, YouTube claiming, or concept-citation semantics. The publication module assumes their existing candidate validation contract and makes only the commit/finalization handoff recoverable.

14. Assumptions: a local Git repository exists when an ingestion batch reaches publication; the configured state directory remains outside the vault and persists across restarts; and the process is the only writer while it holds the existing lock. If these assumptions are not met, the module fails preflight before consuming inputs.

## Testing Decisions

1. Tests exercise behavior only through the existing batch CLI or `BatchRunner.run` with a temporary vault, real local Git repository, temporary SQLite state directory, deterministic source/synthesis adapters, and the internal fault injector. They do not assert private SQL queries, helper calls, or in-memory phase transitions.

2. The current public-seam test style is prior art: temporary vaults with real Git and fixture adapters. Extend that style rather than unit-testing the publication implementation directly.

3. A successful non-empty batch publishes the exact validated sources, concepts, queue acknowledgement, and allowed deletions in one commit whose message identifies the batch; the SQLite jobs are finalized and saved HTML is cleaned afterward.

4. An empty batch creates no journal that reaches publication and no Git commit.

5. Pre-existing unrelated worktree changes stop before snapshot or publication, and none become staged or committed.

6. For each failpoint before Git commit, rerunning restores or preserves the pre-batch vault, does not create a commit, retains source-ready jobs and raw HTML safely, and reports recovery rather than creating a new batch.

7. For each failpoint after Git commit but before SQLite finalization, rerunning locates and verifies the existing commit, finalizes exactly once, performs eventual cleanup, and does not create another commit.

8. Tests cover writes, overwrites, and deletions, including a mix in one candidate. Rollback restores overwritten content, restores deleted content, and removes only files newly created by the candidate.

9. Tests prove that a changed live path with a fingerprint that matches neither the snapshot nor candidate blocks recovery without overwriting it.

10. Tests prove that altered candidate manifests, path traversal, absolute paths, symlink escapes, duplicate paths, untracked generated files, and a commit whose tree differs from the journal all fail safely.

11. Tests prove restart idempotency: invoking recovery repeatedly after an interrupted batch produces the same final vault, SQLite status, and one-or-zero commit count as appropriate.

12. Tests prove raw HTML is retained through every pre-finalization failure and that cleanup can fail and later succeed without re-rendering, re-synthesizing, or committing again.

13. Tests validate JSON/status output sufficiently to distinguish finalized, rollback-complete, committed-unfinalized, recovery-blocked, and cleanup-pending states from an operator’s perspective.

14. The verification run includes focused recovery tests, the full suite, static checking and formatting configured by the repository, plus a fixture-based dry-run and successful partial batch.

## Out of Scope

- Source-version allocation, explicit refresh behavior, or preventing pre-existing source overwrites outside the publication handoff.
- Article extraction, image transcription, HTTP acquisition, canonicalization, or saved-HTML discovery changes.
- Production YouTube OAuth, playlist, or transcript adapters.
- Redesigning the DeepSeek or Codex synthesis contract beyond accepting their already validated change set.
- Remote Git configuration, pushing, pull requests, branches, or issue-tracker migration.
- Concurrent edits made outside the ingestion process after publication begins; these are detected as recovery-blocked rather than automatically merged.
- Automatic repair of a Git repository whose history or object database is corrupt.

## Further Notes

This specification intentionally makes publication a deep module: its small interface hides the implementation complexity of candidate manifests, rollback snapshots, Git staging, commit discovery, SQLite finalization, and cleanup. The deletion test supports this shape: deleting the module would force every caller to coordinate those crash-sensitive details and would destroy locality.

No ADR is created yet. This is an implementation specification, not an accepted permanent decision record; create an ADR only if implementation resolves a contested long-lived policy such as the exact rollback strategy or journal-retention policy.

The Git remote remains intentionally untouched until the requested architecture improvement is implemented and verified.
