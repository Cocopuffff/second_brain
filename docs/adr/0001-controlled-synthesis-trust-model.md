# ADR-0001: Controlled synthesis trust model

## Status

Accepted

## Context

Synthesis combines repository evidence, model responses, tool calls, and a
workspace-capable Codex process. Only deterministic repository code can safely
decide what is eligible for publication.

## Decision

Repository-owned adapters are trusted implementations of one internal adapter
seam, but every model response, tool call, and Codex workspace effect is
untrusted data. The `second_brain.synthesis` package owns catalog discovery,
immutable provenance, bounded reads, candidate workspace diffing, strict
normalization, and failure classification. `BatchRunner` receives only a
validated `SynthesisOutcome` or a safe `SynthesisFailure`.

DeepSeek is limited to the four narrow tools and a bounded loop. Codex runs
through its CLI's native `workspace-write` sandbox with approvals disabled,
ephemeral state, ignored user rules/config, and schema-validated output. No
third-party in-process adapters are supported in production.

## Consequences

Validation and publication policy have one owner and cannot be bypassed by a
legacy `synthesize()` path. Synthesis failures leave jobs source-ready and do
not mutate live vault files, Git, completion state, or raw-input cleanup.
