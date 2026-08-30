# Second Brain Ingestion Specification

**Status:** Living specification; SEC-5 complete, SEC-6 in progress
**Target location:** `Second Brain/System/Second Brain Ingestion Spec.md`  
**Vault:** `Second Brain`  
**Version:** 1.1

## Problem Statement

The user captures articles on Android as plain URLs and YouTube videos in a dedicated playlist. These inputs need to become durable evidence inside an Obsidian vault and feed a continuously reconciled collection of thematic concept notes.

Android capture must remain simple. Processing runs only on the Mac as a manually invoked batch after the user lets Obsidian Sync finish. The system must tolerate crashes and duplicate inputs without losing work.

Article and transcript sources must remain stable, immutable evidence. Concept notes are mutable interpretations that an agent may reorganize freely, but only inside `Concepts/`.

## Solution

Build a local, batch-oriented Python application that:

1. Claims article URLs, saved HTML files, and YouTube playlist entries into SQLite.
2. Records mobile queue acknowledgement intent at claim time and finalizes it only after successful publication.
3. Converts successful inputs into immutable, deterministic Markdown sources.
4. Replaces meaningful article images with in-situ textual explanations rather than retaining image files.
5. Runs concept synthesis once across the successful sources in each batch.
6. Validates source integrity, citations, executor scope, and Git state.
7. Creates one local Git commit for the completed portion of the batch.
8. Retains failure and retry state in SQLite.

SQLite, credentials, OAuth tokens, staging files, and temporary image downloads remain outside the synced vault.

## User Stories

1. As an Android user, I want to capture an article by adding its URL to one Markdown file, so that capture stays frictionless.
2. As an Android user, I want blank lines to remain valid in the capture file, so that its current format does not need to change.
3. As a YouTube user, I want to capture videos through a dedicated playlist, so that mobile capture requires no custom application.
4. As a vault owner, I want queue entries removed only after their source is published, so that capture queues do not hide unpublished work.
5. As a vault owner, I want claimed jobs and acknowledgement intent recorded before publication, so that a crash cannot lose work or acknowledge it prematurely.
6. As a vault owner, I want duplicate URLs and videos to be harmless, so that retrying capture does not duplicate sources.
7. As a vault owner, I want canonical URL matching, so that tracking parameters and fragments do not create duplicate articles.
8. As a vault owner, I want saved HTML preferred over network extraction, so that protected and JavaScript-heavy pages can still be processed.
9. As a vault owner, I want unqueued HTML files processed, so that saving an article is itself a valid capture method.
10. As a vault owner, I want HTML paired through declared URL metadata, so that the system never guesses from filenames.
11. As a vault owner, I want ordinary HTTP extraction attempted when saved HTML is unavailable, so that simple articles require no manual save.
12. As a vault owner, I want useful article structure preserved, so that source notes remain credible evidence.
13. As a vault owner, I want advertising and interface clutter removed, so that source notes contain article content rather than page chrome.
14. As a vault owner, I want meaningful images explained in place, so that their evidential meaning is not lost.
15. As a vault owner, I do not want article image files retained, so that Git and the vault remain text-oriented.
16. As a vault owner, I want uncertain image interpretation to fail visibly, so that the processor does not invent evidence.
17. As a researcher, I want physical source line citations, so that a claim can be checked against the immutable text.
18. As an Obsidian user, I want citations to open near their source line through Advanced URI, so that verification is quick.
19. As a video researcher, I want YouTube citations linked to timestamps, so that cited evidence is directly playable.
20. As a multilingual researcher, I want transcripts preserved in their original language while concepts are written in English.
21. As a researcher, I want manually created YouTube transcripts preferred, so that the best available evidence is used.
22. As a researcher, I want a source to fail when no transcript exists, so that unsupported audio transcription is not silently introduced.
23. As a vault owner, I do not want per-source AI summaries, so that `Sources/` remains evidence rather than interpretation.
24. As a vault owner, I want concept synthesis run over the whole successful batch, so that relationships across sources can emerge.
25. As a vault owner, I want existing concepts and aliases reconciled before new pages are created, so that synonyms do not fragment the knowledge base.
26. As a vault owner, I want disagreements preserved, so that synthesis does not average conflicting evidence into a false consensus.
27. As a vault owner, I want the agent free to restructure `Concepts/`, so that concept organization can improve over time.
28. As a vault owner, I want agent writes confined to `Concepts/`, so that sources and unrelated notes cannot be altered.
29. As an operator, I want pluggable synthesis executors, so that I can choose the DeepSeek tool-calling adapter or the Codex CLI adapter without changing publication semantics.
30. As an operator, I want actionable status and retry commands, so that failures do not require editing SQLite manually.
31. As an operator, I want one Git commit per non-empty completed batch, so that recovery and history remain understandable.
32. As an operator, I want no commit for an empty batch, so that Git history remains meaningful.
33. As an operator, I want interrupted batches recoverable, so that rerunning the command safely resumes work.
34. As an operator, I want a validation and dry-run mode, so that configuration can be checked without consuming queues.
35. As an operator, I want secrets and state excluded from the vault and Git, so that synchronization does not leak credentials.
36. As an operator, I want clear preflight instructions, so that I remember to let Obsidian Sync finish before ingestion.

## Implementation Decisions

### 1. Vault and state boundaries

The expected vault structure is:

```text
Second Brain/
├── ToIngest/
│   ├── To Ingest.md
│   ├── HTML Pairings.yaml
│   └── saved-article.html
├── Sources/
│   ├── Articles/
│   └── YouTube/
├── Concepts/
└── System/
```

Application code and its documentation may live under `System/`, but runtime state must not.

SQLite, lock files, staging data, downloaded images, API responses, OAuth tokens, and credentials live in a configurable platform-local state directory outside the vault.

Raw HTML, SQLite files, secrets, temporary files, and downloaded image binaries must be Git-ignored. Immutable source Markdown and mutable concept Markdown are Git-tracked.

Article image binaries are not copied into `Sources/Assets/` and are not Git-tracked.

### 2. Batch interface

The public CLI seam provides these capabilities:

- Initialize or validate configuration.
- Run one ingestion batch.
- Show batches, jobs, failures, and pending cleanup.
- Retry one job or eligible failed jobs.
- Validate the vault without consuming inputs.
- Perform a dry run against fixtures or discovered inputs.

A normal batch performs:

```text
acquire single-process lock
→ recover interrupted work
→ claim article URLs, saved HTML, and YouTube entries
→ record tracked-queue and external-acknowledgement intent
→ ingest every processable source
→ synthesize once over successful batch sources
→ validate candidate changes
→ publish candidate files
→ create one Git commit
→ finalize SQLite state
→ delete committed raw HTML payloads
→ report results and exit
```

There is no daemon, watcher, or assumption of continuous availability.

The CLI reminds the user to open Obsidian and allow Sync to finish before proceeding. Obsidian Sync itself is not automated.

### 3. SQLite job model

SQLite is the source of truth for work after capture.

The model records, at minimum:

- Stable job identifier.
- Source kind: article or YouTube.
- Canonical source key.
- Original locator.
- Associated input artifact.
- Batch identifier.
- Status.
- Attempt count.
- Retry eligibility.
- Structured failure code and human-readable error.
- Content hash and immutable source version.
- Claim, processing, completion, and update timestamps.
- Queue-acknowledgement state.
- Raw-payload cleanup state.
- Git commit identifier after completion.

Canonical source keys are unique. Claiming uses an atomic transaction and uniqueness constraint rather than a read-then-insert check.

Suggested lifecycle:

```text
claimed → processing → source_ready → complete
                    ↘ failed
```

`source_ready` means preparation succeeded and an exact source candidate is durably available outside the live vault. It is not completion. Only publication finalization may move a source-ready job to `complete`. A synthesis failure leaves the job and candidate source-ready for retry without refetching, retranscribing, reinterpreting images, reallocating a version, or changing rendered bytes.

An interrupted `processing` job is recoverable because batches are explicit finite runs. On restart, the application identifies an unfinished batch and safely resumes or resets its incomplete jobs.

Failures stay in SQLite. Retrying does not require re-adding a URL or video to a mobile queue.

### 4. Article URL claiming

At batch start:

1. Read `ToIngest/To Ingest.md`.
2. Parse plain HTTP or HTTPS URLs; allow blank lines.
3. Canonicalize and deduplicate them.
4. Insert or recognize their jobs in one committed SQLite transaction.
5. Record an acknowledgement intent for the claimed canonical jobs.
6. Before publication, reread the queue file and derive a candidate rewrite that removes only matching claimed URL lines while preserving unrelated lines.
7. Hand the tracked queue rewrite and acknowledgement job IDs to publication with the validated source and concept candidates.

The database claim always precedes queue removal. Because the Markdown queue is tracked vault content, SEC-5's publication transaction writes and commits the candidate queue rewrite. Finalization then records queue acknowledgement in SQLite. A failed synthesis or failed publication leaves the live queue unchanged.

If the process crashes after the database claim, the next run recognizes the existing jobs. Publication recovery either completes or rolls back the journaled queue rewrite with the rest of the batch.

Malformed or unsupported lines remain in the file and are reported rather than discarded.

### 5. URL canonicalization

Canonicalization is deterministic and conservative:

- Normalize scheme and hostname case.
- Apply IDNA hostname normalization.
- Remove fragments.
- Remove default ports.
- Normalize empty paths and safe dot segments.
- Remove a documented allowlist of known tracking parameters.
- Preserve unknown and potentially semantic query parameters.
- Normalize YouTube URLs to their video ID where applicable.
- Use declared canonical metadata only after validating that it is a supported absolute URL.

Canonicalization must not perform a network request.

A normal duplicate URL is not refetched. An explicit refresh creates a new immutable source version rather than overwriting the existing source.

### 6. Saved HTML discovery and pairing

Every `.html` or `.htm` file directly eligible under `ToIngest/` is discovered even when its URL does not appear in `To Ingest.md`.

Pairing priority is:

1. Valid HTML canonical-link metadata.
2. Valid `og:url` metadata.
3. An explicit mapping in `ToIngest/HTML Pairings.yaml`.

The manifest maps a relative HTML filename to an absolute original URL. Paths must resolve inside `ToIngest/`; traversal and symlink escapes are rejected.

The system never infers a URL from an HTML filename.

If no valid URL can be established, the HTML remains untouched and an actionable discovery error is reported.

A queued URL and an HTML file resolving to the same canonical URL become one job with the HTML as its preferred payload.

If multiple non-identical HTML files claim the same canonical URL in one batch, processing fails for that input rather than choosing arbitrarily. Byte-identical duplicates may be collapsed by hash.

Raw HTML is not deleted when claimed. It is deleted only after its canonical Markdown is included in a successful Git commit. Cleanup is independently retryable after crashes.

### 7. Article acquisition and extraction

Acquisition priority is:

1. Explicitly paired saved HTML.
2. Ordinary HTTP retrieval.
3. Actionable failure retained in SQLite.

HTTP retrieval uses bounded timeouts, redirects with a maximum limit, a descriptive user agent, response-size limits, and safe content-type validation. It does not automate browser sessions, logins, paywalls, CAPTCHA, or Cloudflare challenges.

The extractor preserves, when available:

- Title.
- Author.
- Publication date.
- Canonical URL.
- Headings.
- Paragraphs and their boundaries.
- Ordered and unordered lists.
- Blockquotes.
- Code blocks.
- Useful tables.
- Meaningful images and captions through the image-description rule.

It removes navigation, sidebars, footers, forms, cookie notices, advertisements, promotional units, related-content widgets, comments, tracking pixels, avatars, decorative icons, and other page chrome.

### 8. Image-description rule

Meaningful article-body images are replaced at their original position with this Markdown construct:

````markdown
```image
A concise description of what the image communicates and why it matters in this position.
Caption: Original caption, when present.
```
````

The fenced language identifier `image` is the unique decorator. Formatting and field ordering are deterministic.

The description must distinguish visible content from an inference supplied by surrounding article context. It must not invent illegible values, hidden labels, or unsupported conclusions.

Original captions are preserved verbatim subject to normal whitespace normalization. Useful alt text may be retained when it adds evidence rather than duplicating the description.

Decorative, tracking, branding, avatar, and advertising images are omitted without an image block.

Image bytes may be downloaded into temporary state solely for inspection and must be deleted afterward. They are never copied into the vault or committed.

This is the only permitted derived-description exception inside immutable `Sources/`. It is treated as visual transcription, not a source summary.

Image inspection is a distinct pluggable capability. If a meaningful image cannot be inspected reliably by the configured processor and its caption or alt text is insufficient to preserve its meaning, the article fails rather than silently losing or fabricating evidence.

### 9. Immutable article Markdown

Source Markdown uses UTF-8, LF line endings, a trailing newline, fixed YAML key ordering, stable heading conventions, and deterministic whitespace normalization.

Prose is hard-wrapped at 120 characters. Existing code blocks, tables, URLs, image blocks, and constructs whose meaning would be damaged by wrapping are exempt.

Fixed metadata includes:

- Source identifier.
- Source type.
- Canonical URL.
- Title.
- Author when known.
- Publication date when known.
- Capture timestamp fixed at first claim.
- Input method.
- Content hash.
- Source format version.
- Immutable source version.

The content hash covers the normalized evidence body and relevant stable metadata. A rerun of the same claimed payload produces the same candidate output.

Once committed, a source version is immutable. Explicit refresh creates a new version with a new hash and file rather than overwriting the old source.

Line numbers become stable only after final deterministic rendering. No later process may reformat committed source Markdown.

### 10. YouTube claiming

The application polls a configured playlist named `To Ingest`.

For each playlist item:

1. Extract and validate its video ID.
2. Insert or recognize the job in committed SQLite state.
3. Record the playlist-item acknowledgement intent with the claimed job.
4. After successful publication commit and recovery, remove the playlist item during idempotent finalization.
5. Record acknowledgement success or retryable failure.

If database claim succeeds but synthesis or publication fails, the playlist item remains. If the publication commit succeeds but playlist removal fails, finalization retries the acknowledgement without reacquiring or republishing the source. Seeing the same playlist item again is harmless.

YouTube credentials and OAuth refresh tokens remain outside the vault and Git.

### 11. YouTube transcript policy

Transcript priority is:

1. Manually created transcript in the source language.
2. Automatically generated transcript in the source language.
3. Failure if neither exists.

No audio download or Whisper fallback is included in version 1.

The immutable YouTube source preserves the transcript’s original language. It stores time ranges sufficiently granular for provenance and includes stable video metadata.

Concept synthesis is in English even when the evidence is Mandarin, Chinese, or another language.

### 12. Source evidence boundary

`Sources/` contains only:

- Stable metadata.
- Normalized article evidence.
- Original-language timestamped transcripts.
- Image descriptions allowed by the visual-transcription exception.

There is no per-source LLM summary.

Concept interpretation, thematic synthesis, recommendations, and cross-source conclusions must not be written into `Sources/`.

### 13. Concept synthesis

Synthesis runs once over all successfully prepared sources in the batch.

The public synthesis runner receives only:

- The batch identifier.
- The successful batch sources.

The controlled-synthesis package owns concept-catalog loading, committed immutable-source discovery, narrow read construction, executor selection, candidate workspaces, normalization, validation, and safe failure classification. Catalog descriptors contain normalized paths, titles, aliases, and ordered outbound links. Full concept and source bodies are available only through bounded reads, except that Codex receives the complete verified concept baseline in its isolated candidate workspace.

Before creating a concept, it searches existing filenames, titles, aliases, and closely matching concepts. It updates or restructures an existing concept when appropriate.

The agent chooses useful sections based on the evidence. No rigid concept template is imposed.

The agent may create, rewrite, rename, merge, split, reorganize, or delete redundant material inside `Concepts/`. It must preserve meaningful disagreements and cite conflicting sources separately.

The executor may not mutate `Sources/`, `System/`, `ToIngest/`, Obsidian configuration, or unrelated vault content.

### 14. Executor contract

Source discovery, SQLite state, source rendering, validation, Git operations, and filesystem boundaries remain deterministic application responsibilities.

The public synthesis seam is:

```text
build_controlled_synthesis(config) → SynthesisRunner
SynthesisRunner.run(batch_id, batch_sources) → SynthesisOutcome | SynthesisFailure
```

`SynthesisOutcome` contains one normalized `ChangeSet` and typed metadata. `SynthesisFailure` contains a stable failure category, a safe message, and typed executor identity. `BatchRunner` never parses executor output, builds provenance aliases, or publishes an adapter result directly.

At least two synthesis adapters are supported:

1. A DeepSeek API adapter using an explicitly configured supported model and a strict bounded tool loop. The API key comes from the environment.
2. A Codex CLI adapter whose invocation is constructed by the application and whose only writable project context is a fixed isolated candidate workspace.

Both adapters implement the same contract. They are not separate pipelines.

Direct API tools are exactly: list concept descriptors, read one concept, search concept titles and aliases, and read one exact immutable source version from the new batch or committed source catalog. Writes are represented as an untrusted structured result rather than unrestricted filesystem access. The loop allows at most 16 rounds, 64 tool calls, 25 search results, 100 KB per body, and the configured total deadline. Initial requests contain identifiers, descriptors, tool schemas, and the output contract, never evidence bodies.

The Codex adapter accepts `codex_executable` and optional `codex_model`; the retired `codex_command` setting is a migration error. Preflight checks the executable and required flags. The application invokes `codex exec` with `workspace-write`, approval policy `never`, ephemeral state, ignored user config and rules, strict config, no extra writable roots, a fixed candidate directory, and schema-validated output. After execution, the deterministic application validates the actual workspace diff and rejects the entire synthesis if any effect escapes `Concepts/`, modifies inputs, creates unexpected directories, or introduces a symlink or non-Markdown file. Optional stdout write/delete claims must match the observed diff exactly.

Repository-owned adapters are trusted implementations of the internal seam. Model responses, tool calls, and Codex workspace effects are untrusted data. Third-party in-process production adapters are unsupported. The durable decision is recorded in `docs/adr/0001-controlled-synthesis-trust-model.md`.

Path traversal, absolute paths, symlink escapes, case-folding tricks, and unexpected deletions outside the allowed scope must be rejected.

Image description is a separate capability from concept synthesis. An executor that lacks reliable image inspection may process image-free articles but must fail articles containing evidentially important images it cannot interpret.

### 15. Candidate staging and crash recovery

Candidate source and concept changes are prepared outside the live tracked directories until validation succeeds.

Publishing, committing, and finalizing are recoverable steps:

- Every batch has a stable batch ID.
- The Git commit includes that batch ID in its message or trailer.
- If files were published but the process crashed before commit, rerunning validates and completes or safely rolls back the same candidate state.
- If Git committed but SQLite was not finalized, rerunning locates the commit by batch ID and finalizes state without creating a duplicate commit.
- Raw HTML cleanup occurs only after the committed batch is confirmed.
- Cleanup interrupted after commit is retried without reprocessing the source.

Successful jobs may be committed even when other jobs in the same run fail. Failed jobs remain retryable and the batch report clearly states that it completed partially.

If synthesis fails, publication does not start. Prepared source candidates and their jobs remain `source_ready`, raw HTML is retained, tracked queue content stays unchanged, and live source or concept files, Git, completion state, finalization, and cleanup do not change.

### 16. Provenance

Every substantive synthesized paragraph normally has at least one provenance citation. Sentence-level citations are not forced when one paragraph-level citation clearly supports the whole paragraph.

Article citations use a readable physical line range as the canonical reference:

```markdown
[Source title · L84-L97](obsidian://adv-uri?vault=Second%20Brain&filepath=Sources%2FArticles%2F...&line=84)
```

Rules:

- The displayed range is canonical provenance.
- The Advanced URI hyperlink is a navigation convenience.
- Line and column values are 1-indexed.
- The link opens the starting line.
- Vault and filepath values are URL-encoded.
- The human-readable citation remains useful if Advanced URI is unavailable.

YouTube citations display a timestamp range and link to the start timestamp:

```markdown
[Video title · 12:14–13:02](https://www.youtube.com/watch?v=VIDEO_ID&t=734s)
```

Conflicting perspectives cite each side separately.

The README instructs the user to install and enable the Obsidian Advanced URI community plugin. The pipeline does not install or configure the plugin automatically.

### 17. Validation

A batch may commit only after validation confirms:

- All candidate Markdown is valid UTF-8 with LF endings and trailing newlines.
- YAML metadata uses the expected schema and fixed ordering.
- Source filenames and identifiers are stable.
- Content hashes match normalized content.
- No committed source version was overwritten.
- Article line citations reference an existing source and valid line range.
- YouTube citations use valid video IDs and timestamp ranges.
- Concept citations point only to known source versions.
- No executor change escapes `Concepts/`.
- No source image binary entered the vault or Git candidate.
- Every retained image block is well formed.
- Raw HTML scheduled for deletion has a safely committed source.
- Secrets, state, tokens, and staging files are not staged.
- The Git worktree has no unrelated changes that the batch would accidentally commit.

Unrelated pre-existing user changes are a stop condition for automatic batch commit unless they can be excluded safely and unequivocally.

### 18. Git behavior

Git is local only in version 1.

A non-empty successful or partially successful ingestion batch creates exactly one automatic commit containing that batch’s validated source and concept changes.

The commit message identifies the batch and summarizes successful and failed job counts.

The application stages only explicit validated paths. It never uses a broad “stage everything” operation.

No branch, pull request, remote push, or human approval gate is required.

Git provides history and recovery; SQLite remains the operational state store.

### 19. Configuration and secrets

Non-secret configuration covers:

- Vault name and location.
- External state-directory location.
- YouTube playlist identifier.
- Selected synthesis executor.
- Selected image-description processor.
- Retry and network limits.
- Advanced URI link behavior.
- DeepSeek base URL and model selection where applicable.
- Codex executable and optional model selection where applicable.

Secrets come from environment variables or OS-managed credential files outside the vault.

At minimum, the DeepSeek key uses an environment variable and `deepseek_model` is explicit. YouTube OAuth client material and refresh tokens remain outside the vault. Codex configuration accepts `codex_executable` and optional `codex_model`; `codex_command` is rejected.

Configuration errors fail during preflight before queue consumption.

### 20. Operational scale

The design targets approximately:

- 10 articles per week.
- 15 YouTube videos per week.
- Typical video length of 15 minutes.
- A modest historical backlog of about 20 articles and 40 videos.

SQLite and filesystem search are sufficient. The implementation must not introduce a vector database, graph database, hosted queue, daemon, or RAG infrastructure.

## Testing Decisions

### Public seams

Tests exercise behavior through two public seams:

1. The batch CLI using temporary vault, state, Git, HTTP, and YouTube fixtures.
2. The `second_brain.synthesis` public package using a fake DeepSeek transport, a fake Codex CLI executable, real temporary candidate workspaces, and deterministic image-processor adapters.

Tests do not directly assert private helpers or internal SQL implementation details.

### Required tests

1. A URL claim and acknowledgement intent are committed to SQLite before publication changes the capture file.
2. A crash after claim but before publication loses no job and leaves the live capture file unchanged.
3. A crash after the publication commit safely resumes queue acknowledgement from the journal and SQLite.
4. Duplicate and canonically equivalent URLs produce one job.
5. Blank lines remain accepted.
6. Malformed lines are preserved and reported.
7. Queue rewriting does not discard concurrently visible unrelated lines.
8. A queued URL uses matching saved HTML in preference to HTTP.
9. An unqueued HTML file is independently claimed and processed.
10. Canonical metadata takes precedence over `og:url`.
11. The explicit pairing manifest is used when HTML metadata is absent.
12. A filename alone is never treated as a URL.
13. Missing pairing information leaves HTML untouched.
14. Conflicting HTML payloads for one URL fail rather than being chosen arbitrarily.
15. HTTP extraction is attempted when no HTML exists.
16. HTTP extraction failure remains retryable in SQLite.
17. Page chrome and advertisement fixtures are removed.
18. Article paragraphs, lists, blockquotes, code, and tables are preserved.
19. Meaningful images become well-formed `image` fences at their original positions.
20. Decorative and advertisement images produce no block.
21. No image binary appears in the vault or staged Git paths.
22. An essential image with no reliable processor fails the article.
23. Reprocessing identical input produces byte-identical candidate Markdown.
24. YAML ordering, line wrapping, LF endings, and trailing newline are stable.
25. Refresh creates a new source version without overwriting the old version.
26. Physical line ranges remain stable after creation.
27. Advanced URI links encode the vault and path and use 1-indexed lines.
28. YouTube timestamp citations link to the correct starting second.
29. Manual source-language transcripts are preferred over automatic transcripts.
30. Automatic source-language transcripts are accepted when manual transcripts are unavailable.
31. A missing transcript fails without audio fallback.
32. A video is removed from the playlist only during finalization after successful publication.
33. Failed playlist removal is retried without duplicating, reacquiring, or republishing the job.
34. Concept alias reconciliation can reuse an existing concept.
35. Conflicting claims remain separately represented and cited.
36. An executor write outside `Concepts/` rejects the complete change set.
37. Traversal and symlink escape attempts are rejected.
38. A failed synthesis produces no commit, preserves the durable source-ready candidate, and deletes no raw HTML.
39. A successful batch creates one narrowly staged commit.
40. A partial batch commits successful work and retains failed jobs.
41. A crash after Git commit but before SQLite finalization creates no duplicate commit.
42. Raw HTML cleanup happens only after the corresponding commit is confirmed.
43. An empty batch creates no commit.
44. Pre-existing unrelated changes are never included in an automatic commit.
45. Secrets and state cannot enter the staging set.
46. DeepSeek's initial request contains no evidence bodies and all tool, round, search, body, and deadline limits fail safely.
47. Codex execution uses the fixed safety flags and rejects mismatched claims, modified inputs, unexpected directories, symlinks, and non-Markdown effects.
48. A failed synthesis preserves the exact durable source candidate and `source_ready` state while leaving publication, completion, queue acknowledgement, Git, and cleanup unchanged.
49. Retrying a source-ready job reuses the exact prepared candidate without reacquisition or rerendering.

Failpoints should be exposed at durable boundaries so crash-window tests remain deterministic.

Golden fixtures are appropriate for finalized source Markdown and citations, provided expected files are independently authored and not generated using the production formatter.

The final verification run includes focused tests, the complete suite, static checking, formatting checks, and one fixture-based dry-run batch.

## Out of Scope

Version 1 does not include:

- Obsidian Sync automation.
- Continuous monitoring or a daemon.
- Browser automation.
- Login, paywall, CAPTCHA, or Cloudflare circumvention.
- Audio download or Whisper transcription.
- Retained article image files.
- Git LFS.
- Per-source AI summaries.
- A vector database.
- A graph database.
- Hosted queues or workers.
- Remote Git pushes.
- Pull requests or review branches.
- Automatic installation of Obsidian plugins.
- Full-text semantic search infrastructure.
- Automatic research beyond the captured batch.
- Agent writes outside `Concepts/`.

## Further Notes

Before each real batch:

1. Open Obsidian on the Mac.
2. Wait for Obsidian Sync to finish.
3. Close or avoid editing queue/source/concept files during the batch.
4. Run preflight or dry-run when configuration has changed.
5. Run ingestion.
6. Review the concise batch report.
7. Allow Obsidian Sync to propagate the committed changes.

The implementation README must document initial YouTube OAuth setup, DeepSeek configuration, the Codex executor, the image-processing capability, Advanced URI installation, retries, recovery, source refresh, and safe handling of a dirty Git worktree.

This is the living product specification. Accepted architectural decisions in `docs/adr/` and domain terms in `CONTEXT.md` refine it. Implementers should not reopen settled product decisions unless the existing vault contains a direct technical conflict that cannot be resolved without changing observable behavior.
