# Second Brain Ingestion Domain

- **Ingestion batch**: one finite, manually invoked attempt to claim inputs, prepare successful source evidence, synthesize concepts, validate candidates, commit them locally, finalize durable state, and clean up committed raw HTML.
- **Job**: the durable SQLite record for one canonical article or YouTube source after it has been claimed.
- **Candidate workspace**: state outside live tracked vault directories where proposed source and concept changes are assembled and validated.
- **Publication**: the recoverable transition that makes a validated candidate visible in the vault and records one local Git commit for the batch.
- **Publication journal**: the durable SQLite record and external manifests that identify a batch candidate, rollback snapshot, publication phase, commit, and recovery result.
- **Finalization**: recording the batch commit and completed job state in SQLite after Git confirms the commit.
- **Raw-payload cleanup**: removal of a saved HTML input only after its source is safely committed; it is independently retryable.
- **Source evidence**: immutable, deterministic Markdown in `Sources/`; it is distinct from mutable concept interpretation in `Concepts/`.
- **Source preparation**: the durable seam between acquisition and synthesis. It extracts and renders an acquired payload once, validates the exact UTF-8 bytes, allocates an immutable `source_id:vN` identity, persists a manifest and rendered payload under the external state directory, and only then marks the job `source_ready`.
- **Source candidate**: a rehydratable prepared source with its immutable identity, final vault path, exact rendered bytes, content and byte hashes, evidence bounds, and tracked queue/raw-input intent. Its manifest and identity hashes remain after publication cleanup.
- **Controlled synthesis**: the single trusted application boundary that discovers catalog and immutable provenance, exposes bounded reads to an adapter, normalizes untrusted output, and hands publication either a validated change set or a typed failure.
- **Synthesis outcome**: the validated pair of a `ChangeSet` and typed metadata. Publication may consume it; no adapter result is publishable by itself.
- **Source-ready job**: a prepared source job awaiting successful synthesis and publication. It is durable preparation, not completion; synthesis failure leaves it unchanged and performs no publication or cleanup.
- **Source-ready recovery**: the oldest valid source-ready batch is resumed before new acquisition. Recovery loads candidates from state, so it does not rediscover files, access HTTP or YouTube, rerun extraction, or rerender images. Legacy source-ready rows without candidates fail closed as retryable `candidate_missing` jobs.
