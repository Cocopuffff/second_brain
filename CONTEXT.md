# Second Brain Ingestion Domain

- **Ingestion batch**: one finite, manually invoked attempt to claim inputs, prepare successful source evidence, synthesize concepts, validate candidates, commit them locally, finalize durable state, and clean up committed raw HTML.
- **Job**: the durable SQLite record for one canonical article or YouTube source after it has been claimed.
- **Candidate workspace**: state outside live tracked vault directories where proposed source and concept changes are assembled and validated.
- **Publication**: the recoverable transition that makes a validated candidate visible in the vault and records one local Git commit for the batch.
- **Publication journal**: the durable SQLite record and external manifests that identify a batch candidate, rollback snapshot, publication phase, commit, and recovery result.
- **Finalization**: recording the batch commit and completed job state in SQLite after Git confirms the commit.
- **Raw-payload cleanup**: removal of a saved HTML input only after its source is safely committed; it is independently retryable.
- **Source evidence**: immutable, deterministic Markdown in `Sources/`; it is distinct from mutable concept interpretation in `Concepts/`.
