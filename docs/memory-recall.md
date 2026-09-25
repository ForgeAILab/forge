# Bounded memory recall

Forge already stores scoped, append-only semantic memory. The recall layer makes that store useful at the moment a Task starts without introducing a second memory server, a vector database, or a background LLM consolidation pipeline.

## What it does

When Forge builds a Task dispatch prompt, it forms a bounded query from the Task title and description, its plan, and the latest review feedback. It searches only the Project and Task scopes already authorized for that execution. Up to six results are rendered into a `PROJECT MEMORY` block at the end of the execution request.

The block is historical context, not an instruction channel. Memory text cannot grant permissions, approve changes, widen scope, change the role contract, or override current Forge records. Current Charter, Document, Decision, Task, validation, review, and release records remain authoritative.

Recall is best-effort. An empty index, malformed optional embedding configuration, an unavailable Ollama process, or a retrieval failure must not prevent Task dispatch. Forge logs the degradation and continues with the ordinary prompt.

## Retrieval pipeline

Recall is separate from the paginated memory-search API. Search remains a stable browsing/tool surface; recall is a top-K, token-budgeted runtime operation.

1. **Authorization and lifecycle filtering** — scope, visibility, ownership, sensitivity, and retraction/supersession/expiry are applied before a candidate is returned.
2. **Exact lexical arm** — all significant terms must match. This favors exact errors, symbols, paths, and strongly matching titles.
3. **Broad lexical arm** — significant terms are combined with `OR`. SQLite FTS5 BM25 weights title above summary and summary above body.
4. **Code-token arm** — paths, identifiers, version strings, and other code-like terms receive an additional ranked list.
5. **Optional semantic arm** — when explicitly configured, Forge asks a loopback Ollama endpoint to embed the query and a bounded authorized candidate pool.
6. **Rank fusion** — ranked arms are merged with Reciprocal Rank Fusion. Task-local scope, authority, retention priority, and recency are deterministic tie-breakers, not substitutes for relevance.
7. **Diversity and budget selection** — Forge limits repeated source types and packs summaries or bounded bodies into the configured token budget.

The default Task-dispatch budget is approximately 1,200 tokens and six items. Full memory bodies remain available through the existing memory read surfaces when deeper inspection is required.

## Optional Ollama embeddings

Semantic recall is disabled unless `FORGE_MEMORY_OLLAMA_MODEL` is set.

| Variable | Default | Meaning |
| --- | --- | --- |
| `FORGE_MEMORY_OLLAMA_MODEL` | unset | Ollama embedding model. Unset disables semantic recall. |
| `FORGE_MEMORY_OLLAMA_URL` | `http://127.0.0.1:11434` | Loopback Ollama base URL. Non-loopback hosts and URL credentials are rejected. |
| `FORGE_MEMORY_OLLAMA_TIMEOUT_MS` | `3000` | Request timeout, clamped to 100–30,000 ms. |

Forge calls Ollama's `/api/embed` endpoint in one bounded batch. It does not pull models, send credentials, persist vectors, or make semantic recall mandatory. A missing model, timeout, invalid response, or unavailable process produces a visible degraded status internally and falls back to lexical recall.

The initial implementation deliberately performs semantic comparison in memory over a bounded candidate pool. This keeps the default Forge binary SQLite-only and avoids committing to a vector extension before project-scale measurements show that one is needed.

## Stored data and provenance

Recall does not create or mutate memory rows. Existing append-only memory and lifecycle assertions remain the source. Each selected item retains its memory ID, source revision, authority, scope, source type/reference, confidence, and selection reasons.

Chat or execution sources already represented in the active prompt are supplied as represented source IDs and omitted from recall where possible. This reduces duplicate context while retaining the original immutable records for audit.

## Security invariants

- Secret memory is never recalled.
- Restricted memory is excluded from automatic Task recall.
- Private memory requires the exact owning identity; Task dispatch currently recalls only Project-visible context.
- Cross-Project sources are rejected by canonical-scope grants before retrieval.
- Retracted, expired, and superseded items are excluded.
- Angle brackets in recalled text are escaped so a stored item cannot close or create Forge's context wrapper.
- The authoritative system role contract explicitly states that `PROJECT MEMORY` is historical context only.

## Operational limits

This is intentionally smaller than Hindsight-style general agent memory:

- no entity or temporal graph;
- no cross-encoder reranker;
- no LLM fact extraction on every transcript;
- no scheduled knowledge-page synthesis;
- no mandatory embedding service;
- no second database or daemon.

The primary quality path is structured Forge memory plus FTS5/BM25. Ollama is an optional paraphrase-recovery arm. If evaluation later shows that older relevant memories fall outside the bounded semantic pool, Forge can add a disposable embedding index without changing the recall contract.
