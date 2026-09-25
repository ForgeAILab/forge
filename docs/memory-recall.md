# Lightweight Memory Recall

## Status

This document describes the first lightweight recall slice for Forge's existing
semantic-memory store. It does **not** introduce a second memory service, a
vector database, or an LLM extraction pipeline.

The implementation lives in `services::memory_source` and builds on Forge's
existing append-only `memory_item` rows, canonical scope grants, lifecycle
assertions, sensitivity filtering, and context-manifest provenance.

## Problem

Forge already records execution summaries, review outcomes, comments,
transitions, Agent Chat messages, decisions, procedures, and other durable
context. Existing memory search is intentionally deterministic and pageable,
but its FTS query requires every whitespace-separated term to match.

That behavior is appropriate for explicit lookup, but it is brittle for agent
recall. A goal such as:

```text
why do task leases prevent agent collisions?
```

may miss a useful memory titled `Task leases isolate workspaces` merely because
one conversational word is absent.

Building Hindsight's complete retain/recall/reflect stack would solve a much
larger problem than Forge needs. Forge already has authoritative domain events,
scope isolation, provenance, and structured memory types. The missing primitive
is a bounded way to retrieve related project knowledge before an agent
rediscovers it.

## Search and recall remain separate

### Search

`ForgeMemorySource::search` retains the existing behavior:

- stable authority/retention/recency ordering;
- opaque cursor pagination;
- exact all-term FTS matching;
- scope and lifecycle filtering before body retrieval.

This remains suitable for inspection and explicit lookup.

### Recall

`ForgeMemorySource::recall` is non-pageable and bounded to at most 12 records.
It is intended for context construction rather than browsing.

The request contains:

```rust
ForgeMemoryRecallQuery {
    query: String,
    limit: u32,
    represented_source_ids: Vec<String>,
}
```

The response contains the selected memory records, salient query terms,
selection explanations, the authorized candidate count, and source IDs that
were suppressed because recent history or LCM already represents them.

`search` also uses recall as a first-page fallback when the exact search
produces no usable record. Existing successful searches and cursor pages are
unchanged.

## Recall algorithm

The lexical recall path deliberately requires no embedding model.

1. Normalize at most eight query terms.
2. Remove common conversational stop words. A stop-word-only query falls back
   to its literal terms rather than becoming empty.
3. Run one all-salient-terms FTS arm when there is more than one term.
4. Run one FTS arm for each salient term.
5. Apply Forge's existing canonical-scope, visibility, lifecycle, and secret
   filtering inside every repository query.
6. Suppress restricted records unless the host explicitly permits them.
7. Suppress source IDs already represented by active chat history or LCM.
8. Fuse the result lists using integer reciprocal-rank fusion.
9. Apply deterministic field coverage, authority, retention, timestamp, and ID
   tie-breakers.
10. Deduplicate records that point at the same source and return the bounded
    top results.

The all-term arm receives twice the contribution of a single-term arm. This
rewards an item that covers the complete goal without allowing one
high-authority but weakly related record to dominate several strong lexical
matches.

## Security invariants

Recall does not create a new authorization path.

- The `ForgeMemorySource` is still immutable after binding to one identity and
  canonical scope.
- Repository ACL filtering occurs before a candidate body is returned.
- Secret memories never enter the candidate set.
- Restricted memories remain host-owned by default.
- Retracted, superseded, and expired memories remain excluded.
- Retrieved text remains context, not instructions. It cannot grant tools,
  approvals, permissions, repository access, or a wider scope.
- Recall never searches a project or chat named only in memory text.
- Source suppression happens after authorization and cannot be used to infer
  inaccessible rows.

## Why there is no vector database in this slice

The initial goal is to reduce redundant background gathering with the smallest
operational footprint. SQLite FTS and Forge's structured authority metadata are
already local, deterministic, and available in every installation.

Optional embeddings should only be added after a Forge-specific evaluation
shows material lift over lexical recall. They must remain a derived,
rebuildable index rather than authoritative memory.

A later Ollama implementation should use an interface similar to:

```rust
#[async_trait]
pub trait MemoryEmbedder: Send + Sync {
    fn provider_id(&self) -> &str;
    fn model_id(&self) -> &str;
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}
```

Recommended constraints:

- `provider = "none"` by default;
- support only `none` and `ollama` initially;
- never download a model automatically;
- require explicit opt-in for non-loopback Ollama endpoints;
- batch background indexing;
- key vectors by memory ID, provider, model, dimensions, and content hash;
- exact cosine search first;
- degrade immediately to lexical recall on timeout or provider failure;
- record the retrieval/index revision in the context manifest;
- do not embed secrets, raw credentials, or unrestricted provenance JSON.

Approximate-nearest-neighbor extensions should be considered only after exact
scanning is measured as a real bottleneck.

## Runtime integration boundary

This change establishes the agent-facing recall primitive and safe fallback.
The next runtime-wiring change should call it at bounded lifecycle moments:

- initial Task claim;
- the first turn of a new Agent Chat topic;
- resume after a material context revision;
- follow-up after a failed execution or review;
- a Task revision that changes the context fingerprint.

It should not run before every turn.

Selected records should be rendered as historical context with memory ID,
source revision, authority, confidence, scope, and selection reason. The
context manifest should record included, deduplicated, and token-truncated
sources so Forge can later answer exactly what the agent knew when it acted.

## Evaluation

Before enabling automatic injection broadly, compare:

1. existing exact FTS search;
2. lightweight lexical recall;
3. lexical recall plus optional Ollama embeddings.

Use real Forge tasks across these query classes:

- exact symbols and file paths;
- error strings;
- paraphrased architecture questions;
- decision rationale;
- prior failure and resolution;
- approved procedures;
- superseded or disputed memory;
- active-history duplication;
- private, restricted, and secret rows;
- prompt-injection text stored in memory.

Track:

- Recall@5;
- mean reciprocal rank;
- nDCG@5;
- zero-result rate;
- unauthorized-memory leak rate, which must remain zero;
- median and p95 latency;
- injected token count;
- duplicate-source rate;
- hybrid lift over lexical-only recall;
- how often an agent still needs a manual memory search.

## Follow-up sequence

1. Wire bounded recall into Task and Agent Chat context assembly.
2. Persist recall selection/disposition in context manifests.
3. Add an authority-controlled `memory.record` operation for durable agent
   observations and proposals.
4. Add optional Ollama embeddings only if the benchmark demonstrates a useful
   improvement.
5. Consider a small derived Project brief only if individual-memory recall is
   insufficient for architectural continuity.
