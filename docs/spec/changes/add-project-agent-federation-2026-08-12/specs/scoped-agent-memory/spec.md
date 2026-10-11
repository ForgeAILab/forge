## ADDED Requirements

### Requirement: Scoped and ACL-First Memory

Every memory record SHALL carry explicit account, Agent Chat, Project, or Task scope; visibility; optional owner identity; authority; provenance; and lifecycle metadata. Forge SHALL authorize candidates before body retrieval, full-text matching output, snippet generation, counts, cursor construction, or ranking metadata so inaccessible records are neither returned nor existence-leaked.

#### Scenario: Identity-private isolation

- **WHEN** identity A stores private memory and identity B serves the same Project later
- **THEN** identity B receives no body, title, snippet, count, cursor effect, or distinguishable error for A's private record

#### Scenario: Project isolation

- **WHEN** an embedded agent searches memory in Project A
- **THEN** no Project B record affects candidates, counts, snippets, cursors, or results

#### Scenario: Main scope does not imply Project memory

- **WHEN** the Main Agent searches global memory without an explicitly authorized published Project record
- **THEN** no Project Agent Chat, Project-private, or Task record affects candidates, counts, snippets, cursors, or results

#### Scenario: Reference does not grant access

- **WHEN** an inaccessible memory ID appears in a chat message, handoff, repository file, or model output
- **THEN** memory get/search still applies the caller's original authorization

### Requirement: Explicit Memory Authority and Promotion

Memory authority SHALL be observation, hypothesis, proposal, decision, verified fact, or procedure and SHALL remain distinct from confidence or model wording. A record SHALL NOT be surfaced as a Decision, verified fact, or procedure without explicit authorized promotion/evidence.

#### Scenario: Hypothesis stays a hypothesis

- **WHEN** an agent stores a confident hypothesis
- **THEN** normal context labels and ranks it as a hypothesis
- **AND** it cannot satisfy a required Decision or verified-fact context slot

#### Scenario: Promote with evidence

- **WHEN** an authorized actor promotes a hypothesis using accepted Decision or verification evidence
- **THEN** Forge creates an attributed promotion/new record linked to the source
- **AND** provenance shows both records and the promotion evidence

### Requirement: Immutable Publication and Lifecycle

Memory bodies SHALL remain append-only. Publishing private/global memory to a Project or Project memory to the bounded global portfolio SHALL create a new record linked to its source; supersession, retraction, dispute, and expiry SHALL use new records or lifecycle assertions rather than body mutation or in-place ACL widening.

#### Scenario: Publish global discovery memory in a handoff

- **WHEN** the Main Agent is authorized to publish selected discovery context to a Project
- **THEN** Forge creates a distinct Project-visible record with its own ID, authority, creator, and source link
- **AND** the original global/private record remains unchanged and inaccessible outside its prior ACL

#### Scenario: Publish bounded Project outcome globally

- **WHEN** Project policy publishes an outcome for portfolio use
- **THEN** Forge creates a bounded account-visible projection/record
- **AND** it does not expose the Project Agent Chat or private source body implicitly

#### Scenario: Supersession and retraction preserve audit

- **WHEN** a record is superseded or retracted
- **THEN** normal search/context omits it as active truth
- **AND** authorized provenance inspection retains the original body, attribution, and lifecycle evidence

### Requirement: Bounded Forge Memory Source

Forge SHALL expose scoped semantic memory to Agent Runtime through a `MemorySource` bound immutably to the admitted identity and canonical Main Chat, Project Chat, or Task scope. It SHALL return already-authorized, ranked, bounded records with stable IDs, revisions, sensitivity, and retention priorities; Agent Runtime SHALL NOT write Forge memory or infer broader scope.

`ForgeMemorySource` was removed as unwired and must be reintroduced by the change that wires this memory source.

#### Scenario: Required records survive pressure

- **WHEN** selected context exceeds the runtime budget
- **THEN** accepted Decisions/procedures and active commitments have higher retention priority than optional low-authority memory
- **AND** final inclusion/truncation is recorded by the Agent Runtime manifest

#### Scenario: Secret content is excluded

- **WHEN** a candidate contains credentials, protected checkpoint content, or a disallowed sensitivity class
- **THEN** it is not returned as normal memory or included in a normal context manifest

#### Scenario: Active chat history is not duplicated

- **WHEN** a raw chat-message memory record is already represented by the active identity's LCM/recent canonical history
- **THEN** `ForgeMemorySource` suppresses that candidate
- **AND** the Forge context manifest records a bounded deduplication reason

### Requirement: Chat-Derived Memory Preserves Source Authorization

Automatic indexing SHALL create semantic records only from finalized canonical Agent Chat messages and SHALL preserve source chat, owning account/Project, author identity, and publication authorization. Indexing SHALL consume committed domain events idempotently, and failure SHALL NOT roll back the source message or turn outcome.

#### Scenario: Project Chat memory is absent from Main context

- **WHEN** the Main Agent builds context without an explicit published Project record
- **THEN** Project Chat-derived records are excluded before matching, snippets, counts, cursors, and ranking

#### Scenario: Final chat message is indexed once

- **WHEN** a canonical assistant message and domain event commit
- **THEN** the indexing consumer creates at most one scoped memory record for that source message
- **AND** replay does not duplicate it

#### Scenario: Partial stream is not indexed

- **WHEN** a turn streams partial deltas but never commits a canonical assistant message
- **THEN** no semantic memory record is created from those deltas

#### Scenario: Indexing failure is non-fatal

- **WHEN** semantic indexing fails after a chat message commits
- **THEN** the message and responder outcome remain successful
- **AND** the consumer can retry from durable event/idempotency state

### Requirement: Handoff Memory Is Explicit Publication

A Main-to-Project handoff SHALL NOT make the Main Agent timeline or memory namespace readable by the Project Agent. Any memory included in a handoff SHALL be a new authorized publication or bounded copied fragment with source provenance and active content guarding.

#### Scenario: Handoff cites private global memory

- **WHEN** a proposed handoff references Main Agent private memory without publication authority
- **THEN** Forge omits/denies the protected body before target message or memory creation
- **AND** the Project Agent cannot retrieve it by source ID

#### Scenario: Published handoff memory is revised

- **WHEN** the Main Agent later corrects published discovery context
- **THEN** Forge appends a new linked publication/supersession record
- **AND** the original handoff and memory remain immutable and auditable
