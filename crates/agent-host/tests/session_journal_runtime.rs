//! Session journal runtime test modules.

#[path = "support/metered_journal.rs"]
mod metered;

use agent_runtime::{
    core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        checkpoint::{CheckpointStore, TurnCheckpoint},
        clock::{Deadline, Timestamp},
        content::{Message, UserInput},
        ids::{SessionId, TurnId},
        journal::*,
        provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent},
        store::{SessionSnapshot, SessionStore, VersionedSessionState},
    },
    provider::fake::{FakeProvider, ScriptedStream},
    registry::RegistryRevision,
    runtime::{Runtime, RuntimeBuilder, StartSession},
};
use forge_agent_host::{
    SqliteProtectedRuntimeStore,
    session_journal::ForgeJournalStorage,
    session_journals::{ForgeJournalProjection, JOURNAL_MEMO_BYTES, SessionJournals},
    test_support::FileDb,
};
use std::sync::Arc;

#[path = "support/crash_matrix.rs"]
mod crash_matrix;
#[path = "support/credential_exclusion.rs"]
mod credential_exclusion;
#[path = "support/import_matrix.rs"]
mod import_matrix;

use helpers::*;

#[path = "session_journal_runtime/controls_and_collection.rs"]
mod controls_and_collection;
#[path = "session_journal_runtime/helpers.rs"]
mod helpers;
#[path = "session_journal_runtime/legacy_import.rs"]
mod legacy_import;
#[path = "session_journal_runtime/legacy_lcm.rs"]
mod legacy_lcm;
#[path = "session_journal_runtime/native_runtime.rs"]
mod native_runtime;
