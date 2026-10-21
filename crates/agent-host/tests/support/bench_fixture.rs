//! Fixed A/B/C generators from agent-runtime v0.9.0 (8c19be5b), tests/support/fixture.rs.
use super::metrics::Drive;
use agent_runtime::{
    context::ProviderCacheCapability,
    core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        checkpoint::CheckpointStore,
        clock::{Clock, Timestamp},
        content::{Message, UserInput},
        error::RuntimeError,
        ids::SessionId,
        journal::RUNTIME_PREVIOUS_CACHE_NAMESPACE,
        provider::{
            CacheEndpointIdentity, Capabilities, FinishReason, ModelId, PromptCacheControl,
            ProviderStreamEvent,
        },
        store::SessionStore,
        tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome},
    },
    prelude::AllowAll,
    provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments},
    registry::RegistryRevision,
    runtime::{RuntimeBuilder, StartSession},
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Instant;
#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(100)
    }
}
#[async_trait]
pub trait TerminalMaintenance: Sync {
    async fn terminal(&self, session: &SessionId);
    async fn sample_wal(&self) -> u64;
}
#[derive(Debug)]
struct Echo;
#[async_trait]
impl LegacyTool for Echo {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Returns the exact text argument."
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object", "properties":{"text":{"type":"string"}}, "required":["text"], "additionalProperties":false})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default().with_spawn()
    }
    async fn invoke_legacy(
        &self,
        value: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(
            value["text"].as_str().unwrap_or_default(),
        ))
    }
}
fn reply(text: String) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}
fn tool(text: String, index: usize) -> ScriptedStream {
    let mut events = tool_call_fragments(
        0,
        &format!("call-{index}"),
        "echo",
        &json!({"text":text}).to_string(),
    );
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    ScriptedStream::new(events)
}

// Fixed xorshift64 generator. No RNG dependency; content and lengths are reproducible.
struct Text(u64);
impl Text {
    fn next(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as usize
    }
    fn prose(&mut self, low: usize, high: usize) -> String {
        const WORDS: &str = "the project contains several small services which process customer requests through secure queues every morning our team reviews recent changes and checks whether each result matches the expected behavior before releasing another version a clear boundary helps developers understand storage failures without losing useful context from earlier conversations please inspect configuration history compare response timing update relevant tests document decisions explain tradeoffs using concrete examples application network database model tools cache messages durable session operation replay window checkpoint transaction collection snapshot manifest compression identity content server client request summary user assistant task source branch commit index search report measure physical bytes logical object memory disk table query schema page value reference stable random ordered protected ordinary production runtime engine implementation performance correctness recovery restart error validation policy domain access permission token budget limit stream final text result structured language json field array chunk tree leaf node batch sequence timestamp revision writer reader pin retained released shared current previous next system workspace file directory code build test verify review support release baseline fixture generated realistic synthetic English words common natural varied details reasoning evidence answer example analysis design feature dependency isolated publish reusable atomic conflict stale lease retry grant authorization response turn planned terminal accepted pending ready completed finalized source target settings cache fragment exact canonical snapshot immutable incremental compact sealed nonce tag envelope compressed frame offset prefix bitmap hash digest fingerprint";
        let words: Vec<_> = WORDS.split_whitespace().collect();
        let len = low + self.next() % (high - low + 1);
        let mut out = String::new();
        while out.len() < len {
            if self.next() % 12 == 0 {
                out.push_str(&format!("{}", self.next() % 100_000));
            } else {
                out.push_str(words[self.next() % words.len()]);
            }
            out.push(' ');
        }
        out.truncate(len);
        out
    }
}
fn seed(n: usize, realistic: bool) -> Vec<Message> {
    let mut text = Text(0xbadc_0ffe_e123_4567);
    (0..n)
        .map(|index| {
            if !realistic {
                return Message::user(format!("seed message number {index}"));
            }
            Message::user(text.prose(100, 800))
        })
        .collect()
}
fn scripts(fixture: &str, tools: bool, turns: usize) -> Vec<ScriptedStream> {
    let mut text = Text(0x1234_9876_abcd_5678);
    let mut all = Vec::new();
    for turn in 0..turns {
        if tools && (fixture == "C" || turn != 0) {
            all.push(tool(
                if fixture == "A" {
                    "task-27".into()
                } else {
                    text.prose(500, 8000)
                },
                turn,
            ));
        }
        all.push(reply(if fixture == "A" {
            if turn == 0 {
                "bootstrap".into()
            } else if tools {
                "tool complete".into()
            } else {
                "delta reply".into()
            }
        } else {
            text.prose(200, 2000)
        }));
    }
    all
}
pub async fn drive<S: SessionStore + CheckpointStore + TerminalMaintenance + 'static>(
    store: Arc<S>,
    fixture: &str,
    n: usize,
    tools: bool,
    id: SessionId,
) -> Drive {
    let turns = if fixture == "C" { n / 4 } else { 2 };
    let mut capabilities = Capabilities::basic_streaming();
    if fixture != "A" {
        capabilities.cache = true;
        capabilities.override_prompt_cache(PromptCacheControl::Implicit);
    }
    let mut scripted = scripts(fixture, tools, turns).into_iter();
    let mut make = || {
        // C already builds a runtime per turn. Keep its unused fake-provider
        // request/call diagnostics bounded to that turn too. Script events,
        // model capabilities and the explicit cache endpoint remain identical.
        let count = if fixture == "C" {
            if tools { 2 } else { 1 }
        } else {
            scripted.len()
        };
        let provider = Arc::new(FakeProvider::new(
            "fake",
            capabilities.clone(),
            scripted.by_ref().take(count).collect(),
        ));
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider.clone())
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(4_000_000, 4_000_000, 4096),
            ))
            .clock(Arc::new(FixedClock))
            .session_store(store.clone())
            .checkpoint_store(store.clone());
        if fixture != "A" {
            builder = builder
                .cache_capability(ProviderCacheCapability::from_control(
                    RegistryRevision::new("bench-cache-1"),
                    "fake",
                    PromptCacheControl::Implicit,
                ))
                .cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
                    "bench-local-endpoint",
                    RegistryRevision::new("endpoint-1"),
                ));
        }
        if tools {
            builder = builder
                .legacy_approval_authority()
                .approval(Arc::new(AllowAll))
                .tool(Arc::new(Echo));
        }
        builder.build().unwrap()
    };
    let opening = Instant::now();
    let mut session = make()
        .start_session(StartSession::create(
            id.clone(),
            if fixture == "C" {
                vec![]
            } else {
                seed(n, fixture != "A")
            },
        ))
        .await
        .unwrap();
    let mut opens_us = vec![opening.elapsed().as_micros() as u64];
    let mut turns_us = vec![];
    let mut maintenance_us = vec![];
    let mut wal_bytes = vec![];
    let mut text = Text(0x9876_1234_5678_abcd);
    for turn in 0..turns {
        if fixture == "C" && turn % 25 == 0 {
            eprintln!("C/{n} {} turn {turn}/{turns}", std::any::type_name::<S>());
        }
        if fixture == "C" && turn != 0 {
            let opening = Instant::now();
            session = make()
                .start_session(StartSession::resume(id.clone()))
                .await
                .unwrap();
            opens_us.push(opening.elapsed().as_micros() as u64);
        }
        let input = if fixture == "A" {
            if turn == 0 {
                "bootstrap turn".into()
            } else if tools {
                "use the tool".into()
            } else {
                "one more message".into()
            }
        } else {
            text.prose(100, 800)
        };
        let start = Instant::now();
        session
            .run(UserInput::text(input))
            .await
            .unwrap()
            .completed()
            .await;
        turns_us.push(start.elapsed().as_micros() as u64);
        let start = Instant::now();
        store.terminal(&id).await;
        maintenance_us.push(start.elapsed().as_micros() as u64);
        wal_bytes.push(store.sample_wal().await);
        if fixture == "C" && turn + 1 != turns {
            session.shutdown().await.unwrap();
        }
    }
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    // Shutdown returns after publishing the final idle view and closing all pins.
    let snapshot = session.snapshot();
    session.shutdown().await.unwrap();
    wal_bytes.push(store.sample_wal().await);
    if fixture == "C" {
        assert_eq!(snapshot.history.len(), n);
        assert_eq!(snapshot.manifests.len(), 32);
    }
    if fixture != "A" {
        assert!(
            snapshot.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE].value["cache_identity"]
                .is_object()
        );
    }
    Drive {
        snapshot,
        opens_us,
        turns_us,
        maintenance_us,
        wal_bytes,
    }
}
