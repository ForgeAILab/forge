//! Journal commit failures and cancellation preserve recoverable chat turns.

use super::*;

// ---- one failed journal commit during an ordinary chat turn ----

type CutPoint = agent_runtime::core::journal::JournalCommitFailurePoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CutDomain {
    Ordinary,
    Protected,
}

/// Which commit of the turn the cut lands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CutPhase {
    /// Armed before the turn: the domain's first commit of the turn (the
    /// protected acceptance checkpoint, the ordinary terminal publication).
    TurnStart,
    /// Armed when the turn's provider call begins: the domain's first commit
    /// after provider I/O started.
    ProviderCall,
}

/// Arms one cut on `domain`'s next commit. Arming an engine whose cut is
/// still pending is refused.
fn arm_cut(
    journals: &forge_agent_host::session_journals::SessionJournals,
    domain: CutDomain,
    point: CutPoint,
) -> Result<(), agent_runtime::core::error::RuntimeError> {
    match domain {
        CutDomain::Ordinary => journals.ordinary().fail_next_commit_at(point),
        CutDomain::Protected => journals.protected().fail_next_commit_at(point),
    }
}

/// A scripted provider that can arm a journal cut as its next call begins,
/// so the cut lands on a commit after provider I/O started.
struct CutOnProviderCall {
    inner: Arc<FakeProvider>,
    journals: forge_agent_host::session_journals::SessionJournals,
    pending: std::sync::Mutex<Option<(CutDomain, CutPoint)>>,
}

impl std::fmt::Debug for CutOnProviderCall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CutOnProviderCall")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl agent_runtime::core::provider::Provider for CutOnProviderCall {
    fn describe(&self) -> Vec<agent_runtime::core::provider::ModelDescriptor> {
        self.inner.describe()
    }

    fn capabilities(&self, model: &agent_runtime::core::provider::ModelId) -> Option<Capabilities> {
        self.inner.capabilities(model)
    }

    async fn stream(
        &self,
        request: agent_runtime::core::provider::ProviderRequest,
        ctx: agent_runtime::core::provider::ProviderCallContext,
    ) -> Result<
        agent_runtime::core::provider::ProviderStream,
        agent_runtime::core::provider::ProviderError,
    > {
        let pending = self.pending.lock().unwrap().take();
        if let Some((domain, point)) = pending {
            arm_cut(&self.journals, domain, point).expect("the cut arms");
        }
        self.inner.stream(request, ctx).await
    }
}

/// What one journal commit cut during a normal Main-chat turn did to the
/// chat: the cut turn itself, the next turn on the same backend, and two
/// turns after a restart (fresh service, cold journal engines).
#[derive(Debug)]
struct CutOutcome {
    phase: CutPhase,
    domain: CutDomain,
    point: CutPoint,
    fired_in_cut_turn: bool,
    cut_turn: Result<(), String>,
    next_turn: Result<(), String>,
    restarted: [Result<(), String>; 2],
    /// User texts of the first provider request after the cut, to show the
    /// history the chat continued from.
    next_request_user_texts: Vec<String>,
}

impl CutOutcome {
    fn label(&self) -> String {
        format!("{:?}/{:?}/{:?}", self.phase, self.domain, self.point)
    }

    fn wedged(&self) -> bool {
        self.next_turn.is_err() || self.restarted.iter().any(Result::is_err)
    }
}

fn turn_result(
    result: Result<forge_agent_host::AgentTurnOutput, forge_agent_host::AgentHostError>,
) -> Result<(), String> {
    result.map(|_| ()).map_err(|error| format!("{error:?}"))
}

/// Two good turns, then one cut at `point` on one `domain` commit of a third
/// turn, all through Forge's real turn path
/// (`NativeAgentRuntimeBackend::run_turn`, which persists the session after
/// a failed turn).
async fn cut_chat_turn(
    phase: CutPhase,
    domain: CutDomain,
    point: CutPoint,
) -> (
    ChatFixture,
    NativeAgentRuntimeBackend,
    Arc<FakeProvider>,
    Result<(), String>,
) {
    let fixture = chat_fixture().await;
    let journals = fixture.service.protected_store().journals();
    let scripted = text_only_provider(8);
    let provider = Arc::new(CutOnProviderCall {
        inner: scripted.clone(),
        journals: fixture.service.protected_store().journals(),
        pending: std::sync::Mutex::new(None),
    });
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(provider.clone());
    for input in ["First turn", "Second turn"] {
        backend
            .run_turn(fixture.turn(input), Arc::new(NoopSink))
            .await
            .unwrap_or_else(|error| panic!("{input}: {error:?}"));
    }
    match phase {
        CutPhase::TurnStart => arm_cut(&journals, domain, point).expect("the cut arms"),
        CutPhase::ProviderCall => *provider.pending.lock().unwrap() = Some((domain, point)),
    }
    let cut = turn_result(
        backend
            .run_turn(fixture.turn("Cut turn"), Arc::new(NoopSink))
            .await,
    );
    assert!(
        provider.pending.lock().unwrap().is_none(),
        "{phase:?}: the cut turn made no provider call"
    );
    (fixture, backend, scripted, cut)
}

async fn run_cut_case(phase: CutPhase, domain: CutDomain, point: CutPoint) -> CutOutcome {
    // A second arm right after the turn tells whether the cut fired in it.
    // That probe re-arms the engine, so it runs on its own chat.
    let fired_in_cut_turn = {
        let (fixture, _, _, _) = cut_chat_turn(phase, domain, point).await;
        arm_cut(&fixture.service.protected_store().journals(), domain, point).is_ok()
    };
    let (fixture, backend, provider, cut_turn) = cut_chat_turn(phase, domain, point).await;
    let requests_before = provider.requests().len();
    let next_turn = turn_result(
        backend
            .run_turn(fixture.turn("Next turn"), Arc::new(NoopSink))
            .await,
    );
    let next_request_user_texts = provider
        .requests()
        .get(requests_before)
        .map(user_texts)
        .unwrap_or_default();
    let restarted_service =
        EmbeddedAgentService::new(Arc::clone(&fixture.db), b"tool-call-id-reuse-test-key");
    let restarted_backend = NativeAgentRuntimeBackend::new(restarted_service.protected_store())
        .with_provider_override(text_only_provider(2));
    let mut restarted = Vec::new();
    for input in ["After restart", "After restart again"] {
        restarted.push(turn_result(
            restarted_backend
                .run_turn(fixture.turn(input), Arc::new(NoopSink))
                .await,
        ));
    }
    CutOutcome {
        phase,
        domain,
        point,
        fired_in_cut_turn,
        cut_turn,
        next_turn,
        restarted: restarted.try_into().expect("two restarted turns"),
        next_request_user_texts,
    }
}

/// A transient journal failure (SQLite busy, a crash cut) on one commit of a
/// normal chat turn must cost at most that turn: the next turn — on the same
/// backend and after a restart — runs without manual repair, for every cut
/// point in both journal domains.
async fn assert_one_failed_commit_never_wedges_the_chat(phase: CutPhase) {
    let mut outcomes = Vec::new();
    for domain in [CutDomain::Ordinary, CutDomain::Protected] {
        for point in CutPoint::ALL.iter().copied() {
            outcomes.push(run_cut_case(phase, domain, point).await);
        }
    }
    for outcome in &outcomes {
        println!(
            "CUT {} fired_in_cut_turn={} wedged={} cut_turn={:?} next_turn={:?} \
             restarted={:?} next_request_users={:?}",
            outcome.label(),
            outcome.fired_in_cut_turn,
            outcome.wedged(),
            outcome.cut_turn,
            outcome.next_turn,
            outcome.restarted,
            outcome.next_request_user_texts,
        );
    }
    let unfired = outcomes
        .iter()
        .filter(|outcome| !outcome.fired_in_cut_turn)
        .map(CutOutcome::label)
        .collect::<Vec<_>>();
    assert!(unfired.is_empty(), "cuts that never fired: {unfired:?}");
    let wedged = outcomes
        .iter()
        .filter(|outcome| outcome.wedged())
        .map(CutOutcome::label)
        .collect::<Vec<_>>();
    assert!(wedged.is_empty(), "wedged chats: {wedged:?}");
    for outcome in &outcomes {
        // An unaccepted turn leaves nothing behind; an accepted one is a
        // normal (completed or failed) turn of the chat. Either way the
        // continued history holds each input at most once.
        let texts = &outcome.next_request_user_texts;
        assert!(
            texts.iter().filter(|text| *text == "Cut turn").count() <= 1
                && texts.last().map(String::as_str) == Some("Next turn"),
            "{}: next request users {texts:?}",
            outcome.label()
        );
    }
}

/// Regression: a protected cut on the turn's acceptance checkpoint failed the
/// turn before provider I/O, but the runtime left the unaccepted user input
/// in the live session's history. Forge's post-turn `persist()` published
/// that history to the ordinary domain while the protected head stayed at
/// the previous terminal checkpoint, and every later open failed "canonical
/// session history and terminal checkpoint are from different boundaries".
/// Needs agent-runtime with the unaccepted-turn rollback (after v0.8.0).
#[tokio::test]
async fn one_failed_journal_commit_at_turn_start_never_wedges_the_chat() {
    assert_one_failed_commit_never_wedges_the_chat(CutPhase::TurnStart).await;
}

#[tokio::test]
async fn one_failed_journal_commit_after_provider_io_never_wedges_the_chat() {
    assert_one_failed_commit_never_wedges_the_chat(CutPhase::ProviderCall).await;
}

/// A failed commit whose recovery read also fails fences the handle. Forge
/// reports the original turn failure, then reopens on the next request.
#[tokio::test]
async fn unresolved_acceptance_preserves_turn_failure_and_reopens() {
    for point in [CutPoint::AfterObjects, CutPoint::AfterCommit] {
        let fixture = chat_fixture().await;
        let store = fixture.service.protected_store();
        let provider = text_only_provider(6);
        let backend =
            NativeAgentRuntimeBackend::new(store.clone()).with_provider_override(provider.clone());
        backend
            .run_turn(fixture.turn("First turn"), Arc::new(NoopSink))
            .await
            .unwrap();
        let requests = provider.requests().len();
        store
            .journals()
            .storage()
            .fail_commit_observation_for_test();
        store
            .journals()
            .protected()
            .fail_next_commit_at(point)
            .unwrap();
        let error = backend
            .run_turn(fixture.turn("Unresolved input"), Arc::new(NoopSink))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                forge_agent_host::AgentHostError::RuntimeWithUsage { .. }
            ),
            "{error:?}"
        );
        assert!(error.to_string().contains("turn failed"), "{error:?}");
        assert!(
            !error.to_string().contains("fenced"),
            "cleanup must not replace the turn failure: {error:?}"
        );
        assert_eq!(
            provider.requests().len(),
            requests,
            "no provider call before acceptance"
        );
        backend
            .run_turn(fixture.turn("Next turn"), Arc::new(NoopSink))
            .await
            .expect("fresh handle recovers");
        let texts = user_texts(provider.requests().last().unwrap());
        assert_eq!(
            texts
                .iter()
                .filter(|text| *text == "Unresolved input")
                .count(),
            usize::from(point.is_after_commit()),
            "{point:?}: reopened history follows the durable outcome"
        );
        assert_eq!(texts.last().map(String::as_str), Some("Next turn"));
    }
}

async fn assert_cancellation_after_journal_failure_preserves_envelope(after_provider: bool) {
    for point in [CutPoint::AfterObjects, CutPoint::AfterCommit] {
        let fixture = chat_fixture().await;
        let store = fixture.service.protected_store();
        let scripted = text_only_provider(8);
        let provider = Arc::new(CutOnProviderCall {
            inner: scripted.clone(),
            journals: store.journals(),
            pending: std::sync::Mutex::new(None),
        });
        let backend =
            NativeAgentRuntimeBackend::new(store.clone()).with_provider_override(provider.clone());
        backend
            .run_turn(fixture.turn("First turn"), Arc::new(NoopSink))
            .await
            .unwrap();
        let previous_calls = scripted.requests().len();
        store
            .journals()
            .storage()
            .fail_commit_observation_for_test();
        if after_provider {
            *provider.pending.lock().unwrap() = Some((CutDomain::Protected, point));
        } else {
            arm_cut(&store.journals(), CutDomain::Protected, point).unwrap();
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        backend.set_failure_barrier_for_test(barrier.clone());
        let request = fixture.turn("Unresolved cancelled input");
        let cancellation = request.cancellation.clone();
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(backend.run_turn(request, Arc::new(NoopSink)), async {
                // The host consumed the failure event before cancellation becomes ready.
                barrier.wait().await;
                cancellation.cancel();
                barrier.wait().await;
            })
        })
        .await
        .expect("failure observation and cancellation complete at the barrier");
        let error = result.unwrap_err();
        assert!(
            !error.to_string().contains("fenced"),
            "cleanup escaped: {error:?}"
        );
        let forge_agent_host::AgentHostError::RuntimeWithUsage {
            message,
            failure,
            usage_reports,
            provider_auth_rejected,
        } = error
        else {
            panic!("cancelled turn lost its envelope: {error:?}");
        };
        assert_eq!(message, "turn cancelled");
        assert_eq!(failure, api_types::TurnFailure::Unclassified);
        assert!(!provider_auth_rejected);
        if after_provider {
            assert_eq!(scripted.requests().len(), previous_calls + 1);
            assert_eq!(
                usage_reports.len(),
                1,
                "{point:?}: provider usage remains per-turn"
            );
            assert_eq!(usage_reports[0].input_tokens, Some(600));
            assert_eq!(usage_reports[0].output_tokens, Some(60));
            assert_eq!(
                usage_reports[0].telemetry_state,
                forge_agent_host::AgentTurnTelemetryState::Metered
            );
        } else {
            assert_eq!(scripted.requests().len(), previous_calls);
            assert!(
                usage_reports.is_empty(),
                "no provider I/O before acceptance"
            );
        }
        backend
            .run_turn(fixture.turn("Next turn"), Arc::new(NoopSink))
            .await
            .expect("cancelled handle closes and reopens");
        if !after_provider {
            let texts = user_texts(scripted.requests().last().unwrap());
            assert_eq!(
                texts
                    .iter()
                    .filter(|text| *text == "Unresolved cancelled input")
                    .count(),
                usize::from(point.is_after_commit())
            );
        }
    }
}

#[tokio::test]
async fn cancellation_after_unresolved_acceptance_preserves_envelope_and_reopens() {
    assert_cancellation_after_journal_failure_preserves_envelope(false).await;
}

#[tokio::test]
async fn cancellation_after_provider_journal_failure_preserves_nonzero_usage_and_reopens() {
    assert_cancellation_after_journal_failure_preserves_envelope(true).await;
}
