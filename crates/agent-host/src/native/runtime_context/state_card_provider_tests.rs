//! State-card provider request and wire-requirement tests.

use super::*;
use agent_runtime::{
    core::provider::{
        Capabilities, FinishReason, ModelDescriptor, ProviderCallContext, ProviderError,
        ProviderRequest, ProviderStream, ProviderStreamEvent,
    },
    provider::fake::{FakeProvider, ScriptedStream},
};

/// A recording provider whose wire, like Anthropic's, cannot carry an
/// assistant message emptied by reasoning projection.
#[derive(Debug)]
struct StrictWire(Arc<FakeProvider>);

#[async_trait]
impl Provider for StrictWire {
    fn requires_nonempty_assistant_content(&self) -> bool {
        true
    }
    fn describe(&self) -> Vec<ModelDescriptor> {
        self.0.describe()
    }
    fn capabilities(&self, model: &ModelId) -> Option<Capabilities> {
        self.0.capabilities(model)
    }
    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.0.stream(request, ctx).await
    }
}

/// The state-card adapter wraps every chat provider. When the runtime
/// drops another model's reasoning from a reasoning-only assistant
/// message, the wrapped wire's requirement must reach the planner so the
/// emptied message is left out of the request instead of being planned
/// (and cache-marked) as an empty message the wire then discards.
#[tokio::test]
async fn state_card_adapter_keeps_the_wire_nonempty_assistant_requirement() {
    let marker = "[forge-state-card:test]\n".to_owned();
    let recorder = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "answer".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));
    let provider = ServerStateCardProvider {
        inner: Arc::new(StrictWire(recorder.clone())),
        marker: marker.clone(),
    };
    assert!(provider.requires_nonempty_assistant_content());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(Arc::new(provider))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(8_000, 8_000, 1_000),
        ))
        .context_contributor(Arc::new(ServerStateCard {
            card: format!("{marker}## SERVER-PROVIDED STATE CARD\n- v1\n"),
        }))
        .build()
        .expect("runtime builds");
    let foreign_only = Message::assistant(vec![ContentPart::Reasoning {
        text: "another model's thought".into(),
        redacted: false,
        signature: Some("foreign-signature".into()),
        producer: Some(agent_runtime::core::content::ReasoningProducer {
            provider: "other".into(),
            model: ModelId::new("other-model"),
        }),
    }]);
    let session = runtime
        .start_session(StartSession::new().with_history(vec![
            Message::user("earlier question"),
            foreign_only,
            Message::user("follow-up"),
            Message::assistant(vec![ContentPart::text("earlier answer")]),
        ]))
        .await
        .expect("session starts");
    session
        .run(UserInput::text("next"))
        .await
        .expect("turn runs");

    let requests = recorder.requests();
    assert_eq!(requests.len(), 1);
    let messages = &requests[0].messages;
    assert!(
        messages
            .iter()
            .all(|message| message.role != Role::Assistant || !message.content.is_empty()),
        "an emptied assistant message reached the provider: {messages:?}"
    );
    assert!(
        messages
            .iter()
            .any(|message| message.joined_text() == "earlier answer")
    );
}
