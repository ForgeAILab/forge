use std::io::{self, Read};

use anyhow::{bail, Context, Result};
use api_types::{
    AgentChatDetailResponse, AgentChatListQuery, AgentChatListResponse,
    AgentChatMessageListResponse, AgentChatMessagesQuery, AgentHandoffResponse,
    AgentProfileResponse, AgentProviderId, AgentResponse, AgentSessionResponse,
    CanonicalScopeRequest, CommitmentEvidenceResponse, CommitmentResponse,
    CompleteCommitmentRequest, ConnectEmbeddedProfileRequest, ConnectedEmbeddedAgentResponse,
    ConnectedEmbeddedProfileResponse, ContextManifestListResponse, ContextManifestResponse,
    CreateAgentHandoffRequest, CreateAgentSessionRequest, CreateCommitmentRequest,
    CreateEmbeddedAgentRequest, CreateProviderEntryRequest, DisconnectCredentialResponse,
    EffectivePermissionsResponse, MainAgentBindingResponse, ProjectAgentBindingResponse,
    ProviderEntriesResponse, ProviderEntryResponse, RenameProviderEntryRequest,
    SendAgentChatMessageRequest, SendAgentChatMessageResponse, SessionVersionRequest,
    SetMainAgentBindingRequest, SetProjectAgentBindingRequest, SteerAgentSessionRequest,
    TransferCommitmentRequest, UpdateCommitmentRequest,
};
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    client::ForgeClient,
    output::print_json,
    password_prompt::{prompt_password, stdin_is_terminal},
    provider_login, OutputFormat,
};

#[derive(Args)]
pub struct EmbeddedArgs {
    #[command(subcommand)]
    command: EmbeddedCommand,
}

#[derive(Subcommand)]
enum EmbeddedCommand {
    /// Create a direct (embedded-runtime) agent from an existing provider entry.
    Create(ConnectArgs),
    /// Inspect or replace immutable executable profiles.
    Profile(ProfileArgs),
    /// Create, inspect, and explicitly control canonical sessions.
    Session(SessionArgs),
    /// Inspect or replace the singular account-level Main Agent binding.
    Main(MainArgs),
    /// Inspect or replace a singular Project Agent binding.
    Project(ProjectAgentArgs),
    /// Inspect and send messages in the singular Main/Project Agent Chats.
    Chat(ChatArgs),
    /// Inspect and publish explicit Main-to-Project handoffs.
    Handoff(HandoffArgs),
    /// Inspect authorized context-manifest provenance without source bodies.
    Context(ContextArgs),
    /// Inspect and administer durable identity-owned commitments.
    Commitment(CommitmentArgs),
    /// List, add, rename, or remove account provider entries.
    Provider(ProviderArgs),
}

#[derive(Args)]
struct ConnectArgs {
    #[arg(long)]
    name: String,
    /// Provider entry (credential handle) that powers this agent.
    #[arg(long)]
    credential_id: String,
    #[arg(long)]
    model: String,
    #[arg(long)]
    description: Option<String>,
    #[arg(long)]
    system_prompt: Option<String>,
    #[arg(long)]
    tool_policy: Option<String>,
    #[arg(long)]
    context_tokens: Option<u32>,
    #[arg(long)]
    max_input_tokens: Option<u32>,
    #[arg(long)]
    max_output_tokens: Option<u32>,
}

#[derive(Args)]
struct ProfileArgs {
    #[command(subcommand)]
    command: ProfileCommand,
}

#[derive(Subcommand)]
enum ProfileCommand {
    List {
        identity_id: String,
    },
    Connect(ProfileConnectArgs),
    Select {
        identity_id: String,
        profile_id: String,
        #[arg(long)]
        version: i64,
    },
}

#[derive(Args)]
struct ProfileConnectArgs {
    identity_id: String,
    #[arg(long)]
    version: i64,
    /// Provider entry (credential handle) that powers the new profile.
    #[arg(long)]
    credential_id: String,
    #[arg(long)]
    model: String,
    #[arg(long)]
    system_prompt: Option<String>,
    #[arg(long)]
    permission_policy: Option<String>,
    #[arg(long)]
    tool_policy: Option<String>,
    #[arg(long)]
    context_tokens: Option<u32>,
    #[arg(long)]
    max_input_tokens: Option<u32>,
    #[arg(long)]
    max_output_tokens: Option<u32>,
}

#[derive(Args)]
struct SessionArgs {
    #[command(subcommand)]
    command: SessionCommand,
}

#[derive(Subcommand)]
enum SessionCommand {
    Create(SessionCreateArgs),
    List {
        identity_id: String,
    },
    Rotate {
        session_id: String,
        #[arg(long)]
        version: i64,
    },
    Suspend {
        session_id: String,
        #[arg(long)]
        version: i64,
    },
    Resume {
        session_id: String,
        #[arg(long)]
        version: i64,
    },
    Cancel {
        session_id: String,
    },
    Steer {
        session_id: String,
        content: String,
    },
    EffectivePermissions(SessionScopeArgs),
}

#[derive(Args)]
struct SessionCreateArgs {
    identity_id: String,
    #[arg(long)]
    profile_id: Option<String>,
    #[command(flatten)]
    scope: SessionScopeArgs,
}

#[derive(Args)]
struct SessionScopeArgs {
    #[arg(long)]
    identity_id: Option<String>,
    /// Canonical scope: main, project, or task.
    #[arg(long)]
    scope: String,
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long)]
    task_id: Option<String>,
    #[arg(long)]
    role: Option<String>,
}

#[derive(Args)]
struct MainArgs {
    #[command(subcommand)]
    command: MainCommand,
}

#[derive(Subcommand)]
enum MainCommand {
    /// Inspect the current binding and setup state.
    Get,
    /// Bind an account-owned agent; the binding follows its settings.
    Set(BindingSetArgs),
}

#[derive(Args)]
struct ProjectAgentArgs {
    #[command(subcommand)]
    command: ProjectAgentCommand,
}

#[derive(Subcommand)]
enum ProjectAgentCommand {
    /// Inspect the current Project Agent binding and setup state.
    Get { project_id: String },
    /// Bind an account-owned agent; the binding follows its settings.
    Set(ProjectAgentSetArgs),
}

#[derive(Args)]
struct BindingSetArgs {
    identity_id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    autonomy_policy: Option<String>,
}

#[derive(Args)]
struct ProjectAgentSetArgs {
    project_id: String,
    identity_id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    permission_ceiling: Option<String>,
    #[arg(long)]
    autonomy_policy: Option<String>,
    #[arg(long = "subscription")]
    subscriptions: Vec<String>,
    #[arg(long, default_value_t = 0)]
    wake_budget: i64,
}

#[derive(Args)]
struct ChatArgs {
    #[command(subcommand)]
    command: ChatCommand,
}

#[derive(Subcommand)]
enum ChatCommand {
    /// List the global Main Chat and authorized Project Agent Chats.
    List(ChatListArgs),
    /// Inspect one authorized Agent Chat and its turn state.
    Get { chat_id: String },
    /// List immutable messages for one Agent Chat.
    Messages(ChatMessagesArgs),
    /// Send a user message to one Agent Chat.
    Send(ChatSendArgs),
}

#[derive(Args)]
struct ChatListArgs {
    #[arg(long)]
    cursor: Option<String>,
    #[arg(long)]
    limit: Option<i64>,
}

#[derive(Args)]
struct ChatMessagesArgs {
    chat_id: String,
    #[arg(long)]
    before_sequence: Option<i64>,
    #[arg(long)]
    cursor: Option<String>,
    #[arg(long)]
    limit: Option<i64>,
}

#[derive(Args)]
struct ChatSendArgs {
    chat_id: String,
    content: String,
    #[arg(long)]
    dedupe_key: Option<String>,
}

#[derive(Args)]
struct HandoffArgs {
    #[command(subcommand)]
    command: HandoffCommand,
}

#[derive(Subcommand)]
enum HandoffCommand {
    /// List immutable handoffs delivered to a Project.
    List(HandoffListArgs),
    /// Inspect one handoff and its delivery outcome.
    Get {
        project_id: String,
        handoff_id: String,
    },
    /// Publish a bounded Main-to-Project handoff.
    Create(HandoffCreateArgs),
}

#[derive(Args)]
struct HandoffListArgs {
    project_id: String,
    #[arg(long)]
    cursor: Option<String>,
    #[arg(long)]
    limit: Option<i64>,
}

#[derive(Args)]
struct HandoffCreateArgs {
    project_id: String,
    content: String,
    #[arg(long)]
    source_message_id: Option<String>,
    #[arg(long)]
    source_turn_job_id: Option<String>,
    #[arg(long)]
    dedupe_key: String,
}

#[derive(Args)]
struct ProviderArgs {
    #[command(subcommand)]
    command: ProviderCommand,
}

#[derive(Subcommand)]
enum ProviderCommand {
    /// List configured provider entries and discovered CLI runtimes.
    List,
    /// Add an API-key provider entry. OAuth entries come from `login`.
    Add(ProviderAddArgs),
    /// Sign in to a provider with OAuth from this machine. Use this when Forge
    /// runs on another host: the browser callback is bound here and only the
    /// authorization code is relayed to the server.
    Login(ProviderLoginArgs),
    /// Rename a provider entry.
    Rename {
        id: String,
        #[arg(long)]
        label: String,
        #[arg(long)]
        version: i64,
    },
    /// Disconnect a provider entry; referencing agents become visibly unhealthy.
    Remove {
        id: String,
        #[arg(long)]
        version: i64,
    },
}

#[derive(Args)]
struct ProviderAddArgs {
    #[arg(long, value_enum)]
    provider: ProviderKind,
    #[arg(long, default_value = "default")]
    label: String,
    /// Read the API key from stdin. Without this flag a terminal prompt is used.
    #[arg(long)]
    credential_stdin: bool,
    /// Required for openai-compatible; defaults to the provider endpoint otherwise.
    #[arg(long)]
    base_url: Option<String>,
}

#[derive(Args)]
struct ProviderLoginArgs {
    #[arg(long, value_enum)]
    provider: ProviderKind,
    #[arg(long, default_value = "default")]
    label: String,
    /// `browser` runs the localhost-callback ceremony from this machine;
    /// `device` prints a code to enter on another device.
    #[arg(long, value_enum, default_value = "browser")]
    method: ProviderLoginMethod,
    /// Print the authorization URL without launching a browser.
    #[arg(long)]
    no_open: bool,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum ProviderLoginMethod {
    Browser,
    Device,
}

#[derive(Clone, clap::ValueEnum)]
enum ProviderKind {
    Openai,
    Xai,
    Gemini,
    Openrouter,
    OpenaiCompatible,
}

impl From<ProviderKind> for AgentProviderId {
    fn from(kind: ProviderKind) -> Self {
        match kind {
            ProviderKind::Openai => AgentProviderId::OpenAi,
            ProviderKind::Xai => AgentProviderId::XAi,
            ProviderKind::Gemini => AgentProviderId::Gemini,
            ProviderKind::Openrouter => AgentProviderId::OpenRouter,
            ProviderKind::OpenaiCompatible => AgentProviderId::OpenAiCompatible,
        }
    }
}

#[derive(Args)]
struct ContextArgs {
    #[command(subcommand)]
    command: ContextCommand,
}

#[derive(Subcommand)]
enum ContextCommand {
    /// List recent authorized manifests for an identity.
    List(ContextListArgs),
    /// Inspect one authorized manifest and its bounded source decisions.
    Get(ContextGetArgs),
}

#[derive(Args)]
struct ContextListArgs {
    identity_id: String,
    #[arg(long)]
    context_scope_id: Option<String>,
    #[arg(long)]
    limit: Option<u32>,
}

#[derive(Args)]
struct ContextGetArgs {
    manifest_id: String,
    #[arg(long)]
    identity_id: String,
    #[arg(long)]
    context_scope_id: String,
}

#[derive(Args)]
struct CommitmentArgs {
    #[command(subcommand)]
    command: CommitmentCommand,
}

#[derive(Subcommand)]
enum CommitmentCommand {
    /// List commitments owned by an identity, optionally within one scope.
    List(CommitmentListArgs),
    /// Create a durable commitment for an identity in an authorized scope.
    Create(CommitmentCreateArgs),
    /// Inspect one authorized commitment.
    Get { commitment_id: String },
    /// Apply a versioned commitment lifecycle/metadata update.
    Update(CommitmentUpdateArgs),
    /// Complete a commitment with an evidence reference.
    Complete(CommitmentCompleteArgs),
    /// Transfer a commitment with an explicit reason.
    Transfer(CommitmentTransferArgs),
    /// Cancel a commitment with an explicit reason.
    Cancel(CommitmentCancelArgs),
    /// List append-only evidence for a commitment.
    Evidence { commitment_id: String },
}

#[derive(Args)]
struct CommitmentListArgs {
    identity_id: String,
    #[arg(long)]
    status: Option<String>,
    #[arg(long)]
    scope_type: Option<String>,
    #[arg(long)]
    scope_id: Option<String>,
    #[arg(long)]
    limit: Option<i64>,
}

#[derive(Args)]
struct CommitmentCreateArgs {
    identity_id: String,
    #[arg(long)]
    scope_type: String,
    #[arg(long)]
    scope_id: String,
    #[arg(long)]
    title: String,
    #[arg(long)]
    description: Option<String>,
    #[arg(long)]
    status: Option<String>,
    #[arg(long)]
    due_at: Option<String>,
    #[arg(long)]
    correlation_id: String,
    #[arg(long)]
    originating_action_id: Option<String>,
    #[arg(long)]
    originating_task_id: Option<String>,
    #[arg(long)]
    evidence_required: Option<bool>,
}

#[derive(Args)]
struct CommitmentUpdateArgs {
    commitment_id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    status: Option<String>,
    #[arg(long)]
    due_at: Option<String>,
    #[arg(long)]
    description: Option<String>,
    #[arg(long)]
    blocked_reason: Option<String>,
    #[arg(long)]
    cancellation_reason: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long)]
    evidence_id: Option<String>,
    #[arg(long)]
    dedupe_key: String,
}

#[derive(Args)]
struct CommitmentCompleteArgs {
    commitment_id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    evidence_type: String,
    #[arg(long)]
    evidence_id: String,
    #[arg(long)]
    description: Option<String>,
    #[arg(long)]
    metadata: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long)]
    dedupe_key: String,
}

#[derive(Args)]
struct CommitmentTransferArgs {
    commitment_id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    to_identity_id: String,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    dedupe_key: String,
}

#[derive(Args)]
struct CommitmentCancelArgs {
    commitment_id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    dedupe_key: String,
}

impl EmbeddedArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.command {
            EmbeddedCommand::Create(args) => connect(client, output, args).await,
            EmbeddedCommand::Profile(args) => profile(client, output, args).await,
            EmbeddedCommand::Session(args) => session(client, output, args).await,
            EmbeddedCommand::Main(args) => main_binding(client, output, args).await,
            EmbeddedCommand::Project(args) => project_binding(client, output, args).await,
            EmbeddedCommand::Chat(args) => chat(client, output, args).await,
            EmbeddedCommand::Handoff(args) => handoff(client, output, args).await,
            EmbeddedCommand::Context(args) => context(client, output, args).await,
            EmbeddedCommand::Commitment(args) => commitment(client, output, args).await,
            EmbeddedCommand::Provider(args) => provider(client, output, args).await,
        }
    }
}

async fn connect(client: &ForgeClient, output: &OutputFormat, args: &ConnectArgs) -> Result<()> {
    let request = CreateEmbeddedAgentRequest {
        name: args.name.clone(),
        description: args.description.clone(),
        credential_id: args.credential_id.clone(),
        model: args.model.clone(),
        system_prompt: args.system_prompt.clone(),
        account_permission_ceiling: None,
        tool_policy: args.tool_policy.as_deref().map(parse_json).transpose()?,
        context_tokens: args.context_tokens,
        max_input_tokens: args.max_input_tokens,
        max_output_tokens: args.max_output_tokens,
    };
    let response: ConnectedEmbeddedAgentResponse =
        client.post("/api/v1/embedded-agents", &request).await?;
    print_result(output, &response)
}

async fn profile(client: &ForgeClient, output: &OutputFormat, args: &ProfileArgs) -> Result<()> {
    match &args.command {
        ProfileCommand::List { identity_id } => {
            let response: Vec<AgentProfileResponse> = client
                .get(&format!("/api/v1/agents/{identity_id}/profiles"))
                .await?;
            print_result(output, &response)
        }
        ProfileCommand::Connect(args) => {
            let request = ConnectEmbeddedProfileRequest {
                version: args.version,
                credential_id: args.credential_id.clone(),
                model: args.model.clone(),
                system_prompt: args.system_prompt.clone(),
                permission_policy: args.permission_policy.clone(),
                tool_policy: args.tool_policy.as_deref().map(parse_json).transpose()?,
                context_tokens: args.context_tokens,
                max_input_tokens: args.max_input_tokens,
                max_output_tokens: args.max_output_tokens,
            };
            let response: ConnectedEmbeddedProfileResponse = client
                .post(
                    &format!("/api/v1/agents/{}/profiles/connect", args.identity_id),
                    &request,
                )
                .await?;
            print_result(output, &response)
        }
        ProfileCommand::Select {
            identity_id,
            profile_id,
            version,
        } => {
            let response: AgentResponse = client
                .post(
                    &format!("/api/v1/agents/{identity_id}/profiles/{profile_id}/select"),
                    &SessionVersionRequest { version: *version },
                )
                .await?;
            print_result(output, &response)
        }
    }
}

async fn session(client: &ForgeClient, output: &OutputFormat, args: &SessionArgs) -> Result<()> {
    match &args.command {
        SessionCommand::Create(args) => {
            let request = CreateAgentSessionRequest {
                profile_id: args.profile_id.clone(),
                scope: canonical_scope(&args.scope)?,
            };
            let response: AgentSessionResponse = client
                .post(
                    &format!("/api/v1/agents/{}/sessions", args.identity_id),
                    &request,
                )
                .await?;
            print_result(output, &response)
        }
        SessionCommand::List { identity_id } => {
            let response: Vec<AgentSessionResponse> = client
                .get(&format!("/api/v1/agents/{identity_id}/sessions"))
                .await?;
            print_result(output, &response)
        }
        SessionCommand::Rotate {
            session_id,
            version,
        } => {
            let response: AgentSessionResponse = client
                .post(
                    &format!("/api/v1/agent-sessions/{session_id}/rotate"),
                    &SessionVersionRequest { version: *version },
                )
                .await?;
            print_result(output, &response)
        }
        SessionCommand::Suspend {
            session_id,
            version,
        } => session_status(client, output, session_id, *version, "suspend").await,
        SessionCommand::Resume {
            session_id,
            version,
        } => session_status(client, output, session_id, *version, "resume").await,
        SessionCommand::Cancel { session_id } => {
            client
                .post_empty(
                    &format!("/api/v1/agent-sessions/{session_id}/cancel"),
                    &json!({}),
                )
                .await?;
            Ok(())
        }
        SessionCommand::Steer {
            session_id,
            content,
        } => {
            client
                .post_empty(
                    &format!("/api/v1/agent-sessions/{session_id}/steer"),
                    &SteerAgentSessionRequest {
                        content: content.clone(),
                    },
                )
                .await?;
            Ok(())
        }
        SessionCommand::EffectivePermissions(args) => {
            let identity_id = required(&args.identity_id, "--identity-id")?;
            let response: EffectivePermissionsResponse = client
                .post(
                    &format!("/api/v1/agents/{identity_id}/effective-permissions"),
                    &canonical_scope(args)?,
                )
                .await?;
            print_result(output, &response)
        }
    }
}

async fn session_status(
    client: &ForgeClient,
    output: &OutputFormat,
    session_id: &str,
    version: i64,
    action: &str,
) -> Result<()> {
    let response: AgentSessionResponse = client
        .post(
            &format!("/api/v1/agent-sessions/{session_id}/{action}"),
            &SessionVersionRequest { version },
        )
        .await?;
    print_result(output, &response)
}

async fn main_binding(client: &ForgeClient, output: &OutputFormat, args: &MainArgs) -> Result<()> {
    match &args.command {
        MainCommand::Get => {
            let response: MainAgentBindingResponse =
                client.get("/api/v1/account/main-agent").await?;
            print_result(output, &response)
        }
        MainCommand::Set(args) => {
            let response: MainAgentBindingResponse = client
                .put(
                    "/api/v1/account/main-agent",
                    &SetMainAgentBindingRequest {
                        identity_id: args.identity_id.clone(),
                        expected_version: args.version,
                        autonomy_policy: args
                            .autonomy_policy
                            .as_deref()
                            .map(parse_json)
                            .transpose()?
                            .unwrap_or_else(|| json!({})),
                    },
                )
                .await?;
            print_result(output, &response)
        }
    }
}

async fn project_binding(
    client: &ForgeClient,
    output: &OutputFormat,
    args: &ProjectAgentArgs,
) -> Result<()> {
    match &args.command {
        ProjectAgentCommand::Get { project_id } => {
            let response: ProjectAgentBindingResponse = client
                .get(&format!("/api/v1/projects/{project_id}/project-agent"))
                .await?;
            print_result(output, &response)
        }
        ProjectAgentCommand::Set(args) => {
            let response: ProjectAgentBindingResponse = client
                .put(
                    &format!("/api/v1/projects/{}/project-agent", args.project_id),
                    &SetProjectAgentBindingRequest {
                        identity_id: args.identity_id.clone(),
                        expected_version: args.version,
                        permission_ceiling: args
                            .permission_ceiling
                            .as_deref()
                            .map(parse_json)
                            .transpose()?
                            .unwrap_or_else(|| json!({})),
                        autonomy_policy: args
                            .autonomy_policy
                            .as_deref()
                            .map(parse_json)
                            .transpose()?
                            .unwrap_or_else(|| json!({})),
                        subscriptions: args.subscriptions.clone(),
                        wake_budget: args.wake_budget,
                    },
                )
                .await?;
            print_result(output, &response)
        }
    }
}

async fn chat(client: &ForgeClient, output: &OutputFormat, args: &ChatArgs) -> Result<()> {
    match &args.command {
        ChatCommand::List(args) => {
            let response: AgentChatListResponse = client
                .get(&format!(
                    "/api/v1/agent-chats?{}",
                    agent_chat_list_query(args)
                ))
                .await?;
            print_result(output, &response)
        }
        ChatCommand::Get { chat_id } => {
            let response: AgentChatDetailResponse = client
                .get(&format!("/api/v1/agent-chats/{chat_id}"))
                .await?;
            print_result(output, &response)
        }
        ChatCommand::Messages(args) => {
            let response: AgentChatMessageListResponse = client
                .get(&format!(
                    "/api/v1/agent-chats/{}/messages?{}",
                    args.chat_id,
                    agent_chat_messages_query(args)
                ))
                .await?;
            print_result(output, &response)
        }
        ChatCommand::Send(args) => {
            if args.content.trim().is_empty() {
                bail!("chat content must not be empty");
            }
            let response: SendAgentChatMessageResponse = client
                .post(
                    &format!("/api/v1/agent-chats/{}/messages", args.chat_id),
                    &SendAgentChatMessageRequest {
                        content: args.content.clone(),
                        dedupe_key: args.dedupe_key.clone(),
                    },
                )
                .await?;
            print_result(output, &response)
        }
    }
}

async fn handoff(client: &ForgeClient, output: &OutputFormat, args: &HandoffArgs) -> Result<()> {
    match &args.command {
        HandoffCommand::List(args) => {
            let response: Vec<AgentHandoffResponse> = client
                .get(&format!(
                    "/api/v1/projects/{}/agent-handoffs?{}",
                    args.project_id,
                    handoff_list_query(args)
                ))
                .await?;
            print_result(output, &response)
        }
        HandoffCommand::Get {
            project_id,
            handoff_id,
        } => {
            let response: AgentHandoffResponse = client
                .get(&format!(
                    "/api/v1/projects/{project_id}/agent-handoffs/{handoff_id}"
                ))
                .await?;
            print_result(output, &response)
        }
        HandoffCommand::Create(args) => {
            if args.content.trim().is_empty() {
                bail!("handoff content must not be empty");
            }
            let response: AgentHandoffResponse = client
                .post(
                    &format!("/api/v1/projects/{}/agent-handoffs", args.project_id),
                    &CreateAgentHandoffRequest {
                        source_message_id: args.source_message_id.clone(),
                        source_turn_job_id: args.source_turn_job_id.clone(),
                        content: args.content.clone(),
                        dedupe_key: args.dedupe_key.clone(),
                    },
                )
                .await?;
            print_result(output, &response)
        }
    }
}

async fn provider(client: &ForgeClient, output: &OutputFormat, args: &ProviderArgs) -> Result<()> {
    match &args.command {
        ProviderCommand::List => {
            let response: ProviderEntriesResponse = client.get("/api/v1/providers").await?;
            print_result(output, &response)
        }
        ProviderCommand::Add(args) => {
            let request = CreateProviderEntryRequest {
                provider: args.provider.clone().into(),
                label: args.label.clone(),
                credential: read_credential(args.credential_stdin)?,
                base_url: args.base_url.clone(),
            };
            let response: ProviderEntryResponse =
                client.post("/api/v1/providers", &request).await?;
            print_result(output, &response)
        }
        ProviderCommand::Login(args) => {
            let provider = args.provider.clone().into();
            let operation = match args.method {
                ProviderLoginMethod::Browser => {
                    provider_login::browser_login(client, provider, &args.label, args.no_open)
                        .await?
                }
                ProviderLoginMethod::Device => {
                    provider_login::device_login(client, provider, &args.label).await?
                }
            };
            print_result(output, &operation)?;
            match provider_login::failed_reason(&operation) {
                Some(reason) => Err(anyhow::anyhow!(reason)),
                None => Ok(()),
            }
        }
        ProviderCommand::Rename { id, label, version } => {
            let request = RenameProviderEntryRequest {
                label: label.clone(),
                version: *version,
            };
            let response: ProviderEntryResponse = client
                .patch(&format!("/api/v1/providers/{id}"), &request)
                .await?;
            print_result(output, &response)
        }
        ProviderCommand::Remove { id, version } => {
            let response: DisconnectCredentialResponse = client
                .delete_json(&format!("/api/v1/providers/{id}?version={version}"))
                .await?;
            print_result(output, &response)
        }
    }
}

async fn context(client: &ForgeClient, output: &OutputFormat, args: &ContextArgs) -> Result<()> {
    match &args.command {
        ContextCommand::List(args) => {
            let response: ContextManifestListResponse = client
                .get(&format!(
                    "/api/v1/agents/{}/context-manifests?{}",
                    args.identity_id,
                    context_manifest_list_query(args)
                ))
                .await?;
            print_result(output, &response)
        }
        ContextCommand::Get(args) => {
            let response: ContextManifestResponse = client
                .get(&format!(
                    "/api/v1/context-manifests/{}?{}",
                    args.manifest_id,
                    context_manifest_get_query(args)
                ))
                .await?;
            print_result(output, &response)
        }
    }
}

async fn commitment(
    client: &ForgeClient,
    output: &OutputFormat,
    args: &CommitmentArgs,
) -> Result<()> {
    match &args.command {
        CommitmentCommand::List(args) => {
            let response: Vec<CommitmentResponse> = client
                .get(&format!(
                    "/api/v1/agents/{}/commitments?{}",
                    args.identity_id,
                    commitment_list_query(args)
                ))
                .await?;
            print_result(output, &response)
        }
        CommitmentCommand::Create(args) => {
            let response: CommitmentResponse = client
                .post(
                    &format!("/api/v1/agents/{}/commitments", args.identity_id),
                    &CreateCommitmentRequest {
                        scope_type: args.scope_type.clone(),
                        scope_id: args.scope_id.clone(),
                        title: args.title.clone(),
                        description: args.description.clone(),
                        status: args.status.clone(),
                        due_at: args.due_at.clone(),
                        correlation_id: args.correlation_id.clone(),
                        originating_action_id: args.originating_action_id.clone(),
                        originating_task_id: args.originating_task_id.clone(),
                        evidence_required: args.evidence_required,
                    },
                )
                .await?;
            print_result(output, &response)
        }
        CommitmentCommand::Get { commitment_id } => {
            let response: CommitmentResponse = client
                .get(&format!("/api/v1/commitments/{commitment_id}"))
                .await?;
            print_result(output, &response)
        }
        CommitmentCommand::Update(args) => {
            let response: CommitmentResponse = client
                .patch(
                    &format!("/api/v1/commitments/{}", args.commitment_id),
                    &UpdateCommitmentRequest {
                        expected_version: args.version,
                        status: args.status.clone(),
                        due_at: args.due_at.clone().map(Some),
                        description: args.description.clone().map(Some),
                        blocked_reason: args.blocked_reason.clone().map(Some),
                        cancellation_reason: args.cancellation_reason.clone().map(Some),
                        reason: args.reason.clone(),
                        evidence_id: args.evidence_id.clone(),
                        dedupe_key: args.dedupe_key.clone(),
                    },
                )
                .await?;
            print_result(output, &response)
        }
        CommitmentCommand::Complete(args) => {
            let response: CommitmentResponse = client
                .post(
                    &format!("/api/v1/commitments/{}/complete", args.commitment_id),
                    &CompleteCommitmentRequest {
                        expected_version: args.version,
                        evidence_type: args.evidence_type.clone(),
                        evidence_id: args.evidence_id.clone(),
                        description: args.description.clone(),
                        metadata: args
                            .metadata
                            .as_deref()
                            .map(parse_json)
                            .transpose()?
                            .unwrap_or_else(|| json!({})),
                        reason: args.reason.clone(),
                        dedupe_key: args.dedupe_key.clone(),
                    },
                )
                .await?;
            print_result(output, &response)
        }
        CommitmentCommand::Transfer(args) => {
            let response: CommitmentResponse = client
                .post(
                    &format!("/api/v1/commitments/{}/transfer", args.commitment_id),
                    &TransferCommitmentRequest {
                        expected_version: args.version,
                        to_identity_id: args.to_identity_id.clone(),
                        reason: args.reason.clone(),
                        dedupe_key: args.dedupe_key.clone(),
                    },
                )
                .await?;
            print_result(output, &response)
        }
        CommitmentCommand::Cancel(args) => {
            let response: CommitmentResponse = client
                .post(
                    &format!("/api/v1/commitments/{}/cancel", args.commitment_id),
                    &UpdateCommitmentRequest {
                        expected_version: args.version,
                        status: None,
                        due_at: None,
                        description: None,
                        blocked_reason: None,
                        cancellation_reason: Some(Some(args.reason.clone())),
                        reason: Some(args.reason.clone()),
                        evidence_id: None,
                        dedupe_key: args.dedupe_key.clone(),
                    },
                )
                .await?;
            print_result(output, &response)
        }
        CommitmentCommand::Evidence { commitment_id } => {
            let response: Vec<CommitmentEvidenceResponse> = client
                .get(&format!("/api/v1/commitments/{commitment_id}/evidence"))
                .await?;
            print_result(output, &response)
        }
    }
}

fn agent_chat_list_query(options: &ChatListArgs) -> String {
    let options = AgentChatListQuery {
        cursor: options.cursor.clone(),
        limit: options.limit,
    };
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(cursor) = &options.cursor {
        query.append_pair("cursor", cursor);
    }
    if let Some(limit) = options.limit {
        query.append_pair("limit", &limit.to_string());
    }
    query.finish()
}

fn agent_chat_messages_query(options: &ChatMessagesArgs) -> String {
    let options = AgentChatMessagesQuery {
        cursor: options.cursor.clone(),
        limit: options.limit,
        before_sequence: options.before_sequence,
    };
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(sequence) = options.before_sequence {
        query.append_pair("before_sequence", &sequence.to_string());
    }
    if let Some(cursor) = &options.cursor {
        query.append_pair("cursor", cursor);
    }
    if let Some(limit) = options.limit {
        query.append_pair("limit", &limit.to_string());
    }
    query.finish()
}

fn handoff_list_query(options: &HandoffListArgs) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(cursor) = &options.cursor {
        query.append_pair("cursor", cursor);
    }
    if let Some(limit) = options.limit {
        query.append_pair("limit", &limit.to_string());
    }
    query.finish()
}

fn context_manifest_list_query(options: &ContextListArgs) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(scope_id) = &options.context_scope_id {
        query.append_pair("context_scope_id", scope_id);
    }
    if let Some(limit) = options.limit {
        query.append_pair("limit", &limit.to_string());
    }
    query.finish()
}

fn context_manifest_get_query(options: &ContextGetArgs) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("identity_id", &options.identity_id);
    query.append_pair("context_scope_id", &options.context_scope_id);
    query.finish()
}

fn commitment_list_query(options: &CommitmentListArgs) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(status) = &options.status {
        query.append_pair("status", status);
    }
    if let Some(scope_type) = &options.scope_type {
        query.append_pair("scope_type", scope_type);
    }
    if let Some(scope_id) = &options.scope_id {
        query.append_pair("scope_id", scope_id);
    }
    if let Some(limit) = options.limit {
        query.append_pair("limit", &limit.to_string());
    }
    query.finish()
}

fn canonical_scope(args: &SessionScopeArgs) -> Result<CanonicalScopeRequest> {
    match args.scope.as_str() {
        "main" | "project" => Ok(CanonicalScopeRequest::AgentChat {
            chat_id: required(&args.chat_id, "--chat-id")?,
        }),
        "task" => Ok(CanonicalScopeRequest::Task {
            task_id: required(&args.task_id, "--task-id")?,
            role: required(&args.role, "--role")?,
        }),
        other => bail!("invalid --scope `{other}`; expected main, project, or task"),
    }
}

fn required(value: &Option<String>, flag: &str) -> Result<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("{flag} is required for this scope"))
}

fn parse_json(value: &str) -> Result<Value> {
    serde_json::from_str(value).with_context(|| "expected a JSON object")
}

fn read_credential(from_stdin: bool) -> Result<String> {
    if from_stdin {
        let mut credential = String::new();
        io::stdin()
            .read_to_string(&mut credential)
            .context("read credential from stdin")?;
        let credential = credential.trim_end_matches(&['\r', '\n'][..]).to_owned();
        if credential.is_empty() {
            bail!("credential stdin was empty");
        }
        return Ok(credential);
    }
    if stdin_is_terminal() {
        return prompt_password();
    }
    bail!("credential input is not a terminal; pass --credential-stdin")
}

fn print_result<T: Serialize>(output: &OutputFormat, value: &T) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(value),
        // Embedded resources are intentionally emitted as JSON in table mode
        // too: profile/session/chat fields are nested and a lossy table would
        // hide authorization/provenance details.
        OutputFormat::Table => print_json(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_manifest_queries_encode_scope_and_identity() {
        let list = context_manifest_list_query(&ContextListArgs {
            identity_id: "identity-1".to_owned(),
            context_scope_id: Some("scope/1".to_owned()),
            limit: Some(7),
        });
        assert_eq!(list, "context_scope_id=scope%2F1&limit=7");

        let get = context_manifest_get_query(&ContextGetArgs {
            manifest_id: "manifest-1".to_owned(),
            identity_id: "identity-1".to_owned(),
            context_scope_id: "scope-1".to_owned(),
        });
        assert_eq!(get, "identity_id=identity-1&context_scope_id=scope-1");
    }

    #[test]
    fn commitment_list_query_omits_unset_filters() {
        let query = commitment_list_query(&CommitmentListArgs {
            identity_id: "identity-1".to_owned(),
            status: None,
            scope_type: Some("project".to_owned()),
            scope_id: Some("project/1".to_owned()),
            limit: Some(20),
        });
        assert_eq!(query, "scope_type=project&scope_id=project%2F1&limit=20");
    }

    #[test]
    fn commitment_requests_do_not_carry_actor_or_credential_fields() {
        let request = serde_json::to_value(UpdateCommitmentRequest {
            expected_version: 3,
            status: Some("blocked".to_owned()),
            due_at: None,
            description: None,
            blocked_reason: Some(Some("waiting".to_owned())),
            cancellation_reason: None,
            reason: Some("dependency".to_owned()),
            evidence_id: None,
            dedupe_key: "dedupe-1".to_owned(),
        })
        .expect("request serializes");
        assert!(request.get("actor_id").is_none());
        assert!(request.get("credential").is_none());
        assert_eq!(request["expected_version"], 3);
    }

    #[test]
    fn chat_and_handoff_queries_encode_only_supported_filters() {
        let chats = agent_chat_list_query(&ChatListArgs {
            cursor: Some("cursor/1".to_owned()),
            limit: Some(25),
        });
        assert_eq!(chats, "cursor=cursor%2F1&limit=25");

        let messages = agent_chat_messages_query(&ChatMessagesArgs {
            chat_id: "chat-1".to_owned(),
            before_sequence: Some(10),
            cursor: None,
            limit: Some(20),
        });
        assert_eq!(messages, "before_sequence=10&limit=20");

        let handoffs = handoff_list_query(&HandoffListArgs {
            project_id: "project-1".to_owned(),
            cursor: None,
            limit: Some(10),
        });
        assert_eq!(handoffs, "limit=10");
    }

    #[test]
    fn canonical_scope_rejects_retired_room_scope() {
        let error = canonical_scope(&SessionScopeArgs {
            identity_id: None,
            scope: "room".to_owned(),
            chat_id: None,
            task_id: None,
            role: None,
        })
        .expect_err("Room is not a canonical embedded scope");
        assert!(error
            .to_string()
            .contains("expected main, project, or task"));
    }

    #[test]
    fn canonical_chat_scopes_use_server_chat_identity() {
        let scope = canonical_scope(&SessionScopeArgs {
            identity_id: None,
            scope: "project".to_owned(),
            chat_id: Some("chat-1".to_owned()),
            task_id: None,
            role: None,
        })
        .expect("project chat scope is valid");
        assert_eq!(
            serde_json::to_value(scope).expect("scope serializes"),
            json!({ "type": "agent_chat", "chat_id": "chat-1" })
        );
    }
}
