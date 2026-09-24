//! Automatic pricing: resolve the models.dev row for a runtime model and
//! apply the owner's adjustment.
//!
//! Owners do not maintain per-model bindings. At admission Forge
//!
//! 1. provisions the pricing subject (provider entry or CLI runtime) and its
//!    current non-secret revision,
//! 2. picks the effective [`db::PricingAdjustment`]: the agent's own, else the
//!    subject's, else plain list price,
//! 3. matches the runtime model to one models.dev row (a pin, the subject's
//!    provider, the model family, or the only provider listing it), and
//! 4. materializes exactly one active binding in the scope admission will
//!    resolve: `''` provider-wide, `agent:<id>` for an agent with its own
//!    adjustment.
//!
//! Discounted and fixed prices become manual rate revisions, so the frozen
//! selection and settlement machinery is unchanged: a selection still points
//! at one immutable rate revision through one active binding.

use db::{
    CredentialHandle, PricingAdjustment, PricingAdjustmentMode, PricingAdjustmentScope,
    PricingRateRevision, PricingRateSourceKind, PricingSubjectKind, RateBuckets, SqliteDb,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, SqliteConnection, Transaction};

use crate::{pricing, Result, ServiceError};

const SUBJECT_SCHEMA_REVISION: &str = "pricing-subject-v1";
const CLI_FINGERPRINT_DOMAIN: &str = "forge-pricing-cli-runtime-fingerprint-v1";
const BPS_PER_WHOLE: i128 = 10_000;

/// The provider entry or CLI runtime an admission runs through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectRef {
    pub provider_entry_id: Option<String>,
    pub daemon_id: Option<String>,
    pub executor_type: Option<String>,
}

impl SubjectRef {
    /// The adjustment scope that configures this subject for every agent.
    pub fn adjustment_scope(&self) -> Option<PricingAdjustmentScope> {
        if let Some(id) = &self.provider_entry_id {
            return Some(PricingAdjustmentScope::ProviderEntry(id.clone()));
        }
        match (&self.daemon_id, &self.executor_type) {
            (Some(daemon_id), Some(executor_type)) => Some(PricingAdjustmentScope::CliRuntime {
                daemon_id: daemon_id.clone(),
                executor_type: executor_type.clone(),
            }),
            _ => None,
        }
    }
}

/// Binding scope key for an agent that carries its own adjustment.
pub fn agent_scope_key(agent_id: &str) -> String {
    format!("agent:{agent_id}")
}

// ---------------------------------------------------------------------------
// Non-secret subject identity
// ---------------------------------------------------------------------------

/// Identity of a provider entry. Labels and credentials never participate.
pub fn provider_identity(handle: &CredentialHandle) -> pricing::PricingSubjectIdentity {
    let endpoint_class = crate::embedded_agent_service::entry_base_url(handle)
        .ok()
        .and_then(|base_url| canonical_endpoint_class(&base_url))
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| handle.provider.clone());
    let credential_method = if handle.credential_method == "oauth_bundle" {
        "oauth".to_owned()
    } else {
        handle.credential_method.clone()
    };
    pricing::PricingSubjectIdentity {
        subject_kind: "provider_entry".to_owned(),
        provider_kind: handle.provider.clone(),
        credential_method,
        endpoint_class,
        runtime_fingerprint: None,
        schema_revision: SUBJECT_SCHEMA_REVISION.to_owned(),
    }
}

/// Canonicalizes the non-secret endpoint identity used by pricing subjects.
/// Scheme, host, explicit port, and custom path all participate in the
/// digest; credentials, query parameters, and fragments never do. Trailing
/// path slashes are normalized so equivalent root/API spellings do not create
/// needless subject revisions.
pub fn canonical_endpoint_class(base_url: &str) -> Option<String> {
    let parsed = url::Url::parse(base_url).ok()?;
    let scheme = parsed.scheme().trim().to_ascii_lowercase();
    let host = parsed.host_str()?.trim().to_ascii_lowercase();
    if scheme.is_empty() || host.is_empty() {
        return None;
    }
    let port = parsed
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    let trimmed_path = parsed.path().trim_end_matches('/');
    let path = if trimmed_path.is_empty() {
        "/"
    } else {
        trimmed_path
    };
    Some(format!("{scheme}://{host}{port}{path}"))
}

/// Identity of a discovered CLI runtime.
///
/// Daemon reports are untrusted input. In particular, CLI paths and config
/// paths may be URL-shaped or contain copied credentials. Keep the raw values
/// in memory only while computing a domain-separated digest; the subject row
/// receives only that fixed-size opaque value.
pub fn cli_identity(
    executor_type: &str,
    detected: Option<&api_types::DetectedCli>,
) -> pricing::PricingSubjectIdentity {
    pricing::PricingSubjectIdentity {
        subject_kind: "cli_runtime".to_owned(),
        provider_kind: executor_type.to_owned(),
        credential_method: "cli_login".to_owned(),
        endpoint_class: "cli_runtime".to_owned(),
        runtime_fingerprint: Some(cli_fingerprint(executor_type, detected)),
        schema_revision: SUBJECT_SCHEMA_REVISION.to_owned(),
    }
}

fn cli_fingerprint(executor_type: &str, detected: Option<&api_types::DetectedCli>) -> String {
    let mut bytes = Vec::with_capacity(256);
    append_fingerprint_field(&mut bytes, Some(CLI_FINGERPRINT_DOMAIN));
    append_fingerprint_field(&mut bytes, Some(executor_type));
    // Availability/authentication is deliberately excluded: it is a
    // reversible eligibility state, not runtime identity, and must not mint
    // a new subject revision on routine login/disable transitions.
    append_fingerprint_field(&mut bytes, detected.and_then(|cli| cli.version.as_deref()));
    append_fingerprint_field(&mut bytes, detected.and_then(|cli| cli.path.as_deref()));
    append_fingerprint_field(
        &mut bytes,
        detected.and_then(|cli| cli.config_path.as_deref()),
    );
    format!("sha256:{}", pricing::sha256_hex(&bytes))
}

fn append_fingerprint_field(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
    }
}

/// The detected CLI of one kind in a daemon's report, if any.
pub fn detected_cli(
    detected_clis_json: &str,
    executor_type: &str,
) -> Option<api_types::DetectedCli> {
    serde_json::from_str::<Vec<api_types::DetectedCli>>(detected_clis_json)
        .ok()?
        .into_iter()
        .find(|cli| cli.kind == executor_type)
}

// ---------------------------------------------------------------------------
// Catalog matching
// ---------------------------------------------------------------------------

/// models.dev provider that bills a Forge provider kind or CLI executor
/// directly, used to break ties between providers listing the same model.
pub fn subject_catalog_provider(provider_kind: &str) -> Option<&'static str> {
    Some(match provider_kind {
        "openai" | "codex" => "openai",
        "gemini" => "google",
        "xai" => "xai",
        "openrouter" => "openrouter",
        "anthropic" | "claude_code" => "anthropic",
        _ => return None,
    })
}

/// models.dev provider that publishes a model family, by model-id prefix.
pub fn model_family_provider(model: &str) -> Option<&'static str> {
    let model = model.to_ascii_lowercase();
    let starts = |prefixes: &[&str]| prefixes.iter().any(|prefix| model.starts_with(prefix));
    Some(
        if starts(&["gpt-", "o1", "o3", "o4", "codex-", "chatgpt-"]) {
            "openai"
        } else if starts(&["claude-"]) {
            "anthropic"
        } else if starts(&["gemini-", "gemma-"]) {
            "google"
        } else if starts(&["grok-"]) {
            "xai"
        } else if starts(&["glm-"]) {
            "zai"
        } else if starts(&["deepseek-"]) {
            "deepseek"
        } else if starts(&["kimi-"]) {
            "moonshotai"
        } else if starts(&["qwen"]) {
            "alibaba"
        } else if starts(&["mistral-", "devstral-", "codestral-", "magistral-"]) {
            "mistral"
        } else {
            return None;
        },
    )
}

/// Outcome of matching a runtime model to the active models.dev snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogMatch {
    Found(Box<PricingRateRevision>),
    /// Several providers list the model and nothing picks one.
    Ambiguous(Vec<String>),
    NotFound,
    /// No models.dev snapshot has been loaded yet.
    CatalogAbsent,
}

/// What to match: an explicit pin, and tie-break hints in priority order.
#[derive(Debug, Clone, Default)]
pub struct CatalogQuery<'a> {
    pub runtime_model: &'a str,
    pub pin_provider: Option<&'a str>,
    pub pin_model: Option<&'a str>,
    pub hints: Vec<&'a str>,
}

async fn active_snapshot_id(conn: &mut SqliteConnection) -> Result<Option<String>> {
    Ok(sqlx::query_scalar::<_, Option<String>>(
        "SELECT active_snapshot_id FROM pricing_catalog_state WHERE id = 'models_dev_catalog'",
    )
    .fetch_optional(&mut *conn)
    .await?
    .flatten())
}

async fn catalog_rows(
    conn: &mut SqliteConnection,
    snapshot_id: &str,
    provider_id: Option<&str>,
    model_id: &str,
) -> Result<Vec<PricingRateRevision>> {
    let rows = sqlx::query(
        "SELECT id FROM pricing_rate_revision
         WHERE catalog_snapshot_id = ? AND catalog_model_id = ?
           AND (? IS NULL OR catalog_provider_id = ?)
           AND source_kind = 'models_dev_catalog'
         ORDER BY catalog_provider_id ASC",
    )
    .bind(snapshot_id)
    .bind(model_id)
    .bind(provider_id)
    .bind(provider_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut revisions = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.try_get("id")?;
        if let Some(revision) = rate_revision(conn, &id).await? {
            revisions.push(revision);
        }
    }
    Ok(revisions)
}

async fn rate_revision(
    conn: &mut SqliteConnection,
    id: &str,
) -> Result<Option<PricingRateRevision>> {
    let row = sqlx::query(
        "SELECT id, source_kind, owner_user_id, catalog_snapshot_id, catalog_provider_id,
                catalog_model_id, pricing_subject_revision_id, pricing_subject_revision_digest,
                runtime_model, source_model_key, source_last_updated, currency,
                input_nano_usd_per_million, output_nano_usd_per_million,
                cache_read_nano_usd_per_million, cache_write_nano_usd_per_million,
                tiers_json, legacy_context_over_200k_json, context_tier_state,
                received_rates_json, rate_digest, effective_at, created_at
         FROM pricing_rate_revision WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let source_kind: String = row.try_get("source_kind")?;
    Ok(Some(PricingRateRevision {
        id: row.try_get("id")?,
        source_kind: source_kind
            .parse()
            .map_err(|error: String| ServiceError::invalid_operation(error))?,
        owner_user_id: row.try_get("owner_user_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        catalog_provider_id: row.try_get("catalog_provider_id")?,
        catalog_model_id: row.try_get("catalog_model_id")?,
        pricing_subject_revision_id: row.try_get("pricing_subject_revision_id")?,
        pricing_subject_revision_digest: row.try_get("pricing_subject_revision_digest")?,
        runtime_model: row.try_get("runtime_model")?,
        source_model_key: row.try_get("source_model_key")?,
        source_last_updated: row.try_get("source_last_updated")?,
        currency: row.try_get("currency")?,
        rates: RateBuckets::new(
            row.try_get("input_nano_usd_per_million")?,
            row.try_get("output_nano_usd_per_million")?,
            row.try_get("cache_read_nano_usd_per_million")?,
            row.try_get("cache_write_nano_usd_per_million")?,
        ),
        tiers_json: row.try_get("tiers_json")?,
        legacy_context_over_200k_json: row.try_get("legacy_context_over_200k_json")?,
        context_tier_state: row.try_get("context_tier_state")?,
        received_rates_json: row.try_get("received_rates_json")?,
        rate_digest: row.try_get("rate_digest")?,
        effective_at: row.try_get("effective_at")?,
        created_at: row.try_get("created_at")?,
    }))
}

/// Matches one runtime model to exactly one row of the active snapshot.
pub async fn match_catalog(
    conn: &mut SqliteConnection,
    query: &CatalogQuery<'_>,
) -> Result<CatalogMatch> {
    let Some(snapshot_id) = active_snapshot_id(conn).await? else {
        return Ok(CatalogMatch::CatalogAbsent);
    };
    if let Some(pin_provider) = query.pin_provider {
        let model = query.pin_model.unwrap_or(query.runtime_model);
        return Ok(
            match catalog_rows(conn, &snapshot_id, Some(pin_provider), model)
                .await?
                .into_iter()
                .next()
            {
                Some(row) => CatalogMatch::Found(Box::new(row)),
                None => CatalogMatch::NotFound,
            },
        );
    }
    // `provider/model` runtime ids name their catalog provider outright.
    if let Some((provider, model)) = query.runtime_model.split_once('/') {
        if let Some(row) = catalog_rows(conn, &snapshot_id, Some(provider), model)
            .await?
            .into_iter()
            .next()
        {
            return Ok(CatalogMatch::Found(Box::new(row)));
        }
    }
    let candidates = catalog_rows(conn, &snapshot_id, None, query.runtime_model).await?;
    let family = model_family_provider(query.runtime_model);
    for hint in query.hints.iter().copied().chain(family) {
        if let Some(row) = candidates
            .iter()
            .find(|row| row.catalog_provider_id.as_deref() == Some(hint))
        {
            return Ok(CatalogMatch::Found(Box::new(row.clone())));
        }
    }
    Ok(match candidates.len() {
        0 => CatalogMatch::NotFound,
        1 => CatalogMatch::Found(Box::new(
            candidates.into_iter().next().expect("one candidate"),
        )),
        _ => CatalogMatch::Ambiguous(
            candidates
                .into_iter()
                .filter_map(|row| row.catalog_provider_id)
                .collect(),
        ),
    })
}

// ---------------------------------------------------------------------------
// Adjustments
// ---------------------------------------------------------------------------

/// Where the effective adjustment came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjustmentSource {
    Agent,
    Provider,
    Default,
}

/// The adjustment admission applies, and the binding scope it resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveAdjustment {
    pub source: AdjustmentSource,
    pub adjustment: Option<PricingAdjustment>,
    /// Pin carried from the provider when the agent does not pin itself.
    pub provider_pin: Option<String>,
    pub scope_key: String,
}

impl EffectiveAdjustment {
    pub fn mode(&self) -> PricingAdjustmentMode {
        self.adjustment
            .as_ref()
            .map_or(PricingAdjustmentMode::List, |adjustment| adjustment.mode)
    }

    fn pin(&self) -> (Option<&str>, Option<&str>) {
        match &self.adjustment {
            Some(adjustment) if adjustment.catalog_provider_id.is_some() => (
                adjustment.catalog_provider_id.as_deref(),
                adjustment.catalog_model_id.as_deref(),
            ),
            _ => (self.provider_pin.as_deref(), None),
        }
    }
}

async fn adjustment_on(
    conn: &mut SqliteConnection,
    owner_user_id: &str,
    scope: &PricingAdjustmentScope,
) -> Result<Option<PricingAdjustment>> {
    let (provider_entry_id, daemon_id, executor_type, agent_id) = match scope {
        PricingAdjustmentScope::ProviderEntry(id) => (Some(id.as_str()), None, None, None),
        PricingAdjustmentScope::CliRuntime {
            daemon_id,
            executor_type,
        } => (
            None,
            Some(daemon_id.as_str()),
            Some(executor_type.as_str()),
            None,
        ),
        PricingAdjustmentScope::Agent(id) => (None, None, None, Some(id.as_str())),
    };
    let row = sqlx::query(
        "SELECT id, mode, discount_bps, input_nano_usd_per_million,
                output_nano_usd_per_million, cache_read_nano_usd_per_million,
                cache_write_nano_usd_per_million, catalog_provider_id, catalog_model_id,
                version, created_at, updated_at
         FROM pricing_adjustment
         WHERE owner_user_id = ? AND scope_kind = ?
           AND provider_entry_id IS ? AND daemon_id IS ? AND executor_type IS ?
           AND agent_id IS ?",
    )
    .bind(owner_user_id)
    .bind(scope.kind())
    .bind(provider_entry_id)
    .bind(daemon_id)
    .bind(executor_type)
    .bind(agent_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mode: String = row.try_get("mode")?;
    Ok(Some(PricingAdjustment {
        id: row.try_get("id")?,
        owner_user_id: owner_user_id.to_owned(),
        scope: scope.clone(),
        mode: mode
            .parse()
            .map_err(|error: String| ServiceError::invalid_operation(error))?,
        discount_bps: row.try_get("discount_bps")?,
        fixed_rates: RateBuckets::new(
            row.try_get("input_nano_usd_per_million")?,
            row.try_get("output_nano_usd_per_million")?,
            row.try_get("cache_read_nano_usd_per_million")?,
            row.try_get("cache_write_nano_usd_per_million")?,
        ),
        catalog_provider_id: row.try_get("catalog_provider_id")?,
        catalog_model_id: row.try_get("catalog_model_id")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    }))
}

/// Agent adjustment, else the subject's, else list price.
pub async fn effective_adjustment(
    conn: &mut SqliteConnection,
    owner_user_id: &str,
    subject: &SubjectRef,
    agent_id: Option<&str>,
) -> Result<EffectiveAdjustment> {
    let provider = match subject.adjustment_scope() {
        Some(scope) => adjustment_on(conn, owner_user_id, &scope).await?,
        None => None,
    };
    let provider_pin = provider
        .as_ref()
        .and_then(|adjustment| adjustment.catalog_provider_id.clone());
    if let Some(agent_id) = agent_id {
        if let Some(agent) = adjustment_on(
            conn,
            owner_user_id,
            &PricingAdjustmentScope::Agent(agent_id.to_owned()),
        )
        .await?
        {
            return Ok(EffectiveAdjustment {
                source: AdjustmentSource::Agent,
                adjustment: Some(agent),
                provider_pin,
                scope_key: agent_scope_key(agent_id),
            });
        }
    }
    Ok(match provider {
        Some(provider) => EffectiveAdjustment {
            source: AdjustmentSource::Provider,
            adjustment: Some(provider),
            provider_pin,
            scope_key: String::new(),
        },
        None => EffectiveAdjustment {
            source: AdjustmentSource::Default,
            adjustment: None,
            provider_pin: None,
            scope_key: String::new(),
        },
    })
}

fn discounted(rate: Option<i64>, discount_bps: i64) -> Option<i64> {
    rate.map(|rate| {
        let kept = BPS_PER_WHOLE - i128::from(discount_bps.clamp(0, 10_000));
        // Non-negative operands: adding half the divisor rounds half away
        // from zero, matching Forge's cost rounding mode.
        ((i128::from(rate) * kept + BPS_PER_WHOLE / 2) / BPS_PER_WHOLE) as i64
    })
}

fn discounted_buckets(rates: RateBuckets, discount_bps: i64) -> RateBuckets {
    RateBuckets::new(
        discounted(rates.input, discount_bps),
        discounted(rates.output, discount_bps),
        discounted(rates.cache_read, discount_bps),
        discounted(rates.cache_write, discount_bps),
    )
}

fn usd_json(nano_per_million: i64) -> Value {
    // A decimal JSON number with at most nine fractional digits parses back
    // to the same nano-USD value through the models.dev number parser.
    let usd = pricing::NanoUsdPerMillion::from_nano_usd(nano_per_million)
        .map(pricing::NanoUsdPerMillion::to_usd_decimal_per_million)
        .unwrap_or_else(|| "0".to_owned());
    serde_json::from_str(&usd).unwrap_or(Value::Null)
}

/// Rewrites one models.dev rate object (a tier or the legacy context rate)
/// with every known bucket discounted, keeping all other fields.
fn discount_rate_object(raw: &Value, discount_bps: i64) -> Value {
    let mut object = raw.clone();
    if let Some(map) = object.as_object_mut() {
        for field in [
            "input",
            "output",
            "cache_read",
            "cache_write",
            "reasoning",
            "input_audio",
            "output_audio",
        ] {
            let Some(raw_number) = map.get(field).map(Value::to_string) else {
                continue;
            };
            if let Ok(rate) = pricing::NanoUsdPerMillion::parse_models_dev_json_number(&raw_number)
            {
                let scaled = discounted(Some(rate.as_nano_usd_per_million()), discount_bps)
                    .unwrap_or_default();
                map.insert(field.to_owned(), usd_json(scaled));
            }
        }
    }
    object
}

/// A price admission will materialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DesiredPrice {
    Catalog(PricingRateRevision),
    Manual {
        rates: RateBuckets,
        tiers_json: String,
        legacy_context_over_200k_json: Option<String>,
        context_tier_state: String,
        /// Catalog row the manual price was derived from, when discounted.
        derived_from: Option<PricingRateRevision>,
    },
}

impl DesiredPrice {
    pub fn rates(&self) -> RateBuckets {
        match self {
            Self::Catalog(row) => row.rates,
            Self::Manual { rates, .. } => *rates,
        }
    }

    pub fn catalog_row(&self) -> Option<&PricingRateRevision> {
        match self {
            Self::Catalog(row) => Some(row),
            Self::Manual { derived_from, .. } => derived_from.as_ref(),
        }
    }
}

/// Result of resolving one runtime model under an effective adjustment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceResolution {
    pub adjustment: EffectiveAdjustment,
    pub catalog: CatalogMatch,
    pub desired: Option<DesiredPrice>,
}

/// Resolves the price for one runtime model without writing anything.
pub async fn resolve_price(
    conn: &mut SqliteConnection,
    owner_user_id: &str,
    subject: &SubjectRef,
    subject_provider_kind: Option<&str>,
    agent_id: Option<&str>,
    runtime_model: &str,
) -> Result<PriceResolution> {
    let adjustment = effective_adjustment(conn, owner_user_id, subject, agent_id).await?;
    let (pin_provider, pin_model) = adjustment.pin();
    let hints = subject_provider_kind
        .and_then(subject_catalog_provider)
        .into_iter()
        .collect();
    let catalog = match_catalog(
        conn,
        &CatalogQuery {
            runtime_model,
            pin_provider,
            pin_model,
            hints,
        },
    )
    .await?;
    let catalog_row = match &catalog {
        CatalogMatch::Found(row) => Some((**row).clone()),
        _ => None,
    };
    let desired = match adjustment.adjustment.as_ref().map(|value| value.mode) {
        Some(PricingAdjustmentMode::Fixed) => {
            let fixed = adjustment
                .adjustment
                .as_ref()
                .map(|value| value.fixed_rates)
                .unwrap_or_default();
            Some(DesiredPrice::Manual {
                rates: fixed,
                tiers_json: "[]".to_owned(),
                legacy_context_over_200k_json: None,
                context_tier_state: "none".to_owned(),
                derived_from: None,
            })
        }
        Some(PricingAdjustmentMode::Discount) => {
            let bps = adjustment
                .adjustment
                .as_ref()
                .and_then(|value| value.discount_bps)
                .unwrap_or(0);
            catalog_row.map(|row| {
                let tiers = serde_json::from_str::<Vec<Value>>(&row.tiers_json)
                    .unwrap_or_default()
                    .iter()
                    .map(|tier| discount_rate_object(tier, bps))
                    .collect::<Vec<_>>();
                DesiredPrice::Manual {
                    rates: discounted_buckets(row.rates, bps),
                    tiers_json: Value::Array(tiers).to_string(),
                    legacy_context_over_200k_json: row
                        .legacy_context_over_200k_json
                        .as_deref()
                        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                        .map(|legacy| discount_rate_object(&legacy, bps).to_string()),
                    context_tier_state: row.context_tier_state.clone(),
                    derived_from: Some(row),
                }
            })
        }
        Some(PricingAdjustmentMode::List) | None => catalog_row.map(DesiredPrice::Catalog),
    };
    Ok(PriceResolution {
        adjustment,
        catalog,
        desired,
    })
}

// ---------------------------------------------------------------------------
// Admission-time materialization
// ---------------------------------------------------------------------------

struct SubjectState {
    id: String,
    revision_id: String,
    revision_digest: String,
    provider_kind: String,
}

fn digest(tag: &str, fields: &[Option<&str>]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tag.as_bytes());
    for field in fields {
        match field {
            None => hasher.update([0u8]),
            Some(value) => {
                hasher.update([1u8]);
                hasher.update((value.len() as u64).to_be_bytes());
                hasher.update(value.as_bytes());
            }
        }
    }
    hex::encode(hasher.finalize())
}

/// Loads the subject identity an admission runs through. `None` when the
/// admission names neither a provider entry nor a CLI runtime, or the
/// provider entry is gone.
async fn subject_identity(
    conn: &mut SqliteConnection,
    owner_user_id: &str,
    subject: &SubjectRef,
) -> Result<Option<(PricingSubjectKind, pricing::PricingSubjectIdentity)>> {
    if let Some(entry_id) = &subject.provider_entry_id {
        let row = sqlx::query(
            "SELECT id, owner_user_id, provider, label, status, enabled, credential_method,
                    metadata_json, version, created_at, updated_at
             FROM credential_handle WHERE id = ? AND owner_user_id = ?",
        )
        .bind(entry_id)
        .bind(owner_user_id)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let handle = CredentialHandle {
            id: row.try_get("id")?,
            owner_user_id: row.try_get("owner_user_id")?,
            provider: row.try_get("provider")?,
            label: row.try_get("label")?,
            status: row.try_get("status")?,
            enabled: row.try_get("enabled")?,
            credential_method: row.try_get("credential_method")?,
            metadata_json: row.try_get("metadata_json")?,
            version: row.try_get("version")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        };
        if handle.status == "revoked" {
            return Ok(None);
        }
        return Ok(Some((
            PricingSubjectKind::ProviderEntry,
            provider_identity(&handle),
        )));
    }
    let (Some(daemon_id), Some(executor_type)) = (&subject.daemon_id, &subject.executor_type)
    else {
        return Ok(None);
    };
    let detected_json: Option<String> =
        sqlx::query_scalar("SELECT detected_clis_json FROM daemon WHERE id = ?")
            .bind(daemon_id)
            .fetch_optional(&mut *conn)
            .await?;
    let detected = detected_json
        .as_deref()
        .and_then(|json| detected_cli(json, executor_type));
    Ok(Some((
        PricingSubjectKind::CliRuntime,
        cli_identity(executor_type, detected.as_ref()),
    )))
}

/// Provisions the subject and its current revision if needed. `None` when
/// the subject is retired or cannot be identified; such work stays unpriced.
async fn ensure_subject(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    owner_user_id: &str,
    subject: &SubjectRef,
    now: &str,
) -> Result<Option<SubjectState>> {
    let Some((kind, identity)) = subject_identity(tx, owner_user_id, subject).await? else {
        return Ok(None);
    };
    let expected_digest = pricing::pricing_subject_revision_digest(&identity);
    let (provider_entry_id, daemon_id, executor_type) = match kind {
        PricingSubjectKind::ProviderEntry => (subject.provider_entry_id.clone(), None, None),
        PricingSubjectKind::CliRuntime => (
            None,
            subject.daemon_id.clone(),
            subject.executor_type.clone(),
        ),
    };
    let existing = sqlx::query(
        "SELECT s.id, s.state, s.version, s.current_revision_id, r.revision, r.revision_digest
         FROM pricing_subject s
         LEFT JOIN pricing_subject_revision r ON r.id = s.current_revision_id
         WHERE s.owner_user_id = ? AND s.subject_kind = ?
           AND s.provider_entry_id IS ? AND s.daemon_id IS ? AND s.executor_type IS ?",
    )
    .bind(owner_user_id)
    .bind(kind.to_string())
    .bind(provider_entry_id.as_deref())
    .bind(daemon_id.as_deref())
    .bind(executor_type.as_deref())
    .fetch_optional(&mut **tx)
    .await?;

    let (subject_id, subject_version, next_revision) = match existing {
        Some(row) => {
            let state: String = row.try_get("state")?;
            if state != "active" {
                return Ok(None);
            }
            let id: String = row.try_get("id")?;
            let current_revision_id: Option<String> = row.try_get("current_revision_id")?;
            let current_digest: Option<String> = row.try_get("revision_digest")?;
            if let (Some(revision_id), Some(current_digest)) = (current_revision_id, current_digest)
            {
                if current_digest == expected_digest {
                    return Ok(Some(SubjectState {
                        id,
                        revision_id,
                        revision_digest: current_digest,
                        provider_kind: identity.provider_kind,
                    }));
                }
            }
            let revision: Option<i64> = row.try_get("revision")?;
            (
                id,
                row.try_get::<i64, _>("version")?,
                revision.unwrap_or(0) + 1,
            )
        }
        None => {
            let created = db::PricingSubjectRepo::create_pricing_subject_in_tx(
                db,
                tx,
                db::CreatePricingSubject {
                    id: db::new_uuid_v4(),
                    owner_user_id: owner_user_id.to_owned(),
                    subject_kind: kind,
                    provider_entry_id: provider_entry_id.clone(),
                    daemon_id: daemon_id.clone(),
                    executor_type: executor_type.clone(),
                    current_revision_id: None,
                    state: db::PricingSubjectState::Active,
                    last_idempotency_key: None,
                    last_update_digest: None,
                    created_at: now.to_owned(),
                    updated_at: now.to_owned(),
                },
            )
            .await?;
            (created.id, created.version, 1)
        }
    };
    let revision = db::PricingSubjectRepo::create_pricing_subject_revision_in_tx(
        db,
        tx,
        db::CreatePricingSubjectRevision {
            id: db::new_uuid_v4(),
            subject_id: subject_id.clone(),
            owner_user_id: owner_user_id.to_owned(),
            revision: next_revision,
            revision_digest: expected_digest.clone(),
            subject_kind: kind,
            provider_entry_id,
            daemon_id,
            executor_type,
            provider_kind: identity.provider_kind.clone(),
            credential_method: identity.credential_method.clone(),
            endpoint_class: identity.endpoint_class.clone(),
            runtime_fingerprint: identity.runtime_fingerprint.clone(),
            schema_revision: identity.schema_revision.clone(),
            non_secret_identity_json: json!({
                "subject_kind": identity.subject_kind,
                "provider_kind": identity.provider_kind,
                "credential_method": identity.credential_method,
                "endpoint_class": identity.endpoint_class,
                "runtime_fingerprint": identity.runtime_fingerprint,
                "schema_revision": identity.schema_revision,
            })
            .to_string(),
            created_at: now.to_owned(),
        },
    )
    .await?;
    db::PricingSubjectRepo::update_pricing_subject_in_tx(
        db,
        tx,
        db::UpdatePricingSubject {
            id: subject_id.clone(),
            expected_version: subject_version,
            current_revision_id: Some(Some(revision.id.clone())),
            state: None,
            last_idempotency_key: None,
            last_update_digest: None,
            updated_at: now.to_owned(),
        },
    )
    .await?;
    Ok(Some(SubjectState {
        id: subject_id,
        revision_id: revision.id,
        revision_digest: expected_digest,
        provider_kind: identity.provider_kind,
    }))
}

fn manual_received_rates_json(rates: RateBuckets) -> String {
    json!({
        "input_nano_usd_per_million": rates.input,
        "output_nano_usd_per_million": rates.output,
        "cache_read_nano_usd_per_million": rates.cache_read,
        "cache_write_nano_usd_per_million": rates.cache_write,
    })
    .to_string()
}

/// Returns the manual rate revision for this subject revision/model/price,
/// creating it on first use. Identical prices share one immutable row.
async fn manual_rate_revision(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    owner_user_id: &str,
    subject: &SubjectState,
    runtime_model: &str,
    desired: &DesiredPrice,
    now: &str,
) -> Result<String> {
    let DesiredPrice::Manual {
        rates,
        tiers_json,
        legacy_context_over_200k_json,
        context_tier_state,
        ..
    } = desired
    else {
        return Err(ServiceError::invalid_operation(
            "manual rate requested for a catalog price",
        ));
    };
    let rate_strings = [
        rates.input,
        rates.output,
        rates.cache_read,
        rates.cache_write,
    ]
    .map(|rate| rate.map(|value| value.to_string()));
    let id = digest(
        "auto-manual-rate-v1",
        &[
            Some(&subject.revision_id),
            Some(&subject.revision_digest),
            Some(runtime_model),
            rate_strings[0].as_deref(),
            rate_strings[1].as_deref(),
            rate_strings[2].as_deref(),
            rate_strings[3].as_deref(),
            Some(tiers_json),
            legacy_context_over_200k_json.as_deref(),
            Some(context_tier_state),
        ],
    );
    let exists: Option<String> =
        sqlx::query_scalar("SELECT id FROM pricing_rate_revision WHERE id = ?")
            .bind(&id)
            .fetch_optional(&mut **tx)
            .await?;
    if exists.is_none() {
        db::PricingCatalogRepo::create_pricing_rate_revision_in_tx(
            db,
            tx,
            db::CreatePricingRateRevision {
                id: id.clone(),
                source_kind: PricingRateSourceKind::ManualOverride,
                owner_user_id: Some(owner_user_id.to_owned()),
                catalog_snapshot_id: None,
                catalog_provider_id: None,
                catalog_model_id: None,
                pricing_subject_revision_id: Some(subject.revision_id.clone()),
                pricing_subject_revision_digest: Some(subject.revision_digest.clone()),
                runtime_model: Some(runtime_model.to_owned()),
                source_model_key: None,
                source_last_updated: None,
                currency: "USD".to_owned(),
                rates: *rates,
                tiers_json: tiers_json.clone(),
                legacy_context_over_200k_json: legacy_context_over_200k_json.clone(),
                context_tier_state: context_tier_state.clone(),
                received_rates_json: manual_received_rates_json(*rates),
                rate_digest: id.clone(),
                effective_at: now.to_owned(),
                created_at: now.to_owned(),
            },
        )
        .await?;
    }
    Ok(id)
}

/// Makes the one binding admission should resolve for this subject, agent,
/// and runtime model active, retiring whatever the scope held before.
/// Returns the binding scope key to resolve with.
pub(crate) async fn prepare_price_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    owner_user_id: &str,
    subject_ref: &SubjectRef,
    agent_id: Option<&str>,
    runtime_model: &str,
    now: &str,
) -> Result<String> {
    let Some(subject) = ensure_subject(db, tx, owner_user_id, subject_ref, now).await? else {
        return Ok(String::new());
    };
    let resolution = resolve_price(
        tx,
        owner_user_id,
        subject_ref,
        Some(&subject.provider_kind),
        agent_id,
        runtime_model,
    )
    .await?;
    let scope_key = resolution.adjustment.scope_key.clone();
    let desired = match &resolution.desired {
        None => None,
        Some(DesiredPrice::Catalog(row)) => Some((
            PricingRateSourceKind::ModelsDevCatalog,
            row.id.clone(),
            row.catalog_provider_id.clone(),
            row.catalog_model_id.clone(),
        )),
        Some(desired) => Some((
            PricingRateSourceKind::ManualOverride,
            manual_rate_revision(db, tx, owner_user_id, &subject, runtime_model, desired, now)
                .await?,
            None,
            None,
        )),
    };

    let active = sqlx::query(
        "SELECT id, version, source_kind, rate_revision_id FROM pricing_subject_binding
         WHERE subject_id = ? AND subject_revision_id = ? AND scope_key = ?
           AND runtime_model = ? AND state = 'active'",
    )
    .bind(&subject.id)
    .bind(&subject.revision_id)
    .bind(&scope_key)
    .bind(runtime_model)
    .fetch_all(&mut **tx)
    .await?;
    let mut already_active = false;
    for row in active {
        let id: String = row.try_get("id")?;
        let source_kind: String = row.try_get("source_kind")?;
        let rate_revision_id: String = row.try_get("rate_revision_id")?;
        let matches = desired.as_ref().is_some_and(|(kind, rate, _, _)| {
            kind.to_string() == source_kind && *rate == rate_revision_id
        });
        if matches {
            already_active = true;
            continue;
        }
        db::PricingSubjectRepo::retire_pricing_subject_binding_in_tx(
            db,
            tx,
            db::RetirePricingSubjectBinding {
                id,
                expected_version: row.try_get("version")?,
                retired_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await?;
    }
    if let (false, Some((source_kind, rate_revision_id, catalog_provider_id, catalog_model_id))) =
        (already_active, desired)
    {
        let binding_digest = digest(
            "auto-binding-v1",
            &[
                Some(&subject.id),
                Some(&subject.revision_digest),
                Some(&scope_key),
                Some(runtime_model),
                Some(&source_kind.to_string()),
                Some(&rate_revision_id),
            ],
        );
        db::PricingSubjectRepo::create_pricing_subject_binding_in_tx(
            db,
            tx,
            db::CreatePricingSubjectBinding {
                id: db::new_uuid_v4(),
                owner_user_id: owner_user_id.to_owned(),
                subject_id: subject.id.clone(),
                subject_revision_id: subject.revision_id.clone(),
                subject_revision_digest: subject.revision_digest.clone(),
                scope_key: scope_key.clone(),
                runtime_model: runtime_model.to_owned(),
                source_kind,
                catalog_provider_id,
                catalog_model_id,
                rate_revision_id,
                binding_digest,
                effective_at: now.to_owned(),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await?;
    }
    Ok(scope_key)
}

/// Provider kind recorded for a subject reference, for catalog tie-breaks
/// outside admission (settings previews).
pub async fn subject_provider_kind(
    conn: &mut SqliteConnection,
    owner_user_id: &str,
    subject: &SubjectRef,
) -> Result<Option<String>> {
    Ok(subject_identity(conn, owner_user_id, subject)
        .await?
        .map(|(_, identity)| identity.provider_kind))
}

/// An agent's adjustments and the price its next run would freeze.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPricePreview {
    pub runtime_model: Option<String>,
    pub agent_adjustment: Option<PricingAdjustment>,
    pub provider_adjustment: Option<PricingAdjustment>,
    /// `None` when the agent has no model.
    pub resolution: Option<PriceResolution>,
}

/// The pricing subject an agent's selected profile runs through.
pub fn agent_subject(agent: &db::Agent) -> SubjectRef {
    SubjectRef {
        provider_entry_id: agent.credential_ref.clone(),
        daemon_id: agent.daemon_id.clone(),
        executor_type: Some(agent.executor_type.clone()),
    }
}

/// Resolves an agent's price without writing anything.
pub async fn preview_agent_price(
    db: &SqliteDb,
    owner_user_id: &str,
    agent: &db::Agent,
) -> Result<AgentPricePreview> {
    let mut conn = db.pool().acquire().await?;
    let subject = agent_subject(agent);
    let agent_adjustment = adjustment_on(
        &mut conn,
        owner_user_id,
        &PricingAdjustmentScope::Agent(agent.id.clone()),
    )
    .await?;
    let provider_adjustment = match subject.adjustment_scope() {
        Some(scope) => adjustment_on(&mut conn, owner_user_id, &scope).await?,
        None => None,
    };
    let runtime_model = agent.model.clone().filter(|model| !model.trim().is_empty());
    let resolution = match runtime_model.as_deref() {
        Some(runtime_model) => {
            let provider_kind = subject_provider_kind(&mut conn, owner_user_id, &subject).await?;
            Some(
                resolve_price(
                    &mut conn,
                    owner_user_id,
                    &subject,
                    provider_kind.as_deref(),
                    Some(&agent.id),
                    runtime_model,
                )
                .await?,
            )
        }
        None => None,
    };
    Ok(AgentPricePreview {
        runtime_model,
        agent_adjustment,
        provider_adjustment,
        resolution,
    })
}

/// Whether the active snapshot lists a provider (and, if given, a model).
pub async fn catalog_lists(
    db: &SqliteDb,
    provider_id: &str,
    model_id: Option<&str>,
) -> Result<bool> {
    let mut conn = db.pool().acquire().await?;
    let Some(snapshot_id) = active_snapshot_id(&mut conn).await? else {
        return Ok(false);
    };
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM pricing_rate_revision
         WHERE catalog_snapshot_id = ? AND catalog_provider_id = ?
           AND (? IS NULL OR catalog_model_id = ?)
         LIMIT 1",
    )
    .bind(&snapshot_id)
    .bind(provider_id)
    .bind(model_id)
    .bind(model_id)
    .fetch_optional(&mut *conn)
    .await?
    .is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discount_rounds_half_away_from_zero() {
        assert_eq!(discounted(Some(1_000_000_000), 2_000), Some(800_000_000));
        assert_eq!(discounted(Some(3), 5_000), Some(2));
        assert_eq!(discounted(Some(7), 10_000), Some(0));
        assert_eq!(discounted(None, 2_000), None);
    }

    #[test]
    fn discounted_tier_json_round_trips_through_the_catalog_parser() {
        let tier = json!({"tier": {"type": "context", "size": 200000}, "input": 2.5, "output": 15});
        let scaled = discount_rate_object(&tier, 2_000);
        let tiers = pricing::parse_persisted_context_tiers(&Value::Array(vec![scaled]).to_string())
            .expect("discounted tier parses");
        assert_eq!(tiers[0].threshold_tokens, 200_000);
        assert_eq!(
            tiers[0]
                .rates
                .input
                .map(pricing::NanoUsdPerMillion::as_nano_usd_per_million),
            Some(2_000_000_000)
        );
        assert_eq!(
            tiers[0]
                .rates
                .output
                .map(pricing::NanoUsdPerMillion::as_nano_usd_per_million),
            Some(12_000_000_000)
        );
    }

    #[test]
    fn model_family_names_the_publishing_provider() {
        assert_eq!(model_family_provider("glm-5.3"), Some("zai"));
        assert_eq!(model_family_provider("gpt-5.6-sol"), Some("openai"));
        assert_eq!(model_family_provider("claude-sonnet-5"), Some("anthropic"));
        assert_eq!(model_family_provider("gemini-3.8-flash"), Some("google"));
        assert_eq!(model_family_provider("mystery-1"), None);
    }
}
