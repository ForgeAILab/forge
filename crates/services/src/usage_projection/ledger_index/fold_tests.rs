// Differential reference: the index fold against aggregate_usage_with_sources for
// arbitrary rows, including shapes the V135 guards forbid today (legacy data).
use super::super::*;
use db::CostCoverageReasonCode as Code;

fn rnd(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
fn pick<T: Clone>(rng: &mut u64, items: &[T]) -> T {
    items[(rnd(rng) % items.len() as u64) as usize].clone()
}
fn shuffled<T: Clone>(rng: &mut u64, items: &[T]) -> Vec<T> {
    let mut out = items.to_vec();
    for i in (1..out.len()).rev() {
        let j = (rnd(rng) % (i as u64 + 1)) as usize;
        out.swap(i, j);
    }
    out
}

type Exec = (String, Option<String>, bool);

fn make_event(
    rng: &mut u64,
    inv: &UsageInvocation,
    id: String,
    force_estimated: bool,
) -> EffectiveUsageEvent {
    let agents = [None, Some("a"), Some("b"), Some("c")];
    let mut e = crate::task_usage_fixture::event(&id, &inv.id);
    e.source_id = inv.source_id.clone();
    e.surface = inv.surface;
    e.attempt_ordinal = inv.attempt_ordinal;
    e.agent_id = pick(rng, &agents).map(str::to_owned);
    e.occurred_at = format!("2026-01-01T00:00:0{}Z", rnd(rng) % 3);
    e.input_tokens = Some((rnd(rng) % 100) as i64);
    e.output_tokens = (!rnd(rng).is_multiple_of(3)).then_some((rnd(rng) % 50) as i64);
    e.cache_read_tokens = (rnd(rng).is_multiple_of(2)).then_some((rnd(rng) % 20) as i64);
    e.cache_write_tokens = None;
    e.telemetry_state = pick(
        rng,
        &[
            DbUsageTelemetryState::Metered,
            DbUsageTelemetryState::Unmetered,
        ],
    );
    e.provider_reported_nano_usd = None;
    let kind = if force_estimated { 2 } else { rnd(rng) % 5 };
    let source = match kind {
        0 => {
            e.provider_reported_nano_usd = Some((rnd(rng) % 1000) as i64);
            e.cost_kind = UsageCostKind::ProviderReported;
            event_source(&e).unwrap()
        }
        1 => {
            e.provenance_kind = UsageEventProvenanceKind::LegacyExecutionAggregate;
            e.cost_kind = UsageCostKind::ProviderReported;
            if rnd(rng).is_multiple_of(2) {
                e.legacy_cost_usd_raw = Some("0.000123".into());
            } else {
                e.provider_reported_nano_usd = Some((rnd(rng) % 1000) as i64);
            }
            event_source(&e).unwrap()
        }
        2 | 3 => {
            e.cost_kind = UsageCostKind::Estimated;
            e.estimated_nano_usd = Some((rnd(rng) % 1000) as i64);
            e.rate_revision_id = Some(format!("rate-{}", rnd(rng) % 2));
            e.catalog_snapshot_id = Some("cat-0".into());
            e.formula_revision = Some(format!("f{}", rnd(rng) % 3));
            e.retrospective = rnd(rng).is_multiple_of(2);
            if rnd(rng).is_multiple_of(7) {
                None
            } else {
                Some((
                    format!(
                        "estimated:models_dev_catalog:{}:cat-0",
                        e.rate_revision_id.clone().unwrap()
                    ),
                    CostSourceRef {
                        source_kind: CostSourceKind::ModelsDevCatalog,
                        rate_revision_id: e.rate_revision_id.clone(),
                        catalog_snapshot_id: e.catalog_snapshot_id.clone(),
                        catalog_digest: None,
                        effective_at: None,
                        fetched_at: None,
                        freshness: pick(
                            rng,
                            &[
                                CostSourceFreshness::Fresh,
                                CostSourceFreshness::Stale,
                                CostSourceFreshness::RefreshFailed,
                            ],
                        ),
                        retrospective: e.retrospective,
                        formula_revision: e.formula_revision.clone(),
                    },
                ))
            }
        }
        _ => {
            e.cost_kind = UsageCostKind::None;
            e.coverage_reason_code = pick(
                rng,
                &[
                    None,
                    Some(Code::MissingBinding),
                    Some(Code::MissingRate),
                    Some(Code::Unmetered),
                    Some(Code::Pending),
                    Some(Code::UnresolvedTier),
                    Some(Code::InvalidLegacyUsage),
                ],
            );
            None
        }
    };
    EffectiveUsageEvent { event: e, source }
}

fn set_execution(state: &mut State, exec: &Exec, present: bool) {
    let run = Arc::new(RunKey {
        surface: DbUsageSurface::TaskExecution,
        source_id: exec.0.clone(),
    });
    let domain = present.then_some(exec.2);
    state
        .run_change((None, run.clone()), None, None, Some(domain))
        .unwrap();
    if let Some(owner) = &exec.1 {
        state
            .run_change(
                (Some(Arc::from(owner.as_str())), run),
                None,
                None,
                Some(domain),
            )
            .unwrap();
    }
}

fn check(
    state: &mut State,
    invocations: &[UsageInvocation],
    events: &[EffectiveUsageEvent],
    executions: &[Exec],
    context: &str,
) {
    super::repair_citations(state, events);
    let mut invs = invocations.to_vec();
    invs.sort_by(|a, b| {
        (&a.source_id, a.attempt_ordinal, &a.id).cmp(&(&b.source_id, b.attempt_ordinal, &b.id))
    });
    let mut sorted = events.to_vec();
    sorted.sort_by(|a, b| {
        (&a.event.occurred_at, &a.event.id).cmp(&(&b.event.occurred_at, &b.event.id))
    });
    let mut by_inv = HashMap::<String, Vec<EffectiveUsageEvent>>::new();
    for e in &sorted {
        by_inv
            .entry(e.event.invocation_id.clone())
            .or_default()
            .push(e.clone());
    }
    // Operations reference (operations_usage_in_snapshot)
    let mut domain = invs
        .iter()
        .map(|i| UsageDomainRun {
            surface: i.surface,
            source_id: i.source_id.clone(),
            pending: false,
        })
        .collect::<Vec<_>>();
    for (id, _, pending) in executions {
        domain.push(UsageDomainRun {
            surface: DbUsageSurface::TaskExecution,
            source_id: id.clone(),
            pending: *pending,
        });
    }
    let all_events = invs
        .iter()
        .map(|i| (i.id.clone(), by_inv.get(&i.id).cloned().unwrap_or_default()))
        .collect::<HashMap<_, _>>();
    let expected = aggregate_usage_with_sources(&invs, &all_events, &domain).unwrap();
    let actual = state.scopes.entry(None).or_default().public().unwrap();
    assert_eq!(
        serde_json::to_value(&actual).unwrap(),
        serde_json::to_value(&expected).unwrap(),
        "operations {context}"
    );
    // Agent reference (agent_usage_in_snapshot)
    for agent in ["a", "b", "c"] {
        let mut sources = HashSet::new();
        for i in &invs {
            if i.agent_id.as_deref() == Some(agent) {
                sources.insert(i.source_id.clone());
            }
        }
        for e in &sorted {
            if e.event.agent_id.as_deref() == Some(agent) {
                sources.insert(e.event.source_id.clone());
            }
        }
        let mut matching = Vec::new();
        let mut matching_events = HashMap::new();
        for i in &invs {
            if !sources.contains(&i.source_id) {
                continue;
            }
            let matches = i.agent_id.as_deref() == Some(agent);
            let filtered = by_inv
                .get(&i.id)
                .into_iter()
                .flatten()
                .filter(|e| matches || e.event.agent_id.as_deref() == Some(agent))
                .cloned()
                .collect::<Vec<_>>();
            if !matches && filtered.is_empty() {
                continue;
            }
            matching.push(i.clone());
            matching_events.insert(i.id.clone(), filtered);
        }
        let domain = executions
            .iter()
            .filter(|e| e.1.as_deref() == Some(agent))
            .map(|e| UsageDomainRun {
                surface: DbUsageSurface::TaskExecution,
                source_id: e.0.clone(),
                pending: e.2,
            })
            .collect::<Vec<_>>();
        let expected = aggregate_usage_with_sources(&matching, &matching_events, &domain).unwrap();
        let key = Some(Arc::<str>::from(agent));
        let actual = match state.scopes.get_mut(&key) {
            Some(scope) => scope.public().unwrap(),
            None => Totals::default().public(Vec::new()).unwrap(),
        };
        assert_eq!(
            serde_json::to_value(&actual).unwrap(),
            serde_json::to_value(&expected).unwrap(),
            "agent {agent} {context}"
        );
    }
}

fn run_seed(seed: u64, legacy_shapes: bool) {
    let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let agents = [None, Some("a"), Some("b"), Some("c")];
    let surfaces = [
        DbUsageSurface::TaskExecution,
        DbUsageSurface::TaskExecution,
        DbUsageSurface::TaskExecution,
        DbUsageSurface::ProjectChat,
        DbUsageSurface::MainChat,
        DbUsageSurface::GenesisChat,
        DbUsageSurface::MainInquiry,
    ];
    let lifecycles = [
        UsageInvocationLifecycle::Admitted,
        UsageInvocationLifecycle::Started,
        UsageInvocationLifecycle::PendingSettlement,
        UsageInvocationLifecycle::Settled,
        UsageInvocationLifecycle::Settled,
        UsageInvocationLifecycle::Settled,
        UsageInvocationLifecycle::Unsettled,
    ];
    let mut invocations = Vec::new();
    let mut events = Vec::new();
    for k in 0..(1 + rnd(&mut rng) % 10) {
        let mut inv =
            crate::task_usage_fixture::invocation(&format!("inv-{}-{k}", rnd(&mut rng) % 5));
        inv.source_id = format!("src-{}", rnd(&mut rng) % 4);
        inv.surface = pick(&mut rng, &surfaces);
        inv.attempt_ordinal = (rnd(&mut rng) % 3) as i64;
        inv.agent_id = pick(&mut rng, &agents).map(str::to_owned);
        inv.lifecycle = pick(&mut rng, &lifecycles);
        inv.telemetry_state = match inv.lifecycle {
            UsageInvocationLifecycle::Settled => pick(
                &mut rng,
                &[
                    DbUsageTelemetryState::Metered,
                    DbUsageTelemetryState::Metered,
                    DbUsageTelemetryState::Unmetered,
                ],
            ),
            UsageInvocationLifecycle::Unsettled => DbUsageTelemetryState::Unsettled,
            _ => DbUsageTelemetryState::Pending,
        };
        let allowed = legacy_shapes || inv.lifecycle == UsageInvocationLifecycle::Settled;
        if allowed {
            for j in 0..(rnd(&mut rng) % 4) {
                let id = format!("ev-{}-{k}-{j}", rnd(&mut rng) % 7);
                events.push(make_event(&mut rng, &inv, id, false));
            }
        }
        invocations.push(inv);
    }
    let mut executions: Vec<Exec> = Vec::new();
    for k in 0..(rnd(&mut rng) % 6) {
        if executions.iter().all(|e| e.0 != format!("src-{k}")) && !rnd(&mut rng).is_multiple_of(3)
        {
            executions.push((
                format!("src-{k}"),
                pick(&mut rng, &agents).map(str::to_owned),
                rnd(&mut rng).is_multiple_of(2),
            ));
        }
    }
    let mut state = State::default();
    for inv in shuffled(&mut rng, &invocations) {
        state.invocation(inv).unwrap();
    }
    for e in shuffled(&mut rng, &events) {
        state.event(e, 1).unwrap();
    }
    for e in &executions {
        set_execution(&mut state, e, true);
    }
    check(
        &mut state,
        &invocations,
        &events,
        &executions,
        &format!("seed={seed} legacy={legacy_shapes} built"),
    );

    // Repricing-style replacement: subtract an event, add its new effective form.
    for event_slot in &mut events {
        if rnd(&mut rng).is_multiple_of(3)
            && event_slot.event.cost_kind != UsageCostKind::ProviderReported
        {
            let old = (*event_slot).clone();
            state.event(old.clone(), -1).unwrap();
            let inv = invocations
                .iter()
                .find(|i| i.id == old.event.invocation_id)
                .unwrap();
            let mut new = make_event(&mut rng, inv, old.event.id.clone(), true);
            new.event.agent_id = old.event.agent_id.clone();
            new.event.occurred_at = old.event.occurred_at.clone();
            new.event.input_tokens = old.event.input_tokens;
            new.event.output_tokens = old.event.output_tokens;
            new.event.cache_read_tokens = old.event.cache_read_tokens;
            new.event.telemetry_state = old.event.telemetry_state;
            new.event.coverage_reason_code = old.event.coverage_reason_code;
            state.event(new.clone(), 1).unwrap();
            (*event_slot) = new;
        }
    }
    check(
        &mut state,
        &invocations,
        &events,
        &executions,
        &format!("seed={seed} legacy={legacy_shapes} repriced"),
    );

    // Lifecycle deltas on attempts without events.
    for inv in invocations.iter_mut() {
        if events.iter().any(|e| e.event.invocation_id == inv.id) {
            continue;
        }
        let next = match inv.lifecycle {
            UsageInvocationLifecycle::Admitted => UsageInvocationLifecycle::Started,
            UsageInvocationLifecycle::Started => pick(
                &mut rng,
                &[
                    UsageInvocationLifecycle::PendingSettlement,
                    UsageInvocationLifecycle::Settled,
                    UsageInvocationLifecycle::Unsettled,
                ],
            ),
            UsageInvocationLifecycle::PendingSettlement => pick(
                &mut rng,
                &[
                    UsageInvocationLifecycle::Settled,
                    UsageInvocationLifecycle::Unsettled,
                ],
            ),
            other => other,
        };
        inv.lifecycle = next;
        inv.telemetry_state = match next {
            UsageInvocationLifecycle::Settled => pick(
                &mut rng,
                &[
                    DbUsageTelemetryState::Metered,
                    DbUsageTelemetryState::Unmetered,
                ],
            ),
            UsageInvocationLifecycle::Unsettled => DbUsageTelemetryState::Unsettled,
            _ => DbUsageTelemetryState::Pending,
        };
        state.invocation(inv.clone()).unwrap();
    }
    check(
        &mut state,
        &invocations,
        &events,
        &executions,
        &format!("seed={seed} legacy={legacy_shapes} transitions"),
    );

    // Execution status change, owner move and removal.
    for execution_slot in &mut executions {
        let old = execution_slot.clone();
        set_execution(&mut state, &old, false);
        match rnd(&mut rng) % 3 {
            0 => {
                execution_slot.2 = !old.2;
                set_execution(&mut state, &execution_slot.clone(), true);
            }
            1 => {
                execution_slot.1 = pick(&mut rng, &agents).map(str::to_owned);
                set_execution(&mut state, &execution_slot.clone(), true);
            }
            _ => {
                execution_slot.0 = String::new();
            }
        }
    }
    executions.retain(|e| !e.0.is_empty());
    check(
        &mut state,
        &invocations,
        &events,
        &executions,
        &format!("seed={seed} legacy={legacy_shapes} executions"),
    );
}

#[test]
fn pure_fold_guarded_shapes() {
    for seed in 1..=1500u64 {
        run_seed(seed, false);
    }
}

#[test]
fn pure_fold_legacy_shapes() {
    for seed in 1..=1500u64 {
        run_seed(seed, true);
    }
}
