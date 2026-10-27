//! Workflow liveness, recovery and replan-authority invariants.

use super::*;

// Model-only observations for invariant (l). Whether a Task is a
// lead-managed root is read inside the transaction that writes each `replan`
// charge and each planning entry over the `replan` bridge, so a later change
// of the assignment cannot rewrite what was true when the re-plan was decided.
//
// The product decides a re-plan once, at its charge (`charge_replan`), and
// the entry that carries the same receipt is that decision's second half:
// the owner's removal of the lead may land between the two commands, and the
// product keeps the bridge on purpose (services test
// `a_rejection_decided_as_a_replan_keeps_its_bridge_when_the_lead_is_removed`).
// So an entry is authorized by a lead at its own commit, or by the charge of
// its own receipt having been written under a lead. Every other charge or
// entry on a Task that is not a lead-managed root is a violation, and stays
// one after the lead is assigned again.
pub(super) const MODEL_LEAD_ROOT: &str = "t.parent_task_id IS NULL AND EXISTS (SELECT 1 FROM task c WHERE c.parent_task_id = t.id AND c.deleted_at IS NULL) AND EXISTS (SELECT 1 FROM task_role_assignment a WHERE a.task_id = t.id AND a.role_name = 'planner' AND a.assignee_type = 'agent' AND a.assignee_id IS NOT NULL)";

pub(super) async fn install_replan_audit(pool: &sqlx::SqlitePool) {
    sqlx::query("CREATE TABLE IF NOT EXISTS model_replan_authority (task_id TEXT NOT NULL, kind TEXT NOT NULL, receipt TEXT NOT NULL, eligible INTEGER NOT NULL)")
        .execute(pool).await.unwrap();
    let lead_root_now =
        format!("COALESCE((SELECT ({MODEL_LEAD_ROOT}) FROM task t WHERE t.id=NEW.task_id),0)");
    let charged_under_a_lead = "EXISTS(SELECT 1 FROM model_replan_authority a WHERE a.task_id=NEW.task_id AND a.kind='charge' AND a.eligible=1 AND a.receipt=json_extract(NEW.bridge_payload,'$.receipt'))";
    for (kind, table, filter, receipt, eligible) in [
        (
            "charge",
            "task_budget_charge",
            "NEW.kind = 'replan'",
            "NEW.step_id",
            lead_root_now.clone(),
        ),
        (
            "entry",
            "transition_log",
            "NEW.bridge_kind = 'replan'",
            "COALESCE(json_extract(NEW.bridge_payload,'$.receipt'), NEW.id)",
            format!("({lead_root_now} OR {charged_under_a_lead})"),
        ),
    ] {
        sqlx::raw_sql(&format!(
            "CREATE TRIGGER IF NOT EXISTS model_replan_{kind} AFTER INSERT ON {table}
             WHEN {filter} BEGIN INSERT INTO model_replan_authority(task_id,kind,receipt,eligible)
             VALUES(NEW.task_id, '{kind}', {receipt}, {eligible}); END"
        ))
        .execute(pool)
        .await
        .unwrap();
    }
}

impl World {
    /// The invariants that hold in every committed state.
    pub(super) async fn check_committed(&mut self) -> Result<(), String> {
        if let Some(error) = self.shared.lead_operation_errors.lock().unwrap().first() {
            return Err(format!(
                "scripted lead operation failed through the native entry: {error}"
            ));
        }
        // (e) the condition invariant the code already asserts.
        let violations = self
            .live()
            .state
            .db
            .task_condition_violations()
            .await
            .map_err(|error| error.to_string())?;
        if !violations.is_empty() {
            let indexes: Vec<_> = violations
                .iter()
                .map(|id| self.tasks.iter().position(|task| task == id))
                .collect();
            return Err(format!(
                "(e) stored condition disagrees with durable state for tasks {indexes:?}"
            ));
        }
        // (f) terminal Tasks stay terminal.
        for index in 0..self.tasks.len() {
            let status: String = sqlx::query_scalar("SELECT status FROM task WHERE id = ?")
                .bind(&self.tasks[index])
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
            match self.settled.get(&index) {
                Some(settled) if *settled != status => {
                    return Err(format!(
                        "(f) task {index} left terminal state {settled} for {status}"
                    ));
                }
                None if TERMINAL.contains(&status.as_str()) => {
                    self.settled.insert(index, status);
                }
                _ => {}
            }
        }
        self.check_roots(false).await?;
        self.check_replans(false).await?;
        self.check_step_checks(false).await?;
        Ok(())
    }

    /// The step-check invariants of plan 3.10 stage 3 (`lead_check`).
    ///
    /// Real here: the no-lead rule. Every subtask the model finishes now
    /// passes through `lead_check`, and under a root without a lead it must
    /// never rest there (the gate is skipped in the entry's own step, and the
    /// dispatcher passes a subtask it finds there). With the skip target
    /// mutated back to "the first implementation state" this fails on the
    /// first finished subtask.
    ///
    /// (m) and (n) bind where a root has a lead: `Action::AssignLead` and
    /// the scripted lead exist, and `a_root_with_a_lead_plans_and_has_every_
    /// step_checked` runs every invariant on states with a live step check, a
    /// returned step and a charged `gate:lead_check`. Random sequences also
    /// create coordination roots, assign/remove leads and run their operations.
    /// The operation corpus exhausts the check budget and recovers the step.
    pub(super) async fn check_step_checks(&self, quiescent: bool) -> Result<(), String> {
        let pool = &self.live().pool;
        let rows = |sql: &'static str| async move {
            sqlx::query_scalar::<_, String>(sql)
                .fetch_all(pool)
                .await
                .map_err(|error| error.to_string())
        };
        // (n) the returns of a step are bounded by the gate's limit.
        let overspent = rows(
            "SELECT task_id || ' spent ' || spent FROM task_budget
             WHERE kind = 'gate:lead_check' AND spent > 2",
        )
        .await?;
        if !overspent.is_empty() {
            return Err(format!(
                "(n) step checks returned past their limit: {overspent:?}"
            ));
        }
        // A step check is a planner run and nothing else: no other role ever
        // runs on a subtask in the gate.
        let foreign = rows(
            "SELECT c.id || ' runs ' || e.role FROM task c JOIN execution e ON e.task_id = c.id
             WHERE c.parent_task_id IS NOT NULL AND c.deleted_at IS NULL
               AND c.status = 'lead_check' AND e.status = 'running' AND e.role != 'planner'",
        )
        .await?;
        if !foreign.is_empty() {
            return Err(format!(
                "a subtask in the step check runs another role: {foreign:?}"
            ));
        }
        if !quiescent {
            return Ok(());
        }
        const LEAD: &str = "EXISTS (SELECT 1 FROM task_role_assignment a WHERE a.task_id = r.id AND a.role_name = 'planner' AND a.assignee_type = 'agent' AND a.assignee_id IS NOT NULL)";
        // No lead: a subtask never rests in the gate. A hold, a failure or a
        // wait on its root is a rest with an owner; anything else is not: a
        // clear condition, and equally a park that asks somebody to assign
        // the role (the form a broken skip takes: mutation-tested by removing
        // the gate's skip hook and the dispatcher's `gate_skip` fact).
        let resting: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT c.id || ' ' || COALESCE(c.condition_json, '') FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.deleted_at IS NULL AND c.status = 'lead_check'
               AND NOT {LEAD}
               AND r.status IN ('in_progress', 'merge_failed')
               AND COALESCE(json_extract(c.condition_json, '$.kind'), 'clear') != 'failed'
               AND COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') NOT IN ('held', 'failure')
               AND json_extract(r.condition_json, '$.kind') = 'clear'
               AND NOT EXISTS (SELECT 1 FROM project p WHERE p.id = c.project_id AND p.paused_at IS NOT NULL)"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !resting.is_empty() {
            return Err(format!(
                "subtasks of a root without a lead rest in the step check: {resting:?}"
            ));
        }
        // (m) under a lead, a subtask in the gate has its check running or a
        // typed park.
        let unowned: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT c.id FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.deleted_at IS NULL AND c.status = 'lead_check'
               AND {LEAD}
               AND json_extract(c.condition_json, '$.kind') = 'clear'
               AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.task_id = c.id AND e.role = 'planner' AND e.status = 'running')"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !unowned.is_empty() {
            return Err(format!(
                "(m) subtasks in the step check under a lead with no check and no park: {unowned:?}"
            ));
        }
        Ok(())
    }

    /// The re-plan invariants of plan 3.10 stage 2.
    ///
    /// (l) in every committed state: only a coordination root with a lead
    /// ever spends `replan` or enters planning over the `replan` bridge;
    /// what is spent is within the limit, each charge has one receipt, no
    /// two planning entries share a receipt, and at most one charge is ahead
    /// of its entry (the crash window between the charge and the entry).
    /// (o) at quiescence: a root in aggregate review, merging or a terminal
    /// state has no subtask left to run, so a re-plan can add work only
    /// before the root returns to implementation.
    ///
    /// Both halves of (l), and (o), bind on the operation corpus: a real
    /// re-plan charges its receipt, and the lead creates, recovers and cancels
    /// subtasks before the root returns to implementation and aggregate review.
    /// Random sequences additionally assign and remove the lead.
    pub(super) async fn check_replans(&self, quiescent: bool) -> Result<(), String> {
        let pool = &self.live().pool;
        let rows = |sql: &'static str| async move {
            sqlx::query_scalar::<_, String>(sql)
                .fetch_all(pool)
                .await
                .map_err(|error| error.to_string())
        };
        let strangers = rows(
            "SELECT task_id || ' ' || kind || ' ' || receipt FROM model_replan_authority WHERE eligible=0"
        ).await?;
        if !strangers.is_empty() {
            return Err(format!(
                "(l) Tasks that are not lead-managed roots re-planned: {strangers:?}"
            ));
        }
        let uncharged = rows(
            "SELECT l.task_id || ' ' || l.id FROM transition_log l WHERE l.bridge_kind='replan'
             AND NOT EXISTS(SELECT 1 FROM task_budget_charge c WHERE c.task_id=l.task_id AND c.kind='replan'
                            AND c.step_id=json_extract(l.bridge_payload,'$.receipt'))"
        ).await?;
        if !uncharged.is_empty() {
            return Err(format!(
                "(l) re-plan entries lack their charge: {uncharged:?}"
            ));
        }
        let overspent = rows(
            "SELECT b.task_id || ' spent ' || b.spent FROM task_budget b WHERE b.kind = 'replan'
               AND (b.spent > 5
                    OR b.spent != (SELECT COUNT(*) FROM task_budget_charge c
                                   WHERE c.task_id = b.task_id AND c.kind = 'replan' AND c.window_id = b.window_id)
                    OR (SELECT COUNT(*) FROM task_budget_charge c
                        WHERE c.task_id = b.task_id AND c.kind = 'replan' AND c.window_id = b.window_id
                          AND NOT EXISTS (SELECT 1 FROM transition_log l WHERE l.task_id = b.task_id AND l.bridge_kind = 'replan'
                                          AND json_extract(l.bridge_payload, '$.receipt') = c.step_id)) > 1)",
        )
        .await?;
        if !overspent.is_empty() {
            return Err(format!(
                "(l) re-plan charges disagree with their receipts or entries: {overspent:?}"
            ));
        }
        let repeated = rows(
            "SELECT l.task_id || ' ' || json_extract(l.bridge_payload, '$.receipt') FROM transition_log l
             WHERE l.bridge_kind = 'replan' GROUP BY l.task_id, json_extract(l.bridge_payload, '$.receipt')
             HAVING COUNT(*) > 1",
        )
        .await?;
        if !repeated.is_empty() {
            return Err(format!(
                "(l) one re-plan trigger entered planning twice: {repeated:?}"
            ));
        }
        if !quiescent {
            return Ok(());
        }
        let open = rows(
            "SELECT r.id || ' in ' || r.status || ' with ' || c.id || ' in ' || c.status
             FROM task r JOIN task c ON c.parent_task_id = r.id
             WHERE r.parent_task_id IS NULL AND r.deleted_at IS NULL AND c.deleted_at IS NULL
               AND r.status IN ('review', 'merging', 'done', 'cancelled')
               AND c.status NOT IN ('done', 'cancelled')",
        )
        .await?;
        if !open.is_empty() {
            // What happened to the subtask left behind: its Task steps and
            // transitions, so a failing seed names the writer that skipped it.
            let history = rows(
                "SELECT c.id || ' created ' || c.created_at || ' root-cancelled ' || COALESCE((SELECT MAX(l.created_at) FROM transition_log l WHERE l.task_id=r.id AND l.to_state='cancelled'),'-')
                   || ' now ' || strftime('%Y-%m-%dT%H:%M:%f','now') || ' steps ' || COALESCE((SELECT group_concat(s.seq || ':' || s.kind || ':' || s.status || ':' || s.attempts || ':' || s.priority || ':' || COALESCE(s.lane,'') || ':avail=' || COALESCE(s.available_at,'') || ':lease=' || COALESCE(s.lease_until,'') || ':' || COALESCE(json_extract(s.payload_json,'$.operation'),'') || ':' || COALESCE(s.last_error,''), ' | ') FROM task_step s WHERE s.task_id=c.id),'none')
                   || ' log ' || COALESCE((SELECT group_concat(l.from_state || '->' || l.to_state || ' ' || l.trigger_reason, ' | ') FROM transition_log l WHERE l.task_id=c.id),'none')
                 FROM task r JOIN task c ON c.parent_task_id = r.id
                 WHERE r.parent_task_id IS NULL AND r.status IN ('review','merging','done','cancelled') AND c.status NOT IN ('done','cancelled')",
            )
            .await
            .unwrap_or_default();
            return Err(format!(
                "(o) roots past implementation with a subtask left to run: {open:?}\n  {}",
                history.join("\n  ")
            ));
        }
        Ok(())
    }

    /// The coordination-root invariants of plan 3.10 stage 1, for roots with
    /// and without a planner (the lead).
    ///
    /// (k) holds in every committed state. (g), (h), (i) and (j) are checked
    /// where nothing is left for the dispatcher to do: a root restored from
    /// an older release may hold a running subtask in `todo` until its first
    /// pass, and a stopping execution is still `running` until it reports.
    ///
    /// What is real and what is vacuous here. (g) is real: with the product
    /// mutated back to the old rule (a subtask of a `todo` root dispatches
    /// and the root is not kicked off) `a_coordination_root_merges_through_
    /// the_queue` fails on it. The no-lead halves of (h) and (k) are real for
    /// the same roots. (i), (j) and the lead halves of (h) and (k) bind where
    /// a root has a lead: `a_root_with_a_lead_plans_and_has_every_step_
    /// checked` (`Action::AssignLead`, the scripted lead) checks them on a
    /// root that rests in `planning` with its lead's run live. They also bind
    /// during the operation corpus's re-plan and in random sequences, which
    /// create coordination roots and assign/remove leads.
    pub(super) async fn check_roots(&self, quiescent: bool) -> Result<(), String> {
        let pool = &self.live().pool;
        let rows = |sql: &'static str| async move {
            sqlx::query_scalar::<_, String>(sql)
                .fetch_all(pool)
                .await
                .map_err(|error| error.to_string())
        };
        const ROOT: &str = "r.parent_task_id IS NULL AND r.deleted_at IS NULL AND EXISTS (SELECT 1 FROM task c WHERE c.parent_task_id = r.id AND c.deleted_at IS NULL)";
        const LEAD: &str = "EXISTS (SELECT 1 FROM task_role_assignment a WHERE a.task_id = r.id AND a.role_name = 'planner' AND a.assignee_type = 'agent' AND a.assignee_id IS NOT NULL)";
        // (k) a root without a planner never runs one.
        let ran: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT r.id FROM task r WHERE {ROOT} AND NOT {LEAD} AND EXISTS (SELECT 1 FROM execution e WHERE e.task_id = r.id AND e.role = 'planner' AND e.status = 'running')"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !ran.is_empty() {
            return Err(format!("(k) roots without a planner run one: {ran:?}"));
        }
        if !quiescent {
            return Ok(());
        }
        // (k) ... and never rests in `planning`.
        let resting: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT r.id FROM task r WHERE {ROOT} AND NOT {LEAD} AND r.status = 'planning'
               AND json_extract(r.condition_json, '$.kind') = 'clear'"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !resting.is_empty() {
            return Err(format!(
                "(k) roots without a planner rest in planning: {resting:?}"
            ));
        }
        // (g) no subtask runs while its root is not in an implementation state.
        let early = rows(
            "SELECT c.id || ' under ' || r.status FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.status NOT IN ('in_progress', 'merge_failed')
               AND EXISTS (SELECT 1 FROM execution e WHERE e.task_id = c.id AND e.status = 'running')",
        )
        .await?;
        if !early.is_empty() {
            return Err(format!(
                "(g) subtasks run while their root does not implement: {early:?}"
            ));
        }
        // (h) one running execution between a root and its subtasks: the
        // planner on the root counts.
        let crowded = rows(
            "SELECT r.id FROM task r WHERE r.parent_task_id IS NULL AND r.deleted_at IS NULL
               AND (SELECT COUNT(*) FROM execution e JOIN task t ON t.id = e.task_id
                    WHERE e.status = 'running' AND (t.id = r.id OR t.parent_task_id = r.id)
                      AND EXISTS (SELECT 1 FROM task c WHERE c.parent_task_id = r.id)) > 1",
        )
        .await?;
        if !crowded.is_empty() {
            return Err(format!(
                "(h) roots with more than one running execution in their tree: {crowded:?}"
            ));
        }
        // (i) a root that plans for its lead is never parked on its children.
        let stranded: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT r.id FROM task r WHERE {ROOT} AND {LEAD} AND r.status = 'planning'
               AND json_extract(r.condition_json, '$.primary.kind') = 'children'"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !stranded.is_empty() {
            return Err(format!(
                "(i) planning roots parked on their children: {stranded:?}"
            ));
        }
        // (j) a subtask of a planning root is held by a typed park.
        let unheld = rows(
            "SELECT c.id || ' ' || COALESCE(c.condition_json, '') FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.deleted_at IS NULL AND r.status = 'planning'
               AND c.status IN ('todo', 'in_progress')
               AND NOT (COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') = 'parent'
                        AND json_extract(c.condition_json, '$.primary.cause') IN ('planning', 'held', 'blocked'))
               AND COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') NOT IN ('held', 'failure', 'budget_exhausted', 'agent_timeout')
               AND NOT (EXISTS (SELECT 1 FROM project p WHERE p.id = c.project_id AND p.paused_at IS NOT NULL)
                        AND COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') = 'project_paused')",
        )
        .await?;
        if !unheld.is_empty() {
            return Err(format!(
                "(j) subtasks of a planning root without a parent park: {unheld:?}"
            ));
        }
        Ok(())
    }

    /// The invariants of a quiescent state. Returns one line per Task.
    pub(super) async fn check_quiescent(&mut self) -> Result<String, String> {
        self.check_committed().await?;
        self.check_roots(true).await?;
        self.check_replans(true).await?;
        self.check_step_checks(true).await?;
        // (a) the dispatcher's own "owned or parked" assertion.
        bounded(
            &self.probe,
            "the dispatcher's sweep and assertion",
            self.live().runtime.task_dispatcher.sweep_and_assert(),
        )
        .await
        .map_err(|error| format!("(a) {error}"))?;
        let mut line = String::new();
        for index in 0..self.tasks.len() {
            let task = self.task(index).await?;
            let status = task["status"].as_str().unwrap_or_default().to_owned();
            let kind = task["condition"]["kind"].as_str().unwrap_or("?").to_owned();
            let reason = ["primary", "failure", "reason"]
                .iter()
                .find_map(|key| task["condition"][key]["kind"].as_str())
                .unwrap_or("-")
                .to_owned();
            let details = &task["condition"]["details"];
            let cause = details["diagnostic"]["blocking_reason"]
                .as_str()
                .or(details["failure_kind"].as_str())
                .map(|cause| format!("({cause})"))
                .unwrap_or_default();
            line.push_str(&format!("{index}:{status}/{kind}/{reason}{cause} "));
            if TERMINAL.contains(&status.as_str()) {
                if kind != "settled" {
                    return Err(format!(
                        "(e) terminal task {index} ({status}) has condition {}",
                        task["condition"]
                    ));
                }
                continue;
            }
            let (_, offers) = self.offers(index).await?;
            let verbs: Vec<&str> = offers
                .iter()
                .filter_map(|offer| offer["action"]["verb"].as_str())
                .collect();
            self.check_exit(index, &task, &status, &kind, &reason, &verbs)
                .await?;
        }
        Ok(line)
    }

    /// Invariants (a) and (c): a Task that is not terminal has a live owner,
    /// or waits for something that exists and can clear the wait.
    pub(super) async fn check_exit(
        &self,
        index: usize,
        task: &Value,
        status: &str,
        kind: &str,
        reason: &str,
        verbs: &[&str],
    ) -> Result<(), String> {
        let offered = |wanted: &[&str]| wanted.iter().any(|verb| verbs.contains(verb));
        let wedge = |why: &str| {
            Err(format!(
                "(c) task {index} in `{status}` is {kind}/{reason} and {why}; offers {verbs:?}; condition {}",
                task["condition"]
            ))
        };
        if self.waiting(index) {
            return Ok(()); // a live coder run: the model can finish it.
        }
        let unsettled = |ids: Vec<String>| async move {
            let mut open = false;
            for id in ids {
                let status: Option<String> =
                    sqlx::query_scalar("SELECT status FROM task WHERE id = ?")
                        .bind(id)
                        .fetch_optional(&self.live().pool)
                        .await
                        .map_err(|error| error.to_string())?;
                open |= status.is_some_and(|status| !TERMINAL.contains(&status.as_str()));
            }
            Ok::<bool, String>(open)
        };
        if task["condition"]["details"]["diagnostic"]["blocking_reason"] == "dependency_cancelled" {
            // By design this park offers cancellation only; its other exit
            // is removing the cancelled dependency, which the settlement
            // drive takes and which must then let the Task continue.
            return if offered(&["cancel"]) {
                Ok(())
            } else {
                wedge("`cancel` is not offered for a cancelled dependency")
            };
        }
        match (kind, reason) {
            ("clear" | "entering" | "running" | "deferred", _) => {
                // Quiescence: nothing is queued for this Task and it has no
                // live execution. It may only be waiting for something that
                // exists: capacity another run holds, an open child, a pause
                // the owner can lift, or (a backoff) the retry it offers.
                let children: Vec<String> =
                    sqlx::query_scalar("SELECT id FROM task WHERE parent_task_id = ?")
                        .bind(&self.tasks[index])
                        .fetch_all(&self.live().pool)
                        .await
                        .map_err(|error| error.to_string())?;
                // Another Task's run excuses only a Task still waiting for
                // its first slot. A started Task with no run of its own must
                // show why (a deferral, a park), whatever the others do.
                let capacity_held = INITIAL.contains(&status)
                    && (0..self.tasks.len()).any(|other| self.waiting(other));
                if capacity_held
                    || self.project_paused
                    || self.agent_paused
                    || unsettled(children).await?
                    || kind == "deferred"
                // a deadline `AdvanceClock` lets pass.
                {
                    return Ok(());
                }
                wedge("has no queued step, no live execution, no park and nothing it could be waiting for")
            }
            // Project pause owns no Task action: its exit is Resume Project.
            // Require the live Project fact, so a stale pause park is a wedge.
            (_, "project_paused") => {
                let paused: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM project WHERE id=? AND paused_at IS NOT NULL)",
                )
                .bind(&self.project_id)
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if paused {
                    Ok(())
                } else {
                    wedge("the Project it waits for is not paused")
                }
            }
            (_, "held") => {
                if offered(&["release"]) {
                    Ok(())
                } else {
                    wedge("`release` is not offered")
                }
            }
            (_, "dependencies") => {
                let dependencies: Vec<String> = sqlx::query_scalar(
                    "SELECT depends_on_id FROM task_dependency WHERE task_id = ?",
                )
                .bind(&self.tasks[index])
                .fetch_all(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if unsettled(dependencies).await? || offered(&["cancel", "retry", "start"]) {
                    Ok(())
                } else {
                    wedge("every dependency is settled and nothing is offered")
                }
            }
            (_, "children") => {
                let children: Vec<String> =
                    sqlx::query_scalar("SELECT id FROM task WHERE parent_task_id = ?")
                        .bind(&self.tasks[index])
                        .fetch_all(&self.live().pool)
                        .await
                        .map_err(|error| error.to_string())?;
                if unsettled(children).await? || offered(&["retry", "restart", "approve"]) {
                    Ok(())
                } else {
                    wedge("every child is settled and nothing is offered")
                }
            }
            // A subtask its parent does not let run: the exit is on the
            // parent, which must exist, be open and (when it is held) offer
            // its release.
            (_, "parent") => {
                let parent = task["condition"]["primary"]["parent_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                let cause = task["condition"]["primary"]["cause"]
                    .as_str()
                    .unwrap_or_default();
                let Some(parent_index) = self.tasks.iter().position(|id| *id == parent) else {
                    return wedge("the parent it names is not a Task of this Project");
                };
                if !unsettled(vec![parent]).await? {
                    return wedge("the parent it waits for is settled");
                }
                if cause == "held" {
                    let (_, offers) = self.offers(parent_index).await?;
                    if !offers
                        .iter()
                        .any(|offer| offer["action"]["verb"] == "release")
                    {
                        return wedge("its held parent offers no release");
                    }
                }
                if verbs.contains(&"start") {
                    return wedge("`start` is offered although the parent does not let it run");
                }
                Ok(())
            }
            // The Agent that would run the Task is paused or unreachable:
            // the exit is on the Agent, which the model can resume.
            (_, "agent") => {
                let agent = task["condition"]["primary"]["agent_id"]
                    .as_str()
                    .unwrap_or_default();
                if agent != self.coder_id && agent != self.reviewer_id {
                    return wedge("the Agent it names is not one of this Project's");
                }
                if self.agent_paused || self.project_paused {
                    Ok(())
                } else {
                    wedge("the Agent it waits for is not paused")
                }
            }
            (_, "capacity") => {
                let mut busy = false;
                for other in 0..self.tasks.len() {
                    busy |= other != index && self.waiting(other);
                }
                if busy {
                    Ok(())
                } else {
                    wedge("no execution holds the capacity it waits for")
                }
            }
            // Review-entry CI queued behind a full machine: the exit is a run
            // ending, which frees the slot the check worker then gives it.
            (_, "check") if task["condition"]["primary"]["wait"]["phase"] == "slot" => {
                if (0..self.tasks.len()).any(|other| other != index && self.waiting(other)) {
                    Ok(())
                } else {
                    wedge("no execution holds the slot its check waits for")
                }
            }
            // In the merge queue (`waiting`, `owned`, `applied`): the queue
            // worker owns the Task and no owner action is needed, so none
            // being offered is not a wedge. It is one when nothing can move
            // the queue: its head check waits for a machine slot no run
            // holds, or the queue has no live head and cannot be claimed.
            (_, "integration")
                if matches!(
                    task["condition"]["primary"]["reason"]["kind"].as_str(),
                    Some("waiting" | "owned" | "applied")
                ) =>
            {
                let slot_wait = task["condition"]["additional"]
                    .as_array()
                    .is_some_and(|reasons| {
                        reasons
                            .iter()
                            .any(|r| r["kind"] == "check" && r["wait"]["phase"] == "slot")
                    });
                if slot_wait {
                    return if (0..self.tasks.len())
                        .any(|other| other != index && self.waiting(other))
                    {
                        Ok(())
                    } else {
                        wedge("no execution holds the slot its head check waits for")
                    };
                }
                let moving: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM integration_attempt a JOIN integration_queue q ON q.id=a.queue_id \
                     WHERE a.task_ref = ? AND a.current = 1 \
                       AND ((q.head_attempt_id IS NOT NULL AND julianday(q.lease_until) > julianday('now')) \
                         OR (q.head_attempt_id IS NULL AND q.state = 'open' AND a.state = 'queued')))",
                )
                .bind(&self.tasks[index])
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                // A paused Project stops the queue for its Tasks; the exit
                // is the resume, which the model can do.
                if moving || self.project_paused {
                    Ok(())
                } else {
                    wedge("the merge queue that owns it has no live head and cannot be claimed")
                }
            }
            // An accepted action refused admission by an unfinished
            // dependency waits for that Task; `cancel` stays on offer.
            (_, "dispatch_refusal")
                if unsettled(
                    sqlx::query_scalar(
                        "SELECT depends_on_id FROM task_dependency WHERE task_id = ?",
                    )
                    .bind(&self.tasks[index])
                    .fetch_all(&self.live().pool)
                    .await
                    .map_err(|error| error.to_string())?,
                )
                .await?
                    && offered(&["cancel"]) =>
            {
                Ok(())
            }
            // Deadline parks: the scheduler re-reads them at a wall-clock
            // deadline the model does not advance. Owner action is the exit
            // the model can check.
            (
                _,
                "failure" | "agent_timeout" | "budget_exhausted" | "entry_blocked"
                | "human_decision" | "human_work" | "execution_stopped" | "review_checks"
                | "dispatch_refusal" | "integration" | "retry_deadline" | "review_grace",
            ) => {
                if offered(&[
                    "retry",
                    "restart",
                    "approve",
                    "send_back",
                    "start",
                    "release",
                ]) {
                    Ok(())
                } else {
                    wedge("no recovery action is offered")
                }
            }
            _ => wedge("the model knows no exit for this reason"),
        }
    }

    /// Liveness, end to end. Every pause is lifted, every run succeeds and
    /// every review passes; a Task parked for its owner gets the way forward
    /// the API offers, a Task with a cancelled dependency loses it, and
    /// everything else must move by itself. Every Task must then settle.
    pub(super) async fn drive_to_settlement(&mut self) -> Result<(), String> {
        self.shared.verdicts.lock().unwrap().clear();
        if self.project_paused {
            self.apply(&Action::ResumeProject).await?;
        }
        if self.agent_paused {
            self.apply(&Action::ResumeAgent).await?;
        }
        self.quiesce().await?;
        let mut last = self.check_quiescent().await?;
        self.trace.push(format!("pauses lifted: {last}"));
        let mut unchanged = 0;
        let mut attempts: HashMap<usize, usize> = HashMap::new();
        let mut reopened: std::collections::HashSet<usize> = Default::default();
        for round in 0..SETTLE_ROUNDS {
            let mut open = 0;
            let mut moves = 0;
            let mut taken = Vec::new();
            let mut deferred = false;
            for index in 0..self.tasks.len() {
                let task = self.task(index).await?;
                let status = task["status"].as_str().unwrap_or_default().to_owned();
                if TERMINAL.contains(&status.as_str()) {
                    continue;
                }
                open += 1;
                if self.waiting(index) {
                    self.apply(&Action::Finish(index, Outcome::Success)).await?;
                    moves += 1;
                    continue;
                }
                let cancelled = task["condition"]["details"]["diagnostic"]["hook"]
                    ["cancelled_dependency_ids"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                if !cancelled.is_empty() {
                    for dependency in cancelled {
                        let (status, body) = self
                            .request(
                                Method::DELETE,
                                &format!(
                                    "/api/v1/tasks/{}/dependencies/{}",
                                    self.tasks[index],
                                    dependency.as_str().unwrap_or_default()
                                ),
                                None,
                            )
                            .await?;
                        if !status.is_success() {
                            return Err(format!(
                                "(c) removing the cancelled dependency of task {index} was refused: {status} {body}"
                            ));
                        }
                    }
                    moves += 1;
                    continue;
                }
                let kind = task["condition"]["kind"].as_str().unwrap_or_default();
                let reason = ["primary", "failure"]
                    .iter()
                    .find_map(|key| task["condition"][key]["kind"].as_str())
                    .unwrap_or_default();
                if kind == "deferred" {
                    deferred = true; // waits for a deadline, not for the owner.
                    continue;
                }
                // These wait for other Tasks, not for the owner.
                if !matches!(kind, "parked" | "failed")
                    || matches!(
                        reason,
                        "dependencies" | "children" | "capacity" | "parent" | "agent"
                    )
                    || (reason == "check"
                        && task["condition"]["primary"]["wait"]["phase"] == "slot")
                {
                    continue;
                }
                // The owner's ways forward, least intrusive first. When one
                // was accepted and changed nothing, the next round takes the
                // next one: a park is a wedge only when none of them helps.
                let (version, offers) = self.offers(index).await?;
                // Reopening a parent for corrective work is taken once per
                // parent: it changes state every time (the parent leaves
                // review, held, and comes back), so the rotation below would
                // never get past it to the owner's override.
                let forward: Vec<Value> = ["release", "retry", "send_back", "restart", "approve"]
                    .iter()
                    .filter_map(|verb| offers.iter().find(|offer| offer["action"]["verb"] == *verb))
                    .filter(|offer| {
                        offer["reason"] != "root_review_reopen" || !reopened.contains(&index)
                    })
                    // Corrective work in this harness is supplied through
                    // subtasks. Prefer that offered route to claiming that a
                    // manual edit of the root's checkout has been completed.
                    .cloned()
                    .collect();
                if !forward.is_empty() {
                    let turn = attempts.entry(index).or_insert(0);
                    let offer = &forward[*turn % forward.len()];
                    *turn += 1;
                    let verb = self.apply_offer(index, version, offer).await?;
                    taken.push(format!("{verb}({index})"));
                    moves += 1;
                    if offer["reason"] == "root_review_reopen" {
                        // What the reopening is for: the parent must now
                        // accept a corrective subtask.
                        reopened.insert(index);
                        let created = self.create(Some(index)).await?;
                        if created.starts_with("refused") {
                            return Err(format!(
                                "(b) task {index} was sent back for corrective work and refuses a subtask: {created}"
                            ));
                        }
                        taken.push(format!("corrective child of {index}"));
                    }
                }
            }
            if open == 0 {
                self.trace.push(format!("settled: {last}"));
                return Ok(());
            }
            if deferred && self.move_deadlines(true).await? > 0 {
                taken.push("clock".to_owned());
                moves += 1;
            }
            if moves == 0 {
                return Err(format!(
                    "(c) wedge: with every pause lifted, {open} Task(s) stay unsettled and nothing can move them: no run is live, no owner action is offered for a park, and the scheduler does nothing: {last}"
                ));
            }
            self.quiesce().await?;
            let state = self.check_quiescent().await?;
            self.trace
                .push(format!("settle {round} {taken:?}: {state}"));
            // Invariant (b), second half: an accepted offer changes state.
            if state == last {
                unchanged += 1;
            } else {
                unchanged = 0;
                attempts.clear();
            }
            last = state;
            // Twice through every way forward an owner can be offered.
            if unchanged >= 10 {
                let mut conditions = String::new();
                for index in 0..self.tasks.len() {
                    let task = self.task(index).await?;
                    if !TERMINAL.contains(&task["status"].as_str().unwrap_or_default()) {
                        conditions.push_str(&format!("\n  task {index}: {}", task["condition"]));
                    }
                }
                return Err(format!(
                    "(b) wedge: every offered way forward is accepted and changes nothing, round after round (last taken: {taken:?}): {last}{conditions}"
                ));
            }
        }
        Err(format!(
            "(d) no settlement after {SETTLE_ROUNDS} rounds of successful runs, passing reviews and offered recoveries: {last}"
        ))
    }
}
