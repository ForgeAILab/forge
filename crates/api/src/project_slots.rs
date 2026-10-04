//! API-only, bounded slot memo. Its key comes from the already-loaded Project
//! row; a hit issues no SQL. Admission never uses this module.

use std::{collections::HashMap, sync::Mutex};

use api_types::ProjectSlots;
use db::ProjectSlotRead;
use services::task_dispatcher::slots::load_projects_slots;

const MAX_PROJECTS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Key {
    list_revision: i64,
    project_version: i64,
}

impl From<&ProjectSlotRead> for Key {
    fn from(project: &ProjectSlotRead) -> Self {
        Self {
            list_revision: project.list_revision,
            project_version: project.project.version,
        }
    }
}

#[derive(Default)]
pub struct ProjectSlotsMemo {
    entries: Mutex<HashMap<String, (Key, ProjectSlots)>>,
}

impl ProjectSlotsMemo {
    pub async fn load(
        &self,
        db: &db::SqliteDb,
        projects: &[ProjectSlotRead],
    ) -> services::Result<HashMap<String, ProjectSlots>> {
        let mut result = HashMap::with_capacity(projects.len());
        let mut missing = Vec::new();
        {
            let entries = self.entries.lock().expect("slot memo lock");
            for project in projects {
                match entries.get(&project.project.id) {
                    Some((key, slots)) if *key == Key::from(project) => {
                        result.insert(project.project.id.clone(), slots.clone());
                    }
                    _ => missing.push(project.clone()),
                }
            }
        }
        if missing.is_empty() {
            return Ok(result);
        }
        let inputs: Vec<_> = missing.iter().map(|read| read.project.clone()).collect();
        let mut reads = load_projects_slots(db, &inputs).await?;
        let mut entries = self.entries.lock().expect("slot memo lock");
        for project in missing {
            let read = reads
                .remove(&project.project.id)
                .expect("requested Project");
            let key = Key::from(&project);
            // A write between the Project SELECT and the aggregate must not
            // associate newer counts with an older key. Overlapping requests
            // can replace entries, but every hit still checks the exact key.
            if read
                .revision
                .is_none_or(|revision| revision == (key.list_revision, key.project_version))
            {
                if entries.len() == MAX_PROJECTS && !entries.contains_key(&project.project.id) {
                    let evicted = entries.keys().next().cloned().expect("full memo");
                    entries.remove(&evicted);
                }
                entries.insert(project.project.id.clone(), (key, read.slots.clone()));
            }
            result.insert(project.project.id, read.slots);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{ProjectSettings, StateKind};
    use services::task_dispatcher::slots::load_project_slots;

    async fn fixture() -> (db::SqliteDb, ProjectSlotsMemo) {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = db::SqliteDb::new(pool);
        sqlx::query("INSERT INTO project (id, name, created_at, updated_at) VALUES ('p', 'Slots', 'now', 'now'), ('other', 'Other', 'now', 'now')")
            .execute(db.pool()).await.unwrap();
        (db, ProjectSlotsMemo::default())
    }

    async fn project(db: &db::SqliteDb, id: &str) -> ProjectSlotRead {
        db.get_visible_project_slot_read(id, "reader")
            .await
            .unwrap()
            .unwrap()
    }

    async fn check(
        db: &db::SqliteDb,
        memo: &ProjectSlotsMemo,
        id: &str,
        expected: (u32, u32, u32, u32),
    ) {
        let project = project(db, id).await;
        let before = memo.entries.lock().unwrap().get(id).map(|(key, _)| *key);
        if let Some(before) = before {
            assert_ne!(before, Key::from(&project), "mutation must move the key");
        }
        let slots = memo
            .load(db, std::slice::from_ref(&project))
            .await
            .unwrap()
            .remove(id)
            .unwrap();
        assert_eq!(
            (slots.limit, slots.active, slots.parked, slots.queued),
            expected
        );
        assert_eq!(
            slots,
            load_project_slots(db, &project.project).await.unwrap()
        );
        // A database with no schema proves the unchanged hit executes no SQL.
        let no_tables = db::SqliteDb::new(db::create_sqlite_pool("sqlite::memory:").await.unwrap());
        assert_eq!(memo.load(&no_tables, &[project]).await.unwrap()[id], slots);
    }

    async fn mutate(db: &db::SqliteDb, sql: &str) {
        sqlx::query(sql).execute(db.pool()).await.unwrap();
    }

    #[tokio::test]
    async fn machine_capacity_wait_invalidates_project_slots_memo() {
        let (db, memo) = fixture().await;
        mutate(&db, "INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES ('wait', 'p', 'Wait', 'in_progress', 'now', 'now')").await;
        check(&db, &memo, "p", (5, 1, 0, 0)).await;
        mutate(&db, "UPDATE task SET metadata_json = json_object('dispatch_disposition', json_object('capability', 'machine_capacity', 'task_version', version)) WHERE id = 'wait'").await;
        check(&db, &memo, "p", (5, 0, 1, 0)).await;
        mutate(&db, "UPDATE task SET metadata_json = json_remove(metadata_json, '$.dispatch_disposition') WHERE id = 'wait'").await;
        check(&db, &memo, "p", (5, 1, 0, 0)).await;
    }

    #[tokio::test]
    async fn memo_tracks_task_hierarchy_visibility_and_holds() {
        let (db, memo) = fixture().await;
        check(&db, &memo, "p", (5, 0, 0, 0)).await;
        let mutations = [
            ("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES ('root', 'p', 'Root', 'todo', 'now', 'now')", (5, 0, 0, 1)),
            ("UPDATE task SET status = 'in_progress' WHERE id = 'root'", (5, 1, 0, 0)),
            ("INSERT INTO task (id, project_id, parent_task_id, title, status, created_at, updated_at) VALUES ('child', 'p', 'root', 'Child', 'todo', 'now', 'now')", (5, 0, 0, 1)),
            ("UPDATE task SET status = 'in_progress' WHERE id = 'child'", (5, 1, 0, 0)),
            ("UPDATE task SET blocked_json = '{}' WHERE id = 'child'", (5, 0, 1, 0)),
            ("UPDATE task SET blocked_json = NULL, failed_json = '{}' WHERE id = 'child'", (5, 0, 1, 0)),
            ("UPDATE task SET failed_json = NULL, error_annotation = '{\"type\":\"review_needs_owner\"}' WHERE id = 'child'", (5, 0, 1, 0)),
            ("UPDATE task SET error_annotation = '{\"type\":\"merge_conflict\"}' WHERE id = 'child'", (5, 1, 0, 0)),
            ("UPDATE task SET parent_task_id = NULL WHERE id = 'child'", (5, 2, 0, 0)),
            ("UPDATE task SET archived_at = 'now' WHERE id = 'child'", (5, 1, 0, 0)),
            ("UPDATE task SET archived_at = NULL WHERE id = 'child'", (5, 2, 0, 0)),
            ("UPDATE task SET deleted_at = 'now' WHERE id = 'child'", (5, 1, 0, 0)),
            ("UPDATE task SET deleted_at = NULL WHERE id = 'child'", (5, 2, 0, 0)),
            ("UPDATE task SET project_id = 'other' WHERE id = 'child'", (5, 1, 0, 0)),
            ("UPDATE task SET project_id = 'p' WHERE id = 'child'", (5, 2, 0, 0)),
            ("DELETE FROM task WHERE id = 'child'", (5, 1, 0, 0)),
        ];
        for (sql, expected) in mutations {
            mutate(&db, sql).await;
            check(&db, &memo, "p", expected).await;
        }
    }

    #[tokio::test]
    async fn memo_tracks_reviews_execution_capacity_and_workflow_semantics() {
        let (db, memo) = fixture().await;
        mutate(&db, "INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES ('root', 'p', 'Root', 'in_progress', 'now', 'now')").await;
        mutate(&db, "INSERT INTO task (id, project_id, parent_task_id, title, status, created_at, updated_at) VALUES ('child', 'p', 'root', 'Child', 'done', 'now', 'now')").await;
        check(&db, &memo, "p", (5, 0, 0, 0)).await;
        let mutations = [
            ("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES ('e', 'root', 'coder', 'running', 'now', 'now')", (5, 1, 0, 0)),
            ("UPDATE execution SET status = 'completed' WHERE id = 'e'", (5, 0, 0, 0)),
            ("INSERT INTO review (id, task_id, execution_id, attempt_number, status, started_at, created_at, updated_at) VALUES ('r', 'root', 'e', 1, 'awaiting_human', 'now', 'now', 'now')", (5, 0, 1, 0)),
            ("UPDATE review SET status = 'passed' WHERE id = 'r'", (5, 0, 0, 0)),
            ("INSERT INTO review (id, task_id, execution_id, attempt_number, status, started_at, created_at, updated_at) VALUES ('r2', 'root', 'e', 2, 'awaiting_human', 'now', 'now', 'now')", (5, 0, 1, 0)),
            ("UPDATE review SET attempt_number = 3 WHERE id = 'r'", (5, 0, 0, 0)),
            ("UPDATE review SET attempt_number = 4, created_at = 'zzz' WHERE id = 'r'", (5, 0, 0, 0)),
            ("DELETE FROM review WHERE id = 'r'", (5, 0, 1, 0)),
            ("DELETE FROM review WHERE id = 'r2'", (5, 0, 0, 0)),
            ("DELETE FROM execution WHERE id = 'e'", (5, 0, 0, 0)),
            ("UPDATE project SET settings = '{\"max_active_tasks\":2}', version = version + 1 WHERE id = 'p'", (2, 0, 0, 0)),
        ];
        for (sql, expected) in mutations {
            mutate(&db, sql).await;
            check(&db, &memo, "p", expected).await;
        }
        let mut workflow = services::workflow::default_workflow::default_workflow();
        for (role, phase, merge, kind, active) in [
            (
                Some("reviewer"),
                api_types::CanonicalPhase::Working,
                false,
                StateKind::Active,
                1,
            ),
            (
                Some("coder"),
                api_types::CanonicalPhase::Review,
                false,
                StateKind::Active,
                1,
            ),
            (
                Some("coder"),
                api_types::CanonicalPhase::Working,
                false,
                StateKind::Active,
                0,
            ),
            (
                Some("coder"),
                api_types::CanonicalPhase::Working,
                true,
                StateKind::Active,
                1,
            ),
            (
                Some("coder"),
                api_types::CanonicalPhase::Working,
                false,
                StateKind::Custom,
                0,
            ),
        ] {
            let state = workflow
                .states
                .iter_mut()
                .find(|s| s.name == "in_progress")
                .unwrap();
            state.role = role.map(str::to_owned);
            state.canonical_phase = Some(phase);
            state.kind = kind;
            state.hooks.on_enter.clear();
            if merge {
                state.hooks.on_enter.push(
                    serde_json::from_value(serde_json::json!({"action":"run_merge"})).unwrap(),
                );
            }
            sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = 'p'")
                .bind(serde_json::to_string(&workflow).unwrap())
                .execute(db.pool())
                .await
                .unwrap();
            check(&db, &memo, "p", (2, active, 0, 0)).await;
        }
        // An inherited subtask state remains Active when the root's kind is Custom.
        mutate(
            &db,
            "UPDATE task SET status = 'in_progress' WHERE id = 'child'",
        )
        .await;
        check(&db, &memo, "p", (2, 1, 0, 0)).await;
    }

    #[tokio::test]
    async fn memo_tracks_cross_project_child_existence_and_reparenting() {
        let (db, memo) = fixture().await;
        mutate(&db, "INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES ('root', 'p', 'Root', 'in_progress', 'now', 'now')").await;
        check(&db, &memo, "p", (5, 1, 0, 0)).await;
        for (sql, active) in [
            ("INSERT INTO task (id, project_id, parent_task_id, title, status, created_at, updated_at) VALUES ('child', 'other', 'root', 'Child', 'done', 'now', 'now')", 0),
            ("UPDATE task SET deleted_at = 'now' WHERE id = 'child'", 1),
            ("UPDATE task SET deleted_at = NULL WHERE id = 'child'", 0),
            ("UPDATE task SET parent_task_id = NULL WHERE id = 'child'", 1),
            ("UPDATE task SET parent_task_id = 'root' WHERE id = 'child'", 0),
            ("DELETE FROM task WHERE id = 'child'", 1),
        ] {
            mutate(&db, sql).await;
            check(&db, &memo, "p", (5, active, 0, 0)).await;
        }
    }

    #[tokio::test]
    async fn memo_rejects_raced_revisions_and_is_bounded() {
        let (db, memo) = fixture().await;
        let old = project(&db, "p").await;
        mutate(&db, "INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES ('root', 'p', 'Root', 'in_progress', 'now', 'now')").await;
        assert_eq!(memo.load(&db, &[old]).await.unwrap()["p"].active, 1);
        assert!(
            memo.entries.lock().unwrap().is_empty(),
            "raced counts must not be stored under the old key"
        );
        let old = project(&db, "p").await;
        mutate(&db, "UPDATE project SET settings = '{\"max_active_tasks\":2}', version = version + 1 WHERE id = 'p'").await;
        memo.load(&db, &[old]).await.unwrap();
        assert!(
            memo.entries.lock().unwrap().is_empty(),
            "raced settings must not be memoized"
        );
        let mut unlimited = project(&db, "p").await;
        unlimited.project.settings = serde_json::to_string(&ProjectSettings {
            max_active_tasks: 0,
            ..ProjectSettings::default()
        })
        .unwrap();
        for i in 0..MAX_PROJECTS + 10 {
            unlimited.project.id = format!("unlimited-{i}");
            memo.load(&db, &[unlimited.clone()]).await.unwrap();
        }
        assert_eq!(memo.entries.lock().unwrap().len(), MAX_PROJECTS);
    }
}
