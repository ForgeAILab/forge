//! Read-only projections of the queue for the public surfaces (REST, MCP,
//! `forge-ctl`, the SSE projector). Plain SELECTs on the caller's executor:
//! nothing here opens a write transaction or changes a row.
use super::{
    IntegrationAttemptState, IntegrationFailureKind, IntegrationPhaseTimings, IntegrationQueueState,
};
use crate::{DbError, Result, SqliteDb};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite};

/// An attempt "in progress". A finished attempt (`completed`, `cancelled`,
/// `superseded`) is never current: the table's CHECK says so.
const LIVE: &str = "a.current=1";
/// Members ahead of `a`: 0 for the head, the head (if any) plus the earlier
/// waiters for a member in line, NULL for one that is not in line. A queued
/// member carrying a cancel request is never claimed, so it is not counted.
const AHEAD: &str = "CASE WHEN q.head_attempt_id=a.id THEN 0 WHEN a.state IN ('queued','path_wait') THEN (q.head_attempt_id IS NOT NULL) + (SELECT COUNT(*) FROM integration_attempt b WHERE b.queue_id=a.queue_id AND b.current=1 AND b.state IN ('queued','path_wait') AND b.cancel_requested_at IS NULL AND b.queue_seq<a.queue_seq AND b.id IS NOT q.head_attempt_id) END";
const MEMBER_COLUMNS: &str = "a.id AS attempt_id,a.queue_id,a.task_ref,a.project_ref,a.queue_seq,a.attempt_number,a.state,a.failure_kind,a.failure_message,a.available_at,a.enqueued_at,a.started_at,a.updated_at,a.cancel_requested_at,a.phase_timings_json,a.revision,q.repo_id,q.target_branch,q.state AS queue_state,q.head_attempt_id,q.last_error_kind AS queue_error_kind,q.last_error AS queue_error,d.id AS machine_id,d.hostname AS machine_name,p.paused_at IS NOT NULL AS project_paused";
/// The machine a wait can be on: the owner of the queue's target checkout,
/// else the daemon that holds the Task's own checkout. The member's own
/// Project says whether it is paused: the row carries it, so no reader asks
/// for the Project a second time.
const MEMBER_JOINS: &str = "LEFT JOIN integration_queue q ON q.id=a.queue_id LEFT JOIN daemon d ON d.id=COALESCE(json_extract(q.target_owner_json,'$.daemon_id'),a.daemon_id) LEFT JOIN project p ON p.id=a.project_ref";

/// Members a queue read returns per queue.
pub const INTEGRATION_QUEUE_MEMBER_LIMIT: u32 = 100;

/// One attempt in progress with what a reader needs of its queue. No
/// receipts, fences or path sets are read.
#[derive(Debug, Clone, PartialEq)]
pub struct IntegrationMemberRead {
    pub attempt_id: String,
    pub queue_id: Option<String>,
    pub task_ref: String,
    pub project_ref: String,
    pub queue_seq: i64,
    pub attempt_number: i64,
    pub state: IntegrationAttemptState,
    pub failure_kind: Option<IntegrationFailureKind>,
    pub failure_message: Option<String>,
    pub available_at: Option<String>,
    pub enqueued_at: String,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub cancel_requested: bool,
    pub timings: Option<IntegrationPhaseTimings>,
    pub revision: i64,
    pub repo_id: Option<String>,
    pub target_branch: Option<String>,
    pub queue_state: Option<IntegrationQueueState>,
    pub is_head: bool,
    pub queue_error_kind: Option<IntegrationFailureKind>,
    pub queue_error: Option<String>,
    pub machine_id: Option<String>,
    pub machine_name: Option<String>,
    /// The member's own Project is paused.
    pub project_paused: bool,
    /// Members ahead; `None` when the attempt is not in line.
    pub ahead: Option<u32>,
    /// Members in progress in the same queue (set by the queue read only).
    pub queue_members: u32,
}

fn member(row: sqlx::sqlite::SqliteRow) -> Result<IntegrationMemberRead> {
    let attempt_id: String = row.try_get("attempt_id")?;
    let head: Option<String> = row.try_get("head_attempt_id")?;
    Ok(IntegrationMemberRead {
        is_head: head.as_deref() == Some(attempt_id.as_str()),
        attempt_id,
        queue_id: row.try_get("queue_id")?,
        task_ref: row.try_get("task_ref")?,
        project_ref: row.try_get("project_ref")?,
        queue_seq: row.try_get("queue_seq")?,
        attempt_number: row.try_get("attempt_number")?,
        state: row.try_get::<String, _>("state")?.parse()?,
        failure_kind: row
            .try_get::<Option<String>, _>("failure_kind")?
            .map(|kind| kind.parse())
            .transpose()?,
        failure_message: row.try_get("failure_message")?,
        available_at: row.try_get("available_at")?,
        enqueued_at: row.try_get("enqueued_at")?,
        started_at: row.try_get("started_at")?,
        updated_at: row.try_get("updated_at")?,
        cancel_requested: row
            .try_get::<Option<String>, _>("cancel_requested_at")?
            .is_some(),
        // A document this build cannot read is no timings, never a failed read.
        timings: row
            .try_get::<Option<String>, _>("phase_timings_json")?
            .and_then(|raw| serde_json::from_str(&raw).ok()),
        revision: row.try_get("revision")?,
        repo_id: row.try_get("repo_id")?,
        target_branch: row.try_get("target_branch")?,
        queue_state: row
            .try_get::<Option<String>, _>("queue_state")?
            .map(|state| state.parse())
            .transpose()?,
        queue_error_kind: row
            .try_get::<Option<String>, _>("queue_error_kind")?
            .map(|kind| kind.parse())
            .transpose()?,
        queue_error: row.try_get("queue_error")?,
        machine_id: row.try_get("machine_id")?,
        machine_name: row.try_get("machine_name")?,
        project_paused: row
            .try_get::<Option<bool>, _>("project_paused")?
            .unwrap_or(false),
        ahead: row
            .try_get::<Option<i64>, _>("ahead")?
            .and_then(|ahead| u32::try_from(ahead).ok()),
        queue_members: row
            .try_get::<Option<i64>, _>("queue_members")
            .ok()
            .flatten()
            .and_then(|count| u32::try_from(count).ok())
            .unwrap_or(0),
    })
}

fn ids_json(ids: &[&str]) -> Result<String> {
    serde_json::to_string(ids).map_err(|e| DbError::Check(e.to_string()))
}

/// The attempt in progress of each given Task, in one statement whatever the
/// number of Tasks (`integration_attempt_current_task` finds each attempt,
/// `integration_attempt_current_seq` counts the waiters ahead of it).
pub async fn integration_members_of_tasks<'e, E>(
    executor: E,
    task_ids: &[&str],
) -> Result<Vec<IntegrationMemberRead>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query(&format!(
        "SELECT {MEMBER_COLUMNS},{AHEAD} AS ahead FROM integration_attempt a {MEMBER_JOINS} WHERE a.task_ref IN (SELECT value FROM json_each(?)) AND {LIVE}"
    ))
    .bind(ids_json(task_ids)?)
    .fetch_all(executor)
    .await?
    .into_iter()
    .map(member)
    .collect()
}

fn members_of_queues_sql() -> String {
    format!("SELECT * FROM (SELECT {MEMBER_COLUMNS},{AHEAD} AS ahead,COUNT(*) OVER (PARTITION BY a.queue_id) AS queue_members,ROW_NUMBER() OVER (PARTITION BY a.queue_id ORDER BY (q.head_attempt_id IS a.id) DESC,a.queue_seq,a.attempt_number,a.id) AS member_rank FROM integration_attempt a INDEXED BY integration_attempt_current_seq {MEMBER_JOINS} WHERE a.queue_id IN (SELECT value FROM json_each(?)) AND {LIVE}) WHERE member_rank<=? ORDER BY queue_id,member_rank")
}

/// The members in progress of each given queue: the head first, then queue
/// order; at most `per_queue` of each (`queue_members` is the whole count).
pub async fn integration_members_of_queues<'e, E>(
    executor: E,
    queue_ids: &[&str],
    per_queue: u32,
) -> Result<Vec<IntegrationMemberRead>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    if queue_ids.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query(&members_of_queues_sql())
        .bind(ids_json(queue_ids)?)
        .bind(i64::from(per_queue.clamp(1, 1000)))
        .fetch_all(executor)
        .await?
        .into_iter()
        .map(member)
        .collect()
}

/// A queue as a reader sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct IntegrationQueueRead {
    pub id: String,
    pub repo_id: String,
    pub target_branch: String,
    pub state: IntegrationQueueState,
    pub head_attempt_id: Option<String>,
    pub last_error_kind: Option<IntegrationFailureKind>,
    pub last_error: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct QueueCursor {
    /// The last target branch of the page before.
    b: String,
}

/// A page of one repository's queues by target branch (a keyset on
/// `UNIQUE(repo_id,target_branch)`), or the one queue of `target_branch`.
/// The cursor is opaque to callers and holds only the last branch of the
/// page before: the repository is always the caller's own bind, so a cursor
/// taken from one repository reads nothing of another, and a queue created
/// or removed between two pages neither repeats nor skips a queue.
pub async fn integration_queues_of_repo<'e, E>(
    executor: E,
    repo_id: &str,
    target_branch: Option<&str>,
    cursor: Option<&str>,
    limit: i64,
) -> Result<crate::Page<IntegrationQueueRead>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let after = match cursor {
        None => String::new(),
        Some(cursor) => {
            let bytes = base64::Engine::decode(&URL_SAFE_NO_PAD, cursor)
                .map_err(|_| DbError::InvalidCursor)?;
            serde_json::from_slice::<QueueCursor>(&bytes)
                .map_err(|_| DbError::InvalidCursor)?
                .b
        }
    };
    let limit = crate::clamp_page_limit(Some(limit));
    let mut items = sqlx::query("SELECT id,repo_id,target_branch,state,head_attempt_id,last_error_kind,last_error FROM integration_queue WHERE repo_id=?1 AND target_branch>?2 AND (?3 IS NULL OR target_branch=?3) ORDER BY target_branch LIMIT ?4")
        .bind(repo_id)
        .bind(after)
        .bind(target_branch)
        .bind(limit + 1)
        .fetch_all(executor)
        .await?
        .into_iter()
        .map(|row| {
            Ok(IntegrationQueueRead {
                id: row.try_get("id")?,
                repo_id: row.try_get("repo_id")?,
                target_branch: row.try_get("target_branch")?,
                state: row.try_get::<String, _>("state")?.parse()?,
                head_attempt_id: row.try_get("head_attempt_id")?,
                last_error_kind: row
                    .try_get::<Option<String>, _>("last_error_kind")?
                    .map(|kind| kind.parse())
                    .transpose()?,
                last_error: row.try_get("last_error")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = match (more, items.last()) {
        (true, Some(last)) => Some(base64::Engine::encode(
            &URL_SAFE_NO_PAD,
            serde_json::to_vec(&QueueCursor {
                b: last.target_branch.clone(),
            })
            .map_err(|_| DbError::InvalidCursor)?,
        )),
        _ => None,
    };
    Ok(crate::Page {
        items,
        next_cursor,
        total_count: None,
    })
}

/// What a Task list of this Project shows of the queue, as one opaque value:
/// it changes whenever a member in progress or its queue's state or head
/// does. The queue worker writes no Task and no Project row, so this is the
/// only validator a cached list has for the `integration` summary. A lease
/// renewal changes nothing here. Every member of a queue belongs to the
/// queue's own Project (admission refuses any other), so the Project's
/// members are all the members whose place can move. The sums are over
/// values that only grow (`revision`, `queue_seq`), so a member that leaves
/// and one that joins between two reads cannot cancel out.
pub async fn integration_list_revision<'e, E>(executor: E, project_id: &str) -> Result<String>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let row = sqlx::query(LIST_REVISION)
        .bind(project_id)
        .fetch_one(executor)
        .await?;
    Ok(format!(
        "{}:{}:{}:{}:{}",
        row.try_get::<i64, _>("members")?,
        row.try_get::<i64, _>("revisions")?,
        row.try_get::<i64, _>("seqs")?,
        row.try_get::<String, _>("newest")?,
        row.try_get::<String, _>("queues")?
    ))
}
const LIST_REVISION: &str = "SELECT COUNT(*) AS members,COALESCE(SUM(a.revision),0) AS revisions,COALESCE(SUM(a.queue_seq),0) AS seqs,COALESCE(MAX(a.updated_at),'') AS newest,COALESCE(group_concat(DISTINCT q.id||':'||q.state||':'||COALESCE(q.head_attempt_id,'')),'') AS queues FROM integration_attempt a LEFT JOIN integration_queue q ON q.id=a.queue_id WHERE a.project_ref=? AND a.current=1";

/// One row of the change watch: a queue, and one member in progress (or
/// none, for a queue with no member). `member_view` is every attempt column
/// the public summary is projected from, as one value to compare: receipts,
/// fences and the row's `revision` are not in it, so a write that changes
/// nothing a reader sees leaves it equal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationWatchRow {
    pub queue_id: String,
    pub repo_id: String,
    /// The queue columns a reader sees (state, head, why it is not open).
    pub queue_view: String,
    pub has_head: bool,
    pub attempt_id: Option<String>,
    pub task_ref: Option<String>,
    pub project_ref: Option<String>,
    pub is_head: bool,
    /// `queued` or `path_wait`: the member stands in line.
    pub in_line: bool,
    /// A waiter carrying a cancel request is never claimed: it is ahead of
    /// nobody (the same rule as the summary's position).
    pub cancel_requested: bool,
    pub member_view: Option<String>,
}

// The planner prefers the table's UNIQUE(queue_id,..) index, which holds
// every attempt a queue ever had; the watch is told to walk the members in
// progress only.
pub(crate) const WATCH_ROWS: &str = "SELECT q.id AS queue_id,q.repo_id,json_array(q.state,q.head_attempt_id,q.last_error_kind,q.last_error) AS queue_view,q.head_attempt_id IS NOT NULL AS has_head,a.id AS attempt_id,a.task_ref,a.project_ref,q.head_attempt_id IS a.id AS is_head,a.state IN ('queued','path_wait') AS in_line,a.cancel_requested_at IS NOT NULL AS cancel_requested,CASE WHEN a.id IS NOT NULL THEN json_array(a.state,a.failure_kind,a.failure_message,a.available_at,a.started_at,a.cancel_requested_at IS NOT NULL,a.phase_timings_json,a.daemon_id) END AS member_view FROM integration_queue q LEFT JOIN integration_attempt a INDEXED BY integration_attempt_current_seq ON a.queue_id=q.id AND a.current=1 ORDER BY q.id,a.queue_seq,a.id";

impl SqliteDb {
    /// Every queue with its members in progress, in queue order. The SSE
    /// projector compares two of these reads; it is one statement, a plain
    /// SELECT on the pool, and the attempt history is never walked.
    pub async fn integration_watch_rows(&self) -> Result<Vec<IntegrationWatchRow>> {
        sqlx::query(WATCH_ROWS)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(|row| {
                let flag = |name: &str| -> Result<bool> {
                    Ok(row.try_get::<Option<bool>, _>(name)?.unwrap_or(false))
                };
                Ok(IntegrationWatchRow {
                    queue_id: row.try_get("queue_id")?,
                    repo_id: row.try_get("repo_id")?,
                    queue_view: row.try_get("queue_view")?,
                    has_head: flag("has_head")?,
                    attempt_id: row.try_get("attempt_id")?,
                    task_ref: row.try_get("task_ref")?,
                    project_ref: row.try_get("project_ref")?,
                    is_head: flag("is_head")?,
                    in_line: flag("in_line")?,
                    cancel_requested: flag("cancel_requested")?,
                    member_view: row.try_get("member_view")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn plan(db: &SqliteDb, sql: &str, binds: &[&str]) -> String {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = query.bind(*bind);
        }
        query
            .fetch_all(db.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The reads of a Task list stay on their indexes: none of them walks the
    /// attempt table, whose history only grows.
    #[tokio::test]
    async fn task_list_reads_use_the_current_attempt_indexes() {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let revision = plan(
            &db,
            &format!("EXPLAIN QUERY PLAN {LIST_REVISION}"),
            &["project"],
        )
        .await;
        assert!(
            revision.contains("integration_attempt_project_live") && !revision.contains("SCAN a"),
            "{revision}"
        );
        let summaries = plan(
            &db,
            &format!("EXPLAIN QUERY PLAN SELECT {MEMBER_COLUMNS},{AHEAD} AS ahead FROM integration_attempt a {MEMBER_JOINS} WHERE a.task_ref IN (SELECT value FROM json_each(?)) AND {LIVE}"),
            &["[]"],
        )
        .await;
        assert!(
            summaries.contains("integration_attempt_current_task"),
            "{summaries}"
        );
        assert!(
            summaries.contains("integration_attempt_current_seq"),
            "{summaries}"
        );
        assert!(
            !summaries.contains("SCAN a") && !summaries.contains("SCAN b"),
            "{summaries}"
        );

        // The repository route: the queues by the UNIQUE(repo_id,target_branch)
        // keyset, their members by the queue's own index.
        let queues = plan(
            &db,
            "EXPLAIN QUERY PLAN SELECT id,repo_id,target_branch,state,head_attempt_id,last_error_kind,last_error FROM integration_queue WHERE repo_id=?1 AND target_branch>?2 AND (?3 IS NULL OR target_branch=?3) ORDER BY target_branch LIMIT ?4",
            &["repo", "", "main", "2"],
        )
        .await;
        assert!(
            queues.contains("SEARCH integration_queue USING INDEX")
                && queues.contains("repo_id=? AND target_branch>?")
                && !queues.contains("TEMP B-TREE"),
            "{queues}"
        );
        let members = plan(
            &db,
            &format!("EXPLAIN QUERY PLAN {}", members_of_queues_sql()),
            &["[]", "100"],
        )
        .await;
        // The members in progress only, never the queue's whole history.
        assert!(
            members.contains("SEARCH a USING INDEX integration_attempt_current_seq")
                && !members.contains("SCAN a")
                && !members.contains("SCAN b"),
            "{members}"
        );
        // The projector's watch: every queue (there is one per target
        // branch), and only the members in progress of each.
        let watch = plan(&db, &format!("EXPLAIN QUERY PLAN {WATCH_ROWS}"), &[]).await;
        assert!(
            watch.contains("SEARCH a USING INDEX integration_attempt_current_seq")
                && !watch.contains("SCAN a"),
            "{watch}"
        );
    }

    #[tokio::test]
    async fn an_idle_queue_keeps_its_list_revision_and_a_member_change_moves_it() {
        use crate::IntegrationQueueRepo;
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let now = crate::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','r','main',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','merging',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        let empty = integration_list_revision(db.pool(), "p").await.unwrap();
        let queue = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let attempt = db
            .admit_integration_attempt(crate::IntegrationAttempt::new(
                Some(queue.id.clone()),
                "t".into(),
                "p".into(),
                "admission".into(),
                "merging".into(),
                0,
                1,
            ))
            .await
            .unwrap();
        let queued = integration_list_revision(db.pool(), "p").await.unwrap();
        assert_ne!(empty, queued);
        // A lease renewal rewrites the queue row and nothing a list shows.
        sqlx::query("UPDATE integration_queue SET revision=revision+1,updated_at=? WHERE id=?")
            .bind(crate::now_rfc3339())
            .bind(&queue.id)
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(
            queued,
            integration_list_revision(db.pool(), "p").await.unwrap()
        );
        assert_eq!(
            integration_list_revision(db.pool(), "other").await.unwrap(),
            empty
        );
        sqlx::query("UPDATE integration_attempt SET state='parked',revision=revision+1 WHERE id=?")
            .bind(&attempt.id)
            .execute(db.pool())
            .await
            .unwrap();
        assert_ne!(
            queued,
            integration_list_revision(db.pool(), "p").await.unwrap()
        );
        // A member that leaves and one that joins between two reads do not
        // cancel out, whatever their revisions.
        let parked = integration_list_revision(db.pool(), "p").await.unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('u','p','u','merging',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        let next = db
            .admit_integration_attempt(crate::IntegrationAttempt::new(
                Some(queue.id.clone()),
                "u".into(),
                "p".into(),
                "admission-u".into(),
                "merging".into(),
                0,
                1,
            ))
            .await
            .unwrap();
        sqlx::query("UPDATE integration_attempt SET state='cancelled',current=0,completed_at=updated_at WHERE id=?")
            .bind(&attempt.id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE integration_attempt SET state='parked',revision=(SELECT revision FROM integration_attempt WHERE id=?),updated_at=(SELECT updated_at FROM integration_attempt WHERE id=?) WHERE id=?")
            .bind(&attempt.id)
            .bind(&attempt.id)
            .bind(&next.id)
            .execute(db.pool())
            .await
            .unwrap();
        assert_ne!(
            parked,
            integration_list_revision(db.pool(), "p").await.unwrap()
        );
        sqlx::query(
            "UPDATE integration_attempt SET state='parked',current=1,completed_at=NULL WHERE id=?",
        )
        .bind(&attempt.id)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE integration_attempt SET state='cancelled',current=0,completed_at=updated_at WHERE id=?")
            .bind(&next.id)
            .execute(db.pool())
            .await
            .unwrap();
        let rows = integration_members_of_tasks(db.pool(), &["t", "missing"])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, IntegrationAttemptState::Parked);
        assert_eq!(rows[0].ahead, None);
        assert!(!rows[0].project_paused);
        sqlx::query("UPDATE project SET paused_at=? WHERE id='p'")
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(
            integration_members_of_tasks(db.pool(), &["t"])
                .await
                .unwrap()[0]
                .project_paused
        );
        assert_eq!(db.integration_watch_rows().await.unwrap().len(), 1);
    }
}
