//! Full diagnostic text is independent of the small condition projection.
use super::*;
use sha2::{Digest, Sha256};

pub const DIAGNOSTIC_TEXT_LIMIT: usize = 1024 * 1024;

/// The reason or durable witness that supplied text. Command bodies never belong here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "owner", rename_all = "snake_case")]
pub enum DiagnosticOwner {
    Reason { reason: ParkReason },
    Witness { witness: ConditionWitness },
    StoredCondition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDiagnosticText {
    pub id: String,
    pub task_id: String,
    pub kind: String,
    pub source: String,
    pub owner: DiagnosticOwner,
    pub text: String,
    /// Original UTF-8 byte length, before the per-entry cap.
    pub byte_length: i64,
    /// SHA-256 of the original bytes, including the part beyond the cap.
    pub digest: String,
    pub created_at: String,
    pub last_seen: String,
    pub occurrence_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDiagnosticTextPage {
    pub items: Vec<TaskDiagnosticText>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

pub struct RecordTaskDiagnosticText<'a> {
    pub kind: &'a str,
    pub source: &'a str,
    pub owner: &'a DiagnosticOwner,
    pub text: &'a str,
}

pub(super) fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(super) fn truncate(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    let mut end = limit - PRESENTATION_TRUNCATION_MARKER.len();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{PRESENTATION_TRUNCATION_MARKER}", &text[..end])
}

pub(super) async fn record(
    c: &mut SqliteConnection,
    task_id: &str,
    input: RecordTaskDiagnosticText<'_>,
) -> Result<String> {
    let id = crate::new_uuid_v4();
    let at = crate::now_rfc3339();
    let owner = serde_json::to_string(input.owner).map_err(payload::json_error)?;
    let digest = digest(input.text.as_bytes());
    let text = truncate(input.text, DIAGNOSTIC_TEXT_LIMIT);
    Ok(sqlx::query_scalar("INSERT INTO task_diagnostic_text(id,task_id,kind,source,owner_json,text,byte_length,digest,created_at,last_seen) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id,kind,digest) DO UPDATE SET occurrence_count=occurrence_count+1,last_seen=excluded.last_seen RETURNING id")
        .bind(id).bind(task_id).bind(input.kind).bind(input.source).bind(owner)
        .bind(text).bind(input.text.len() as i64).bind(digest).bind(&at).bind(&at)
        .fetch_one(c).await?)
}

fn from_row(row: sqlx::sqlite::SqliteRow) -> Result<TaskDiagnosticText> {
    let raw: String = row.try_get("owner_json")?;
    Ok(TaskDiagnosticText {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        kind: row.try_get("kind")?,
        source: row.try_get("source")?,
        owner: serde_json::from_str(&raw).map_err(|e| DbError::Check(e.to_string()))?,
        text: row.try_get("text")?,
        byte_length: row.try_get("byte_length")?,
        digest: row.try_get("digest")?,
        created_at: row.try_get("created_at")?,
        last_seen: row.try_get("last_seen")?,
        occurrence_count: row.try_get("occurrence_count")?,
    })
}

impl SqliteDb {
    /// Capture text independently after a legacy writer has committed. Its caller
    /// may leave capture for later when the full legacy text is still durable.
    pub async fn record_task_diagnostic_text(
        &self,
        input_task_id: &str,
        input: RecordTaskDiagnosticText<'_>,
    ) -> Result<String> {
        let mut tx = self.pool().begin().await?;
        let id = record(&mut tx, input_task_id, input).await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Shares the caller's transaction: failure of the Task write rolls back its text too.
    pub async fn record_task_diagnostic_text_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        input: RecordTaskDiagnosticText<'_>,
    ) -> Result<String> {
        record(tx, task_id, input).await
    }

    /// On-demand detail read, scoped by Task as well as diagnostic id.
    pub async fn task_diagnostic_text(
        &self,
        task_id: &str,
        id: &str,
    ) -> Result<Option<TaskDiagnosticText>> {
        sqlx::query("SELECT * FROM task_diagnostic_text WHERE task_id=? AND id=?")
            .bind(task_id)
            .bind(id)
            .fetch_optional(self.pool())
            .await?
            .map(from_row)
            .transpose()
    }

    /// Keyset page for the Task detail reader. Normal Task reads never join this table.
    pub async fn task_diagnostic_texts(
        &self,
        task_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<TaskDiagnosticTextPage> {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let after: Option<(String, String)> = cursor
            .map(|cursor| {
                let bytes = URL_SAFE_NO_PAD
                    .decode(cursor)
                    .map_err(|_| DbError::InvalidCursor)?;
                serde_json::from_slice(&bytes).map_err(|_| DbError::InvalidCursor)
            })
            .transpose()?;
        let limit = limit.clamp(1, 100);
        let mut items: Vec<_> = sqlx::query("SELECT * FROM task_diagnostic_text WHERE task_id=?1 AND (?2 IS NULL OR (created_at,id)>(?2,?3)) ORDER BY created_at,id LIMIT ?4")
            .bind(task_id).bind(after.as_ref().map(|a| &a.0)).bind(after.as_ref().map(|a| &a.1))
            .bind((limit + 1) as i64).fetch_all(self.pool()).await?
            .into_iter().map(from_row).collect::<Result<_>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if has_more {
            items
                .last()
                .map(|item| {
                    serde_json::to_vec(&(&item.created_at, &item.id))
                        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                        .map_err(|_| DbError::InvalidCursor)
                })
                .transpose()?
        } else {
            None
        };
        Ok(TaskDiagnosticTextPage {
            items,
            has_more,
            next_cursor,
        })
    }
}
