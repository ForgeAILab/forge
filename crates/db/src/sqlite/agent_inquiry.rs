use super::*;
use crate::now_rfc3339;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

const AGENT_INQUIRY_COLUMNS: &str = "id, chat_id, turn_job_id, identity_id, owner_user_id, \
     title, question, status, findings, findings_path, workspace_path, error, \
     input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, duration_ms, \
     version, created_at, updated_at, started_at, finished_at";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentInquiryCursor {
    created_at: String,
    id: String,
}

fn decode_agent_inquiry_cursor(cursor: Option<&str>) -> Result<Option<AgentInquiryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let bytes =
        base64::Engine::decode(&URL_SAFE_NO_PAD, cursor).map_err(|_| DbError::InvalidCursor)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| DbError::InvalidCursor)
}

fn encode_agent_inquiry_cursor(created_at: &str, id: &str) -> Result<String> {
    let bytes = serde_json::to_vec(&AgentInquiryCursor {
        created_at: created_at.to_owned(),
        id: id.to_owned(),
    })
    .map_err(|_| DbError::InvalidCursor)?;
    Ok(base64::Engine::encode(&URL_SAFE_NO_PAD, bytes))
}

#[async_trait]
impl AgentInquiryRepo for SqliteDb {
    async fn create_agent_inquiry(&self, input: CreateAgentInquiry) -> Result<AgentInquiry> {
        let now = now_rfc3339();
        let sql = format!(
            "INSERT INTO agent_inquiry (
                id, chat_id, turn_job_id, identity_id, owner_user_id, title, question,
                status, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                version, created_at, updated_at, started_at, workspace_path
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running', 0, 0, 0, 0, 1, ?, ?, ?, ?)
             RETURNING {AGENT_INQUIRY_COLUMNS}"
        );
        sqlx::query(&sql)
            .bind(&input.id)
            .bind(&input.chat_id)
            .bind(input.turn_job_id.as_deref())
            .bind(&input.identity_id)
            .bind(&input.owner_user_id)
            .bind(&input.title)
            .bind(&input.question)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .bind(input.workspace_path.as_deref())
            .fetch_one(&self.pool)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_inquiry)
    }

    async fn get_agent_inquiry(&self, id: &str) -> Result<Option<AgentInquiry>> {
        let sql = format!("SELECT {AGENT_INQUIRY_COLUMNS} FROM agent_inquiry WHERE id = ?");
        sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_inquiry)
            .transpose()
    }

    async fn list_agent_inquiries(
        &self,
        chat_id: &str,
        limit: i64,
        cursor: Option<&str>,
    ) -> Result<Page<AgentInquiry>> {
        let cursor = decode_agent_inquiry_cursor(cursor)?;
        let limit = limit.clamp(1, 500);
        let rows = if let Some(cursor) = cursor {
            let sql = format!(
                "SELECT {AGENT_INQUIRY_COLUMNS} FROM agent_inquiry \
                 WHERE chat_id = ? \
                   AND (created_at < ? OR (created_at = ? AND id < ?)) \
                 ORDER BY created_at DESC, id DESC LIMIT ?"
            );
            sqlx::query(&sql)
                .bind(chat_id)
                .bind(&cursor.created_at)
                .bind(&cursor.created_at)
                .bind(&cursor.id)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await?
        } else {
            let sql = format!(
                "SELECT {AGENT_INQUIRY_COLUMNS} FROM agent_inquiry \
                 WHERE chat_id = ? ORDER BY created_at DESC, id DESC LIMIT ?"
            );
            sqlx::query(&sql)
                .bind(chat_id)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await?
        };
        let mut items = rows
            .into_iter()
            .map(map_agent_inquiry)
            .collect::<Result<Vec<_>>>()?;
        let has_next = items.len() > limit as usize;
        if has_next {
            items.truncate(limit as usize);
        }
        let next_cursor = if has_next {
            items
                .last()
                .map(|item| encode_agent_inquiry_cursor(&item.created_at, &item.id))
                .transpose()?
        } else {
            None
        };
        Ok(Page {
            items,
            next_cursor,
            total_count: None,
        })
    }

    async fn complete_agent_inquiry(&self, input: CompleteAgentInquiry) -> Result<AgentInquiry> {
        let now = now_rfc3339();
        let sql = format!(
            "UPDATE agent_inquiry
             SET status = ?, findings = ?, findings_path = ?, error = ?,
                 input_tokens = ?, output_tokens = ?, cache_read_tokens = ?,
                 cache_write_tokens = ?, duration_ms = ?, version = version + 1,
                 updated_at = ?, finished_at = ?
             WHERE id = ? AND version = ? AND status = 'running'
             RETURNING {AGENT_INQUIRY_COLUMNS}"
        );
        let updated = sqlx::query(&sql)
            .bind(input.status.to_string())
            .bind(input.findings.as_deref())
            .bind(input.findings_path.as_deref())
            .bind(input.error.as_deref())
            .bind(input.input_tokens)
            .bind(input.output_tokens)
            .bind(input.cache_read_tokens)
            .bind(input.cache_write_tokens)
            .bind(input.duration_ms)
            .bind(&now)
            .bind(&now)
            .bind(&input.id)
            .bind(input.expected_version)
            .fetch_optional(&self.pool)
            .await?;
        match updated {
            Some(row) => map_agent_inquiry(row),
            None => match self.get_agent_inquiry(&input.id).await? {
                None => Err(DbError::NotFound),
                Some(_) => Err(DbError::VersionConflict),
            },
        }
    }

    async fn cancel_agent_inquiry(&self, id: &str, expected_version: i64) -> Result<AgentInquiry> {
        let now = now_rfc3339();
        let sql = format!(
            // The runner's completing write is refused once this row is
            // terminal, so it never gets to record how long the run lasted.
            // Derive it here instead, or a cancelled inquiry would be the one
            // outcome that shows no duration at all.
            "UPDATE agent_inquiry
             SET status = 'cancelled', version = version + 1, updated_at = ?, finished_at = ?,
                 duration_ms = COALESCE(
                     duration_ms,
                     CAST((julianday(?) - julianday(started_at)) * 86400000.0 AS INTEGER)
                 )
             WHERE id = ? AND version = ? AND status = 'running'
             RETURNING {AGENT_INQUIRY_COLUMNS}"
        );
        let updated = sqlx::query(&sql)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .bind(id)
            .bind(expected_version)
            .fetch_optional(&self.pool)
            .await?;
        match updated {
            Some(row) => map_agent_inquiry(row),
            None => match self.get_agent_inquiry(id).await? {
                None => Err(DbError::NotFound),
                Some(_) => Err(DbError::VersionConflict),
            },
        }
    }
}

fn map_agent_inquiry(row: SqliteRow) -> Result<AgentInquiry> {
    Ok(AgentInquiry {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        turn_job_id: row.try_get("turn_job_id")?,
        identity_id: row.try_get("identity_id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        title: row.try_get("title")?,
        question: row.try_get("question")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        findings: row.try_get("findings")?,
        findings_path: row.try_get("findings_path")?,
        workspace_path: row.try_get("workspace_path")?,
        error: row.try_get("error")?,
        input_tokens: row.try_get("input_tokens")?,
        output_tokens: row.try_get("output_tokens")?,
        cache_read_tokens: row.try_get("cache_read_tokens")?,
        cache_write_tokens: row.try_get("cache_write_tokens")?,
        duration_ms: row.try_get("duration_ms")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}
