use super::*;

#[async_trait]
impl AgentInboxRepo for SqliteDb {
    async fn create_inbox_item(&self, input: CreateAgentInboxItem) -> Result<AgentInboxItem> {
        sqlx::query(
            "INSERT INTO agent_inbox_item (
                id, recipient_identity_id, scope_type, scope_id, kind, status, title, body,
                payload_json, source_type, source_id, correlation_id, causation_id, dedupe_key,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
             ON CONFLICT(recipient_identity_id, dedupe_key) DO NOTHING",
        )
        .bind(&input.id)
        .bind(&input.recipient_identity_id)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(input.kind.to_string())
        .bind(input.status.to_string())
        .bind(&input.title)
        .bind(&input.body)
        .bind(&input.payload_json)
        .bind(input.source_type.as_deref())
        .bind(input.source_id.as_deref())
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(&input.dedupe_key)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(check_error)?;
        let stored = sqlx::query(
            "SELECT * FROM agent_inbox_item
             WHERE recipient_identity_id = ? AND dedupe_key = ?",
        )
        .bind(&input.recipient_identity_id)
        .bind(&input.dedupe_key)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_inbox_item)
        .transpose()?
        .ok_or(DbError::NotFound)?;
        if stored.scope_type != input.scope_type
            || stored.scope_id != input.scope_id
            || stored.kind != input.kind
            || stored.body != input.body
            || stored.payload_json != input.payload_json
            || stored.correlation_id != input.correlation_id
        {
            return Err(DbError::Check(
                "inbox dedupe key was reused with different content".to_owned(),
            ));
        }
        Ok(stored)
    }

    async fn get_inbox_item(&self, id: &str) -> Result<Option<AgentInboxItem>> {
        sqlx::query("SELECT * FROM agent_inbox_item WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_inbox_item)
            .transpose()
    }

    async fn list_inbox_items(&self, query: AgentInboxListQuery) -> Result<Vec<AgentInboxItem>> {
        let mut builder = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT * FROM agent_inbox_item WHERE recipient_identity_id = ",
        );
        builder.push_bind(&query.recipient_identity_id);
        if let Some(status) = &query.status {
            builder.push(" AND status = ").push_bind(status.to_string());
        }
        if let Some(scope_type) = &query.scope_type {
            builder.push(" AND scope_type = ").push_bind(scope_type);
        }
        if let Some(scope_id) = &query.scope_id {
            builder.push(" AND scope_id = ").push_bind(scope_id);
        }
        builder
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(query.limit.clamp(1, 500));
        let rows = builder.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(map_agent_inbox_item).collect()
    }

    async fn update_inbox_item(&self, input: UpdateAgentInboxItem) -> Result<AgentInboxItem> {
        let status = input.status.to_string();
        let result = sqlx::query(
            "UPDATE agent_inbox_item SET
                status = ?,
                read_at = CASE WHEN ? IN ('read', 'acknowledged', 'dismissed')
                               THEN COALESCE(read_at, ?) ELSE read_at END,
                acknowledged_at = CASE WHEN ? = 'acknowledged'
                                       THEN COALESCE(acknowledged_at, ?) ELSE acknowledged_at END,
                version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&status)
        .bind(&status)
        .bind(&input.updated_at)
        .bind(&status)
        .bind(&input.updated_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            let exists = self.get_inbox_item(&input.id).await?.is_some();
            return Err(if exists {
                DbError::VersionConflict
            } else {
                DbError::NotFound
            });
        }
        self.get_inbox_item(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_question_with_inbox(
        &self,
        inbox: CreateAgentInboxItem,
        question: CreateAgentQuestion,
    ) -> Result<AgentQuestion> {
        let question_inbox_id = question
            .inbox_item_id
            .as_deref()
            .ok_or_else(|| DbError::Check("question must reference its inbox item".to_owned()))?;
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO agent_inbox_item (
                id, recipient_identity_id, scope_type, scope_id, kind, status, title, body,
                payload_json, source_type, source_id, correlation_id, causation_id, dedupe_key,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&inbox.id)
        .bind(&inbox.recipient_identity_id)
        .bind(&inbox.scope_type)
        .bind(&inbox.scope_id)
        .bind(inbox.kind.to_string())
        .bind(inbox.status.to_string())
        .bind(&inbox.title)
        .bind(&inbox.body)
        .bind(&inbox.payload_json)
        .bind(inbox.source_type.as_deref())
        .bind(inbox.source_id.as_deref())
        .bind(&inbox.correlation_id)
        .bind(inbox.causation_id.as_deref())
        .bind(&inbox.dedupe_key)
        .bind(&inbox.created_at)
        .bind(&inbox.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let stored_inbox = map_agent_inbox_item(
            sqlx::query(
                "SELECT * FROM agent_inbox_item
             WHERE recipient_identity_id = ? AND dedupe_key = ?",
            )
            .bind(&inbox.recipient_identity_id)
            .bind(&inbox.dedupe_key)
            .fetch_one(&mut *transaction)
            .await?,
        )?;
        if stored_inbox.scope_type != inbox.scope_type
            || stored_inbox.scope_id != inbox.scope_id
            || stored_inbox.kind != inbox.kind
            || stored_inbox.body != inbox.body
            || stored_inbox.payload_json != inbox.payload_json
        {
            return Err(DbError::Check(
                "inbox dedupe key was reused with different content".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT OR IGNORE INTO agent_question (
                id, recipient_identity_id, scope_type, scope_id, status, question,
                context_json, asked_by_type, asked_by_id, inbox_item_id, due_at,
                correlation_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&question.id)
        .bind(&question.recipient_identity_id)
        .bind(&question.scope_type)
        .bind(&question.scope_id)
        .bind(&question.question)
        .bind(&question.context_json)
        .bind(&question.asked_by_type)
        .bind(&question.asked_by_id)
        .bind(question.inbox_item_id.as_deref())
        .bind(question.due_at.as_deref())
        .bind(&question.correlation_id)
        .bind(&question.created_at)
        .bind(&question.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let stored_question = map_agent_question(
            sqlx::query("SELECT * FROM agent_question WHERE inbox_item_id = ?")
                .bind(question_inbox_id)
                .fetch_one(&mut *transaction)
                .await?,
        )?;
        if stored_question.question != question.question
            || stored_question.context_json != question.context_json
            || stored_question.recipient_identity_id != question.recipient_identity_id
        {
            return Err(DbError::Check(
                "question inbox dedupe key was reused with different content".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(stored_question)
    }

    async fn create_question(&self, input: CreateAgentQuestion) -> Result<AgentQuestion> {
        sqlx::query(
            "INSERT OR IGNORE INTO agent_question (
                id, recipient_identity_id, scope_type, scope_id, status, question,
                context_json, asked_by_type, asked_by_id, inbox_item_id, due_at,
                correlation_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.recipient_identity_id)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(&input.question)
        .bind(&input.context_json)
        .bind(&input.asked_by_type)
        .bind(&input.asked_by_id)
        .bind(input.inbox_item_id.as_deref())
        .bind(input.due_at.as_deref())
        .bind(&input.correlation_id)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(check_error)?;
        if let Some(inbox_item_id) = input.inbox_item_id.as_deref() {
            return sqlx::query("SELECT * FROM agent_question WHERE inbox_item_id = ?")
                .bind(inbox_item_id)
                .fetch_optional(&self.pool)
                .await?
                .map(map_agent_question)
                .transpose()?
                .ok_or(DbError::NotFound);
        }
        self.get_question(&input.id).await?.ok_or(DbError::NotFound)
    }

    async fn get_question(&self, id: &str) -> Result<Option<AgentQuestion>> {
        sqlx::query("SELECT * FROM agent_question WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_question)
            .transpose()
    }

    async fn list_questions(&self, query: AgentQuestionListQuery) -> Result<Vec<AgentQuestion>> {
        let mut builder = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT * FROM agent_question WHERE recipient_identity_id = ",
        );
        builder.push_bind(&query.recipient_identity_id);
        if let Some(status) = &query.status {
            builder.push(" AND status = ").push_bind(status.to_string());
        }
        if let Some(scope_type) = &query.scope_type {
            builder.push(" AND scope_type = ").push_bind(scope_type);
        }
        if let Some(scope_id) = &query.scope_id {
            builder.push(" AND scope_id = ").push_bind(scope_id);
        }
        builder
            .push(" ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END, created_at DESC, id DESC LIMIT ")
            .push_bind(query.limit.clamp(1, 500));
        let rows = builder.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(map_agent_question).collect()
    }

    async fn answer_question(&self, input: AnswerAgentQuestion) -> Result<AgentQuestion> {
        let result = sqlx::query(
            "UPDATE agent_question SET status = 'answered', answer = ?,
                answered_by_type = ?, answered_by_id = ?, answered_at = ?,
                version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND status = 'open'",
        )
        .bind(&input.answer)
        .bind(&input.answered_by_type)
        .bind(&input.answered_by_id)
        .bind(&input.updated_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            let question = self.get_question(&input.id).await?;
            return match question {
                Some(question) if question.version != input.expected_version => {
                    Err(DbError::VersionConflict)
                }
                Some(_) => Err(DbError::InvalidTransition),
                None => Err(DbError::NotFound),
            };
        }
        self.get_question(&input.id).await?.ok_or(DbError::NotFound)
    }
}

fn map_agent_inbox_item(row: SqliteRow) -> Result<AgentInboxItem> {
    Ok(AgentInboxItem {
        id: row.try_get("id")?,
        recipient_identity_id: row.try_get("recipient_identity_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        kind: parse_enum(row.try_get("kind")?)?,
        status: parse_enum(row.try_get("status")?)?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        payload_json: row.try_get("payload_json")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        read_at: row.try_get("read_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent_question(row: SqliteRow) -> Result<AgentQuestion> {
    Ok(AgentQuestion {
        id: row.try_get("id")?,
        recipient_identity_id: row.try_get("recipient_identity_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        status: parse_enum(row.try_get("status")?)?,
        question: row.try_get("question")?,
        context_json: row.try_get("context_json")?,
        answer: row.try_get("answer")?,
        asked_by_type: row.try_get("asked_by_type")?,
        asked_by_id: row.try_get("asked_by_id")?,
        answered_by_type: row.try_get("answered_by_type")?,
        answered_by_id: row.try_get("answered_by_id")?,
        inbox_item_id: row.try_get("inbox_item_id")?,
        due_at: row.try_get("due_at")?,
        correlation_id: row.try_get("correlation_id")?,
        version: row.try_get("version")?,
        answered_at: row.try_get("answered_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}
