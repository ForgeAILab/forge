use super::*;
/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProjectTurnAdmissionReceiptRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProjectTurnAdmissionReceiptRow {
    pub fn project_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn source_kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_kind")?)
    }
    pub fn handoff_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("handoff_id")?)
    }
    pub fn initial_charter_approval_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("initial_charter_approval_id")?)
    }
    pub fn initial_charter_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("initial_charter_id")?)
    }
    pub fn initial_charter_revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("initial_charter_revision_id")?)
    }
    pub fn payload_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("payload_digest")?)
    }
    pub fn validation_schema_version(&self) -> crate::Result<String> {
        Ok(self.row.try_get("validation_schema_version")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProjectTurnApprovalEventRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProjectTurnApprovalEventRow {
    pub fn authorization_basis(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authorization_basis")?)
    }
    pub fn action(&self) -> crate::Result<String> {
        Ok(self.row.try_get("action")?)
    }
    pub fn explicit_event(&self) -> crate::Result<String> {
        Ok(self.row.try_get("explicit_event")?)
    }
    pub fn occurred_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("occurred_at")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProjectTurnCharterApprovalRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProjectTurnCharterApprovalRow {
    pub fn charter_id_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_id")?)
    }
    pub fn charter_account_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_account_id")?)
    }
    pub fn charter_revision_id_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_revision_id")?)
    }
    pub fn current_approved_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_approved_revision_id")?)
    }
    pub fn charter_revision_lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_revision_lifecycle")?)
    }
    pub fn charter_version_required(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("charter_version")?)
    }
    pub fn expected_charter_version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("expected_charter_version")?)
    }
    pub fn approval_lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_lifecycle")?)
    }
    pub fn approval_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_id")?)
    }
    pub fn approval_content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_content_digest")?)
    }
    pub fn approval_render_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_render_digest")?)
    }
    pub fn charter_content_digest_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_content_digest")?)
    }
    pub fn charter_render_digest_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_render_digest")?)
    }
    pub fn consumed_project_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("consumed_project_id")?)
    }
    pub fn selected_operating_skill_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selected_operating_skill_revision_id")?)
    }
    pub fn approval_event_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("approval_event_id")?)
    }
    pub fn approving_principal_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approving_principal_type")?)
    }
    pub fn approving_principal_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approving_principal_id")?)
    }
    pub fn approval_authorization_basis(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_authorization_basis")?)
    }
    pub fn approval_authorization_action(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_authorization_action")?)
    }
    pub fn approval_authorization_event_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_authorization_event_id")?)
    }
    pub fn approval_authorization_occurred_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_authorization_occurred_at")?)
    }
    pub fn approval_created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("approval_created_at")?)
    }
    pub fn project_mode(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_mode")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProjectTurnOperatingSkillRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProjectTurnOperatingSkillRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn skill_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("skill_key")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
    pub fn schema_version(&self) -> crate::Result<String> {
        Ok(self.row.try_get("schema_version")?)
    }
    pub fn render_version(&self) -> crate::Result<String> {
        Ok(self.row.try_get("render_version")?)
    }
    pub fn canonical_body(&self) -> crate::Result<String> {
        Ok(self.row.try_get("canonical_body")?)
    }
    pub fn policy_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_json")?)
    }
    pub fn policy_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_digest")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
    pub fn current_revision_id_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_revision_id")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct MainTurnOperatingSkillRow {
    row: sqlx::sqlite::SqliteRow,
}
impl MainTurnOperatingSkillRow {
    pub fn current_revision_id_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("current_revision_id")?)
    }
    pub fn skill_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("skill_key")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
    pub fn schema_version(&self) -> crate::Result<String> {
        Ok(self.row.try_get("schema_version")?)
    }
    pub fn render_version(&self) -> crate::Result<String> {
        Ok(self.row.try_get("render_version")?)
    }
    pub fn canonical_body(&self) -> crate::Result<String> {
        Ok(self.row.try_get("canonical_body")?)
    }
    pub fn policy_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_json")?)
    }
    pub fn policy_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("policy_digest")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct MainTurnGenesisInstructionRow {
    row: sqlx::sqlite::SqliteRow,
}
impl MainTurnGenesisInstructionRow {
    pub fn instruction_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("instruction_id")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
    pub fn body(&self) -> crate::Result<String> {
        Ok(self.row.try_get("body")?)
    }
    pub fn genesis_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("genesis_id")?)
    }
    pub fn genesis_version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("genesis_version")?)
    }
    pub fn genesis_lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("genesis_lifecycle")?)
    }
    pub fn charter_id_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_id")?)
    }
    pub fn charter_revision_id_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_revision_id")?)
    }
    pub fn charter_version_optional(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("charter_version")?)
    }
    pub fn charter_content_digest_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_content_digest")?)
    }
    pub fn charter_render_digest_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_render_digest")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct FrozenTurnOperatingSkillRow {
    row: sqlx::sqlite::SqliteRow,
}
impl FrozenTurnOperatingSkillRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn skill_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("skill_key")?)
    }
    pub fn canonical_body(&self) -> crate::Result<String> {
        Ok(self.row.try_get("canonical_body")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
}

impl SqliteDb {
    pub async fn frozen_turn_operating_skill(
        &self,
        revision_id: &str,
    ) -> crate::Result<Option<FrozenTurnOperatingSkillRow>> {
        let rows = sqlx::query(
            "SELECT id, skill_key, canonical_body, content_digest
             FROM operating_skill_revision WHERE id = ?",
        )
        .bind(revision_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| FrozenTurnOperatingSkillRow { row }))
    }
}

impl SqliteDb {
    pub async fn main_turn_genesis_instruction_at_admission(
        &self,
        chat_id: &str,
        admitted_at: &str,
    ) -> crate::Result<Option<MainTurnGenesisInstructionRow>> {
        let rows = sqlx::query("SELECT instruction.id AS instruction_id, instruction.revision,
                                instruction.body,
                                genesis.id AS genesis_id, genesis.version AS genesis_version,
                                genesis.lifecycle AS genesis_lifecycle,
                                genesis.charter_id, genesis.charter_revision_id, genesis.charter_version,
                                revision.content_digest AS charter_content_digest,
                                revision.rendered_digest AS charter_render_digest
                         FROM agent_chat_instruction_revision AS instruction
                         JOIN product_genesis_session AS genesis
                           ON genesis.id = instruction.source_id
                          AND genesis.main_chat_id = instruction.chat_id
                         LEFT JOIN project_charter_revision AS revision
                           ON revision.id = genesis.charter_revision_id
                         WHERE instruction.chat_id = ?
                           AND instruction.source_type = 'native'
                           AND instruction.created_at <= ?
                           AND genesis.lifecycle IN (
                               'discovering', 'ready_for_project', 'handed_off', 'cancelled'
                           )
                         ORDER BY instruction.revision DESC, instruction.id DESC
                         LIMIT 1",)
.bind(chat_id)
.bind(admitted_at)
.fetch_optional(self.pool()).await?;
        Ok(rows.map(|row| MainTurnGenesisInstructionRow { row }))
    }
}

impl SqliteDb {
    pub async fn main_turn_active_genesis_instruction(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<MainTurnGenesisInstructionRow>> {
        let rows = sqlx::query("SELECT instruction.id AS instruction_id, instruction.revision,
                                instruction.body,
                                genesis.id AS genesis_id, genesis.version AS genesis_version,
                                genesis.lifecycle AS genesis_lifecycle,
                                genesis.charter_id, genesis.charter_revision_id, genesis.charter_version,
                                revision.content_digest AS charter_content_digest,
                                revision.rendered_digest AS charter_render_digest
                         FROM agent_chat_instruction_revision AS instruction
                         JOIN product_genesis_session AS genesis
                           ON genesis.id = instruction.source_id
                          AND genesis.main_chat_id = instruction.chat_id
                         LEFT JOIN project_charter_revision AS revision
                           ON revision.id = genesis.charter_revision_id
                         WHERE instruction.chat_id = ?
                           AND instruction.source_type = 'native'
                           AND genesis.lifecycle IN ('discovering', 'ready_for_project')
                         ORDER BY instruction.revision DESC, instruction.id DESC
                         LIMIT 1",)
.bind(chat_id)
.fetch_optional(self.pool()).await?;
        Ok(rows.map(|row| MainTurnGenesisInstructionRow { row }))
    }
}

impl SqliteDb {
    pub async fn main_turn_frozen_operating_skill(
        &self,
        revision_id: &str,
    ) -> crate::Result<Option<MainTurnOperatingSkillRow>> {
        let rows = sqlx::query(
            "SELECT sr.id AS current_revision_id,
                                    sr.id, sr.skill_key, sr.revision,
                                    sr.schema_version, sr.render_version,
                                    sr.canonical_body, sr.policy_json,
                                    sr.policy_digest, sr.content_digest
                             FROM operating_skill_revision AS sr
                             WHERE sr.id = ?",
        )
        .bind(revision_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MainTurnOperatingSkillRow { row }))
    }
}

impl SqliteDb {
    pub async fn main_turn_active_operating_skill(
        &self,
        skill_key: &str,
    ) -> crate::Result<Option<MainTurnOperatingSkillRow>> {
        let rows = sqlx::query(
            "SELECT os.current_revision_id,
                                    sr.id, sr.skill_key, sr.revision,
                                    sr.schema_version, sr.render_version,
                                    sr.canonical_body, sr.policy_json,
                                    sr.policy_digest, sr.content_digest
                             FROM operating_skill AS os
                             JOIN operating_skill_revision AS sr
                               ON sr.id = os.current_revision_id
                             WHERE os.id = ? AND os.lifecycle = 'active'",
        )
        .bind(skill_key)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MainTurnOperatingSkillRow { row }))
    }
}

impl SqliteDb {
    pub async fn main_turn_active_genesis_id(
        &self,
        chat_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT id
                             FROM product_genesis_session
                             WHERE main_chat_id = ?
                               AND lifecycle IN ('discovering', 'ready_for_project')
                             ORDER BY version DESC, id DESC
                             LIMIT 1",
        )
        .bind(chat_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn project_turn_setup_operating_skill(
        &self,
        skill_key: &str,
    ) -> crate::Result<Option<ProjectTurnOperatingSkillRow>> {
        let rows = sqlx::query(
            "SELECT sr.id, sr.skill_key, sr.revision,
                        sr.schema_version, sr.render_version, sr.canonical_body,
                        sr.policy_json, sr.policy_digest, sr.content_digest,
                        os.current_revision_id
                 FROM operating_skill_revision AS sr
                 JOIN operating_skill AS os ON os.id = sr.operating_skill_id
                  AND os.current_revision_id = sr.id
                 WHERE os.lifecycle = 'active' AND sr.skill_key = ?
                 ORDER BY sr.revision DESC, sr.id DESC LIMIT 1",
        )
        .bind(skill_key)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectTurnOperatingSkillRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_turn_charter_approval(
        &self,
        project_id: &str,
        charter_id: &str,
        revision_id: &str,
        approval_id: Option<&str>,
    ) -> crate::Result<Option<ProjectTurnCharterApprovalRow>> {
        let rows = sqlx::query(
            "SELECT c.id AS charter_id, c.account_id AS charter_account_id,
                    c.current_approved_revision_id,
                    c.project_mode, c.version AS charter_version,
                    r.id AS charter_revision_id, r.lifecycle AS charter_revision_lifecycle,
                    r.content_digest AS charter_content_digest,
                    r.rendered_digest AS charter_render_digest,
                    a.id AS approval_id, a.lifecycle AS approval_lifecycle,
                    a.content_digest AS approval_content_digest,
                    a.rendered_digest AS approval_render_digest,
                    a.expected_charter_version,
                    a.selected_identity_id, a.selected_profile_id,
                    a.selected_operating_skill_revision_id,
                    a.selected_policy_revision, a.selected_policy_digest,
                    a.approval_event_id,
                    a.consumed_project_id,
                    a.approving_principal_type,
                    a.approving_principal_id,
                    a.authorization_basis AS approval_authorization_basis,
                    a.authorization_action AS approval_authorization_action,
                    a.explicit_event AS approval_authorization_event_id,
                    a.authorization_occurred_at AS approval_authorization_occurred_at,
                    a.created_at AS approval_created_at
             FROM project_charter AS c
             JOIN project_charter_revision AS r
               ON r.id = c.current_approved_revision_id
              AND r.lifecycle = 'approved'
             JOIN project_charter_approval AS a
               ON a.charter_id = c.id AND a.revision_id = r.id
              AND a.lifecycle IN ('active', 'consumed')
             WHERE c.project_id = ? AND c.id = ? AND r.id = ? AND a.id = ?
             ORDER BY a.created_at DESC, a.id DESC
             LIMIT 1",
        )
        .bind(project_id)
        .bind(charter_id)
        .bind(revision_id)
        .bind(approval_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectTurnCharterApprovalRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_turn_selected_operating_skill(
        &self,
        revision_id: &str,
    ) -> crate::Result<Option<ProjectTurnOperatingSkillRow>> {
        let rows = sqlx::query(
            "SELECT sr.id, sr.skill_key, sr.revision,
                    sr.schema_version, sr.render_version, sr.canonical_body,
                    sr.policy_json, sr.policy_digest, sr.content_digest,
                    os.current_revision_id
             FROM operating_skill_revision AS sr
             JOIN operating_skill AS os ON os.id = sr.operating_skill_id
             WHERE sr.id = ?",
        )
        .bind(revision_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectTurnOperatingSkillRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_turn_approval_event(
        &self,
        approval_id: &str,
        event_id: &str,
        owner_id: &str,
    ) -> crate::Result<Option<ProjectTurnApprovalEventRow>> {
        let rows = sqlx::query(
            "SELECT principal_type, principal_id, authorization_basis, action,
                    explicit_event, occurred_at
             FROM project_charter_approval_event
             WHERE approval_id = ? AND id = ? AND principal_type = 'user'
               AND principal_id = ? LIMIT 1",
        )
        .bind(approval_id)
        .bind(event_id)
        .bind(owner_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectTurnApprovalEventRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_turn_admission_receipt(
        &self,
        receipt_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ProjectTurnAdmissionReceiptRow>> {
        let rows = sqlx::query(
            "SELECT id, project_id, source_kind, handoff_id,
                        initial_charter_approval_id, initial_charter_id,
                        initial_charter_revision_id, payload_digest,
                        validation_schema_version
                 FROM project_admission_receipt
                 WHERE id = ? AND project_id = ?",
        )
        .bind(receipt_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectTurnAdmissionReceiptRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_turn_adoption_approval_digest(
        &self,
        approval_id: &str,
        charter_id: &str,
        revision_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT content_digest FROM project_charter_approval
                         WHERE id = ? AND charter_id = ? AND revision_id = ?
                           AND approval_type = 'adoption' AND lifecycle = 'consumed'
                           AND consumed_project_id = ?",
        )
        .bind(approval_id)
        .bind(charter_id)
        .bind(revision_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn admission_current_operating_skill_revision(
        &self,
        skill_key: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
         WHERE skill.skill_key = ? AND skill.lifecycle = 'active'
         LIMIT 1",
        )
        .bind(skill_key)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn project_agent_active_operating_skill_revision(
        &self,
        skill_key: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
          AND revision.skill_key = skill.skill_key
         WHERE skill.skill_key = ?
           AND skill.lifecycle = 'active'
           AND skill.current_revision_id IS NOT NULL
         LIMIT 1",
        )
        .bind(skill_key)
        .fetch_optional(self.pool())
        .await?)
    }
}
