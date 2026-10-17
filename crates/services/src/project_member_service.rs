use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, CreateProjectMember, ProjectMember, ProjectMemberRepo, SqliteDb,
};

use crate::{Result, ServiceError};

#[derive(Clone)]
pub struct ProjectMemberService {
    db: Arc<SqliteDb>,
}

impl ProjectMemberService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    pub async fn list_members(
        &self,
        project_id: &str,
        caller_user_id: &str,
    ) -> Result<Vec<ProjectMember>> {
        self.check_access(project_id, caller_user_id).await?;
        ProjectMemberRepo::list_members(&*self.db, project_id)
            .await
            .map_err(Into::into)
    }

    pub async fn add_member(
        &self,
        project_id: &str,
        caller_user_id: &str,
        target_user_id: &str,
        role: &str,
    ) -> Result<ProjectMember> {
        let caller = self.require_member(project_id, caller_user_id).await?;
        if !is_admin_or_owner(&caller.role) {
            return Err(ServiceError::InvalidOperation {
                message: "insufficient_role".to_owned(),
            });
        }
        if caller.role == "admin" && role == "owner" {
            return Err(ServiceError::InvalidOperation {
                message: "not_owner".to_owned(),
            });
        }
        let now = now_rfc3339();
        ProjectMemberRepo::add_member(
            &*self.db,
            CreateProjectMember {
                id: new_uuid_v4(),
                project_id: project_id.to_owned(),
                user_id: target_user_id.to_owned(),
                role: role.to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn update_role(
        &self,
        project_id: &str,
        caller_user_id: &str,
        target_user_id: &str,
        role: &str,
    ) -> Result<ProjectMember> {
        let caller = self.require_member(project_id, caller_user_id).await?;
        let target = self.require_member(project_id, target_user_id).await?;
        if !is_admin_or_owner(&caller.role) {
            return Err(ServiceError::InvalidOperation {
                message: "insufficient_role".to_owned(),
            });
        }
        if caller.role == "admin" && (target.role == "owner" || role == "owner") {
            return Err(ServiceError::InvalidOperation {
                message: "not_owner".to_owned(),
            });
        }
        if target.role == "owner" && role != "owner" {
            let owners = self.owner_count(project_id).await?;
            if owners <= 1 {
                return Err(ServiceError::Conflict("last_owner".to_owned()));
            }
        }

        ProjectMemberRepo::update_member_role(
            &*self.db,
            project_id,
            target_user_id,
            role,
            &now_rfc3339(),
        )
        .await
        .map_err(Into::into)
    }

    pub async fn remove_member(
        &self,
        project_id: &str,
        caller_user_id: &str,
        target_user_id: &str,
    ) -> Result<()> {
        let caller = self.require_member(project_id, caller_user_id).await?;
        let target = self.require_member(project_id, target_user_id).await?;
        if !is_admin_or_owner(&caller.role) {
            return Err(ServiceError::InvalidOperation {
                message: "insufficient_role".to_owned(),
            });
        }
        if caller.role == "admin" && target.role == "owner" {
            return Err(ServiceError::InvalidOperation {
                message: "not_owner".to_owned(),
            });
        }
        if target.role == "owner" {
            let owners = self.owner_count(project_id).await?;
            if owners <= 1 {
                return Err(ServiceError::Conflict("last_owner".to_owned()));
            }
        }
        ProjectMemberRepo::remove_member(&*self.db, project_id, target_user_id).await?;
        Ok(())
    }

    pub async fn check_access(&self, project_id: &str, user_id: &str) -> Result<ProjectMember> {
        self.require_member(project_id, user_id).await
    }

    async fn require_member(&self, project_id: &str, user_id: &str) -> Result<ProjectMember> {
        ProjectMemberRepo::get_member(&*self.db, project_id, user_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))
    }

    async fn owner_count(&self, project_id: &str) -> Result<usize> {
        let members = ProjectMemberRepo::list_members(&*self.db, project_id).await?;
        Ok(members
            .iter()
            .filter(|member| member.role == "owner")
            .count())
    }
}

fn is_admin_or_owner(role: &str) -> bool {
    role == "owner" || role == "admin"
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations, SqliteDb};

    async fn test_service() -> ProjectMemberService {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        ProjectMemberService::new(Arc::new(SqliteDb::new(pool)))
    }

    async fn seed_user(db: &SqliteDb, user_id: &str, email: &str) {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, is_admin, created_at, updated_at) VALUES (?, ?, ?, 0, ?, ?)",
        )
        .bind(user_id)
        .bind(email)
        .bind("pw")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn seed_project(db: &SqliteDb, project_id: &str) {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO project (id, name, settings, workflow_definition, owner_id, created_at, updated_at) VALUES (?, 'p', '{}', '{}', ?, ?, ?)",
        )
        .bind(project_id)
        .bind("u_owner")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn add_member(db: &SqliteDb, project_id: &str, user_id: &str, role: &str) {
        let now = now_rfc3339();
        ProjectMemberRepo::add_member(
            db,
            CreateProjectMember {
                id: new_uuid_v4(),
                project_id: project_id.to_owned(),
                user_id: user_id.to_owned(),
                role: role.to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn admin_cannot_assign_owner() {
        let svc = test_service().await;
        let db = &svc.db;
        seed_user(db, "u_owner", "owner@test.com").await;
        seed_user(db, "u_admin", "admin@test.com").await;
        seed_user(db, "u_new", "new@test.com").await;
        seed_project(db, "p1").await;
        add_member(db, "p1", "u_owner", "owner").await;
        add_member(db, "p1", "u_admin", "admin").await;

        let result = svc.add_member("p1", "u_admin", "u_new", "owner").await;
        assert!(matches!(
            result,
            Err(ServiceError::InvalidOperation { message }) if message == "not_owner"
        ));
    }

    #[tokio::test]
    async fn prevents_removing_last_owner() {
        let svc = test_service().await;
        let db = &svc.db;
        seed_user(db, "u_owner", "owner@test.com").await;
        seed_user(db, "u_admin", "admin@test.com").await;
        seed_project(db, "p1").await;
        add_member(db, "p1", "u_owner", "owner").await;
        add_member(db, "p1", "u_admin", "admin").await;

        let result = svc.remove_member("p1", "u_owner", "u_owner").await;
        assert!(matches!(
            result,
            Err(ServiceError::Conflict(message)) if message == "last_owner"
        ));
    }

    #[tokio::test]
    async fn check_access_requires_membership() {
        let svc = test_service().await;
        let db = &svc.db;
        seed_user(db, "u_owner", "owner@test.com").await;
        seed_user(db, "u_other", "other@test.com").await;
        seed_project(db, "p1").await;
        add_member(db, "p1", "u_owner", "owner").await;

        let ok = svc.check_access("p1", "u_owner").await;
        assert!(ok.is_ok());

        let denied = svc.check_access("p1", "u_other").await;
        assert!(matches!(denied, Err(ServiceError::NotFound { .. })));
    }
}
