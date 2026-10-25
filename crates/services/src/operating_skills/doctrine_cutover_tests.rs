//! Doctrine cutover rendering and immutable historical revision tests.

use super::*;

#[test]
fn overrides_are_revision_bound_and_historical_rendering_is_exact() {
    for (current, historical, render) in [
        (
            MAIN_BASELINE_PROTOCOL,
            MAIN_BASELINE_PROTOCOL_V4,
            render_main_baseline_operating_skill(&MainBaselineSkillContext::default()),
        ),
        (
            MAIN_PROTOCOL,
            LEGACY_V7_MAIN_PROTOCOL,
            render_main_operating_skill(&MainOperatingSkillContext::default()).unwrap(),
        ),
        (
            PROJECT_PROTOCOL,
            LEGACY_V21_PROJECT_PROTOCOL,
            render_project_operating_skill(&ProjectOperatingSkillContext::new("p", "b")),
        ),
    ] {
        assert_eq!(current.matches("\n## SERVER-OWNED OVERRIDES\n").count(), 1);
        assert_eq!(render.matches("\n## SERVER-OWNED OVERRIDES\n").count(), 1);
        let mut expected = historical.to_owned();
        if current == MAIN_BASELINE_PROTOCOL {
            append_legacy_baseline_overrides(&mut expected);
        } else if current == MAIN_PROTOCOL {
            append_legacy_main_overrides(&mut expected);
        } else {
            append_legacy_project_overrides(&mut expected);
        }
        expected.push_str(render.strip_prefix(current).unwrap());
        assert_eq!(
            replace_rendered_skill_body(render.clone(), Some(historical), current).unwrap(),
            expected
        );
        // A future cutover keeps this revision's overrides as part of its
        // frozen body, rather than replacing them with older blanket rules.
        let future = format!("future protocol{}", render.strip_prefix(current).unwrap());
        assert_eq!(
            replace_rendered_skill_body(future, Some(current), "future protocol").unwrap(),
            render
        );
    }
    let project = render_project_operating_skill(&ProjectOperatingSkillContext::new("p", "b"));
    assert!(project.contains("disposable verification"));
    assert!(!project.contains("Never access a repository Workspace"));
    let main = render_main_operating_skill(&MainOperatingSkillContext::default()).unwrap();
    assert!(!main.contains("repository, Workspace, credential"));
}

#[tokio::test]
async fn migration_preserves_every_historical_revision_and_advances_only_current_pointers() {
    use sha2::{Digest, Sha256};
    let directory = tempfile::tempdir().unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../db/migrations");
    let cutover = "V202610120418__operation_registry_doctrine.sql";
    for entry in std::fs::read_dir(&source).unwrap() {
        let path = entry.unwrap().path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name == cutover || !name.ends_with(".sql") {
            continue;
        }
        let version = name
            .strip_prefix('V')
            .unwrap()
            .split("__")
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        if version < 202610120418 {
            std::fs::copy(&path, directory.path().join(name)).unwrap();
        }
    }
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations_from(&pool, directory.path())
        .await
        .unwrap();
    let before:Vec<(String,String,String,String)>=sqlx::query_as("SELECT id,canonical_body,content_digest,policy_json FROM operating_skill_revision ORDER BY id").fetch_all(&pool).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../db/migrations/V202610120418__operation_registry_doctrine.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    for row in before {
        let after:(String,String,String,String)=sqlx::query_as("SELECT id,canonical_body,content_digest,policy_json FROM operating_skill_revision WHERE id=?").bind(&row.0).fetch_one(&pool).await.unwrap();
        assert_eq!(after, row, "historical revision {} is immutable", row.0);
    }
    for (key, id, body) in [
        (
            MAIN_OPERATING_SKILL_KEY,
            "forge.main.project-discovery/v2@8",
            MAIN_PROTOCOL,
        ),
        (
            PROJECT_OPERATING_SKILL_KEY,
            "forge.project.orchestration/v1@22",
            PROJECT_PROTOCOL,
        ),
    ] {
        let (current,stored,digest):(String,String,String)=sqlx::query_as("SELECT s.current_revision_id,r.canonical_body,r.content_digest FROM operating_skill s JOIN operating_skill_revision r ON r.id=s.current_revision_id WHERE s.skill_key=?").bind(key).fetch_one(&pool).await.unwrap();
        assert_eq!(current, id);
        assert_eq!(stored, body);
        assert_eq!(digest, hex::encode(Sha256::digest(body.as_bytes())));
    }
}
