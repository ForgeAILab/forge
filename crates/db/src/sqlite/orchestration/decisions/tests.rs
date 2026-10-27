use super::*;

#[tokio::test]
async fn charter_accessors_preserve_distinct_null_and_decode_error_semantics() {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    let row = sqlx::query("SELECT NULL AS charter_revision_id")
        .fetch_one(&pool)
        .await
        .unwrap();
    let decision = ProjectDecisionCommandSourceRow { row };
    assert_eq!(decision.charter_revision_id().unwrap(), None);
    assert_eq!(decision.required_charter_revision_id().unwrap(), "");
    assert_eq!(
        decision.required_charter_revision_id().unwrap(),
        decision
            .row
            .try_get::<String, _>("charter_revision_id")
            .unwrap()
    );

    let row = sqlx::query("SELECT 'charter-revision' AS charter_revision_id")
        .fetch_one(&pool)
        .await
        .unwrap();
    let decision = ProjectDecisionCommandSourceRow { row };
    assert_eq!(
        decision.charter_revision_id().unwrap().as_deref(),
        Some("charter-revision")
    );
    assert_eq!(
        decision.required_charter_revision_id().unwrap(),
        decision
            .row
            .try_get::<String, _>("charter_revision_id")
            .unwrap()
    );

    let row = sqlx::query("SELECT CAST('charter-revision' AS BLOB) AS charter_revision_id")
        .fetch_one(&pool)
        .await
        .unwrap();
    let decision = ProjectDecisionCommandSourceRow { row };
    let optional_error = decision.charter_revision_id().unwrap_err();
    let required_error = decision.required_charter_revision_id().unwrap_err();
    let original_error = decision
        .row
        .try_get::<String, _>("charter_revision_id")
        .unwrap_err();
    assert!(matches!(
        optional_error,
        DbError::Sqlx(sqlx::Error::ColumnDecode { .. })
    ));
    assert!(matches!(
        required_error,
        DbError::Sqlx(sqlx::Error::ColumnDecode { .. })
    ));
    assert_eq!(
        required_error.to_string(),
        DbError::from(original_error).to_string()
    );
    assert_ne!(optional_error.to_string(), required_error.to_string());
}
