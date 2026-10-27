//! Users repository tests.
use super::*;

#[tokio::test]
async fn user_crud() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user = User {
        id: new_uuid_v4(),
        email: "crud@example.com".to_owned(),
        password_hash: "hash".to_owned(),
        display_name: Some("Test User".to_owned()),
        is_admin: false,
        created_at: now.clone(),
        updated_at: now,
    };

    UserRepo::create_user(&db, &user)
        .await
        .expect("creates user");

    let by_id = UserRepo::get_user_by_id(&db, &user.id)
        .await
        .expect("no error")
        .expect("user exists");
    assert_eq!(by_id.email, user.email);
    assert_eq!(by_id.display_name.as_deref(), Some("Test User"));

    let by_email = UserRepo::get_user_by_email(&db, &user.email)
        .await
        .expect("no error")
        .expect("user found by email");
    assert_eq!(by_email.id, user.id);

    let deleted = UserRepo::delete_user(&db, &user.id)
        .await
        .expect("no error");
    assert!(deleted);

    let gone = UserRepo::get_user_by_id(&db, &user.id)
        .await
        .expect("no error");
    assert!(gone.is_none());
}

#[tokio::test]
async fn user_email_uniqueness() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let make_user = |id: String| User {
        id,
        email: "dup@example.com".to_owned(),
        password_hash: "hash".to_owned(),
        display_name: None,
        is_admin: false,
        created_at: now.clone(),
        updated_at: now.clone(),
    };

    UserRepo::create_user(&db, &make_user(new_uuid_v4()))
        .await
        .expect("first user creates");
    let err = UserRepo::create_user(&db, &make_user(new_uuid_v4())).await;
    assert!(err.is_err(), "duplicate email must fail");
}

#[tokio::test]
async fn refresh_token_lifecycle() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    let token = RefreshToken {
        id: new_uuid_v4(),
        user_id: user_id.clone(),
        token_hash: "hash-abc".to_owned(),
        family_id: "family-1".to_owned(),
        expires_at: "2099-01-01T00:00:00Z".to_owned(),
        created_at: now,
    };
    RefreshTokenRepo::create_refresh_token(&db, &token)
        .await
        .expect("creates token");

    let found = RefreshTokenRepo::delete_refresh_token_by_hash(&db, "hash-abc")
        .await
        .expect("no error")
        .expect("token returned on first delete");
    assert_eq!(found.user_id, user_id);

    let not_found = RefreshTokenRepo::delete_refresh_token_by_hash(&db, "hash-abc")
        .await
        .expect("no error");
    assert!(not_found.is_none(), "token must not exist after deletion");
}

#[tokio::test]
async fn refresh_token_concurrent_single_winner() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    let token = RefreshToken {
        id: new_uuid_v4(),
        user_id,
        token_hash: "race-hash".to_owned(),
        family_id: "family-race".to_owned(),
        expires_at: "2099-01-01T00:00:00Z".to_owned(),
        created_at: now,
    };
    RefreshTokenRepo::create_refresh_token(&db, &token)
        .await
        .expect("creates token");

    // Two concurrent DELETE RETURNING on the same hash: exactly one wins.
    let (r1, r2) = tokio::join!(
        RefreshTokenRepo::delete_refresh_token_by_hash(&db, "race-hash"),
        RefreshTokenRepo::delete_refresh_token_by_hash(&db, "race-hash"),
    );
    let r1 = r1.expect("no error on r1");
    let r2 = r2.expect("no error on r2");

    let winners = [r1.is_some(), r2.is_some()].iter().filter(|&&b| b).count();
    assert_eq!(
        winners, 1,
        "exactly one concurrent caller must win the token"
    );
}

#[tokio::test]
async fn refresh_token_family_revocation() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    for i in 0..3 {
        RefreshTokenRepo::create_refresh_token(
            &db,
            &RefreshToken {
                id: new_uuid_v4(),
                user_id: user_id.clone(),
                token_hash: format!("fam-hash-{i}"),
                family_id: "family-revoke".to_owned(),
                expires_at: "2099-01-01T00:00:00Z".to_owned(),
                created_at: now.clone(),
            },
        )
        .await
        .expect("creates token");
    }
    RefreshTokenRepo::create_refresh_token(
        &db,
        &RefreshToken {
            id: new_uuid_v4(),
            user_id: user_id.clone(),
            token_hash: "other-fam-hash".to_owned(),
            family_id: "family-other".to_owned(),
            expires_at: "2099-01-01T00:00:00Z".to_owned(),
            created_at: now,
        },
    )
    .await
    .expect("creates other token");

    let deleted = RefreshTokenRepo::delete_refresh_tokens_by_family(&db, "family-revoke")
        .await
        .expect("no error");
    assert_eq!(deleted, 3);

    let remaining = RefreshTokenRepo::get_refresh_tokens_by_user(&db, &user_id)
        .await
        .expect("no error");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].family_id, "family-other");
}

#[tokio::test]
async fn refresh_token_expired_cleanup() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    RefreshTokenRepo::create_refresh_token(
        &db,
        &RefreshToken {
            id: new_uuid_v4(),
            user_id: user_id.clone(),
            token_hash: "expired-hash".to_owned(),
            family_id: "family-exp".to_owned(),
            expires_at: "2020-01-01T00:00:00Z".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("creates expired token");

    RefreshTokenRepo::create_refresh_token(
        &db,
        &RefreshToken {
            id: new_uuid_v4(),
            user_id: user_id.clone(),
            token_hash: "valid-hash".to_owned(),
            family_id: "family-valid".to_owned(),
            expires_at: "2099-01-01T00:00:00Z".to_owned(),
            created_at: now,
        },
    )
    .await
    .expect("creates valid token");

    let cleaned = RefreshTokenRepo::delete_expired_refresh_tokens(&db)
        .await
        .expect("no error");
    assert_eq!(cleaned, 1);

    let remaining = RefreshTokenRepo::get_refresh_tokens_by_user(&db, &user_id)
        .await
        .expect("no error");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].token_hash, "valid-hash");
}
