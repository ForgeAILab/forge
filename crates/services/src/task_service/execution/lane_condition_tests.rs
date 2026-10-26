use crate::worker_runtime::queue::lane_conditions::case;

#[tokio::test]
async fn planning_metadata() {
    for execution_id in [Some("execution"), None] {
        case!(
            &format!("planning-metadata-{}", execution_id.is_some()),
            "planning",
            f,
            {
                super::set_planning_awaiting_review_metadata(&f.db, &f.task, execution_id, true)
                    .await
                    .unwrap();
            }
        );
    }
}

#[tokio::test]
async fn publication_review_metadata() {
    for token in [None, Some("entry".to_owned())] {
        case!(
            &format!("publication-metadata-{}", token.is_some()),
            "planning",
            f,
            {
                let execution = f.execution(db::ExecutionStatus::Completed).await;
                let project = db::ProjectRepo::get_by_id(&*f.db, &f.task.project_id)
                    .await
                    .unwrap()
                    .unwrap();
                let claim = super::PlanPublicationClaim {
                    execution_id: execution.id.clone(),
                    state: f.task.status.clone(),
                    state_entry_token: token.clone(),
                    project_version: project.version,
                };
                db::TaskRepo::mutate_metadata(
                    &*f.db,
                    &f.task.id,
                    None,
                    vec![db::TaskMetadataMutation::Set {
                        key: super::PLAN_PUBLICATION_CLAIM_KEY.into(),
                        value: claim.value(),
                    }],
                    &db::now_rfc3339(),
                )
                .await
                .unwrap();
                let task = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
                    .await
                    .unwrap()
                    .unwrap();
                super::settle_plan_publication_for_review(&f.db, &task, &execution)
                    .await
                    .unwrap();
            }
        );
    }
}
