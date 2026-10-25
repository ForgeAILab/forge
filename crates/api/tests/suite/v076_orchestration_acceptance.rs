#![allow(dead_code)]

//! Focused V076 acceptance coverage for the approved Main/Project orchestration
//! flow.  These tests intentionally use the public API surface for mutations;
//! SQL is used only for read-only projections which have no compact API
//! projection (for example, counting the atomic handoff rows).

use crate::common;

use api_types::ProjectCharterContent;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use chrono::{Duration, Utc};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use tower::ServiceExt;

use helpers::*;

#[path = "v076_orchestration_acceptance/doctrine_upgrades.rs"]
mod doctrine_upgrades;
#[path = "v076_orchestration_acceptance/genesis_handoff.rs"]
mod genesis_handoff;
#[path = "v076_orchestration_acceptance/helpers.rs"]
mod helpers;
#[path = "v076_orchestration_acceptance/project_evidence.rs"]
mod project_evidence;
#[path = "v076_orchestration_acceptance/project_proposals.rs"]
mod project_proposals;
#[path = "v076_orchestration_acceptance/prompt_previews.rs"]
mod prompt_previews;
#[path = "v076_orchestration_acceptance/release_readiness.rs"]
mod release_readiness;
#[path = "v076_orchestration_acceptance/terminal_denials.rs"]
mod terminal_denials;
