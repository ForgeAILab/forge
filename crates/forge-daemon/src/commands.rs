use std::{path::Path, sync::Arc};

use api_types::{
    DaemonErrorPayload, DaemonFrame, ExecutionCancelParams, ExecutionStartParams,
    ExecutionTerminalAckParams, FsBranchesParams, FsListParams, TerminalInputParams,
    TerminalResizeParams, TerminalStartParams, TerminalTerminateParams, INVALID_FRAME,
    METHOD_EXECUTION_CANCEL, METHOD_EXECUTION_START, METHOD_EXECUTION_TERMINAL_ACK,
    METHOD_FS_BRANCHES, METHOD_FS_LIST, METHOD_TERMINAL_INPUT, METHOD_TERMINAL_RESIZE,
    METHOD_TERMINAL_START, METHOD_TERMINAL_TERMINATE, UNSUPPORTED_METHOD,
};
use serde::{de::DeserializeOwned, Serialize};

const TERMINAL_UNAVAILABLE: &str = "terminal_unavailable";

#[allow(dead_code)]
pub async fn handle_request(frame: DaemonFrame, workspace_root: &Path) -> DaemonFrame {
    handle_request_with_terminal(frame, workspace_root, None, None).await
}

pub async fn handle_request_with_terminal(
    frame: DaemonFrame,
    workspace_root: &Path,
    terminal: Option<&Arc<crate::terminal::TerminalRuntime>>,
    daemon_runtime: Option<&Arc<forge_client::daemon_runtime::DaemonRuntime>>,
) -> DaemonFrame {
    let DaemonFrame::Request { id, method, params } = frame else {
        return error_frame(
            None,
            INVALID_FRAME,
            "daemon command handler expected a request frame",
            None,
        );
    };

    match method.as_str() {
        METHOD_TERMINAL_START => match terminal {
            Some(terminal) => match decode_params::<TerminalStartParams>(&id, params) {
                Ok(params) => match terminal.start(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            None => terminal_unavailable_frame(id),
        },
        METHOD_TERMINAL_INPUT => match terminal {
            Some(terminal) => match decode_params::<TerminalInputParams>(&id, params) {
                Ok(params) => match terminal.input(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            None => terminal_unavailable_frame(id),
        },
        METHOD_TERMINAL_RESIZE => match terminal {
            Some(terminal) => match decode_params::<TerminalResizeParams>(&id, params) {
                Ok(params) => match terminal.resize(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            None => terminal_unavailable_frame(id),
        },
        METHOD_TERMINAL_TERMINATE => match terminal {
            Some(terminal) => match decode_params::<TerminalTerminateParams>(&id, params) {
                Ok(params) => match terminal.terminate(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            None => terminal_unavailable_frame(id),
        },
        METHOD_FS_LIST => match decode_params::<FsListParams>(&id, params) {
            Ok(params) => match forge_client::daemon_fs::list_entries(params, workspace_root).await
            {
                Ok(result) => response_frame(id, result),
                Err(error) => DaemonFrame::Error {
                    id: Some(id),
                    error,
                },
            },
            Err(frame) => frame,
        },
        METHOD_FS_BRANCHES => match decode_params::<FsBranchesParams>(&id, params) {
            Ok(params) => {
                match forge_client::daemon_fs::list_branches(params, workspace_root).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                }
            }
            Err(frame) => frame,
        },
        METHOD_EXECUTION_START => match decode_params::<ExecutionStartParams>(&id, params) {
            Ok(params) => match daemon_runtime {
                Some(runtime) => match runtime.start(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                None => error_frame(
                    Some(id),
                    UNSUPPORTED_METHOD,
                    "execution.start is not available in this daemon command context",
                    None,
                ),
            },
            Err(frame) => frame,
        },
        METHOD_EXECUTION_CANCEL => match decode_params::<ExecutionCancelParams>(&id, params) {
            Ok(params) => match daemon_runtime {
                Some(runtime) => match runtime.cancel(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                None => error_frame(
                    Some(id),
                    UNSUPPORTED_METHOD,
                    "execution.cancel is not available in this daemon command context",
                    None,
                ),
            },
            Err(frame) => frame,
        },
        METHOD_EXECUTION_TERMINAL_ACK => {
            match decode_params::<ExecutionTerminalAckParams>(&id, params) {
                Ok(params) => match daemon_runtime {
                    Some(runtime) => match runtime.acknowledge_terminal(params).await {
                        Ok(result) => response_frame(id, result),
                        Err(error) => DaemonFrame::Error {
                            id: Some(id),
                            error,
                        },
                    },
                    None => error_frame(
                        Some(id),
                        UNSUPPORTED_METHOD,
                        "execution.terminal.ack is not available in this daemon command context",
                        None,
                    ),
                },
                Err(frame) => frame,
            }
        }
        _ => error_frame(
            Some(id),
            UNSUPPORTED_METHOD,
            format!("unsupported daemon command method: {method}"),
            None,
        ),
    }
}

fn terminal_unavailable_frame(id: String) -> DaemonFrame {
    error_frame(
        Some(id),
        TERMINAL_UNAVAILABLE,
        "terminal support is not available in this daemon command context",
        None,
    )
}

fn decode_params<T: DeserializeOwned>(
    id: &str,
    params: serde_json::Value,
) -> std::result::Result<T, DaemonFrame> {
    serde_json::from_value(params).map_err(|error| {
        error_frame(
            Some(id.to_owned()),
            INVALID_FRAME,
            format!("invalid daemon command params: {error}"),
            None,
        )
    })
}

fn response_frame<T: Serialize>(id: String, result: T) -> DaemonFrame {
    match serde_json::to_value(result) {
        Ok(result) => DaemonFrame::Response { id, result },
        Err(error) => error_frame(
            Some(id),
            INVALID_FRAME,
            format!("failed to serialize daemon command result: {error}"),
            None,
        ),
    }
}

fn error_frame(
    id: Option<String>,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> DaemonFrame {
    DaemonFrame::Error {
        id,
        error: DaemonErrorPayload {
            code: code.into(),
            message: message.into(),
            details,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use api_types::{
        DaemonErrorPayload, DaemonFrame, FsListResult, METHOD_FS_BRANCHES, METHOD_FS_LIST,
        PATH_GUARDRAIL, UNSUPPORTED_METHOD,
    };
    use serde_json::json;

    use super::{handle_request, handle_request_with_terminal};

    #[tokio::test]
    async fn test_fs_list_basic() {
        let dir = temp_dir("fs-list-basic");
        fs::create_dir_all(dir.join("src")).expect("create src");
        fs::write(dir.join("README.md"), "readme").expect("write file");
        fs::create_dir_all(dir.join(".git")).expect("create .git");
        fs::create_dir_all(dir.join("node_modules")).expect("create node_modules");
        fs::create_dir_all(dir.join("target")).expect("create target");
        fs::write(dir.join(".DS_Store"), "").expect("write .DS_Store");

        let frame = DaemonFrame::Request {
            id: "cmd-1".to_owned(),
            method: METHOD_FS_LIST.to_owned(),
            params: json!({ "path": dir.to_string_lossy() }),
        };

        let response = handle_request(frame, &dir).await;
        let DaemonFrame::Response { id, result } = response else {
            panic!("expected response frame");
        };
        assert_eq!(id, "cmd-1");
        let result: FsListResult = serde_json::from_value(result).expect("fs list result");
        let names = result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert!(names.contains(&"src"));
        assert!(names.contains(&"README.md"));
        assert!(!names.contains(&".git"));
        assert!(!names.contains(&"node_modules"));
        assert!(!names.contains(&"target"));
        assert!(!names.contains(&".DS_Store"));

        remove_dir(&dir);
    }

    #[tokio::test]
    async fn test_fs_list_rejects_absolute_path_outside_workspace() {
        let root = create_test_root("absolute-outside");
        let frame = fs_list_frame("cmd-absolute-outside", "/etc");

        let response = handle_request(frame, &root).await;
        assert_path_guardrail(response, "cmd-absolute-outside");

        remove_dir(&root);
    }

    #[tokio::test]
    async fn test_fs_branches_rejects_absolute_path_outside_workspace() {
        let root = create_test_root("branches-absolute-outside");
        let frame = DaemonFrame::Request {
            id: "cmd-branches-outside".to_owned(),
            method: METHOD_FS_BRANCHES.to_owned(),
            params: json!({ "path": "/etc" }),
        };

        let response = handle_request(frame, &root).await;
        assert_path_guardrail(response, "cmd-branches-outside");

        remove_dir(&root);
    }

    #[tokio::test]
    async fn test_fs_list_rejects_relative_traversal_outside_workspace() {
        let root = create_test_root("relative-traversal");
        let frame = fs_list_frame("cmd-relative-traversal", "../..");

        let response = handle_request(frame, &root).await;
        assert_path_guardrail(response, "cmd-relative-traversal");

        remove_dir(&root);
    }

    #[tokio::test]
    async fn test_fs_list_accepts_absolute_subdir_inside_workspace() {
        let root = create_test_root("absolute-inside");
        let subdir = root.join("project");
        fs::create_dir_all(&subdir).expect("create subdir");
        fs::write(subdir.join("README.md"), "readme").expect("write file");
        let frame = fs_list_frame("cmd-absolute-inside", &subdir.to_string_lossy());

        let response = handle_request(frame, &root).await;
        let DaemonFrame::Response { id, result } = response else {
            panic!("expected response frame");
        };
        assert_eq!(id, "cmd-absolute-inside");
        let result: FsListResult = serde_json::from_value(result).expect("fs list result");
        assert_eq!(PathBuf::from(result.path), subdir.canonicalize().unwrap());
        assert!(result.entries.iter().any(|entry| entry.name == "README.md"));

        remove_dir(&root);
    }

    #[tokio::test]
    async fn test_fs_list_rejects_home_path_outside_workspace() {
        let root = create_test_root("home-outside");
        let frame = fs_list_frame("cmd-home-outside", "~/something-not-in-root");

        let response = handle_request(frame, &root).await;
        assert_path_guardrail(response, "cmd-home-outside");

        remove_dir(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_fs_list_rejects_symlink_escape() {
        let root = create_test_root("symlink-root");
        let outside = create_test_root("symlink-outside");
        std::os::unix::fs::symlink(&outside, root.join("escape")).expect("create symlink");
        let frame = fs_list_frame("cmd-symlink-escape", "escape");

        let response = handle_request(frame, &root).await;
        assert_path_guardrail(response, "cmd-symlink-escape");

        remove_dir(&root);
        remove_dir(&outside);
    }

    #[tokio::test]
    async fn test_unknown_method() {
        let dir = temp_dir("unknown-method");
        fs::create_dir_all(&dir).expect("create temp dir");

        let frame = DaemonFrame::Request {
            id: "cmd-unknown".to_owned(),
            method: "unknown.method".to_owned(),
            params: json!({}),
        };

        let response = handle_request(frame, &dir).await;
        let DaemonFrame::Error { id, error } = response else {
            panic!("expected error frame");
        };
        assert_eq!(id.as_deref(), Some("cmd-unknown"));
        assert_eq!(error.code, UNSUPPORTED_METHOD);

        remove_dir(&dir);
    }

    #[tokio::test]
    async fn terminal_ack_is_routed_to_runtime_store() {
        let dir = create_test_root("terminal-ack");
        let (responses_tx, _responses_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = forge_client::daemon_runtime::DaemonRuntime::new(responses_tx, dir.clone());
        let frame = DaemonFrame::Request {
            id: "cmd-terminal-ack".to_owned(),
            method: api_types::METHOD_EXECUTION_TERMINAL_ACK.to_owned(),
            params: json!({
                "terminal_report_id": "missing-report",
                "execution_id": "execution-1"
            }),
        };

        let response = handle_request_with_terminal(frame, &dir, None, Some(&runtime)).await;
        let DaemonFrame::Response { result, .. } = response else {
            panic!("expected terminal ack response");
        };
        assert_eq!(result["acknowledged"], false);

        remove_dir(&dir);
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos();
        std::env::temp_dir()
            .join("forge-daemon-test")
            .join(format!("{name}-{}-{nanos}", std::process::id()))
    }

    fn create_test_root(name: &str) -> PathBuf {
        let dir = temp_dir(name);
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn fs_list_frame(id: &str, path: &str) -> DaemonFrame {
        DaemonFrame::Request {
            id: id.to_owned(),
            method: METHOD_FS_LIST.to_owned(),
            params: json!({ "path": path }),
        }
    }

    fn assert_path_guardrail(response: DaemonFrame, expected_id: &str) -> DaemonErrorPayload {
        let DaemonFrame::Error { id, error } = response else {
            panic!("expected error frame");
        };
        assert_eq!(id.as_deref(), Some(expected_id));
        assert_eq!(error.code, PATH_GUARDRAIL);
        error
    }

    fn remove_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }
}
