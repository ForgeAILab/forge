use api_types::{
    AgentResponse, DaemonResponse, ProjectResponse, RepoLocationResponse, RepoResponse,
    TaskListItemResponse,
};
use serde::Serialize;
use tabled::Table;

pub fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub fn print_table_tasks(items: &[TaskListItemResponse]) {
    let rows = items
        .iter()
        .map(|value| {
            vec![
                short_id(&value.id),
                value.title.clone(),
                serialized_label(&value.status),
                value.priority.to_string(),
                value.updated_at.clone(),
            ]
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        Table::from_rows(&["ID", "Title", "Status", "Priority", "Updated"], rows)
    );
}

pub fn print_table_agents(items: &[AgentResponse]) {
    let rows = items
        .iter()
        .map(|value| {
            vec![
                short_id(&value.id),
                value.name.clone(),
                serialized_label(&value.status),
                value.executor_type.clone(),
                value.daemon_id.clone().unwrap_or_else(|| "auto".to_owned()),
            ]
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        Table::from_rows(&["ID", "Name", "Status", "Executor", "DaemonID"], rows)
    );
}

pub fn print_table_daemons(items: &[DaemonResponse]) {
    let rows = items
        .iter()
        .map(|value| {
            vec![
                short_id(&value.id),
                value.hostname.clone(),
                value.status.clone(),
                format!("{} / {}", value.os, value.arch),
                value
                    .last_report_at
                    .clone()
                    .unwrap_or_else(|| "never".to_owned()),
                daemon_disk_cell(value.disk.as_ref()),
            ]
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        Table::from_rows(
            &["ID", "Hostname", "Status", "Platform", "LastReport", "Disk"],
            rows
        )
    );
}

/// The free space of a machine's workspace filesystem, and `LOW` with what
/// it is short of while it is under its floor (no new worktree or check
/// starts there until it recovers). `-` before the first reading.
pub fn daemon_disk_cell(disk: Option<&api_types::MachineDisk>) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let Some(disk) = disk else {
        return "-".to_owned();
    };
    let free = format!("{:.1} GiB free", disk.facts.free_bytes as f64 / GIB);
    match disk.pressure {
        Some(kind) => format!(
            "LOW ({}) {free}, floor {:.1} GiB",
            kind.as_str(),
            disk.floor_bytes as f64 / GIB
        ),
        None => free,
    }
}

pub fn print_table_projects(items: &[ProjectResponse]) {
    let rows = items
        .iter()
        .map(|value| {
            vec![
                short_id(&value.id),
                value.name.clone(),
                value.updated_at.clone(),
            ]
        })
        .collect::<Vec<_>>();
    println!("{}", Table::from_rows(&["ID", "Name", "UpdatedAt"], rows));
}

pub fn print_table_repos(items: &[RepoResponse]) {
    let rows = items
        .iter()
        .map(|value| {
            vec![
                short_id(&value.id),
                value.name.clone(),
                repo_source(value),
                value.default_branch.clone(),
            ]
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        Table::from_rows(&["ID", "Name", "Source", "DefaultBranch"], rows)
    );
}

fn repo_source(value: &RepoResponse) -> String {
    value
        .remote_url
        .as_deref()
        .or(value.local_path.as_deref())
        .unwrap_or("—")
        .to_owned()
}

pub fn print_table_repo_locations(items: &[RepoLocationResponse]) {
    let rows = items
        .iter()
        .map(|location| {
            vec![
                location.id.clone(),
                serialized_label(&location.owner_kind),
                location.daemon_id.clone().unwrap_or_else(|| "-".to_owned()),
                location
                    .runtime_id
                    .clone()
                    .unwrap_or_else(|| "-".to_owned()),
                location.path.clone(),
                serialized_label(&location.kind),
                location.is_default.to_string(),
                serialized_label(&location.status),
                location.version.to_string(),
                location
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "-".to_owned()),
            ]
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        Table::from_rows(
            &[
                "ID",
                "Owner",
                "DaemonID",
                "RuntimeID",
                "Path",
                "Kind",
                "Default",
                "Status",
                "Version",
                "LastError"
            ],
            rows
        )
    );
}

fn short_id(value: &str) -> String {
    value.chars().take(8).collect()
}

fn serialized_label<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(value)) => value,
        Ok(value) => value.to_string(),
        Err(_) => "<unknown>".to_owned(),
    }
}

#[cfg(test)]
mod disk_cell_tests {
    use super::daemon_disk_cell;
    use api_types::{DiskFloor, MachineDisk, MachineDiskFacts};

    fn disk(free_gib: u64) -> MachineDisk {
        const GIB: u64 = 1024 * 1024 * 1024;
        MachineDisk::new(
            MachineDiskFacts {
                free_bytes: free_gib * GIB,
                total_bytes: 100 * GIB,
                free_inodes: None,
                total_inodes: None,
                measured_at: "2026-10-10T00:00:00Z".to_owned(),
                gc_state: None,
                compiler_cache_bytes: None,
            },
            &DiskFloor::of_bytes(10 * GIB, 0),
        )
    }

    #[test]
    fn daemon_table_shows_free_space_and_marks_a_machine_under_its_floor() {
        assert_eq!(daemon_disk_cell(None), "-");
        assert_eq!(daemon_disk_cell(Some(&disk(40))), "40.0 GiB free");
        assert_eq!(
            daemon_disk_cell(Some(&disk(4))),
            "LOW (bytes) 4.0 GiB free, floor 10.0 GiB"
        );
    }
}
