use std::sync::OnceLock;

/// Half the logical cores, with room for at least two runs on small machines.
pub fn automatic_run_cap_for_cores(logical_cores: usize) -> u32 {
    u32::try_from((logical_cores / 2).max(2)).unwrap_or(u32::MAX)
}

pub fn resolved_run_cap(configured: Option<u32>) -> u32 {
    configured.unwrap_or_else(|| {
        automatic_run_cap_for_cores(std::thread::available_parallelism().map_or(1, usize::from))
    })
}

/// Stable identity for the daemon running inside this server process.
pub fn embedded_machine_id() -> String {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                std::process::Command::new("hostname")
                    .output()
                    .ok()
                    .and_then(|output| String::from_utf8(output.stdout).ok())
                    .map(|value| value.trim().to_owned())
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "localhost".to_owned())
            });
        format!(
            "embedded:{hostname}:{}:{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })
    .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn machine_capacity_file_and_cli_precedence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forge.yaml");
        std::fs::write(&path, "server:\n  max_concurrent_runs: 3\n").unwrap();
        let cfg = crate::ForgeConfig::load(
            Some(&path),
            crate::ConfigOverrides {
                data_dir: Some(dir.path().join("data")),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(cfg.server.max_concurrent_runs, Some(3));
        let cfg = crate::ForgeConfig::load(
            Some(&path),
            crate::ConfigOverrides {
                server_max_concurrent_runs: Some(0),
                data_dir: Some(dir.path().join("data")),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(cfg.server.max_concurrent_runs, Some(0));
    }

    #[test]
    fn automatic_machine_run_cap() {
        assert_eq!(automatic_run_cap_for_cores(8), 4);
        assert_eq!(automatic_run_cap_for_cores(2), 2);
        assert_eq!(automatic_run_cap_for_cores(1), 2);
    }
}
