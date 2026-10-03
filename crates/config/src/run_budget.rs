/// Positive niceness increment applied only to run children on Unix.
pub const fn default_run_nice() -> u32 {
    10
}

pub fn logical_cores() -> usize {
    std::thread::available_parallelism().map_or(1, usize::from)
}

/// Unlimited admission still uses the automatic run cap to divide build jobs.
pub fn resolved_build_jobs_for_cores(jobs: Option<u32>, cap: Option<u32>, cores: usize) -> u32 {
    jobs.unwrap_or_else(|| {
        let cap = cap
            .filter(|cap| *cap > 0)
            .unwrap_or_else(|| crate::automatic_run_cap_for_cores(cores));
        u32::try_from((cores / cap as usize).max(1)).unwrap_or(u32::MAX)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunBudget {
    pub max_concurrent_runs: Option<u32>,
    pub build_jobs_per_run: Option<u32>,
    pub run_nice: u32,
}
impl Default for RunBudget {
    fn default() -> Self {
        Self {
            max_concurrent_runs: None,
            build_jobs_per_run: None,
            run_nice: default_run_nice(),
        }
    }
}
impl From<&crate::ServerConfig> for RunBudget {
    fn from(server: &crate::ServerConfig) -> Self {
        Self {
            max_concurrent_runs: server.max_concurrent_runs,
            build_jobs_per_run: server.build_jobs_per_run,
            run_nice: server.run_nice,
        }
    }
}
impl RunBudget {
    pub fn build_jobs(self) -> u32 {
        resolved_build_jobs_for_cores(
            self.build_jobs_per_run,
            self.max_concurrent_runs,
            logical_cores(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn automatic_budget_and_explicit_values() {
        for (cores, cap, expected) in [
            (8, None, 2),
            (8, Some(4), 2),
            (8, Some(2), 4),
            (8, Some(0), 2),
            (1, None, 1),
            (8, Some(20), 1),
            (0, None, 1),
        ] {
            assert_eq!(resolved_build_jobs_for_cores(None, cap, cores), expected);
            assert_eq!(resolved_build_jobs_for_cores(Some(0), cap, cores), 0);
            assert_eq!(resolved_build_jobs_for_cores(Some(7), cap, cores), 7);
        }
    }
}
