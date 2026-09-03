pub const DEFAULT_SERVER_BIND: &str = "127.0.0.1:0";
pub const DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS: u64 = 86_400;
pub const DEFAULT_AGENT_MAX_CONCURRENT_TASKS: u32 = 1;
pub const DEFAULT_AGENT_HEARTBEAT_INTERVAL_SECONDS: u64 = 30;
pub const DEFAULT_AGENT_MAX_MISSED_HEARTBEATS: u32 = 3;
pub const DEFAULT_BCRYPT_COST: u32 = 12;
pub const DEFAULT_CORS_ORIGIN: &str = "http://localhost:5173";
pub const DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES: u64 = 104_857_600;
/// The create-spark invocation Genesis provisioning runs when an approved
/// Charter carries a scaffold. Pinned so a Project's first commit is
/// reproducible; `bunx` resolves it, so `bun` is the only host dependency.
pub const DEFAULT_SCAFFOLD_COMMAND: &str = "bunx @forgeailab/create-spark@0.4.5";
