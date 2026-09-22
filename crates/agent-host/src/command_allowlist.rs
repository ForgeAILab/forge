//! Which programs a Forge Agent may spawn inside its own workspace.
//!
//! The allowlist is a blast-radius bound, not a sandbox: `bash` and `sh` are
//! on it because real build tooling needs them, and a shell can run anything
//! the worktree's own files can reach. What the list does buy is that a model
//! cannot reach for a tool the host never intended to expose — a package
//! publisher, a deployment CLI, a remote shell — and that the set is visible
//! and owner-controlled rather than implied by whatever happens to be on
//! `PATH`.
//!
//! The built-in set covers building and testing software. Anything that can
//! mount the host filesystem (`docker`, `podman`), open an outbound session
//! (`curl`, `wget`, `ssh`), or run a program chosen at runtime (`xargs`,
//! `env`) is deliberately absent and must be added by the owner.

use std::collections::BTreeSet;

/// Programs every Forge workspace allows without configuration.
pub const BUILTIN_COMMAND_ALLOWLIST: &[&str] = &[
    // Shells and core file/text inspection.
    "awk", "basename", "bash", "cat", "cp", "cut", "diff", "dirname", "echo", "false", "find",
    "grep", "head", "jq", "ls", "mkdir", "mv", "printf", "rg", "sed", "sh", "sort", "tail",
    "touch", "tr", "true", "uniq", "wc", // Rust.
    "cargo", "rustc", // JavaScript and TypeScript.
    "bun", "bunx", "deno", "eslint", "jest", "node", "npm", "npx", "pnpm", "prettier", "tsc",
    "vitest", "yarn", // Python.
    "pip", "pip3", "pipx", "poetry", "pytest", "python", "python3", "uv", "uvx",
    // JVM and Kotlin.
    "gradle", "java", "kotlinc", "mvn", "sbt", // Go, Swift, C/C++, and other toolchains.
    "cc", "clang", "cmake", "gcc", "go", "ninja", "swift", "zig",
    // Ruby, PHP, .NET, Dart, Elixir, Lua.
    "bundle", "composer", "dart", "dotnet", "elixir", "flutter", "gem", "lua", "mix", "php", "rake",
    "ruby", // Task runners and version control.
    "git", "just", "make",
];

/// The effective set of programs one composition may spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandAllowlist {
    programs: BTreeSet<String>,
}

impl Default for CommandAllowlist {
    fn default() -> Self {
        Self::builtin()
    }
}

impl CommandAllowlist {
    /// The built-in set, with nothing added or replaced.
    pub fn builtin() -> Self {
        Self {
            programs: BUILTIN_COMMAND_ALLOWLIST
                .iter()
                .map(|program| (*program).to_owned())
                .collect(),
        }
    }

    /// Resolves an effective allowlist from owner configuration.
    ///
    /// `only` replaces the built-in set outright; `allow` adds to whichever
    /// base applies. Entries that are not bare program names are dropped and
    /// returned, so a typo or a smuggled path (`../../bin/sh`) narrows
    /// nothing and stays visible to the caller instead of failing a turn or,
    /// worse, silently widening the set.
    pub fn resolve(only: Option<&[String]>, allow: &[String]) -> (Self, Vec<String>) {
        let mut rejected = Vec::new();
        let mut programs: BTreeSet<String> = match only {
            Some(exact) => Self::accept(exact, &mut rejected),
            None => BUILTIN_COMMAND_ALLOWLIST
                .iter()
                .map(|program| (*program).to_owned())
                .collect(),
        };
        programs.extend(Self::accept(allow, &mut rejected));
        (Self { programs }, rejected)
    }

    /// Layers a narrower scope's configuration over this one, by the same
    /// rules: a scope that declares `only` replaces the inherited set, and
    /// `allow` adds to it.
    pub fn layer(&self, only: Option<&[String]>, allow: &[String]) -> (Self, Vec<String>) {
        let mut rejected = Vec::new();
        let mut programs = match only {
            Some(exact) => Self::accept(exact, &mut rejected),
            None => self.programs.clone(),
        };
        programs.extend(Self::accept(allow, &mut rejected));
        (Self { programs }, rejected)
    }

    /// Whether `program` may be spawned.
    pub fn allows(&self, program: &str) -> bool {
        self.programs.contains(program)
    }

    /// The effective programs, in stable order.
    pub fn programs(&self) -> impl Iterator<Item = &str> {
        self.programs.iter().map(String::as_str)
    }

    /// How many programs the list holds.
    pub fn len(&self) -> usize {
        self.programs.len()
    }

    /// Whether the list denies everything.
    pub fn is_empty(&self) -> bool {
        self.programs.is_empty()
    }

    fn accept(candidates: &[String], rejected: &mut Vec<String>) -> BTreeSet<String> {
        let mut accepted = BTreeSet::new();
        for candidate in candidates {
            let program = candidate.trim();
            if is_bare_program_name(program) {
                accepted.insert(program.to_owned());
            } else {
                rejected.push(candidate.clone());
            }
        }
        accepted
    }
}

/// Whether `name` is a bare program name rather than a path or an argument.
///
/// The spawn path resolves the program through `PATH` inside the workspace,
/// so anything carrying a separator, a shell metacharacter, or whitespace is
/// not a program name and is refused here rather than at spawn time.
fn is_bare_program_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | '+')
        })
        && name != "."
        && name != ".."
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn the_builtin_set_covers_building_but_not_escaping() {
        let allowlist = CommandAllowlist::builtin();
        for expected in ["cargo", "git", "pnpm", "uv", "go", "make"] {
            assert!(allowlist.allows(expected), "{expected} must be built in");
        }
        for withheld in ["docker", "podman", "curl", "wget", "ssh", "xargs", "env"] {
            assert!(
                !allowlist.allows(withheld),
                "{withheld} must require explicit owner configuration"
            );
        }
    }

    #[test]
    fn allow_adds_to_the_builtin_set_and_only_replaces_it() {
        let (extended, rejected) = CommandAllowlist::resolve(None, &owned(&["docker"]));
        assert!(rejected.is_empty());
        assert!(extended.allows("docker") && extended.allows("cargo"));

        let (exact, rejected) = CommandAllowlist::resolve(Some(&owned(&["cargo"])), &[]);
        assert!(rejected.is_empty());
        assert!(exact.allows("cargo"));
        assert!(!exact.allows("git"), "`only` replaces the built-in set");
        assert_eq!(exact.len(), 1);
    }

    #[test]
    fn a_narrower_scope_layers_over_the_configured_set() {
        let (configured, _) = CommandAllowlist::resolve(None, &owned(&["docker"]));
        let (project, _) = configured.layer(None, &owned(&["terraform"]));
        assert!(project.allows("docker") && project.allows("terraform") && project.allows("cargo"));

        let (pinned, _) = configured.layer(Some(&owned(&["cargo", "git"])), &owned(&["just"]));
        assert!(pinned.allows("cargo") && pinned.allows("git") && pinned.allows("just"));
        assert!(
            !pinned.allows("docker"),
            "a project that pins `only` does not inherit the configured additions"
        );
    }

    #[test]
    fn a_path_or_argument_cannot_enter_the_list() {
        let mut malformed = owned(&["../../bin/sh", "/bin/sh", "sh -c", "sh;rm", "", ".."]);
        malformed.push("a".repeat(65));
        let (allowlist, rejected) = CommandAllowlist::resolve(Some(&owned(&["cargo"])), &malformed);
        assert_eq!(rejected.len(), 7, "every malformed entry is reported");
        assert_eq!(allowlist.len(), 1, "and none of them widened the list");
        assert!(allowlist.allows("cargo"));
    }
}
