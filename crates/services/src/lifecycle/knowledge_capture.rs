use std::path::{Path, PathBuf};

use chrono::Utc;
use tokio::{fs, process::Command};
use tracing::warn;

use crate::lifecycle::{LifecycleHookContext, LifecyclePlugin, PluginError, PluginResult};

const MAX_ENTRIES_PER_TASK: usize = 5;

const KEYWORDS_GOTCHA: &[&str] = &[
    "gotcha",
    "pitfall",
    "careful",
    "warning",
    "actually",
    "turns out",
    "doesn't work",
    "does not work",
    "workaround",
    "watch out",
    "subtle",
];
const KEYWORDS_PATTERN: &[&str] = &["pattern", "convention", "approach", "technique", "idiom"];
const KEYWORDS_ARCHITECTURE: &[&str] = &["architecture", "design", "structure", "module", "layout"];
const KEYWORDS_DEBUGGING: &[&str] = &[
    "debug",
    "fix",
    "root cause",
    "issue",
    "solution",
    "resolved",
    "stack trace",
];

pub struct KnowledgeCapturePlugin;

#[async_trait::async_trait]
impl LifecyclePlugin for KnowledgeCapturePlugin {
    fn name(&self) -> &str {
        "knowledge-capture"
    }

    fn supported_events(&self) -> &[api_types::LifecycleEvent] {
        &[api_types::LifecycleEvent::OnTaskDone]
    }

    async fn execute(&self, ctx: &LifecycleHookContext) -> Result<PluginResult, PluginError> {
        let log_dir = match ctx.log_dir.as_ref() {
            Some(d) if d.exists() => d.clone(),
            _ => {
                return Ok(PluginResult::Skipped {
                    reason: "no_log_dir".into(),
                });
            }
        };

        let base = match ctx.worktree_path.as_deref() {
            Some(p) if !p.is_empty() && Path::new(p).exists() => PathBuf::from(p),
            _ if !ctx.repo_path.is_empty() && Path::new(&ctx.repo_path).exists() => {
                PathBuf::from(&ctx.repo_path)
            }
            _ => {
                return Ok(PluginResult::Skipped {
                    reason: "no_repo_path".into(),
                });
            }
        };

        let snippets = extract_from_logs(&log_dir).await;
        if snippets.is_empty() {
            return Ok(PluginResult::Skipped {
                reason: "no_knowledge_found".into(),
            });
        }

        let knowledge_dir = base.join("docs").join("knowledge");
        ensure_knowledge_dir(&knowledge_dir).await?;

        let now = Utc::now().to_rfc3339();
        let mut written = Vec::new();

        for snippet in snippets.iter().take(MAX_ENTRIES_PER_TASK) {
            let filename = to_kebab_case(&snippet.title);
            let category_dir = knowledge_dir.join(&snippet.category);
            fs::create_dir_all(&category_dir)
                .await
                .map_err(|e| PluginError {
                    message: format!("create category dir: {e}"),
                })?;

            let file_path = category_dir.join(format!("{filename}.md"));
            if file_path.exists() {
                continue;
            }

            let entry_content = format!(
                "---\ntitle: \"{}\"\ncategory: {}\ntags:\n{}\ncreated_by: knowledge-capture\ntask_id: \"{}\"\ncreated_at: \"{}\"\nupdated_at: \"{}\"\n---\n\n{}\n",
                snippet.title,
                snippet.category,
                snippet
                    .tags
                    .iter()
                    .map(|t| format!("  - {t}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
                ctx.task_id,
                now,
                now,
                snippet.body,
            );

            fs::write(&file_path, &entry_content)
                .await
                .map_err(|e| PluginError {
                    message: format!("write entry {}: {e}", file_path.display()),
                })?;

            written.push(IndexEntry {
                title: snippet.title.clone(),
                category: snippet.category.clone(),
                filename: format!("{filename}.md"),
                tags: snippet.tags.clone(),
            });
        }

        if written.is_empty() {
            return Ok(PluginResult::Skipped {
                reason: "all_duplicates".into(),
            });
        }

        update_index(&knowledge_dir, &written).await?;

        if ctx.worktree_path.is_some() {
            let _ = git_commit(&base, &ctx.task_id).await;
        }

        Ok(PluginResult::Success)
    }

    async fn execute_in_workspace(
        &self,
        ctx: &LifecycleHookContext,
        workspace: &crate::workspace_backend::ResolvedWorkspace,
    ) -> Result<PluginResult, PluginError> {
        if workspace.placement.owner_kind == db::PlacementOwnerKind::Server {
            let mut ctx = ctx.clone();
            ctx.worktree_path = Some(
                workspace
                    .embedded_path()
                    .map_err(|error| PluginError {
                        message: error.to_string(),
                    })?
                    .to_string_lossy()
                    .into_owned(),
            );
            return self.execute(&ctx).await;
        }
        let Some(log_dir) = ctx.log_dir.as_ref().filter(|directory| directory.exists()) else {
            return Ok(PluginResult::Skipped {
                reason: "no_log_dir".into(),
            });
        };
        let snippets = extract_from_logs(log_dir).await;
        if snippets.is_empty() {
            return Ok(PluginResult::Skipped {
                reason: "no_knowledge_found".into(),
            });
        }
        let existing = workspace
            .read_files("docs/knowledge", 1024, 1024 * 1024)
            .await
            .map_err(|error| PluginError {
                message: error.to_string(),
            })?;
        let index = existing
            .iter()
            .find(|file| file.path == "docs/knowledge/KNOWLEDGE.md")
            .map(|file| String::from_utf8_lossy(&file.bytes).into_owned())
            .unwrap_or_else(|| "# Knowledge Base\n".into());
        let now = Utc::now().to_rfc3339();
        let mut files = Vec::new();
        let mut written = Vec::new();
        for snippet in snippets.iter().take(MAX_ENTRIES_PER_TASK) {
            let filename = format!("{}.md", to_kebab_case(&snippet.title));
            let path = format!("docs/knowledge/{}/{filename}", snippet.category);
            if existing.iter().any(|file| file.path == path) {
                continue;
            }
            let content = format!("---\ntitle: \"{}\"\ncategory: {}\ntags:\n{}\ncreated_by: knowledge-capture\ntask_id: \"{}\"\ncreated_at: \"{}\"\nupdated_at: \"{}\"\n---\n\n{}\n", snippet.title, snippet.category,
                snippet.tags.iter().map(|tag| format!("  - {tag}")).collect::<Vec<_>>().join("\n"), ctx.task_id, now, now, snippet.body);
            files.push(api_types::WorkspaceFileContent {
                path,
                bytes: content.into_bytes(),
            });
            written.push(IndexEntry {
                title: snippet.title.clone(),
                category: snippet.category.clone(),
                filename,
                tags: snippet.tags.clone(),
            });
        }
        if written.is_empty() {
            return Ok(PluginResult::Skipped {
                reason: "all_duplicates".into(),
            });
        }
        workspace
            .write_knowledge(files, true, None)
            .await
            .map_err(|error| PluginError {
                message: error.to_string(),
            })?;
        workspace
            .write_knowledge(
                vec![api_types::WorkspaceFileContent {
                    path: "docs/knowledge/KNOWLEDGE.md".into(),
                    bytes: updated_index(&index, &written).into_bytes(),
                }],
                false,
                Some(ctx.task_id.clone()),
            )
            .await
            .map_err(|error| PluginError {
                message: error.to_string(),
            })?;
        Ok(PluginResult::Success)
    }
}

struct Snippet {
    title: String,
    category: String,
    tags: Vec<String>,
    body: String,
}

struct IndexEntry {
    title: String,
    category: String,
    filename: String,
    tags: Vec<String>,
}

async fn extract_from_logs(log_dir: &Path) -> Vec<Snippet> {
    let mut snippets = Vec::new();
    let mut seen_titles = std::collections::HashSet::new();

    let entries = match std::fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(_) => return snippets,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if !name.ends_with(".jsonl") || name.starts_with("hook-") {
            continue;
        }
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        for line in content.lines() {
            let value: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            if kind != "assistant" && kind != "tool_result" {
                continue;
            }

            let text = extract_text(&value);
            if text.is_empty() {
                continue;
            }

            let text_lower = text.to_lowercase();

            for &kw in KEYWORDS_GOTCHA {
                if text_lower.contains(kw) {
                    if let Some(snippet) = build_snippet(&text, kw, "gotchas", &mut seen_titles) {
                        snippets.push(snippet);
                    }
                    break;
                }
            }

            for &kw in KEYWORDS_DEBUGGING {
                if text_lower.contains(kw) {
                    if let Some(snippet) = build_snippet(&text, kw, "debugging", &mut seen_titles) {
                        snippets.push(snippet);
                    }
                    break;
                }
            }

            for &kw in KEYWORDS_PATTERN {
                if text_lower.contains(kw) {
                    if let Some(snippet) = build_snippet(&text, kw, "patterns", &mut seen_titles) {
                        snippets.push(snippet);
                    }
                    break;
                }
            }

            for &kw in KEYWORDS_ARCHITECTURE {
                if text_lower.contains(kw) {
                    if let Some(snippet) =
                        build_snippet(&text, kw, "architecture", &mut seen_titles)
                    {
                        snippets.push(snippet);
                    }
                    break;
                }
            }

            if snippets.len() >= MAX_ENTRIES_PER_TASK {
                return snippets;
            }
        }
    }

    snippets
}

fn extract_text(value: &serde_json::Value) -> String {
    if let Some(text) = value.get("payload").and_then(|p| p.as_str()) {
        return text.to_owned();
    }
    if let Some(obj) = value.get("payload").and_then(|p| p.as_object()) {
        if let Some(text) = obj.get("text").and_then(|t| t.as_str()) {
            return text.to_owned();
        }
        if let Some(content) = obj.get("content").and_then(|c| c.as_str()) {
            return content.to_owned();
        }
    }
    String::new()
}

fn build_snippet(
    text: &str,
    keyword: &str,
    category: &str,
    seen: &mut std::collections::HashSet<String>,
) -> Option<Snippet> {
    let sentences: Vec<&str> = text
        .split(['.', '!', '\n'])
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let keyword_lower = keyword.to_lowercase();
    let relevant: Vec<&&str> = sentences
        .iter()
        .filter(|s| s.to_lowercase().contains(&keyword_lower))
        .collect();

    if relevant.is_empty() {
        return None;
    }

    let summary: String = relevant
        .iter()
        .take(3)
        .map(|s| **s)
        .collect::<Vec<_>>()
        .join(". ");
    if summary.len() < 20 {
        return None;
    }

    let title = generate_title(&summary);
    if seen.contains(&title) {
        return None;
    }
    seen.insert(title.clone());

    let tags = extract_tags(&summary, category);

    Some(Snippet {
        title,
        category: category.to_owned(),
        tags,
        body: if summary.len() > 500 {
            format!("{}...", &summary[..500])
        } else {
            summary
        },
    })
}

fn generate_title(text: &str) -> String {
    let words: Vec<&str> = text.split_whitespace().take(8).collect();
    let title = words.join(" ");
    if title.len() > 80 {
        format!("{}...", &title[..77])
    } else {
        title
    }
}

fn extract_tags(text: &str, category: &str) -> Vec<String> {
    let mut tags = vec![category.to_owned()];
    let lower = text.to_lowercase();
    let candidates = [
        "rust",
        "sql",
        "sqlx",
        "tokio",
        "async",
        "git",
        "merge",
        "review",
        "workflow",
        "ci",
        "timeout",
        "migration",
        "api",
        "frontend",
        "react",
    ];
    for candidate in &candidates {
        if lower.contains(candidate) {
            tags.push((*candidate).to_owned());
        }
    }
    tags.truncate(5);
    tags
}

fn to_kebab_case(title: &str) -> String {
    title
        .chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

async fn ensure_knowledge_dir(dir: &Path) -> Result<(), PluginError> {
    fs::create_dir_all(dir).await.map_err(|e| PluginError {
        message: format!("create knowledge dir: {e}"),
    })?;

    let index = dir.join("KNOWLEDGE.md");
    if !index.exists() {
        fs::write(&index, "# Knowledge Base\n")
            .await
            .map_err(|e| PluginError {
                message: format!("write KNOWLEDGE.md: {e}"),
            })?;
    }
    Ok(())
}

async fn update_index(knowledge_dir: &Path, entries: &[IndexEntry]) -> Result<(), PluginError> {
    let index_path = knowledge_dir.join("KNOWLEDGE.md");
    let existing = fs::read_to_string(&index_path).await.unwrap_or_default();
    let content = updated_index(&existing, entries);
    if content != existing {
        fs::write(&index_path, &content)
            .await
            .map_err(|e| PluginError {
                message: format!("update KNOWLEDGE.md: {e}"),
            })?;
    }
    Ok(())
}

fn updated_index(existing: &str, entries: &[IndexEntry]) -> String {
    let mut content = existing.to_owned();

    for entry in entries {
        let link = format!(
            "- [{}]({}/{}) — {}",
            entry.title,
            entry.category,
            entry.filename,
            entry.tags.join(", ")
        );
        if content.contains(&link) {
            continue;
        }
        let heading = format!("\n## {}\n", entry.category);
        if !content.contains(&heading) {
            content.push_str(&heading);
        }
        let insert_pos = content
            .find(&heading)
            .map(|p| p + heading.len())
            .unwrap_or(content.len());
        content.insert_str(insert_pos, &format!("{link}\n"));
    }

    content
}

pub(crate) async fn git_commit(base: &Path, task_id: &str) -> Result<(), String> {
    let add = Command::new("git")
        .args(["add", "docs/knowledge/"])
        .current_dir(base)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .map_err(|e| format!("git add: {e}"))?;

    if !add.status.success() {
        return Err(format!(
            "git add failed: {}",
            String::from_utf8_lossy(&add.stderr)
        ));
    }

    let diff = Command::new("git")
        .args(["diff", "--cached", "--quiet"])
        .current_dir(base)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .status()
        .await
        .map_err(|e| format!("git diff: {e}"))?;

    if diff.success() {
        return Ok(());
    }

    let msg = format!("docs: capture knowledge from task {task_id}");
    let commit = Command::new("git")
        .args(["commit", "-m", &msg])
        .current_dir(base)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .map_err(|e| format!("git commit: {e}"))?;

    if !commit.status.success() {
        warn!(
            stderr = %String::from_utf8_lossy(&commit.stderr),
            "knowledge capture git commit failed"
        );
    }
    Ok(())
}
