use std::{
    collections::HashSet,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use tokio::fs;

use crate::lifecycle::{LifecycleHookContext, LifecyclePlugin, PluginError, PluginResult};

const MAX_CONTEXT_CHARS: usize = 8_000;
const MAX_ENTRIES: usize = 5;
const SUPPORTED_EVENTS: [api_types::LifecycleEvent; 1] = [api_types::LifecycleEvent::BeforeWork];

pub struct KnowledgeInjectPlugin;

struct ParsedKnowledge {
    title: Option<String>,
    tags: Vec<String>,
    category: Option<String>,
    body: String,
}

struct KnowledgeEntry {
    title: String,
    tags: Vec<String>,
    category: Option<String>,
    body: String,
    relative_path: String,
    score: usize,
}

#[async_trait::async_trait]
impl LifecyclePlugin for KnowledgeInjectPlugin {
    fn name(&self) -> &str {
        "knowledge-inject"
    }

    fn supported_events(&self) -> &[api_types::LifecycleEvent] {
        &SUPPORTED_EVENTS
    }

    async fn execute(&self, ctx: &LifecycleHookContext) -> Result<PluginResult, PluginError> {
        let Some(base_path) = find_base_path(ctx).await? else {
            return Ok(PluginResult::Skipped {
                reason: "no_knowledge_base".to_owned(),
            });
        };

        let knowledge_dir = base_path.join("docs/knowledge");
        let task_keywords = tokenize(&ctx.task_title);
        let mut entries = collect_entries(&knowledge_dir, &base_path, &task_keywords).await?;

        entries.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.title.cmp(&right.title))
                .then_with(|| left.relative_path.cmp(&right.relative_path))
        });
        entries.truncate(MAX_ENTRIES);

        let forge_dir = base_path.join(".forge");
        fs::create_dir_all(&forge_dir)
            .await
            .map_err(|err| path_error("create knowledge context directory", &forge_dir, err))?;

        let output_path = forge_dir.join("knowledge-context.md");
        let content = truncate_chars(
            &format_knowledge_context(&ctx.task_title, &entries),
            MAX_CONTEXT_CHARS,
        );
        fs::write(&output_path, content)
            .await
            .map_err(|err| path_error("write knowledge context", &output_path, err))?;

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
        let mut files = workspace
            .read_files("docs/knowledge", 1024, 1024 * 1024)
            .await
            .map_err(|error| PluginError {
                message: error.to_string(),
            })?;
        let repository = !files
            .iter()
            .any(|file| file.path == "docs/knowledge/KNOWLEDGE.md");
        if repository {
            files = workspace
                .read_repository_files("docs/knowledge", 1024, 1024 * 1024)
                .await
                .map_err(|error| PluginError {
                    message: error.to_string(),
                })?;
        }
        if !files
            .iter()
            .any(|file| file.path == "docs/knowledge/KNOWLEDGE.md")
        {
            return Ok(PluginResult::Skipped {
                reason: "no_knowledge_base".into(),
            });
        }
        let keywords = tokenize(&ctx.task_title);
        let mut entries = Vec::new();
        for file in files {
            let path = Path::new(&file.path);
            if path.file_name().and_then(|name| name.to_str()) == Some("KNOWLEDGE.md")
                || path.extension().and_then(|extension| extension.to_str()) != Some("md")
            {
                continue;
            }
            let text = String::from_utf8(file.bytes).map_err(|error| PluginError {
                message: error.to_string(),
            })?;
            let parsed = parse_knowledge(&text);
            let title = parsed.title.unwrap_or_else(|| {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("Untitled")
                    .to_owned()
            });
            let searchable = format!(
                "{} {} {} {}",
                title,
                parsed.category.as_deref().unwrap_or_default(),
                parsed.tags.join(" "),
                parsed.body
            );
            let score = keywords.intersection(&tokenize(&searchable)).count();
            if score > 0 {
                entries.push(KnowledgeEntry {
                    title,
                    tags: parsed.tags,
                    category: parsed.category,
                    body: parsed.body,
                    relative_path: file.path,
                    score,
                });
            }
        }
        entries.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.title.cmp(&right.title))
                .then_with(|| left.relative_path.cmp(&right.relative_path))
        });
        entries.truncate(MAX_ENTRIES);
        let content = truncate_chars(
            &format_knowledge_context(&ctx.task_title, &entries),
            MAX_CONTEXT_CHARS,
        );
        let files = vec![api_types::WorkspaceFileContent {
            path: ".forge/knowledge-context.md".into(),
            bytes: content.into_bytes(),
        }];
        let result = if repository {
            workspace.write_repository_knowledge(files).await
        } else {
            workspace.write_knowledge(files, false, None).await
        };
        result.map_err(|error| PluginError {
            message: error.to_string(),
        })?;
        Ok(PluginResult::Success)
    }
}

async fn find_base_path(ctx: &LifecycleHookContext) -> Result<Option<PathBuf>, PluginError> {
    let mut candidates = Vec::new();
    if let Some(worktree_path) = ctx.worktree_path.as_ref() {
        candidates.push(PathBuf::from(worktree_path));
    }

    let repo_path = PathBuf::from(&ctx.repo_path);
    if candidates.iter().all(|path| path != &repo_path) {
        candidates.push(repo_path);
    }

    for base_path in candidates {
        let index_path = base_path.join("docs/knowledge/KNOWLEDGE.md");
        if path_exists(&index_path).await? {
            return Ok(Some(base_path));
        }
    }

    Ok(None)
}

async fn collect_entries(
    knowledge_dir: &Path,
    base_path: &Path,
    task_keywords: &HashSet<String>,
) -> Result<Vec<KnowledgeEntry>, PluginError> {
    let mut entries = Vec::new();
    let mut dirs = vec![knowledge_dir.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        let mut dir_entries = fs::read_dir(&dir)
            .await
            .map_err(|err| path_error("read knowledge directory", &dir, err))?;

        while let Some(entry) = dir_entries
            .next_entry()
            .await
            .map_err(|err| path_error("read knowledge directory entry", &dir, err))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|err| path_error("read knowledge file type", &path, err))?;

            if file_type.is_dir() {
                dirs.push(path);
                continue;
            }

            if !file_type.is_file() {
                continue;
            }

            if path.file_name().and_then(|name| name.to_str()) == Some("KNOWLEDGE.md") {
                continue;
            }

            if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
                continue;
            }

            let content = fs::read_to_string(&path)
                .await
                .map_err(|err| path_error("read knowledge file", &path, err))?;
            let parsed = parse_knowledge(&content);
            let title = parsed.title.unwrap_or_else(|| {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("Untitled")
                    .to_owned()
            });

            let mut searchable = String::new();
            searchable.push_str(&title);
            if let Some(category) = parsed.category.as_deref() {
                searchable.push(' ');
                searchable.push_str(category);
            }
            if !parsed.tags.is_empty() {
                searchable.push(' ');
                searchable.push_str(&parsed.tags.join(" "));
            }
            if !parsed.body.is_empty() {
                searchable.push(' ');
                searchable.push_str(&parsed.body);
            }

            let score = task_keywords.intersection(&tokenize(&searchable)).count();

            if score == 0 {
                continue;
            }

            let relative_path = path
                .strip_prefix(base_path)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();

            entries.push(KnowledgeEntry {
                title,
                tags: parsed.tags,
                category: parsed.category,
                body: parsed.body,
                relative_path,
                score,
            });
        }
    }

    Ok(entries)
}

fn parse_knowledge(content: &str) -> ParsedKnowledge {
    let normalized = content.replace("\r\n", "\n");
    let (frontmatter, body) = split_frontmatter(&normalized);

    let mut title = None;
    let mut tags = Vec::new();
    let mut category = None;
    let mut reading_tags = false;

    if let Some(frontmatter) = frontmatter {
        for raw_line in frontmatter.lines() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }

            if let Some(value) = line.strip_prefix("title:") {
                title = parse_scalar(value);
                reading_tags = false;
                continue;
            }

            if let Some(value) = line.strip_prefix("category:") {
                category = parse_scalar(value);
                reading_tags = false;
                continue;
            }

            if let Some(value) = line.strip_prefix("tags:") {
                let value = value.trim();
                if value.is_empty() {
                    reading_tags = true;
                } else {
                    tags.extend(parse_tags(value));
                    reading_tags = false;
                }
                continue;
            }

            if reading_tags {
                if let Some(value) = line.strip_prefix("- ") {
                    if let Some(tag) = parse_scalar(value) {
                        tags.push(tag);
                    }
                    continue;
                }

                if !raw_line.starts_with(' ') && !raw_line.starts_with('\t') {
                    reading_tags = false;
                }
            }
        }
    }

    ParsedKnowledge {
        title,
        tags,
        category,
        body: body.trim().to_owned(),
    }
}

fn split_frontmatter(content: &str) -> (Option<&str>, &str) {
    let Some(rest) = content.strip_prefix("---\n") else {
        return (None, content);
    };

    if let Some(end) = rest.find("\n---\n") {
        let frontmatter = &rest[..end];
        let body = &rest[end + 5..];
        return (Some(frontmatter), body);
    }

    if let Some(rest) = rest.strip_suffix("\n---") {
        return (Some(rest), "");
    }

    (None, content)
}

fn parse_scalar(value: &str) -> Option<String> {
    let trimmed = value.trim().trim_matches('"').trim_matches('\'').trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn parse_tags(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(trimmed);

    inner.split(',').filter_map(parse_scalar).collect()
}

fn tokenize(text: &str) -> HashSet<String> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter_map(|token| {
            let token = token.trim().to_ascii_lowercase();
            if token.len() >= 2 {
                Some(token)
            } else {
                None
            }
        })
        .collect()
}

fn format_knowledge_context(task_title: &str, entries: &[KnowledgeEntry]) -> String {
    let mut output = String::new();
    output.push_str("# Knowledge Context\n\n");
    output.push_str("Task: ");
    output.push_str(task_title);
    output.push('\n');

    if entries.is_empty() {
        output.push_str("\nNo relevant knowledge entries found.\n");
        return output;
    }

    for entry in entries {
        output.push_str("\n## ");
        output.push_str(&entry.title);
        output.push_str("\n\n");
        output.push_str("- Source: `");
        output.push_str(&entry.relative_path);
        output.push_str("`\n");

        if let Some(category) = entry.category.as_deref() {
            output.push_str("- Category: ");
            output.push_str(category);
            output.push('\n');
        }

        if !entry.tags.is_empty() {
            output.push_str("- Tags: ");
            output.push_str(&entry.tags.join(", "));
            output.push('\n');
        }

        output.push('\n');
        output.push_str(&entry.body);
        output.push('\n');
    }

    output
}

fn truncate_chars(content: &str, max_chars: usize) -> String {
    content.chars().take(max_chars).collect()
}

async fn path_exists(path: &Path) -> Result<bool, PluginError> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(path_error("check knowledge path", path, err)),
    }
}

fn path_error(action: &str, path: &Path, err: std::io::Error) -> PluginError {
    PluginError {
        message: format!("{action} at {}: {err}", path.display()),
    }
}
