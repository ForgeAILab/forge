use std::collections::{BTreeMap, BTreeSet};

use db::MemoryItem;

const MAX_QUERY_TERMS: usize = 8;
const MAX_RAW_QUERY_TERMS: usize = 32;
const MAX_QUERY_TERM_CHARS: usize = 64;
const RRF_K: u64 = 60;
const RRF_SCALE: u64 = 1_000_000;

const STOP_WORDS: &[&str] = &[
    "a", "about", "after", "an", "and", "are", "as", "at", "be", "before", "by", "can", "could",
    "did", "do", "does", "for", "from", "had", "has", "have", "how", "i", "in", "into", "is", "it",
    "of", "on", "or", "our", "should", "that", "the", "this", "to", "was", "we", "were", "what",
    "when", "where", "which", "why", "with", "would", "you", "your",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueryArm {
    pub query: String,
    pub matched_term: Option<String>,
    pub weight: u64,
}

#[derive(Debug, Clone)]
pub(super) struct RecallCandidate {
    pub item: MemoryItem,
    pub rrf_score: u64,
    pub matched_terms: BTreeSet<String>,
    pub all_terms_match: bool,
}

pub(super) fn query_arms(query: &str) -> (Vec<String>, Vec<QueryArm>) {
    let terms = query_terms(query);
    if terms.is_empty() {
        return (terms, Vec::new());
    }

    let exact_arm_count = if terms.len() > 1 { 1 } else { 0 };
    let mut arms = Vec::with_capacity(terms.len() + exact_arm_count);
    if terms.len() > 1 {
        arms.push(QueryArm {
            query: terms.join(" "),
            matched_term: None,
            // An item admitted by this arm contains every salient term. Give
            // that evidence more weight than any single-term arm.
            weight: 2,
        });
    }
    arms.extend(terms.iter().cloned().map(|term| QueryArm {
        query: term.clone(),
        matched_term: Some(term),
        weight: 1,
    }));
    (terms, arms)
}

pub(super) fn merge_ranked_items(
    candidates: &mut BTreeMap<String, RecallCandidate>,
    items: Vec<MemoryItem>,
    arm: &QueryArm,
) {
    for (index, item) in items.into_iter().enumerate() {
        let rank = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        let contribution = arm
            .weight
            .saturating_mul(RRF_SCALE / RRF_K.saturating_add(rank));
        let entry = candidates
            .entry(item.id.clone())
            .or_insert_with(|| RecallCandidate {
                item,
                rrf_score: 0,
                matched_terms: BTreeSet::new(),
                all_terms_match: false,
            });
        entry.rrf_score = entry.rrf_score.saturating_add(contribution);
        if let Some(term) = arm.matched_term.as_ref() {
            entry.matched_terms.insert(term.clone());
        } else {
            entry.all_terms_match = true;
        }
    }
}

pub(super) fn rank_candidates(
    query_terms: &[String],
    candidates: BTreeMap<String, RecallCandidate>,
) -> Vec<RecallCandidate> {
    let mut candidates = candidates.into_values().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        let left_text = lexical_score(query_terms, &left.item);
        let right_text = lexical_score(query_terms, &right.item);
        right
            .rrf_score
            .cmp(&left.rrf_score)
            .then_with(|| right_text.cmp(&left_text))
            .then_with(|| authority_rank(&right.item).cmp(&authority_rank(&left.item)))
            .then_with(|| {
                right
                    .item
                    .retention_priority
                    .cmp(&left.item.retention_priority)
            })
            .then_with(|| right.item.created_at.cmp(&left.item.created_at))
            .then_with(|| right.item.id.cmp(&left.item.id))
    });
    candidates
}

pub(super) fn selection_reason(candidate: &RecallCandidate, query_terms: &[String]) -> String {
    let matched = candidate.matched_terms.iter().cloned().collect::<Vec<_>>();
    if candidate.all_terms_match {
        return format!(
            "all {} salient query terms matched; fused lexical recall",
            query_terms.len()
        );
    }
    format!(
        "matched {}/{} salient query terms; fused lexical recall",
        matched.len(),
        query_terms.len()
    )
}

fn query_terms(query: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut all_terms = Vec::new();

    for raw in query.split_whitespace() {
        let Some(term) = normalize_query_term(raw) else {
            continue;
        };
        if !seen.insert(term.clone()) {
            continue;
        }
        all_terms.push(term);
        if all_terms.len() == MAX_RAW_QUERY_TERMS {
            break;
        }
    }

    if all_terms.is_empty() {
        return all_terms;
    }

    let salient = all_terms
        .iter()
        .filter(|term| !STOP_WORDS.contains(&term.as_str()))
        .take(MAX_QUERY_TERMS)
        .cloned()
        .collect::<Vec<_>>();
    if salient.is_empty() {
        all_terms.into_iter().take(MAX_QUERY_TERMS).collect()
    } else {
        salient
    }
}

fn normalize_query_term(raw: &str) -> Option<String> {
    let term = raw.trim_matches(|character: char| {
        character.is_whitespace()
            || matches!(
                character,
                '"' | '\'' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';' | '!' | '?'
            )
    });
    if term.is_empty() {
        return None;
    }
    let term = term
        .chars()
        .take(MAX_QUERY_TERM_CHARS)
        .collect::<String>()
        .to_lowercase();
    (!term.is_empty()).then_some(term)
}

fn lexical_score(query_terms: &[String], item: &MemoryItem) -> u64 {
    if query_terms.is_empty() {
        return 0;
    }

    let title = item.title.to_lowercase();
    let summary = item.summary.as_deref().unwrap_or_default().to_lowercase();
    let body = item.body.to_lowercase();
    let normalized_query = query_terms.join(" ");

    let mut score = 0_u64;
    if title.contains(&normalized_query) {
        score = score.saturating_add(800);
    }
    if summary.contains(&normalized_query) {
        score = score.saturating_add(400);
    }
    if body.contains(&normalized_query) {
        score = score.saturating_add(150);
    }

    let mut matched = 0_u64;
    for term in query_terms {
        let mut term_matched = false;
        if title.contains(term) {
            score = score.saturating_add(120);
            term_matched = true;
        }
        if summary.contains(term) {
            score = score.saturating_add(50);
            term_matched = true;
        }
        if body.contains(term) {
            score = score.saturating_add(10);
            term_matched = true;
        }
        if term_matched {
            matched = matched.saturating_add(1);
        }
    }

    let denominator = u64::try_from(query_terms.len()).unwrap_or(1).max(1);
    let all_terms_bonus = if matched == denominator { 300 } else { 0 };
    score
        .saturating_add(matched.saturating_mul(300) / denominator)
        .saturating_add(all_terms_bonus)
}

fn authority_rank(item: &MemoryItem) -> i64 {
    let base = match item.authority.as_str() {
        "decision" => 600,
        "procedure" => 500,
        "verified_fact" => 450,
        "proposal" => 300,
        "hypothesis" => 200,
        _ => 100,
    };
    base + item.retention_priority
}

#[cfg(test)]
mod tests {
    use db::MemoryItem;

    use super::{query_arms, rank_candidates, selection_reason, RecallCandidate};
    use std::collections::BTreeMap;

    #[test]
    fn recall_query_removes_stop_words_and_bounds_work() {
        let (terms, arms) = query_arms(
            "why do the task leases prevent collisions in the shared workspace after retries",
        );
        assert_eq!(
            terms,
            vec![
                "task".to_owned(),
                "leases".to_owned(),
                "prevent".to_owned(),
                "collisions".to_owned(),
                "shared".to_owned(),
                "workspace".to_owned(),
                "retries".to_owned(),
            ]
        );
        assert_eq!(arms.len(), 8);
        assert_eq!(arms[0].query, terms.join(" "));
        assert_eq!(arms[0].weight, 2);
    }

    #[test]
    fn multi_term_match_outranks_high_authority_single_term_match() {
        let target = memory(
            "target",
            "observation",
            10,
            "Task leases prevent workspace collisions",
            "Separate task workspaces keep agent changes isolated.",
        );
        let weak = memory(
            "weak",
            "decision",
            100,
            "Task policy",
            "The current task owner maintains a checklist.",
        );
        let mut map = BTreeMap::new();
        map.insert(
            target.id.clone(),
            RecallCandidate {
                item: target,
                rrf_score: 30_000,
                matched_terms: ["task", "leases", "collisions"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                all_terms_match: false,
            },
        );
        map.insert(
            weak.id.clone(),
            RecallCandidate {
                item: weak,
                rrf_score: 16_000,
                matched_terms: ["task"].into_iter().map(str::to_owned).collect(),
                all_terms_match: false,
            },
        );

        let terms = vec![
            "task".to_owned(),
            "leases".to_owned(),
            "prevent".to_owned(),
            "collisions".to_owned(),
        ];
        let ranked = rank_candidates(&terms, map);
        assert_eq!(ranked[0].item.id, "target");
        assert!(selection_reason(&ranked[0], &terms).contains("3/4"));
    }

    fn memory(
        id: &str,
        authority: &str,
        retention_priority: i64,
        title: &str,
        body: &str,
    ) -> MemoryItem {
        MemoryItem {
            row_id: 0,
            id: id.to_owned(),
            project_id: None,
            task_id: None,
            execution_id: None,
            scope_type: "project".to_owned(),
            scope_id: "project".to_owned(),
            visibility: "project".to_owned(),
            owner_identity_id: None,
            authority: authority.to_owned(),
            sensitivity: "internal".to_owned(),
            retention_priority,
            provenance_json: "{}".to_owned(),
            publication_source_id: None,
            supersedes_id: None,
            valid_from: None,
            valid_until: None,
            source_event_id: None,
            source_scope_type: Some("project".to_owned()),
            source_scope_id: Some("project".to_owned()),
            source_revision: Some("1".to_owned()),
            source_type: "test".to_owned(),
            kind: "observation".to_owned(),
            title: title.to_owned(),
            summary: None,
            body: body.to_owned(),
            metadata_json: "{}".to_owned(),
            confidence: Some("confirmed".to_owned()),
            quality_score: None,
            created_by_type: Some("test".to_owned()),
            created_by_id: None,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
        }
    }
}
