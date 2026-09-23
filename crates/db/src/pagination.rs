#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub total_count: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    pub cursor: Option<String>,
    pub limit: i64,
    pub include_total: bool,
    pub sort_by: SortBy,
    pub sort_order: SortOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortBy {
    CreatedAt,
    UpdatedAt,
    Priority,
    BoardPosition,
    Title,
    Status,
    Agent,
    TaskType,
    Id,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// The page size a caller gets when it asks for none.
pub const DEFAULT_PAGE_LIMIT: i64 = 20;

/// The largest page any surface will serve.
pub const MAX_PAGE_LIMIT: i64 = 100;

/// A sort name no surface knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownSortBy(pub String);

impl std::fmt::Display for UnknownSortBy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid sort_by: {}", self.0)
    }
}

/// Bounds a requested page size.
///
/// REST and MCP each carried this clamp, and a page size is not something two
/// surfaces should be free to disagree about: the bound exists to keep one
/// query from pulling an unbounded result set out of SQLite.
#[must_use]
pub fn clamp_page_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT)
}

/// Resolves a sort name shared by every surface, defaulting to newest-first.
///
/// Task listings accept more names than this; see [`task_sort_by_from_name`].
pub fn sort_by_from_name(value: Option<&str>) -> Result<SortBy, UnknownSortBy> {
    match value.unwrap_or("created_at") {
        "created_at" => Ok(SortBy::CreatedAt),
        "updated_at" => Ok(SortBy::UpdatedAt),
        "priority" => Ok(SortBy::Priority),
        "board_position" => Ok(SortBy::BoardPosition),
        "id" => Ok(SortBy::Id),
        other => Err(UnknownSortBy(other.to_owned())),
    }
}

/// Resolves a sort name for a Task listing, which orders by board columns as
/// well as by the shared names, and defaults to the board's own order.
///
/// A Task listing therefore accepts strictly more names than
/// [`sort_by_from_name`]. MCP deliberately serves only the shared set, so a
/// name accepted over REST may still be refused there.
pub fn task_sort_by_from_name(value: Option<&str>) -> Result<SortBy, UnknownSortBy> {
    match value.unwrap_or("board_position") {
        "title" => Ok(SortBy::Title),
        "status" => Ok(SortBy::Status),
        "agent" => Ok(SortBy::Agent),
        "task_type" => Ok(SortBy::TaskType),
        other => sort_by_from_name(Some(other)),
    }
}

/// How a Task listing sorts when the caller names no order: the board's own
/// column order, ascending, so a listing matches what the board shows.
#[must_use]
pub fn task_sort_defaults() -> (SortBy, SortOrder) {
    (SortBy::BoardPosition, SortOrder::Asc)
}

#[cfg(test)]
mod paging_tests {
    use super::*;

    #[test]
    fn a_page_size_is_bounded_whatever_a_surface_asks_for() {
        assert_eq!(clamp_page_limit(None), DEFAULT_PAGE_LIMIT);
        assert_eq!(clamp_page_limit(Some(0)), 1);
        assert_eq!(clamp_page_limit(Some(-5)), 1);
        assert_eq!(clamp_page_limit(Some(50)), 50);
        assert_eq!(clamp_page_limit(Some(10_000)), MAX_PAGE_LIMIT);
    }

    #[test]
    fn the_shared_sort_names_resolve_and_anything_else_is_named_back() {
        assert_eq!(sort_by_from_name(None).expect("default"), SortBy::CreatedAt);
        assert_eq!(
            sort_by_from_name(Some("priority")).expect("priority"),
            SortBy::Priority
        );
        let error = sort_by_from_name(Some("colour")).expect_err("unknown name");
        assert!(
            error.to_string().contains("colour"),
            "the refusal names what was asked for: {error}"
        );
    }

    #[test]
    fn a_task_listing_sorts_by_the_board_and_accepts_more_names() {
        assert_eq!(
            task_sort_by_from_name(None).expect("default"),
            SortBy::BoardPosition
        );
        assert_eq!(
            task_sort_defaults(),
            (SortBy::BoardPosition, SortOrder::Asc)
        );
        for (name, expected) in [
            ("title", SortBy::Title),
            ("status", SortBy::Status),
            ("agent", SortBy::Agent),
            ("task_type", SortBy::TaskType),
            ("created_at", SortBy::CreatedAt),
        ] {
            assert_eq!(
                task_sort_by_from_name(Some(name)).unwrap_or_else(|_| panic!("{name}")),
                expected
            );
        }
        // Task-only names stay out of the shared set.
        assert!(sort_by_from_name(Some("title")).is_err());
    }
}
