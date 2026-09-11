//! Ratatui renderer for the reducer state in [`crate::app`].
//!
//! Rendering is intentionally one-way: this module only reads `AppState` and
//! writes widgets to a frame.  All service calls, projection reads, and
//! command decisions happen outside the renderer.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame,
};

use crate::app::{
    ActivityItem, ActivityKind, AppState, AttentionItem, FocusTarget, LayoutMode, LiveActivity,
    MessageRole, ModalState, ProjectReadiness, ProjectTab, RuntimeState, SetupState, TaskState,
    TurnState,
};

const BG: Color = Color::Rgb(14, 18, 24);
const PANEL: Color = Color::Rgb(22, 28, 36);
const PANEL_ALT: Color = Color::Rgb(27, 34, 43);
const TEXT: Color = Color::Rgb(226, 232, 240);
const MUTED: Color = Color::Rgb(145, 157, 174);
const ACCENT: Color = Color::Rgb(93, 190, 255);
const SUCCESS: Color = Color::Rgb(105, 211, 145);
const WARNING: Color = Color::Rgb(255, 194, 92);
const DANGER: Color = Color::Rgb(255, 119, 119);

/// Render the complete Solo screen for the current terminal size.
pub fn render(frame: &mut Frame<'_>, state: &AppState) {
    let area = frame.area();
    if area.width == 0 || area.height == 0 {
        return;
    }
    frame.render_widget(Block::default().style(Style::default().bg(BG)), area);

    let footer_height = 1;
    let composer_height = composer_height(state, area);
    let [header, body, composer, footer] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(composer_height),
            Constraint::Length(footer_height),
        ])
        .areas(area);

    render_header(frame, header, state);
    match state.layout {
        LayoutMode::Wide => render_wide(frame, body, state),
        LayoutMode::Narrow => render_narrow(frame, body, state),
    }
    render_composer(frame, composer, state);
    render_footer(frame, footer, state);

    if let Some(modal) = &state.modal {
        render_modal(frame, area, modal, state);
    } else if matches!(
        state.setup,
        SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
    ) {
        render_setup_overlay(frame, area, state);
    }
}

/// Alias with a descriptive name for embedders that call the module directly.
pub fn render_app(frame: &mut Frame<'_>, state: &AppState) {
    render(frame, state);
}

fn composer_height(state: &AppState, area: Rect) -> u16 {
    // Keep enough room for a two-line draft and status, while allowing very
    // short terminals to preserve the composer over optional rail details.
    let desired = if state.composer.text.contains('\n') || state.composer.error.is_some() {
        5
    } else {
        4
    };
    desired.min(area.height.saturating_sub(5).max(3))
}

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let readiness = state.header.readiness.label();
    let runtime = state.header.runtime.label();
    let repository = if state.header.repository.is_empty() {
        "repository not resolved"
    } else {
        state.header.repository.as_str()
    };
    let project = if state.header.project.is_empty() {
        "Project pending"
    } else {
        state.header.project.as_str()
    };
    let agent = if state.header.agent.is_empty() {
        "Agent pending"
    } else {
        state.header.agent.as_str()
    };

    let title = Line::from(vec![
        Span::styled(
            " FORGE SOLO ",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Span::styled("· ", Style::default().fg(MUTED)),
        Span::styled(repository, Style::default().fg(TEXT)),
    ]);
    let status = Line::from(vec![
        Span::styled("Project ", Style::default().fg(MUTED)),
        Span::styled(project, Style::default().fg(TEXT)),
        Span::styled("  │  ", Style::default().fg(MUTED)),
        Span::styled("Agent ", Style::default().fg(MUTED)),
        Span::styled(agent, Style::default().fg(TEXT)),
        Span::styled("  │  ", Style::default().fg(MUTED)),
        Span::styled("[", Style::default().fg(MUTED)),
        Span::styled(readiness, readiness_style(&state.header.readiness)),
        Span::styled("] ", Style::default().fg(MUTED)),
        Span::styled("[", Style::default().fg(MUTED)),
        Span::styled(runtime, runtime_style(state.header.runtime)),
        Span::styled("]", Style::default().fg(MUTED)),
    ]);
    frame.render_widget(
        Paragraph::new(Text::from(vec![title, status]))
            .block(
                Block::default()
                    .borders(Borders::BOTTOM)
                    .border_style(Style::default().fg(PANEL_ALT)),
            )
            .style(Style::default().bg(BG).fg(TEXT)),
        area,
    );
}

fn render_wide(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    if area.width < 2 {
        render_chat(frame, area, state);
        return;
    }
    let columns = if state.rail.collapsed {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(1), Constraint::Length(2)])
            .split(area)
    } else {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(1), Constraint::Length(34)])
            .split(area)
    };
    render_chat(frame, columns[0], state);
    if state.rail.collapsed {
        frame.render_widget(
            Paragraph::new("P")
                .alignment(ratatui::layout::Alignment::Center)
                .block(
                    Block::default()
                        .borders(Borders::LEFT)
                        .border_style(Style::default().fg(PANEL_ALT)),
                )
                .style(Style::default().fg(ACCENT).bg(PANEL)),
            columns[1],
        );
    } else {
        render_rail(frame, columns[1], state);
    }
}

fn render_narrow(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let strip_height = 2.min(area.height);
    let project_height = if area.height >= 12 {
        6
    } else {
        3.min(area.height.saturating_sub(strip_height))
    };
    let [tabs, project, chat] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(strip_height),
            Constraint::Length(project_height),
            Constraint::Min(1),
        ])
        .areas(area);
    render_narrow_tabs(frame, tabs, state);
    render_rail(frame, project, state);
    render_chat(frame, chat, state);
}

fn render_narrow_tabs(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let tabs = [
        ProjectTab::Tasks,
        ProjectTab::Attention,
        ProjectTab::Approvals,
    ];
    let mut spans = vec![Span::styled(" ", Style::default())];
    for tab in tabs {
        let selected = tab == state.rail.tab;
        spans.push(Span::styled(
            format!("{} {} ", if selected { "▸" } else { "·" }, tab.label()),
            if selected {
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(MUTED)
            },
        ));
    }
    spans.push(Span::styled(
        "  (Ctrl-[ / Ctrl-])",
        Style::default().fg(MUTED),
    ));
    frame.render_widget(
        Paragraph::new(Line::from(spans))
            .block(
                Block::default()
                    .borders(Borders::BOTTOM)
                    .border_style(Style::default().fg(PANEL_ALT)),
            )
            .style(Style::default().bg(PANEL)),
        area,
    );
}

fn render_chat(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    if area.height == 0 {
        return;
    }
    let activity_height = if state.live_activity.is_some() {
        if state
            .live_activity
            .as_ref()
            .is_some_and(|activity| activity.expanded)
        {
            (area.height / 3).clamp(5, 9)
        } else {
            3.min(area.height)
        }
    } else {
        1.min(area.height)
    };
    let [activity, timeline] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(activity_height), Constraint::Min(1)])
        .areas(area);
    render_activity(frame, activity, state.live_activity.as_ref(), state);
    render_timeline(frame, timeline, state);
}

fn render_activity(
    frame: &mut Frame<'_>,
    area: Rect,
    activity: Option<&LiveActivity>,
    state: &AppState,
) {
    let focused = state.focus == FocusTarget::Activity;
    let border = if focused { ACCENT } else { PANEL_ALT };
    let Some(activity) = activity else {
        frame.render_widget(
            Paragraph::new(" Activity  ·  idle")
                .block(
                    Block::default()
                        .borders(Borders::BOTTOM)
                        .border_style(Style::default().fg(border)),
                )
                .style(Style::default().fg(MUTED).bg(PANEL)),
            area,
        );
        return;
    };

    let mut lines = Vec::new();
    let worker = if activity.worker.is_empty() {
        "worker pending"
    } else {
        activity.worker.as_str()
    };
    lines.push(Line::from(vec![
        Span::styled(
            format!(" {} ", activity.state.label()),
            activity_style(activity.state),
        ),
        Span::styled(
            if activity.state == TurnState::Failed
                && state
                    .retryable_turn
                    .as_ref()
                    .is_some_and(|turn| turn.turn_id == activity.turn_id)
            {
                "  [r retry]"
            } else {
                ""
            },
            Style::default().fg(ACCENT),
        ),
        Span::styled(
            if activity.expanded {
                "  [− details]"
            } else {
                "  [+ details]"
            },
            Style::default().fg(ACCENT),
        ),
        Span::styled(format!("  {}", activity.summary), Style::default().fg(TEXT)),
        Span::styled(format!("  ·  {}", worker), Style::default().fg(MUTED)),
    ]));

    if activity.expanded {
        for item in activity.items.iter().skip(state.scroll.activity_offset) {
            if item.kind == ActivityKind::Reasoning && !activity.reasoning_expanded {
                continue;
            }
            lines.push(activity_line(item));
        }
        if activity
            .items
            .iter()
            .any(|item| item.kind == ActivityKind::Reasoning)
            && !activity.reasoning_expanded
        {
            lines.push(Line::from(Span::styled(
                "   · reasoning collapsed (Shift+R to expand)",
                Style::default().fg(MUTED),
            )));
        }
    }
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(
                Block::default()
                    .borders(Borders::BOTTOM)
                    .border_style(Style::default().fg(border)),
            )
            .style(Style::default().bg(PANEL)),
        area,
    );
}

fn activity_line(item: &ActivityItem) -> Line<'static> {
    let status = item.status.marker();
    let label = if item.visibility.is_public() {
        item.label.clone()
    } else {
        "protected activity".to_owned()
    };
    let detail = if item.visibility.is_public() {
        if item.detail.is_empty() {
            "".to_owned()
        } else {
            format!("  {}", item.detail)
        }
    } else {
        "  details hidden".to_owned()
    };
    Line::from(vec![
        Span::styled(
            format!("   {:>4}  {:<7} ", status, item.kind.label()),
            activity_status_style(item.status),
        ),
        Span::styled(label, Style::default().fg(TEXT)),
        Span::styled(detail, Style::default().fg(MUTED)),
    ])
}

fn render_timeline(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let focused = state.focus == FocusTarget::Timeline;
    let border = if focused { ACCENT } else { PANEL_ALT };
    let mut lines: Vec<Line<'static>> = Vec::new();
    for message in &state.timeline {
        if !message.is_renderable() {
            continue;
        }
        let role_style = role_style(message.role);
        let attempt = message
            .attempt
            .map_or_else(String::new, |attempt| format!("  attempt {attempt}"));
        let mut first = true;
        for content_line in message.content.lines() {
            let prefix = if first {
                format!("{}{}  ", message.role.label(), attempt)
            } else {
                "             ".to_owned()
            };
            lines.push(Line::from(vec![
                Span::styled(prefix, role_style),
                Span::styled(content_line.to_owned(), Style::default().fg(TEXT)),
            ]));
            first = false;
        }
        if first {
            lines.push(Line::from(Span::styled(
                format!("{}{}", message.role.label(), attempt),
                role_style,
            )));
        }
        if !message.timestamp.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("             {}", message.timestamp),
                Style::default().fg(MUTED),
            )));
        }
        lines.push(Line::from(""));
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            if state.timeline.is_empty() {
                "No messages yet. Start with the Project outcome."
            } else {
                "Protected messages are omitted from the terminal."
            },
            Style::default().fg(MUTED),
        )));
    }

    // Paragraph scrolling is measured in rendered rows, while `lines` only
    // contains logical lines. Count after wrapping at the paragraph's inner
    // width so the live-tail offset reaches the newest content in narrow
    // terminals as well as wide ones.
    let paragraph = Paragraph::new(Text::from(lines))
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(if state.scroll.follow_tail {
                    " Chat  ↓ live tail"
                } else {
                    " Chat  ↑ scrollback"
                })
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border)),
        )
        .style(Style::default().bg(BG));
    let inner_height = area.height.saturating_sub(2) as usize;
    let inner_width = area.width.saturating_sub(2);
    let rendered_rows = paragraph.line_count(inner_width).saturating_sub(2);
    let max_offset = rendered_rows.saturating_sub(inner_height);
    // `timeline_offset` is intentionally measured back from the live tail:
    // PageUp/↑ increases it to reveal older rows, while PageDown/↓ brings it
    // back toward zero. Paragraph scrolls from the top, so convert that
    // user-facing offset before rendering.
    let from_tail = if state.scroll.follow_tail {
        0
    } else {
        state.scroll.timeline_offset.min(max_offset)
    };
    let offset = max_offset.saturating_sub(from_tail) as u16;
    frame.render_widget(paragraph.scroll((offset, 0)), area);
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let focused = state.focus == FocusTarget::Composer;
    let border = if focused { ACCENT } else { PANEL_ALT };
    let status = if state.composer.submitting {
        "sending…"
    } else if state.composer.error.is_some() {
        "send failed · draft preserved"
    } else if state.composer_enabled() {
        "Enter send · Shift+Enter newline"
    } else {
        "composer unavailable while Project is not ready"
    };
    let mut text = Text::default();
    let cursor = state.composer.cursor.min(state.composer.text.len());
    let (before, after) = state.composer.text.split_at(cursor);
    if !before.is_empty() {
        text.extend(Text::from(before.to_owned()));
    }
    if focused && !state.composer.submitting {
        text.extend(Text::from(Line::from(Span::styled(
            "▌",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ))));
    }
    if !after.is_empty() {
        text.extend(Text::from(after.to_owned()));
    }
    if state.composer.text.is_empty() && !focused {
        text.extend(Text::from(Line::from(Span::styled(
            "Type a message…",
            Style::default().fg(MUTED),
        ))));
    }
    if let Some(error) = &state.composer.error {
        let error_line = if error.visibility.is_public() && !error.message.is_empty() {
            error.message.as_str()
        } else {
            "Action failed; details hidden."
        };
        text.extend(Text::from(Line::from(Span::styled(
            format!("\n{error_line}"),
            Style::default().fg(DANGER),
        ))));
    }

    frame.render_widget(
        Paragraph::new(text)
            .block(
                Block::default()
                    .title(format!(" Composer  ·  {status}"))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(border)),
            )
            .wrap(Wrap { trim: false })
            .style(Style::default().bg(PANEL)),
        area,
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let mut spans = Vec::new();
    if let Some(notification) = state.notifications.last() {
        spans.push(Span::styled(
            if notification.visibility.is_public() && !notification.message.is_empty() {
                format!(" {}  ·  ", fit_inline(&notification.message, 36))
            } else {
                " action completed; protected details hidden  ·  ".to_owned()
            },
            Style::default().fg(MUTED),
        ));
    }
    spans.push(Span::styled(
        "? help  Tab focus  ↑↓ scroll/select  P project  Ctrl-C cancel/quit  ",
        Style::default().fg(MUTED),
    ));
    if state.layout == LayoutMode::Narrow {
        spans.push(Span::styled(
            "Ctrl-[ / Ctrl-] tabs  ",
            Style::default().fg(MUTED),
        ));
    }
    spans.push(Span::styled(
        if state.shutdown_requested {
            "shutting down"
        } else {
            "q quit"
        },
        Style::default().fg(ACCENT),
    ));
    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(BG)),
        area,
    );
}

fn render_rail(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let title = format!(" Project  ·  {}  (P collapse)", state.rail.tab.label());
    let border = if state.focus == FocusTarget::ProjectRail {
        ACCENT
    } else {
        PANEL_ALT
    };
    let items = match state.rail.tab {
        ProjectTab::Tasks => task_items(state),
        ProjectTab::Attention => attention_items(&state.attention),
        ProjectTab::Approvals => approval_items(state),
    };
    let list = if items.is_empty() {
        List::new(vec![ListItem::new(Line::from(Span::styled(
            "No items",
            Style::default().fg(MUTED),
        )))])
    } else {
        List::new(items)
    };
    frame.render_widget(
        list.block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border)),
        )
        .style(Style::default().bg(PANEL)),
        area,
    );
}

fn task_items(state: &AppState) -> Vec<ListItem<'static>> {
    state
        .tasks
        .iter()
        .map(|task| {
            let selected = if task.selected { "▸" } else { " " };
            let title = fit_inline(&task.title, 26);
            let mut lines = vec![Line::from(vec![
                Span::styled(
                    format!("{selected} {} ", task.state.marker()),
                    task_state_style(task.state),
                ),
                Span::styled(title, Style::default().fg(TEXT)),
            ])];
            lines.push(Line::from(Span::styled(
                format!("    {}", task.state.label()),
                task_state_style(task.state),
            )));
            if let Some(blocker) = &task.blocker {
                lines.push(Line::from(Span::styled(
                    format!("    ! {}", fit_inline(blocker, 25)),
                    Style::default().fg(WARNING),
                )));
            }
            ListItem::new(Text::from(lines))
        })
        .collect()
}

fn attention_items(items: &[AttentionItem]) -> Vec<ListItem<'static>> {
    items
        .iter()
        .map(|item| {
            let (title, detail) = if item.visibility.is_public() {
                (item.title.as_str(), item.detail.as_str())
            } else {
                ("Protected attention", "Details hidden")
            };
            ListItem::new(Text::from(vec![
                Line::from(vec![
                    Span::styled(
                        format!("{} ", item.severity.marker()),
                        severity_style(item.severity),
                    ),
                    Span::styled(fit_inline(title, 27), Style::default().fg(TEXT)),
                ]),
                Line::from(Span::styled(
                    format!("  {}", fit_inline(detail, 28)),
                    Style::default().fg(MUTED),
                )),
            ]))
        })
        .collect()
}

fn approval_items(state: &AppState) -> Vec<ListItem<'static>> {
    state
        .approvals
        .iter()
        .enumerate()
        .map(|(index, approval)| {
            let selected = if index == state.rail.selected_approval {
                "▸"
            } else {
                " "
            };
            let title = if approval.visibility.is_public() {
                approval.title.as_str()
            } else {
                "Protected approval"
            };
            ListItem::new(Line::from(vec![
                Span::styled(format!("{selected} ? "), Style::default().fg(WARNING)),
                Span::styled(fit_inline(title, 27), Style::default().fg(TEXT)),
            ]))
        })
        .collect()
}

fn render_modal(frame: &mut Frame<'_>, area: Rect, modal: &ModalState, state: &AppState) {
    let width = if state.layout == LayoutMode::Narrow {
        area.width.saturating_sub(2).max(1)
    } else {
        ((area.width as u32 * 78) / 100)
            .max(40)
            .min(area.width as u32) as u16
    };
    let height = if state.layout == LayoutMode::Narrow {
        area.height.saturating_sub(2).max(1)
    } else {
        ((area.height as u32 * 82) / 100)
            .max(10)
            .min(area.height as u32) as u16
    };
    let popup = centered_rect(width, height, area);
    frame.render_widget(Clear, popup);
    let (text, border) = modal_text(modal);
    frame.render_widget(
        Paragraph::new(text)
            .block(
                Block::default()
                    .title(format!(" {}  ·  Esc close · Enter confirm ", modal.title()))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(border)),
            )
            .wrap(Wrap { trim: false })
            .style(Style::default().bg(PANEL_ALT).fg(TEXT)),
        popup,
    );
}

fn render_setup_overlay(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let width = if state.layout == LayoutMode::Narrow {
        area.width.saturating_sub(2).max(1)
    } else {
        ((area.width as u32 * 70) / 100)
            .max(44)
            .min(area.width as u32) as u16
    };
    let height = ((area.height as u32 * 72) / 100)
        .max(10)
        .min(area.height as u32) as u16;
    let popup = centered_rect(width, height, area);
    frame.render_widget(Clear, popup);
    let (text, border) = setup_text(&state.setup);
    frame.render_widget(
        Paragraph::new(text)
            .block(
                Block::default()
                    .title(" FIRST RUN  ·  Project setup ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(border)),
            )
            .wrap(Wrap { trim: false })
            .style(Style::default().bg(PANEL_ALT).fg(TEXT)),
        popup,
    );
}

fn setup_text(setup: &SetupState) -> (Text<'static>, Color) {
    match setup {
        SetupState::AgentPicker {
            candidates,
            selected,
            detail,
        } => {
            let mut lines = vec![
                Line::from(Span::styled(
                    "Choose the authenticated local CLI harness for the Project Agent.",
                    Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
                )),
                Line::from(
                    "Task execution remains scope-separated and follows Forge workflow gates.",
                ),
                Line::from(""),
            ];
            if candidates.is_empty() {
                lines.push(Line::from(Span::styled(
                    "No eligible authenticated harnesses were found.",
                    Style::default().fg(DANGER),
                )));
                lines.push(Line::from(
                    "Install/login with a supported CLI, then press r to retry.",
                ));
            } else {
                for (index, candidate) in candidates.iter().enumerate() {
                    let selected_marker = if index == *selected { "▸" } else { " " };
                    let status = if candidate.eligible() {
                        "ready"
                    } else if !candidate.available {
                        "unavailable"
                    } else {
                        "not authenticated"
                    };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{selected_marker} "),
                            if index == *selected {
                                Style::default().fg(ACCENT)
                            } else {
                                Style::default().fg(MUTED)
                            },
                        ),
                        Span::styled(fit_inline(&candidate.label, 30), Style::default().fg(TEXT)),
                        Span::styled(
                            format!("  [{status}]"),
                            if candidate.eligible() {
                                Style::default().fg(SUCCESS)
                            } else {
                                Style::default().fg(WARNING)
                            },
                        ),
                    ]));
                    if index == *selected && !candidate.detail.is_empty() {
                        lines.push(Line::from(Span::styled(
                            format!("    {}", fit_inline(&candidate.detail, 60)),
                            Style::default().fg(MUTED),
                        )));
                    }
                }
            }
            if !detail.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    detail.clone(),
                    Style::default().fg(MUTED),
                )));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "↑↓ choose    Enter confirm selected Agent    Esc leave setup",
                Style::default().fg(WARNING),
            )));
            (Text::from(lines), ACCENT)
        }
        SetupState::Unavailable { detail, retryable } => {
            let action = if *retryable {
                "r retry discovery    Esc leave setup"
            } else {
                "Esc leave setup"
            };
            (
                Text::from(vec![
                    Line::from(Span::styled(
                        "Solo setup is unavailable.",
                        Style::default().fg(DANGER).add_modifier(Modifier::BOLD),
                    )),
                    Line::from(detail.clone()),
                    Line::from(""),
                    Line::from(action),
                ]),
                DANGER,
            )
        }
        SetupState::Adoption { outcome, approval } => {
            let mut lines = vec![
                Line::from(Span::styled(
                    "Project adoption",
                    Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
                )),
                Line::from(outcome.clone()),
            ];
            if let Some(approval) = approval {
                lines.push(Line::from(""));
                lines.push(Line::from(format!("Exact target: {}", approval.target)));
                lines.push(Line::from(format!("Impact: {}", approval.impact)));
                lines.push(Line::from(
                    "Open the approval card to execute the permitted action.",
                ));
            }
            (Text::from(lines), WARNING)
        }
        SetupState::NotStarted | SetupState::Ready => (Text::from(""), ACCENT),
    }
}

fn modal_text(modal: &ModalState) -> (Text<'static>, Color) {
    match modal {
        ModalState::Help => (help_text(), ACCENT),
        ModalState::Cancel(card) => {
            let mut lines = vec![
                Line::from(Span::styled(
                    "A turn is still live. Cancel this exact turn?",
                    Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
                )),
                Line::from(format!(
                    "Turn: {}  ·  attempt {}",
                    card.turn_id, card.attempt
                )),
                Line::from(format!("Summary: {}", card.summary)),
                Line::from(""),
                Line::from(Span::styled(
                    "Enter cancel turn    Esc keep running",
                    Style::default().fg(WARNING),
                )),
            ];
            (Text::from(std::mem::take(&mut lines)), WARNING)
        }
        ModalState::Question(card) => {
            if !card.visibility.is_public() {
                return (
                    Text::from("A runtime question is pending, but its contents are protected."),
                    WARNING,
                );
            }
            let mut lines = vec![
                Line::from(Span::styled(
                    card.title.clone(),
                    Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
                )),
                Line::from(card.prompt.clone()),
                Line::from(""),
            ];
            if card.options.is_empty() {
                lines.push(Line::from("No selectable options were provided."));
            }
            for (index, option) in card.options.iter().enumerate() {
                let selected = index == card.selected_option;
                lines.push(Line::from(vec![
                    Span::styled(
                        if selected { "▸ " } else { "  " },
                        if selected {
                            Style::default().fg(ACCENT)
                        } else {
                            Style::default().fg(MUTED)
                        },
                    ),
                    Span::styled(option.label.clone(), Style::default().fg(TEXT)),
                    Span::styled(
                        if option.detail.is_empty() {
                            String::new()
                        } else {
                            format!("  {}", option.detail)
                        },
                        Style::default().fg(MUTED),
                    ),
                ]));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "↑↓ choose    Enter answer    Esc defer",
                Style::default().fg(WARNING),
            )));
            (Text::from(lines), WARNING)
        }
        ModalState::Approval(card) => approval_text(card),
        ModalState::Review(card) => review_text(card),
        ModalState::Error(error) => {
            let detail = if error.visibility.is_public() && !error.message.is_empty() {
                error.message.clone()
            } else {
                "Action failed; protected details are hidden.".to_owned()
            };
            let retry = if error.retryable {
                "Enter retry    Esc close"
            } else {
                "Enter/Esc close"
            };
            (
                Text::from(vec![Line::from(detail), Line::from(""), Line::from(retry)]),
                DANGER,
            )
        }
    }
}

fn approval_text(card: &crate::app::ApprovalCard) -> (Text<'static>, Color) {
    if !card.visibility.is_public() {
        return (
            Text::from("This approval target is protected and cannot be shown."),
            WARNING,
        );
    }
    let mut lines = vec![
        Line::from(Span::styled(
            card.kind.label(),
            Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            card.title.clone(),
            Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
        )),
        Line::from(format!("Exact target: {}", card.target)),
        Line::from(format!("Impact: {}", card.impact)),
    ];
    if let Some(version) = card.expected_version {
        lines.push(Line::from(format!("Expected version: {version}")));
    }
    if let Some(digest) = &card.expected_digest {
        lines.push(Line::from(format!("Expected digest: {digest}")));
    }
    for detail in &card.details {
        lines.push(Line::from(format!("· {detail}")));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Permitted actions:",
        Style::default().fg(MUTED),
    )));
    for (index, action) in card.permitted_actions.iter().enumerate() {
        let selected = index == card.selected_action;
        lines.push(Line::from(vec![
            Span::styled(
                if selected { "▸ " } else { "  " },
                if selected {
                    Style::default().fg(ACCENT)
                } else {
                    Style::default().fg(MUTED)
                },
            ),
            Span::styled(action.label(), Style::default().fg(TEXT)),
        ]));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "↑↓ choose    Enter execute selected action    Esc cancel",
        Style::default().fg(WARNING),
    )));
    (Text::from(lines), WARNING)
}

fn review_text(card: &crate::app::ReviewCard) -> (Text<'static>, Color) {
    if !card.visibility.is_public() {
        return (
            Text::from("This review evidence is protected and cannot be shown."),
            WARNING,
        );
    }
    let mut lines = vec![
        Line::from(Span::styled(
            card.title.clone(),
            Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
        )),
        Line::from(format!(
            "Task: {}  ·  status {}",
            card.task_id,
            card.status.label()
        )),
        Line::from(format!("Worker: {}", card.worker)),
        Line::from(format!("Reviewer: {}", card.reviewer)),
        Line::from("Checks:"),
    ];
    if card.checks.is_empty() {
        lines.push(Line::from("  no checks reported"));
    } else {
        for check in &card.checks {
            let detail = if check.visibility.is_public() {
                check.detail.as_str()
            } else {
                "details hidden"
            };
            lines.push(Line::from(format!(
                "  {} {}  {}",
                check.state.marker(),
                check.name,
                detail
            )));
        }
    }
    lines.push(Line::from(format!(
        "Changed files: {}",
        if card.changed_files.is_empty() {
            "none reported".to_owned()
        } else {
            card.changed_files.join(", ")
        }
    )));
    lines.push(Line::from(format!(
        "Commit: {}",
        card.commit.as_deref().unwrap_or("not recorded")
    )));
    lines.push(Line::from(format!(
        "Merge: {}",
        card.merge_commit.as_deref().unwrap_or("not recorded")
    )));
    lines.push(Line::from(""));
    for (index, action) in card.permitted_actions.iter().enumerate() {
        lines.push(Line::from(vec![
            Span::styled(
                if index == card.selected_action {
                    "▸ "
                } else {
                    "  "
                },
                if index == card.selected_action {
                    Style::default().fg(ACCENT)
                } else {
                    Style::default().fg(MUTED)
                },
            ),
            Span::styled(action.label(), Style::default().fg(TEXT)),
        ]));
    }
    lines.push(Line::from(Span::styled(
        "↑↓ choose    Enter execute    Esc close",
        Style::default().fg(WARNING),
    )));
    (Text::from(lines), WARNING)
}

fn help_text() -> Text<'static> {
    Text::from(vec![
        Line::from(Span::styled(
            "Navigation",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        )),
        Line::from("Tab / Shift+Tab     move focus"),
        Line::from("↑↓ / PageUp/Down   scroll or select"),
        Line::from("Home / End          top or live tail"),
        Line::from("P                   collapse Project rail / switch narrow tab"),
        Line::from(""),
        Line::from(Span::styled(
            "Conversation",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        )),
        Line::from("Enter               send message"),
        Line::from("Shift+Enter         insert newline"),
        Line::from("a                   expand live activity"),
        Line::from("Shift+R             show/hide reasoning details"),
        Line::from("r                   retry a retryable turn"),
        Line::from(""),
        Line::from(Span::styled(
            "Safety",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        )),
        Line::from("Ctrl-C              cancel live turn, or quit when idle"),
        Line::from("Ctrl-C again         force terminal restoration during shutdown"),
        Line::from("Esc                 close modal / keep a turn running"),
        Line::from(""),
        Line::from(Span::styled(
            "Esc or Enter closes this help",
            Style::default().fg(MUTED),
        )),
    ])
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

fn fit_inline(value: &str, max: usize) -> String {
    let mut result = value.chars().take(max).collect::<String>();
    if value.chars().count() > max {
        result.pop();
        result.push('…');
    }
    result
}

fn readiness_style(readiness: &ProjectReadiness) -> Style {
    match readiness {
        ProjectReadiness::Ready => Style::default().fg(SUCCESS),
        ProjectReadiness::Blocked { .. } | ProjectReadiness::Unavailable { .. } => {
            Style::default().fg(DANGER)
        }
        ProjectReadiness::AwaitingAdoption => Style::default().fg(WARNING),
        ProjectReadiness::Setup { .. } | ProjectReadiness::Recovering => {
            Style::default().fg(WARNING)
        }
    }
}

fn runtime_style(runtime: RuntimeState) -> Style {
    match runtime {
        RuntimeState::Ready => Style::default().fg(SUCCESS),
        RuntimeState::Busy | RuntimeState::Recovering | RuntimeState::Starting => {
            Style::default().fg(WARNING)
        }
        RuntimeState::ShuttingDown | RuntimeState::ForcedShutdown | RuntimeState::Stopped => {
            Style::default().fg(MUTED)
        }
        RuntimeState::Failed => Style::default().fg(DANGER),
    }
}

fn role_style(role: MessageRole) -> Style {
    match role {
        MessageRole::User => Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        MessageRole::Assistant => Style::default().fg(SUCCESS).add_modifier(Modifier::BOLD),
        MessageRole::System => Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
        MessageRole::Worker => Style::default()
            .fg(Color::Magenta)
            .add_modifier(Modifier::BOLD),
        MessageRole::Reviewer => Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
        MessageRole::Error => Style::default().fg(DANGER).add_modifier(Modifier::BOLD),
    }
}

fn activity_style(state: TurnState) -> Style {
    match state {
        TurnState::Failed => Style::default().fg(DANGER).add_modifier(Modifier::BOLD),
        TurnState::Succeeded => Style::default().fg(SUCCESS).add_modifier(Modifier::BOLD),
        TurnState::Cancelled => Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
        _ => Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
    }
}

fn activity_status_style(status: crate::app::ActivityStatus) -> Style {
    match status {
        crate::app::ActivityStatus::Complete => Style::default().fg(SUCCESS),
        crate::app::ActivityStatus::Failed => Style::default().fg(DANGER),
        crate::app::ActivityStatus::Skipped => Style::default().fg(MUTED),
        crate::app::ActivityStatus::Running => Style::default().fg(WARNING),
    }
}

fn task_state_style(state: TaskState) -> Style {
    match state {
        TaskState::Succeeded => Style::default().fg(SUCCESS),
        TaskState::Failed | TaskState::Blocked => Style::default().fg(DANGER),
        TaskState::AwaitingReview => Style::default().fg(WARNING),
        TaskState::Running | TaskState::Merging | TaskState::CleaningUp => {
            Style::default().fg(ACCENT)
        }
        TaskState::Queued | TaskState::Cancelled => Style::default().fg(MUTED),
    }
}

fn severity_style(severity: crate::app::AttentionSeverity) -> Style {
    match severity {
        crate::app::AttentionSeverity::Info => Style::default().fg(ACCENT),
        crate::app::AttentionSeverity::Warning => Style::default().fg(WARNING),
        crate::app::AttentionSeverity::Blocking => Style::default().fg(DANGER),
    }
}

#[cfg(test)]
mod tests {
    use ratatui::{backend::TestBackend, Terminal};

    use super::*;
    use crate::app::{
        ActivityStatus, ApprovalAction, ApprovalCard, ApprovalKind, ChatMessage, ContentVisibility,
        MessageRole, Notification, TaskSummary,
    };

    fn draw(state: &AppState, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| render(frame, state)).expect("draw");
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn wide_layout_contains_header_chat_composer_and_project_rail() {
        let mut state = AppState::new();
        state.header.repository = "demo-repo".to_owned();
        state.header.project = "Demo".to_owned();
        state.header.agent = "Codex".to_owned();
        state.header.runtime = RuntimeState::Ready;
        state.header.readiness = ProjectReadiness::Ready;
        state.timeline.push(ChatMessage::new(
            "m1",
            MessageRole::Assistant,
            "Welcome to Solo",
            "now",
        ));
        state
            .tasks
            .push(TaskSummary::new("t1", "Ship TUI", TaskState::Running));
        let text = draw(&state, 140, 40);
        assert!(text.contains("FORGE SOLO"));
        assert!(text.contains("Welcome to Solo"));
        assert!(text.contains("Project"));
        assert!(text.contains("Composer"));
    }

    #[test]
    fn setup_picker_renders_only_structured_agent_health_and_explicit_confirmation() {
        let mut state = AppState::new();
        state.setup = SetupState::AgentPicker {
            candidates: vec![crate::app::AgentCandidate {
                id: "codex".to_owned(),
                label: "Codex CLI".to_owned(),
                kind: "local-cli".to_owned(),
                available: true,
                authenticated: true,
                detail: "authenticated and ready".to_owned(),
            }],
            selected: 0,
            detail: "One eligible harness found; confirm it to continue.".to_owned(),
        };
        let text = draw(&state, 120, 32);
        assert!(text.contains("FIRST RUN"));
        assert!(text.contains("Codex CLI"));
        assert!(text.contains("Enter confirm selected Agent"));
    }

    #[test]
    fn narrow_layout_preserves_project_tab_and_complete_modal_target() {
        let mut state = AppState::new();
        state.reduce(crate::app::AppAction::Resize {
            width: 70,
            height: 30,
        });
        state.rail.tab = ProjectTab::Approvals;
        let tabs = draw(&state, 70, 30);
        assert!(tabs.contains("APPROVALS"));
        state.modal = Some(ModalState::Approval(ApprovalCard {
            id: "adopt-1".to_owned(),
            kind: ApprovalKind::CharterAdoption,
            title: "Adopt Project Charter".to_owned(),
            target: "the exact repository Project".to_owned(),
            impact: "enables repository-mutating Tasks".to_owned(),
            expected_version: Some(7),
            expected_digest: Some("sha256:abc".to_owned()),
            details: vec!["No task writes before approval".to_owned()],
            permitted_actions: vec![ApprovalAction::Approve, ApprovalAction::Reject],
            selected_action: 0,
            visibility: ContentVisibility::Public,
        }));
        let text = draw(&state, 70, 30);
        assert!(text.contains("the exact repository Project"));
        assert!(text.contains("Expected version: 7"));
    }

    #[test]
    fn protected_error_body_never_reaches_test_backend() {
        let mut state = AppState::new();
        state.header.runtime = RuntimeState::Ready;
        state.header.readiness = ProjectReadiness::Ready;
        state
            .timeline
            .push(ChatMessage::protected("e", MessageRole::Error, "now"));
        state.notifications.push(Notification::protected());
        state.live_activity = Some(LiveActivity {
            turn_id: "turn".to_owned(),
            attempt: 1,
            state: TurnState::Failed,
            summary: "failed".to_owned(),
            worker: "worker".to_owned(),
            items: vec![ActivityItem::protected(
                1,
                ActivityKind::Error,
                ActivityStatus::Failed,
                "provider error",
            )],
            expanded: true,
            reasoning_expanded: false,
        });
        let text = draw(&state, 120, 32);
        assert!(!text.contains("provider error"));
        assert!(!text.contains("protected error body"));
    }

    #[test]
    fn failed_activity_renders_recovery_affordance_without_live_cancel_state() {
        let mut state = AppState::new();
        state.live_activity = Some(LiveActivity {
            turn_id: "failed".to_owned(),
            attempt: 2,
            state: TurnState::Failed,
            summary: "Agent Chat turn failed: very long runtime failure detail ".repeat(20),
            worker: String::new(),
            items: Vec::new(),
            expanded: false,
            reasoning_expanded: false,
        });
        state.retryable_turn = Some(crate::app::TurnReference {
            turn_id: "failed".to_owned(),
            attempt: 2,
            expected_version: 9,
        });
        let text = draw(&state, 120, 32);
        assert!(text.contains("FAILED"));
        assert!(text.contains("[r retry]"));
        assert!(state.live_turn_id().is_none());
    }
}
