//! Render a Project Charter exactly as the server does.
//!
//! The REST save-revision route requires the server's `rendered_view` and
//! `render_version` (only the Agent tool path may omit them), so a client
//! authoring a Charter over REST needs the canonical renderer. Usage:
//!
//! ```text
//! cargo run -p services --example charter_render -- content.json
//! ```
//!
//! Prints a JSON object with `rendered_view`, `render_version`,
//! `content_digest`, and `render_digest` for the content file.

use api_types::ProjectCharterContent;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: charter_render <content.json>");
    let raw = std::fs::read_to_string(&path).expect("read the content file");
    let content: ProjectCharterContent =
        serde_json::from_str(&raw).expect("content is a ProjectCharterContent");
    let render = services::render_and_digest_charter(&content);
    let output = serde_json::json!({
        "rendered_view": render.rendered_view,
        "render_version": render.render_version,
        "content_digest": render.content_digest,
        "render_digest": render.render_digest,
    });
    println!(
        "{}",
        serde_json::to_string(&output).expect("render output serializes")
    );
}
