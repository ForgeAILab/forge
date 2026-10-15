//! Bounded, streaming test identities. Unknown formats remain step failures.
use std::{collections::BTreeSet, path::Path, sync::LazyLock};

use regex::Regex;

pub(crate) const MAX_FAILURES: usize = 200;
const MAX_SIGNATURE_CHARS: usize = 300;
const MAX_LINE_BYTES: usize = 16 * 1024;
const MAX_REPORT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_REPORT_FILES: usize = 200;
/// Report bytes one command may make Forge read and parse, over all files.
/// Parsing is synchronous; 200 files at the per-file bound would be 800 MiB.
const MAX_REPORT_TOTAL_BYTES: u64 = 16 * 1024 * 1024;
static DURATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\s*\(?\b\d+(?:\.\d+)?\s*(?:ms|µs|μs|ns|s|seconds?)\)?\s*$").unwrap()
});

/// ANSI parser state survives pipe chunk boundaries, including OSC hyperlinks.
#[derive(Default)]
pub(crate) struct Ansi {
    state: u8,
}
impl Ansi {
    pub(crate) fn strip(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut plain = Vec::with_capacity(bytes.len());
        for &byte in bytes {
            match (self.state, byte) {
                (0, 0x1b) => self.state = 1,
                (0, _) => plain.push(byte),
                (1, b'[') => self.state = 2,
                (1, b']' | b'P' | b'X' | b'^' | b'_') => self.state = 3,
                (1, 0x20..=0x2f) => {}
                (1, _) => self.state = 0,
                (2, 0x40..=0x7e) => self.state = 0,
                (3, 7) | (4, b'\\') => self.state = 0,
                (3, 0x1b) => self.state = 4,
                (4, _) => self.state = 3,
                _ => {}
            }
        }
        plain
    }
}

pub fn strip_ansi(text: &str) -> String {
    String::from_utf8_lossy(&Ansi::default().strip(text.as_bytes())).into_owned()
}

#[derive(Default)]
pub(crate) struct Failures {
    pub signatures: Vec<String>,
    pub truncated: bool,
    seen: BTreeSet<String>,
}
impl Failures {
    /// `roots` are absolute prefixes removed from identities, most specific
    /// first: the command's checkout, then its Task root. A candidate run in
    /// a Task worktree and an exact-commit run elsewhere then name the same
    /// test the same way.
    pub fn add(
        &mut self,
        signature: &str,
        roots: &[&Path],
        env: &std::collections::BTreeMap<String, String>,
    ) {
        // Redact the text as printed: separator and duration normalisation
        // would otherwise rewrite a secret (or cut its suffix) past redaction.
        let signature =
            executors::environment::redact_environment_values(&strip_ansi(signature), env)
                .replace('\\', "/");
        let signature = roots.iter().fold(signature, |signature, root| {
            let prefix = root.to_string_lossy().replace('\\', "/");
            signature.replace(&format!("{}/", prefix.trim_end_matches('/')), "")
        });
        let signature = DURATION.replace(signature.trim(), "");
        let signature: String = signature
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(MAX_SIGNATURE_CHARS)
            .collect();
        if signature.is_empty() || self.seen.contains(&signature) {
            return;
        }
        if self.signatures.len() == MAX_FAILURES {
            self.truncated = true;
            return;
        }
        self.seen.insert(signature.clone());
        self.signatures.push(signature);
    }
    /// `other` holds identities [`Self::add`] already normalised; a second
    /// pass would cut another duration-like suffix, so a test reported on
    /// stderr or in a JUnit file would get a different identity than on stdout.
    pub fn merge(&mut self, other: Self) {
        self.truncated |= other.truncated;
        for signature in other.signatures {
            if self.seen.contains(&signature) {
                continue;
            }
            if self.signatures.len() == MAX_FAILURES {
                self.truncated = true;
                return;
            }
            self.seen.insert(signature.clone());
            self.signatures.push(signature);
        }
    }
}

pub(crate) struct Stream<'a> {
    ansi: Ansi,
    tail: Vec<u8>,
    line: Vec<u8>,
    file: Option<String>,
    limit: usize,
    roots: &'a [&'a Path],
    env: &'a std::collections::BTreeMap<String, String>,
    pub failures: Failures,
    pub truncated: bool,
}
impl<'a> Stream<'a> {
    pub fn new(
        limit: usize,
        roots: &'a [&'a Path],
        env: &'a std::collections::BTreeMap<String, String>,
    ) -> Self {
        Self {
            ansi: Ansi::default(),
            tail: vec![],
            line: vec![],
            file: None,
            limit,
            roots,
            env,
            failures: Failures::default(),
            truncated: false,
        }
    }
    pub fn push(&mut self, bytes: &[u8]) {
        let bytes = self.ansi.strip(bytes);
        self.truncated |= self.tail.len().saturating_add(bytes.len()) > self.limit;
        self.tail.extend_from_slice(&bytes);
        let remove = self.tail.len().saturating_sub(self.limit);
        self.tail.drain(..remove);
        for byte in bytes {
            if matches!(byte, b'\n' | b'\r') {
                self.end_line();
            } else if self.line.len() < MAX_LINE_BYTES {
                self.line.push(byte);
            }
        }
    }
    fn end_line(&mut self) {
        let line = String::from_utf8_lossy(&self.line);
        let line = line.trim();
        let identity = if let Some(test) = line
            .strip_prefix("test ")
            .and_then(|s| s.split_once(" ... FAILED").map(|(name, _)| name))
        {
            Some(test.to_owned())
        } else if let Some(test) = line.strip_prefix("FAILED ").filter(|s| s.contains("::")) {
            Some(
                test.split_once(" - ")
                    .map_or(test, |(name, _)| name)
                    .to_owned(),
            )
        } else if let Some(test) = line.strip_prefix("--- FAIL: ") {
            Some(test.to_owned())
        } else if let Some(test) = line.strip_prefix("FAIL ") {
            if test.contains(" > ") {
                Some(test.to_owned())
            } else {
                self.file = Some(test.to_owned());
                None
            }
        } else if let Some(test) = line
            .strip_prefix('×')
            .or_else(|| line.strip_prefix('✗'))
            .or_else(|| line.strip_prefix('●'))
        {
            let test = test.trim().replace(" › ", " > ");
            if test.contains(" > ") && self.file.is_none() {
                Some(test)
            } else {
                Some(
                    self.file
                        .as_ref()
                        .map_or(test.clone(), |file| format!("{file} > {test}")),
                )
            }
        } else {
            if let Some(file) = line
                .strip_prefix('❯')
                .and_then(|s| s.trim().split_once(" ("))
            {
                self.file = Some(file.0.to_owned());
            }
            None
        };
        if let Some(identity) = identity {
            self.failures.add(&identity, self.roots, self.env);
        }
        self.line.clear();
    }
    pub fn finish(mut self) -> (Vec<u8>, bool, Failures) {
        self.end_line();
        (self.tail, self.truncated, self.failures)
    }
}

/// Reports are optional evidence: malformed/oversized files do not invent a
/// test identity or alter the command's real exit status. Never follow links.
pub(crate) fn junit(
    directory: &Path,
    roots: &[&Path],
    env: &std::collections::BTreeMap<String, String>,
    failures: &mut Failures,
) {
    let mut directories = vec![directory.to_owned()];
    let mut visited = 0;
    let mut total = 0_u64;
    while let Some(directory) = directories.pop() {
        let Ok(entries) = std::fs::read_dir(directory) else {
            continue;
        };
        let mut entries: Vec<_> = entries.flatten().take(MAX_REPORT_FILES + 1).collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            visited += 1;
            if visited > MAX_REPORT_FILES {
                return;
            }
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                directories.push(entry.path());
                continue;
            }
            if !kind.is_file() || entry.path().extension().is_none_or(|ext| ext != "xml") {
                continue;
            }
            // Read through a bound even if the file grows after metadata.
            use std::io::Read;
            let Ok(file) = std::fs::File::open(entry.path()) else {
                continue;
            };
            let mut bytes = Vec::new();
            let read = file.take(MAX_REPORT_BYTES + 1).read_to_end(&mut bytes);
            total = total.saturating_add(bytes.len() as u64);
            if total > MAX_REPORT_TOTAL_BYTES {
                return;
            }
            let (Ok(_), Ok(xml)) = (read, std::str::from_utf8(&bytes)) else {
                continue;
            };
            if bytes.len() as u64 > MAX_REPORT_BYTES {
                continue;
            }
            parse_junit(xml, roots, env, failures);
        }
    }
}
fn parse_junit(
    xml: &str,
    roots: &[&Path],
    env: &std::collections::BTreeMap<String, String>,
    failures: &mut Failures,
) {
    use quick_xml::{events::Event, Reader};
    let mut reader = Reader::from_str(xml);
    let mut parsed = Failures::default();
    let mut depth = 0_usize;
    let mut case: Option<(usize, String, bool)> = None;
    loop {
        match reader.read_event() {
            Ok(event @ (Event::Start(_) | Event::Empty(_))) => {
                let empty = matches!(event, Event::Empty(_));
                let tag = match event {
                    Event::Start(tag) | Event::Empty(tag) => tag,
                    _ => unreachable!(),
                };
                if tag.local_name().as_ref() == b"testcase" {
                    let mut name = None;
                    let mut class = None;
                    for attribute in tag.attributes() {
                        let Ok(attribute) = attribute else { return };
                        let Ok(value) = attribute.decoded_and_normalized_value(
                            quick_xml::XmlVersion::Implicit1_0,
                            reader.decoder(),
                        ) else {
                            return;
                        };
                        match attribute.key.as_ref() {
                            b"name" => name = Some(value.into_owned()),
                            b"classname" => class = Some(value.into_owned()),
                            _ => {}
                        }
                    }
                    if !empty {
                        case = name.map(|name| {
                            (
                                depth,
                                class.filter(|s| !s.is_empty()).map_or_else(
                                    || name.clone(),
                                    |class| format!("{class} > {name}"),
                                ),
                                false,
                            )
                        });
                    }
                } else if matches!(tag.local_name().as_ref(), b"failure" | b"error") {
                    if let Some((case_depth, _, failed)) = case.as_mut() {
                        if depth == *case_depth + 1 {
                            *failed = true;
                        }
                    }
                }
                if !empty {
                    depth += 1;
                }
            }
            Ok(Event::End(_)) => {
                let Some(next) = depth.checked_sub(1) else {
                    return;
                };
                depth = next;
                if case
                    .as_ref()
                    .is_some_and(|(case_depth, _, _)| *case_depth == depth)
                {
                    let (_, identity, failed) = case.take().unwrap();
                    if failed {
                        parsed.add(&identity, roots, env);
                    }
                }
            }
            Ok(Event::Eof) if depth == 0 => break,
            Ok(Event::Eof | Event::DocType(_)) | Err(_) => return,
            _ => {}
        }
    }
    failures.merge(parsed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn extract(text: &str) -> Vec<String> {
        let env = BTreeMap::new();
        let roots = [Path::new("/task")];
        let mut stream = Stream::new(64, &roots, &env);
        // Pipe boundaries can cut a Unicode character or ANSI sequence.
        for chunk in text.as_bytes().chunks(7) {
            stream.push(chunk);
        }
        stream.finish().2.signatures
    }
    #[test]
    fn cargo_extractor_captured_output() {
        assert_eq!(extract(include_str!("fixtures/cargo-test.txt")), [
            "integration_steps::tests::gate_tests::an_unreadable_task_is_quarantined_and_the_import_continues",
            "integration_steps::tests::imported_rows_that_wait_for_their_owner_stay_put_with_a_typed_reason",
        ]);
    }
    #[test]
    fn vitest_extractor_captured_output() {
        assert_eq!(
            extract(include_str!("fixtures/vitest.txt")),
            [
                "parser.test.js > parser > empty input",
                "parser.test.js > parser > unicode input"
            ]
        );
    }
    #[test]
    fn jest_extractor_captured_output() {
        assert_eq!(
            extract(include_str!("fixtures/jest.txt")),
            [
                "./parser.test.js > parser > empty input",
                "./parser.test.js > parser > unicode input"
            ]
        );
    }
    #[test]
    fn pytest_extractor_captured_output() {
        assert_eq!(
            extract(include_str!("fixtures/pytest.txt")),
            [
                "test_parser.py::test_empty_input",
                "test_parser.py::test_unicode_input"
            ]
        );
    }
    #[test]
    fn go_extractor_captured_output() {
        assert_eq!(
            extract(include_str!("fixtures/go-test.txt")),
            ["TestParser", "TestParser/Empty", "TestParser/Unicode"]
        );
    }
    #[test]
    fn signature_normalisation_deduplicates_caps_and_redacts() {
        let env = BTreeMap::from([("TOKEN".into(), "private-value".into())]);
        let root: &[&Path] = &[Path::new("/absolute/task")];
        let mut failures = Failures::default();
        failures.add(
            "\x1b[31m/absolute/task/repo/a.test.ts > case (12.34s)\x1b[0m",
            root,
            &env,
        );
        failures.add("repo/a.test.ts > case 2ms", root, &env);
        failures.add("private-value", root, &env);
        failures.add(&"猫".repeat(400), root, &env);
        assert_eq!(failures.signatures[0], "repo/a.test.ts > case");
        assert_eq!(failures.signatures[1], "[REDACTED]");
        assert_eq!(failures.signatures[2].chars().count(), 300);
        for i in 0..197 {
            failures.add(&format!("test_{i}"), root, &env);
        }
        assert_eq!(failures.signatures.len(), 200);
        assert!(!failures.truncated);
        failures.add("test_196", root, &env);
        assert!(!failures.truncated, "duplicates do not truncate");
        failures.add("test_197", root, &env);
        assert!(failures.truncated);
        assert_eq!(failures.signatures.len(), 200);
    }
    #[test]
    fn signature_redaction_runs_before_separator_and_duration_normalisation() {
        let env = BTreeMap::from([
            ("PASSWORD".into(), r"pa\ss".into()),
            ("TOKEN".into(), "tok-15s".into()),
        ]);
        let mut failures = Failures::default();
        failures.add(r"test login with pa\ss", &[Path::new("/task")], &env);
        failures.add("test uses tok-15s", &[Path::new("/task")], &env);
        assert_eq!(
            failures.signatures,
            ["test login with [REDACTED]", "test uses [REDACTED]"]
        );
    }
    #[test]
    fn junit_collection_stops_at_the_total_byte_budget() {
        let dir = tempfile::tempdir().unwrap();
        let padding = " ".repeat(3 * 1024 * 1024 + 512 * 1024);
        for name in ["a", "b", "c", "d", "e"] {
            std::fs::write(
                dir.path().join(format!("{name}.xml")),
                format!(
                    r#"<testsuite><!--{padding}--><testcase name="{name}"><failure/></testcase></testsuite>"#
                ),
            )
            .unwrap();
        }
        let mut failures = Failures::default();
        junit(dir.path(), &[dir.path()], &BTreeMap::new(), &mut failures);
        assert_eq!(failures.signatures, ["a", "b", "c", "d"]);
    }
    #[test]
    fn merged_identities_are_not_normalised_twice() {
        let env = BTreeMap::new();
        let roots = [Path::new("/task")];
        let mut stderr = Failures::default();
        stderr.add("api > retries after 5 s 12ms", &roots, &env);
        assert_eq!(stderr.signatures, ["api > retries after 5 s"]);
        let mut all = Failures::default();
        all.merge(stderr);
        assert_eq!(all.signatures, ["api > retries after 5 s"]);
    }
    #[test]
    fn junit_captured_report_and_errors_skip_passes_and_malformed_xml() {
        let env = BTreeMap::new();
        let mut failures = Failures::default();
        parse_junit(
            include_str!("fixtures/junit.xml"),
            &[Path::new("/task")],
            &env,
            &mut failures,
        );
        assert_eq!(
            failures.signatures,
            [
                "parser.test.js > parser > empty input",
                "parser.test.js > parser > unicode input"
            ]
        );
        parse_junit(
            r#"<testsuites><testsuite><testcase name="pass"/><testcase name="skip"><skipped/></testcase><testcase classname="suite" name="猫 &amp; dog"><error/></testcase></testsuite></testsuites>"#,
            &[Path::new("/task")],
            &env,
            &mut failures,
        );
        assert_eq!(failures.signatures.last().unwrap(), "suite > 猫 & dog");
        let previous = failures.signatures.clone();
        parse_junit(
            r#"<testsuite><testcase name="bad"><failure/></testcase>"#,
            &[Path::new("/task")],
            &env,
            &mut failures,
        );
        assert_eq!(failures.signatures, previous);
    }
    #[test]
    fn ansi_strip_handles_csi_osc_and_split_sequences_before_tail_budget() {
        let env = BTreeMap::new();
        let roots = [Path::new("/task")];
        let mut stream = Stream::new(19, &roots, &env);
        let text = "\x1b]8;;https://example.test\x1b\\\x1b[31m4 failed\x1b[39m | \x1b[32m316 passed\x1b[39m\x1b]8;;\x07";
        for chunk in text.as_bytes().chunks(1) {
            stream.push(chunk);
        }
        let (tail, truncated, _) = stream.finish();
        assert_eq!(String::from_utf8(tail).unwrap(), "failed | 316 passed");
        assert!(truncated);
        assert_eq!(strip_ansi(text), "4 failed | 316 passed");
    }
    #[test]
    fn unknown_runner_has_no_signature_and_unterminated_failure_is_kept() {
        assert!(extract("compiler: nope\nunknown runner: 9 failed\n").is_empty());
        assert_eq!(extract("test parser::empty ... FAILED"), ["parser::empty"]);
    }
}
