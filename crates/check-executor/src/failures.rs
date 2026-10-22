//! Bounded, streaming test identities. Unknown formats remain step failures.
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::LazyLock,
};

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
/// The final status of a nextest test that failed, timed out or died of a
/// signal (`TRY n` and short statuses such as `ABRT`/`TMT` under retries).
/// An attempt that will be retried shows `(───)` instead of its counter and
/// is not matched: a test that passes on a retry did not fail.
static NEXTEST_FAILURE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:TRY \d+\s+)?(?:FAIL|LEAK-FAIL|TIMEOUT|TMT|ABORT|ABRT|SIG[A-Z]+)\s+\[\s*\d+(?:\.\d+)?s\]\s+(?:\(\d+/\d+\)\s+)?([^\s(]\S*)\s+(.+)$").unwrap()
});
/// A JS/TS source path. jest `testRegex`/`testMatch` and vitest `include` are
/// configurable (NestJS names e2e suites `*.e2e-spec.ts`), so a test-name
/// convention cannot decide whether a `FAIL <file>` line opens a runner block.
static JS_SOURCE_FILE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\.[cm]?[jt]sx?(?:\s|$)").unwrap());
static VITEST_HEADER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:RUN|DEV)\s+v\d+\.").unwrap());

/// Package decorators from pnpm recursive test scripts and turbo test tasks.
/// Strip only their test-task grammar; e.g. Go's `--- FAIL: Test` is evidence,
/// not a package decorator. Nested wrappers may contribute both prefixes.
/// Also returns the packages: test paths are often package-relative, so two
/// packages can report the same test, and only the package tells them apart.
fn strip_runner_prefix(mut line: &str) -> (Option<String>, Vec<&str>, &str) {
    let mut producer = None::<String>;
    let mut packages = Vec::new();
    while let Some((prefix, rest)) = line.split_once(": ") {
        let Some((package, task)) = prefix.rsplit_once(' ').or_else(|| prefix.split_once(':'))
        else {
            break;
        };
        let package_token = !package.is_empty()
            && package
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"@/._-".contains(&byte));
        let test_task = (task == "test" || task.starts_with("test:"))
            && task
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b":_-".contains(&byte));
        if !package_token || !test_task {
            break;
        }
        let key = producer.get_or_insert_with(String::new);
        key.push_str(prefix);
        key.push('|');
        packages.push(package);
        line = rest.trim_start();
    }
    (producer, packages, line)
}

#[derive(Default, PartialEq, Eq)]
enum Runner {
    #[default]
    Unknown,
    JavaScript,
    Nextest,
}

#[derive(Default)]
struct RunnerContext {
    file: Option<String>,
    runner: Runner,
}

/// ANSI parser state survives pipe chunk boundaries, including OSC hyperlinks.
#[derive(Default)]
pub(crate) struct Ansi {
    state: u8,
}
impl Ansi {
    pub(crate) fn strip(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut plain = Vec::with_capacity(bytes.len());
        for &byte in bytes {
            // A line never continues an escape: a stray or unterminated
            // introducer must not swallow the lines (and failures) after it.
            if matches!(byte, b'\n' | b'\r') {
                self.state = 0;
                plain.push(byte);
                continue;
            }
            // CAN and SUB abort any sequence.
            if self.state != 0 && matches!(byte, 0x18 | 0x1a) {
                self.state = 0;
                continue;
            }
            // ESC inside a string is ST before '\\', else a new escape.
            if self.state == 4 {
                if byte == b'\\' {
                    self.state = 0;
                    continue;
                }
                self.state = 1;
            }
            match (self.state, byte) {
                (0, 0x1b) => self.state = 1,
                (0, _) => plain.push(byte),
                (1, b'[') => self.state = 2,
                (1, b']' | b'P' | b'X' | b'^' | b'_') => self.state = 3,
                (1, 0x20..=0x2f) => {}
                (1 | 2, 0x1b) => self.state = 1,
                (1, _) => self.state = 0,
                (2, 0x40..=0x7e) => self.state = 0,
                (3, 7) => self.state = 0,
                (3, 0x1b) => self.state = 4,
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
        let signature = crate::redact_check_output(&strip_ansi(signature), env).replace('\\', "/");
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
    context: RunnerContext,
    producers: BTreeMap<String, RunnerContext>,
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
            context: RunnerContext::default(),
            producers: BTreeMap::new(),
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
        let (producer, packages, line) = strip_runner_prefix(line.trim());
        let mut overflow = RunnerContext::default();
        let context = if let Some(producer) = producer {
            // Parallel packages must not lend one another their file or runner
            // block. Bound this state just like the signature collection.
            if self.producers.len() == MAX_FAILURES && !self.producers.contains_key(&producer) {
                self.failures.truncated = true;
                &mut overflow
            } else {
                self.producers.entry(producer).or_default()
            }
        } else {
            &mut self.context
        };
        // A runner block ends at its summary. A later linter or progress
        // renderer may print the same glyphs, without reporting any tests.
        if line.starts_with("Test Suites:")
            || line.starts_with("Test Files ")
            || line.starts_with("Summary [")
            || line.starts_with("SUMMARY [")
        {
            context.runner = Runner::Unknown;
            context.file = None;
        } else if VITEST_HEADER.is_match(line) {
            context.runner = Runner::JavaScript;
            context.file = None;
        } else if line.starts_with("Nextest run ID ") {
            context.runner = Runner::Nextest;
            context.file = None;
        }
        let identity = if let Some(failure) = NEXTEST_FAILURE.captures(line) {
            context.runner = Runner::Nextest;
            context.file = None;
            Some(format!("{} > {}", &failure[1], &failure[2]))
        } else if context.runner == Runner::Nextest {
            // Both nextest's --- STDOUT:/STDERR: blocks and newer indented
            // stdout/stderr sections can repeat libtest failures. The binary
            // plus test on nextest's own FAIL line is the identity, once.
            if line == "error: test run failed" {
                context.runner = Runner::Unknown;
            }
            None
        } else if let Some(test) = line
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
            let test = test.trim();
            if test.contains(" > ") {
                Some(test.to_owned())
            } else {
                // Jest appends a slow suite's run time to its FAIL line. It
                // must not reach the identity, which keeps only a trailing one.
                let file = DURATION.replace(test, "");
                if JS_SOURCE_FILE.is_match(&file) {
                    context.runner = Runner::JavaScript;
                    context.file = Some(file.into_owned());
                }
                None
            }
        } else if let Some(test) = line
            .strip_prefix('×')
            .or_else(|| line.strip_prefix('✗'))
            .or_else(|| line.strip_prefix('●'))
            .filter(|_| context.runner == Runner::JavaScript)
        {
            let test = test.trim().replace(" › ", " > ");
            if test.contains(" > ") && context.file.is_none() {
                Some(test)
            } else {
                Some(
                    context
                        .file
                        .as_ref()
                        .map_or(test.clone(), |file| format!("{file} > {test}")),
                )
            }
        } else {
            if let Some(file) = line
                .strip_prefix('❯')
                .and_then(|s| s.trim().split_once(" ("))
                .filter(|(file, summary)| JS_SOURCE_FILE.is_match(file) && summary.contains("test"))
            {
                context.runner = Runner::JavaScript;
                context.file = Some(file.0.to_owned());
            }
            None
        };
        if let Some(identity) = identity {
            let identity = if packages.is_empty() {
                identity
            } else {
                format!("{} > {identity}", packages.join(" > "))
            };
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
    fn monorepo_prefixes_from_captured_pnpm_and_turbo_output() {
        assert_eq!(
            extract(include_str!("fixtures/pnpm-monorepo.txt")),
            [
                "packages/beta > packages/beta/beta.test.js > monorepo suite > rejects invalid input",
                "packages/alpha > packages/alpha/alpha.test.js > monorepo suite > rejects invalid input",
            ]
        );
        assert_eq!(
            extract(include_str!("fixtures/turbo-monorepo.txt")),
            [
                "beta > packages/beta/beta.test.js > monorepo suite > rejects invalid input",
                "alpha > packages/alpha/alpha.test.js > monorepo suite > rejects invalid input",
            ]
        );
    }
    #[test]
    fn monorepo_packages_with_the_same_relative_test_stay_distinct() {
        // Each package's runner reports paths relative to that package.
        assert_eq!(
            extract("a:test: FAIL src/index.test.ts\nb:test: FAIL src/index.test.ts\na:test: ● parse › rejects\nb:test: ● parse › rejects\na test: test parser::empty ... FAILED\nb test: test parser::empty ... FAILED\n"),
            [
                "a > src/index.test.ts > parse > rejects",
                "b > src/index.test.ts > parse > rejects",
                "a > parser::empty",
                "b > parser::empty",
            ]
        );
    }
    #[test]
    fn monorepo_prefixes_preserve_glyph_scope_and_other_runner_syntax() {
        assert!(extract("pkg test: × lint starting\npkg:test: RUN v4.1.11 /task\npkg:test: Test Files 1 passed (1)\npkg:test: ● bundling\n").is_empty());
        assert_eq!(extract("pkg test: inner:test: test parser::empty ... FAILED\n--- FAIL: TestParser (0.01s)\n"),["pkg > inner > parser::empty","TestParser"]);
    }
    #[test]
    fn monorepo_packages_keep_independent_runner_blocks() {
        assert_eq!(extract("a:test: FAIL src/a.test.ts\nb test: FAIL src/b.test.ts\na:test: ● suite-a › broken\na:test: Test Suites: 1 failed\na:test: × linter\nb test: ● suite-b › broken\nb test: Test Suites: 1 failed\n"),[
            "a > src/a.test.ts > suite-a > broken", "b > src/b.test.ts > suite-b > broken",
        ]);
    }
    #[test]
    fn monorepo_runner_context_overflow_is_incomplete_evidence() {
        let env = BTreeMap::new();
        let roots = [Path::new("/task")];
        let mut stream = Stream::new(64, &roots, &env);
        for index in 0..=MAX_FAILURES {
            stream.push(format!("pkg{index}:test: RUN v4.1.11 /task\n").as_bytes());
        }
        stream.push("pkg0:test: × suite > failure\n".as_bytes());
        let (_, _, failures) = stream.finish();
        assert!(failures.truncated);
        assert_eq!(failures.signatures, ["pkg0 > suite > failure"]);
    }
    #[test]
    fn monorepo_prefixes_remain_in_the_stored_tail() {
        let env = BTreeMap::new();
        let roots = [Path::new("/task")];
        let mut stream = Stream::new(4096, &roots, &env);
        stream.push(b"pkg:test: test parser::empty ... FAILED\n");
        let (tail, _, failures) = stream.finish();
        assert_eq!(failures.signatures, ["pkg > parser::empty"]);
        assert!(String::from_utf8(tail).unwrap().starts_with("pkg:test: "));
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
    fn a_malformed_escape_never_swallows_the_following_lines() {
        // Unterminated OSC title, an OSC cut short by a CSI, a stray ESC
        // before a newline: the failure lines after each still count.
        assert_eq!(
            extract("\x1b]0;title\ntest a ... FAILED\n\x1b]8;;u\x1b[31mtest b ... FAILED\x1b[0m\nx\x1b\ntest c ... FAILED\n"),
            ["a", "b", "c"]
        );
        assert_eq!(strip_ansi("\x1b]0;title\nkept"), "\nkept");
    }
    #[test]
    fn unknown_runner_has_no_signature_and_unterminated_failure_is_kept() {
        assert!(extract("compiler: nope\nunknown runner: 9 failed\n").is_empty());
        assert_eq!(extract("test parser::empty ... FAILED"), ["parser::empty"]);
    }

    #[test]
    fn linter_and_progress_glyphs_are_not_test_signatures() {
        assert!(extract("lint starting\n× unused import\n✗ checking dependencies\n● compiling > modules\nFAIL compilation\n● packaging\n").is_empty());
        // A completed test block cannot lend its context to the next tool.
        assert!(extract(
            "RUN v4.1.11 /task\nTest Files  1 passed (1)\n× unused import\n✗ done\n● bundling\n"
        )
        .is_empty());
        assert_eq!(extract("FAIL ./parser.test.js\n● parser › empty input\nTest Suites: 1 failed, 1 total\n× lint error\n✗ progress\n● bundling\n"), ["./parser.test.js > parser > empty input"]);
        assert_eq!(extract("FAIL [ 0.123s] crate::module test_name\n--- STDOUT: crate::module test_name ---\n× custom progress\n✗ custom lint\n● custom progress\nSUMMARY [ 0.123s] 1 test run: 1 failed\n"), ["crate::module > test_name"]);
    }

    #[test]
    fn nextest_extractor_captured_output_has_stable_binary_test_identities() {
        let captured = include_str!("fixtures/cargo-nextest.txt");
        let expected = [
            "guards-nextest-fixture > parser::empty_input",
            "guards-nextest-fixture > parser::unicode_input",
        ];
        assert_eq!(extract(captured), expected);
        let changed_durations = Regex::new(r"\[\s*\d+\.\d+s\]")
            .unwrap()
            .replace_all(captured, "[ 12.345s]");
        assert_eq!(extract(&changed_durations), expected);
        assert_eq!(extract("FAIL [ 0.123s] crate::module test_name\n--- STDOUT: crate::module test_name ---\ntest test_name ... FAILED\nSUMMARY [ 0.123s] 1 test run: 1 failed\nFAIL [ 0.123s] crate::module test_name\n"), ["crate::module > test_name"]);
        assert_eq!(extract("Nextest run ID captured\nSummary [ 0.123s] 1 test run: 1 passed\ntest other_runner::failure ... FAILED\n"), ["other_runner::failure"]);
    }

    #[test]
    fn jest_suites_outside_the_default_test_name_pattern_keep_their_failures() {
        // Captured from jest 30 with `"testRegex": ".e2e-spec.js$"`.
        let captured = "FAIL test/app.e2e-spec.js\n  \u{25cf} AppController (e2e) \u{203a} / (GET)\n\n    expect(received).toBe(expected) // Object.is equality\n\nTest Suites: 1 failed, 1 total\nTests:       1 failed, 1 total\n";
        assert_eq!(
            extract(captured),
            ["test/app.e2e-spec.js > AppController (e2e) > / (GET)"]
        );
        assert_eq!(
            extract(&captured.replace('\n', "\r\n")),
            ["test/app.e2e-spec.js > AppController (e2e) > / (GET)"]
        );
    }

    #[test]
    fn a_slow_jest_suite_duration_is_not_part_of_the_identity() {
        // Captured from jest 30: a suite over the slow threshold prints its
        // run time, which differs between the candidate run and its re-run.
        let run = |seconds: &str| {
            extract(&format!("FAIL test/slow.e2e-spec.js ({seconds} s)\n  \u{25cf} slow \u{203a} waits\n\nTest Suites: 1 failed, 1 total\n"))
        };
        assert_eq!(run("5.296"), ["test/slow.e2e-spec.js > slow > waits"]);
        assert_eq!(run("6.01"), run("5.296"));
    }

    #[test]
    fn nextest_timeouts_signals_and_retried_failures_keep_their_identities() {
        // Captured from cargo-nextest 0.9.144 with a 1 s terminate-after, and
        // again with `--retries 1`, where statuses shorten and gain `TRY n`.
        assert_eq!(
            extract(include_str!("fixtures/cargo-nextest-statuses.txt")),
            [
                "nextest-status > parser::fails",
                "nextest-status > parser::aborts",
                "nextest-status > parser::hangs"
            ]
        );
        assert_eq!(
            extract(include_str!("fixtures/cargo-nextest-retries.txt")),
            [
                "nextest-status > parser::aborts",
                "nextest-status > parser::fails",
                "nextest-status > parser::hangs"
            ]
        );
        // A failed attempt that passes on its retry is flaky, not a failure.
        assert!(extract("Nextest run ID captured\n  TRY 1 FAIL [   0.008s] (\u{2500}\u{2500}\u{2500}) crate parser::flaky\n  TRY 2 PASS [   0.008s] (1/1) crate parser::flaky\n     Summary [   0.020s] 1 test run: 1 passed (1 flaky), 0 skipped\n").is_empty());
    }
}
