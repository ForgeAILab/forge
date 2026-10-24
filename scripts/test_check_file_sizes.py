"""Exercise the file-size guard through its CLI in isolated Git repositories."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check-file-sizes.py").resolve()


class FileSizeGuardTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True, capture_output=True)
        self.allowlist = self.root / "scripts/file-size-allowlist.txt"
        self.allowlist.parent.mkdir()
        self.allowlist.write_text("# Test baseline\n", encoding="utf-8")

    def source(self, name, lines, tracked=True, content=None):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content if content is not None else b"x\n" * lines)
        if tracked:
            subprocess.run(["git", "update-index", "--add", "--", name],
                           cwd=self.root, check=True, capture_output=True)
        return path

    def entries(self, entries):
        self.allowlist.write_text("# Test baseline\n" + "".join(
            f"{maximum} {name}\n" for name, maximum in sorted(entries.items())
        ), encoding="utf-8")

    def read_entries(self):
        return {
            name: int(maximum)
            for line in self.allowlist.read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")
            for maximum, name in [line.split(maxsplit=1)]
        }

    def run_guard(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *args], cwd=self.root,
                              text=True, capture_output=True)

    def test_unlisted_over_cap_fails(self):
        for name, cap in [("crates/example/src/lib.rs", 2000),
                          ("web/src/example.ts", 1000), ("web/src/Example.tsx", 1000)]:
            self.source(name, cap + 1)
        result = self.run_guard()
        self.assertEqual(result.returncode, 1, result.stderr)
        for name, cap in [("crates/example/src/lib.rs", 2000),
                          ("web/src/example.ts", 1000), ("web/src/Example.tsx", 1000)]:
            self.assertIn(f"{name}: {cap + 1} lines, limit {cap}", result.stderr)
        self.assertIn("Split by responsibility into a module directory", result.stderr)
        self.assertIn("foo.rs", result.stderr)
        self.assertIn("foo/<part>.rs", result.stderr)
        self.assertIn("TS: sibling modules", result.stderr)
        self.assertIn("Never use _part2-style files", result.stderr)
        self.assertIn("never raise or add an allowlist entry", result.stderr)

    def test_listed_growth_fails(self):
        name = "crates/example/src/lib.rs"
        self.source(name, 2101)
        self.entries({name: 2100})
        before = self.allowlist.read_bytes()
        result = self.run_guard()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f"{name}: 2101 lines, limit 2100 (allowlist); file grew", result.stderr)
        self.assertEqual(self.allowlist.read_bytes(), before)

    def test_listed_shrink_fails_until_update(self):
        name = "web/src/Example.tsx"
        self.source(name, 1100)
        self.entries({name: 1200})
        result = self.run_guard()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f"{name}: 1100 lines, limit 1200", result.stderr)
        self.assertIn("python3 scripts/check-file-sizes.py --update", result.stderr)
        updated = self.run_guard("--update")
        self.assertEqual(updated.returncode, 0, updated.stderr)
        self.assertEqual(self.read_entries(), {name: 1100})
        checked = self.run_guard()
        self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_update_never_raises_or_adds(self):
        growing = "crates/example/src/growing.rs"
        shrinking = "crates/example/src/shrinking.rs"
        resolved = "web/src/resolved.ts"
        deleted = "web/src/deleted.tsx"
        unlisted = "web/src/unlisted.ts"
        self.source(growing, 2101)
        self.source(shrinking, 2010)
        self.source(resolved, 1000)
        self.source(deleted, 1100).unlink()
        self.source(unlisted, 1001)
        self.entries({growing: 2100, shrinking: 2020, resolved: 1100, deleted: 1100})
        result = self.run_guard("--update")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f"{growing}: 2101 lines, limit 2100", result.stderr)
        self.assertIn(f"{unlisted}: 1001 lines, limit 1000", result.stderr)
        self.assertEqual(self.read_entries(), {growing: 2100, shrinking: 2010})

    def test_generated_bindings_and_build_directories_are_excluded(self):
        for name in ["web/src/types/generated/bindings/Example.ts",
                     "web/src/types/generated/bindings/nested/Example.tsx",
                     "web/node_modules/example/index.ts", "web/dist/assets/Example.tsx",
                     "web/target/example.ts", "crates/example/target/generated.rs",
                     "crates/example/dist/generated.rs", "crates/node_modules/example/lib.rs"]:
            self.source(name, 3000)
        self.source("other/example.rs", 3000)
        self.source("crates/example/src/example.ts", 3000)
        self.source("web/src/example.rs", 3000)
        self.source("web/src/types/generated/api.ts", 1001)
        result = self.run_guard("--init")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.read_entries(), {"web/src/types/generated/api.ts": 1001})
        self.assertIn("1 source files checked", result.stdout)

    def test_clean_tree_passes_and_ignores_untracked_files(self):
        self.source("crates/example/src/lib.rs", 2000)
        self.source("web/src/example.ts", 1000)
        self.source("web/src/Example.tsx", 1000)
        self.source("crates/example/src/legacy.rs", 2100)
        self.source("web/src/untracked.ts", 5000, tracked=False)
        self.entries({"crates/example/src/legacy.rs": 2100})
        result = self.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(len(result.stdout.splitlines()), 1)
        self.assertIn("4 source files checked; 1 allowlist entries checked (1 Rust, 0 TS/TSX)",
                      result.stdout)

    def test_resolved_and_deleted_entries_are_stale(self):
        names = ["crates/example/src/resolved.rs", "web/src/deleted.ts"]
        self.source(names[0], 2000)
        self.source(names[1], 1100).unlink()
        self.entries({names[0]: 2100, names[1]: 1100})
        result = self.run_guard()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f"{names[0]}: 2000 lines, limit 2100", result.stderr)
        self.assertIn(f"{names[1]}: 0 lines (deleted or no longer scanned), limit 1100", result.stderr)
        self.assertEqual(result.stderr.count("Run python3 scripts/check-file-sizes.py --update."), 2)
        updated = self.run_guard("--update")
        self.assertEqual(updated.returncode, 0, updated.stderr)
        self.assertEqual(self.read_entries(), {})

    def test_counts_newlines_like_wc(self):
        self.source("crates/example/src/lib.rs", 0, content=b"x\r\n" * 2000 + b"last\rline")
        path = self.source("web/src/example.ts", 0, content=b"x\n" * 1000 + b"last line")
        result = self.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)
        with path.open("ab") as output:
            output.write(b"\n")
        exceeded = self.run_guard()
        self.assertEqual(exceeded.returncode, 1, exceeded.stderr)
        self.assertIn("web/src/example.ts: 1001 lines, limit 1000", exceeded.stderr)

    def test_init_writes_current_oversized_files_sorted_by_path(self):
        self.source("web/src/z.ts", 1001)
        self.source("crates/example/src/a.rs", 2001)
        self.source("web/src/clean.tsx", 1000)
        self.entries({"web/src/deleted.ts": 9000, "web/src/z.ts": 9999})
        result = self.run_guard("--init")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(list(self.read_entries().items()), [
            ("crates/example/src/a.rs", 2001), ("web/src/z.ts", 1001)
        ])
        self.assertIn("Maintainers only", self.allowlist.read_text(encoding="utf-8"))

    def test_allowlist_comments_and_paths_with_spaces(self):
        name = "web/src/a file.ts"
        self.source(name, 1001)
        self.allowlist.write_text(f"# Baseline\n\n1001 {name} # Existing file\n", encoding="utf-8")
        result = self.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unsorted_entries_fail_until_update(self):
        self.source("web/src/a.ts", 1001)
        self.source("web/src/z.ts", 1001)
        self.allowlist.write_text("1001 web/src/z.ts\n1001 web/src/a.ts\n", encoding="utf-8")
        result = self.run_guard()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("entries must be sorted by path", result.stderr)
        updated = self.run_guard("--update")
        self.assertEqual(updated.returncode, 0, updated.stderr)
        self.assertEqual(list(self.read_entries()), ["web/src/a.ts", "web/src/z.ts"])

    def test_malformed_or_duplicate_entries_fail_without_rewriting(self):
        for content in ["not-a-number web/src/example.ts\n", "1001\n",
                        "1001 ../example.ts\n", "1001 /example.ts\n",
                        "1001 web/src/example.ts\n1001 web/src/example.ts\n"]:
            with self.subTest(content=content):
                self.allowlist.write_text(content, encoding="utf-8")
                result = self.run_guard("--update")
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("File-size guard failed: scripts/file-size-allowlist.txt:", result.stderr)
                self.assertEqual(self.allowlist.read_text(encoding="utf-8"), content)


if __name__ == "__main__":
    unittest.main()
