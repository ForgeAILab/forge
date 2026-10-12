"""Coverage and argv regressions for the Rust CI partition."""

import copy
import importlib.util
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("ci_rust_shards", ROOT / "scripts/ci_rust_shards.py")
shards = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shards)


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.workspace = {
            "services": {"targets": [{"name": "services", "kind": ["lib"]},
                                     {"name": "main", "kind": ["test"]}]},
            "api": {"targets": [{"name": "api", "kind": ["lib"]}] +
                                [{"name": name, "kind": ["test"]}
                                 for names in shards.API_TARGETS.values() for name in names]},
            "db": {"targets": []},
            "new-crate": {"targets": []},
        }

    def test_new_package_is_in_rest_and_services_targets_are_partitioned(self):
        self.assertEqual(shards.coverage_errors(self.workspace), [])
        self.assertEqual(shards.selected("rest", self.workspace), {"new-crate"})
        self.assertEqual(shards.named_packages(), ["api", "db", "services"])
        self.assertEqual(shards.workflow_shards(), list(shards.SHARDS))

    def test_filter_typo_cannot_silently_drop_or_duplicate_tests(self):
        broken = copy.deepcopy(shards.SHARDS)
        broken["services-unit-b"][-1] = "task_services::"
        with patch.object(shards, "SHARDS", broken):
            self.assertTrue(shards.coverage_errors(self.workspace))

    def test_new_services_target_needs_explicit_coverage(self):
        self.workspace["services"]["targets"].append({"name": "new-bin", "kind": ["bin"]})
        self.assertIn("uncovered services target", shards.coverage_errors(self.workspace)[0])

    def test_integration_cannot_include_lib_a_second_time(self):
        broken = copy.deepcopy(shards.SHARDS)
        broken["services-integration"] = ["-p", "services", "--tests"]
        with patch.object(shards, "SHARDS", broken):
            self.assertTrue(shards.coverage_errors(self.workspace))

    def test_new_api_integration_target_needs_assignment(self):
        self.workspace["api"]["targets"].append({"name": "new-suite", "kind": ["test"]})
        self.assertTrue(any("'new-suite' has 0 assignments" in error
                            for error in shards.coverage_errors(self.workspace)))

    def test_api_target_cannot_be_duplicated_or_disappear(self):
        broken = copy.deepcopy(shards.API_TARGETS)
        broken["api-b"].append(broken["api-a"][0])
        with patch.object(shards, "API_TARGETS", broken):
            self.assertTrue(any("has 2 assignments" in error
                                for error in shards.coverage_errors(self.workspace)))
        self.workspace["api"]["targets"] = [t for t in self.workspace["api"]["targets"]
                                            if t["name"] != "main"]
        self.assertTrue(any("exists: False" in error
                            for error in shards.coverage_errors(self.workspace)))

    def test_api_shard_cannot_select_lib_or_all_targets_twice(self):
        for selection in ("--all-targets", "--lib", "--tests"):
            broken = copy.deepcopy(shards.SHARDS)
            broken["api-a"].append(selection)
            with self.subTest(selection=selection), patch.object(shards, "SHARDS", broken):
                self.assertTrue(shards.coverage_errors(self.workspace))

    def test_api_non_integration_targets_have_one_owner(self):
        for kind in ("bin", "example", "bench"):
            self.workspace["api"]["targets"].append({"name": kind, "kind": [kind]})
        self.assertEqual(shards.coverage_errors(self.workspace), [])
        self.assertNotIn("--lib", shards.cargo_args("api-a"))
        for flag in ("--lib", "--bins", "--examples", "--benches"):
            self.assertIn(flag, shards.cargo_args(shards.API_OTHER_TARGETS_SHARD))
        with patch.object(shards, "API_OTHER_TARGETS_SHARD", "missing"):
            self.assertTrue(shards.coverage_errors(self.workspace))


class ShellTests(unittest.TestCase):
    def commands(self, *args, timings=False, failing=None):
        with tempfile.TemporaryDirectory() as scratch:
            tmp = pathlib.Path(scratch)
            cargo = tmp / "cargo"
            log = tmp / "commands.jsonl"
            cargo.write_text(
                "#!/usr/bin/env python3\nimport json, os, sys\n"
                "with open(os.environ['CI_TEST_COMMAND_LOG'], 'a') as out:\n"
                "    out.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "sys.exit(1 if sys.argv[1] == os.environ.get('CI_TEST_FAIL') else 0)\n"
            )
            cargo.chmod(0o755)
            env = dict(os.environ, PATH=str(tmp) + os.pathsep + os.environ["PATH"],
                       CI_TEST_COMMAND_LOG=str(log), CI_RUST_TIMINGS="1" if timings else "",
                       CI_TEST_FAIL=failing or "")
            run = subprocess.run(["/bin/bash", str(ROOT / "scripts/ci-rust.sh"), *args],
                                 env=env, text=True, capture_output=True)
            return run, [json.loads(line) for line in log.read_text().splitlines()]

    def test_filter_is_only_sent_to_libtest_not_the_timing_build(self):
        run, commands = self.commands("test", "services-unit-b", timings=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(commands, [
            ["test", "-p", "services", "--lib", "--no-run", "--timings"],
            ["test", "-p", "services", "--lib", "--no-fail-fast", "--",
             "--test-threads=4", "--skip", "task_service::"],
        ])

    def test_integration_glob_remains_literal_and_does_not_add_all_targets(self):
        run, commands = self.commands("test", "services-integration")
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(commands, [["test", "-p", "services", "--test", "*", "--examples",
                                     "--no-fail-fast", "--", "--test-threads=4"]])

    def test_api_binary_selection_is_preserved_without_all_targets(self):
        for shard in shards.API_TARGETS:
            with self.subTest(shard=shard):
                run, commands = self.commands("test", shard, timings=True)
                self.assertEqual(run.returncode, 0, run.stderr)
                selection = shards.cargo_args(shard)
                self.assertEqual(commands, [
                    ["test", *selection, "--no-run", "--timings"],
                    ["test", *selection, "--no-fail-fast", "--", "--test-threads=4"],
                ])
                self.assertNotIn("--all-targets", selection)

    def test_all_still_runs_every_stage_after_lint_failure(self):
        run, commands = self.commands(failing="fmt")
        self.assertEqual(run.returncode, 1)
        self.assertEqual([cmd[0] for cmd in commands], ["fmt", "clippy", "test"])
        self.assertEqual(commands[-1], ["test", "--workspace", "--all-targets",
                                       "--no-fail-fast", "--", "--test-threads=4"])


if __name__ == "__main__":
    unittest.main()
