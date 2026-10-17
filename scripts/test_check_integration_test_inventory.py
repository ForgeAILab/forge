"""The inventory proof must detect omissions even when aggregate counts match."""

import importlib.util
import pathlib
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "check_inventory", pathlib.Path(__file__).with_name("check_integration_test_inventory.py"))
check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check)


class InventoryTests(unittest.TestCase):
    def listing(self, text):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        path = pathlib.Path(scratch.name) / "list.log"
        path.write_text(text)
        return path

    def test_normalizes_only_binary_prefix_and_retains_nested_names(self):
        before = self.listing("Running tests/first.rs (/target/first)\nnested::same: test\n"
                              "ignored_case: test\n2 tests, 0 benchmarks\n"
                              "Running tests/second.rs (/target/second)\nsame: test\n"
                              "1 test, 0 benchmarks\n")
        after = self.listing("Running tests/main.rs (/target/main)\nfirst::nested::same: test\n"
                             "first::ignored_case: test\nsecond::same: test\n"
                             "3 tests, 0 benchmarks\n")
        self.assertEqual(check.inventory(before)[0], check.inventory(after)[0])

    def test_same_count_with_a_renamed_or_missing_test_is_rejected(self):
        before = self.listing("Running tests/first.rs (/target/first)\nold: test\n1 test, 0 benchmarks\n")
        after = self.listing("Running tests/main.rs (/target/main)\nfirst::new: test\n1 test, 0 benchmarks\n")
        self.assertEqual(check.main(["check", str(before), str(after)]), 1)

    def test_truncated_listing_is_rejected(self):
        with self.assertRaises(ValueError):
            check.inventory(self.listing("Running tests/first.rs (/target/first)\nsame: test\n"))

    def test_duplicate_identity_is_rejected(self):
        with self.assertRaises(ValueError):
            check.inventory(self.listing("Running tests/main.rs (/target/main)\nfirst::same: test\n"
                                         "first::same: test\n2 tests, 0 benchmarks\n"))


if __name__ == "__main__":
    unittest.main()
