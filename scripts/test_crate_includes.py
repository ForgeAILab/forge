"""Rust compile-time includes must stay inside the release image build context.

The Dockerfile copies only `Cargo.toml`, `Cargo.lock` and `crates/` (and
`.dockerignore` excludes everything else), so a crate that includes a file
from elsewhere in the repository builds locally and in CI but breaks the image.
Test code is not built for the image: files under a `tests` directory, test
files, and everything after a file's first `#[cfg(test)]` are skipped.
"""
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
CRATES = (ROOT / "crates").resolve()
INCLUDE = re.compile(r'include_(?:str|bytes)!\(\s*"([^"]+)"')


class CrateIncludeTests(unittest.TestCase):
    def test_includes_resolve_inside_crates(self):
        escaped = []
        for source in sorted(CRATES.rglob("*.rs")):
            relative = source.relative_to(CRATES)
            test_file = source.stem == "tests" or source.stem.endswith("_tests")
            if test_file or "tests" in relative.parts[:-1]:
                continue
            text = source.read_text(encoding="utf-8").split("#[cfg(test)]", 1)[0]
            for match in INCLUDE.finditer(text):
                resolved = (source.parent / match.group(1)).resolve()
                if not resolved.is_relative_to(CRATES):
                    escaped.append(f"{source.relative_to(ROOT)}: {match.group(1)}")
        self.assertEqual(escaped, [])


if __name__ == "__main__":
    unittest.main()
