#!/usr/bin/env python3
"""Compare combined stdout/stderr from cargo test -p <crate> --tests -- --list.

Usage: check_integration_test_inventory.py before.log after.log

Old tests/foo.rs binaries become foo:: modules in tests/main.rs. Preserve all
other path components and compare multisets, including ignored tests. Unit
names stay unchanged. Fail on truncated listings, duplicate identities, missing
or added tests, or an unrecognized test target. No compilation is done here.
See docs/ci.md for recording and comparing the inventories.
"""

import collections
import pathlib
import re
import sys


def inventory(path):
    names = collections.Counter()
    target = None
    listed = 0
    binaries = 0
    summaries = 0
    for line in path.read_text().splitlines():
        line = re.sub(r"\x1b\[[0-9;]*m", "", line)
        run = re.search(r"Running (?:unittests )?(\S+\.rs) \(", line)
        if run:
            if target is not None:
                raise ValueError(f"{path}: target {target} has no summary")
            target = run[1]
            listed = 0
            binaries += 1
            continue
        test = re.fullmatch(r"(.+): (test|benchmark)", line)
        if test:
            if target == "src/lib.rs":
                name = "lib::" + test[1]
            elif target == "tests/main.rs":
                name = test[1]
                if "::" not in name:
                    raise ValueError(f"{path}: unnamespaced consolidated test {name}")
            elif target and target.startswith("tests/"):
                name = pathlib.Path(target).stem + "::" + test[1]
            else:
                raise ValueError(f"{path}: unexpected target {target}")
            names[(name, test[2])] += 1
            listed += 1
        summary = re.fullmatch(r"(\d+) tests?, (\d+) benchmarks?", line)
        if summary:
            if target is None or listed != int(summary[1]) + int(summary[2]):
                raise ValueError(f"{path}: incomplete listing for {target}")
            target = None
            summaries += 1
    if not names or target is not None or summaries != binaries:
        raise ValueError(f"{path}: empty or incomplete inventory")
    duplicates = [name for name, count in names.items() if count != 1]
    if duplicates:
        raise ValueError(f"{path}: duplicate test identities {duplicates}")
    return names, binaries


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        before, old_bins = inventory(pathlib.Path(argv[1]))
        after, new_bins = inventory(pathlib.Path(argv[2]))
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1
    missing, added = before - after, after - before
    for label, diff in (("missing", missing), ("added", added)):
        for name, kind in sorted(diff):
            print(f"{label}: {name} ({kind})", file=sys.stderr)
    if missing or added:
        return 1
    unit = sum(count for (name, _), count in before.items() if name.startswith("lib::"))
    print(f"ok: {sum(before.values())} tests unchanged ({unit} unit, "
          f"{sum(before.values()) - unit} integration); binaries {old_bins} -> {new_bins}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
