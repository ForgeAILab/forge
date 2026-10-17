"""Python binding to the same child writer used by process-supervisor."""
import os
from pathlib import Path
import subprocess
import tempfile


def write_executable(path: Path, contents: str) -> None:
    # Only the child opens the inode for writing; wait for all writers to close
    # before publishing, including for fixtures invoked indirectly by a shell.
    with tempfile.TemporaryDirectory(prefix=".forge-executable-", dir=path.parent) as scratch:
        staged = Path(scratch) / "script"
        subprocess.run(
            ["/bin/sh", str(Path(__file__).with_name("write-executable.sh")), str(staged)],
            input=contents.encode(), check=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
        )
        with staged.open("rb") as script:
            os.fsync(script.fileno())
            os.fchmod(script.fileno(), 0o755)
        os.replace(staged, path)
