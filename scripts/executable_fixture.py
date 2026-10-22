"""The same child writer process-supervisor uses, for Python fixtures."""
import os
from pathlib import Path
import subprocess
import tempfile

# Same program as `WRITER` in crates/process-supervisor/src/executable.rs.
WRITER = 'umask 077\nset -C\nexec /bin/cat > "$1"\n'


def write_executable(path: Path, contents: str) -> None:
    # Only the child opens the inode for writing; wait for all writers to close
    # before publishing, including for fixtures invoked indirectly by a shell.
    with tempfile.TemporaryDirectory(prefix=".forge-executable-", dir=path.parent) as scratch:
        staged = Path(scratch) / "script"
        subprocess.run(
            ["/bin/sh", "-c", WRITER, "forge-write-executable", str(staged)],
            input=contents.encode(), check=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
        )
        with staged.open("rb") as script:
            os.fsync(script.fileno())
            os.fchmod(script.fileno(), 0o755)
        os.replace(staged, path)
