import ast
import base64
import gzip
from pathlib import Path

ORIGINAL = Path("scripts/apply_memory_context_patch.py")
text = ORIGINAL.read_text(encoding="utf-8")
module = ast.parse(text)
payload = None
for node in module.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "PAYLOAD":
                payload = ast.literal_eval(node.value)
                break
    if payload is not None:
        break
if not isinstance(payload, str):
    raise RuntimeError("original memory-context patch payload is missing")

source = gzip.decompress(base64.b64decode(payload)).decode("utf-8")
source = source.replace(
    'raise RuntimeError(f"{path}: no MemoryAccessQuery cutoff inserted")',
    "return",
)
exec(compile(source, "apply_memory_context_patch.py.gz", "exec"))

# The service-level scoped search constructor uses a shorthand `cursor` field,
# unlike the other constructors handled by the staged patch. Keep ordinary
# interactive search unbounded while recall supplies its admission cutoff.
memory_service = Path("crates/services/src/memory.rs")
service_text = memory_service.read_text(encoding="utf-8")
needle = "                cursor,\n                include_retracted: false,"
replacement = "                cursor,\n                not_after: None,\n                include_retracted: false,"
if needle in service_text:
    service_text = service_text.replace(needle, replacement, 1)
elif replacement not in service_text:
    raise RuntimeError("crates/services/src/memory.rs: scoped query constructor not found")
memory_service.write_text(service_text, encoding="utf-8")

# Dereference the mutable ordinal before checked arithmetic.
context_path = Path("crates/services/src/memory_context.rs")
context_text = context_path.read_text(encoding="utf-8")
context_text = context_text.replace(
    "*ordinal = ordinal.checked_add(1).ok_or_else(|| {",
    "*ordinal = (*ordinal).checked_add(1).ok_or_else(|| {",
)
context_path.write_text(context_text, encoding="utf-8")
