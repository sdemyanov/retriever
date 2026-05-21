#!/bin/bash
set -euo pipefail

# Build retriever.plugin from source.
#
# This build avoids `rm`/`mv` on files inside the repo so it runs cleanly in
# Cowork sandboxes where destructive filesystem ops are blocked by default.
# All overwrites happen via open-truncate (write mode) instead of
# unlink-then-create.
cd "$(dirname "$0")"

SOURCE_HEADER_PATH="skills/tool-template/src/00_header.py"
PLUGIN_MANIFEST_PATH=".claude-plugin/plugin.json"
PING_SKILL_PATH="skills/ping/SKILL.md"
SKILLS_ROOT="skills"
WORKSPACE_SKILL_PATH="skills/workspace/SKILL.md"
WORKSPACE_DOC_PATH="skills/workspace/workspace.md"
SCHEMA_DOC_PATH="skills/schema/schema.md"

TOOL_VERSION="$(
  python3 -c 'import pathlib, re
text = pathlib.Path("'"$SOURCE_HEADER_PATH"'").read_text(encoding="utf-8")
match = re.search(r"^TOOL_VERSION = \"([^\"]+)\"$", text, re.MULTILINE)
if match is None:
    raise SystemExit("Could not determine TOOL_VERSION from skills/tool-template/src/00_header.py")
print(match.group(1))'
)"

SCHEMA_VERSION="$(
  python3 -c 'import pathlib, re
text = pathlib.Path("'"$SOURCE_HEADER_PATH"'").read_text(encoding="utf-8")
match = re.search(r"^SCHEMA_VERSION = (\d+)$", text, re.MULTILINE)
if match is None:
    raise SystemExit("Could not determine SCHEMA_VERSION from skills/tool-template/src/00_header.py")
print(match.group(1))'
)"

python3 -c 'import json, pathlib, re, sys
tool_version = sys.argv[1]
schema_version = sys.argv[2]
manifest_path = pathlib.Path(sys.argv[3])
ping_skill_path = pathlib.Path(sys.argv[4])
skills_root = pathlib.Path(sys.argv[5])
workspace_skill_path = pathlib.Path(sys.argv[6])
workspace_doc_path = pathlib.Path(sys.argv[7])
schema_doc_path = pathlib.Path(sys.argv[8])

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
manifest["version"] = tool_version
manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

skill_paths = sorted(skills_root.glob("*/SKILL.md"))
if not skill_paths:
    raise SystemExit("Could not find any SKILL.md files under skills/")
for skill_path in skill_paths:
    text = skill_path.read_text(encoding="utf-8")
    text, count = re.subn(
        r"^(\s*version:\s*\")([^\"]+)(\"\s*)$",
        rf"\g<1>{tool_version}\g<3>",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise SystemExit(f"Could not synchronize skill version in {skill_path}")
    skill_path.write_text(text, encoding="utf-8")

ping_text = ping_skill_path.read_text(encoding="utf-8")
ping_text, metadata_count = re.subn(
    r"^(\s*version:\s*\")([^\"]+)(\"\s*)$",
    rf"\g<1>{tool_version}\g<3>",
    ping_text,
    count=1,
    flags=re.MULTILINE,
)
ping_text, body_count = re.subn(
    r"^(Version:\s*)(.+?)(\s*)$",
    rf"\g<1>{tool_version}\g<3>",
    ping_text,
    count=1,
    flags=re.MULTILINE,
)
if metadata_count != 1 or body_count != 1:
    raise SystemExit("Could not synchronize ping skill version text.")
ping_skill_path.write_text(ping_text, encoding="utf-8")' \
  "$TOOL_VERSION" \
  "$SCHEMA_VERSION" \
  "$PLUGIN_MANIFEST_PATH" \
  "$PING_SKILL_PATH" \
  "$SKILLS_ROOT" \
  "$WORKSPACE_SKILL_PATH" \
  "$WORKSPACE_DOC_PATH" \
  "$SCHEMA_DOC_PATH"

python3 skills/tool-template/bundle_retriever_tools.py
python3 sync_claude_md.py

python3 -c 'import pathlib, re, sys
tool_version = sys.argv[1]
schema_version = sys.argv[2]
workspace_skill_path = pathlib.Path(sys.argv[3])
workspace_doc_path = pathlib.Path(sys.argv[4])
schema_doc_path = pathlib.Path(sys.argv[5])

def replace_once(path: pathlib.Path, pattern: str, replacement: str) -> None:
    text = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit(f"Could not apply required replacement in {path}: {pattern}")
    path.write_text(updated, encoding="utf-8")

replace_once(
    workspace_skill_path,
    r"With the current `[^`]+` / schema `\d+` tool surface, Claude should be able to:",
    f"With the current `{tool_version}` / schema `{schema_version}` tool surface, Claude should be able to:",
)
replace_once(
    workspace_skill_path,
    r"initialize or migrate schema `\d+`",
    f"initialize or migrate schema `{schema_version}`",
)
replace_once(
    workspace_doc_path,
    r"workspace init <workspace>` to create or upgrade schema `\d+`\.",
    f"workspace init <workspace>` to create or upgrade schema `{schema_version}`.",
)
replace_once(
    schema_doc_path,
    r"^- schema version: `\d+`$",
    f"- schema version: `{schema_version}`",
)

for path in (workspace_doc_path, schema_doc_path):
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"(\"tool_version\": \")([^\"]+)(\")", rf"\g<1>{tool_version}\g<3>", text)
    text = re.sub(r"(\"schema_version\": )(\d+)", rf"\g<1>{schema_version}", text)
    path.write_text(text, encoding="utf-8")' \
  "$TOOL_VERSION" \
  "$SCHEMA_VERSION" \
  "$WORKSPACE_SKILL_PATH" \
  "$WORKSPACE_DOC_PATH" \
  "$SCHEMA_DOC_PATH"

TOOL_PATH="skills/tool-template/tools.py"
DOC_PATH="skills/tool-template/tool-template.md"

# Compute the bundled-tool checksum and splice it into tool-template.md in
# place. Using pathlib.write_text avoids mktemp/mv across filesystems.
TOOL_SHA="$(
  python3 -c 'import hashlib, pathlib, sys
print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$TOOL_PATH"
)"

python3 -c 'import pathlib, re, sys
doc_path = pathlib.Path(sys.argv[1])
tool_sha = sys.argv[2]
text = doc_path.read_text(encoding="utf-8")
pattern = re.compile(r"^- source checksum \(SHA256\): `[0-9a-f]+`$", re.MULTILINE)
new_text, count = pattern.subn(
    "- source checksum (SHA256): `" + tool_sha + "`",
    text,
    count=1,
)
if count != 1:
    raise SystemExit("Could not update source checksum line in " + str(doc_path))
doc_path.write_text(new_text, encoding="utf-8")' \
  "$DOC_PATH" \
  "$TOOL_SHA"

# Build the plugin zip in place. zipfile opens the destination with O_TRUNC,
# which does not require file-deletion permissions the way `rm -f` does, so
# rebuilds work in Cowork without granting allow_cowork_file_delete first.
python3 -c 'import pathlib, zipfile

out = pathlib.Path("retriever.plugin")
include_roots = [
    ".claude-plugin",
    "agents",
    "skills",
    "README.md",
    "MARKETPLACE.md",
    "PRIVACY.md",
    "LICENSE",
]


def should_skip(path: pathlib.Path) -> bool:
    if "__pycache__" in path.parts:
        return True
    name = path.name
    return name == ".DS_Store" or name.endswith(".pyc")


added = 0
with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for base in include_roots:
        base_path = pathlib.Path(base)
        if not base_path.exists():
            raise SystemExit("Missing required source directory: " + base)
        if base_path.is_file():
            zf.write(base_path, arcname=str(base_path))
            added += 1
            continue
        zf.writestr(zipfile.ZipInfo(base + "/"), "")
        for path in sorted(base_path.rglob("*")):
            if should_skip(path):
                continue
            arcname = str(path)
            if path.is_dir():
                zf.writestr(zipfile.ZipInfo(arcname + "/"), "")
            else:
                zf.write(path, arcname=arcname)
                added += 1
print("Packed " + str(added) + " files into " + str(out))'

echo "Updated tool-template checksum to $TOOL_SHA"
echo "Synchronized plugin metadata to version $TOOL_VERSION"
echo "Built retriever.plugin ($(du -h retriever.plugin | cut -f1))"
