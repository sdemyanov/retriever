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

TOOL_VERSION="$(
  python3 -c 'import pathlib, re
text = pathlib.Path("'"$SOURCE_HEADER_PATH"'").read_text(encoding="utf-8")
match = re.search(r"^TOOL_VERSION = \"([^\"]+)\"$", text, re.MULTILINE)
if match is None:
    raise SystemExit("Could not determine TOOL_VERSION from skills/tool-template/src/00_header.py")
print(match.group(1))'
)"

python3 -c 'import json, pathlib, re, sys
tool_version = sys.argv[1]
manifest_path = pathlib.Path(sys.argv[2])
ping_skill_path = pathlib.Path(sys.argv[3])

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
manifest["version"] = tool_version
manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

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
  "$PLUGIN_MANIFEST_PATH" \
  "$PING_SKILL_PATH"

python3 skills/tool-template/bundle_retriever_tools.py

python3 skills/routing/generate_reference.py

# Diagnostic: allow excluding specific skills from the archive via env var,
# e.g. RETRIEVER_EXCLUDE_SKILLS=routing,foo


TOOL_PATH="skills/tool-template/retriever_tools.py"
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
python3 -c 'import os, pathlib, zipfile

out = pathlib.Path("retriever.plugin")
include_roots = [".claude-plugin", "skills"]
excluded_skills = set(filter(None, os.environ.get("RETRIEVER_EXCLUDE_SKILLS", "").split(",")))
if excluded_skills:
    print("Excluding skills: " + ", ".join(sorted(excluded_skills)))


def should_skip(path: pathlib.Path) -> bool:
    if "__pycache__" in path.parts:
        return True
    name = path.name
    return name == ".DS_Store" or name.endswith(".pyc")


def is_empty_skill_dir(path: pathlib.Path) -> bool:
    # Skill subdirectories under skills/ must contain a SKILL.md. Empty
    # leftover directories (e.g. from renames) would otherwise ship as
    # invalid skills and fail plugin validation.
    if not path.is_dir():
        return False
    parts = path.parts
    if len(parts) != 2 or parts[0] != "skills":
        return False
    return not (path / "SKILL.md").exists()


def is_excluded_skill_path(path: pathlib.Path) -> bool:
    if not excluded_skills:
        return False
    parts = path.parts
    return len(parts) >= 2 and parts[0] == "skills" and parts[1] in excluded_skills


added = 0
with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for base in include_roots:
        base_path = pathlib.Path(base)
        if not base_path.exists():
            raise SystemExit("Missing required source directory: " + base)
        zf.writestr(zipfile.ZipInfo(base + "/"), "")
        skipped_skills = []
        for path in sorted(base_path.rglob("*")):
            if should_skip(path):
                continue
            if is_empty_skill_dir(path):
                skipped_skills.append(path.name)
                continue
            if is_excluded_skill_path(path):
                continue
            # Skip anything nested under an empty skill dir.
            if any(is_empty_skill_dir(pathlib.Path(*path.parts[:i+1])) for i in range(len(path.parts))):
                continue
            arcname = str(path)
            if path.is_dir():
                zf.writestr(zipfile.ZipInfo(arcname + "/"), "")
            else:
                zf.write(path, arcname=arcname)
                added += 1
        if skipped_skills:
            print("Skipped empty skill dirs: " + ", ".join(skipped_skills))
print("Packed " + str(added) + " files into " + str(out))'

echo "Updated tool-template checksum to $TOOL_SHA"
echo "Synchronized plugin metadata to version $TOOL_VERSION"
echo "Built retriever.plugin ($(du -h retriever.plugin | cut -f1))"
