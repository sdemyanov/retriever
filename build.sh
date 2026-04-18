#!/bin/bash
set -euo pipefail

# Build retriever.plugin from source.
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

TOOL_PATH="skills/tool-template/retriever_tools.py"
DOC_PATH="skills/tool-template/tool-template.md"
TOOL_SHA="$(shasum -a 256 "$TOOL_PATH" | awk '{print $1}')"
TMP_DOC="$(mktemp)"

cleanup() {
  rm -f "$TMP_DOC"
}
trap cleanup EXIT

awk -v sha="$TOOL_SHA" '
$0 ~ /^- source checksum \(SHA256\): `[^`]+`$/ {
  print "- source checksum (SHA256): `" sha "`"
  updated = 1
  next
}
{ print }
END {
  if (!updated) {
    exit 1
  }
}
' "$DOC_PATH" > "$TMP_DOC"

mv "$TMP_DOC" "$DOC_PATH"
trap - EXIT
cleanup

rm -f retriever.plugin
zip -r retriever.plugin .claude-plugin/ skills/ -x "*.DS_Store" "*/__pycache__/*" "*.pyc"
echo "Updated tool-template checksum to $TOOL_SHA"
echo "Synchronized plugin metadata to version $TOOL_VERSION"
echo "Built retriever.plugin ($(du -h retriever.plugin | cut -f1))"
