#!/bin/bash
set -euo pipefail

# Build retriever.plugin from source.
cd "$(dirname "$0")"

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
echo "Built retriever.plugin ($(du -h retriever.plugin | cut -f1))"
