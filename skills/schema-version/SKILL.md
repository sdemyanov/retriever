---
name: schema-version
description: >
  Use this skill when the user wants to know the current Retriever tool or schema version
  for a workspace, or explicitly asks to run schema-version.
metadata:
  version: "0.9.4"
---

# Retriever Schema Version

Use this skill when the user says things like:

- "what schema version is this workspace on?"
- "run schema-version"
- "which Retriever tool version is installed?"

## Load order

1. Read [../workspace/workspace.md](../workspace/workspace.md).
2. Use [../tool-template/tools.py](../tool-template/tools.py) as the canonical tool entrypoint.

## Execution rules

- Confirm or infer the workspace root.
- Run the canonical bundled tool against the workspace root.
- Report the returned `schema_version` and `tool_version` faithfully.
