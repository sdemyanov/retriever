---
name: add-field
description: >
  Use this skill when the user wants Retriever to add a custom document field,
  register field instructions, or extend the documents table with user-managed metadata.
metadata:
  version: "0.9.4"
---

# Retriever Add Field

Use this skill when the user says things like:

- "add a custom field"
- "create a field called privilege_status"
- "track this metadata on documents"

## Load order

1. Read [../schema/schema.md](../schema/schema.md).
2. Read [../workspace/workspace.md](../workspace/workspace.md).
3. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the canonical workspace tool bundle if materialization is needed.

## Execution rules

- Confirm or infer the workspace root.
- Use this command only for user-managed custom fields, not built-in or system-managed fields.
- Supported custom field types are `text`, `integer`, `real`, and `boolean`.
- Pass a field instruction when the user provides semantics the field should capture.
- Materialize or upgrade the workspace tool first when needed, then run `bootstrap` before schema-changing commands.
- Summarize the normalized field name, chosen type, instruction, and updated custom field registry state.
