---
name: set-field
description: >
  Use this skill when the user wants Retriever to set or correct a field value on a
  specific document, including editable built-in fields and custom fields.
metadata:
  version: "0.9.4"
---

# Retriever Set Field

Use this skill when the user says things like:

- "set title on this document"
- "update the author field"
- "mark this document as privileged"

## Load order

1. Read [../schema/schema.md](../schema/schema.md).
2. Read [../search/SKILL.md](../search/SKILL.md) if you need to find the target document id first.
3. Read [../workspace/workspace.md](../workspace/workspace.md).
4. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the canonical workspace tool bundle if materialization is needed.

## Execution rules

- Confirm or infer the workspace root.
- If the user did not provide a document id, identify it first through Retriever search before running `set-field`.
- Use this only for editable built-in fields and custom fields.
- Never use this for system-managed fields such as hashes, ingest timestamps, path identity, production bookkeeping, or control-number helper fields.
- Let the tool coerce `integer`, `real`, and `boolean` values to the correct storage type.
- Remember that `set-field` automatically adds the target field to `manual_field_locks_json` after a successful update.
- Summarize the document id, field name, typed value, and resulting manual locks.
