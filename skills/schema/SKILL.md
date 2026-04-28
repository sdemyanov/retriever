---
name: schema
description: >
  Use this skill when creating, inspecting, or migrating Retriever's SQLite schema.
  It is the source of truth for table layouts, path rules, custom field registry
  behavior, manual field locks, and schema-version behavior.
metadata:
  version: "1.1.11"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Schema

Use this skill whenever a task touches:

- SQLite table definitions
- schema migrations
- relative path rules
- preview records
- chunk storage
- custom field registry entries
- manual-value overwrite protection rules
- structured filter validation rules

## Required references

1. Treat [../tool-template/src/10_core.py](../tool-template/src/10_core.py), [../tool-template/src/40_schema_runtime.py](../tool-template/src/40_schema_runtime.py), and the generated [../tool-template/tools.py](../tool-template/tools.py) as the current source of truth.
2. Use `python3 skills/tool-template/tools.py schema-version` to confirm the current `schema_version` and `tool_version`.
3. Read [schema.md](schema.md) for background terminology only; if it conflicts with the source fragments or `schema-version`, the code wins.

## Rules

- Do not invent new table names when an existing table fits the need.
- Keep document paths relative to the workspace root.
- Keep preview paths relative to `.retriever/`.
- Preserve manual edits by locking corrected document fields until explicitly overwritten.
- Prefer structured filter contracts over raw SQL filter strings.
- Make migrations idempotent.
- Prefer additive schema changes over destructive rewrites.
