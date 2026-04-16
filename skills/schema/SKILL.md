---
name: schema
description: >
  Use this skill when creating, inspecting, or migrating Retriever's SQLite schema.
  It is the source of truth for table layouts, path rules, custom field registry
  behavior, manual field locks, and schema-version behavior.
metadata:
  version: "0.9.4"
---

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

## Required reference

Read [schema.md](schema.md) before making schema decisions.

## Rules

- Do not invent new table names when an existing table fits the need.
- Keep document paths relative to the workspace root.
- Keep preview paths relative to `.retriever/`.
- Preserve manual edits by locking corrected document fields until explicitly overwritten.
- Prefer structured filter contracts over raw SQL filter strings.
- Make migrations idempotent.
- Prefer additive schema changes over destructive rewrites.
