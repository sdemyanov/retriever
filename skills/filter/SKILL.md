---
name: filter
description: >
  This skill should be used when the user types "/filter", "/filter ...",
  or "/filter clear". It exposes Retriever's visible slash-command surface for
  SQL-like filter refinement.
metadata:
  version: "0.17.2"
---

# Retriever /filter

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) if field names or operators are unclear.

## Behavior

- Treat this skill as the slash command `/filter`.
- Supported forms:
  - `/filter` shows the active filter selector.
  - `/filter <expression>` applies a SQL-WHERE-subset refinement.
  - `/filter clear` clears the active filter selector.
- Prefer SQL-like filter expressions over tuple-style field/operator/value fragments.
- Use canonical field names from the schema.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
