---
name: columns
description: >
  This skill should be used when the user types "/columns", "/columns list",
  "/columns set ...", "/columns add ...", "/columns remove ...", or "/columns default".
  It exposes Retriever's visible slash-command surface for inspecting and changing
  displayed columns.
metadata:
  version: "0.17.2"
---

# Retriever /columns

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) if displayable field names are unclear.

## Behavior

- Treat this skill as the slash command `/columns`.
- Supported forms:
  - `/columns` shows the active display columns.
  - `/columns list` lists displayable columns.
  - `/columns set ...` replaces the active column set.
  - `/columns add ...` adds one or more columns.
  - `/columns remove ...` removes one or more columns.
  - `/columns default` restores the default columns.
- Keep bare `/columns` read-only and use `list` for discoverability.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
