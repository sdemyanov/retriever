---
name: bates
description: >
  This skill should be used when the user types "/bates", "/bates ABC0001-ABC0010",
  or "/bates clear". It exposes Retriever's visible slash-command surface for
  Bates-aware browsing.
metadata:
  version: "0.17.2"
---

# Retriever /bates

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) if field names or operators are unclear.

## Behavior

- Treat this skill as the slash command `/bates`.
- Supported forms:
  - `/bates` shows the active Bates selector.
  - `/bates <token-or-range>` applies a Bates-aware scope or browse.
  - `/bates clear` clears the active Bates selector.
- Prefer the Bates-aware path over plain keyword FTS when the user provides a single control token or a Bates range.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
