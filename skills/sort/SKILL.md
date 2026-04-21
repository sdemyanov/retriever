---
name: sort
description: >
  This skill should be used when the user types "/sort", "/sort list",
  "/sort date_created desc", or "/sort default". It exposes Retriever's visible
  slash-command surface for sort inspection and changes.
metadata:
  version: "0.17.2"
---

# Retriever /sort

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) if sortable field names are unclear.

## Behavior

- Treat this skill as the slash command `/sort`.
- Supported forms:
  - `/sort` shows the active sort.
  - `/sort list` lists sortable fields.
  - `/sort <field> <asc|desc>` changes the sort.
  - `/sort default` restores the default sort.
- Keep bare `/sort` read-only and use `list` for discoverability.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
