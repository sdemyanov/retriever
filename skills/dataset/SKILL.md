---
name: dataset
description: >
  This skill should be used when the user types "/dataset", "/dataset list",
  "/dataset production", or "/dataset clear". It exposes Retriever's visible slash-command
  surface for dataset scoping and discovery.
metadata:
  version: "0.17.2"
---

# Retriever /dataset

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) if field names are unclear.

## Behavior

- Treat this skill as the slash command `/dataset`.
- Supported forms:
  - `/dataset` shows the active dataset selector.
  - `/dataset list` lists available datasets.
  - `/dataset <name>` scopes the browse surface to a dataset.
  - `/dataset clear` clears the active dataset selector.
- Keep bare `/dataset` read-only and use `list` for available options.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
