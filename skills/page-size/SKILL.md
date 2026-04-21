---
name: page-size
description: >
  This skill should be used when the user types "/page-size" or "/page-size 25".
  It exposes Retriever's visible slash-command surface for inspecting and changing
  rows per page.
metadata:
  version: "0.17.2"
---

# Retriever /page-size

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).

## Behavior

- Treat this skill as the slash command `/page-size`.
- Supported forms:
  - `/page-size` shows the active page size.
  - `/page-size <N>` changes the page size.
- Keep bare `/page-size` read-only and use an argument to change it.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
