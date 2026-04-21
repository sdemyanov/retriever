---
name: page
description: >
  This skill should be used when the user types "/page", "/page 3",
  "/page first", "/page last", "/page next", or "/page previous". It exposes
  Retriever's visible slash-command surface for pagination.
metadata:
  version: "0.17.2"
---

# Retriever /page

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).

## Behavior

- Treat this skill as the slash command `/page`.
- Supported forms:
  - `/page` shows the current page state.
  - `/page <N>` jumps to a specific page.
  - `/page first`, `/page last`, `/page next`, and `/page previous` navigate relative to the current page.
- Keep bare `/page` read-only and use arguments to navigate.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
