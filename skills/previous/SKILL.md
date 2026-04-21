---
name: previous
description: >
  This skill should be used when the user types "/previous" or asks for the previous
  page of the active Retriever browse results. It exposes Retriever's visible
  slash-command surface for backward pagination.
metadata:
  version: "0.17.2"
---

# Retriever /previous

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).

## Behavior

- Treat this skill as the slash command `/previous`.
- This is equivalent to `/page previous`.
- Fetch the previous page of the active Retriever browse state.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
