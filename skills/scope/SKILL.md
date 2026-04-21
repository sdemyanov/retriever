---
name: scope
description: >
  This skill should be used when the user types "/scope", "/scope list",
  "/scope clear", "/scope save ...", or "/scope load ...". It exposes Retriever's
  visible slash-command surface for inspecting and managing the active scope.
metadata:
  version: "0.17.2"
---

# Retriever /scope

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).

## Behavior

- Treat this skill as the slash command `/scope`.
- Supported forms:
  - `/scope` shows the active scope without truncation.
  - `/scope list` lists saved scopes.
  - `/scope clear` clears the active scope.
  - `/scope save <name>` saves the current scope.
  - `/scope load <name>` loads a saved scope.
- Keep bare `/scope` read-only and use `list` for discoverability.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
