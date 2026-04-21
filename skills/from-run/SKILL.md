---
name: from-run
description: >
  This skill should be used when the user types "/from-run", "/from-run 42",
  or "/from-run clear". It exposes Retriever's visible slash-command surface for
  scoping results to a processing run.
metadata:
  version: "0.17.2"
---

# Retriever /from-run

This skill is a thin visible alias for Retriever's internal slash-command browse surface.

## Load order

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../run-job/SKILL.md](../run-job/SKILL.md) only if run-id semantics are unclear.

## Behavior

- Treat this skill as the slash command `/from-run`.
- Supported forms:
  - `/from-run` shows the active run selector.
  - `/from-run <run-id>` scopes the browse surface to results from that run.
  - `/from-run clear` clears the active run selector.
- Use the run id exactly as provided unless the user asks for help finding it.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion around the slash-command result.
