---
name: parsing
description: >
  Use this skill when Retriever needs to ingest, parse, normalize, or preview
  supported document types. It defines file-type support, encoding behavior,
  preview artifact rules, and per-file failure isolation.
metadata:
  version: "1.1.11"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Parsing

Use this skill whenever a task changes or depends on document extraction behavior.

## Required reference

Read [parsing.md](parsing.md) before changing ingest logic or parser dependencies.
For spreadsheet parser redesign work, also read [SPREADSHEET_PARSING_PLAN.md](../../plans/SPREADSHEET_PARSING_PLAN.md).
For ingest execution-model redesign, also read [parallel-ingest-plan.md](../../plans/parallel-ingest-plan.md).

## Rules

- Keep ingest transactional per file so one bad document never blocks the rest.
- Normalize decoded text to UTF-8 before storing it.
- Prefer preserving the original file and writing previews under `.retriever/previews/`.
- Treat unsupported or corrupt files as structured failures, not fatal batch errors.
