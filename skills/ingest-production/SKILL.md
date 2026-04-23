---
name: ingest-production
description: >
  Use this skill when the user wants Retriever to ingest a processed litigation or
  eDiscovery production such as a DAT/OPT/TEXT/IMAGES set, a Bates-numbered volume,
  or explicitly asks to run ingest-production.
metadata:
  version: "0.9.4"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `retriever_tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Production Ingest

Use this skill when the user says things like:

- "ingest this production"
- "load this DAT/OPT set"
- "index this Bates production"
- "run ingest-production"

## Load order

1. Read [../workspace/workspace.md](../workspace/workspace.md).
2. Read [../parsing/parsing.md](../parsing/parsing.md).
3. Read [../schema/schema.md](../schema/schema.md) if Bates fields, family reconstruction, or source parts matter.
4. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the canonical workspace tool bundle if materialization is needed.

## Execution rules

- Confirm or infer both the workspace root and the candidate production root.
- Run `workspace status --quick` if runtime state is unclear.
- Follow the shared ingest preflight in [../workspace/workspace.md](../workspace/workspace.md) before running workspace-local commands. That contract handles missing tools, clean-but-stale auto-upgrades, and user-modified tool protection without changing the intended `ingest-production` command.
- Validate that the target looks like a supported processed production root, not just a loose folder of files.
- Run `ingest-production` against the production root, not plain `ingest`.
- Do not fall back to plain loose-file ingest unless the user explicitly wants that behavior.
- Summarize created, updated, unchanged, and retired logical documents.
- Call out family reconstruction, linked page images, docs missing linked text, docs missing linked images, and docs missing linked natives.
- Note that produced Bates values become `control_number` and that linked `TEXT/`, `IMAGES/`, and `NATIVES/` files remain source parts rather than top-level documents.
