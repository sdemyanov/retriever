---
name: dataset
description: >
  This skill should be used when the user types "/dataset", "/dataset list",
  "/dataset production", "/dataset production, priority", "/dataset clear",
  or "/dataset rename old-name new-name". It exposes Retriever's visible
  slash-command surface for dataset scoping, discovery, and renaming.
metadata:
  version: "0.17.3"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `retriever_tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /dataset

Use this skill for `/dataset`, `/dataset list`, `/dataset NAME`, `/dataset NAME, NAME`, `/dataset clear`, and `/dataset rename OLD NEW`.

## Read-only fast path

For the exact read-only forms `/dataset` and `/dataset list`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Do not read schema docs.
- Run exactly one Bash command from the workspace root:
  - `/dataset`: `python3 .retriever/bin/retriever_tools.py slash . /dataset`
  - `/dataset list`: `python3 .retriever/bin/retriever_tools.py slash . /dataset list`
- `/dataset list` renders a dataset stats table with `Dataset`, `Docs`, `Size`, `Custodians`, `Types`, and `Time Range` columns.
- If the workspace tool is stale or missing, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/dataset NAME`, comma-separated dataset selection, `/dataset clear`, and `/dataset rename OLD NEW`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Preserve comma-separated dataset selectors exactly as written; quote names with spaces when needed.
3. Treat this skill as the slash command `/dataset`.
4. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
