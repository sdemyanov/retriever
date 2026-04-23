---
name: sort
description: >
  This skill should be used when the user types "/sort", "/sort list",
  "/sort date_created desc", "/sort date_created desc, file_name asc",
  or "/sort default". It exposes Retriever's visible slash-command surface for
  sort inspection and changes.
metadata:
  version: "0.17.3"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `retriever_tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /sort

Use this skill for `/sort`, `/sort list`, `/sort <field> <asc|desc>`, `/sort <field> <asc|desc>, <field> <asc|desc>`, and `/sort default`.

## Read-only fast path

For the exact read-only forms `/sort` and `/sort list`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Do not read schema docs.
- Run exactly one Bash command from the workspace root:
  - `/sort`: `python3 .retriever/bin/retriever_tools.py slash . /sort`
  - `/sort list`: `python3 .retriever/bin/retriever_tools.py slash . /sort list`
- If the workspace tool is stale or missing, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/sort <field> <asc|desc>`, comma-separated multi-column sort specs, and `/sort default`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) only if sortable field names are unclear.
3. Treat this skill as the slash command `/sort`.
4. Preserve comma-separated sort specs exactly as written; do not collapse them to a single column.
5. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
