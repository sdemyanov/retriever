---
name: filter
description: >
  This skill should be used when the user types "/filter", "/filter ...",
  or "/filter clear". It exposes Retriever's visible slash-command surface for
  SQL-like filter refinement.
metadata:
  version: "1.1.11"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /filter

Use this skill for `/filter`, `/filter <expression>`, and `/filter clear`.

## Read-only fast path

For the exact read-only form `/filter`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Do not read schema docs.
- Run exactly one Bash command from the workspace root:
  - `/filter`: `python3 skills/tool-template/tools.py slash . /filter`
- If canonical tool auto-discovery fails, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/tools.py](../tool-template/tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/filter <expression>` and `/filter clear`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) only if field names or operators are unclear.
3. Treat this skill as the slash command `/filter`.
4. Prefer SQL-like filter expressions over tuple-style field/operator/value fragments.
5. Use canonical field names from the schema.
6. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
