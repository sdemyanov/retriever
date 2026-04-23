---
name: field
description: >
  Use this skill when the user wants to manage the schema of custom document
  fields — list, add, rename, delete, re-describe, or change the type of a
  custom field — or when the user types "/field", "/field list", "/field add",
  "/field rename", "/field delete", "/field describe", or "/field type".
metadata:
  version: "1.0.2"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /field

Use this skill for `/field`, `/field list`, `/field add`, `/field rename`, `/field delete`, `/field describe`, and `/field type`.

## Read-only fast path

For the exact read-only forms `/field` and `/field list`:

- Do not read [../schema/schema.md](../schema/schema.md).
- Do not read other Retriever skills.
- Run exactly one Bash command from the workspace root:
  - `/field`: `python3 skills/tool-template/tools.py slash . /field`
  - `/field list`: `python3 skills/tool-template/tools.py slash . /field list`
- If canonical tool auto-discovery fails, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/tools.py](../tool-template/tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

1. Read [../schema/schema.md](../schema/schema.md).
2. Read [../workspace/workspace.md](../workspace/workspace.md).
3. Treat this skill as the slash command `/field`.
4. Use `/field add`, `/field rename`, `/field describe`, `/field type`, and `/field delete` as appropriate.
5. For deletes, stop at the preview unless the user has explicitly confirmed the irreversible removal; only then send the `--confirm` form.
6. Return only the resulting Retriever output. Do not add a preamble, trailing summary, or follow-up suggestion.
