---
name: next
description: >
  This skill should be used when the user types "/next" or asks for the next page
  of the active Retriever browse results. It exposes Retriever's visible slash-command
  surface for forward pagination.
metadata:
  version: "0.17.2"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /next

Use this skill for the exact slash command `/next`.

## Fast path

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Run exactly one Bash command from the workspace root:
  - `/next`: `python3 skills/tool-template/tools.py slash . /next`
- If canonical tool auto-discovery fails, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/tools.py](../tool-template/tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Behavior

- Treat this skill as the slash command `/next`.
- This is equivalent to `/page next`.
- Fetch the next page of the active Retriever browse state.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
