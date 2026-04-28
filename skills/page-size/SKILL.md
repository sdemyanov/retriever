---
name: page-size
description: >
  This skill should be used when the user types "/page-size" or "/page-size 25".
  It exposes Retriever's visible slash-command surface for inspecting and changing
  rows per page.
metadata:
  version: "1.1.11"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /page-size

Use this skill for `/page-size` and `/page-size <N>`.

## Read-only fast path

For the exact read-only form `/page-size`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Run exactly one Bash command from the workspace root:
  - `/page-size`: `python3 skills/tool-template/tools.py slash . /page-size`
- If canonical tool auto-discovery fails, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/tools.py](../tool-template/tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/page-size <N>`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Treat this skill as the slash command `/page-size`.
3. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
