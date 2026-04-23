---
name: page
description: >
  This skill should be used when the user types "/page", "/page 3",
  "/page first", "/page last", "/page next", or "/page previous". It exposes
  Retriever's visible slash-command surface for pagination.
metadata:
  version: "0.17.2"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `retriever_tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /page

Use this skill for `/page`, `/page <N>`, `/page first`, `/page last`, `/page next`, and `/page previous`.

## Read-only fast path

For the exact read-only form `/page`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Run exactly one Bash command from the workspace root:
  - `/page`: `python3 .retriever/bin/retriever_tools.py slash . /page`
- If the workspace tool is stale or missing, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/page <N>`, `/page first`, `/page last`, `/page next`, and `/page previous`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Treat this skill as the slash command `/page`.
3. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
