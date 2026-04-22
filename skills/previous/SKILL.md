---
name: previous
description: >
  This skill should be used when the user types "/previous" or asks for the previous
  page of the active Retriever browse results. It exposes Retriever's visible
  slash-command surface for backward pagination.
metadata:
  version: "0.17.2"
---

# Retriever /previous

Use this skill for the exact slash command `/previous`.

## Fast path

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Run exactly one Bash command from the workspace root:
  - `/previous`: `python3 .retriever/bin/retriever_tools.py slash . /previous`
- If the workspace tool is stale or missing, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Behavior

- Treat this skill as the slash command `/previous`.
- This is equivalent to `/page previous`.
- Fetch the previous page of the active Retriever browse state.
- Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
