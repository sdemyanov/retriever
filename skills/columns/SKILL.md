---
name: columns
description: >
  This skill should be used when the user types "/columns", "/columns list",
  "/columns set ...", "/columns add ...", "/columns remove ...", or "/columns default".
  It exposes Retriever's visible slash-command surface for inspecting and changing
  displayed columns.
metadata:
  version: "0.17.2"
---

# Retriever /columns

Use this skill for `/columns`, `/columns list`, `/columns set ...`, `/columns add ...`, `/columns remove ...`, and `/columns default`.

## Read-only fast path

For the exact read-only forms `/columns` and `/columns list`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Do not read schema docs.
- Run exactly one Bash command from the workspace root:
  - `/columns`: `python3 .retriever/bin/retriever_tools.py slash . /columns`
  - `/columns list`: `python3 .retriever/bin/retriever_tools.py slash . /columns list`
- If the workspace tool is stale or missing, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/columns set ...`, `/columns add ...`, `/columns remove ...`, and `/columns default`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) only if displayable field names are unclear.
3. Treat this skill as the slash command `/columns`.
4. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
