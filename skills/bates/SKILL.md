---
name: bates
description: >
  This skill should be used when the user types "/bates", "/bates ABC0001-ABC0010",
  or "/bates clear". It exposes Retriever's visible slash-command surface for
  Bates-aware browsing.
metadata:
  version: "1.1.11"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /bates

Use this skill for `/bates`, `/bates <token-or-range>`, and `/bates clear`.

## Read-only fast path

For the exact read-only form `/bates`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Do not read schema docs.
- Run exactly one Bash command from the workspace root:
  - `/bates`: `python3 skills/tool-template/tools.py slash . /bates`
- If canonical tool auto-discovery fails, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/tools.py](../tool-template/tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/bates <token-or-range>` and `/bates clear`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../schema/schema.md](../schema/schema.md) only if Bates-like identifier semantics are unclear.
3. Treat this skill as the slash command `/bates`.
4. Prefer the Bates-aware path over plain keyword FTS when the user provides a single control token or a Bates range.
5. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
