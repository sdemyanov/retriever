---
name: from-run
description: >
  This skill should be used when the user types "/from-run", "/from-run 42",
  or "/from-run clear". It exposes Retriever's visible slash-command surface for
  scoping results to a processing run.
metadata:
  version: "0.17.2"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /from-run

Use this skill for `/from-run`, `/from-run <run-id>`, and `/from-run clear`.

## Read-only fast path

For the exact read-only form `/from-run`:

- Do not read [../search/SKILL.md](../search/SKILL.md).
- Do not read [../run-job/SKILL.md](../run-job/SKILL.md) unless the command fails.
- Run exactly one Bash command from the workspace root:
  - `/from-run`: `python3 skills/tool-template/tools.py slash . /from-run`
- If canonical tool auto-discovery fails, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/tools.py](../tool-template/tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Other forms

For `/from-run <run-id>` and `/from-run clear`:

1. Read [../search/SKILL.md](../search/SKILL.md).
2. Read [../run-job/SKILL.md](../run-job/SKILL.md) only if run-id semantics are unclear.
3. Treat this skill as the slash command `/from-run`.
4. Use the run id exactly as provided unless the user asks for help finding it.
5. Return only the resulting Retriever state or table output. Do not add a preamble, trailing summary, or follow-up suggestion.
