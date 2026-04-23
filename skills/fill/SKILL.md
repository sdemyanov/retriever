---
name: fill
description: >
  Use this skill when the user wants to populate, set, tag, mark, label,
  classify, annotate, flag, or clear a custom or editable built-in field value
  on one document or on a filtered/scoped result set — or when the user types
  "/fill".
metadata:
  version: "1.0.2"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /fill

Use this skill for `/fill <field> <value>`, `/fill <field> clear`, `/fill ... on <doc-ref>`, and `/fill ... on <doc-ref,doc-ref,...>`.

## Execution rules

1. Prefer the slash surface: `python3 skills/tool-template/tools.py slash . /fill ...`.
2. If you need to establish or inspect the active browse state first, read [../search/SKILL.md](../search/SKILL.md).
3. If the user supplied explicit document references, preserve them and pass them through the `on <doc-ref[,doc-ref,...]>` form.
4. If there is no explicit `on ...`, rely on the active browse state. If no active selection exists yet, narrow it first with `retriever:search`, `retriever:dataset`, `retriever:filter`, `retriever:bates`, `retriever:from-run`, or by asking the user for the target documents.
5. Bulk fills require `--confirm`; single explicit-document fills do not.
6. Do not target derived or system-managed fields such as `custodian`, `dataset_name`, `production_name`, hashes, ids, or ingest timestamps.
7. Return only the resulting Retriever output. Do not add a preamble, trailing summary, or follow-up suggestion.
