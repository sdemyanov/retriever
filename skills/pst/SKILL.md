---
name: pst
description: >
  Use this skill when the user wants Retriever to ingest or inspect Outlook PST mail archives.
  It covers the first-class libpff-python / pypff-backed PST container pipeline, where one PST source expands into
  message documents plus attachment children.
metadata:
  version: "0.9.4"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `retriever_tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever PST

Use this skill when the user says things like:

- "ingest this PST"
- "index this Outlook archive"
- "scan this mailbox.pst"

## Load order

1. Read [../workspace/workspace.md](../workspace/workspace.md).
2. Read [../parsing/parsing.md](../parsing/parsing.md).
3. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the canonical workspace tool bundle if materialization is needed.

## Execution rules

- Confirm or infer the workspace root.
- Run `doctor` and inspect `pst_backend` before attempting PST ingest.
- If `pst_backend.status` is not `pass`, stop and explain that the required `libpff-python` / `pypff` PST backend is unavailable for PST ingest in the current runtime.
- Use regular `ingest` for PST sources, not `ingest-production`.
- Treat a `.pst` file as a container source that expands into one logical parent document per message, with one level of attachment child documents.
- When a PST source is unchanged, expect Retriever to skip reparsing the source and just refresh seen timestamps.
- When a PST source changes, expect Retriever to update matching messages in place, preserve stable control numbers, and retire removed messages.
- Call out `source_kind = pst`, `source_rel_path`, `source_item_id`, and `source_folder_path` when those details help the user understand the results.
