---
name: ingest
description: >
  Use this skill when the user wants to ingest, index, import, load, add, upload,
  or process files or folders into Retriever, or to refresh changed files, ingest a
  processed production, or explain what was ingested — including phrasings like
  "index this folder", "import these files", "load the Downloads directory",
  "add this PST to the collection", "upload and process this batch", "re-index the
  gmail-max mbox", or "what did you ingest". It bootstraps the workspace if needed,
  chooses between ingest and ingest-production, and summarizes the results.
metadata:
  version: "0.9.4"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Ingest

Use this skill when the user says things like:

- "index these documents"
- "refresh the workspace"
- "re-ingest this folder"
- "scan for new files"
- "ingest this PST"
- "ingest this production"
- "load this DAT/OPT set"
- "index this Bates production"

## Load order

1. Read [../workspace/workspace.md](../workspace/workspace.md).
2. Read [../parsing/parsing.md](../parsing/parsing.md) if parsing or failure behavior matters.
3. Use [../tool-template/tools.py](../tool-template/tools.py) as the canonical tool entrypoint.

## Execution rules

- Confirm or infer the workspace root.
- Run `workspace status --quick` if runtime state is unclear.
- Follow the shared ingest preflight in [../workspace/workspace.md](../workspace/workspace.md) before running workspace-local commands. That contract handles missing tools, clean-but-stale auto-upgrades, and user-modified tool protection without changing the intended `ingest` vs. `ingest-production` command.
- For `.pst` sources, use regular `ingest`, not `ingest-production`.
- If `workspace status` reports `pst_backend.status == fail`, explain that PST ingest needs the required `libpff-python` / `pypff` backend installed.
- If the user target is a processed production root, or the user explicitly asks for production ingest, run `ingest-production` instead of plain `ingest`.
- Keep `ingest-production` as the precise entrypoint for targeted reruns, scripts, or one-off production-only ingest.
- Treat `DAT` + companion `TEXT/`, `IMAGES/`, and optional `NATIVES/` folders as a production signature, not as loose files.
- Run plain `ingest` with `--recursive` when the user wants the whole tree scanned.
- When the user asks to reingest only a subtree or file inside the workspace, keep the positional workspace root unchanged and pass one or more `--path <relative-path>` flags.
- Plain `ingest` without `--file-types` now auto-routes detected production roots through the production ingest pipeline while still indexing loose files elsewhere in the tree.
- If plain `ingest` reports `skipped_production_roots`, explain that Retriever detected a processed production but skipped it because a file-type filter was present; use `ingest-production` when the user's intent is to index that production root explicitly.
- Summarize `new`, `updated`, `renamed`, `missing`, `skipped`, and `failed` for plain ingest.
- When plain ingest auto-routes productions, also summarize `ingested_production_roots` plus the production document create/update/unchanged/retired and linked-part counters.
- Summarize `created`, `updated`, `unchanged`, `retired`, family reconstruction, linked images, and missing linked parts for `ingest-production`.
- If failures are present, list the failed relative paths and short reasons.
