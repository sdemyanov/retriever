---
name: ingest
description: >
  Use this skill when the user wants Retriever to index a folder, refresh changed files,
  ingest a processed production, or explain what was ingested. It bootstraps the workspace
  if needed, chooses between ingest and ingest-production, and summarizes the results.
metadata:
  version: "0.9.4"
---

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
3. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the canonical workspace tool bundle if materialization is needed.

## Execution rules

- Confirm or infer the workspace root.
- Run `doctor --quick` if runtime state is unclear.
- Inspect `.retriever/runtime.json` when it exists.
- Compare the installed plugin's canonical template checksum from `../tool-template/tool-template.md` to `runtime.json.template_sha256`.
- If `.retriever/bin/retriever_tools.py` is missing, materialize it from the canonical template before running workspace-local commands.
- If the canonical checksum changed and the workspace tool still matches `runtime.json`, back up the old workspace tool, replace it with the canonical template, run `bootstrap`, and only then continue to `ingest`.
- If the workspace tool checksum differs from `runtime.json`, treat it as user-modified and require explicit approval before replacement.
- Never run `ingest` against a stale workspace tool after reinstall. Schema and tool upgrades happen before reindexing.
- Run `bootstrap` before the first ingest or after any schema/tool upgrade.
- For `.pst` sources, use regular `ingest`, not `ingest-production`.
- If `doctor` reports `pst_backend.status != pass`, explain that the workspace is not fully bootstrapped until the required `libpff-python` / `pypff` backend is installed.
- If the user target is a processed production root, or the user explicitly asks for production ingest, run `ingest-production` instead of plain `ingest`.
- Keep `ingest-production` as the precise entrypoint for targeted reruns, scripts, or one-off production-only ingest.
- Treat `DAT` + companion `TEXT/`, `IMAGES/`, and optional `NATIVES/` folders as a production signature, not as loose files.
- Run plain `ingest` with `--recursive` when the user wants the whole tree scanned.
- Plain `ingest` without `--file-types` now auto-routes detected production roots through the production ingest pipeline while still indexing loose files elsewhere in the tree.
- If plain `ingest` reports `skipped_production_roots`, explain that Retriever detected a processed production but skipped it because a file-type filter was present; use `ingest-production` when the user's intent is to index that production root explicitly.
- Summarize `new`, `updated`, `renamed`, `missing`, `skipped`, and `failed` for plain ingest.
- When plain ingest auto-routes productions, also summarize `ingested_production_roots` plus the production document create/update/unchanged/retired and linked-part counters.
- Summarize `created`, `updated`, `unchanged`, `retired`, family reconstruction, linked images, and missing linked parts for `ingest-production`.
- If failures are present, list the failed relative paths and short reasons.
