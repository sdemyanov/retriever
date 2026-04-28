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
  version: "1.1.11"
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
- Follow the shared ingest preflight in [../workspace/workspace.md](../workspace/workspace.md) before running workspace-local commands. That contract handles missing tools, clean-but-stale auto-upgrades, and user-modified tool protection without changing the chosen ingest intent.
- Prefer plain `ingest` for normal Cowork indexing and refresh tasks. It is the bounded V2 facade by default.
- Use one bounded command per Cowork/bash call, normally:
  `python3 skills/tool-template/tools.py ingest <workspace> --recursive --budget-seconds 35`
- If the result has `more_work_remaining: true`, continue with the returned `next_recommended_commands` until status is `completed`, `failed`, or `canceled`.
- Do not use background jobs or shell loops for ingest. Do not manually call `ingest-start` / `ingest-run-step` unless you need to inspect or recover a specific active run.
- If an active ingest run already exists, resume it from `next_recommended_commands` or cancel it intentionally; do not start a second run.
- For `.pst` sources, use regular `ingest`, not `ingest-production`.
- If `workspace status` reports `pst_backend.status == fail`, explain that PST ingest needs the required `libpff-python` / `pypff` backend installed.
- If the user target is a processed production root during normal Cowork work, still prefer the bounded plain ingest facade with `--path <production-root-relative-path>` and no `--file-types`; it will auto-route the production.
- Use `ingest-production` only when the user explicitly asks for that command, when running a targeted script outside the Cowork time limit, or when debugging parity with the production-only path.
- Use legacy ingest only when explicitly requested or when debugging parity with the old one-shot pipeline: `python3 skills/tool-template/tools.py ingest <workspace> --recursive --legacy`.
- Treat `DAT` + companion `TEXT/`, `IMAGES/`, and optional `NATIVES/` folders as a production signature, not as loose files.
- Run plain `ingest` with `--recursive` when the user wants the whole tree scanned.
- When the user asks to reingest only a subtree or file inside the workspace, keep the positional workspace root unchanged and pass one or more `--path <relative-path>` flags.
- Plain `ingest` without `--file-types` now auto-routes detected production roots through the production ingest pipeline while still indexing loose files elsewhere in the tree.
- If plain `ingest` reports `skipped_production_roots`, explain that Retriever detected a processed production but skipped it because a file-type filter was present; use `ingest-production` when the user's intent is to index that production root explicitly.
- Summarize `new`, `updated`, `renamed`, `missing`, `skipped`, and `failed` for plain ingest.
- When plain ingest auto-routes productions, also summarize `ingested_production_roots` plus the production document create/update/unchanged/retired and linked-part counters.
- Summarize `created`, `updated`, `unchanged`, `retired`, family reconstruction, linked images, and missing linked parts for `ingest-production`.
- If failures are present, list the failed relative paths and short reasons.
