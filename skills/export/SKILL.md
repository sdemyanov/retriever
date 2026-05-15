---
name: export
description: >
  Use this skill when the user wants to export the current review scope as a
  CSV table or archive, inspect export progress, or otherwise "save", "download",
  "bundle", or "hand off" Retriever results — or when the user types "/export".
metadata:
  version: "1.1.16"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever /export

Use this skill for `/export table ...`, `/export archive ...`, `/export status ...`,
and natural-language requests like:

- "export this review set to CSV"
- "make a spreadsheet of the current results"
- "build a zip I can share"
- "is the export still running?"

## Exact slash fast path

For exact slash forms that begin with `/export`:

- Do not read other skill docs first.
- Run exactly one Bash command from the workspace root that passes the slash
  command through unchanged:
  - `/export table documents`: `python3 skills/tool-template/tools.py slash . '/export table documents'`
  - `/export archive --portable-workspace`: `python3 skills/tool-template/tools.py slash . '/export archive --portable-workspace'`
  - `/export status`: `python3 skills/tool-template/tools.py slash . '/export status'`
- Return stdout exactly as the entire response. No preamble. No commentary. No
  reformatting.

## Natural-language mapping

1. If the user wants a spreadsheet or row-based handoff, prefer `/export table documents`.
2. If the user wants files, previews, or a shareable bundle, prefer `/export archive`.
3. Use the active scope by default. If there is no active scope yet, establish
   it first with `retriever:search`, `retriever:dataset`, `retriever:filter`,
   `retriever:bates`, or `retriever:scope`.
4. Use `--portable-workspace` when the user wants a self-contained handoff.
5. Use `/export status` when the user asks about progress, the latest export
   run, or the next recommended export command.
6. Default to document exports unless the user explicitly asks for conversation
   or entity tables.
7. Keep the response outcome-focused: tell the user what was exported or return
   the export status/result directly. Do not add a long explanation.
