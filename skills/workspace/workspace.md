# Workspace Bootstrap Contract

## Purpose

This file defines how Retriever initializes and maintains workspace-local state.

Retriever is local-first:

- the user picks a workspace root
- Retriever stores its private state under `.retriever/`
- the original documents remain untouched

## Directory layout

Create this structure under the selected workspace root:

```text
.retriever/
├── retriever.db
├── previews/
├── text-revisions/
├── jobs/
├── locks/
├── logs/
└── runtime.json
```

### Directory purposes

- `retriever.db`: primary SQLite database
- `previews/`: generated preview artifacts for unsupported native formats
- `text-revisions/`: derived OCR, translation, and image-description text revisions
- `jobs/`: planned processing-run state and related artifacts
- `locks/`: local process coordination for bounded ingest and rebuild workflows
- `logs/`: structured logs and diagnostics
- `runtime.json`: local installation metadata

## First-run bootstrap

Follow this order:

1. Confirm the workspace root.
2. Run `python3 skills/tool-template/tools.py workspace status --quick <workspace>`.
3. Create the `.retriever/` directory tree.
4. Install pinned dependencies from [requirements.lock.md](requirements.lock.md), including the required PST backend.
5. Resolve the canonical [../tool-template/tools.py](../tool-template/tools.py) bundle that will manage the workspace.
6. Run `python3 skills/tool-template/tools.py workspace init <workspace>` to create or upgrade schema `25`.
7. Write `runtime.json`.

## Subsequent sessions

On every later session:

1. Run `workspace status --quick`.
2. Inspect `.retriever/runtime.json` if present.
3. Confirm the canonical tool exists at `skills/tool-template/tools.py`.
4. Compare the current canonical tool checksum to the stored checksum in `runtime.json.template_sha256`.
5. If the canonical template checksum changed, treat that as a runtime refresh signal even when the plugin version string did not change.
6. The canonical tool refreshes `runtime.json` / `workspace_meta` on the next non-exempt command when the recorded checksum is stale.

## Runtime refresh rules

Retriever now runs the canonical [../tool-template/tools.py](../tool-template/tools.py) bundle directly instead of copying a tool snapshot into each workspace.

A reinstall with a changed canonical template is still an upgrade, even if the plugin version string stayed the same.

Before executing any command other than `schema-version` or the grouped `workspace` maintenance surface, the tool calls its internal `maybe_upgrade_workspace_tool(root)` helper. That helper does not replace files inside the workspace. It only refreshes `runtime.json` / `workspace_meta` when:

- `.retriever/` exists
- `runtime.json` exists
- the canonical `skills/tool-template/tools.py` bundle can be located
- the recorded checksum differs from the current canonical bundle checksum

### Explicit runtime refresh command

`tools.py workspace update <workspace> [--from <path>] [--force]`

- default is to auto-discover the canonical tool
- `--force` is accepted for backward compatibility but ignored

## runtime.json contract

Write a JSON object with these fields:

```json
{
  "tool_version": "1.1.11",
  "schema_version": 25,
  "requirements_version": "2026-04-21-phase11-document-deduplication",
  "template_source": "skills/tool-template/tools.py",
  "template_sha256": "<sha256 of canonical tools.py bundle>",
  "python_version": "3.10.12",
  "generated_at": "2026-04-14T00:00:00Z",
  "last_verified_at": "2026-04-14T00:00:00Z"
}
```

Notes:

- `template_sha256` should reflect the canonical `tools.py` bundle used most recently for the workspace
- timestamps must be UTC ISO 8601 with `Z`
- `schema_version` tracks the actual database schema, not the plugin version
- `workspace status` reports parser/runtime readiness, including `pst_backend.status`
- for future schema changes with real migration vs. reindex tradeoffs, stop and ask the user before assuming migration is preferred
- existing workspaces should reindex parent/container sources when a new parser or metadata field needs backfill

## Failure behavior

If bootstrap cannot complete:

- do not leave a half-written `runtime.json` claiming success
- preserve any valid files already created
- report which step failed
- prefer idempotent reruns over cleanup-heavy rollback logic

## Path rules

- Store document paths in SQLite relative to the workspace root
- Store preview paths relative to `.retriever/`
- Absolute paths may appear in diagnostics and runtime metadata, but not in document records

## Manual value protection rule

- Manual user edits must never be overwritten by automated ingest or review unless the user explicitly requests overwrite.
- Retriever preserves those manual values by recording the corrected field names in `documents.manual_field_locks_json`.
- The rule applies to both editable built-in metadata fields and custom field columns added to `documents`.
- Future ingest or review runs may refresh unlocked fields, but must leave locked fields unchanged until the user explicitly clears the lock.

## Current scope note

- Ingest and entity rebuild have bounded/resumable workflows. Prefer the facade commands (`ingest`, `rebuild-entities-run-step`) over long one-shot commands.
- Planned processing runs should use `run-job-step` for Cowork-safe execution; `execute-run` is legacy/debug only.
