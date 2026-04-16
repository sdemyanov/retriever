# Workspace Bootstrap Contract

## Purpose

This file defines how Retriever initializes and maintains a workspace-local installation.

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
├── bin/
│   ├── retriever_tools.py
│   └── backups/
├── jobs/
├── logs/
└── runtime.json
```

### Directory purposes

- `retriever.db`: primary SQLite database
- `previews/`: generated preview artifacts for unsupported native formats
- `bin/retriever_tools.py`: workspace-local tool copy
- `bin/backups/`: preserved copies of previously materialized tool versions
- `jobs/`: reserved workspace state for future bounded or background workflows
- `logs/`: structured logs and diagnostics
- `runtime.json`: local installation metadata

## First-run bootstrap

Follow this order:

1. Confirm the workspace root.
2. Run the runtime check.
3. Create the `.retriever/` directory tree.
4. Install pinned dependencies from [requirements.lock.md](requirements.lock.md), including the required PST backend.
5. Materialize `retriever_tools.py` from the canonical template in [../tool-template/tool-template.md](../tool-template/tool-template.md).
6. Run the tool's `bootstrap` command to create or upgrade schema v9.
7. Write `runtime.json`.

## Subsequent sessions

On every later session:

1. Run a quick runtime check.
2. Inspect `.retriever/runtime.json` if present.
3. Confirm the tool exists at `.retriever/bin/retriever_tools.py`.
4. Compare the current tool checksum to the stored checksum.
5. Compare the installed plugin's canonical template checksum to `runtime.json.template_sha256`.
6. Reuse the existing tool only when the workspace checksum matches `runtime.json` and the canonical template checksum still matches the current workspace tool checksum.
7. If the canonical template checksum changed, treat that as an upgrade signal even when the plugin version string did not change.
8. Before any reindex or ingest after reinstall, upgrade the workspace tool if needed, run `bootstrap`, and only then continue to `ingest`.

## Upgrade rules

Treat the workspace tool as plugin-managed but user-modifiable.

A reinstall with a changed canonical template is still an upgrade, even if the plugin version string stayed the same.

### Unmodified tool

If:

- the tool file exists
- the checksum matches `runtime.json`
- either the version is older than the plugin version, or the installed plugin's canonical template checksum differs from `runtime.json.template_sha256`

Then:

- back up the old copy to `bin/backups/`
- replace it with the new template
- run schema migrations if needed
- update `runtime.json`
- only then run `ingest` or any reindex flow

### Modified tool

If:

- the tool file exists
- the checksum does not match `runtime.json`

Then:

- assume the workspace copy was modified
- do not replace it automatically
- explain the mismatch to the user
- offer backup + replace as an explicit action

## runtime.json contract

Write a JSON object with these fields:

```json
{
  "tool_version": "0.9.4",
  "schema_version": 9,
  "requirements_version": "2026-04-16-phase4-pst",
  "template_source": "skills/tool-template/retriever_tools.py",
  "template_sha256": "<sha256 of workspace tool file>",
  "python_version": "3.10.12",
  "generated_at": "2026-04-14T00:00:00Z",
  "last_verified_at": "2026-04-14T00:00:00Z"
}
```

Notes:

- `template_sha256` should reflect the materialized workspace copy
- timestamps must be UTC ISO 8601 with `Z`
- `schema_version` tracks the actual database schema, not the plugin version
- a successful workspace bootstrap implies the pinned PST backend imports cleanly; `doctor` should fail until `pst_backend.status` is `pass`
- for future schema changes with real migration vs. reindex tradeoffs, stop and ask the user before assuming migration is preferred
- schema v7 renames `display_id` to `control_number`, renames the related helper columns and batch table, and keeps one-level email attachment families; existing workspaces should reindex parent emails to populate child attachment rows and preserve family numbering

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

- Phase 1 and Phase 2 do not require resumable background review jobs.
- If review work is present before that later phase, treat it as bounded or synchronous work rather than as a checkpointed job system.
