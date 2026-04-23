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
8. The workspace tool's own dispatcher performs the auto-upgrade on the next non-exempt command (see below), so the runner does not need an out-of-band upgrade step before `ingest` or reindex; running any ordinary command from a stale-but-clean workspace will upgrade it and re-exec the command transparently.

## Upgrade rules

Treat the workspace tool as plugin-managed but user-modifiable.

A reinstall with a changed canonical template is still an upgrade, even if the plugin version string stayed the same.

The workspace tool enforces these rules itself on every non-exempt command. The runner is still free to call `upgrade-workspace` explicitly; the auto-upgrade path just removes the need to remember to do it.

### Ingest command preflight

Use this as the shared preflight contract for runners that are about to call `ingest` or `ingest-production`.

- Confirm or infer the workspace root first, and run `doctor --quick` if runtime state is unclear.
- If `.retriever/bin/retriever_tools.py` or `.retriever/runtime.json` is missing, materialize the canonical workspace tool and run `bootstrap` before continuing.
- Otherwise, run the intended workspace-local command through the existing workspace tool and rely on the dispatcher's auto-upgrade path for clean-but-stale copies.
- If the dispatcher reports `retriever-auto-upgrade: {"status": "blocked", ...}` because the workspace tool is user-modified, stop and ask before `upgrade-workspace --force`.
- After bootstrap or an explicit forced upgrade, resume the original intended command. Do not swap `ingest` for `ingest-production`, or vice versa, just because the workspace tool was refreshed.

### Auto-upgrade dispatch hook

Before executing any command other than `schema-version`, `bootstrap`, `doctor`, `upgrade-workspace`, or `slash`, the tool calls its internal `maybe_upgrade_workspace_tool(root)` helper.

The helper:

- no-ops when `.retriever/` is absent, when `runtime.json` is missing, when the workspace tool is missing, or when the plugin's canonical copy cannot be located (e.g., on a portable workspace without the plugin installed)
- finds the plugin's canonical `skills/tool-template/retriever_tools.py` via the `RETRIEVER_CANONICAL_TOOL_PATH` environment variable, then by walking the parents of the currently running tool
- compares workspace sha vs. canonical sha; if equal, no-op
- if the workspace sha equals `runtime.template_sha256` (clean-but-stale), upgrades in place: backs up the old copy to `bin/backups/`, replaces the tool via `pathlib.Path.write_bytes` (open-with-O_TRUNC, so no `unlink` in Cowork sandboxes), re-runs `write_runtime` / `write_workspace_meta`, and re-execs the new tool so the current command is handled by the new code
- if the workspace sha differs from `runtime.template_sha256` (user-modified), refuses to touch the file, writes a `retriever-auto-upgrade: {"status": "blocked", ...}` line to stderr, and lets the current command continue to run from the user's modified tool
- emits a single `retriever-auto-upgrade: <json>` line to stderr describing the outcome so automation can observe it without polluting the JSON payload on stdout

### Explicit upgrade command

`retriever_tools.py upgrade-workspace <workspace> [--from <path>] [--force]`

- default is to auto-discover the canonical tool the same way the auto path does
- `--force` is required to overwrite a user-modified workspace tool (it adds a `.user-modified` suffix to the backup name so the edit is recoverable)

### Unmodified tool

If:

- the tool file exists
- the checksum matches `runtime.json`
- the canonical template checksum differs

Then the dispatcher auto-upgrades:

- back up the old copy to `bin/backups/`
- replace it with the canonical template via open-with-O_TRUNC (no `rm`, no `mv`)
- re-run `write_runtime` / `write_workspace_meta`
- re-exec the command in the new tool

### Modified tool

If:

- the tool file exists
- the checksum does not match `runtime.json`

Then:

- the dispatcher refuses to replace it
- it emits a `retriever-auto-upgrade: {"status": "blocked", ...}` warning on stderr
- the current command still runs (from the user's modified copy)
- an explicit `upgrade-workspace --force` is the documented recovery path

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
- a successful workspace bootstrap does not require loading optional parser backends up front; `doctor` should probe `pst_backend` explicitly while ordinary non-PST commands remain ready
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
