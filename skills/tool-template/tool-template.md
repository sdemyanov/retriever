# Retriever Tool Template

## Template metadata

- tool version: `1.1.12`
- schema version: `25`
- requirements version: `2026-04-21-phase11-document-deduplication`
- canonical bundled output file: [tools.py](tools.py)
- repo source directory: [src/](src/)
- source checksum (SHA256): `bffde7a36150ebfc0fa06c7bab07fc5b7ebf0d50cbbb7c5750a97deb9e6424bc`

## Current command surface

Use `python3 skills/tool-template/tools.py --help` as the authoritative command list. The current high-level surfaces are:

- workspace maintenance: `workspace init`, `workspace status`, `workspace update`, `schema-version`
- bounded ingest: `ingest`, `ingest-status`, `ingest-run-step`, `ingest-cancel`, plus lower-level `ingest-*` step commands
- production/PST diagnostics: `ingest-production`, `inspect-pst-properties`
- browse/search/export: `slash`, `search`, `search-docs`, `search-chunks`, `get-doc`, `list-chunks`, `catalog`, `aggregate`, `export-csv`, `export-csv-*`, `export-archive`, `export-archive-*`, `export-previews`; slash-facing bounded export commands are `/export table documents|entities|conversations`, `/export archive`, and `/export status`
- datasets and fields: `list-datasets`, `create-dataset`, `add-to-dataset`, `remove-from-dataset`, `delete-dataset`, `list-fields`, `add-field`, `rename-field`, `delete-field`, `describe-field`, `change-field-type`, `fill-field`
- conversations and previews: `merge-into-conversation`, `split-from-conversation`, `clear-conversation-assignment`, `refresh-previews`, `refresh-conversation-previews`, `rebuild-conversations`
- entities: `entities`, `rebuild-entities-*`, `list-entities`, `show-entity`, `create-entity`, `edit-entity`, `similar-entities`, `merge-entities`, `block-entity-merge`, `ignore-entity`, `split-entity`, `assign-entity`, `unassign-entity`
- jobs and runs: `list-jobs`, `create-job`, `add-job-output`, `list-job-versions`, `create-job-version`, `list-runs`, `get-run`, `create-run`, `run-status`, `run-job-step`, `cancel-run`, `publish-run-results`, `list-results`, `list-text-revisions`, `activate-text-revision`
- low-level worker protocol: `claim-run-items`, `prepare-run-batch`, `get-run-item-context`, `heartbeat-run-items`, `finish-run-worker`, `complete-run-item`, `fail-run-item`

For Cowork-agent execution, prefer the bounded run step:

- `run-job-step`

For low-level worker protocol work, use:

- `claim-run-items`
- `prepare-run-batch`
- `get-run-item-context`
- `complete-run-item`
- `fail-run-item`

`execute-run` remains the legacy direct executor for deterministic tests and parity checks.

## Output modes

- `search`, `search-docs`, `get-doc`, and `search-chunks` return compact JSON by default to keep model context small.
- Add `--verbose` when you need full document/source metadata, preview target variants, attachment-child payloads, or raw chunk text.
- `export-previews` writes HTML under `.retriever/exports/` and returns a manifest describing the generated index, unit files, and per-document anchor targets.
- For Cowork-safe large exports, prefer `/export table documents|entities|conversations`, `/export archive`, and `/export status` from the slash surface, or the lower-level `export-csv-start` + `export-csv-run-step` + `export-csv-status` / `export-archive-start` + `export-archive-run-step` + `export-archive-status` commands. The direct `export-csv` and `export-archive` commands remain useful for tiny exports, deterministic tests, and parity checks. `/export previews` is deferred until preview export is resumable; use direct `export-previews` only for small/debug exports.
- Export preview ownership is shared by the most inclusive useful unit:
  - email export units expand to the full conversation chain
  - chat export units merge contiguous selected documents inside the conversation timeline
- `refresh-previews` defaults to `--scope conversations`, regenerating the generated per-message/document and full conversation preview artifacts for email/chat conversations. `--scope documents` refreshes standalone generated document previews from stored production state or, with `--from-source`, by re-running source-backed preview extraction without updating document metadata/text. `--scope all` runs both scopes. The command can be narrowed with `--conversation-id`, `--doc-id`, `--dataset-id`, or `--dataset-name`; `--missing-only` limits refresh to previews with missing rows/files.

## Runtime usage rules

- Run [tools.py](tools.py) directly against the target workspace
- Record the canonical bundle checksum in `.retriever/runtime.json`
- Do not create any workspace-local tool snapshot during normal bootstrap or ingest

## Runtime refresh dispatch

The tool's `main()` calls `maybe_upgrade_workspace_tool(root)` before commands outside the exempt set `{schema-version, workspace}`. In the current design that helper only refreshes workspace runtime metadata when the canonical bundle checksum has changed; it does not replace files inside `.retriever/` and it does not re-exec the process.

The canonical plugin template is discovered via:

1. `RETRIEVER_CANONICAL_TOOL_PATH` (environment variable)
2. Prefer a sibling `tools.py` when running from `skills/tool-template/`
3. Parent-walk from the currently running file looking for `skills/tool-template/tools.py`

`workspace update <workspace> [--from <path>] [--force]` is the explicit equivalent of the runtime metadata refresh path. `--force` is accepted for compatibility but no longer changes behavior.

## Implementation note

The canonical bundled tool lives in [tools.py](tools.py).

Within the repo, `tools.py` is a generated artifact, not the authored source of truth. It is built from the ordered source fragments under [`src/`](src/) by [bundle_retriever_tools.py](bundle_retriever_tools.py) during `build.sh`.
