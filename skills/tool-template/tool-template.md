# Retriever Tool Template

## Template metadata

- tool version: `1.0.4`
- schema version: `25`
- requirements version: `2026-04-21-phase11-document-deduplication`
- canonical bundled output file: [tools.py](tools.py)
- repo source directory: [src/](src/)
- source checksum (SHA256): `5b0609c1a8d84a12fafe9fcabf020ba8451ebe27fcfa53deaf8e9ffd70a6c65e`

## Current command surface

The current template implements:

- `doctor`
- `bootstrap`
- `schema-version`
- `ingest`
- `ingest-start`
- `ingest-status`
- `ingest-cancel`
- `ingest-plan-step`
- `ingest-prepare-step`
- `ingest-commit-step`
- `ingest-finalize-step`
- `ingest-production`
- `inspect-pst-properties`
- `search`
- `search-docs`
- `catalog`
- `export-csv`
- `export-previews`
- `export-archive`
- `get-doc`
- `list-chunks`
- `search-chunks`
- `aggregate`
- `add-field`
- `promote-field-type`
- `set-field`
- `merge-into-conversation`
- `split-from-conversation`
- `clear-conversation-assignment`
- `upgrade-workspace`
- `list-datasets`
- `create-dataset`
- `add-to-dataset`
- `remove-from-dataset`
- `delete-dataset`
- `list-jobs`
- `create-job`
- `add-job-output`
- `list-job-versions`
- `create-job-version`
- `list-runs`
- `get-run`
- `create-run`
- `run-status`
- `cancel-run`
- `claim-run-items`
- `get-run-item-context`
- `heartbeat-run-items`
- `complete-run-item`
- `fail-run-item`
- `list-results`
- `execute-run`
- `publish-run-results`
- `list-text-revisions`
- `activate-text-revision`

Stats and review commands remain later-phase work.

For Cowork-agent execution, prefer the queue path:

- `claim-run-items`
- `get-run-item-context`
- `complete-run-item`
- `fail-run-item`

`execute-run` remains the legacy direct executor for deterministic tests and future external-provider work.

## Output modes

- `search`, `search-docs`, `get-doc`, and `search-chunks` return compact JSON by default to keep model context small.
- Add `--verbose` when you need full document/source metadata, preview target variants, attachment-child payloads, or raw chunk text.
- `export-previews` writes HTML under `.retriever/exports/` and returns a manifest describing the generated index, unit files, and per-document anchor targets.
- Export preview ownership is shared by the most inclusive useful unit:
  - email export units expand to the full conversation chain
  - chat export units merge contiguous selected documents inside the conversation timeline

## Runtime usage rules

- Run [tools.py](tools.py) directly against the target workspace
- Record the canonical bundle checksum in `.retriever/runtime.json`
- Do not create any workspace-local tool snapshot during normal bootstrap or ingest

## Runtime refresh dispatch

The tool's `main()` calls `maybe_upgrade_workspace_tool(root)` before any command outside the exempt set `{schema-version, bootstrap, doctor, upgrade-workspace, slash}`. In the current design that helper only refreshes workspace runtime metadata when the canonical bundle checksum has changed; it does not replace files inside `.retriever/` and it does not re-exec the process.

The canonical plugin template is discovered via:

1. `RETRIEVER_CANONICAL_TOOL_PATH` (environment variable)
2. Prefer a sibling `tools.py` when running from `skills/tool-template/`
3. Parent-walk from the currently running file looking for `skills/tool-template/tools.py`

`upgrade-workspace <workspace> [--from <path>] [--force]` is the explicit equivalent of the runtime metadata refresh path. `--force` is accepted for compatibility but no longer changes behavior.

## Implementation note

The canonical bundled tool lives in [tools.py](tools.py).

Within the repo, `tools.py` is a generated artifact, not the authored source of truth. It is built from the ordered source fragments under [`src/`](src/) by [bundle_retriever_tools.py](bundle_retriever_tools.py) during `build.sh`.
