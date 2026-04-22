# Retriever Tool Template

## Template metadata

- tool version: `0.19.0`
- schema version: `22`
- requirements version: `2026-04-21-phase11-document-deduplication`
- workspace output path: `.retriever/bin/retriever_tools.py`
- canonical bundled output file: [retriever_tools.py](retriever_tools.py)
- repo source directory: [src/](src/)
- source checksum (SHA256): `48d293afd066ec78a3a4425f9bc24be3dbeb78a0c36facd0cb2a0f74e0f7dd7e`

## Current command surface

The current template implements:

- `doctor`
- `bootstrap`
- `schema-version`
- `ingest`
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

## Materialization rules

- Copy [retriever_tools.py](retriever_tools.py) byte-for-byte into `.retriever/bin/retriever_tools.py`
- Mark it executable
- Record the copied file checksum in `.retriever/runtime.json`
- Do not overwrite a modified workspace copy without backup and explicit user approval

## Auto-upgrade dispatch

The tool's `main()` calls `maybe_upgrade_workspace_tool(root)` before any command outside the exempt set `{schema-version, bootstrap, doctor, upgrade-workspace, slash}`. If the workspace copy is clean-but-stale relative to the plugin's canonical template, it is backed up to `.retriever/bin/backups/`, replaced via `pathlib.Path.write_bytes` (open-with-O_TRUNC, so no `unlink` is needed in Cowork sandboxes), and the tool re-execs so the current command runs from the new code. A user-modified copy is refused and a `retriever-auto-upgrade: {"status": "blocked", ...}` line is written to stderr; the current command still runs in that case.

The canonical plugin template is discovered via:

1. `RETRIEVER_CANONICAL_TOOL_PATH` (environment variable)
2. Parent-walk from the currently running tool looking for `skills/tool-template/retriever_tools.py`

`upgrade-workspace <workspace> [--from <path>] [--force]` is the explicit equivalent of the auto path.

## Implementation note

The canonical bundled workspace tool still lives in [retriever_tools.py](retriever_tools.py) so the plugin can carry an exact file and checksum together.

Within the repo, `retriever_tools.py` is a generated artifact, not the authored source of truth. It is built from the ordered source fragments under [`src/`](src/) by [bundle_retriever_tools.py](bundle_retriever_tools.py) during `build.sh`.
