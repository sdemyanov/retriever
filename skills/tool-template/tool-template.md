# Retriever Tool Template

## Template metadata

- tool version: `0.13.5`
- schema version: `15`
- requirements version: `2026-04-16-phase4-pst`
- workspace output path: `.retriever/bin/retriever_tools.py`
- canonical bundled output file: [retriever_tools.py](retriever_tools.py)
- repo source directory: [src/](src/)
- source checksum (SHA256): `1e788ff06ab5539340f3243950fd246d127004d36267c13530e7821808c24fab`

## Current command surface

The current template implements:

- `doctor`
- `bootstrap`
- `schema-version`
- `ingest`
- `ingest-production`
- `search`
- `search-docs`
- `catalog`
- `export-csv`
- `export-archive`
- `get-doc`
- `list-chunks`
- `search-chunks`
- `aggregate`
- `add-field`
- `promote-field-type`
- `set-field`
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

## Materialization rules

- Copy [retriever_tools.py](retriever_tools.py) byte-for-byte into `.retriever/bin/retriever_tools.py`
- Mark it executable
- Record the copied file checksum in `.retriever/runtime.json`
- Do not overwrite a modified workspace copy without backup and explicit user approval

## Implementation note

The canonical bundled workspace tool still lives in [retriever_tools.py](retriever_tools.py) so the plugin can carry an exact file and checksum together.

Within the repo, `retriever_tools.py` is a generated artifact, not the authored source of truth. It is built from the ordered source fragments under [`src/`](src/) by [bundle_retriever_tools.py](bundle_retriever_tools.py) during `build.sh`.
