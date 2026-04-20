# Retriever Tool Template

## Template metadata

- tool version: `0.13.5`
- schema version: `18`
- requirements version: `2026-04-19-phase9-export-preview-materialization`
- workspace output path: `.retriever/bin/retriever_tools.py`
- canonical bundled output file: [retriever_tools.py](retriever_tools.py)
- repo source directory: [src/](src/)
- source checksum (SHA256): `8ec0c47ff9ce6029fa67f54f99324c79392c82215c8c52fd6569f7bd85a2587e`

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
- `export-previews`
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

## Implementation note

The canonical bundled workspace tool still lives in [retriever_tools.py](retriever_tools.py) so the plugin can carry an exact file and checksum together.

Within the repo, `retriever_tools.py` is a generated artifact, not the authored source of truth. It is built from the ordered source fragments under [`src/`](src/) by [bundle_retriever_tools.py](bundle_retriever_tools.py) during `build.sh`.
