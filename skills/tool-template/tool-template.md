# Retriever Tool Template

## Template metadata

- tool version: `0.9.4`
- schema version: `9`
- requirements version: `2026-04-16-phase4-pst`
- workspace output path: `.retriever/bin/retriever_tools.py`
- canonical bundled output file: [retriever_tools.py](retriever_tools.py)
- repo source directory: [src/](src/)
- source checksum (SHA256): `6cab7c3a092457ecce1d11f386f1a8cef7ff29d7610061e724bf58d47b7b7bc5`

## Current command surface

The current template implements:

- `doctor`
- `bootstrap`
- `schema-version`
- `ingest`
- `ingest-production`
- `search`
- `add-field`
- `set-field`

Stats, export, and review commands remain later-phase work.

## Materialization rules

- Copy [retriever_tools.py](retriever_tools.py) byte-for-byte into `.retriever/bin/retriever_tools.py`
- Mark it executable
- Record the copied file checksum in `.retriever/runtime.json`
- Do not overwrite a modified workspace copy without backup and explicit user approval

## Implementation note

The canonical bundled workspace tool still lives in [retriever_tools.py](retriever_tools.py) so the plugin can carry an exact file and checksum together.

Within the repo, `retriever_tools.py` is a generated artifact, not the authored source of truth. It is built from the ordered source fragments under [`src/`](src/) by [bundle_retriever_tools.py](bundle_retriever_tools.py) during `build.sh`.
