# Retriever

Retriever is designed for Claude Code. When working in this repository, keep Claude-facing behavior and Retriever backend behavior aligned.

## Priority Order

Use the highest-level Retriever surface that can answer the request:

1. Prefer Retriever slash commands and their command templates when the task maps cleanly to a user-facing Claude command.
2. If no slash command fits, use the Retriever CLI at `python3 -m retriever ...`.
3. Query `./.retriever/retriever.db` directly only for debugging, verification, or investigating backend bugs that Retriever cannot already explain.

## Next Command Guidance

- After a successful Retriever command, suggest the single most reasonable next Retriever command when there is an obvious next step.
- Prefer a concrete `/retriever:*` command with arguments when possible.
- Do not force a next command when the user's task is already complete or they explicitly want to stop.

## Browse Model

- Preserve Retriever's stateful browse/session behavior.
- Prefer the existing search/session verbs over replacing them with stateless one-off queries.
- Keep per-table user settings like columns, page size, sort, and browse mode intact for documents, conversations, and entities.

## Long-Running Work

- Keep the resumable backend for ingest, export, entity rebuild, and processing jobs.
- Default processing commands to the Claude Code provider when the `claude` CLI is installed. Treat `claude_code` and `cowork_agent` as the same Claude-backed execution path.
- Prefer one-shot Claude-facing commands that run to a terminal state: `/retriever:ingest`, `/retriever:run`, `/retriever:translate`, `/retriever:extract`, `/retriever:ocr`, `/retriever:describe-images`, and `/retriever:export`.
- Only surface `...-status`, `...-run-step`, or lower-level recovery commands when the original command was interrupted or the user explicitly wants stepwise control.

## Preview Behavior

- Prefer preview targets that work in Claude Code's preview panel.
- Do not assume HTML anchors are respected there.
- If a Claude-facing preview behavior differs from browser behavior, optimize for the Claude Code preview experience first.

## Source Of Truth

- Treat `skills/tool-template/src/` as the authored backend source of truth.
- Treat `skills/tool-template/tools.py` as a generated support artifact.
- Treat `retriever/` as the Claude Code package surface. Native processing orchestration belongs there, even when it reuses backend storage/runtime helpers.
- Keep repository-map and doc-boundary details in `ARCHITECTURE.md`.
- Keep test-suite structure and migration guidance in `TESTING.md`.
- When source fragments change, keep generated artifacts, installer output, and tests in sync.

## Common Commands

- Build and sync generated artifacts: `./build.sh`
- Run tests: `python3 -m pytest tests/test_retriever_tools.py`
- Inspect CLI surface: `python3 -m retriever --help`
- Generated bundle help: `python3 skills/tool-template/tools.py --help`
