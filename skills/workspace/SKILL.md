---
name: workspace
description: >
  Use this skill when initializing, checking, or upgrading a Retriever workspace.
  It defines the .retriever directory layout, bootstrap flow, upgrade safety rules,
  the pinned dependency contract for the current runtime, and the current non-resumable MVP runtime scope.
metadata:
  version: "0.9.4"
---

# Retriever Workspace

Use this skill for any task that needs to bootstrap or maintain a workspace-local Retriever installation.

## Load order

1. Read [workspace.md](workspace.md) first.
2. Read [requirements.lock.md](requirements.lock.md) when dependency installation or verification is involved.
3. Read [../tool-template/tool-template.md](../tool-template/tool-template.md) when materializing or upgrading `retriever_tools.py`.
4. Read [../schema/schema.md](../schema/schema.md) when initializing or migrating the database schema.

## Core rules

- The workspace root is the user-selected folder, not `.retriever/`.
- All persistent Retriever state lives under `.retriever/` inside that root.
- Database paths stored in SQLite must be relative, never absolute.
- Never silently overwrite a modified `.retriever/bin/retriever_tools.py`.
- Before replacing a modified tool, back it up under `.retriever/bin/backups/` and get explicit user approval.
- Treat `runtime.json` as the local state record for the installed tool, schema version, and checksum.
- Treat canonical template checksum drift as an upgrade signal even if the plugin version string is unchanged.
- On reinstall, refresh the workspace tool and run `bootstrap` before any reindex whenever the installed canonical template changed.
- If environment checks fail, stop and report the issue clearly instead of partially bootstrapping.

## Current outcome

With the current Phase 2 tool surface, Claude should be able to:

- verify the runtime with `doctor`
- create the `.retriever/` directory structure
- materialize the pinned `retriever_tools.py` template
- initialize schema v9
- write a stable `runtime.json`
- ingest supported documents into `.retriever/retriever.db`
- verify the required PST backend is present before calling the runtime ready
- populate built-in `participants` for email and chat-like documents during ingest
- ingest supported PST container sources through the same regular `ingest` surface
- materialize one-level EML/MSG attachment families with stable `control_number` values during ingest
- search indexed documents with structured filters

Do not improvise the runtime contract beyond the referenced files.
