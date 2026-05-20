# Architecture

This document explains how Retriever is organized as a repository and where the
load-bearing boundaries live.

For product framing and user-facing workflows, see [README.md](README.md). For
contributor workflow, see [CONTRIBUTING.md](CONTRIBUTING.md). For test-suite
layout and the planned split of the consolidated regression file, see
[TESTING.md](TESTING.md).

## Core Idea

Retriever is a local-first review workspace for Claude-driven document review.
The repo has to keep four surfaces aligned:

- the authored backend source
- the generated compatibility bundle
- the native `python3 -m retriever` package surface
- the Claude-facing slash-command and skill surfaces

The main architectural risk in this repo is drift between those surfaces. Most
of the build, documentation, and testing rules exist to catch that drift early.

## Repository Map

- `retriever/`
  Native Claude-first package surface. Long-running ingest, export, and
  processing orchestration belongs here even when it reuses lower-level storage
  or runtime helpers.
- `skills/`
  Claude-facing command and skill materials, plus the compatibility backend
  source tree and generated artifacts.
- `skills/tool-template/src/`
  Authored backend source of truth for the compatibility bundle.
- `skills/tool-template/tools.py`
  Generated compatibility artifact. Treat it as derived output until the bundle
  is retired.
- `skills/routing/SKILL.md`
  Backend routing and reference material for Claude-facing behavior.
- `agents/`
  Persona-oriented wrappers such as `retriever-legal`.
- `setup` and `setup-claude-v0`
  Installer entrypoints for the global Claude Code install flow and the
  project-local v0 bridge.
- `tests/`
  Current regression, installer, package, and repository-integrity tests.
- `.claude-plugin/plugin.json`
  Plugin metadata and version surface that must stay in sync with the generated
  bundle metadata.

## Source Of Truth

When a change spans multiple layers, keep this order in mind:

1. `skills/tool-template/src/` is the authored backend source of truth.
2. `skills/tool-template/tools.py` is generated from that source.
3. `retriever/` is the primary native package surface for human-friendly CLI
   behavior and one-shot long-running commands.
4. Installer output, skill metadata, routing docs, and tests must stay aligned
   with the current tool and schema versions.

`./build.sh` is the canonical synchronization step. It rebundles
`skills/tool-template/tools.py`, refreshes generated command tables in
`skills/routing/SKILL.md`, synchronizes version metadata, and rebuilds the
legacy compatibility artifact.

## Workspace And Runtime Model

Retriever treats the selected review folder as the workspace root. Persistent
workspace state lives under `.retriever/` in that root, while heavyweight parser
dependencies live in the shared repo runtime under
`.retriever-plugin-runtime/`.

That split is intentional:

- the workspace keeps review state, previews, logs, jobs, and runtime metadata
- the shared runtime keeps parser dependencies out of each workspace
- multiple workspaces on one machine can share the same parser installation

## Command Surfaces

Retriever currently has three relevant execution surfaces:

- Claude-facing slash commands installed by `./setup`
- the native package CLI at `python3 -m retriever`
- the compatibility bundle at `python3 skills/tool-template/tools.py`

The intended priority is:

1. Use Retriever slash commands when the task maps cleanly to the user-facing
   command set.
2. Use `python3 -m retriever ...` when no slash command fits or when native
   package behavior is the right surface.
3. Use the compatibility bundle for legacy parity, compatibility testing, or
   lower-level access that has not yet moved to the native package.

Direct SQLite inspection is a last resort for debugging and verification, not a
normal user-facing workflow.

## Long-Running Jobs

Ingest, export, entity rebuild, and processing flows use resumable backend job
models. The Claude-first and native package surfaces should usually present
one-shot commands that drive those backends to a terminal state. Lower-level
recovery commands such as `...-status` and `...-run-step` should stay available
for interruption recovery and debugging without becoming the default user path.

## Documentation Boundaries

Keep repository docs separated by audience and purpose:

- `README.md`: product story, install paths, supported workflows, and first-use
  guidance
- `docs/browse-reference.md`: slash-command reference, `/search` and `/filter`
  syntax, display tips, and CLI quick reference
- `CLAUDE.md`: short repository prompt for coding agents working in this repo
- `CONTRIBUTING.md`: contributor workflow and build/test expectations
- `TESTING.md`: test-suite shape, invariants, and the planned test-file split

Avoid turning `README.md` into the maintainer handbook or turning `CLAUDE.md`
into a full architecture document.

## Load-Bearing Invariants

- Keep Claude-facing behavior and backend behavior aligned.
- Keep generated artifacts, installer output, routing docs, and tests in sync
  when source fragments change.
- Keep the root `CLAUDE.md` concise, but preserve its priority order and
  next-command guidance.
- Prefer preview behaviors that work in Claude Code's preview panel.
- Preserve Retriever's stateful browse/session model rather than replacing it
  with stateless one-off queries.
