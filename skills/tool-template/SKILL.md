---
name: tool-template
description: >
  Use this skill when materializing or upgrading Retriever's canonical workspace tool.
  It defines the pinned generated retriever_tools.py bundle, checksum expectations, and
  the current command surface.
metadata:
  version: "0.9.5"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `retriever_tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Tool Template

Use this skill when a task needs the exact workspace tool bundle.

## Required references

1. Read [tool-template.md](tool-template.md) for version, checksum, and materialization rules.
2. Read the ordered fragments under [src/](src/) for the authored repo-local source.
3. Read [retriever_tools.py](retriever_tools.py) only when you need the exact generated bundled workspace artifact. If it is absent in a source checkout, regenerate it via [bundle_retriever_tools.py](bundle_retriever_tools.py) or `build.sh`.
4. Read [../schema/schema.md](../schema/schema.md) if schema changes are involved.

## Rules

- Materialize the workspace tool by copying the canonical source exactly.
- Do not rewrite the source ad hoc from prose.
- Treat `src/` as the authored source of truth and `retriever_tools.py` as a generated bundle.
- Keep the repo-local source fragments and generated bundled `retriever_tools.py` in sync.
- Keep `TOOL_VERSION`, schema version, and documented checksum aligned.
- Treat the documented SHA256 checksum as the authoritative upgrade signal for the canonical tool source.
- If the canonical checksum changes, replace the unmodified workspace copy and run `bootstrap` before any ingest, even if the version string is unchanged.
- Bump version metadata when releasing schema or tool changes, but do not rely on version alone to trigger workspace upgrades.
- If the source changes, update `tool-template.md` and the workspace skill references too.
- When multiple Retriever tool calls are already independent, emit them in one assistant turn rather than serializing them across turns.
