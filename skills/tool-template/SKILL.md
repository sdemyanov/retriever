---
name: tool-template
description: >
  Use this skill when working with Retriever's canonical tool bundle.
  It defines the pinned generated tools.py bundle, checksum expectations, and
  the current command surface.
metadata:
  version: "1.1.16"
---

# Retriever Tool Template

Use this skill when a task needs the exact canonical tool bundle.

## Required references

1. Read [tool-template.md](tool-template.md) for version, checksum, and materialization rules.
2. Read the ordered fragments under [src/](src/) for the authored repo-local source.
3. Read [tools.py](tools.py) when you need the exact generated bundled artifact.
4. Read [../schema/schema.md](../schema/schema.md) if schema changes are involved.

## Rules

- Do not rewrite the source ad hoc from prose.
- Treat `src/` as the authored source of truth and `tools.py` as the generated bundle.
- Keep the repo-local source fragments and generated bundled `tools.py` in sync.
- Keep `TOOL_VERSION`, schema version, and documented checksum aligned.
- Use `python3 skills/tool-template/tools.py --help` and subcommand `--help` output as the authoritative command surface before updating routing or skill docs.
- Treat the documented SHA256 checksum as the authoritative upgrade signal for the canonical tool source.
- If the canonical checksum changes, refresh runtime metadata with `workspace update` or `workspace init` before ingest, even if the version string is unchanged.
- Bump version metadata when releasing schema or tool changes, but do not rely on version alone to trigger workspace upgrades.
- If the source changes, update `tool-template.md` and the workspace skill references too.
- When multiple Retriever tool calls are already independent, emit them in one assistant turn rather than serializing them across turns.
