---
name: doctor
description: >
  This skill should be used when the user says "check retriever",
  "retriever doctor", "check runtime", "is the environment ready",
  or "diagnose retriever". It runs Retriever's workspace-aware doctor
  command so Claude can inspect runtime readiness, workspace state,
  installed tool integrity, and schema application status.
metadata:
  version: "0.9.4"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `retriever_tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Doctor

Inspect Retriever's actual workspace/runtime state, not just the ambient VM.

## Load order

1. Read [../workspace/workspace.md](../workspace/workspace.md).
2. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the fallback command source if the workspace-local tool is missing.

## Steps

1. Confirm or infer the workspace root.
2. If `.retriever/bin/retriever_tools.py` exists under that root, run the workspace-local tool:
   ```bash
   python3 .retriever/bin/retriever_tools.py doctor /path/to/workspace
   ```
3. If the workspace-local tool is missing, run the canonical plugin tool against the workspace root:
   ```bash
   python3 /path/to/plugin/skills/tool-template/retriever_tools.py doctor /path/to/workspace
   ```
4. Report the returned JSON faithfully. Do not replace it with a generic Python/SQLite-only summary.

## Report Format

Reply with a concise summary that includes:

- `overall`
- workspace root and workspace state
- tool version and schema version
- `pst_backend` status
- whether `runtime.json`, the DB, and the workspace tool are present
- tool integrity status (`current_sha256`, `runtime_sha256`, `matches_runtime`)
- `schema_apply` if present, especially after upgrades
- a short note calling out stale or mismatched workspace tools

## Rules

- Prefer the workspace-local tool when it exists, because that is what the workspace actually uses.
- Use the canonical plugin tool only as a fallback when the workspace-local tool is absent.
- Do not silently swap in a generic environment-only probe.
- If `matches_runtime` is `false` or the workspace is missing expected files, say so explicitly.
- `doctor` should probe PST backend readiness under `pst_backend` even though ordinary non-PST commands stay lazy.
- If `pst_backend.status` is `fail`, say PST ingest is not ready until the `libpff-python` / `pypff` backend is installed.
