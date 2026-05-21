# Testing

This document explains what the current Retriever test suite covers, why some
of its repository-integrity checks are important, and how to split the current
consolidated test module without losing coverage.

## Quick Commands

Use these commands before packaging or publishing:

```bash
./build.sh
python3 -m pytest tests/test_retriever_tools.py
```

`./build.sh` is the sync step. The pytest run is the current full regression
suite.

## Current Suite Shape

Retriever currently keeps most of its tests in one large module:

- `tests/test_retriever_tools.py`

That file mixes several different responsibilities:

- repository-integrity checks for generated artifacts and version metadata
- installer coverage for `setup`
- native package entrypoint and processing tests
- broad regression coverage for ingest, search, browse, export, metadata, and
  structured processing behavior
- format-specific parsing tests such as attachment handling and calendar invite
  behavior

The suite works today, but it is harder to navigate than it needs to be.

## Repository-Integrity Checks To Keep

The early assertions in `tests/test_retriever_tools.py` are worth preserving as
first-class tests, not demoting to ad hoc scripts:

- `assert_bundled_tooling_current()`
- `assert_version_metadata_current()`

Those checks catch drift between:

- `skills/tool-template/src/`
- `skills/tool-template/tools.py`
- `skills/tool-template/tool-template.md`
- `.claude-plugin/plugin.json`
- root `CLAUDE.md`
- skill frontmatter version text
- schema and workspace documentation

This is one of the strongest repo-health patterns already present in Retriever,
and it is worth keeping even after the suite is split.

## Recommended Target Split

When we split `tests/test_retriever_tools.py`, use a support module plus
behavioral slices instead of creating many tiny files.

Suggested structure:

- `tests/support.py`
  Shared module loader, repo-root constants, helper fixtures, and common CLI
  helpers.
- `tests/test_repo_integrity.py`
  Generated bundle freshness, checksum enforcement, version-metadata sync, and
  documentation/install-surface sync checks.
- `tests/test_installers.py`
  `ClaudeGlobalInstallerTests`.
- `tests/test_package_entrypoint.py`
  `RetrieverPackageEntrypointTests` and native package smoke coverage.
- `tests/test_processing_commands.py`
  Native package processing flows and long-running command behavior that belongs
  to the Claude-first package surface.
- `tests/test_search_and_browse.py`
  Search, filter, Bates, paging, columns, sort, scope, and browse-state
  behavior.
- `tests/test_fields_and_metadata.py`
  Custom fields, `/fill`, editable built-ins, dataset state, and schema
  operations.
- `tests/test_export.py`
  CSV, preview, and archive export behavior.
- `tests/test_ingest_v1.py`
  Legacy or compatibility ingest paths that are still intentionally supported.
- `tests/test_ingest_v2.py`
  Resumable V2 planning, preparation, commit, finalize, worker events, and
  recovery behavior.
- `tests/test_parsing_email_and_calendar.py`
  Attachment resolution, CID inlining, invite parsing, and other format-specific
  parsing coverage.

If a future split shows one of those files is still too large, split by domain
boundary rather than by random line count.

## Migration Order

To keep the split low-risk, migrate in this order:

1. Extract shared helpers and constants into `tests/support.py` without changing
   assertions.
2. Move the repository-integrity checks first. They are the least coupled to
   runtime fixtures and the easiest to validate independently.
3. Move installer and native package entrypoint tests next.
4. Peel off domain clusters such as search/browse, export, and fields/metadata.
5. Move ingest and format-specific parsing tests last, since they tend to share
   the heaviest fixtures and helpers.

The goal is mechanical movement first, behavioral change second.

## Near-Term Rule

Until the split happens, add new tests near the closest existing section and
prefer reusable helpers over more one-off scaffolding in the main test file.
