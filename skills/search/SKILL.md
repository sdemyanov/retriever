---
name: search
description: >
  Use this skill when the user wants to find documents, filter the collection,
  or browse search results. It runs Retriever's search command with structured
  filters and presents results using the search-strategy contract.
metadata:
  version: "0.9.4"
---

# Retriever Search

Use this skill when the user says things like:

- "find documents mentioning Smith"
- "show only PDFs from 2023"
- "search for Latin emails"
- "filter to NDA contracts"

## Load order

1. Read [../search-strategy/search-strategy.md](../search-strategy/search-strategy.md).
2. Read [../schema/schema.md](../schema/schema.md) if field names or operators are unclear.
3. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the command source if workspace materialization is needed.

## Execution rules

- Prefer structured `--filter <field> <op> <value>` filters.
- Validate field names against built-in document columns or registered custom fields.
- Use browse mode when the user is mostly filtering, and keyword search when they provide terms.
- For a single Bates/control token or a Bates range expression, prefer the Bates-aware search path over plain keyword FTS.
- By default, render search results as a four-column table:
  - `Content type`
  - `Datetime (UTC)`
  - `Author`
  - `Title preview`
- Keep those same four leading columns for ranked browse requests such as "largest", "newest", and "oldest" unless the user explicitly asks for different columns.
- You may append request-relevant columns after `Title preview`, such as `Size` for a "largest documents" query, but do not replace or reorder the default leading columns.
- When listing or showing documents, present every shown result title as a clickable link to its first preview target.
- Prefer `preview_abs_path`; fall back to `abs_path` for native-preview files.
- Show attachment children as indented `↳` rows beneath the parent result when they are present in the response.
- Apply the same clickable-title rule to attachment rows and any document rows shown in tables.
- Include paging state and the active filters in the response summary.
