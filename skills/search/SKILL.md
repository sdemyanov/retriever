---
name: search
description: >
  Use this skill when the user wants to find documents, filter the collection,
  or browse search results. It runs Retriever's search command with structured
  filters and presents results using the search-strategy contract.
metadata:
  version: "0.9.5"
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

- Prefer SQL-like `--filter "<expression>"` filters over tuple-style field/operator/value fragments.
- The filter grammar targets Retriever's logical document fields, including schema-defined virtual fields such as `production_name`, `is_attachment`, and `has_attachments`.
- Use the canonical stateless `search` CLI flags: `--filter`, `--sort`, `--order`, `--page`, `--per-page`, and `--columns`.
- For requests like "show 10 ...", map the requested count to `--page 1 --per-page 10`; do not invent `--limit`.
- For sorted browse requests, use `--sort <field>` and `--order asc|desc`; do not invent `--sort-by` or `--sort-order`.
- Use canonical built-in field names such as `date_created`, not ad hoc variants like `created_date`.
- Validate field names against built-in document columns, registered custom fields, and supported virtual filter fields.
- Use browse mode when the user is mostly filtering, and keyword search when they provide terms.
- For a single Bates/control token or a Bates range expression, prefer the Bates-aware search path over plain keyword FTS.
- For persistent investigation flows, prefer the slash surface: `/search`, `/bates`, `/filter`, `/dataset`, `/from-run`, `/scope`, `/sort`, `/page`, `/next`, `/previous`, `/page-size`, and `/columns`.
- Start with Retriever's default compact output; add `--verbose` only when you need attachment rows, alternate preview targets, or extended metadata not present in compact mode.
- Always show the active scope/header before the result table so the user can see the selector, sort, and page state.
- **Mandatory output format** — render every result set as a table driven by the active display columns from search-strategy.md:

  ```
  Scope: ...
  Sort: ...
  Page: ...
  ```

  ```
  | content_type | title | author | date_created | control_number |
  ```

  Example rows:
  ```
  | Email | [Re: Q4 Budget](computer:///path/to/preview.html) | John Smith | 2024-03-15 09:22 | DOC001.00000042 |
  | Chat | [#general - Dec 16, 2022](computer:///path/to/preview.html) | Sergey, Udit | 2022-12-17 00:03 | DOC003.00000003 |
  ```

  Followed by: `Documents 1–10 of 85. Ask for the next page to see more.`

- **Title is the link** — whenever `title` is in the active column set, render that cell as a clickable `[text](computer://...)` link. NEVER add a separate Link/View/Preview column.
- Default columns are `content_type`, `title`, `author`, `date_created`, and `control_number` when no display preference overrides them.
- **Never add a `#` row-number column** — it is not part of the standard format.
- Honor explicit `/columns` or `--columns` choices; do not silently substitute a different field unless the user asks for a friendlier presentation.
- Prefer `preview_abs_path`; fall back to `abs_path` for native-preview files.
- Show attachment children as indented `↳` rows beneath the parent result when they are present in the response.
- Apply the same clickable-title rule to attachment rows and any document rows shown in tables.
- Always include the scope/sort/page header and mention active filters.
