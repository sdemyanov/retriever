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
- Use the canonical `search` CLI flags: `--filter`, `--sort`, `--order`, `--page`, and `--per-page`.
- For requests like "show 10 ...", map the requested count to `--page 1 --per-page 10`; do not invent `--limit`.
- For sorted browse requests, use `--sort <field>` and `--order asc|desc`; do not invent `--sort-by` or `--sort-order`.
- Use canonical built-in field names such as `date_created`, not ad hoc variants like `created_date`.
- Validate field names against built-in document columns or registered custom fields.
- Use browse mode when the user is mostly filtering, and keyword search when they provide terms.
- For a single Bates/control token or a Bates range expression, prefer the Bates-aware search path over plain keyword FTS.
- **Mandatory output format** — render every result set as the standard table from search-strategy.md:

  ```
  | Type | Title | Author | Datetime (UTC) | Control Number |
  ```

  Example rows:
  ```
  | Email | [Re: Q4 Budget](computer:///path/to/preview.html) | John Smith | 2024-03-15 09:22 | DOC001.00000042 |
  | Chat | [#general - Dec 16, 2022](computer:///path/to/preview.html) | Sergey, Udit | 2022-12-17 00:03 | DOC003.00000003 |
  ```

  Followed by: `Documents 1–10 of 85. Ask for the next page to see more.`

- **Title is the link** — every Title cell must be a clickable `[text](computer://...)` link. NEVER add a separate Link/View/Preview column.
- **Smart column substitution** — if Author is null for ALL rows in the result page, replace with Participants (same column position). If both are empty, drop the column. Never show a full column of `—` dashes when a better field is available.
- **Never add a `#` row-number column** — it is not part of the standard format.
- You may append extra columns after Datetime (UTC) (e.g. Size for "largest" queries), but keep Control Number rightmost.
- If a filter constrains one field to a single value across all rows, omit only that one column.
- Prefer `preview_abs_path`; fall back to `abs_path` for native-preview files.
- Show attachment children as indented `↳` rows beneath the parent result when they are present in the response.
- Apply the same clickable-title rule to attachment rows and any document rows shown in tables.
- Always include a paging summary line and mention active filters.
