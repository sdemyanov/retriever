---
name: search
description: >
  Use this skill when the user wants to find documents, filter the collection,
  or browse search results. It runs Retriever's search command with structured
  filters and presents results using the search-strategy contract.
metadata:
  version: "0.9.6"
---

# Retriever Search

Use this skill when the user says things like:

- "find documents mentioning Smith"
- "show only PDFs from 2023"
- "search for Latin emails"
- "filter to NDA contracts"

## Exact /search fast path

For the exact bare slash form `/search`:

- Do not read [../search-strategy/search-strategy.md](../search-strategy/search-strategy.md).
- Do not read schema docs.
- Run exactly one Bash command from the workspace root:
  - `/search`: `python3 .retriever/bin/retriever_tools.py slash . /search`
- If the workspace tool is stale or missing, retry once with `RETRIEVER_CANONICAL_TOOL_PATH` pointed at [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py).
- Return stdout exactly as the entire response. No preamble. No commentary. No reformatting.

## Load order

1. Read [../search-strategy/search-strategy.md](../search-strategy/search-strategy.md).
2. Read [../schema/schema.md](../schema/schema.md) if field names or operators are unclear.
3. Use [../tool-template/retriever_tools.py](../tool-template/retriever_tools.py) as the command source if workspace materialization is needed.

## View vs compose

- Use `--mode view` whenever the user primarily wants to see a document listing, not just when they literally say "table". Treat verbs like "show", "show me", "list", "display", "browse", "which documents", "what files", "show 10", and "show only" as view requests unless the user explicitly asks for a summary, explanation, or a different layout.
- Use `--mode view` for the slash browse surface (`/search`, `/bates`, `/filter`, `/dataset`, `/from-run`, `/scope`, `/sort`, `/page`, `/next`, `/previous`, `/page-size`, `/columns`, plus read-only `list` forms such as `/scope list`, `/dataset list`, `/sort list`, and `/columns list`).
- For document-listing requests inside an active investigation, prefer the slash/session browse surface over a fresh stateless search so the current `/page-size`, `/columns`, `/sort`, and related browse state apply automatically.
- The standard `/search` rendered table is the default output contract for every document-listing request unless the user explicitly asks for a different presentation.
- In `view` mode, the tool returns a `rendered_markdown` field containing the complete pre-formatted result table.
- When `rendered_markdown` is present for a view request, your entire reply MUST be the exact contents of that field and nothing else: no preamble, no trailing commentary, no code fences, no reformatting, and no extra summary sentence. Treat any text before or after the markdown footer as a bug.
- The reply must terminate immediately after the table footer, even if the results seem self-explanatory or worth summarizing.
- Use `--mode compose` only when the user wants a summary, count, explanation, drafting, comparison, or other prose answer that is about the documents rather than a document listing itself.

## Execution rules

- Prefer SQL-like `--filter "<expression>"` filters over tuple-style field/operator/value fragments.
- The filter grammar targets Retriever's logical document fields, including schema-defined virtual fields such as `production_name`, `is_attachment`, and `has_attachments`.
- Use the canonical stateless `search` CLI flags: `--filter`, `--sort`, `--order`, `--page`, `--per-page`, `--columns`, and `--mode`.
- For requests like "show 10 ...", map the requested count to `--page 1 --per-page 10`; do not invent `--limit`.
- For sorted browse requests, use `--sort <field>` and `--order asc|desc`; do not invent `--sort-by` or `--sort-order`.
- Use canonical built-in field names such as `date_created`, not ad hoc variants like `created_date`.
- Validate field names against built-in document columns, registered custom fields, and supported virtual filter fields.
- Use browse mode when the user is mostly filtering, and keyword search when they provide terms.
- For a single Bates/control token or a Bates range expression, prefer the Bates-aware search path over plain keyword FTS.
- For persistent investigation flows, prefer the slash surface: `/search`, `/bates`, `/filter`, `/dataset`, `/from-run`, `/scope`, `/sort`, `/page`, `/next`, `/previous`, `/page-size`, and `/columns`.
- Bare slash commands are read-only state inspection when supported: `/scope` shows the active scope, `/dataset` the active dataset selector, `/sort` the active sort, `/page` the current page state, `/page-size` the active page size, and `/columns` the active display columns.
- Use `list` subcommands for discoverability: `/scope list` lists saved scopes, `/dataset list` lists available datasets, `/sort list` lists sortable fields, and `/columns list` lists displayable fields.
- Start with Retriever's default compact output; add `--verbose` only when you need attachment rows, alternate preview targets, or extended metadata not present in compact mode.
- If the user asked to show/list/display/browse documents, or asked to see a table, use the slash/session browse surface when practical, otherwise call search with `--mode view`, and reply with only `rendered_markdown` unless the user explicitly asks for a different layout or a non-standard summary. Never append an interpretive summary in the same turn.
- Honor the active `/page-size` for document listings unless the user explicitly asks for a different count, asks for all results, or changes `/page-size`.
- Never manually concatenate multiple result pages or enumerate more rows than the active page size in one reply unless the user explicitly asks for more than the current page.
- In compose mode, default to a concise answer: one short paragraph for straightforward summaries, or two short paragraphs when a timeline/contrast genuinely helps. Do not turn ordinary summaries into long narrative memos unless the user asked for depth.
- In compose mode, ground the main claims in the top matching documents. Prefer naming or citing the most relevant `control_number` items instead of synthesizing a broad story from distant/low-signal hits.
- Do not volunteer collection-noise commentary, unrelated matches, or corpus-quality caveats unless the user asked for noise analysis, asked what matched unexpectedly, or the noise materially changes the answer.
- Always show the active scope/header before the result table so the user can see the selector, sort, and page state.
- **Standard listing output format** — unless the user specifically asks otherwise, whenever your answer shows documents, whether from `--mode view` or embedded in a compose-mode answer, use the exact standard `/search` table schema driven by the active display columns from search-strategy.md. Prefer the tool-returned `rendered_markdown` whenever it is present instead of hand-building a list:

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
