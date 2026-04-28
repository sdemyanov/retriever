# Search Strategy

## Query model

- Use full-text search when the user supplies keywords or phrases.
- Use browse mode when the user primarily filters by metadata or custom fields.
- Retriever now exposes two complementary search surfaces:
  - stateless CLI `search ...`
  - persistent slash commands `/search`, `/bates`, `/filter`, `/dataset`, `/from-run`, `/scope`, `/sort`, `/page`, `/next`, `/previous`, `/page-size`, and `/columns`
  - bare commands inspect active state where supported (`/scope`, `/dataset`, `/sort`, `/page`, `/page-size`, `/columns`)
  - `list` subcommands show available options (`/scope list`, `/dataset list`, `/sort list`, `/columns list`)
- A scope is a conjunctive selector over document fields. In the current implementation it may include a keyword slot, a Bates slot, a SQL-like filter slot, a dataset slot, and a `from_run_id` slot.
- Build metadata constraints with repeatable SQL-like `--filter "<expression>"` clauses. Repeated `--filter` flags AND-compose.
- The filter grammar applies to Retriever's logical document field set, not only raw table columns. Supported names include built-in fields, registered custom fields, and schema-defined virtual fields such as `production_name`, `is_attachment`, and `has_attachments`.
- Use the canonical stateless `search` CLI flags `--sort`, `--order`, `--page`, `--per-page`, `--columns`, and `--mode` for sorting, paging, display control, and response mode.
- Map "show N" style requests to `--page 1 --per-page N`; do not invent `--limit`.
- Use canonical built-in field names such as `date_created`, not ad hoc variants like `created_date`.

## View vs compose

- `--mode compose` is the default. Use it when the user wants a summary, count, explanation, draft, comparison, or any answer that is about the documents rather than the listing itself.
- `--mode view` is the default for document-listing requests, not just when the user literally says "table". Treat verbs like "show", "show me", "list", "display", "browse", "which documents", "what files", "show 10", and "show only" as view requests unless the user explicitly asks for a summary, explanation, or different layout.
- For document listings in an active browse flow, prefer the slash/session surface over a fresh stateless search so saved state like `/page-size`, `/columns`, and `/sort` is preserved.
- The standard `/search` rendered table is the default output contract for any answer that shows documents unless the user explicitly asks for another presentation.
- In view mode the tool returns a `rendered_markdown` field containing the complete pre-formatted result table.
- When `rendered_markdown` is present for a view request, forward it as the entire reply and nothing else: no preamble, no trailing commentary, no code fences, no reformatting, and no follow-up summary sentence.
- The view-mode response must terminate immediately after the table footer. Any extra prose after `Documents X–Y of Z ...` is a contract violation, not a stylistic choice.

## Compose-mode answer style

- Default to a concise answer. Use one short paragraph for straightforward summaries; use two short paragraphs only when chronology, comparison, or uncertainty genuinely needs the extra space.
- Ground the answer in the top matching documents rather than in a sweeping collection-wide narrative. When the main conclusion rests on a few documents, name or cite those `control_number` items directly.
- Prefer "the top hits indicate ..." over definitive broad claims when the answer is based on a limited result page.
- Do not add commentary about unrelated matches, corpus noise, or synthetic-dataset structure unless the user asked for noise analysis or that caveat materially changes the answer.
- If there is real ambiguity or mixed evidence, say so briefly instead of smoothing it into a single confident storyline.

Supported SQL-like filter operators:

- `=`
- `<>`
- `!=`
- `<`
- `<=`
- `>`
- `>=`
- `LIKE`
- `IS NULL`
- `IS NOT NULL`
- `IN (...)`
- `BETWEEN ... AND ...`

Boolean composition:

- `AND`
- `OR`
- `NOT`
- parentheses

Virtual attachment-family filters:

- `is_attachment = 1` to show only child attachment documents
- `has_attachments = 1` to show documents that currently have one or more child attachments
- `production_name LIKE '%Acme%'` to filter production-derived documents by production name

Production-aware query behavior:

- A single Bates/control token such as `SR000123` should prefer Bates-aware lookup over plain keyword FTS.
- A normalized Bates range such as `SR000123-SR000150` should return all logical documents whose Bates spans overlap that range.
- Bates range matching must use normalized prefix + numeric parsing, not raw lexicographic string comparison.
- Slash commands may store Bates state in a dedicated scope slot via `/bates` or `/search <bates>`.

## OUTPUT FORMAT (mandatory)

Whenever your answer shows documents, every result set MUST use the standard `/search` table driven by the active display column set.
This is mandatory for all result types: keyword searches, filtered browses, ranked requests ("show 10 largest"), and any other document listing, even when the answer also includes a short compose-mode summary.
Prefer the tool-returned `rendered_markdown` instead of hand-building a custom list whenever it is available.
Honor the active `/page-size` when deciding how many rows to show, unless the user explicitly asks for a different count or asks for all results.
Never concatenate multiple pages into one reply unless the user explicitly asks for more than the current page.
Always show the active search header immediately before the table:

```
Scope: keyword='...', filter=..., dataset=...
Sort: date_created desc
Page: 1 of 3  (docs 1-20 of 55)
```

### Default columns

When no display override is present, use this default column set and order:

| content_type | title | author | date_created | control_number |
|--------------|-------|--------|--------------|----------------|

- `title` is **always a clickable link** and should fall back to subject, then file name when the stored title is null
- `control_number` should remain rightmost in the default set
- `/columns ...` or `search --columns ...` may replace the default set entirely

### Column rules

- The active display column set is the source of truth. Do not silently swap in a different field just because it looks nicer.
- `title`, when present, is always the clickable cell. Do not create a separate link column.
- `control_number`, when present, should be rendered as its own column rather than folded into the title.
- Boolean display columns such as `is_attachment` should render as `Yes` or `No`.
- Virtual fields that are marked filter-only, such as `has_attachments`, must not be offered as display columns.
- If the user asks for a different column set, honor it directly via `/columns ...` or `--columns ...`.

### Correct examples

Default-column result set:
```
| content_type | title | author | date_created | control_number |
|--------------|-------|--------|--------------|----------------|
| Email | [Re: Q4 Budget Review](computer:///path/to/.retriever/previews/file.html) | John Smith | 2024-03-15 09:22 | DOC001.00000042 |
| Email | [FW: Contract Draft](computer:///path/to/.retriever/previews/file2.html) | Jane Doe | 2024-03-16 11:05 | DOC001.00000043 |

Documents 1–10 of 85. Ask for the next page to see more.
```

User-chosen display columns:
```
| title | participants | control_number |
|-------|--------------|----------------|
| [#general - Dec 16, 2022](computer:///path/to/.retriever/previews/general/2022-12-16.json.html) | Sergey Demyanov, Udit Sood | DOC003.00000003 |
| [#general - Dec 17, 2022](computer:///path/to/.retriever/previews/general/2022-12-17.json.html) | Max, Artur Chakhvadze | DOC003.00000004 |

Documents 1–10 of 231. Ask for the next page to see more.
```

### NEVER do any of these

- NEVER add a separate "Link", "View", "Preview", or "Open" column — the Title cell IS the link
- NEVER ignore an explicit `/columns` or `--columns` choice and silently render a different field set
- NEVER show results without a paging summary line
- NEVER show a bare unlinked title when a preview or native path is available
- NEVER put a row number `#` column — it is not part of the standard format
- NEVER fold Control Number into the Title cell

### Post-search checklist — verify before responding

1. Columns match the active display preference or explicit `--columns` override
2. Every `title` cell is a clickable `[text](computer://...)` link
3. Paging summary is present: "Documents X–Y of Z"
4. `control_number`, when present, remains its own column
5. Attachment children use `↳` prefix and are indented below their parent row

### Additional rules

- For production-derived documents, keep the produced Bates/control number in the Control Number column rather than replacing it with a generated `DOC...` value
- For ranked browse requests, keep the active display columns and describe the sort key/order in the heading or summary
- You may suggest a different column set when it materially improves the requested view, but only change it when the user asks or when a persisted display preference already exists
- Keep the primary document column clickable for every row

Whenever you show files, documents, or attachment children, render each shown item as a clickable link that opens in the preview pane. Do not show a bare document name when a preview/open target is available.

When the user asks to inspect fields or columns:

- Default to user-facing fields and custom fields.
- Keep `control_number`, `content_type`, and `participants` in the default visible set.
- Hide pure bookkeeping/helper fields by default:
  - `id`
  - `file_hash`, `content_hash`
  - `text_status`, `lifecycle_status`
  - `ingested_at`, `last_seen_at`, `updated_at`
  - `parent_document_id`
  - `control_number_batch`, `control_number_family_sequence`, `control_number_attachment_sequence`
  - `manual_field_locks_json`, `locked_metadata_fields_json`
- If the user explicitly asks for "all fields", show every column and label helper fields as system/read-only rather than silently dropping them.

## Links

- Prefer `preview_abs_path` when the tool returns one.
- Fall back to `abs_path` for native-preview files.
- If `preview_targets` are present, use the first target as the primary clickable link unless the user asks for a specific preview variant.
- If multiple preview targets exist, surface the first by default and mention labels when relevant.
- For parent email hits with attachments, show child attachments as indented sub-rows directly below the parent row.
- Apply that same family presentation rule to processed-production attachments when `parent_document_id` is set from production family spans.
- Prefix attachment title rows with `↳` and keep the attachment title itself clickable.
- Matching attachments may still appear as standalone hits; when they do, render them with the same `↳` attachment marker and include parent email context when available.
- If the user asks to "show" files or documents, every listed result should include a clickable link, not just the top result, and the listing should stay in the standard `/search` schema unless they asked otherwise.
- Retriever search commands now return compact JSON by default; rerun with `--verbose` when you need attachment child rows, alternate preview targets, or the full metadata/source payload.

## Parallel follow-ups

- If a search, aggregate, or SQL step already gave you multiple independent paths or IDs, fan out the follow-up reads in one assistant turn rather than serializing them.
- Batch independent Retriever reads the same way you would batch independent grep or sqlite calls: one gating query first, then one batched follow-up turn once the inputs are known.
- Keep dependent steps serialized only when later arguments genuinely depend on earlier results.

## Sorting

- Keyword query + no explicit sort: `relevance asc`
- Bates lookup + no explicit sort: `bates asc`
- Filter-only browse + no explicit sort: `date_created desc`
- Browse ordering pushes null `date_created` values to the end of the list
- `relevance` is only valid when query text is present.
- Validate explicit sort fields against built-in or registered custom fields.
- Break equal primary sort values by `id asc` so page boundaries stay stable.

## Paging

- Default page size: 10
- Show `Documents X-Y of Z`
- Tell the user how to ask for the next page when there are more results

## Deferred

- full-text search over custom-field values
- semantic ranking
- proximity and boolean query operators beyond FTS5 defaults
