# Search Strategy

## Query model

- Use full-text search when the user supplies keywords or phrases.
- Use browse mode when the user primarily filters by metadata or custom fields.
- Build filters with repeatable `--filter <field> <op> <value>` clauses.

Supported MVP operators:

- `eq`
- `neq`
- `gt`
- `gte`
- `lt`
- `lte`
- `contains`
- `is-null`
- `not-null`

Virtual attachment-family filters:

- `is_attachment eq true` to show only child attachment documents
- `has_attachments eq true` to show documents that currently have one or more child attachments
- `production_name contains <text>` to filter production-derived documents by production name

Production-aware query behavior:

- A single Bates/control token such as `SR000123` should prefer Bates-aware lookup over plain keyword FTS.
- A normalized Bates range such as `SR000123-SR000150` should return all logical documents whose Bates spans overlap that range.
- Bates range matching must use normalized prefix + numeric parsing, not raw lexicographic string comparison.

## Result presentation

Default to a four-column result table unless the user explicitly asks for another format.
This default layout also applies to ranked browse requests such as "show 10 largest documents", "newest documents", or "oldest emails".
Do not replace or reorder the default leading columns unless the user explicitly asks for different columns.
It is fine to append one or more request-relevant columns after `Title preview` when that improves the result, for example adding `Size` to a "largest documents" browse view.

Whenever you show files, documents, or attachment children, render each shown item as a clickable link that opens in the preview pane. Do not show a bare document name when a preview/open target is available.

Default column order:

- `Content type`
- `Datetime (UTC)`
- `Author`
- `Title preview`

Default table rules:

- `Datetime (UTC)` should use the best available document datetime, preferring `date_created`, then `date_modified`, then `updated_at`
- the `Title preview` cell should contain the primary clickable link for the document
- use the document title when available; otherwise fall back to subject, then file name
- show `control_number` inline with the title cell when available
- for production-derived documents, keep the produced Bates/control number inline rather than replacing it with a generated `DOC...` value
- the default leading columns must remain, in this order: `Content type`, `Datetime (UTC)`, `Author`, `Title preview`
- for ranked browse requests, keep those default leading columns and describe the sort key/order in the heading or summary
- if a ranked browse request benefits from showing the sort metric as a column, append it after `Title preview` rather than replacing the default leading columns
- keep the primary document column clickable for every row
- show paging summary above or below the table when relevant

Table format should:

- include only the columns the user asked for, plus file name when needed for navigation
- keep the primary document column clickable for every row
- if the user does not ask for different columns, keep the default leading four-column order above
- if you append helpful extra columns, add them after `Title preview`

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
- If the user asks to "show" files or documents, every listed result should include a clickable link, not just the top result.

## Sorting

- Keyword query + no explicit sort: `relevance asc`
- Filter-only browse + no explicit sort: `updated_at desc`
- `relevance` is only valid when query text is present.
- Validate explicit sort fields against built-in or registered custom fields.
- Break equal primary sort values by `id asc` so page boundaries stay stable.

## Paging

- Default page size: 20
- Show `Documents X-Y of Z`
- Tell the user how to ask for the next page when there are more results

## Deferred from MVP

- full-text search over custom-field values
- semantic ranking
- proximity and boolean query operators beyond FTS5 defaults
