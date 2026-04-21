# Spreadsheet Parsing Plan

Status: revised, not started
Author: design discussion (Sergey + Codex), 2026-04-20
Scope: `.xlsx`, `.xls`, and `.csv` ingest

## Goal

Stop indexing raw spreadsheet cell bodies. Index the structural surface that users actually search for: sheet names, headers, inferred column types, named ranges, comments, chart and pivot labels, hyperlinks, data-validation enums, and workbook core properties.

The current spreadsheet extractors in `skills/tool-template/src/20_extractors.py` flatten `.xlsx` and `.xls` sheets to CSV, concatenate the CSV into `documents.text_content`, and rely on that raw cell stream for chunk search. This produces too many low-signal numeric and formula-derived tokens, crowds out the human-authored structure, and makes spreadsheet hits less useful than document or presentation hits. The new parser keeps previews but changes what enters the index.

## Locked decisions

- One workbook stays one document in v1.
- Multi-sheet workbooks are represented as one structural summary with explicit per-sheet sections.
- Chunking should prefer sheet boundaries so different tabs can match independently while still returning one workbook result.
- `.xlsx` and `.xls` keep generated per-sheet CSV previews for inspection.
- `.csv` keeps its native file preview; it does not get a generated preview artifact.
- `documents.text_content` stores the structural summary in v1.
- v1 does not add schema columns or rebuild FTS for spreadsheet-specific fields.
- v1 does not add `documents.description`.
- v1 does not create sheet child documents or treat workbooks as container sources.
- v1 drops formula/dependency-graph extraction.
- Any later LLM summarization pass should write a spreadsheet-specific combined `active_search_text` revision, not a new built-in metadata column.
- For user questions, Retriever should find spreadsheet candidates first; if the question is about actual values, formulas, or tab contents, the answering agent should inspect the native spreadsheet with the built-in spreadsheet capability instead of relying on the index alone.

## Current implementation

- `.xlsx` is parsed with `openpyxl.load_workbook(..., read_only=True, data_only=True)` and each sheet is indexed as flattened CSV text.
- `.xls` is parsed with `xlrd` and indexed the same way.
- `.csv` is treated as plain text and indexed verbatim.
- `.xlsx` and `.xls` already emit one CSV preview artifact per sheet under `.retriever/previews/`.
- Workbook core properties are not currently surfaced into the built-in document metadata columns for spreadsheets.

## Why spreadsheets need a different parser

For prose documents, storing extracted body text works because the human-authored signal dominates the bytes. Spreadsheets invert that ratio: the workbook often contains a small amount of useful human-authored structure and a much larger amount of machine-like values. The fix is not better decoding. The fix is extracting a different surface.

The closest existing mental model in Retriever is PowerPoint, not Word. PPTX is already indexed as one deck-level structural document. Spreadsheet ingest should take the same approach: index workbook structure, preserve previews, and avoid flattening the entire cell grid into the search corpus.

## V1 indexed surface

### Common surface

- Workbook title: default to file stem, but prefer workbook title metadata when present and unlocked.
- Workbook core properties:
  - creator -> `documents.author`
  - title -> `documents.title`
  - subject -> `documents.subject`
  - last modified by -> appended to `documents.participants` when distinct
  - created / modified -> `documents.date_created` / `documents.date_modified`
- Sheet names in workbook order.
- Per-sheet column headers and inferred types.
- Named ranges, excluding obvious auto-generated print names.
- Classic cell comments.
- Hyperlinks.

### `.xlsx` high-fidelity surface

- Data-validation enums for list validations when they can be resolved cheaply.
- Chart titles and axis labels.
- Pivot field names.
- Preserve `page_count = len(workbook.worksheets)` semantics.

### Best-effort `.xlsx` extras

These are worthwhile when available, but they must not block ingest:

- Threaded comments.
- XML fallback for chart, pivot, or validation labels when the openpyxl object model is empty or incomplete.

If any best-effort feature fails, skip it and continue. v1 does not add persistent per-document warning storage just for spreadsheet parsing.

### `.xls` reduced surface

v1 accepts lower fidelity for legacy `.xls`:

- sheet names
- column headers and inferred types
- workbook metadata when exposed by `xlrd`
- basic comments or links only when available through the library
- preserve `page_count = len(workbook.sheets())`

No LibreOffice conversion fallback is part of v1.

### `.csv` surface

Treat a CSV as a single logical sheet named after the file stem:

- detect whether the first row looks like headers
- infer column types from a bounded sample of rows
- emit a compact structural summary instead of indexing the raw file body
- preserve `page_count = 1`
- strip BOMs
- ignore Excel-style leading `sep=` directives when present
- wrap dialect sniffing in a `try/except` and fall back to a default dialect on degenerate inputs

The original CSV file remains the preview target.

## What not to index

- Cell values themselves
- Formula text
- Formula dependency graphs
- Conditional formatting rules
- Cell styles, fonts, colors, borders, merges, and freeze panes
- Raw number-format strings
- Revision history or track changes
- Embedded images or OCR output
- Internal OOXML IDs, shared-string IDs, pivot cache payloads, or calc-chain internals

## Structural summary shape

The summary should be stable, compact, and easy to chunk. Use a fixed order:

1. Workbook header
2. Workbook metadata block when any fields are present
3. One section per sheet in workbook order
4. Named ranges block

Apply hard caps so one pathological workbook cannot recreate index bloat:

- columns listed per sheet: 100
- comments listed per sheet: 200
- hyperlinks listed per workbook: 200
- named ranges listed per workbook: 500
- hard ceiling on final structural-summary size: 64 KB

When a cap is hit, append a visible truncation note in the structural summary itself, for example:

```text
[Truncated: 437 additional hyperlinks omitted]
```

Suggested shape:

```text
Workbook: Finance Ops 2026
Sheets: Budget, Hiring Plan, Vendor Notes

[Sheet: Budget]
Columns: cost_center (enum), amount (currency), quarter (text)
Validations:
- quarter in {Q1, Q2, Q3, Q4}
Charts:
- Revenue by Quarter

[Sheet: Hiring Plan]
Columns: role (text), recruiter (text), start_date (date), status (enum)
Comments:
- VP Eng: backfill approved

[Sheet: Vendor Notes]
Hyperlinks:
- https://example.com/renewal

Named ranges:
- HeadcountPlan -> Hiring Plan!A1:D40
```

## Multi-sheet behavior

V1 does not split a workbook into separate document rows even when sheets are unrelated.

Instead:

- every sheet gets an explicit labeled section in the structural summary
- chunking should align to sheet boundaries whenever practical
- search can therefore match the relevant sheet section independently
- the search result still returns one workbook document, not one row per sheet

This keeps document identity, locking, preview, export, and family behavior simple while still letting sheet-specific queries rank well.

One likely follow-up after v1 is preview routing: if a chunk match clearly came from a later sheet, the UI/search payload may want a `matched_sheet_name` hint so opening the workbook favors the relevant sheet preview instead of always defaulting to the first one.

## Metadata and lock behavior

- Workbook properties should populate existing built-in fields only.
- Existing `manual_field_locks_json` behavior remains unchanged.
- Locked built-in fields continue to win over re-extracted values on re-ingest.
- v1 does not introduce new built-in spreadsheet-only metadata fields.

## Storage and schema

- No schema migration in v1.
- `documents.text_content` changes from raw sheet CSV dumps to structural summaries for spreadsheet rows.
- `content_hash` continues to derive from stored `text_content`.
- The source text revision seeded during ingest therefore becomes the structural summary body.
- Any later spreadsheet LLM pass should reuse the existing text-revision path by creating a spreadsheet-specific active-search revision instead of introducing a new document column.

## Parser strategy

### Phase 1: `.xlsx` structural extractor

- Replace raw cell-dump indexing with structural summary generation.
- Use `read_only=False` for normal-sized workbooks so sheet-level structure is available: comments, validations, hyperlinks, charts, pivots, and related worksheet DOM features.
- Add a size and/or sheet-count fallback to `read_only=True` for very large workbooks.
- When the fallback is used, keep the workbook searchable with lower-fidelity structure rather than failing the file.
- Under the `read_only=True` fallback, still extract workbook-level metadata and named ranges when available, but accept reduced sheet-level fidelity.
- Preserve existing sheet preview generation.
- Fill workbook properties into built-in metadata columns when unlocked.

Acceptance:

- re-indexing existing fixtures still succeeds
- a hand-built workbook with multiple sheets, comments, links, validations, charts, and named ranges yields a structural summary containing the expected labels
- indexed text shrinks materially relative to the current cell-dump parser

### Phase 2: `.xls` and `.csv` structural extractors

- Implement reduced-surface `.xls` extraction using the same summary shape.
- Implement CSV header detection, type inference, and structural summary generation.
- Handle practical CSV edge cases: BOMs, leading `sep=` directives, and `csv.Sniffer()` fallback behavior on sparse or degenerate files.
- Preserve current preview behavior: generated per-sheet CSV previews for `.xls`, native CSV preview for `.csv`.

Acceptance:

- representative `.xls` and `.csv` fixtures ingest successfully
- header/type extraction is correct on simple cases
- index size drops materially for CSV-heavy samples

### Phase 3: search and retrieval polish

- Prefer chunk boundaries at sheet section boundaries rather than arbitrary character windows.
- Confirm that sheet-specific queries return the workbook based on the relevant chunk.
- Keep one workbook result row while allowing independent sheet matches.

Acceptance:

- a query that should only hit one sheet matches that sheet section
- search snippets mention the correct sheet label
- multi-sheet workbooks do not produce duplicate document rows

### Phase 4: eval and benchmark

- add spreadsheet fixtures that exercise comments, validations, charts, pivots, links, and workbook metadata
- measure index-size reduction against the current parser
- compare query quality on a small spreadsheet-specific search set

## Optional follow-on: spreadsheet search revision

This is deliberately out of the core parser slice.

If structural summaries alone are not enough, add a spreadsheet-specific post-processing step that creates a combined `active_search_text` revision for spreadsheet documents. That revision should be based on the structural summary and should include concise natural-language phrasing plus the structural labels, not raw cell dumps.

This follow-on should not:

- add `documents.description`
- add spreadsheet-specific schema columns
- replace the source text revision

## Companion answering workflow

This is a workflow rule, not part of the parser implementation itself:

- use Retriever search to identify spreadsheet candidates from structural summaries
- if the user asks a value-level question about a matched spreadsheet, inspect the native `.xlsx`, `.xls`, or `.csv` file with the built-in spreadsheet capability before answering
- keep index-only answers for discovery questions such as "which workbook mentions headcount budget?"

## Failure modes and fallbacks

- Very large workbook:
  - fall back to lower-fidelity parsing rather than failing ingest
- Missing or malformed threaded-comment/chart/pivot XML:
  - skip the affected feature and continue
- Broken data-validation range reference:
  - skip that validation block and continue
- Password-protected spreadsheet:
  - fail closed with a structured ingest failure

## Non-goals

- indexing cell values
- indexing formula text
- dependency-graph extraction
- sheet child documents
- workbook-as-container behavior
- OCR for embedded spreadsheet images
- cloud or shared workbook APIs
- password-cracking or protected-workbook bypasses
- LibreOffice conversion fallback for `.xls`

## Remaining implementation choices

- exact threshold for `read_only` fallback on large `.xlsx` files
- final structural-summary wording and ordering details
- whether sheet-aware preview targeting ships with the parser or as a follow-up
- whether the optional spreadsheet `active_search_text` revision ships in the same milestone or later
