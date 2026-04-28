# Retriever

Retriever is a local-first document intelligence plugin for Claude Cowork. It makes a selected folder searchable and filterable, helps users ask questions about their data, and supports intelligent document review at scale without modifying the original source files.

This README is the user guide for the project: what Retriever is good at, what it can ingest, how the browse model works, which slash commands exist, and how to use `/search` and `/filter` effectively.

## What Retriever is for

Retriever works best when you need to review or analyze a local document collection and keep that work grounded in the files on disk.

Common use cases:

- searching a matter, investigation, or diligence workspace
- reviewing PDFs, Office documents, loose emails, PST/MBOX archives, and Slack exports together
- jumping directly to a Bates number or Bates range in a production
- building reusable scopes such as "emails from 2023 in the Hot Docs set"
- creating review sets, CSV exports, preview bundles, or portable archives
- adding custom metadata fields and running structured extraction jobs over a frozen document set

## Core capabilities

- Local-first storage. Retriever keeps its database, previews, logs, job state, and runtime metadata under `.retriever/` in the workspace root.
- Broad ingest support. It can index common review formats including PDF, DOCX, TXT/Markdown, CSV, JSON, HTML, ICS, RTF, XLS/XLSX, PPTX, EML, MSG, PST, MBOX, Slack exports, and processed productions.
- Search and browse. You can search by keyword, filter by metadata, browse by dataset, jump by Bates number, page through results, and persist scope/display preferences between commands.
- Preview-first review. Search results render as a standard table with clickable titles. Native preview files are used when possible; Retriever generates HTML or CSV previews when needed.
- Stable document identity. Documents receive stable `control_number` values for review and export. Production documents use produced Bates values as the control number.
- Dataset-aware workflows. Documents can belong to one or more datasets, and datasets can be source-backed or manually curated.
- Exports. Retriever can export selected rows to CSV, generate HTML preview bundles, or build zip archives containing source files, previews, and an optional portable workspace subset.
- Metadata enrichment. You can add custom fields, set values manually, and run structured processing jobs that operate on frozen run snapshots.

## How Retriever works

### Workspace model

Retriever treats the selected folder as the workspace root. All persistent state lives under `.retriever/`:

```text
.retriever/
├── retriever.db
├── previews/
├── text-revisions/
├── jobs/
├── locks/
├── logs/
└── runtime.json
```

Important consequences:

- your original documents stay in place and are not rewritten
- document paths in the database are workspace-relative
- the workspace carries its own Retriever state, so browsing, datasets, and exports stay tied to that folder
- the workspace records which canonical Retriever bundle last touched it, and commands run directly through the plugin's canonical tool
- heavy parser dependencies live in the shared plugin runtime (`<plugin-root>/.retriever-plugin-runtime/...`), not under `.retriever/`; see *Runtime and dependencies* for details

### Document model

Retriever indexes logical documents, not just files.

That means:

- EML and MSG emails can create child attachment documents
- PST and MBOX files are treated as container sources, with one logical message document per message and one level of attachment children
- Slack exports become conversation/day documents, with reply threads represented as child documents
- processed productions create one logical document per load-file row, not one document per page image or text file

### Browse model

Retriever has a persistent browse session per workspace.

That session keeps three kinds of state:

- scope state: keyword, Bates, filter, dataset, and `from-run` selectors
- browsing state: current sort and current page/offset
- display state: visible columns and page size

Scope changes reset paging. Display settings and browse preferences persist until you change them or reset them.

### Result format

Document listings use a standard table:

- a header showing `Scope`, `Sort`, and `Page`
- a table whose `title` cell is the clickable preview link
- a footer like `Documents 1-10 of 85. Ask for the next page to see more.`

Default behavior:

- default page size: `10`
- maximum page size: `100`
- default columns: `content_type`, `title`, `author`, `date_created`, `control_number`
- default sort for keyword search: `relevance asc`
- default sort for Bates lookup: `bates asc`
- default sort for filter-only browse: `date_created desc`

## Supported content

Retriever can ingest these source types:

- PDFs
- DOCX
- TXT, Markdown, CSV, JSON, HTML, ICS, and many code/config text formats
- EML and MSG, including one level of extracted attachment children
- PST mail archives
- MBOX mail archives
- RTF
- XLS and XLSX
- PPTX
- common image formats as preview-only documents
- Slack export roots
- processed productions such as Concordance-style `DAT` + `OPT` with `TEXT/`, `IMAGES/`, and optional `NATIVES/`

Ingest-path behaviors worth knowing:

- calendar invites (`.ics`/`.ifb`/`.vcal`/`.vcs`) that arrive as email attachments are promoted into the parent email — the invite's organizer, attendees, when, location, join URL, UID, and sequence are rolled into the email's indexed text and rendered as a structured invite header in the preview
- standalone calendar files ingest as their own documents
- no OCR for scanned PDFs or image files in the default path (OCR is available as a processing job that writes text back through `activate-text-revision`)
- images are previewable but not text-searchable by default (image descriptions can likewise be generated through a processing job)
- archive contents such as `.zip`, `.rar`, `.7z` are not unpacked or indexed automatically
- Retriever does not rely on semantic ranking in the default ingest/search path

## Runtime and dependencies

Retriever maintains a **shared plugin runtime** under the plugin directory:

```text
<plugin-root>/.retriever-plugin-runtime/<system>-<machine>-pyX.Y/venv/
```

Heavy parser dependencies (`pdfplumber`, `python-docx`, `openpyxl`, `xlrd`, `extract-msg`, `libpff-python`, `striprtf`, `Pillow`, `charset-normalizer`) are **lazy-installed** into that shared venv the first time a command actually needs them. Non-parsing commands do not pay that cost.

Consequences:

- the workspace's `.retriever/` folder stays lightweight — it holds data, state, and logs, not Python packages
- multiple workspaces on the same machine share one parser install
- parser installs are keyed by platform and Python version, so swapping Python versions triggers a fresh install
- first use of a new parser type (for example, the first PST ingest) can briefly block while the dependency installs; `workspace status` will report the runtime state and warn if something needed is missing
- the runtime is advisory — if you prefer to manage Python yourself, the tool still falls back to whatever is importable in the active interpreter

## Loading Retriever

The exact install flow depends on the host environment, but for local Claude CLI testing the fastest load is:

```bash
claude --plugin-dir /path/to/retriever-plugin
```

Once loaded:

- use natural-language requests such as "index this workspace" or "run retriever workspace status" for setup, ingest, exports, and job operations
- use Retriever's persistent slash commands for day-to-day browsing and narrowing once a workspace is active

## Typical workflows

### 1. Initialize and index a workspace

Use this when you are starting with a new folder of files.

In conversation:

- ask Retriever to run `workspace status` to check the runtime
- ask it to run `workspace init` to set up the folder
- ask it to ingest the folder, usually recursively

Direct CLI equivalents:

```bash
python3 skills/tool-template/tools.py workspace status .
python3 skills/tool-template/tools.py workspace init .
python3 skills/tool-template/tools.py ingest . --recursive
```

The `workspace` command groups runtime and schema maintenance into subcommands:

- `workspace init` prepares or repairs `.retriever/` state and runtime metadata for a folder.
- `workspace status` reports runtime readiness and schema state without rewriting anything.
- `workspace update` refreshes runtime metadata from the canonical `tools.py` bundle after a plugin upgrade.

Use `ingest-production` when you want to target a processed production root explicitly:

```bash
python3 skills/tool-template/tools.py ingest-production . productions/VOL001
```

### 2. Browse and narrow a collection

This is the main interactive workflow.

Example progression:

```text
/search nda
/filter content_type = 'Email'
/filter date_created BETWEEN '2023-01-01' AND '2023-12-31'
/sort date_created desc
/page-size 25
/next
```

What happens here:

- `/search` sets the keyword or Bates slot
- `/filter` adds metadata constraints
- `/sort` changes the current browse ordering
- `/page-size` changes how many rows each page shows
- `/next` advances within the same persistent browse session

### 3. Review by Bates number

Retriever treats Bates-like input as a first-class lookup mode.

Examples:

```text
/bates ABC000123
/bates ABC000123-ABC000150
```

You can also set Bates scope through `/search` because it auto-detects Bates-shaped input:

```text
/search ABC000123-ABC000150
```

If you need plain full-text search for something that looks like a Bates value, force FTS:

```text
/search --fts ABC000123
```

### 4. Save and reuse a review scope

A scope is the conjunction of:

- keyword
- Bates selector
- filter expression
- dataset selector
- `from-run` selector

Typical pattern:

```text
/search merger
/filter content_type = 'Email'
/dataset "Hot Docs"
/scope save merger-email-hotdocs
```

Later:

```text
/scope load merger-email-hotdocs
```

Useful related commands:

```text
/scope
/scope list
/scope clear
```

### 5. Build or use datasets

Datasets are named document collections. They are useful for review sets, source-backed groupings, and repeatable exports.

Interactive scoping:

```text
/dataset
/dataset list
/dataset "Review Set"
/dataset "Hot Docs", "Witness Files"
/dataset clear
```

`/dataset list` renders as a compact stats table so you can see each dataset's document count, top custodians, and activity range at a glance without drilling in.

Power-user CLI lifecycle:

```bash
python3 skills/tool-template/tools.py create-dataset . "Review Set"
python3 skills/tool-template/tools.py add-to-dataset . --dataset-name "Review Set" --doc-id 12 --doc-id 14
python3 skills/tool-template/tools.py remove-from-dataset . --dataset-name "Review Set" --doc-id 12
python3 skills/tool-template/tools.py delete-dataset . --dataset-name "Review Set"
```

### 6. Export the current selection

Once your scope is right, you can export it.

Examples:

```bash
python3 skills/tool-template/tools.py export-csv . review.csv --field control_number --field title --field dataset_name --select-from-scope
python3 skills/tool-template/tools.py export-previews . preview-bundle --doc-id 12 --doc-id 19
python3 skills/tool-template/tools.py export-archive . review.zip --select-from-scope --portable-workspace
```

Use cases:

- CSV for downstream review or QC
- preview bundles for sharing HTML previews outside the main workspace
- zip archives when you want source files, previews, and a portable subset together

### 7. Add fields and enrich metadata

Retriever supports user-managed custom fields plus manual corrections to editable built-in fields.

The interactive path uses the `/field` and `/fill` slash commands:

```text
/field add privilege_status text
/field describe privilege_status "Privilege designation"
/fill privilege_status privileged on DOC001.00000042
/fill privilege_status clear on DOC001.00000042
```

`/fill` can also populate a value across the active scope — those bulk forms require `--confirm`:

```text
/search privileged
/filter content_type = 'Email' AND custodian = 'Garcia'
/fill privilege_status privileged --confirm
```

Direct CLI equivalents (useful for scripts and non-interactive work):

```bash
python3 skills/tool-template/tools.py add-field . privilege_status text --instruction "Privilege designation"
python3 skills/tool-template/tools.py fill-field . --doc-id 42 --field privilege_status --value "privileged"
python3 skills/tool-template/tools.py set-field . --doc-id 42 --field title --value "Board Minutes"
```

Important details:

- manual fills on custom fields and manual corrections to editable built-ins are locked and preserved on later ingest or review passes until you explicitly overwrite them
- `/fill` refuses to target derived or system-managed fields (`custodian`, `dataset_name`, `production_name`, hashes, ids, ingest timestamps); correct those through the appropriate ingest or conversation command instead
- `/field delete` is permanent; the slash surface previews the removal and requires `--confirm` before actually dropping the field

### 8. Run structured processing jobs

Retriever can freeze a selector into a run and process it later.

High-level flow:

1. Create a job.
2. Define its outputs.
3. Create a job version.
4. Freeze a selector into a run with `create-run`.
5. Execute or supervise that run.
6. Optionally scope future work with `/from-run <run-id>`.

Key commands:

```bash
python3 skills/tool-template/tools.py list-jobs .
python3 skills/tool-template/tools.py create-job . "Issue Tags" structured_extraction
python3 skills/tool-template/tools.py add-job-output . issue_tags primary_issue --value-type text
python3 skills/tool-template/tools.py create-job-version . issue_tags --provider <provider> --model <model> --input-basis active_search_text --instruction "Extract the primary issue."
python3 skills/tool-template/tools.py create-run . --job-name issue_tags --job-version 1 --select-from-scope
python3 skills/tool-template/tools.py run-status . --run-id 7
python3 skills/tool-template/tools.py run-job-step . --run-id 7 --budget-seconds 35
```

Notes:

- job display names are normalized to handles such as `issue_tags`
<!-- Use run-job-step as the documented path because Cowork/bash calls may be killed around 45 seconds; the bounded step returns next_recommended_commands so agents can resume safely. -->
- `run-job-step` is the normal Cowork-safe executor. If it returns `more_work_remaining: true`, continue with `next_recommended_commands`.
- `execute-run` is the legacy direct executor for debugging, deterministic tests, or parity checks.

## Slash command reference

Retriever's persistent browse surface consists of these commands.

| Command | Purpose | Examples |
|---|---|---|
| `/search` | Show or set the current keyword/Bates search slot | `/search`, `/search contract`, `/search --within renewal`, `/search clear`, `/search --fts ABC000123` |
| `/filter` | Show, add, or clear the current SQL-like filter expression | `/filter`, `/filter content_type = 'Email'`, `/filter clear` |
| `/bates` | Show, set, or clear the current Bates selector | `/bates`, `/bates ABC000123-ABC000150`, `/bates clear` |
| `/dataset` | Show, list, set, clear, or rename dataset selectors | `/dataset`, `/dataset list`, `/dataset "Review Set"`, `/dataset clear`, `/dataset rename "Old Set" "New Set"` |
| `/scope` | Show, list, save, load, or clear the whole current scope | `/scope`, `/scope list`, `/scope save hotdocs`, `/scope load hotdocs`, `/scope clear` |
| `/sort` | Show, list, set, or reset browse sorting | `/sort`, `/sort list`, `/sort file_name asc`, `/sort date_created desc, file_name asc`, `/sort default` |
| `/page` | Show current page state or jump to another page | `/page`, `/page 3`, `/page first`, `/page last`, `/page next`, `/page previous` |
| `/next` | Go to the next page | `/next` |
| `/previous` | Go to the previous page | `/previous` |
| `/page-size` | Show or set rows per page | `/page-size`, `/page-size 25` |
| `/columns` | Show, list, set, add, remove, or reset visible columns | `/columns`, `/columns list`, `/columns set title, control_number`, `/columns add dataset_name`, `/columns remove author`, `/columns default` |
| `/from-run` | Show, set, or clear a prior run selector | `/from-run`, `/from-run 42`, `/from-run clear` |
| `/field` | Inspect or manage the custom-field schema | `/field`, `/field list`, `/field add privilege_status text`, `/field rename old_tag new_tag`, `/field describe privilege_status "Privilege designation"`, `/field type issue_tag text`, `/field delete old_tag --confirm` |
| `/fill` | Set or clear a field value on one document or a scoped result set | `/fill privilege_status privileged on DOC001.00000042`, `/fill privilege_status clear on DOC001.00000042`, `/fill reviewer "J. Doe" --confirm` (bulk fill against the active scope) |

Notes:

- bare forms such as `/scope`, `/dataset`, `/sort`, `/page`, `/page-size`, `/columns`, and `/field` are read-only state inspection
- `/next` is equivalent to `/page next`
- `/previous` is equivalent to `/page previous`
- `/field delete` and any bulk `/fill` (one that targets the active scope rather than explicit `on <doc-ref>` documents) require `--confirm` as a safety rail
- `/fill` will not target derived or system-managed fields such as `custodian`, `dataset_name`, `production_name`, hashes, ids, or ingest timestamps; use the appropriate ingest or conversation command instead
- values with spaces should be quoted
- comma-separated lists are supported for commands such as `/dataset`, `/columns set`, and `/sort`

## `/search` syntax

`/search` controls the current keyword or Bates slot in the persistent scope.

### Forms

```text
/search
/search clear
/search <text>
/search --within <text>
/search --fts <text>
```

### Behavior

- `/search` by itself shows the current keyword slot.
- `/search clear` clears both the current keyword slot and the current Bates slot.
- `/search <text>` usually sets the keyword slot.
- If `<text>` looks like a single Bates/control token or a Bates range, Retriever treats it as a Bates lookup instead of full-text search.
- `/search --fts <text>` forces full-text search even when the text looks Bates-like.
- `/search --within <text>` narrows the current slot instead of replacing it.

### `--within` rules

- if the current slot is a keyword slot, Retriever AND-composes the new text with the existing keyword
- if the current slot is a Bates slot, Retriever intersects the current and new Bates ranges
- `--within` does not compose across slots

Examples:

```text
/search alpha
/search --within beta
```

Result: keyword scope becomes `(alpha) AND (beta)`.

```text
/search ABC000001-ABC000100
/search --within ABC000010-ABC000020
```

Result: Bates scope is narrowed to `ABC000010-ABC000020`.

```text
/search --fts ABC000123
```

Result: search for the literal text instead of switching to Bates mode.

## `/filter` syntax

`/filter` adds SQL-like metadata constraints to the persistent scope.

### Forms

```text
/filter
/filter clear
/filter <expression>
```

### How `/filter` composes

- `/filter` by itself shows the current filter expression
- `/filter clear` removes the current filter slot
- each new `/filter <expression>` is AND-composed with the existing filter slot

Example:

```text
/filter content_type = 'Email'
/filter date_created >= '2024-01-01'
```

Effective filter:

```text
(content_type = 'Email') AND (date_created >= '2024-01-01')
```

### Supported operators

- `=`
- `!=`
- `<>`
- `<`
- `<=`
- `>`
- `>=`
- `LIKE`
- `IS NULL`
- `IS NOT NULL`
- `IN (...)`
- `BETWEEN ... AND ...`

### Boolean syntax

- `AND`
- `OR`
- `NOT`
- parentheses for grouping

### Literal syntax

- strings in single or double quotes
- numbers as unquoted literals
- booleans as `TRUE` or `FALSE`
- `NULL`
- `%` and `_` wildcards with `LIKE`

### Useful field types

You can filter on:

- built-in fields such as `title`, `subject`, `author`, `participants`, `content_type`, `file_name`, `file_type`, `file_size`, `page_count`, `custodian`, `date_created`, `date_modified`, and `control_number`
- custom fields added with `add-field`
- virtual fields such as `dataset_name`, `production_name`, `is_attachment`, and `has_attachments`

Prefer canonical field names such as `date_created` instead of ad hoc variants.

### Filter examples

```text
/filter content_type = 'Email'
/filter file_type IN ('pdf', 'docx')
/filter date_created BETWEEN '2023-01-01' AND '2023-12-31'
/filter dataset_name = 'Hot Docs'
/filter production_name LIKE '%Acme%'
/filter is_attachment = TRUE
/filter has_attachments = TRUE
/filter (content_type = 'Email' OR content_type = 'Calendar') AND custodian = 'Smith'
/filter title IS NOT NULL
```

### When to use `/search` vs `/filter`

Use `/search` when you have keywords or a Bates lookup.

Use `/filter` when you want structured metadata constraints such as:

- file type
- content type
- date ranges
- dataset membership
- attachment state
- production name
- custom review fields

In practice you often use both:

```text
/search indemnification
/filter content_type = 'Email'
/filter custodian = 'Garcia'
```

## Discovering fields, columns, and sort keys

Use these commands when you are not sure what is available:

```text
/columns list
/sort list
/dataset list
/scope list
```

For full field discovery and aggregation metadata, use the CLI:

```bash
python3 skills/tool-template/tools.py catalog .
```

`catalog` is the source of truth for:

- searchable/filterable built-in fields
- custom fields currently registered in the workspace
- virtual fields such as `dataset_name`
- which date fields support `year`, `quarter`, `month`, and `week` aggregation buckets

## Display and paging tips

- use `/columns set ...` when you want a completely different table shape
- use `/columns add ...` or `/columns remove ...` for smaller adjustments
- use `/columns default` to reset to the standard layout
- `dataset_name` and `production_name` are displayable virtual columns
- some fields are filter-only and cannot be displayed, such as `has_attachments`
- use `/sort default` to go back to Retriever's automatic sort choice for the current scope
- page size changes affect both slash browsing and later view-mode listings until you change it again

Examples:

```text
/columns set title, control_number, dataset_name
/sort file_name asc
/page-size 50
/page 3
```

## Advanced CLI quick reference

### Health and setup

```bash
python3 skills/tool-template/tools.py workspace status .
python3 skills/tool-template/tools.py workspace status . --quick
python3 skills/tool-template/tools.py workspace init .
python3 skills/tool-template/tools.py workspace update .
python3 skills/tool-template/tools.py schema-version
```

### Search and retrieval

```bash
python3 skills/tool-template/tools.py search . "merger" --filter "content_type = 'Email'" --mode view
python3 skills/tool-template/tools.py get-doc . --doc-id 42 --include-text summary
python3 skills/tool-template/tools.py list-chunks . --doc-id 42 --page 1 --per-page 20
python3 skills/tool-template/tools.py search-chunks . "indemnification" --top-k 20
python3 skills/tool-template/tools.py aggregate . --group-by dataset_name --metric count
```

### Export

```bash
python3 skills/tool-template/tools.py export-csv . review.csv --field control_number --field title --select-from-scope
python3 skills/tool-template/tools.py export-previews . preview-bundle --doc-id 42
python3 skills/tool-template/tools.py export-archive . review.zip --select-from-scope
```

### Metadata and review operations

```bash
python3 skills/tool-template/tools.py list-fields .
python3 skills/tool-template/tools.py add-field . privilege_status text
python3 skills/tool-template/tools.py rename-field . old_tag new_tag
python3 skills/tool-template/tools.py describe-field . privilege_status --description "Privilege designation"
python3 skills/tool-template/tools.py change-field-type . issue_tag text
python3 skills/tool-template/tools.py delete-field . old_tag --confirm
python3 skills/tool-template/tools.py fill-field . --doc-id 42 --field privilege_status --value privileged
python3 skills/tool-template/tools.py set-field . --doc-id 42 --field title --value "Board Minutes"
python3 skills/tool-template/tools.py merge-into-conversation . --doc-id 42 --target-doc-id 17
python3 skills/tool-template/tools.py split-from-conversation . --doc-id 42
python3 skills/tool-template/tools.py clear-conversation-assignment . --doc-id 42
python3 skills/tool-template/tools.py reconcile-duplicates .
```

## Important details to remember

- Retriever is workspace-local. Changing workspaces means changing the database, browse state, datasets, and saved scopes you are working against.
- Re-ingest updates changed files in place, preserves stable document identity where possible, and marks missing items instead of silently forgetting them.
- PST support depends on the required `pypff` backend being available. Use `workspace status` if PST ingest is not ready; parser dependencies are lazy-installed into the shared plugin runtime (see *Runtime and dependencies* below), so the status check will also tell you if the runtime needs to be (re)populated.
- Production ingest is not the same as loose-file ingest. Use `ingest-production` when you want to target a production root explicitly.
- Manual field edits are protected from later automated overwrite.
- Results stay grounded in the active scope. If something looks missing, check `/scope`, `/dataset`, `/from-run`, `/sort`, and `/page-size` before assuming the underlying data is gone.

## Suggested first session

If you are trying Retriever for the first time, this sequence is a good starting point:

```text
1. Run retriever workspace status (and workspace init if needed)
2. Ask Retriever to index the workspace
3. /search <your first keyword>
4. /filter content_type = 'Email'
5. /columns add dataset_name
6. /scope save first-pass
7. export the current scope if needed
```

That path exercises the setup, browse, narrowing, display, persistence, and export surfaces that most users rely on first.

## License

Retriever is licensed under the Elastic License 2.0 (ELv2). The SPDX identifier is `Elastic-2.0`. See the [Elastic License 2.0](https://www.elastic.co/licensing/elastic-license) for the license terms.
