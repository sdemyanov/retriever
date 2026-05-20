# Browse And CLI Reference

This document holds Retriever's detailed browse, slash-command, and CLI
reference material.

For install paths, product framing, and typical workflows, see
[README.md](../README.md).

## Slash Command Reference

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

- bare forms such as `/scope`, `/dataset`, `/sort`, `/page`, `/page-size`,
  `/columns`, and `/field` are read-only state inspection
- `/next` is equivalent to `/page next`
- `/previous` is equivalent to `/page previous`
- `/field delete` and any bulk `/fill` (one that targets the active scope
  rather than explicit `on <doc-ref>` documents) require `--confirm` as a
  safety rail
- `/fill` will not target derived or system-managed fields such as
  `custodian`, `dataset_name`, `production_name`, hashes, ids, or ingest
  timestamps; use the appropriate ingest or conversation command instead
- values with spaces should be quoted
- comma-separated lists are supported for commands such as `/dataset`,
  `/columns set`, and `/sort`

## `/search` Syntax

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
- `/search clear` clears both the current keyword slot and the current Bates
  slot.
- `/search <text>` usually sets the keyword slot.
- If `<text>` looks like a single Bates/control token or a Bates range,
  Retriever treats it as a Bates lookup instead of full-text search.
- `/search --fts <text>` forces full-text search even when the text looks
  Bates-like.
- `/search --within <text>` narrows the current slot instead of replacing it.

### `--within` Rules

- if the current slot is a keyword slot, Retriever AND-composes the new text
  with the existing keyword
- if the current slot is a Bates slot, Retriever intersects the current and new
  Bates ranges
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

## `/filter` Syntax

`/filter` adds SQL-like metadata constraints to the persistent scope.

### Forms

```text
/filter
/filter clear
/filter <expression>
```

### How `/filter` Composes

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

### Supported Operators

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

### Boolean Syntax

- `AND`
- `OR`
- `NOT`
- parentheses for grouping

### Literal Syntax

- strings in single or double quotes
- numbers as unquoted literals
- booleans as `TRUE` or `FALSE`
- `NULL`
- `%` and `_` wildcards with `LIKE`

### Useful Field Types

You can filter on:

- built-in fields such as `title`, `subject`, `author`, `participants`,
  `content_type`, `file_name`, `file_type`, `file_size`, `page_count`,
  `custodian`, `date_created`, `date_modified`, and `control_number`
- custom fields added with `add-field`
- virtual fields such as `dataset_name`, `production_name`, `is_attachment`,
  and `has_attachments`

Prefer canonical field names such as `date_created` instead of ad hoc variants.

### Filter Examples

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

### When To Use `/search` Vs `/filter`

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

## Discovering Fields, Columns, And Sort Keys

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
- which date fields support `year`, `quarter`, `month`, and `week`
  aggregation buckets

## Display And Paging Tips

- use `/columns set ...` when you want a completely different table shape
- use `/columns add ...` or `/columns remove ...` for smaller adjustments
- use `/columns default` to reset to the standard layout
- `dataset_name` and `production_name` are displayable virtual columns
- some fields are filter-only and cannot be displayed, such as
  `has_attachments`
- use `/sort default` to go back to Retriever's automatic sort choice for the
  current scope
- page size changes affect both slash browsing and later view-mode listings
  until you change it again

Examples:

```text
/columns set title, control_number, dataset_name
/sort file_name asc
/page-size 50
/page 3
```

## Advanced CLI Quick Reference

### Health And Setup

```bash
python3 -m retriever workspace status .
python3 -m retriever workspace status . --quick
python3 -m retriever workspace init .
python3 -m retriever workspace update .
python3 -m retriever schema-version
```

### Search And Retrieval

```bash
python3 -m retriever search . "merger" --filter "content_type = 'Email'" --mode view
python3 -m retriever get-doc . --doc-id 42 --include-text summary
python3 -m retriever list-chunks . --doc-id 42 --page 1 --per-page 20
python3 -m retriever search-chunks . "indemnification" --top-k 20
python3 -m retriever aggregate . --group-by dataset_name --metric count
```

### Export

```bash
python3 skills/tool-template/tools.py export-csv . review.csv --field control_number --field title --select-from-scope
python3 skills/tool-template/tools.py export-previews . preview-bundle --doc-id 42
python3 skills/tool-template/tools.py export-archive . review.zip --select-from-scope
```

### Metadata And Review Operations

```bash
python3 skills/tool-template/tools.py list-fields .
python3 skills/tool-template/tools.py add-field . privilege_status text
python3 skills/tool-template/tools.py rename-field . old_tag new_tag
python3 skills/tool-template/tools.py describe-field . privilege_status --description "Privilege designation"
python3 skills/tool-template/tools.py change-field-type . issue_tag text
python3 skills/tool-template/tools.py delete-field . old_tag --confirm
python3 skills/tool-template/tools.py fill-field . --doc-id 42 --field privilege_status --value privileged
python3 skills/tool-template/tools.py set-field . --doc-id 42 --field title --value "Board Minutes"
python3 skills/tool-template/tools.py set-field . --doc-id 42 --doc-id 43 --field title --value "Board Minutes"
python3 skills/tool-template/tools.py merge-into-conversation . --doc-id 42 --target-doc-id 17
python3 skills/tool-template/tools.py split-from-conversation . --doc-id 42
python3 skills/tool-template/tools.py clear-conversation-assignment . --doc-id 42
python3 skills/tool-template/tools.py reconcile-duplicates .
```
