# Browse And CLI Reference

This document holds Retriever's detailed browse, slash-command, and CLI
reference material for Claude Code use.

For install paths, product framing, and typical workflows, see
[README.md](../README.md).

## Slash Command Reference

These are the persistent browse commands users run inside Claude Code after
installing Retriever.

| Command | Purpose | Examples |
|---|---|---|
| `/retriever:search` | Show or set the current keyword/Bates search slot | `/retriever:search`, `/retriever:search contract`, `/retriever:search --within renewal`, `/retriever:search clear`, `/retriever:search --fts ABC000123` |
| `/retriever:filter` | Show, add, or clear the current SQL-like filter expression | `/retriever:filter`, `/retriever:filter content_type = 'Email'`, `/retriever:filter clear` |
| `/retriever:bates` | Show, set, or clear the current Bates selector | `/retriever:bates`, `/retriever:bates ABC000123-ABC000150`, `/retriever:bates clear` |
| `/retriever:dataset` | Show, list, set, clear, or rename dataset selectors | `/retriever:dataset`, `/retriever:dataset list`, `/retriever:dataset "Review Set"`, `/retriever:dataset clear`, `/retriever:dataset rename "Old Set" "New Set"` |
| `/retriever:scope` | Show, list, save, load, or clear the whole current scope | `/retriever:scope`, `/retriever:scope list`, `/retriever:scope save hotdocs`, `/retriever:scope load hotdocs`, `/retriever:scope clear` |
| `/retriever:sort` | Show, list, set, or reset browse sorting | `/retriever:sort`, `/retriever:sort list`, `/retriever:sort file_name asc`, `/retriever:sort date_created desc, file_name asc`, `/retriever:sort default` |
| `/retriever:page` | Show current page state or jump to another page | `/retriever:page`, `/retriever:page 3`, `/retriever:page first`, `/retriever:page last`, `/retriever:page next`, `/retriever:page previous` |
| `/retriever:next` | Go to the next page | `/retriever:next` |
| `/retriever:previous` | Go to the previous page | `/retriever:previous` |
| `/retriever:page-size` | Show or set rows per page | `/retriever:page-size`, `/retriever:page-size 25` |
| `/retriever:columns` | Show, list, set, add, remove, or reset visible columns | `/retriever:columns`, `/retriever:columns list`, `/retriever:columns set title, control_number`, `/retriever:columns add dataset_name`, `/retriever:columns remove author`, `/retriever:columns default` |
| `/retriever:from-run` | Show, set, or clear a prior run selector | `/retriever:from-run`, `/retriever:from-run 42`, `/retriever:from-run clear` |
| `/retriever:field` | Inspect or manage the custom-field schema | `/retriever:field`, `/retriever:field list`, `/retriever:field add privilege_status text`, `/retriever:field rename old_tag new_tag`, `/retriever:field describe privilege_status "Privilege designation"`, `/retriever:field type issue_tag text`, `/retriever:field delete old_tag --confirm` |
| `/retriever:fill` | Set or clear a field value on one document or a scoped result set | `/retriever:fill privilege_status privileged on DOC001.00000042`, `/retriever:fill privilege_status clear on DOC001.00000042`, `/retriever:fill reviewer "J. Doe" --confirm` (bulk fill against the active scope) |

Notes:

- bare forms such as `/retriever:scope`, `/retriever:dataset`,
  `/retriever:sort`, `/retriever:page`, `/retriever:page-size`,
  `/retriever:columns`, and `/retriever:field` are read-only state inspection
- `/retriever:next` is equivalent to `/retriever:page next`
- `/retriever:previous` is equivalent to `/retriever:page previous`
- `/retriever:field delete` and any bulk `/retriever:fill` (one that targets
  the active scope rather than explicit `on <doc-ref>` documents) require
  `--confirm` as a safety rail
- `/retriever:fill` will not target derived or system-managed fields such as
  `custodian`, `dataset_name`, `production_name`, hashes, ids, or ingest
  timestamps; use the appropriate ingest or conversation command instead
- values with spaces should be quoted
- comma-separated lists are supported for commands such as
  `/retriever:dataset`, `/retriever:columns set`, and `/retriever:sort`

## `/retriever:search` Syntax

`/retriever:search` controls the current keyword or Bates slot in the
persistent scope.

### Forms

```text
/retriever:search
/retriever:search clear
/retriever:search <text>
/retriever:search --within <text>
/retriever:search --fts <text>
```

### Behavior

- `/retriever:search` by itself shows the current keyword slot.
- `/retriever:search clear` clears both the current keyword slot and the
  current Bates slot.
- `/retriever:search <text>` usually sets the keyword slot.
- If `<text>` looks like a single Bates/control token or a Bates range,
  Retriever treats it as a Bates lookup instead of full-text search.
- `/retriever:search --fts <text>` forces full-text search even when the text
  looks Bates-like.
- `/retriever:search --within <text>` narrows the current slot instead of
  replacing it.

### `--within` Rules

- if the current slot is a keyword slot, Retriever AND-composes the new text
  with the existing keyword
- if the current slot is a Bates slot, Retriever intersects the current and new
  Bates ranges
- `--within` does not compose across slots

Examples:

```text
/retriever:search alpha
/retriever:search --within beta
```

Result: keyword scope becomes `(alpha) AND (beta)`.

```text
/retriever:search ABC000001-ABC000100
/retriever:search --within ABC000010-ABC000020
```

Result: Bates scope is narrowed to `ABC000010-ABC000020`.

```text
/retriever:search --fts ABC000123
```

Result: search for the literal text instead of switching to Bates mode.

## `/retriever:filter` Syntax

`/retriever:filter` adds SQL-like metadata constraints to the persistent
scope.

### Forms

```text
/retriever:filter
/retriever:filter clear
/retriever:filter <expression>
```

### How `/retriever:filter` Composes

- `/retriever:filter` by itself shows the current filter expression
- `/retriever:filter clear` removes the current filter slot
- each new `/retriever:filter <expression>` is AND-composed with the existing
  filter slot

Example:

```text
/retriever:filter content_type = 'Email'
/retriever:filter date_created >= '2024-01-01'
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
/retriever:filter content_type = 'Email'
/retriever:filter file_type IN ('pdf', 'docx')
/retriever:filter date_created BETWEEN '2023-01-01' AND '2023-12-31'
/retriever:filter dataset_name = 'Hot Docs'
/retriever:filter production_name LIKE '%Acme%'
/retriever:filter is_attachment = TRUE
/retriever:filter has_attachments = TRUE
/retriever:filter (content_type = 'Email' OR content_type = 'Calendar') AND custodian = 'Smith'
/retriever:filter title IS NOT NULL
```

### When To Use `/retriever:search` Vs `/retriever:filter`

Use `/retriever:search` when you have keywords or a Bates lookup.

Use `/retriever:filter` when you want structured metadata constraints such as:

- file type
- content type
- date ranges
- dataset membership
- attachment state
- production name
- custom metadata fields

In practice you often use both:

```text
/retriever:search indemnification
/retriever:filter content_type = 'Email'
/retriever:filter custodian = 'Garcia'
```

## Discovering Fields, Columns, And Sort Keys

Use these commands when you are not sure what is available:

```text
/retriever:columns list
/retriever:sort list
/retriever:dataset list
/retriever:scope list
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

- use `/retriever:columns set ...` when you want a completely different table
  shape
- use `/retriever:columns add ...` or `/retriever:columns remove ...` for
  smaller adjustments
- use `/retriever:columns default` to reset to the standard layout
- `dataset_name` and `production_name` are displayable virtual columns
- some fields are filter-only and cannot be displayed, such as
  `has_attachments`
- use `/retriever:sort default` to go back to Retriever's automatic sort
  choice for the current scope
- page size changes affect both slash browsing and later view-mode listings
  until you change it again

Examples:

```text
/retriever:columns set title, control_number, dataset_name
/retriever:sort file_name asc
/retriever:page-size 50
/retriever:page 3
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
python3 skills/tool-template/tools.py export-csv . results.csv --field control_number --field title --select-from-scope
python3 skills/tool-template/tools.py export-previews . preview-bundle --doc-id 42
python3 skills/tool-template/tools.py export-archive . results.zip --select-from-scope
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
python3 skills/tool-template/tools.py reconcile-duplicates . --doc-id 15 --doc-id 16 --apply
python3 skills/tool-template/tools.py reconcile-duplicates . --dataset "Slack Export" --apply
```
