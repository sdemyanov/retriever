# Schema v12

## Versioning

- schema version: `12`
- database file: `.retriever/retriever.db`
- timestamps: UTC ISO 8601 with `Z`
- booleans: `0` or `1`

## SQLite pragmas

Apply these pragmas on every write connection:

```sql
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
```

Journal mode policy:

- prefer `WAL` on normal local workspaces
- if the filesystem or mount rejects `WAL`, fall back to `DELETE`
- `bootstrap` may remove obviously stale zero-byte SQLite artifacts and retry once before surfacing an error

## Table definitions

### `workspace_meta`

Stores installation metadata for the current workspace.

```sql
CREATE TABLE IF NOT EXISTS workspace_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  schema_version INTEGER NOT NULL,
  tool_version TEXT NOT NULL,
  requirements_version TEXT NOT NULL,
  template_source TEXT NOT NULL,
  template_sha256 TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

### `datasets`

Named document collections. Source-backed datasets keep their primary source identity here for compatibility and migration, while canonical membership lives in `dataset_documents`.

```sql
CREATE TABLE IF NOT EXISTS datasets (
  id INTEGER PRIMARY KEY,
  source_kind TEXT NOT NULL,
  dataset_locator TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

### `dataset_sources`

Source bindings that automatically feed a dataset on ingest/reingest.

```sql
CREATE TABLE IF NOT EXISTS dataset_sources (
  id INTEGER PRIMARY KEY,
  dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_locator TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

### `dataset_documents`

Dataset membership rows. One document may belong to many datasets, and one dataset may contain many documents.

```sql
CREATE TABLE IF NOT EXISTS dataset_documents (
  id INTEGER PRIMARY KEY,
  dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
  document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  dataset_source_id INTEGER REFERENCES dataset_sources(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

### `documents`

Core registry for indexed documents.

```sql
CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY,
  control_number TEXT UNIQUE,
  dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
  parent_document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
  source_kind TEXT,
  source_rel_path TEXT,
  source_item_id TEXT,
  source_folder_path TEXT,
  production_id INTEGER REFERENCES productions(id) ON DELETE SET NULL,
  begin_bates TEXT,
  end_bates TEXT,
  begin_attachment TEXT,
  end_attachment TEXT,
  rel_path TEXT NOT NULL UNIQUE,
  file_name TEXT NOT NULL,
  file_type TEXT,
  file_size INTEGER,
  page_count INTEGER,
  author TEXT,
  content_type TEXT,
  custodian TEXT,
  date_created TEXT,
  date_modified TEXT,
  title TEXT,
  subject TEXT,
  participants TEXT,
  recipients TEXT,
  manual_field_locks_json TEXT NOT NULL DEFAULT '[]',
  file_hash TEXT,
  content_hash TEXT,
  text_status TEXT NOT NULL DEFAULT 'ok',
  lifecycle_status TEXT NOT NULL DEFAULT 'active',
  ingested_at TEXT,
  last_seen_at TEXT,
  updated_at TEXT,
  control_number_batch INTEGER,
  control_number_family_sequence INTEGER,
  control_number_attachment_sequence INTEGER
);
```

Allowed values:

- `text_status`: `ok`, `partial`, `failed`, `empty`
- `lifecycle_status`: `active`, `missing`, `deleted`

#### Manual field lock rule

Retriever treats the document columns themselves as the effective values users see and query. Manual user edits are preserved by locking the edited field names against future automated writes.

- `manual_field_locks_json` stores a JSON array of user-editable document field names whose current values came from a manual action.
- This applies to:
  - editable built-in metadata fields such as `page_count`, `author`, `content_type`, `custodian`, `date_created`, `date_modified`, `title`, `subject`, `participants`, and `recipients`
  - custom field columns added to `documents` through `ALTER TABLE`
- When a user manually edits one of those fields, Retriever updates the column itself and adds that field name to `manual_field_locks_json`.
- Automated ingest or review may refresh unlocked fields, but it must not overwrite any field named in `manual_field_locks_json`.
- To accept a newly extracted or AI-produced value later, the lock must be cleared explicitly before or during the overwrite workflow.
- System-managed columns such as `control_number`, `dataset_id`, `parent_document_id`, `source_kind`, `production_id`, `begin_bates`, `end_bates`, `begin_attachment`, `end_attachment`, `rel_path`, `file_name`, `file_type`, `file_size`, `file_hash`, `content_hash`, `text_status`, `lifecycle_status`, `ingested_at`, `last_seen_at`, `updated_at`, `control_number_batch`, `control_number_family_sequence`, and `control_number_attachment_sequence` are never manually settable or lockable.
- `manual_field_locks_json` and the legacy `locked_metadata_fields_json` helper column are also never manually settable or lockable. Lock state must flow through dedicated Retriever behavior, not ad hoc edits to helper JSON.

Because manual values live in the primary document columns, the FTS indexes and normal SQL filters continue to use the user-visible values automatically.

#### Field visibility policy

Retriever distinguishes between user-facing document metadata and internal helper fields when presenting field lists.

- Default field views should show user-facing document fields plus custom fields:
  - `control_number`
  - `rel_path`, `file_name`, `file_type`, `file_size`
  - `page_count`, `author`, `content_type`, `custodian`, `date_created`, `date_modified`
  - `title`, `subject`, `participants`, `recipients`
  - custom fields registered in `custom_fields_registry`
- Hide pure bookkeeping/helper columns by default:
  - `id`
  - `file_hash`, `content_hash`
  - `text_status`, `lifecycle_status`
  - `ingested_at`, `last_seen_at`, `updated_at`
  - `dataset_id`
  - `parent_document_id`
  - `source_kind`, `source_rel_path`, `source_item_id`, `source_folder_path`, `production_id`
  - `begin_bates`, `end_bates`, `begin_attachment`, `end_attachment`
  - `control_number_batch`, `control_number_family_sequence`, `control_number_attachment_sequence`
  - `manual_field_locks_json`, `locked_metadata_fields_json`
- If the user explicitly asks for "all fields" or is debugging schema/runtime behavior, show every column and label helper fields as system/read-only instead of silently omitting them.
- `control_number`, `content_type`, `custodian`, and `participants` are user-facing built-in metadata fields and should remain visible in default field views.
- `dataset_name` is a virtual/user-facing projection derived from `dataset_documents` membership rows; it may be shown or filtered, but it is not stored as a column on `documents`.

#### Control Number rule

`control_number` is a stable user-facing document label for review and export.

- Standalone documents and parent emails use the format `DOCXXX.YYYYYYYY`.
- Child attachments use the format `DOCXXX.YYYYYYYY.ZZZ`.
- `XXX` is the immutable first-seen ingestion batch of the parent family.
- `YYYYYYYY` is the parent/family sequence within that first-seen batch.
- `ZZZ` is the attachment sequence within the parent family.
- Reindex is not a renumbering event.
- New attachments on an existing parent get the next unused `.ZZZ`.
- Removed attachment suffixes are retired and not reused.

#### Content type rule

`content_type` is a built-in metadata field on `documents`.

- Default value comes from extension-to-type mapping.
- That default may be overwritten during ingest when actual content inspection is more trustworthy.
- Example: a `.pdf` defaults to `E-Doc`, but a PDF whose first-page body contains email headers may be classified as `Email`.
- Typical override signals include:
  - email-style headers near the start of the document
  - calendar markers such as `BEGIN:VCALENDAR`

#### Custodian rule

`custodian` is a built-in metadata field on `documents`.

- It identifies who the source material was collected from when Retriever can determine that provenance reliably.
- Retriever should populate it using source-kind-specific rules rather than blindly copying `source_rel_path`.
- For PST-derived message rows, default `custodian` comes from the owning `.pst` container name without the extension.
- PST-derived child attachment rows inherit `custodian` from their parent message row.
- Existing workspaces may require reindex or migration backfill to populate `custodian` on already ingested rows.

#### Dataset rule

Datasets are first-class collections, and document membership is many-to-many.

- Canonical dataset membership lives in `dataset_documents`, not in `documents.dataset_id`.
- A document may belong to multiple datasets.
- A dataset may be:
  - source-backed, through one or more `dataset_sources` rows
  - manually curated, with explicit `dataset_documents` rows and no `dataset_sources`
  - mixed, with both source-fed and manual memberships
- Reingestion of the same source must reuse the existing source-backed dataset instead of allocating a new one.
- Filesystem ingest uses one source binding per selected workspace root, with `source_kind = filesystem` and `source_locator = '.'`.
- PST ingest uses one source binding per `.pst` container, with `source_kind = pst` and `source_locator = source_rel_path`.
- Production ingest uses one source binding per production root, with `source_kind = production` and `source_locator = productions.rel_root`.
- Derived child attachment rows inherit the same dataset memberships as their source parent at ingest/reingest time.
- Normal search/browse only returns documents that still have at least one `dataset_documents` membership row.
- User-facing filtering/display should use `dataset_name`, not the raw compatibility/cache columns.
- `documents.dataset_id` remains a system-managed compatibility/source-hint field during the transition from the older scalar model and must not be treated as the authoritative membership source.

#### Participants rule

`participants` is a built-in metadata field on `documents`.

- For email documents, it stores the union of all senders and recipients found across the full chain contained in the document text.
- For chat-like documents, it stores the union of speaker names found in the transcript.
- Existing workspaces should reindex after upgrading to populate `participants` on already ingested rows.

#### Email family rule

For EML and MSG parents, Retriever may create one level of child attachment documents.

- Parent emails keep `parent_document_id = NULL`.
- Extracted attachments become child rows with `parent_document_id` pointing to the parent email row.
- CID-backed image parts that are rendered inline in the HTML preview are not materialized as child attachment rows.
- Child attachment rows are derived documents backed by extracted blobs under `.retriever/previews/<parent>/attachments/`.
- Child attachment rows are reconciled only through parent-email ingest, not the top-level filesystem scan.
- If a parent email is re-ingested, matched unchanged child attachments keep their `control_number`, locked built-in values, and custom-field values.
- Removed child attachments are marked `deleted` and their derived blobs/previews are cleaned up.

#### Production document rule

For processed productions ingested through `ingest-production`, Retriever stores one logical document row per load-file row.

- `source_kind = production`
- `control_number` stores the produced Bates/control number, using `Begin Bates` when present
- `begin_bates` and `end_bates` store the produced document span
- `begin_attachment` and `end_attachment` store the produced family span when present
- linked `TEXT`, `IMAGES`, and `NATIVES` files are recorded as source parts rather than as separate top-level documents
- production-family child rows use `parent_document_id` so they can be shown beneath the parent in search/detail views

### `productions`

Tracks each ingested processed production root.

```sql
CREATE TABLE IF NOT EXISTS productions (
  id INTEGER PRIMARY KEY,
  dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
  rel_root TEXT NOT NULL UNIQUE,
  production_name TEXT NOT NULL,
  metadata_load_rel_path TEXT NOT NULL,
  image_load_rel_path TEXT,
  source_type TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

`productions.dataset_id` is a source-backed compatibility/cache pointer to the dataset associated with that production root. Canonical document membership still lives in `dataset_documents`.

### `document_source_parts`

Records the linked source artifacts that make up one logical production document.

```sql
CREATE TABLE IF NOT EXISTS document_source_parts (
  id INTEGER PRIMARY KEY,
  document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  part_kind TEXT NOT NULL,
  rel_source_path TEXT NOT NULL,
  ordinal INTEGER NOT NULL DEFAULT 0,
  label TEXT,
  created_at TEXT NOT NULL
);
```

### `container_sources`

Tracks scanned source containers such as PST files.

```sql
CREATE TABLE IF NOT EXISTS container_sources (
  id INTEGER PRIMARY KEY,
  dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
  source_kind TEXT NOT NULL,
  source_rel_path TEXT NOT NULL UNIQUE,
  file_size INTEGER,
  file_mtime TEXT,
  file_hash TEXT,
  message_count INTEGER,
  last_scan_started_at TEXT,
  last_scan_completed_at TEXT,
  last_ingested_at TEXT
);
```

`container_sources.dataset_id` is a source-backed compatibility/cache pointer to the dataset associated with that container source.

### `document_previews`

Maps a document to one or more preview artifacts.

```sql
CREATE TABLE IF NOT EXISTS document_previews (
  id INTEGER PRIMARY KEY,
  document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  rel_preview_path TEXT NOT NULL,
  preview_type TEXT NOT NULL,
  label TEXT,
  ordinal INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
```

### `document_chunks`

Stores chunked text for large-document retrieval.

```sql
CREATE TABLE IF NOT EXISTS document_chunks (
  id INTEGER PRIMARY KEY,
  document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,
  char_start INTEGER NOT NULL,
  char_end INTEGER NOT NULL,
  token_estimate INTEGER,
  text_content TEXT NOT NULL,
  UNIQUE(document_id, chunk_index)
);
```

### `documents_fts`

Metadata-only full-text index.

```sql
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
  document_id UNINDEXED,
  file_name,
  title,
  subject,
  author,
  custodian,
  participants,
  recipients
);
```

### `chunks_fts`

Chunk-text full-text index.

```sql
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
  chunk_id UNINDEXED,
  document_id UNINDEXED,
  text_content
);
```

### `custom_fields_registry`

Registry for custom fields added directly to `documents` via `ALTER TABLE`.

```sql
CREATE TABLE IF NOT EXISTS custom_fields_registry (
  id INTEGER PRIMARY KEY,
  field_name TEXT NOT NULL UNIQUE,
  field_type TEXT NOT NULL,
  instruction TEXT,
  created_at TEXT NOT NULL
);
```

Allowed `field_type` values:

- `text`
- `integer`
- `real`
- `boolean`

### `control_number_batches`

Internal allocator state for first-seen family numbering.

```sql
CREATE TABLE IF NOT EXISTS control_number_batches (
  batch_number INTEGER PRIMARY KEY,
  next_family_sequence INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

## Custom fields via `ALTER TABLE`

Custom fields are stored as real columns on `documents`, not as EAV rows.

- `add-field` runs `ALTER TABLE documents ADD COLUMN <name> <type>` and inserts a row into `custom_fields_registry`.
- Queries use structured filters that compile to ordinary SQL predicates such as `contract_type = ?`.
- The registry exists to track which columns are user-defined, their intended types, and the extraction instruction Claude should use.
- Field names must be sanitized to avoid collisions with built-in document columns.
- If `ALTER TABLE` succeeds but the registry insert fails, Retriever must reconcile against `PRAGMA table_info(documents)` before retrying field creation.
- Manual `set-field` updates must add the target field name to `manual_field_locks_json`.
- Automated review workflows must skip locked custom fields unless the user explicitly requests overwrite.

Tradeoffs accepted for MVP:

- SQLite cannot drop columns added this way without rebuilding the table.
- Custom fields can be equality-filtered and sorted like normal columns, but adding them to FTS requires explicit future indexing work.
- No per-value provenance tracking yet.

## Structured filter contract

Search does not accept raw SQL filter strings.

- Use repeatable filters in the form `--filter <field> <op> <value>`.
- `field` must resolve to a built-in `documents` column or a valid `custom_fields_registry.field_name`.
- Virtual filter fields are also allowed:
  - `dataset_name`
  - `production_name`
  - `is_attachment`
  - `has_attachments`
- `op` must be validated against field type.
- Supported MVP operators:
  - `eq`
  - `neq`
  - `gt`
  - `gte`
  - `lt`
  - `lte`
  - `contains`
  - `is-null`
  - `not-null`
- The tool must compile filters into parameterized SQL and must not splice raw user strings into the query.
- Custom fields participate in structured filtering and sorting in MVP.
- Virtual filter fields participate in filtering but not sorting.
- Field aliases may normalize to virtual fields, for example `dataset` and `dataset_label` both resolve to `dataset_name`.
- `is_attachment` and `has_attachments` are the virtual boolean filters in MVP.
- `is_attachment eq true` returns child attachment documents.
- `has_attachments eq true` returns documents that currently have at least one non-deleted child attachment.
- Full-text search over custom field values is deferred to a later phase.

## Structured sort contract

- `search` accepts `--sort` and `--order`
- Valid sort keys are:
  - `relevance` when query text is non-empty
  - any built-in `documents` column
  - any valid `custom_fields_registry.field_name`
- `--order` may be `asc` or `desc`
- If no explicit sort is provided:
  - keyword search defaults to `relevance asc`
  - browse/filter-only search defaults to `updated_at desc`
- Equal primary sort values must break on `documents.id asc` so pagination is stable

## Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_documents_file_hash ON documents(file_hash);
CREATE INDEX IF NOT EXISTS idx_documents_content_hash ON documents(content_hash);
CREATE INDEX IF NOT EXISTS idx_documents_lifecycle_status ON documents(lifecycle_status);
CREATE INDEX IF NOT EXISTS idx_documents_parent_document_id ON documents(parent_document_id);
CREATE INDEX IF NOT EXISTS idx_documents_dataset_id ON documents(dataset_id);
CREATE INDEX IF NOT EXISTS idx_documents_control_number_sort
  ON documents(control_number_batch, control_number_family_sequence, control_number_attachment_sequence);
CREATE UNIQUE INDEX IF NOT EXISTS idx_datasets_source_locator_unique
  ON datasets(source_kind, dataset_locator);
CREATE INDEX IF NOT EXISTS idx_dataset_sources_dataset_id
  ON dataset_sources(dataset_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_sources_locator_unique
  ON dataset_sources(source_kind, source_locator);
CREATE INDEX IF NOT EXISTS idx_dataset_documents_document_id
  ON dataset_documents(document_id);
CREATE INDEX IF NOT EXISTS idx_dataset_documents_dataset_id
  ON dataset_documents(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dataset_documents_source_id
  ON dataset_documents(dataset_source_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_documents_membership_unique
  ON dataset_documents(dataset_id, document_id, COALESCE(dataset_source_id, 0));
CREATE INDEX IF NOT EXISTS idx_productions_dataset_id ON productions(dataset_id);
CREATE INDEX IF NOT EXISTS idx_container_sources_dataset_id ON container_sources(dataset_id);
CREATE INDEX IF NOT EXISTS idx_previews_document_id ON document_previews(document_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON document_chunks(document_id, chunk_index);
```

## Path rules

- `documents.rel_path` is relative to the workspace root
- top-level parent documents use workspace-relative `rel_path` values
- derived child attachment rows may use `.retriever/...` workspace-relative paths that point at extracted attachment blobs
- `document_previews.rel_preview_path` is relative to `.retriever/`
- no document record should store an absolute filesystem path

## Re-ingest rules

- re-ingest only processes new, changed, renamed, or missing files
- unchanged files are skipped
- changed files may refresh extracted metadata and text
- EML/MSG parent re-ingest must reconcile one-level child attachment rows inside the same parent transaction
- child attachment rows must not participate in the top-level "missing file" scan
- any manually locked field must be preserved exactly as currently stored
- ingest is transactional per file, not per batch

## Migration rules

- v1 initialization is idempotent
- v2 added `documents.locked_metadata_fields_json` as a metadata-only lock field
- v3 adds `documents.manual_field_locks_json` if it is missing
- if legacy `locked_metadata_fields_json` exists, v3 migration must merge its values into `manual_field_locks_json`
- v4 adds `documents.content_type` if it is missing
- v4 backfills missing `content_type` values from `file_type` extension mapping
- v5 adds `documents.participants` if it is missing
- v5 rebuilds `documents_fts` with a `participants` column
- v5 does not backfill `participants`; reindex existing documents to populate it from extracted content
- v7 renames `documents.display_id` to `documents.control_number`
- v7 renames `documents.display_batch`, `documents.display_family_sequence`, and `documents.display_attachment_sequence` to the matching `control_number_*` helper columns
- v7 renames `display_id_batches` to `control_number_batches`
- v7 backfills stable `control_number` values for existing top-level rows
- v7 also repairs partially populated control number identity fields when a row has `control_number` but missing batch/family sequence columns
- v8 adds production-aware document columns: `source_kind`, `production_id`, `begin_bates`, `end_bates`, `begin_attachment`, and `end_attachment`
- v8 adds the `productions` and `document_source_parts` tables
- v8 backfills existing non-production rows to `source_kind = filesystem` or `source_kind = email_attachment`
- v7 keeps future child attachment numbering stable across parent re-ingest
- v9 adds provenance-aware document columns: `source_rel_path`, `source_item_id`, and `source_folder_path`
- v10 adds `documents.custodian`
- v10 rebuilds `documents_fts` with a `custodian` column
- v10 may backfill derivable `custodian` values from existing source metadata
- v11 adds the initial scalar dataset model: the `datasets` table plus `documents.dataset_id`, `productions.dataset_id`, and `container_sources.dataset_id`
- v11 backfills source-backed dataset rows and scalar `dataset_id` assignments for existing documents using source-specific locators
- v12 adds `dataset_sources` and `dataset_documents`
- v12 moves canonical dataset membership to `dataset_documents`, while retaining the older scalar dataset fields as compatibility/cache columns
- v12 backfills source bindings and dataset memberships for pre-v12 workspaces
- for future migrations with non-obvious tradeoffs, stop and ask the user whether to migrate in place or reindex everything
- future migrations must preserve user data
- destructive downgrades are not supported
- if a migration changes the tool materially, back up the old workspace tool before replacing it

## JSON contracts

### `doctor`

Expected JSON shape:

```json
{
  "overall": "pass",
  "tool_version": "0.9.4",
  "schema_version": 12,
  "python_version": "3.10.12",
  "pip_version": "25.3",
  "sqlite_version": "3.37.2",
  "sqlite_journal_mode": "wal",
  "fts5": {
    "status": "pass",
    "detail": "FTS5 virtual table created successfully"
  },
  "pst_backend": {
    "status": "pass",
    "detail": "PST backend import succeeded"
  },
  "workspace": {
    "root": "/path/to/workspace",
    "state": "initialized",
    "db_present": true,
    "db_size_bytes": 98304,
    "runtime_present": true,
    "tool_present": true
  },
  "workspace_inventory": {
    "parent_documents": 11,
    "missing_parent_documents": 0,
    "attachment_children": 0,
    "documents_total": 11
  }
}
```

### `bootstrap`

Expected JSON shape:

```json
{
  "status": "initialized",
  "workspace_root": "/path/to/workspace",
  "schema_version": 12,
  "tool_version": "0.9.4",
  "requirements_version": "2026-04-16-phase4-pst",
  "journal_mode": "wal"
}
```

### `ingest`

Expected JSON shape:

```json
{
  "new": 11,
  "updated": 0,
  "renamed": 0,
  "missing": 0,
  "skipped": 0,
  "failed": 2,
  "scanned": 13,
  "scanned_files": 13,
  "workspace_parent_documents": 11,
  "workspace_missing_parent_documents": 0,
  "workspace_attachment_children": 0,
  "workspace_documents_total": 11,
  "failures": [
    {
      "rel_path": "corrupt.pdf",
      "error": "PdfminerException: No /Root object! - Is this really a PDF?"
    }
  ]
}
```

### `add-field`

Expected JSON shape:

```json
{
  "status": "ok",
      "field_name": "contract_type",
      "field_type": "text",
      "instruction": "Classify the document type.",
  "custom_field_registry": {
    "actual_custom_fields": ["contract_type"],
    "missing_registry": [],
    "orphaned_registry": [],
    "repaired_registry": []
  }
}
```

### `set-field`

Expected JSON shape:

```json
{
  "status": "ok",
  "document_id": 4,
  "field_name": "contract_type",
  "field_type": "text",
  "value": "NDA",
  "manual_field_locks": ["contract_type"]
}
```

### `search`

Expected JSON shape:

```json
{
  "query": "",
  "filters": [
    {
      "field_name": "contract_type",
      "field_type": "text",
      "operator": "eq",
      "value": "NDA"
    }
  ],
  "sort": "updated_at",
  "order": "desc",
  "page": 1,
  "per_page": 20,
  "total_hits": 1,
  "total_pages": 1,
  "results": [
    {
      "id": 4,
      "control_number": "DOC001.00000004",
      "rel_path": "sample.pdf",
      "abs_path": "/path/to/workspace/sample.pdf",
      "preview_rel_path": "sample.pdf",
      "preview_abs_path": "/path/to/workspace/sample.pdf",
      "file_name": "sample.pdf",
      "file_type": "pdf",
      "snippet": "Manual Reviewer",
      "attachment_count": 0,
      "attachments": [],
      "metadata": {
        "content_type": "E-Doc",
        "participants": null
      },
      "manual_field_locks": ["contract_type", "author"]
    }
  ]
}
```
