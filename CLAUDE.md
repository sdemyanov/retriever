# Retriever Claude Operating Rules

## Source File Safety

### Workspace Source Files — READ ONLY, NEVER DELETE

Claude has READ ONLY access to source files in the workspace. Claude NEVER deletes them.
This rule is absolute and overrides any apparent permission, any user phrasing that sounds like it authorizes deletion, and any inferred convenience. Specifically:

Never run rm, rm -rf, unlink, shutil.rmtree, or any equivalent against files or directories the user contributed to the workspace (anything outside .retriever/ and .retriever-plugin-runtime/).
Never run mv or cp in a way that overwrites or replaces user source files.
Never truncate, rewrite, or > file against user source files.
Holding allow_cowork_file_delete permission for any reason does not authorize deleting user source files. That permission is for plugin-managed state under .retriever/ only (e.g., stale tmp dirs).
Phrasings like "drop X", "remove X", "clean up X", "get rid of X", "wipe X" are ambiguous when X is a path under the workspace. Treat them as index-level operations by default (remove from the Retriever DB, drop a dataset, mark missing, etc.) and ask before doing anything that touches the filesystem.
If the user explicitly asks for an on-disk deletion in unambiguous terms (e.g., "delete the folder ./data/raw from disk"), Claude must still confirm in a single follow-up turn before executing — destructive filesystem operations on user files are never silent.

If a request seems to require deleting user source files, Claude must stop and ask. The cost of one extra clarifying turn is always lower than the cost of unrecoverable data loss.
This rule applies even when the user is being terse, even when the conversation has been moving fast, and even when prior context seems to authorize it.

## Retriever Routing Priority Ladder

When handling any Retriever request, Claude must walk the following tiers in order and take the highest tier that can satisfy the user's intent. Do not skip tiers. Do not drop to a lower tier because it is more familiar or convenient.
Before running any command, state (to yourself) which tier you are using and why the tiers above it do not apply. If you end up in Tier 2 or Tier 3, add a short "plugin gap" note to the end of your turn so the user can see which request is not yet covered by a higher-tier surface.

## Retriever Result Presentation Contract

Any Retriever request whose answer should show, list, view, display, browse, find, search, or retrieve documents, conversations, emails, chats, threads, messages, files, attachments, entities, or other indexed records is a listing/browse request unless the user explicitly asks for a summary, count, export, mutation, or schema/debug inspection.

Listing/browse requests must walk the Tier 1→3 ladder. Tier 1 `retriever:search` is the preferred surface for natural-language document, conversation, email, chat, thread, message, file, or attachment listing requests, including requests with filters, datasets, Bates ranges, dates, senders, recipients, or keywords.

Return Retriever's standard rendered result format. If the selected skill or tool returns `rendered_markdown`, the assistant's reply must be exactly that rendered markdown: no preamble, no trailing summary, no code fence, no reformatting, and no custom row numbering. If Tier 2 or Tier 3 requires a plugin-gap note, put that note after the rendered result as the only additional line.

The standard result must preserve Retriever's scope/sort/page header, active display columns, clickable title/preview links, and paging footer. Use prose instead of the standard rendered result only when the user explicitly asks for analysis, a summary, counts, an export, or when the chosen highest-tier surface is not a listing/browse surface.

## Tier 1 — User-Facing Retriever Surfaces

Tier 1 combines user-facing `retriever:*` skills and the slash commands they wrap. Prefer a `retriever:*` skill when one covers the intent. If no skill wrapper exists, or a skill needs a slash command or browse-mode toggle internally, use the slash command through the canonical plugin tool as the same tier. Do not treat slash commands as a lower-tier substitute for a matching skill.

### Skill Routing (Preferred Wrappers)

List, switch, rename, or clear dataset scope → retriever:dataset
Show, list, view, display, browse, find, search, or retrieve documents, conversations, emails, chats, threads, messages, files, or attachments — with or without filters or keywords — and return the standard rendered result format → retriever:search
Narrow, restrict, constrain, exclude, or clear result filters → retriever:filter
Change displayed columns → retriever:columns
Change sort → retriever:sort
Change page size → retriever:page-size
Navigate pages (next, previous, jump to page N, first/last) → retriever:next, retriever:previous, retriever:page
Scope browsing to a Bates range → retriever:bates
Scope browsing to a processing run → retriever:from-run
Inspect, save, load, or clear a scope → retriever:scope
Ingest a folder or refresh changed files → retriever:ingest
Ingest a processed production volume → retriever:ingest-production
Ingest or inspect a PST archive → retriever:pst
Register, list, rename, delete, or re-describe a custom field, or change a field's storage type → retriever:field
Populate, tag, mark, label, classify, annotate, flag, or clear values on one document or a filtered result set → retriever:fill
Execute a planned processing run → retriever:run-job
Inspect the SQLite schema or the current tool/schema version → retriever:schema, retriever:schema-version
Initialize, check, or update a workspace → retriever:workspace
Confirm the plugin is installed and responding → retriever:ping
Understand file-type support and preview rules → retriever:parsing
Understand result presentation and paging defaults → retriever:search-strategy
Materialize or upgrade the canonical workspace tool → retriever:tool-template

If the user's intent maps to one of the rows above, stop. Use that skill. Continue to the slash list only when no `retriever:*` skill wrapper exists for the intent, when the user explicitly asks for a slash command, or when a skill's own instructions call for a slash command internally.

### Slash Commands (Tier 1 Fallback and Implementation Surface)

If no Tier 1 skill wrapper exists for the intent, use a slash command via the
canonical plugin tool as Tier 1. Run exactly one command from the repo root:

```bash
python3 skills/tool-template/tools.py slash . /<command> [args]
```

Return Retriever state only for state-inspection commands. For listing/browse commands, return the tool-rendered standard table/result exactly as produced; prefer `rendered_markdown` when present and do not add prose or custom formatting.

`/documents`, `/conversations`, and `/entities` are browse-mode toggles. Use them internally when intent is clear; do not route ordinary natural-language listing requests to those toggles before checking `retriever:search`.

The authoritative current list of slash commands is regenerated at build time into the section below.
<!-- BEGIN: slash-commands -->
- `/bates <range>` — scope browsing to a Bates range. **Use when:** the user asks to limit or scope browsing to a Bates or production-number range — phrasings like "show ABC0001 to ABC0050", "just the ABC0100 docs", "Bates range", "production numbers X to Y", or "clear the Bates range".
- `/columns [list|set|add|remove|default]` — inspect or change displayed columns. **Use when:** the user asks to show, hide, add, remove, reorder, or reset which columns appear in the result table — phrasings like "add the author column", "hide date_received", "show file size", "what columns are available", or "reset columns".
- `/conversations` — switch the browse mode to conversations. **Use when:** the user asks to list, show, or browse conversations/threads — pair with `/search`, `/filter`, `/dataset`, or other scope commands to populate results; by itself it only switches the browse mode.
- `/dataset [list|<name>[,<name>...]|clear|rename <old> <new>]` — scope to one or more datasets, list them, rename, or clear. **Use when:** the user asks to list, show, enumerate, switch, pick, select, rename, or clear datasets — phrasings like "what datasets do I have", "show me my datasets", "switch to gmail-max", "use the production dataset", or "rename X to Y".
- `/documents` — switch the browse mode to documents. **Use when:** the user asks to list, show, or browse individual documents/messages — pair with `/search`, `/filter`, `/dataset`, or other scope commands to populate results; by itself it only switches the browse mode.
- `/entities` — switch the browse mode to entities. **Use when:** the user asks to return to, page through, sort, resize, or re-display the active entity list — pair with `list-entities` to seed a query; by itself it switches to the saved entity browse state.
- `/field [list|add|rename|delete|describe|type]` — list or manage custom field definitions. **Use when:** the user asks to list, add, rename, delete, re-describe, or retype a custom field — phrasings like "add a responsiveness field", "rename privilege_status", "drop the old tag", "update the field description", or "change this field to date".
- `/fill <field> <value-or-clear> [on <doc-ref[,doc-ref,...]>] [--confirm]` — set or clear field values on documents. **Use when:** the user asks to populate, tag, mark, label, classify, annotate, flag, or clear a field value on one document or on the current filtered result set — phrasings like "mark these responsive", "fill reviewer=jdoe", "clear the review status", or "tag DOC001 as privileged".
- `/filter [<expression>|clear]` — add or clear SQL-like filters. **Use when:** the user asks to narrow, restrict, constrain, or exclude results — phrasings like "only PDFs", "show just emails from alice", "exclude attachments", "hide chats", "only 2023", or a SQL-like predicate — or asks to drop/clear current filters.
- `/from-run <run-id|clear>` — scope browsing to a processing run. **Use when:** the user asks to limit or scope browsing to documents produced by a specific processing run — phrasings like "only docs from run 42", "show what run 5 produced", "filter to the last OCR run", "just the image-description outputs", or "clear the run filter".
- `/next` — go to the next page of active results. **Use when:** the user asks for more results or the next page — phrasings like "show more", "keep going", "next batch", "next page", "continue", or "what else".
- `/page [<n>|first|last|next|previous]` — jump to a specific page. **Use when:** the user asks to jump to a specific page — phrasings like "go to page 3", "first page", "last page", "skip to the end", "back to the start", or "where am I in the results".
- `/page-size [<n>]` — inspect or change rows per page. **Use when:** the user asks to change how many rows appear per page — phrasings like "show 50 at a time", "more per page", "smaller page size", "25 rows please", or "what's my current page size".
- `/previous` — go to the previous page of active results. **Use when:** the user asks to go back to earlier results or the previous page — phrasings like "go back", "previous page", "back one page", "earlier results", or "the page before".
- `/scope [list|clear|save <name>|load <name>]` — inspect or manage the active scope. **Use when:** the user asks to inspect, save, bookmark, restore, load, or clear the current combination of dataset/filter/sort/column state — phrasings like "save this view as X", "go back to my saved scope", "what's my current scope", "list saved scopes", or "clear scope".
- `/search [<query>]` — run a keyword search. **Use when:** the user asks to show, list, view, display, browse, find, search, or retrieve documents, conversations, emails, chats, threads, messages, files, or attachments — with or without a keyword — including requests like "show me emails from alice", "list PDFs from 2023", "find docs mentioning indemnification", or "what's in gmail-max".
- `/sort [list|<field> <asc|desc>|default]` — inspect or change sort order. **Use when:** the user asks to change or reset the order of results — phrasings like "newest first", "oldest first", "sort by date", "order by file name", "alphabetical", "by size", or "reset sort".
<!-- END: slash-commands -->

## Python Runtime (applies to command surfaces)

Retriever maintains a shared plugin runtime venv under
.retriever-plugin-runtime/<platform>/venv/.
For Retriever commands, bare python3 is acceptable. The tool can activate the
shared plugin runtime for optional dependencies and may provision it during
workspace init or first dependency use.
Do not install Retriever dependencies into system Python or user-site. If
manual interpreter or pip access is needed, resolve
plugin_runtime.python_executable from .retriever/runtime.json and use that
interpreter.
If a Retriever command fails with ModuleNotFoundError, ImportError, or
Missing dependency for .<ext> parsing: install <package>, do not install into
system Python. Prefer workspace init; if manual installation is truly needed,
use the shared plugin runtime venv.

## Bounded Retriever Workflows
Cowork/bash commands may be killed around 45 seconds. Do not run long, one-shot mutation commands when a bounded/resumable workflow exists.

Use plain `ingest` as the preferred entrypoint. It is a bounded V2 facade by default.

Recommended:

```bash
python3 skills/tool-template/tools.py ingest ./data --recursive --budget-seconds 35
```

If the result has `more_work_remaining: true`, repeat the command from `next_recommended_commands` until `status` is `completed`, `failed`, or `canceled`.

Do not use background jobs. Do not manually loop inside one bash command. Run one bounded command per Cowork call.

Use legacy ingest only when explicitly requested, or when debugging parity with the old pipeline:

```bash
python3 skills/tool-template/tools.py ingest ./data --recursive --legacy
```

Do not use legacy ingest for normal Cowork reingest tasks. It is a one-shot command and may exceed the 45-second command limit on large workspaces.

Advanced/manual ingest control is available through:

```bash
python3 skills/tool-template/tools.py ingest-status ./data
python3 skills/tool-template/tools.py ingest-start ./data --recursive --budget-seconds 35
python3 skills/tool-template/tools.py ingest-run-step ./data --run-id <RUN_ID> --budget-seconds 35
python3 skills/tool-template/tools.py ingest-cancel ./data --run-id <RUN_ID>
```

Prefer the plain `ingest` facade unless you need to inspect or recover a specific run.

For large workspaces, use the resumable entity rebuild flow:

```bash
python3 skills/tool-template/tools.py rebuild-entities-start ./data --budget-seconds 35
python3 skills/tool-template/tools.py rebuild-entities-run-step ./data --run-id <RUN_ID> --budget-seconds 35
python3 skills/tool-template/tools.py rebuild-entities-status ./data --run-id <RUN_ID>
```

Repeat `rebuild-entities-run-step` until terminal status. Legacy `rebuild-entities` may exceed Cowork limits on large workspaces.

For planned processing runs, prefer `retriever:run-job` at Tier 1. If using Tier 2 directly, prefer:

```bash
python3 skills/tool-template/tools.py run-job-step . --run-id <RUN_ID> --budget-seconds 35
```

If it returns a non-empty `batch`, process those items and call `complete-run-item` or `fail-run-item`, then continue with `next_recommended_commands`.

Do not use `execute-run` for normal Cowork execution. It is the legacy direct executor and may exceed the command limit.

For any tool result with `more_work_remaining: true`, continue with the returned `next_recommended_commands`. Stop only on terminal status: `completed`, `failed`, or `canceled`.

If an active run exists, do not start a new one. Resume it or cancel it intentionally.

## Tier 2 — tools.py Subcommands

If no Tier 1 user-facing surface covers the intent, use a named subcommand of
the canonical plugin tool:

```bash
python3 skills/tool-template/tools.py <subcommand> . [flags]
```

Tier 2 is for gaps and explicit programmatic/stateless needs. Do not use Tier 2 search/list subcommands for ordinary document, conversation, email, chat, thread, message, file, or attachment listing requests when Tier 1 `retriever:search` or a Tier 1 slash surface can satisfy the user's intent.

The authoritative current list of subcommands is regenerated at build time into the section below.
<!-- BEGIN: tool-subcommands -->
### Workspace & maintenance

- you need the current schema/tool version string → `schema-version` — report the current schema/tool version
- you need to initialize a fresh workspace, diagnose runtime or install integrity, or refresh the workspace tool → `workspace` — initialize, inspect, or update workspace installation and schema

### Datasets

- the user asks to add, put, tag, include, or assign documents into an existing dataset — phrasings like "put these in X", "tag these into priority", "add these docs to the responsive set", or "include these in Y" → `add-to-dataset` — add documents to a dataset
- the user asks to create, start, or make a new dataset/collection/group — phrasings like "start a new collection called X", "make a dataset for these", "create a group called priority", or "new dataset Y" → `create-dataset` — create a manual dataset
- the user asks to delete, trash, remove, or get rid of an entire dataset — phrasings like "delete the X dataset", "trash the old collection", "get rid of the priority group", or "remove dataset Y entirely" → `delete-dataset` — delete a dataset
- you need the current dataset list with document counts (programmatic form; prefer `retriever:dataset` / `/dataset list` for user-facing intent) → `list-datasets` — list datasets in the workspace
- the user asks to remove, drop, take out, or exclude documents from an existing dataset — phrasings like "remove these from X", "drop these out of priority", "pull these from the responsive set", or "unassign from Y" → `remove-from-dataset` — remove documents from a dataset
- you need to enable, disable, or tune source-backed entity auto-merge settings for one dataset → `set-dataset-policy` — update a source-backed dataset's entity auto-merge policy
- you need to inspect source-backed entity auto-merge settings for one dataset → `show-dataset-policy` — show a source-backed dataset's entity auto-merge policy

### Ingestion

- you need the top-level folder ingest/refresh facade, which starts or resumes bounded V2 work by default → `ingest` — start or resume a bounded V2 ingest for workspace documents
- you need to cancel an active resumable V2 ingest run → `ingest-cancel` — cancel a resumable V2 ingest run
- you need to commit prepared work items for a resumable V2 ingest run → `ingest-commit-step` — commit prepared resumable V2 ingest work items
- you need to advance finalization for a resumable V2 ingest run → `ingest-finalize-step` — advance resumable V2 ingest finalization
- you need to advance only the planning phase of a resumable V2 ingest run → `ingest-plan-step` — advance resumable V2 ingest planning
- you need to prepare pending work items for a resumable V2 ingest run → `ingest-prepare-step` — prepare resumable V2 ingest work items
- you need to ingest a processed production (DAT/OPT/TEXT/IMAGES) → `ingest-production` — ingest a processed production volume
- you need to advance a resumable V2 ingest run through whichever step is currently recommended within one bounded call → `ingest-run-step` — run recommended resumable V2 ingest steps within a bounded call budget
- you need the lower-level lifecycle command to create a resumable V2 ingest run without immediately driving all recommended steps → `ingest-start` — start a resumable V2 ingest run
- you need to inspect status, phase, counts, or next recommended commands for a resumable V2 ingest run → `ingest-status` — show resumable V2 ingest status
- you are debugging PST ingestion or conversation scoping → `inspect-pst-properties` — inspect raw PST message fields for debugging

### Search & browse

- you need a programmatic search with explicit filters/sort/columns → `search` — search indexed documents
- you need citation-ready chunk hits for a query → `search-chunks` — search matching text chunks with citations
- you need a programmatic document-level search (over parents only) → `search-docs` — search indexed documents at the document level
- you need to invoke a Tier 1 slash command programmatically → `slash` — execute a scope-aware slash command (see Tier 1)

### Documents & text

- you need to switch a document's active search text to a specific revision → `activate-text-revision` — promote a stored text revision to active indexed text
- the user asks to delete, trash, remove, or purge specific documents or documents matching a filter/path/scope — phrasings like "delete these docs", "remove everything under raw/", "purge docs matching this filter", or "drop the selected documents from the index" → `delete-docs` — delete selected documents or matching occurrences
- you need full metadata, text, or chunks for one document → `get-doc` — fetch one document with optional summary text or exact chunks
- you need the chunk layout for one document → `list-chunks` — list chunk metadata for one document
- you need to see all text revisions stored for a document → `list-text-revisions` — list stored text revisions for a document

### Catalog & aggregation

- the user asks for counts, sums, distinct values, breakdowns, or groupings across filtered documents or entities — phrasings like "how many emails per sender", "count by dataset", "breakdown by content type", "group by author", "entities by type", "entities by origin", "entities by status", "entities by role", or "total size by year" → `aggregate` — run bounded metadata aggregations across documents
- the user asks what fields, columns, or attributes exist or are searchable/filterable/aggregatable — phrasings like "what fields exist", "what can I search on", "show me the columns I can filter by", or "list available attributes" → `catalog` — describe searchable, filterable, and aggregatable fields

### Entities

- the user asks to manually add an entity as a document author, participant, recipient, or custodian → `assign-entity` — manually assign an entity to a document role
- the user asks to prevent two entities from being suggested or merged together → `block-entity-merge` — prevent a suggested entity merge
- the user asks to manually create a person, mailbox, organization, or entity profile → `create-entity` — create a manual entity
- the user asks to edit an entity's display name, type, notes, email, phone, name, handle, or external id → `edit-entity` — edit a manual entity profile
- the user asks to hide, suppress, or ignore a junk/non-entity record → `ignore-entity` — ignore a junk or non-entity record
- the user asks to list, browse, show, enumerate, or search recognized people, mailboxes, organizations, or entities — phrasings like "show me the first 20 entities", "list person entities", "find entity alice@example.com", or "show ignored entities" → `list-entities` — list recognized entities
- the user asks which entities appear as authors, participants, recipients, or custodians in a document scope → `list-entity-role-inventory` — list entity counts by role for a document scope
- the user asks to merge, combine, or deduplicate two entities → `merge-entities` — merge one active entity into another
- you need to dry-run or apply index-level cleanup for synthetic Google Vault MBOX filename custodians after custodian inference fixes → `purge-vault-filename-custodians` — dry-run or apply cleanup for synthetic Google Vault MBOX filename custodians
- you need to refresh or repair entity links after metadata, source, or policy changes → `rebuild-entities` — rebuild entity recognition state from stored document metadata
- you need to cancel an active resumable entity rebuild → `rebuild-entities-cancel` — cancel a resumable entity rebuild run
- you need to advance a resumable entity rebuild within one bounded call → `rebuild-entities-run-step` — advance a resumable entity rebuild within a bounded call budget
- you need to start a bounded, resumable entity rebuild after metadata, source, or policy changes → `rebuild-entities-start` — start a resumable entity rebuild run
- you need status, counts, or next recommended commands for a resumable entity rebuild → `rebuild-entities-status` — show resumable entity rebuild status
- the user asks to inspect one entity by id, including identifiers, roles, and linked documents → `show-entity` — show one recognized entity
- the user asks for possible duplicates or merge candidates for an entity → `similar-entities` — suggest active entities similar to one entity
- the user asks to split wrongly combined entity identifiers or document links into a separate entity → `split-entity` — move selected identifiers or document links to another entity
- the user asks to remove or suppress an entity's role on a document → `unassign-entity` — remove or suppress an entity link on a document role

### Export

- only when the user asks for a tiny/debug/parity zip archive export where one direct command is acceptable → `export-archive` — write selected documents, previews, and source artifacts to a zip in one direct pass
- you need to advance a resumable archive export within one bounded call → `export-archive-run-step` — advance a resumable archive export within a bounded call budget
- the user asks to export, download, or package results as a zip archive with previews/source files and the export may exceed a single Cowork call → `export-archive-start` — start a bounded, resumable archive export run
- you need status, counts, or next recommended commands for a resumable archive export → `export-archive-status` — show resumable archive export status
- only when the user asks for a tiny/debug/parity CSV export where one direct command is acceptable → `export-csv` — write selected documents and fields to CSV in one direct pass
- you need to advance a resumable CSV export within one bounded call → `export-csv-run-step` — advance a resumable CSV export within a bounded call budget
- the user asks to export, download, or save results as CSV/spreadsheet and the export may exceed a single Cowork call → `export-csv-start` — start a bounded, resumable CSV export run
- you need status, counts, or next recommended commands for a resumable CSV export → `export-csv-status` — show resumable CSV export status
- the user asks to export or save HTML previews of selected documents — phrasings like "export the previews", "save rendered HTML", or "give me browsable preview files" → `export-previews` — write HTML preview exports under `.retriever/exports`

### Custom fields

- you need to register a new custom field definition → `add-field` — register a custom field
- you need to change a custom field's storage type → `change-field-type` — change a field's storage type
- you need to delete an existing custom field definition → `delete-field` — delete a custom field
- you need to set or clear a custom field description → `describe-field` — set or clear a custom field description
- you need to write or clear a field value on one or more documents → `fill-field` — set or clear a field value on one or more documents
- you need the registered custom-field inventory → `list-fields` — list registered custom fields
- you need to rename an existing custom field definition → `rename-field` — rename a custom field

### Conversations

- you need to drop a document's conversation assignment → `clear-conversation-assignment` — clear a document's conversation assignment
- the user asks to list, browse, page through, sort, or inspect conversation/thread summaries through a stateless Tier 2 command — phrasings like "show conversations 51-100", "list threads sorted by last activity", or "page conversation summaries" → `list-conversations` — list conversation summaries
- the user asks to merge, join, link, or attach a document into a specific conversation/thread — phrasings like "join these emails into one thread", "merge this into thread X", "link this message to conversation Y", or "group these as one conversation" → `merge-into-conversation` — merge a document into a conversation
- you need to re-run conversation assignment and regenerate previews after ingest or metadata changes → `rebuild-conversations` — re-run conversation assignment and regenerate conversation previews
- you need to resolve detected duplicates → `reconcile-duplicates` — reconcile detected duplicates
- you need to rebuild conversation preview HTML → `refresh-conversation-previews` — rebuild conversation preview artifacts
- you need to regenerate generated document and/or conversation previews with --scope documents, --scope conversations, or --scope all; selectors must stay narrow, --missing-only covers missing rows/files, and document scope includes conversation-owned production documents → `refresh-previews` — regenerate generated document and conversation preview artifacts with bounded selectors
- the user asks to split, detach, separate, or remove a document from its conversation/thread — phrasings like "split this email off its thread", "detach this message", "separate this from the conversation", or "remove from thread" → `split-from-conversation` — split a document out of a conversation

### Runs — planning & lifecycle

- you need to stop a run from claiming further work → `cancel-run` — stop claiming new work for a run
- you need to plan a new processing run → `create-run` — create a frozen processing run snapshot
- you explicitly need the legacy direct executor for debugging, deterministic tests, or parity checks → `execute-run` — execute one planned processing run via the legacy direct executor
- you need to finalize an image-description run's outputs → `finalize-image-description-run` — finalize an image-description run
- you need to finalize an OCR run's outputs → `finalize-ocr-run` — finalize an OCR run
- you need the snapshot of one planned run → `get-run` — fetch one planned processing run
- you need the list of planned/active processing runs → `list-runs` — list planned processing runs
- you need to publish completed-run results → `publish-run-results` — publish results from a completed run
- you need progress, claim health, next recommended commands, or recent failures for a run → `run-status` — summarize run progress, claims, and recent failures

### Runs — worker execution

- you are using the low-level worker protocol to claim pending items without contexts → `claim-run-items` — atomically claim pending run items for one worker
- you are a run worker marking an item completed → `complete-run-item` — mark one claimed run item completed
- you are a run worker marking an item failed → `fail-run-item` — mark one claimed run item failed
- you are a run worker finalizing its session → `finish-run-worker` — mark one worker as finished and persist its summary
- you are a run worker loading context for one item → `get-run-item-context` — load the execution context for one run item
- you are a run worker refreshing its heartbeats → `heartbeat-run-items` — refresh heartbeat timestamps for one worker's claimed items
- you are using the low-level worker protocol to claim one bounded batch of work → `prepare-run-batch` — claim one worker batch and return execution contexts
- you need to advance, resume, or execute a planned processing run under the Cowork 45-second command limit → `run-job-step` — advance one Cowork-safe processing-run step or return one prepared worker batch

### Jobs

- you need to attach an output artifact to a job → `add-job-output` — attach an output to a job
- you need to register a new job → `create-job` — create a job
- you need to cut a new version of a job → `create-job-version` — create a job version
- you need to see a job's versions → `list-job-versions` — list job versions
- you need the list of registered jobs → `list-jobs` — list jobs

### Results

- you need stored processing results for inspection → `list-results` — list stored processing results

<!-- END: tool-subcommands -->

## Tier 3 — Direct SQLite Access (Last Resort Only)

Allowed only when Tiers 1–2 cannot satisfy the request. Before running any sqlite3 CLI, python3 -c "import sqlite3 …", or equivalent client against .retriever/retriever.db, Claude must:

State explicitly that no higher-tier surface covers the request, and name the gap (for example, "no slash or subcommand returns conversation-level participant counts").
Read-only queries only, unless the user has explicitly asked for a mutation that cannot be expressed via Tier 2.
Include a "plugin gap" line at the end of the response identifying the missing command, so the gap can be closed on a later iteration.

Never modify .retriever/retriever.db with direct SQL when a Tier 2 subcommand or Tier 1 skill/slash surface could achieve the same change.

## Pre-flight Algorithm

Every time Claude is about to act on a Retriever workspace:

1. Identify the user's intent in one sentence.
2. Scan the Tier 1 skill table above for a matching `retriever:*` skill. If one matches, stop and use it.
3. Otherwise, scan the Tier 1 slash list for a matching command. If one matches, run it via slash ..
4. Otherwise, scan the Tier 2 subcommand list. If one matches, run it.
5. Otherwise, fall to Tier 3 under the constraints above and emit the "plugin gap" note.

This ladder applies to every Retriever request, including requests phrased in natural language. For example, "show me conversations in gmail-max" is Tier 1 `retriever:search`; that skill may use dataset scope and conversation browse surfaces internally, but Claude must not bypass Tier 1 or jump to Tier 3 SQL.

For any show/list/view/display/browse/find/search/retrieve request, the final answer must follow the Retriever Result Presentation Contract above.
