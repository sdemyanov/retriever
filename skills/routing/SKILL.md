---
name: routing
description: Authoritative routing rules for Retriever. Load and obey this skill whenever Claude operates inside a Retriever workspace — any time the user asks Claude to explore, inspect, search, filter, list, browse, answer questions about, summarize, ingest, export, or modify data such as datasets, conversations, emails, chats, documents, runs, jobs, or fields; or whenever Claude is about to invoke `retriever_tools.py`, call its `slash` subcommand, or touch `.retriever/retriever.db`. Enforces the four-tier priority ladder so Claude prefers user-facing skills over slashes, slashes over CLI subcommands, and CLI subcommands over direct database access.
---

# Retriever routing — priority ladder

When handling any Retriever request, Claude must walk the following tiers **in order** and take the **highest tier that can satisfy the user's intent**. Do not skip tiers. Do not drop to a lower tier because it is more familiar or convenient.

Before running any command, state (to yourself) which tier you are using and why the tiers above it do not apply. If you end up in Tier 3 or Tier 4, add a short "plugin gap" note to the end of your turn so the user can see which request is not yet covered by a higher-tier surface.

## Tier 1 — user-facing `retriever:*` skills (preferred)

If a `retriever:*` skill covers the intent, invoke it instead of the underlying command. Intent-to-skill routing:

- List, switch, rename, or clear dataset scope → `retriever:dataset`
- Search or browse documents, filter the collection, answer "find / show me / list emails or docs matching …" → `retriever:search`
- Change displayed columns → `retriever:columns`
- Change sort → `retriever:sort`
- Change page size → `retriever:page-size`
- Navigate pages (next, previous, jump to page N, first/last) → `retriever:next`, `retriever:previous`, `retriever:page`
- Scope browsing to a Bates range → `retriever:bates`
- Scope browsing to a processing run → `retriever:from-run`
- Inspect, save, load, or clear a scope → `retriever:scope`
- Ingest a folder or refresh changed files → `retriever:ingest`
- Ingest a processed production volume → `retriever:ingest-production`
- Ingest or inspect a PST archive → `retriever:pst`
- Register, list, rename, delete, or re-describe a custom field, or change a field's storage type → `retriever:field`
- Populate, tag, mark, label, classify, annotate, flag, or clear values on one document or a filtered result set → `retriever:fill`
- Execute a planned processing run → `retriever:run-job`
- Inspect the SQLite schema or the current tool/schema version → `retriever:schema`, `retriever:schema-version`
- Bootstrap, check, or upgrade a workspace → `retriever:workspace`, `retriever:doctor`
- Confirm the plugin is installed and responding → `retriever:ping`
- Understand file-type support and preview rules → `retriever:parsing`
- Understand result presentation and paging defaults → `retriever:search-strategy`
- Materialize or upgrade the canonical workspace tool → `retriever:tool-template`

If the user's intent maps to one of the rows above, stop. Use that skill.

## Tier 2 — `retriever_tools.py slash` commands

If no Tier 1 skill exists for the intent, use a slash command via the workspace tool. Run exactly one command from the workspace root:

```
python3 .retriever/bin/retriever_tools.py slash . /<command> [args]
```

Return the resulting Retriever state or table. The authoritative current list of slash commands is regenerated at build time into the section below.

<!-- BEGIN: slash-commands -->
- `/bates <range>` — scope browsing to a Bates range. **Use when:** the user asks to limit or scope browsing to a Bates or production-number range — phrasings like "show ABC0001 to ABC0050", "just the ABC0100 docs", "Bates range", "production numbers X to Y", or "clear the Bates range".
- `/columns [list|set|add|remove|default]` — inspect or change displayed columns. **Use when:** the user asks to show, hide, add, remove, reorder, or reset which columns appear in the result table — phrasings like "add the author column", "hide date_received", "show file size", "what columns are available", or "reset columns".
- `/conversations` — switch the browse mode to conversations. **Use when:** the user asks to list, show, or browse conversations/threads — pair with `/search`, `/filter`, `/dataset`, or other scope commands to populate results; by itself it only switches the browse mode.
- `/dataset [list|<name>[,<name>...]|clear|rename <old> <new>]` — scope to one or more datasets, list them, rename, or clear. **Use when:** the user asks to list, show, enumerate, switch, pick, select, rename, or clear datasets — phrasings like "what datasets do I have", "show me my datasets", "switch to gmail-max", "use the production dataset", or "rename X to Y".
- `/documents` — switch the browse mode to documents. **Use when:** the user asks to list, show, or browse individual documents/messages — pair with `/search`, `/filter`, `/dataset`, or other scope commands to populate results; by itself it only switches the browse mode.
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

## Tier 3 — `retriever_tools.py` subcommands

If no slash form covers the intent, use a named subcommand of the workspace tool:

```
python3 .retriever/bin/retriever_tools.py <subcommand> . [flags]
```

The authoritative current list of subcommands is regenerated at build time into the section below.

<!-- BEGIN: tool-subcommands -->
### Workspace & maintenance

- you need to initialize a fresh workspace → `bootstrap` — create workspace layout and schema
- you need to diagnose runtime, workspace readiness, or install integrity → `doctor` — check runtime and workspace readiness
- you need the current schema/tool version string → `schema-version` — report the current schema/tool version
- the schema version is behind the bundled tool → `upgrade-workspace` — upgrade the workspace schema

### Datasets

- the user asks to add, put, tag, include, or assign documents into an existing dataset — phrasings like "put these in X", "tag these into priority", "add these docs to the responsive set", or "include these in Y" → `add-to-dataset` — add documents to a dataset
- the user asks to create, start, or make a new dataset/collection/group — phrasings like "start a new collection called X", "make a dataset for these", "create a group called priority", or "new dataset Y" → `create-dataset` — create a manual dataset
- the user asks to delete, trash, remove, or get rid of an entire dataset — phrasings like "delete the X dataset", "trash the old collection", "get rid of the priority group", or "remove dataset Y entirely" → `delete-dataset` — delete a dataset
- you need the current dataset list with document counts (programmatic form; prefer `retriever:dataset` / `/dataset list` for user-facing intent) → `list-datasets` — list datasets in the workspace
- the user asks to remove, drop, take out, or exclude documents from an existing dataset — phrasings like "remove these from X", "drop these out of priority", "pull these from the responsive set", or "unassign from Y" → `remove-from-dataset` — remove documents from a dataset

### Ingestion

- you need to index or refresh a folder → `ingest` — index documents in the workspace
- you need to ingest a processed production (DAT/OPT/TEXT/IMAGES) → `ingest-production` — ingest a processed production volume
- you are debugging PST ingestion or conversation scoping → `inspect-pst-properties` — inspect raw PST message fields for debugging

### Search & browse

- you need a programmatic search with explicit filters/sort/columns → `search` — search indexed documents
- you need citation-ready chunk hits for a query → `search-chunks` — search matching text chunks with citations
- you need a programmatic document-level search (over parents only) → `search-docs` — search indexed documents at the document level
- you need to invoke a Tier 2 slash programmatically → `slash` — execute a scope-aware slash command (see Tier 2)

### Documents & text

- you need to switch a document's active search text to a specific revision → `activate-text-revision` — promote a stored text revision to active indexed text
- you need full metadata, text, or chunks for one document → `get-doc` — fetch one document with optional summary text or exact chunks
- you need the chunk layout for one document → `list-chunks` — list chunk metadata for one document
- you need to see all text revisions stored for a document → `list-text-revisions` — list stored text revisions for a document

### Catalog & aggregation

- the user asks for counts, sums, distinct values, breakdowns, or groupings across filtered documents — phrasings like "how many emails per sender", "count by dataset", "breakdown by content type", "group by author", or "total size by year" → `aggregate` — run bounded metadata aggregations across documents
- the user asks what fields, columns, or attributes exist or are searchable/filterable/aggregatable — phrasings like "what fields exist", "what can I search on", "show me the columns I can filter by", or "list available attributes" → `catalog` — describe searchable, filterable, and aggregatable fields

### Export

- the user asks to export, download, or package results as a zip archive with previews and source files — phrasings like "zip up the matches", "download everything", "package these docs", or "give me a bundle of these" → `export-archive` — write selected documents, previews, and source artifacts to a zip
- the user asks to export, download, or save results as a CSV or spreadsheet — phrasings like "export these as CSV", "save to Excel", "download as spreadsheet", or "give me a CSV of the matches" → `export-csv` — write selected documents and fields to CSV
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
- the user asks to merge, join, link, or attach a document into a specific conversation/thread — phrasings like "join these emails into one thread", "merge this into thread X", "link this message to conversation Y", or "group these as one conversation" → `merge-into-conversation` — merge a document into a conversation
- you need to resolve detected duplicates → `reconcile-duplicates` — reconcile detected duplicates
- you need to rebuild conversation preview HTML → `refresh-conversation-previews` — rebuild conversation preview artifacts
- the user asks to split, detach, separate, or remove a document from its conversation/thread — phrasings like "split this email off its thread", "detach this message", "separate this from the conversation", or "remove from thread" → `split-from-conversation` — split a document out of a conversation

### Runs — planning & lifecycle

- you need to stop a run from claiming further work → `cancel-run` — stop claiming new work for a run
- you need to plan a new processing run → `create-run` — create a frozen processing run snapshot
- you need to execute a planned run inline via the legacy executor → `execute-run` — execute one planned processing run via the legacy direct executor
- you need to finalize an image-description run's outputs → `finalize-image-description-run` — finalize an image-description run
- you need to finalize an OCR run's outputs → `finalize-ocr-run` — finalize an OCR run
- you need the snapshot of one planned run → `get-run` — fetch one planned processing run
- you need the list of planned/active processing runs → `list-runs` — list planned processing runs
- you need to publish completed-run results → `publish-run-results` — publish results from a completed run
- you need progress, claims, and recent failures for a run → `run-status` — summarize run progress, claims, and recent failures

### Runs — worker execution

- you are a run worker claiming pending items → `claim-run-items` — atomically claim pending run items for one worker
- you are a run worker marking an item completed → `complete-run-item` — mark one claimed run item completed
- you are a run worker marking an item failed → `fail-run-item` — mark one claimed run item failed
- you are a run worker finalizing its session → `finish-run-worker` — mark one worker as finished and persist its summary
- you are a run worker loading context for one item → `get-run-item-context` — load the execution context for one run item
- you are a run worker refreshing its heartbeats → `heartbeat-run-items` — refresh heartbeat timestamps for one worker's claimed items
- you are a run worker preparing one batch of work → `prepare-run-batch` — claim one worker batch and return execution contexts

### Jobs

- you need to attach an output artifact to a job → `add-job-output` — attach an output to a job
- you need to register a new job → `create-job` — create a job
- you need to cut a new version of a job → `create-job-version` — create a job version
- you need to see a job's versions → `list-job-versions` — list job versions
- you need the list of registered jobs → `list-jobs` — list jobs

### Results

- you need stored processing results for inspection → `list-results` — list stored processing results










<!-- END: tool-subcommands -->

## Tier 4 — direct SQLite access (last resort only)

Allowed **only** when Tiers 1–3 cannot satisfy the request. Before running any `sqlite3` CLI, `python3 -c "import sqlite3 …"`, or equivalent client against `.retriever/retriever.db`, Claude must:

1. State explicitly that no higher-tier surface covers the request, and name the gap (for example, "no slash or subcommand returns conversation-level participant counts").
2. Read-only queries only, unless the user has explicitly asked for a mutation that cannot be expressed via Tier 3.
3. Include a "plugin gap" line at the end of the response identifying the missing command, so the gap can be closed on a later iteration.

Never modify `.retriever/retriever.db` with direct SQL when a Tier 3 subcommand or Tier 2 slash could achieve the same change.

## Pre-flight algorithm

Every time Claude is about to act on a Retriever workspace:

1. Identify the user's intent in one sentence.
2. Scan the Tier 1 table above for a matching `retriever:*` skill. If one matches, stop and use it.
3. Otherwise, scan the Tier 2 slash list for a matching command. If one matches, run it via `slash .`.
4. Otherwise, scan the Tier 3 subcommand list. If one matches, run it.
5. Otherwise, fall to Tier 4 under the constraints above and emit the "plugin gap" note.

This ladder applies to every Retriever request, including requests phrased in natural language (for example, "show me conversations in gmail-max" is a Tier 2 `/conversations` request under dataset scope `gmail-max`, not a Tier 4 SQL query).
