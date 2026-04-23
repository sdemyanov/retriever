# Retriever

You have a folder full of documents. Maybe it's a new matter that just landed on your desk. Maybe it's a data room your team has to get through by Friday. Maybe it's the contents of someone's laptop, a PST from opposing counsel, or a Slack export somebody handed you with no context. You need to understand what's in there, find the things that matter, and not miss anything important.

Retriever is built for that situation.

It's a local-first plugin for Claude Cowork that turns the folder you select into a searchable, filterable collection. Your files stay exactly where they are — Retriever never moves or rewrites them — but now you can ask questions about them, narrow by metadata, jump to a Bates number, save your progress, and export what you need.

The rest of this document has two halves. The first half is for humans trying to figure out whether Retriever fits their work. The second half is the reference: every command, every filter operator, every CLI flag. Skim what's useful. Come back to the rest when you need it.

## Who this is for

Retriever is most useful if you are:

- A lawyer, paralegal, or investigator sifting through productions, custodian collections, or discovery responses.
- A due-diligence team trying to understand what's in a data room without reading every file end to end.
- An internal investigator or compliance reviewer handling a mix of emails, chats, and office documents.
- Anyone who has been handed a big folder and told "figure out what's in here."

If you have ever opened a folder, stared at thousands of files, and thought "where do I even start" — Retriever is designed to be that start.

## What it takes off your plate

A few specific frustrations it's built to address.

**Mixed file types, one place to look.** PDFs, Word documents, Excel, PowerPoint, loose emails, PST and MBOX archives, Slack exports, and processed productions all share the same browse surface. You do not have to bounce between three different programs to follow a single thread of evidence.

**A document view and a conversation view of the same collection.** Ask for "documents" or "emails" and Retriever shows you one row per file or message. Ask for "threads," "conversations," "channels," or "DMs" and the same scope reshapes into one row per discussion — with participants, date range, and a message count — so you can see the shape of a thread before opening any one message. Filters, datasets, and saved scopes apply the same way in either view.

**A big pile narrowed down to what actually matters.** Start broad with a keyword, then filter by date range, file type, custodian, or any custom field you have added. Each filter is additive. You watch the pile shrink until you have something reviewable.

**Your place, saved.** Review work rarely happens in one sitting. Save a scope today, come back tomorrow, and pick up exactly where you left off. Build named review sets as you go. Re-run the same scope a month later when the next batch of documents lands.

**Bates numbers that just work.** If someone in a meeting says "look at ABC000412," you don't have to figure out which load file it lives in or which volume it came from. Type it and you're there. Ranges work the same way.

**Your own fields, preserved.** Mark documents as privileged, flag them for a partner, tag them by issue. Your edits are locked against being overwritten the next time new documents get ingested.

**Clean outputs when you're done.** CSV for downstream review, HTML preview bundles for sharing, or zip archives that bundle source files, previews, and a portable workspace subset for handoff.

**Nothing leaves the folder.** The database, the previews, the logs — all of it sits under a hidden `.retriever/` folder inside the workspace you picked. Your originals are untouched. When you switch to a different matter, you pick a different folder, and nothing bleeds across.

**Extendable through jobs.** The default ingest is deliberately fast and deterministic — it pulls out text and metadata, and that's it. Processing jobs are where you layer in the expensive or AI-assisted work on demand. Run OCR against scanned PDFs so they become full-text searchable. Use a vision model to generate short descriptions for images so they turn up in keyword searches. Extract structured values across thousands of documents with a single prompt. Each job writes its output back to the document's indexed text or to custom fields, so the results live alongside everything else and filter, search, and export the same way.

## Limitations

Worth being upfront about before you commit to a workflow:

- **No semantic search.** Keyword, Bates, and metadata filters are the search surfaces; there's no vector or LLM-reranked ranking in the default path.
- **No production authoring.** Retriever ingests processed productions (Concordance-style `DAT`/`OPT` with `TEXT/`, `IMAGES/`, and optional `NATIVES/`) but does not create them — no Bates-burn, no redaction workflow, no load-file output, no production-export pipeline. Use a dedicated production tool when you need to produce, and bring the finished production back into Retriever for review.
- **Single machine.** No shared service, no cloud sync, no concurrent access from multiple users on the same workspace. Sharing happens through exports and portable workspace archives, not through a live shared store.

A few behaviors that sometimes get mistaken for limitations are actually deliberate design choices:

- **Archives aren't expanded during ingest.** `.zip`, `.rar`, `.7z`, and similar are left as-is. Unpacking them silently would hide surprising volume, nest in unpredictable ways, and change what a single ingest actually touches. If you want archive contents indexed, unpack the archive yourself into the workspace and re-ingest — Retriever will pick up the new files normally.
- **Default ingest doesn't do OCR or AI enrichment.** That work happens through processing jobs instead (see "Extendable through jobs" above). Keeping the default path minimal means ingest is fast, deterministic, and easy to reason about; the expensive or probabilistic work is opt-in per document set.

If any of the real limitations above are dealbreakers, better to know now.

## Key concepts

Retriever uses a small vocabulary that shows up in every section that follows. Each entry below gives the definition plus a short note on how the concept actually works in practice.

**Workspace.** The folder you point Retriever at. Everything Retriever knows about your collection — the database, previews, logs, job state, runtime metadata — lives in a hidden `.retriever/` subdirectory inside that folder. Switching to a different workspace means switching to a different folder, and each folder carries its own separate state. Your originals are never moved or rewritten.

**Ingest.** The process of scanning the workspace, extracting text and metadata from each supported file, and registering one or more documents per file. Re-ingesting the same folder updates changed files, preserves identity where it can, and marks missing items rather than silently forgetting them.

**Document (logical document).** The unit Retriever indexes. Usually one per file, but not always: an email with attachments becomes an email document plus one child per attachment, a PST file explodes into a message document per message plus attachment children, a Slack export becomes conversation-day documents with reply children, and a processed production creates one document per load-file row. When the same file shows up in more than one place — for example, the same PDF appears in two custodian collections — Retriever treats those as separate occurrences of a single logical document. The document's metadata (title, dates, author, custodians) is the union across active occurrences, and custodian in particular is kept as a list so a document can legitimately belong to multiple custodians.

**Control number.** The stable identifier assigned to each document. It survives re-ingest, it's what exports use to reference a row, and for production documents it is the produced Bates value.

**Production.** A processed eDiscovery vendor deliverable — typically a Concordance-style `DAT` file paired with `OPT`, `TEXT/`, `IMAGES/`, and sometimes `NATIVES/`. Productions ingest differently from loose files; use `ingest-production` when you want to target one explicitly.

**Bates number, Bates range.** The produced identifier (or range of identifiers) for a document in a production. Retriever treats Bates input as a first-class lookup mode, so a Bates token or range is something you can jump straight to. See the `/search` and `/filter` syntax sections below for the exact behavior.

**Scope.** The combined selector for what documents are currently "in play": keyword, Bates, filter, dataset membership, and a `from-run` reference. Everything you browse, sort, export, or run a job against is evaluated against the current scope. Scopes can be saved by name and reloaded later so you can pick up exactly where you left off.

**Search.** The keyword or Bates slot inside a scope. The keyword side can hold a full boolean query, not just a single word. The Bates side accepts a token or a range. Retriever auto-detects which kind you gave it. See the `/search` syntax section below for the exact rules.

**Filter.** SQL-like metadata constraint added to a scope. You can filter on built-in fields (content type, dates, custodian, file type, and so on), on any custom field you've added, and on a handful of virtual fields like dataset membership. See the `/filter` syntax section below for operators and the full field list.

**Dataset.** A named collection of documents. Useful for review sets, source-backed groupings (a production, a custodian's collection), and repeatable exports. A document can belong to more than one dataset.

**Columns.** The fields shown for each row in the results table. Change them to reshape what you see without changing which documents are returned. Different column layouts for privilege review, date triage, and production QC are a common pattern.

**Sort, page, page size.** Display-level controls. Sort changes ordering. Page moves you through the current scope. Page size changes how many rows per page. Scope changes reset paging; display preferences persist until you change them.

**Preview / Preview panel.** The document viewer that opens when you click a result's title. Retriever uses the native renderer where possible and generates HTML or CSV previews where it needs to. The preview panel sits alongside the conversation so you can confirm a hit and move on.

**Custom field.** A user-defined metadata field added with `add-field` or `/field add`. You can set values on individual documents by hand with `fill-field --doc-id ...` or `/fill ... on ...`, or populate them in bulk through `/fill`, `fill-field`, or a processing job. Manual edits are locked against being overwritten by later automated passes unless you explicitly override them.

**Job.** A structured processing operation applied to a set of documents. Jobs are how you extend Retriever beyond its default ingest behavior — examples include extracting an "issue tag" per document, OCRing scanned PDFs and writing the result back as the document's indexed text, or generating short descriptions of images so they become searchable.

**Job version.** A specific configuration of a job: which provider, which model, what input the job should read, what instruction to follow. A single job can have multiple versions as you iterate on the prompt or the model.

**Run.** A job version applied to a specific frozen set of documents. Creating a run locks the current scope (or an explicit selector) into an immutable input, then executes the job version against it. `/from-run <id>` lets you scope later browsing or processing to whatever a prior run produced.

**Skill.** A prewritten recipe Claude can follow for a specific Retriever operation. Skills are what make natural-language requests map reliably to the right command with the right arguments — once Claude matches your request to a skill, the skill spells out exactly how to execute.

**Slash command.** A deterministic control like `/search`, `/filter`, `/bates`, `/columns`. No reasoning required — you type it, it executes exactly as written.

**Browse mode (documents vs conversations).** A session-level toggle between the per-document view and the per-conversation view. Documents mode shows one row per file, email, message, or attachment. Conversations mode groups those documents back into their parent thread (an email chain, a Slack channel-day, a chat thread) and shows one row per conversation. Each mode carries its own default columns, default sort, and page size, and your current scope (keyword, filter, dataset, Bates, from-run) applies in both. Natural-language verbs like "show threads," "list email chains," "browse channels" switch to conversation mode; "show documents," "list emails," "files" switch back to documents mode.

**Export.** Pulling documents out of a scope into a shareable form. Retriever supports CSV (for downstream review or QC), HTML preview bundles (for sharing previews outside the workspace), and zip archives that can include source files, previews, and an optional portable workspace subset.

**Portable workspace.** A self-contained zip that bundles a subset of the documents plus the Retriever state needed to open it elsewhere as its own standalone workspace. The intended handoff primitive when you need to give someone else a scope of work they can open and browse locally.

## How you actually use Retriever

Retriever lives inside Claude Cowork. That means the way you drive it looks more like a conversation than a piece of review software.

You tell Retriever what you want in plain English. Point it at a folder and say "index this." Ask "show me the emails from Garcia in the last quarter that mention indemnification." Say "add a privilege field and mark this document as privileged." Say "export the current selection to CSV with control number, title, and custodian." Retriever turns those requests into the right ingest, search, field, or export operations on the local database.

When you want to be precise, you use slash commands. `/search`, `/filter`, `/sort`, `/columns`, `/dataset`, `/scope`, `/page` — these are short, deterministic controls for narrowing a result set and shaping how it displays. You can mix them freely with natural language: ask Claude to narrow the view in words, then pin the exact filter with a slash command, then ask Claude again to "export whatever I'm looking at right now."

Results render inline as a table. Each row's title is a link that opens the document in the preview panel alongside the conversation, so you can skim a hit, confirm it, and move to the next one without leaving the workflow. Native files preview natively where possible; Retriever generates HTML or CSV previews for types that need them.

Skills are the other half of the form factor. A skill is a prewritten recipe Claude can follow for a specific Retriever operation — `workspace`, `ingest`, `search`, `bates`, `filter`, `scope`, `page`, `page-size`, `columns`, `sort`, `field`, `fill`, `ingest-production`, `pst`, `run-job`, and others.

The division of labor is worth understanding:

- **Slash commands are deterministic.** `/search contract`, `/bates ABC000412`, `/filter content_type = 'Email'`, `/columns set title, control_number` — these run without any reasoning. You type them, they execute. Use them when you already know exactly what you want.
- **Natural-language requests still require reasoning.** Phrases like "run retriever workspace status," "ingest this production," "add a field called issue_tag," or "jump to Bates ABC000412" go through Claude's interpretation — figuring out which operation you mean, which arguments to supply, whether to use `ingest` or `ingest-production`, what type the new field should be.
- **Skills anchor that reasoning.** Once Claude identifies that a request matches a skill, the skill itself spells out how to execute — which CLI command, which arguments, what the output should look like, how to handle edge cases. That's what makes the reasoned path predictable rather than improvised.

The net effect: you get the flexibility of plain English and the reliability of a prewritten recipe. If you want to skip the reasoning step entirely, use the slash commands directly.

The things you can drive this way, end to end:

- Which folder to ingest, recursively or not, and whether it's a loose-file collection or a processed production.
- Which documents to surface — the scope: keyword, Bates, filter, dataset membership.
- What to show about each document — the visible columns, with different layouts for privilege review, date triage, or production QC.
- How to order and page through results — sort, page, page size.
- What custom fields to add and how to populate them — manually on individual documents, or in bulk through a structured processing run.
- What to export — CSV for review teams, HTML preview bundles, or portable zip archives — and what scope to export from.

The shape is consistent across all of it: you describe the outcome you want, Retriever figures out the underlying operation, and the result shows up right in the conversation with clickable previews.

## Installing Retriever

Retriever installs as a Claude Cowork plugin.

### Normal install

1. Download `retriever.plugin` from the [Releases](../../releases) page.
2. Open Claude Cowork.
3. Customize → Add plugin → Personal → Upload, then select the downloaded `retriever.plugin` file.
4. Restart Claude so the plugin loads.
5. Verify it's alive:

   ```text
   /retriever:ping
   ```

   You should see something like:

   ```text
   Retriever plugin smoke test OK.
   ```

6. Check the runtime before your first real ingest:

   ```text
   /retriever:workspace status
   ```

   `workspace status` confirms Python and SQLite are available, FTS5 is supported, and the environment is ready for ingest, PST handling, and the rest of the pipeline.

### Local load for development or evaluation

If you have the plugin source on disk and want to try it without installing globally, load it for a single session:

```bash
claude --plugin-dir /path/to/retriever-plugin
```

Then run `/retriever:ping` inside that session to confirm it's loaded.

### Local marketplace install

If you want to test the full install path end to end from a local copy:

1. Start `claude` from any directory.
2. Add the bundled test marketplace:

   ```text
   /plugin marketplace add /path/to/retriever-plugin/test-marketplace
   ```

3. Install the plugin:

   ```text
   /plugin install retriever@retriever-local-test-marketplace
   ```

4. Restart Claude.
5. Run `/retriever:ping` to confirm, then `/retriever:workspace status` to check the runtime.

For the full set of install scenarios and validation steps, see [`SMOKE_TEST.md`](./SMOKE_TEST.md).

## A first session, start to finish

If you are trying Retriever for the first time, this is a sensible path:

1. Point Retriever at the folder and ask Retriever to initialize it (`retriever workspace init`) or inspect it (`retriever workspace status`).
2. Ask it to index the workspace.
3. Run your first keyword: `/search <term>`.
4. Narrow it down: `/filter content_type = 'Email'`.
5. Widen the view: `/columns add dataset_name`.
6. Save your progress: `/scope save first-pass`.
7. Export the result if you need to hand it off.

That sequence exercises setup, browse, narrowing, display, persistence, and export — the surfaces most reviewers end up leaning on.

## FAQ

**Is my data secure?**

Retriever is local-first. The database, previews, logs, job state, and runtime metadata all live inside a hidden `.retriever/` folder at the root of the workspace you pick. Your original documents are not copied out of that folder, rewritten, or sent anywhere by Retriever itself. When you switch to a different matter, you pick a different folder and it carries its own separate state.

The one nuance worth naming: the natural-language layer runs through Claude. When you ask Claude to search, tag, or summarize something, the content of the prompts and any text excerpts included in them are handled by the Claude Cowork app the same way any other Cowork conversation is. If you have data that cannot leave your machine under any circumstance, review your Cowork privacy and connector settings against your own policy before ingesting it.

**Can my team share a workspace?**

Not directly, and on purpose. Retriever is designed for one reviewer, one workspace, one folder at a time. There is no built-in multi-user sync, no shared server, no concurrent-edit coordination.

What you can do is share outputs. Export a CSV for a review team. Build an HTML preview bundle and send it over. Package a scope as a portable zip archive — optionally bundled with a portable workspace subset — and hand it to a colleague who can open it as their own local Retriever workspace. The archive format is the intended handoff primitive.

**Does Retriever need an internet connection?**

Ingest, indexing, filtering, and export are local and do not require internet. The Claude Cowork app itself needs a network connection for the conversation layer, so if you want to drive Retriever through natural language you need to be online. The underlying database and CLI keep working offline.

**What happens when I close Cowork and come back?**

Your database, custom fields, datasets, saved scopes, and column/sort preferences are all stored in `.retriever/` inside the workspace folder. Next session, open the same folder and everything is where you left it. The persistent browse session (current scope, current page) is preserved the same way.

**Can I move or back up a Retriever workspace?**

Yes. Document paths are workspace-relative, so moving or renaming the workspace folder is safe. Backing up the workspace means backing up the folder — `.retriever/` and all — as one unit. Retriever also keeps its own timestamped database backups under `.retriever/bin/backups/`.

**What if I re-ingest after files change?**

Re-ingest updates changed files in place, preserves stable document identity where it can, and marks missing items instead of silently forgetting them. Manual field edits you made are preserved.

**How big a collection can it handle?**

Retriever is built on SQLite with FTS5, which comfortably handles collections into the hundreds of thousands of documents on ordinary hardware. Very large productions (millions of records) are not the target workload, and you should expect ingest and browse performance to degrade as you approach that scale.

**Who is Retriever not for?**

If you need enterprise eDiscovery with hosted review, concurrent reviewer assignment, redaction workflow, production export with branded Bates burn-in, or predictive coding — Retriever is not a replacement for those systems. It is a local, flexible, conversational review surface, not a full EDD platform.

---

The rest of this document is reference material.

## Core capabilities (the technical list)

- **Local-first storage.** Retriever keeps its database, previews, logs, job state, and runtime metadata under `.retriever/` in the workspace root.
- **Broad ingest support.** It indexes PDF, DOCX, TXT/Markdown, CSV, JSON, HTML, ICS, RTF, XLS/XLSX, PPTX, EML, MSG, PST, MBOX, Slack exports, and processed productions.
- **Search and browse.** Keyword search, metadata filtering, dataset browse, Bates jump, pagination, and persistent scope/display preferences between commands.
- **Preview-first review.** Results render as a standard table with clickable titles. Native previews when possible; generated HTML or CSV previews when not.
- **Stable document identity.** Documents receive stable `control_number` values. Production documents use produced Bates values as the control number.
- **Dataset-aware workflows.** Documents can belong to one or more datasets; datasets can be source-backed or manually curated.
- **Exports.** CSV, HTML preview bundles, or zip archives containing source files, previews, and an optional portable workspace subset.
- **Metadata enrichment.** Custom fields, manual corrections to editable built-in fields, and structured processing jobs that operate on frozen run snapshots.

## How Retriever works

### Workspace model

Retriever treats the selected folder as the workspace root. All persistent state lives under `.retriever/`:

```text
.retriever/
├── retriever.db
├── previews/
├── bin/
│   ├── retriever_tools.py
│   └── backups/
├── jobs/
├── logs/
└── runtime.json
```

Important consequences:

- Your original documents stay in place and are not rewritten.
- Document paths in the database are workspace-relative.
- The workspace carries its own Retriever state, so browsing, datasets, and exports stay tied to that folder.
- The workspace tool copy can auto-upgrade when the plugin template changes, as long as the local copy has not been manually modified.

### Document model

Retriever indexes logical documents, not just files.

That means:

- EML and MSG emails can create child attachment documents.
- PST and MBOX files are treated as container sources, with one logical message document per message and one level of attachment children.
- Slack exports become conversation/day documents, with reply threads represented as child documents.
- Processed productions create one logical document per load-file row, not one document per page image or text file.

### Browse model

Retriever has a persistent browse session per workspace.

That session keeps three kinds of state:

- **Scope state:** keyword, Bates, filter, dataset, and `from-run` selectors.
- **Browsing state:** current sort and current page/offset.
- **Display state:** visible columns and page size.

Scope changes reset paging. Display settings and browse preferences persist until you change them or reset them.

### Result format

Document listings use a standard table.

The header above the table shows the active scope split across one line per selector, so it is obvious at a glance what is narrowing the current page:

- `Keyword: ...` (only when a keyword is active)
- `Bates: ...` (only when a Bates selector is active)
- `Active filters: ...` (only when a filter expression is active; truncated if very long)
- `Datasets: ...` (only when one or more datasets are selected)
- `From run: ...` (only when a `/from-run` selector is active)
- `Sort: ...`
- `Page: N of M  (docs X-Y of Z)` — or `(conversations X-Y of Z)` in conversation mode

When no selector is set, the header collapses to a single `Scope: (none)` line plus the sort and page lines.

Between the header and the table, an `Overview:` line summarizes the current page — how many datasets, custodians, rows with attachments, and rows flagged as `text_status=empty` are represented. It is there to give you a quick sense of whether the page is homogeneous or spans a lot of different buckets.

The table itself:

- One row per document in documents mode, or one row per conversation in conversation mode.
- The `title` cell (when shown) is a clickable preview link.

Below the table, a footer suggests next steps:

- A `Navigate:` line hints at `/retriever:next` for the next page and `/retriever:previous` to go back when those are available.
- A `Narrow:` line appears when the page spans multiple datasets, processing runs, or custodians and the active scope does not already narrow by them — it proposes a concrete `/filter` or `/from-run` you can paste to focus further.

Default behavior:

- Default page size: `10`.
- Maximum page size: `100`.
- Default columns in documents mode: `content_type`, `title`, `author`, `date_created`, `control_number`.
- Default columns in conversation mode: `conversation_type`, `title`, `participants`, `last_activity`, `document_count`.
- Default sort for keyword search: `relevance asc`.
- Default sort for Bates lookup: `bates asc`.
- Default sort for filter-only browse in documents mode: `date_created desc`.
- Default sort in conversation mode: `last_activity desc`.
- Each browse mode keeps its own `/columns`, `/sort`, and `/page-size` — switching modes does not clobber the other mode's preferences.

## Supported content

Retriever can ingest these source types today:

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

Ingest path notes that matter in practice:

- The default ingest path does not OCR scanned PDFs or image files, and does not generate image descriptions. Both are available as processing jobs that write output back to the document's indexed text — intentionally kept out of the default path so ingest stays fast and deterministic.
- Archive contents such as `.zip` are not unpacked automatically. Unpack the archive into the workspace yourself and re-ingest if you want the contents indexed.
- Retriever does not rely on semantic ranking in the default ingest/search path.
- Production authoring is out of scope. Retriever ingests existing processed productions but does not create them — there is no Bates-burn, redaction workflow, load-file writer, or production-export pipeline.

## Loading Retriever

The exact install flow depends on the host environment, but for local Claude CLI testing the fastest load is:

```bash
claude --plugin-dir /path/to/retriever-plugin
```

Once loaded:

- Use natural-language requests such as "index this workspace" or "run retriever workspace status" for setup, ingest, exports, and job operations.
- Use Retriever's persistent slash commands for day-to-day browsing and narrowing once a workspace is active.

## Typical workflows

### 1. Initialize and index a workspace

Use this when you are starting with a new folder of files.

In conversation:

- Ask Retriever to run `workspace status`.
- Ask it to run `workspace init`.
- Ask it to ingest the folder, usually recursively.

Direct CLI equivalents:

```bash
python3 .retriever/bin/retriever_tools.py workspace status .
python3 .retriever/bin/retriever_tools.py workspace init .
python3 .retriever/bin/retriever_tools.py ingest . --recursive
```

Use `ingest-production` when you want to target a processed production root explicitly:

```bash
python3 .retriever/bin/retriever_tools.py ingest-production . productions/VOL001
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

- `/search` sets the keyword or Bates slot.
- `/filter` adds metadata constraints.
- `/sort` changes the current browse ordering.
- `/page-size` changes how many rows each page shows.
- `/next` advances within the same persistent browse session.

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

Power-user CLI lifecycle:

```bash
python3 .retriever/bin/retriever_tools.py create-dataset . "Review Set"
python3 .retriever/bin/retriever_tools.py add-to-dataset . --dataset-name "Review Set" --doc-id 12 --doc-id 14
python3 .retriever/bin/retriever_tools.py remove-from-dataset . --dataset-name "Review Set" --doc-id 12
python3 .retriever/bin/retriever_tools.py delete-dataset . --dataset-name "Review Set"
```

### 6. Export the current selection

Once your scope is right, you can export it.

Examples:

```bash
python3 .retriever/bin/retriever_tools.py export-csv . review.csv --field control_number --field title --field dataset_name --select-from-scope
python3 .retriever/bin/retriever_tools.py export-previews . preview-bundle --doc-id 12 --doc-id 19
python3 .retriever/bin/retriever_tools.py export-archive . review.zip --select-from-scope --portable-workspace
```

Use cases:

- CSV for downstream review or QC.
- Preview bundles for sharing HTML previews outside the main workspace.
- Zip archives when you want source files, previews, and a portable subset together.

### 7. Add fields and enrich metadata

Retriever supports user-managed custom fields plus manual corrections to editable built-in fields.

Examples:

```bash
python3 .retriever/bin/retriever_tools.py add-field . privilege_status text --instruction "Privilege designation"
python3 .retriever/bin/retriever_tools.py fill-field . --field privilege_status --value privileged --doc-id 42
python3 .retriever/bin/retriever_tools.py fill-field . --field title --value "Board Minutes" --doc-id 42
python3 .retriever/bin/retriever_tools.py slash . /fill privilege_status privileged on 42
```

Important detail:

- Manual `fill-field` / `/fill` edits are locked and preserved on later ingest or review passes until you explicitly overwrite them.

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
python3 .retriever/bin/retriever_tools.py list-jobs .
python3 .retriever/bin/retriever_tools.py create-job . "Issue Tags" structured_extraction
python3 .retriever/bin/retriever_tools.py add-job-output . issue_tags primary_issue --value-type text
python3 .retriever/bin/retriever_tools.py create-job-version . issue_tags --provider <provider> --model <model> --input-basis active_search_text --instruction "Extract the primary issue."
python3 .retriever/bin/retriever_tools.py create-run . --job-name issue_tags --job-version 1 --select-from-scope
python3 .retriever/bin/retriever_tools.py run-status . --run-id 7
python3 .retriever/bin/retriever_tools.py execute-run . --run-id 7
```

Notes:

- Job display names are normalized to handles such as `issue_tags`.
- `execute-run` is the direct executor; advanced unattended/background orchestration uses the queue-oriented run commands instead.

## Slash command reference

Retriever's persistent browse surface consists of these commands.

| Command | Purpose | Examples |
|---|---|---|
| `/search` | Show or set the current keyword/Bates search slot | `/search`, `/search contract`, `/search --within renewal`, `/search clear`, `/search --fts ABC000123` |
| `/filter` | Show, add, or clear the current SQL-like filter expression | `/filter`, `/filter content_type = 'Email'`, `/filter clear` |
| `/bates` | Show, set, or clear the current Bates selector | `/bates`, `/bates ABC000123-ABC000150`, `/bates clear` |
| `/dataset` | Show, list, set, clear, or rename dataset selectors | `/dataset`, `/dataset list`, `/dataset "Review Set"`, `/dataset "Review Set", production`, `/dataset clear`, `/dataset rename "Old Set" "New Set"` |
| `/scope` | Show, list, save, load, or clear the whole current scope | `/scope`, `/scope list`, `/scope save hotdocs`, `/scope load hotdocs`, `/scope clear` |
| `/sort` | Show, list, set, or reset browse sorting | `/sort`, `/sort list`, `/sort file_name asc`, `/sort date_created desc, file_name asc`, `/sort default` |
| `/page` | Show current page state or jump to another page | `/page`, `/page 3`, `/page first`, `/page last`, `/page next`, `/page previous` |
| `/next` | Go to the next page | `/next` |
| `/previous` | Go to the previous page | `/previous` |
| `/page-size` | Show or set rows per page | `/page-size`, `/page-size 25` |
| `/columns` | Show, list, set, add, remove, or reset visible columns | `/columns`, `/columns list`, `/columns set title, control_number`, `/columns add dataset_name`, `/columns remove author`, `/columns default` |
| `/from-run` | Show, set, or clear a prior run selector | `/from-run`, `/from-run 42`, `/from-run clear` |

Notes:

- Bare forms such as `/scope`, `/dataset`, `/sort`, `/page`, `/page-size`, and `/columns` are read-only state inspection.
- `/next` is equivalent to `/page next`.
- `/previous` is equivalent to `/page previous`.
- Retriever also has internal browse-mode toggles for document and conversation views. Agents or app integrations may use those on your behalf, but they are intentionally omitted from the public slash-command surface.
- Values with spaces should be quoted.
- Comma-separated lists are supported for commands such as `/dataset`, `/columns set`, and `/sort`.

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

- If the current slot is a keyword slot, Retriever AND-composes the new text with the existing keyword.
- If the current slot is a Bates slot, Retriever intersects the current and new Bates ranges.
- `--within` does not compose across slots.

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

- `/filter` by itself shows the current filter expression.
- `/filter clear` removes the current filter slot.
- Each new `/filter <expression>` is AND-composed with the existing filter slot.

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
- Parentheses for grouping.

### Literal syntax

- Strings in single or double quotes.
- Numbers as unquoted literals.
- Booleans as `TRUE` or `FALSE`.
- `NULL`.
- `%` and `_` wildcards with `LIKE`.

### Useful field types

You can filter on:

- Built-in fields such as `title`, `subject`, `author`, `participants`, `content_type`, `file_name`, `file_type`, `file_size`, `page_count`, `custodian`, `date_created`, `date_modified`, and `control_number`.
- Custom fields added with `add-field` or `/field add`.
- Virtual fields such as `dataset_name`, `production_name`, `is_attachment`, and `has_attachments`.

Prefer canonical field names such as `date_created` instead of ad hoc variants.

A note on `custodian`. Because the same logical document can appear under more than one custodian, `custodian` filters match against the document's active occurrences rather than a single stored value. `custodian = 'Smith'` returns a document if any of its active occurrences have `Smith` as custodian, and `custodian LIKE '%Garcia%'` / `IN (...)` follow the same any-occurrence rule. Displayed custodian cells show the full list, joined by commas, so it is clear when a document spans multiple collections.

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
python3 .retriever/bin/retriever_tools.py catalog .
```

`catalog` is the source of truth for:

- Searchable/filterable built-in fields.
- Custom fields currently registered in the workspace.
- Virtual fields such as `dataset_name`.
- Which date fields support `year`, `quarter`, `month`, and `week` aggregation buckets.

## Display and paging tips

- Use `/columns set ...` when you want a completely different table shape.
- Use `/columns add ...` or `/columns remove ...` for smaller adjustments.
- Use `/columns default` to reset to the standard layout.
- `dataset_name` and `production_name` are displayable virtual columns.
- Some fields are filter-only and cannot be displayed, such as `has_attachments`.
- Use `/sort default` to go back to Retriever's automatic sort choice for the current scope.
- Page size changes affect both slash browsing and later view-mode listings until you change it again.

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
python3 .retriever/bin/retriever_tools.py workspace status .
python3 .retriever/bin/retriever_tools.py workspace status . --quick
python3 .retriever/bin/retriever_tools.py workspace init .
python3 .retriever/bin/retriever_tools.py workspace update . --force
python3 .retriever/bin/retriever_tools.py schema-version
```

### Search and retrieval

```bash
python3 .retriever/bin/retriever_tools.py search . "merger" --filter "content_type = 'Email'" --mode view
python3 .retriever/bin/retriever_tools.py get-doc . --doc-id 42 --include-text summary
python3 .retriever/bin/retriever_tools.py list-chunks . --doc-id 42 --page 1 --per-page 20
python3 .retriever/bin/retriever_tools.py search-chunks . "indemnification" --top-k 20
python3 .retriever/bin/retriever_tools.py aggregate . --group-by dataset_name --metric count
```

### Export

```bash
python3 .retriever/bin/retriever_tools.py export-csv . review.csv --field control_number --field title --select-from-scope
python3 .retriever/bin/retriever_tools.py export-previews . preview-bundle --doc-id 42
python3 .retriever/bin/retriever_tools.py export-archive . review.zip --select-from-scope
```

### Metadata and review operations

```bash
python3 .retriever/bin/retriever_tools.py add-field . privilege_status text
python3 .retriever/bin/retriever_tools.py fill-field . --field privilege_status --value privileged --doc-id 42
python3 .retriever/bin/retriever_tools.py merge-into-conversation . --doc-id 42 --target-doc-id 17
python3 .retriever/bin/retriever_tools.py split-from-conversation . --doc-id 42
python3 .retriever/bin/retriever_tools.py clear-conversation-assignment . --doc-id 42
```

## Important details to remember

- Retriever is workspace-local. Changing workspaces means changing the database, browse state, datasets, and saved scopes you are working against.
- Re-ingest updates changed files in place, preserves stable document identity where possible, and marks missing items instead of silently forgetting them.
- PST support depends on the required `pypff` backend being available. `workspace status` probes PST backend status separately, and missing PST support no longer blocks ordinary non-PST workflows.
- Production ingest is not the same as loose-file ingest. Use `ingest-production` when you want to target a production root explicitly.
- Manual field edits are protected from later automated overwrite.
- Results stay grounded in the active scope. If something looks missing, check `/scope`, `/dataset`, `/from-run`, `/sort`, and `/page-size` before assuming the underlying data is gone.
