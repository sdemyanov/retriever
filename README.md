# Retriever

Retriever is an open source document intelligence plugin for Claude Code. It
turns local folders, productions, mailbox exports, and other supported
document collections into a workspace you can search, browse, review, enrich,
analyze, and export with Claude. Users work with Retriever through Claude
Code: install it once, open Claude Code in the target workspace, and use
plain-English requests or `/retriever:*` commands to ingest, inspect, review,
process, and export. It fits legal teams, investigations, compliance,
diligence, and other document-heavy workflows that need local-first search,
review, analysis, and handoff.

## What Retriever Does

1. Build a document workspace
   Turn a local folder, processed production, PST/MBOX archive, or other
   supported source into a Retriever workspace that keeps search state,
   previews, datasets, processing runs, and exports together.
2. Search, review, enrich, and analyze documents
   Find key documents and communications, preview files, filter and page
   through results, run first-pass review and triage, add metadata, run OCR or
   extraction, and keep the workspace context intact between turns.
3. Export results and handoff artifacts
   Once the scope is right, export a shareable table, preview bundle, or
   archive for handoff, QA, downstream loading, or another tool.

Typical uses include first-pass review and hot-doc triage, contract review,
mailbox and production analysis, Bates lookup, OCR or extraction over a
selected set, and CSV or archive export for handoff.

Project docs:

- [PRIVACY.md](PRIVACY.md)
- [ARCHITECTURE.md](ARCHITECTURE.md)
- [TESTING.md](TESTING.md)
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [docs/browse-reference.md](docs/browse-reference.md)

## Privacy

IMPORTANT: IF YOU HANDLE SENSITIVE, CLIENT, OR INTERNAL DOCUMENTS, TURN OFF
`Help improve Claude` IN `Settings -> Privacy` BEFORE USING RETRIEVER.
Anthropic says that when this setting is off, new Claude chats and coding
sessions are not used for future model training, although flagged
conversations may still be used for trust and safety purposes.
Review the [Anthropic privacy policy](https://www.anthropic.com/privacy) and
confirm it satisfies your practice, client, regulatory, and organizational
requirements before using Retriever with sensitive material.

Zero Data Retention (ZDR) is available for Claude Code on Claude for
Enterprise. Anthropic says ZDR is enabled per organization and covers Claude
Code inference on Claude for Enterprise. See [Zero data
retention](https://code.claude.com/docs/en/zero-data-retention) for the
current scope, limitations, and enablement details.

## How to Install

Open Claude Code and paste this. Claude does the rest.

> Install Retriever: run
> `if [ -d ~/.claude/skills/retriever/.git ]; then git -C ~/.claude/skills/retriever pull --ff-only origin main; else git clone --single-branch --depth 1 https://github.com/sdemyanov/retriever.git ~/.claude/skills/retriever; fi`.
> Then run `cd ~/.claude/skills/retriever && ./setup`.

To test Retriever with public sample data, open Claude Code and paste this:

> Set up Retriever sample data: run
> `if [ -d ~/retriever-data-public/.git ]; then git -C ~/retriever-data-public pull --ff-only origin main; else git clone --single-branch --depth 1 https://github.com/sdemyanov/retriever-data-public.git ~/retriever-data-public; fi`.

Then open Claude Code in `~/retriever-data-public` and run
`Ingest this folder`.

## Quick Start

1. Open Claude Code in the local folder for one document collection, matter,
   inbox export, or dataset.
2. Ask Claude Code to ingest that folder with Retriever.
3. Ask for the next job in plain English or use `/retriever:*` commands to
   search, review, analyze, enrich, and export.

Example prompts:

- `Ingest this folder`
- `Find documents mentioning indemnification`
- `Review the workspace for hot docs`
- `Show emails from Alice in 2023`
- `Extract counterparties from the current contracts`
- `Export the current results`

## Suggested first session

If you are trying Retriever for the first time, after opening Claude Code in
the target folder, a good starting point is:

- `Check the workspace status for this folder`
- `Ingest this folder`
- `Show me the first results for <your first keyword>`
- `Only show emails` if that helps
- `Add dataset name to the visible columns` if you want more context
- `Save this scope as first-pass` if you want to reuse it
- `Export the current results` if you want a handoff artifact

That path exercises the setup, browse, narrowing, display, persistence, and
export surfaces that most users rely on first.

## Supported inputs

### Supported file types

- PDFs
- DOCX
- TXT, Markdown, CSV, JSON, HTML, ICS, and many code/config text formats
- EML and MSG, including one level of extracted attachment children
- RTF
- XLS and XLSX
- PPTX
- common image formats as preview-only documents

### Supported archive and export sources

- PST mail archives
- MBOX mail archives
- Slack export roots
- processed productions such as Concordance-style `DAT` + `OPT` with `TEXT/`,
  `IMAGES/`, and optional `NATIVES/`

Ingest-path behaviors worth knowing:

- calendar invites (`.ics`/`.ifb`/`.vcal`/`.vcs`) that arrive as email
  attachments are promoted into the parent email — the invite's organizer,
  attendees, when, location, join URL, UID, and sequence are rolled into the
  email's indexed text and rendered as a structured invite header in the
  preview
- standalone calendar files ingest as their own documents
- no OCR for scanned PDFs or image files in the default path (OCR is
  available as a processing job that writes text back through
  `activate-text-revision`)
- images are previewable but not text-searchable by default (image
  descriptions can likewise be generated through a processing job)
- archive contents such as `.zip`, `.rar`, `.7z` are not unpacked or indexed automatically
- Retriever does not rely on semantic ranking in the default ingest/search path

## Core capabilities

- Open source and local-first. Retriever is open source, keeps its database,
  previews, logs, job state, and runtime metadata under `.retriever/` in the
  workspace root, and leaves original source files in place.
- Broad ingest support. It can index common document formats including PDF,
  DOCX, TXT/Markdown, CSV, JSON, HTML, ICS, RTF, XLS/XLSX, PPTX, EML, MSG,
  PST, MBOX, Slack exports, and processed productions.
- Search and browse. You can search by keyword, filter by metadata, browse by
  dataset, jump by Bates number, page through results, and persist
  scope/display preferences between commands.
- Document review workflows. Retriever supports first-pass review, hot-doc
  triage, contract review, diligence, and investigation workflows without
  forcing you to leave the main workspace.
- Preview-first document analysis. Search results render as a standard table
  with clickable titles. Native preview files are used when possible;
  Retriever generates HTML or CSV previews when needed.
- Stable document identity. Documents receive stable `control_number` values
  for referencing, analysis, and export. Production documents use produced
  Bates values as the control number.
- Dataset-aware workflows. Documents can belong to one or more datasets, and
  datasets can be source-backed or manually curated.
- Exports. Retriever can export selected rows to CSV, generate HTML preview
  bundles, or build zip archives containing source files, previews, and an
  optional portable workspace subset.
- Metadata enrichment. You can add custom fields, set values manually, and
  run structured processing jobs that operate on frozen run snapshots.
- Long-running processing. Translation, structured extraction, OCR, and
  image-description runs now have first-class Claude Code commands and native
  `python3 -m retriever` entrypoints that drive the resumable backend to a
  terminal state.

## Current limitations

- Single-user only. Retriever is designed for one analyst working in one local
  Claude Code session, not as a shared multi-user review platform.
- Local documents only. Retriever works on local folders and local export
  archives you already have on disk, not as a live cloud document system or
  remote collaboration layer.
- No semantic search. Search and ranking are keyword- and metadata-driven, not
  embedding- or vector-based.
- No redactions. Retriever does not create, manage, or burn redactions.
- No production creation. Retriever can ingest a processed production as local
  input, but it does not create outbound legal productions.

## Typical workflows

Natural-language instructions in Claude Code are the primary interface. The
slash commands below are optional shortcuts, and some natural-language
instructions may map to them internally when that is the clearest fit.

### 1. Initialize and index a workspace

Use this when you are starting with a new folder of files.

Primary interface in Claude Code:

- ask Retriever to check the workspace status
- ask it to initialize the workspace if needed
- ask it to ingest the folder, usually recursively

If you want to target a processed production root explicitly, ask Retriever to
ingest that production root rather than the whole folder.

### 2. Browse and narrow a collection

This is the main interactive workflow, and plain English is usually the best
place to start.

Natural-language examples:

- `Show emails about the NDA`
- `Only show messages from 2023`
- `Sort newest first and show 25 at a time`

Some requests like these may map to slash commands such as:

```text
/retriever:search nda
/retriever:filter content_type = 'Email'
/retriever:filter date_created BETWEEN '2023-01-01' AND '2023-12-31'
/retriever:sort date_created desc
/retriever:page-size 25
/retriever:next
```

### 3. Review by Bates number

Retriever treats Bates-like input as a first-class lookup mode.

Natural-language examples:

- `Show Bates ABC000123`
- `Show Bates range ABC000123-ABC000150`

Claude may use commands like:

```text
/retriever:bates ABC000123
/retriever:bates ABC000123-ABC000150
```

You can also set Bates scope through `/retriever:search` because it
auto-detects Bates-shaped input:

```text
/retriever:search ABC000123-ABC000150
```

### 4. Run long processing jobs

Once the scope is right, start with plain-English requests in Claude Code.

Natural-language examples:

- `Translate the current results into Spanish`
- `Extract counterparties from the current contracts`
- `OCR the documents with empty text`
- `Describe the images in the current results`

If a processing command is interrupted, ask Claude Code to resume the run.

If you need plain full-text search for something that looks like a Bates value,
force FTS:

```text
/retriever:search --fts ABC000123
```

### 5. Save and reuse a scope

A scope is the conjunction of:

- keyword
- Bates selector
- filter expression
- dataset selector
- `from-run` selector

Natural-language examples:

- `Save this view as merger-email-hotdocs`
- `Load the scope merger-email-hotdocs`
- `Show the current scope`

Some scope-management requests may map to commands like:

```text
/retriever:search merger
/retriever:filter content_type = 'Email'
/retriever:dataset "Hot Docs"
/retriever:scope save merger-email-hotdocs
```

Later:

```text
/retriever:scope load merger-email-hotdocs
```

Useful related commands:

```text
/retriever:scope
/retriever:scope list
/retriever:scope clear
```

### 6. Build or use datasets

Datasets are named document collections. They are useful for saved result sets,
source-backed groupings, and repeatable exports.

Natural-language examples:

- `Create a dataset called Priority Set`
- `Put these documents in Priority Set`
- `List datasets`
- `Clear the active dataset`

Some dataset requests may map to commands like:

```text
/retriever:dataset
/retriever:dataset list
/retriever:dataset "Priority Set"
/retriever:dataset "Hot Docs", "Witness Files"
/retriever:dataset clear
```

`/retriever:dataset list` renders as a compact stats table so you can see each
dataset's document count, top custodians, and activity range at a glance
without drilling in.

### 7. Export the current result set

Once your scope is right, plain-English export requests are the normal path.

Natural-language examples:

- `Export the current results to CSV`
- `Build a preview bundle for these documents`
- `Create a portable archive of the current results`

If you need specific filenames or export options, say that directly in the
request.

Use cases:

- CSV for downstream analysis, review, or QC
- preview bundles for sharing HTML previews outside the main workspace
- zip archives when you want source files, previews, and a portable subset together

### 8. Add fields and enrich metadata

Retriever supports user-managed custom fields plus manual corrections to
editable built-in fields.

Natural-language examples:

- `Add a field called privilege_status`
- `Describe privilege_status as Privilege designation`
- `Mark DOC001.00000042 as privileged`
- `Clear privilege_status on DOC001.00000042`

Some field and metadata requests may map to commands like:

```text
/retriever:field add privilege_status text
/retriever:field describe privilege_status "Privilege designation"
/retriever:fill privilege_status privileged on DOC001.00000042
/retriever:fill privilege_status clear on DOC001.00000042
```

`/retriever:fill` can also populate a value across the active scope. Those bulk
forms require `--confirm`:

```text
/retriever:search privileged
/retriever:filter content_type = 'Email' AND custodian = 'Garcia'
/retriever:fill privilege_status privileged --confirm
```

Important details:

- manual fills on custom fields and manual corrections to editable built-ins
  are locked and preserved on later ingest or processing passes until you
  explicitly overwrite them
- `/retriever:fill` refuses to target derived or system-managed fields
  (`custodian`, `dataset_name`, `production_name`, hashes, ids, ingest
  timestamps); correct those through the appropriate ingest or conversation
  command instead
- `/retriever:field delete` is permanent; the slash surface previews the
  removal and requires `--confirm` before actually dropping the field

### 9. Run structured processing jobs

Retriever can freeze a selector into a run and process it later. For most
users, it is better to describe the outcome you want in plain English and let
Claude guide the setup.

High-level flow:

1. Create a job.
2. Define its outputs.
3. Create a job version.
4. Freeze a selector into a run with `create-run`.
5. Execute or supervise that run.
6. Optionally scope future work with `/retriever:from-run <run-id>`.

Natural-language example:

- `Create an Issue Tags extraction job, add a primary_issue output, and run it on the current scope`

## Browse and CLI reference

The detailed slash-command reference, `/retriever:search` and
`/retriever:filter` syntax, field/column discovery notes, display and paging
tips, and advanced CLI quick reference now live in
[docs/browse-reference.md](docs/browse-reference.md).

## How Retriever works

### Workspace model

Retriever treats the selected folder as the workspace root. All persistent
state lives under `.retriever/`:

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
- the workspace carries its own Retriever state, so browsing, datasets, and
  exports stay tied to that folder
- the workspace records which canonical Retriever tool build last touched it,
  so the native package surface and compatibility bundle stay aligned
- optional parser dependencies are loaded from the shared Retriever runtime
  (`<repo-root>/.retriever-plugin-runtime/...`) when needed, not under
  `.retriever/`; the main `python3 -m retriever` entrypoint still runs in the
  active Python interpreter. See *Runtime and dependencies* for details.

### Document model

Retriever indexes logical documents, not just files.

That means:

- EML and MSG emails can create child attachment documents
- PST and MBOX files are treated as container sources, with one logical
  message document per message and one level of attachment children
- Slack exports become conversation/day documents, with reply threads represented as child documents
- processed productions create one logical document per load-file row, not
  one document per page image or text file

### Browse model

Retriever has a persistent browse session per workspace.

That session keeps three kinds of state:

- scope state: keyword, Bates, filter, dataset, and `from-run` selectors
- browsing state: current sort and current page/offset
- display state: visible columns and page size

Scope changes reset paging. Display settings and browse preferences persist
until you change them or reset them.

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

## Runtime and dependencies

Retriever uses two Python layers:

- the active Python interpreter for the main `python3 -m retriever` entrypoint
- a shared repo-local runtime for optional parser dependencies:

```text
<repo-root>/.retriever-plugin-runtime/<system>-<machine>-pyX.Y/venv/
```

Heavy parser dependencies (`pdfplumber`, `python-docx`, `openpyxl`, `xlrd`,
`extract-msg`, `libpff-python`, `striprtf`, `Pillow`,
`charset-normalizer`) are **lazy-installed** into that shared venv the first
time a command actually needs them. Retriever first uses whatever is already
importable in the active interpreter, then falls back to the shared runtime for
optional parser packages. Non-parsing commands do not pay that cost.

Consequences:

- the workspace's `.retriever/` folder stays lightweight — it holds data,
  state, and logs, not Python packages
- multiple workspaces on the same machine share one parser install
- parser installs are keyed by platform and Python version, so swapping
  Python versions triggers a fresh install
- first use of a new parser type (for example, the first PST ingest) can
  briefly block while the dependency installs; `workspace status` will report
  the runtime state and warn if something needed is missing
- the shared runtime is advisory, not the only execution environment — if you
  prefer to manage Python yourself, Retriever still works with whatever is
  importable in the active interpreter

The on-disk directory name still uses `.retriever-plugin-runtime/` for
compatibility with older workspaces and generated tooling, but users interact
with Retriever through Claude Code.

## Operational notes

- Retriever is workspace-local. Changing workspaces means changing the
  database, browse state, datasets, and saved scopes you are working against.
- Re-ingest updates changed files in place, preserves stable document identity
  where possible, and marks missing items instead of silently forgetting them.
- PST support depends on the required `pypff` backend being available. Use
  `workspace status` if PST ingest is not ready; parser dependencies are
  lazy-installed into the shared plugin runtime (see *Runtime and
  dependencies* below), so the status check will also tell you if the runtime
  needs to be (re)populated.
- Production ingest is not the same as loose-file ingest. Use
  `ingest-production` when you want to target a production root explicitly.
- Manual field edits are protected from later automated overwrite.
- Results stay grounded in the active scope. If something looks missing, check
  `/retriever:scope`, `/retriever:dataset`, `/retriever:from-run`,
  `/retriever:sort`, and `/retriever:page-size` before assuming the underlying
  data is gone.

## Uninstall

Open Claude Code and paste this. Claude does the rest.

> Uninstall Retriever: remove `~/.claude/commands/retriever`,
> `~/.claude/retriever-manifest.json`, `~/.claude/skills/retriever`, and the
> `retriever.pth` file from your Python user site-packages. Remove the
> Retriever section from `~/.claude/CLAUDE.md`; if that leaves the file empty,
> delete the file. Skip anything that does not exist. Then give a short
> summary of what was removed.

## License

Retriever is licensed under the Elastic License 2.0 (ELv2). The SPDX
identifier is `Elastic-2.0`. See the [Elastic License
2.0](https://www.elastic.co/licensing/elastic-license) for the license terms.
