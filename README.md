# Retriever

Retriever is a local-first review workspace for people who need to get through
a messy document set with Claude. The product story is intentionally simple:
use Retriever to do three jobs well.

## The three jobs

1. Build a review workspace
   Turn a local folder, processed production, PST/MBOX archive, or bounded
   Google Drive set into a Retriever workspace that keeps search state, saved
   scopes, previews, and exports together.
2. Find the documents that matter
   Ask for hot docs, key communications, contract-risk terms, privilege
   candidates, or specific Bates ranges without rebuilding the review context
   every turn.
3. Export a review set
   Once the scope is right, export a shareable table or archive for handoff,
   QA, or downstream loading.

## Guided surfaces

- `retriever-legal` agent: persona-first wrapper for first-pass legal review,
  diligence, privilege sweeps, and hot-doc workflows
- `retriever:export` skill: user-facing CSV/archive export wrapper

Google Drive remains supported as an intake/source option, but not as a
separate top-level Retriever capability.

Submission and trust docs:

- [MARKETPLACE.md](MARKETPLACE.md)
- [CONNECTORS.md](CONNECTORS.md)
- [PRIVACY.md](PRIVACY.md)

Repository docs:

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [TESTING.md](TESTING.md)
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [docs/browse-reference.md](docs/browse-reference.md)

## Quick Start

1. Put the documents for one matter or review into a single local folder.
2. Ask Retriever to ingest that folder.
3. Ask for the next job in plain English: find hot docs, review contracts, show
   key emails, or export the current review set.

Example prompts:

- `Ingest /path/to/folder`
- `Review this folder for hot docs`
- `Show emails from Alice in 2023`
- `Export the current review set`

If your source starts as a processed production, PST/MBOX archive, or bounded
Google Drive set, first turn it into a local Retriever workspace, then continue
the same way.

## What Retriever is for

Retriever works best when you need to review or analyze a local document
collection and keep that work grounded in the files on disk.

Best-fit users: legal review, investigations, diligence, and internal document
analysis.

Common use cases:

- searching a matter, investigation, or diligence workspace
- reviewing PDFs, Office documents, loose emails, PST/MBOX archives, and Slack
  exports together
- jumping directly to a Bates number or Bates range in a production
- building reusable scopes such as "emails from 2023 in the Hot Docs set"
- creating review sets, CSV exports, preview bundles, or portable archives
- adding custom metadata fields and running structured extraction jobs over a
  frozen document set

## Core capabilities

- Local-first storage. Retriever keeps its database, previews, logs, job state, and runtime metadata under `.retriever/` in the workspace root.
- Broad ingest support. It can index common review formats including PDF, DOCX, TXT/Markdown, CSV, JSON, HTML, ICS, RTF, XLS/XLSX, PPTX, EML, MSG, PST, MBOX, Slack exports, and processed productions.
- Search and browse. You can search by keyword, filter by metadata, browse by dataset, jump by Bates number, page through results, and persist scope/display preferences between commands.
- Preview-first review. Search results render as a standard table with clickable titles. Native preview files are used when possible; Retriever generates HTML or CSV previews when needed.
- Stable document identity. Documents receive stable `control_number` values for review and export. Production documents use produced Bates values as the control number.
- Dataset-aware workflows. Documents can belong to one or more datasets, and datasets can be source-backed or manually curated.
- Exports. Retriever can export selected rows to CSV, generate HTML preview bundles, or build zip archives containing source files, previews, and an optional portable workspace subset.
- Metadata enrichment. You can add custom fields, set values manually, and run structured processing jobs that operate on frozen run snapshots.
- Long-running processing. Translation, structured extraction, OCR, and image-description runs now have first-class Claude Code commands and native `python3 -m retriever` entrypoints that drive the resumable backend to a terminal state.

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
- the workspace records which canonical Retriever tool build last touched it, so the native package surface and compatibility bundle stay aligned
- heavy parser dependencies live in the shared Retriever runtime (`<repo-root>/.retriever-plugin-runtime/...`), not under `.retriever/`; see *Runtime and dependencies* for details

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

Retriever maintains a **shared Retriever runtime** under the repo directory:

```text
<repo-root>/.retriever-plugin-runtime/<system>-<machine>-pyX.Y/venv/
```

Heavy parser dependencies (`pdfplumber`, `python-docx`, `openpyxl`, `xlrd`, `extract-msg`, `libpff-python`, `striprtf`, `Pillow`, `charset-normalizer`) are **lazy-installed** into that shared venv the first time a command actually needs them. Non-parsing commands do not pay that cost.

Consequences:

- the workspace's `.retriever/` folder stays lightweight — it holds data, state, and logs, not Python packages
- multiple workspaces on the same machine share one parser install
- parser installs are keyed by platform and Python version, so swapping Python versions triggers a fresh install
- first use of a new parser type (for example, the first PST ingest) can briefly block while the dependency installs; `workspace status` will report the runtime state and warn if something needed is missing
- the runtime is advisory — if you prefer to manage Python yourself, the tool still falls back to whatever is importable in the active interpreter

The on-disk directory name still uses `.retriever-plugin-runtime/` for compatibility with older workspaces and generated tooling, but Claude Code is now the primary surface.

## Loading Retriever

For Claude Code, the primary install flow is a one-time global command install:

```bash
git clone --single-branch --depth 1 https://github.com/sdemyanov/retriever.git ~/.claude/skills/retriever
cd ~/.claude/skills/retriever
./setup
```

That writes namespaced personal commands under:

```text
~/.claude/commands/retriever/
```

It also updates your user-level Claude memory at:

```text
~/.claude/CLAUDE.md
```

The installed Retriever section tells Claude to prefer `/retriever:*` commands
first, then call the Retriever backend directly if needed, and only inspect
`./.retriever/retriever.db` as a last resort for debugging or verification.

After running `./setup`:

- open Claude Code in the Retriever workspace root you want to work in
- run `/help` to confirm the new namespaced commands are present
- `python3 -m retriever --help` now works from any directory that uses the same
  Python interpreter as the installer
- use `/retriever:init`, `/retriever:status`, `/retriever:ingest`,
  `/retriever:run`, `/retriever:translate`, `/retriever:extract`,
  `/retriever:ocr`, `/retriever:describe-images`, `/retriever:search`,
  `/retriever:open`, and `/retriever:export` as the main Claude-facing flow
- use `/retriever:filter`, `/retriever:dataset`, `/retriever:sort`,
  `/retriever:columns`, `/retriever:page`, `/retriever:page-size`,
  `/retriever:next`, `/retriever:previous`, `/retriever:scope`,
  `/retriever:bates`, `/retriever:from-run`, `/retriever:documents`,
  `/retriever:conversations`, and `/retriever:entities` to keep the existing
  persistent browse/session UX
- `/retriever:ingest` automatically includes `--run-to-completion`, so it
  keeps stepping the resumable ingest backend until the run reaches a terminal
  state
- `/retriever:run`, `/retriever:translate`, `/retriever:extract`,
  `/retriever:ocr`, and `/retriever:describe-images` are native package
  commands that keep stepping the resumable processing backend until the run
  reaches a terminal state
- `/retriever:export table ...` and `/retriever:export archive ...` also append
  hidden `--run-to-completion`, so exports default to a one-shot user flow
- `python3 -m retriever` now defaults to human-readable output, so installed
  commands come back as short summaries instead of raw JSON where Retriever
  supports it
- if a long-running command is interrupted, use `python3 -m retriever ...`
  recovery commands like `ingest-status`, `ingest-run-step`, or `cancel-run`
  directly instead of expecting those lower-level verbs in the normal slash
  surface

`skills/tool-template/tools.py` still works as a compatibility path and keeps
JSON as its default output mode, but `python3 -m retriever ...` is now the
primary CLI entrypoint and defaults to human-readable output.

### Legacy compatibility paths

For legacy compatibility testing, you can still load the bundled plugin:

```bash
claude --plugin-dir /path/to/retriever-plugin
```

For project-local Claude Code testing without the global install, you can still
use the workspace bridge below.

### Claude Code v0 command bridge

If you want to test Retriever in Claude Code without using the global install,
install a small project-local command bridge into the target workspace:

```bash
./setup-claude-v0 /path/to/workspace
```

That writes Claude Code command files under:

```text
/path/to/workspace/.claude/commands/
```

The generated commands call this checkout's canonical bundled backend:
`skills/tool-template/tools.py`.

After running the installer:

- open Claude Code in the target workspace, not in the Retriever repo
- run `/help` to confirm the Retriever commands are present
- use the existing Retriever browse vocabulary directly: `/search`, `/filter`,
  `/dataset`, `/sort`, `/columns`, `/page`, `/page-size`, `/next`,
  `/previous`, `/scope`, `/bates`, `/from-run`, and `/export`
- use `/show-doc --doc-id <id>` to open one preview with a short extracted-text
  summary
- use the longer admin commands for generic maintenance verbs:
  `/workspace-status`, `/init-workspace`, `/update-workspace`, `/ingest`,
  `/ingest-status`, `/ingest-run-step`, `/ingest-cancel`,
  `/ingest-production`, `/run-status`, `/run-job-step`, and `/cancel-run`
- if you run `/ingest` with no arguments, the v0 bridge defaults to
  `--recursive`
- the v0 `/ingest` command always includes `--run-to-completion`, so it keeps
  advancing the resumable ingest backend until the run reaches a terminal state
- the v0 `/export table ...` and `/export archive ...` commands append hidden
  `--run-to-completion`, so exports also default to one-shot completion
- the generated commands call Retriever with `--human`, so workspace, ingest,
  export, and `show-doc` return concise human output directly from Retriever

The installer defaults to unprefixed command names so Retriever can be tested
with its natural vocabulary. If the target workspace already has colliding
Claude command names, re-run with a prefix:

```bash
./setup-claude-v0 /path/to/workspace --prefix retriever-
```

## Typical workflows

### 1. Initialize and index a workspace

Use this when you are starting with a new folder of files.

In conversation:

- ask Retriever to run `workspace status` to check the runtime
- ask it to run `workspace init` to set up the folder
- ask it to ingest the folder, usually recursively

Direct CLI equivalents:

```bash
python3 -m retriever workspace status .
python3 -m retriever workspace init .
python3 -m retriever ingest . --recursive
```

The `workspace` command groups runtime and schema maintenance into subcommands:

- `workspace init` prepares or repairs `.retriever/` state and runtime metadata for a folder.
- `workspace status` reports runtime readiness and schema state without rewriting anything.
- `workspace update` refreshes runtime metadata from the canonical `tools.py` compatibility bundle after a tool update.

Use `ingest-production` when you want to target a processed production root explicitly:

```bash
python3 -m retriever ingest-production . productions/VOL001
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

### 4. Run long processing jobs

Once the scope is right, use the first-class processing commands instead of
manually stepping `run-job-step`.

Examples:

```bash
python3 -m retriever translate . --target-language Spanish --select-from-scope --model gpt-4.1-mini
python3 -m retriever extract . counterparty --instruction "Extract the named counterparty." --select-from-scope --model gpt-4.1-mini
python3 -m retriever ocr . --filter "text_status = 'empty'" --model gpt-4.1-mini
python3 -m retriever describe-images . --filter "content_type = 'Image'" --model gpt-4.1-mini
```

If a processing command is interrupted, resume it with:

```bash
python3 -m retriever run . --run-id <id>
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
python3 skills/tool-template/tools.py set-field . --doc-id 42 --doc-id 43 --field title --value "Board Minutes"
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

## Browse And CLI Reference

The detailed slash-command reference, `/search` and `/filter` syntax,
field/column discovery notes, display and paging tips, and advanced CLI quick
reference now live in [docs/browse-reference.md](docs/browse-reference.md).

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
