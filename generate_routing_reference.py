#!/usr/bin/env python3
"""Regenerate the Tier 2 slash list and Tier 3 subcommand list inside
`CLAUDE.md` so the routing ladder never goes stale.

Tier 2 (slash commands) is extracted from the tool-template source by scanning
for every literal `command_name == "..."` occurrence inside the slash
dispatcher. Tier 3 (named subcommands of `retriever_tools.py`) is extracted
by shelling out to `--help` on the freshly bundled tool.

For each command we keep three hand-authored pieces of metadata:

  - SLASH_BLURBS / SUBCOMMAND_BLURBS: the action the command performs.
  - USE_WHEN: the natural-language intent that should route to this command.
  - SUBCOMMAND_GROUPS: the topical bucket for Tier 3 (no groups at Tier 2 —
    14 items fit on one screen).

The renderer emits intent-first lines under topical headings so selection
works the same way at Tier 2 and Tier 3 as it does at Tier 1. Anything new —
a command without a blurb, without a "use when," or a Tier 3 command without
a group — shows up as a `TODO: …` line so it is visible in a build diff and
can be filled in before shipping.
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent
SLASH_SOURCE = REPO_ROOT / "skills" / "tool-template" / "src" / "60_search_cli.py"
TOOL_PATH = REPO_ROOT / "skills" / "tool-template" / "retriever_tools.py"
CLAUDE_PATH = REPO_ROOT / "CLAUDE.md"

# ---------------------------------------------------------------------------
# Tier 2 — slash commands. No grouping.
# ---------------------------------------------------------------------------

SLASH_BLURBS: dict[str, str] = {
    "bates": "scope browsing to a Bates range",
    "columns": "inspect or change displayed columns",
    "conversations": "switch the browse mode to conversations",
    "dataset": "scope to one or more datasets, list them, rename, or clear",
    "documents": "switch the browse mode to documents",
    "field": "list or manage custom field definitions",
    "filter": "add or clear SQL-like filters",
    "fill": "set or clear field values on documents",
    "from-run": "scope browsing to a processing run",
    "next": "go to the next page of active results",
    "page": "jump to a specific page",
    "page-size": "inspect or change rows per page",
    "previous": "go to the previous page of active results",
    "scope": "inspect or manage the active scope",
    "search": "run a keyword search",
    "sort": "inspect or change sort order",
}

SLASH_SYNTAX: dict[str, str] = {
    "bates": "`/bates <range>`",
    "columns": "`/columns [list|set|add|remove|default]`",
    "conversations": "`/conversations`",
    "dataset": "`/dataset [list|<name>[,<name>...]|clear|rename <old> <new>]`",
    "documents": "`/documents`",
    "field": "`/field [list|add|rename|delete|describe|type]`",
    "filter": "`/filter [<expression>|clear]`",
    "fill": "`/fill <field> <value-or-clear> [on <doc-ref[,doc-ref,...]>] [--confirm]`",
    "from-run": "`/from-run <run-id|clear>`",
    "next": "`/next`",
    "page": "`/page [<n>|first|last|next|previous]`",
    "page-size": "`/page-size [<n>]`",
    "previous": "`/previous`",
    "scope": "`/scope [list|clear|save <name>|load <name>]`",
    "search": "`/search [<query>]`",
    "sort": "`/sort [list|<field> <asc|desc>|default]`",
}

SLASH_USE_WHEN: dict[str, str] = {
    "bates": "the user asks to limit or scope browsing to a Bates or production-number range — phrasings like \"show ABC0001 to ABC0050\", \"just the ABC0100 docs\", \"Bates range\", \"production numbers X to Y\", or \"clear the Bates range\"",
    "columns": "the user asks to show, hide, add, remove, reorder, or reset which columns appear in the result table — phrasings like \"add the author column\", \"hide date_received\", \"show file size\", \"what columns are available\", or \"reset columns\"",
    "conversations": "the user asks to list, show, or browse conversations/threads — pair with `/search`, `/filter`, `/dataset`, or other scope commands to populate results; by itself it only switches the browse mode",
    "dataset": "the user asks to list, show, enumerate, switch, pick, select, rename, or clear datasets — phrasings like \"what datasets do I have\", \"show me my datasets\", \"switch to gmail-max\", \"use the production dataset\", or \"rename X to Y\"",
    "documents": "the user asks to list, show, or browse individual documents/messages — pair with `/search`, `/filter`, `/dataset`, or other scope commands to populate results; by itself it only switches the browse mode",
    "field": "the user asks to list, add, rename, delete, re-describe, or retype a custom field — phrasings like \"add a responsiveness field\", \"rename privilege_status\", \"drop the old tag\", \"update the field description\", or \"change this field to date\"",
    "filter": "the user asks to narrow, restrict, constrain, or exclude results — phrasings like \"only PDFs\", \"show just emails from alice\", \"exclude attachments\", \"hide chats\", \"only 2023\", or a SQL-like predicate — or asks to drop/clear current filters",
    "fill": "the user asks to populate, tag, mark, label, classify, annotate, flag, or clear a field value on one document or on the current filtered result set — phrasings like \"mark these responsive\", \"fill reviewer=jdoe\", \"clear the review status\", or \"tag DOC001 as privileged\"",
    "from-run": "the user asks to limit or scope browsing to documents produced by a specific processing run — phrasings like \"only docs from run 42\", \"show what run 5 produced\", \"filter to the last OCR run\", \"just the image-description outputs\", or \"clear the run filter\"",
    "next": "the user asks for more results or the next page — phrasings like \"show more\", \"keep going\", \"next batch\", \"next page\", \"continue\", or \"what else\"",
    "page": "the user asks to jump to a specific page — phrasings like \"go to page 3\", \"first page\", \"last page\", \"skip to the end\", \"back to the start\", or \"where am I in the results\"",
    "page-size": "the user asks to change how many rows appear per page — phrasings like \"show 50 at a time\", \"more per page\", \"smaller page size\", \"25 rows please\", or \"what's my current page size\"",
    "previous": "the user asks to go back to earlier results or the previous page — phrasings like \"go back\", \"previous page\", \"back one page\", \"earlier results\", or \"the page before\"",
    "scope": "the user asks to inspect, save, bookmark, restore, load, or clear the current combination of dataset/filter/sort/column state — phrasings like \"save this view as X\", \"go back to my saved scope\", \"what's my current scope\", \"list saved scopes\", or \"clear scope\"",
    "search": "the user asks to show, list, view, display, browse, find, search, or retrieve documents, conversations, emails, chats, threads, messages, files, or attachments — with or without a keyword — including requests like \"show me emails from alice\", \"list PDFs from 2023\", \"find docs mentioning indemnification\", or \"what's in gmail-max\"",
    "sort": "the user asks to change or reset the order of results — phrasings like \"newest first\", \"oldest first\", \"sort by date\", \"order by file name\", \"alphabetical\", \"by size\", or \"reset sort\"",
}

# ---------------------------------------------------------------------------
# Tier 3 — retriever_tools.py subcommands, grouped by topic.
# ---------------------------------------------------------------------------

SUBCOMMAND_BLURBS: dict[str, str] = {
    # Workspace & maintenance
    "workspace": "initialize, inspect, or update workspace installation and schema",
    "schema-version": "report the current schema/tool version",
    # Datasets
    "list-datasets": "list datasets in the workspace",
    "create-dataset": "create a manual dataset",
    "add-to-dataset": "add documents to a dataset",
    "remove-from-dataset": "remove documents from a dataset",
    "delete-dataset": "delete a dataset",
    # Ingestion
    "ingest": "index documents in the workspace",
    "ingest-production": "ingest a processed production volume",
    "inspect-pst-properties": "inspect raw PST message fields for debugging",
    # Search & browse
    "search": "search indexed documents",
    "search-docs": "search indexed documents at the document level",
    "search-chunks": "search matching text chunks with citations",
    "slash": "execute a scope-aware slash command (see Tier 2)",
    # Documents & text
    "get-doc": "fetch one document with optional summary text or exact chunks",
    "list-chunks": "list chunk metadata for one document",
    "list-text-revisions": "list stored text revisions for a document",
    "activate-text-revision": "promote a stored text revision to active indexed text",
    # Catalog & aggregation
    "catalog": "describe searchable, filterable, and aggregatable fields",
    "aggregate": "run bounded metadata aggregations across documents",
    # Export
    "export-csv": "write selected documents and fields to CSV",
    "export-archive": "write selected documents, previews, and source artifacts to a zip",
    "export-previews": "write HTML preview exports under `.retriever/exports`",
    # Custom fields
    "list-fields": "list registered custom fields",
    "add-field": "register a custom field",
    "rename-field": "rename a custom field",
    "delete-field": "delete a custom field",
    "describe-field": "set or clear a custom field description",
    "change-field-type": "change a field's storage type",
    "fill-field": "set or clear a field value on one or more documents",
    # Conversations
    "merge-into-conversation": "merge a document into a conversation",
    "split-from-conversation": "split a document out of a conversation",
    "clear-conversation-assignment": "clear a document's conversation assignment",
    "refresh-conversation-previews": "rebuild conversation preview artifacts",
    "reconcile-duplicates": "reconcile detected duplicates",
    # Runs — planning & lifecycle
    "list-runs": "list planned processing runs",
    "get-run": "fetch one planned processing run",
    "create-run": "create a frozen processing run snapshot",
    "run-status": "summarize run progress, claims, and recent failures",
    "cancel-run": "stop claiming new work for a run",
    "execute-run": "execute one planned processing run via the legacy direct executor",
    "publish-run-results": "publish results from a completed run",
    "finalize-ocr-run": "finalize an OCR run",
    "finalize-image-description-run": "finalize an image-description run",
    # Runs — worker execution
    "claim-run-items": "atomically claim pending run items for one worker",
    "prepare-run-batch": "claim one worker batch and return execution contexts",
    "get-run-item-context": "load the execution context for one run item",
    "heartbeat-run-items": "refresh heartbeat timestamps for one worker's claimed items",
    "finish-run-worker": "mark one worker as finished and persist its summary",
    "complete-run-item": "mark one claimed run item completed",
    "fail-run-item": "mark one claimed run item failed",
    # Jobs
    "list-jobs": "list jobs",
    "create-job": "create a job",
    "add-job-output": "attach an output to a job",
    "list-job-versions": "list job versions",
    "create-job-version": "create a job version",
    # Results
    "list-results": "list stored processing results",
}

SUBCOMMAND_USE_WHEN: dict[str, str] = {
    # Workspace & maintenance
    "workspace": "you need to initialize a fresh workspace, diagnose runtime or install integrity, or refresh the workspace tool",
    "schema-version": "you need the current schema/tool version string",
    # Datasets
    "list-datasets": "you need the current dataset list with document counts (programmatic form; prefer `retriever:dataset` / `/dataset list` for user-facing intent)",
    "create-dataset": "the user asks to create, start, or make a new dataset/collection/group — phrasings like \"start a new collection called X\", \"make a dataset for these\", \"create a group called priority\", or \"new dataset Y\"",
    "add-to-dataset": "the user asks to add, put, tag, include, or assign documents into an existing dataset — phrasings like \"put these in X\", \"tag these into priority\", \"add these docs to the responsive set\", or \"include these in Y\"",
    "remove-from-dataset": "the user asks to remove, drop, take out, or exclude documents from an existing dataset — phrasings like \"remove these from X\", \"drop these out of priority\", \"pull these from the responsive set\", or \"unassign from Y\"",
    "delete-dataset": "the user asks to delete, trash, remove, or get rid of an entire dataset — phrasings like \"delete the X dataset\", \"trash the old collection\", \"get rid of the priority group\", or \"remove dataset Y entirely\"",
    # Ingestion
    "ingest": "you need to index or refresh a folder",
    "ingest-production": "you need to ingest a processed production (DAT/OPT/TEXT/IMAGES)",
    "inspect-pst-properties": "you are debugging PST ingestion or conversation scoping",
    # Search & browse
    "search": "you need a programmatic search with explicit filters/sort/columns",
    "search-docs": "you need a programmatic document-level search (over parents only)",
    "search-chunks": "you need citation-ready chunk hits for a query",
    "slash": "you need to invoke a Tier 2 slash programmatically",
    # Documents & text
    "get-doc": "you need full metadata, text, or chunks for one document",
    "list-chunks": "you need the chunk layout for one document",
    "list-text-revisions": "you need to see all text revisions stored for a document",
    "activate-text-revision": "you need to switch a document's active search text to a specific revision",
    # Catalog & aggregation
    "catalog": "the user asks what fields, columns, or attributes exist or are searchable/filterable/aggregatable — phrasings like \"what fields exist\", \"what can I search on\", \"show me the columns I can filter by\", or \"list available attributes\"",
    "aggregate": "the user asks for counts, sums, distinct values, breakdowns, or groupings across filtered documents — phrasings like \"how many emails per sender\", \"count by dataset\", \"breakdown by content type\", \"group by author\", or \"total size by year\"",
    # Export
    "export-csv": "the user asks to export, download, or save results as a CSV or spreadsheet — phrasings like \"export these as CSV\", \"save to Excel\", \"download as spreadsheet\", or \"give me a CSV of the matches\"",
    "export-archive": "the user asks to export, download, or package results as a zip archive with previews and source files — phrasings like \"zip up the matches\", \"download everything\", \"package these docs\", or \"give me a bundle of these\"",
    "export-previews": "the user asks to export or save HTML previews of selected documents — phrasings like \"export the previews\", \"save rendered HTML\", or \"give me browsable preview files\"",
    # Custom fields
    "list-fields": "you need the registered custom-field inventory",
    "add-field": "you need to register a new custom field definition",
    "rename-field": "you need to rename an existing custom field definition",
    "delete-field": "you need to delete an existing custom field definition",
    "describe-field": "you need to set or clear a custom field description",
    "change-field-type": "you need to change a custom field's storage type",
    "fill-field": "you need to write or clear a field value on one or more documents",
    # Conversations
    "merge-into-conversation": "the user asks to merge, join, link, or attach a document into a specific conversation/thread — phrasings like \"join these emails into one thread\", \"merge this into thread X\", \"link this message to conversation Y\", or \"group these as one conversation\"",
    "split-from-conversation": "the user asks to split, detach, separate, or remove a document from its conversation/thread — phrasings like \"split this email off its thread\", \"detach this message\", \"separate this from the conversation\", or \"remove from thread\"",
    "clear-conversation-assignment": "you need to drop a document's conversation assignment",
    "refresh-conversation-previews": "you need to rebuild conversation preview HTML",
    "reconcile-duplicates": "you need to resolve detected duplicates",
    # Runs — planning & lifecycle
    "list-runs": "you need the list of planned/active processing runs",
    "get-run": "you need the snapshot of one planned run",
    "create-run": "you need to plan a new processing run",
    "run-status": "you need progress, claims, and recent failures for a run",
    "cancel-run": "you need to stop a run from claiming further work",
    "execute-run": "you need to execute a planned run inline via the legacy executor",
    "publish-run-results": "you need to publish completed-run results",
    "finalize-ocr-run": "you need to finalize an OCR run's outputs",
    "finalize-image-description-run": "you need to finalize an image-description run's outputs",
    # Runs — worker execution
    "claim-run-items": "you are a run worker claiming pending items",
    "prepare-run-batch": "you are a run worker preparing one batch of work",
    "get-run-item-context": "you are a run worker loading context for one item",
    "heartbeat-run-items": "you are a run worker refreshing its heartbeats",
    "finish-run-worker": "you are a run worker finalizing its session",
    "complete-run-item": "you are a run worker marking an item completed",
    "fail-run-item": "you are a run worker marking an item failed",
    # Jobs
    "list-jobs": "you need the list of registered jobs",
    "create-job": "you need to register a new job",
    "add-job-output": "you need to attach an output artifact to a job",
    "list-job-versions": "you need to see a job's versions",
    "create-job-version": "you need to cut a new version of a job",
    # Results
    "list-results": "you need stored processing results for inspection",
}

SUBCOMMAND_GROUPS: dict[str, str] = {
    "workspace": "Workspace & maintenance",
    "schema-version": "Workspace & maintenance",
    "list-datasets": "Datasets",
    "create-dataset": "Datasets",
    "add-to-dataset": "Datasets",
    "remove-from-dataset": "Datasets",
    "delete-dataset": "Datasets",
    "ingest": "Ingestion",
    "ingest-production": "Ingestion",
    "inspect-pst-properties": "Ingestion",
    "search": "Search & browse",
    "search-docs": "Search & browse",
    "search-chunks": "Search & browse",
    "slash": "Search & browse",
    "get-doc": "Documents & text",
    "list-chunks": "Documents & text",
    "list-text-revisions": "Documents & text",
    "activate-text-revision": "Documents & text",
    "catalog": "Catalog & aggregation",
    "aggregate": "Catalog & aggregation",
    "export-csv": "Export",
    "export-archive": "Export",
    "export-previews": "Export",
    "list-fields": "Custom fields",
    "add-field": "Custom fields",
    "rename-field": "Custom fields",
    "delete-field": "Custom fields",
    "describe-field": "Custom fields",
    "change-field-type": "Custom fields",
    "fill-field": "Custom fields",
    "merge-into-conversation": "Conversations",
    "split-from-conversation": "Conversations",
    "clear-conversation-assignment": "Conversations",
    "refresh-conversation-previews": "Conversations",
    "reconcile-duplicates": "Conversations",
    "list-runs": "Runs — planning & lifecycle",
    "get-run": "Runs — planning & lifecycle",
    "create-run": "Runs — planning & lifecycle",
    "run-status": "Runs — planning & lifecycle",
    "cancel-run": "Runs — planning & lifecycle",
    "execute-run": "Runs — planning & lifecycle",
    "publish-run-results": "Runs — planning & lifecycle",
    "finalize-ocr-run": "Runs — planning & lifecycle",
    "finalize-image-description-run": "Runs — planning & lifecycle",
    "claim-run-items": "Runs — worker execution",
    "prepare-run-batch": "Runs — worker execution",
    "get-run-item-context": "Runs — worker execution",
    "heartbeat-run-items": "Runs — worker execution",
    "finish-run-worker": "Runs — worker execution",
    "complete-run-item": "Runs — worker execution",
    "fail-run-item": "Runs — worker execution",
    "list-jobs": "Jobs",
    "create-job": "Jobs",
    "add-job-output": "Jobs",
    "list-job-versions": "Jobs",
    "create-job-version": "Jobs",
    "list-results": "Results",
}

GROUP_ORDER: list[str] = [
    "Workspace & maintenance",
    "Datasets",
    "Ingestion",
    "Search & browse",
    "Documents & text",
    "Catalog & aggregation",
    "Export",
    "Custom fields",
    "Conversations",
    "Runs — planning & lifecycle",
    "Runs — worker execution",
    "Jobs",
    "Results",
]

UNCLASSIFIED_GROUP = "Unclassified — TODO"
IGNORED_SUBCOMMANDS = {"promote-field-type", "set-field"}


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def extract_slash_commands() -> list[str]:
    text = SLASH_SOURCE.read_text(encoding="utf-8")
    matches: set[str] = set(re.findall(r'command_name\s*==\s*"([a-z][a-z0-9\-]*)"', text))
    in_set_matches = re.findall(r"command_name\s+in\s*\{([^}]+)\}", text)
    for group in in_set_matches:
        matches.update(re.findall(r'"([a-z][a-z0-9\-]*)"', group))
    return sorted(matches)


def extract_subcommands() -> list[str]:
    result = subprocess.run(
        [sys.executable, str(TOOL_PATH), "--help"],
        capture_output=True,
        text=True,
        check=True,
    )
    match = re.search(r"\{([^}]+)\}", result.stdout)
    if not match:
        raise SystemExit("Could not parse subcommand list from retriever_tools.py --help")
    raw = match.group(1)
    return sorted(
        command_name
        for command_name in (x.strip() for x in raw.split(","))
        if command_name and command_name not in IGNORED_SUBCOMMANDS
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _render_slash_line(name: str) -> str:
    syntax = SLASH_SYNTAX.get(name, f"`/{name}`")
    blurb = SLASH_BLURBS.get(name, "TODO: describe")
    use_when = SLASH_USE_WHEN.get(name, "TODO: add use-when")
    return f"- {syntax} — {blurb}. **Use when:** {use_when}."


def render_slash_section(names: list[str]) -> str:
    return "\n".join(_render_slash_line(n) for n in names)


def _render_subcommand_line(name: str) -> str:
    blurb = SUBCOMMAND_BLURBS.get(name, "TODO: describe")
    use_when = SUBCOMMAND_USE_WHEN.get(name, "TODO: add use-when")
    # Use-when values read as full clauses ("you need to …", "you are a run
    # worker …"), so no leading prefix is needed — that keeps Tier 3 intent-
    # first without the awkward "when you need to you need to …" doubling.
    return f"- {use_when} → `{name}` — {blurb}"


def render_subcommand_section(names: list[str]) -> str:
    buckets: dict[str, list[str]] = {g: [] for g in GROUP_ORDER}
    buckets[UNCLASSIFIED_GROUP] = []
    for name in names:
        group = SUBCOMMAND_GROUPS.get(name, UNCLASSIFIED_GROUP)
        buckets.setdefault(group, []).append(name)
    sections: list[str] = []
    for group in GROUP_ORDER + [UNCLASSIFIED_GROUP]:
        entries = sorted(buckets.get(group, []))
        if not entries:
            continue
        sections.append(f"### {group}\n")
        sections.append("\n".join(_render_subcommand_line(n) for n in entries))
        sections.append("")
    return "\n".join(sections).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Splice
# ---------------------------------------------------------------------------


def splice(source: str, marker_begin: str, marker_end: str, body: str) -> str:
    pattern = re.compile(
        rf"({re.escape(marker_begin)}\s*\n)(.*?)(\n\s*{re.escape(marker_end)})",
        re.DOTALL,
    )
    replacement, count = pattern.subn(
        lambda m: f"{m.group(1)}{body}{m.group(3)}",
        source,
        count=1,
    )
    if count != 1:
        raise SystemExit(f"Could not find markers {marker_begin} / {marker_end} in {CLAUDE_PATH}")
    return replacement


def _collect_todos(names: list[str], kind: str) -> list[str]:
    todos: list[str] = []
    blurb_map = SLASH_BLURBS if kind == "slash" else SUBCOMMAND_BLURBS
    use_when_map = SLASH_USE_WHEN if kind == "slash" else SUBCOMMAND_USE_WHEN
    for name in names:
        if name not in blurb_map:
            todos.append(f"{kind}:{name} missing blurb")
        if name not in use_when_map:
            todos.append(f"{kind}:{name} missing use-when")
        if kind == "subcommand" and name not in SUBCOMMAND_GROUPS:
            todos.append(f"{kind}:{name} missing group")
    return todos


def main() -> None:
    if not SLASH_SOURCE.exists():
        raise SystemExit(f"Missing slash source: {SLASH_SOURCE}")
    if not TOOL_PATH.exists():
        raise SystemExit(f"Missing bundled tool: {TOOL_PATH} (run bundle_retriever_tools.py first)")
    if not CLAUDE_PATH.exists():
        raise SystemExit(f"Missing CLAUDE.md: {CLAUDE_PATH}")

    slash_names = extract_slash_commands()
    subcommand_names = extract_subcommands()

    claude_text = CLAUDE_PATH.read_text(encoding="utf-8")
    claude_text = splice(
        claude_text,
        "<!-- BEGIN: slash-commands -->",
        "<!-- END: slash-commands -->",
        render_slash_section(slash_names),
    )
    claude_text = splice(
        claude_text,
        "<!-- BEGIN: tool-subcommands -->",
        "<!-- END: tool-subcommands -->",
        render_subcommand_section(subcommand_names),
    )
    CLAUDE_PATH.write_text(claude_text, encoding="utf-8")

    todos = _collect_todos(slash_names, "slash") + _collect_todos(subcommand_names, "subcommand")
    print(
        f"Regenerated routing reference: {len(slash_names)} slash commands, "
        f"{len(subcommand_names)} subcommands"
    )
    if todos:
        print("TODOs flagged in CLAUDE.md:")
        for t in todos:
            print(f"  - {t}")


if __name__ == "__main__":
    main()
