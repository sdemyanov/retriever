---
name: legal-review
description: >
  Use this skill when the user wants a first-pass legal review workflow rather
  than low-level Retriever mechanics — for example, "review this matter",
  "find hot docs", "triage these contracts", "build a privilege set", or
  "export a first-pass review set".
metadata:
  version: "1.1.16"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Legal Review

Use this skill as a thin wrapper over Retriever's core review engine. The user
should not have to learn `/search`, `/filter`, `/scope`, or export syntax
before they can do useful legal work.

## Primary jobs

1. Turn a folder, production, PST, or bounded Google Drive set into a Retriever
   workspace.
2. Find the documents or conversations that matter for first-pass legal review.
3. Export a review set or handoff artifact.

## First thing to tell the user

When the user is new or the next step is unclear, start with the simplest
Retriever instruction:

- put the documents for one matter or review into a single local folder
- ask Retriever to ingest that folder
- then ask for the next job in plain English

Good examples:

- "Ingest /path/to/folder"
- "Review this folder for hot docs"
- "Export the current review set"

## Operating workflow

1. Start with the right intake path:
   - local folder -> `retriever:workspace` + `retriever:ingest`
   - processed production -> `retriever:ingest-production`
   - PST -> `retriever:pst`
   - Google Drive -> use Drive to locate/select the material, then move the
     bounded set into `retriever:workspace` + `retriever:ingest` for
     persistent review; use live Drive access only for quick one-off questions
2. Keep the language plain. Prefer "hot docs", "key emails", "privilege
   candidates", "contract terms", and "review set" over internal command names.
3. For discovery and narrowing, route to the existing core skills:
   `retriever:search`, `retriever:dataset`, `retriever:filter`,
   `retriever:bates`, `retriever:scope`, and paging/sort controls.
4. If the user wants first-pass coding, use `retriever:field` and
   `retriever:fill`, but suggest new custom fields before creating them unless
   the request clearly implies field creation.
5. Sensible first-pass field suggestions include `privilege_status`, `hot_doc`,
   `issue_tag`, and `review_notes`.
6. For summaries or structured extraction over a scoped set, use
   `retriever:run-job` when a processing run is more appropriate than ad hoc
   browsing.
7. For handoff artifacts, use `retriever:export`.

## Response style

- Lead with the next review step, not a product lecture.
- When returning Retriever listings, preserve the tool-rendered output exactly.
- Keep non-list responses to one or two short paragraphs unless the user asks
  for depth.
