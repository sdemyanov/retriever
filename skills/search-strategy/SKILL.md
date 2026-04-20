---
name: search-strategy
description: >
  Use this skill when presenting Retriever search results or translating user intent
  into structured search filters. It defines paging, sorting, preview-link behavior,
  and the default result formats for browsing versus targeted lookups.
metadata:
  version: "0.9.5"
---

# Retriever Search Strategy

Read [search-strategy.md](search-strategy.md) before presenting search results or constructing filters.

## Rules

- Prefer SQL-like `--filter "<expression>"` filters over tuple-style field/operator/value input.
- The filter grammar applies to Retriever's logical document fields, including supported virtual fields such as `production_name`, `is_attachment`, and `has_attachments`.
- Default to relevance sorting for keyword queries, Bates ordering for Bates lookups, and `date_created desc` with nulls last for browse/filter-only views.
- Use Retriever's canonical search CLI flags: `--filter`, `--sort`, `--order`, `--page`, and `--per-page`.
- Map "show N" style requests to `--page 1 --per-page N`; do not invent `--limit`.
- Use canonical field names such as `date_created`; do not invent variants like `created_date`.
- For persistent browsing, use the slash surface: `/search`, `/bates`, `/filter`, `/dataset`, `/from-run`, `/scope`, `/sort`, `/page`, `/next`, and `/previous`.
- Unless the user explicitly asks for a different layout, show document results in the standard table with columns `Type`, `Title`, `Author`, `Datetime (UTC)`, and `Control number` when available.
- When the user asks to show files, documents, or attachment children, make every shown result clickable with its preview/open link.
- The `Title` cell is always the clickable cell in the standard format; do not create a separate `Link` column.
- Use the tool-returned preview path when one exists, and fall back to the source/native path when no generated preview exists.
- If the active filters constrain a field to one specific value across the shown rows, you may omit only that one redundant field from the standard columns unless the user explicitly asks to see it.
- When `control_number` values are available, show them in a separate rightmost `Control number` column rather than embedding them in the title link.
- Show the active scope, sort, and page clearly so the user knows what they are looking at.
- Use Retriever's default compact search payload first; rerun with `--verbose` only when you need attachment child rows, alternate preview targets, or extended metadata that compact mode omits.
- When a search or DB step yields multiple independent follow-up paths, issue those reads/searches in one assistant turn instead of one-by-one.
