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
- Use Retriever's canonical search CLI flags: `--filter`, `--sort`, `--order`, `--page`, `--per-page`, `--columns`, and `--mode`.
- Map "show N" style requests to `--page 1 --per-page N`; do not invent `--limit`.
- Use canonical field names such as `date_created`; do not invent variants like `created_date`.
- For persistent browsing, use the slash surface: `/search`, `/bates`, `/filter`, `/dataset`, `/from-run`, `/scope`, `/sort`, `/page`, `/next`, `/previous`, `/page-size`, and `/columns`.
- Bare slash commands are read-only state inspection when supported: `/scope`, `/dataset`, `/sort`, `/page`, `/page-size`, and `/columns`.
- Use `list` subcommands for discoverability: `/scope list`, `/dataset list`, `/sort list`, and `/columns list`.
- Unless the user explicitly asks for a different layout, show document results using the active display column set. The default when no override is present is `content_type`, `title`, `author`, `date_created`, and `control_number`.
- When the user asked to see the table itself, prefer `--mode view` and forward the tool's `rendered_markdown` as the entire reply. Do not add any text before or after the table.
- In compose mode, keep summaries short by default. Use one short paragraph unless the user asked for a detailed write-up or the material genuinely needs a second paragraph for chronology or contrast.
- In compose mode, anchor the main answer to the top matching documents instead of broad collection-wide storytelling. Name the most relevant `control_number` items when they support the conclusion.
- Do not add side remarks about unrelated hits, dataset noise, or collection quality unless the user asked for that analysis or it materially changes the answer.
- When the user asks to show files, documents, or attachment children, make every shown result clickable with its preview/open link.
- The `Title` cell is always the clickable cell in the standard format; do not create a separate `Link` column.
- Use the tool-returned preview path when one exists, and fall back to the source/native path when no generated preview exists.
- When `control_number` is present in the active column set, keep it in its own column rather than embedding it in the title link.
- Show the active scope, sort, and page clearly so the user knows what they are looking at.
- Use Retriever's default compact search payload first; rerun with `--verbose` only when you need attachment child rows, alternate preview targets, or extended metadata that compact mode omits.
- When a search or DB step yields multiple independent follow-up paths, issue those reads/searches in one assistant turn instead of one-by-one.
