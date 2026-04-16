---
name: search-strategy
description: >
  Use this skill when presenting Retriever search results or translating user intent
  into structured search filters. It defines paging, sorting, preview-link behavior,
  and the default result formats for browsing versus targeted lookups.
metadata:
  version: "0.9.4"
---

# Retriever Search Strategy

Read [search-strategy.md](search-strategy.md) before presenting search results or constructing filters.

## Rules

- Prefer structured `--filter <field> <op> <value>` filters over raw SQL.
- Default to relevance sorting for keyword queries and `updated_at desc` for browse/filter-only views.
- When the user asks to show files, documents, or attachment children, make every shown result clickable with its preview/open link.
- Use the tool-returned preview path when one exists, and fall back to the source/native path when no generated preview exists.
- If the active filters constrain a field to one specific value across the shown rows, omit that redundant field from the displayed columns unless the user explicitly asks to see it.
- When `control_number` values are available, show them in a separate rightmost `Control number` column rather than embedding them in the title link.
- Show the active query and filters clearly so the user knows what they are looking at.
