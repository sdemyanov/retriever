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
- Use the tool-returned preview path when one exists.
- Show the active query and filters clearly so the user knows what they are looking at.
