---
name: parsing
description: >
  Use this skill when Retriever needs to ingest, parse, normalize, or preview
  supported document types. It defines file-type support, encoding behavior,
  preview artifact rules, and per-file failure isolation.
metadata:
  version: "0.9.5"
---

# Retriever Parsing

Use this skill whenever a task changes or depends on document extraction behavior.

## Required reference

Read [parsing.md](parsing.md) before changing ingest logic or parser dependencies.

## Rules

- Keep ingest transactional per file so one bad document never blocks the rest.
- Normalize decoded text to UTF-8 before storing it.
- Prefer preserving the original file and writing previews under `.retriever/previews/`.
- Treat unsupported or corrupt files as structured failures, not fatal batch errors.
