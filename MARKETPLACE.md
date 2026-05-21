# Retriever Marketplace Copy

## Short Description

Open source document intelligence and review for Claude Code.

## Medium Description

<<<<<<< HEAD
Retriever is a local-first document intelligence and review plugin for Claude. It turns a
folder, production, or mailbox export into a persistent review workspace, helps
you find key documents and communications, and exports the scoped result as a
table or archive.
=======
Retriever is an open source, local-first document intelligence plugin for
Claude Code. It turns folders, productions, mailbox exports, and other
supported document collections into a persistent workspace, helps you search,
preview, review, enrich, and analyze documents, and exports the current result
set as a table or archive.
>>>>>>> 1629fd2 (Refresh Claude Code docs and remove old setup bridge)

## First Run

Open Claude Code in the target folder, ask Retriever to ingest it, then ask
what you want next.

Example prompts:

- `/retriever:ingest`
- `Find documents mentioning indemnification`
- `Review this folder for hot docs`
- `Extract counterparties from the current contracts`
- `Export the current results`

## What Retriever Does

1. Build a document workspace
   Ingest local files, processed productions, PST/MBOX mailboxes, and similar
   document collections into a workspace-local Retriever index.
2. Search, review, enrich, and analyze documents
   Search, filter, page, sort, save scope, preview files, run first-pass
   review and triage, add metadata, and run OCR or extraction over selected
   result sets.
3. Export results and handoff artifacts
   Save the current scope as a CSV table, preview bundle, or portable archive
   for handoff, QA, or downstream loading.

## Best Fit

- Legal, investigations, and compliance
- Diligence and internal document analysis
- Document review, triage, and evidence workflows
- Knowledge work that starts from messy local document collections

## Interface

<<<<<<< HEAD
- `retriever:export` for CSV/archive export flows

## Trust Hook

Local-first by default. Original files stay in place.
=======
Users interact with Retriever through Claude Code. Install it once, open
Claude Code in a target workspace, and use `/retriever:*` commands or
plain-English requests.

## Trust Hook

Open source and local-first by default. Original files stay in place, and the
workspace state lives alongside the source corpus rather than in a remote
service.
>>>>>>> 1629fd2 (Refresh Claude Code docs and remove old setup bridge)
