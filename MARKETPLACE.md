# Retriever Marketplace Copy

## Short Description

Build a review workspace, find the documents that matter, and export a
shareable review set.

## Medium Description

Retriever is a local-first document intelligence and review plugin for Claude. It turns a
folder, production, mailbox export, or bounded Google Drive set into a
persistent review workspace, helps you find key documents and communications,
and exports the scoped result as a table or archive.

## First Run

Put the documents for one matter or review into a single folder, ask Retriever
to ingest that folder, then ask what you want next.

Example prompts:

- `Ingest /path/to/folder`
- `Review this folder for hot docs`
- `Export the current review set`

## The Three Jobs

1. Build a review workspace
   Ingest local files, processed productions, PST/MBOX mailboxes, and similar
   review sets into a workspace-local Retriever index.
2. Find the documents that matter
   Search, filter, page, sort, save scope, and triage hot docs, key emails,
   privilege candidates, and contract-risk material.
3. Export a review set
   Save the current scope as a CSV table or portable archive for handoff, QA, or
   downstream loading.

## Best Fit

- Legal review and litigation support
- Investigations and compliance
- Diligence and internal document analysis

## Guided Surfaces

- `retriever-legal` agent for first-pass legal review
- `retriever:export` for CSV/archive export flows

## Trust Hook

Local-first by default. Original files stay in place. Optional Google Drive
connector use is user-directed rather than automatic mirroring.
