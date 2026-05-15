# Connectors

Retriever's product layer currently assumes one optional connector:
Google Drive.

## Why Google Drive

Many first-time users already keep the relevant documents in Drive. The Google
Drive connector helps Claude find the right files quickly and can save final
exports back to Drive, while Retriever remains the local workspace for
persistent indexing, scope, tagging, and export.

## What the connector is for

- locate candidate files and folders in Drive
- inspect a bounded set of Drive documents for quick triage
- move the right material into a local Retriever workspace for deeper review
- save CSV or ZIP outputs back to Drive when the user wants a shared
  destination

## What it is not

- not a background sync engine
- not an automatic full-Drive mirror
- not a replacement for Retriever's local `.retriever/` workspace state

## Recommended workflow

1. Use Google Drive to find the relevant folder or documents.
2. If the task is small and one-off, answer directly from Drive.
3. If the task is iterative review, create a local workspace and ingest the
   selected files into Retriever.
4. Use Retriever to search, page, tag, save scope, and export.
5. Optionally save the final CSV or ZIP back to Drive.

## If the connector is unavailable

Fall back to the standard local-folder workflow. Ask the user to point
Retriever at a local folder, processed production, PST, or MBOX source
instead.

See [PRIVACY.md](PRIVACY.md) for the trust boundary between the local workspace
and connector-backed source access.
