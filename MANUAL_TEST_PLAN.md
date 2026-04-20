# Manual Test Plan

## Scope

This checklist covers the conversation model, shared browsing previews, and explicit export-preview materialization.

## Suggested Fixtures

- a two-message loose email chain
- an extracted Slack export with one rooted thread crossing a day boundary
- a PST containing at least one email and one chat-like / Teams-style item
- one document with a real attachment child

## Checklist

### Dataset routing

- Run `list-datasets` after ingesting a Slack export and confirm it appears as its own `slack_export` dataset.
- Confirm `users.json`, `channels.json`, `groups.json`, `dms.json`, `mpims.json`, and `canvases.json` do not appear as loose searchable documents.
- Confirm ordinary filesystem documents still land in the generic filesystem dataset.

### Conversation grouping

- Ingest the loose email chain and confirm all messages share one `conversation_id`.
- Ingest PST email and confirm it follows the same email conversation assignment rules.
- Ingest PST chat-like items and confirm documents from the same group, channel, or DM share one `conversation_id`.
- Ingest the Slack export and confirm all day docs and child `reply_thread` docs for the same channel or DM share one `conversation_id`.

### Browse behavior

- Open an email document and confirm it resolves to the shared conversation preview at `#doc-<document_id>`.
- Open a Slack day doc and confirm it resolves to the shared conversation preview, not to a standalone per-document preview.
- Open a Slack `reply_thread` child doc and confirm it resolves to the same shared preview with the correct anchor.
- Confirm the TOC page links to the expected monthly chat segments or yearly email segments.
- Scroll through the shared preview and confirm multiple related documents remain in chronological order with stable anchors.

### Child-document behavior

- Confirm real file attachments still appear under `attachments`.
- Confirm Slack `reply_thread` children appear under `child_documents` and are not labeled as attachments.
- Confirm parent/child navigation between a day doc and its `reply_thread` child works in both directions.

### Export preview materialization

- Run `export-previews <workspace> email-preview --doc-id <reply-doc-id>`.
- Confirm the exported HTML unit includes the selected email plus earlier messages in the same chain.
- Confirm the selected email target resolves to the exported unit file plus `#doc-<document_id>`.
- Run `export-previews <workspace> slack-preview --doc-id <day-doc-id> --doc-id <reply-thread-doc-id>`.
- Confirm contiguous selected Slack documents are emitted into one export unit.
- Run `export-previews <workspace> slack-preview-split --doc-id <noncontiguous-doc-a> --doc-id <noncontiguous-doc-b>`.
- Confirm noncontiguous selections produce separate export unit HTML files.

### Re-ingest and correction behavior

- Manually split or merge a conversation, then re-ingest and confirm `conversation_assignment_mode = manual` prevents overwrite.
- Clear the manual assignment and confirm re-ingest restores automatic conversation assignment.
- Re-ingest a changed source and confirm `control_number` and family numbering remain stable while `conversation_id` is allowed to change for `auto` documents.

## Notes

- This plan records what should be validated manually; it does not imply the checklist has already been executed.
- Quantitative scorecards still belong in the evaluation workflow rather than this manual checklist.
