# Privacy and Trust

## Retriever core

- Retriever is local-first.
- Original source files stay where they are; Retriever does not rewrite them.
- Workspace state, previews, logs, and exports live under `.retriever/` in the
  chosen workspace.
- Retriever's shared runtime lives under the plugin directory, not inside the
  user's source folders.

## Connector boundary

- Google Drive is optional and user-enabled in Claude or Cowork.
- Treat the connector as a source-access layer, not as Retriever's storage
  layer.
- Use the connector when you need to locate or inspect Drive documents, or when
  you want to save outputs back to Drive.
- If you need persistent review state, ingest the selected material into a
  local Retriever workspace.

## Tell the user clearly

- whether work is happening against local workspace files or live connector data
- where exported files will be written
- whether any action writes back to Drive
- that Retriever does not silently mirror or background-sync Google Drive

## Good default posture

- prefer local workspaces for repeatable review
- use the smallest useful connector scope
- avoid creating fields or exports the user did not ask for
