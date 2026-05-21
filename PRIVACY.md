# Privacy and Trust

## Retriever core

- Retriever is local-first.
- Original source files stay where they are; Retriever does not rewrite them.
- Workspace state, previews, logs, and exports live under `.retriever/` in the
  chosen workspace.
- Retriever's shared runtime lives under the plugin directory, not inside the
  user's source folders.

## Tell the user clearly

- which local workspace or source folder Retriever is using
- where exported files will be written
- whether any action changes workspace state or generated outputs

## Good default posture

- prefer local workspaces for repeatable review
- avoid creating fields or exports the user did not ask for
