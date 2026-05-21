# Privacy and Trust

## Claude Code account privacy

IMPORTANT: IF YOU HANDLE SENSITIVE, CLIENT, OR INTERNAL DOCUMENTS, TURN OFF
`Help improve Claude` IN `Settings -> Privacy` BEFORE USING RETRIEVER.
Anthropic says that when this setting is off, new Claude chats and coding
sessions are not used for future model training, although flagged
conversations may still be used for trust and safety purposes.
Review the [Anthropic privacy policy](https://www.anthropic.com/privacy) and
confirm it satisfies your practice, client, regulatory, and organizational
requirements before using Retriever with sensitive material.

## Zero Data Retention

Zero Data Retention (ZDR) is available for Claude Code on Claude for
Enterprise. Anthropic says ZDR is enabled per organization and covers Claude
Code inference on Claude for Enterprise. See [Zero data
retention](https://code.claude.com/docs/en/zero-data-retention) for the
current scope, disabled features, and enablement details.

## Retriever storage model

- Retriever is open source and local-first.
- Original source files stay where they are; Retriever does not rewrite them.
- Workspace state, previews, logs, and exports live under `.retriever/` in the
  chosen workspace.
- Retriever's shared runtime lives under the plugin directory, not inside the
  user's source folders.

<<<<<<< HEAD
## Tell the user clearly

- which local workspace or source folder Retriever is using
- where exported files will be written
- whether any action changes workspace state or generated outputs

## Good default posture

- prefer local workspaces for repeatable review
- avoid creating fields or exports the user did not ask for
=======
## Good default posture

- prefer the smallest useful workspace or document set
- avoid putting sensitive material into Claude Code unless the privacy posture
  is acceptable for that account or organization
- be explicit about where exports are written
- be explicit when processing jobs create new previews, text revisions, fields,
  or exports
>>>>>>> 1629fd2 (Refresh Claude Code docs and remove old setup bridge)
