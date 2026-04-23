# Retriever plugin — agent behavior

<!-- TEST_MARKER: retriever-plugin-claudemd-v1 -->

When working in a Retriever workspace (a directory containing `.retriever/`), prefer higher-tier surfaces over lower ones:

1. **Tier 1 — `retriever:*` skills.** If a `retriever:*` skill covers the intent, invoke it instead of the underlying command.
2. **Tier 2 — `retriever_tools.py slash` commands.** If no Tier 1 skill fits, use a slash command via the workspace tool.
3. **Tier 3 — `retriever_tools.py` subcommands.** If no slash form covers the intent, use a named subcommand.
4. **Tier 4 — direct `.retriever/retriever.db` SQLite.** Last resort; leave a short "plugin gap" note so the user sees which request is not yet covered by a higher-tier surface.

Do not skip tiers. Do not drop to a lower tier because it is more familiar or convenient.

For the authoritative intent-to-skill table and the current inventory of slash commands and subcommands, see `skills/routing/SKILL.md`.
