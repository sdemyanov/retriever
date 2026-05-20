# Contributing

Retriever's shared Claude Code operating rules now live in the repo-root
`CLAUDE.md`. Keep user-facing Claude guidance there, and keep Retriever's
backend routing/reference material in `skills/routing/SKILL.md`.

## Build And Validate

Use these commands before packaging or publishing:

```bash
./build.sh
python3 -m pytest tests/test_retriever_tools.py
```

`./build.sh` is the canonical sync step. It synchronizes version metadata,
rebundles `skills/tool-template/tools.py`, refreshes the generated command
tables in `skills/routing/SKILL.md`, and rebuilds the legacy compatibility
artifact `retriever.plugin`.
