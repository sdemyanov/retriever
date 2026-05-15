# Contributing

Retriever's shared plugin-level operating rules live in
`skills/routing/SKILL.md`, not in a root `CLAUDE.md`. Claude Code ignores a
plugin-root `CLAUDE.md`, so putting load-bearing routing or safety logic there
causes validation warnings and leaves published plugins without the intended
context.

## Build And Validate

Use these commands before packaging or publishing:

```bash
./build.sh
claude plugin validate .
python3 -m pytest tests/test_retriever_tools.py
```

`./build.sh` is the canonical release step. It synchronizes version metadata,
rebundles `skills/tool-template/tools.py`, refreshes the generated command
tables in `skills/routing/SKILL.md`, and rebuilds `retriever.plugin`.
