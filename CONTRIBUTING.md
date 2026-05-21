# Contributing

Retriever's shared Claude Code operating rules live in the repo-root
`CLAUDE.md`. Keep user-facing Claude guidance there, and keep Retriever's
backend routing/reference material in `skills/routing/SKILL.md`.

For the repository map and architectural boundaries, see
[`ARCHITECTURE.md`](ARCHITECTURE.md). For the current test-suite shape and the
planned split of the consolidated regression file, see
[`TESTING.md`](TESTING.md).

## Build And Validate

Use these commands before packaging or publishing:

```bash
./build.sh
python3 -m pytest tests/test_retriever_tools.py
```

`./build.sh` is the canonical sync step. It synchronizes version metadata,
rebundles `skills/tool-template/tools.py`, refreshes the generated command
tables in `skills/routing/SKILL.md`, and rebuilds the bundled support
artifact `retriever.plugin`.

## Day-To-Day Workflow

Use this loop for normal development:

1. Edit the authored source or docs for the surface you are changing.
2. Run `./build.sh` if the change touches generated backend or command-table
   material.
3. Run `python3 -m pytest tests/test_retriever_tools.py`.
4. If you changed user-facing behavior, update the relevant docs:
   `README.md`, `docs/browse-reference.md`, `skills/routing/SKILL.md`, or
   installer text as appropriate.

## Generated Artifacts

Keep these boundaries in mind:

- `skills/tool-template/src/` is the authored generated-backend source of
  truth
- `skills/tool-template/tools.py` is generated output
- `retriever/` is the native Claude Code package surface
- installer output, version metadata, and tests should stay aligned with both

Avoid making hand edits to generated artifacts without also updating the source
that produces them.
