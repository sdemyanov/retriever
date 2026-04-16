# Plugin Install Smoke Test

This repo now contains the smallest Retriever plugin needed to validate installation and one working skill.

For the full end-to-end manual pass, see [MANUAL_TEST_PLAN.md](/Users/sergey/Projects/retriever-plugin/MANUAL_TEST_PLAN.md).

## Included

- plugin manifest: [.claude-plugin/plugin.json](/Users/sergey/Projects/retriever-plugin/.claude-plugin/plugin.json)
- smoke-test skill: [skills/ping/SKILL.md](/Users/sergey/Projects/retriever-plugin/skills/ping/SKILL.md)
- Phase 0 runtime check skill: [skills/doctor/SKILL.md](/Users/sergey/Projects/retriever-plugin/skills/doctor/SKILL.md)
- Phase 1 workspace skill: [skills/workspace/SKILL.md](/Users/sergey/Projects/retriever-plugin/skills/workspace/SKILL.md)
- Phase 1 schema skill: [skills/schema/SKILL.md](/Users/sergey/Projects/retriever-plugin/skills/schema/SKILL.md)
- Phase 1 tool template skill: [skills/tool-template/SKILL.md](/Users/sergey/Projects/retriever-plugin/skills/tool-template/SKILL.md)
- local test marketplace: [test-marketplace/.claude-plugin/marketplace.json](/Users/sergey/Projects/retriever-plugin/test-marketplace/.claude-plugin/marketplace.json)
- local marketplace plugin link: [test-marketplace/plugins](/Users/sergey/Projects/retriever-plugin/test-marketplace/plugins)

## Validation

Validate the plugin manifest:

```bash
claude plugin validate /Users/sergey/Projects/retriever-plugin
```

Validate the local test marketplace:

```bash
claude plugin validate /Users/sergey/Projects/retriever-plugin/test-marketplace
```

## Fastest Local Load

This does not install globally. It loads the plugin for one Claude session:

```bash
claude --plugin-dir /Users/sergey/Projects/retriever-plugin
```

Then invoke the smoke test skill. If namespaced plugin skill invocation is available, run:

```text
/retriever:ping
```

If the environment prefers natural-language invocation, ask:

```text
retriever ping
```

## Local Install Flow

If you want to test the full install path with a local marketplace:

1. Start `claude` from any directory.
2. Add the test marketplace:

```text
/plugin marketplace add /Users/sergey/Projects/retriever-plugin/test-marketplace
```

3. Install the plugin:

```text
/plugin install retriever@retriever-local-test-marketplace
```

4. Restart Claude.
5. Invoke the smoke test skill:

```text
/retriever:ping
```

Expected response:

```text
Retriever plugin smoke test OK.
Version: 0.6.0
Skill: ping
```

Optional:

- pass additional context like `retriever ping hello` and confirm the skill echoes it on a `Note:` line

## Phase 0 Skill

Once the smoke test is working, run:

```text
/retriever:doctor
```

This checks the active plugin runtime for:

- Python availability
- pip availability
- SQLite version
- FTS5 support
- basic platform identification

Or invoke it in plain language:

```text
retriever doctor
```

Use it as the first real runtime check inside Cowork before moving deeper into Phase 0.

## Implementation Note

The local marketplace points at `./plugins/retriever` because the Claude validator rejects marketplace entries that reference parent directories with `..`.
