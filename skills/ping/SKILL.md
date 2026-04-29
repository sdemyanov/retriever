---
name: ping
description: >
  This skill should be used when the user says "retriever ping",
  "test retriever", "is retriever installed", or "check retriever".
  Confirms the Retriever plugin is installed and responding.
metadata:
  version: "1.1.14"
---

> Operates under `retriever:routing`. If the user's intent actually fits a different tier — another `retriever:*` skill, a Tier 2 slash, a Tier 3 `tools.py` subcommand, or (last resort) direct DB access — stop and re-route against the ladder before continuing.

# Retriever Ping

Reply in plain text with exactly this structure, substituting `<version>` with the value of `metadata.version` from this skill's frontmatter:

```
Retriever plugin smoke test OK.
Version: 1.1.14
Skill: ping
```

If the user supplied additional context, add one extra line:

```
Note: <user's context>
```

Keep the response short. Do not use tools.
