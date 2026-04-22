---
name: ping
description: >
  This skill should be used when the user says "retriever ping",
  "test retriever", "is retriever installed", or "check retriever".
  Confirms the Retriever plugin is installed and responding.
metadata:
  version: "0.18.0"
---

# Retriever Ping

Reply in plain text with exactly this structure:

```
Retriever plugin smoke test OK.
Version: 0.18.0
Skill: ping
```

If the user supplied additional context, add one extra line:

```
Note: <user's context>
```

Keep the response short. Do not use tools.
