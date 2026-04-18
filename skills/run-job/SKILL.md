# Run Job

Use this skill when the user wants to execute a planned Retriever processing run inline or in the background.

## Purpose

The processing DB is the source of truth. This skill decides whether to:

- run the work inline in the current conversation, or
- spawn a subagent for background execution

The Python CLI does not own a `--background` flag.

## Inputs

Expected user inputs:

- a `run_id`, or enough context to identify one
- optional preference to run inline or in the background

## Decision Rule

- Prefer inline execution for small runs.
- Prefer a subagent when:
  - the user explicitly asks for background execution
  - or the run is large enough that it would pollute the current conversation

When unsure, inspect `run-status` first.

## Execution Loop

Whether running inline or in a subagent, use the same tool commands:

1. `claim-run-items --run-id ... --claimed-by ... --limit ...`
2. For each returned item:
   - `get-run-item-context --run-item-id ...`
   - use `context.execution.task_prompt` plus the provided input payload to perform the capability work in-turn
   - use `context.execution.completion_template` as the shape for `complete-run-item`
   - `complete-run-item ...` or `fail-run-item ...`
3. `heartbeat-run-items --run-id ... --claimed-by ...` between batches
4. `run-status --run-id ...` to check for completion or cancellation

Claim only small batches at a time. A good default is `5`.

## Cancellation

- Before claiming a new batch, check `run-status`.
- If the run is canceled, stop claiming work and exit cleanly.

## Background Mode

In background mode:

- spawn a subagent
- give it the `run_id`
- tell it to loop in small batches
- tell it to write results through the retriever CLI/tool commands
- tell it to return a short summary only

The main conversation should use `run-status` to inspect progress. Do not stream per-document OCR or extraction output back into chat.
