---
name: run-job
description: >
  Use this skill when the user wants to execute a planned Retriever processing run
  inline or in the background. It decides between inline execution and spawning a
  subagent, and drives the claim / complete / heartbeat loop against the processing DB.
metadata:
  version: "0.9.5"
---

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

- Start with `run-status`.
- Use `run.worker.recommended_execution_mode` as the default:
  - `inline` means stay in the current conversation
  - `background` means spawn a subagent unless the user explicitly wants inline execution
- Prefer a subagent when:
  - the user explicitly asks for background execution
  - or `run.worker.recommended_execution_mode` is `background`

When unsure, inspect `run-status` first.

## Execution Loop

Whether running inline or in a subagent, use the same small-batch loop.

The main orchestration command is:

1. `prepare-run-batch --run-id ... --claimed-by ... [--limit ...]`

It returns:

- current `run` status
- `worker` hints
- a claimed `batch` of run items with their execution contexts already loaded

Read `worker.next_action`:

- `process_batch`: work the returned batch
- `finalize_ocr`: call `finalize-ocr-run`
- `stop`: stop the worker loop cleanly

If `worker.stop_reason` is `canceled`, exit immediately.

## Batch Processing

For each item in `batch`:

1. Read `batch[i].context.execution.task_prompt`
2. Use the provided input payload to perform the capability work in-turn
3. Use `batch[i].context.execution.completion_template` as the exact shape for completion
4. Call `complete-run-item ...` or `fail-run-item ...`

Use these supporting commands during the loop:

- `heartbeat-run-items --run-id ... --claimed-by ...` between long batches
- `run-status --run-id ...` whenever you need an external progress check
- `finalize-ocr-run --run-id ...` when `worker.next_action` says `finalize_ocr`

Use small batches only. The worker hints expose:

- `worker.recommended_batch_size`
- `worker.recommended_max_batches_per_worker`

Do not use `execute-run` for the normal Cowork path. That command remains the legacy direct executor for deterministic tests and future external-provider work.

## Cancellation

- `prepare-run-batch` already reflects cancellation in `worker.next_action` / `worker.stop_reason`.
- Between batches, if `worker.stop_reason` becomes `canceled`, stop claiming work and exit cleanly.

## Background Mode

In background mode:

- spawn a subagent
- give it the `run_id`
- tell it to call `prepare-run-batch` in a loop
- tell it to process at most `worker.recommended_max_batches_per_worker` batches before returning
- tell it to write results through the retriever CLI/tool commands
- tell it to return only a short summary:
  - processed count
  - failed count
  - whether OCR finalization was performed
  - final run status

The main conversation should use `run-status` to inspect progress. Do not stream per-document OCR or extraction output back into chat.

## Inline Mode

In inline mode:

- stay in the current conversation
- process one prepared batch at a time
- if the run remains active after one batch, continue by calling `prepare-run-batch` again
- if the run is large enough to become noisy, switch to background mode on the next batch boundary
