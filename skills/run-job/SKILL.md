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

For long unattended runs, use the supervision hints from `run-status`:

- `run.supervision.should_schedule_wakeup`
- `run.supervision.wake_interval_seconds`
- `run.supervision.suggested_worker_count`
- `run.supervision.spawn_additional_worker_count`
- `run.supervision.max_parallel_workers`

If `should_schedule_wakeup` is true, keep a thread wake alive on the suggested interval (currently 60 seconds). Stop scheduling wakes when it becomes false.

## Unattended Continuation

For unattended long runs, use a thread heartbeat automation attached to the current conversation.

When to create or update it:

- the user explicitly asks to run in the background or keep going unattended
- `run.supervision.should_schedule_wakeup` is true
- the run still has pending work, active workers, or pending OCR / image-description finalization

How to configure it:

- create or update a thread heartbeat on this thread
- use the interval from `run.supervision.wake_interval_seconds`
- default cadence is once per minute
- keep the prompt short and durable:
  - resume Retriever run `<run_id>`
  - inspect `run-status`
  - if work remains, continue orchestration using this skill
  - if the run is done or canceled, stop the heartbeat and report a short summary

When to stop it:

- `run.supervision.should_schedule_wakeup` is false
- the run status is `completed`, `failed`, or `canceled`
- there are no pending/running items and no pending finalization actions

Do not build a second supervisor inside Python. The heartbeat is the wake mechanism; the skill remains the orchestrator.

## Execution Loop

Whether running inline or in a subagent, use the same small-batch loop.

The main orchestration command is:

1. `prepare-run-batch --run-id ... --claimed-by ... --launch-mode ... [--worker-task-id ...] [--max-batches ...] [--limit ...]`

It returns:

- current `run` status
- `worker` hints
- a claimed `batch` of run items with their execution contexts already loaded

Read `worker.next_action`:

- `process_batch`: work the returned batch
- `finalize_ocr`: call `finalize-ocr-run`
- `finalize_image_description`: call `finalize-image-description-run`
- `handoff`: stop this worker cleanly and let a fresh inline step or subagent continue
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
- `finish-run-worker --run-id ... --claimed-by ... --worker-status ...` before the worker exits
- `run-status --run-id ...` whenever you need an external progress check
- `finalize-ocr-run --run-id ...` when `worker.next_action` says `finalize_ocr`
- `finalize-image-description-run --run-id ...` when `worker.next_action` says `finalize_image_description`

Use small batches only. The worker hints expose:

- `worker.recommended_batch_size`
- `worker.recommended_max_batches_per_worker`
- `worker.after_batch_action`
- `worker.should_exit_after_batch`

Do not use `execute-run` for the normal Cowork path. That command remains the legacy direct executor for deterministic tests and future external-provider work.

## Cancellation

- `prepare-run-batch` already reflects cancellation in `worker.next_action` / `worker.stop_reason`.
- Between batches, if `worker.stop_reason` becomes `canceled`, stop claiming work and exit cleanly.
- If a user explicitly requests force cancel, use `cancel-run --force` and inspect the returned `force_stop_task_ids`.

## Background Mode

In background mode:

- spawn a subagent
- give it the `run_id`
- give it a stable `claimed_by` worker id
- pass the subagent task id as `--worker-task-id` when available
- tell it to call `prepare-run-batch` in a loop
- tell it to process at most `worker.recommended_max_batches_per_worker` batches before returning
- tell it to write results through the retriever CLI/tool commands
- tell it to call `finish-run-worker --worker-status completed|stopped|failed` before it exits
- tell it to return only a short summary:
  - processed count
  - failed count
  - whether OCR finalization was performed
  - whether image-description finalization was performed
  - final run status

The main conversation should use `run-status` to inspect progress. Do not stream per-document OCR or extraction output back into chat.

Use a few durable workers, not one subagent per document.

- Let `run.supervision.spawn_additional_worker_count` guide how many extra subagents to start on a wake.
- Never exceed `run.supervision.max_parallel_workers`.
- A good pattern is to keep 1-4 workers alive for large queues, let each worker process up to its batch cap, then wake again and top the pool back up if needed.
- If the runtime exposes no native progress indicator, keep progress DB-backed and send only short completion / failure summaries in-thread.

On each wake:

1. Start with `run-status`.
2. If `run.supervision.should_schedule_wakeup` is false:
   - stop/delete the heartbeat
   - report final status if useful
   - do not spawn new workers
3. If `run.supervision.recommended_action` is `finalize_ocr` or `finalize_image_description`:
   - perform that finalization first
   - then re-check `run-status`
4. If `run.supervision.recommended_action` is `spawn_background_worker`:
   - spawn up to `run.supervision.spawn_additional_worker_count` subagents
   - never exceed `run.supervision.max_parallel_workers`
   - each worker gets:
     - the same `run_id`
     - a stable unique `claimed_by`
     - `launch_mode=background`
     - its own task id when available
     - the recommended batch cap
5. If `run.supervision.recommended_action` is `wait`:
   - leave existing workers alone
   - keep the heartbeat alive
6. If `run.supervision.recommended_action` is `stop`:
   - do not spawn workers
   - stop/delete the heartbeat

The agent may choose the exact worker count dynamically, but it must stay within the runtime bounds from `run.supervision`.

If `prepare-run-batch` returns `next_action = handoff`, stop this worker with `finish-run-worker --worker-status stopped` and let a fresh subagent continue from the DB state.

## Inline Mode

In inline mode:

- stay in the current conversation
- process one prepared batch at a time
- if the run remains active after one batch, continue by calling `prepare-run-batch` again
- if the run is large enough to become noisy, switch to background mode on the next batch boundary
- if you intentionally stop at a batch boundary, call `finish-run-worker --worker-status stopped`
