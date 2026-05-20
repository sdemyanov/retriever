from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import mimetypes
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Sequence

from ._compat import load_tools_module


CLAUDE_CODE_PROVIDER_NAMES = {"claude_code", "cowork_agent"}
OPENAI_RESPONSES_PROVIDER_NAMES = {"openai", "openai_responses"}
STATIC_JSON_PROVIDER_NAMES = {"builtin_static_json", "static_json"}
STATIC_TEXT_PROVIDER_NAMES = {"builtin_static_text", "static_text"}
NATIVE_COMMAND_NAMES = {"run", "translate", "extract", "ocr", "describe-images"}
TEXT_MODEL_ENV_VARS = ("RETRIEVER_OPENAI_TEXT_MODEL", "RETRIEVER_OPENAI_MODEL", "OPENAI_MODEL")
VISION_MODEL_ENV_VARS = ("RETRIEVER_OPENAI_VISION_MODEL", "RETRIEVER_OPENAI_MODEL", "OPENAI_MODEL")
CLAUDE_TEXT_MODEL_ENV_VARS = ("RETRIEVER_CLAUDE_TEXT_MODEL", "RETRIEVER_CLAUDE_MODEL", "ANTHROPIC_MODEL")
CLAUDE_VISION_MODEL_ENV_VARS = ("RETRIEVER_CLAUDE_VISION_MODEL", "RETRIEVER_CLAUDE_MODEL", "ANTHROPIC_MODEL")
DEFAULT_NATIVE_STEP_BUDGET_SECONDS = 35
DEFAULT_NATIVE_CLAIM_STALE_SECONDS = 300
NATIVE_PROGRESS_INTERVAL_SECONDS = 60.0
DEFAULT_CLAUDE_CODE_MAX_TURNS = 6
SUPPORTED_OPENAI_IMAGE_MIME_TYPES = {
    "image/gif",
    "image/jpeg",
    "image/png",
    "image/webp",
}
SUPPORTED_CLAUDE_CODE_IMAGE_SUFFIXES = {
    ".gif",
    ".jpeg",
    ".jpg",
    ".png",
    ".webp",
}


def native_command_name(argv: Sequence[str]) -> str | None:
    index = 0
    while index < len(argv):
        token = str(argv[index])
        if token == "--output":
            index += 2
            continue
        if token.startswith("--output="):
            index += 1
            continue
        if token == "--human":
            index += 1
            continue
        if token.startswith("-"):
            return None
        return token
    return None


def handles_native_command(argv: Sequence[str]) -> bool:
    return native_command_name(argv) in NATIVE_COMMAND_NAMES


def render_native_help_section() -> str:
    return (
        "\nNative Claude-first processing commands:\n"
        "  run              Resume or finish an existing processing run\n"
        "  translate        Create a translation run and execute it to completion\n"
        "  extract          Create a structured extraction run and publish its outputs\n"
        "  ocr              Create an OCR run and execute it to completion\n"
        "  describe-images  Create an image-description run and execute it to completion\n"
        "\nUse `python -m retriever <command> --help` for the native command details.\n"
    )


def add_selector_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--doc-id", dest="document_ids", action="append", type=int, help="Document id (repeatable)")
    parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable SQL-like filter expression",
    )
    parser.add_argument("--bates", help="Bates token or Bates range")
    parser.add_argument(
        "--dataset",
        dest="dataset_names",
        action="append",
        help="Exact dataset name (repeatable)",
    )
    parser.add_argument("--from-run-id", type=int, help="Restrict to documents already present in a prior run")
    parser.add_argument(
        "--select-from-scope",
        action="store_true",
        help="AND-narrow the selector with the persisted workspace scope",
    )
    parser.add_argument("--keyword", dest="query", default="", help="Optional keyword query text")
    parser.add_argument(
        "--family-mode",
        choices=("exact", "with_family"),
        default="exact",
        help="Whether to include attachment/document family members for matched seeds",
    )
    parser.add_argument("--limit", dest="seed_limit", type=int, help="Limit the directly matched seed set")


def build_parser(tools: Any) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m retriever",
        description="Retriever native processing commands",
    )
    parser.add_argument(
        "--output",
        choices=("json", "human"),
        default="human",
        help="Output mode. Defaults to human-readable summaries.",
    )
    parser.add_argument(
        "--human",
        action="store_true",
        help="Shortcut for `--output human`.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Resume or finish a processing run to a terminal state")
    run_parser.add_argument("workspace", help="Workspace root path")
    run_parser.add_argument("--run-id", type=int, required=True, help="Existing run id")
    run_parser.add_argument("--claimed-by", help="Stable worker/session id to use for this runner")
    run_parser.add_argument(
        "--as-worker",
        action="store_true",
        help="Process one native worker session and stop at handoff or terminal state",
    )
    run_parser.add_argument(
        "--budget-seconds",
        type=int,
        default=DEFAULT_NATIVE_STEP_BUDGET_SECONDS,
        help="Per-step budget used when advancing the resumable backend",
    )
    run_parser.add_argument(
        "--claim-stale-seconds",
        type=int,
        default=DEFAULT_NATIVE_CLAIM_STALE_SECONDS,
        help="How long this runner keeps claims alive before another caller can reclaim them",
    )
    run_parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of run items to claim when preparing a batch",
    )
    run_parser.add_argument(
        "--max-steps",
        type=int,
        help="Optional maximum number of backend step calls before stopping with an error",
    )
    run_parser.add_argument(
        "--no-publish",
        action="store_true",
        help="Do not auto-publish bound structured extraction outputs after completion",
    )

    translate_parser = subparsers.add_parser("translate", help="Create a translation run and execute it to completion")
    translate_parser.add_argument("workspace", help="Workspace root path")
    translate_parser.add_argument("--target-language", required=True, help="Target language name or code")
    translate_parser.add_argument("--instruction", help="Optional extra translation instruction")
    translate_parser.add_argument("--job-name", help="Optional stable job name override")
    translate_parser.add_argument("--provider", help="Processing provider identifier")
    translate_parser.add_argument("--model", help="Provider model name")
    translate_parser.add_argument("--parameters-json", help="Optional provider parameters as a JSON object")
    translate_parser.add_argument(
        "--activation-policy",
        choices=sorted(getattr(tools, "RUN_ACTIVATION_POLICIES", {"always", "if_empty", "if_poor", "manual"})),
        default="always",
        help="Activation policy for created translation revisions",
    )
    translate_parser.add_argument(
        "--budget-seconds",
        type=int,
        default=DEFAULT_NATIVE_STEP_BUDGET_SECONDS,
        help="Per-step budget used when advancing the resumable backend",
    )
    translate_parser.add_argument(
        "--claim-stale-seconds",
        type=int,
        default=DEFAULT_NATIVE_CLAIM_STALE_SECONDS,
        help="How long this runner keeps claims alive before another caller can reclaim them",
    )
    translate_parser.add_argument(
        "--max-steps",
        type=int,
        help="Optional maximum number of backend step calls before stopping with an error",
    )
    add_selector_arguments(translate_parser)

    extract_parser = subparsers.add_parser("extract", help="Create a structured extraction run and publish its outputs")
    extract_parser.add_argument("workspace", help="Workspace root path")
    extract_parser.add_argument("field_name", help="Custom field name to populate")
    extract_parser.add_argument("--instruction", required=True, help="Extraction instruction")
    extract_parser.add_argument(
        "--field-type",
        choices=sorted(getattr(tools, "REGISTRY_FIELD_TYPES", {"boolean", "date", "integer", "real", "text"})),
        default="text",
        help="Custom field type",
    )
    extract_parser.add_argument("--job-name", help="Optional stable job name override")
    extract_parser.add_argument("--provider", help="Processing provider identifier")
    extract_parser.add_argument("--model", help="Provider model name")
    extract_parser.add_argument("--parameters-json", help="Optional provider parameters as a JSON object")
    extract_parser.add_argument("--response-schema-json", help="Optional response schema JSON object")
    extract_parser.add_argument(
        "--budget-seconds",
        type=int,
        default=DEFAULT_NATIVE_STEP_BUDGET_SECONDS,
        help="Per-step budget used when advancing the resumable backend",
    )
    extract_parser.add_argument(
        "--claim-stale-seconds",
        type=int,
        default=DEFAULT_NATIVE_CLAIM_STALE_SECONDS,
        help="How long this runner keeps claims alive before another caller can reclaim them",
    )
    extract_parser.add_argument(
        "--max-steps",
        type=int,
        help="Optional maximum number of backend step calls before stopping with an error",
    )
    add_selector_arguments(extract_parser)

    ocr_parser = subparsers.add_parser("ocr", help="Create an OCR run and execute it to completion")
    ocr_parser.add_argument("workspace", help="Workspace root path")
    ocr_parser.add_argument("--instruction", help="Optional OCR instruction")
    ocr_parser.add_argument("--job-name", help="Optional stable job name override")
    ocr_parser.add_argument("--provider", help="Processing provider identifier")
    ocr_parser.add_argument("--model", help="Provider model name")
    ocr_parser.add_argument("--parameters-json", help="Optional provider parameters as a JSON object")
    ocr_parser.add_argument(
        "--activation-policy",
        choices=sorted(getattr(tools, "RUN_ACTIVATION_POLICIES", {"always", "if_empty", "if_poor", "manual"})),
        default="always",
        help="Activation policy for created OCR revisions",
    )
    ocr_parser.add_argument(
        "--budget-seconds",
        type=int,
        default=DEFAULT_NATIVE_STEP_BUDGET_SECONDS,
        help="Per-step budget used when advancing the resumable backend",
    )
    ocr_parser.add_argument(
        "--claim-stale-seconds",
        type=int,
        default=DEFAULT_NATIVE_CLAIM_STALE_SECONDS,
        help="How long this runner keeps claims alive before another caller can reclaim them",
    )
    ocr_parser.add_argument(
        "--max-steps",
        type=int,
        help="Optional maximum number of backend step calls before stopping with an error",
    )
    add_selector_arguments(ocr_parser)

    describe_parser = subparsers.add_parser(
        "describe-images",
        help="Create an image-description run and execute it to completion",
    )
    describe_parser.add_argument("workspace", help="Workspace root path")
    describe_parser.add_argument("--instruction", help="Optional image-description instruction")
    describe_parser.add_argument("--job-name", help="Optional stable job name override")
    describe_parser.add_argument("--provider", help="Processing provider identifier")
    describe_parser.add_argument("--model", help="Provider model name")
    describe_parser.add_argument("--parameters-json", help="Optional provider parameters as a JSON object")
    describe_parser.add_argument(
        "--activation-policy",
        choices=sorted(getattr(tools, "RUN_ACTIVATION_POLICIES", {"always", "if_empty", "if_poor", "manual"})),
        default="always",
        help="Activation policy for created image-description revisions",
    )
    describe_parser.add_argument(
        "--budget-seconds",
        type=int,
        default=DEFAULT_NATIVE_STEP_BUDGET_SECONDS,
        help="Per-step budget used when advancing the resumable backend",
    )
    describe_parser.add_argument(
        "--claim-stale-seconds",
        type=int,
        default=DEFAULT_NATIVE_CLAIM_STALE_SECONDS,
        help="How long this runner keeps claims alive before another caller can reclaim them",
    )
    describe_parser.add_argument(
        "--max-steps",
        type=int,
        help="Optional maximum number of backend step calls before stopping with an error",
    )
    add_selector_arguments(describe_parser)

    return parser


def parse_json_object(value: str | None, *, label: str) -> dict[str, object]:
    if value is None:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} must be valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must decode to a JSON object.")
    return parsed


def has_explicit_selectors(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "document_ids", None)
        or getattr(args, "filters", None)
        or getattr(args, "bates", None)
        or getattr(args, "dataset_names", None)
        or getattr(args, "from_run_id", None) is not None
        or str(getattr(args, "query", "") or "").strip()
        or bool(getattr(args, "select_from_scope", False))
    )


def model_from_environment(job_kind: str) -> str | None:
    env_names = TEXT_MODEL_ENV_VARS if job_kind in {"structured_extraction", "translation"} else VISION_MODEL_ENV_VARS
    for env_name in env_names:
        candidate = str(os.environ.get(env_name, "") or "").strip()
        if candidate:
            return candidate
    return None


def claude_model_from_environment(job_kind: str) -> str | None:
    env_names = CLAUDE_TEXT_MODEL_ENV_VARS if job_kind in {"structured_extraction", "translation"} else CLAUDE_VISION_MODEL_ENV_VARS
    for env_name in env_names:
        candidate = str(os.environ.get(env_name, "") or "").strip()
        if candidate:
            return candidate
    return None


def claude_cli_path() -> str | None:
    return shutil.which("claude")


def default_provider(job_kind: str) -> str | None:
    del job_kind
    if claude_cli_path():
        return "claude_code"
    api_key = str(os.environ.get("OPENAI_API_KEY", "") or "").strip()
    if api_key:
        return "openai_responses"
    return None


def choose_provider(tools: Any, job_kind: str, explicit_provider: str | None) -> str:
    normalized_provider = str(explicit_provider or "").strip()
    if normalized_provider:
        return normalized_provider
    provider = default_provider(job_kind)
    if provider:
        return provider
    if job_kind == "structured_extraction":
        raise tools.RetrieverError(
            "No extraction provider configured. Install Claude Code so `claude` is available, set OPENAI_API_KEY "
            "and --model, or pass --provider static_json with --parameters-json."
        )
    if job_kind == "translation":
        raise tools.RetrieverError(
            "No translation provider configured. Install Claude Code so `claude` is available, set OPENAI_API_KEY "
            "and --model, or pass --provider static_text with --parameters-json."
        )
    raise tools.RetrieverError(
        "No vision provider configured. Install Claude Code so `claude` is available, set OPENAI_API_KEY and "
        "--model, or pass --provider static_text with --parameters-json."
    )


def choose_model(tools: Any, job_kind: str, provider: str, explicit_model: str | None) -> str | None:
    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider in CLAUDE_CODE_PROVIDER_NAMES:
        return str(explicit_model or "").strip() or claude_model_from_environment(job_kind) or None
    if normalized_provider not in OPENAI_RESPONSES_PROVIDER_NAMES:
        return str(explicit_model or "").strip() or None
    model = str(explicit_model or "").strip() or model_from_environment(job_kind)
    if model:
        return model
    raise tools.RetrieverError(
        f"{provider} {job_kind} execution requires --model or a Retriever/OpenAI model environment variable."
    )


def processing_template_context(context: dict[str, object], text_input: str | None = None) -> dict[str, object]:
    document = dict(context.get("document") or {})
    input_payload = dict(context.get("input") or {})
    return {
        "document_id": document.get("id") or "",
        "control_number": document.get("control_number") or "",
        "file_name": document.get("file_name") or "",
        "rel_path": document.get("rel_path") or document.get("source_rel_path") or "",
        "title": document.get("title") or "",
        "subject": document.get("subject") or "",
        "text": text_input or "",
        "page_number": input_payload.get("page_number") or "",
        "artifact_path": input_payload.get("artifact_path") or "",
        "artifact_rel_path": input_payload.get("artifact_rel_path") or "",
        "source_path": input_payload.get("source_path") or document.get("source_path") or "",
        "source_rel_path": input_payload.get("source_rel_path") or document.get("source_rel_path") or "",
    }


def text_input_from_context(tools: Any, context: dict[str, object]) -> str | None:
    input_payload = dict(context.get("input") or {})
    inline_text = input_payload.get("inline_text")
    if isinstance(inline_text, str):
        return inline_text
    text_path = input_payload.get("text_path")
    if isinstance(text_path, str) and text_path.strip():
        return Path(text_path).read_text(encoding="utf-8")
    return None


def compact_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def prepare_openai_image_data_url(tools: Any, artifact_path: Path) -> str:
    mime_type, _ = mimetypes.guess_type(str(artifact_path))
    if mime_type in SUPPORTED_OPENAI_IMAGE_MIME_TYPES:
        image_bytes = artifact_path.read_bytes()
        return f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}"

    pil_image = tools.load_dependency("PilImage", allow_auto_install=True)
    if pil_image is None:
        raise tools.RetrieverError(
            f"Could not load Pillow to convert unsupported image artifact {artifact_path.name!r} for OpenAI vision."
        )

    with pil_image.open(artifact_path) as source_image:
        converted = source_image.convert("RGB")
        buffer = io.BytesIO()
        converted.save(buffer, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode("ascii")


def ensure_claude_code_image_path(tools: Any, root: Path, artifact_path: Path) -> Path:
    if artifact_path.suffix.lower() in SUPPORTED_CLAUDE_CODE_IMAGE_SUFFIXES:
        return artifact_path

    pil_image = tools.load_dependency("PilImage", allow_auto_install=True)
    if pil_image is None:
        raise tools.RetrieverError(
            f"Could not load Pillow to convert unsupported image artifact {artifact_path.name!r} for Claude Code vision."
        )

    paths = tools.workspace_paths(root)
    output_dir = Path(paths["tmp_dir"]) / "claude-code-images"
    output_dir.mkdir(parents=True, exist_ok=True)
    stat_result = artifact_path.stat()
    fingerprint = hashlib.sha256(
        f"{artifact_path.resolve()}:{stat_result.st_size}:{stat_result.st_mtime_ns}".encode("utf-8")
    ).hexdigest()[:12]
    target_path = output_dir / f"{artifact_path.stem}-{fingerprint}.png"
    if target_path.exists():
        return target_path

    with pil_image.open(artifact_path) as source_image:
        converted = source_image.convert("RGB")
        converted.save(target_path, format="PNG")
    return target_path


def claude_code_runner_cwd(tools: Any, root: Path) -> Path:
    paths = tools.workspace_paths(root)
    runner_cwd = Path(paths["tmp_dir"]) / "claude-code-runner"
    runner_cwd.mkdir(parents=True, exist_ok=True)
    return runner_cwd


def inline_text_reference_from_context(context: dict[str, object]) -> str | None:
    input_payload = dict(context.get("input") or {})
    inline_text = input_payload.get("inline_text")
    if isinstance(inline_text, str):
        return inline_text
    return None


def text_path_reference_from_context(context: dict[str, object]) -> Path | None:
    input_payload = dict(context.get("input") or {})
    text_path = str(input_payload.get("text_path") or "").strip()
    if not text_path:
        return None
    return Path(text_path)


def claude_code_prompt(context: dict[str, object], *, read_path: Path | None = None, inline_text: str | None = None) -> str:
    execution = dict(context.get("execution") or {})
    document = dict(context.get("document") or {})
    input_payload = dict(context.get("input") or {})
    lines = [
        str(execution.get("task_prompt") or "").strip(),
        "",
        f"Document id: {document.get('id') or ''}",
        f"File name: {document.get('file_name') or ''}",
        f"Source path: {document.get('source_path') or document.get('source_rel_path') or ''}",
    ]
    page_number = input_payload.get("page_number")
    if page_number:
        lines.append(f"Page number: {page_number}")
    if read_path is not None:
        lines.extend(
            [
                "",
                f"Use the Read tool on this file path and use its contents as the only primary input: {read_path}",
            ]
        )
    if inline_text is not None:
        lines.extend(
            [
                "",
                "Use the following input exactly as the primary document text:",
                "<document>",
                inline_text,
                "</document>",
            ]
        )
    lines.extend(
        [
            "",
            "Return only the final requested output. Do not include commentary, Markdown fences, or extra labels.",
        ]
    )
    return "\n".join(lines)


def run_claude_code(
    tools: Any,
    *,
    root: Path,
    prompt_text: str,
    model: str | None,
    json_schema: dict[str, object] | None = None,
) -> str:
    claude_path = claude_cli_path()
    if not claude_path:
        raise tools.RetrieverError("Claude Code CLI (`claude`) is not installed or not on PATH.")

    command = [
        claude_path,
        "-p",
        "--no-session-persistence",
        "--disable-slash-commands",
        "--permission-mode",
        "bypassPermissions",
        "--tools",
        "Read",
        "--add-dir",
        str(root),
        "--max-turns",
        str(DEFAULT_CLAUDE_CODE_MAX_TURNS),
    ]
    if model:
        command.extend(["--model", model])
    if json_schema is not None:
        command.extend(["--json-schema", compact_json(json_schema)])
    command.append(prompt_text)

    result = subprocess.run(
        command,
        cwd=claude_code_runner_cwd(tools, root),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr_text = result.stderr.strip()
        stdout_text = result.stdout.strip()
        detail = stderr_text or stdout_text or f"claude exited with status {result.returncode}"
        raise tools.RetrieverError(f"Claude Code provider failed: {detail}")
    output_text = result.stdout.rstrip("\n")
    if not output_text.strip():
        raise tools.RetrieverError("Claude Code provider returned empty output.")
    return output_text


def claude_code_provider_metadata(job_version: dict[str, object], *, executed_by: str, extra: dict[str, object] | None = None) -> dict[str, object]:
    metadata = {
        "provider": str(job_version.get("provider") or ""),
        "model": job_version.get("model"),
        "executed_by": executed_by,
        "transport": "claude_cli",
    }
    if extra:
        metadata.update(extra)
    return metadata


def execute_static_structured_extraction(tools: Any, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    parameters = dict(job_version.get("parameters") or {})
    output_templates = parameters.get("output_values")
    if not isinstance(output_templates, dict):
        raise tools.RetrieverError("static_json extraction requires parameters_json.output_values to be an object.")
    text_input = text_input_from_context(tools, context)
    template_context = processing_template_context(context, text_input=text_input)
    normalized_output: dict[str, object] = {}
    for job_output in list(context.get("job_outputs") or []):
        output_name = str(job_output.get("output_name") or "")
        normalized_output[output_name] = tools.render_processing_template_value(output_templates.get(output_name), template_context)
    return {
        "raw_output": normalized_output,
        "normalized_output": normalized_output,
        "output_values": normalized_output,
        "provider_request_id": None,
        "input_tokens": tools.token_estimate(text_input or "") if text_input is not None else None,
        "output_tokens": tools.token_estimate(compact_json(normalized_output)) if normalized_output else 0,
        "cost_cents": 0,
        "provider_metadata": {
            "provider": str(job_version.get("provider") or ""),
            "model": job_version.get("model"),
            "executed_by": "retriever_native_static_json",
        },
    }


def execute_openai_structured_extraction(tools: Any, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    parameters = dict(job_version.get("parameters") or {})
    model = str(job_version.get("model") or "").strip()
    if not model:
        raise tools.RetrieverError("openai_responses extraction requires a model name.")
    response_schema = context.get("response_schema")
    if not isinstance(response_schema, dict) or not response_schema:
        raise tools.RetrieverError("Structured extraction run context did not include a response schema.")
    text_input = text_input_from_context(tools, context) or ""
    timeout_seconds = float(parameters.get("timeout_seconds") or 60.0)
    schema_name = tools.sanitize_processing_identifier(
        str(job_version.get("display_name") or context.get("job", {}).get("job_name") or "processing_response"),
        label="Schema name",
        prefix="schema",
    )
    payload: dict[str, object] = {
        "model": model,
        "store": False,
        "input": [
            {"role": "system", "content": str(context.get("execution", {}).get("task_prompt") or "").strip()},
            {"role": "user", "content": f"<document>\n{text_input}\n</document>"},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "schema": response_schema,
                "strict": True,
            }
        },
    }
    payload.update(tools.response_api_request_overrides(parameters))
    response_payload = tools.call_openai_responses_api(payload=payload, timeout_seconds=timeout_seconds)
    response_text = tools.openai_response_text(response_payload)
    try:
        normalized_output = json.loads(response_text)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise tools.RetrieverError("OpenAI extraction response was not valid JSON.") from exc
    validation_issues = tools.validate_processing_schema_value(normalized_output, response_schema)
    if validation_issues:
        raise tools.RetrieverError("; ".join(validation_issues))
    input_tokens, output_tokens = tools.openai_response_usage(response_payload)
    return {
        "raw_output": response_payload,
        "normalized_output": normalized_output,
        "output_values": normalized_output if isinstance(normalized_output, dict) else {},
        "provider_request_id": response_payload.get("id"),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_cents": None,
        "provider_metadata": {
            "provider": str(job_version.get("provider") or ""),
            "model": job_version.get("model"),
            "response_id": response_payload.get("id"),
            "status": response_payload.get("status"),
        },
    }


def execute_claude_code_structured_extraction(tools: Any, root: Path, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    response_schema = context.get("response_schema")
    if not isinstance(response_schema, dict) or not response_schema:
        raise tools.RetrieverError("Structured extraction run context did not include a response schema.")
    inline_text = inline_text_reference_from_context(context)
    text_path = text_path_reference_from_context(context)
    prompt_text = claude_code_prompt(context, read_path=text_path, inline_text=inline_text)
    response_text = run_claude_code(
        tools,
        root=root,
        prompt_text=prompt_text,
        model=str(job_version.get("model") or "").strip() or None,
        json_schema=response_schema,
    )
    try:
        normalized_output = json.loads(response_text)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise tools.RetrieverError("Claude Code extraction response was not valid JSON.") from exc
    validation_issues = tools.validate_processing_schema_value(normalized_output, response_schema)
    if validation_issues:
        raise tools.RetrieverError("; ".join(validation_issues))
    return {
        "raw_output": normalized_output,
        "normalized_output": normalized_output,
        "output_values": normalized_output if isinstance(normalized_output, dict) else {},
        "provider_request_id": None,
        "input_tokens": None,
        "output_tokens": None,
        "cost_cents": None,
        "provider_metadata": claude_code_provider_metadata(
            job_version,
            executed_by="retriever_native_claude_code_extraction",
        ),
    }


def execute_static_translation(tools: Any, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    parameters = dict(job_version.get("parameters") or {})
    translated_template = parameters.get("translated_text")
    if translated_template is None:
        raise tools.RetrieverError("static_text translation requires parameters_json.translated_text.")
    text_input = text_input_from_context(tools, context)
    template_context = processing_template_context(context, text_input=text_input)
    translated_text = tools.render_processing_template_value(translated_template, template_context)
    if not isinstance(translated_text, str):
        translated_text = compact_json(translated_text)
    target_language = str(
        parameters.get("target_language") or parameters.get("target_lang") or parameters.get("language") or ""
    ).strip().lower() or None
    return {
        "raw_output": {"translated_text": translated_text},
        "normalized_output": {"translated_text": translated_text},
        "output_values": {},
        "created_text_revision": {
            "revision_kind": "translation",
            "text_content": translated_text,
            "language": target_language,
        },
        "provider_request_id": None,
        "input_tokens": tools.token_estimate(text_input or "") if text_input is not None else None,
        "output_tokens": tools.token_estimate(translated_text),
        "cost_cents": 0,
        "provider_metadata": {
            "provider": str(job_version.get("provider") or ""),
            "model": job_version.get("model"),
            "executed_by": "retriever_native_static_text_translation",
            "target_language": target_language,
        },
    }


def execute_claude_code_translation(tools: Any, root: Path, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    parameters = dict(job_version.get("parameters") or {})
    target_language = str(
        parameters.get("target_language") or parameters.get("target_lang") or parameters.get("language") or ""
    ).strip().lower() or None
    inline_text = inline_text_reference_from_context(context)
    text_path = text_path_reference_from_context(context)
    prompt_text = claude_code_prompt(context, read_path=text_path, inline_text=inline_text)
    translated_text = run_claude_code(
        tools,
        root=root,
        prompt_text=prompt_text,
        model=str(job_version.get("model") or "").strip() or None,
    )
    return {
        "raw_output": {"translated_text": translated_text},
        "normalized_output": {"translated_text": translated_text},
        "output_values": {},
        "created_text_revision": {
            "revision_kind": "translation",
            "text_content": translated_text,
            "language": target_language,
        },
        "provider_request_id": None,
        "input_tokens": None,
        "output_tokens": None,
        "cost_cents": None,
        "provider_metadata": claude_code_provider_metadata(
            job_version,
            executed_by="retriever_native_claude_code_translation",
            extra={"target_language": target_language},
        ),
    }


def execute_openai_translation(tools: Any, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    parameters = dict(job_version.get("parameters") or {})
    model = str(job_version.get("model") or "").strip()
    if not model:
        raise tools.RetrieverError("openai_responses translation requires a model name.")
    target_language = str(
        parameters.get("target_language") or parameters.get("target_lang") or parameters.get("language") or ""
    ).strip().lower()
    if not target_language:
        raise tools.RetrieverError("Translation job versions require parameters_json.target_language.")
    instruction_text = str(job_version.get("instruction_text") or "").strip()
    prompt_lines = [
        f"Translate the document into {target_language}. Return only the translated text.",
        "Preserve the original structure and line breaks.",
        "Keep dates, numbers, email addresses, URLs, Bates numbers, file names, and header labels/ordering unchanged.",
        "Keep proper names unchanged unless the source itself provides a translated form.",
        "Do not summarize or omit content.",
    ]
    if instruction_text:
        prompt_lines.append(f"Job instruction: {instruction_text}")
    text_input = text_input_from_context(tools, context) or ""
    payload: dict[str, object] = {
        "model": model,
        "store": False,
        "input": [
            {"role": "system", "content": "\n".join(prompt_lines)},
            {"role": "user", "content": f"<document>\n{text_input}\n</document>"},
        ],
        "text": {"format": {"type": "text"}},
    }
    payload.update(tools.response_api_request_overrides(parameters))
    timeout_seconds = float(parameters.get("timeout_seconds") or 60.0)
    response_payload = tools.call_openai_responses_api(payload=payload, timeout_seconds=timeout_seconds)
    translated_text = tools.openai_response_text(response_payload)
    input_tokens, output_tokens = tools.openai_response_usage(response_payload)
    return {
        "raw_output": response_payload,
        "normalized_output": {"translated_text": translated_text},
        "output_values": {},
        "created_text_revision": {
            "revision_kind": "translation",
            "text_content": translated_text,
            "language": target_language,
        },
        "provider_request_id": response_payload.get("id"),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_cents": None,
        "provider_metadata": {
            "provider": str(job_version.get("provider") or ""),
            "model": job_version.get("model"),
            "response_id": response_payload.get("id"),
            "status": response_payload.get("status"),
            "target_language": target_language,
        },
    }


def execute_static_page_text(tools: Any, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    parameters = dict(job_version.get("parameters") or {})
    page_text_template = parameters.get("page_text")
    if page_text_template is None:
        raise tools.RetrieverError("static_text vision execution requires parameters_json.page_text.")
    template_context = processing_template_context(context, text_input=None)
    page_text = tools.render_processing_template_value(page_text_template, template_context)
    if not isinstance(page_text, str):
        page_text = compact_json(page_text)
    job_kind = str(context.get("job", {}).get("job_kind") or "")
    executed_by = "retriever_native_static_text_ocr" if job_kind == "ocr" else "retriever_native_static_text_image_description"
    return {
        "page_text": page_text,
        "provider_request_id": None,
        "input_tokens": None,
        "output_tokens": tools.token_estimate(page_text),
        "cost_cents": 0,
        "provider_metadata": {
            "provider": str(job_version.get("provider") or ""),
            "model": job_version.get("model"),
            "executed_by": executed_by,
        },
    }


def execute_claude_code_page_text(tools: Any, root: Path, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    input_payload = dict(context.get("input") or {})
    artifact_path_text = str(input_payload.get("artifact_path") or "").strip()
    if not artifact_path_text:
        raise tools.RetrieverError("Run item context did not include an artifact path.")
    artifact_path = Path(artifact_path_text)
    if not artifact_path.exists():
        raise tools.RetrieverError(f"Run item artifact is missing: {artifact_path}")
    read_path = ensure_claude_code_image_path(tools, root, artifact_path)
    page_text = run_claude_code(
        tools,
        root=root,
        prompt_text=claude_code_prompt(context, read_path=read_path, inline_text=None),
        model=str(job_version.get("model") or "").strip() or None,
    )
    job_kind = str(context.get("job", {}).get("job_kind") or "")
    executed_by = "retriever_native_claude_code_ocr" if job_kind == "ocr" else "retriever_native_claude_code_image_description"
    return {
        "page_text": page_text,
        "provider_request_id": None,
        "input_tokens": None,
        "output_tokens": None,
        "cost_cents": None,
        "provider_metadata": claude_code_provider_metadata(
            job_version,
            executed_by=executed_by,
            extra={
                "page_number": input_payload.get("page_number"),
                "read_path": str(read_path),
            },
        ),
    }


def execute_openai_page_text(tools: Any, context: dict[str, object]) -> dict[str, object]:
    job_version = dict(context.get("job_version") or {})
    parameters = dict(job_version.get("parameters") or {})
    model = str(job_version.get("model") or "").strip()
    if not model:
        raise tools.RetrieverError("openai_responses vision execution requires a model name.")
    input_payload = dict(context.get("input") or {})
    artifact_path_text = str(input_payload.get("artifact_path") or "").strip()
    if not artifact_path_text:
        raise tools.RetrieverError("Run item context did not include an artifact path.")
    artifact_path = Path(artifact_path_text)
    if not artifact_path.exists():
        raise tools.RetrieverError(f"Run item artifact is missing: {artifact_path}")
    page_number = input_payload.get("page_number")
    prompt_text = str(context.get("execution", {}).get("task_prompt") or "").strip()
    user_prompt = "Process the attached page image."
    if page_number:
        user_prompt += f" This is page {page_number}."
    payload: dict[str, object] = {
        "model": model,
        "store": False,
        "input": [
            {"role": "system", "content": prompt_text},
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_prompt},
                    {"type": "input_image", "image_url": prepare_openai_image_data_url(tools, artifact_path)},
                ],
            },
        ],
        "text": {"format": {"type": "text"}},
    }
    payload.update(tools.response_api_request_overrides(parameters))
    timeout_seconds = float(parameters.get("timeout_seconds") or 60.0)
    response_payload = tools.call_openai_responses_api(payload=payload, timeout_seconds=timeout_seconds)
    page_text = tools.openai_response_text(response_payload)
    input_tokens, output_tokens = tools.openai_response_usage(response_payload)
    return {
        "page_text": page_text,
        "provider_request_id": response_payload.get("id"),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_cents": None,
        "provider_metadata": {
            "provider": str(job_version.get("provider") or ""),
            "model": job_version.get("model"),
            "response_id": response_payload.get("id"),
            "status": response_payload.get("status"),
            "page_number": page_number,
        },
    }


def execute_run_item(tools: Any, root: Path, context: dict[str, object]) -> dict[str, object]:
    job = dict(context.get("job") or {})
    job_version = dict(context.get("job_version") or {})
    provider = str(job_version.get("provider") or "").strip().lower()
    job_kind = str(job.get("job_kind") or "").strip().lower()

    if job_kind == "structured_extraction":
        if provider in STATIC_JSON_PROVIDER_NAMES:
            return execute_static_structured_extraction(tools, context)
        if provider in CLAUDE_CODE_PROVIDER_NAMES:
            return execute_claude_code_structured_extraction(tools, root, context)
        if provider in OPENAI_RESPONSES_PROVIDER_NAMES:
            return execute_openai_structured_extraction(tools, context)
    elif job_kind == "translation":
        if provider in STATIC_TEXT_PROVIDER_NAMES:
            return execute_static_translation(tools, context)
        if provider in CLAUDE_CODE_PROVIDER_NAMES:
            return execute_claude_code_translation(tools, root, context)
        if provider in OPENAI_RESPONSES_PROVIDER_NAMES:
            return execute_openai_translation(tools, context)
    elif job_kind in {"ocr", "image_description"}:
        if provider in STATIC_TEXT_PROVIDER_NAMES:
            return execute_static_page_text(tools, context)
        if provider in CLAUDE_CODE_PROVIDER_NAMES:
            return execute_claude_code_page_text(tools, root, context)
        if provider in OPENAI_RESPONSES_PROVIDER_NAMES:
            return execute_openai_page_text(tools, context)

    raise tools.RetrieverError(
        f"Unsupported provider {provider!r} for native Retriever job kind {job_kind!r}."
    )


def build_complete_run_item_kwargs(tools: Any, execution_result: dict[str, object]) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    if execution_result.get("page_text") is not None:
        kwargs["page_text"] = str(execution_result["page_text"])
    if execution_result.get("raw_output") is not None:
        kwargs["raw_output_json"] = compact_json(execution_result["raw_output"])
    if execution_result.get("normalized_output") is not None:
        kwargs["normalized_output_json"] = compact_json(execution_result["normalized_output"])
    if execution_result.get("output_values") is not None and execution_result.get("output_values") != {}:
        kwargs["output_values_json"] = compact_json(execution_result["output_values"])
    if execution_result.get("created_text_revision") is not None:
        kwargs["created_text_revision_json"] = compact_json(execution_result["created_text_revision"])
    if execution_result.get("provider_metadata") is not None:
        kwargs["provider_metadata_json"] = compact_json(execution_result["provider_metadata"])
    for key in ("provider_request_id", "input_tokens", "output_tokens", "cost_cents"):
        value = execution_result.get(key)
        if value is not None:
            kwargs[key] = value
    return kwargs


def process_batch_entry(
    tools: Any,
    root: Path,
    *,
    batch_entry: dict[str, object],
    claimed_by: str,
) -> dict[str, object]:
    run_item = dict(batch_entry.get("run_item") or {})
    run_item_id = int(run_item["id"])
    context = dict(batch_entry.get("context") or {})
    started = time.perf_counter()
    try:
        execution_result = execute_run_item(tools, root, context)
        latency_ms = int((time.perf_counter() - started) * 1000.0)
        execution_result.setdefault("latency_ms", latency_ms)
        complete_payload = tools.complete_run_item(
            root,
            run_item_id=run_item_id,
            claimed_by=claimed_by,
            **build_complete_run_item_kwargs(tools, execution_result),
        )
        return {
            "status": "ok",
            "run_item_id": run_item_id,
            "complete": complete_payload,
            "execution_result": execution_result,
        }
    except Exception as exc:
        fail_payload = tools.fail_run_item(
            root,
            run_item_id=run_item_id,
            claimed_by=claimed_by,
            error=str(exc),
        )
        return {
            "status": "failed",
            "run_item_id": run_item_id,
            "error": str(exc),
            "fail": fail_payload,
        }


def ensure_job(tools: Any, root: Path, job_name: str, job_kind: str, description: str | None) -> tuple[dict[str, object], bool]:
    normalized_name = tools.sanitize_processing_identifier(job_name, label="Job name", prefix="job")
    for existing_job in list(tools.list_jobs(root).get("jobs") or []):
        if str(existing_job.get("job_name") or "") == normalized_name:
            return dict(existing_job), False
    created_payload = tools.create_job(root, normalized_name, job_kind, description)
    return dict(created_payload["job"]), True


def ensure_output_binding(
    tools: Any,
    root: Path,
    *,
    job_name: str,
    output_name: str,
    value_type: str,
    bound_custom_field: str | None,
    description: str | None = None,
) -> dict[str, object]:
    return tools.add_job_output(
        root,
        job_name,
        output_name,
        value_type,
        bound_custom_field=bound_custom_field,
        description=description,
    )


def create_processing_run(
    tools: Any,
    root: Path,
    *,
    job_version_id: int,
    args: argparse.Namespace,
    activation_policy: str,
) -> dict[str, object]:
    select_from_scope = bool(getattr(args, "select_from_scope", False))
    if not has_explicit_selectors(args):
        select_from_scope = True
    return tools.create_run(
        root,
        job_version_id=job_version_id,
        dataset_names=list(getattr(args, "dataset_names", None) or []),
        document_ids=list(getattr(args, "document_ids", None) or []),
        query=str(getattr(args, "query", "") or ""),
        raw_bates=getattr(args, "bates", None),
        raw_filters=list(getattr(args, "filters", None) or []),
        from_run_id=getattr(args, "from_run_id", None),
        select_from_scope=select_from_scope,
        activation_policy=activation_policy,
        family_mode=str(getattr(args, "family_mode", "exact") or "exact"),
        seed_limit=getattr(args, "seed_limit", None),
    )


def maybe_publish_bound_outputs(tools: Any, root: Path, run_payload: dict[str, object], run_id: int) -> dict[str, object] | None:
    job_version = dict(run_payload.get("job_version") or {})
    job = dict(job_version.get("job") or {})
    if str(job.get("job_kind") or "") != "structured_extraction":
        return None
    outputs = list(job.get("outputs") or [])
    if not any(output.get("bound_custom_field") for output in outputs):
        return None
    return tools.publish_run_results(root, run_id=run_id)


def pending_finalization_action(run_payload: dict[str, object]) -> str | None:
    worker = dict(run_payload.get("worker") or {})
    next_action = str(worker.get("next_action") or "")
    if next_action == "finalize_ocr":
        return "finalize_ocr"
    if next_action == "finalize_image_description":
        return "finalize_image_description"
    supervision = dict(run_payload.get("supervision") or {})
    recommended_action = str(supervision.get("recommended_action") or "")
    if recommended_action == "finalize_ocr":
        return "finalize_ocr"
    if recommended_action == "finalize_image_description":
        return "finalize_image_description"
    return None


def finish_worker_best_effort(
    tools: Any,
    root: Path,
    *,
    run_id: int,
    claimed_by: str,
    worker_status: str,
    summary: dict[str, object] | None = None,
    error_summary: str | None = None,
) -> dict[str, object] | None:
    try:
        return tools.finish_run_worker(
            root,
            run_id=run_id,
            claimed_by=claimed_by,
            worker_status=worker_status,
            summary_json=(compact_json(summary or {}) if summary is not None else None),
            error_summary=error_summary,
        )
    except Exception:
        return None


def native_handoff_claimed_by_hint(run_payload: dict[str, object], claimed_by: str) -> str:
    worker = dict(run_payload.get("worker") or {})
    hinted = str(worker.get("handoff_claimed_by_hint") or "").strip()
    if hinted:
        return hinted
    return f"{claimed_by}-handoff"


def run_status_for_worker(
    tools: Any,
    root: Path,
    *,
    run_id: int,
    budget_seconds: int,
    claimed_by: str,
) -> dict[str, object]:
    return tools.run_status(
        root,
        run_id=run_id,
        budget_seconds=budget_seconds,
        claimed_by=claimed_by,
    )


def should_rotate_native_worker(run_payload: dict[str, object]) -> bool:
    worker = dict(run_payload.get("worker") or {})
    next_action = str(worker.get("next_action") or "")
    if next_action == "handoff" or bool(worker.get("should_exit_after_batch")):
        return True
    if next_action != "stop":
        return False

    supervision = dict(run_payload.get("supervision") or {})
    if bool(supervision.get("continuation_needed")) or bool(supervision.get("finalization_pending")):
        return True
    if int(supervision.get("outstanding_items", 0) or 0) > 0:
        return True
    recommended_action = str(supervision.get("recommended_action") or "")
    return recommended_action in {"spawn_background_worker", "claim_inline"}


def native_progress_headline(command_name: str, run_payload: dict[str, object]) -> str:
    headline_map = {
        "run": "Processing run",
        "translate": "Translation",
        "extract": "Extraction",
        "ocr": "OCR",
        "describe-images": "Image description",
    }
    if command_name != "run":
        return headline_map.get(command_name, "Processing run")
    job_version = dict(run_payload.get("job_version") or {})
    job = dict(job_version.get("job") or {})
    job_kind = str(job.get("job_kind") or "")
    return {
        "translation": "Translation",
        "structured_extraction": "Extraction",
        "ocr": "OCR",
        "image_description": "Image description",
    }.get(job_kind, "Processing run")


def native_progress_line(command_name: str, run_payload: dict[str, object]) -> str | None:
    status = str(run_payload.get("status") or "").strip().lower()
    run_id = run_payload.get("id")
    completed = int(run_payload.get("completed_count") or 0)
    planned = int(run_payload.get("planned_count") or 0)
    failed = int(run_payload.get("failed_count") or 0)
    skipped = int(run_payload.get("skipped_count") or 0)
    run_item_counts = dict(run_payload.get("run_item_counts") or {})
    pending = int(run_item_counts.get("pending", 0) or 0)
    running = int(run_item_counts.get("running", 0) or 0)
    worker = dict(run_payload.get("worker") or {})
    next_action = str(worker.get("next_action") or "").strip()
    headline = native_progress_headline(command_name, run_payload)
    if status in {"completed", "failed", "canceled"}:
        line = f"{headline} {status} — {completed}/{planned} completed, {failed} failed"
    else:
        line = (
            f"{headline} in progress — {completed}/{planned} completed, "
            f"{pending} pending, {running} running, {failed} failed"
        )
    if skipped:
        line += f", {skipped} skipped"
    if next_action:
        line += f", next {next_action}"
    if run_id is not None:
        line += f" (run {run_id})"
    return line


def native_progress_signature(command_name: str, run_payload: dict[str, object]) -> tuple[str, str, str, str, str]:
    worker = dict(run_payload.get("worker") or {})
    return (
        command_name,
        str(run_payload.get("id") or ""),
        str(run_payload.get("status") or ""),
        str(worker.get("next_action") or ""),
        str(run_payload.get("failed_count") or 0),
    )


class NativeProgressPrinter:
    def __init__(self, command_name: str, *, interval_seconds: float = NATIVE_PROGRESS_INTERVAL_SECONDS) -> None:
        self.command_name = command_name
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.last_emit_at = 0.0
        self.last_signature: tuple[str, str, str, str, str] | None = None

    def emit(self, run_payload: dict[str, object], *, force: bool = False) -> None:
        line = native_progress_line(self.command_name, run_payload)
        if not line:
            return
        signature = native_progress_signature(self.command_name, run_payload)
        now = time.perf_counter()
        should_emit = (
            force
            or self.last_signature is None
            or signature != self.last_signature
            or (now - self.last_emit_at) >= self.interval_seconds
        )
        if not should_emit:
            return
        print(f"[progress] {line}", file=sys.stderr, flush=True)
        self.last_emit_at = now
        self.last_signature = signature


def run_worker_session(
    tools: Any,
    root: Path,
    *,
    command_name: str,
    run_id: int,
    claimed_by: str,
    budget_seconds: int,
    claim_stale_seconds: int,
    limit: int | None,
    max_steps: int | None,
    publish_bound_outputs: bool,
    progress: NativeProgressPrinter | None = None,
) -> dict[str, object]:
    normalized_claimed_by = str(claimed_by or "").strip()
    if not normalized_claimed_by:
        raise tools.RetrieverError("claimed_by cannot be empty.")

    step_calls = 0
    batch_count = 0
    batch_item_count = 0
    item_failures = 0
    step_payloads: list[dict[str, object]] = []
    processed_items: list[dict[str, object]] = []
    published_payload = None
    publish_error = None
    results: list[dict[str, object]] = []
    final_run_payload: dict[str, object] | None = None
    session_reason = "idle"
    handoff_claimed_by_hint: str | None = None
    progress_printer = progress or NativeProgressPrinter(command_name)

    try:
        initial_status_payload = run_status_for_worker(
            tools,
            root,
            run_id=run_id,
            budget_seconds=budget_seconds,
            claimed_by=normalized_claimed_by,
        )
        final_run_payload = dict(initial_status_payload["run"])
        progress_printer.emit(final_run_payload, force=True)
        while True:
            if max_steps is not None and step_calls >= max_steps:
                raise tools.RetrieverError(
                    f"Reached --max-steps={max_steps} before run {run_id} reached a terminal state."
                )
            step_payload = tools.run_job_step(
                root,
                run_id=run_id,
                claimed_by=normalized_claimed_by,
                budget_seconds=budget_seconds,
                limit=limit,
                stale_after_seconds=claim_stale_seconds,
            )
            step_calls += 1
            step_payloads.append(step_payload)
            batch = list(step_payload.get("batch") or [])
            if batch:
                batch_count += 1
                tools.heartbeat_run_items(root, run_id=run_id, claimed_by=normalized_claimed_by)
                for batch_entry in batch:
                    item_payload = process_batch_entry(
                        tools,
                        root,
                        batch_entry=batch_entry,
                        claimed_by=normalized_claimed_by,
                    )
                    processed_items.append(item_payload)
                    batch_item_count += 1
                    if item_payload["status"] != "ok":
                        item_failures += 1
                tools.heartbeat_run_items(root, run_id=run_id, claimed_by=normalized_claimed_by)

            status_payload = run_status_for_worker(
                tools,
                root,
                run_id=run_id,
                budget_seconds=budget_seconds,
                claimed_by=normalized_claimed_by,
            )
            final_run_payload = dict(status_payload["run"])
            finalization_action = pending_finalization_action(final_run_payload)
            if finalization_action == "finalize_ocr":
                if max_steps is not None and step_calls >= max_steps:
                    raise tools.RetrieverError(
                        f"Reached --max-steps={max_steps} before run {run_id} reached a terminal state."
                    )
                finalize_payload = tools.finalize_ocr_run(root, run_id=run_id)
                step_calls += 1
                step_payloads.append(
                    {
                        "status": "ok",
                        "executed": True,
                        "executed_step": "finalize_ocr",
                        "reason": "native_finalizer",
                        "step_result": finalize_payload,
                        "run": finalize_payload.get("run"),
                        "more_work_remaining": False,
                    }
                )
                final_run_payload = dict(finalize_payload.get("run") or final_run_payload)
            elif finalization_action == "finalize_image_description":
                if max_steps is not None and step_calls >= max_steps:
                    raise tools.RetrieverError(
                        f"Reached --max-steps={max_steps} before run {run_id} reached a terminal state."
                    )
                finalize_payload = tools.finalize_image_description_run(root, run_id=run_id)
                step_calls += 1
                step_payloads.append(
                    {
                        "status": "ok",
                        "executed": True,
                        "executed_step": "finalize_image_description",
                        "reason": "native_finalizer",
                        "step_result": finalize_payload,
                        "run": finalize_payload.get("run"),
                        "more_work_remaining": False,
                    }
                )
                final_run_payload = dict(finalize_payload.get("run") or final_run_payload)
            progress_printer.emit(final_run_payload)
            final_status = str(final_run_payload.get("status") or "")
            if final_status in {"completed", "failed", "canceled"}:
                session_reason = "terminal"
                break
            if should_rotate_native_worker(final_run_payload):
                session_reason = "handoff"
                handoff_claimed_by_hint = native_handoff_claimed_by_hint(final_run_payload, normalized_claimed_by)
                break

            worker_payload = dict(final_run_payload.get("worker") or {})
            next_action = str(worker_payload.get("next_action") or "")
            if next_action in {"claim", "process_batch"}:
                continue

            supervision = dict(final_run_payload.get("supervision") or {})
            if bool(supervision.get("continuation_needed")) or bool(supervision.get("finalization_pending")):
                continue
            session_reason = "idle"
            break

        if session_reason == "terminal":
            final_status_payload = run_status_for_worker(
                tools,
                root,
                run_id=run_id,
                budget_seconds=budget_seconds,
                claimed_by=normalized_claimed_by,
            )
            final_run_payload = dict(final_status_payload["run"])
            final_status = str(final_run_payload.get("status") or "")
            if final_status == "completed" and publish_bound_outputs:
                try:
                    published_payload = maybe_publish_bound_outputs(tools, root, final_run_payload, run_id)
                except Exception as exc:
                    publish_error = str(exc)
                else:
                    if published_payload is not None:
                        final_status_payload = run_status_for_worker(
                            tools,
                            root,
                            run_id=run_id,
                            budget_seconds=budget_seconds,
                            claimed_by=normalized_claimed_by,
                        )
                        final_run_payload = dict(final_status_payload["run"])
            results_payload = tools.list_results(root, run_id=run_id)
            results = list(results_payload.get("results") or [])

        assert final_run_payload is not None
        progress_printer.emit(final_run_payload, force=True)
        summary = {
            "step_calls": step_calls,
            "batch_count": batch_count,
            "batch_item_count": batch_item_count,
            "item_failures": item_failures,
        }
        if session_reason != "terminal":
            summary["reason"] = session_reason
        if handoff_claimed_by_hint:
            summary["handoff_claimed_by_hint"] = handoff_claimed_by_hint
        final_status = str(final_run_payload.get("status") or "")
        if session_reason == "terminal":
            if final_status == "failed":
                worker_status = "failed"
            elif final_status == "canceled":
                worker_status = "canceled"
            else:
                worker_status = "completed"
        else:
            worker_status = "stopped"
        worker_finish_payload = finish_worker_best_effort(
            tools,
            root,
            run_id=run_id,
            claimed_by=normalized_claimed_by,
            worker_status=worker_status,
            summary=summary,
        )
        return {
            "status": "ok",
            "run_id": run_id,
            "claimed_by": normalized_claimed_by,
            "step_calls": step_calls,
            "batch_count": batch_count,
            "batch_item_count": batch_item_count,
            "item_failures": item_failures,
            "steps": step_payloads,
            "processed_items": processed_items,
            "run": final_run_payload,
            "results": results,
            "publish": published_payload,
            "publish_error": publish_error,
            "worker_finish": worker_finish_payload,
            "session_reason": session_reason,
            "handoff_claimed_by_hint": handoff_claimed_by_hint,
        }
    except Exception as exc:
        finish_worker_best_effort(
            tools,
            root,
            run_id=run_id,
            claimed_by=normalized_claimed_by,
            worker_status="failed",
            summary={
                "step_calls": step_calls,
                "batch_count": batch_count,
                "batch_item_count": batch_item_count,
                "item_failures": item_failures,
            },
            error_summary=str(exc),
        )
        raise


def run_to_completion(
    tools: Any,
    root: Path,
    *,
    command_name: str,
    run_id: int,
    claimed_by: str | None,
    budget_seconds: int,
    claim_stale_seconds: int,
    limit: int | None,
    max_steps: int | None,
    publish_bound_outputs: bool,
) -> dict[str, object]:
    base_claimed_by = str(claimed_by or f"retriever-run-{run_id}").strip()
    if not base_claimed_by:
        raise tools.RetrieverError("claimed_by cannot be empty.")
    active_claimed_by = base_claimed_by
    step_calls = 0
    batch_count = 0
    batch_item_count = 0
    item_failures = 0
    step_payloads: list[dict[str, object]] = []
    processed_items: list[dict[str, object]] = []
    worker_finishes: list[dict[str, object]] = []
    published_payload = None
    publish_error = None
    final_run_payload: dict[str, object] | None = None
    results: list[dict[str, object]] = []
    progress = NativeProgressPrinter(command_name)
    while True:
        remaining_steps = None if max_steps is None else max_steps - step_calls
        if remaining_steps is not None and remaining_steps <= 0:
            raise tools.RetrieverError(
                f"Reached --max-steps={max_steps} before run {run_id} reached a terminal state."
            )
        session_payload = run_worker_session(
            tools,
            root,
            command_name=command_name,
            run_id=run_id,
            claimed_by=active_claimed_by,
            budget_seconds=budget_seconds,
            claim_stale_seconds=claim_stale_seconds,
            limit=limit,
            max_steps=remaining_steps,
            publish_bound_outputs=publish_bound_outputs,
            progress=progress,
        )
        step_calls += int(session_payload.get("step_calls") or 0)
        batch_count += int(session_payload.get("batch_count") or 0)
        batch_item_count += int(session_payload.get("batch_item_count") or 0)
        item_failures += int(session_payload.get("item_failures") or 0)
        step_payloads.extend(list(session_payload.get("steps") or []))
        processed_items.extend(list(session_payload.get("processed_items") or []))
        final_run_payload = dict(session_payload.get("run") or final_run_payload or {})
        if session_payload.get("publish") is not None:
            published_payload = session_payload.get("publish")
        if session_payload.get("publish_error"):
            publish_error = str(session_payload["publish_error"])
        if session_payload.get("results"):
            results = list(session_payload.get("results") or [])
        worker_finish_payload = session_payload.get("worker_finish")
        if isinstance(worker_finish_payload, dict):
            worker_finishes.append(worker_finish_payload)
        session_reason = str(session_payload.get("session_reason") or "")
        if session_reason == "handoff":
            active_claimed_by = str(
                session_payload.get("handoff_claimed_by_hint")
                or native_handoff_claimed_by_hint(final_run_payload, active_claimed_by)
            )
            continue
        break
    return {
        "status": "ok",
        "run_id": run_id,
        "claimed_by": base_claimed_by,
        "step_calls": step_calls,
        "batch_count": batch_count,
        "batch_item_count": batch_item_count,
        "item_failures": item_failures,
        "steps": step_payloads,
        "processed_items": processed_items,
        "run": final_run_payload,
        "results": results,
        "publish": published_payload,
        "publish_error": publish_error,
        "worker_finish": worker_finishes[-1] if worker_finishes else None,
        "worker_finishes": worker_finishes,
    }


def field_value_type(field_type: str) -> str:
    mapping = {
        "boolean": "boolean",
        "date": "date",
        "integer": "integer",
        "real": "real",
        "text": "text",
    }
    normalized = str(field_type or "").strip().lower()
    if normalized not in mapping:
        raise ValueError(f"Unsupported field type: {field_type!r}")
    return mapping[normalized]


def handle_run(tools: Any, args: argparse.Namespace) -> dict[str, object]:
    root = Path(args.workspace).expanduser().resolve()
    if args.as_worker:
        worker_claimed_by = str(args.claimed_by or f"retriever-run-{int(args.run_id)}").strip()
        return {
            "command": "run",
            "worker_mode": True,
            **run_worker_session(
                tools,
                root,
                command_name="run",
                run_id=int(args.run_id),
                claimed_by=worker_claimed_by,
                budget_seconds=int(args.budget_seconds),
                claim_stale_seconds=int(args.claim_stale_seconds),
                limit=args.limit,
                max_steps=args.max_steps,
                publish_bound_outputs=(not args.no_publish),
            ),
        }
    return {
        "command": "run",
        **run_to_completion(
            tools,
            root,
            command_name="run",
            run_id=int(args.run_id),
            claimed_by=args.claimed_by,
            budget_seconds=int(args.budget_seconds),
            claim_stale_seconds=int(args.claim_stale_seconds),
            limit=args.limit,
            max_steps=args.max_steps,
            publish_bound_outputs=(not args.no_publish),
        ),
    }


def handle_translate(tools: Any, args: argparse.Namespace) -> dict[str, object]:
    root = Path(args.workspace).expanduser().resolve()
    provider = choose_provider(tools, "translation", args.provider)
    parameters = parse_json_object(args.parameters_json, label="Translation parameters")
    parameters["target_language"] = str(args.target_language).strip()
    model = choose_model(tools, "translation", provider, args.model)
    job_name = args.job_name or f"translate_{args.target_language}"
    job_payload, job_created = ensure_job(tools, root, job_name, "translation", "Retriever native translation job")
    version_payload = tools.create_job_version(
        root,
        str(job_payload["job_name"]),
        instruction=args.instruction,
        provider=provider,
        capability="text_translation",
        model=model,
        input_basis="active_search_text",
        response_schema_json=None,
        parameters_json=compact_json(parameters),
        segment_profile=None,
        aggregation_strategy=None,
        display_name=None,
    )
    run_payload = create_processing_run(
        tools,
        root,
        job_version_id=int(version_payload["job_version"]["id"]),
        args=args,
        activation_policy=args.activation_policy,
    )
    run_id = int(run_payload["run"]["id"])
    execution_payload = run_to_completion(
        tools,
        root,
        command_name="translate",
        run_id=run_id,
        claimed_by=None,
        budget_seconds=int(args.budget_seconds),
        claim_stale_seconds=int(args.claim_stale_seconds),
        limit=None,
        max_steps=args.max_steps,
        publish_bound_outputs=False,
    )
    return {
        "status": "ok",
        "command": "translate",
        "target_language": parameters["target_language"],
        "job_created": job_created,
        "job": job_payload,
        "job_version": version_payload["job_version"],
        **execution_payload,
    }


def handle_extract(tools: Any, args: argparse.Namespace) -> dict[str, object]:
    root = Path(args.workspace).expanduser().resolve()
    provider = choose_provider(tools, "structured_extraction", args.provider)
    parameters = parse_json_object(args.parameters_json, label="Extraction parameters")
    response_schema_json = None
    if args.response_schema_json is not None:
        response_schema_json = compact_json(parse_json_object(args.response_schema_json, label="Response schema"))
    model = choose_model(tools, "structured_extraction", provider, args.model)
    field_payload = tools.add_field(root, args.field_name, args.field_type, args.instruction)
    normalized_field_name = str(field_payload["field_name"])
    job_name = args.job_name or f"extract_{normalized_field_name}"
    job_payload, job_created = ensure_job(
        tools,
        root,
        job_name,
        "structured_extraction",
        f"Retriever native extraction job for {normalized_field_name}",
    )
    output_payload = ensure_output_binding(
        tools,
        root,
        job_name=str(job_payload["job_name"]),
        output_name=normalized_field_name,
        value_type=field_value_type(args.field_type),
        bound_custom_field=normalized_field_name,
        description=args.instruction,
    )
    version_payload = tools.create_job_version(
        root,
        str(job_payload["job_name"]),
        instruction=args.instruction,
        provider=provider,
        capability="text_structured",
        model=model,
        input_basis="active_search_text",
        response_schema_json=response_schema_json,
        parameters_json=compact_json(parameters),
        segment_profile=None,
        aggregation_strategy=None,
        display_name=None,
    )
    run_payload = create_processing_run(
        tools,
        root,
        job_version_id=int(version_payload["job_version"]["id"]),
        args=args,
        activation_policy="manual",
    )
    run_id = int(run_payload["run"]["id"])
    execution_payload = run_to_completion(
        tools,
        root,
        command_name="extract",
        run_id=run_id,
        claimed_by=None,
        budget_seconds=int(args.budget_seconds),
        claim_stale_seconds=int(args.claim_stale_seconds),
        limit=None,
        max_steps=args.max_steps,
        publish_bound_outputs=True,
    )
    return {
        "status": "ok",
        "command": "extract",
        "field": field_payload,
        "job_created": job_created,
        "job": job_payload,
        "job_output": output_payload["job_output"],
        "job_version": version_payload["job_version"],
        **execution_payload,
    }


def handle_ocr(tools: Any, args: argparse.Namespace) -> dict[str, object]:
    root = Path(args.workspace).expanduser().resolve()
    provider = choose_provider(tools, "ocr", args.provider)
    parameters = parse_json_object(args.parameters_json, label="OCR parameters")
    model = choose_model(tools, "ocr", provider, args.model)
    job_name = args.job_name or "ocr"
    job_payload, job_created = ensure_job(tools, root, job_name, "ocr", "Retriever native OCR job")
    version_payload = tools.create_job_version(
        root,
        str(job_payload["job_name"]),
        instruction=args.instruction,
        provider=provider,
        capability="vision_ocr",
        model=model,
        input_basis="source_parts",
        response_schema_json=None,
        parameters_json=compact_json(parameters),
        segment_profile=None,
        aggregation_strategy=None,
        display_name=None,
    )
    run_payload = create_processing_run(
        tools,
        root,
        job_version_id=int(version_payload["job_version"]["id"]),
        args=args,
        activation_policy=args.activation_policy,
    )
    run_id = int(run_payload["run"]["id"])
    execution_payload = run_to_completion(
        tools,
        root,
        command_name="ocr",
        run_id=run_id,
        claimed_by=None,
        budget_seconds=int(args.budget_seconds),
        claim_stale_seconds=int(args.claim_stale_seconds),
        limit=None,
        max_steps=args.max_steps,
        publish_bound_outputs=False,
    )
    return {
        "status": "ok",
        "command": "ocr",
        "job_created": job_created,
        "job": job_payload,
        "job_version": version_payload["job_version"],
        **execution_payload,
    }


def handle_describe_images(tools: Any, args: argparse.Namespace) -> dict[str, object]:
    root = Path(args.workspace).expanduser().resolve()
    provider = choose_provider(tools, "image_description", args.provider)
    parameters = parse_json_object(args.parameters_json, label="Image description parameters")
    model = choose_model(tools, "image_description", provider, args.model)
    job_name = args.job_name or "describe_images"
    job_payload, job_created = ensure_job(
        tools,
        root,
        job_name,
        "image_description",
        "Retriever native image-description job",
    )
    version_payload = tools.create_job_version(
        root,
        str(job_payload["job_name"]),
        instruction=args.instruction,
        provider=provider,
        capability="vision_description",
        model=model,
        input_basis="source_parts",
        response_schema_json=None,
        parameters_json=compact_json(parameters),
        segment_profile=None,
        aggregation_strategy=None,
        display_name=None,
    )
    run_payload = create_processing_run(
        tools,
        root,
        job_version_id=int(version_payload["job_version"]["id"]),
        args=args,
        activation_policy=args.activation_policy,
    )
    run_id = int(run_payload["run"]["id"])
    execution_payload = run_to_completion(
        tools,
        root,
        command_name="describe-images",
        run_id=run_id,
        claimed_by=None,
        budget_seconds=int(args.budget_seconds),
        claim_stale_seconds=int(args.claim_stale_seconds),
        limit=None,
        max_steps=args.max_steps,
        publish_bound_outputs=False,
    )
    return {
        "status": "ok",
        "command": "describe-images",
        "job_created": job_created,
        "job": job_payload,
        "job_version": version_payload["job_version"],
        **execution_payload,
    }


def render_human_summary(payload: dict[str, object]) -> str:
    command = str(payload.get("command") or "run")
    run = dict(payload.get("run") or {})
    run_id = payload.get("run_id") or run.get("id")
    status = str(run.get("status") or payload.get("status") or "ok")
    worker_mode = bool(payload.get("worker_mode"))
    session_reason = str(payload.get("session_reason") or "")
    completed = int(run.get("completed_count") or 0)
    failed = int(run.get("failed_count") or 0)
    planned = int(run.get("planned_count") or 0)
    skipped = int(run.get("skipped_count") or 0)
    results_count = len(list(payload.get("results") or []))
    headline_map = {
        "run": "Processing run",
        "translate": "Translation",
        "extract": "Extraction",
        "ocr": "OCR",
        "describe-images": "Image description",
    }
    headline = headline_map.get(command, "Processing run")
    if worker_mode and session_reason == "handoff":
        lines = [
            f"Stopped. {headline} worker session reached handoff (run {run_id}, {completed}/{planned} completed, {failed} failed)."
        ]
    elif worker_mode and session_reason == "idle":
        lines = [
            f"Stopped. {headline} worker session paused (run {run_id}, {completed}/{planned} completed, {failed} failed)."
        ]
    elif status == "completed":
        lines = [
            f"Done. {headline} completed (run {run_id}, {completed}/{planned} completed, {failed} failed)."
        ]
    elif status == "canceled":
        lines = [
            f"Stopped. {headline} canceled (run {run_id}, {completed}/{planned} completed, {failed} failed)."
        ]
    else:
        lines = [
            f"{headline} finished with status {status} (run {run_id}, {completed}/{planned} completed, {failed} failed)."
        ]
    notes: list[str] = []
    if skipped:
        notes.append(f"{skipped} skipped")
    if results_count:
        notes.append(f"{results_count} results")
    publish_payload = payload.get("publish")
    if isinstance(publish_payload, dict):
        published = int(publish_payload.get("published_count") or 0)
        if published:
            notes.append(f"{published} field values published")
    if payload.get("publish_error"):
        notes.append(f"publish warning: {payload['publish_error']}")
    if worker_mode:
        notes.append("worker session")
    handoff_claimed_by_hint = str(payload.get("handoff_claimed_by_hint") or "").strip()
    if handoff_claimed_by_hint:
        notes.append(f"next worker `{handoff_claimed_by_hint}`")
    if command == "translate":
        target_language = payload.get("target_language")
        if target_language:
            notes.append(f"target language {target_language}")
    if command == "extract":
        field_payload = dict(payload.get("field") or {})
        if field_payload.get("field_name"):
            notes.append(f"field `{field_payload['field_name']}`")
    if payload.get("batch_count"):
        notes.append(f"{int(payload['batch_count'])} batches")
    if notes:
        lines.append("Notes: " + ", ".join(notes) + ".")
    return "\n".join(lines)


def emit_payload(output_mode: str, payload: dict[str, object]) -> int:
    if output_mode == "json":
        sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    else:
        sys.stdout.write(render_human_summary(payload) + "\n")
    sys.stdout.flush()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    tools = load_tools_module()
    parser = build_parser(tools)
    args = parser.parse_args(list(argv) if argv is not None else sys.argv[1:])
    if args.human:
        args.output = "human"

    root = Path(args.workspace).expanduser().resolve()
    tools.set_active_workspace_root(root)
    try:
        if args.command == "run":
            payload = handle_run(tools, args)
        elif args.command == "translate":
            payload = handle_translate(tools, args)
        elif args.command == "extract":
            payload = handle_extract(tools, args)
        elif args.command == "ocr":
            payload = handle_ocr(tools, args)
        elif args.command == "describe-images":
            payload = handle_describe_images(tools, args)
        else:  # pragma: no cover - argparse keeps this unreachable
            raise tools.RetrieverError(f"Unknown native Retriever command: {args.command}")
    except Exception as exc:
        if args.output == "json":
            sys.stdout.write(json.dumps({"status": "error", "error": str(exc)}, indent=2, sort_keys=True) + "\n")
        else:
            sys.stdout.write(f"Error: {exc}\n")
        sys.stdout.flush()
        return 1
    return emit_payload(str(args.output), payload)
