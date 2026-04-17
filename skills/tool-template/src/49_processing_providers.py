OPENAI_RESPONSES_PROVIDER_NAMES = {"openai", "openai_responses"}
STATIC_STRUCTURED_EXTRACTION_PROVIDER_NAMES = {"builtin_static_json", "static_json"}
STATIC_TRANSLATION_PROVIDER_NAMES = {"builtin_static_text", "static_text"}
OPENAI_RESPONSES_DEFAULT_URL = "https://api.openai.com/v1/responses"


def render_processing_template_value(value: object, context: dict[str, object]) -> object:
    if isinstance(value, str):
        normalized_context = defaultdict(
            str,
            {
                key: "" if item is None else str(item)
                for key, item in context.items()
            },
        )
        try:
            return value.format_map(normalized_context)
        except Exception:
            return value
    if isinstance(value, list):
        return [render_processing_template_value(item, context) for item in value]
    if isinstance(value, dict):
        return {
            str(key): render_processing_template_value(item, context)
            for key, item in value.items()
        }
    return value


def processing_job_output_json_schema(job_output_rows: list[sqlite3.Row]) -> dict[str, object]:
    properties: dict[str, object] = {}
    required: list[str] = []
    for job_output_row in job_output_rows:
        output_name = str(job_output_row["output_name"])
        value_type = normalize_job_output_value_type(str(job_output_row["value_type"] or "text"))
        schema: dict[str, object]
        if value_type == "boolean":
            schema = {"type": "boolean"}
        elif value_type == "date":
            schema = {"type": "string"}
        elif value_type == "integer":
            schema = {"type": "integer"}
        elif value_type == "real":
            schema = {"type": "number"}
        elif value_type == "json":
            schema = {
                "type": "object",
                "additionalProperties": True,
            }
        else:
            schema = {"type": "string"}
        properties[output_name] = schema
        required.append(output_name)
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def processing_response_schema(
    job_version_row: sqlite3.Row,
    job_output_rows: list[sqlite3.Row],
) -> dict[str, object]:
    parsed = decode_json_text(job_version_row["response_schema_json"])
    if isinstance(parsed, dict) and parsed:
        return parsed
    return processing_job_output_json_schema(job_output_rows)


def openai_responses_api_url() -> str:
    explicit_url = normalize_whitespace(os.environ.get("OPENAI_RESPONSES_URL", ""))
    if explicit_url:
        return explicit_url
    base_url = normalize_whitespace(os.environ.get("OPENAI_BASE_URL", ""))
    if base_url:
        return base_url.rstrip("/") + "/responses"
    return OPENAI_RESPONSES_DEFAULT_URL


def openai_api_key() -> str:
    api_key = normalize_whitespace(os.environ.get("OPENAI_API_KEY", ""))
    if not api_key:
        raise RetrieverError("OPENAI_API_KEY is required for openai_responses providers.")
    return api_key


def openai_response_text(response_payload: dict[str, object]) -> str:
    top_level_output_text = response_payload.get("output_text")
    if isinstance(top_level_output_text, str) and top_level_output_text.strip():
        return top_level_output_text
    output_items = response_payload.get("output")
    if not isinstance(output_items, list):
        raise RetrieverError("OpenAI response payload did not contain output text.")
    text_parts: list[str] = []
    for output_item in output_items:
        if not isinstance(output_item, dict):
            continue
        content_items = output_item.get("content")
        if not isinstance(content_items, list):
            continue
        for content_item in content_items:
            if not isinstance(content_item, dict):
                continue
            if str(content_item.get("type") or "") != "output_text":
                continue
            text_value = content_item.get("text")
            if isinstance(text_value, str):
                text_parts.append(text_value)
    if text_parts:
        return "".join(text_parts)
    raise RetrieverError("OpenAI response payload did not contain output_text content.")


def openai_response_usage(response_payload: dict[str, object]) -> tuple[int | None, int | None]:
    usage = response_payload.get("usage")
    if not isinstance(usage, dict):
        return None, None
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    return (
        int(input_tokens) if isinstance(input_tokens, int) else None,
        int(output_tokens) if isinstance(output_tokens, int) else None,
    )


def call_openai_responses_api(
    *,
    payload: dict[str, object],
    timeout_seconds: float,
) -> dict[str, object]:
    request_bytes = compact_json_text(payload).encode("utf-8")
    request = urllib_request.Request(
        openai_responses_api_url(),
        data=request_bytes,
        headers={
            "Authorization": f"Bearer {openai_api_key()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
            response_body = response.read().decode("utf-8")
    except urllib_error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RetrieverError(f"OpenAI Responses API request failed: HTTP {exc.code}: {error_body}") from exc
    except urllib_error.URLError as exc:
        raise RetrieverError(f"OpenAI Responses API request failed: {exc.reason}") from exc

    try:
        parsed = json.loads(response_body)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RetrieverError("OpenAI Responses API returned invalid JSON.") from exc
    if not isinstance(parsed, dict):
        raise RetrieverError("OpenAI Responses API returned an unexpected response shape.")
    return parsed


def response_api_request_overrides(parameters: dict[str, object]) -> dict[str, object]:
    overrides: dict[str, object] = {}
    for key in (
        "max_output_tokens",
        "metadata",
        "reasoning",
        "service_tier",
        "temperature",
        "top_p",
        "truncation",
    ):
        if key in parameters:
            overrides[key] = parameters[key]
    return overrides


def processing_input_context(document_row: sqlite3.Row, text_input: str | None) -> dict[str, object]:
    return {
        "document_id": int(document_row["id"]),
        "control_number": document_row["control_number"],
        "file_name": document_row["file_name"],
        "rel_path": document_row["rel_path"],
        "title": document_row["title"],
        "subject": document_row["subject"],
        "text": text_input or "",
    }


def execute_static_structured_extraction_provider(
    *,
    job_version_row: sqlite3.Row,
    job_output_rows: list[sqlite3.Row],
    document_row: sqlite3.Row,
    text_input: str | None,
) -> dict[str, object]:
    parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
    if not isinstance(parameters, dict):
        parameters = {}
    output_values = parameters.get("output_values")
    if not isinstance(output_values, dict):
        raise RetrieverError("static_json structured extraction requires parameters_json.output_values to be an object.")
    context = processing_input_context(document_row, text_input)
    normalized_outputs: dict[str, object] = {}
    for job_output_row in job_output_rows:
        output_name = str(job_output_row["output_name"])
        normalized_outputs[output_name] = render_processing_template_value(output_values.get(output_name), context)
    output_json = compact_json_text(normalized_outputs)
    return {
        "raw_output": normalized_outputs,
        "normalized_output": normalized_outputs,
        "output_values": normalized_outputs,
        "provider_request_id": None,
        "input_tokens": token_estimate(text_input or "") if text_input is not None else None,
        "output_tokens": token_estimate(output_json) if normalized_outputs else 0,
        "cost_cents": 0,
        "provider_metadata": {
            "provider": str(job_version_row["provider"]),
            "model": job_version_row["model"],
            "executed_by": "structured_extraction_static_provider",
        },
    }


def build_openai_structured_extraction_payload(
    *,
    job_version_row: sqlite3.Row,
    job_output_rows: list[sqlite3.Row],
    document_row: sqlite3.Row,
    text_input: str | None,
) -> dict[str, object]:
    model = normalize_whitespace(str(job_version_row["model"] or ""))
    if not model:
        raise RetrieverError("openai_responses structured extraction requires a model name.")
    parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
    if not isinstance(parameters, dict):
        parameters = {}
    instruction_text = str(job_version_row["instruction_text"] or "").strip() or "Extract the requested structured outputs."
    schema = processing_response_schema(job_version_row, job_output_rows)
    schema_name = sanitize_processing_identifier(
        str(job_version_row["display_name"] or f"job_version_{job_version_row['id']}"),
        label="Schema name",
        prefix="schema",
    )[:64]
    user_content = (
        "Return structured JSON for the following document text.\n\n"
        f"<document>\n{text_input or ''}\n</document>"
    )
    payload: dict[str, object] = {
        "model": model,
        "store": False,
        "input": [
            {"role": "system", "content": instruction_text},
            {"role": "user", "content": user_content},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "schema": schema,
                "strict": True,
            }
        },
    }
    payload.update(response_api_request_overrides(parameters))
    return payload


def execute_openai_structured_extraction_provider(
    *,
    job_version_row: sqlite3.Row,
    job_output_rows: list[sqlite3.Row],
    document_row: sqlite3.Row,
    text_input: str | None,
) -> dict[str, object]:
    parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
    if not isinstance(parameters, dict):
        parameters = {}
    timeout_seconds = float(parameters.get("timeout_seconds") or 60.0)
    request_payload = build_openai_structured_extraction_payload(
        job_version_row=job_version_row,
        job_output_rows=job_output_rows,
        document_row=document_row,
        text_input=text_input,
    )
    response_payload = call_openai_responses_api(
        payload=request_payload,
        timeout_seconds=timeout_seconds,
    )
    output_text = openai_response_text(response_payload)
    try:
        normalized_outputs = json.loads(output_text)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RetrieverError("OpenAI structured extraction did not return valid JSON.") from exc
    if not isinstance(normalized_outputs, dict):
        raise RetrieverError("OpenAI structured extraction must return a JSON object.")
    input_tokens, output_tokens = openai_response_usage(response_payload)
    return {
        "raw_output": response_payload,
        "normalized_output": normalized_outputs,
        "output_values": normalized_outputs,
        "provider_request_id": response_payload.get("id"),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_cents": None,
        "provider_metadata": {
            "provider": str(job_version_row["provider"]),
            "model": job_version_row["model"],
            "response_id": response_payload.get("id"),
            "status": response_payload.get("status"),
        },
    }


def execute_static_translation_provider(
    *,
    job_version_row: sqlite3.Row,
    document_row: sqlite3.Row,
    text_input: str | None,
) -> dict[str, object]:
    parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
    if not isinstance(parameters, dict):
        parameters = {}
    translated_template = parameters.get("translated_text")
    if translated_template is None:
        raise RetrieverError("static_text translation requires parameters_json.translated_text.")
    context = processing_input_context(document_row, text_input)
    translated_text = render_processing_template_value(translated_template, context)
    if not isinstance(translated_text, str):
        translated_text = compact_json_text(translated_text)
    target_language = normalize_whitespace(
        str(parameters.get("target_language") or parameters.get("target_lang") or parameters.get("language") or "")
    ).lower() or None
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
        "input_tokens": token_estimate(text_input or "") if text_input is not None else None,
        "output_tokens": token_estimate(translated_text),
        "cost_cents": 0,
        "provider_metadata": {
            "provider": str(job_version_row["provider"]),
            "model": job_version_row["model"],
            "executed_by": "translation_static_provider",
            "target_language": target_language,
        },
    }


def build_openai_translation_payload(
    *,
    job_version_row: sqlite3.Row,
    text_input: str | None,
) -> tuple[dict[str, object], str | None]:
    model = normalize_whitespace(str(job_version_row["model"] or ""))
    if not model:
        raise RetrieverError("openai_responses translation requires a model name.")
    parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
    if not isinstance(parameters, dict):
        parameters = {}
    target_language = normalize_whitespace(
        str(parameters.get("target_language") or parameters.get("target_lang") or parameters.get("language") or "")
    ).lower()
    if not target_language:
        raise RetrieverError("Translation job versions require parameters_json.target_language.")
    instruction_text = str(job_version_row["instruction_text"] or "").strip() or (
        f"Translate the document into {target_language}. Return only the translated text."
    )
    payload: dict[str, object] = {
        "model": model,
        "store": False,
        "input": [
            {"role": "system", "content": instruction_text},
            {
                "role": "user",
                "content": f"<document>\n{text_input or ''}\n</document>",
            },
        ],
        "text": {"format": {"type": "text"}},
    }
    payload.update(response_api_request_overrides(parameters))
    return payload, target_language


def execute_openai_translation_provider(
    *,
    job_version_row: sqlite3.Row,
    text_input: str | None,
) -> dict[str, object]:
    parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
    if not isinstance(parameters, dict):
        parameters = {}
    timeout_seconds = float(parameters.get("timeout_seconds") or 60.0)
    request_payload, target_language = build_openai_translation_payload(
        job_version_row=job_version_row,
        text_input=text_input,
    )
    response_payload = call_openai_responses_api(
        payload=request_payload,
        timeout_seconds=timeout_seconds,
    )
    translated_text = openai_response_text(response_payload)
    input_tokens, output_tokens = openai_response_usage(response_payload)
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
            "provider": str(job_version_row["provider"]),
            "model": job_version_row["model"],
            "response_id": response_payload.get("id"),
            "status": response_payload.get("status"),
            "target_language": target_language,
        },
    }


async def execute_job_provider(
    *,
    job_row: sqlite3.Row,
    job_version_row: sqlite3.Row,
    job_output_rows: list[sqlite3.Row],
    document_row: sqlite3.Row,
    text_input: str | None,
) -> dict[str, object]:
    provider = normalize_whitespace(str(job_version_row["provider"] or "")).lower()
    job_kind = normalize_job_kind(str(job_row["job_kind"]))

    if job_kind == "structured_extraction":
        if provider in STATIC_STRUCTURED_EXTRACTION_PROVIDER_NAMES:
            return execute_static_structured_extraction_provider(
                job_version_row=job_version_row,
                job_output_rows=job_output_rows,
                document_row=document_row,
                text_input=text_input,
            )
        if provider in OPENAI_RESPONSES_PROVIDER_NAMES:
            return await asyncio.to_thread(
                execute_openai_structured_extraction_provider,
                job_version_row=job_version_row,
                job_output_rows=job_output_rows,
                document_row=document_row,
                text_input=text_input,
            )

    if job_kind == "translation":
        if provider in STATIC_TRANSLATION_PROVIDER_NAMES:
            return execute_static_translation_provider(
                job_version_row=job_version_row,
                document_row=document_row,
                text_input=text_input,
            )
        if provider in OPENAI_RESPONSES_PROVIDER_NAMES:
            return await asyncio.to_thread(
                execute_openai_translation_provider,
                job_version_row=job_version_row,
                text_input=text_input,
            )

    raise RetrieverError(
        f"Unsupported job execution provider {provider!r} for job kind {job_kind!r}."
    )
