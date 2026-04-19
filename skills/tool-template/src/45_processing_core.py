JOB_KINDS = {
    "embedding",
    "image_description",
    "ocr",
    "structured_extraction",
    "translation",
}
JOB_CAPABILITIES = {
    "text_structured",
    "text_translation",
    "vision_description",
    "vision_ocr",
}
JOB_INPUT_BASES = {
    "active_search_text",
    "source_extract",
    "source_file",
    "source_parts",
    "text_revision",
}
JOB_OUTPUT_VALUE_TYPES = {
    "boolean",
    "date",
    "integer",
    "json",
    "real",
    "text",
}
RUN_FAMILY_MODES = {"exact", "with_family"}
RUN_ITEM_KINDS = {"document", "page", "segment"}
RUN_ITEM_STATUSES = {"completed", "failed", "pending", "running", "skipped"}
RUN_STATUSES = {"canceled", "completed", "failed", "planned", "running"}
RUN_WORKER_MODES = {"background", "inline"}
RUN_WORKER_STATUSES = {"active", "canceled", "completed", "failed", "orphaned", "stopped"}
TEXT_REVISION_ACTIVATION_POLICIES = {"always", "if_empty", "if_poor", "manual"}
DEFAULT_RUN_ITEM_CLAIM_STALE_SECONDS = 900
DEFAULT_RUN_ITEM_CONTEXT_INLINE_BYTES = 50 * 1024
DEFAULT_RUN_ITEM_CLAIM_BATCH_SIZE = 10
DEFAULT_WORKER_BATCH_SIZE = 5
DEFAULT_WORKER_INLINE_MAX_ITEMS = 5
DEFAULT_WORKER_INLINE_MAX_BATCHES = 12
DEFAULT_WORKER_BACKGROUND_MAX_BATCHES = 3
DEFAULT_WORKER_BACKGROUND_WAKE_INTERVAL_SECONDS = 60
DEFAULT_WORKER_BACKGROUND_MAX_PARALLEL = 4
DEFAULT_OCR_RENDER_RESOLUTION = 150


def sanitize_processing_identifier(raw_name: str, *, label: str, prefix: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", raw_name.strip()).strip("_").lower()
    if not sanitized:
        raise RetrieverError(f"{label} becomes empty after sanitization.")
    if sanitized[0].isdigit():
        sanitized = f"{prefix}_{sanitized}"
    return sanitized


def normalize_job_kind(job_kind: str) -> str:
    normalized = normalize_whitespace(job_kind).lower()
    if normalized not in JOB_KINDS:
        raise RetrieverError(
            f"Unsupported job kind: {job_kind!r}. Expected one of {', '.join(sorted(JOB_KINDS))}."
        )
    return normalized


def normalize_job_capability(capability: str) -> str:
    normalized = normalize_whitespace(capability).lower()
    if normalized not in JOB_CAPABILITIES:
        raise RetrieverError(
            f"Unsupported capability: {capability!r}. Expected one of {', '.join(sorted(JOB_CAPABILITIES))}."
        )
    return normalized


def default_job_capability_for_kind(job_kind: str) -> str:
    normalized_kind = normalize_job_kind(job_kind)
    if normalized_kind == "structured_extraction":
        return "text_structured"
    if normalized_kind == "translation":
        return "text_translation"
    if normalized_kind == "image_description":
        return "vision_description"
    if normalized_kind == "ocr":
        return "vision_ocr"
    raise RetrieverError(
        f"Job kind {normalized_kind!r} does not have a default Cowork capability. "
        "Pass an explicit capability when creating the job version."
    )


def default_job_input_basis_for_kind(job_kind: str) -> str:
    normalized_kind = normalize_job_kind(job_kind)
    if normalized_kind == "ocr":
        return "source_parts"
    if normalized_kind in {"structured_extraction", "translation", "embedding"}:
        return "active_search_text"
    if normalized_kind == "image_description":
        return "source_parts"
    raise RetrieverError(
        f"Job kind {normalized_kind!r} does not have a default input basis. "
        "Pass an explicit input basis when creating the job version."
    )


def normalize_job_input_basis(input_basis: str) -> str:
    normalized = normalize_whitespace(input_basis).lower()
    if normalized not in JOB_INPUT_BASES:
        raise RetrieverError(
            f"Unsupported input basis: {input_basis!r}. Expected one of {', '.join(sorted(JOB_INPUT_BASES))}."
        )
    return normalized


def normalize_job_output_value_type(value_type: str) -> str:
    normalized = normalize_whitespace(value_type).lower()
    if normalized not in JOB_OUTPUT_VALUE_TYPES:
        raise RetrieverError(
            f"Unsupported job output value type: {value_type!r}. "
            f"Expected one of {', '.join(sorted(JOB_OUTPUT_VALUE_TYPES))}."
        )
    return normalized


def normalize_run_family_mode(family_mode: str) -> str:
    normalized = normalize_whitespace(family_mode).lower()
    if normalized not in RUN_FAMILY_MODES:
        raise RetrieverError(
            f"Unsupported family mode: {family_mode!r}. Expected one of {', '.join(sorted(RUN_FAMILY_MODES))}."
        )
    return normalized


def normalize_run_item_kind(item_kind: str) -> str:
    normalized = normalize_whitespace(item_kind).lower()
    if normalized not in RUN_ITEM_KINDS:
        raise RetrieverError(
            f"Unsupported run item kind: {item_kind!r}. Expected one of {', '.join(sorted(RUN_ITEM_KINDS))}."
        )
    return normalized


def normalize_run_item_status(status: str) -> str:
    normalized = normalize_whitespace(status).lower()
    if normalized not in RUN_ITEM_STATUSES:
        raise RetrieverError(
            f"Unsupported run item status: {status!r}. Expected one of {', '.join(sorted(RUN_ITEM_STATUSES))}."
        )
    return normalized


def normalize_run_worker_mode(mode: str) -> str:
    normalized = normalize_whitespace(mode).lower()
    if normalized not in RUN_WORKER_MODES:
        raise RetrieverError(
            f"Unsupported run worker mode: {mode!r}. Expected one of {', '.join(sorted(RUN_WORKER_MODES))}."
        )
    return normalized


def normalize_run_worker_status(status: str) -> str:
    normalized = normalize_whitespace(status).lower()
    if normalized not in RUN_WORKER_STATUSES:
        raise RetrieverError(
            f"Unsupported run worker status: {status!r}. Expected one of {', '.join(sorted(RUN_WORKER_STATUSES))}."
        )
    return normalized


def normalize_text_revision_activation_policy(policy: str) -> str:
    normalized = normalize_whitespace(policy).lower()
    if normalized not in TEXT_REVISION_ACTIVATION_POLICIES:
        raise RetrieverError(
            f"Unsupported activation policy: {policy!r}. "
            f"Expected one of {', '.join(sorted(TEXT_REVISION_ACTIVATION_POLICIES))}."
        )
    return normalized


def compact_json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def parse_json_argument(raw_value: str | None, *, label: str, default: object) -> object:
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        return json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RetrieverError(f"{label} must be valid JSON.") from exc


def parse_json_object_argument(raw_value: str | None, *, label: str, default: dict[str, object] | None = None) -> dict[str, object]:
    parsed = parse_json_argument(raw_value, label=label, default=default or {})
    if not isinstance(parsed, dict):
        raise RetrieverError(f"{label} must decode to a JSON object.")
    return parsed


def decode_json_text(raw_value: object, *, default: object = None) -> object:
    if raw_value in (None, ""):
        return default
    if isinstance(raw_value, (dict, list, int, float, bool)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def build_text_revision_input_identity(input_revision_id: int) -> str:
    return sha256_text(str(int(input_revision_id)))


def build_ocr_input_identity(
    source_file_hash: str,
    *,
    rendering_settings: dict[str, object] | None = None,
    backend_id: str,
) -> str:
    normalized_settings = compact_json_text(rendering_settings or {})
    return sha256_text(f"{source_file_hash}||{normalized_settings}||{backend_id}")


def build_translation_input_identity(source_revision_id: int, *, target_language: str) -> str:
    normalized_language = normalize_whitespace(target_language).lower()
    return sha256_text(f"{int(source_revision_id)}||{normalized_language}")


def build_image_source_input_identity(
    source_file_hash: str,
    *,
    image_prep_settings: dict[str, object] | None = None,
    backend_id: str,
) -> str:
    normalized_settings = compact_json_text(image_prep_settings or {})
    return sha256_text(f"{source_file_hash}||{normalized_settings}||{backend_id}")


def build_segment_input_identity(segment_id: int) -> str:
    return sha256_text(str(int(segment_id)))
