def job_output_schema_fragment(value_type: str) -> dict[str, object]:
    normalized_value_type = normalize_job_output_value_type(value_type)
    if normalized_value_type == "boolean":
        return {"type": "boolean"}
    if normalized_value_type == "date":
        return {"type": "string", "description": "ISO-8601 date string"}
    if normalized_value_type == "integer":
        return {"type": "integer"}
    if normalized_value_type == "json":
        return {"type": "object"}
    if normalized_value_type == "real":
        return {"type": "number"}
    return {"type": "string"}


def derive_job_output_response_schema(job_output_rows: list[sqlite3.Row]) -> dict[str, object]:
    properties: dict[str, object] = {}
    required: list[str] = []
    for row in job_output_rows:
        output_name = str(row["output_name"])
        properties[output_name] = job_output_schema_fragment(str(row["value_type"]))
        required.append(output_name)
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def best_effort_document_source_path(root: Path, document_row: sqlite3.Row) -> str | None:
    source_rel_path = normalize_whitespace(str(document_row["source_rel_path"] or ""))
    if not source_rel_path:
        return None
    candidate = (root / source_rel_path).resolve()
    if candidate.exists():
        return str(candidate)
    return None


def build_text_input_reference(
    paths: dict[str, Path],
    revision_row: sqlite3.Row,
    *,
    inline_bytes: int = DEFAULT_RUN_ITEM_CONTEXT_INLINE_BYTES,
) -> dict[str, object]:
    revision_body = read_text_revision_body(paths, revision_row["storage_rel_path"])
    if revision_body is None:
        raise RetrieverError(f"Text revision {revision_row['id']} has no readable body on disk.")
    encoded_size = len(revision_body.encode("utf-8"))
    storage_path = str((paths["state_dir"] / str(revision_row["storage_rel_path"])).resolve())
    return {
        "kind": "text_revision",
        "text_revision_id": int(revision_row["id"]),
        "inline_text": revision_body if encoded_size <= inline_bytes else None,
        "text_path": None if encoded_size <= inline_bytes else storage_path,
        "bytes": encoded_size,
    }


def build_run_item_context_payload(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    root: Path,
    run_item_row: sqlite3.Row,
    *,
    inline_bytes: int = DEFAULT_RUN_ITEM_CONTEXT_INLINE_BYTES,
) -> dict[str, object]:
    run_row = require_run_row_by_id(connection, int(run_item_row["run_id"]))
    job_version_row = require_job_version_row_by_id(connection, int(run_row["job_version_id"]))
    job_row = connection.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = ?
        """,
        (job_version_row["job_id"],),
    ).fetchone()
    assert job_row is not None
    snapshot_row = None
    if run_item_row["run_snapshot_document_id"] is not None:
        snapshot_row = connection.execute(
            """
            SELECT *
            FROM run_snapshot_documents
            WHERE id = ?
            """,
            (run_item_row["run_snapshot_document_id"],),
        ).fetchone()
    document_row = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE id = ?
        """,
        (run_item_row["document_id"],),
    ).fetchone()
    if document_row is None:
        raise RetrieverError(f"Unknown document id: {run_item_row['document_id']}")
    job_output_rows = connection.execute(
        """
        SELECT *
        FROM job_outputs
        WHERE job_id = ?
        ORDER BY ordinal ASC, output_name ASC, id ASC
        """,
        (job_row["id"],),
    ).fetchall()
    response_schema = decode_json_text(job_version_row["response_schema_json"])
    if response_schema is None and str(job_version_row["capability"]) == "text_structured":
        response_schema = derive_job_output_response_schema(job_output_rows)

    item_kind = str(run_item_row["item_kind"] or "")
    if item_kind == "page":
        artifact_rel_path = normalize_whitespace(str(run_item_row["input_artifact_rel_path"] or ""))
        artifact_path = resolve_workspace_artifact_path(root, artifact_rel_path)
        if artifact_path is None or not artifact_path.exists():
            raise RetrieverError(f"Run item {run_item_row['id']} points at a missing OCR artifact: {artifact_rel_path!r}")
        input_payload = {
            "kind": "ocr_page_image",
            "page_number": int(run_item_row["page_number"] or 0),
            "artifact_rel_path": artifact_rel_path,
            "artifact_path": str(artifact_path),
            "bytes": artifact_path.stat().st_size,
            "text_revision_id": None,
            "inline_text": None,
            "text_path": None,
            "source_path": best_effort_document_source_path(root, document_row),
            "source_rel_path": document_row["source_rel_path"],
            "file_hash": document_row["file_hash"],
        }
    elif snapshot_row is not None and snapshot_row["pinned_input_revision_id"] is not None:
        revision_row = require_text_revision_row_by_id(connection, int(snapshot_row["pinned_input_revision_id"]))
        input_payload = build_text_input_reference(paths, revision_row, inline_bytes=inline_bytes)
    else:
        input_payload = {
            "kind": str(job_version_row["input_basis"]),
            "text_revision_id": None,
            "inline_text": None,
            "text_path": None,
            "bytes": None,
            "source_path": best_effort_document_source_path(root, document_row),
            "source_rel_path": document_row["source_rel_path"],
            "file_hash": document_row["file_hash"],
        }

    return {
        "run": run_row_to_payload(run_row),
        "run_item": run_item_row_to_payload(run_item_row),
        "job": job_row_to_payload(job_row),
        "job_version": job_version_row_to_payload(job_version_row),
        "job_outputs": [job_output_row_to_payload(row) for row in job_output_rows],
        "response_schema": response_schema,
        "document": {
            "id": int(document_row["id"]),
            "file_name": document_row["file_name"],
            "file_type": document_row["file_type"],
            "source_kind": document_row["source_kind"],
            "source_rel_path": document_row["source_rel_path"],
            "source_path": best_effort_document_source_path(root, document_row),
            "active_search_text_revision_id": document_row["active_search_text_revision_id"],
            "source_text_revision_id": document_row["source_text_revision_id"],
        },
        "input": input_payload,
        "inclusion_reason": (
            decode_json_text(snapshot_row["inclusion_reason_json"], default={}) or {}
            if snapshot_row is not None
            else {}
        ),
    }
