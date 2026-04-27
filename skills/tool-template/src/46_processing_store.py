def job_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "job_name": row["job_name"],
        "job_kind": row["job_kind"],
        "description": row["description"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "archived_at": row["archived_at"],
    }


def job_output_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "job_id": int(row["job_id"]),
        "output_name": row["output_name"],
        "value_type": row["value_type"],
        "bound_custom_field": row["bound_custom_field"],
        "description": row["description"],
        "ordinal": int(row["ordinal"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def job_version_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "job_id": int(row["job_id"]),
        "version": int(row["version"]),
        "display_name": row["display_name"],
        "instruction_text": row["instruction_text"],
        "instruction_hash": row["instruction_hash"],
        "response_schema": decode_json_text(row["response_schema_json"]),
        "capability": row["capability"],
        "provider": row["provider"],
        "model": row["model"],
        "parameters": decode_json_text(row["parameters_json"], default={}) or {},
        "input_basis": row["input_basis"],
        "segment_profile": row["segment_profile"],
        "aggregation_strategy": row["aggregation_strategy"],
        "created_at": row["created_at"],
        "archived_at": row["archived_at"],
    }


def find_job_row_by_name(connection: sqlite3.Connection, job_name: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM jobs
        WHERE job_name = ?
        """,
        (job_name,),
    ).fetchone()


def require_job_row_by_name(connection: sqlite3.Connection, job_name: str) -> sqlite3.Row:
    row = find_job_row_by_name(connection, job_name)
    if row is None:
        raise RetrieverError(f"Unknown job: {job_name}")
    return row


def job_outputs_for_job(connection: sqlite3.Connection, job_id: int) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM job_outputs
        WHERE job_id = ?
        ORDER BY ordinal ASC, output_name ASC, id ASC
        """,
        (job_id,),
    ).fetchall()
    return [job_output_row_to_payload(row) for row in rows]


def job_versions_for_job(connection: sqlite3.Connection, job_id: int) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM job_versions
        WHERE job_id = ?
        ORDER BY version DESC, id DESC
        """,
        (job_id,),
    ).fetchall()
    return [job_version_row_to_payload(row) for row in rows]


def latest_job_version_for_job(connection: sqlite3.Connection, job_id: int) -> dict[str, object] | None:
    row = connection.execute(
        """
        SELECT *
        FROM job_versions
        WHERE job_id = ?
        ORDER BY version DESC, id DESC
        LIMIT 1
        """,
        (job_id,),
    ).fetchone()
    return None if row is None else job_version_row_to_payload(row)


def job_summary_by_id(connection: sqlite3.Connection, job_id: int) -> dict[str, object]:
    row = connection.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = ?
        """,
        (job_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown job id: {job_id}")
    version_count_row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM job_versions
        WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()
    return {
        **job_row_to_payload(row),
        "outputs": job_outputs_for_job(connection, job_id),
        "job_version_count": int(version_count_row["count"] or 0),
        "latest_job_version": latest_job_version_for_job(connection, job_id),
    }


def list_job_summaries(connection: sqlite3.Connection) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM jobs
        ORDER BY job_name ASC, id ASC
        """
    ).fetchall()
    return [job_summary_by_id(connection, int(row["id"])) for row in rows]


def create_job_row(
    connection: sqlite3.Connection,
    *,
    job_name: str,
    job_kind: str,
    description: str | None,
) -> int:
    now = utc_now()
    cursor = connection.execute(
        """
        INSERT INTO jobs (job_name, job_kind, description, created_at, updated_at, archived_at)
        VALUES (?, ?, ?, ?, ?, NULL)
        """,
        (job_name, job_kind, description, now, now),
    )
    return int(cursor.lastrowid)


def next_job_output_ordinal(connection: sqlite3.Connection, job_id: int) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(ordinal), -1) AS max_ordinal
        FROM job_outputs
        WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()
    return int(row["max_ordinal"] or -1) + 1


def upsert_job_output_row(
    connection: sqlite3.Connection,
    *,
    job_id: int,
    output_name: str,
    value_type: str,
    bound_custom_field: str | None,
    description: str | None,
) -> tuple[int, bool]:
    existing_row = connection.execute(
        """
        SELECT *
        FROM job_outputs
        WHERE job_id = ? AND output_name = ?
        """,
        (job_id, output_name),
    ).fetchone()
    now = utc_now()
    if existing_row is not None:
        connection.execute(
            """
            UPDATE job_outputs
            SET value_type = ?, bound_custom_field = ?, description = ?, updated_at = ?
            WHERE id = ?
            """,
            (value_type, bound_custom_field, description, now, existing_row["id"]),
        )
        return int(existing_row["id"]), False

    cursor = connection.execute(
        """
        INSERT INTO job_outputs (
          job_id, output_name, value_type, bound_custom_field, description, ordinal, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            output_name,
            value_type,
            bound_custom_field,
            description,
            next_job_output_ordinal(connection, job_id),
            now,
            now,
        ),
    )
    return int(cursor.lastrowid), True


def next_job_version_number(connection: sqlite3.Connection, job_id: int) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(version), 0) AS max_version
        FROM job_versions
        WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()
    return int(row["max_version"] or 0) + 1


def create_job_version_row(
    connection: sqlite3.Connection,
    *,
    job_id: int,
    job_name: str,
    instruction_text: str,
    response_schema_json: str | None,
    capability: str,
    provider: str,
    model: str | None,
    parameters_json: str,
    input_basis: str,
    segment_profile: str | None,
    aggregation_strategy: str | None,
    display_name: str | None,
) -> int:
    version = next_job_version_number(connection, job_id)
    resolved_display_name = display_name or f"{job_name} v{version}"
    now = utc_now()
    cursor = connection.execute(
        """
        INSERT INTO job_versions (
          job_id,
          version,
          display_name,
          instruction_text,
          instruction_hash,
          response_schema_json,
          capability,
          provider,
          model,
          parameters_json,
          input_basis,
          segment_profile,
          aggregation_strategy,
          created_at,
          archived_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            job_id,
            version,
            resolved_display_name,
            instruction_text,
            sha256_text(instruction_text),
            response_schema_json,
            capability,
            provider,
            model,
            parameters_json,
            input_basis,
            segment_profile,
            aggregation_strategy,
            now,
        ),
    )
    return int(cursor.lastrowid)


def job_version_summary_by_id(connection: sqlite3.Connection, job_version_id: int) -> dict[str, object]:
    row = connection.execute(
        """
        SELECT *
        FROM job_versions
        WHERE id = ?
        """,
        (job_version_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown job version id: {job_version_id}")
    payload = job_version_row_to_payload(row)
    payload["job"] = job_summary_by_id(connection, int(row["job_id"]))
    return payload


def require_job_version_row_by_id(connection: sqlite3.Connection, job_version_id: int) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT *
        FROM job_versions
        WHERE id = ?
        """,
        (job_version_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown job version id: {job_version_id}")
    return row


def require_job_version_row(
    connection: sqlite3.Connection,
    *,
    job_version_id: int | None = None,
    job_name: str | None = None,
    version: int | None = None,
) -> sqlite3.Row:
    if job_version_id is not None:
        return require_job_version_row_by_id(connection, job_version_id)
    if not job_name:
        raise RetrieverError("A job version id or job name is required.")
    job_row = require_job_row_by_name(connection, job_name)
    if version is None:
        row = connection.execute(
            """
            SELECT *
            FROM job_versions
            WHERE job_id = ?
            ORDER BY version DESC, id DESC
            LIMIT 1
            """,
            (job_row["id"],),
        ).fetchone()
    else:
        row = connection.execute(
            """
            SELECT *
            FROM job_versions
            WHERE job_id = ? AND version = ?
            """,
            (job_row["id"], version),
        ).fetchone()
    if row is None:
        if version is None:
            raise RetrieverError(f"Job {job_name!r} has no job versions yet.")
        raise RetrieverError(f"Unknown job version {version} for job {job_name!r}.")
    return row


def text_revision_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "document_id": int(row["document_id"]),
        "revision_kind": row["revision_kind"],
        "language": row["language"],
        "parent_revision_id": row["parent_revision_id"],
        "created_by_job_version_id": row["created_by_job_version_id"],
        "storage_rel_path": row["storage_rel_path"],
        "content_hash": row["content_hash"],
        "char_count": row["char_count"],
        "token_estimate": row["token_estimate"],
        "quality_score": row["quality_score"],
        "provider_metadata": decode_json_text(row["provider_metadata_json"], default={}) or {},
        "created_at": row["created_at"],
        "retracted_at": row["retracted_at"],
        "retraction_reason": row["retraction_reason"],
    }


def require_text_revision_row_by_id(connection: sqlite3.Connection, text_revision_id: int) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT *
        FROM text_revisions
        WHERE id = ?
        """,
        (text_revision_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown text revision id: {text_revision_id}")
    return row


def root_text_revision_id(connection: sqlite3.Connection, text_revision_id: int) -> int:
    current_id = int(text_revision_id)
    seen_ids: set[int] = set()
    while True:
        if current_id in seen_ids:
            raise RetrieverError(f"Detected a text revision parent cycle at revision {current_id}.")
        seen_ids.add(current_id)
        row = require_text_revision_row_by_id(connection, current_id)
        parent_revision_id = row["parent_revision_id"]
        if parent_revision_id is None:
            return current_id
        current_id = int(parent_revision_id)


def text_revision_summary_by_id(connection: sqlite3.Connection, text_revision_id: int) -> dict[str, object]:
    row = require_text_revision_row_by_id(connection, text_revision_id)
    payload = text_revision_row_to_payload(row)
    document_row = connection.execute(
        """
        SELECT source_text_revision_id, active_search_text_revision_id
        FROM documents
        WHERE id = ?
        """,
        (row["document_id"],),
    ).fetchone()
    payload["is_source_revision"] = (
        document_row is not None
        and document_row["source_text_revision_id"] is not None
        and int(document_row["source_text_revision_id"]) == int(text_revision_id)
    )
    payload["is_active_search_revision"] = (
        document_row is not None
        and document_row["active_search_text_revision_id"] is not None
        and int(document_row["active_search_text_revision_id"]) == int(text_revision_id)
    )
    return payload


def list_text_revision_summaries_for_document(connection: sqlite3.Connection, document_id: int) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM text_revisions
        WHERE document_id = ?
        ORDER BY id DESC
        """,
        (document_id,),
    ).fetchall()
    return [text_revision_summary_by_id(connection, int(row["id"])) for row in rows]


def default_quality_score_for_text_status(text_status: object, text_content: str) -> float | None:
    normalized_status = normalize_whitespace(str(text_status or "")).lower()
    if not text_content.strip() or normalized_status == "empty":
        return 0.0
    if normalized_status == "ok":
        return 1.0
    return None


def text_revision_storage_rel_path(document_id: int, revision_kind: str, content_hash: str) -> str:
    revision_slug = sanitize_processing_identifier(revision_kind, label="Revision kind", prefix="revision")
    file_name = f"{revision_slug}-{content_hash}.txt"
    return (Path("text-revisions") / f"doc-{int(document_id):08d}" / file_name).as_posix()


def write_text_revision_body(paths: dict[str, Path], storage_rel_path: str, text_content: str) -> None:
    absolute_path = paths["state_dir"] / storage_rel_path
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_text(text_content, encoding="utf-8")


def read_text_revision_body(paths: dict[str, Path], storage_rel_path: str | None) -> str | None:
    if not storage_rel_path:
        return None
    absolute_path = paths["state_dir"] / storage_rel_path
    if not absolute_path.exists():
        return None
    return absolute_path.read_text(encoding="utf-8")


def document_row_has_seeded_text_revisions(document_row: sqlite3.Row | None) -> bool:
    if document_row is None:
        return False
    keys = document_row.keys()
    return (
        "source_text_revision_id" in keys
        and "active_search_text_revision_id" in keys
        and document_row["source_text_revision_id"] is not None
        and document_row["active_search_text_revision_id"] is not None
    )


DOCUMENT_EXTRACTED_METADATA_FIELDS = (
    "page_count",
    "author",
    "content_type",
    "date_created",
    "date_modified",
    "participants",
    "title",
    "subject",
    "recipients",
    "text_status",
)


def comparable_document_metadata_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    normalized = normalize_whitespace(str(value))
    return normalized or None


def extracted_payload_matches_document_row(document_row: sqlite3.Row | None, extracted: object) -> bool:
    if document_row is None or not isinstance(extracted, dict):
        return True
    keys = set(document_row.keys())
    if "content_hash" in keys:
        extracted_content_hash = sha256_text(str(extracted.get("text_content") or ""))
        if comparable_document_metadata_value(document_row["content_hash"]) != extracted_content_hash:
            return False
    for field_name in DOCUMENT_EXTRACTED_METADATA_FIELDS:
        if field_name not in keys:
            continue
        extracted_value = extracted.get("text_status", "ok") if field_name == "text_status" else extracted.get(field_name)
        if comparable_document_metadata_value(document_row[field_name]) != comparable_document_metadata_value(extracted_value):
            return False
    return True


def container_documents_missing_text_revisions(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
) -> bool:
    document_ids = sorted(
        container_document_ids_for_source(
            connection,
            source_kind=source_kind,
            source_rel_path=source_rel_path,
        )
    )
    if not document_ids:
        return False
    placeholders = ", ".join("?" for _ in document_ids)
    row = connection.execute(
        f"""
        SELECT 1
        FROM documents
        WHERE id IN ({placeholders})
          AND lifecycle_status != 'deleted'
          AND (
            source_text_revision_id IS NULL
            OR active_search_text_revision_id IS NULL
          )
        LIMIT 1
        """,
        document_ids,
    ).fetchone()
    return row is not None


def find_matching_text_revision(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    revision_kind: str,
    content_hash: str,
    parent_revision_id: int | None,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM text_revisions
        WHERE document_id = ?
          AND revision_kind = ?
          AND content_hash = ?
          AND COALESCE(parent_revision_id, 0) = COALESCE(?, 0)
          AND retracted_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (document_id, revision_kind, content_hash, parent_revision_id),
    ).fetchone()


def create_text_revision_row(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    document_id: int,
    revision_kind: str,
    text_content: str,
    language: str | None,
    parent_revision_id: int | None,
    created_by_job_version_id: int | None,
    quality_score: float | None,
    provider_metadata: dict[str, object] | None,
    created_at: str | None = None,
) -> int:
    content_hash = sha256_text(text_content)
    matching_row = find_matching_text_revision(
        connection,
        document_id=document_id,
        revision_kind=revision_kind,
        content_hash=content_hash,
        parent_revision_id=parent_revision_id,
    )
    storage_rel_path = text_revision_storage_rel_path(document_id, revision_kind, content_hash)
    write_text_revision_body(paths, storage_rel_path, text_content)
    if matching_row is not None:
        return int(matching_row["id"])

    timestamp = created_at or utc_now()
    cursor = connection.execute(
        """
        INSERT INTO text_revisions (
          document_id,
          revision_kind,
          language,
          parent_revision_id,
          created_by_job_version_id,
          storage_rel_path,
          content_hash,
          char_count,
          token_estimate,
          quality_score,
          provider_metadata_json,
          created_at,
          retracted_at,
          retraction_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
        """,
        (
            document_id,
            revision_kind,
            language,
            parent_revision_id,
            created_by_job_version_id,
            storage_rel_path,
            content_hash,
            len(text_content),
            token_estimate(text_content) if text_content.strip() else 0,
            quality_score,
            compact_json_text(provider_metadata or {}),
            timestamp,
        ),
    )
    return int(cursor.lastrowid)


def ensure_source_text_revision_for_document(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    document_id: int,
    text_content: str,
    text_status: object,
    existing_source_revision_id: int | None = None,
    language: str | None = None,
    created_at: str | None = None,
) -> int:
    content_hash = sha256_text(text_content)
    if existing_source_revision_id is not None:
        existing_row = connection.execute(
            """
            SELECT *
            FROM text_revisions
            WHERE id = ?
              AND document_id = ?
              AND revision_kind = 'source_extract'
              AND retracted_at IS NULL
            """,
            (existing_source_revision_id, document_id),
        ).fetchone()
        if existing_row is not None and str(existing_row["content_hash"]) == content_hash:
            storage_rel_path = str(existing_row["storage_rel_path"] or "")
            if storage_rel_path:
                write_text_revision_body(paths, storage_rel_path, text_content)
            return int(existing_row["id"])
    return create_text_revision_row(
        connection,
        paths,
        document_id=document_id,
        revision_kind="source_extract",
        text_content=text_content,
        language=language,
        parent_revision_id=None,
        created_by_job_version_id=None,
        quality_score=default_quality_score_for_text_status(text_status, text_content),
        provider_metadata={"text_status": text_status},
        created_at=created_at,
    )


def set_document_text_revision_pointers(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    source_text_revision_id: int,
    active_search_text_revision_id: int | None = None,
) -> None:
    source_row = connection.execute(
        """
        SELECT revision_kind, language, quality_score
        FROM text_revisions
        WHERE id = ?
        """,
        (source_text_revision_id,),
    ).fetchone()
    if source_row is None:
        raise RetrieverError(f"Unknown source text revision id: {source_text_revision_id}")

    active_revision_id = active_search_text_revision_id or source_text_revision_id
    active_row = connection.execute(
        """
        SELECT revision_kind, language, quality_score
        FROM text_revisions
        WHERE id = ?
        """,
        (active_revision_id,),
    ).fetchone()
    if active_row is None:
        raise RetrieverError(f"Unknown active text revision id: {active_revision_id}")

    connection.execute(
        """
        UPDATE documents
        SET source_text_revision_id = ?,
            active_search_text_revision_id = ?,
            active_text_source_kind = ?,
            active_text_language = ?,
            active_text_quality_score = ?
        WHERE id = ?
        """,
        (
            source_text_revision_id,
            active_revision_id,
            active_row["revision_kind"],
            active_row["language"],
            active_row["quality_score"],
            document_id,
        ),
    )


def seed_source_text_revision_for_document(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    document_id: int,
    extracted: dict[str, object],
    existing_row: sqlite3.Row | None = None,
    created_at: str | None = None,
) -> int:
    text_content = str(extracted.get("text_content") or "")
    revision_id = ensure_source_text_revision_for_document(
        connection,
        paths,
        document_id=document_id,
        text_content=text_content,
        text_status=extracted.get("text_status"),
        existing_source_revision_id=(
            int(existing_row["source_text_revision_id"])
            if existing_row is not None and "source_text_revision_id" in existing_row.keys() and existing_row["source_text_revision_id"] is not None
            else None
        ),
        language=(str(extracted.get("language")) if extracted.get("language") else None),
        created_at=created_at,
    )
    set_document_text_revision_pointers(
        connection,
        document_id=document_id,
        source_text_revision_id=revision_id,
        active_search_text_revision_id=revision_id,
    )
    return revision_id


def record_text_revision_activation_event(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    text_revision_id: int,
    activated_by_job_version_id: int | None,
    source_result_id: int | None,
    activation_policy: str,
    created_at: str | None = None,
) -> int:
    timestamp = created_at or utc_now()
    cursor = connection.execute(
        """
        INSERT INTO text_revision_activation_events (
          document_id,
          text_revision_id,
          activated_by_job_version_id,
          source_result_id,
          activation_policy,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            document_id,
            text_revision_id,
            activated_by_job_version_id,
            source_result_id,
            activation_policy,
            timestamp,
        ),
    )
    return int(cursor.lastrowid)


def activate_text_revision_for_document(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    document_id: int,
    text_revision_id: int,
    activation_policy: str = "manual",
    activated_by_job_version_id: int | None = None,
    source_result_id: int | None = None,
) -> dict[str, object]:
    normalized_policy = normalize_text_revision_activation_policy(activation_policy)
    document_row = connection.execute(
        """
        SELECT id, source_text_revision_id
        FROM documents
        WHERE id = ?
        """,
        (document_id,),
    ).fetchone()
    if document_row is None:
        raise RetrieverError(f"Unknown document id: {document_id}")

    text_revision_row = require_text_revision_row_by_id(connection, text_revision_id)
    if int(text_revision_row["document_id"]) != int(document_id):
        raise RetrieverError(
            f"Text revision {text_revision_id} belongs to document {text_revision_row['document_id']}, "
            f"not document {document_id}."
        )

    text_content = read_text_revision_body(paths, text_revision_row["storage_rel_path"])
    if text_content is None:
        raise RetrieverError(f"Text revision {text_revision_id} has no readable body on disk.")

    replace_document_chunks(connection, document_id, chunk_text(text_content))

    source_text_revision_id = (
        int(document_row["source_text_revision_id"])
        if document_row["source_text_revision_id"] is not None
        else root_text_revision_id(connection, text_revision_id)
    )
    set_document_text_revision_pointers(
        connection,
        document_id=document_id,
        source_text_revision_id=source_text_revision_id,
        active_search_text_revision_id=int(text_revision_id),
    )

    now = utc_now()
    normalized_text = normalize_whitespace(text_content)
    connection.execute(
        """
        UPDATE documents
        SET content_hash = ?,
            text_status = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            text_revision_row["content_hash"],
            "ok" if normalized_text else "empty",
            now,
            document_id,
        ),
    )
    activation_event_id = record_text_revision_activation_event(
        connection,
        document_id=document_id,
        text_revision_id=int(text_revision_id),
        activated_by_job_version_id=activated_by_job_version_id,
        source_result_id=source_result_id,
        activation_policy=normalized_policy,
        created_at=now,
    )

    # Best-effort: regenerate the production preview so the on-disk HTML
    # reflects the newly active text. Failures never roll back activation.
    try:
        preview_regen = regenerate_production_preview_for_document(
            connection,
            paths,
            document_id=document_id,
            text_content=text_content,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort regen
        preview_regen = {"status": "failed", "error": str(exc)}

    return {
        "status": "ok",
        "document_id": int(document_id),
        "text_revision": text_revision_summary_by_id(connection, int(text_revision_id)),
        "activation_event_id": activation_event_id,
        "activation_policy": normalized_policy,
        "preview_regen": preview_regen,
    }


def run_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "job_version_id": int(row["job_version_id"]),
        "from_run_id": row["from_run_id"],
        "selector": decode_json_text(row["selector_json"], default={}) or {},
        "exclude_selector": decode_json_text(row["exclude_selector_json"], default={}) or {},
        "activation_policy": str(row["activation_policy"] or "manual"),
        "family_mode": row["family_mode"],
        "seed_limit": row["seed_limit"],
        "status": row["status"],
        "planned_count": int(row["planned_count"] or 0),
        "completed_count": int(row["completed_count"] or 0),
        "failed_count": int(row["failed_count"] or 0),
        "skipped_count": int(row["skipped_count"] or 0),
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "canceled_at": row["canceled_at"],
    }


def run_snapshot_document_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "run_id": int(row["run_id"]),
        "document_id": int(row["document_id"]),
        "ordinal": int(row["ordinal"]),
        "inclusion_reason": decode_json_text(row["inclusion_reason_json"], default={}) or {},
        "pinned_input_revision_id": row["pinned_input_revision_id"],
        "pinned_input_identity": row["pinned_input_identity"],
        "pinned_content_hash": row["pinned_content_hash"],
        "created_at": row["created_at"],
    }


def create_run_row(
    connection: sqlite3.Connection,
    *,
    job_version_id: int,
    selector: dict[str, object],
    exclude_selector: dict[str, object],
    activation_policy: str,
    family_mode: str,
    seed_limit: int | None,
    from_run_id: int | None,
    status: str = "planned",
) -> int:
    now = utc_now()
    cursor = connection.execute(
        """
        INSERT INTO runs (
          job_version_id,
          from_run_id,
          selector_json,
          exclude_selector_json,
          activation_policy,
          family_mode,
          seed_limit,
          status,
          planned_count,
          completed_count,
          failed_count,
          skipped_count,
          created_at,
          started_at,
          completed_at,
          canceled_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, NULL, NULL, NULL)
        """,
        (
            job_version_id,
            from_run_id,
            compact_json_text(selector),
            compact_json_text(exclude_selector),
            activation_policy,
            family_mode,
            seed_limit,
            status,
            now,
        ),
    )
    return int(cursor.lastrowid)


def maybe_activate_created_text_revision(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_row: sqlite3.Row,
    job_version_row: sqlite3.Row,
    document_id: int,
    result_id: int,
    text_revision_id: int | None,
) -> dict[str, object] | None:
    if text_revision_id is None:
        return None
    activation_policy = normalize_run_activation_policy(str(run_row["activation_policy"] or "manual"))
    if activation_policy != "always":
        return None
    return activate_text_revision_for_document(
        connection,
        paths,
        document_id=document_id,
        text_revision_id=text_revision_id,
        activation_policy=activation_policy,
        activated_by_job_version_id=int(job_version_row["id"]),
        source_result_id=result_id,
    )


def replace_run_snapshot_documents(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    snapshot_rows: list[dict[str, object]],
) -> None:
    connection.execute("DELETE FROM run_snapshot_documents WHERE run_id = ?", (run_id,))
    if snapshot_rows:
        connection.executemany(
            """
            INSERT INTO run_snapshot_documents (
              run_id,
              document_id,
              ordinal,
              inclusion_reason_json,
              pinned_input_revision_id,
              pinned_input_identity,
              pinned_content_hash,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    row["document_id"],
                    row["ordinal"],
                    compact_json_text(row["inclusion_reason"]),
                    row["pinned_input_revision_id"],
                    row["pinned_input_identity"],
                    row["pinned_content_hash"],
                    row.get("created_at", utc_now()),
                )
                for row in snapshot_rows
            ],
        )
    connection.execute(
        """
        UPDATE runs
        SET planned_count = ?
        WHERE id = ?
        """,
        (len(snapshot_rows), run_id),
    )


def run_item_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "run_id": int(row["run_id"]),
        "run_snapshot_document_id": row["run_snapshot_document_id"],
        "item_kind": row["item_kind"],
        "document_id": int(row["document_id"]),
        "page_number": row["page_number"],
        "segment_id": row["segment_id"],
        "input_artifact_rel_path": row["input_artifact_rel_path"],
        "input_identity": row["input_identity"],
        "result_id": row["result_id"],
        "status": row["status"],
        "claimed_by": row["claimed_by"],
        "claimed_at": row["claimed_at"],
        "last_heartbeat_at": row["last_heartbeat_at"],
        "attempt_count": int(row["attempt_count"] or 0),
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
    }


def find_run_item_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    run_snapshot_document_id: int | None,
    item_kind: str,
    page_number: int | None,
    segment_id: int | None,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM run_items
        WHERE run_id = ?
          AND ((run_snapshot_document_id = ?) OR (run_snapshot_document_id IS NULL AND ? IS NULL))
          AND item_kind = ?
          AND ((page_number = ?) OR (page_number IS NULL AND ? IS NULL))
          AND ((segment_id = ?) OR (segment_id IS NULL AND ? IS NULL))
        ORDER BY id ASC
        LIMIT 1
        """,
        (
            run_id,
            run_snapshot_document_id,
            run_snapshot_document_id,
            item_kind,
            page_number,
            page_number,
            segment_id,
            segment_id,
        ),
    ).fetchone()


def ensure_run_item_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    run_snapshot_document_id: int | None,
    item_kind: str,
    document_id: int,
    input_identity: str,
    page_number: int | None = None,
    segment_id: int | None = None,
    input_artifact_rel_path: str | None = None,
) -> sqlite3.Row:
    normalized_item_kind = normalize_run_item_kind(item_kind)
    existing_row = find_run_item_row(
        connection,
        run_id=run_id,
        run_snapshot_document_id=run_snapshot_document_id,
        item_kind=normalized_item_kind,
        page_number=page_number,
        segment_id=segment_id,
    )
    if existing_row is not None:
        if (
            str(existing_row["input_identity"]) != input_identity
            or normalize_whitespace(str(existing_row["input_artifact_rel_path"] or ""))
            != normalize_whitespace(str(input_artifact_rel_path or ""))
        ):
            connection.execute(
                """
                UPDATE run_items
                SET input_identity = ?,
                    input_artifact_rel_path = ?
                WHERE id = ?
                """,
                (input_identity, input_artifact_rel_path, existing_row["id"]),
            )
            return connection.execute("SELECT * FROM run_items WHERE id = ?", (existing_row["id"],)).fetchone()
        return existing_row

    now = utc_now()
    connection.execute(
        """
        INSERT OR IGNORE INTO run_items (
          run_id,
          run_snapshot_document_id,
          item_kind,
          document_id,
          page_number,
          segment_id,
          input_artifact_rel_path,
          input_identity,
          result_id,
          status,
          claimed_by,
          claimed_at,
          last_heartbeat_at,
          attempt_count,
          last_error,
          created_at,
          started_at,
          completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'pending', NULL, NULL, NULL, 0, NULL, ?, NULL, NULL)
        """,
        (
            run_id,
            run_snapshot_document_id,
            normalized_item_kind,
            document_id,
            page_number,
            segment_id,
            input_artifact_rel_path,
            input_identity,
            now,
        ),
    )
    ensured_row = find_run_item_row(
        connection,
        run_id=run_id,
        run_snapshot_document_id=run_snapshot_document_id,
        item_kind=normalized_item_kind,
        page_number=page_number,
        segment_id=segment_id,
    )
    assert ensured_row is not None
    return ensured_row


def update_run_item_row(
    connection: sqlite3.Connection,
    *,
    run_item_id: int,
    status: str,
    result_id: int | None = None,
    last_error: str | None = None,
    claimed_by: str | None = None,
    claimed_at: str | None = None,
    last_heartbeat_at: str | None = None,
    started_at: str | None = None,
    completed_at: str | None = None,
    increment_attempt_count: bool = False,
) -> None:
    normalized_status = normalize_run_item_status(status)
    row = connection.execute(
        """
        SELECT attempt_count, claimed_by, claimed_at, last_heartbeat_at, started_at, completed_at
        FROM run_items
        WHERE id = ?
        """,
        (run_item_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown run item id: {run_item_id}")
    next_attempt_count = int(row["attempt_count"] or 0) + (1 if increment_attempt_count else 0)
    effective_claimed_by = claimed_by if claimed_by is not None else row["claimed_by"]
    effective_claimed_at = claimed_at if claimed_at is not None else row["claimed_at"]
    effective_last_heartbeat_at = last_heartbeat_at if last_heartbeat_at is not None else row["last_heartbeat_at"]
    effective_started_at = started_at if started_at is not None else row["started_at"]
    effective_completed_at = completed_at if completed_at is not None else row["completed_at"]
    connection.execute(
        """
        UPDATE run_items
        SET status = ?,
            result_id = ?,
            claimed_by = ?,
            claimed_at = ?,
            last_heartbeat_at = ?,
            attempt_count = ?,
            last_error = ?,
            started_at = ?,
            completed_at = ?
        WHERE id = ?
        """,
        (
            normalized_status,
            result_id,
            effective_claimed_by,
            effective_claimed_at,
            effective_last_heartbeat_at,
            next_attempt_count,
            last_error,
            effective_started_at,
            effective_completed_at,
            run_item_id,
        ),
    )


def list_run_item_payloads_for_run(connection: sqlite3.Connection, run_id: int) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM run_items
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()
    return [run_item_row_to_payload(row) for row in rows]


def run_worker_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "run_id": int(row["run_id"]),
        "claimed_by": row["claimed_by"],
        "launch_mode": row["launch_mode"],
        "worker_task_id": row["worker_task_id"],
        "status": row["status"],
        "max_batches": row["max_batches"],
        "batches_prepared": int(row["batches_prepared"] or 0),
        "items_completed": int(row["items_completed"] or 0),
        "items_failed": int(row["items_failed"] or 0),
        "last_heartbeat_at": row["last_heartbeat_at"],
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "cancel_requested_at": row["cancel_requested_at"],
        "summary": decode_json_text(row["summary_json"], default={}) or {},
    }


def find_run_worker_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    claimed_by: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM run_workers
        WHERE run_id = ?
          AND claimed_by = ?
        """,
        (run_id, claimed_by),
    ).fetchone()


def ensure_run_worker_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    claimed_by: str,
    launch_mode: str,
    worker_task_id: str | None,
    max_batches: int | None,
) -> sqlite3.Row:
    normalized_claimed_by = normalize_whitespace(claimed_by)
    if not normalized_claimed_by:
        raise RetrieverError("claimed_by cannot be empty.")
    normalized_launch_mode = normalize_run_worker_mode(launch_mode)
    normalized_worker_task_id = (
        normalize_whitespace(worker_task_id)
        if worker_task_id is not None
        else ""
    ) or None
    existing_row = find_run_worker_row(connection, run_id=run_id, claimed_by=normalized_claimed_by)
    now = utc_now()
    if existing_row is not None:
        updates: list[str] = []
        params: list[object] = []
        if str(existing_row["launch_mode"]) != normalized_launch_mode:
            updates.append("launch_mode = ?")
            params.append(normalized_launch_mode)
        if normalized_worker_task_id and normalize_whitespace(str(existing_row["worker_task_id"] or "")) != normalized_worker_task_id:
            updates.append("worker_task_id = ?")
            params.append(normalized_worker_task_id)
        if max_batches is not None and existing_row["max_batches"] != max_batches:
            updates.append("max_batches = ?")
            params.append(max_batches)
        if str(existing_row["status"] or "") in {"completed", "failed", "stopped", "orphaned", "canceled"}:
            updates.append("status = 'active'")
            updates.append("completed_at = NULL")
            updates.append("last_error = NULL")
            updates.append("cancel_requested_at = NULL")
            updates.append("summary_json = '{}'")
        if updates:
            connection.execute(
                f"""
                UPDATE run_workers
                SET {", ".join(updates)}
                WHERE id = ?
                """,
                (*params, existing_row["id"]),
            )
        return connection.execute("SELECT * FROM run_workers WHERE id = ?", (existing_row["id"],)).fetchone()

    connection.execute(
        """
        INSERT INTO run_workers (
          run_id,
          claimed_by,
          launch_mode,
          worker_task_id,
          status,
          max_batches,
          batches_prepared,
          items_completed,
          items_failed,
          last_heartbeat_at,
          last_error,
          created_at,
          started_at,
          completed_at,
          cancel_requested_at,
          summary_json
        ) VALUES (?, ?, ?, ?, 'active', ?, 0, 0, 0, NULL, NULL, ?, ?, NULL, NULL, '{}')
        """,
        (
            run_id,
            normalized_claimed_by,
            normalized_launch_mode,
            normalized_worker_task_id,
            max_batches,
            now,
            now,
        ),
    )
    return find_run_worker_row(connection, run_id=run_id, claimed_by=normalized_claimed_by)


def update_run_worker_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    claimed_by: str,
    status: str | None = None,
    worker_task_id: str | None = None,
    max_batches: int | None = None,
    increment_batches_prepared: bool = False,
    increment_items_completed: int = 0,
    increment_items_failed: int = 0,
    heartbeat: bool = False,
    last_error: str | None = None,
    cancel_requested: bool = False,
    summary: dict[str, object] | None = None,
    completed_at: str | None = None,
) -> sqlite3.Row | None:
    row = find_run_worker_row(connection, run_id=run_id, claimed_by=claimed_by)
    if row is None:
        return None
    next_status = normalize_run_worker_status(status) if status is not None else str(row["status"])
    next_batches_prepared = int(row["batches_prepared"] or 0) + (1 if increment_batches_prepared else 0)
    next_items_completed = int(row["items_completed"] or 0) + max(0, int(increment_items_completed))
    next_items_failed = int(row["items_failed"] or 0) + max(0, int(increment_items_failed))
    next_worker_task_id = (
        normalize_whitespace(worker_task_id)
        if worker_task_id is not None
        else normalize_whitespace(str(row["worker_task_id"] or ""))
    ) or row["worker_task_id"]
    next_max_batches = max_batches if max_batches is not None else row["max_batches"]
    next_last_heartbeat_at = utc_now() if heartbeat else row["last_heartbeat_at"]
    next_cancel_requested_at = utc_now() if cancel_requested else row["cancel_requested_at"]
    next_summary = summary if summary is not None else (decode_json_text(row["summary_json"], default={}) or {})
    next_completed_at = completed_at if completed_at is not None else row["completed_at"]
    connection.execute(
        """
        UPDATE run_workers
        SET status = ?,
            worker_task_id = ?,
            max_batches = ?,
            batches_prepared = ?,
            items_completed = ?,
            items_failed = ?,
            last_heartbeat_at = ?,
            last_error = ?,
            completed_at = ?,
            cancel_requested_at = ?,
            summary_json = ?
        WHERE id = ?
        """,
        (
            next_status,
            next_worker_task_id,
            next_max_batches,
            next_batches_prepared,
            next_items_completed,
            next_items_failed,
            next_last_heartbeat_at,
            last_error if last_error is not None else row["last_error"],
            next_completed_at,
            next_cancel_requested_at,
            compact_json_text(next_summary),
            row["id"],
        ),
    )
    return connection.execute("SELECT * FROM run_workers WHERE id = ?", (row["id"],)).fetchone()


def list_run_worker_payloads_for_run(connection: sqlite3.Connection, run_id: int) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM run_workers
        WHERE run_id = ?
        ORDER BY created_at ASC, id ASC
        """,
        (run_id,),
    ).fetchall()
    return [run_worker_row_to_payload(row) for row in rows]


def require_run_row_by_id(connection: sqlite3.Connection, run_id: int) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT *
        FROM runs
        WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown run id: {run_id}")
    return row


def require_run_item_row_by_id(connection: sqlite3.Connection, run_item_id: int) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT *
        FROM run_items
        WHERE id = ?
        """,
        (run_item_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown run item id: {run_item_id}")
    return row


def workspace_relative_artifact_path(paths: dict[str, Path], absolute_path: Path) -> str:
    try:
        relative_to_state = absolute_path.relative_to(paths["state_dir"])
    except ValueError:
        return relative_document_path(paths["root"], absolute_path)
    return str(Path(INTERNAL_REL_PATH_PREFIX) / relative_to_state)


def resolve_workspace_artifact_path(root: Path, rel_path: str | None) -> Path | None:
    normalized = normalize_whitespace(str(rel_path or ""))
    if not normalized:
        return None
    path = Path(normalized)
    if path.parts and path.parts[0] == INTERNAL_REL_PATH_PREFIX:
        # Synthetic rel_paths mirror the state-directory layout.
        state_relative = Path(*path.parts[1:]) if len(path.parts) > 1 else Path()
        return (root / ".retriever" / state_relative).resolve()
    return (root / normalized).resolve()


def freeze_ocr_source_artifact(
    paths: dict[str, Path],
    root: Path,
    *,
    source_rel_path: str,
    run_id: int,
    document_id: int,
    page_number: int,
) -> str:
    source_path = resolve_workspace_artifact_path(root, source_rel_path)
    if source_path is None or not source_path.exists():
        raise RetrieverError(f"OCR source artifact is missing: {source_rel_path!r}")
    output_dir = paths["jobs_dir"] / "ocr" / f"run-{run_id}" / f"doc-{document_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_name = f"page-{page_number:04d}-{source_path.name}"
    output_path = output_dir / output_name
    if not output_path.exists():
        output_path.write_bytes(source_path.read_bytes())
    return workspace_relative_artifact_path(paths, output_path)


def render_pdf_pages_for_ocr(
    paths: dict[str, Path],
    *,
    source_path: Path,
    run_id: int,
    document_id: int,
    resolution: int,
) -> list[dict[str, object]]:
    pdfplumber_module = dependency_guard("pdfplumber", "pdfplumber", "pdf")
    output_dir = paths["jobs_dir"] / "ocr" / f"run-{run_id}" / f"doc-{document_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    page_specs: list[dict[str, object]] = []
    with pdfplumber_module.open(source_path) as pdf:  # type: ignore[union-attr]
        for page_number, page in enumerate(pdf.pages, start=1):
            output_path = output_dir / f"page-{page_number:04d}.png"
            if not output_path.exists():
                page_image = page.to_image(resolution=resolution)
                page_image.save(output_path, format="PNG")
            page_specs.append(
                {
                    "page_number": page_number,
                    "input_artifact_rel_path": workspace_relative_artifact_path(paths, output_path),
                }
            )
    return page_specs


def ocr_page_item_specs_for_document(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    root: Path,
    *,
    run_id: int,
    document_row: sqlite3.Row,
    job_version_row: sqlite3.Row,
) -> list[dict[str, object]]:
    input_basis = str(job_version_row["input_basis"])
    if input_basis == "source_parts":
        source_part_rows = connection.execute(
            """
            SELECT part_kind, rel_source_path, ordinal
            FROM document_source_parts
            WHERE document_id = ?
              AND part_kind IN ('image', 'native')
            ORDER BY CASE part_kind WHEN 'image' THEN 0 WHEN 'native' THEN 1 ELSE 2 END ASC, ordinal ASC, id ASC
            """,
            (document_row["id"],),
        ).fetchall()
        image_part_rows = [row for row in source_part_rows if str(row["part_kind"]) == "image"]
        if image_part_rows:
            return [
                {
                    "page_number": int(row["ordinal"] or index),
                    "input_artifact_rel_path": freeze_ocr_source_artifact(
                        paths,
                        root,
                        source_rel_path=str(row["rel_source_path"]),
                        run_id=run_id,
                        document_id=int(document_row["id"]),
                        page_number=int(row["ordinal"] or index),
                    ),
                }
                for index, row in enumerate(image_part_rows, start=1)
            ]
        native_part_row = next((row for row in source_part_rows if str(row["part_kind"]) == "native"), None)
        if native_part_row is not None:
            native_source_rel_path = str(native_part_row["rel_source_path"])
            native_source_path = resolve_workspace_artifact_path(root, native_source_rel_path)
            if native_source_path is None or not native_source_path.exists():
                raise RetrieverError(
                    f"Document {document_row['id']} has no accessible native source part for OCR: {native_source_rel_path!r}."
                )
            native_file_type = normalize_extension(native_source_path)
            if native_file_type == "pdf":
                parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
                rendering_settings = parameters.get("rendering_settings") if isinstance(parameters, dict) else None
                resolution = DEFAULT_OCR_RENDER_RESOLUTION
                if isinstance(rendering_settings, dict) and rendering_settings.get("resolution") is not None:
                    try:
                        resolution = max(72, int(rendering_settings["resolution"]))
                    except (TypeError, ValueError):
                        resolution = DEFAULT_OCR_RENDER_RESOLUTION
                return render_pdf_pages_for_ocr(
                    paths,
                    source_path=native_source_path,
                    run_id=run_id,
                    document_id=int(document_row["id"]),
                    resolution=resolution,
                )
            if native_file_type in IMAGE_NATIVE_PREVIEW_FILE_TYPES:
                return [
                    {
                        "page_number": 1,
                        "input_artifact_rel_path": freeze_ocr_source_artifact(
                            paths,
                            root,
                            source_rel_path=native_source_rel_path,
                            run_id=run_id,
                            document_id=int(document_row["id"]),
                            page_number=1,
                        ),
                    }
                ]
            raise RetrieverError(
                f"Document {document_row['id']} native source part is not OCR-capable: {native_file_type!r}."
            )
        raise RetrieverError(
            f"Document {document_row['id']} has source parts but no image/native OCR source parts."
        )
    source_path = resolve_workspace_artifact_path(root, str(document_row["rel_path"]))
    if source_path is None or not source_path.exists():
        raise RetrieverError(f"Document {document_row['id']} has no accessible source path for OCR.")
    file_type = normalize_extension(source_path) or normalize_whitespace(str(document_row["file_type"] or "")).lower()
    if file_type == "pdf":
        parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
        rendering_settings = parameters.get("rendering_settings") if isinstance(parameters, dict) else None
        resolution = DEFAULT_OCR_RENDER_RESOLUTION
        if isinstance(rendering_settings, dict) and rendering_settings.get("resolution") is not None:
            try:
                resolution = max(72, int(rendering_settings["resolution"]))
            except (TypeError, ValueError):
                resolution = DEFAULT_OCR_RENDER_RESOLUTION
        return render_pdf_pages_for_ocr(
            paths,
            source_path=source_path,
            run_id=run_id,
            document_id=int(document_row["id"]),
            resolution=resolution,
        )
    if file_type in IMAGE_NATIVE_PREVIEW_FILE_TYPES:
        return [
            {
                "page_number": 1,
                "input_artifact_rel_path": freeze_ocr_source_artifact(
                    paths,
                    root,
                    source_rel_path=str(document_row["rel_path"]),
                    run_id=run_id,
                    document_id=int(document_row["id"]),
                    page_number=1,
                ),
            }
        ]
    raise RetrieverError(
        f"OCR page materialization does not know how to prepare document {document_row['id']} with file type {file_type!r}."
    )


def prior_run_page_item_specs(
    connection: sqlite3.Connection,
    *,
    from_run_id: int,
    document_id: int,
) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT page_number, input_artifact_rel_path
        FROM run_items
        WHERE run_id = ?
          AND document_id = ?
          AND item_kind = 'page'
          AND input_artifact_rel_path IS NOT NULL
        ORDER BY page_number ASC, id ASC
        """,
        (from_run_id, document_id),
    ).fetchall()
    return [
        {
            "page_number": int(row["page_number"] or 0),
            "input_artifact_rel_path": str(row["input_artifact_rel_path"]),
        }
        for row in rows
    ]


def materialize_run_items_for_run(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    root: Path,
    run_id: int,
) -> list[sqlite3.Row]:
    existing_rows = connection.execute(
        """
        SELECT *
        FROM run_items
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()
    if existing_rows:
        return existing_rows

    run_row = require_run_row_by_id(connection, run_id)
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
    job_kind = normalize_job_kind(str(job_row["job_kind"]))
    snapshot_rows = connection.execute(
        """
        SELECT *
        FROM run_snapshot_documents
        WHERE run_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (run_id,),
    ).fetchall()
    materialized_rows: list[sqlite3.Row] = []
    for snapshot_row in snapshot_rows:
        document_row = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE id = ?
            """,
            (snapshot_row["document_id"],),
        ).fetchone()
        if document_row is None:
            continue
        if job_kind in {"ocr", "image_description"}:
            from_run_id = int(run_row["from_run_id"]) if run_row["from_run_id"] is not None else None
            page_specs = (
                prior_run_page_item_specs(
                    connection,
                    from_run_id=from_run_id,
                    document_id=int(snapshot_row["document_id"]),
                )
                if from_run_id is not None
                else []
            )
            if not page_specs:
                page_specs = ocr_page_item_specs_for_document(
                    connection,
                    paths,
                    root,
                    run_id=run_id,
                    document_row=document_row,
                    job_version_row=job_version_row,
                )
            for page_spec in page_specs:
                materialized_rows.append(
                    ensure_run_item_row(
                        connection,
                        run_id=run_id,
                        run_snapshot_document_id=int(snapshot_row["id"]),
                        item_kind="page",
                        document_id=int(snapshot_row["document_id"]),
                        page_number=int(page_spec["page_number"]),
                        input_artifact_rel_path=str(page_spec["input_artifact_rel_path"]),
                        input_identity=str(snapshot_row["pinned_input_identity"]),
                    )
                )
            continue
        materialized_rows.append(
            ensure_run_item_row(
                connection,
                run_id=run_id,
                run_snapshot_document_id=int(snapshot_row["id"]),
                item_kind="document",
                document_id=int(snapshot_row["document_id"]),
                input_identity=str(snapshot_row["pinned_input_identity"]),
            )
        )
    return materialized_rows


def reuse_active_results_for_run(connection: sqlite3.Connection, run_id: int) -> int:
    run_row = require_run_row_by_id(connection, run_id)
    job_version_id = int(run_row["job_version_id"])
    pending_rows = connection.execute(
        """
        SELECT *
        FROM run_items
        WHERE run_id = ?
          AND status = 'pending'
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()
    reused_count = 0
    for row in pending_rows:
        existing_result_row = find_active_result_row(
            connection,
            document_id=int(row["document_id"]),
            job_version_id=job_version_id,
            input_identity=str(row["input_identity"]),
        )
        if existing_result_row is None:
            continue
        update_run_item_row(
            connection,
            run_item_id=int(row["id"]),
            status="skipped",
            result_id=int(existing_result_row["id"]),
            last_error=None,
            completed_at=utc_now(),
        )
        reused_count += 1
    return reused_count


def ocr_page_output_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "run_item_id": int(row["run_item_id"]),
        "run_id": int(row["run_id"]),
        "document_id": int(row["document_id"]),
        "page_number": int(row["page_number"]),
        "text_content": row["text_content"],
        "raw_output": decode_json_text(row["raw_output_json"]),
        "normalized_output": decode_json_text(row["normalized_output_json"]),
        "provider_metadata": decode_json_text(row["provider_metadata_json"], default={}) or {},
        "created_at": row["created_at"],
    }


def find_ocr_page_output_row(connection: sqlite3.Connection, *, run_item_id: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM ocr_page_outputs
        WHERE run_item_id = ?
        """,
        (run_item_id,),
    ).fetchone()


def upsert_ocr_page_output_row(
    connection: sqlite3.Connection,
    *,
    run_item_id: int,
    run_id: int,
    document_id: int,
    page_number: int,
    text_content: str,
    raw_output: object,
    normalized_output: object,
    provider_metadata: dict[str, object] | None,
) -> tuple[int, bool]:
    existing_row = find_ocr_page_output_row(connection, run_item_id=run_item_id)
    if existing_row is not None:
        return int(existing_row["id"]), False
    cursor = connection.execute(
        """
        INSERT INTO ocr_page_outputs (
          run_item_id,
          run_id,
          document_id,
          page_number,
          text_content,
          raw_output_json,
          normalized_output_json,
          provider_metadata_json,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_item_id,
            run_id,
            document_id,
            page_number,
            text_content,
            compact_json_text(raw_output),
            compact_json_text(normalized_output),
            compact_json_text(provider_metadata or {}),
            utc_now(),
        ),
    )
    return int(cursor.lastrowid), True


def list_ocr_page_output_payloads_for_document(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    document_id: int,
) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ocr_page_outputs
        WHERE run_id = ?
          AND document_id = ?
        ORDER BY page_number ASC, id ASC
        """,
        (run_id, document_id),
    ).fetchall()
    return [ocr_page_output_row_to_payload(row) for row in rows]


def image_description_page_output_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "run_item_id": int(row["run_item_id"]),
        "run_id": int(row["run_id"]),
        "document_id": int(row["document_id"]),
        "page_number": int(row["page_number"]),
        "text_content": row["text_content"],
        "raw_output": decode_json_text(row["raw_output_json"]),
        "normalized_output": decode_json_text(row["normalized_output_json"]),
        "provider_metadata": decode_json_text(row["provider_metadata_json"], default={}) or {},
        "created_at": row["created_at"],
    }


def find_image_description_page_output_row(
    connection: sqlite3.Connection,
    *,
    run_item_id: int,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM image_description_page_outputs
        WHERE run_item_id = ?
        """,
        (run_item_id,),
    ).fetchone()


def upsert_image_description_page_output_row(
    connection: sqlite3.Connection,
    *,
    run_item_id: int,
    run_id: int,
    document_id: int,
    page_number: int,
    text_content: str,
    raw_output: object,
    normalized_output: object,
    provider_metadata: dict[str, object] | None,
) -> tuple[int, bool]:
    existing_row = find_image_description_page_output_row(connection, run_item_id=run_item_id)
    if existing_row is not None:
        return int(existing_row["id"]), False
    cursor = connection.execute(
        """
        INSERT INTO image_description_page_outputs (
          run_item_id,
          run_id,
          document_id,
          page_number,
          text_content,
          raw_output_json,
          normalized_output_json,
          provider_metadata_json,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_item_id,
            run_id,
            document_id,
            page_number,
            text_content,
            compact_json_text(raw_output),
            compact_json_text(normalized_output),
            compact_json_text(provider_metadata or {}),
            utc_now(),
        ),
    )
    return int(cursor.lastrowid), True


def list_image_description_page_output_payloads_for_document(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    document_id: int,
) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM image_description_page_outputs
        WHERE run_id = ?
          AND document_id = ?
        ORDER BY page_number ASC, id ASC
        """,
        (run_id, document_id),
    ).fetchall()
    return [image_description_page_output_row_to_payload(row) for row in rows]


def claim_run_item_rows(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    claimed_by: str,
    limit: int,
    stale_after_seconds: int = DEFAULT_RUN_ITEM_CLAIM_STALE_SECONDS,
) -> list[sqlite3.Row]:
    normalized_claimed_by = normalize_whitespace(claimed_by)
    if not normalized_claimed_by:
        raise RetrieverError("claimed_by cannot be empty.")
    if limit < 1:
        return []
    run_row = require_run_row_by_id(connection, run_id)
    if str(run_row["status"] or "") in {"canceled", "completed", "failed"}:
        return []

    stale_cutoff = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=max(1, stale_after_seconds)))
    stale_claimed_rows = connection.execute(
        """
        SELECT DISTINCT claimed_by
        FROM run_items
        WHERE run_id = ?
          AND status = 'running'
          AND result_id IS NULL
          AND claimed_by IS NOT NULL
          AND last_heartbeat_at IS NOT NULL
          AND last_heartbeat_at < ?
        """,
        (run_id, stale_cutoff),
    ).fetchall()
    for stale_row in stale_claimed_rows:
        stale_claimed_by = normalize_whitespace(str(stale_row["claimed_by"] or ""))
        if not stale_claimed_by or stale_claimed_by == normalized_claimed_by:
            continue
        update_run_worker_row(
            connection,
            run_id=run_id,
            claimed_by=stale_claimed_by,
            status="orphaned",
            last_error="Worker claim heartbeat expired and items were reclaimed.",
            completed_at=utc_now(),
        )

    candidate_rows = connection.execute(
        """
        SELECT *
        FROM run_items
        WHERE run_id = ?
          AND (
            status = 'pending'
            OR (
              status = 'running'
              AND result_id IS NULL
              AND claimed_by IS NOT NULL
              AND last_heartbeat_at IS NOT NULL
              AND last_heartbeat_at < ?
            )
          )
        ORDER BY id ASC
        LIMIT ?
        """,
        (run_id, stale_cutoff, limit),
    ).fetchall()
    if not candidate_rows:
        return []

    now = utc_now()
    item_ids = [int(row["id"]) for row in candidate_rows]
    placeholders = ", ".join("?" for _ in item_ids)
    connection.execute(
        f"""
        UPDATE run_items
        SET status = 'running',
            claimed_by = ?,
            claimed_at = ?,
            last_heartbeat_at = ?,
            started_at = COALESCE(started_at, ?),
            completed_at = NULL
        WHERE id IN ({placeholders})
        """,
        (normalized_claimed_by, now, now, now, *item_ids),
    )
    connection.execute(
        """
        UPDATE runs
        SET status = 'running',
            started_at = COALESCE(started_at, ?),
            completed_at = NULL,
            canceled_at = NULL
        WHERE id = ?
        """,
        (now, run_id),
    )
    return connection.execute(
        f"""
        SELECT *
        FROM run_items
        WHERE id IN ({placeholders})
        ORDER BY id ASC
        """,
        item_ids,
    ).fetchall()


def heartbeat_claimed_run_items(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    claimed_by: str,
) -> int:
    normalized_claimed_by = normalize_whitespace(claimed_by)
    if not normalized_claimed_by:
        raise RetrieverError("claimed_by cannot be empty.")
    now = utc_now()
    cursor = connection.execute(
        """
        UPDATE run_items
        SET last_heartbeat_at = ?
        WHERE run_id = ?
          AND status = 'running'
          AND claimed_by = ?
        """,
        (now, run_id, normalized_claimed_by),
    )
    update_run_worker_row(
        connection,
        run_id=run_id,
        claimed_by=normalized_claimed_by,
        heartbeat=True,
    )
    return int(cursor.rowcount or 0)


def cancel_pending_run_items(connection: sqlite3.Connection, *, run_id: int) -> int:
    now = utc_now()
    cursor = connection.execute(
        """
        UPDATE run_items
        SET status = 'skipped',
            last_error = COALESCE(last_error, 'Run canceled.'),
            completed_at = COALESCE(completed_at, ?)
        WHERE run_id = ?
          AND status = 'pending'
        """,
        (now, run_id),
    )
    return int(cursor.rowcount or 0)


def request_run_worker_cancellation(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    force: bool,
) -> list[str]:
    worker_rows = connection.execute(
        """
        SELECT claimed_by, worker_task_id
        FROM run_workers
        WHERE run_id = ?
          AND status = 'active'
        ORDER BY claimed_by ASC
        """,
        (run_id,),
    ).fetchall()
    task_ids: list[str] = []
    for row in worker_rows:
        claimed_by = normalize_whitespace(str(row["claimed_by"] or ""))
        worker_task_id = normalize_whitespace(str(row["worker_task_id"] or "")) or None
        if worker_task_id:
            task_ids.append(worker_task_id)
        update_run_worker_row(
            connection,
            run_id=run_id,
            claimed_by=claimed_by,
            status="canceled",
            cancel_requested=True,
            last_error=("Force-stop requested." if force else "Cancellation requested."),
            completed_at=utc_now() if force else None,
        )
    return task_ids


def run_item_status_counts(connection: sqlite3.Connection, run_id: int) -> dict[str, int]:
    return {
        str(row["status"]): int(row["count"] or 0)
        for row in connection.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM run_items
            WHERE run_id = ?
            GROUP BY status
            """,
            (run_id,),
        ).fetchall()
    }


def run_execution_metadata(connection: sqlite3.Connection, run_id: int) -> tuple[sqlite3.Row, sqlite3.Row, sqlite3.Row]:
    run_row = require_run_row_by_id(connection, run_id)
    job_version_row = require_job_version_row_by_id(connection, int(run_row["job_version_id"]))
    job_row = connection.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = ?
        """,
        (job_version_row["job_id"],),
    ).fetchone()
    if job_row is None:
        raise RetrieverError(f"Run {run_id} references a missing job id: {job_version_row['job_id']}")
    return run_row, job_version_row, job_row


def ocr_run_pending_finalization_count(connection: sqlite3.Connection, run_id: int) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM run_snapshot_documents AS snapshot
        WHERE snapshot.run_id = ?
          AND EXISTS (
            SELECT 1
            FROM run_items AS page_item
            WHERE page_item.run_id = snapshot.run_id
              AND page_item.document_id = snapshot.document_id
              AND page_item.item_kind = 'page'
          )
          AND NOT EXISTS (
            SELECT 1
            FROM run_items AS finalized_item
            WHERE finalized_item.run_id = snapshot.run_id
              AND finalized_item.document_id = snapshot.document_id
              AND finalized_item.result_id IS NOT NULL
          )
        """,
        (run_id,),
    ).fetchone()
    return int(row["count"] or 0)


def image_description_run_pending_finalization_count(connection: sqlite3.Connection, run_id: int) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM run_snapshot_documents AS snapshot
        WHERE snapshot.run_id = ?
          AND EXISTS (
            SELECT 1
            FROM run_items AS page_item
            WHERE page_item.run_id = snapshot.run_id
              AND page_item.document_id = snapshot.document_id
              AND page_item.item_kind = 'page'
          )
          AND NOT EXISTS (
            SELECT 1
            FROM run_items AS finalized_item
            WHERE finalized_item.run_id = snapshot.run_id
              AND finalized_item.document_id = snapshot.document_id
              AND finalized_item.result_id IS NOT NULL
          )
        """,
        (run_id,),
    ).fetchone()
    return int(row["count"] or 0)


def build_run_worker_payload(
    connection: sqlite3.Connection,
    run_id: int,
    *,
    run_payload: dict[str, object],
    claimed_by: str | None = None,
) -> dict[str, object]:
    _, job_version_row, job_row = run_execution_metadata(connection, run_id)
    job_kind = normalize_job_kind(str(job_row["job_kind"]))
    capability = normalize_job_capability(str(job_version_row["capability"]))
    run_item_counts = dict(run_payload.get("run_item_counts") or {})
    pending_count = int(run_item_counts.get("pending", 0) or 0)
    running_count = int(run_item_counts.get("running", 0) or 0)
    completed_count = int(run_item_counts.get("completed", 0) or 0)
    failed_count = int(run_item_counts.get("failed", 0) or 0)
    skipped_count = int(run_item_counts.get("skipped", 0) or 0)
    planned_count = int(run_payload.get("planned_count", 0) or 0)
    total_items = pending_count + running_count + completed_count + failed_count + skipped_count
    outstanding_items = max(planned_count - completed_count - failed_count - skipped_count, 0)
    needs_ocr_finalization = job_kind == "ocr" and ocr_run_pending_finalization_count(connection, run_id) > 0
    needs_image_description_finalization = (
        job_kind == "image_description" and image_description_run_pending_finalization_count(connection, run_id) > 0
    )
    normalized_claimed_by = normalize_whitespace(claimed_by) if claimed_by and claimed_by.strip() else None
    worker_row = (
        find_run_worker_row(connection, run_id=run_id, claimed_by=normalized_claimed_by)
        if normalized_claimed_by
        else None
    )
    recommended_execution_mode = (
        "background"
        if max(total_items, planned_count) > DEFAULT_WORKER_INLINE_MAX_ITEMS
        else "inline"
    )
    recommended_batch_size = min(
        DEFAULT_RUN_ITEM_CLAIM_BATCH_SIZE,
        DEFAULT_WORKER_BATCH_SIZE,
        max(outstanding_items, 1),
    )
    recommended_max_batches = (
        DEFAULT_WORKER_BACKGROUND_MAX_BATCHES
        if recommended_execution_mode == "background"
        else DEFAULT_WORKER_INLINE_MAX_BATCHES
    )
    effective_max_batches = (
        int(worker_row["max_batches"])
        if worker_row is not None and worker_row["max_batches"] is not None
        else recommended_max_batches
    )
    worker_batches_prepared = int(worker_row["batches_prepared"] or 0) if worker_row is not None else 0
    worker_cancel_requested = worker_row is not None and worker_row["cancel_requested_at"] is not None
    worker_should_handoff = (
        worker_row is not None
        and effective_max_batches > 0
        and worker_batches_prepared >= effective_max_batches
        and pending_count > 0
    )

    next_action = "claim"
    stop_reason = None
    run_status = str(run_payload.get("status") or "")
    if run_status == "canceled":
        next_action = "stop"
        stop_reason = "canceled"
    elif worker_cancel_requested:
        next_action = "stop"
        stop_reason = "canceled"
    elif worker_should_handoff:
        next_action = "handoff"
        stop_reason = "max_batches_reached"
    elif needs_ocr_finalization and pending_count == 0 and running_count == 0 and failed_count == 0:
        next_action = "finalize_ocr"
    elif needs_image_description_finalization and pending_count == 0 and running_count == 0 and failed_count == 0:
        next_action = "finalize_image_description"
    elif planned_count == 0:
        next_action = "stop"
        stop_reason = "empty"
    elif outstanding_items == 0 and pending_count == 0 and running_count == 0:
        next_action = "stop"
        if failed_count:
            stop_reason = "failed"
        elif run_status == "completed":
            stop_reason = "completed"
        else:
            stop_reason = run_status or "idle"

    return {
        "job_kind": job_kind,
        "capability": capability,
        "claimed_by": normalized_claimed_by,
        "launch_mode": worker_row["launch_mode"] if worker_row is not None else None,
        "worker_task_id": worker_row["worker_task_id"] if worker_row is not None else None,
        "worker_status": worker_row["status"] if worker_row is not None else None,
        "recommended_execution_mode": recommended_execution_mode,
        "recommended_batch_size": recommended_batch_size,
        "recommended_max_batches_per_worker": recommended_max_batches,
        "max_batches_per_worker": effective_max_batches,
        "batches_prepared": worker_batches_prepared,
        "outstanding_items": outstanding_items,
        "needs_ocr_finalization": needs_ocr_finalization,
        "needs_image_description_finalization": needs_image_description_finalization,
        "should_exit_after_batch": worker_should_handoff,
        "after_batch_action": "handoff" if worker_should_handoff else None,
        "next_action": next_action,
        "stop_reason": stop_reason,
    }


def recent_run_item_failures(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    limit: int = 10,
) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT id, document_id, segment_id, last_error, completed_at
        FROM run_items
        WHERE run_id = ?
          AND status = 'failed'
        ORDER BY completed_at DESC, id DESC
        LIMIT ?
        """,
        (run_id, max(1, limit)),
    ).fetchall()
    return [
        {
            "run_item_id": int(row["id"]),
            "document_id": int(row["document_id"]),
            "segment_id": row["segment_id"],
            "error": row["last_error"],
            "completed_at": row["completed_at"],
        }
        for row in rows
    ]


def build_run_supervision_payload(
    connection: sqlite3.Connection,
    run_id: int,
    *,
    run_payload: dict[str, object],
    worker_payloads: list[dict[str, object]],
) -> dict[str, object]:
    run_status = str(run_payload.get("status") or "")
    run_worker_hint = build_run_worker_payload(connection, run_id, run_payload=run_payload)
    active_workers = [payload for payload in worker_payloads if str(payload["status"]) == "active"]
    background_workers = [payload for payload in active_workers if str(payload["launch_mode"]) == "background"]
    orphaned_workers = [payload for payload in worker_payloads if str(payload["status"]) == "orphaned"]
    max_parallel_workers = DEFAULT_WORKER_BACKGROUND_MAX_PARALLEL
    outstanding_items = int(run_worker_hint["outstanding_items"] or 0)
    finalization_pending = bool(
        run_worker_hint["needs_ocr_finalization"] or run_worker_hint["needs_image_description_finalization"]
    )
    if run_worker_hint["recommended_execution_mode"] == "background" and outstanding_items > 0:
        suggested_worker_count = min(
            max_parallel_workers,
            max(1, (outstanding_items + DEFAULT_WORKER_BATCH_SIZE - 1) // DEFAULT_WORKER_BATCH_SIZE),
        )
    elif outstanding_items > 0:
        suggested_worker_count = 1
    else:
        suggested_worker_count = 0
    spawn_additional_worker_count = max(0, suggested_worker_count - len(active_workers))
    force_stop_task_ids = [
        str(payload["worker_task_id"])
        for payload in worker_payloads
        if payload["cancel_requested_at"] is not None and payload["worker_task_id"]
    ]
    should_schedule_wakeup = bool(
        run_status not in {"canceled", "completed", "failed"}
        and (outstanding_items > 0 or len(active_workers) > 0 or finalization_pending)
    )
    if run_status in {"canceled", "completed", "failed"}:
        wakeup_reason = None
    elif finalization_pending and not active_workers:
        wakeup_reason = "finalization_pending"
    elif len(active_workers) > 0:
        wakeup_reason = "workers_active"
    elif outstanding_items > 0:
        wakeup_reason = "pending_work"
    else:
        wakeup_reason = None

    recommended_action = "wait"
    if run_status == "canceled":
        recommended_action = "stop"
    elif run_worker_hint["next_action"] in {"finalize_ocr", "finalize_image_description"}:
        recommended_action = str(run_worker_hint["next_action"])
    elif run_worker_hint["next_action"] == "stop":
        recommended_action = "stop"
    elif spawn_additional_worker_count > 0:
        recommended_action = "spawn_background_worker" if run_worker_hint["recommended_execution_mode"] == "background" else "claim_inline"

    return {
        "active_worker_count": len(active_workers),
        "background_worker_count": len(background_workers),
        "orphaned_worker_count": len(orphaned_workers),
        "continuation_needed": bool(outstanding_items and not active_workers and run_status not in {"canceled", "completed", "failed"}),
        "outstanding_items": outstanding_items,
        "finalization_pending": finalization_pending,
        "max_parallel_workers": max_parallel_workers,
        "suggested_worker_count": suggested_worker_count,
        "spawn_additional_worker_count": spawn_additional_worker_count,
        "should_schedule_wakeup": should_schedule_wakeup,
        "wake_interval_seconds": (DEFAULT_WORKER_BACKGROUND_WAKE_INTERVAL_SECONDS if should_schedule_wakeup else None),
        "wakeup_reason": wakeup_reason,
        "force_stop_task_ids": force_stop_task_ids,
        "recommended_action": recommended_action,
    }


def attempt_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "run_item_id": int(row["run_item_id"]),
        "attempt_number": int(row["attempt_number"]),
        "provider_request_id": row["provider_request_id"],
        "input_tokens": row["input_tokens"],
        "output_tokens": row["output_tokens"],
        "cost_cents": row["cost_cents"],
        "latency_ms": row["latency_ms"],
        "provider_metadata": decode_json_text(row["provider_metadata_json"], default={}) or {},
        "error_summary": row["error_summary"],
        "created_at": row["created_at"],
    }


def next_attempt_number_for_run_item(connection: sqlite3.Connection, run_item_id: int) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(attempt_number), 0) AS max_attempt_number
        FROM attempts
        WHERE run_item_id = ?
        """,
        (run_item_id,),
    ).fetchone()
    return int(row["max_attempt_number"] or 0) + 1


def create_attempt_row(
    connection: sqlite3.Connection,
    *,
    run_item_id: int,
    provider_request_id: str | None,
    input_tokens: int | None,
    output_tokens: int | None,
    cost_cents: int | None,
    latency_ms: int | None,
    provider_metadata: dict[str, object] | None,
    error_summary: str | None,
) -> int:
    attempt_number = next_attempt_number_for_run_item(connection, run_item_id)
    cursor = connection.execute(
        """
        INSERT INTO attempts (
          run_item_id,
          attempt_number,
          provider_request_id,
          input_tokens,
          output_tokens,
          cost_cents,
          latency_ms,
          provider_metadata_json,
          error_summary,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_item_id,
            attempt_number,
            provider_request_id,
            input_tokens,
            output_tokens,
            cost_cents,
            latency_ms,
            compact_json_text(provider_metadata or {}),
            error_summary,
            utc_now(),
        ),
    )
    return int(cursor.lastrowid)


def result_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "run_id": row["run_id"],
        "document_id": int(row["document_id"]),
        "job_version_id": int(row["job_version_id"]),
        "input_revision_id": row["input_revision_id"],
        "input_identity": row["input_identity"],
        "raw_output": decode_json_text(row["raw_output_json"]),
        "normalized_output": decode_json_text(row["normalized_output_json"]),
        "created_text_revision_id": row["created_text_revision_id"],
        "provider_metadata": decode_json_text(row["provider_metadata_json"], default={}) or {},
        "created_at": row["created_at"],
        "retracted_at": row["retracted_at"],
        "retraction_reason": row["retraction_reason"],
    }


def find_active_result_row(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    job_version_id: int,
    input_identity: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM results
        WHERE document_id = ?
          AND job_version_id = ?
          AND input_identity = ?
          AND retracted_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (document_id, job_version_id, input_identity),
    ).fetchone()


def create_result_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    document_id: int,
    job_version_id: int,
    input_revision_id: int | None,
    input_identity: str,
    raw_output: object,
    normalized_output: object,
    created_text_revision_id: int | None,
    provider_metadata: dict[str, object] | None,
) -> tuple[int, bool]:
    existing_row = find_active_result_row(
        connection,
        document_id=document_id,
        job_version_id=job_version_id,
        input_identity=input_identity,
    )
    if existing_row is not None:
        return int(existing_row["id"]), False

    now = utc_now()
    try:
        cursor = connection.execute(
            """
            INSERT INTO results (
              run_id,
              document_id,
              job_version_id,
              input_revision_id,
              input_identity,
              raw_output_json,
              normalized_output_json,
              created_text_revision_id,
              provider_metadata_json,
              created_at,
              retracted_at,
              retraction_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                run_id,
                document_id,
                job_version_id,
                input_revision_id,
                input_identity,
                compact_json_text(raw_output),
                compact_json_text(normalized_output),
                created_text_revision_id,
                compact_json_text(provider_metadata or {}),
                now,
            ),
        )
        return int(cursor.lastrowid), True
    except sqlite3.IntegrityError:
        existing_row = find_active_result_row(
            connection,
            document_id=document_id,
            job_version_id=job_version_id,
            input_identity=input_identity,
        )
        if existing_row is None:
            raise
        return int(existing_row["id"]), False


def result_output_display_value(output_value: object) -> str | None:
    if output_value is None:
        return None
    if isinstance(output_value, bool):
        return "true" if output_value else "false"
    if isinstance(output_value, (int, float)):
        return str(output_value)
    if isinstance(output_value, str):
        return output_value
    return compact_json_text(output_value)


def upsert_result_output_rows(
    connection: sqlite3.Connection,
    *,
    result_id: int,
    job_output_rows: list[sqlite3.Row],
    output_values_by_name: dict[str, object],
) -> None:
    now = utc_now()
    for job_output_row in job_output_rows:
        output_name = str(job_output_row["output_name"])
        output_value = output_values_by_name.get(output_name)
        connection.execute(
            """
            INSERT INTO result_outputs (
              result_id,
              job_output_id,
              output_value_json,
              display_value,
              score,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(result_id, job_output_id) DO NOTHING
            """,
            (
                result_id,
                int(job_output_row["id"]),
                compact_json_text(output_value),
                result_output_display_value(output_value),
                None,
                now,
            ),
        )


def result_output_row_to_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "result_id": int(row["result_id"]),
        "job_output_id": int(row["job_output_id"]),
        "output_name": row["output_name"],
        "value_type": row["value_type"],
        "bound_custom_field": row["bound_custom_field"],
        "output_value": decode_json_text(row["output_value_json"]),
        "display_value": row["display_value"],
        "score": row["score"],
        "created_at": row["created_at"],
    }


def result_outputs_for_result(connection: sqlite3.Connection, result_id: int) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT ro.*, jo.output_name, jo.value_type, jo.bound_custom_field
        FROM result_outputs ro
        JOIN job_outputs jo ON jo.id = ro.job_output_id
        WHERE ro.result_id = ?
        ORDER BY jo.ordinal ASC, jo.output_name ASC, ro.id ASC
        """,
        (result_id,),
    ).fetchall()
    return [result_output_row_to_payload(row) for row in rows]


def result_summary_by_id(connection: sqlite3.Connection, result_id: int) -> dict[str, object]:
    row = connection.execute(
        """
        SELECT *
        FROM results
        WHERE id = ?
        """,
        (result_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown result id: {result_id}")
    payload = result_row_to_payload(row)
    payload["outputs"] = result_outputs_for_result(connection, result_id)
    return payload


def list_result_summaries(
    connection: sqlite3.Connection,
    *,
    run_id: int | None = None,
    document_id: int | None = None,
) -> list[dict[str, object]]:
    if run_id is not None:
        rows = connection.execute(
            """
            SELECT DISTINCT r.*
            FROM run_items ri
            JOIN results r ON r.id = ri.result_id
            WHERE ri.run_id = ?
            ORDER BY r.id DESC
            """,
            (run_id,),
        ).fetchall()
        if document_id is not None:
            rows = [row for row in rows if int(row["document_id"]) == int(document_id)]
        return [result_summary_by_id(connection, int(row["id"])) for row in rows]

    if document_id is None:
        rows = connection.execute(
            """
            SELECT *
            FROM results
            ORDER BY id DESC
            """
        ).fetchall()
    else:
        rows = connection.execute(
            """
            SELECT *
            FROM results
            WHERE document_id = ?
            ORDER BY id DESC
            """,
            (document_id,),
        ).fetchall()
    return [result_summary_by_id(connection, int(row["id"])) for row in rows]


def refresh_run_progress(connection: sqlite3.Connection, run_id: int) -> None:
    snapshot_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM run_snapshot_documents
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()["count"]
        or 0
    )
    run_item_total = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM run_items
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()["count"]
        or 0
    )
    planned_count = run_item_total or snapshot_count
    counts = {
        str(row["status"]): int(row["count"] or 0)
        for row in connection.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM run_items
            WHERE run_id = ?
            GROUP BY status
            """,
            (run_id,),
        ).fetchall()
    }
    completed_count = counts.get("completed", 0)
    failed_count = counts.get("failed", 0)
    skipped_count = counts.get("skipped", 0)
    running_count = counts.get("running", 0)
    pending_count = counts.get("pending", 0)
    row = connection.execute(
        """
        SELECT started_at, canceled_at
        FROM runs
        WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown run id: {run_id}")
    if row["canceled_at"] is not None:
        status = "canceled"
    elif planned_count == 0:
        status = "completed"
    elif completed_count + failed_count + skipped_count >= planned_count and pending_count == 0 and running_count == 0:
        status = "failed" if failed_count else "completed"
    elif running_count > 0:
        status = "running"
    else:
        status = "planned"
    started_at = row["started_at"] or (utc_now() if status in {"running", "completed", "failed"} else None)
    completed_at = utc_now() if status in {"completed", "failed", "canceled"} else None
    connection.execute(
        """
        UPDATE runs
        SET status = ?,
            planned_count = ?,
            completed_count = ?,
            failed_count = ?,
            skipped_count = ?,
            started_at = ?,
            completed_at = ?
        WHERE id = ?
        """,
        (
            status,
            planned_count,
            completed_count,
            failed_count,
            skipped_count,
            started_at,
            completed_at,
            run_id,
        ),
    )


def run_status_by_id(connection: sqlite3.Connection, run_id: int) -> dict[str, object]:
    refresh_run_progress(connection, run_id)
    payload = run_summary_by_id(connection, run_id)
    status_counts = run_item_status_counts(connection, run_id)
    payload["run_item_counts"] = {
        "pending": status_counts.get("pending", 0),
        "running": status_counts.get("running", 0),
        "completed": status_counts.get("completed", 0),
        "failed": status_counts.get("failed", 0),
        "skipped": status_counts.get("skipped", 0),
    }
    payload["snapshot_document_count"] = len(payload.get("documents", []))
    payload["recent_failures"] = recent_run_item_failures(connection, run_id=run_id)
    active_claim_rows = connection.execute(
        """
        SELECT claimed_by, COUNT(*) AS count, MAX(last_heartbeat_at) AS last_heartbeat_at
        FROM run_items
        WHERE run_id = ?
          AND status = 'running'
          AND claimed_by IS NOT NULL
        GROUP BY claimed_by
        ORDER BY claimed_by ASC
        """,
        (run_id,),
    ).fetchall()
    payload["active_claims"] = [
        {
            "claimed_by": row["claimed_by"],
            "count": int(row["count"] or 0),
            "last_heartbeat_at": row["last_heartbeat_at"],
        }
        for row in active_claim_rows
    ]
    payload["workers"] = list_run_worker_payloads_for_run(connection, run_id)
    payload["worker"] = build_run_worker_payload(connection, run_id, run_payload=payload)
    payload["supervision"] = build_run_supervision_payload(
        connection,
        run_id,
        run_payload=payload,
        worker_payloads=payload["workers"],
    )
    return payload


def finalize_ocr_results_for_run(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: int,
) -> dict[str, object]:
    run_row = require_run_row_by_id(connection, run_id)
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
    if normalize_job_kind(str(job_row["job_kind"])) != "ocr":
        raise RetrieverError("finalize-ocr-run only supports OCR jobs.")

    pending_counts = run_item_status_counts(connection, run_id)
    if pending_counts.get("pending", 0) or pending_counts.get("running", 0) or pending_counts.get("failed", 0):
        raise RetrieverError(
            "OCR run cannot be finalized until all page items are completed. "
            f"pending={pending_counts.get('pending', 0)}, "
            f"running={pending_counts.get('running', 0)}, "
            f"failed={pending_counts.get('failed', 0)}."
        )

    snapshot_rows = connection.execute(
        """
        SELECT *
        FROM run_snapshot_documents
        WHERE run_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (run_id,),
    ).fetchall()
    finalized_results: list[dict[str, object]] = []
    activations: list[dict[str, object]] = []
    for snapshot_row in snapshot_rows:
        document_id = int(snapshot_row["document_id"])
        existing_result_row = find_active_result_row(
            connection,
            document_id=document_id,
            job_version_id=int(job_version_row["id"]),
            input_identity=str(snapshot_row["pinned_input_identity"]),
        )
        if existing_result_row is not None and existing_result_row["created_text_revision_id"] is not None:
            result_id = int(existing_result_row["id"])
            connection.execute(
                """
                UPDATE run_items
                SET result_id = ?
                WHERE run_id = ?
                  AND document_id = ?
                """,
                (result_id, run_id, document_id),
            )
            activation_payload = maybe_activate_created_text_revision(
                connection,
                paths,
                run_row=run_row,
                job_version_row=job_version_row,
                document_id=document_id,
                result_id=result_id,
                text_revision_id=int(existing_result_row["created_text_revision_id"]),
            )
            if activation_payload is not None:
                activations.append(activation_payload)
            finalized_results.append(result_summary_by_id(connection, result_id))
            continue

        page_rows = connection.execute(
            """
            SELECT *
            FROM run_items
            WHERE run_id = ?
              AND document_id = ?
              AND item_kind = 'page'
            ORDER BY page_number ASC, id ASC
            """,
            (run_id, document_id),
        ).fetchall()
        if not page_rows:
            raise RetrieverError(f"OCR run {run_id} has no page items for document {document_id}.")
        page_output_payloads = list_ocr_page_output_payloads_for_document(
            connection,
            run_id=run_id,
            document_id=document_id,
        )
        if len(page_output_payloads) != len(page_rows):
            raise RetrieverError(
                f"OCR run {run_id} is missing page outputs for document {document_id}: "
                f"expected {len(page_rows)}, found {len(page_output_payloads)}."
            )
        merged_text = "\n\n".join(str(payload["text_content"]) for payload in page_output_payloads if str(payload["text_content"]).strip())
        created_text_revision_id = create_text_revision_row(
            connection,
            paths,
            document_id=document_id,
            revision_kind="ocr",
            text_content=merged_text,
            language=None,
            parent_revision_id=(
                int(snapshot_row["pinned_input_revision_id"])
                if snapshot_row["pinned_input_revision_id"] is not None
                else None
            ),
            created_by_job_version_id=int(job_version_row["id"]),
            quality_score=None,
            provider_metadata={
                "run_id": run_id,
                "page_count": len(page_output_payloads),
                "finalized_from": "ocr_page_outputs",
            },
        )
        result_id, _ = create_result_row(
            connection,
            run_id=run_id,
            document_id=document_id,
            job_version_id=int(job_version_row["id"]),
            input_revision_id=(
                int(snapshot_row["pinned_input_revision_id"])
                if snapshot_row["pinned_input_revision_id"] is not None
                else None
            ),
            input_identity=str(snapshot_row["pinned_input_identity"]),
            raw_output={
                "page_count": len(page_output_payloads),
                "finalized_from": "ocr_page_outputs",
            },
            normalized_output={
                "page_count": len(page_output_payloads),
                "created_text_revision_id": created_text_revision_id,
            },
            created_text_revision_id=created_text_revision_id,
            provider_metadata={
                "run_id": run_id,
                "page_count": len(page_output_payloads),
            },
        )
        connection.execute(
            """
            UPDATE run_items
            SET result_id = ?
            WHERE run_id = ?
              AND document_id = ?
            """,
            (result_id, run_id, document_id),
        )
        activation_payload = maybe_activate_created_text_revision(
            connection,
            paths,
            run_row=run_row,
            job_version_row=job_version_row,
            document_id=document_id,
            result_id=result_id,
            text_revision_id=created_text_revision_id,
        )
        if activation_payload is not None:
            activations.append(activation_payload)
        finalized_results.append(result_summary_by_id(connection, result_id))

    refresh_run_progress(connection, run_id)
    return {
        "status": "ok",
        "run": run_status_by_id(connection, run_id),
        "results": finalized_results,
        "activations": activations,
    }


def build_image_description_revision_text(page_output_payloads: list[dict[str, object]]) -> str:
    sections: list[str] = []
    for payload in page_output_payloads:
        page_number = int(payload["page_number"])
        text_content = normalize_whitespace(str(payload["text_content"] or ""))
        if not text_content:
            continue
        sections.append(f"[IMAGE DESCRIPTION - PAGE {page_number}]\n{text_content}")
    return "\n\n".join(sections)


def finalize_image_description_results_for_run(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: int,
) -> dict[str, object]:
    run_row = require_run_row_by_id(connection, run_id)
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
    if normalize_job_kind(str(job_row["job_kind"])) != "image_description":
        raise RetrieverError("finalize-image-description-run only supports image_description jobs.")

    pending_counts = run_item_status_counts(connection, run_id)
    if pending_counts.get("pending", 0) or pending_counts.get("running", 0) or pending_counts.get("failed", 0):
        raise RetrieverError(
            "Image-description run cannot be finalized until all page items are completed. "
            f"pending={pending_counts.get('pending', 0)}, "
            f"running={pending_counts.get('running', 0)}, "
            f"failed={pending_counts.get('failed', 0)}."
        )

    snapshot_rows = connection.execute(
        """
        SELECT *
        FROM run_snapshot_documents
        WHERE run_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (run_id,),
    ).fetchall()
    finalized_results: list[dict[str, object]] = []
    activations: list[dict[str, object]] = []
    for snapshot_row in snapshot_rows:
        document_id = int(snapshot_row["document_id"])
        existing_result_row = find_active_result_row(
            connection,
            document_id=document_id,
            job_version_id=int(job_version_row["id"]),
            input_identity=str(snapshot_row["pinned_input_identity"]),
        )
        if existing_result_row is not None and existing_result_row["created_text_revision_id"] is not None:
            result_id = int(existing_result_row["id"])
            connection.execute(
                """
                UPDATE run_items
                SET result_id = ?
                WHERE run_id = ?
                  AND document_id = ?
                """,
                (result_id, run_id, document_id),
            )
            activation_payload = maybe_activate_created_text_revision(
                connection,
                paths,
                run_row=run_row,
                job_version_row=job_version_row,
                document_id=document_id,
                result_id=result_id,
                text_revision_id=int(existing_result_row["created_text_revision_id"]),
            )
            if activation_payload is not None:
                activations.append(activation_payload)
            finalized_results.append(result_summary_by_id(connection, result_id))
            continue

        page_rows = connection.execute(
            """
            SELECT *
            FROM run_items
            WHERE run_id = ?
              AND document_id = ?
              AND item_kind = 'page'
            ORDER BY page_number ASC, id ASC
            """,
            (run_id, document_id),
        ).fetchall()
        if not page_rows:
            raise RetrieverError(
                f"Image-description run {run_id} has no page items for document {document_id}."
            )
        page_output_payloads = list_image_description_page_output_payloads_for_document(
            connection,
            run_id=run_id,
            document_id=document_id,
        )
        if len(page_output_payloads) != len(page_rows):
            raise RetrieverError(
                f"Image-description run {run_id} is missing page outputs for document {document_id}: "
                f"expected {len(page_rows)}, found {len(page_output_payloads)}."
            )
        merged_text = build_image_description_revision_text(page_output_payloads)
        created_text_revision_id = create_text_revision_row(
            connection,
            paths,
            document_id=document_id,
            revision_kind="image_description",
            text_content=merged_text,
            language=None,
            parent_revision_id=(
                int(snapshot_row["pinned_input_revision_id"])
                if snapshot_row["pinned_input_revision_id"] is not None
                else None
            ),
            created_by_job_version_id=int(job_version_row["id"]),
            quality_score=None,
            provider_metadata={
                "run_id": run_id,
                "page_count": len(page_output_payloads),
                "finalized_from": "image_description_page_outputs",
            },
        )
        result_id, _ = create_result_row(
            connection,
            run_id=run_id,
            document_id=document_id,
            job_version_id=int(job_version_row["id"]),
            input_revision_id=(
                int(snapshot_row["pinned_input_revision_id"])
                if snapshot_row["pinned_input_revision_id"] is not None
                else None
            ),
            input_identity=str(snapshot_row["pinned_input_identity"]),
            raw_output={
                "page_count": len(page_output_payloads),
                "finalized_from": "image_description_page_outputs",
            },
            normalized_output={
                "page_count": len(page_output_payloads),
                "created_text_revision_id": created_text_revision_id,
            },
            created_text_revision_id=created_text_revision_id,
            provider_metadata={
                "run_id": run_id,
                "page_count": len(page_output_payloads),
            },
        )
        connection.execute(
            """
            UPDATE run_items
            SET result_id = ?
            WHERE run_id = ?
              AND document_id = ?
            """,
            (result_id, run_id, document_id),
        )
        activation_payload = maybe_activate_created_text_revision(
            connection,
            paths,
            run_row=run_row,
            job_version_row=job_version_row,
            document_id=document_id,
            result_id=result_id,
            text_revision_id=created_text_revision_id,
        )
        if activation_payload is not None:
            activations.append(activation_payload)
        finalized_results.append(result_summary_by_id(connection, result_id))

    refresh_run_progress(connection, run_id)
    return {
        "status": "ok",
        "run": run_status_by_id(connection, run_id),
        "results": finalized_results,
        "activations": activations,
    }


def coerce_result_output_value_for_publication(field_type: str, output_value: object) -> object:
    if output_value is None:
        return None
    if field_type == "text":
        return result_output_display_value(output_value)
    if isinstance(output_value, bool):
        raw_value = "true" if output_value else "false"
    elif isinstance(output_value, (int, float)):
        raw_value = str(output_value)
    elif isinstance(output_value, str):
        raw_value = output_value
    else:
        raw_value = compact_json_text(output_value)
    return value_from_type(field_type, raw_value)


def publish_result_outputs_for_run(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    output_names: list[str] | None = None,
) -> dict[str, object]:
    normalized_output_names = {
        sanitize_processing_identifier(raw_name, label="Job output name", prefix="output")
        for raw_name in (output_names or [])
    }
    result_output_rows = connection.execute(
        """
        SELECT DISTINCT
          ro.id AS result_output_id,
          ro.result_id,
          ro.output_value_json,
          jo.id AS job_output_id,
          jo.output_name,
          jo.bound_custom_field,
          ri.document_id
        FROM run_items ri
        JOIN result_outputs ro ON ro.result_id = ri.result_id
        JOIN job_outputs jo ON jo.id = ro.job_output_id
        WHERE ri.run_id = ?
          AND ri.result_id IS NOT NULL
          AND jo.bound_custom_field IS NOT NULL
        ORDER BY ri.document_id ASC, jo.output_name ASC, ro.id ASC
        """,
        (run_id,),
    ).fetchall()
    published = 0
    affected_documents: set[int] = set()
    published_outputs: list[dict[str, object]] = []
    now = utc_now()
    for row in result_output_rows:
        output_name = str(row["output_name"])
        if normalized_output_names and output_name not in normalized_output_names:
            continue
        custom_field_name = str(row["bound_custom_field"] or "")
        field_def = resolve_field_definition(connection, custom_field_name)
        if field_def["source"] != "custom":
            raise RetrieverError(
                f"Bound field {custom_field_name!r} for output {output_name!r} must be a custom field."
            )
        document_id = int(row["document_id"])
        output_value = decode_json_text(row["output_value_json"])
        typed_value = coerce_result_output_value_for_publication(field_def["field_type"], output_value)
        connection.execute(
            f"""
            UPDATE documents
            SET {quote_identifier(field_def['field_name'])} = ?, updated_at = ?
            WHERE id = ?
            """,
            (typed_value, now, document_id),
        )
        connection.execute(
            """
            INSERT INTO publications (
              result_output_id,
              document_id,
              job_output_id,
              custom_field_name,
              published_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(row["result_output_id"]),
                document_id,
                int(row["job_output_id"]),
                field_def["field_name"],
                now,
            ),
        )
        published += 1
        affected_documents.add(document_id)
        published_outputs.append(
            {
                "document_id": document_id,
                "result_id": int(row["result_id"]),
                "output_name": output_name,
                "custom_field_name": field_def["field_name"],
                "value": typed_value,
            }
        )
    return {
        "published_count": published,
        "document_count": len(affected_documents),
        "published_outputs": published_outputs,
    }


def run_summary_by_id(connection: sqlite3.Connection, run_id: int) -> dict[str, object]:
    row = connection.execute(
        """
        SELECT *
        FROM runs
        WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown run id: {run_id}")
    payload = run_row_to_payload(row)
    payload["job_version"] = job_version_summary_by_id(connection, int(row["job_version_id"]))
    snapshot_rows = connection.execute(
        """
        SELECT *
        FROM run_snapshot_documents
        WHERE run_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (run_id,),
    ).fetchall()
    payload["documents"] = [run_snapshot_document_row_to_payload(snapshot_row) for snapshot_row in snapshot_rows]
    return payload


def list_run_summaries(connection: sqlite3.Connection) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM runs
        ORDER BY id DESC
        """
    ).fetchall()
    return [run_summary_by_id(connection, int(row["id"])) for row in rows]
