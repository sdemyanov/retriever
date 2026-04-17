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
          provider,
          model,
          parameters_json,
          input_basis,
          segment_profile,
          aggregation_strategy,
          created_at,
          archived_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            job_id,
            version,
            resolved_display_name,
            instruction_text,
            sha256_text(instruction_text),
            response_schema_json,
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
