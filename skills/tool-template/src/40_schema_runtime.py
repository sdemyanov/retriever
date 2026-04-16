def inspect_custom_fields_registry(connection: sqlite3.Connection) -> dict[str, object]:
    columns = table_info(connection, "documents")
    column_map = {row["name"]: row for row in columns}
    actual_custom_fields = [
        name
        for name in column_map
        if name not in BUILTIN_FIELD_TYPES and name not in {LEGACY_METADATA_LOCKS_COLUMN}
    ]
    registry_rows = connection.execute(
        """
        SELECT field_name, field_type, instruction, created_at
        FROM custom_fields_registry
        ORDER BY field_name ASC
        """
    ).fetchall()
    registry_fields = {row["field_name"]: row for row in registry_rows}
    missing_registry = sorted(name for name in actual_custom_fields if name not in registry_fields)
    orphaned_registry = sorted(name for name in registry_fields if name not in column_map)
    return {
        "actual_custom_fields": sorted(actual_custom_fields),
        "missing_registry": missing_registry,
        "orphaned_registry": orphaned_registry,
    }


def reconcile_custom_fields_registry(connection: sqlite3.Connection, repair: bool) -> dict[str, object]:
    status = inspect_custom_fields_registry(connection)
    repaired: list[str] = []
    if repair:
        now = utc_now()
        for field_name in status["missing_registry"]:
            sqlite_type = next(
                (row["type"] for row in table_info(connection, "documents") if row["name"] == field_name),
                "",
            )
            connection.execute(
                """
                INSERT INTO custom_fields_registry (field_name, field_type, instruction, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(field_name) DO NOTHING
                """,
                (field_name, infer_registry_field_type(sqlite_type), None, now),
            )
            repaired.append(field_name)
        if repaired:
            connection.commit()
        status = inspect_custom_fields_registry(connection)
    status["repaired_registry"] = repaired
    return status


def merge_legacy_field_locks(connection: sqlite3.Connection) -> int:
    columns = table_columns(connection, "documents")
    if MANUAL_FIELD_LOCKS_COLUMN not in columns or LEGACY_METADATA_LOCKS_COLUMN not in columns:
        return 0

    rows = connection.execute(
        f"""
        SELECT id, {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} AS manual_locks,
               {quote_identifier(LEGACY_METADATA_LOCKS_COLUMN)} AS legacy_locks
        FROM documents
        """
    ).fetchall()

    merged_count = 0
    for row in rows:
        manual_locks = normalize_string_list(row["manual_locks"])
        legacy_locks = normalize_string_list(row["legacy_locks"])
        if not legacy_locks:
            continue

        merged = list(manual_locks)
        for field_name in legacy_locks:
            if field_name not in merged:
                merged.append(field_name)
        if merged == manual_locks:
            continue

        connection.execute(
            f"""
            UPDATE documents
            SET {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} = ?
            WHERE id = ?
            """,
            (json.dumps(merged), row["id"]),
        )
        merged_count += 1
    return merged_count


def backfill_content_type(connection: sqlite3.Connection) -> int:
    columns = table_columns(connection, "documents")
    if "content_type" not in columns:
        return 0

    rows = connection.execute(
        """
        SELECT id, file_type, content_type
        FROM documents
        WHERE content_type IS NULL OR TRIM(content_type) = ''
        """
    ).fetchall()
    updated = 0
    for row in rows:
        inferred = infer_content_type_from_extension((row["file_type"] or "").lower())
        if not inferred:
            continue
        connection.execute(
            "UPDATE documents SET content_type = ? WHERE id = ?",
            (inferred, row["id"]),
        )
        updated += 1
    return updated


def ensure_control_number_batch_row(
    connection: sqlite3.Connection,
    batch_number: int,
    next_family_sequence: int,
) -> int:
    row = connection.execute(
        """
        SELECT next_family_sequence
        FROM control_number_batches
        WHERE batch_number = ?
        """,
        (batch_number,),
    ).fetchone()
    now = utc_now()
    if row is None:
        connection.execute(
            """
            INSERT INTO control_number_batches (batch_number, next_family_sequence, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (batch_number, next_family_sequence, now, now),
        )
        return next_family_sequence
    normalized_next = max(int(row["next_family_sequence"]), next_family_sequence)
    if normalized_next != int(row["next_family_sequence"]):
        connection.execute(
            """
            UPDATE control_number_batches
            SET next_family_sequence = ?, updated_at = ?
            WHERE batch_number = ?
            """,
            (normalized_next, now, batch_number),
        )
    return normalized_next


def backfill_control_number_batches(connection: sqlite3.Connection) -> int:
    columns = table_columns(connection, "documents")
    required = {"control_number_batch", "control_number_family_sequence"}
    if not required.issubset(columns):
        return 0
    rows = connection.execute(
        """
        SELECT control_number_batch, MAX(control_number_family_sequence) AS max_family_sequence
        FROM documents
        WHERE control_number_batch IS NOT NULL AND control_number_family_sequence IS NOT NULL
        GROUP BY control_number_batch
        """
    ).fetchall()
    updated = 0
    for row in rows:
        before = connection.execute(
            """
            SELECT next_family_sequence
            FROM control_number_batches
            WHERE batch_number = ?
            """,
            (row["control_number_batch"],),
        ).fetchone()
        next_sequence = int(row["max_family_sequence"] or 0) + 1
        ensure_control_number_batch_row(connection, int(row["control_number_batch"]), next_sequence)
        if before is None or int(before["next_family_sequence"]) != next_sequence:
            updated += 1
    return updated


def backfill_control_numbers(connection: sqlite3.Connection) -> int:
    columns = table_columns(connection, "documents")
    required = {
        "control_number",
        "parent_document_id",
        "control_number_batch",
        "control_number_family_sequence",
        "control_number_attachment_sequence",
        "source_kind",
    }
    if not required.issubset(columns):
        return 0

    backfill_control_number_batches(connection)
    updated = 0

    top_level_rows = connection.execute(
        """
        SELECT id, control_number
        FROM documents
        WHERE parent_document_id IS NULL
          AND COALESCE(source_kind, ?) != ?
          AND (
            control_number IS NULL OR TRIM(control_number) = ''
            OR control_number_batch IS NULL
            OR control_number_family_sequence IS NULL
          )
        ORDER BY id ASC
        """
    , (FILESYSTEM_SOURCE_KIND, PRODUCTION_SOURCE_KIND)).fetchall()
    if top_level_rows:
        next_family_sequence = ensure_control_number_batch_row(connection, 1, 1)
        for row in top_level_rows:
            parsed_identity = parse_control_number(row["control_number"])
            if parsed_identity is not None and parsed_identity[2] is None:
                batch_number, family_sequence, _ = parsed_identity
                control_number = str(row["control_number"]).strip()
                ensure_control_number_batch_row(connection, batch_number, family_sequence + 1)
                if batch_number == 1:
                    next_family_sequence = max(next_family_sequence, family_sequence + 1)
            else:
                batch_number = 1
                family_sequence = next_family_sequence
                control_number = format_control_number(batch_number, family_sequence)
                next_family_sequence += 1
            connection.execute(
                """
                UPDATE documents
                SET control_number = ?, control_number_batch = ?, control_number_family_sequence = ?, control_number_attachment_sequence = NULL
                WHERE id = ?
                """,
                (control_number, batch_number, family_sequence, row["id"]),
            )
            updated += 1
        ensure_control_number_batch_row(connection, 1, next_family_sequence)

    parent_identities = {
        int(row["id"]): (int(row["control_number_batch"]), int(row["control_number_family_sequence"]))
        for row in connection.execute(
            """
            SELECT id, control_number_batch, control_number_family_sequence
            FROM documents
            WHERE parent_document_id IS NULL
              AND COALESCE(source_kind, ?) != ?
              AND control_number_batch IS NOT NULL
              AND control_number_family_sequence IS NOT NULL
            """
        , (FILESYSTEM_SOURCE_KIND, PRODUCTION_SOURCE_KIND)).fetchall()
    }
    attachment_counters: dict[int, int] = {}
    child_rows = connection.execute(
        """
        SELECT id, parent_document_id, control_number
        FROM documents
        WHERE parent_document_id IS NOT NULL
          AND COALESCE(source_kind, ?) != ?
          AND (
            control_number IS NULL OR TRIM(control_number) = ''
            OR control_number_batch IS NULL
            OR control_number_family_sequence IS NULL
            OR control_number_attachment_sequence IS NULL
          )
        ORDER BY parent_document_id ASC, id ASC
        """
    , (EMAIL_ATTACHMENT_SOURCE_KIND, PRODUCTION_SOURCE_KIND)).fetchall()
    for row in child_rows:
        parent_document_id = int(row["parent_document_id"])
        parent_identity = parent_identities.get(parent_document_id)
        if parent_identity is None:
            continue
        parsed_identity = parse_control_number(row["control_number"])
        batch_number, family_sequence = parent_identity
        if parsed_identity is not None and parsed_identity[2] is not None:
            _, _, attachment_sequence = parsed_identity
        else:
            if parent_document_id not in attachment_counters:
                max_sequence = connection.execute(
                    """
                    SELECT MAX(control_number_attachment_sequence) AS max_attachment_sequence
                    FROM documents
                    WHERE parent_document_id = ?
                    """,
                    (parent_document_id,),
                ).fetchone()
                attachment_counters[parent_document_id] = int(max_sequence["max_attachment_sequence"] or 0)
            attachment_counters[parent_document_id] += 1
            attachment_sequence = attachment_counters[parent_document_id]
        connection.execute(
            """
            UPDATE documents
            SET control_number = ?, control_number_batch = ?, control_number_family_sequence = ?, control_number_attachment_sequence = ?
            WHERE id = ?
            """,
            (
                format_control_number(batch_number, family_sequence, attachment_sequence),
                batch_number,
                family_sequence,
                attachment_sequence,
                row["id"],
            ),
        )
        updated += 1

    return updated


def allocate_ingestion_batch_number(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT MAX(batch_number) AS max_batch_number FROM control_number_batches").fetchone()
    batch_number = int(row["max_batch_number"] or 0) + 1
    ensure_control_number_batch_row(connection, batch_number, 1)
    return batch_number


def reserve_control_number_family_sequence(connection: sqlite3.Connection, batch_number: int) -> int:
    next_family_sequence = ensure_control_number_batch_row(connection, batch_number, 1)
    connection.execute(
        """
        UPDATE control_number_batches
        SET next_family_sequence = ?, updated_at = ?
        WHERE batch_number = ?
        """,
        (next_family_sequence + 1, utc_now(), batch_number),
    )
    return next_family_sequence


def next_attachment_sequence(connection: sqlite3.Connection, parent_document_id: int) -> int:
    row = connection.execute(
        """
        SELECT MAX(control_number_attachment_sequence) AS max_attachment_sequence
        FROM documents
        WHERE parent_document_id = ?
        """,
        (parent_document_id,),
    ).fetchone()
    return int(row["max_attachment_sequence"] or 0) + 1


def ensure_documents_fts(connection: sqlite3.Connection) -> bool:
    expected_columns = {"document_id", "file_name", "title", "subject", "author", "participants", "recipients"}
    existing_columns = table_columns(connection, "documents_fts")
    if existing_columns == expected_columns:
        return False

    connection.execute("DROP TABLE IF EXISTS documents_fts")
    connection.execute(
        """
        CREATE VIRTUAL TABLE documents_fts USING fts5(
          document_id UNINDEXED,
          file_name,
          title,
          subject,
          author,
          participants,
          recipients
        )
        """
    )
    rows = connection.execute(
        """
        SELECT id, file_name, title, subject, author, participants, recipients
        FROM documents
        """
    ).fetchall()
    if rows:
        connection.executemany(
            """
            INSERT INTO documents_fts (document_id, file_name, title, subject, author, participants, recipients)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["id"],
                    row["file_name"],
                    row["title"],
                    row["subject"],
                    row["author"],
                    row["participants"],
                    row["recipients"],
                )
                for row in rows
            ],
        )
    return True


def apply_schema(connection: sqlite3.Connection) -> dict[str, object]:
    rename_table_if_needed(connection, "display_id_batches", "control_number_batches")
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)

    if table_exists(connection, "documents"):
        rename_column_if_needed(connection, "documents", "display_id", "control_number")
        rename_column_if_needed(connection, "documents", "display_batch", "control_number_batch")
        rename_column_if_needed(connection, "documents", "display_family_sequence", "control_number_family_sequence")
        rename_column_if_needed(connection, "documents", "display_attachment_sequence", "control_number_attachment_sequence")

    ensure_column(connection, "documents", f"{MANUAL_FIELD_LOCKS_COLUMN} TEXT NOT NULL DEFAULT '[]'")
    ensure_column(connection, "documents", "content_type TEXT")
    ensure_column(connection, "documents", "participants TEXT")
    ensure_column(connection, "documents", "control_number TEXT")
    ensure_column(connection, "documents", "parent_document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE")
    ensure_column(connection, "documents", "source_kind TEXT")
    ensure_column(connection, "documents", "source_rel_path TEXT")
    ensure_column(connection, "documents", "source_item_id TEXT")
    ensure_column(connection, "documents", "source_folder_path TEXT")
    ensure_column(connection, "documents", "production_id INTEGER REFERENCES productions(id) ON DELETE SET NULL")
    ensure_column(connection, "documents", "begin_bates TEXT")
    ensure_column(connection, "documents", "end_bates TEXT")
    ensure_column(connection, "documents", "begin_attachment TEXT")
    ensure_column(connection, "documents", "end_attachment TEXT")
    ensure_column(connection, "documents", "control_number_batch INTEGER")
    ensure_column(connection, "documents", "control_number_family_sequence INTEGER")
    ensure_column(connection, "documents", "control_number_attachment_sequence INTEGER")
    backfilled_legacy_control_number = backfill_legacy_column(
        connection,
        "documents",
        "display_id",
        "control_number",
        treat_blank_as_missing=True,
    )
    backfilled_legacy_control_number_batch = backfill_legacy_column(
        connection,
        "documents",
        "display_batch",
        "control_number_batch",
    )
    backfilled_legacy_control_number_family_sequence = backfill_legacy_column(
        connection,
        "documents",
        "display_family_sequence",
        "control_number_family_sequence",
    )
    backfilled_legacy_control_number_attachment_sequence = backfill_legacy_column(
        connection,
        "documents",
        "display_attachment_sequence",
        "control_number_attachment_sequence",
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_parent_document_id ON documents(parent_document_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_source_kind ON documents(source_kind)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_source_rel_path ON documents(source_rel_path)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_production_id ON documents(production_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_begin_bates ON documents(begin_bates)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_end_bates ON documents(end_bates)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_container_sources_source_kind ON container_sources(source_kind)")
    connection.execute("DROP INDEX IF EXISTS idx_documents_display_sort")
    connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_control_number_unique ON documents(control_number)")
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_source_identity_unique
        ON documents(source_rel_path, source_item_id)
        WHERE source_kind IS NOT NULL AND source_item_id IS NOT NULL
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_documents_control_number_sort
        ON documents(control_number_batch, control_number_family_sequence, control_number_attachment_sequence)
        """
    )
    merged_legacy_locks = merge_legacy_field_locks(connection)
    backfilled_content_type = backfill_content_type(connection)
    backfilled_source_kinds = backfill_source_kinds(connection)
    backfilled_control_numbers = backfill_control_numbers(connection)
    rebuilt_control_number_batches = backfill_control_number_batches(connection)
    rebuilt_documents_fts = ensure_documents_fts(connection)
    connection.commit()
    return {
        "schema_version": SCHEMA_VERSION,
        "backfilled_content_type": backfilled_content_type,
        "backfilled_source_kinds": backfilled_source_kinds,
        "backfilled_control_numbers": backfilled_control_numbers,
        "rebuilt_control_number_batches": rebuilt_control_number_batches,
        "backfilled_legacy_control_number": backfilled_legacy_control_number,
        "backfilled_legacy_control_number_batch": backfilled_legacy_control_number_batch,
        "backfilled_legacy_control_number_family_sequence": backfilled_legacy_control_number_family_sequence,
        "backfilled_legacy_control_number_attachment_sequence": backfilled_legacy_control_number_attachment_sequence,
        "rebuilt_documents_fts": rebuilt_documents_fts,
        "merged_legacy_locks": merged_legacy_locks,
    }


def read_runtime(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_runtime(paths: dict[str, Path], tool_sha256: str | None) -> dict[str, object]:
    now = utc_now()
    runtime = {
        "tool_version": TOOL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "requirements_version": REQUIREMENTS_VERSION,
        "template_source": TEMPLATE_SOURCE,
        "template_sha256": tool_sha256,
        "python_version": platform.python_version(),
        "generated_at": now,
        "last_verified_at": now,
    }
    paths["runtime_path"].write_text(json.dumps(runtime, indent=2, sort_keys=True), encoding="utf-8")
    return runtime


def probe_fts5() -> dict[str, str]:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("CREATE VIRTUAL TABLE t USING fts5(x)")
        return {"status": "pass", "detail": "FTS5 virtual table created successfully"}
    except Exception as exc:  # pragma: no cover - runtime probe
        return {"status": "fail", "detail": f"{type(exc).__name__}: {exc}"}
    finally:
        connection.close()


def detect_platform() -> dict[str, str]:
    return {
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
    }


def determine_workspace_state(paths: dict[str, Path]) -> str:
    state_dir = paths["state_dir"].exists()
    tool_path = paths["tool_path"].exists()
    db_path = paths["db_path"].exists()
    db_usable = db_path and (file_size_bytes(paths["db_path"]) or 0) > 0
    runtime_path = paths["runtime_path"].exists()
    if all((state_dir, tool_path, db_usable, runtime_path)):
        return "initialized"
    if any((state_dir, tool_path, db_path, runtime_path)):
        return "partial"
    return "missing"


def doctor(root: Path, quick: bool) -> dict[str, object]:
    paths = workspace_paths(root)
    fts5 = probe_fts5()
    pip_ok, pip_version = run_command([sys.executable, "-m", "pip", "--version"])
    pst_backend = {
        "status": "pass" if pypff is not None else "fail",
        "detail": (
            "pypff import succeeded"
            if pypff is not None
            else "Missing required PST backend import 'pypff'. Install the pinned libpff-python dependency and rerun doctor."
        ),
    }
    runtime = read_runtime(paths["runtime_path"])
    current_sha = sha256_file(paths["tool_path"])
    stored_sha = None if runtime is None else runtime.get("template_sha256")
    workspace_state = determine_workspace_state(paths)
    registry_status = None
    schema_status = None
    workspace_inventory = None
    journal_mode = None
    db_error = None

    if paths["db_path"].exists():
        try:
            connection = connect_db(paths["db_path"])
            try:
                journal_mode = current_journal_mode(connection)
                schema_status = apply_schema(connection)
                registry_status = reconcile_custom_fields_registry(connection, repair=True)
                workspace_inventory = document_inventory_counts(connection)
            finally:
                connection.close()
        except Exception as exc:
            db_error = f"{type(exc).__name__}: {exc}"

    overall = "pass"
    if fts5["status"] != "pass":
        overall = "fail"
    elif pst_backend["status"] != "pass":
        overall = "fail"
    elif db_error is not None:
        overall = "fail"
    elif workspace_state == "partial":
        overall = "partial"

    result: dict[str, object] = {
        "overall": overall,
        "tool_version": TOOL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "python_version": platform.python_version(),
        "pip_version": pip_version if pip_ok else None,
        "pip_status": "pass" if pip_ok else "fail",
        "sqlite_version": sqlite3.sqlite_version,
        "fts5": fts5,
        "pst_backend": pst_backend,
        "platform": detect_platform(),
        "workspace": {
            "root": str(root.resolve()),
            "state": workspace_state,
            "db_present": paths["db_path"].exists(),
            "db_size_bytes": file_size_bytes(paths["db_path"]),
            "runtime_present": paths["runtime_path"].exists(),
            "tool_present": paths["tool_path"].exists(),
        },
        "tool_integrity": {
            "current_sha256": current_sha,
            "runtime_sha256": stored_sha,
            "matches_runtime": current_sha == stored_sha if current_sha and stored_sha else None,
        },
    }
    if journal_mode is not None:
        result["sqlite_journal_mode"] = journal_mode
    if db_error is not None:
        result["db_error"] = db_error
    if workspace_inventory is not None:
        result["workspace_inventory"] = workspace_inventory

    if not quick:
        result["paths"] = {key: str(value) for key, value in paths.items()}
        if runtime is not None:
            result["runtime"] = runtime
        if schema_status is not None:
            result["schema_apply"] = schema_status
        if registry_status is not None:
            result["custom_field_registry"] = registry_status
    return result


def bootstrap(root: Path) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    recovered_sqlite_artifacts = remove_stale_sqlite_artifacts(paths["db_path"])
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            connection = connect_db(paths["db_path"])
            try:
                journal_mode = current_journal_mode(connection)
                apply_schema(connection)
                registry_status = reconcile_custom_fields_registry(connection, repair=True)
                tool_sha = sha256_file(paths["tool_path"])
                write_workspace_meta(connection, tool_sha)
            finally:
                connection.close()
            write_runtime(paths, sha256_file(paths["tool_path"]))
            result = {
                "status": "initialized" if paths["runtime_path"].exists() else "failed",
                "workspace_root": str(root.resolve()),
                "schema_version": SCHEMA_VERSION,
                "tool_version": TOOL_VERSION,
                "requirements_version": REQUIREMENTS_VERSION,
                "custom_field_registry": registry_status,
            }
            if journal_mode is not None:
                result["journal_mode"] = journal_mode
            if recovered_sqlite_artifacts:
                result["recovered_sqlite_artifacts"] = recovered_sqlite_artifacts
            return result
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                retry_artifacts = remove_stale_sqlite_artifacts(paths["db_path"])
                if retry_artifacts:
                    for artifact in retry_artifacts:
                        if artifact not in recovered_sqlite_artifacts:
                            recovered_sqlite_artifacts.append(artifact)
                    continue
            break
    detail = f"{type(last_error).__name__}: {last_error}" if last_error is not None else "unknown bootstrap failure"
    if recovered_sqlite_artifacts:
        detail = (
            f"{detail}. Removed stale SQLite artifacts before retry: "
            f"{', '.join(recovered_sqlite_artifacts)}"
        )
    raise RetrieverError(f"Bootstrap failed for {paths['db_path']}: {detail}") from last_error


def write_workspace_meta(connection: sqlite3.Connection, tool_sha256: str | None) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO workspace_meta (
          id, schema_version, tool_version, requirements_version,
          template_source, template_sha256, created_at, updated_at
        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          schema_version = excluded.schema_version,
          tool_version = excluded.tool_version,
          requirements_version = excluded.requirements_version,
          template_source = excluded.template_source,
          template_sha256 = excluded.template_sha256,
          updated_at = excluded.updated_at
        """,
        (
            SCHEMA_VERSION,
            TOOL_VERSION,
            REQUIREMENTS_VERSION,
            TEMPLATE_SOURCE,
            tool_sha256,
            now,
            now,
        ),
    )
    connection.commit()


def resolve_production_root_argument(workspace_root: Path, raw_production_root: str | Path) -> Path:
    candidate = Path(raw_production_root).expanduser()
    if not candidate.is_absolute():
        candidate = workspace_root / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(workspace_root.resolve())
    except ValueError as exc:
        raise RetrieverError("Production root must be inside the workspace root for Phase 4 ingest.") from exc
    return candidate


def production_row_signature(
    existing_row: sqlite3.Row | None,
    *,
    rel_path: str,
    file_name: str,
    source_kind: str,
    production_id: int,
    begin_bates: str,
    end_bates: str,
    begin_attachment: str | None,
    end_attachment: str | None,
    extracted: dict[str, object],
    source_parts: list[dict[str, object]],
) -> tuple[object, ...]:
    existing_locks = existing_row[MANUAL_FIELD_LOCKS_COLUMN] if existing_row is not None else "[]"
    return (
        rel_path,
        file_name,
        source_kind,
        production_id,
        begin_bates,
        end_bates,
        begin_attachment,
        end_attachment,
        extracted.get("title"),
        extracted.get("content_type"),
        extracted.get("text_status"),
        sha256_text(str(extracted.get("text_content") or "")),
        extracted.get("page_count"),
        tuple((part["part_kind"], part["rel_source_path"], int(part.get("ordinal", 0))) for part in source_parts),
        bool(extracted.get("preview_artifacts")),
        existing_locks,
    )


def existing_production_row_signature(connection: sqlite3.Connection, row: sqlite3.Row | None) -> tuple[object, ...] | None:
    if row is None:
        return None
    source_parts = connection.execute(
        """
        SELECT part_kind, rel_source_path, ordinal
        FROM document_source_parts
        WHERE document_id = ?
        ORDER BY part_kind ASC, ordinal ASC, id ASC
        """,
        (row["id"],),
    ).fetchall()
    return (
        row["rel_path"],
        row["file_name"],
        row["source_kind"],
        row["production_id"],
        row["begin_bates"],
        row["end_bates"],
        row["begin_attachment"],
        row["end_attachment"],
        row["title"],
        row["content_type"],
        row["text_status"],
        row["content_hash"],
        row["page_count"],
        tuple((part["part_kind"], part["rel_source_path"], int(part["ordinal"])) for part in source_parts),
        connection.execute("SELECT COUNT(*) AS count FROM document_previews WHERE document_id = ?", (row["id"],)).fetchone()["count"] > 0,
        row[MANUAL_FIELD_LOCKS_COLUMN],
    )
