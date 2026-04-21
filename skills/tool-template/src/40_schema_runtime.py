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


def backfill_child_document_kinds(connection: sqlite3.Connection) -> int:
    columns = table_columns(connection, "documents")
    required = {"parent_document_id", "child_document_kind"}
    if not required.issubset(columns):
        return 0
    cursor = connection.execute(
        """
        UPDATE documents
        SET child_document_kind = ?
        WHERE parent_document_id IS NOT NULL
          AND (child_document_kind IS NULL OR TRIM(child_document_kind) = '')
        """,
        (CHILD_DOCUMENT_KIND_ATTACHMENT,),
    )
    return int(cursor.rowcount or 0)


def backfill_conversation_assignment_modes(connection: sqlite3.Connection) -> int:
    columns = table_columns(connection, "documents")
    if "conversation_assignment_mode" not in columns:
        return 0
    cursor = connection.execute(
        """
        UPDATE documents
        SET conversation_assignment_mode = ?
        WHERE conversation_assignment_mode IS NULL
          OR TRIM(conversation_assignment_mode) = ''
        """,
        (CONVERSATION_ASSIGNMENT_MODE_AUTO,),
    )
    return int(cursor.rowcount or 0)


def backfill_custodian(connection: sqlite3.Connection) -> int:
    columns = table_columns(connection, "documents")
    required = {"custodian", "source_kind", "source_rel_path", "parent_document_id"}
    if not required.issubset(columns):
        return 0

    rows = connection.execute(
        """
        SELECT id, source_kind, source_rel_path, parent_document_id
        FROM documents
        WHERE custodian IS NULL OR TRIM(custodian) = ''
        ORDER BY CASE WHEN parent_document_id IS NULL THEN 0 ELSE 1 END ASC, id ASC
        """
    ).fetchall()
    updated = 0
    for row in rows:
        parent_custodian = None
        parent_document_id = row["parent_document_id"]
        if parent_document_id is not None:
            parent_row = connection.execute(
                "SELECT custodian FROM documents WHERE id = ?",
                (parent_document_id,),
            ).fetchone()
            if parent_row is not None:
                parent_custodian = parent_row["custodian"]
        custodian = infer_source_custodian(
            source_kind=row["source_kind"],
            source_rel_path=row["source_rel_path"],
            parent_custodian=parent_custodian,
        )
        if not custodian:
            continue
        connection.execute(
            "UPDATE documents SET custodian = ? WHERE id = ?",
            (custodian, row["id"]),
        )
        updated += 1
    return updated


def backfill_dataset_ids(connection: sqlite3.Connection, root: Path | None = None) -> int:
    if not table_exists(connection, "datasets") or not table_exists(connection, "documents"):
        return 0

    productions_by_id: dict[int, sqlite3.Row] = {}
    if table_exists(connection, "productions"):
        production_rows = connection.execute(
            """
            SELECT id, dataset_id, rel_root, production_name
            FROM productions
            ORDER BY id ASC
            """
        ).fetchall()
        for row in production_rows:
            productions_by_id[int(row["id"])] = row

    updated = 0

    if productions_by_id:
        for row in productions_by_id.values():
            if row["dataset_id"] is not None:
                continue
            dataset_id = ensure_dataset_row(
                connection,
                source_kind=PRODUCTION_SOURCE_KIND,
                dataset_locator=str(row["rel_root"]),
                dataset_name=production_dataset_name(str(row["rel_root"]), str(row["production_name"] or "")),
            )
            connection.execute(
                "UPDATE productions SET dataset_id = ? WHERE id = ?",
                (dataset_id, row["id"]),
            )
            updated += 1

    if table_exists(connection, "container_sources"):
        container_rows = connection.execute(
            """
            SELECT id, dataset_id, source_kind, source_rel_path
            FROM container_sources
            ORDER BY id ASC
            """
        ).fetchall()
        for row in container_rows:
            if row["dataset_id"] is not None:
                continue
            source_kind = str(row["source_kind"] or "")
            source_rel_path = str(row["source_rel_path"] or "")
            if not source_kind or not source_rel_path:
                continue
            dataset_name = (
                pst_dataset_name(source_rel_path)
                if source_kind == PST_SOURCE_KIND
                else mbox_dataset_name(source_rel_path)
                if source_kind == MBOX_SOURCE_KIND
                else source_rel_path
            )
            dataset_id = ensure_dataset_row(
                connection,
                source_kind=source_kind,
                dataset_locator=source_rel_path,
                dataset_name=dataset_name,
            )
            connection.execute(
                "UPDATE container_sources SET dataset_id = ? WHERE id = ?",
                (dataset_id, row["id"]),
            )
            updated += 1

    document_columns = table_columns(connection, "documents")
    required_document_columns = {"id", "dataset_id", "parent_document_id", "source_kind", "source_rel_path", "production_id"}
    if not required_document_columns.issubset(document_columns):
        return updated

    filesystem_dataset_id: int | None = None
    rows = connection.execute(
        """
        SELECT id, dataset_id, parent_document_id, source_kind, source_rel_path, production_id
        FROM documents
        WHERE dataset_id IS NULL
        ORDER BY CASE WHEN parent_document_id IS NULL THEN 0 ELSE 1 END ASC, id ASC
        """
    ).fetchall()
    for row in rows:
        dataset_id: int | None = None
        parent_document_id = row["parent_document_id"]
        if parent_document_id is not None:
            parent_row = connection.execute(
                "SELECT dataset_id FROM documents WHERE id = ?",
                (parent_document_id,),
            ).fetchone()
            if parent_row is not None and parent_row["dataset_id"] is not None:
                dataset_id = int(parent_row["dataset_id"])
        if dataset_id is None and row["production_id"] is not None:
            production_row = productions_by_id.get(int(row["production_id"]))
            if production_row is not None:
                if production_row["dataset_id"] is None:
                    dataset_id = ensure_dataset_row(
                        connection,
                        source_kind=PRODUCTION_SOURCE_KIND,
                        dataset_locator=str(production_row["rel_root"]),
                        dataset_name=production_dataset_name(
                            str(production_row["rel_root"]),
                            str(production_row["production_name"] or ""),
                        ),
                    )
                    connection.execute(
                        "UPDATE productions SET dataset_id = ? WHERE id = ?",
                        (dataset_id, production_row["id"]),
                    )
                    productions_by_id[int(production_row["id"])] = connection.execute(
                        "SELECT id, dataset_id, rel_root, production_name FROM productions WHERE id = ?",
                        (production_row["id"],),
                    ).fetchone()
                    updated += 1
                else:
                    dataset_id = int(production_row["dataset_id"])
        source_kind = normalize_whitespace(str(row["source_kind"] or "")).lower()
        source_rel_path = normalize_whitespace(str(row["source_rel_path"] or ""))
        if dataset_id is None and source_kind in {PST_SOURCE_KIND, MBOX_SOURCE_KIND} and source_rel_path:
            dataset_id = ensure_dataset_row(
                connection,
                source_kind=source_kind,
                dataset_locator=source_rel_path,
                dataset_name=(
                    pst_dataset_name(source_rel_path)
                    if source_kind == PST_SOURCE_KIND
                    else mbox_dataset_name(source_rel_path)
                ),
            )
        if dataset_id is None:
            if filesystem_dataset_id is None:
                filesystem_dataset_id = ensure_dataset_row(
                    connection,
                    source_kind=FILESYSTEM_SOURCE_KIND,
                    dataset_locator=filesystem_dataset_locator(),
                    dataset_name=filesystem_dataset_name(root),
                )
            dataset_id = filesystem_dataset_id
        connection.execute(
            "UPDATE documents SET dataset_id = ? WHERE id = ?",
            (dataset_id, row["id"]),
        )
        updated += 1

    return updated


def backfill_dataset_memberships(connection: sqlite3.Connection) -> int:
    if not table_exists(connection, "dataset_documents") or not table_exists(connection, "datasets"):
        return 0

    updated = 0
    dataset_rows = connection.execute(
        """
        SELECT id, source_kind, dataset_locator
        FROM datasets
        ORDER BY id ASC
        """
    ).fetchall()
    for row in dataset_rows:
        source_kind = normalize_whitespace(str(row["source_kind"] or "")).lower()
        source_locator = normalize_whitespace(str(row["dataset_locator"] or ""))
        if not source_kind or not source_locator or source_kind == MANUAL_DATASET_SOURCE_KIND:
            continue
        ensure_dataset_source_row(
            connection,
            dataset_id=int(row["id"]),
            source_kind=source_kind,
            source_locator=source_locator,
        )

    existing_membership_count = int(
        connection.execute("SELECT COUNT(*) AS count FROM dataset_documents").fetchone()["count"] or 0
    )
    if existing_membership_count > 0:
        return updated

    productions_by_id: dict[int, sqlite3.Row] = {}
    if table_exists(connection, "productions"):
        for row in connection.execute(
            """
            SELECT id, dataset_id, rel_root
            FROM productions
            ORDER BY id ASC
            """
        ).fetchall():
            productions_by_id[int(row["id"])] = row

    document_columns = table_columns(connection, "documents")
    required_document_columns = {"id", "dataset_id", "parent_document_id", "source_kind", "source_rel_path", "production_id"}
    if not required_document_columns.issubset(document_columns):
        return updated

    rows = connection.execute(
        """
        SELECT id, dataset_id, parent_document_id, source_kind, source_rel_path, production_id
        FROM documents
        WHERE dataset_id IS NOT NULL
        ORDER BY CASE WHEN parent_document_id IS NULL THEN 0 ELSE 1 END ASC, id ASC
        """
    ).fetchall()
    for row in rows:
        dataset_id = int(row["dataset_id"])
        document_id = int(row["id"])
        source_membership_ids: list[int] = []

        if row["parent_document_id"] is not None:
            parent_memberships = connection.execute(
                """
                SELECT dataset_source_id
                FROM dataset_documents
                WHERE dataset_id = ? AND document_id = ? AND dataset_source_id IS NOT NULL
                ORDER BY dataset_source_id ASC
                """,
                (dataset_id, int(row["parent_document_id"])),
            ).fetchall()
            source_membership_ids = [int(item["dataset_source_id"]) for item in parent_memberships]

        if not source_membership_ids and row["production_id"] is not None:
            production_row = productions_by_id.get(int(row["production_id"]))
            if production_row is not None:
                dataset_source_row = get_dataset_source_row(
                    connection,
                    source_kind=PRODUCTION_SOURCE_KIND,
                    source_locator=str(production_row["rel_root"]),
                )
                if dataset_source_row is not None and int(dataset_source_row["dataset_id"]) == dataset_id:
                    source_membership_ids.append(int(dataset_source_row["id"]))

        source_kind = normalize_whitespace(str(row["source_kind"] or "")).lower()
        source_rel_path = normalize_whitespace(str(row["source_rel_path"] or ""))
        if not source_membership_ids and source_kind in {PST_SOURCE_KIND, MBOX_SOURCE_KIND} and source_rel_path:
            dataset_source_row = get_dataset_source_row(
                connection,
                source_kind=source_kind,
                source_locator=source_rel_path,
            )
            if dataset_source_row is not None and int(dataset_source_row["dataset_id"]) == dataset_id:
                source_membership_ids.append(int(dataset_source_row["id"]))

        if not source_membership_ids and source_kind in {FILESYSTEM_SOURCE_KIND, EMAIL_ATTACHMENT_SOURCE_KIND}:
            dataset_source_row = get_dataset_source_row(
                connection,
                source_kind=FILESYSTEM_SOURCE_KIND,
                source_locator=filesystem_dataset_locator(),
            )
            if dataset_source_row is not None and int(dataset_source_row["dataset_id"]) == dataset_id:
                source_membership_ids.append(int(dataset_source_row["id"]))

        if source_membership_ids:
            for dataset_source_id in source_membership_ids:
                ensure_dataset_document_membership(
                    connection,
                    dataset_id=dataset_id,
                    document_id=document_id,
                    dataset_source_id=dataset_source_id,
                )
                updated += 1
            continue

        ensure_dataset_document_membership(
            connection,
            dataset_id=dataset_id,
            document_id=document_id,
            dataset_source_id=None,
        )
        updated += 1

    return updated


def backfill_dataset_name_normalized(connection: sqlite3.Connection) -> int:
    if not table_exists(connection, "datasets"):
        return 0
    columns = table_columns(connection, "datasets")
    if "dataset_name" not in columns or "dataset_name_normalized" not in columns:
        return 0
    rows = connection.execute(
        """
        SELECT id, dataset_name, dataset_name_normalized
        FROM datasets
        ORDER BY id ASC
        """
    ).fetchall()
    updated = 0
    for row in rows:
        dataset_name = normalized_dataset_name_or_default(str(row["dataset_name"] or ""))
        normalized_name = normalize_dataset_name_for_compare(dataset_name)
        if (
            normalize_inline_whitespace(str(row["dataset_name"] or "")) == dataset_name
            and normalize_inline_whitespace(str(row["dataset_name_normalized"] or "")) == normalized_name
        ):
            continue
        connection.execute(
            """
            UPDATE datasets
            SET dataset_name = ?, dataset_name_normalized = ?, updated_at = ?
            WHERE id = ?
            """,
            (dataset_name, normalized_name, utc_now(), int(row["id"])),
        )
        updated += 1
    return updated


def merge_dataset_identity_duplicates(connection: sqlite3.Connection) -> int:
    if not table_exists(connection, "datasets"):
        return 0
    rows = connection.execute(
        """
        SELECT id, source_kind, dataset_locator
        FROM datasets
        ORDER BY LOWER(source_kind) ASC, dataset_locator ASC, id ASC
        """
    ).fetchall()
    ids_by_identity: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in rows:
        identity = (
            normalize_whitespace(str(row["source_kind"] or "")).lower(),
            normalize_whitespace(str(row["dataset_locator"] or "")),
        )
        ids_by_identity[identity].append(int(row["id"]))

    merged = 0
    for duplicate_ids in ids_by_identity.values():
        if len(duplicate_ids) < 2:
            continue
        keep_id = min(duplicate_ids)
        for drop_id in sorted(dataset_id for dataset_id in duplicate_ids if dataset_id != keep_id):
            source_rows = connection.execute(
                """
                SELECT id, source_kind, source_locator, created_at, updated_at
                FROM dataset_sources
                WHERE dataset_id = ?
                ORDER BY id ASC
                """,
                (drop_id,),
            ).fetchall()
            source_id_map: dict[int, int] = {}
            for source_row in source_rows:
                new_source_id = ensure_dataset_source_row(
                    connection,
                    dataset_id=keep_id,
                    source_kind=str(source_row["source_kind"]),
                    source_locator=str(source_row["source_locator"]),
                )
                source_id_map[int(source_row["id"])] = new_source_id

            membership_rows = connection.execute(
                """
                SELECT id, document_id, dataset_source_id, created_at, updated_at
                FROM dataset_documents
                WHERE dataset_id = ?
                ORDER BY id ASC
                """,
                (drop_id,),
            ).fetchall()
            for membership_row in membership_rows:
                dataset_source_id = membership_row["dataset_source_id"]
                remapped_source_id = (
                    source_id_map.get(int(dataset_source_id))
                    if dataset_source_id is not None
                    else None
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO dataset_documents (
                      dataset_id, document_id, dataset_source_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        keep_id,
                        int(membership_row["document_id"]),
                        remapped_source_id,
                        membership_row["created_at"] or utc_now(),
                        membership_row["updated_at"] or utc_now(),
                    ),
                )
                connection.execute(
                    "DELETE FROM dataset_documents WHERE id = ?",
                    (int(membership_row["id"]),),
                )

            connection.execute("UPDATE documents SET dataset_id = ? WHERE dataset_id = ?", (keep_id, drop_id))
            connection.execute("UPDATE productions SET dataset_id = ? WHERE dataset_id = ?", (keep_id, drop_id))
            connection.execute("UPDATE container_sources SET dataset_id = ? WHERE dataset_id = ?", (keep_id, drop_id))
            connection.execute("DELETE FROM dataset_sources WHERE dataset_id = ?", (drop_id,))
            connection.execute("DELETE FROM datasets WHERE id = ?", (drop_id,))
            merged += 1
    return merged


def suffix_dataset_name_collisions(connection: sqlite3.Connection) -> int:
    if not table_exists(connection, "datasets"):
        return 0
    rows = connection.execute(
        """
        SELECT id, dataset_name
        FROM datasets
        ORDER BY id ASC
        """
    ).fetchall()
    renamed = 0
    used_names: set[str] = set()
    for row in rows:
        dataset_id = int(row["id"])
        base_name = normalized_dataset_name_or_default(str(row["dataset_name"] or ""))
        candidate = base_name
        suffix = 2
        normalized_candidate = normalize_dataset_name_for_compare(candidate)
        while normalized_candidate in used_names:
            candidate = f"{base_name}_{suffix}"
            normalized_candidate = normalize_dataset_name_for_compare(candidate)
            suffix += 1
        used_names.add(normalized_candidate)
        if candidate == base_name and normalize_inline_whitespace(str(row["dataset_name"] or "")) == base_name:
            connection.execute(
                """
                UPDATE datasets
                SET dataset_name_normalized = ?, updated_at = ?
                WHERE id = ?
                """,
                (normalized_candidate, utc_now(), dataset_id),
            )
            continue
        connection.execute(
            """
            UPDATE datasets
            SET dataset_name = ?, dataset_name_normalized = ?, updated_at = ?
            WHERE id = ?
            """,
            (candidate, normalized_candidate, utc_now(), dataset_id),
        )
        if candidate != base_name or normalize_inline_whitespace(str(row["dataset_name"] or "")) != base_name:
            renamed += 1
    return renamed


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


def backfill_internal_rel_path_prefix(connection: sqlite3.Connection) -> int:
    """Rewrite synthetic rel_paths that still use the legacy ``.retriever/`` prefix.

    Container-derived messages, production logical documents, and attachment
    blobs used to be stored with a leading ``.retriever/`` so that
    ``<root>/<rel_path>`` would happen to resolve to the real file under the
    state directory. That made it look like the workspace's opaque state
    directory contained indexed documents, which confused scans and searches.
    The canonical synthetic prefix is now ``_retriever/`` and path resolution
    translates that back to the state directory explicitly.
    """
    if not table_exists(connection, "documents"):
        return 0
    cursor = connection.execute(
        """
        UPDATE documents
        SET rel_path = '_retriever/' || substr(rel_path, length('.retriever/') + 1)
        WHERE rel_path LIKE '.retriever/%'
        """
    )
    return int(cursor.rowcount or 0)


def ensure_documents_fts(connection: sqlite3.Connection) -> bool:
    expected_columns = {"document_id", "file_name", "title", "subject", "author", "custodian", "participants", "recipients"}
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
          custodian,
          participants,
          recipients
        )
        """
    )
    rows = connection.execute(
        """
        SELECT id, file_name, title, subject, author, custodian, participants, recipients
        FROM documents
        """
    ).fetchall()
    if rows:
        connection.executemany(
            """
            INSERT INTO documents_fts (document_id, file_name, title, subject, author, custodian, participants, recipients)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["id"],
                    row["file_name"],
                    row["title"],
                    row["subject"],
                    row["author"],
                    row["custodian"],
                    row["participants"],
                    row["recipients"],
                )
                for row in rows
            ],
        )
    return True


def apply_schema(connection: sqlite3.Connection, root: Path | None = None) -> dict[str, object]:
    prior_schema_version: int | None = None
    if table_exists(connection, "workspace_meta"):
        workspace_meta_row = connection.execute(
            """
            SELECT schema_version
            FROM workspace_meta
            WHERE id = 1
            """
        ).fetchone()
        if workspace_meta_row is not None and workspace_meta_row["schema_version"] is not None:
            prior_schema_version = int(workspace_meta_row["schema_version"])

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
    ensure_column(connection, "documents", "custodian TEXT")
    ensure_column(connection, "documents", "participants TEXT")
    ensure_column(connection, "documents", "control_number TEXT")
    ensure_column(connection, "documents", "conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL")
    ensure_column(connection, "documents", f"conversation_assignment_mode TEXT NOT NULL DEFAULT '{CONVERSATION_ASSIGNMENT_MODE_AUTO}'")
    ensure_column(connection, "documents", "dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL")
    ensure_column(connection, "documents", "parent_document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE")
    ensure_column(connection, "documents", "child_document_kind TEXT")
    ensure_column(connection, "documents", "source_kind TEXT")
    ensure_column(connection, "documents", "source_rel_path TEXT")
    ensure_column(connection, "documents", "source_item_id TEXT")
    ensure_column(connection, "documents", "root_message_key TEXT")
    ensure_column(connection, "documents", "source_folder_path TEXT")
    ensure_column(connection, "documents", "production_id INTEGER REFERENCES productions(id) ON DELETE SET NULL")
    ensure_column(connection, "documents", "begin_bates TEXT")
    ensure_column(connection, "documents", "end_bates TEXT")
    ensure_column(connection, "documents", "begin_attachment TEXT")
    ensure_column(connection, "documents", "end_attachment TEXT")
    ensure_column(connection, "documents", "control_number_batch INTEGER")
    ensure_column(connection, "documents", "control_number_family_sequence INTEGER")
    ensure_column(connection, "documents", "control_number_attachment_sequence INTEGER")
    ensure_column(connection, "documents", "source_text_revision_id INTEGER REFERENCES text_revisions(id) ON DELETE SET NULL")
    ensure_column(connection, "documents", "active_search_text_revision_id INTEGER REFERENCES text_revisions(id) ON DELETE SET NULL")
    ensure_column(connection, "documents", "active_text_source_kind TEXT")
    ensure_column(connection, "documents", "active_text_language TEXT")
    ensure_column(connection, "documents", "active_text_quality_score REAL")
    ensure_column(connection, "document_email_threading", "message_id TEXT")
    ensure_column(connection, "document_email_threading", "in_reply_to TEXT")
    ensure_column(connection, "document_email_threading", "references_json TEXT NOT NULL DEFAULT '[]'")
    ensure_column(connection, "document_email_threading", "conversation_index TEXT")
    ensure_column(connection, "document_email_threading", "conversation_topic TEXT")
    ensure_column(connection, "document_email_threading", "normalized_subject TEXT")
    ensure_column(connection, "document_email_threading", "updated_at TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "document_chat_threading", "thread_id TEXT")
    ensure_column(connection, "document_chat_threading", "message_id TEXT")
    ensure_column(connection, "document_chat_threading", "parent_message_id TEXT")
    ensure_column(connection, "document_chat_threading", "thread_type TEXT")
    ensure_column(connection, "document_chat_threading", "participants_json TEXT NOT NULL DEFAULT '[]'")
    ensure_column(connection, "document_chat_threading", "updated_at TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "document_previews", "target_fragment TEXT")
    ensure_column(connection, "job_versions", "capability TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "runs", "activation_policy TEXT NOT NULL DEFAULT 'manual'")
    ensure_column(connection, "run_items", "result_id INTEGER REFERENCES results(id) ON DELETE SET NULL")
    ensure_column(connection, "run_items", "page_number INTEGER")
    ensure_column(connection, "run_items", "input_artifact_rel_path TEXT")
    ensure_column(connection, "run_items", "claimed_by TEXT")
    ensure_column(connection, "run_items", "claimed_at TEXT")
    ensure_column(connection, "run_items", "last_heartbeat_at TEXT")
    ensure_column(connection, "productions", "dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL")
    ensure_column(connection, "container_sources", "dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL")
    ensure_column(connection, "datasets", "dataset_name_normalized TEXT")
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
    if table_exists(connection, "job_versions") and table_exists(connection, "jobs"):
        blank_capability_rows = connection.execute(
            """
            SELECT jv.id, j.job_kind
            FROM job_versions jv
            JOIN jobs j ON j.id = jv.job_id
            WHERE jv.capability IS NULL OR TRIM(jv.capability) = ''
            """
        ).fetchall()
        for row in blank_capability_rows:
            job_kind = normalize_whitespace(str(row["job_kind"] or "")).lower()
            try:
                capability = default_job_capability_for_kind(job_kind)
            except RetrieverError:
                capability = "text_structured"
            connection.execute(
                "UPDATE job_versions SET capability = ? WHERE id = ?",
                (capability, int(row["id"])),
            )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_conversation_id ON documents(conversation_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_conversation_assignment_mode ON documents(conversation_assignment_mode)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_parent_document_id ON documents(parent_document_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_child_document_kind ON documents(child_document_kind)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_dataset_id ON documents(dataset_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_source_kind ON documents(source_kind)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_source_rel_path ON documents(source_rel_path)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_root_message_key ON documents(root_message_key)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_production_id ON documents(production_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_begin_bates ON documents(begin_bates)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_end_bates ON documents(end_bates)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_documents_source_text_revision_id ON documents(source_text_revision_id)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_documents_active_search_text_revision_id ON documents(active_search_text_revision_id)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_dataset_sources_dataset_id ON dataset_sources(dataset_id)")
    connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_sources_locator_unique ON dataset_sources(source_kind, source_locator)")
    connection.execute("DROP INDEX IF EXISTS idx_conversations_source_kind_key")
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_source_locator_key_unique
        ON conversations(source_kind, source_locator, conversation_key)
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_conversations_source_locator ON conversations(source_locator)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_dataset_documents_document_id ON dataset_documents(document_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_dataset_documents_dataset_id ON dataset_documents(dataset_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_dataset_documents_source_id ON dataset_documents(dataset_source_id)")
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_documents_membership_unique
        ON dataset_documents(dataset_id, document_id, COALESCE(dataset_source_id, 0))
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_productions_dataset_id ON productions(dataset_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_container_sources_dataset_id ON container_sources(dataset_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_container_sources_source_kind ON container_sources(source_kind)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_document_email_threading_message_id ON document_email_threading(message_id)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_email_threading_conversation_index ON document_email_threading(conversation_index)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_email_threading_conversation_topic ON document_email_threading(conversation_topic)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_email_threading_normalized_subject ON document_email_threading(normalized_subject)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_document_chat_threading_thread_id ON document_chat_threading(thread_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_document_chat_threading_message_id ON document_chat_threading(message_id)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_chat_threading_parent_message_id ON document_chat_threading(parent_message_id)"
    )
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
    connection.execute("CREATE INDEX IF NOT EXISTS idx_job_outputs_job_id ON job_outputs(job_id, ordinal)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_job_versions_job_id ON job_versions(job_id, version DESC)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_job_versions_capability ON job_versions(capability)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_runs_job_version_id ON runs(job_version_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_runs_from_run_id ON runs(from_run_id)")
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_run_snapshot_documents_run_id_ordinal
        ON run_snapshot_documents(run_id, ordinal, id)
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_run_snapshot_documents_document_id ON run_snapshot_documents(document_id)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_items_run_id_status ON run_items(run_id, status)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_items_document_id ON run_items(document_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_items_result_id ON run_items(result_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_items_page_number ON run_items(run_id, document_id, page_number)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_items_run_claim ON run_items(run_id, status, claimed_by)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_items_heartbeat ON run_items(last_heartbeat_at)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_workers_run_id ON run_workers(run_id, status)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_workers_claimed_by ON run_workers(run_id, claimed_by)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_run_workers_task_id ON run_workers(worker_task_id)")
    connection.execute("DROP INDEX IF EXISTS idx_run_items_snapshot_kind_unique")
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_run_items_snapshot_kind_unique
        ON run_items(
          run_id,
          COALESCE(run_snapshot_document_id, 0),
          item_kind,
          COALESCE(page_number, 0),
          COALESCE(segment_id, 0)
        )
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_attempts_run_item_id ON attempts(run_item_id, attempt_number)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_ocr_page_outputs_run_id_doc_page ON ocr_page_outputs(run_id, document_id, page_number)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_image_description_page_outputs_run_id_doc_page "
        "ON image_description_page_outputs(run_id, document_id, page_number)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_results_job_version_id ON results(job_version_id, created_at)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_results_document_id ON results(document_id, created_at)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_results_input_revision_id ON results(input_revision_id)")
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_results_active_identity_unique
        ON results(document_id, job_version_id, input_identity)
        WHERE retracted_at IS NULL
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_result_outputs_result_id ON result_outputs(result_id)")
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_text_revisions_document_created_at
        ON text_revisions(document_id, created_at, id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_text_revision_segments_revision_profile
        ON text_revision_segments(revision_id, segment_profile, level, ordinal)
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_vectors_active_segment_unique
        ON embedding_vectors(job_version_id, segment_id)
        WHERE retracted_at IS NULL
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_publications_document_field
        ON publications(document_id, custom_field_name, published_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_text_revision_activation_events_document_created_at
        ON text_revision_activation_events(document_id, created_at, id)
        """
    )
    merged_legacy_locks = merge_legacy_field_locks(connection)
    backfilled_content_type = backfill_content_type(connection)
    backfilled_child_document_kinds = backfill_child_document_kinds(connection)
    backfilled_conversation_assignment_modes = backfill_conversation_assignment_modes(connection)
    backfilled_source_kinds = backfill_source_kinds(connection)
    rewrote_internal_rel_path_prefix = backfill_internal_rel_path_prefix(connection)
    dataset_membership_migration_needed = prior_schema_version is None or prior_schema_version < 12
    backfilled_dataset_ids = backfill_dataset_ids(connection, root) if dataset_membership_migration_needed else 0
    backfilled_dataset_memberships = backfill_dataset_memberships(connection) if dataset_membership_migration_needed else 0
    backfilled_dataset_name_normalized = backfill_dataset_name_normalized(connection)
    merged_duplicate_dataset_identities = merge_dataset_identity_duplicates(connection)
    suffixed_dataset_name_collisions = suffix_dataset_name_collisions(connection)
    connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_datasets_source_locator_unique ON datasets(source_kind, dataset_locator)")
    connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_datasets_name_normalized_unique ON datasets(dataset_name_normalized)")
    backfilled_custodian = backfill_custodian(connection)
    backfilled_control_numbers = backfill_control_numbers(connection)
    rebuilt_control_number_batches = backfill_control_number_batches(connection)
    rebuilt_documents_fts = ensure_documents_fts(connection)
    connection.commit()
    return {
        "schema_version": SCHEMA_VERSION,
        "backfilled_content_type": backfilled_content_type,
        "backfilled_child_document_kinds": backfilled_child_document_kinds,
        "backfilled_conversation_assignment_modes": backfilled_conversation_assignment_modes,
        "backfilled_custodian": backfilled_custodian,
        "backfilled_dataset_ids": backfilled_dataset_ids,
        "backfilled_dataset_memberships": backfilled_dataset_memberships,
        "backfilled_dataset_name_normalized": backfilled_dataset_name_normalized,
        "merged_duplicate_dataset_identities": merged_duplicate_dataset_identities,
        "suffixed_dataset_name_collisions": suffixed_dataset_name_collisions,
        "backfilled_source_kinds": backfilled_source_kinds,
        "rewrote_internal_rel_path_prefix": rewrote_internal_rel_path_prefix,
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


def probe_processing_providers(connection: sqlite3.Connection | None) -> dict[str, object]:
    configured_providers: list[str] = []
    configured_capabilities: list[str] = []
    if connection is not None and table_exists(connection, "job_versions"):
        configured_providers = sorted(
            {
                normalize_whitespace(str(row["provider"] or "")).lower()
                for row in connection.execute(
                    """
                    SELECT DISTINCT provider
                    FROM job_versions
                    WHERE provider IS NOT NULL AND TRIM(provider) != ''
                    """
                ).fetchall()
                if normalize_whitespace(str(row["provider"] or ""))
            }
        )
        configured_capabilities = sorted(
            {
                normalize_whitespace(str(row["capability"] or "")).lower()
                for row in connection.execute(
                    """
                    SELECT DISTINCT capability
                    FROM job_versions
                    WHERE capability IS NOT NULL AND TRIM(capability) != ''
                    """
                ).fetchall()
                if normalize_whitespace(str(row["capability"] or ""))
            }
        )
    return {
        "configured_providers": configured_providers,
        "configured_capabilities": configured_capabilities,
        "cowork_runtime": {
            "status": "pass",
            "detail": "Cowork agent execution is the primary runtime path for processing jobs.",
        },
        "external_providers": {
            "status": "warn" if configured_providers else "pass",
            "detail": (
                "External provider identifiers are stored for future integrations, but are not required for Cowork execution."
                if configured_providers
                else "No external provider identifiers are configured."
            ),
        },
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
    processing_providers = probe_processing_providers(None)
    journal_mode = None
    db_error = None

    if paths["db_path"].exists():
        try:
            connection = connect_db(paths["db_path"])
            try:
                journal_mode = current_journal_mode(connection)
                schema_status = apply_schema(connection, root)
                registry_status = reconcile_custom_fields_registry(connection, repair=True)
                workspace_inventory = document_inventory_counts(connection)
                processing_providers = probe_processing_providers(connection)
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
        "processing_providers": processing_providers,
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
                apply_schema(connection, root)
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


# Commands that must not trigger an auto-upgrade. `schema-version` needs to
# work even when a workspace does not exist yet. `bootstrap` installs the
# runtime; running the upgrade before it would be meaningless. `doctor`
# reports on the workspace and should not mutate it. `upgrade-workspace`
# performs the upgrade explicitly. `slash` delegates to sub-commands that
# already go through the dispatcher.
AUTO_UPGRADE_EXEMPT_COMMANDS = frozenset(
    {
        "schema-version",
        "bootstrap",
        "doctor",
        "upgrade-workspace",
        "slash",
    }
)


def locate_canonical_plugin_tool(current_file: str | None = None) -> Path | None:
    """Find the plugin's canonical ``skills/tool-template/retriever_tools.py``.

    Resolution order:

    1. ``RETRIEVER_CANONICAL_TOOL_PATH`` environment variable (absolute path).
    2. Walk ancestors of ``current_file`` (defaults to ``__file__``) looking
       for ``skills/tool-template/retriever_tools.py``.

    The search always skips ``current_file`` itself, which prevents
    self-upgrades when the workspace tool happens to live in a path that
    superficially matches the canonical layout.
    """

    def _safe_resolve(path: Path) -> Path | None:
        try:
            return path.resolve()
        except OSError:
            return None

    running_path: Path | None = None
    if current_file is None:
        current_file = __file__
    if current_file:
        try:
            running_path = Path(current_file).resolve()
        except OSError:
            running_path = None

    env_path = os.environ.get("RETRIEVER_CANONICAL_TOOL_PATH")
    if env_path:
        candidate = Path(env_path).expanduser()
        if candidate.is_file():
            resolved = _safe_resolve(candidate) or candidate
            if running_path is None or resolved != running_path:
                return resolved

    if current_file:
        try:
            start = Path(current_file).resolve()
        except OSError:
            start = None
        if start is not None:
            for parent in [start.parent, *start.parents]:
                candidate = parent / "skills" / "tool-template" / "retriever_tools.py"
                if not candidate.is_file():
                    continue
                resolved = _safe_resolve(candidate)
                if resolved is None:
                    continue
                if running_path is not None and resolved == running_path:
                    continue
                return resolved
    return None


def _upgrade_backup_name(runtime_version: str | None, user_modified: bool) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    previous_version = runtime_version or "unknown"
    suffix = ".user-modified" if user_modified else ""
    return f"retriever_tools.py.{timestamp}.pre-{previous_version}{suffix}"


def upgrade_workspace_tool(
    root: Path,
    canonical_path: Path,
    *,
    force: bool = False,
    reason: str = "manual",
) -> dict[str, object]:
    """Replace the workspace tool with the canonical plugin copy.

    Writes happen via ``Path.write_bytes`` (open-with-O_TRUNC), so this
    works under Cowork sandboxes that block explicit ``unlink``/``rm``.
    """
    paths = workspace_paths(root)
    ensure_layout(paths)
    runtime = read_runtime(paths["runtime_path"])
    runtime_sha = runtime.get("template_sha256") if isinstance(runtime, dict) else None
    runtime_version = runtime.get("tool_version") if isinstance(runtime, dict) else None
    workspace_sha = sha256_file(paths["tool_path"])
    canonical_sha = sha256_file(canonical_path)

    if canonical_sha is None:
        raise RetrieverError(f"Canonical tool not readable at {canonical_path}")

    if workspace_sha == canonical_sha:
        return {
            "status": "no-op",
            "reason": "already current",
            "canonical_sha256": canonical_sha,
            "canonical_path": str(canonical_path),
            "tool_path": str(paths["tool_path"]),
            "workspace_tool_version": runtime_version,
            "canonical_tool_version": TOOL_VERSION,
        }

    user_modified = (
        workspace_sha is not None
        and runtime_sha is not None
        and workspace_sha != runtime_sha
    )
    if user_modified and not force:
        raise RetrieverError(
            "Workspace tool has been modified in place "
            f"(workspace sha {workspace_sha}, runtime sha {runtime_sha}). "
            "Re-run upgrade-workspace --force to overwrite the modified tool."
        )

    backup_path: Path | None = None
    if paths["tool_path"].exists():
        paths["backups_dir"].mkdir(parents=True, exist_ok=True)
        backup_path = paths["backups_dir"] / _upgrade_backup_name(runtime_version, user_modified)
        backup_path.write_bytes(paths["tool_path"].read_bytes())

    paths["bin_dir"].mkdir(parents=True, exist_ok=True)
    paths["tool_path"].write_bytes(canonical_path.read_bytes())

    new_sha = sha256_file(paths["tool_path"])
    write_runtime(paths, new_sha)

    # Best-effort: keep workspace_meta in sync. If the database is missing
    # or unreadable we leave it to the next bootstrap/doctor run rather
    # than failing the upgrade.
    meta_updated = False
    meta_error: str | None = None
    try:
        connection = connect_db(paths["db_path"])
        try:
            write_workspace_meta(connection, new_sha)
            meta_updated = True
        finally:
            connection.close()
    except Exception as exc:  # pragma: no cover - best-effort path
        meta_error = f"{type(exc).__name__}: {exc}"

    result: dict[str, object] = {
        "status": "upgraded",
        "reason": reason,
        "force": force,
        "was_user_modified": user_modified,
        "previous_tool_sha256": workspace_sha,
        "previous_tool_version": runtime_version,
        "new_tool_sha256": new_sha,
        "new_tool_version": TOOL_VERSION,
        "canonical_path": str(canonical_path),
        "tool_path": str(paths["tool_path"]),
        "backup_path": str(backup_path) if backup_path else None,
        "workspace_meta_updated": meta_updated,
    }
    if meta_error is not None:
        result["workspace_meta_error"] = meta_error
    return result


def maybe_upgrade_workspace_tool(root: Path) -> dict[str, object] | None:
    """Auto-upgrade the workspace tool if it is cleanly stale.

    Returns ``None`` when no action is needed or the situation is ambiguous.
    Returns a result dict when an upgrade was performed OR explicitly
    blocked because the workspace tool looks user-modified.

    Rules:

    * No ``.retriever`` directory, no runtime, or no workspace tool -> no-op.
    * Cannot locate a canonical plugin copy -> no-op (e.g., workspace
      was copied to a machine without the plugin source).
    * Workspace tool sha already matches canonical -> no-op.
    * Workspace tool sha matches ``runtime.template_sha256`` but differs
      from canonical -> clean-but-stale, upgrade in place.
    * Workspace tool sha differs from ``runtime.template_sha256`` ->
      user modified, refuse and return a block result.
    """
    paths = workspace_paths(root)
    if not paths["state_dir"].exists():
        return None
    if not paths["tool_path"].exists():
        return None
    runtime = read_runtime(paths["runtime_path"])
    if not isinstance(runtime, dict):
        return None

    canonical_path = locate_canonical_plugin_tool()
    if canonical_path is None:
        return None

    workspace_sha = sha256_file(paths["tool_path"])
    canonical_sha = sha256_file(canonical_path)
    if workspace_sha is None or canonical_sha is None:
        return None
    if workspace_sha == canonical_sha:
        return None

    recorded_sha = runtime.get("template_sha256")
    runtime_version = runtime.get("tool_version")

    if recorded_sha and workspace_sha != recorded_sha:
        return {
            "status": "blocked",
            "reason": "workspace tool has been modified in place",
            "workspace_tool_sha256": workspace_sha,
            "workspace_runtime_sha256": recorded_sha,
            "canonical_sha256": canonical_sha,
            "canonical_path": str(canonical_path),
            "workspace_tool_version": runtime_version,
            "canonical_tool_version": TOOL_VERSION,
            "hint": (
                "Run `upgrade-workspace --force` to overwrite the modified "
                "tool, or revert your local edits."
            ),
        }

    try:
        return upgrade_workspace_tool(
            root,
            canonical_path,
            force=False,
            reason="auto-stale",
        )
    except RetrieverError as exc:
        return {
            "status": "error",
            "reason": "auto-upgrade failed",
            "detail": str(exc),
            "canonical_path": str(canonical_path),
            "canonical_sha256": canonical_sha,
            "workspace_tool_sha256": workspace_sha,
            "workspace_tool_version": runtime_version,
            "canonical_tool_version": TOOL_VERSION,
        }


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
