def ingest_production(root: Path, production_root: Path | str) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection)
        reconcile_custom_fields_registry(connection, repair=True)
        resolved_production_root = resolve_production_root_argument(root, production_root)
        signature = production_signature_for_root(root, resolved_production_root)
        if signature is None:
            raise RetrieverError(f"Path does not look like a supported processed production: {resolved_production_root}")

        metadata_load_path = Path(signature["metadata_load_path"])
        image_load_path = Path(signature["image_load_path"]) if signature["image_load_path"] is not None else None
        metadata = parse_production_metadata_load(metadata_load_path)
        image_rows = parse_production_image_load(image_load_path)
        production_id = upsert_production_row(
            connection,
            rel_root=str(signature["rel_root"]),
            production_name=str(signature["production_name"]),
            metadata_load_rel_path=relative_document_path(root, metadata_load_path),
            image_load_rel_path=relative_document_path(root, image_load_path) if image_load_path is not None else None,
            source_type=str(signature["source_type"]),
        )
        connection.commit()

        existing_rows = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE production_id = ?
            """,
            (production_id,),
        ).fetchall()
        existing_by_control_number = {str(row["control_number"]): row for row in existing_rows if row["control_number"]}
        seen_control_numbers: set[str] = set()

        resolved_image_rows: list[dict[str, object]] = []
        for image_row in image_rows:
            resolved_path = resolve_production_source_path(root, resolved_production_root, image_row["image_path"])
            resolved_image_rows.append({**image_row, "resolved_path": resolved_path})

        stats: dict[str, int] = {
            "created": 0,
            "updated": 0,
            "unchanged": 0,
            "retired": 0,
            "families_reconstructed": 0,
            "page_images_linked": 0,
            "docs_missing_linked_text": 0,
            "docs_missing_linked_images": 0,
            "docs_missing_linked_natives": 0,
        }
        failures: list[dict[str, str]] = []

        for record in metadata["rows"]:
            begin_bates = str(record.get("begin_bates") or "").strip()
            end_bates = str(record.get("end_bates") or begin_bates).strip()
            if not begin_bates:
                continue
            control_number = begin_bates
            seen_control_numbers.add(control_number)
            existing_row = existing_by_control_number.get(control_number)
            connection.execute("BEGIN")
            try:
                existing_signature = existing_production_row_signature(connection, existing_row)
                text_path = resolve_production_source_path(root, resolved_production_root, record.get("text_path"))
                native_path = resolve_production_source_path(root, resolved_production_root, record.get("native_path"))
                matching_image_paths = [
                    Path(image_row["resolved_path"])
                    for image_row in resolved_image_rows
                    if image_row.get("resolved_path") is not None
                    and bates_inclusive_contains(begin_bates, end_bates, image_row["page_bates"])
                ]
                if record.get("text_path") and (text_path is None or not text_path.exists()):
                    stats["docs_missing_linked_text"] += 1
                if image_rows and not matching_image_paths:
                    stats["docs_missing_linked_images"] += 1
                if record.get("native_path") and (native_path is None or not native_path.exists()):
                    stats["docs_missing_linked_natives"] += 1

                extracted_payload = build_production_extracted_payload(
                    root,
                    production_name=str(signature["production_name"]),
                    control_number=control_number,
                    begin_bates=begin_bates,
                    end_bates=end_bates,
                    begin_attachment=record.get("begin_attachment"),
                    end_attachment=record.get("end_attachment"),
                    text_path=text_path if text_path is not None and text_path.exists() else None,
                    image_paths=matching_image_paths,
                    native_path=native_path if native_path is not None and native_path.exists() else None,
                )
                preferred_native = extracted_payload.pop("preferred_native", None)
                extracted = apply_manual_locks(existing_row, extracted_payload)
                source_parts = production_source_parts(
                    root,
                    text_path=text_path if text_path is not None and text_path.exists() else None,
                    image_paths=matching_image_paths,
                    native_path=native_path if native_path is not None and native_path.exists() else None,
                )
                rel_path = production_logical_rel_path(str(signature["rel_root"]), control_number).as_posix()
                file_name = (
                    (preferred_native.name if isinstance(preferred_native, Path) else None)
                    or (native_path.name if native_path is not None and native_path.exists() else None)
                    or f"{control_number}.production"
                )
                desired_signature = production_row_signature(
                    existing_row,
                    rel_path=rel_path,
                    file_name=file_name,
                    source_kind=PRODUCTION_SOURCE_KIND,
                    production_id=production_id,
                    begin_bates=begin_bates,
                    end_bates=end_bates,
                    begin_attachment=record.get("begin_attachment"),
                    end_attachment=record.get("end_attachment"),
                    extracted=extracted,
                    source_parts=source_parts,
                )
                if existing_row is not None:
                    cleanup_document_artifacts(paths, connection, existing_row)
                document_id = upsert_document_row(
                    connection,
                    rel_path,
                    preferred_native if isinstance(preferred_native, Path) else (text_path if text_path is not None and text_path.exists() else None),
                    existing_row,
                    extracted,
                    file_name=file_name,
                    parent_document_id=None,
                    control_number=control_number,
                    control_number_batch=None,
                    control_number_family_sequence=None,
                    control_number_attachment_sequence=None,
                    source_kind=PRODUCTION_SOURCE_KIND,
                    production_id=production_id,
                    begin_bates=begin_bates,
                    end_bates=end_bates,
                    begin_attachment=record.get("begin_attachment"),
                    end_attachment=record.get("end_attachment"),
                    file_type_override=(
                        normalize_extension(preferred_native)
                        if isinstance(preferred_native, Path)
                        else (normalize_extension(native_path) if native_path is not None else None)
                    ),
                    file_size_override=production_document_file_size(
                        text_path if text_path is not None and text_path.exists() else None,
                        matching_image_paths,
                        native_path if native_path is not None and native_path.exists() else None,
                    ),
                    file_hash_override=(
                        sha256_file(preferred_native)
                        if isinstance(preferred_native, Path)
                        else (sha256_file(text_path) if text_path is not None and text_path.exists() else None)
                    ),
                )
                preview_rows = write_preview_artifacts(paths, rel_path, list(extracted.get("preview_artifacts", [])))
                chunks = chunk_text(str(extracted.get("text_content") or ""))
                replace_document_related_rows(connection, document_id, extracted | {"file_name": file_name}, chunks, preview_rows)
                replace_document_source_parts(connection, document_id, source_parts)
                connection.commit()

                if existing_row is None:
                    stats["created"] += 1
                elif existing_row["lifecycle_status"] == "active" and existing_signature == desired_signature:
                    stats["unchanged"] += 1
                else:
                    stats["updated"] += 1
                stats["page_images_linked"] += len(matching_image_paths)
            except Exception as exc:
                connection.rollback()
                failures.append({"control_number": control_number, "error": f"{type(exc).__name__}: {exc}"})

        for row in existing_rows:
            control_number = str(row["control_number"] or "")
            if control_number and control_number in seen_control_numbers:
                continue
            connection.execute("BEGIN")
            try:
                cleanup_document_artifacts(paths, connection, row)
                delete_document_related_rows(connection, int(row["id"]))
                connection.execute(
                    """
                    UPDATE documents
                    SET lifecycle_status = 'deleted', parent_document_id = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (utc_now(), row["id"]),
                )
                connection.commit()
                stats["retired"] += 1
            except Exception:
                connection.rollback()
                raise

        connection.execute("BEGIN")
        try:
            parent_link_updates = update_production_family_relationships(connection, production_id)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        stats["families_reconstructed"] = len(
            connection.execute(
                """
                SELECT id
                FROM documents
                WHERE production_id = ?
                  AND parent_document_id IS NOT NULL
                  AND lifecycle_status != 'deleted'
                """,
                (production_id,),
            ).fetchall()
        )
        stats["parent_link_updates"] = parent_link_updates

        return {
            "status": "ok",
            "workspace_root": str(root.resolve()),
            "production_root": str(resolved_production_root),
            "production_rel_root": str(signature["rel_root"]),
            "production_name": str(signature["production_name"]),
            "production_id": production_id,
            "metadata_load_rel_path": relative_document_path(root, metadata_load_path),
            "image_load_rel_path": relative_document_path(root, image_load_path) if image_load_path is not None else None,
            "tool_version": TOOL_VERSION,
            "schema_version": SCHEMA_VERSION,
            "failures": failures,
            **stats,
        }
    finally:
        connection.close()


def ingest(root: Path, recursive: bool, raw_file_types: str | None) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    allowed_types = parse_file_types(raw_file_types)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection)
        reconcile_custom_fields_registry(connection, repair=True)

        production_signatures = find_production_root_signatures(root, recursive, connection)
        production_root_paths = [Path(signature["root"]).resolve() for signature in production_signatures]
        scanned_files = [
            path
            for path in collect_files(root, recursive, allowed_types)
            if not any(production_root == path.resolve() or production_root in path.resolve().parents for production_root in production_root_paths)
        ]
        scanned_rel_paths: set[str] = set()
        scanned_pst_source_rel_paths: set[str] = set()
        scanned_items: list[dict[str, object]] = []
        for path in scanned_files:
            rel_path = relative_document_path(root, path)
            file_type = normalize_extension(path)
            scanned_rel_paths.add(rel_path)
            if file_type == PST_SOURCE_KIND:
                scanned_pst_source_rel_paths.add(rel_path)
            scanned_items.append(
                {
                    "path": path,
                    "rel_path": rel_path,
                    "file_type": file_type,
                    "file_hash": None if file_type == PST_SOURCE_KIND else sha256_file(path),
                }
            )

        existing_rows = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE parent_document_id IS NULL
              AND COALESCE(source_kind, ?) = ?
            """
        , (FILESYSTEM_SOURCE_KIND, FILESYSTEM_SOURCE_KIND)).fetchall()
        existing_by_rel = {row["rel_path"]: row for row in existing_rows}
        unseen_existing_by_hash: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in existing_rows:
            if row["rel_path"] not in scanned_rel_paths and row["file_hash"]:
                unseen_existing_by_hash[row["file_hash"]].append(row)

        stats = {
            "new": 0,
            "updated": 0,
            "renamed": 0,
            "missing": 0,
            "skipped": 0,
            "failed": 0,
            "pst_sources_skipped": 0,
            "pst_messages_created": 0,
            "pst_messages_updated": 0,
            "pst_messages_deleted": 0,
            "pst_sources_missing": 0,
            "pst_documents_missing": 0,
        }
        failures: list[dict[str, str]] = []
        current_ingestion_batch: int | None = None

        for item in scanned_items:
            rel_path = str(item["rel_path"])
            path = item["path"]
            file_type = str(item["file_type"])
            file_hash = item["file_hash"]
            if file_type == PST_SOURCE_KIND:
                try:
                    pst_result = ingest_pst_source(connection, paths, path, rel_path)
                    stats[str(pst_result["action"])] += 1
                    stats["pst_sources_skipped"] += int(pst_result["pst_sources_skipped"])
                    stats["pst_messages_created"] += int(pst_result["pst_messages_created"])
                    stats["pst_messages_updated"] += int(pst_result["pst_messages_updated"])
                    stats["pst_messages_deleted"] += int(pst_result["pst_messages_deleted"])
                    continue
                except Exception as exc:
                    stats["failed"] += 1
                    failures.append({"rel_path": rel_path, "error": f"{type(exc).__name__}: {exc}"})
                    continue
            existing_row = existing_by_rel.get(rel_path)
            action = "new"
            if existing_row is not None:
                if existing_row["file_hash"] == file_hash and existing_row["lifecycle_status"] == "active":
                    connection.execute("BEGIN")
                    try:
                        mark_seen_without_reingest(connection, existing_row)
                        connection.commit()
                        stats["skipped"] += 1
                        continue
                    except Exception:
                        connection.rollback()
                        raise
                action = "updated"
            else:
                rename_candidates = unseen_existing_by_hash.get(file_hash) or []
                if rename_candidates:
                    existing_row = rename_candidates.pop(0)
                    action = "renamed"

            connection.execute("BEGIN")
            try:
                extracted_payload = extract_document(path, include_attachments=True)
                attachments = list(extracted_payload.get("attachments", []))
                extracted_payload.pop("attachments", None)
                extracted = apply_manual_locks(existing_row, extracted_payload)
                if existing_row is None:
                    if current_ingestion_batch is None:
                        current_ingestion_batch = allocate_ingestion_batch_number(connection)
                    control_number_batch = current_ingestion_batch
                    control_number_family_sequence = reserve_control_number_family_sequence(connection, control_number_batch)
                    control_number = format_control_number(control_number_batch, control_number_family_sequence)
                    control_number_attachment_sequence = None
                else:
                    control_number_batch = int(existing_row["control_number_batch"])
                    control_number_family_sequence = int(existing_row["control_number_family_sequence"])
                    control_number = str(existing_row["control_number"])
                    control_number_attachment_sequence = existing_row["control_number_attachment_sequence"]
                    cleanup_document_artifacts(paths, connection, existing_row)
                document_id = upsert_document_row(
                    connection,
                    rel_path,
                    path,
                    existing_row,
                    extracted,
                    file_name=path.name,
                    parent_document_id=None,
                    control_number=control_number,
                    control_number_batch=control_number_batch,
                    control_number_family_sequence=control_number_family_sequence,
                    control_number_attachment_sequence=control_number_attachment_sequence,
                )
                preview_rows = write_preview_artifacts(paths, rel_path, list(extracted.get("preview_artifacts", [])))
                chunks = chunk_text(str(extracted.get("text_content") or ""))
                replace_document_related_rows(connection, document_id, extracted | {"file_name": path.name}, chunks, preview_rows)
                reconcile_attachment_documents(
                    connection,
                    paths,
                    document_id,
                    rel_path,
                    control_number_batch,
                    control_number_family_sequence,
                    attachments,
                )
                connection.commit()
                stats[action] += 1
            except Exception as exc:
                connection.rollback()
                stats["failed"] += 1
                failures.append({"rel_path": rel_path, "error": f"{type(exc).__name__}: {exc}"})

        filesystem_missing = mark_missing_documents(connection, scanned_rel_paths)
        pst_sources_missing = 0
        pst_documents_missing = 0
        if allowed_types is None or PST_SOURCE_KIND in allowed_types:
            pst_sources_missing, pst_documents_missing = mark_missing_pst_documents(connection, scanned_pst_source_rel_paths)
        stats["pst_sources_missing"] = pst_sources_missing
        stats["pst_documents_missing"] = pst_documents_missing
        stats["missing"] = filesystem_missing + pst_sources_missing
        workspace_inventory = document_inventory_counts(connection)
        result = dict(stats)
        result["failures"] = failures
        result["scanned"] = len(scanned_items)
        result["scanned_files"] = len(scanned_items)
        result["skipped_production_roots"] = [str(signature["rel_root"]) for signature in production_signatures]
        if production_signatures:
            result["warnings"] = [
                f"Detected processed production root at {signature['rel_root']}; use ingest-production instead."
                for signature in production_signatures
            ]
        result["workspace_parent_documents"] = workspace_inventory["parent_documents"]
        result["workspace_missing_parent_documents"] = workspace_inventory["missing_parent_documents"]
        result["workspace_attachment_children"] = workspace_inventory["attachment_children"]
        result["workspace_documents_total"] = workspace_inventory["documents_total"]
        return result
    finally:
        connection.close()


def value_from_type(field_type: str, value: str | None) -> object:
    if value is None:
        return None
    if field_type == "integer":
        try:
            return int(value)
        except ValueError as exc:
            raise RetrieverError(f"Expected integer value, got {value!r}") from exc
    if field_type == "real":
        try:
            return float(value)
        except ValueError as exc:
            raise RetrieverError(f"Expected real value, got {value!r}") from exc
    if field_type == "boolean":
        lowered = value.strip().lower()
        truthy = {"1", "true", "yes", "y", "on"}
        falsy = {"0", "false", "no", "n", "off"}
        if lowered in truthy:
            return 1
        if lowered in falsy:
            return 0
        raise RetrieverError(f"Expected boolean value, got {value!r}")
    return value


def resolve_field_definition(connection: sqlite3.Connection, field_name: str) -> dict[str, str]:
    if field_name in BUILTIN_FIELD_TYPES:
        return {"field_name": field_name, "field_type": BUILTIN_FIELD_TYPES[field_name], "source": "builtin"}
    if field_name in VIRTUAL_FILTER_FIELD_TYPES:
        return {"field_name": field_name, "field_type": VIRTUAL_FILTER_FIELD_TYPES[field_name], "source": "virtual"}

    row = connection.execute(
        """
        SELECT field_name, field_type
        FROM custom_fields_registry
        WHERE field_name = ?
        """,
        (field_name,),
    ).fetchone()
    columns = table_columns(connection, "documents")
    if row is not None and row["field_name"] in columns:
        return {"field_name": row["field_name"], "field_type": row["field_type"], "source": "custom"}
    if field_name in columns:
        sqlite_type = next(
            (info["type"] for info in table_info(connection, "documents") if info["name"] == field_name),
            "",
        )
        return {"field_name": field_name, "field_type": infer_registry_field_type(sqlite_type), "source": "column"}
    raise RetrieverError(f"Unknown field: {field_name}")


def lock_field(connection: sqlite3.Connection, document_id: int, field_name: str) -> list[str]:
    row = connection.execute(
        f"SELECT {MANUAL_FIELD_LOCKS_COLUMN} FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown document id: {document_id}")
    locks = normalize_string_list(row[MANUAL_FIELD_LOCKS_COLUMN])
    if field_name not in locks:
        locks.append(field_name)
        connection.execute(
            f"UPDATE documents SET {MANUAL_FIELD_LOCKS_COLUMN} = ?, updated_at = ? WHERE id = ?",
            (json.dumps(locks), utc_now(), document_id),
        )
    return locks


def add_field(root: Path, raw_field_name: str, field_type: str, instruction: str | None) -> dict[str, object]:
    normalized_field_name = sanitize_field_name(raw_field_name)
    normalized_field_type = field_type.strip().lower()
    if normalized_field_type not in REGISTRY_FIELD_TYPES:
        raise RetrieverError(f"Unsupported field type: {field_type}")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection)
        reconcile_custom_fields_registry(connection, repair=True)
        columns = table_columns(connection, "documents")
        sql_type = REGISTRY_FIELD_TYPES[normalized_field_type]
        if normalized_field_name not in columns:
            connection.execute(
                f"ALTER TABLE documents ADD COLUMN {quote_identifier(normalized_field_name)} {sql_type}"
            )
        connection.execute(
            """
            INSERT INTO custom_fields_registry (field_name, field_type, instruction, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(field_name) DO UPDATE SET
              field_type = excluded.field_type,
              instruction = excluded.instruction
            """,
            (normalized_field_name, normalized_field_type, instruction, utc_now()),
        )
        connection.commit()
        registry_status = reconcile_custom_fields_registry(connection, repair=True)
        return {
            "status": "ok",
            "field_name": normalized_field_name,
            "field_type": normalized_field_type,
            "instruction": instruction,
            "custom_field_registry": registry_status,
        }
    finally:
        connection.close()


def set_field(root: Path, document_id: int, field_name: str, value: str | None) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection)
        field_def = resolve_field_definition(connection, field_name)
        column_name = field_def["field_name"]
        if column_name in SYSTEM_MANAGED_FIELDS:
            raise RetrieverError(f"Field '{column_name}' is system-managed and cannot be manually set.")
        typed_value = value_from_type(field_def["field_type"], value)
        connection.execute("BEGIN")
        try:
            row = connection.execute("SELECT id FROM documents WHERE id = ?", (document_id,)).fetchone()
            if row is None:
                raise RetrieverError(f"Unknown document id: {document_id}")
            connection.execute(
                f"UPDATE documents SET {quote_identifier(column_name)} = ?, updated_at = ? WHERE id = ?",
                (typed_value, utc_now(), document_id),
            )
            if column_name in {"author", "participants", "recipients", "subject", "title"}:
                refresh_documents_fts_row(connection, document_id)
            locks = lock_field(connection, document_id, column_name)
            connection.commit()
            return {
                "status": "ok",
                "document_id": document_id,
                "field_name": column_name,
                "field_type": field_def["field_type"],
                "value": typed_value,
                "manual_field_locks": locks,
            }
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()

