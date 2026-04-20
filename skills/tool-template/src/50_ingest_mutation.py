def ingest_production(root: Path, production_root: Path | str) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        resolved_production_root = resolve_production_root_argument(root, production_root)
        signature = production_signature_for_root(root, resolved_production_root)
        if signature is None:
            raise RetrieverError(f"Path does not look like a supported processed production: {resolved_production_root}")

        metadata_load_path = Path(signature["metadata_load_path"])
        image_load_path = Path(signature["image_load_path"]) if signature["image_load_path"] is not None else None
        metadata = parse_production_metadata_load(metadata_load_path)
        image_rows = parse_production_image_load(image_load_path)
        dataset_id, dataset_source_id = ensure_source_backed_dataset(
            connection,
            source_kind=PRODUCTION_SOURCE_KIND,
            source_locator=str(signature["rel_root"]),
            dataset_name=production_dataset_name(str(signature["rel_root"]), str(signature["production_name"])),
        )
        production_id = upsert_production_row(
            connection,
            dataset_id=dataset_id,
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
                    dataset_id=dataset_id,
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
                seed_source_text_revision_for_document(
                    connection,
                    paths,
                    document_id=document_id,
                    extracted=extracted,
                    existing_row=existing_row,
                )
                ensure_dataset_document_membership(
                    connection,
                    dataset_id=dataset_id,
                    document_id=document_id,
                    dataset_source_id=dataset_source_id,
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
            attachment_preview_updates = 0
            preview_document_rows = connection.execute(
                """
                SELECT DISTINCT documents.id
                FROM documents
                JOIN document_previews ON document_previews.document_id = documents.id
                WHERE documents.production_id = ?
                  AND documents.lifecycle_status != 'deleted'
                  AND document_previews.preview_type = 'html'
                ORDER BY documents.id ASC
                """,
                (production_id,),
            ).fetchall()
            for preview_document_row in preview_document_rows:
                attachment_preview_updates += sync_document_attachment_preview_links(
                    connection,
                    paths,
                    int(preview_document_row["id"]),
                )
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
        stats["attachment_preview_updates"] = attachment_preview_updates

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
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        filesystem_dataset_id: int | None = None
        filesystem_dataset_source_id: int | None = None

        def ensure_filesystem_dataset() -> tuple[int, int]:
            nonlocal filesystem_dataset_id, filesystem_dataset_source_id
            if filesystem_dataset_id is None or filesystem_dataset_source_id is None:
                filesystem_dataset_id, filesystem_dataset_source_id = ensure_source_backed_dataset(
                    connection,
                    source_kind=FILESYSTEM_SOURCE_KIND,
                    source_locator=filesystem_dataset_locator(),
                    dataset_name=filesystem_dataset_name(root),
                )
                connection.commit()
            return filesystem_dataset_id, filesystem_dataset_source_id

        production_signatures = find_production_root_signatures(root, recursive, connection)
        production_root_paths = [Path(signature["root"]).resolve() for signature in production_signatures]
        scanned_files = [
            path
            for path in collect_files(root, recursive, allowed_types)
            if not any(production_root == path.resolve() or production_root in path.resolve().parents for production_root in production_root_paths)
        ]
        scanned_rel_paths: set[str] = set()
        scanned_pst_source_rel_paths: set[str] = set()
        scanned_mbox_source_rel_paths: set[str] = set()
        scanned_items: list[dict[str, object]] = []
        for path in scanned_files:
            rel_path = relative_document_path(root, path)
            file_type = normalize_extension(path)
            scanned_rel_paths.add(rel_path)
            if file_type == PST_SOURCE_KIND:
                scanned_pst_source_rel_paths.add(rel_path)
            if file_type == MBOX_SOURCE_KIND:
                scanned_mbox_source_rel_paths.add(rel_path)
            scanned_items.append(
                {
                    "path": path,
                    "rel_path": rel_path,
                    "file_type": file_type,
                    "file_hash": None if file_type in {PST_SOURCE_KIND, MBOX_SOURCE_KIND} else sha256_file(path),
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
            "mbox_sources_skipped": 0,
            "mbox_messages_created": 0,
            "mbox_messages_updated": 0,
            "mbox_messages_deleted": 0,
            "mbox_sources_missing": 0,
            "mbox_documents_missing": 0,
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
            if file_type == MBOX_SOURCE_KIND:
                try:
                    mbox_result = ingest_mbox_source(connection, paths, path, rel_path)
                    stats[str(mbox_result["action"])] += 1
                    stats["mbox_sources_skipped"] += int(mbox_result["mbox_sources_skipped"])
                    stats["mbox_messages_created"] += int(mbox_result["mbox_messages_created"])
                    stats["mbox_messages_updated"] += int(mbox_result["mbox_messages_updated"])
                    stats["mbox_messages_deleted"] += int(mbox_result["mbox_messages_deleted"])
                    continue
                except Exception as exc:
                    stats["failed"] += 1
                    failures.append({"rel_path": rel_path, "error": f"{type(exc).__name__}: {exc}"})
                    continue
            existing_row = existing_by_rel.get(rel_path)
            action = "new"
            if existing_row is not None:
                if (
                    existing_row["file_hash"] == file_hash
                    and existing_row["lifecycle_status"] == "active"
                    and document_row_has_seeded_text_revisions(existing_row)
                ):
                    filesystem_dataset_id, filesystem_dataset_source_id = ensure_filesystem_dataset()
                    connection.execute("BEGIN")
                    try:
                        mark_seen_without_reingest(
                            connection,
                            existing_row,
                            dataset_id=filesystem_dataset_id,
                            dataset_source_id=filesystem_dataset_source_id,
                        )
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

            filesystem_dataset_id, filesystem_dataset_source_id = ensure_filesystem_dataset()
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
                    dataset_id=filesystem_dataset_id,
                    control_number_batch=control_number_batch,
                    control_number_family_sequence=control_number_family_sequence,
                    control_number_attachment_sequence=control_number_attachment_sequence,
                )
                seed_source_text_revision_for_document(
                    connection,
                    paths,
                    document_id=document_id,
                    extracted=extracted,
                    existing_row=existing_row,
                )
                ensure_dataset_document_membership(
                    connection,
                    dataset_id=filesystem_dataset_id,
                    document_id=document_id,
                    dataset_source_id=filesystem_dataset_source_id,
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
                    [(filesystem_dataset_id, filesystem_dataset_source_id)],
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
        mbox_sources_missing = 0
        mbox_documents_missing = 0
        if allowed_types is None or PST_SOURCE_KIND in allowed_types:
            pst_sources_missing, pst_documents_missing = mark_missing_pst_documents(connection, scanned_pst_source_rel_paths)
        if allowed_types is None or MBOX_SOURCE_KIND in allowed_types:
            mbox_sources_missing, mbox_documents_missing = mark_missing_mbox_documents(connection, scanned_mbox_source_rel_paths)
        stats["pst_sources_missing"] = pst_sources_missing
        stats["pst_documents_missing"] = pst_documents_missing
        stats["mbox_sources_missing"] = mbox_sources_missing
        stats["mbox_documents_missing"] = mbox_documents_missing
        stats["missing"] = filesystem_missing + pst_sources_missing + mbox_sources_missing
        connection.execute("BEGIN")
        try:
            pruned_unused_filesystem_dataset = prune_unused_filesystem_dataset(connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        workspace_inventory = document_inventory_counts(connection)
        result = dict(stats)
        result["failures"] = failures
        result["scanned"] = len(scanned_items)
        result["scanned_files"] = len(scanned_items)
        result["pruned_unused_filesystem_dataset"] = int(pruned_unused_filesystem_dataset)
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
    if field_type == "date":
        normalized = normalize_date_field_value(value)
        if normalized is None:
            raise RetrieverError(f"Expected ISO date value, got {value!r}")
        return normalized
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
    field_name = FIELD_NAME_ALIASES.get(field_name, field_name)
    if field_name in BUILTIN_FIELD_TYPES:
        return {
            "field_name": field_name,
            "field_type": BUILTIN_FIELD_TYPES[field_name],
            "source": "builtin",
            "displayable": "true",
        }
    if field_name in VIRTUAL_FILTER_FIELD_TYPES:
        return {
            "field_name": field_name,
            "field_type": VIRTUAL_FILTER_FIELD_TYPES[field_name],
            "source": "virtual",
            "displayable": "true" if field_name in DISPLAYABLE_VIRTUAL_FIELDS else "false",
        }

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
        return {
            "field_name": row["field_name"],
            "field_type": row["field_type"],
            "source": "custom",
            "displayable": "true",
        }
    if field_name in columns:
        sqlite_type = next(
            (info["type"] for info in table_info(connection, "documents") if info["name"] == field_name),
            "",
        )
        return {
            "field_name": field_name,
            "field_type": infer_registry_field_type(sqlite_type),
            "source": "column",
            "displayable": "true",
        }
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


def list_datasets(root: Path) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        return {
            "status": "ok",
            "datasets": list_dataset_summaries(connection),
        }
    finally:
        connection.close()


def create_dataset(root: Path, dataset_name: str) -> dict[str, object]:
    normalized_name = normalize_whitespace(dataset_name)
    if not normalized_name:
        raise RetrieverError("Dataset name cannot be empty.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        existing_rows = find_dataset_rows_by_name(connection, normalized_name)
        if existing_rows:
            ids = ", ".join(str(row["id"]) for row in existing_rows)
            raise RetrieverError(
                f"Dataset name {normalized_name!r} already exists; use the existing dataset id(s): {ids}."
            )
        connection.execute("BEGIN")
        try:
            dataset_id = create_dataset_row(connection, normalized_name)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "dataset": dataset_summary_by_id(connection, dataset_id),
        }
    finally:
        connection.close()


def add_to_dataset(
    root: Path,
    document_ids: list[int],
    *,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        dataset_row = resolve_dataset_row(connection, dataset_id=dataset_id, dataset_name=dataset_name)
        connection.execute("BEGIN")
        try:
            result = add_documents_to_dataset(
                connection,
                dataset_id=int(dataset_row["id"]),
                document_ids=document_ids,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "requested_document_ids": sorted(dict.fromkeys(int(document_id) for document_id in document_ids)),
            **result,
            "dataset": dataset_summary_by_id(connection, int(dataset_row["id"])),
        }
    finally:
        connection.close()


def remove_from_dataset(
    root: Path,
    document_ids: list[int],
    *,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        dataset_row = resolve_dataset_row(connection, dataset_id=dataset_id, dataset_name=dataset_name)
        connection.execute("BEGIN")
        try:
            result = remove_documents_from_dataset(
                connection,
                dataset_id=int(dataset_row["id"]),
                document_ids=document_ids,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "requested_document_ids": sorted(dict.fromkeys(int(document_id) for document_id in document_ids)),
            **result,
            "dataset": dataset_summary_by_id(connection, int(dataset_row["id"])),
        }
    finally:
        connection.close()


def delete_dataset(
    root: Path,
    *,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        dataset_row = resolve_dataset_row(connection, dataset_id=dataset_id, dataset_name=dataset_name)
        connection.execute("BEGIN")
        try:
            result = delete_dataset_row(connection, int(dataset_row["id"]))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            **result,
        }
    finally:
        connection.close()


def rename_dataset(root: Path, old_name: str, new_name: str) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        dataset_row = resolve_dataset_row(connection, dataset_name=old_name)
        connection.execute("BEGIN")
        try:
            renamed_summary = rename_dataset_row(connection, int(dataset_row["id"]), new_name)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "dataset": renamed_summary,
        }
    finally:
        connection.close()


def list_runs(root: Path) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        return {
            "status": "ok",
            "runs": list_run_summaries(connection),
        }
    finally:
        connection.close()


def get_run(root: Path, run_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        return {
            "status": "ok",
            "run": run_summary_by_id(connection, run_id),
        }
    finally:
        connection.close()


def create_run(
    root: Path,
    *,
    job_version_id: int | None = None,
    raw_job_name: str | None = None,
    job_version_number: int | None = None,
    dataset_ids: list[int] | None = None,
    dataset_names: list[str] | None = None,
    document_ids: list[int] | None = None,
    control_numbers: list[str] | None = None,
    query: str | None = None,
    raw_filters: list[list[str]] | None = None,
    from_run_id: int | None = None,
    exclude_dataset_ids: list[int] | None = None,
    exclude_dataset_names: list[str] | None = None,
    exclude_document_ids: list[int] | None = None,
    exclude_control_numbers: list[str] | None = None,
    exclude_query: str | None = None,
    exclude_filters: list[list[str]] | None = None,
    family_mode: str = "exact",
    seed_limit: int | None = None,
) -> dict[str, object]:
    normalized_job_name = (
        sanitize_processing_identifier(raw_job_name, label="Job name", prefix="job")
        if raw_job_name is not None
        else None
    )
    normalized_family_mode = normalize_run_family_mode(family_mode)
    if seed_limit is not None and seed_limit < 1:
        raise RetrieverError("Run limit must be >= 1.")

    selector = normalize_run_selector_spec(
        dataset_ids=dataset_ids,
        dataset_names=dataset_names,
        document_ids=document_ids,
        control_numbers=control_numbers,
        query=query,
        raw_filters=raw_filters,
        from_run_id=from_run_id,
    )
    exclude_selector = normalize_run_selector_spec(
        dataset_ids=exclude_dataset_ids,
        dataset_names=exclude_dataset_names,
        document_ids=exclude_document_ids,
        control_numbers=exclude_control_numbers,
        query=exclude_query,
        raw_filters=exclude_filters,
        from_run_id=None,
    )
    if not selector_has_inputs(selector):
        raise RetrieverError("Run selector must include at least one inclusion input.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        job_version_row = require_job_version_row(
            connection,
            job_version_id=job_version_id,
            job_name=normalized_job_name,
            version=job_version_number,
        )
        job_row = connection.execute(
            "SELECT * FROM jobs WHERE id = ?",
            (job_version_row["job_id"],),
        ).fetchone()
        assert job_row is not None
        snapshot_rows = plan_run_snapshot_rows(
            connection,
            root=root,
            job_row=job_row,
            job_version_row=job_version_row,
            selector=selector,
            exclude_selector=exclude_selector,
            family_mode=normalized_family_mode,
            seed_limit=seed_limit,
        )
        connection.execute("BEGIN")
        try:
            run_id = create_run_row(
                connection,
                job_version_id=int(job_version_row["id"]),
                selector=selector,
                exclude_selector=exclude_selector,
                family_mode=normalized_family_mode,
                seed_limit=seed_limit,
                from_run_id=from_run_id,
                status="planned",
            )
            replace_run_snapshot_documents(
                connection,
                run_id=run_id,
                snapshot_rows=snapshot_rows,
            )
            if normalize_job_kind(str(job_row["job_kind"])) == "ocr":
                materialize_run_items_for_run(connection, paths, root, run_id)
            refresh_run_progress(connection, run_id)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "run": run_summary_by_id(connection, run_id),
        }
    finally:
        connection.close()


def list_text_revisions(root: Path, *, document_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        document_row = connection.execute(
            """
            SELECT id
            FROM documents
            WHERE id = ?
            """,
            (document_id,),
        ).fetchone()
        if document_row is None:
            raise RetrieverError(f"Unknown document id: {document_id}")
        return {
            "status": "ok",
            "document_id": int(document_id),
            "text_revisions": list_text_revision_summaries_for_document(connection, int(document_id)),
        }
    finally:
        connection.close()


def activate_text_revision(
    root: Path,
    *,
    document_id: int,
    text_revision_id: int,
    activation_policy: str = "manual",
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN")
        try:
            payload = activate_text_revision_for_document(
                connection,
                paths,
                document_id=document_id,
                text_revision_id=text_revision_id,
                activation_policy=activation_policy,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def list_results(
    root: Path,
    *,
    run_id: int | None = None,
    document_id: int | None = None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        return {
            "status": "ok",
            "results": list_result_summaries(connection, run_id=run_id, document_id=document_id),
        }
    finally:
        connection.close()


def execute_run(root: Path, *, run_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        payload = asyncio.run(execute_run_async(connection, paths, run_id=run_id))
        return payload
    finally:
        connection.close()


def claim_run_items(
    root: Path,
    *,
    run_id: int,
    claimed_by: str,
    limit: int = DEFAULT_RUN_ITEM_CLAIM_BATCH_SIZE,
    stale_after_seconds: int = DEFAULT_RUN_ITEM_CLAIM_STALE_SECONDS,
    launch_mode: str = "inline",
    worker_task_id: str | None = None,
    max_batches: int | None = None,
) -> dict[str, object]:
    if limit < 1:
        raise RetrieverError("Claim limit must be >= 1.")
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN IMMEDIATE")
        try:
            materialize_run_items_for_run(connection, paths, root, run_id)
            ensure_run_worker_row(
                connection,
                run_id=run_id,
                claimed_by=claimed_by,
                launch_mode=launch_mode,
                worker_task_id=worker_task_id,
                max_batches=max_batches,
            )
            reused_count = reuse_active_results_for_run(connection, run_id)
            claimed_rows = claim_run_item_rows(
                connection,
                run_id=run_id,
                claimed_by=claimed_by,
                limit=limit,
                stale_after_seconds=stale_after_seconds,
            )
            if claimed_rows:
                update_run_worker_row(
                    connection,
                    run_id=run_id,
                    claimed_by=normalize_whitespace(claimed_by),
                    heartbeat=True,
                    increment_batches_prepared=True,
                )
            refresh_run_progress(connection, run_id)
            payload = {
                "status": "ok",
                "run": run_status_by_id(connection, run_id),
                "claimed_by": normalize_whitespace(claimed_by),
                "reused_count": reused_count,
                "run_items": [run_item_row_to_payload(row) for row in claimed_rows],
            }
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def prepare_run_batch(
    root: Path,
    *,
    run_id: int,
    claimed_by: str,
    limit: int | None = None,
    stale_after_seconds: int = DEFAULT_RUN_ITEM_CLAIM_STALE_SECONDS,
    launch_mode: str = "inline",
    worker_task_id: str | None = None,
    max_batches: int | None = None,
) -> dict[str, object]:
    if limit is not None and limit < 1:
        raise RetrieverError("Claim limit must be >= 1.")
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN IMMEDIATE")
        try:
            materialize_run_items_for_run(connection, paths, root, run_id)
            ensure_run_worker_row(
                connection,
                run_id=run_id,
                claimed_by=claimed_by,
                launch_mode=launch_mode,
                worker_task_id=worker_task_id,
                max_batches=max_batches,
            )
            reused_count = reuse_active_results_for_run(connection, run_id)
            initial_run_payload = run_status_by_id(connection, run_id)
            initial_worker_payload = build_run_worker_payload(
                connection,
                run_id,
                run_payload=initial_run_payload,
                claimed_by=claimed_by,
            )
            effective_limit = limit if limit is not None else int(initial_worker_payload["recommended_batch_size"])
            claimed_rows: list[sqlite3.Row] = []
            batch_payloads: list[dict[str, object]] = []

            if initial_worker_payload["next_action"] == "claim":
                claimed_rows = claim_run_item_rows(
                    connection,
                    run_id=run_id,
                    claimed_by=claimed_by,
                    limit=effective_limit,
                    stale_after_seconds=stale_after_seconds,
                )
                batch_payloads = [
                    {
                        "run_item": run_item_row_to_payload(row),
                        "context": build_run_item_context_payload(connection, paths, root, row),
                    }
                    for row in claimed_rows
                ]
                if batch_payloads:
                    update_run_worker_row(
                        connection,
                        run_id=run_id,
                        claimed_by=normalize_whitespace(claimed_by),
                        heartbeat=True,
                        increment_batches_prepared=True,
                    )

            current_run_payload = run_status_by_id(connection, run_id)
            worker_payload = build_run_worker_payload(
                connection,
                run_id,
                run_payload=current_run_payload,
                claimed_by=claimed_by,
            )
            if batch_payloads:
                worker_payload["next_action"] = "process_batch"
                worker_payload["stop_reason"] = None
            elif worker_payload["next_action"] == "claim":
                worker_payload["next_action"] = "stop"
                worker_payload["stop_reason"] = "no_claimable_items"
            worker_payload["prepared_batch_size"] = len(batch_payloads)

            payload = {
                "status": "ok",
                "run": current_run_payload,
                "worker": worker_payload,
                "claimed_by": normalize_whitespace(claimed_by),
                "requested_limit": effective_limit,
                "reused_count": reused_count,
                "batch": batch_payloads,
                "worker_record": run_worker_row_to_payload(
                    find_run_worker_row(connection, run_id=run_id, claimed_by=normalize_whitespace(claimed_by))
                ),
            }
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def get_run_item_context(root: Path, *, run_item_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        run_item_row = require_run_item_row_by_id(connection, run_item_id)
        return {
            "status": "ok",
            "context": build_run_item_context_payload(connection, paths, root, run_item_row),
        }
    finally:
        connection.close()


def heartbeat_run_items(
    root: Path,
    *,
    run_id: int,
    claimed_by: str,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN")
        try:
            updated_count = heartbeat_claimed_run_items(connection, run_id=run_id, claimed_by=claimed_by)
            payload = {
                "status": "ok",
                "updated_count": updated_count,
                "run": run_status_by_id(connection, run_id),
            }
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def complete_run_item(
    root: Path,
    *,
    run_item_id: int,
    claimed_by: str,
    page_text: str | None = None,
    raw_output_json: str | None = None,
    normalized_output_json: str | None = None,
    output_values_json: str | None = None,
    created_text_revision_json: str | None = None,
    provider_metadata_json: str | None = None,
    provider_request_id: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cost_cents: int | None = None,
    latency_ms: int | None = None,
) -> dict[str, object]:
    normalized_claimed_by = normalize_whitespace(claimed_by)
    if not normalized_claimed_by:
        raise RetrieverError("claimed_by cannot be empty.")
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN")
        try:
            run_item_row = require_run_item_row_by_id(connection, run_item_id)
            if str(run_item_row["status"] or "") == "completed":
                result_payload = (
                    result_summary_by_id(connection, int(run_item_row["result_id"]))
                    if run_item_row["result_id"] is not None
                    else None
                )
                ocr_page_output_payload = None
                image_description_page_output_payload = None
                if str(run_item_row["item_kind"] or "") == "page":
                    run_row = require_run_row_by_id(connection, int(run_item_row["run_id"]))
                    job_version_row = require_job_version_row_by_id(connection, int(run_row["job_version_id"]))
                    job_row = connection.execute(
                        "SELECT * FROM jobs WHERE id = ?",
                        (job_version_row["job_id"],),
                    ).fetchone()
                    assert job_row is not None
                    page_job_kind = normalize_job_kind(str(job_row["job_kind"]))
                    if page_job_kind == "ocr":
                        existing_page_output_row = find_ocr_page_output_row(connection, run_item_id=run_item_id)
                        if existing_page_output_row is not None:
                            ocr_page_output_payload = ocr_page_output_row_to_payload(existing_page_output_row)
                    elif page_job_kind == "image_description":
                        existing_page_output_row = find_image_description_page_output_row(
                            connection,
                            run_item_id=run_item_id,
                        )
                        if existing_page_output_row is not None:
                            image_description_page_output_payload = image_description_page_output_row_to_payload(
                                existing_page_output_row
                            )
                payload = {
                    "status": "ok",
                    "idempotent": True,
                    "run_item": run_item_row_to_payload(run_item_row),
                    "result": result_payload,
                    "ocr_page_output": ocr_page_output_payload,
                    "image_description_page_output": image_description_page_output_payload,
                    "run": run_status_by_id(connection, int(run_item_row["run_id"])),
                }
                connection.commit()
                return payload
            if str(run_item_row["status"] or "") == "failed":
                raise RetrieverError(f"Run item {run_item_id} is already failed; reclaim it before completing it.")
            current_claimed_by = normalize_whitespace(str(run_item_row["claimed_by"] or ""))
            if current_claimed_by and current_claimed_by != normalized_claimed_by:
                raise RetrieverError(
                    f"Run item {run_item_id} is claimed by {current_claimed_by!r}, not {normalized_claimed_by!r}."
                )
            if str(run_item_row["status"] or "") != "running":
                raise RetrieverError(f"Run item {run_item_id} must be running before it can be completed.")

            run_row = require_run_row_by_id(connection, int(run_item_row["run_id"]))
            job_version_row = require_job_version_row_by_id(connection, int(run_row["job_version_id"]))
            job_row = connection.execute(
                "SELECT * FROM jobs WHERE id = ?",
                (job_version_row["job_id"],),
            ).fetchone()
            assert job_row is not None
            job_kind = normalize_job_kind(str(job_row["job_kind"]))
            snapshot_row = None
            if run_item_row["run_snapshot_document_id"] is not None:
                snapshot_row = connection.execute(
                    "SELECT * FROM run_snapshot_documents WHERE id = ?",
                    (run_item_row["run_snapshot_document_id"],),
                ).fetchone()
            job_output_rows = connection.execute(
                """
                SELECT *
                FROM job_outputs
                WHERE job_id = ?
                ORDER BY ordinal ASC, output_name ASC, id ASC
                """,
                (job_row["id"],),
            ).fetchall()

            raw_output = parse_json_argument(raw_output_json, label="Raw output", default=None)
            normalized_output = parse_json_argument(
                normalized_output_json,
                label="Normalized output",
                default=raw_output,
            )
            output_values_default = normalized_output if isinstance(normalized_output, dict) else {}
            output_values = parse_json_object_argument(
                output_values_json,
                label="Output values",
                default=output_values_default if isinstance(output_values_default, dict) else {},
            )
            provider_metadata = parse_json_object_argument(
                provider_metadata_json,
                label="Provider metadata",
                default={},
            )
            created_text_revision_payload = (
                parse_json_object_argument(
                    created_text_revision_json,
                    label="Created text revision",
                    default={},
                )
                if created_text_revision_json is not None
                else None
            )

            created_text_revision_id = None
            if job_kind in {"ocr", "image_description"} and str(run_item_row["item_kind"] or "") == "page":
                resolved_page_text = page_text if page_text is not None else None
                if resolved_page_text is None:
                    normalized_candidate = normalized_output if isinstance(normalized_output, str) else None
                    raw_candidate = raw_output if isinstance(raw_output, str) else None
                    resolved_page_text = normalized_candidate or raw_candidate
                if resolved_page_text is None:
                    raise RetrieverError(
                        "Page completion requires --page-text or a raw/normalized string payload."
                    )
                page_raw_output = raw_output if raw_output is not None else {"page_text": resolved_page_text}
                page_normalized_output = (
                    normalized_output if normalized_output is not None else {"page_text": resolved_page_text}
                )
                page_output_payload_key = "ocr_page_output"
                if job_kind == "ocr":
                    upsert_ocr_page_output_row(
                        connection,
                        run_item_id=run_item_id,
                        run_id=int(run_item_row["run_id"]),
                        document_id=int(run_item_row["document_id"]),
                        page_number=int(run_item_row["page_number"] or 0),
                        text_content=str(resolved_page_text),
                        raw_output=page_raw_output,
                        normalized_output=page_normalized_output,
                        provider_metadata=provider_metadata,
                    )
                    page_output_payload = ocr_page_output_row_to_payload(
                        find_ocr_page_output_row(connection, run_item_id=run_item_id)  # type: ignore[arg-type]
                    )
                else:
                    page_output_payload_key = "image_description_page_output"
                    upsert_image_description_page_output_row(
                        connection,
                        run_item_id=run_item_id,
                        run_id=int(run_item_row["run_id"]),
                        document_id=int(run_item_row["document_id"]),
                        page_number=int(run_item_row["page_number"] or 0),
                        text_content=str(resolved_page_text),
                        raw_output=page_raw_output,
                        normalized_output=page_normalized_output,
                        provider_metadata=provider_metadata,
                    )
                    page_output_payload = image_description_page_output_row_to_payload(
                        find_image_description_page_output_row(connection, run_item_id=run_item_id)  # type: ignore[arg-type]
                    )
                create_attempt_row(
                    connection,
                    run_item_id=run_item_id,
                    provider_request_id=provider_request_id,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cost_cents=cost_cents,
                    latency_ms=latency_ms,
                    provider_metadata=provider_metadata or {"executor": "cowork_agent"},
                    error_summary=None,
                )
                completion_time = utc_now()
                update_run_item_row(
                    connection,
                    run_item_id=run_item_id,
                    status="completed",
                    result_id=None,
                    last_error=None,
                    claimed_by=normalized_claimed_by,
                    claimed_at=str(run_item_row["claimed_at"] or completion_time),
                    last_heartbeat_at=completion_time,
                    completed_at=completion_time,
                    increment_attempt_count=True,
                )
                update_run_worker_row(
                    connection,
                    run_id=int(run_item_row["run_id"]),
                    claimed_by=normalized_claimed_by,
                    heartbeat=True,
                    increment_items_completed=1,
                )
                payload = {
                    "status": "ok",
                    "idempotent": False,
                    "run_item": run_item_row_to_payload(require_run_item_row_by_id(connection, run_item_id)),
                    "run": run_status_by_id(connection, int(run_item_row["run_id"])),
                }
                payload[page_output_payload_key] = page_output_payload
                connection.commit()
                return payload
            if created_text_revision_payload:
                text_content = str(created_text_revision_payload.get("text_content") or "")
                if not text_content:
                    raise RetrieverError("Created text revision payload must include text_content.")
                created_text_revision_id = create_text_revision_row(
                    connection,
                    paths,
                    document_id=int(run_item_row["document_id"]),
                    revision_kind=str(created_text_revision_payload.get("revision_kind") or job_kind),
                    text_content=text_content,
                    language=(
                        str(created_text_revision_payload["language"])
                        if created_text_revision_payload.get("language")
                        else None
                    ),
                    parent_revision_id=(
                        int(snapshot_row["pinned_input_revision_id"])
                        if snapshot_row is not None and snapshot_row["pinned_input_revision_id"] is not None
                        else None
                    ),
                    created_by_job_version_id=int(job_version_row["id"]),
                    quality_score=(
                        float(created_text_revision_payload["quality_score"])
                        if created_text_revision_payload.get("quality_score") is not None
                        else None
                    ),
                    provider_metadata=provider_metadata,
                )
            elif job_kind == "translation":
                raise RetrieverError("Translation run items must include a created text revision payload.")

            if raw_output is None:
                if created_text_revision_id is not None:
                    raw_output = {
                        "created_text_revision_id": created_text_revision_id,
                        "job_kind": job_kind,
                    }
                else:
                    raw_output = output_values
            if normalized_output is None:
                normalized_output = output_values if output_values else raw_output

            result_id, created = create_result_row(
                connection,
                run_id=int(run_item_row["run_id"]),
                document_id=int(run_item_row["document_id"]),
                job_version_id=int(job_version_row["id"]),
                input_revision_id=(
                    int(snapshot_row["pinned_input_revision_id"])
                    if snapshot_row is not None and snapshot_row["pinned_input_revision_id"] is not None
                    else None
                ),
                input_identity=str(run_item_row["input_identity"]),
                raw_output=raw_output,
                normalized_output=normalized_output,
                created_text_revision_id=created_text_revision_id,
                provider_metadata=provider_metadata,
            )
            if created and job_output_rows:
                upsert_result_output_rows(
                    connection,
                    result_id=result_id,
                    job_output_rows=job_output_rows,
                    output_values_by_name=output_values,
                )
            create_attempt_row(
                connection,
                run_item_id=run_item_id,
                provider_request_id=provider_request_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_cents=cost_cents,
                latency_ms=latency_ms,
                provider_metadata=provider_metadata or {"executor": "cowork_agent"},
                error_summary=None,
            )
            completion_time = utc_now()
            update_run_item_row(
                connection,
                run_item_id=run_item_id,
                status="completed",
                result_id=result_id,
                last_error=None,
                claimed_by=normalized_claimed_by,
                claimed_at=str(run_item_row["claimed_at"] or completion_time),
                last_heartbeat_at=completion_time,
                completed_at=completion_time,
                increment_attempt_count=True,
            )
            update_run_worker_row(
                connection,
                run_id=int(run_item_row["run_id"]),
                claimed_by=normalized_claimed_by,
                heartbeat=True,
                increment_items_completed=1,
            )
            payload = {
                "status": "ok",
                "idempotent": False,
                "run_item": run_item_row_to_payload(require_run_item_row_by_id(connection, run_item_id)),
                "result": result_summary_by_id(connection, result_id),
                "run": run_status_by_id(connection, int(run_item_row["run_id"])),
            }
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def fail_run_item(
    root: Path,
    *,
    run_item_id: int,
    claimed_by: str,
    error_summary: str,
    provider_metadata_json: str | None = None,
    provider_request_id: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cost_cents: int | None = None,
    latency_ms: int | None = None,
) -> dict[str, object]:
    normalized_claimed_by = normalize_whitespace(claimed_by)
    if not normalized_claimed_by:
        raise RetrieverError("claimed_by cannot be empty.")
    normalized_error = normalize_whitespace(error_summary)
    if not normalized_error:
        raise RetrieverError("error cannot be empty.")
    provider_metadata = parse_json_object_argument(
        provider_metadata_json,
        label="Provider metadata",
        default={},
    )
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN")
        try:
            run_item_row = require_run_item_row_by_id(connection, run_item_id)
            if str(run_item_row["status"] or "") == "failed":
                payload = {
                    "status": "ok",
                    "idempotent": True,
                    "run_item": run_item_row_to_payload(run_item_row),
                    "run": run_status_by_id(connection, int(run_item_row["run_id"])),
                }
                connection.commit()
                return payload
            if str(run_item_row["status"] or "") == "completed":
                raise RetrieverError(f"Run item {run_item_id} is already completed and cannot be failed.")
            current_claimed_by = normalize_whitespace(str(run_item_row["claimed_by"] or ""))
            if current_claimed_by and current_claimed_by != normalized_claimed_by:
                raise RetrieverError(
                    f"Run item {run_item_id} is claimed by {current_claimed_by!r}, not {normalized_claimed_by!r}."
                )
            if str(run_item_row["status"] or "") != "running":
                raise RetrieverError(f"Run item {run_item_id} must be running before it can be failed.")

            create_attempt_row(
                connection,
                run_item_id=run_item_id,
                provider_request_id=provider_request_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_cents=cost_cents,
                latency_ms=latency_ms,
                provider_metadata=provider_metadata or {"executor": "cowork_agent"},
                error_summary=normalized_error,
            )
            completion_time = utc_now()
            update_run_item_row(
                connection,
                run_item_id=run_item_id,
                status="failed",
                result_id=None,
                last_error=normalized_error,
                claimed_by=normalized_claimed_by,
                claimed_at=str(run_item_row["claimed_at"] or completion_time),
                last_heartbeat_at=completion_time,
                completed_at=completion_time,
                increment_attempt_count=True,
            )
            update_run_worker_row(
                connection,
                run_id=int(run_item_row["run_id"]),
                claimed_by=normalized_claimed_by,
                heartbeat=True,
                increment_items_failed=1,
                last_error=normalized_error,
            )
            payload = {
                "status": "ok",
                "idempotent": False,
                "run_item": run_item_row_to_payload(require_run_item_row_by_id(connection, run_item_id)),
                "run": run_status_by_id(connection, int(run_item_row["run_id"])),
            }
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def run_status(root: Path, *, run_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        return {
            "status": "ok",
            "run": run_status_by_id(connection, run_id),
        }
    finally:
        connection.close()


def cancel_run(root: Path, *, run_id: int, force: bool = False) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN IMMEDIATE")
        try:
            run_row = require_run_row_by_id(connection, run_id)
            already_canceled = str(run_row["status"] or "") == "canceled" or run_row["canceled_at"] is not None
            materialize_run_items_for_run(connection, paths, root, run_id)
            canceled_at = str(run_row["canceled_at"] or utc_now())
            connection.execute(
                """
                UPDATE runs
                SET status = 'canceled',
                    canceled_at = COALESCE(canceled_at, ?)
                WHERE id = ?
                """,
                (canceled_at, run_id),
            )
            skipped_count = cancel_pending_run_items(connection, run_id=run_id)
            force_stop_task_ids = request_run_worker_cancellation(connection, run_id=run_id, force=force)
            payload = {
                "status": "ok",
                "idempotent": already_canceled,
                "canceled_pending_items": skipped_count,
                "force_stop_requested": force,
                "force_stop_task_ids": force_stop_task_ids,
                "run": run_status_by_id(connection, run_id),
            }
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def finish_run_worker(
    root: Path,
    *,
    run_id: int,
    claimed_by: str,
    worker_status: str,
    summary_json: str | None = None,
    error_summary: str | None = None,
) -> dict[str, object]:
    normalized_claimed_by = normalize_whitespace(claimed_by)
    if not normalized_claimed_by:
        raise RetrieverError("claimed_by cannot be empty.")
    normalized_status = normalize_run_worker_status(worker_status)
    if normalized_status == "active":
        raise RetrieverError("finish-run-worker requires a terminal worker status, not 'active'.")
    summary_payload = decode_json_text(summary_json, default={}) if summary_json is not None else {}
    if summary_json is not None and not isinstance(summary_payload, dict):
        raise RetrieverError("summary_json must decode to a JSON object.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN IMMEDIATE")
        try:
            run_row = require_run_row_by_id(connection, run_id)
            worker_row = find_run_worker_row(connection, run_id=run_id, claimed_by=normalized_claimed_by)
            if worker_row is None:
                raise RetrieverError(
                    f"Run {run_id} does not have a registered worker for claimed_by={normalized_claimed_by!r}."
                )
            if str(run_row["status"] or "") != "canceled" and normalized_status == "canceled":
                raise RetrieverError("Worker cannot be finished as canceled unless the run itself is canceled.")

            already_terminal = (
                str(worker_row["status"] or "") == normalized_status and worker_row["completed_at"] is not None
            )
            if already_terminal:
                payload = {
                    "status": "ok",
                    "idempotent": True,
                    "worker": run_worker_row_to_payload(worker_row),
                    "run": run_status_by_id(connection, run_id),
                }
                connection.commit()
                return payload

            updated_row = update_run_worker_row(
                connection,
                run_id=run_id,
                claimed_by=normalized_claimed_by,
                status=normalized_status,
                last_error=(normalize_whitespace(error_summary) if error_summary is not None else "") or None,
                summary=summary_payload if isinstance(summary_payload, dict) else {},
                completed_at=utc_now(),
            )
            assert updated_row is not None
            payload = {
                "status": "ok",
                "idempotent": False,
                "worker": run_worker_row_to_payload(updated_row),
                "run": run_status_by_id(connection, run_id),
            }
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def finalize_ocr_run(root: Path, *, run_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN IMMEDIATE")
        try:
            materialize_run_items_for_run(connection, paths, root, run_id)
            payload = finalize_ocr_results_for_run(connection, paths, run_id=run_id)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def finalize_image_description_run(root: Path, *, run_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN")
        try:
            materialize_run_items_for_run(connection, paths, root, run_id)
            payload = finalize_image_description_results_for_run(connection, paths, run_id=run_id)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return payload
    finally:
        connection.close()


def publish_run_results(
    root: Path,
    *,
    run_id: int,
    raw_output_names: list[str] | None = None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        connection.execute("BEGIN")
        try:
            publish_summary = publish_result_outputs_for_run(
                connection,
                run_id=run_id,
                output_names=raw_output_names,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "run": run_summary_by_id(connection, run_id),
            **publish_summary,
        }
    finally:
        connection.close()


def list_jobs(root: Path) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        return {
            "status": "ok",
            "jobs": list_job_summaries(connection),
        }
    finally:
        connection.close()


def create_job(root: Path, raw_job_name: str, job_kind: str, description: str | None) -> dict[str, object]:
    job_name = sanitize_processing_identifier(raw_job_name, label="Job name", prefix="job")
    normalized_kind = normalize_job_kind(job_kind)
    normalized_description = normalize_whitespace(description) if description and description.strip() else None

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        existing_row = find_job_row_by_name(connection, job_name)
        if existing_row is not None:
            raise RetrieverError(f"Job {job_name!r} already exists.")
        connection.execute("BEGIN")
        try:
            job_id = create_job_row(
                connection,
                job_name=job_name,
                job_kind=normalized_kind,
                description=normalized_description,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "job": job_summary_by_id(connection, job_id),
        }
    finally:
        connection.close()


def add_job_output(
    root: Path,
    raw_job_name: str,
    raw_output_name: str,
    value_type: str,
    *,
    bound_custom_field: str | None = None,
    description: str | None = None,
) -> dict[str, object]:
    job_name = sanitize_processing_identifier(raw_job_name, label="Job name", prefix="job")
    output_name = sanitize_processing_identifier(raw_output_name, label="Job output name", prefix="output")
    normalized_value_type = normalize_job_output_value_type(value_type)
    normalized_description = normalize_whitespace(description) if description and description.strip() else None
    normalized_bound_field = None
    if bound_custom_field and bound_custom_field.strip():
        normalized_bound_field = sanitize_field_name(bound_custom_field)

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        job_row = require_job_row_by_name(connection, job_name)
        connection.execute("BEGIN")
        try:
            output_id, created = upsert_job_output_row(
                connection,
                job_id=int(job_row["id"]),
                output_name=output_name,
                value_type=normalized_value_type,
                bound_custom_field=normalized_bound_field,
                description=normalized_description,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        output_row = connection.execute(
            """
            SELECT *
            FROM job_outputs
            WHERE id = ?
            """,
            (output_id,),
        ).fetchone()
        assert output_row is not None
        return {
            "status": "ok",
            "created": created,
            "job": job_summary_by_id(connection, int(job_row["id"])),
            "job_output": job_output_row_to_payload(output_row),
        }
    finally:
        connection.close()


def list_job_versions(root: Path, raw_job_name: str) -> dict[str, object]:
    job_name = sanitize_processing_identifier(raw_job_name, label="Job name", prefix="job")
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        job_row = require_job_row_by_name(connection, job_name)
        return {
            "status": "ok",
            "job": job_summary_by_id(connection, int(job_row["id"])),
            "job_versions": job_versions_for_job(connection, int(job_row["id"])),
        }
    finally:
        connection.close()


def create_job_version(
    root: Path,
    raw_job_name: str,
    *,
    instruction: str | None,
    provider: str | None,
    capability: str | None,
    model: str | None,
    input_basis: str | None,
    response_schema_json: str | None,
    parameters_json: str | None,
    segment_profile: str | None,
    aggregation_strategy: str | None,
    display_name: str | None,
) -> dict[str, object]:
    job_name = sanitize_processing_identifier(raw_job_name, label="Job name", prefix="job")
    normalized_provider = normalize_whitespace(provider) if provider and provider.strip() else "cowork_agent"
    normalized_instruction = (instruction or "").strip()
    normalized_model = normalize_whitespace(model) if model and model.strip() else None
    normalized_segment_profile = (
        sanitize_processing_identifier(segment_profile, label="Segment profile", prefix="profile")
        if segment_profile and segment_profile.strip()
        else None
    )
    normalized_aggregation = (
        sanitize_processing_identifier(aggregation_strategy, label="Aggregation strategy", prefix="aggregation")
        if aggregation_strategy and aggregation_strategy.strip()
        else None
    )
    normalized_display_name = normalize_whitespace(display_name) if display_name and display_name.strip() else None
    parsed_response_schema = parse_json_argument(
        response_schema_json,
        label="Response schema",
        default=None,
    )
    response_schema_text = None if parsed_response_schema is None else compact_json_text(parsed_response_schema)
    parameters = parse_json_object_argument(
        parameters_json,
        label="Parameters",
        default={},
    )

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        job_row = require_job_row_by_name(connection, job_name)
        normalized_capability = (
            normalize_job_capability(capability)
            if capability and capability.strip()
            else default_job_capability_for_kind(str(job_row["job_kind"]))
        )
        normalized_input_basis = (
            normalize_job_input_basis(input_basis)
            if input_basis and input_basis.strip()
            else default_job_input_basis_for_kind(str(job_row["job_kind"]))
        )
        connection.execute("BEGIN")
        try:
            version_id = create_job_version_row(
                connection,
                job_id=int(job_row["id"]),
                job_name=job_name,
                instruction_text=normalized_instruction,
                response_schema_json=response_schema_text,
                capability=normalized_capability,
                provider=normalized_provider,
                model=normalized_model,
                parameters_json=compact_json_text(parameters),
                input_basis=normalized_input_basis,
                segment_profile=normalized_segment_profile,
                aggregation_strategy=normalized_aggregation,
                display_name=normalized_display_name,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        version_row = connection.execute(
            """
            SELECT *
            FROM job_versions
            WHERE id = ?
            """,
            (version_id,),
        ).fetchone()
        assert version_row is not None
        return {
            "status": "ok",
            "job": job_summary_by_id(connection, int(job_row["id"])),
            "job_version": job_version_row_to_payload(version_row),
        }
    finally:
        connection.close()


def get_custom_field_registry_row(connection: sqlite3.Connection, field_name: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT field_name, field_type, instruction, created_at
        FROM custom_fields_registry
        WHERE field_name = ?
        """,
        (field_name,),
    ).fetchone()


def add_field(root: Path, raw_field_name: str, field_type: str, instruction: str | None) -> dict[str, object]:
    normalized_field_name = sanitize_field_name(raw_field_name)
    normalized_field_type = field_type.strip().lower()
    if normalized_field_type not in REGISTRY_FIELD_TYPES:
        raise RetrieverError(f"Unsupported field type: {field_type}")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        columns = table_columns(connection, "documents")
        existing_registry_row = get_custom_field_registry_row(connection, normalized_field_name)
        if existing_registry_row is not None and existing_registry_row["field_type"] != normalized_field_type:
            if existing_registry_row["field_type"] == "text" and normalized_field_type == "date":
                raise RetrieverError(
                    f"Field '{normalized_field_name}' already exists as text; use promote-field-type to validate and promote it to date."
                )
            raise RetrieverError(
                f"Field '{normalized_field_name}' already exists with type {existing_registry_row['field_type']!r}."
            )
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


def promote_field_type(root: Path, raw_field_name: str, target_field_type: str) -> dict[str, object]:
    normalized_field_name = sanitize_field_name(raw_field_name)
    normalized_target_type = target_field_type.strip().lower()
    if normalized_target_type != "date":
        raise RetrieverError("Only text -> date field promotion is supported right now.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        registry_row = get_custom_field_registry_row(connection, normalized_field_name)
        if registry_row is None:
            raise RetrieverError(f"Unknown custom field: {normalized_field_name}")
        current_type = str(registry_row["field_type"])
        if current_type == normalized_target_type:
            return {
                "status": "ok",
                "field_name": normalized_field_name,
                "from_type": current_type,
                "to_type": normalized_target_type,
                "normalized_values_updated": 0,
                "documents_checked": 0,
                "documents_with_values": 0,
                "promotion_applied": False,
            }
        if current_type != "text":
            raise RetrieverError(
                f"Field '{normalized_field_name}' has type {current_type!r}; only text -> date promotion is supported."
            )
        if normalized_field_name not in table_columns(connection, "documents"):
            raise RetrieverError(f"Field column '{normalized_field_name}' does not exist on documents.")

        value_rows = connection.execute(
            f"""
            SELECT id, {quote_identifier(normalized_field_name)} AS value
            FROM documents
            WHERE {quote_identifier(normalized_field_name)} IS NOT NULL
              AND TRIM(CAST({quote_identifier(normalized_field_name)} AS TEXT)) != ''
            ORDER BY id ASC
            """
        ).fetchall()

        invalid_values: list[dict[str, object]] = []
        normalized_updates: list[tuple[str, int]] = []
        for row in value_rows:
            raw_value = str(row["value"])
            normalized_value = normalize_date_field_value(raw_value)
            if normalized_value is None:
                if len(invalid_values) < 10:
                    invalid_values.append({"document_id": int(row["id"]), "value": raw_value})
                continue
            if normalized_value != raw_value:
                normalized_updates.append((normalized_value, int(row["id"])))

        if invalid_values:
            return {
                "status": "blocked",
                "field_name": normalized_field_name,
                "from_type": current_type,
                "to_type": normalized_target_type,
                "documents_checked": len(value_rows),
                "documents_with_values": len(value_rows),
                "invalid_value_samples": invalid_values,
                "promotion_applied": False,
            }

        connection.execute("BEGIN")
        try:
            for normalized_value, document_id in normalized_updates:
                connection.execute(
                    f"""
                    UPDATE documents
                    SET {quote_identifier(normalized_field_name)} = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (normalized_value, utc_now(), document_id),
                )
            connection.execute(
                """
                UPDATE custom_fields_registry
                SET field_type = ?
                WHERE field_name = ?
                """,
                (normalized_target_type, normalized_field_name),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        return {
            "status": "ok",
            "field_name": normalized_field_name,
            "from_type": current_type,
            "to_type": normalized_target_type,
            "documents_checked": len(value_rows),
            "documents_with_values": len(value_rows),
            "normalized_values_updated": len(normalized_updates),
            "promotion_applied": True,
        }
    finally:
        connection.close()


def set_field(root: Path, document_id: int, field_name: str, value: str | None) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
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
            if column_name in {"author", "custodian", "participants", "recipients", "subject", "title"}:
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
