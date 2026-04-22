def ingest_production(root: Path, production_root: Path | str) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        resolved_production_root = resolve_production_root_argument(root, production_root)
        return ingest_resolved_production_root(connection, paths, root, resolved_production_root)
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
        slack_export_descriptors = find_slack_export_roots(root, recursive, allowed_types)
        slack_export_root_paths = [Path(descriptor["root"]).resolve() for descriptor in slack_export_descriptors]
        gmail_export_descriptors = find_gmail_export_roots(root, recursive, allowed_types)
        gmail_owned_paths = {
            Path(path).resolve()
            for descriptor in gmail_export_descriptors
            for path in list(descriptor.get("owned_paths") or [])
        }
        gmail_owned_rel_paths = {
            relative_document_path(root, owned_path)
            for owned_path in gmail_owned_paths
            if root.resolve() == owned_path.resolve() or root.resolve() in owned_path.resolve().parents
        }
        scanned_files = [
            path
            for path in collect_files(root, recursive, allowed_types)
            if not any(production_root == path.resolve() or production_root in path.resolve().parents for production_root in production_root_paths)
            and not any(slack_root == path.resolve() or slack_root in path.resolve().parents for slack_root in slack_export_root_paths)
            and path.resolve() not in gmail_owned_paths
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

        current_ingestion_batch: int | None = None
        slack_day_documents_missing = 0

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
            "gmail_exports_detected": len(gmail_export_descriptors),
            "gmail_linked_documents_created": 0,
            "gmail_linked_documents_updated": 0,
            "gmail_documents_scanned": 0,
            "slack_exports_detected": len(slack_export_descriptors),
            "slack_day_documents_scanned": 0,
            "slack_documents_created": 0,
            "slack_documents_updated": 0,
            "slack_documents_missing": 0,
            "slack_conversations": 0,
            "email_conversations": 0,
            "email_documents_reassigned": 0,
            "email_child_documents_updated": 0,
            "pst_chat_conversations": 0,
            "pst_chat_documents_reassigned": 0,
            "pst_chat_child_documents_updated": 0,
            "production_documents_created": 0,
            "production_documents_updated": 0,
            "production_documents_unchanged": 0,
            "production_documents_retired": 0,
            "production_families_reconstructed": 0,
            "production_docs_missing_linked_text": 0,
            "production_docs_missing_linked_images": 0,
            "production_docs_missing_linked_natives": 0,
        }
        failures: list[dict[str, str]] = []
        ingested_production_roots: list[str] = []
        skipped_production_roots: list[str] = []
        warnings: list[str] = []

        for descriptor in gmail_export_descriptors:
            export_root = Path(descriptor["root"])
            try:
                gmail_result = ingest_gmail_export_root(
                    connection,
                    paths,
                    root,
                    descriptor,
                    allowed_file_types=allowed_types,
                )
                stats["new"] += int(gmail_result["new"])
                stats["updated"] += int(gmail_result["updated"])
                stats["failed"] += int(gmail_result["failed"])
                stats["gmail_documents_scanned"] += int(gmail_result["scanned_files"])
                stats["mbox_sources_skipped"] += int(gmail_result["mbox_sources_skipped"])
                stats["mbox_messages_created"] += int(gmail_result["mbox_messages_created"])
                stats["mbox_messages_updated"] += int(gmail_result["mbox_messages_updated"])
                stats["mbox_messages_deleted"] += int(gmail_result["mbox_messages_deleted"])
                stats["gmail_linked_documents_created"] += int(gmail_result["gmail_linked_documents_created"])
                stats["gmail_linked_documents_updated"] += int(gmail_result["gmail_linked_documents_updated"])
                scanned_rel_paths.update(str(rel_path) for rel_path in list(gmail_result.get("scanned_filesystem_rel_paths", [])))
                scanned_mbox_source_rel_paths.update(
                    str(rel_path) for rel_path in list(gmail_result.get("scanned_mbox_source_rel_paths", []))
                )
                failures.extend(list(gmail_result.get("failures", [])))
            except Exception as exc:
                stats["failed"] += 1
                failures.append(
                    {
                        "rel_path": relative_document_path(root, export_root),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        for descriptor in slack_export_descriptors:
            export_root = Path(descriptor["root"])
            try:
                slack_result = ingest_slack_export_root(
                    connection,
                    paths,
                    export_root,
                    ingestion_batch_number=current_ingestion_batch,
                )
                current_ingestion_batch = (
                    int(slack_result["ingestion_batch_number"])
                    if slack_result.get("ingestion_batch_number") is not None
                    else current_ingestion_batch
                )
                stats["new"] += int(slack_result["new"])
                stats["updated"] += int(slack_result["updated"])
                stats["failed"] += int(slack_result["failed"])
                stats["slack_day_documents_scanned"] += int(slack_result["scanned_day_files"])
                stats["slack_documents_created"] += int(slack_result["new"])
                stats["slack_documents_updated"] += int(slack_result["updated"])
                stats["slack_conversations"] += int(slack_result["conversations"])
                slack_day_documents_missing += int(slack_result["missing"])
                failures.extend(list(slack_result.get("failures", [])))
            except Exception as exc:
                stats["failed"] += 1
                failures.append(
                    {
                        "rel_path": relative_document_path(root, export_root),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        if allowed_types is None:
            for signature in production_signatures:
                production_rel_root = str(signature["rel_root"])
                resolved_production_root = Path(signature["root"]).resolve()
                try:
                    production_result = ingest_resolved_production_root(
                        connection,
                        paths,
                        root,
                        resolved_production_root,
                    )
                    ingested_production_roots.append(production_rel_root)
                    stats["new"] += int(production_result["created"])
                    stats["updated"] += int(production_result["updated"])
                    stats["production_documents_created"] += int(production_result["created"])
                    stats["production_documents_updated"] += int(production_result["updated"])
                    stats["production_documents_unchanged"] += int(production_result["unchanged"])
                    stats["production_documents_retired"] += int(production_result["retired"])
                    stats["production_families_reconstructed"] += int(production_result["families_reconstructed"])
                    stats["production_docs_missing_linked_text"] += int(production_result["docs_missing_linked_text"])
                    stats["production_docs_missing_linked_images"] += int(production_result["docs_missing_linked_images"])
                    stats["production_docs_missing_linked_natives"] += int(production_result["docs_missing_linked_natives"])
                    for failure in list(production_result.get("failures", [])):
                        control_number = normalize_whitespace(str(failure.get("control_number") or ""))
                        failure_entry: dict[str, str] = {
                            "rel_path": (
                                production_logical_rel_path(production_rel_root, control_number).as_posix()
                                if control_number
                                else production_rel_root
                            ),
                            "production_rel_root": production_rel_root,
                            "error": str(failure.get("error") or ""),
                        }
                        if control_number:
                            failure_entry["control_number"] = control_number
                        failures.append(failure_entry)
                    stats["failed"] += len(list(production_result.get("failures", [])))
                except Exception as exc:
                    stats["failed"] += 1
                    failures.append(
                        {
                            "rel_path": production_rel_root,
                            "production_rel_root": production_rel_root,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
        else:
            skipped_production_roots = [str(signature["rel_root"]) for signature in production_signatures]
            warnings = [
                f"Detected processed production root at {signature['rel_root']}; use ingest-production instead."
                for signature in production_signatures
            ]

        existing_rows = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE parent_document_id IS NULL
              AND COALESCE(source_kind, ?) = ?
            """
        , (FILESYSTEM_SOURCE_KIND, FILESYSTEM_SOURCE_KIND)).fetchall()
        existing_rows = [row for row in existing_rows if str(row["rel_path"]) not in gmail_owned_rel_paths]
        existing_by_rel = {row["rel_path"]: row for row in existing_rows}
        unseen_existing_by_hash: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in existing_rows:
            if row["rel_path"] not in scanned_rel_paths and row["file_hash"]:
                unseen_existing_by_hash[row["file_hash"]].append(row)

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
                    and document_row_has_email_threading(connection, existing_row)
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
                replace_document_email_threading_row(
                    connection,
                    document_id=document_id,
                    email_threading=extracted.get("email_threading"),
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
                chunks = extracted_search_chunks(extracted)
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
        stats["slack_documents_missing"] = slack_day_documents_missing
        stats["missing"] = filesystem_missing + pst_sources_missing + mbox_sources_missing + slack_day_documents_missing
        connection.execute("BEGIN")
        try:
            conversation_assignment = assign_supported_conversations(connection)
            refresh_conversation_previews(connection, paths)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        stats["email_conversations"] = int(conversation_assignment["email_conversations"])
        stats["email_documents_reassigned"] = int(conversation_assignment["email_documents_reassigned"])
        stats["email_child_documents_updated"] = int(conversation_assignment["email_child_documents_updated"])
        stats["pst_chat_conversations"] = int(conversation_assignment["pst_chat_conversations"])
        stats["pst_chat_documents_reassigned"] = int(conversation_assignment["pst_chat_documents_reassigned"])
        stats["pst_chat_child_documents_updated"] = int(conversation_assignment["pst_chat_child_documents_updated"])
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
        result["scanned"] = len(scanned_items) + stats["slack_day_documents_scanned"] + stats["gmail_documents_scanned"]
        result["scanned_files"] = len(scanned_items) + stats["slack_day_documents_scanned"] + stats["gmail_documents_scanned"]
        result["pruned_unused_filesystem_dataset"] = int(pruned_unused_filesystem_dataset)
        result["ingested_production_roots"] = ingested_production_roots
        result["skipped_production_roots"] = skipped_production_roots
        if warnings:
            result["warnings"] = warnings
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
    dataset_names: list[str] | None = None,
    document_ids: list[int] | None = None,
    query: str = "",
    raw_bates: str | None = None,
    raw_filters: list[list[str]] | None = None,
    from_run_id: int | None = None,
    select_from_scope: bool = False,
    activation_policy: str = "manual",
    family_mode: str = "exact",
    seed_limit: int | None = None,
) -> dict[str, object]:
    normalized_job_name = (
        sanitize_processing_identifier(raw_job_name, label="Job name", prefix="job")
        if raw_job_name is not None
        else None
    )
    normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in (document_ids or [])))
    normalized_family_mode = normalize_run_family_mode(family_mode)
    normalized_activation_policy = normalize_run_activation_policy(activation_policy)
    if normalized_document_ids and (query.strip() or raw_bates or raw_filters or dataset_names or from_run_id is not None or select_from_scope):
        raise RetrieverError("create-run accepts either --doc-id selectors or scope/query selectors, not both.")
    if seed_limit is not None and seed_limit < 1:
        raise RetrieverError("Run limit must be >= 1.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        if normalized_document_ids:
            selector = {"document_ids": normalized_document_ids}
            preferred_from_run_id = None
        else:
            selector = build_effective_scope_selector(
                connection,
                paths,
                query=query,
                raw_bates=raw_bates,
                raw_filters=raw_filters,
                dataset_names=dataset_names,
                from_run_id=from_run_id,
                select_from_scope=select_from_scope,
            )
            if not scope_run_selector_has_inputs(selector):
                raise RetrieverError("Run selector must include at least one inclusion input.")
            preferred_from_run_id = preferred_scope_selector_from_run_id(selector)
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
        job_kind = normalize_job_kind(str(job_row["job_kind"]))
        if normalized_activation_policy != "manual" and job_kind not in REVISION_PRODUCING_JOB_KINDS:
            raise RetrieverError(
                f"Run activation policy '{normalized_activation_policy}' is only supported for "
                f"revision-producing jobs ({', '.join(sorted(REVISION_PRODUCING_JOB_KINDS))}); "
                f"job '{job_row['job_name']}' is kind '{job_kind}'."
            )
        if normalized_document_ids:
            selected_document_rows = fetch_visible_document_rows_by_ids(connection, normalized_document_ids)
            seed_document_ids = [int(row["id"]) for row in selected_document_rows]
            if seed_limit is not None:
                seed_document_ids = seed_document_ids[:seed_limit]
            reasons_by_document_id = {
                document_id: {
                    "direct_reasons": [{"type": "document_id", "document_id": document_id}],
                    "family_seed_document_ids": [],
                }
                for document_id in seed_document_ids
            }
            if normalized_family_mode == "with_family":
                final_document_ids = expand_seed_documents_with_family(connection, seed_document_ids, reasons_by_document_id)
            else:
                final_document_ids = list(seed_document_ids)
            if final_document_ids:
                document_rows = connection.execute(
                    f"""
                    SELECT *
                    FROM documents
                    WHERE id IN ({', '.join('?' for _ in final_document_ids)})
                    ORDER BY id ASC
                    """,
                    final_document_ids,
                ).fetchall()
                document_row_by_id = {int(row["id"]): row for row in document_rows}
            else:
                document_row_by_id = {}
            snapshot_rows = []
            for ordinal, document_id in enumerate(final_document_ids):
                document_row = document_row_by_id.get(int(document_id))
                if document_row is None:
                    continue
                pinned_input = compute_document_input_reference_for_job_version(
                    connection,
                    root=root,
                    document_row=document_row,
                    job_row=job_row,
                    job_version_row=job_version_row,
                    frozen_input_revision_id=None,
                    frozen_content_hash=None,
                )
                snapshot_rows.append(
                    {
                        "document_id": int(document_id),
                        "ordinal": ordinal,
                        "inclusion_reason": reasons_by_document_id.get(
                            int(document_id),
                            {"direct_reasons": [], "family_seed_document_ids": []},
                        ),
                        "pinned_input_revision_id": pinned_input["pinned_input_revision_id"],
                        "pinned_input_identity": pinned_input["pinned_input_identity"],
                        "pinned_content_hash": pinned_input["pinned_content_hash"],
                    }
                )
        else:
            snapshot_rows = plan_scope_run_snapshot_rows(
                connection,
                root=root,
                job_row=job_row,
                job_version_row=job_version_row,
                selector=selector,
                family_mode=normalized_family_mode,
                seed_limit=seed_limit,
            )
        connection.execute("BEGIN")
        try:
            run_id = create_run_row(
                connection,
                job_version_id=int(job_version_row["id"]),
                selector=selector,
                exclude_selector={},
                activation_policy=normalized_activation_policy,
                family_mode=normalized_family_mode,
                seed_limit=seed_limit,
                from_run_id=preferred_from_run_id,
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
            result_row = connection.execute(
                """
                SELECT *
                FROM results
                WHERE id = ?
                """,
                (result_id,),
            ).fetchone()
            assert result_row is not None
            activation_payload = maybe_activate_created_text_revision(
                connection,
                paths,
                run_row=require_run_row_by_id(connection, int(run_item_row["run_id"])),
                job_version_row=job_version_row,
                document_id=int(run_item_row["document_id"]),
                result_id=result_id,
                text_revision_id=(
                    int(result_row["created_text_revision_id"])
                    if result_row["created_text_revision_id"] is not None
                    else None
                ),
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
            if activation_payload is not None:
                payload["activation"] = activation_payload
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


def get_document_row_for_conversation_assignment(connection: sqlite3.Connection, document_id: int) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT
          id,
          parent_document_id,
          rel_path,
          control_number,
          content_type,
          source_kind,
          source_rel_path,
          source_folder_path,
          title,
          subject,
          conversation_id,
          conversation_assignment_mode,
          lifecycle_status
        FROM documents
        WHERE id = ?
        """,
        (document_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown document id: {document_id}")
    if row["lifecycle_status"] in {"missing", "deleted"}:
        raise RetrieverError(f"Document {document_id} is not active.")
    return row


def get_document_family_root_row_for_assignment(connection: sqlite3.Connection, document_id: int) -> sqlite3.Row:
    row = get_document_row_for_conversation_assignment(connection, document_id)
    while row["parent_document_id"] is not None:
        row = get_document_row_for_conversation_assignment(connection, int(row["parent_document_id"]))
    return row


def document_conversation_assignment_category(row: sqlite3.Row) -> str | None:
    content_type = normalize_whitespace(str(row["content_type"] or ""))
    source_kind = normalize_whitespace(str(row["source_kind"] or "")).lower()
    if content_type == "Email":
        return "email"
    if content_type == "Chat" and source_kind == PST_SOURCE_KIND:
        return "pst_chat"
    return None


def ensure_document_supports_manual_conversation_assignment(row: sqlite3.Row) -> str:
    category = document_conversation_assignment_category(row)
    if category is None:
        raise RetrieverError(
            "Manual conversation changes currently support top-level email documents and PST chat documents only."
        )
    return category


def manual_conversation_display_name(
    root_row: sqlite3.Row,
    *,
    category: str,
    existing_conversation_row: sqlite3.Row | None,
) -> str:
    if category == "email":
        for candidate in (root_row["subject"], root_row["title"]):
            display_name = normalize_email_thread_subject(candidate, preserve_case=True)
            if display_name:
                return display_name
        if existing_conversation_row is not None:
            existing_display = normalize_whitespace(str(existing_conversation_row["display_name"] or ""))
            if existing_display:
                return existing_display
        return "Email conversation"

    title = normalize_whitespace(str(root_row["title"] or ""))
    if title:
        return title
    folder_path = normalize_whitespace(str(root_row["source_folder_path"] or ""))
    if folder_path:
        leaf_name = normalize_whitespace(folder_path.split("/")[-1])
        if leaf_name:
            return leaf_name
    if existing_conversation_row is not None:
        existing_display = normalize_whitespace(str(existing_conversation_row["display_name"] or ""))
        if existing_display:
            return existing_display
    return "Chat conversation"


def create_manual_singleton_conversation(
    connection: sqlite3.Connection,
    root_row: sqlite3.Row,
) -> int:
    category = ensure_document_supports_manual_conversation_assignment(root_row)
    existing_conversation_row = None
    if root_row["conversation_id"] is not None:
        existing_conversation_row = connection.execute(
            "SELECT * FROM conversations WHERE id = ?",
            (int(root_row["conversation_id"]),),
        ).fetchone()

    if existing_conversation_row is not None:
        source_kind = str(existing_conversation_row["source_kind"])
        source_locator = str(existing_conversation_row["source_locator"])
        conversation_type = str(existing_conversation_row["conversation_type"])
    elif category == "email":
        source_kind = EMAIL_CONVERSATION_SOURCE_KIND
        source_locator = filesystem_dataset_locator()
        conversation_type = "email"
    else:
        source_kind = PST_SOURCE_KIND
        source_locator = normalize_whitespace(str(root_row["source_rel_path"] or "")) or filesystem_dataset_locator()
        conversation_type = "chat"

    return upsert_conversation_row(
        connection,
        source_kind=source_kind,
        source_locator=source_locator,
        conversation_key=f"manual:{category}:{int(root_row['id'])}",
        conversation_type=conversation_type,
        display_name=manual_conversation_display_name(
            root_row,
            category=category,
            existing_conversation_row=existing_conversation_row,
        ),
    )


def reassign_conversations_and_refresh_previews(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
) -> dict[str, int]:
    assignment = assign_supported_conversations(connection)
    refresh_conversation_previews(connection, paths)
    return assignment


def merge_into_conversation(root: Path, document_id: int, target_document_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        source_root_row = get_document_family_root_row_for_assignment(connection, document_id)
        target_root_row = get_document_family_root_row_for_assignment(connection, target_document_id)
        source_category = ensure_document_supports_manual_conversation_assignment(source_root_row)
        target_category = ensure_document_supports_manual_conversation_assignment(target_root_row)
        if source_category != target_category:
            raise RetrieverError("Source and target documents must belong to the same conversation-compatible category.")
        target_conversation_id = (
            int(target_root_row["conversation_id"])
            if target_root_row["conversation_id"] is not None
            else None
        )
        if target_conversation_id is None:
            raise RetrieverError(f"Target document {int(target_root_row['id'])} does not belong to a conversation.")

        connection.execute("BEGIN")
        try:
            connection.execute(
                """
                UPDATE documents
                SET conversation_id = ?, conversation_assignment_mode = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    target_conversation_id,
                    CONVERSATION_ASSIGNMENT_MODE_MANUAL,
                    utc_now(),
                    int(source_root_row["id"]),
                ),
            )
            assignment = reassign_conversations_and_refresh_previews(connection, paths)
            updated_row = get_document_row_for_conversation_assignment(connection, int(source_root_row["id"]))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        return {
            "status": "ok",
            "document_id": document_id,
            "root_document_id": int(source_root_row["id"]),
            "target_document_id": target_document_id,
            "target_root_document_id": int(target_root_row["id"]),
            "conversation_id": int(updated_row["conversation_id"]) if updated_row["conversation_id"] is not None else None,
            "conversation_assignment_mode": effective_conversation_assignment_mode(
                updated_row["conversation_assignment_mode"]
            ),
            "assignment_summary": assignment,
        }
    finally:
        connection.close()


def split_from_conversation(root: Path, document_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        root_row = get_document_family_root_row_for_assignment(connection, document_id)
        ensure_document_supports_manual_conversation_assignment(root_row)

        connection.execute("BEGIN")
        try:
            singleton_conversation_id = create_manual_singleton_conversation(connection, root_row)
            connection.execute(
                """
                UPDATE documents
                SET conversation_id = ?, conversation_assignment_mode = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    singleton_conversation_id,
                    CONVERSATION_ASSIGNMENT_MODE_MANUAL,
                    utc_now(),
                    int(root_row["id"]),
                ),
            )
            assignment = reassign_conversations_and_refresh_previews(connection, paths)
            updated_row = get_document_row_for_conversation_assignment(connection, int(root_row["id"]))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        return {
            "status": "ok",
            "document_id": document_id,
            "root_document_id": int(root_row["id"]),
            "conversation_id": int(updated_row["conversation_id"]) if updated_row["conversation_id"] is not None else None,
            "conversation_assignment_mode": effective_conversation_assignment_mode(
                updated_row["conversation_assignment_mode"]
            ),
            "assignment_summary": assignment,
        }
    finally:
        connection.close()


def clear_conversation_assignment(root: Path, document_id: int) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        root_row = get_document_family_root_row_for_assignment(connection, document_id)
        ensure_document_supports_manual_conversation_assignment(root_row)

        connection.execute("BEGIN")
        try:
            connection.execute(
                """
                UPDATE documents
                SET conversation_assignment_mode = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    CONVERSATION_ASSIGNMENT_MODE_AUTO,
                    utc_now(),
                    int(root_row["id"]),
                ),
            )
            assignment = reassign_conversations_and_refresh_previews(connection, paths)
            updated_row = get_document_row_for_conversation_assignment(connection, int(root_row["id"]))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        return {
            "status": "ok",
            "document_id": document_id,
            "root_document_id": int(root_row["id"]),
            "conversation_id": int(updated_row["conversation_id"]) if updated_row["conversation_id"] is not None else None,
            "conversation_assignment_mode": effective_conversation_assignment_mode(
                updated_row["conversation_assignment_mode"]
            ),
            "assignment_summary": assignment,
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
