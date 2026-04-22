CONTAINER_SOURCE_FILE_TYPES = frozenset({PST_SOURCE_KIND, MBOX_SOURCE_KIND})


def default_ingest_stats(slack_export_count: int, gmail_export_count: int) -> dict[str, int]:
    return {
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
        "gmail_exports_detected": gmail_export_count,
        "gmail_linked_documents_created": 0,
        "gmail_linked_documents_updated": 0,
        "gmail_documents_scanned": 0,
        "slack_exports_detected": slack_export_count,
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


def source_file_snapshot(path: Path) -> tuple[int | None, int | None]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None, None
    return stat.st_size, stat.st_mtime_ns


def refresh_ingest_item_filesystem_facts(item: dict[str, object]) -> dict[str, object]:
    refreshed_item = dict(item)
    path = Path(refreshed_item["path"])
    file_size, file_mtime_ns = source_file_snapshot(path)
    refreshed_item["source_file_size"] = file_size
    refreshed_item["source_file_mtime_ns"] = file_mtime_ns
    refreshed_item["file_hash"] = (
        None if str(refreshed_item["file_type"]) in CONTAINER_SOURCE_FILE_TYPES else sha256_file(path)
    )
    return refreshed_item


def plan_ingest_work(
    root: Path,
    recursive: bool,
    allowed_types: set[str] | None,
    connection: sqlite3.Connection,
) -> dict[str, object]:
    scan_hash_ms = 0.0
    production_signatures = find_production_root_signatures(root, recursive, connection)
    production_root_paths = [Path(signature["root"]).resolve() for signature in production_signatures]
    slack_export_descriptors = find_slack_export_roots(root, recursive, allowed_types)
    slack_export_root_paths = [Path(descriptor["root"]).resolve() for descriptor in slack_export_descriptors]
    gmail_export_descriptors = find_gmail_export_roots(root, recursive, allowed_types)
    pst_export_descriptors = find_pst_export_roots(root, recursive) if allowed_types is None or PST_SOURCE_KIND in allowed_types else []
    gmail_owned_paths = {
        Path(path).resolve()
        for descriptor in gmail_export_descriptors
        for path in list(descriptor.get("owned_paths") or [])
    }
    pst_export_owned_paths = {
        Path(path).resolve()
        for descriptor in pst_export_descriptors
        for path in list(descriptor.get("owned_paths") or [])
    }
    gmail_owned_rel_paths = {
        relative_document_path(root, owned_path)
        for owned_path in gmail_owned_paths
        if root.resolve() == owned_path.resolve() or root.resolve() in owned_path.resolve().parents
    }
    pst_export_owned_rel_paths = {
        relative_document_path(root, owned_path)
        for owned_path in pst_export_owned_paths
        if root.resolve() == owned_path.resolve() or root.resolve() in owned_path.resolve().parents
    }
    pst_export_descriptors_by_pst_path = {
        Path(path).resolve().as_posix(): descriptor
        for descriptor in pst_export_descriptors
        for path in list(descriptor.get("pst_paths") or [])
    }
    scanned_files = [
        path
        for path in collect_files(root, recursive, allowed_types)
        if not any(production_root == path.resolve() or production_root in path.resolve().parents for production_root in production_root_paths)
        and not any(slack_root == path.resolve() or slack_root in path.resolve().parents for slack_root in slack_export_root_paths)
        and path.resolve() not in gmail_owned_paths
        and path.resolve() not in pst_export_owned_paths
    ]
    scanned_rel_paths: set[str] = set()
    scanned_pst_source_rel_paths: set[str] = set()
    scanned_mbox_source_rel_paths: set[str] = set()
    scanned_items: list[dict[str, object]] = []
    loose_file_items: list[dict[str, object]] = []
    for path in scanned_files:
        item_started = time.perf_counter()
        rel_path = relative_document_path(root, path)
        file_type = normalize_extension(path)
        scanned_rel_paths.add(rel_path)
        if file_type == PST_SOURCE_KIND:
            scanned_pst_source_rel_paths.add(rel_path)
        if file_type == MBOX_SOURCE_KIND:
            scanned_mbox_source_rel_paths.add(rel_path)
        item = refresh_ingest_item_filesystem_facts(
            {
                "path": path,
                "rel_path": rel_path,
                "file_type": file_type,
            }
        )
        scanned_items.append(item)
        if file_type not in CONTAINER_SOURCE_FILE_TYPES:
            loose_file_items.append(item)
        scan_hash_ms += (time.perf_counter() - item_started) * 1000.0
    return {
        "production_signatures": production_signatures,
        "slack_export_descriptors": slack_export_descriptors,
        "gmail_export_descriptors": gmail_export_descriptors,
        "pst_export_descriptors": pst_export_descriptors,
        "gmail_owned_rel_paths": gmail_owned_rel_paths,
        "pst_export_owned_rel_paths": pst_export_owned_rel_paths,
        "pst_export_descriptors_by_pst_path": pst_export_descriptors_by_pst_path,
        "scanned_files": scanned_files,
        "scanned_items": scanned_items,
        "loose_file_items": loose_file_items,
        "scanned_rel_paths": scanned_rel_paths,
        "scanned_pst_source_rel_paths": scanned_pst_source_rel_paths,
        "scanned_mbox_source_rel_paths": scanned_mbox_source_rel_paths,
        "scan_hash_ms": scan_hash_ms,
    }


def ingest_serial_special_sources(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    root: Path,
    ingest_tmp_dir: Path | None,
    allowed_types: set[str] | None,
    production_signatures: list[dict[str, object]],
    slack_export_descriptors: list[dict[str, object]],
    gmail_export_descriptors: list[dict[str, object]],
    scanned_rel_paths: set[str],
    scanned_mbox_source_rel_paths: set[str],
    stats: dict[str, int],
    failures: list[dict[str, str]],
) -> dict[str, object]:
    current_ingestion_batch: int | None = None
    slack_day_documents_missing = 0
    ingested_production_roots: list[str] = []
    skipped_production_roots: list[str] = []
    warnings: list[str] = []
    gmail_ms = 0.0
    for descriptor in gmail_export_descriptors:
        export_root = Path(descriptor["root"])
        source_started = time.perf_counter()
        try:
            gmail_result = ingest_gmail_export_root(
                connection,
                paths,
                root,
                descriptor,
                allowed_file_types=allowed_types,
                staging_root=ingest_tmp_dir,
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
        finally:
            gmail_ms += (time.perf_counter() - source_started) * 1000.0
    slack_ms = 0.0
    for descriptor in slack_export_descriptors:
        export_root = Path(descriptor["root"])
        source_started = time.perf_counter()
        try:
            slack_result = ingest_slack_export_root(
                connection,
                paths,
                export_root,
                ingestion_batch_number=current_ingestion_batch,
                staging_root=ingest_tmp_dir,
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
        finally:
            slack_ms += (time.perf_counter() - source_started) * 1000.0

    production_ms = 0.0
    if allowed_types is None:
        for signature in production_signatures:
            production_rel_root = str(signature["rel_root"])
            resolved_production_root = Path(signature["root"]).resolve()
            source_started = time.perf_counter()
            try:
                production_result = ingest_resolved_production_root(
                    connection,
                    paths,
                    root,
                    resolved_production_root,
                    staging_root=ingest_tmp_dir,
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
            finally:
                production_ms += (time.perf_counter() - source_started) * 1000.0
    else:
        skipped_production_roots = [str(signature["rel_root"]) for signature in production_signatures]
        warnings = [
            f"Detected processed production root at {signature['rel_root']}; use ingest-production instead."
            for signature in production_signatures
        ]

    return {
        "current_ingestion_batch": current_ingestion_batch,
        "slack_day_documents_missing": slack_day_documents_missing,
        "ingested_production_roots": ingested_production_roots,
        "skipped_production_roots": skipped_production_roots,
        "warnings": warnings,
        "gmail_ms": gmail_ms,
        "slack_ms": slack_ms,
        "production_ms": production_ms,
    }


def load_loose_file_commit_state(
    connection: sqlite3.Connection,
    scanned_rel_paths: set[str],
    gmail_owned_rel_paths: set[str],
    pst_export_owned_rel_paths: set[str],
) -> tuple[dict[str, sqlite3.Row], dict[str, list[sqlite3.Row]]]:
    existing_occurrence_rows = connection.execute(
        """
        SELECT *
        FROM document_occurrences
        WHERE parent_occurrence_id IS NULL
          AND source_kind = ?
          AND lifecycle_status != 'deleted'
        """,
        (FILESYSTEM_SOURCE_KIND,),
    ).fetchall()
    existing_occurrence_rows = [
        row
        for row in existing_occurrence_rows
        if str(row["rel_path"]) not in gmail_owned_rel_paths
        and str(row["rel_path"]) not in pst_export_owned_rel_paths
    ]
    existing_by_rel = {str(row["rel_path"]): row for row in existing_occurrence_rows}
    unseen_existing_by_hash: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in existing_occurrence_rows:
        if str(row["rel_path"]) not in scanned_rel_paths and row["file_hash"]:
            unseen_existing_by_hash[row["file_hash"]].append(row)
    return existing_by_rel, unseen_existing_by_hash


def ingest_prepare_worker_count() -> int:
    default_workers = min(8, os.cpu_count() or 4)
    raw_value = os.environ.get("RETRIEVER_INGEST_WORKERS")
    if raw_value is None:
        return default_workers
    raw_value = raw_value.strip()
    if not raw_value:
        return default_workers
    try:
        return max(1, int(raw_value))
    except ValueError:
        return default_workers


def ingest_container_prepare_worker_count() -> int:
    raw_value = os.environ.get("RETRIEVER_INGEST_CONTAINER_WORKERS")
    if raw_value is None:
        return 1
    raw_value = raw_value.strip()
    if not raw_value:
        return 1
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 1


def ingest_prepare_queue_capacity(prepare_workers: int) -> int:
    return max(2, prepare_workers * 2)


def ingest_prepare_queue_max_bytes() -> int:
    raw_value = os.environ.get("RETRIEVER_INGEST_PREPARED_QUEUE_BYTES")
    default_value = 512 * 1024 * 1024
    if raw_value is None:
        return default_value
    raw_value = raw_value.strip()
    if not raw_value:
        return default_value
    try:
        return max(1, int(raw_value))
    except ValueError:
        return default_value


def ingest_prepare_spill_threshold_bytes() -> int:
    raw_value = os.environ.get("RETRIEVER_INGEST_PREPARE_SPILL_BYTES")
    default_value = 32 * 1024 * 1024
    if raw_value is None:
        return default_value
    raw_value = raw_value.strip()
    if not raw_value:
        return default_value
    try:
        return max(1, int(raw_value))
    except ValueError:
        return default_value


def serialize_prepared_ingest_item(prepared_item: dict[str, object]) -> bytes:
    return pickle.dumps(prepared_item, protocol=pickle.HIGHEST_PROTOCOL)


def stage_prepared_ingest_item(
    spill_dir: Path,
    prepared_index: int,
    serialized_payload: bytes,
) -> Path:
    spill_dir.mkdir(parents=True, exist_ok=True)
    spill_path = spill_dir / f"{prepared_index:08d}.prepared"
    spill_path.write_bytes(serialized_payload)
    return spill_path


def hydrate_staged_prepared_ingest_item(spill_path: Path) -> dict[str, object]:
    try:
        return pickle.loads(spill_path.read_bytes())
    finally:
        remove_file_if_exists(spill_path)


def iter_staged_prepared_items(
    items: list[dict[str, object]] | Iterator[dict[str, object]],
    *,
    prepare_item,
    config_benchmark_name: str,
    queue_done_benchmark_name: str,
    spill_subdir_name: str,
    staging_root: Path | None = None,
    prepare_workers: int | None = None,
) -> Iterator[tuple[dict[str, object], float]]:
    effective_prepare_workers = prepare_workers if prepare_workers is not None else ingest_prepare_worker_count()
    max_prepared_items = ingest_prepare_queue_capacity(effective_prepare_workers)
    queue_max_bytes = ingest_prepare_queue_max_bytes()
    spill_threshold_bytes = ingest_prepare_spill_threshold_bytes()
    benchmark_mark(
        config_benchmark_name,
        prepare_workers=effective_prepare_workers,
        max_prepared_items=max_prepared_items,
        queue_max_bytes=queue_max_bytes,
        spill_threshold_bytes=spill_threshold_bytes,
    )
    own_staging_dir = staging_root is None
    spill_dir = (
        Path(tempfile.mkdtemp(prefix="retriever-ingest-prepared-"))
        if staging_root is None
        else Path(staging_root) / spill_subdir_name
    )
    ready_entries_by_index: dict[int, dict[str, object]] = {}
    ready_bytes_in_memory = 0
    peak_ready_bytes_in_memory = 0
    spilled_items = 0
    spilled_bytes = 0
    try:
        with ThreadPoolExecutor(max_workers=effective_prepare_workers) as executor:
            pending_by_index: dict[int, object] = {}
            future_to_index: dict[object, int] = {}
            next_yield_index = 0
            item_iterator = enumerate(items)
            input_exhausted = False

            def submit_available() -> None:
                nonlocal input_exhausted
                while (
                    not input_exhausted
                    and len(pending_by_index) + len(ready_entries_by_index) < max_prepared_items
                ):
                    try:
                        prepared_index, item = next(item_iterator)
                    except StopIteration:
                        input_exhausted = True
                        break
                    future = executor.submit(
                        prepare_item,
                        item,
                    )
                    pending_by_index[prepared_index] = future
                    future_to_index[future] = prepared_index

            def record_completed_future(future) -> None:
                nonlocal ready_bytes_in_memory, peak_ready_bytes_in_memory, spilled_items, spilled_bytes
                prepared_index = future_to_index.pop(future)
                pending_by_index.pop(prepared_index, None)
                prepared_item = future.result()
                serialized_payload = serialize_prepared_ingest_item(prepared_item)
                serialized_size = len(serialized_payload)
                should_spill = (
                    serialized_size >= spill_threshold_bytes
                    or ready_bytes_in_memory + serialized_size > queue_max_bytes
                )
                if should_spill:
                    spill_path = stage_prepared_ingest_item(spill_dir, prepared_index, serialized_payload)
                    ready_entries_by_index[prepared_index] = {
                        "storage": "spill",
                        "spill_path": spill_path,
                        "serialized_size": serialized_size,
                    }
                    spilled_items += 1
                    spilled_bytes += serialized_size
                    return
                ready_entries_by_index[prepared_index] = {
                    "storage": "memory",
                    "prepared_item": prepared_item,
                    "serialized_size": serialized_size,
                }
                ready_bytes_in_memory += serialized_size
                peak_ready_bytes_in_memory = max(peak_ready_bytes_in_memory, ready_bytes_in_memory)

            submit_available()
            while next_yield_index in ready_entries_by_index or pending_by_index or not input_exhausted:
                wait_started = time.perf_counter()
                while next_yield_index not in ready_entries_by_index:
                    if not pending_by_index and input_exhausted:
                        return
                    if not pending_by_index:
                        submit_available()
                        if not pending_by_index and input_exhausted:
                            return
                        if not pending_by_index:
                            raise RetrieverError(
                                f"Prepared ingest queue drained before index {next_yield_index} was ready."
                            )
                    done, _ = wait(list(pending_by_index.values()), return_when=FIRST_COMPLETED)
                    for future in done:
                        record_completed_future(future)
                    submit_available()
                wait_ms = (time.perf_counter() - wait_started) * 1000.0
                entry = ready_entries_by_index.pop(next_yield_index)
                if entry["storage"] == "spill":
                    prepared_item = hydrate_staged_prepared_ingest_item(Path(entry["spill_path"]))
                else:
                    prepared_item = dict(entry["prepared_item"])
                    ready_bytes_in_memory = max(0, ready_bytes_in_memory - int(entry["serialized_size"]))
                yield prepared_item, wait_ms
                next_yield_index += 1
                submit_available()
    finally:
        benchmark_mark(
            queue_done_benchmark_name,
            spilled_items=spilled_items,
            spilled_bytes=spilled_bytes,
            peak_ready_bytes=peak_ready_bytes_in_memory,
            spill_dir=str(spill_dir),
        )
        if own_staging_dir:
            remove_directory_tree(spill_dir)


def prepare_loose_file_item(item: dict[str, object]) -> dict[str, object]:
    prepared_item = dict(item)
    prepare_started = time.perf_counter()
    try:
        extracted_payload = extract_document(Path(item["path"]), include_attachments=True)
        attachments = list(extracted_payload.get("attachments", []))
        extracted_payload = dict(extracted_payload)
        extracted_payload.pop("attachments", None)
        chunk_started = time.perf_counter()
        prepared_chunks = extracted_search_chunks(extracted_payload)
        prepared_item["extracted_payload"] = extracted_payload
        prepared_item["attachments"] = attachments
        prepared_item["prepared_chunks"] = prepared_chunks
        prepared_item["prepare_chunk_ms"] = (time.perf_counter() - chunk_started) * 1000.0
        prepared_item["prepare_error"] = None
    except Exception as exc:
        prepared_item["extracted_payload"] = None
        prepared_item["attachments"] = []
        prepared_item["prepared_chunks"] = []
        prepared_item["prepare_chunk_ms"] = 0.0
        prepared_item["prepare_error"] = f"{type(exc).__name__}: {exc}"
    prepared_item["prepare_ms"] = (time.perf_counter() - prepare_started) * 1000.0
    return prepared_item


def refresh_prepared_loose_file_item_if_stale(
    prepared_item: dict[str, object],
) -> tuple[dict[str, object], bool]:
    path = Path(prepared_item["path"])
    current_file_size, current_file_mtime_ns = source_file_snapshot(path)
    if (
        current_file_size == prepared_item.get("source_file_size")
        and current_file_mtime_ns == prepared_item.get("source_file_mtime_ns")
    ):
        return prepared_item, False
    benchmark_mark(
        "ingest_loose_file_freshness_fallback",
        rel_path=str(prepared_item["rel_path"]),
        planned_file_size=prepared_item.get("source_file_size"),
        current_file_size=current_file_size,
        planned_file_mtime_ns=prepared_item.get("source_file_mtime_ns"),
        current_file_mtime_ns=current_file_mtime_ns,
    )
    refreshed_item = refresh_ingest_item_filesystem_facts(prepared_item)
    return prepare_loose_file_item(refreshed_item), True


def iter_prepared_loose_file_items(
    loose_file_items: list[dict[str, object]],
    staging_root: Path | None = None,
) -> Iterator[tuple[dict[str, object], float]]:
    yield from iter_staged_prepared_items(
        loose_file_items,
        prepare_item=prepare_loose_file_item,
        config_benchmark_name="ingest_loose_prepare_config",
        queue_done_benchmark_name="ingest_loose_prepare_queue_done",
        spill_subdir_name="prepared-loose",
        staging_root=staging_root,
    )


def commit_prepared_loose_file(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    prepared_item: dict[str, object],
    existing_by_rel: dict[str, sqlite3.Row],
    unseen_existing_by_hash: dict[str, list[sqlite3.Row]],
    ensure_filesystem_dataset,
    current_ingestion_batch: int | None,
) -> dict[str, object]:
    prepared_item, freshness_fallback = refresh_prepared_loose_file_item_if_stale(prepared_item)
    rel_path = str(prepared_item["rel_path"])
    path = Path(prepared_item["path"])
    file_type = str(prepared_item["file_type"])
    file_hash = prepared_item.get("file_hash")
    existing_occurrence_row = existing_by_rel.get(rel_path)
    action = "new"
    if existing_occurrence_row is not None:
        existing_row = connection.execute(
            "SELECT * FROM documents WHERE id = ?",
            (existing_occurrence_row["document_id"],),
        ).fetchone()
        if existing_row is None:
            raise RetrieverError(f"Occurrence {existing_occurrence_row['id']} points at a missing document.")
        if (
            existing_occurrence_row["file_hash"] == file_hash
            and existing_occurrence_row["lifecycle_status"] == ACTIVE_OCCURRENCE_STATUS
            and document_row_has_seeded_text_revisions(existing_row)
            and document_row_has_email_threading(connection, existing_row)
        ):
            filesystem_dataset_id, filesystem_dataset_source_id = ensure_filesystem_dataset()
            connection.execute("BEGIN")
            try:
                mark_seen_without_reingest(
                    connection,
                    existing_occurrence_row,
                    dataset_id=filesystem_dataset_id,
                    dataset_source_id=filesystem_dataset_source_id,
                )
                connection.commit()
                return {
                    "action": "skipped",
                    "current_ingestion_batch": current_ingestion_batch,
                    "freshness_fallback": freshness_fallback,
                }
            except Exception:
                connection.rollback()
                raise
        action = "updated"
    else:
        existing_row = None
        rename_candidates = unseen_existing_by_hash.get(str(file_hash)) or []
        if rename_candidates:
            existing_occurrence_row = rename_candidates.pop(0)
            action = "renamed"

    prepare_error = prepared_item.get("prepare_error")
    if prepare_error:
        return {
            "action": "failed",
            "current_ingestion_batch": current_ingestion_batch,
            "error": str(prepare_error),
            "freshness_fallback": freshness_fallback,
        }

    if existing_occurrence_row is None and file_hash:
        exact_duplicate_document = get_document_by_dedupe_key(
            connection,
            basis="file_hash",
            key_value=str(file_hash),
        )
        if exact_duplicate_document is not None:
            filesystem_dataset_id, filesystem_dataset_source_id = ensure_filesystem_dataset()
            connection.execute("BEGIN")
            try:
                now = utc_now()
                duplicate_occurrence_id = attach_occurrence_to_existing_document(
                    connection,
                    exact_duplicate_document,
                    existing_occurrence_row=None,
                    rel_path=rel_path,
                    file_name=path.name,
                    file_type=file_type,
                    file_size=path.stat().st_size,
                    file_hash=str(file_hash),
                    source_kind=FILESYSTEM_SOURCE_KIND,
                    source_rel_path=rel_path,
                    source_item_id=None,
                    source_folder_path=None,
                    custodian=infer_source_custodian(
                        source_kind=FILESYSTEM_SOURCE_KIND,
                        source_rel_path=rel_path,
                    ),
                    occurrence_control_number=str(exact_duplicate_document["control_number"] or ""),
                    ingested_at=now,
                    last_seen_at=now,
                    updated_at=now,
                )
                clone_duplicate_family_child_occurrences(
                    connection,
                    paths,
                    parent_document_id=int(exact_duplicate_document["id"]),
                    parent_occurrence_id=duplicate_occurrence_id,
                    parent_rel_path=rel_path,
                    custodian=infer_source_custodian(
                        source_kind=FILESYSTEM_SOURCE_KIND,
                        source_rel_path=rel_path,
                    ),
                    ingested_at=now,
                    last_seen_at=now,
                    updated_at=now,
                )
                ensure_dataset_document_membership(
                    connection,
                    dataset_id=filesystem_dataset_id,
                    document_id=int(exact_duplicate_document["id"]),
                    dataset_source_id=filesystem_dataset_source_id,
                )
                connection.commit()
                return {
                    "action": "new",
                    "current_ingestion_batch": current_ingestion_batch,
                }
            except Exception:
                connection.rollback()
                raise

    filesystem_dataset_id, filesystem_dataset_source_id = ensure_filesystem_dataset()
    connection.execute("BEGIN")
    try:
        existing_row = None
        reused_existing_occurrence_row = existing_occurrence_row
        superseded_document_id: int | None = None
        if existing_occurrence_row is not None:
            existing_row = connection.execute(
                "SELECT * FROM documents WHERE id = ?",
                (existing_occurrence_row["document_id"],),
            ).fetchone()
            if existing_row is None:
                raise RetrieverError(f"Occurrence {existing_occurrence_row['id']} points at a missing document.")
            active_occurrence_rows = active_occurrence_rows_for_document(connection, int(existing_row["id"]))
            if (
                action == "updated"
                and len(active_occurrence_rows) > 1
                and existing_occurrence_row["file_hash"] != file_hash
            ):
                connection.execute(
                    """
                    UPDATE document_occurrences
                    SET lifecycle_status = 'superseded', updated_at = ?
                    WHERE id = ?
                    """,
                    (utc_now(), existing_occurrence_row["id"]),
                )
                superseded_document_id = int(existing_row["id"])
                refresh_source_backed_dataset_memberships_for_document(connection, superseded_document_id)
                refresh_document_from_occurrences(connection, superseded_document_id)
                existing_row = None
                reused_existing_occurrence_row = None
        extracted = apply_manual_locks(existing_row, dict(prepared_item["extracted_payload"] or {}))
        attachments = list(prepared_item.get("attachments", []))
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
            existing_occurrence_row=reused_existing_occurrence_row,
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
        replace_document_related_rows(
            connection,
            document_id,
            extracted | {"file_name": path.name},
            list(prepared_item.get("prepared_chunks", [])),
            preview_rows,
        )
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
        if superseded_document_id is not None and superseded_document_id != document_id:
            refresh_source_backed_dataset_memberships_for_document(connection, superseded_document_id)
            refresh_document_from_occurrences(connection, superseded_document_id)
        connection.commit()
        return {
            "action": action,
            "current_ingestion_batch": current_ingestion_batch,
            "freshness_fallback": freshness_fallback,
        }
    except Exception as exc:
        connection.rollback()
        return {
            "action": "failed",
            "current_ingestion_batch": current_ingestion_batch,
            "error": f"{type(exc).__name__}: {exc}",
            "freshness_fallback": freshness_fallback,
        }


def finalize_ingest_postpass(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    allowed_types: set[str] | None,
    scanned_rel_paths: set[str],
    scanned_pst_source_rel_paths: set[str],
    scanned_mbox_source_rel_paths: set[str],
    slack_day_documents_missing: int,
    stats: dict[str, int],
    pst_export_owned_rel_paths: set[str] | None = None,
) -> int:
    if pst_export_owned_rel_paths:
        connection.execute("BEGIN")
        try:
            retire_standalone_filesystem_documents_by_rel_paths(
                connection,
                paths,
                rel_paths=pst_export_owned_rel_paths,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
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
    return int(pruned_unused_filesystem_dataset)


def ingest_production(root: Path, production_root: Path | str) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    total_started = time.perf_counter()
    benchmark_mark("ingest_production_begin")
    with workspace_ingest_session(paths, command_name="ingest-production") as ingest_session:
        connection = connect_db(paths["db_path"])
        try:
            setup_started = time.perf_counter()
            apply_schema(connection, root)
            reconcile_custom_fields_registry(connection, repair=True)
            resolved_production_root = resolve_production_root_argument(root, production_root)
            benchmark_mark(
                "ingest_production_setup_done",
                setup_ms=round((time.perf_counter() - setup_started) * 1000.0, 3),
                production_root=str(resolved_production_root),
            )
            production_started = time.perf_counter()
            result = ingest_resolved_production_root(
                connection,
                paths,
                root,
                resolved_production_root,
                staging_root=Path(ingest_session["tmp_dir"]),
            )
            benchmark_mark(
                "ingest_production_done",
                production_ms=round((time.perf_counter() - production_started) * 1000.0, 3),
                total_ms=round((time.perf_counter() - total_started) * 1000.0, 3),
                created=int(result.get("created") or 0),
                updated=int(result.get("updated") or 0),
                failed=len(list(result.get("failures", []))),
            )
            return result
        except Exception as exc:
            benchmark_mark(
                "ingest_production_failed",
                total_ms=round((time.perf_counter() - total_started) * 1000.0, 3),
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        finally:
            connection.close()


def ingest(root: Path, recursive: bool, raw_file_types: str | None) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    allowed_types = parse_file_types(raw_file_types)
    total_started = time.perf_counter()
    benchmark_mark(
        "ingest_begin",
        recursive=recursive,
        file_type_filter_count=(len(allowed_types) if allowed_types is not None else 0),
    )
    with workspace_ingest_session(paths, command_name="ingest") as ingest_session:
        connection = connect_db(paths["db_path"])
        try:
            setup_started = time.perf_counter()
            apply_schema(connection, root)
            reconcile_custom_fields_registry(connection, repair=True)
            benchmark_mark(
                "ingest_setup_done",
                setup_ms=round((time.perf_counter() - setup_started) * 1000.0, 3),
            )
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

            scan_started = time.perf_counter()
            ingest_plan = plan_ingest_work(root, recursive, allowed_types, connection)
            production_signatures = list(ingest_plan["production_signatures"])
            slack_export_descriptors = list(ingest_plan["slack_export_descriptors"])
            gmail_export_descriptors = list(ingest_plan["gmail_export_descriptors"])
            pst_export_descriptors = list(ingest_plan["pst_export_descriptors"])
            scanned_items = list(ingest_plan["scanned_items"])
            loose_file_items = list(ingest_plan["loose_file_items"])
            scanned_rel_paths = set(ingest_plan["scanned_rel_paths"])
            scanned_pst_source_rel_paths = set(ingest_plan["scanned_pst_source_rel_paths"])
            scanned_mbox_source_rel_paths = set(ingest_plan["scanned_mbox_source_rel_paths"])
            gmail_owned_rel_paths = set(ingest_plan["gmail_owned_rel_paths"])
            pst_export_owned_rel_paths = set(ingest_plan["pst_export_owned_rel_paths"])
            pst_export_descriptors_by_pst_path = dict(ingest_plan["pst_export_descriptors_by_pst_path"])
            benchmark_mark(
                "ingest_scan_done",
                scan_ms=round((time.perf_counter() - scan_started) * 1000.0, 3),
                hash_ms=round(float(ingest_plan["scan_hash_ms"]), 3),
                scanned_files=len(list(ingest_plan["scanned_files"])),
                production_roots=len(production_signatures),
                slack_export_roots=len(slack_export_descriptors),
                gmail_export_roots=len(gmail_export_descriptors),
                pst_export_roots=len(pst_export_descriptors),
            )

            stats = default_ingest_stats(len(slack_export_descriptors), len(gmail_export_descriptors))
            failures: list[dict[str, str]] = []
            special_source_state = ingest_serial_special_sources(
                connection,
                paths,
                root,
                Path(ingest_session["tmp_dir"]),
                allowed_types,
                production_signatures,
                slack_export_descriptors,
                gmail_export_descriptors,
                scanned_rel_paths,
                scanned_mbox_source_rel_paths,
                stats,
                failures,
            )
            current_ingestion_batch = special_source_state["current_ingestion_batch"]
            slack_day_documents_missing = int(special_source_state["slack_day_documents_missing"])
            ingested_production_roots = list(special_source_state["ingested_production_roots"])
            skipped_production_roots = list(special_source_state["skipped_production_roots"])
            warnings = list(special_source_state["warnings"])
            benchmark_mark(
                "ingest_special_sources_done",
                gmail_ms=round(float(special_source_state["gmail_ms"]), 3),
                slack_ms=round(float(special_source_state["slack_ms"]), 3),
                production_ms=round(float(special_source_state["production_ms"]), 3),
                source_failures=stats["failed"],
            )

            existing_by_rel, unseen_existing_by_hash = load_loose_file_commit_state(
                connection,
                scanned_rel_paths,
                gmail_owned_rel_paths,
                pst_export_owned_rel_paths,
            )

            loop_started = time.perf_counter()
            container_source_ms = 0.0
            container_prepare_ms = 0.0
            container_chunk_ms = 0.0
            container_prepare_wait_ms = 0.0
            container_commit_ms = 0.0
            loose_file_ms = 0.0
            loose_extract_ms = 0.0
            loose_chunk_ms = 0.0
            loose_prepare_wait_ms = 0.0
            loose_commit_ms = 0.0
            loose_freshness_fallbacks = 0
            prepared_loose_items = iter_prepared_loose_file_items(
                loose_file_items,
                Path(ingest_session["tmp_dir"]),
            )
            for item in scanned_items:
                rel_path = str(item["rel_path"])
                path = item["path"]
                file_type = str(item["file_type"])
                item_started = time.perf_counter()
                if file_type == PST_SOURCE_KIND:
                    try:
                        pst_export_descriptor = pst_export_descriptors_by_pst_path.get(path.resolve().as_posix())
                        pst_message_metadata_by_source_item = None
                        pst_message_match_records = None
                        pst_message_sidecar_hash = None
                        if pst_export_descriptor is not None:
                            pst_message_metadata_by_source_item = dict(
                                dict(pst_export_descriptor.get("message_metadata_by_pst_path") or {}).get(path.resolve().as_posix()) or {}
                            )
                            pst_message_match_records = list(
                                dict(pst_export_descriptor.get("message_match_records_by_pst_path") or {}).get(path.resolve().as_posix()) or []
                            )
                            pst_message_sidecar_hash = normalize_whitespace(
                                str(pst_export_descriptor.get("message_sidecar_hash") or "")
                            ) or None
                        pst_result = ingest_pst_source(
                            connection,
                            paths,
                            path,
                            rel_path,
                            message_metadata_by_source_item=pst_message_metadata_by_source_item,
                            message_match_records=pst_message_match_records,
                            message_sidecar_hash=pst_message_sidecar_hash,
                            staging_root=Path(ingest_session["tmp_dir"]),
                        )
                        stats[str(pst_result["action"])] += 1
                        stats["pst_sources_skipped"] += int(pst_result["pst_sources_skipped"])
                        stats["pst_messages_created"] += int(pst_result["pst_messages_created"])
                        stats["pst_messages_updated"] += int(pst_result["pst_messages_updated"])
                        stats["pst_messages_deleted"] += int(pst_result["pst_messages_deleted"])
                        container_prepare_ms += float(pst_result.get("pst_prepare_ms") or 0.0)
                        container_chunk_ms += float(pst_result.get("pst_chunk_ms") or 0.0)
                        container_prepare_wait_ms += float(pst_result.get("pst_prepare_wait_ms") or 0.0)
                        container_commit_ms += float(pst_result.get("pst_commit_ms") or 0.0)
                    except Exception as exc:
                        stats["failed"] += 1
                        failures.append({"rel_path": rel_path, "error": f"{type(exc).__name__}: {exc}"})
                    finally:
                        container_source_ms += (time.perf_counter() - item_started) * 1000.0
                    continue
                if file_type == MBOX_SOURCE_KIND:
                    try:
                        mbox_result = ingest_mbox_source(
                            connection,
                            paths,
                            path,
                            rel_path,
                            staging_root=Path(ingest_session["tmp_dir"]),
                        )
                        stats[str(mbox_result["action"])] += 1
                        stats["mbox_sources_skipped"] += int(mbox_result["mbox_sources_skipped"])
                        stats["mbox_messages_created"] += int(mbox_result["mbox_messages_created"])
                        stats["mbox_messages_updated"] += int(mbox_result["mbox_messages_updated"])
                        stats["mbox_messages_deleted"] += int(mbox_result["mbox_messages_deleted"])
                        container_prepare_ms += float(mbox_result.get("mbox_prepare_ms") or 0.0)
                        container_chunk_ms += float(mbox_result.get("mbox_chunk_ms") or 0.0)
                        container_prepare_wait_ms += float(mbox_result.get("mbox_prepare_wait_ms") or 0.0)
                        container_commit_ms += float(mbox_result.get("mbox_commit_ms") or 0.0)
                    except Exception as exc:
                        stats["failed"] += 1
                        failures.append({"rel_path": rel_path, "error": f"{type(exc).__name__}: {exc}"})
                    finally:
                        container_source_ms += (time.perf_counter() - item_started) * 1000.0
                    continue
                try:
                    prepared_item, wait_ms = next(prepared_loose_items)
                    if str(prepared_item["rel_path"]) != rel_path:
                        raise RetrieverError(
                            f"Prepared loose-file order drifted: expected {rel_path}, got {prepared_item['rel_path']}"
                        )
                    loose_prepare_wait_ms += wait_ms
                    loose_extract_ms += float(prepared_item["prepare_ms"])
                    loose_chunk_ms += float(prepared_item.get("prepare_chunk_ms") or 0.0)
                    commit_started = time.perf_counter()
                    commit_result = commit_prepared_loose_file(
                        connection,
                        paths,
                        prepared_item,
                        existing_by_rel,
                        unseen_existing_by_hash,
                        ensure_filesystem_dataset,
                        current_ingestion_batch,
                    )
                    loose_commit_ms += (time.perf_counter() - commit_started) * 1000.0
                    current_ingestion_batch = commit_result["current_ingestion_batch"]
                    if bool(commit_result.get("freshness_fallback")):
                        loose_freshness_fallbacks += 1
                    action = str(commit_result["action"])
                    if action == "failed":
                        stats["failed"] += 1
                        failures.append({"rel_path": rel_path, "error": str(commit_result["error"])})
                    else:
                        stats[action] += 1
                finally:
                    loose_file_ms += (time.perf_counter() - item_started) * 1000.0
            benchmark_mark(
                "ingest_item_loop_done",
                loop_ms=round((time.perf_counter() - loop_started) * 1000.0, 3),
                loose_file_ms=round(loose_file_ms, 3),
                loose_extract_ms=round(loose_extract_ms, 3),
                loose_chunk_ms=round(loose_chunk_ms, 3),
                loose_prepare_wait_ms=round(loose_prepare_wait_ms, 3),
                loose_commit_ms=round(loose_commit_ms, 3),
                loose_freshness_fallbacks=loose_freshness_fallbacks,
                container_source_ms=round(container_source_ms, 3),
                container_prepare_ms=round(container_prepare_ms, 3),
                container_chunk_ms=round(container_chunk_ms, 3),
                container_prepare_wait_ms=round(container_prepare_wait_ms, 3),
                container_commit_ms=round(container_commit_ms, 3),
            )

            postpass_started = time.perf_counter()
            pruned_unused_filesystem_dataset = finalize_ingest_postpass(
                connection,
                paths,
                allowed_types,
                scanned_rel_paths,
                scanned_pst_source_rel_paths,
                scanned_mbox_source_rel_paths,
                slack_day_documents_missing,
                stats,
                pst_export_owned_rel_paths=pst_export_owned_rel_paths,
            )
            benchmark_mark(
                "ingest_postpass_done",
                postpass_ms=round((time.perf_counter() - postpass_started) * 1000.0, 3),
                missing=stats["missing"],
                email_conversations=stats["email_conversations"],
                pst_chat_conversations=stats["pst_chat_conversations"],
            )
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
            benchmark_mark(
                "ingest_done",
                total_ms=round((time.perf_counter() - total_started) * 1000.0, 3),
                scanned=result["scanned"],
                new=stats["new"],
                updated=stats["updated"],
                failed=stats["failed"],
            )
            return result
        except Exception as exc:
            benchmark_mark(
                "ingest_failed",
                total_ms=round((time.perf_counter() - total_started) * 1000.0, 3),
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
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


def row_to_plain_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def normalize_merge_field_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = normalize_whitespace(value)
        return normalized or None
    return value


def serialize_merge_field_value(value: object) -> object:
    normalized = normalize_merge_field_value(value)
    if normalized is None:
        return None
    if isinstance(normalized, (str, int, float, bool)):
        return normalized
    return json.dumps(normalized, ensure_ascii=True, sort_keys=True)


def merge_field_value_key(value: object) -> str | None:
    normalized = normalize_merge_field_value(value)
    if normalized is None:
        return None
    return json.dumps(normalized, ensure_ascii=True, sort_keys=True)


def reconcile_custom_field_names(connection: sqlite3.Connection) -> list[str]:
    document_columns = table_columns(connection, "documents")
    rows = connection.execute(
        """
        SELECT field_name
        FROM custom_fields_registry
        ORDER BY field_name ASC
        """
    ).fetchall()
    return [str(row["field_name"]) for row in rows if str(row["field_name"]) in document_columns]


def document_has_non_deleted_children(connection: sqlite3.Connection, document_id: int) -> list[int]:
    rows = connection.execute(
        """
        SELECT id
        FROM documents
        WHERE parent_document_id = ?
          AND lifecycle_status != 'deleted'
        ORDER BY id ASC
        """,
        (document_id,),
    ).fetchall()
    return [int(row["id"]) for row in rows]


def document_text_length(connection: sqlite3.Connection, document_id: int) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(char_end), 0) AS text_length
        FROM document_chunks
        WHERE document_id = ?
        """,
        (document_id,),
    ).fetchone()
    if row is None:
        return 0
    return int(row["text_length"] or 0)


def document_active_occurrence_count(connection: sqlite3.Connection, document_id: int) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS occurrence_count
        FROM document_occurrences
        WHERE document_id = ?
          AND lifecycle_status = ?
        """,
        (document_id, ACTIVE_OCCURRENCE_STATUS),
    ).fetchone()
    if row is None:
        return 0
    return int(row["occurrence_count"] or 0)


def document_earliest_active_occurrence_ingested_at(
    connection: sqlite3.Connection,
    document_id: int,
) -> datetime:
    row = connection.execute(
        """
        SELECT MIN(ingested_at) AS earliest_ingested_at
        FROM document_occurrences
        WHERE document_id = ?
          AND lifecycle_status = ?
        """,
        (document_id, ACTIVE_OCCURRENCE_STATUS),
    ).fetchone()
    parsed = parse_utc_timestamp(row["earliest_ingested_at"]) if row is not None else None
    return parsed or datetime.max.replace(tzinfo=timezone.utc)


def document_canonical_field_count(row: sqlite3.Row | dict[str, object]) -> int:
    field_names = [
        "author",
        "content_type",
        "date_created",
        "date_modified",
        "page_count",
        "participants",
        "recipients",
        "subject",
        "title",
    ]
    return sum(1 for field_name in field_names if normalize_merge_field_value(row[field_name]) is not None)


def collect_distinct_document_field_values(
    document_rows: list[sqlite3.Row],
    field_name: str,
) -> list[dict[str, object]]:
    distinct_values: dict[str, dict[str, object]] = {}
    for row in document_rows:
        value = normalize_merge_field_value(row[field_name])
        if value is None:
            continue
        value_key = merge_field_value_key(value)
        assert value_key is not None
        entry = distinct_values.setdefault(
            value_key,
            {
                "value": value,
                "document_ids": [],
                "locked_document_ids": [],
            },
        )
        entry["document_ids"].append(int(row["id"]))
        if field_name in normalize_string_list(row[MANUAL_FIELD_LOCKS_COLUMN]):
            entry["locked_document_ids"].append(int(row["id"]))
    return list(distinct_values.values())


def choose_reconcile_survivor(
    connection: sqlite3.Connection,
    document_rows: list[sqlite3.Row],
) -> sqlite3.Row:
    ranked_rows = sorted(
        document_rows,
        key=lambda row: (
            text_status_priority(row["text_status"]),
            -document_canonical_field_count(row),
            -document_text_length(connection, int(row["id"])),
            -document_active_occurrence_count(connection, int(row["id"])),
            document_earliest_active_occurrence_ingested_at(connection, int(row["id"])),
            int(row["id"]),
        ),
    )
    return ranked_rows[0]


def document_artifact_counts(connection: sqlite3.Connection, document_id: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in (
        "document_chunks",
        "chunks_fts",
        "document_previews",
        "document_source_parts",
        "documents_fts",
        "text_revisions",
    ):
        row = connection.execute(
            f"SELECT COUNT(*) AS row_count FROM {table_name} WHERE document_id = ?",
            (document_id,),
        ).fetchone()
        counts[table_name] = int(row["row_count"] or 0) if row is not None else 0
    return counts


def document_merge_snapshot(connection: sqlite3.Connection, document_id: int) -> dict[str, object]:
    document_row = connection.execute(
        "SELECT * FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    occurrence_rows = connection.execute(
        """
        SELECT *
        FROM document_occurrences
        WHERE document_id = ?
        ORDER BY id ASC
        """,
        (document_id,),
    ).fetchall()
    dataset_rows = connection.execute(
        """
        SELECT *
        FROM dataset_documents
        WHERE document_id = ?
        ORDER BY dataset_id ASC, COALESCE(dataset_source_id, 0) ASC, id ASC
        """,
        (document_id,),
    ).fetchall()
    alias_rows = connection.execute(
        """
        SELECT *
        FROM document_control_number_aliases
        WHERE document_id = ?
        ORDER BY id ASC
        """,
        (document_id,),
    ).fetchall()
    text_revision_rows = connection.execute(
        """
        SELECT *
        FROM text_revisions
        WHERE document_id = ?
        ORDER BY id ASC
        """,
        (document_id,),
    ).fetchall()
    return {
        "schema_version": SCHEMA_VERSION,
        "document": row_to_plain_dict(document_row),
        "occurrences": [row_to_plain_dict(row) for row in occurrence_rows],
        "dataset_memberships": [row_to_plain_dict(row) for row in dataset_rows],
        "control_number_aliases": [row_to_plain_dict(row) for row in alias_rows],
        "text_revisions": [row_to_plain_dict(row) for row in text_revision_rows],
    }


def insert_document_merge_event(
    connection: sqlite3.Connection,
    *,
    survivor_document_id: int,
    loser_document_id: int,
    merge_basis: str,
    pre_merge_survivor_snapshot: dict[str, object],
    pre_merge_loser_snapshot: dict[str, object],
    artifact_counts: dict[str, object],
) -> int:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO document_merge_events (
          survivor_document_id, loser_document_id, merge_basis, actor, schema_version,
          pre_merge_survivor_json, pre_merge_loser_json, artifact_counts_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            survivor_document_id,
            loser_document_id,
            merge_basis,
            "reconcile-duplicates",
            SCHEMA_VERSION,
            json.dumps(pre_merge_survivor_snapshot, ensure_ascii=True, sort_keys=True),
            json.dumps(pre_merge_loser_snapshot, ensure_ascii=True, sort_keys=True),
            json.dumps(artifact_counts, ensure_ascii=True, sort_keys=True),
            now,
        ),
    )
    return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])


def insert_document_field_conflicts(
    connection: sqlite3.Connection,
    *,
    merge_event_id: int,
    survivor_document_id: int,
    final_survivor_row: sqlite3.Row,
    pre_merge_survivor_row: sqlite3.Row,
    pre_merge_loser_row: sqlite3.Row,
    custom_field_names: list[str],
) -> None:
    tracked_fields = sorted(set(custom_field_names) | set(EDITABLE_BUILTIN_FIELDS))
    rows_to_insert: list[tuple[int, int, str, object, object, str, str]] = []
    now = utc_now()
    for field_name in tracked_fields:
        pre_survivor_value = normalize_merge_field_value(pre_merge_survivor_row[field_name])
        loser_value = normalize_merge_field_value(pre_merge_loser_row[field_name])
        if pre_survivor_value == loser_value:
            continue
        final_value = normalize_merge_field_value(final_survivor_row[field_name])
        if final_value == loser_value and loser_value is not None:
            resolution = "adopted_loser"
        elif final_value == pre_survivor_value:
            resolution = "kept_survivor"
        else:
            resolution = "recomputed"
        rows_to_insert.append(
            (
                merge_event_id,
                survivor_document_id,
                field_name,
                serialize_merge_field_value(pre_survivor_value),
                serialize_merge_field_value(loser_value),
                resolution,
                now,
            )
        )
    if rows_to_insert:
        connection.executemany(
            """
            INSERT INTO document_field_conflicts (
              merge_event_id, document_id, field_name, survivor_value, loser_value, resolution, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )


def tombstone_rel_path_for_merged_document(document_id: int) -> str:
    return (Path(".retriever") / "merged" / f"{document_id}.merged").as_posix()


def active_child_rows_for_parent_document(
    connection: sqlite3.Connection,
    parent_document_id: int,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM documents
        WHERE parent_document_id = ?
          AND lifecycle_status = 'active'
        ORDER BY id ASC
        """,
        (parent_document_id,),
    ).fetchall()


def family_child_signature_payload(row: sqlite3.Row) -> dict[str, object]:
    canonical_kind = normalize_whitespace(str(row["canonical_kind"] or "")).lower()
    return {
        "child_document_kind": normalize_whitespace(str(row["child_document_kind"] or "")).lower() or None,
        "file_name": normalize_whitespace(str(row["file_name"] or "")).lower() or None,
        "content_hash": normalize_whitespace(str(row["content_hash"] or "")) or None,
        "file_hash": normalize_whitespace(str(row["file_hash"] or "")) or None,
        "content_type": normalize_whitespace(str(row["content_type"] or "")) or None,
        "canonical_kind": canonical_kind if canonical_kind not in {"", "unknown"} else None,
    }


def family_child_signature_key(row: sqlite3.Row) -> str:
    return sha256_json_value(family_child_signature_payload(row))


def document_family_descriptor(
    connection: sqlite3.Connection,
    document_row: sqlite3.Row,
) -> dict[str, object]:
    child_rows = active_child_rows_for_parent_document(connection, int(document_row["id"]))
    ordered_children = sorted(
        child_rows,
        key=lambda row: (
            family_child_signature_key(row),
            int(row["id"]),
        ),
    )
    child_slot_keys = [family_child_signature_key(row) for row in ordered_children]
    return {
        "document_id": int(document_row["id"]),
        "fingerprint": sha256_json_value({"child_slots": child_slot_keys}),
        "child_rows": ordered_children,
        "child_slot_keys": child_slot_keys,
    }


def survivor_selection_payload(
    connection: sqlite3.Connection,
    survivor_row: sqlite3.Row,
    *,
    rule: str,
) -> dict[str, object]:
    survivor_id = int(survivor_row["id"])
    return {
        "rule": rule,
        "text_status": survivor_row["text_status"],
        "canonical_field_count": document_canonical_field_count(survivor_row),
        "text_length": document_text_length(connection, survivor_id),
        "active_occurrence_count": document_active_occurrence_count(connection, survivor_id),
        "earliest_active_ingested_at": format_utc_timestamp(
            document_earliest_active_occurrence_ingested_at(connection, survivor_id)
        ),
    }


def evaluate_document_merge_group(
    connection: sqlite3.Connection,
    document_rows: list[sqlite3.Row],
    *,
    custom_fields: list[str],
    forced_survivor_document_id: int | None = None,
    selection_rule: str | None = None,
) -> dict[str, object]:
    if forced_survivor_document_id is None:
        survivor_row = choose_reconcile_survivor(connection, document_rows)
        resolved_selection_rule = selection_rule or "text_status>field_count>text_length>occurrence_count>earliest_ingested_at>document_id"
    else:
        survivor_row = next(
            (row for row in document_rows if int(row["id"]) == int(forced_survivor_document_id)),
            None,
        )
        if survivor_row is None:
            raise RetrieverError(f"Forced survivor document id {forced_survivor_document_id} is not present in the merge group.")
        resolved_selection_rule = selection_rule or "family_slot_survivor"

    survivor_id = int(survivor_row["id"])
    loser_document_ids = [int(row["id"]) for row in document_rows if int(row["id"]) != survivor_id]
    survivor_locks = set(normalize_string_list(survivor_row[MANUAL_FIELD_LOCKS_COLUMN]))

    blockers: list[dict[str, object]] = []
    machine_field_conflicts: list[dict[str, object]] = []
    custom_field_updates: dict[str, object] = {}
    builtin_field_updates: dict[str, object] = {}
    fields_to_lock: set[str] = set(survivor_locks)

    non_unknown_kinds = sorted(
        {
            normalize_whitespace(str(row["canonical_kind"] or "")).lower()
            for row in document_rows
            if normalize_whitespace(str(row["canonical_kind"] or "")).lower() not in {"", "unknown"}
        }
    )
    if len(non_unknown_kinds) > 1:
        blockers.append(
            {
                "type": "canonical_kind_conflict",
                "canonical_kinds": non_unknown_kinds,
                "document_ids": [int(row["id"]) for row in document_rows],
            }
        )

    for field_name in custom_fields:
        distinct_values = collect_distinct_document_field_values(document_rows, field_name)
        if len(distinct_values) > 1:
            blockers.append(
                {
                    "type": "custom_field_conflict",
                    "field_name": field_name,
                    "values": [
                        {
                            "value": serialize_merge_field_value(item["value"]),
                            "document_ids": item["document_ids"],
                        }
                        for item in distinct_values
                    ],
                }
            )
            continue
        if not distinct_values:
            continue
        chosen_value = distinct_values[0]["value"]
        if normalize_merge_field_value(survivor_row[field_name]) is None:
            custom_field_updates[field_name] = chosen_value
        if field_name in survivor_locks or distinct_values[0]["locked_document_ids"]:
            fields_to_lock.add(field_name)

    for field_name in sorted(EDITABLE_BUILTIN_FIELDS):
        distinct_values = collect_distinct_document_field_values(document_rows, field_name)
        if len(distinct_values) > 1 and any(item["locked_document_ids"] for item in distinct_values):
            blockers.append(
                {
                    "type": "locked_builtin_conflict",
                    "field_name": field_name,
                    "values": [
                        {
                            "value": serialize_merge_field_value(item["value"]),
                            "document_ids": item["document_ids"],
                            "locked_document_ids": item["locked_document_ids"],
                        }
                        for item in distinct_values
                    ],
                }
            )
            continue
        if len(distinct_values) > 1:
            machine_field_conflicts.append(
                {
                    "field_name": field_name,
                    "values": [
                        {
                            "value": serialize_merge_field_value(item["value"]),
                            "document_ids": item["document_ids"],
                        }
                        for item in distinct_values
                    ],
                }
            )
        if not distinct_values:
            continue
        chosen_value = (
            distinct_values[0]["value"]
            if len(distinct_values) == 1
            else normalize_merge_field_value(survivor_row[field_name])
        )
        field_should_lock = field_name in survivor_locks or (len(distinct_values) == 1 and bool(distinct_values[0]["locked_document_ids"]))
        if field_should_lock:
            fields_to_lock.add(field_name)
            if chosen_value is not None:
                builtin_field_updates[field_name] = chosen_value
            continue
        if (
            field_name == "page_count"
            and len(distinct_values) == 1
            and normalize_merge_field_value(survivor_row[field_name]) is None
            and chosen_value is not None
        ):
            builtin_field_updates[field_name] = chosen_value

    return {
        "document_ids": [int(row["id"]) for row in document_rows],
        "survivor_document_id": survivor_id,
        "loser_document_ids": loser_document_ids,
        "status": "blocked" if blockers else "ready",
        "blocking_conflicts": blockers,
        "machine_field_conflicts": machine_field_conflicts,
        "survivor_selection": survivor_selection_payload(
            connection,
            survivor_row,
            rule=resolved_selection_rule,
        ),
        "_custom_field_names": custom_fields,
        "_custom_field_updates": custom_field_updates,
        "_builtin_field_updates": builtin_field_updates,
        "_fields_to_lock": sorted(fields_to_lock),
    }


def evaluate_reconcile_candidate_group(
    connection: sqlite3.Connection,
    document_rows: list[sqlite3.Row],
) -> dict[str, object]:
    custom_fields = reconcile_custom_field_names(connection)
    root_group = evaluate_document_merge_group(
        connection,
        document_rows,
        custom_fields=custom_fields,
    )
    survivor_document_id = int(root_group["survivor_document_id"])
    family_descriptors = {
        int(row["id"]): document_family_descriptor(connection, row)
        for row in document_rows
    }
    survivor_family = family_descriptors[survivor_document_id]
    child_merge_groups: list[dict[str, object]] = []
    child_blockers: list[dict[str, object]] = []

    for slot_index, survivor_child_row in enumerate(list(survivor_family["child_rows"])):
        child_group_rows = [
            family_descriptors[int(row["id"])]["child_rows"][slot_index]
            for row in document_rows
        ]
        child_group = evaluate_document_merge_group(
            connection,
            child_group_rows,
            custom_fields=custom_fields,
            forced_survivor_document_id=int(survivor_child_row["id"]),
            selection_rule=f"family_slot[{slot_index}]",
        )
        child_merge_groups.append(child_group)
        if child_group["status"] != "ready":
            child_blockers.append(
                {
                    "type": "family_child_conflict",
                    "slot_index": slot_index,
                    "survivor_document_id": int(survivor_child_row["id"]),
                    "child_document_ids": child_group["document_ids"],
                    "blocking_conflicts": child_group["blocking_conflicts"],
                }
            )

    blocking_conflicts = [*root_group["blocking_conflicts"], *child_blockers]
    root_group["content_hash"] = document_rows[0]["content_hash"] if document_rows else None
    root_group["family_fingerprint"] = survivor_family["fingerprint"]
    root_group["family_child_group_count"] = len(child_merge_groups)
    root_group["status"] = "blocked" if blocking_conflicts else "ready"
    root_group["blocking_conflicts"] = blocking_conflicts
    root_group["_child_merge_groups"] = child_merge_groups
    return root_group


def find_reconcile_candidate_groups(
    connection: sqlite3.Connection,
    *,
    basis: str,
) -> list[dict[str, object]]:
    if basis != "content_hash":
        raise RetrieverError(f"Unsupported reconciliation basis: {basis}")

    candidate_rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE canonical_status = ?
          AND lifecycle_status = 'active'
          AND parent_document_id IS NULL
          AND content_hash IS NOT NULL
          AND text_status IN ('ok', 'partial')
        ORDER BY content_hash ASC, id ASC
        """,
        (CANONICAL_STATUS_ACTIVE,),
    ).fetchall()

    groups_by_hash: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in candidate_rows:
        groups_by_hash[str(row["content_hash"])].append(row)

    candidate_groups: list[dict[str, object]] = []
    for content_hash, grouped_rows in sorted(groups_by_hash.items()):
        if len(grouped_rows) < 2:
            continue
        grouped_rows_by_family: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in grouped_rows:
            family_descriptor = document_family_descriptor(connection, row)
            grouped_rows_by_family[str(family_descriptor["fingerprint"])].append(row)
        for family_fingerprint, family_rows in sorted(grouped_rows_by_family.items()):
            if len(family_rows) < 2:
                continue
            candidate_group = evaluate_reconcile_candidate_group(connection, family_rows)
            candidate_group["content_hash"] = content_hash
            candidate_group["family_fingerprint"] = family_fingerprint
            candidate_groups.append(candidate_group)
    return candidate_groups


def rebind_document_dedupe_keys(
    connection: sqlite3.Connection,
    *,
    survivor_document_id: int,
    loser_document_id: int,
) -> None:
    rows = connection.execute(
        """
        SELECT basis, key_value
        FROM document_dedupe_keys
        WHERE document_id = ?
        ORDER BY basis ASC, key_value ASC
        """,
        (loser_document_id,),
    ).fetchall()
    for row in rows:
        bind_document_dedupe_key(
            connection,
            basis=str(row["basis"]),
            key_value=str(row["key_value"]),
            document_id=survivor_document_id,
        )
    connection.execute(
        "DELETE FROM document_dedupe_keys WHERE document_id = ?",
        (loser_document_id,),
    )


def transfer_manual_dataset_memberships(
    connection: sqlite3.Connection,
    *,
    survivor_document_id: int,
    loser_document_id: int,
) -> None:
    rows = connection.execute(
        """
        SELECT dataset_id
        FROM dataset_documents
        WHERE document_id = ?
          AND dataset_source_id IS NULL
        ORDER BY dataset_id ASC
        """,
        (loser_document_id,),
    ).fetchall()
    for row in rows:
        ensure_dataset_document_membership(
            connection,
            dataset_id=int(row["dataset_id"]),
            document_id=survivor_document_id,
            dataset_source_id=None,
        )


def apply_evaluated_document_merge_group(
    connection: sqlite3.Connection,
    *,
    paths: dict[str, Path],
    merge_basis: str,
    merge_group: dict[str, object],
) -> dict[str, object]:
    survivor_document_id = int(merge_group["survivor_document_id"])
    loser_document_ids = [int(document_id) for document_id in merge_group["loser_document_ids"]]
    custom_field_names = [str(field_name) for field_name in merge_group["_custom_field_names"]]
    custom_field_updates = dict(merge_group["_custom_field_updates"])
    builtin_field_updates = dict(merge_group["_builtin_field_updates"])
    fields_to_lock = list(merge_group["_fields_to_lock"])

    pending_conflicts: list[tuple[int, sqlite3.Row, sqlite3.Row]] = []
    merge_event_ids: list[int] = []
    moved_occurrence_count = 0

    for loser_document_id in loser_document_ids:
        survivor_row = connection.execute(
            "SELECT * FROM documents WHERE id = ?",
            (survivor_document_id,),
        ).fetchone()
        loser_row = connection.execute(
            "SELECT * FROM documents WHERE id = ?",
            (loser_document_id,),
        ).fetchone()
        if survivor_row is None or loser_row is None:
            raise RetrieverError(f"Unable to load merge pair survivor={survivor_document_id} loser={loser_document_id}.")

        merge_event_id = insert_document_merge_event(
            connection,
            survivor_document_id=survivor_document_id,
            loser_document_id=loser_document_id,
            merge_basis=merge_basis,
            pre_merge_survivor_snapshot=document_merge_snapshot(connection, survivor_document_id),
            pre_merge_loser_snapshot=document_merge_snapshot(connection, loser_document_id),
            artifact_counts={
                "survivor": document_artifact_counts(connection, survivor_document_id),
                "loser": document_artifact_counts(connection, loser_document_id),
            },
        )
        merge_event_ids.append(merge_event_id)
        pending_conflicts.append((merge_event_id, survivor_row, loser_row))

        transfer_manual_dataset_memberships(
            connection,
            survivor_document_id=survivor_document_id,
            loser_document_id=loser_document_id,
        )
        rebind_document_dedupe_keys(
            connection,
            survivor_document_id=survivor_document_id,
            loser_document_id=loser_document_id,
        )
        occurrence_rows = connection.execute(
            """
            SELECT id
            FROM document_occurrences
            WHERE document_id = ?
            ORDER BY id ASC
            """,
            (loser_document_id,),
        ).fetchall()
        moved_occurrence_count += len(occurrence_rows)
        connection.execute(
            """
            UPDATE document_occurrences
            SET document_id = ?, updated_at = ?
            WHERE document_id = ?
            """,
            (survivor_document_id, utc_now(), loser_document_id),
        )
        connection.execute(
            "DELETE FROM dataset_documents WHERE document_id = ?",
            (loser_document_id,),
        )
        connection.execute(
            "DELETE FROM document_control_number_aliases WHERE document_id IN (?, ?)",
            (survivor_document_id, loser_document_id),
        )
        connection.execute(
            "DELETE FROM canonical_metadata_conflicts WHERE document_id = ?",
            (loser_document_id,),
        )
        cleanup_document_artifacts(paths, connection, loser_row)
        delete_document_related_rows(connection, loser_document_id)
        connection.execute(
            """
            UPDATE documents
            SET control_number = NULL,
                dataset_id = NULL,
                canonical_status = ?,
                merged_into_document_id = ?,
                rel_path = ?,
                lifecycle_status = 'deleted',
                updated_at = ?
            WHERE id = ?
            """,
            (
                CANONICAL_STATUS_MERGED,
                survivor_document_id,
                tombstone_rel_path_for_merged_document(loser_document_id),
                utc_now(),
                loser_document_id,
            ),
        )

    if custom_field_updates or builtin_field_updates or fields_to_lock:
        survivor_row = connection.execute(
            "SELECT * FROM documents WHERE id = ?",
            (survivor_document_id,),
        ).fetchone()
        if survivor_row is None:
            raise RetrieverError(f"Unknown survivor document id: {survivor_document_id}")
        update_fields: dict[str, object] = {}
        update_fields.update(custom_field_updates)
        update_fields.update(builtin_field_updates)
        if fields_to_lock:
            update_fields[MANUAL_FIELD_LOCKS_COLUMN] = json.dumps(sorted(dict.fromkeys(fields_to_lock)))
        if update_fields:
            update_fields["updated_at"] = utc_now()
            assignments = ", ".join(f"{quote_identifier(field_name)} = ?" for field_name in update_fields)
            connection.execute(
                f"UPDATE documents SET {assignments} WHERE id = ?",
                [*update_fields.values(), survivor_document_id],
            )

    refresh_source_backed_dataset_memberships_for_document(connection, survivor_document_id)
    refresh_document_from_occurrences(connection, survivor_document_id)

    final_survivor_row = connection.execute(
        "SELECT * FROM documents WHERE id = ?",
        (survivor_document_id,),
    ).fetchone()
    if final_survivor_row is None:
        raise RetrieverError(f"Unknown survivor document id after merge: {survivor_document_id}")
    for merge_event_id, pre_merge_survivor_row, pre_merge_loser_row in pending_conflicts:
        insert_document_field_conflicts(
            connection,
            merge_event_id=merge_event_id,
            survivor_document_id=survivor_document_id,
            final_survivor_row=final_survivor_row,
            pre_merge_survivor_row=pre_merge_survivor_row,
            pre_merge_loser_row=pre_merge_loser_row,
            custom_field_names=custom_field_names,
        )

    return {
        "survivor_document_id": survivor_document_id,
        "loser_document_ids": loser_document_ids,
        "merge_event_ids": merge_event_ids,
        "moved_occurrence_count": moved_occurrence_count,
    }


def apply_reconcile_group(
    connection: sqlite3.Connection,
    *,
    paths: dict[str, Path],
    basis: str,
    candidate_group: dict[str, object],
) -> dict[str, object]:
    root_merge_result = apply_evaluated_document_merge_group(
        connection,
        paths=paths,
        merge_basis=basis,
        merge_group=candidate_group,
    )
    child_merge_groups = list(candidate_group.get("_child_merge_groups") or [])
    child_merge_event_ids: list[int] = []
    child_moved_occurrence_count = 0
    for child_group in child_merge_groups:
        child_merge_result = apply_evaluated_document_merge_group(
            connection,
            paths=paths,
            merge_basis=f"{basis}:family_child",
            merge_group=child_group,
        )
        child_merge_event_ids.extend(list(child_merge_result["merge_event_ids"]))
        child_moved_occurrence_count += int(child_merge_result["moved_occurrence_count"])

    return {
        "content_hash": candidate_group["content_hash"],
        "document_ids": candidate_group["document_ids"],
        "survivor_document_id": root_merge_result["survivor_document_id"],
        "loser_document_ids": root_merge_result["loser_document_ids"],
        "status": "merged",
        "merge_event_ids": [*root_merge_result["merge_event_ids"], *child_merge_event_ids],
        "moved_occurrence_count": int(root_merge_result["moved_occurrence_count"]) + child_moved_occurrence_count,
        "blocking_conflicts": [],
        "machine_field_conflicts": candidate_group["machine_field_conflicts"],
        "survivor_selection": candidate_group["survivor_selection"],
        "family_child_group_count": len(child_merge_groups),
    }


def reconcile_duplicates(
    root: Path,
    *,
    basis: str,
    apply_changes: bool,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        candidate_groups = find_reconcile_candidate_groups(connection, basis=basis)
        if not apply_changes:
            return {
                "status": "ok",
                "basis": basis,
                "mode": "dry-run",
                "candidate_group_count": len(candidate_groups),
                "mergeable_group_count": sum(1 for group in candidate_groups if group["status"] == "ready"),
                "blocked_group_count": sum(1 for group in candidate_groups if group["status"] == "blocked"),
                "candidate_groups": [
                    {
                        key: value
                        for key, value in group.items()
                        if not key.startswith("_")
                    }
                    for group in candidate_groups
                ],
            }

        applied_groups: list[dict[str, object]] = []
        blocked_groups: list[dict[str, object]] = []
        connection.execute("BEGIN")
        try:
            for candidate_group in candidate_groups:
                if candidate_group["status"] != "ready":
                    blocked_groups.append(
                        {
                            key: value
                            for key, value in candidate_group.items()
                            if not key.startswith("_")
                        }
                    )
                    continue
                applied_groups.append(
                    apply_reconcile_group(
                        connection,
                        paths=paths,
                        basis=basis,
                        candidate_group=candidate_group,
                    )
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "basis": basis,
            "mode": "apply",
            "candidate_group_count": len(candidate_groups),
            "merged_group_count": len(applied_groups),
            "blocked_group_count": len(blocked_groups),
            "applied_groups": applied_groups,
            "blocked_groups": blocked_groups,
        }
    finally:
        connection.close()


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


def require_custom_field_registry_row(connection: sqlite3.Connection, field_name: str) -> sqlite3.Row:
    registry_row = get_custom_field_registry_row(connection, field_name)
    if registry_row is None:
        raise RetrieverError(f"Unknown custom field: {field_name}")
    return registry_row


def count_documents_with_non_null_field_value(connection: sqlite3.Connection, field_name: str) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS value_count
        FROM documents
        WHERE {quote_identifier(field_name)} IS NOT NULL
        """,
    ).fetchone()
    return int(row["value_count"]) if row is not None else 0


def list_fields(root: Path) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        columns = table_columns(connection, "documents")
        fields: list[dict[str, object]] = []
        rows = connection.execute(
            """
            SELECT field_name, field_type, instruction, created_at
            FROM custom_fields_registry
            ORDER BY field_name ASC
            """
        ).fetchall()
        for row in rows:
            field_name = str(row["field_name"])
            if field_name not in columns:
                continue
            fields.append(
                {
                    "field_name": field_name,
                    "field_type": str(row["field_type"]),
                    "instruction": row["instruction"],
                    "created_at": row["created_at"],
                    "documents_with_values": count_documents_with_non_null_field_value(connection, field_name),
                }
            )
        return {"status": "ok", "fields": fields}
    finally:
        connection.close()


def describe_field(
    root: Path,
    raw_field_name: str,
    *,
    text: str | None = None,
    clear: bool = False,
) -> dict[str, object]:
    if clear and text is not None:
        raise RetrieverError("Choose either --text or --clear, not both.")
    if not clear and text is None:
        raise RetrieverError("Provide --text or --clear.")

    normalized_field_name = sanitize_field_name(raw_field_name)
    next_instruction = None if clear else text

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        require_custom_field_registry_row(connection, normalized_field_name)
        connection.execute(
            """
            UPDATE custom_fields_registry
            SET instruction = ?
            WHERE field_name = ?
            """,
            (next_instruction, normalized_field_name),
        )
        connection.commit()
        updated_row = require_custom_field_registry_row(connection, normalized_field_name)
        return {
            "status": "ok",
            "field_name": normalized_field_name,
            "field_type": str(updated_row["field_type"]),
            "instruction": updated_row["instruction"],
        }
    finally:
        connection.close()


def ensure_fill_target_field_definition(connection: sqlite3.Connection, field_name: str) -> dict[str, str]:
    try:
        field_def = resolve_field_definition(connection, field_name)
    except RetrieverError as exc:
        suggestions = field_name_suggestions(connection, field_name)
        suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
        raise RetrieverError(f"Unknown field '{field_name}'.{suggestion_text}") from exc
    canonical_name = str(field_def["field_name"])
    source = str(field_def.get("source") or "")
    if source == "virtual":
        raise RetrieverError(f"Field '{canonical_name}' is derived and cannot be filled manually.")
    if canonical_name in SYSTEM_MANAGED_FIELDS:
        raise RetrieverError(f"Field '{canonical_name}' is system-managed and cannot be filled manually.")
    if source == "builtin":
        if canonical_name not in EDITABLE_BUILTIN_FIELDS:
            raise RetrieverError(f"Field '{canonical_name}' is not an editable built-in field.")
        return field_def
    if source == "custom":
        return field_def
    raise RetrieverError(
        f"Field '{canonical_name}' is not a registered custom field or editable built-in field."
    )


def replace_document_field_locks(
    connection: sqlite3.Connection,
    old_field_name: str,
    new_field_name: str,
) -> int:
    rows = connection.execute(
        f"""
        SELECT id, {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} AS locks_json
        FROM documents
        WHERE {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} LIKE ?
        """,
        (f'%"{old_field_name}"%',),
    ).fetchall()
    updated = 0
    for row in rows:
        locks = normalize_string_list(row["locks_json"])
        if old_field_name not in locks:
            continue
        next_locks: list[str] = []
        for lock_name in locks:
            target_name = new_field_name if lock_name == old_field_name else lock_name
            if target_name not in next_locks:
                next_locks.append(target_name)
        connection.execute(
            f"""
            UPDATE documents
            SET {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} = ?
            WHERE id = ?
            """,
            (json.dumps(next_locks), int(row["id"])),
        )
        updated += 1
    return updated


def sample_documents_for_fill(
    connection: sqlite3.Connection,
    document_ids: list[int],
    field_name: str,
    *,
    limit: int = 5,
) -> list[dict[str, object]]:
    normalized_ids = list(dict.fromkeys(int(document_id) for document_id in document_ids))[:limit]
    if not normalized_ids:
        return []
    placeholders = ", ".join("?" for _ in normalized_ids)
    rows = connection.execute(
        f"""
        SELECT
          id,
          control_number,
          file_name,
          title,
          {quote_identifier(field_name)} AS field_value
        FROM documents
        WHERE id IN ({placeholders})
        """,
        tuple(normalized_ids),
    ).fetchall()
    rows_by_id = {int(row["id"]): row for row in rows}
    sample_rows: list[dict[str, object]] = []
    for document_id in normalized_ids:
        row = rows_by_id.get(document_id)
        if row is None:
            continue
        sample_rows.append(
            {
                "document_id": int(row["id"]),
                "control_number": row["control_number"],
                "file_name": row["file_name"],
                "title": row["title"],
                "value": row["field_value"],
            }
        )
    return sample_rows


def round_half_away_from_zero(raw_value: float) -> int:
    if raw_value >= 0:
        return int(raw_value + 0.5)
    return -int(abs(raw_value) + 0.5)


def convert_field_value_for_type_change(
    raw_value: object,
    from_type: str,
    to_type: str,
) -> tuple[object, bool]:
    if raw_value is None:
        return None, False
    normalized_from_type = from_type.strip().lower()
    normalized_to_type = to_type.strip().lower()
    if normalized_from_type == normalized_to_type:
        return raw_value, False

    if normalized_from_type == "text":
        raw_text = str(raw_value)
        if not raw_text.strip():
            return None, True
        if normalized_to_type == "text":
            return raw_text, False
        converted_value = value_from_type(normalized_to_type, raw_text)
        return converted_value, raw_text != str(converted_value)

    if normalized_from_type == "date":
        if normalized_to_type != "text":
            raise RetrieverError(f"Cannot convert {normalized_from_type} to {normalized_to_type}.")
        return str(raw_value), False

    if normalized_from_type == "boolean":
        int_value = int(raw_value)
        if normalized_to_type == "integer":
            return int_value, False
        if normalized_to_type == "real":
            return float(int_value), True
        if normalized_to_type == "text":
            return ("true" if int_value else "false"), True
        raise RetrieverError(f"Cannot convert {normalized_from_type} to {normalized_to_type}.")

    if normalized_from_type == "integer":
        int_value = int(raw_value)
        if normalized_to_type == "boolean":
            if int_value not in {0, 1}:
                raise RetrieverError(f"Expected 0 or 1 for boolean conversion, got {int_value!r}")
            return int_value, False
        if normalized_to_type == "real":
            return float(int_value), True
        if normalized_to_type == "text":
            return str(int_value), True
        raise RetrieverError(f"Cannot convert {normalized_from_type} to {normalized_to_type}.")

    if normalized_from_type == "real":
        float_value = float(raw_value)
        if normalized_to_type == "integer":
            rounded_value = round_half_away_from_zero(float_value)
            return rounded_value, rounded_value != float_value
        if normalized_to_type == "text":
            return str(float_value), True
        raise RetrieverError(f"Cannot convert {normalized_from_type} to {normalized_to_type}.")

    raise RetrieverError(f"Unsupported field type conversion: {normalized_from_type} -> {normalized_to_type}")


def field_type_conversion_allowed(from_type: str, to_type: str) -> bool:
    normalized_from = from_type.strip().lower()
    normalized_to = to_type.strip().lower()
    if normalized_from == normalized_to:
        return True
    allowed_pairs = {
        ("boolean", "integer"),
        ("boolean", "real"),
        ("boolean", "text"),
        ("date", "text"),
        ("integer", "boolean"),
        ("integer", "real"),
        ("integer", "text"),
        ("real", "integer"),
        ("real", "text"),
        ("text", "boolean"),
        ("text", "date"),
        ("text", "integer"),
        ("text", "real"),
    }
    return (normalized_from, normalized_to) in allowed_pairs


def build_no_document_selection_error() -> str:
    return (
        "No document selection active. Provide 'on <doc-ref[,...]>', narrow the current browse state "
        "with /dataset / /filter / /search / /bates / /from-run, or use fill-field with explicit selectors."
    )


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


def rename_field(root: Path, old_name: str, new_name: str) -> dict[str, object]:
    normalized_old_name = sanitize_field_name(old_name)
    normalized_new_name = sanitize_field_name(new_name)

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        require_custom_field_registry_row(connection, normalized_old_name)
        if normalized_old_name not in table_columns(connection, "documents"):
            raise RetrieverError(f"Field column '{normalized_old_name}' does not exist on documents.")
        if normalized_old_name == normalized_new_name:
            return {
                "status": "ok",
                "renamed_from": normalized_old_name,
                "field_name": normalized_new_name,
                "locks_updated": 0,
                "state_updates": {},
            }
        if normalized_new_name in table_columns(connection, "documents") or get_custom_field_registry_row(
            connection,
            normalized_new_name,
        ) is not None:
            raise RetrieverError(f"Field '{normalized_new_name}' already exists.")

        state_plan = plan_field_rename_state_changes(paths, normalized_old_name, normalized_new_name)
        blockers = state_plan.get("blockers")
        if isinstance(blockers, list) and blockers:
            return {
                "status": "blocked",
                "renamed_from": normalized_old_name,
                "field_name": normalized_new_name,
                "blockers": blockers,
            }

        connection.execute("BEGIN")
        try:
            connection.execute(
                f"""
                ALTER TABLE documents
                RENAME COLUMN {quote_identifier(normalized_old_name)} TO {quote_identifier(normalized_new_name)}
                """
            )
            connection.execute(
                """
                UPDATE custom_fields_registry
                SET field_name = ?
                WHERE field_name = ?
                """,
                (normalized_new_name, normalized_old_name),
            )
            locks_updated = replace_document_field_locks(connection, normalized_old_name, normalized_new_name)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        apply_field_state_change_plan(paths, state_plan)
        return {
            "status": "ok",
            "renamed_from": normalized_old_name,
            "field_name": normalized_new_name,
            "locks_updated": locks_updated,
            "state_updates": state_plan.get("changes") or {},
        }
    finally:
        connection.close()


def delete_field(root: Path, raw_field_name: str, *, confirm: bool = False) -> dict[str, object]:
    normalized_field_name = sanitize_field_name(raw_field_name)

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        require_custom_field_registry_row(connection, normalized_field_name)
        if normalized_field_name not in table_columns(connection, "documents"):
            raise RetrieverError(f"Field column '{normalized_field_name}' does not exist on documents.")

        non_null_values_removed = count_documents_with_non_null_field_value(connection, normalized_field_name)
        preview_payload = {
            "field_name": normalized_field_name,
            "non_null_values_removed": non_null_values_removed,
            "documents_affected": non_null_values_removed,
        }

        state_plan = plan_field_delete_state_changes(paths, normalized_field_name)
        blockers = state_plan.get("blockers")
        if isinstance(blockers, list) and blockers:
            result = {"status": "blocked", **preview_payload, "blockers": blockers}
            pending_changes = state_plan.get("changes")
            if pending_changes:
                result["state_updates"] = pending_changes
            return result

        if not confirm:
            return {
                "status": "confirm_required",
                **preview_payload,
                "state_updates": state_plan.get("changes") or {},
            }

        connection.execute("BEGIN")
        try:
            locks_removed = drop_document_field_locks(connection, normalized_field_name)
            connection.execute(
                f"ALTER TABLE documents DROP COLUMN {quote_identifier(normalized_field_name)}"
            )
            connection.execute(
                """
                DELETE FROM custom_fields_registry
                WHERE field_name = ?
                """,
                (normalized_field_name,),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        apply_field_state_change_plan(paths, state_plan)
        return {
            "status": "ok",
            "deleted": normalized_field_name,
            "non_null_values_removed": non_null_values_removed,
            "documents_affected": non_null_values_removed,
            "locks_removed": locks_removed,
            "state_updates": state_plan.get("changes") or {},
        }
    finally:
        connection.close()


def change_field_type(root: Path, raw_field_name: str, target_field_type: str) -> dict[str, object]:
    normalized_field_name = sanitize_field_name(raw_field_name)
    normalized_target_type = target_field_type.strip().lower()
    if normalized_target_type not in REGISTRY_FIELD_TYPES:
        raise RetrieverError(f"Unsupported field type: {target_field_type}")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        registry_row = require_custom_field_registry_row(connection, normalized_field_name)
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
                "conversion_applied": False,
                "promotion_applied": False,
            }
        if not field_type_conversion_allowed(current_type, normalized_target_type):
            raise RetrieverError(f"Field '{normalized_field_name}' cannot convert from {current_type!r} to {normalized_target_type!r}.")
        if normalized_field_name not in table_columns(connection, "documents"):
            raise RetrieverError(f"Field column '{normalized_field_name}' does not exist on documents.")

        value_rows = connection.execute(
            f"""
            SELECT id, {quote_identifier(normalized_field_name)} AS value
            FROM documents
            WHERE {quote_identifier(normalized_field_name)} IS NOT NULL
            ORDER BY id ASC
            """
        ).fetchall()

        invalid_values: list[dict[str, object]] = []
        normalized_updates: list[tuple[object, int]] = []
        warnings: list[str] = (
            ["real -> integer rounds values to the nearest integer (half away from zero)."]
            if current_type == "real" and normalized_target_type == "integer"
            else []
        )
        for row in value_rows:
            raw_value = row["value"]
            try:
                normalized_value, changed = convert_field_value_for_type_change(
                    raw_value,
                    current_type,
                    normalized_target_type,
                )
            except RetrieverError:
                if len(invalid_values) < 10:
                    invalid_values.append({"document_id": int(row["id"]), "value": raw_value})
                continue
            if changed or normalized_value != raw_value:
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
                "conversion_applied": False,
                "promotion_applied": False,
            }

        source_sql_type = REGISTRY_FIELD_TYPES[current_type]
        target_sql_type = REGISTRY_FIELD_TYPES[normalized_target_type]
        conversion_requires_column_rebuild = source_sql_type != target_sql_type
        temp_column_name = f"{normalized_field_name}__tmp_{normalized_target_type}"

        connection.execute("BEGIN")
        try:
            if conversion_requires_column_rebuild:
                if temp_column_name in table_columns(connection, "documents"):
                    raise RetrieverError(f"Temporary field column '{temp_column_name}' already exists.")
                connection.execute(
                    f"ALTER TABLE documents ADD COLUMN {quote_identifier(temp_column_name)} {target_sql_type}"
                )
                if value_rows:
                    for row in value_rows:
                        document_id = int(row["id"])
                        raw_value = row["value"]
                        converted_value, _ = convert_field_value_for_type_change(
                            raw_value,
                            current_type,
                            normalized_target_type,
                        )
                        connection.execute(
                            f"""
                            UPDATE documents
                            SET {quote_identifier(temp_column_name)} = ?, updated_at = ?
                            WHERE id = ?
                            """,
                            (converted_value, utc_now(), document_id),
                        )
                connection.execute(
                    f"ALTER TABLE documents DROP COLUMN {quote_identifier(normalized_field_name)}"
                )
                connection.execute(
                    f"""
                    ALTER TABLE documents
                    RENAME COLUMN {quote_identifier(temp_column_name)} TO {quote_identifier(normalized_field_name)}
                    """
                )
            else:
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
            "conversion_applied": True,
            "promotion_applied": True,
            "column_rebuilt": conversion_requires_column_rebuild,
            "warnings": warnings,
        }
    finally:
        connection.close()


def promote_field_type(root: Path, raw_field_name: str, target_field_type: str) -> dict[str, object]:
    if target_field_type.strip().lower() != "date":
        raise RetrieverError("Only text -> date field promotion is supported via promote-field-type.")
    payload = change_field_type(root, raw_field_name, target_field_type)
    payload.setdefault("promotion_applied", bool(payload.get("conversion_applied")))
    return payload


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


def resolve_conversation_preview_refresh_ids(
    connection: sqlite3.Connection,
    *,
    conversation_ids: list[int] | None = None,
    document_ids: list[int] | None = None,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
) -> tuple[list[int], dict[str, object] | None]:
    target_conversation_ids: set[int] = set()
    dataset_summary: dict[str, object] | None = None

    if conversation_ids:
        requested_conversation_ids = sorted(dict.fromkeys(int(conversation_id) for conversation_id in conversation_ids))
        rows = connection.execute(
            f"""
            SELECT id
            FROM conversations
            WHERE id IN ({", ".join("?" for _ in requested_conversation_ids)})
            """,
            tuple(requested_conversation_ids),
        ).fetchall()
        found_conversation_ids = {int(row["id"]) for row in rows}
        missing_conversation_ids = [
            conversation_id
            for conversation_id in requested_conversation_ids
            if conversation_id not in found_conversation_ids
        ]
        if missing_conversation_ids:
            missing_text = ", ".join(str(conversation_id) for conversation_id in missing_conversation_ids)
            raise RetrieverError(f"Unknown conversation id(s): {missing_text}")
        target_conversation_ids.update(requested_conversation_ids)

    if document_ids:
        for document_id in sorted(dict.fromkeys(int(document_id) for document_id in document_ids)):
            root_row = get_document_family_root_row_for_assignment(connection, document_id)
            conversation_id = root_row["conversation_id"]
            if conversation_id is None:
                raise RetrieverError(
                    f"Document {document_id} does not belong to a conversation, so there are no conversation previews to refresh."
                )
            target_conversation_ids.add(int(conversation_id))

    if dataset_id is not None or dataset_name is not None:
        dataset_row = resolve_dataset_row(connection, dataset_id=dataset_id, dataset_name=dataset_name)
        dataset_summary = dataset_summary_by_id(connection, int(dataset_row["id"]))
        rows = connection.execute(
            """
            SELECT DISTINCT documents.conversation_id AS conversation_id
            FROM dataset_documents
            JOIN documents ON documents.id = dataset_documents.document_id
            WHERE dataset_documents.dataset_id = ?
              AND documents.conversation_id IS NOT NULL
              AND documents.lifecycle_status NOT IN ('missing', 'deleted')
            ORDER BY documents.conversation_id ASC
            """,
            (int(dataset_row["id"]),),
        ).fetchall()
        target_conversation_ids.update(
            int(row["conversation_id"])
            for row in rows
            if row["conversation_id"] is not None
        )

    if not target_conversation_ids and conversation_ids is None and document_ids is None and dataset_id is None and dataset_name is None:
        target_conversation_ids.update(list_active_conversation_ids(connection))

    return sorted(target_conversation_ids), dataset_summary


def refresh_generated_previews(
    root: Path,
    *,
    conversation_ids: list[int] | None = None,
    document_ids: list[int] | None = None,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        target_conversation_ids, dataset_summary = resolve_conversation_preview_refresh_ids(
            connection,
            conversation_ids=conversation_ids,
            document_ids=document_ids,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
        )
        connection.execute("BEGIN")
        try:
            refreshed = refresh_conversation_previews(connection, paths, target_conversation_ids)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        result: dict[str, object] = {
            "status": "ok",
            "refreshed_conversations": int(refreshed),
        }
        if conversation_ids is None and document_ids is None and dataset_id is None and dataset_name is None:
            result["target_scope"] = "all_active_conversations"
        else:
            result["target_conversation_ids"] = target_conversation_ids
        if document_ids:
            result["requested_document_ids"] = sorted(dict.fromkeys(int(document_id) for document_id in document_ids))
        if conversation_ids:
            result["requested_conversation_ids"] = sorted(
                dict.fromkeys(int(conversation_id) for conversation_id in conversation_ids)
            )
        if dataset_summary is not None:
            result["dataset"] = dataset_summary
        return result
    finally:
        connection.close()


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


def fill_field(
    root: Path,
    *,
    field_name: str,
    value: str | None = None,
    clear: bool = False,
    document_ids: list[int] | None = None,
    query: str = "",
    raw_bates: str | None = None,
    raw_filters: list[list[str]] | None = None,
    dataset_names: list[str] | None = None,
    from_run_id: int | None = None,
    select_from_scope: bool = False,
    dry_run: bool = False,
    confirm: bool = False,
) -> dict[str, object]:
    normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in (document_ids or [])))
    selector_inputs_present = bool(query.strip() or raw_bates or raw_filters or dataset_names or from_run_id is not None)
    if normalized_document_ids and (selector_inputs_present or select_from_scope):
        raise RetrieverError("fill-field accepts either --doc-id selectors or query/filter/scope selectors, not both.")
    if clear and value is not None:
        raise RetrieverError("Choose either --value or --clear, not both.")
    if not clear and value is None:
        raise RetrieverError("Provide --value or --clear.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        field_def = ensure_fill_target_field_definition(connection, field_name)
        column_name = str(field_def["field_name"])
        typed_value = None if clear else value_from_type(str(field_def["field_type"]), value)

        if normalized_document_ids:
            target_rows = fetch_visible_document_rows_by_ids(connection, normalized_document_ids)
            target_document_ids = [int(row["id"]) for row in target_rows]
            selector_payload: dict[str, object] = {
                "mode": "document_ids",
                "document_ids": target_document_ids,
            }
            selected_from_scope = False
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
                raise RetrieverError(build_no_document_selection_error())
            target_document_ids, _, _ = resolve_seed_documents_for_scope_selector(connection, selector)
            selector_payload = {
                "mode": "scope_search",
                "scope": selector,
            }
            selected_from_scope = bool(select_from_scope)

        single_explicit_document = bool(normalized_document_ids) and len(target_document_ids) == 1
        preview_payload = {
            "field_name": column_name,
            "field_type": str(field_def["field_type"]),
            "value": typed_value,
            "clear": clear,
            "selector": selector_payload,
            "selected_from_scope": selected_from_scope,
            "would_write": len(target_document_ids),
            "document_ids": target_document_ids,
            "sample": sample_documents_for_fill(connection, target_document_ids, column_name),
        }

        if dry_run:
            return {"status": "ok", "dry_run": True, **preview_payload}
        if target_document_ids and not single_explicit_document and not confirm:
            return {"status": "confirm_required", **preview_payload}
        if not target_document_ids:
            return {
                "status": "ok",
                "field_name": column_name,
                "field_type": str(field_def["field_type"]),
                "value": typed_value,
                "selector": selector_payload,
                "selected_from_scope": selected_from_scope,
                "written": 0,
                "skipped": 0,
                "failed": 0,
                "document_ids": [],
                "sample": [],
            }

        placeholders = ", ".join("?" for _ in target_document_ids)
        rows = connection.execute(
            f"""
            SELECT
              id,
              {quote_identifier(column_name)} AS field_value,
              {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} AS locks_json
            FROM documents
            WHERE id IN ({placeholders})
            """,
            tuple(target_document_ids),
        ).fetchall()
        rows_by_id = {int(row["id"]): row for row in rows}

        connection.execute("BEGIN")
        try:
            written = 0
            skipped = 0
            updated_document_ids: list[int] = []
            for document_id in target_document_ids:
                row = rows_by_id.get(document_id)
                if row is None:
                    raise RetrieverError(f"Unknown document id: {document_id}")
                locks = normalize_string_list(row["locks_json"])
                next_locks = list(locks)
                if column_name not in next_locks:
                    next_locks.append(column_name)
                if row["field_value"] == typed_value and next_locks == locks:
                    skipped += 1
                    continue
                connection.execute(
                    f"""
                    UPDATE documents
                    SET
                      {quote_identifier(column_name)} = ?,
                      {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} = ?,
                      updated_at = ?
                    WHERE id = ?
                    """,
                    (typed_value, json.dumps(next_locks), utc_now(), document_id),
                )
                updated_document_ids.append(document_id)
                written += 1
            if column_name in {"author", "participants", "recipients", "subject", "title"}:
                for document_id in updated_document_ids:
                    refresh_documents_fts_row(connection, document_id)
            connection.commit()
            return {
                "status": "ok",
                "field_name": column_name,
                "field_type": str(field_def["field_type"]),
                "value": typed_value,
                "selector": selector_payload,
                "selected_from_scope": selected_from_scope,
                "written": written,
                "skipped": skipped,
                "failed": 0,
                "document_ids": target_document_ids,
                "sample": sample_documents_for_fill(connection, target_document_ids, column_name),
            }
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()


def set_field(root: Path, document_id: int, field_name: str, value: str | None) -> dict[str, object]:
    payload = fill_field(
        root,
        field_name=field_name,
        value=value,
        clear=value is None,
        document_ids=[document_id],
        confirm=True,
    )
    paths = workspace_paths(root)
    connection = connect_db(paths["db_path"])
    try:
        field_def = ensure_fill_target_field_definition(connection, field_name)
        row = connection.execute(
            f"""
            SELECT
              {quote_identifier(field_def['field_name'])} AS field_value,
              {quote_identifier(MANUAL_FIELD_LOCKS_COLUMN)} AS locks_json
            FROM documents
            WHERE id = ?
            """,
            (document_id,),
        ).fetchone()
        locks = normalize_string_list(row["locks_json"] if row is not None else None)
        return {
            "status": str(payload.get("status") or "ok"),
            "document_id": document_id,
            "field_name": str(field_def["field_name"]),
            "field_type": str(field_def["field_type"]),
            "value": row["field_value"] if row is not None else None,
            "manual_field_locks": locks,
        }
    finally:
        connection.close()
