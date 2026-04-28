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


INGEST_V2_PIPELINE_SCHEMA_VERSION = 1
INGEST_V2_ACTIVE_STATUSES = {"planning", "preparing", "committing", "finalizing"}
INGEST_V2_TERMINAL_STATUSES = {"completed", "canceled", "failed"}
INGEST_V2_WORK_ITEM_STATUSES = (
    "pending",
    "leased",
    "prepared",
    "committing",
    "committed",
    "failed",
    "deferred_timeout",
    "cancelled",
)
INGEST_V2_WORK_ITEM_LEASE_SECONDS = 45
INGEST_V2_PREPARE_BATCH_SIZE = DEFAULT_WORKER_BATCH_SIZE
INGEST_V2_PREPARE_MIN_START_SECONDS = 1.0
INGEST_V2_COMMIT_MIN_START_SECONDS = 1.0
INGEST_V2_RUN_STEP_MIN_REMAINING_SECONDS = 1.25
INGEST_V2_RUN_STEP_MAX_INNER_STEPS = 100
INGEST_V2_MAX_SINGLE_STEP_HASH_BYTES = 2 * 1024 * 1024 * 1024
INGEST_V2_BYTES_B64_KEY = "__retriever_bytes_b64__"
INGEST_V2_PLAN_CURSOR_SAVE_INTERVAL = 25
INGEST_V2_MBOX_PLAN_BATCH_SIZE = 50
INGEST_V2_PREPARED_COMMIT_BATCH_TARGET = max(25, INGEST_V2_PREPARE_BATCH_SIZE * 5)
INGEST_V2_PRODUCTION_PREVIEW_BATCH_SIZE = 12
INGEST_V2_PRODUCTION_PREVIEW_IMAGE_MAX_DIMENSION = 1400
INGEST_PIPELINE_LEGACY = "legacy"
INGEST_PIPELINE_V2 = "v2"
INGEST_PIPELINE_MODE = INGEST_PIPELINE_V2


def ingest_v2_elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000.0


def ingest_v2_percentile_ms(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(float(value) for value in values)
    index = int(((percentile / 100.0) * len(sorted_values)) + 0.999999) - 1
    index = max(0, min(len(sorted_values) - 1, index))
    return round(sorted_values[index], 3)


def ingest_v2_timing_summary(values: list[float]) -> dict[str, object]:
    if not values:
        return {
            "count": 0,
            "total_ms": 0.0,
            "avg_ms": None,
            "p95_ms": None,
            "max_ms": None,
        }
    total = sum(float(value) for value in values)
    return {
        "count": len(values),
        "total_ms": round(total, 3),
        "avg_ms": round(total / len(values), 3),
        "p95_ms": ingest_v2_percentile_ms(values, 95.0),
        "max_ms": round(max(float(value) for value in values), 3),
    }


def new_ingest_v2_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{secrets.token_hex(4)}"


def ingest_v2_scope_payload(
    root: Path,
    *,
    recursive: bool,
    raw_file_types: str | None,
    raw_paths: list[str] | None,
) -> dict[str, object]:
    allowed_types = parse_file_types(raw_file_types)
    scan_scope = build_ingest_scan_scope(root, raw_paths)
    return {
        "recursive": bool(recursive),
        "file_types": sorted(allowed_types) if allowed_types is not None else None,
        "scan_paths": list(scan_scope.get("display_paths") or []),
    }


def ingest_v2_scan_scope_from_run(root: Path, row: sqlite3.Row) -> dict[str, object]:
    scope = decode_json_text(row["scope_json"], default={}) or {}
    raw_paths = list(scope.get("scan_paths") or []) if isinstance(scope, dict) else []
    return build_ingest_scan_scope(root, [str(path) for path in raw_paths])


def ingest_v2_cursor_rel_path(root: Path, path: Path) -> str:
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    if resolved_path == resolved_root:
        return ""
    return resolved_path.relative_to(resolved_root).as_posix()


def ingest_v2_cursor_path(root: Path, rel_path: object) -> Path:
    normalized = normalize_whitespace(str(rel_path or "")).replace("\\", "/").strip("/")
    return root if not normalized else root / normalized


def ingest_v2_sorted_pending_paths(paths: list[str]) -> list[str]:
    return sorted(dict.fromkeys(str(path).replace("\\", "/").strip("/") for path in paths))


def ingest_v2_initial_pending_paths(root: Path, scan_scope: dict[str, object]) -> list[str]:
    pending: list[str] = []
    for scan_path in list(scan_scope.get("paths") or [root]):
        pending.append(ingest_v2_cursor_rel_path(root, Path(scan_path)))
    return ingest_v2_sorted_pending_paths(pending)


def ingest_v2_add_excluded_path(root: Path, path: Path, *, exact: set[str], prefixes: set[str]) -> None:
    if not path_is_at_or_under(path, root):
        return
    rel_path = ingest_v2_cursor_rel_path(root, path)
    if not rel_path:
        return
    exact.add(rel_path)
    if path.is_dir():
        prefixes.add(rel_path.rstrip("/") + "/")


def ingest_v2_gmail_drive_record_payload(root: Path, record: dict[str, object]) -> dict[str, object]:
    payload = dict(record)
    file_path = payload.pop("file_path", None)
    if isinstance(file_path, Path) and path_is_at_or_under(file_path, root):
        payload["file_rel_path"] = relative_document_path(root, file_path)
    return ingest_v2_json_safe_value(payload)


def ingest_v2_gmail_drive_record_from_payload(root: Path, record: dict[str, object]) -> dict[str, object]:
    restored = dict(record)
    file_rel_path = normalize_whitespace(str(restored.pop("file_rel_path", "") or ""))
    if file_rel_path:
        restored["file_path"] = ingest_v2_cursor_path(root, file_rel_path)
    return restored


def ingest_v2_gmail_mbox_source_payloads(
    root: Path,
    descriptors: list[dict[str, object]],
) -> list[dict[str, object]]:
    source_payloads: list[dict[str, object]] = []
    for descriptor in descriptors:
        root_rel_path = ingest_v2_cursor_rel_path(root, Path(descriptor["root"]))
        linked_drive_attachment_records_by_message_id = {
            str(message_id): [
                ingest_v2_gmail_drive_record_payload(root, dict(record))
                for record in list(records)
            ]
            for message_id, records in dict(descriptor.get("linked_drive_attachment_records_by_message_id") or {}).items()
        }
        linked_drive_rel_paths = sorted(
            {
                str(record.get("file_rel_path"))
                for records in linked_drive_attachment_records_by_message_id.values()
                for record in records
                if normalize_whitespace(str(record.get("file_rel_path") or ""))
            }
        )
        for mbox_path in sorted([Path(path) for path in list(descriptor.get("mbox_paths") or [])], key=lambda path: path.as_posix()):
            source_payloads.append(
                {
                    "source_plan_kind": "gmail",
                    "gmail_export_root_rel_path": root_rel_path,
                    "source_rel_path": relative_document_path(root, mbox_path),
                    "message_sidecar_hash": normalize_whitespace(
                        str(descriptor.get("message_sidecar_hash") or "")
                    ) or None,
                    "email_metadata_by_message_id": ingest_v2_json_safe_value(
                        dict(descriptor.get("email_metadata_by_message_id") or {})
                    ),
                    "linked_drive_records_by_message_id": ingest_v2_json_safe_value(
                        dict(descriptor.get("linked_drive_records_by_message_id") or {})
                    ),
                    "linked_drive_attachment_records_by_message_id": linked_drive_attachment_records_by_message_id,
                    "linked_drive_rel_paths": linked_drive_rel_paths,
                }
            )
    return source_payloads


def ingest_v2_gmail_mbox_source_scan_hash(path: Path, source_payload: dict[str, object]) -> str:
    return sha256_json_value(
        {
            "mbox_hash": sha256_file(path),
            "message_sidecar_hash": normalize_whitespace(
                str(source_payload.get("message_sidecar_hash") or "")
            ) or None,
            "source_rel_path": str(source_payload["source_rel_path"]),
        }
    )


def ingest_v2_pst_export_source_payloads_by_rel_path(
    root: Path,
    descriptors: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    payloads: dict[str, dict[str, object]] = {}
    for descriptor in descriptors:
        message_metadata_by_pst_path = dict(descriptor.get("message_metadata_by_pst_path") or {})
        message_match_records_by_pst_path = dict(descriptor.get("message_match_records_by_pst_path") or {})
        message_sidecar_hash = normalize_whitespace(str(descriptor.get("message_sidecar_hash") or "")) or None
        for pst_path in sorted([Path(path) for path in list(descriptor.get("pst_paths") or [])], key=lambda path: path.as_posix()):
            resolved_key = pst_path.resolve().as_posix()
            rel_path = relative_document_path(root, pst_path)
            payloads[rel_path] = {
                "source_plan_kind": "pst_export",
                "source_rel_path": rel_path,
                "message_sidecar_hash": message_sidecar_hash,
                "message_metadata_by_source_item": ingest_v2_json_safe_value(
                    dict(message_metadata_by_pst_path.get(resolved_key) or {})
                ),
                "message_match_records": ingest_v2_json_safe_value(
                    list(message_match_records_by_pst_path.get(resolved_key) or [])
                ),
            }
    return payloads


def ingest_v2_pst_source_scan_hash(path: Path, source_payload: dict[str, object] | None = None) -> str:
    payload = dict(source_payload or {})
    message_sidecar_hash = normalize_whitespace(str(payload.get("message_sidecar_hash") or "")) or None
    if message_sidecar_hash:
        return sha256_json_value(
            {
                "pst_hash": sha256_file(path),
                "message_sidecar_hash": message_sidecar_hash,
                "sidecar_match_version": "pst-export-sidecar-v2",
                "source_rel_path": str(payload.get("source_rel_path") or ""),
            }
        )
    return sha256_text(f"pst-ingest-v5:{sha256_file(path) or ''}")


def ingest_v2_slack_export_descriptor_payload(root: Path, descriptor: dict[str, object]) -> dict[str, object]:
    export_root = Path(descriptor["root"]).resolve()
    return {
        "rel_root": relative_document_path(root, export_root),
        "day_rel_paths": [
            relative_document_path(root, Path(day_file))
            for day_file in list(descriptor.get("day_files") or [])
        ],
    }


def ingest_v2_planning_exclusions(
    root: Path,
    recursive: bool,
    allowed_types: set[str] | None,
    connection: sqlite3.Connection,
    scan_scope: dict[str, object],
) -> dict[str, object]:
    exact: set[str] = set()
    prefixes: set[str] = set()

    production_signatures = find_production_root_signatures(root, recursive, connection, scan_scope=scan_scope)
    for signature in production_signatures:
        ingest_v2_add_excluded_path(root, Path(signature["root"]), exact=exact, prefixes=prefixes)

    slack_export_descriptors = find_scoped_source_roots(
        find_slack_export_roots,
        root,
        recursive,
        scan_scope,
        allowed_types,
    )
    for descriptor in slack_export_descriptors:
        ingest_v2_add_excluded_path(root, Path(descriptor["root"]), exact=exact, prefixes=prefixes)
    slack_export_payloads = [
        ingest_v2_slack_export_descriptor_payload(root, descriptor)
        for descriptor in slack_export_descriptors
    ]

    gmail_export_descriptors = find_scoped_source_roots(
        find_gmail_export_roots,
        root,
        recursive,
        scan_scope,
        allowed_types,
    )
    for descriptor in gmail_export_descriptors:
        ingest_v2_add_excluded_path(root, Path(descriptor["root"]), exact=exact, prefixes=prefixes)
        for owned_path in list(descriptor.get("owned_paths") or []):
            ingest_v2_add_excluded_path(root, Path(owned_path), exact=exact, prefixes=prefixes)
    gmail_mbox_source_payloads = (
        ingest_v2_gmail_mbox_source_payloads(root, gmail_export_descriptors)
        if allowed_types is None or MBOX_SOURCE_KIND in allowed_types
        else []
    )

    pst_export_descriptors = (
        find_scoped_source_roots(find_pst_export_roots, root, recursive, scan_scope)
        if allowed_types is None or PST_SOURCE_KIND in allowed_types
        else []
    )
    pst_source_payloads_by_rel_path = ingest_v2_pst_export_source_payloads_by_rel_path(root, pst_export_descriptors)
    for descriptor in pst_export_descriptors:
        for owned_path in list(descriptor.get("owned_paths") or []):
            ingest_v2_add_excluded_path(root, Path(owned_path), exact=exact, prefixes=prefixes)

    return {
        "exact_rel_paths": sorted(exact),
        "dir_prefixes": sorted(prefixes),
        "slack_export_payloads": slack_export_payloads,
        "gmail_mbox_source_payloads": gmail_mbox_source_payloads,
        "pst_source_payloads_by_rel_path": pst_source_payloads_by_rel_path,
        "counts": {
            "production_roots": len(production_signatures),
            "slack_export_roots": len(slack_export_descriptors),
            "gmail_export_roots": len(gmail_export_descriptors),
            "pst_export_roots": len(pst_export_descriptors),
        },
    }


def ingest_v2_rel_path_excluded(cursor: dict[str, object], rel_path: str) -> bool:
    if not rel_path:
        return False
    exact = set(cursor.get("excluded_exact_rel_paths") or [])
    prefixes = list(cursor.get("excluded_dir_prefixes") or [])
    return rel_path in exact or any(rel_path.startswith(str(prefix)) for prefix in prefixes)


def ingest_v2_save_phase_cursor(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    phase: str,
    cursor_key: str,
    cursor: dict[str, object],
    status: str,
) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO ingest_phase_cursors (
          run_id, phase, cursor_key, cursor_json, status, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, phase, cursor_key) DO UPDATE SET
          cursor_json = excluded.cursor_json,
          status = excluded.status,
          updated_at = excluded.updated_at
        """,
        (run_id, phase, cursor_key, compact_json_text(cursor), status, now),
    )


def ingest_v2_save_planning_cursor_heartbeat(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    cursor: dict[str, object],
    status: str = "pending",
) -> float:
    started = time.perf_counter()
    if not connection.in_transaction:
        connection.execute("BEGIN")
    try:
        ingest_v2_save_phase_cursor(
            connection,
            run_id=run_id,
            phase="planning",
            cursor_key="loose_file_scan",
            cursor=cursor,
            status=status,
        )
        connection.execute(
            """
            UPDATE ingest_runs
            SET last_heartbeat_at = ?
            WHERE run_id = ?
            """,
            (utc_now(), run_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return ingest_v2_elapsed_ms(started)


def ingest_v2_load_or_create_loose_file_plan_cursor(
    connection: sqlite3.Connection,
    root: Path,
    row: sqlite3.Row,
) -> dict[str, object]:
    run_id = str(row["run_id"])
    cursor_row = connection.execute(
        """
        SELECT cursor_json
        FROM ingest_phase_cursors
        WHERE run_id = ?
          AND phase = 'planning'
          AND cursor_key = 'loose_file_scan'
        """,
        (run_id,),
    ).fetchone()
    if cursor_row is not None:
        cursor = decode_json_text(cursor_row["cursor_json"], default={}) or {}
        if isinstance(cursor, dict):
            cursor.setdefault("pending_production_rel_roots", [])
            cursor.setdefault("production_roots_by_rel_root", {})
            cursor.setdefault("planned_production_roots", [])
            cursor.setdefault("skipped_production_roots", [])
            cursor.setdefault("planned_production_rows", 0)
            cursor.setdefault("production_failures", [])
            cursor.setdefault("production_docs_missing_linked_text", 0)
            cursor.setdefault("production_docs_missing_linked_images", 0)
            cursor.setdefault("production_docs_missing_linked_natives", 0)
            cursor.setdefault("pending_slack_export_roots", [])
            cursor.setdefault("slack_export_roots_by_rel_root", {})
            cursor.setdefault("planned_slack_export_roots", [])
            cursor.setdefault("planned_slack_conversations", 0)
            cursor.setdefault("planned_slack_day_documents", 0)
            cursor.setdefault("slack_failures", [])
            cursor.setdefault("current_mbox_source", None)
            cursor.setdefault("pending_gmail_mbox_sources", [])
            cursor.setdefault("planned_mbox_sources", [])
            cursor.setdefault("planned_mbox_messages", 0)
            cursor.setdefault("planned_gmail_mbox_sources", 0)
            cursor.setdefault("skipped_mbox_sources", 0)
            cursor.setdefault("scanned_mbox_source_rel_paths", [])
            cursor.setdefault("mbox_failures", [])
            cursor.setdefault("current_pst_source", None)
            cursor.setdefault("pst_source_payloads_by_rel_path", {})
            cursor.setdefault("planned_pst_sources", [])
            cursor.setdefault("planned_pst_messages", 0)
            cursor.setdefault("skipped_pst_sources", 0)
            cursor.setdefault("scanned_pst_source_rel_paths", [])
            cursor.setdefault("pst_failures", [])
            return cursor

    recursive = bool(row["recursive"])
    allowed_types = parse_file_types(row["raw_file_types"])
    scan_scope = ingest_v2_scan_scope_from_run(root, row)
    exclusions = ingest_v2_planning_exclusions(root, recursive, allowed_types, connection, scan_scope)
    production_signatures = find_production_root_signatures(root, recursive, connection, scan_scope=scan_scope)
    production_payloads = [
        ingest_v2_production_signature_payload(root, signature)
        for signature in production_signatures
    ]
    next_order_row = connection.execute(
        """
        SELECT COALESCE(MAX(commit_order), 0) + 1 AS next_order
        FROM ingest_work_items
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    cursor = {
        "schema_version": 1,
        "pending_paths": ingest_v2_initial_pending_paths(root, scan_scope),
        "next_commit_order": int(next_order_row["next_order"] or 1),
        "scanned_paths": 0,
        "planned_loose_files": 0,
        "skipped_container_files": 0,
        "skipped_extensionless_files": 0,
        "skipped_filtered_files": 0,
        "skipped_excluded_paths": 0,
        "skipped_missing_paths": 0,
        "listed_directories": 0,
        "pending_production_rel_roots": (
            [str(payload["rel_root"]) for payload in production_payloads]
            if allowed_types is None
            else []
        ),
        "production_roots_by_rel_root": {
            str(payload["rel_root"]): payload
            for payload in production_payloads
        },
        "planned_production_roots": [],
        "skipped_production_roots": (
            [str(payload["rel_root"]) for payload in production_payloads]
            if allowed_types is not None
            else []
        ),
        "planned_production_rows": 0,
        "production_failures": [],
        "production_docs_missing_linked_text": 0,
        "production_docs_missing_linked_images": 0,
        "production_docs_missing_linked_natives": 0,
        "pending_slack_export_roots": [str(payload["rel_root"]) for payload in list(exclusions.get("slack_export_payloads") or [])],
        "slack_export_roots_by_rel_root": {
            str(payload["rel_root"]): payload
            for payload in list(exclusions.get("slack_export_payloads") or [])
        },
        "planned_slack_export_roots": [],
        "planned_slack_conversations": 0,
        "planned_slack_day_documents": 0,
        "slack_failures": [],
        "current_mbox_source": None,
        "pending_gmail_mbox_sources": list(exclusions.get("gmail_mbox_source_payloads") or []),
        "planned_mbox_sources": [],
        "planned_mbox_messages": 0,
        "planned_gmail_mbox_sources": 0,
        "skipped_mbox_sources": 0,
        "scanned_mbox_source_rel_paths": [],
        "mbox_failures": [],
        "current_pst_source": None,
        "pst_source_payloads_by_rel_path": dict(exclusions.get("pst_source_payloads_by_rel_path") or {}),
        "planned_pst_sources": [],
        "planned_pst_messages": 0,
        "skipped_pst_sources": 0,
        "scanned_pst_source_rel_paths": [],
        "pst_failures": [],
        "excluded_exact_rel_paths": list(exclusions["exact_rel_paths"]),
        "excluded_dir_prefixes": list(exclusions["dir_prefixes"]),
        "special_source_counts": exclusions["counts"],
    }
    connection.execute("BEGIN")
    try:
        ingest_v2_save_phase_cursor(
            connection,
            run_id=run_id,
            phase="planning",
            cursor_key="loose_file_scan",
            cursor=cursor,
            status="pending",
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return cursor


def ingest_v2_plan_loose_file_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    rel_path: str,
    file_type: str,
    file_size: int | None,
    file_mtime_ns: int | None,
    commit_order: int,
) -> bool:
    now = utc_now()
    payload = {
        "rel_path": rel_path,
        "file_type": file_type,
        "source_file_size": file_size,
        "source_file_mtime_ns": file_mtime_ns,
        "planned_at": now,
    }
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order,
          payload_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "loose_file",
            FILESYSTEM_SOURCE_KIND,
            rel_path,
            rel_path,
            int(commit_order),
            compact_json_text(payload),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_plan_slack_conversation_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    rel_root: str,
    conversation_plan: dict[str, object],
    commit_order: int,
) -> bool:
    now = utc_now()
    conversation_key = str(conversation_plan["conversation_key"])
    rel_paths = [str(rel_path) for rel_path in list(conversation_plan.get("rel_paths") or [])]
    payload = {
        **ingest_v2_json_safe_value(conversation_plan),
        "source_locator": rel_root,
        "planned_at": now,
    }
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order,
          payload_json, affected_conversation_keys_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "slack_conversation",
            SLACK_EXPORT_SOURCE_KIND,
            f"{rel_root}:{conversation_key}",
            rel_paths[0] if rel_paths else rel_root,
            int(commit_order),
            compact_json_text(payload),
            compact_json_text([f"{rel_root}:{conversation_key}"]),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_plan_slack_export_root(
    connection: sqlite3.Connection,
    root: Path,
    *,
    run_id: str,
    payload: dict[str, object],
    next_commit_order: int,
) -> dict[str, object]:
    rel_root = str(payload["rel_root"])
    export_root = ingest_v2_cursor_path(root, rel_root)
    conversation_directory = load_slack_export_conversation_directory(export_root)
    user_directory = load_slack_user_directory(export_root)
    day_files = [
        ingest_v2_cursor_path(root, str(day_rel_path))
        for day_rel_path in list(payload.get("day_rel_paths") or [])
    ]
    if not day_files:
        day_files = iter_slack_export_day_files(export_root)
    conversation_plans = plan_slack_export_conversations(
        root,
        export_root,
        conversation_directory=conversation_directory,
        user_directory=user_directory,
        day_files=day_files,
    )
    commit_order = int(next_commit_order)
    planned_conversations = 0
    planned_day_documents = 0
    rel_paths: list[str] = []
    for conversation_plan in conversation_plans:
        inserted = ingest_v2_plan_slack_conversation_item(
            connection,
            run_id=run_id,
            rel_root=rel_root,
            conversation_plan=conversation_plan,
            commit_order=commit_order,
        )
        commit_order += 1
        if inserted:
            planned_conversations += 1
            planned_day_documents += len(list(conversation_plan.get("day_documents") or []))
            rel_paths.extend(str(rel_path) for rel_path in list(conversation_plan.get("rel_paths") or []))
    return {
        "rel_root": rel_root,
        "next_commit_order": commit_order,
        "planned_conversations": planned_conversations,
        "planned_day_documents": planned_day_documents,
        "seen_rel_paths": sorted(set(rel_paths)),
    }


def ingest_v2_mbox_source_scan_hash(path: Path) -> str:
    return sha256_text(f"mbox-ingest-v1:{sha256_file(path) or ''}")


def ingest_v2_plan_mbox_message_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_rel_path: str,
    source_plan_kind: str,
    message_index: int,
    message_key: object,
    source_item_id: str,
    payload_hash: str,
    source_file_size: int | None,
    source_file_mtime: str | None,
    source_file_hash: str,
    scan_started_at: str,
    commit_order: int,
    message_metadata: dict[str, object] | None = None,
    linked_drive_records: list[dict[str, object]] | None = None,
    linked_drive_attachment_records: list[dict[str, object]] | None = None,
) -> bool:
    now = utc_now()
    rel_path = mbox_message_rel_path(source_rel_path, source_item_id)
    payload = {
        "source_rel_path": source_rel_path,
        "source_plan_kind": source_plan_kind,
        "message_index": int(message_index),
        "message_key": message_key,
        "source_item_id": source_item_id,
        "payload_hash": payload_hash,
        "source_file_size": source_file_size,
        "source_file_mtime": source_file_mtime,
        "source_file_hash": source_file_hash,
        "scan_started_at": scan_started_at,
        "planned_at": now,
    }
    if message_metadata:
        payload["message_metadata"] = dict(message_metadata)
    if linked_drive_records:
        payload["linked_drive_records"] = list(linked_drive_records)
    if linked_drive_attachment_records:
        payload["linked_drive_attachment_records"] = list(linked_drive_attachment_records)
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order, parent_order,
          payload_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "mbox_message",
            MBOX_SOURCE_KIND,
            f"{source_rel_path}:{source_item_id}",
            rel_path,
            int(commit_order),
            int(message_index),
            compact_json_text(ingest_v2_json_safe_value(payload)),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_plan_mbox_source_finalizer_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_rel_path: str,
    source_file_size: int | None,
    source_file_mtime: str | None,
    source_file_hash: str,
    scan_started_at: str,
    message_count: int,
    skip_source: bool,
    commit_order: int,
    source_plan_kind: str = "mbox",
    linked_drive_rel_paths: list[str] | None = None,
) -> bool:
    now = utc_now()
    payload = {
        "source_rel_path": source_rel_path,
        "source_plan_kind": source_plan_kind,
        "source_file_size": source_file_size,
        "source_file_mtime": source_file_mtime,
        "source_file_hash": source_file_hash,
        "scan_started_at": scan_started_at,
        "message_count": int(message_count),
        "skip_source": bool(skip_source),
        "linked_drive_rel_paths": list(linked_drive_rel_paths or []),
        "planned_at": now,
    }
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order,
          payload_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "mbox_source_finalizer",
            MBOX_SOURCE_KIND,
            f"{source_rel_path}:__finalize__",
            source_rel_path,
            int(commit_order),
            compact_json_text(ingest_v2_json_safe_value(payload)),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_existing_mbox_message_count(
    connection: sqlite3.Connection,
    *,
    source_rel_path: str,
) -> int:
    source_row = get_container_source_row(connection, MBOX_SOURCE_KIND, source_rel_path)
    if source_row is not None and int(source_row["message_count"] or 0) > 0:
        return int(source_row["message_count"] or 0)
    return len(
        container_root_occurrence_rows_for_source(
            connection,
            source_kind=MBOX_SOURCE_KIND,
            source_rel_path=source_rel_path,
        )
    )


def ingest_v2_existing_pst_message_count(
    connection: sqlite3.Connection,
    *,
    source_rel_path: str,
) -> int:
    source_row = get_container_source_row(connection, PST_SOURCE_KIND, source_rel_path)
    if source_row is not None and int(source_row["message_count"] or 0) > 0:
        return int(source_row["message_count"] or 0)
    return len(
        container_root_occurrence_rows_for_source(
            connection,
            source_kind=PST_SOURCE_KIND,
            source_rel_path=source_rel_path,
        )
    )


def ingest_v2_begin_mbox_source_plan(
    connection: sqlite3.Connection,
    root: Path,
    *,
    run_id: str,
    rel_path: str,
    commit_order: int,
    source_plan_kind: str = "mbox",
    source_scan_hash: str | None = None,
    gmail_source_payload: dict[str, object] | None = None,
) -> tuple[dict[str, object] | None, int, bool]:
    path = ingest_v2_cursor_path(root, rel_path)
    source_file_size = file_size_bytes(path)
    source_file_mtime = file_mtime_timestamp(path)
    source_file_hash = source_scan_hash or ingest_v2_mbox_source_scan_hash(path)
    gmail_payload = dict(gmail_source_payload or {})
    existing_source = get_container_source_row(connection, MBOX_SOURCE_KIND, rel_path)
    scan_started_at = next_monotonic_utc_timestamp(
        [
            existing_source["last_scan_started_at"] if existing_source is not None else None,
            existing_source["last_scan_completed_at"] if existing_source is not None else None,
        ]
    )
    if (
        existing_source is not None
        and container_source_scan_completed(existing_source)
        and not container_documents_missing_text_revisions(
            connection,
            source_kind=MBOX_SOURCE_KIND,
            source_rel_path=rel_path,
        )
        and not container_email_documents_missing_threading(
            connection,
            source_kind=MBOX_SOURCE_KIND,
            source_rel_path=rel_path,
        )
        and existing_source["file_size"] == source_file_size
        and existing_source["file_hash"] == source_file_hash
        and (existing_source["file_mtime"] == source_file_mtime or existing_source["file_hash"])
    ):
        message_count = ingest_v2_existing_mbox_message_count(connection, source_rel_path=rel_path)
        ingest_v2_plan_mbox_source_finalizer_item(
            connection,
            run_id=run_id,
            source_rel_path=rel_path,
            source_plan_kind=source_plan_kind,
            source_file_size=source_file_size,
            source_file_mtime=source_file_mtime,
            source_file_hash=source_file_hash,
            scan_started_at=scan_started_at,
            message_count=message_count,
            skip_source=True,
            commit_order=commit_order,
            linked_drive_rel_paths=list(gmail_payload.get("linked_drive_rel_paths") or []),
        )
        return None, commit_order + 1, True
    return (
        {
            "source_rel_path": rel_path,
            "source_plan_kind": source_plan_kind,
            "source_file_size": source_file_size,
            "source_file_mtime": source_file_mtime,
            "source_file_hash": source_file_hash,
            "scan_started_at": scan_started_at,
            "next_message_index": 0,
            "planned_message_count": 0,
            "duplicate_source_item_counts": {},
            "next_commit_order": int(commit_order),
            "email_metadata_by_message_id": dict(gmail_payload.get("email_metadata_by_message_id") or {}),
            "linked_drive_records_by_message_id": dict(gmail_payload.get("linked_drive_records_by_message_id") or {}),
            "linked_drive_attachment_records_by_message_id": dict(
                gmail_payload.get("linked_drive_attachment_records_by_message_id") or {}
            ),
            "linked_drive_rel_paths": list(gmail_payload.get("linked_drive_rel_paths") or []),
        },
        commit_order,
        False,
    )


def ingest_v2_plan_current_mbox_source(
    connection: sqlite3.Connection,
    root: Path,
    *,
    run_id: str,
    current_mbox_source: dict[str, object],
    deadline: float,
) -> tuple[dict[str, object] | None, int, int, bool]:
    source_rel_path = str(current_mbox_source["source_rel_path"])
    source_plan_kind = str(current_mbox_source.get("source_plan_kind") or "mbox")
    path = ingest_v2_cursor_path(root, source_rel_path)
    next_message_index = int(current_mbox_source.get("next_message_index") or 0)
    next_commit_order = int(current_mbox_source.get("next_commit_order") or 1)
    processed_this_step = 0
    duplicate_counts = {
        str(key): int(value)
        for key, value in dict(current_mbox_source.get("duplicate_source_item_counts") or {}).items()
    }
    archive = mailbox.mbox(str(path), factory=mailbox.mboxMessage, create=False)
    reached_end = True
    try:
        for message_index, (message_key, raw_message) in enumerate(archive.iteritems()):
            if message_index < next_message_index:
                continue
            if (
                processed_this_step >= INGEST_V2_MBOX_PLAN_BATCH_SIZE
                or ingest_v2_deadline_remaining_seconds(deadline) < 1.0
            ):
                reached_end = False
                break
            payload_bytes = raw_message.as_bytes(policy=policy.default, unixfrom=False)
            payload_hash = sha256_bytes(payload_bytes)
            parsed_message = BytesParser(policy=policy.default).parsebytes(payload_bytes)
            explicit_source_item_id = normalize_whitespace(
                str(parsed_message.get("Message-ID") or parsed_message.get("Message-Id") or "")
            ) or None
            base_source_item_id = explicit_source_item_id or f"mbox-hash:{payload_hash}"
            duplicate_counts[base_source_item_id] = int(duplicate_counts.get(base_source_item_id) or 0) + 1
            occurrence = int(duplicate_counts[base_source_item_id])
            source_item_id = base_source_item_id if occurrence == 1 else f"{base_source_item_id}#{occurrence}"
            message_lookup_key = gmail_normalized_message_lookup_key(source_item_id)
            message_metadata = (
                dict(dict(current_mbox_source.get("email_metadata_by_message_id") or {}).get(message_lookup_key) or {})
                if message_lookup_key is not None
                else {}
            )
            linked_drive_records = (
                list(dict(current_mbox_source.get("linked_drive_records_by_message_id") or {}).get(message_lookup_key) or [])
                if message_lookup_key is not None
                else []
            )
            linked_drive_attachment_records = (
                list(
                    dict(current_mbox_source.get("linked_drive_attachment_records_by_message_id") or {}).get(
                        message_lookup_key
                    )
                    or []
                )
                if message_lookup_key is not None
                else []
            )
            ingest_v2_plan_mbox_message_item(
                connection,
                run_id=run_id,
                source_rel_path=source_rel_path,
                source_plan_kind=source_plan_kind,
                message_index=message_index,
                message_key=message_key,
                source_item_id=source_item_id,
                payload_hash=payload_hash,
                source_file_size=(
                    int(current_mbox_source["source_file_size"])
                    if current_mbox_source.get("source_file_size") is not None
                    else None
                ),
                source_file_mtime=(
                    str(current_mbox_source["source_file_mtime"])
                    if current_mbox_source.get("source_file_mtime") is not None
                    else None
                ),
                source_file_hash=str(current_mbox_source["source_file_hash"]),
                scan_started_at=str(current_mbox_source["scan_started_at"]),
                commit_order=next_commit_order,
                message_metadata=message_metadata,
                linked_drive_records=linked_drive_records,
                linked_drive_attachment_records=linked_drive_attachment_records,
            )
            processed_this_step += 1
            next_commit_order += 1
            next_message_index = message_index + 1
    finally:
        try:
            archive.close()
        except Exception:
            pass

    current_mbox_source["next_message_index"] = next_message_index
    current_mbox_source["next_commit_order"] = next_commit_order
    current_mbox_source["planned_message_count"] = (
        int(current_mbox_source.get("planned_message_count") or 0) + processed_this_step
    )
    current_mbox_source["duplicate_source_item_counts"] = duplicate_counts
    if not reached_end:
        return current_mbox_source, next_commit_order, processed_this_step, False

    ingest_v2_plan_mbox_source_finalizer_item(
        connection,
        run_id=run_id,
        source_rel_path=source_rel_path,
        source_plan_kind=source_plan_kind,
        source_file_size=(
            int(current_mbox_source["source_file_size"])
            if current_mbox_source.get("source_file_size") is not None
            else None
        ),
        source_file_mtime=(
            str(current_mbox_source["source_file_mtime"])
            if current_mbox_source.get("source_file_mtime") is not None
            else None
        ),
        source_file_hash=str(current_mbox_source["source_file_hash"]),
        scan_started_at=str(current_mbox_source["scan_started_at"]),
        message_count=int(current_mbox_source.get("planned_message_count") or 0),
        skip_source=False,
        commit_order=next_commit_order,
        linked_drive_rel_paths=list(current_mbox_source.get("linked_drive_rel_paths") or []),
    )
    return None, next_commit_order + 1, processed_this_step, True


def ingest_v2_plan_pst_message_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_rel_path: str,
    source_plan_kind: str,
    message_index: int,
    raw_message: dict[str, object],
    source_item_id: str,
    source_file_size: int | None,
    source_file_mtime: str | None,
    source_file_hash: str,
    scan_started_at: str,
    commit_order: int,
    message_metadata: dict[str, object] | None = None,
    message_match_records: list[dict[str, object]] | None = None,
) -> bool:
    now = utc_now()
    rel_path = pst_message_rel_path(source_rel_path, source_item_id)
    payload = {
        "source_rel_path": source_rel_path,
        "source_plan_kind": source_plan_kind,
        "message_index": int(message_index),
        "raw_message": ingest_v2_json_safe_value(dict(raw_message)),
        "source_item_id": source_item_id,
        "source_file_size": source_file_size,
        "source_file_mtime": source_file_mtime,
        "source_file_hash": source_file_hash,
        "scan_started_at": scan_started_at,
        "planned_at": now,
    }
    if message_metadata:
        payload["message_metadata_by_source_item"] = {source_item_id: dict(message_metadata)}
    if message_match_records:
        payload["message_match_records"] = list(message_match_records)
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order, parent_order,
          payload_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "pst_message",
            PST_SOURCE_KIND,
            f"{source_rel_path}:{source_item_id}",
            rel_path,
            int(commit_order),
            int(message_index),
            compact_json_text(ingest_v2_json_safe_value(payload)),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_plan_pst_source_finalizer_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_rel_path: str,
    source_file_size: int | None,
    source_file_mtime: str | None,
    source_file_hash: str,
    scan_started_at: str,
    message_count: int,
    skip_source: bool,
    commit_order: int,
    source_plan_kind: str = "pst",
) -> bool:
    now = utc_now()
    payload = {
        "source_rel_path": source_rel_path,
        "source_plan_kind": source_plan_kind,
        "source_file_size": source_file_size,
        "source_file_mtime": source_file_mtime,
        "source_file_hash": source_file_hash,
        "scan_started_at": scan_started_at,
        "message_count": int(message_count),
        "skip_source": bool(skip_source),
        "planned_at": now,
    }
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order,
          payload_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "pst_source_finalizer",
            PST_SOURCE_KIND,
            f"{source_rel_path}:__finalize__",
            source_rel_path,
            int(commit_order),
            compact_json_text(ingest_v2_json_safe_value(payload)),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_begin_pst_source_plan(
    connection: sqlite3.Connection,
    root: Path,
    *,
    run_id: str,
    rel_path: str,
    commit_order: int,
    source_payload: dict[str, object] | None = None,
) -> tuple[dict[str, object] | None, int, bool]:
    path = ingest_v2_cursor_path(root, rel_path)
    source_file_size = file_size_bytes(path)
    source_file_mtime = file_mtime_timestamp(path)
    pst_payload = dict(source_payload or {})
    source_plan_kind = str(pst_payload.get("source_plan_kind") or "pst")
    source_file_hash = ingest_v2_pst_source_scan_hash(
        path,
        {**pst_payload, "source_rel_path": rel_path},
    )
    existing_source = get_container_source_row(connection, PST_SOURCE_KIND, rel_path)
    scan_started_at = next_monotonic_utc_timestamp(
        [
            existing_source["last_scan_started_at"] if existing_source is not None else None,
            existing_source["last_scan_completed_at"] if existing_source is not None else None,
        ]
    )
    if (
        existing_source is not None
        and container_source_scan_completed(existing_source)
        and not container_documents_missing_text_revisions(
            connection,
            source_kind=PST_SOURCE_KIND,
            source_rel_path=rel_path,
        )
        and not container_email_documents_missing_threading(
            connection,
            source_kind=PST_SOURCE_KIND,
            source_rel_path=rel_path,
        )
        and existing_source["file_size"] == source_file_size
        and existing_source["file_hash"] == source_file_hash
        and (existing_source["file_mtime"] == source_file_mtime or existing_source["file_hash"])
    ):
        message_count = ingest_v2_existing_pst_message_count(connection, source_rel_path=rel_path)
        ingest_v2_plan_pst_source_finalizer_item(
            connection,
            run_id=run_id,
            source_rel_path=rel_path,
            source_plan_kind=source_plan_kind,
            source_file_size=source_file_size,
            source_file_mtime=source_file_mtime,
            source_file_hash=source_file_hash,
            scan_started_at=scan_started_at,
            message_count=message_count,
            skip_source=True,
            commit_order=commit_order,
        )
        return None, commit_order + 1, True
    return (
        {
            "source_rel_path": rel_path,
            "source_plan_kind": source_plan_kind,
            "source_file_size": source_file_size,
            "source_file_mtime": source_file_mtime,
            "source_file_hash": source_file_hash,
            "scan_started_at": scan_started_at,
            "next_message_index": 0,
            "planned_message_count": 0,
            "next_commit_order": int(commit_order),
            "message_metadata_by_source_item": dict(pst_payload.get("message_metadata_by_source_item") or {}),
            "message_match_records": list(pst_payload.get("message_match_records") or []),
        },
        commit_order,
        False,
    )


def ingest_v2_plan_current_pst_source(
    connection: sqlite3.Connection,
    root: Path,
    *,
    run_id: str,
    current_pst_source: dict[str, object],
    deadline: float,
) -> tuple[dict[str, object] | None, int, int, bool]:
    source_rel_path = str(current_pst_source["source_rel_path"])
    source_plan_kind = str(current_pst_source.get("source_plan_kind") or "pst")
    path = ingest_v2_cursor_path(root, source_rel_path)
    next_message_index = int(current_pst_source.get("next_message_index") or 0)
    next_commit_order = int(current_pst_source.get("next_commit_order") or 1)
    processed_this_step = 0
    reached_end = True
    for message_index, raw_message in enumerate(iter_pst_messages(path)):
        if message_index < next_message_index:
            continue
        if (
            processed_this_step >= INGEST_V2_MBOX_PLAN_BATCH_SIZE
            or ingest_v2_deadline_remaining_seconds(deadline) < 1.0
        ):
            reached_end = False
            break
        raw_message_dict = dict(raw_message)
        source_item_id = normalize_source_item_id(raw_message_dict.get("source_item_id")) or f"pst-index:{message_index}"
        exact_metadata = dict(
            dict(current_pst_source.get("message_metadata_by_source_item") or {}).get(source_item_id) or {}
        )
        ingest_v2_plan_pst_message_item(
            connection,
            run_id=run_id,
            source_rel_path=source_rel_path,
            source_plan_kind=source_plan_kind,
            message_index=message_index,
            raw_message=raw_message_dict,
            source_item_id=source_item_id,
            source_file_size=(
                int(current_pst_source["source_file_size"])
                if current_pst_source.get("source_file_size") is not None
                else None
            ),
            source_file_mtime=(
                str(current_pst_source["source_file_mtime"])
                if current_pst_source.get("source_file_mtime") is not None
                else None
            ),
            source_file_hash=str(current_pst_source["source_file_hash"]),
            scan_started_at=str(current_pst_source["scan_started_at"]),
            commit_order=next_commit_order,
            message_metadata=exact_metadata,
            message_match_records=list(current_pst_source.get("message_match_records") or []),
        )
        processed_this_step += 1
        next_commit_order += 1
        next_message_index = message_index + 1

    current_pst_source["next_message_index"] = next_message_index
    current_pst_source["next_commit_order"] = next_commit_order
    current_pst_source["planned_message_count"] = (
        int(current_pst_source.get("planned_message_count") or 0) + processed_this_step
    )
    if not reached_end:
        return current_pst_source, next_commit_order, processed_this_step, False

    ingest_v2_plan_pst_source_finalizer_item(
        connection,
        run_id=run_id,
        source_rel_path=source_rel_path,
        source_plan_kind=source_plan_kind,
        source_file_size=(
            int(current_pst_source["source_file_size"])
            if current_pst_source.get("source_file_size") is not None
            else None
        ),
        source_file_mtime=(
            str(current_pst_source["source_file_mtime"])
            if current_pst_source.get("source_file_mtime") is not None
            else None
        ),
        source_file_hash=str(current_pst_source["source_file_hash"]),
        scan_started_at=str(current_pst_source["scan_started_at"]),
        message_count=int(current_pst_source.get("planned_message_count") or 0),
        skip_source=False,
        commit_order=next_commit_order,
    )
    return None, next_commit_order + 1, processed_this_step, True


def ingest_v2_production_signature_payload(root: Path, signature: dict[str, object]) -> dict[str, object]:
    metadata_load_path = Path(signature["metadata_load_path"])
    image_load_path = Path(signature["image_load_path"]) if signature.get("image_load_path") is not None else None
    return {
        "rel_root": str(signature["rel_root"]),
        "production_name": str(signature["production_name"]),
        "metadata_load_rel_path": relative_document_path(root, metadata_load_path),
        "image_load_rel_path": relative_document_path(root, image_load_path) if image_load_path is not None else None,
        "source_type": str(signature["source_type"]),
    }


def ingest_v2_production_signature_from_payload(root: Path, payload: dict[str, object]) -> dict[str, object]:
    image_load_rel_path = normalize_whitespace(str(payload.get("image_load_rel_path") or "")) or None
    return {
        "root": (root / str(payload["rel_root"])).resolve(),
        "rel_root": str(payload["rel_root"]),
        "production_name": str(payload["production_name"]),
        "metadata_load_path": root / str(payload["metadata_load_rel_path"]),
        "image_load_path": (root / image_load_rel_path) if image_load_rel_path else None,
        "source_type": str(payload["source_type"]),
    }


def ingest_v2_plan_production_row_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plan: dict[str, object],
    signature_payload: dict[str, object],
    commit_order: int,
) -> bool:
    now = utc_now()
    production_rel_root = str(plan["production_rel_root"])
    control_number = str(plan["control_number"])
    rel_path = production_logical_rel_path(production_rel_root, control_number).as_posix()
    payload = {
        **plan,
        "rel_path": rel_path,
        "metadata_load_rel_path": signature_payload["metadata_load_rel_path"],
        "image_load_rel_path": signature_payload.get("image_load_rel_path"),
        "source_type": signature_payload["source_type"],
        "planned_at": now,
    }
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order,
          payload_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "production_row",
            PRODUCTION_SOURCE_KIND,
            f"{production_rel_root}:{control_number}",
            rel_path,
            int(commit_order),
            compact_json_text(ingest_v2_json_safe_value(payload)),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_plan_production_preview_batch_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plan: dict[str, object],
    signature_payload: dict[str, object],
    rel_path: str,
    page_refs: list[dict[str, object]],
    batch_index: int,
    parent_order: int,
    commit_order: int,
) -> bool:
    if not page_refs:
        return False
    now = utc_now()
    production_rel_root = str(plan["production_rel_root"])
    control_number = str(plan["control_number"])
    payload = {
        "production_rel_root": production_rel_root,
        "production_name": plan["production_name"],
        "control_number": control_number,
        "rel_path": rel_path,
        "metadata_load_rel_path": signature_payload["metadata_load_rel_path"],
        "image_load_rel_path": signature_payload.get("image_load_rel_path"),
        "source_type": signature_payload["source_type"],
        "batch_index": int(batch_index),
        "page_refs": page_refs,
        "planned_at": now,
    }
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO ingest_work_items (
          run_id, unit_type, source_kind, source_key, rel_path, commit_order, parent_order,
          payload_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "production_preview_batch",
            PRODUCTION_SOURCE_KIND,
            f"{production_rel_root}:{control_number}:preview:{batch_index}",
            rel_path,
            int(commit_order),
            int(parent_order),
            compact_json_text(ingest_v2_json_safe_value(payload)),
            "pending",
            now,
            now,
        ),
    )
    return int(cursor.rowcount or 0) > 0


def ingest_v2_plan_conversation_preview_items(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    conversation_ids: list[int],
    next_commit_order: int,
) -> dict[str, int]:
    now = utc_now()
    planned = 0
    current_order = int(next_commit_order)
    for conversation_id in sorted(dict.fromkeys(int(value) for value in conversation_ids)):
        payload = {
            "conversation_id": conversation_id,
            "planned_at": now,
        }
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO ingest_work_items (
              run_id, unit_type, source_kind, source_key, rel_path, commit_order,
              payload_json, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                "conversation_preview",
                "conversation",
                f"conversation:{conversation_id}",
                None,
                current_order,
                compact_json_text(ingest_v2_json_safe_value(payload)),
                "pending",
                now,
                now,
            ),
        )
        current_order += 1
        if int(cursor.rowcount or 0) > 0:
            planned += 1
    return {"planned": planned, "next_commit_order": current_order}


def ingest_v2_plan_production_root(
    connection: sqlite3.Connection,
    root: Path,
    *,
    run_id: str,
    signature_payload: dict[str, object],
    next_commit_order: int,
) -> dict[str, object]:
    signature = ingest_v2_production_signature_from_payload(root, signature_payload)
    production_root = Path(signature["root"])
    metadata_load_path = Path(signature["metadata_load_path"])
    image_load_path = Path(signature["image_load_path"]) if signature.get("image_load_path") is not None else None
    metadata = parse_production_metadata_load(metadata_load_path)
    image_rows = parse_production_image_load(image_load_path)
    resolved_image_rows: list[dict[str, object]] = []
    for image_row in image_rows:
        resolved_path = resolve_production_source_path(root, production_root, image_row["image_path"])
        resolved_image_rows.append({**image_row, "resolved_path": resolved_path})
    production_row_plans, seen_control_numbers = plan_production_record_work(
        root,
        production_root,
        signature,
        list(metadata["rows"]),
        resolved_image_rows,
    )
    planned_rows = 0
    planned_preview_batches = 0
    current_order = int(next_commit_order)
    for plan in production_row_plans:
        row_order = current_order
        production_rel_root = str(plan["production_rel_root"])
        control_number = str(plan["control_number"])
        rel_path = production_logical_rel_path(production_rel_root, control_number).as_posix()
        inserted = ingest_v2_plan_production_row_item(
            connection,
            run_id=run_id,
            plan=plan,
            signature_payload=signature_payload,
            commit_order=row_order,
        )
        current_order += 1
        if inserted:
            planned_rows += 1
        native_path = Path(str(plan["native_path"])) if plan.get("native_path") is not None else None
        page_refs = (
            production_preview_page_asset_refs(
                rel_path,
                control_number,
                [
                    Path(str(path))
                    for path in list(plan.get("matching_image_paths") or [])
                    if path
                ],
            )
            if production_previewable_native(native_path) is None
            else []
        )
        for batch_index, start in enumerate(range(0, len(page_refs), INGEST_V2_PRODUCTION_PREVIEW_BATCH_SIZE), start=1):
            batch_refs = page_refs[start : start + INGEST_V2_PRODUCTION_PREVIEW_BATCH_SIZE]
            batch_inserted = ingest_v2_plan_production_preview_batch_item(
                connection,
                run_id=run_id,
                plan=plan,
                signature_payload=signature_payload,
                rel_path=rel_path,
                page_refs=batch_refs,
                batch_index=batch_index,
                parent_order=row_order,
                commit_order=current_order,
            )
            current_order += 1
            if batch_inserted:
                planned_preview_batches += 1
    return {
        **signature_payload,
        "planned_rows": planned_rows,
        "planned_preview_batches": planned_preview_batches,
        "seen_control_numbers": sorted(seen_control_numbers),
        "docs_missing_linked_text": sum(int(plan["missing_linked_text"]) for plan in production_row_plans),
        "docs_missing_linked_images": sum(int(plan["missing_linked_images"]) for plan in production_row_plans),
        "docs_missing_linked_natives": sum(int(plan["missing_linked_natives"]) for plan in production_row_plans),
        "next_commit_order": current_order,
    }


def ingest_v2_planning_child_paths(root: Path, directory: Path, *, recursive: bool) -> list[str]:
    child_paths: list[str] = []
    for child in sorted(directory.iterdir(), key=lambda item: item.name):
        if not path_is_at_or_under(child, root):
            continue
        rel_path = ingest_v2_cursor_rel_path(root, child)
        if ".retriever" in child.resolve().relative_to(root.resolve()).parts:
            continue
        if child.is_dir() and not recursive:
            continue
        child_paths.append(rel_path)
    return child_paths


def ingest_v2_worker_id(prefix: str) -> str:
    return f"{prefix}-{os.getpid()}-{secrets.token_hex(4)}"


def ingest_v2_json_safe_value(value: object) -> object:
    if isinstance(value, bytes):
        return {INGEST_V2_BYTES_B64_KEY: base64.b64encode(value).decode("ascii")}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, sqlite3.Row):
        return {key: ingest_v2_json_safe_value(value[key]) for key in value.keys()}
    if isinstance(value, dict):
        return {str(key): ingest_v2_json_safe_value(child_value) for key, child_value in value.items()}
    if isinstance(value, (list, tuple)):
        return [ingest_v2_json_safe_value(child_value) for child_value in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def ingest_v2_json_restore_value(value: object) -> object:
    if isinstance(value, dict):
        if set(value.keys()) == {INGEST_V2_BYTES_B64_KEY}:
            try:
                return base64.b64decode(str(value[INGEST_V2_BYTES_B64_KEY]).encode("ascii"))
            except Exception:
                return b""
        return {str(key): ingest_v2_json_restore_value(child_value) for key, child_value in value.items()}
    if isinstance(value, list):
        return [ingest_v2_json_restore_value(child_value) for child_value in value]
    return value


def ingest_v2_deadline_remaining_seconds(deadline: float) -> float:
    return max(0.0, deadline - time.perf_counter())


def ingest_v2_stale_lease_cutoff(now_dt: datetime | None = None) -> str:
    return format_utc_timestamp(
        (now_dt or datetime.now(timezone.utc)) - timedelta(seconds=INGEST_V2_WORK_ITEM_LEASE_SECONDS)
    )


def ingest_v2_lease_is_active(
    expires_at: object,
    refreshed_at: object,
    *,
    now: datetime | None = None,
) -> bool:
    now_dt = now or datetime.now(timezone.utc)
    parsed_refresh = parse_utc_timestamp(refreshed_at)
    if parsed_refresh is None:
        return False
    if parsed_refresh <= now_dt - timedelta(seconds=INGEST_V2_WORK_ITEM_LEASE_SECONDS):
        return False
    return lease_is_active(expires_at, now=now_dt)


def ingest_v2_reclaim_stale_prepare_items(connection: sqlite3.Connection, *, run_id: str) -> int:
    now_dt = datetime.now(timezone.utc)
    now = format_utc_timestamp(now_dt)
    stale_cutoff = ingest_v2_stale_lease_cutoff(now_dt)
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            """
            UPDATE ingest_work_items
            SET status = 'pending',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE run_id = ?
              AND status = 'leased'
              AND lease_expires_at IS NOT NULL
              AND (
                lease_expires_at <= ?
                OR updated_at <= ?
              )
            """,
            (now, run_id, now, stale_cutoff),
        )
        reclaimed = int(cursor.rowcount or 0)
        if reclaimed:
            connection.execute(
                """
                INSERT INTO ingest_worker_events (
                  run_id, worker_id, event_type, phase, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    None,
                    "reclaim_stale_prepare_items",
                    "prepare",
                    compact_json_text({"reclaimed": reclaimed}),
                    now,
                ),
            )
        connection.commit()
        return reclaimed
    except Exception:
        connection.rollback()
        raise


def ingest_v2_claim_prepare_items(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    worker_id: str,
    limit: int = INGEST_V2_PREPARE_BATCH_SIZE,
) -> tuple[list[sqlite3.Row], bool]:
    now_dt = datetime.now(timezone.utc)
    now = format_utc_timestamp(now_dt)
    stale_cutoff = ingest_v2_stale_lease_cutoff(now_dt)
    lease_expires_at = lease_expiration_after(INGEST_V2_WORK_ITEM_LEASE_SECONDS, now=now_dt)
    connection.execute("BEGIN IMMEDIATE")
    try:
        row = require_ingest_v2_run_row(connection, run_id)
        if (
            str(row["phase"]) != "preparing"
            or str(row["status"]) in INGEST_V2_TERMINAL_STATUSES
            or row["cancel_requested_at"] is not None
        ):
            connection.rollback()
            return [], False

        active_workers = int(
            connection.execute(
                """
                SELECT COUNT(DISTINCT lease_owner)
                FROM ingest_work_items
                WHERE run_id = ?
                  AND status = 'leased'
                  AND lease_owner IS NOT NULL
                  AND lease_owner != ?
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at > ?
                  AND updated_at > ?
                """,
                (run_id, worker_id, now, stale_cutoff),
            ).fetchone()[0]
            or 0
        )
        soft_limit = int(row["prepare_worker_soft_limit"] or DEFAULT_WORKER_BACKGROUND_MAX_PARALLEL)
        if active_workers >= soft_limit:
            connection.execute(
                """
                UPDATE ingest_runs
                SET last_heartbeat_at = ?
                WHERE run_id = ?
                """,
                (now, run_id),
            )
            connection.commit()
            return [], True

        claim_rows = connection.execute(
            """
            SELECT *
            FROM ingest_work_items
            WHERE run_id = ?
              AND unit_type IN (
                'loose_file', 'production_row', 'production_preview_batch', 'slack_conversation',
                'conversation_preview',
                'mbox_message', 'mbox_source_finalizer',
                'pst_message', 'pst_source_finalizer'
              )
              AND status = 'pending'
            ORDER BY commit_order ASC, id ASC
            LIMIT ?
            """,
            (run_id, max(1, int(limit))),
        ).fetchall()
        claim_ids = [int(claim_row["id"]) for claim_row in claim_rows]
        if claim_ids:
            placeholders = ",".join("?" for _ in claim_ids)
            connection.execute(
                f"""
                UPDATE ingest_work_items
                SET status = 'leased',
                    lease_owner = ?,
                    lease_expires_at = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE run_id = ?
                  AND status = 'pending'
                  AND id IN ({placeholders})
                """,
                (worker_id, lease_expires_at, now, run_id, *claim_ids),
            )
        connection.execute(
            """
            UPDATE ingest_runs
            SET last_heartbeat_at = ?
            WHERE run_id = ?
            """,
            (now, run_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    if not claim_ids:
        return [], False
    placeholders = ",".join("?" for _ in claim_ids)
    claimed_rows = connection.execute(
        f"""
        SELECT *
        FROM ingest_work_items
        WHERE run_id = ?
          AND lease_owner = ?
          AND status = 'leased'
          AND id IN ({placeholders})
        ORDER BY commit_order ASC, id ASC
        """,
        (run_id, worker_id, *claim_ids),
    ).fetchall()
    return list(claimed_rows), False


def ingest_v2_release_prepare_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    worker_id: str,
    reason: str,
) -> bool:
    now = utc_now()
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            """
            UPDATE ingest_work_items
            SET status = 'pending',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?,
                last_error = ?
            WHERE run_id = ?
              AND id = ?
              AND status = 'leased'
              AND lease_owner = ?
            """,
            (now, reason, run_id, work_item_id, worker_id),
        )
        released = int(cursor.rowcount or 0) > 0
        connection.commit()
        return released
    except Exception:
        connection.rollback()
        raise


def ingest_v2_prepare_claim_limit() -> int:
    return max(
        INGEST_V2_PREPARE_BATCH_SIZE,
        ingest_prepare_worker_count(),
        ingest_container_prepare_worker_count(),
    )


def ingest_v2_mark_prepare_deferred_timeout(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    worker_id: str,
    message: str,
) -> bool:
    now = utc_now()
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            """
            UPDATE ingest_work_items
            SET status = 'deferred_timeout',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?,
                last_error = ?
            WHERE run_id = ?
              AND id = ?
              AND status = 'leased'
              AND lease_owner = ?
            """,
            (now, message, run_id, work_item_id, worker_id),
        )
        marked = int(cursor.rowcount or 0) > 0
        if marked:
            connection.execute(
                """
                INSERT INTO ingest_worker_events (
                  run_id, worker_id, event_type, work_item_id, phase, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    worker_id,
                    "prepare_deferred_timeout",
                    work_item_id,
                    "prepare",
                    compact_json_text({"message": message}),
                    now,
                ),
            )
        connection.commit()
        return marked
    except Exception:
        connection.rollback()
        raise


def ingest_v2_prepare_worker_count_for_rows(rows: list[sqlite3.Row]) -> int:
    if not rows:
        return 1
    unit_types = {str(row["unit_type"] or "") for row in rows}
    container_unit_types = {
        "mbox_message",
        "mbox_source_finalizer",
        "pst_message",
        "pst_source_finalizer",
    }
    if unit_types and unit_types <= container_unit_types:
        return ingest_container_prepare_worker_count()
    if unit_types.isdisjoint(container_unit_types):
        return ingest_prepare_worker_count()
    return max(ingest_prepare_worker_count(), ingest_container_prepare_worker_count())


def ingest_v2_prepare_claimed_work_item(
    root: Path,
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> dict[str, object]:
    work_item_id = int(work_item_row["id"])
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return {
            "work_item_id": work_item_id,
            "payload_kind": str(work_item_row["unit_type"] or "loose_file"),
            "prepared_item": None,
            "source_fingerprint": {},
            "defer_message": "Not enough budget remaining to start prepare.",
        }

    unit_type = str(work_item_row["unit_type"] or "")
    if unit_type == "production_row":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_production_row_item(
            root,
            work_item_row,
            deadline=deadline,
        )
        payload_kind = "production_row"
    elif unit_type == "production_preview_batch":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_production_preview_batch_item(
            root,
            work_item_row,
            deadline=deadline,
        )
        payload_kind = "production_preview_batch"
    elif unit_type == "slack_conversation":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_slack_conversation_item(
            work_item_row,
            deadline=deadline,
        )
        payload_kind = "slack_conversation"
    elif unit_type == "conversation_preview":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_conversation_preview_item(
            work_item_row,
            deadline=deadline,
        )
        payload_kind = "conversation_preview"
    elif unit_type == "mbox_message":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_mbox_message_item(
            root,
            work_item_row,
            deadline=deadline,
        )
        payload_kind = "mbox_message"
    elif unit_type == "mbox_source_finalizer":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_mbox_source_finalizer_item(
            work_item_row,
        )
        payload_kind = "mbox_source_finalizer"
    elif unit_type == "pst_message":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_pst_message_item(
            root,
            work_item_row,
            deadline=deadline,
        )
        payload_kind = "pst_message"
    elif unit_type == "pst_source_finalizer":
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_pst_source_finalizer_item(
            work_item_row,
        )
        payload_kind = "pst_source_finalizer"
    else:
        prepared_item, source_fingerprint, defer_message = ingest_v2_prepare_loose_file_item(
            root,
            work_item_row,
            deadline=deadline,
        )
        payload_kind = "loose_file"
    return {
        "work_item_id": work_item_id,
        "payload_kind": payload_kind,
        "prepared_item": prepared_item,
        "source_fingerprint": source_fingerprint,
        "defer_message": defer_message,
    }


def ingest_v2_prepare_claimed_work_items_parallel(
    root: Path,
    rows: list[sqlite3.Row],
    *,
    deadline: float,
    prepare_workers: int,
) -> list[dict[str, object]]:
    if not rows:
        return []
    effective_workers = max(1, min(int(prepare_workers), len(rows)))
    if effective_workers == 1:
        return [
            ingest_v2_prepare_claimed_work_item(root, row, deadline=deadline)
            for row in rows
        ]
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = [
            executor.submit(
                ingest_v2_prepare_claimed_work_item,
                root,
                row,
                deadline=deadline,
            )
            for row in rows
        ]
        return [future.result() for future in futures]


def ingest_v2_prepare_loose_file_item(
    root: Path,
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> tuple[dict[str, object] | None, dict[str, object], str | None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    rel_path = str(work_item_row["rel_path"] or payload_dict.get("rel_path") or "")
    path = ingest_v2_cursor_path(root, rel_path)
    file_size, file_mtime_ns = source_file_snapshot(path)
    source_fingerprint = {
        "rel_path": rel_path,
        "size": file_size,
        "mtime_ns": file_mtime_ns,
        "hash": None,
    }
    if file_size is not None and file_size > INGEST_V2_MAX_SINGLE_STEP_HASH_BYTES:
        return (
            None,
            source_fingerprint,
            (
                f"File is too large for one bounded prepare step ({file_size} bytes > "
                f"{INGEST_V2_MAX_SINGLE_STEP_HASH_BYTES} bytes)."
            ),
        )
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return None, source_fingerprint, "Not enough budget remaining to start prepare."

    file_type = str(payload_dict.get("file_type") or normalize_extension(path))
    item = {
        "rel_path": rel_path,
        "path": str(path),
        "file_type": file_type,
        "source_file_size": file_size,
        "source_file_mtime_ns": file_mtime_ns,
    }
    hash_started = time.perf_counter()
    item["file_hash"] = sha256_file(path)
    hash_ms = ingest_v2_elapsed_ms(hash_started)
    source_fingerprint["hash"] = item["file_hash"]
    prepared_item = prepare_loose_file_item(item)
    prepared_item["prepare_hash_ms"] = hash_ms
    return prepared_item, source_fingerprint, None


def ingest_v2_prepare_production_row_item(
    root: Path,
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> tuple[dict[str, object] | None, dict[str, object], str | None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    production_rel_root = str(payload_dict.get("production_rel_root") or "")
    control_number = str(payload_dict.get("control_number") or "")
    source_fingerprint = {
        "production_rel_root": production_rel_root,
        "control_number": control_number,
        "metadata_load_rel_path": payload_dict.get("metadata_load_rel_path"),
        "image_load_rel_path": payload_dict.get("image_load_rel_path"),
    }
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return None, source_fingerprint, "Not enough budget remaining to start prepare."
    rel_path = str(payload_dict.get("rel_path") or production_logical_rel_path(production_rel_root, control_number))
    matching_image_paths = [
        Path(str(path))
        for path in list(payload_dict.get("matching_image_paths") or [])
        if path
    ]
    prepared_item = prepare_production_row_plan(
        root,
        payload_dict,
        preview_image_refs=production_preview_page_asset_refs(rel_path, control_number, matching_image_paths),
    )
    prepared_item["prepare_hash_ms"] = 0.0
    return prepared_item, source_fingerprint, None


def ingest_v2_prepare_production_preview_batch_item(
    root: Path,
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> tuple[dict[str, object] | None, dict[str, object], str | None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    page_refs = [
        dict(ref)
        for ref in list(payload_dict.get("page_refs") or [])
        if isinstance(ref, dict)
    ]
    source_fingerprint = {
        "production_rel_root": payload_dict.get("production_rel_root"),
        "control_number": payload_dict.get("control_number"),
        "batch_index": payload_dict.get("batch_index"),
        "page_count": len(page_refs),
    }
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return None, source_fingerprint, "Not enough budget remaining to start prepare."
    prepare_started = time.perf_counter()
    page_assets: list[dict[str, object]] = []
    for ref in page_refs:
        if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
            return None, source_fingerprint, "Not enough budget remaining to start prepare."
        source_path = Path(str(ref.get("source_path") or ""))
        if not source_path.exists():
            continue
        png_bytes = image_path_png_bytes(
            source_path,
            max_dimension=INGEST_V2_PRODUCTION_PREVIEW_IMAGE_MAX_DIMENSION,
        )
        if png_bytes is None:
            continue
        page_assets.append(
            {
                "ordinal": int(ref.get("ordinal") or 0),
                "label": str(ref.get("label") or ""),
                "rel_preview_path": str(ref.get("rel_preview_path") or ""),
                "payload": png_bytes,
            }
        )
    prepared_item = {
        **payload_dict,
        "payload_kind": "production_preview_batch",
        "source_kind": PRODUCTION_SOURCE_KIND,
        "page_assets": page_assets,
        "prepare_ms": ingest_v2_elapsed_ms(prepare_started),
        "prepare_hash_ms": 0.0,
        "prepare_extract_ms": ingest_v2_elapsed_ms(prepare_started),
        "prepare_chunk_ms": 0.0,
        "prepare_error": None,
    }
    return prepared_item, source_fingerprint, None


def ingest_v2_prepare_conversation_preview_item(
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> tuple[dict[str, object] | None, dict[str, object], str | None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    conversation_id = int(payload_dict.get("conversation_id") or 0)
    source_fingerprint = {"conversation_id": conversation_id}
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return None, source_fingerprint, "Not enough budget remaining to start prepare."
    return (
        {
            "payload_kind": "conversation_preview",
            "conversation_id": conversation_id,
            "prepare_ms": 0.0,
            "prepare_hash_ms": 0.0,
            "prepare_extract_ms": 0.0,
            "prepare_chunk_ms": 0.0,
            "prepare_error": None,
        },
        source_fingerprint,
        None,
    )


def ingest_v2_prepare_slack_conversation_item(
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> tuple[dict[str, object] | None, dict[str, object], str | None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    rel_paths = [str(rel_path) for rel_path in list(payload_dict.get("rel_paths") or [])]
    conversation_identity = payload_dict.get("conversation_identity")
    identity_root = ""
    if isinstance(conversation_identity, list) and len(conversation_identity) > 1:
        identity_root = str(conversation_identity[1])
    source_locator = str(payload_dict.get("source_locator") or identity_root)
    source_fingerprint = {
        "source_locator": source_locator,
        "conversation_key": payload_dict.get("conversation_key"),
        "rel_paths": rel_paths,
    }
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return None, source_fingerprint, "Not enough budget remaining to start prepare."
    prepared_item = prepare_slack_conversation_plan(payload_dict)
    prepared_item["payload_kind"] = "slack_conversation"
    prepared_item["source_kind"] = SLACK_EXPORT_SOURCE_KIND
    prepared_item["source_locator"] = source_locator
    prepared_item["prepare_hash_ms"] = 0.0
    prepared_item["prepare_extract_ms"] = max(
        0.0,
        float(prepared_item.get("prepare_ms") or 0.0) - float(prepared_item.get("prepare_chunk_ms") or 0.0),
    )
    return prepared_item, source_fingerprint, None


def ingest_v2_mbox_message_from_payload(root: Path, payload: dict[str, object]) -> dict[str, object]:
    source_rel_path = str(payload["source_rel_path"])
    path = ingest_v2_cursor_path(root, source_rel_path)
    message_key = payload.get("message_key")
    message_index = int(payload.get("message_index") or 0)
    source_item_id = str(payload["source_item_id"])
    archive = mailbox.mbox(str(path), factory=mailbox.mboxMessage, create=False)
    try:
        raw_message = None
        if message_key is not None:
            try:
                raw_message = archive.get_message(message_key)
            except Exception:
                try:
                    raw_message = archive.get_message(int(message_key))
                except Exception:
                    raw_message = None
        if raw_message is None:
            for index, candidate in enumerate(archive):
                if index == message_index:
                    raw_message = candidate
                    break
        if raw_message is None:
            raise RetrieverError(f"MBOX message {message_index} is missing from {source_rel_path}.")
        payload_bytes = raw_message.as_bytes(policy=policy.default, unixfrom=False)
        payload_hash = sha256_bytes(payload_bytes)
        parsed_message = BytesParser(policy=policy.default).parsebytes(payload_bytes)
        return {
            "source_item_id": source_item_id,
            "payload_hash": payload_hash,
            "parsed_message": parsed_message,
        }
    finally:
        try:
            archive.close()
        except Exception:
            pass


def ingest_v2_pst_message_from_payload(payload: dict[str, object]) -> dict[str, object]:
    raw_message = ingest_v2_json_restore_value(payload.get("raw_message") or {})
    if not isinstance(raw_message, dict):
        raise RetrieverError("PST work item is missing a raw message payload.")
    restored = dict(raw_message)
    restored["source_item_id"] = str(payload.get("source_item_id") or restored.get("source_item_id") or "")
    return restored


def ingest_v2_prepare_mbox_message_item(
    root: Path,
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> tuple[dict[str, object] | None, dict[str, object], str | None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    source_rel_path = str(payload_dict.get("source_rel_path") or "")
    path = ingest_v2_cursor_path(root, source_rel_path)
    source_fingerprint = {
        "source_rel_path": source_rel_path,
        "message_index": payload_dict.get("message_index"),
        "message_key": payload_dict.get("message_key"),
        "source_item_id": payload_dict.get("source_item_id"),
        "size": file_size_bytes(path),
        "mtime": file_mtime_timestamp(path),
        "hash": payload_dict.get("source_file_hash"),
    }
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return None, source_fingerprint, "Not enough budget remaining to start prepare."
    if (
        payload_dict.get("source_file_size") is not None
        and int(payload_dict.get("source_file_size") or 0) != int(source_fingerprint["size"] or 0)
    ):
        return (
            {
                "prepare_error": f"MBOX source changed after planning: {source_rel_path}",
                "prepare_ms": 0.0,
                "prepare_hash_ms": 0.0,
                "prepare_extract_ms": 0.0,
                "prepare_chunk_ms": 0.0,
                "source_rel_path": source_rel_path,
                "source_item_id": str(payload_dict.get("source_item_id") or ""),
            },
            source_fingerprint,
            None,
        )
    prepare_started = time.perf_counter()
    raw_message = ingest_v2_mbox_message_from_payload(root, payload_dict)
    source_plan_kind = str(payload_dict.get("source_plan_kind") or "mbox")
    if source_plan_kind == "gmail":
        def normalize_v2_gmail_message(
            source_rel_path_for_message: str,
            message_dict: dict[str, object],
        ) -> dict[str, object] | None:
            normalized = normalize_mbox_message(source_rel_path_for_message, message_dict)
            message_metadata = dict(payload_dict.get("message_metadata") or {})
            linked_drive_records = [
                dict(record)
                for record in list(payload_dict.get("linked_drive_records") or [])
                if isinstance(record, dict)
            ]
            linked_drive_attachment_records = [
                ingest_v2_gmail_drive_record_from_payload(root, dict(record))
                for record in list(payload_dict.get("linked_drive_attachment_records") or [])
                if isinstance(record, dict)
            ]
            extracted = apply_gmail_email_export_metadata(
                dict(normalized["extracted"]),
                message_metadata=message_metadata,
                linked_drive_records=linked_drive_records,
            )
            attachment_payloads = [
                attachment
                for attachment in (
                    gmail_drive_attachment_payload(dict(record))
                    for record in linked_drive_attachment_records
                )
                if attachment is not None
            ]
            if attachment_payloads:
                extracted["attachments"] = [*list(extracted.get("attachments") or []), *attachment_payloads]
            normalized["extracted"] = extracted
            normalized["file_hash"] = gmail_enriched_message_file_hash(
                normalized.get("file_hash"),
                message_metadata=message_metadata,
                linked_drive_records=linked_drive_records,
                linked_drive_attachment_records=linked_drive_attachment_records,
            )
            return normalized

        normalize_message = normalize_v2_gmail_message
    else:
        normalize_message = normalize_mbox_message
    prepared_item = prepare_container_message_item(
        source_rel_path,
        raw_message,
        normalize_message,
    )
    prepared_item["source_kind"] = MBOX_SOURCE_KIND
    prepared_item["source_plan_kind"] = source_plan_kind
    prepared_item["source_rel_path"] = source_rel_path
    prepared_item["scan_started_at"] = str(payload_dict["scan_started_at"])
    prepared_item["source_file_size"] = payload_dict.get("source_file_size")
    prepared_item["source_file_mtime"] = payload_dict.get("source_file_mtime")
    prepared_item["source_file_hash"] = payload_dict.get("source_file_hash")
    prepared_item["prepare_hash_ms"] = 0.0
    prepared_item["prepare_extract_ms"] = max(0.0, float(prepared_item.get("prepare_ms") or 0.0) - float(prepared_item.get("prepare_chunk_ms") or 0.0))
    prepared_item["prepare_total_step_ms"] = ingest_v2_elapsed_ms(prepare_started)
    return prepared_item, source_fingerprint, None


def ingest_v2_prepare_pst_message_item(
    root: Path,
    work_item_row: sqlite3.Row,
    *,
    deadline: float,
) -> tuple[dict[str, object] | None, dict[str, object], str | None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    source_rel_path = str(payload_dict.get("source_rel_path") or "")
    path = ingest_v2_cursor_path(root, source_rel_path)
    source_fingerprint = {
        "source_rel_path": source_rel_path,
        "message_index": payload_dict.get("message_index"),
        "source_item_id": payload_dict.get("source_item_id"),
        "size": file_size_bytes(path),
        "mtime": file_mtime_timestamp(path),
        "hash": payload_dict.get("source_file_hash"),
    }
    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_PREPARE_MIN_START_SECONDS:
        return None, source_fingerprint, "Not enough budget remaining to start prepare."
    if (
        payload_dict.get("source_file_size") is not None
        and int(payload_dict.get("source_file_size") or 0) != int(source_fingerprint["size"] or 0)
    ):
        return (
            {
                "prepare_error": f"PST source changed after planning: {source_rel_path}",
                "prepare_ms": 0.0,
                "prepare_hash_ms": 0.0,
                "prepare_extract_ms": 0.0,
                "prepare_chunk_ms": 0.0,
                "source_rel_path": source_rel_path,
                "source_item_id": str(payload_dict.get("source_item_id") or ""),
            },
            source_fingerprint,
            None,
        )
    prepare_started = time.perf_counter()
    raw_message = ingest_v2_pst_message_from_payload(payload_dict)
    source_plan_kind = str(payload_dict.get("source_plan_kind") or "pst")
    exact_metadata_by_source_item = {
        str(key): dict(value)
        for key, value in dict(payload_dict.get("message_metadata_by_source_item") or {}).items()
        if isinstance(value, dict)
    }
    message_match_records = [
        dict(record)
        for record in list(payload_dict.get("message_match_records") or [])
        if isinstance(record, dict)
    ]

    def normalize_v2_pst_message(
        source_rel_path_for_message: str,
        message_dict: dict[str, object],
    ) -> dict[str, object] | None:
        normalized = normalize_pst_message(source_rel_path_for_message, message_dict)
        if normalized is None:
            return None
        message_metadata = select_pst_export_message_metadata(
            normalized,
            exact_metadata_by_source_item=exact_metadata_by_source_item,
            message_match_records=message_match_records,
        )
        if not message_metadata:
            return normalized
        enriched = dict(normalized)
        enriched["extracted"] = apply_pst_export_message_metadata(
            dict(normalized["extracted"]),
            message_metadata=message_metadata,
            identifier_scope=source_rel_path_for_message,
        )
        enriched["file_hash"] = pst_export_enriched_message_file_hash(
            normalized.get("file_hash"),
            message_metadata=message_metadata,
        )
        return enriched

    prepared_item = prepare_container_message_item(
        source_rel_path,
        raw_message,
        normalize_v2_pst_message,
    )
    prepared_item["source_kind"] = PST_SOURCE_KIND
    prepared_item["source_plan_kind"] = source_plan_kind
    prepared_item["source_rel_path"] = source_rel_path
    prepared_item["scan_started_at"] = str(payload_dict["scan_started_at"])
    prepared_item["source_file_size"] = payload_dict.get("source_file_size")
    prepared_item["source_file_mtime"] = payload_dict.get("source_file_mtime")
    prepared_item["source_file_hash"] = payload_dict.get("source_file_hash")
    prepared_item["prepare_hash_ms"] = 0.0
    prepared_item["prepare_extract_ms"] = max(0.0, float(prepared_item.get("prepare_ms") or 0.0) - float(prepared_item.get("prepare_chunk_ms") or 0.0))
    prepared_item["prepare_total_step_ms"] = ingest_v2_elapsed_ms(prepare_started)
    return prepared_item, source_fingerprint, None


def ingest_v2_prepare_mbox_source_finalizer_item(
    work_item_row: sqlite3.Row,
) -> tuple[dict[str, object], dict[str, object], None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    prepared_item = {
        **payload_dict,
        "source_kind": MBOX_SOURCE_KIND,
        "prepare_ms": 0.0,
        "prepare_hash_ms": 0.0,
        "prepare_extract_ms": 0.0,
        "prepare_chunk_ms": 0.0,
        "prepare_error": None,
    }
    source_fingerprint = {
        "source_rel_path": payload_dict.get("source_rel_path"),
        "size": payload_dict.get("source_file_size"),
        "mtime": payload_dict.get("source_file_mtime"),
        "hash": payload_dict.get("source_file_hash"),
        "message_count": payload_dict.get("message_count"),
    }
    return prepared_item, source_fingerprint, None


def ingest_v2_prepare_pst_source_finalizer_item(
    work_item_row: sqlite3.Row,
) -> tuple[dict[str, object], dict[str, object], None]:
    payload = decode_json_text(work_item_row["payload_json"], default={}) or {}
    payload_dict = payload if isinstance(payload, dict) else {}
    prepared_item = {
        **payload_dict,
        "payload_kind": "pst_source_finalizer",
        "source_kind": PST_SOURCE_KIND,
        "prepare_ms": 0.0,
        "prepare_hash_ms": 0.0,
        "prepare_extract_ms": 0.0,
        "prepare_chunk_ms": 0.0,
        "prepare_error": None,
    }
    source_fingerprint = {
        "source_rel_path": payload_dict.get("source_rel_path"),
        "size": payload_dict.get("source_file_size"),
        "mtime": payload_dict.get("source_file_mtime"),
        "hash": payload_dict.get("source_file_hash"),
        "message_count": payload_dict.get("message_count"),
    }
    return prepared_item, source_fingerprint, None


def ingest_v2_hydrate_prepared_production_item(prepared_item: dict[str, object]) -> dict[str, object]:
    hydrated = dict(prepared_item)
    for key in (
        "text_path",
        "native_path",
        "available_text_path",
        "available_native_path",
        "preferred_native",
        "preferred_source_path",
    ):
        value = hydrated.get(key)
        if value:
            hydrated[key] = Path(str(value))
    hydrated["matching_image_paths"] = [
        Path(str(path))
        for path in list(hydrated.get("matching_image_paths") or [])
        if path
    ]
    return hydrated


def ingest_v2_store_prepared_items_batch(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    worker_id: str,
    entries: list[dict[str, object]],
) -> dict[str, object]:
    if not entries:
        return {
            "stored": 0,
            "serialize_ms_values": [],
            "prepared_write_ms_values": [],
            "payload_bytes": 0,
        }

    serialized_entries: list[dict[str, object]] = []
    serialize_ms_values: list[float] = []
    payload_bytes_total = 0
    for entry in entries:
        prepared_item = dict(entry["prepared_item"])  # type: ignore[index]
        source_fingerprint = dict(entry["source_fingerprint"])  # type: ignore[index]
        serialize_started = time.perf_counter()
        payload_json = compact_json_text(ingest_v2_json_safe_value({"prepared_item": prepared_item}))
        source_fingerprint_json = compact_json_text(ingest_v2_json_safe_value(source_fingerprint))
        serialize_ms_values.append(ingest_v2_elapsed_ms(serialize_started))
        payload_bytes = len(payload_json.encode("utf-8"))
        payload_bytes_total += payload_bytes
        prepare_error = prepared_item.get("prepare_error")
        serialized_entries.append(
            {
                "work_item_id": int(entry["work_item_id"]),
                "payload_kind": str(entry["payload_kind"]),
                "prepared_item": prepared_item,
                "payload_json": payload_json,
                "payload_bytes": payload_bytes,
                "source_fingerprint_json": source_fingerprint_json,
                "error_json": compact_json_text({"prepare_error": prepare_error} if prepare_error else {}),
                "prepare_error": prepare_error,
            }
        )

    now = utc_now()
    write_started = time.perf_counter()
    connection.execute("BEGIN")
    try:
        run_row = require_ingest_v2_run_row(connection, run_id)
        if str(run_row["status"]) in INGEST_V2_TERMINAL_STATUSES or run_row["cancel_requested_at"] is not None:
            connection.rollback()
            return {
                "stored": 0,
                "serialize_ms_values": serialize_ms_values,
                "prepared_write_ms_values": [],
                "payload_bytes": payload_bytes_total,
            }
        work_item_ids = [int(entry["work_item_id"]) for entry in serialized_entries]
        placeholders = ",".join("?" for _ in work_item_ids)
        item_rows = connection.execute(
            f"""
            SELECT id, status, lease_owner
            FROM ingest_work_items
            WHERE run_id = ?
              AND id IN ({placeholders})
            """,
            (run_id, *work_item_ids),
        ).fetchall()
        eligible_ids = {
            int(row["id"])
            for row in item_rows
            if row["status"] == "leased" and row["lease_owner"] == worker_id
        }
        if not eligible_ids:
            connection.rollback()
            return {
                "stored": 0,
                "serialize_ms_values": serialize_ms_values,
                "prepared_write_ms_values": [],
                "payload_bytes": payload_bytes_total,
            }

        stored = 0
        for entry in serialized_entries:
            work_item_id = int(entry["work_item_id"])
            if work_item_id not in eligible_ids:
                continue
            prepared_item = dict(entry["prepared_item"])  # type: ignore[index]
            prepare_error = entry["prepare_error"]
            connection.execute(
                """
                INSERT INTO ingest_prepared_items (
                  run_id, work_item_id, payload_kind, payload_json, spill_rel_path,
                  payload_bytes, source_fingerprint_json, prepared_at, error_json
                ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?)
                ON CONFLICT(work_item_id) DO UPDATE SET
                  payload_kind = excluded.payload_kind,
                  payload_json = excluded.payload_json,
                  spill_rel_path = excluded.spill_rel_path,
                  payload_bytes = excluded.payload_bytes,
                  source_fingerprint_json = excluded.source_fingerprint_json,
                  prepared_at = excluded.prepared_at,
                  error_json = excluded.error_json
                """,
                (
                    run_id,
                    work_item_id,
                    entry["payload_kind"],
                    entry["payload_json"],
                    entry["payload_bytes"],
                    entry["source_fingerprint_json"],
                    now,
                    entry["error_json"],
                ),
            )
            connection.execute(
                """
                UPDATE ingest_work_items
                SET status = 'prepared',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?,
                    last_error = ?
                WHERE run_id = ?
                  AND id = ?
                """,
                (now, str(prepare_error) if prepare_error else None, run_id, work_item_id),
            )
            connection.execute(
                """
                INSERT INTO ingest_worker_events (
                  run_id, worker_id, event_type, work_item_id, phase, duration_ms, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    worker_id,
                    "prepare_item",
                    work_item_id,
                    "prepare",
                    prepared_item.get("prepare_ms"),
                    compact_json_text(
                        {
                            "payload_kind": entry["payload_kind"],
                            "rel_path": prepared_item.get("rel_path"),
                            "prepare_error": prepare_error,
                        }
                    ),
                    now,
                ),
            )
            stored += 1
        connection.execute(
            """
            UPDATE ingest_runs
            SET last_heartbeat_at = ?
            WHERE run_id = ?
            """,
            (now, run_id),
        )
        connection.commit()
        return {
            "stored": stored,
            "serialize_ms_values": serialize_ms_values,
            "prepared_write_ms_values": [ingest_v2_elapsed_ms(write_started)],
            "payload_bytes": payload_bytes_total,
        }
    except Exception:
        connection.rollback()
        raise


def ingest_v2_store_prepared_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    worker_id: str,
    payload_kind: str,
    prepared_item: dict[str, object],
    source_fingerprint: dict[str, object],
) -> bool:
    result = ingest_v2_store_prepared_items_batch(
        connection,
        run_id=run_id,
        worker_id=worker_id,
        entries=[
            {
                "work_item_id": work_item_id,
                "payload_kind": payload_kind,
                "prepared_item": prepared_item,
                "source_fingerprint": source_fingerprint,
            }
        ],
    )
    return int(result.get("stored") or 0) > 0


def ingest_v2_maybe_advance_after_prepare(connection: sqlite3.Connection, *, run_id: str) -> bool:
    row = require_ingest_v2_run_row(connection, run_id)
    if str(row["phase"]) != "preparing" or row["cancel_requested_at"] is not None:
        return False
    remaining_prepare_items = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM ingest_work_items
            WHERE run_id = ?
              AND status IN ('pending', 'leased')
            """,
            (run_id,),
        ).fetchone()[0]
        or 0
    )
    prepared_items = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM ingest_work_items
            WHERE run_id = ?
              AND status = 'prepared'
            """,
            (run_id,),
        ).fetchone()[0]
        or 0
    )
    if not prepared_items and remaining_prepare_items:
        return False
    if prepared_items and remaining_prepare_items and prepared_items < INGEST_V2_PREPARED_COMMIT_BATCH_TARGET:
        return False
    next_phase = "committing" if prepared_items else "finalizing"
    now = utc_now()
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            """
            UPDATE ingest_runs
            SET phase = ?,
                status = ?,
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND phase = 'preparing'
              AND cancel_requested_at IS NULL
            """,
            (next_phase, next_phase, now, run_id),
        )
        advanced = int(cursor.rowcount or 0) > 0
        connection.commit()
        return advanced
    except Exception:
        connection.rollback()
        raise


def ingest_v2_acquire_writer_lease(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    writer_id: str,
) -> bool:
    now_dt = datetime.now(timezone.utc)
    now = format_utc_timestamp(now_dt)
    stale_cutoff = ingest_v2_stale_lease_cutoff(now_dt)
    lease_expires_at = lease_expiration_after(INGEST_V2_WORK_ITEM_LEASE_SECONDS, now=now_dt)
    connection.execute("BEGIN IMMEDIATE")
    try:
        cursor = connection.execute(
            """
            UPDATE ingest_runs
            SET committer_lease_owner = ?,
                committer_lease_expires_at = ?,
                committer_heartbeat_at = ?,
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND phase = 'committing'
              AND status = 'committing'
              AND cancel_requested_at IS NULL
              AND (
                committer_lease_owner IS NULL
                OR committer_lease_expires_at IS NULL
                OR committer_lease_expires_at <= ?
                OR committer_heartbeat_at IS NULL
                OR committer_heartbeat_at <= ?
                OR committer_lease_owner = ?
              )
            """,
            (writer_id, lease_expires_at, now, now, run_id, now, stale_cutoff, writer_id),
        )
        acquired = int(cursor.rowcount or 0) > 0
        connection.commit()
        return acquired
    except Exception:
        connection.rollback()
        raise


def ingest_v2_release_writer_lease(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    writer_id: str,
) -> None:
    now = utc_now()
    connection.execute("BEGIN")
    try:
        connection.execute(
            """
            UPDATE ingest_runs
            SET committer_lease_owner = NULL,
                committer_lease_expires_at = NULL,
                committer_heartbeat_at = ?,
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND committer_lease_owner = ?
            """,
            (now, now, run_id, writer_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def ingest_v2_reclaim_stale_commit_items(connection: sqlite3.Connection, *, run_id: str) -> int:
    now_dt = datetime.now(timezone.utc)
    now = format_utc_timestamp(now_dt)
    stale_cutoff = ingest_v2_stale_lease_cutoff(now_dt)
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            """
            UPDATE ingest_work_items
            SET status = 'prepared',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE run_id = ?
              AND status = 'committing'
              AND lease_expires_at IS NOT NULL
              AND (
                lease_expires_at <= ?
                OR updated_at <= ?
              )
            """,
            (now, run_id, now, stale_cutoff),
        )
        reclaimed = int(cursor.rowcount or 0)
        if reclaimed:
            connection.execute(
                """
                INSERT INTO ingest_worker_events (
                  run_id, worker_id, event_type, phase, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    None,
                    "reclaim_stale_commit_items",
                    "commit",
                    compact_json_text({"reclaimed": reclaimed}),
                    now,
                ),
            )
        connection.commit()
        return reclaimed
    except Exception:
        connection.rollback()
        raise


def ingest_v2_reclaim_stale_work_items(connection: sqlite3.Connection, *, run_id: str) -> dict[str, int]:
    return {
        "prepare": ingest_v2_reclaim_stale_prepare_items(connection, run_id=run_id),
        "commit": ingest_v2_reclaim_stale_commit_items(connection, run_id=run_id),
    }


def ingest_v2_load_commit_cursor(connection: sqlite3.Connection, *, run_id: str) -> dict[str, object]:
    cursor_row = connection.execute(
        """
        SELECT cursor_json
        FROM ingest_phase_cursors
        WHERE run_id = ?
          AND phase = 'committing'
          AND cursor_key = 'loose_file_commit'
        """,
        (run_id,),
    ).fetchone()
    if cursor_row is not None:
        cursor = decode_json_text(cursor_row["cursor_json"], default={}) or {}
        if isinstance(cursor, dict):
            cursor.setdefault("schema_version", 1)
            cursor.setdefault("current_ingestion_batch", None)
            cursor.setdefault("actions", {})
            cursor.setdefault("freshness_fallbacks", 0)
            cursor.setdefault("production_stats", {})
            cursor.setdefault("slack_stats", {})
            cursor.setdefault("mbox_stats", {})
            cursor.setdefault("pst_stats", {})
            cursor.setdefault("container_current_ingestion_batches", {})
            return cursor
    return {
        "schema_version": 1,
        "current_ingestion_batch": None,
        "actions": {},
        "freshness_fallbacks": 0,
        "production_stats": {},
        "slack_stats": {},
        "mbox_stats": {},
        "pst_stats": {},
        "container_current_ingestion_batches": {},
    }


def ingest_v2_run_loose_rel_paths(connection: sqlite3.Connection, *, run_id: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT rel_path
        FROM ingest_work_items
        WHERE run_id = ?
          AND unit_type = 'loose_file'
          AND rel_path IS NOT NULL
        """,
        (run_id,),
    ).fetchall()
    return {str(row["rel_path"]) for row in rows}


def ingest_v2_run_slack_seen_rel_paths_by_root(
    connection: sqlite3.Connection,
    *,
    run_id: str,
) -> dict[str, set[str]]:
    rows = connection.execute(
        """
        SELECT payload_json
        FROM ingest_work_items
        WHERE run_id = ?
          AND unit_type = 'slack_conversation'
        """,
        (run_id,),
    ).fetchall()
    seen_by_root: dict[str, set[str]] = {}
    for row in rows:
        payload = decode_json_text(row["payload_json"], default={}) or {}
        if not isinstance(payload, dict):
            continue
        conversation_identity = payload.get("conversation_identity")
        identity_root = ""
        if isinstance(conversation_identity, list) and len(conversation_identity) > 1:
            identity_root = str(conversation_identity[1])
        rel_root = normalize_whitespace(str(payload.get("source_locator") or identity_root))
        if not rel_root:
            continue
        rel_paths = {
            normalize_whitespace(str(rel_path or "")).replace("\\", "/").strip("/")
            for rel_path in list(payload.get("rel_paths") or [])
            if normalize_whitespace(str(rel_path or ""))
        }
        seen_by_root.setdefault(rel_root, set()).update(rel_paths)
    return seen_by_root


def ingest_v2_mark_missing_slack_documents(connection: sqlite3.Connection, *, run_id: str) -> int:
    missing = 0
    for rel_root, seen_rel_paths in sorted(ingest_v2_run_slack_seen_rel_paths_by_root(connection, run_id=run_id).items()):
        dataset_row = get_dataset_row(
            connection,
            source_kind=SLACK_EXPORT_SOURCE_KIND,
            dataset_locator=rel_root,
        )
        if dataset_row is None:
            continue
        missing += mark_missing_slack_export_documents(
            connection,
            dataset_id=int(dataset_row["id"]),
            seen_rel_paths=seen_rel_paths,
        )
    return missing


def ingest_v2_run_mbox_source_rel_paths(connection: sqlite3.Connection, *, run_id: str) -> set[str]:
    cursor_row = connection.execute(
        """
        SELECT cursor_json
        FROM ingest_phase_cursors
        WHERE run_id = ?
          AND phase = 'planning'
          AND cursor_key = 'loose_file_scan'
        """,
        (run_id,),
    ).fetchone()
    source_rel_paths: set[str] = set()
    if cursor_row is not None:
        cursor = decode_json_text(cursor_row["cursor_json"], default={}) or {}
        if isinstance(cursor, dict):
            source_rel_paths.update(
                str(rel_path)
                for rel_path in list(cursor.get("scanned_mbox_source_rel_paths") or [])
                if normalize_whitespace(str(rel_path or ""))
            )
    rows = connection.execute(
        """
        SELECT rel_path
        FROM ingest_work_items
        WHERE run_id = ?
          AND unit_type = 'mbox_source_finalizer'
          AND rel_path IS NOT NULL
        """,
        (run_id,),
    ).fetchall()
    source_rel_paths.update(str(row["rel_path"]) for row in rows)
    return source_rel_paths


def ingest_v2_redelete_retired_mbox_documents(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: str,
) -> int:
    source_rel_paths = sorted(ingest_v2_run_mbox_source_rel_paths(connection, run_id=run_id))
    if not source_rel_paths:
        return 0
    placeholders = ", ".join("?" for _ in source_rel_paths)
    root_occurrence_rows = connection.execute(
        f"""
        SELECT id
        FROM document_occurrences
        WHERE parent_occurrence_id IS NULL
          AND source_kind = ?
          AND source_rel_path IN ({placeholders})
          AND lifecycle_status IN ('deleted', 'missing')
        ORDER BY id ASC
        """,
        [MBOX_SOURCE_KIND, *source_rel_paths],
    ).fetchall()
    root_occurrence_ids = [int(row["id"]) for row in root_occurrence_rows]
    if not root_occurrence_ids:
        return 0
    now = utc_now()
    occurrence_placeholders = ", ".join("?" for _ in root_occurrence_ids)
    connection.execute(
        f"""
        UPDATE document_occurrences
        SET lifecycle_status = 'deleted', updated_at = ?
        WHERE lifecycle_status != 'deleted'
          AND (id IN ({occurrence_placeholders}) OR parent_occurrence_id IN ({occurrence_placeholders}))
        """,
        [now, *root_occurrence_ids, *root_occurrence_ids],
    )
    document_ids = container_document_ids_for_root_occurrence_ids(connection, root_occurrence_ids)
    deleted = delete_documents_with_only_deleted_occurrences(
        connection,
        paths,
        document_ids,
        deleted_at=now,
    )
    return deleted + force_delete_documents_with_no_active_occurrences(
        connection,
        document_ids,
        deleted_at=now,
    )


def force_delete_documents_with_no_active_occurrences(
    connection: sqlite3.Connection,
    document_ids: set[int],
    *,
    deleted_at: str,
) -> int:
    forced = 0
    for document_id in sorted(int(value) for value in document_ids):
        active_row = connection.execute(
            """
            SELECT 1
            FROM document_occurrences
            WHERE document_id = ?
              AND lifecycle_status != 'deleted'
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()
        if active_row is not None:
            continue
        cursor = connection.execute(
            """
            UPDATE documents
            SET lifecycle_status = 'deleted', updated_at = ?
            WHERE id = ?
              AND lifecycle_status != 'deleted'
            """,
            (deleted_at, document_id),
        )
        forced += int(cursor.rowcount or 0)
    return forced


def ingest_v2_run_pst_source_rel_paths(connection: sqlite3.Connection, *, run_id: str) -> set[str]:
    cursor_row = connection.execute(
        """
        SELECT cursor_json
        FROM ingest_phase_cursors
        WHERE run_id = ?
          AND phase = 'planning'
          AND cursor_key = 'loose_file_scan'
        """,
        (run_id,),
    ).fetchone()
    source_rel_paths: set[str] = set()
    if cursor_row is not None:
        cursor = decode_json_text(cursor_row["cursor_json"], default={}) or {}
        if isinstance(cursor, dict):
            source_rel_paths.update(
                str(rel_path)
                for rel_path in list(cursor.get("scanned_pst_source_rel_paths") or [])
                if normalize_whitespace(str(rel_path or ""))
            )
    rows = connection.execute(
        """
        SELECT rel_path
        FROM ingest_work_items
        WHERE run_id = ?
          AND unit_type = 'pst_source_finalizer'
          AND rel_path IS NOT NULL
        """,
        (run_id,),
    ).fetchall()
    source_rel_paths.update(str(row["rel_path"]) for row in rows)
    return source_rel_paths


def ingest_v2_redelete_retired_pst_documents(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: str,
) -> int:
    source_rel_paths = sorted(ingest_v2_run_pst_source_rel_paths(connection, run_id=run_id))
    if not source_rel_paths:
        return 0
    placeholders = ", ".join("?" for _ in source_rel_paths)
    root_occurrence_rows = connection.execute(
        f"""
        SELECT id
        FROM document_occurrences
        WHERE parent_occurrence_id IS NULL
          AND source_kind = ?
          AND source_rel_path IN ({placeholders})
          AND lifecycle_status IN ('deleted', 'missing')
        ORDER BY id ASC
        """,
        [PST_SOURCE_KIND, *source_rel_paths],
    ).fetchall()
    root_occurrence_ids = [int(row["id"]) for row in root_occurrence_rows]
    if not root_occurrence_ids:
        return 0
    now = utc_now()
    occurrence_placeholders = ", ".join("?" for _ in root_occurrence_ids)
    connection.execute(
        f"""
        UPDATE document_occurrences
        SET lifecycle_status = 'deleted', updated_at = ?
        WHERE lifecycle_status != 'deleted'
          AND (id IN ({occurrence_placeholders}) OR parent_occurrence_id IN ({occurrence_placeholders}))
        """,
        [now, *root_occurrence_ids, *root_occurrence_ids],
    )
    document_ids = container_document_ids_for_root_occurrence_ids(connection, root_occurrence_ids)
    deleted = delete_documents_with_only_deleted_occurrences(
        connection,
        paths,
        document_ids,
        deleted_at=now,
    )
    return deleted + force_delete_documents_with_no_active_occurrences(
        connection,
        document_ids,
        deleted_at=now,
    )


def ingest_v2_load_loose_file_commit_state(
    connection: sqlite3.Connection,
    *,
    root: Path,
    run_row: sqlite3.Row,
) -> tuple[dict[str, sqlite3.Row], dict[str, list[sqlite3.Row]]]:
    run_id = str(run_row["run_id"])
    scanned_rel_paths = ingest_v2_run_loose_rel_paths(connection, run_id=run_id)
    scan_scope = ingest_v2_scan_scope_from_run(root, run_row)
    existing_by_rel, unseen_existing_by_hash = load_loose_file_commit_state(
        connection,
        scanned_rel_paths,
        set(),
        set(),
        scan_scope=scan_scope,
    )
    consumed_rows = connection.execute(
        """
        SELECT source_occurrence_id
        FROM ingest_rename_consumptions
        WHERE run_id = ?
          AND source_occurrence_id IS NOT NULL
        """,
        (run_id,),
    ).fetchall()
    consumed_occurrence_ids = {int(row["source_occurrence_id"]) for row in consumed_rows}
    if consumed_occurrence_ids:
        for file_hash, rows in list(unseen_existing_by_hash.items()):
            filtered_rows = [row for row in rows if int(row["id"]) not in consumed_occurrence_ids]
            if filtered_rows:
                unseen_existing_by_hash[file_hash] = filtered_rows
            else:
                unseen_existing_by_hash.pop(file_hash, None)
    return existing_by_rel, unseen_existing_by_hash


def ingest_v2_claim_next_commit_item(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    writer_id: str,
) -> sqlite3.Row | None:
    now_dt = datetime.now(timezone.utc)
    now = format_utc_timestamp(now_dt)
    lease_expires_at = lease_expiration_after(INGEST_V2_WORK_ITEM_LEASE_SECONDS, now=now_dt)
    connection.execute("BEGIN IMMEDIATE")
    try:
        run_row = require_ingest_v2_run_row(connection, run_id)
        if (
            str(run_row["phase"]) != "committing"
            or str(run_row["status"]) != "committing"
            or run_row["cancel_requested_at"] is not None
            or run_row["committer_lease_owner"] != writer_id
            or not lease_is_active(run_row["committer_lease_expires_at"], now=now_dt)
        ):
            connection.rollback()
            return None
        item_row = connection.execute(
            """
            SELECT id
            FROM ingest_work_items
            WHERE run_id = ?
              AND status = 'prepared'
            ORDER BY commit_order ASC, id ASC
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        if item_row is None:
            connection.commit()
            return None
        work_item_id = int(item_row["id"])
        connection.execute(
            """
            UPDATE ingest_work_items
            SET status = 'committing',
                lease_owner = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE run_id = ?
              AND id = ?
              AND status = 'prepared'
            """,
            (writer_id, lease_expires_at, now, run_id, work_item_id),
        )
        connection.execute(
            """
            UPDATE ingest_runs
            SET committer_heartbeat_at = ?,
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND committer_lease_owner = ?
            """,
            (now, now, run_id, writer_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return connection.execute(
        """
        SELECT wi.*, pi.payload_kind, pi.payload_json AS prepared_payload_json,
               pi.source_fingerprint_json, pi.error_json
        FROM ingest_work_items wi
        JOIN ingest_prepared_items pi ON pi.work_item_id = wi.id
        WHERE wi.run_id = ?
          AND wi.id = ?
          AND wi.status = 'committing'
          AND wi.lease_owner = ?
        """,
        (run_id, work_item_id, writer_id),
    ).fetchone()


def ingest_v2_prepared_item_from_row(row: sqlite3.Row) -> dict[str, object]:
    payload = decode_json_text(row["prepared_payload_json"], default={}) or {}
    restored_payload = ingest_v2_json_restore_value(payload)
    if isinstance(restored_payload, dict) and isinstance(restored_payload.get("prepared_item"), dict):
        return dict(restored_payload["prepared_item"])
    raise RetrieverError(f"Prepared payload for work item {row['id']} is malformed.")


def ingest_v2_mark_commit_failed(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    message: str,
) -> bool:
    now = utc_now()
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            """
            UPDATE ingest_work_items
            SET status = 'failed',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?,
                last_error = ?
            WHERE run_id = ?
              AND id = ?
              AND status = 'committing'
              AND lease_owner = ?
            """,
            (now, message, run_id, work_item_id, writer_id),
        )
        marked = int(cursor.rowcount or 0) > 0
        if marked:
            connection.execute(
                """
                INSERT INTO ingest_worker_events (
                  run_id, worker_id, event_type, work_item_id, phase, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    writer_id,
                    "commit_failed",
                    work_item_id,
                    "commit",
                    compact_json_text({"error": message}),
                    now,
                ),
            )
        connection.commit()
        return marked
    except Exception:
        connection.rollback()
        raise


def ingest_v2_commit_work_item_hook(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    result: dict[str, object],
) -> None:
    now = utc_now()
    action = str(result.get("action") or "")
    affected_document_ids = [
        int(result["document_id"])
    ] if result.get("document_id") is not None else []
    if action == "renamed" and result.get("source_occurrence_id") is not None:
        connection.execute(
            """
            INSERT INTO ingest_rename_consumptions (
              run_id, target_work_item_id, source_document_id, source_occurrence_id, file_hash, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                work_item_id,
                result.get("source_document_id"),
                result.get("source_occurrence_id"),
                str(result.get("file_hash") or ""),
                now,
            ),
        )
    cursor["current_ingestion_batch"] = result.get("current_ingestion_batch")
    actions = cursor.setdefault("actions", {})
    if isinstance(actions, dict):
        actions[action] = int(actions.get(action) or 0) + 1
    if bool(result.get("freshness_fallback")):
        cursor["freshness_fallbacks"] = int(cursor.get("freshness_fallbacks") or 0) + 1
    ingest_v2_save_phase_cursor(
        connection,
        run_id=run_id,
        phase="committing",
        cursor_key="loose_file_commit",
        cursor=cursor,
        status="pending",
    )
    update_cursor = connection.execute(
        """
        UPDATE ingest_work_items
        SET status = 'committed',
            lease_owner = NULL,
            lease_expires_at = NULL,
            affected_document_ids_json = ?,
            artifact_manifest_json = ?,
            updated_at = ?,
            last_error = NULL
        WHERE run_id = ?
          AND id = ?
          AND status = 'committing'
          AND lease_owner = ?
        """,
        (
            compact_json_text(affected_document_ids),
            compact_json_text(
                {
                    "commit_action": action,
                    "freshness_fallback": bool(result.get("freshness_fallback")),
                    "document_id": result.get("document_id"),
                }
            ),
            now,
            run_id,
            work_item_id,
            writer_id,
        ),
    )
    if int(update_cursor.rowcount or 0) != 1:
        raise RetrieverError(f"Could not mark V2 ingest work item {work_item_id} committed.")
    connection.execute(
        """
        UPDATE ingest_runs
        SET committer_heartbeat_at = ?,
            last_heartbeat_at = ?
        WHERE run_id = ?
          AND committer_lease_owner = ?
        """,
        (now, now, run_id, writer_id),
    )


def ingest_v2_commit_conversation_preview_work_item(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    prepared_item: dict[str, object],
) -> dict[str, object]:
    conversation_id = int(prepared_item.get("conversation_id") or 0)
    if conversation_id <= 0:
        raise RetrieverError("Conversation preview work item is missing conversation_id.")
    now = utc_now()
    connection.execute("BEGIN")
    try:
        connection.execute("SAVEPOINT ingest_v2_conversation_preview_item")
        try:
            refreshed = refresh_conversation_previews(connection, paths, [conversation_id])
            connection.execute("RELEASE SAVEPOINT ingest_v2_conversation_preview_item")
        except Exception:
            connection.execute("ROLLBACK TO SAVEPOINT ingest_v2_conversation_preview_item")
            connection.execute("RELEASE SAVEPOINT ingest_v2_conversation_preview_item")
            raise
        actions = cursor.setdefault("actions", {})
        if isinstance(actions, dict):
            actions["conversation_preview"] = int(actions.get("conversation_preview") or 0) + 1
        stats = cursor.setdefault("conversation_preview_stats", {})
        if isinstance(stats, dict):
            stats["refreshed"] = int(stats.get("refreshed") or 0) + int(refreshed)
        ingest_v2_save_phase_cursor(
            connection,
            run_id=run_id,
            phase="committing",
            cursor_key="loose_file_commit",
            cursor=cursor,
            status="pending",
        )
        update_cursor = connection.execute(
            """
            UPDATE ingest_work_items
            SET status = 'committed',
                lease_owner = NULL,
                lease_expires_at = NULL,
                affected_conversation_keys_json = ?,
                artifact_manifest_json = ?,
                updated_at = ?,
                last_error = NULL
            WHERE run_id = ?
              AND id = ?
              AND status = 'committing'
              AND lease_owner = ?
            """,
            (
                compact_json_text([str(conversation_id)]),
                compact_json_text(
                    {
                        "commit_action": "conversation_preview",
                        "conversation_id": conversation_id,
                        "refreshed": int(refreshed),
                    }
                ),
                now,
                run_id,
                work_item_id,
                writer_id,
            ),
        )
        if int(update_cursor.rowcount or 0) != 1:
            raise RetrieverError(f"Could not mark V2 ingest conversation preview work item {work_item_id} committed.")
        connection.execute(
            """
            UPDATE ingest_runs
            SET committer_heartbeat_at = ?,
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND committer_lease_owner = ?
            """,
            (now, now, run_id, writer_id),
        )
        connection.commit()
        return {
            "action": "conversation_preview",
            "conversation_id": conversation_id,
            "refreshed": int(refreshed),
        }
    except Exception:
        connection.rollback()
        raise


def ingest_v2_ensure_production_context(
    connection: sqlite3.Connection,
    root: Path,
    prepared_item: dict[str, object],
) -> tuple[int, int, int]:
    production_rel_root = str(prepared_item["production_rel_root"])
    production_name = str(prepared_item["production_name"])
    metadata_load_rel_path = str(prepared_item["metadata_load_rel_path"])
    image_load_rel_path = normalize_whitespace(str(prepared_item.get("image_load_rel_path") or "")) or None
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=PRODUCTION_SOURCE_KIND,
        source_locator=production_rel_root,
        dataset_name=production_dataset_name(production_rel_root, production_name),
    )
    production_id = upsert_production_row(
        connection,
        dataset_id=dataset_id,
        rel_root=production_rel_root,
        production_name=production_name,
        metadata_load_rel_path=metadata_load_rel_path,
        image_load_rel_path=image_load_rel_path,
        source_type=str(prepared_item["source_type"]),
    )
    connection.commit()
    return dataset_id, dataset_source_id, production_id


def ingest_v2_commit_production_work_item_hook(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    result: dict[str, object],
) -> None:
    now = utc_now()
    action = str(result.get("action") or "")
    affected_document_ids = [
        int(result["document_id"])
    ] if result.get("document_id") is not None else []
    production_stats = cursor.setdefault("production_stats", {})
    if isinstance(production_stats, dict):
        production_stats[action] = int(production_stats.get(action) or 0) + 1
        production_stats["page_images_linked"] = (
            int(production_stats.get("page_images_linked") or 0)
            + int(result.get("page_images_linked") or 0)
        )
    ingest_v2_save_phase_cursor(
        connection,
        run_id=run_id,
        phase="committing",
        cursor_key="loose_file_commit",
        cursor=cursor,
        status="pending",
    )
    update_cursor = connection.execute(
        """
        UPDATE ingest_work_items
        SET status = 'committed',
            lease_owner = NULL,
            lease_expires_at = NULL,
            affected_document_ids_json = ?,
            affected_entity_ids_json = ?,
            artifact_manifest_json = ?,
            updated_at = ?,
            last_error = NULL
        WHERE run_id = ?
          AND id = ?
          AND status = 'committing'
          AND lease_owner = ?
        """,
        (
            compact_json_text(affected_document_ids),
            compact_json_text([]),
            compact_json_text(
                {
                    "commit_action": action,
                    "document_id": result.get("document_id"),
                    "production_id": result.get("production_id"),
                    "control_number": result.get("control_number"),
                    "page_images_linked": int(result.get("page_images_linked") or 0),
                }
            ),
            now,
            run_id,
            work_item_id,
            writer_id,
        ),
    )
    if int(update_cursor.rowcount or 0) != 1:
        raise RetrieverError(f"Could not mark V2 ingest production work item {work_item_id} committed.")
    connection.execute(
        """
        UPDATE ingest_runs
        SET committer_heartbeat_at = ?,
            last_heartbeat_at = ?
        WHERE run_id = ?
          AND committer_lease_owner = ?
        """,
        (now, now, run_id, writer_id),
    )


def ingest_v2_commit_production_preview_batch(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    production_id: int,
    prepared_item: dict[str, object],
) -> dict[str, object]:
    control_number = str(prepared_item["control_number"])
    prepare_error = normalize_whitespace(str(prepared_item.get("prepare_error") or "")) or None
    if prepare_error:
        raise RetrieverError(prepare_error)
    page_assets = [
        dict(asset)
        for asset in list(prepared_item.get("page_assets") or [])
        if isinstance(asset, dict)
    ]
    connection.execute("BEGIN")
    try:
        document_row = connection.execute(
            """
            SELECT id
            FROM documents
            WHERE production_id = ?
              AND control_number = ?
              AND lifecycle_status != 'deleted'
            ORDER BY id ASC
            LIMIT 1
            """,
            (production_id, control_number),
        ).fetchone()
        if document_row is None:
            raise RetrieverError(f"Production document {control_number} is not committed yet.")
        document_id = int(document_row["id"])
        preview_rows: list[dict[str, object]] = []
        rel_preview_paths: list[str] = []
        for asset in page_assets:
            rel_preview_path = normalize_whitespace(str(asset.get("rel_preview_path") or ""))
            payload = asset.get("payload")
            if not rel_preview_path or not isinstance(payload, (bytes, bytearray)):
                continue
            abs_path = paths["state_dir"] / rel_preview_path
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            abs_path.write_bytes(bytes(payload))
            rel_preview_paths.append(rel_preview_path)
            preview_rows.append(
                {
                    "rel_preview_path": rel_preview_path,
                    "preview_type": "image",
                    "target_fragment": None,
                    "label": asset.get("label"),
                    "ordinal": int(asset.get("ordinal") or 0),
                    "created_at": utc_now(),
                }
            )
        if rel_preview_paths:
            placeholders = ",".join("?" for _ in rel_preview_paths)
            connection.execute(
                f"""
                DELETE FROM document_previews
                WHERE document_id = ?
                  AND rel_preview_path IN ({placeholders})
                """,
                (document_id, *rel_preview_paths),
            )
            insert_document_preview_rows(connection, document_id, preview_rows)
        result = {
            "action": "preview_batch",
            "control_number": control_number,
            "document_id": document_id,
            "production_id": production_id,
            "page_images_linked": 0,
            "preview_pages_generated": len(preview_rows),
        }
        ingest_v2_commit_production_work_item_hook(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            writer_id=writer_id,
            cursor=cursor,
            result=result,
        )
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise


def ingest_v2_commit_slack_conversation_work_item_hook(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    result: dict[str, object],
) -> None:
    now = utc_now()
    action = str(result.get("action") or "")
    affected_document_ids = [
        int(document_id)
        for document_id in list(result.get("affected_document_ids") or [])
        if document_id is not None
    ]
    source_locator = str(result.get("source_locator") or "")
    conversation_key = str(result.get("conversation_key") or "")
    current_batch = result.get("current_batch")
    if current_batch is not None:
        cursor["current_ingestion_batch"] = int(current_batch)
    slack_stats = cursor.setdefault("slack_stats", {})
    if isinstance(slack_stats, dict):
        slack_stats["slack_conversations"] = int(slack_stats.get("slack_conversations") or 0) + 1
        slack_stats["slack_documents_created"] = (
            int(slack_stats.get("slack_documents_created") or 0)
            + int(result.get("new") or 0)
        )
        slack_stats["slack_documents_updated"] = (
            int(slack_stats.get("slack_documents_updated") or 0)
            + int(result.get("updated") or 0)
        )
    ingest_v2_save_phase_cursor(
        connection,
        run_id=run_id,
        phase="committing",
        cursor_key="loose_file_commit",
        cursor=cursor,
        status="pending",
    )
    update_cursor = connection.execute(
        """
        UPDATE ingest_work_items
        SET status = 'committed',
            lease_owner = NULL,
            lease_expires_at = NULL,
            affected_document_ids_json = ?,
            affected_conversation_keys_json = ?,
            affected_entity_ids_json = ?,
            artifact_manifest_json = ?,
            updated_at = ?,
            last_error = NULL
        WHERE run_id = ?
          AND id = ?
          AND status = 'committing'
          AND lease_owner = ?
        """,
        (
            compact_json_text(affected_document_ids),
            compact_json_text([f"{source_locator}:{conversation_key}"] if source_locator and conversation_key else []),
            compact_json_text([]),
            compact_json_text(
                {
                    "commit_action": action,
                    "source_kind": SLACK_EXPORT_SOURCE_KIND,
                    "source_locator": source_locator,
                    "conversation_key": conversation_key,
                    "new": int(result.get("new") or 0),
                    "updated": int(result.get("updated") or 0),
                    "rel_paths": list(result.get("rel_paths") or []),
                }
            ),
            now,
            run_id,
            work_item_id,
            writer_id,
        ),
    )
    if int(update_cursor.rowcount or 0) != 1:
        raise RetrieverError(f"Could not mark V2 ingest Slack work item {work_item_id} committed.")
    connection.execute(
        """
        UPDATE ingest_runs
        SET committer_heartbeat_at = ?,
            last_heartbeat_at = ?
        WHERE run_id = ?
          AND committer_lease_owner = ?
        """,
        (now, now, run_id, writer_id),
    )


def ingest_v2_commit_mbox_work_item_hook(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    result: dict[str, object],
) -> None:
    now = utc_now()
    action = str(result.get("action") or "")
    source_rel_path = str(result.get("source_rel_path") or "")
    affected_document_ids = [
        int(result["document_id"])
    ] if result.get("document_id") is not None else []
    current_batch = result.get("current_ingestion_batch")
    if source_rel_path and current_batch is not None:
        current_batches = cursor.setdefault("container_current_ingestion_batches", {})
        if isinstance(current_batches, dict):
            current_batches[source_rel_path] = int(current_batch)
    mbox_stats = cursor.setdefault("mbox_stats", {})
    if isinstance(mbox_stats, dict):
        if action == "new":
            mbox_stats["mbox_messages_created"] = int(mbox_stats.get("mbox_messages_created") or 0) + 1
        elif action == "updated":
            mbox_stats["mbox_messages_updated"] = int(mbox_stats.get("mbox_messages_updated") or 0) + 1
        for key in ("mbox_sources_skipped", "mbox_sources_finalized", "mbox_messages_deleted"):
            if result.get(key) is not None:
                mbox_stats[key] = int(mbox_stats.get(key) or 0) + int(result.get(key) or 0)
        if result.get("gmail_linked_drive_retired") is not None:
            mbox_stats["gmail_linked_drive_retired"] = (
                int(mbox_stats.get("gmail_linked_drive_retired") or 0)
                + int(result.get("gmail_linked_drive_retired") or 0)
            )
    ingest_v2_save_phase_cursor(
        connection,
        run_id=run_id,
        phase="committing",
        cursor_key="loose_file_commit",
        cursor=cursor,
        status="pending",
    )
    update_cursor = connection.execute(
        """
        UPDATE ingest_work_items
        SET status = 'committed',
            lease_owner = NULL,
            lease_expires_at = NULL,
            affected_document_ids_json = ?,
            affected_entity_ids_json = ?,
            artifact_manifest_json = ?,
            updated_at = ?,
            last_error = NULL
        WHERE run_id = ?
          AND id = ?
          AND status = 'committing'
          AND lease_owner = ?
        """,
        (
            compact_json_text(affected_document_ids),
            compact_json_text([]),
            compact_json_text(
                {
                    "commit_action": action,
                    "document_id": result.get("document_id"),
                    "source_kind": MBOX_SOURCE_KIND,
                    "source_plan_kind": result.get("source_plan_kind"),
                    "source_rel_path": source_rel_path,
                    "source_item_id": result.get("source_item_id"),
                    "mbox_messages_deleted": int(result.get("mbox_messages_deleted") or 0),
                }
            ),
            now,
            run_id,
            work_item_id,
            writer_id,
        ),
    )
    if int(update_cursor.rowcount or 0) != 1:
        raise RetrieverError(f"Could not mark V2 ingest MBOX work item {work_item_id} committed.")
    connection.execute(
        """
        UPDATE ingest_runs
        SET committer_heartbeat_at = ?,
            last_heartbeat_at = ?
        WHERE run_id = ?
          AND committer_lease_owner = ?
        """,
        (now, now, run_id, writer_id),
    )


def ingest_v2_commit_pst_work_item_hook(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    result: dict[str, object],
) -> None:
    now = utc_now()
    action = str(result.get("action") or "")
    source_rel_path = str(result.get("source_rel_path") or "")
    affected_document_ids = [
        int(result["document_id"])
    ] if result.get("document_id") is not None else []
    current_batch = result.get("current_ingestion_batch")
    if source_rel_path and current_batch is not None:
        current_batches = cursor.setdefault("container_current_ingestion_batches", {})
        if isinstance(current_batches, dict):
            current_batches[source_rel_path] = int(current_batch)
    pst_stats = cursor.setdefault("pst_stats", {})
    if isinstance(pst_stats, dict):
        if action == "new":
            pst_stats["pst_messages_created"] = int(pst_stats.get("pst_messages_created") or 0) + 1
        elif action == "updated":
            pst_stats["pst_messages_updated"] = int(pst_stats.get("pst_messages_updated") or 0) + 1
        for key in ("pst_sources_skipped", "pst_sources_finalized", "pst_messages_deleted"):
            if result.get(key) is not None:
                pst_stats[key] = int(pst_stats.get(key) or 0) + int(result.get(key) or 0)
    ingest_v2_save_phase_cursor(
        connection,
        run_id=run_id,
        phase="committing",
        cursor_key="loose_file_commit",
        cursor=cursor,
        status="pending",
    )
    update_cursor = connection.execute(
        """
        UPDATE ingest_work_items
        SET status = 'committed',
            lease_owner = NULL,
            lease_expires_at = NULL,
            affected_document_ids_json = ?,
            affected_entity_ids_json = ?,
            artifact_manifest_json = ?,
            updated_at = ?,
            last_error = NULL
        WHERE run_id = ?
          AND id = ?
          AND status = 'committing'
          AND lease_owner = ?
        """,
        (
            compact_json_text(affected_document_ids),
            compact_json_text([]),
            compact_json_text(
                {
                    "commit_action": action,
                    "document_id": result.get("document_id"),
                    "source_kind": PST_SOURCE_KIND,
                    "source_plan_kind": result.get("source_plan_kind"),
                    "source_rel_path": source_rel_path,
                    "source_item_id": result.get("source_item_id"),
                    "pst_messages_deleted": int(result.get("pst_messages_deleted") or 0),
                }
            ),
            now,
            run_id,
            work_item_id,
            writer_id,
        ),
    )
    if int(update_cursor.rowcount or 0) != 1:
        raise RetrieverError(f"Could not mark V2 ingest PST work item {work_item_id} committed.")
    connection.execute(
        """
        UPDATE ingest_runs
        SET committer_heartbeat_at = ?,
            last_heartbeat_at = ?
        WHERE run_id = ?
          AND committer_lease_owner = ?
        """,
        (now, now, run_id, writer_id),
    )


def ingest_v2_ensure_mbox_commit_context(
    connection: sqlite3.Connection,
    root: Path,
    *,
    prepared_item: dict[str, object],
    cursor: dict[str, object],
    contexts: dict[str, dict[str, object]],
) -> dict[str, object]:
    source_rel_path = str(prepared_item["source_rel_path"])
    if source_rel_path in contexts:
        return contexts[source_rel_path]
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=MBOX_SOURCE_KIND,
        source_locator=source_rel_path,
        dataset_name=mbox_dataset_name(source_rel_path),
    )
    if connection.in_transaction:
        connection.commit()
    connection.execute("BEGIN")
    try:
        write_container_source_scan_started(
            connection,
            dataset_id=dataset_id,
            source_kind=MBOX_SOURCE_KIND,
            source_rel_path=source_rel_path,
            file_size=(
                int(prepared_item["source_file_size"])
                if prepared_item.get("source_file_size") is not None
                else None
            ),
            file_mtime=(
                str(prepared_item["source_file_mtime"])
                if prepared_item.get("source_file_mtime") is not None
                else None
            ),
            file_hash=str(prepared_item.get("source_file_hash") or ""),
            scan_started_at=str(prepared_item["scan_started_at"]),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    current_batches = cursor.setdefault("container_current_ingestion_batches", {})
    current_batch = None
    if isinstance(current_batches, dict) and current_batches.get(source_rel_path) is not None:
        current_batch = int(current_batches[source_rel_path])
    context = {
        "dataset_id": int(dataset_id),
        "dataset_source_id": int(dataset_source_id) if dataset_source_id is not None else None,
        "current_ingestion_batch": current_batch,
        "existing_entries_by_source_item": existing_container_entries_by_source_item(
            connection,
            source_kind=MBOX_SOURCE_KIND,
            source_rel_path=source_rel_path,
        ),
    }
    contexts[source_rel_path] = context
    return context


def ingest_v2_mbox_failed_message_count(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_rel_path: str,
) -> int:
    prefix = f"{source_rel_path}:"
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM ingest_work_items
        WHERE run_id = ?
          AND unit_type = 'mbox_message'
          AND status = 'failed'
          AND SUBSTR(COALESCE(source_key, ''), 1, ?) = ?
        """,
        (run_id, len(prefix), prefix),
    ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def ingest_v2_ensure_pst_commit_context(
    connection: sqlite3.Connection,
    root: Path,
    *,
    prepared_item: dict[str, object],
    cursor: dict[str, object],
    contexts: dict[str, dict[str, object]],
) -> dict[str, object]:
    source_rel_path = str(prepared_item["source_rel_path"])
    if source_rel_path in contexts:
        return contexts[source_rel_path]
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=PST_SOURCE_KIND,
        source_locator=source_rel_path,
        dataset_name=pst_dataset_name(source_rel_path),
    )
    if connection.in_transaction:
        connection.commit()
    connection.execute("BEGIN")
    try:
        write_container_source_scan_started(
            connection,
            dataset_id=dataset_id,
            source_kind=PST_SOURCE_KIND,
            source_rel_path=source_rel_path,
            file_size=(
                int(prepared_item["source_file_size"])
                if prepared_item.get("source_file_size") is not None
                else None
            ),
            file_mtime=(
                str(prepared_item["source_file_mtime"])
                if prepared_item.get("source_file_mtime") is not None
                else None
            ),
            file_hash=str(prepared_item.get("source_file_hash") or ""),
            scan_started_at=str(prepared_item["scan_started_at"]),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    current_batches = cursor.setdefault("container_current_ingestion_batches", {})
    current_batch = None
    if isinstance(current_batches, dict) and current_batches.get(source_rel_path) is not None:
        current_batch = int(current_batches[source_rel_path])
    context = {
        "dataset_id": int(dataset_id),
        "dataset_source_id": int(dataset_source_id) if dataset_source_id is not None else None,
        "current_ingestion_batch": current_batch,
        "existing_entries_by_source_item": existing_container_entries_by_source_item(
            connection,
            source_kind=PST_SOURCE_KIND,
            source_rel_path=source_rel_path,
        ),
    }
    contexts[source_rel_path] = context
    return context


def ingest_v2_pst_failed_message_count(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_rel_path: str,
) -> int:
    prefix = f"{source_rel_path}:"
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM ingest_work_items
        WHERE run_id = ?
          AND unit_type = 'pst_message'
          AND status = 'failed'
          AND SUBSTR(COALESCE(source_key, ''), 1, ?) = ?
        """,
        (run_id, len(prefix), prefix),
    ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def ingest_v2_commit_mbox_source_finalizer(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    prepared_item: dict[str, object],
) -> dict[str, object]:
    source_rel_path = str(prepared_item["source_rel_path"])
    failed_messages = ingest_v2_mbox_failed_message_count(
        connection,
        run_id=run_id,
        source_rel_path=source_rel_path,
    )
    if failed_messages:
        raise RetrieverError(
            f"Cannot finalize MBOX source {source_rel_path}: {failed_messages} message work item(s) failed."
        )
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=MBOX_SOURCE_KIND,
        source_locator=source_rel_path,
        dataset_name=mbox_dataset_name(source_rel_path),
    )
    if connection.in_transaction:
        connection.commit()
    source_file_size = (
        int(prepared_item["source_file_size"])
        if prepared_item.get("source_file_size") is not None
        else None
    )
    source_file_mtime = (
        str(prepared_item["source_file_mtime"])
        if prepared_item.get("source_file_mtime") is not None
        else None
    )
    source_file_hash = str(prepared_item.get("source_file_hash") or "")
    scan_started_at = str(prepared_item["scan_started_at"])
    message_count = int(prepared_item.get("message_count") or 0)
    skip_source = bool(prepared_item.get("skip_source"))
    source_plan_kind = str(prepared_item.get("source_plan_kind") or "mbox")
    connection.execute("BEGIN")
    try:
        linked_drive_retired = 0
        linked_drive_rel_paths = {
            normalize_whitespace(str(rel_path or "")).replace("\\", "/").strip("/")
            for rel_path in list(prepared_item.get("linked_drive_rel_paths") or [])
            if normalize_whitespace(str(rel_path or ""))
        }
        if source_plan_kind == "gmail" and linked_drive_rel_paths:
            linked_drive_retired = retire_standalone_filesystem_documents_by_rel_paths(
                connection,
                paths,
                rel_paths=linked_drive_rel_paths,
            )
        if skip_source:
            mark_container_source_documents_active(
                connection,
                source_kind=MBOX_SOURCE_KIND,
                source_rel_path=source_rel_path,
                seen_at=scan_started_at,
            )
            assign_dataset_to_container_documents(
                connection,
                source_kind=MBOX_SOURCE_KIND,
                source_rel_path=source_rel_path,
                dataset_id=dataset_id,
                dataset_source_id=dataset_source_id,
            )
            messages_deleted = 0
            action = "skipped"
            source_stats = {"mbox_sources_skipped": 1}
            scan_completed_at = scan_started_at
        else:
            messages_deleted = retire_unseen_container_messages(
                connection,
                paths,
                source_kind=MBOX_SOURCE_KIND,
                source_rel_path=source_rel_path,
                scan_started_at=scan_started_at,
            )
            action = "finalized"
            source_stats = {"mbox_sources_finalized": 1}
            scan_completed_at = next_monotonic_utc_timestamp([scan_started_at])
        write_container_source_scan_completed(
            connection,
            dataset_id=dataset_id,
            source_kind=MBOX_SOURCE_KIND,
            source_rel_path=source_rel_path,
            file_size=source_file_size,
            file_mtime=source_file_mtime,
            file_hash=source_file_hash,
            message_count=message_count,
            scan_started_at=scan_started_at,
            scan_completed_at=scan_completed_at,
        )
        result = {
            "action": action,
            "source_kind": MBOX_SOURCE_KIND,
            "source_rel_path": source_rel_path,
            "source_plan_kind": source_plan_kind,
            "current_ingestion_batch": None,
            "document_id": None,
            "mbox_messages_deleted": messages_deleted,
            "gmail_linked_drive_retired": linked_drive_retired,
            **source_stats,
        }
        ingest_v2_commit_mbox_work_item_hook(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            writer_id=writer_id,
            cursor=cursor,
            result=result,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return result


def ingest_v2_commit_pst_source_finalizer(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: str,
    work_item_id: int,
    writer_id: str,
    cursor: dict[str, object],
    prepared_item: dict[str, object],
) -> dict[str, object]:
    source_rel_path = str(prepared_item["source_rel_path"])
    failed_messages = ingest_v2_pst_failed_message_count(
        connection,
        run_id=run_id,
        source_rel_path=source_rel_path,
    )
    if failed_messages:
        raise RetrieverError(
            f"Cannot finalize PST source {source_rel_path}: {failed_messages} message work item(s) failed."
        )
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=PST_SOURCE_KIND,
        source_locator=source_rel_path,
        dataset_name=pst_dataset_name(source_rel_path),
    )
    if connection.in_transaction:
        connection.commit()
    source_file_size = (
        int(prepared_item["source_file_size"])
        if prepared_item.get("source_file_size") is not None
        else None
    )
    source_file_mtime = (
        str(prepared_item["source_file_mtime"])
        if prepared_item.get("source_file_mtime") is not None
        else None
    )
    source_file_hash = str(prepared_item.get("source_file_hash") or "")
    scan_started_at = str(prepared_item["scan_started_at"])
    skip_source = bool(prepared_item.get("skip_source"))
    source_plan_kind = str(prepared_item.get("source_plan_kind") or "pst")
    connection.execute("BEGIN")
    try:
        if skip_source:
            mark_container_source_documents_active(
                connection,
                source_kind=PST_SOURCE_KIND,
                source_rel_path=source_rel_path,
                seen_at=scan_started_at,
            )
            assign_dataset_to_container_documents(
                connection,
                source_kind=PST_SOURCE_KIND,
                source_rel_path=source_rel_path,
                dataset_id=dataset_id,
                dataset_source_id=dataset_source_id,
            )
            messages_deleted = 0
            action = "skipped"
            source_stats = {"pst_sources_skipped": 1}
            scan_completed_at = scan_started_at
            message_count = int(prepared_item.get("message_count") or 0)
        else:
            messages_deleted = retire_unseen_container_messages(
                connection,
                paths,
                source_kind=PST_SOURCE_KIND,
                source_rel_path=source_rel_path,
                scan_started_at=scan_started_at,
            )
            action = "finalized"
            source_stats = {"pst_sources_finalized": 1}
            scan_completed_at = next_monotonic_utc_timestamp([scan_started_at])
            message_count = len(
                container_root_occurrence_rows_for_source(
                    connection,
                    source_kind=PST_SOURCE_KIND,
                    source_rel_path=source_rel_path,
                )
            )
        write_container_source_scan_completed(
            connection,
            dataset_id=dataset_id,
            source_kind=PST_SOURCE_KIND,
            source_rel_path=source_rel_path,
            file_size=source_file_size,
            file_mtime=source_file_mtime,
            file_hash=source_file_hash,
            message_count=message_count,
            scan_started_at=scan_started_at,
            scan_completed_at=scan_completed_at,
        )
        result = {
            "action": action,
            "source_kind": PST_SOURCE_KIND,
            "source_rel_path": source_rel_path,
            "source_plan_kind": source_plan_kind,
            "current_ingestion_batch": None,
            "document_id": None,
            "pst_messages_deleted": messages_deleted,
            **source_stats,
        }
        ingest_v2_commit_pst_work_item_hook(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            writer_id=writer_id,
            cursor=cursor,
            result=result,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return result


def ingest_v2_maybe_advance_after_commit(connection: sqlite3.Connection, *, run_id: str) -> bool:
    row = require_ingest_v2_run_row(connection, run_id)
    if str(row["phase"]) != "committing" or row["cancel_requested_at"] is not None:
        return False
    remaining_commit_items = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM ingest_work_items
            WHERE run_id = ?
              AND status IN ('prepared', 'committing')
            """,
            (run_id,),
        ).fetchone()[0]
        or 0
    )
    if remaining_commit_items:
        return False
    remaining_prepare_items = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM ingest_work_items
            WHERE run_id = ?
              AND status IN ('pending', 'leased')
            """,
            (run_id,),
        ).fetchone()[0]
        or 0
    )
    next_phase = "preparing" if remaining_prepare_items else "finalizing"
    cursor = ingest_v2_load_commit_cursor(connection, run_id=run_id)
    now = utc_now()
    connection.execute("BEGIN")
    try:
        ingest_v2_save_phase_cursor(
            connection,
            run_id=run_id,
            phase="committing",
            cursor_key="loose_file_commit",
            cursor=cursor,
            status="pending" if remaining_prepare_items else "complete",
        )
        update_cursor = connection.execute(
            """
            UPDATE ingest_runs
            SET phase = ?,
                status = ?,
                committer_lease_owner = NULL,
                committer_lease_expires_at = NULL,
                committer_heartbeat_at = ?,
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND phase = 'committing'
              AND cancel_requested_at IS NULL
            """,
            (next_phase, next_phase, now, now, run_id),
        )
        advanced = int(update_cursor.rowcount or 0) > 0
        connection.commit()
        return advanced
    except Exception:
        connection.rollback()
        raise


def ingest_v2_planned_production_roots_by_rel_root(connection: sqlite3.Connection, *, run_id: str) -> dict[str, dict[str, object]]:
    cursor_row = connection.execute(
        """
        SELECT cursor_json
        FROM ingest_phase_cursors
        WHERE run_id = ?
          AND phase = 'planning'
          AND cursor_key = 'loose_file_scan'
        """,
        (run_id,),
    ).fetchone()
    if cursor_row is None:
        return {}
    cursor = decode_json_text(cursor_row["cursor_json"], default={}) or {}
    if not isinstance(cursor, dict):
        return {}
    roots_by_rel_root = dict(cursor.get("production_roots_by_rel_root") or {})
    planned_roots = [str(rel_root) for rel_root in list(cursor.get("planned_production_roots") or [])]
    return {
        rel_root: dict(roots_by_rel_root.get(rel_root) or {})
        for rel_root in planned_roots
        if roots_by_rel_root.get(rel_root)
    }


def ingest_v2_finalize_production_root(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    root: Path,
    *,
    production_payload: dict[str, object],
) -> dict[str, int]:
    dataset_id, _dataset_source_id, production_id = ingest_v2_ensure_production_context(
        connection,
        root,
        {
            "production_rel_root": production_payload["rel_root"],
            "production_name": production_payload["production_name"],
            "metadata_load_rel_path": production_payload["metadata_load_rel_path"],
            "image_load_rel_path": production_payload.get("image_load_rel_path"),
            "source_type": production_payload["source_type"],
        },
    )
    seen_control_numbers = {str(control_number) for control_number in list(production_payload.get("seen_control_numbers") or [])}
    retired = 0
    existing_rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE production_id = ?
        """,
        (production_id,),
    ).fetchall()
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
                SET lifecycle_status = 'deleted',
                    parent_document_id = NULL,
                    dataset_id = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (dataset_id, utc_now(), row["id"]),
            )
            connection.commit()
            retired += 1
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

    families_reconstructed = len(
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
    return {
        "retired": retired,
        "families_reconstructed": families_reconstructed,
        "parent_link_updates": int(parent_link_updates),
        "attachment_preview_updates": int(attachment_preview_updates),
    }


def ingest_v2_load_finalize_cursor(connection: sqlite3.Connection, *, run_id: str) -> dict[str, object]:
    cursor_row = connection.execute(
        """
        SELECT cursor_json
        FROM ingest_phase_cursors
        WHERE run_id = ?
          AND phase = 'finalizing'
          AND cursor_key = 'loose_file_finalize'
        """,
        (run_id,),
    ).fetchone()
    if cursor_row is not None:
        cursor = decode_json_text(cursor_row["cursor_json"], default={}) or {}
        if isinstance(cursor, dict):
            cursor.setdefault("schema_version", 1)
            cursor.setdefault("stage", "production")
            cursor.setdefault("pending_production_rel_roots", [])
            cursor.setdefault("production_roots_by_rel_root", {})
            cursor.setdefault("production_finalized_roots", [])
            cursor.setdefault("production_stats", {})
            cursor.setdefault("filesystem_missing", 0)
            cursor.setdefault("mbox_sources_missing", 0)
            cursor.setdefault("mbox_documents_missing", 0)
            cursor.setdefault("mbox_documents_redeleted", 0)
            cursor.setdefault("conversation_assignment", {})
            cursor.setdefault("conversation_previews_refreshed", 0)
            cursor.setdefault("pruned_unused_filesystem_dataset", False)
            return cursor
    production_roots_by_rel_root = ingest_v2_planned_production_roots_by_rel_root(connection, run_id=run_id)
    return {
        "schema_version": 1,
        "stage": "production" if production_roots_by_rel_root else "missing",
        "pending_production_rel_roots": sorted(production_roots_by_rel_root),
        "production_roots_by_rel_root": production_roots_by_rel_root,
        "production_finalized_roots": [],
        "production_stats": {},
        "filesystem_missing": 0,
        "mbox_sources_missing": 0,
        "mbox_documents_missing": 0,
        "mbox_documents_redeleted": 0,
        "conversation_assignment": {},
        "conversation_previews_refreshed": 0,
        "pruned_unused_filesystem_dataset": False,
    }


def ingest_v2_save_finalize_cursor(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    cursor: dict[str, object],
    status: str,
) -> None:
    ingest_v2_save_phase_cursor(
        connection,
        run_id=run_id,
        phase="finalizing",
        cursor_key="loose_file_finalize",
        cursor=cursor,
        status=status,
    )
    connection.execute(
        """
        UPDATE ingest_runs
        SET last_heartbeat_at = ?
        WHERE run_id = ?
        """,
        (utc_now(), run_id),
    )


def active_ingest_v2_run_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM ingest_runs
        WHERE status NOT IN ('completed', 'canceled', 'failed')
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()


def require_ingest_v2_run_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT *
        FROM ingest_runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown resumable ingest run: {run_id}")
    return row


def latest_ingest_v2_run_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM ingest_runs
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()


def ingest_v2_conflict_payload(root: Path, active_row: sqlite3.Row, *, message: str) -> dict[str, object]:
    run_id = str(active_row["run_id"])
    quoted_root = shlex.quote(str(root))
    quoted_run_id = shlex.quote(run_id)
    return {
        "ok": False,
        "error": "active_ingest_run",
        "active_run_id": run_id,
        "message": message,
        "status_command": f"ingest-status {quoted_root} --run-id {quoted_run_id}",
        "cancel_command": f"ingest-cancel {quoted_root} --run-id {quoted_run_id}",
    }


def raise_if_ingest_v2_active(connection: sqlite3.Connection, root: Path, *, command_name: str) -> None:
    active_row = active_ingest_v2_run_row(connection)
    if active_row is None:
        return
    raise RetrieverStructuredError(
        f"{command_name} cannot run while resumable ingest run {active_row['run_id']} is active.",
        ingest_v2_conflict_payload(
            root,
            active_row,
            message=f"{command_name} cannot run while a resumable ingest run is active.",
        ),
    )


def ingest_v2_status_counts(connection: sqlite3.Connection, *, run_id: str) -> dict[str, int]:
    counts = {status: 0 for status in INGEST_V2_WORK_ITEM_STATUSES}
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM ingest_work_items
        WHERE run_id = ?
        GROUP BY status
        """,
        (run_id,),
    ).fetchall()
    for row in rows:
        counts[str(row["status"])] = int(row["count"] or 0)
    return counts


def ingest_v2_unit_type_counts(connection: sqlite3.Connection, *, run_id: str) -> dict[str, dict[str, int]]:
    rows = connection.execute(
        """
        SELECT unit_type, status, COUNT(*) AS count
        FROM ingest_work_items
        WHERE run_id = ?
        GROUP BY unit_type, status
        ORDER BY unit_type ASC, status ASC
        """,
        (run_id,),
    ).fetchall()
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        unit_type = str(row["unit_type"] or "unknown")
        counts.setdefault(unit_type, {status: 0 for status in INGEST_V2_WORK_ITEM_STATUSES})
        counts[unit_type][str(row["status"])] = int(row["count"] or 0)
    return counts


def ingest_v2_artifact_counts(connection: sqlite3.Connection, *, run_id: str) -> dict[str, object]:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count, MIN(updated_at) AS oldest_updated_at
        FROM ingest_artifact_sweeps
        WHERE run_id = ?
          AND state = 'orphan_pending_sweep'
        """,
        (run_id,),
    ).fetchone()
    orphan_count = int(row["count"] or 0) if row is not None else 0
    oldest_age: int | None = None
    if row is not None and row["oldest_updated_at"]:
        parsed = parse_utc_timestamp(row["oldest_updated_at"])
        if parsed is not None:
            oldest_age = max(0, int((datetime.now(timezone.utc) - parsed).total_seconds()))
    return {
        "orphan_pending_sweep": orphan_count,
        "oldest_unswept_age_seconds": oldest_age,
    }


def ingest_v2_lease_health(connection: sqlite3.Connection, row: sqlite3.Row, *, run_id: str) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    now_text = format_utc_timestamp(now)
    stale_cutoff = ingest_v2_stale_lease_cutoff(now)
    active_prepare_workers = int(
        connection.execute(
            """
            SELECT COUNT(DISTINCT lease_owner)
            FROM ingest_work_items
            WHERE run_id = ?
              AND status = 'leased'
              AND lease_owner IS NOT NULL
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at > ?
              AND updated_at > ?
            """,
            (run_id, now_text, stale_cutoff),
        ).fetchone()[0]
        or 0
    )
    active_lease_row = connection.execute(
        """
        SELECT MIN(updated_at) AS oldest_updated_at
        FROM ingest_work_items
        WHERE run_id = ?
          AND status IN ('leased', 'committing')
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at > ?
          AND updated_at > ?
        """,
        (run_id, now_text, stale_cutoff),
    ).fetchone()
    stale_lease_row = connection.execute(
        """
        SELECT MIN(
          CASE
            WHEN lease_expires_at <= ? THEN lease_expires_at
            ELSE updated_at
          END
        ) AS oldest_expired_at
        FROM ingest_work_items
        WHERE run_id = ?
          AND status IN ('leased', 'committing')
          AND lease_expires_at IS NOT NULL
          AND (
            lease_expires_at <= ?
            OR updated_at <= ?
          )
        """,
        (now_text, run_id, now_text, stale_cutoff),
    ).fetchone()

    def age_seconds(raw_value: object) -> int | None:
        parsed = parse_utc_timestamp(raw_value)
        if parsed is None:
            return None
        return max(0, int((now - parsed).total_seconds()))

    return {
        "oldest_active_lease_age_seconds": age_seconds(active_lease_row["oldest_updated_at"] if active_lease_row else None),
        "oldest_stale_lease_age_seconds": age_seconds(stale_lease_row["oldest_expired_at"] if stale_lease_row else None),
        "writer_busy": ingest_v2_lease_is_active(
            row["committer_lease_expires_at"],
            row["committer_heartbeat_at"],
            now=now,
        ),
        "active_prepare_workers": active_prepare_workers,
        "prepare_worker_soft_limit": int(row["prepare_worker_soft_limit"] or DEFAULT_WORKER_BACKGROUND_MAX_PARALLEL),
    }


def ingest_v2_next_commands(
    root: Path,
    row: sqlite3.Row,
    *,
    counts: dict[str, int],
    leases: dict[str, object],
    artifacts: dict[str, object],
    budget_seconds: int,
) -> list[str]:
    if row["cancel_requested_at"] is not None or str(row["status"]) in INGEST_V2_TERMINAL_STATUSES:
        return [f"ingest-status {shlex.quote(str(root))} --run-id {shlex.quote(str(row['run_id']))}"]
    run_id_arg = shlex.quote(str(row["run_id"]))
    root_arg = shlex.quote(str(root))
    budget_arg = str(int(budget_seconds))
    runnable_commands: list[str] = []
    phase = str(row["phase"] or "")
    if phase == "planning":
        runnable_commands.append(f"ingest-plan-step {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}")
    if (
        phase == "preparing"
        and (counts.get("pending", 0) > 0 or counts.get("leased", 0) > 0)
        and int(leases.get("active_prepare_workers") or 0) < int(leases.get("prepare_worker_soft_limit") or 0)
    ):
        runnable_commands.append(f"ingest-prepare-step {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}")
    if phase == "committing" and counts.get("prepared", 0) > 0 and not bool(leases.get("writer_busy")):
        runnable_commands.append(f"ingest-commit-step {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}")
    if phase == "finalizing" or int(artifacts.get("orphan_pending_sweep") or 0) > 0:
        runnable_commands.append(f"ingest-finalize-step {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}")
    commands: list[str] = []
    if runnable_commands:
        commands.append(f"ingest-run-step {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}")
        commands.extend(runnable_commands)
    if leases.get("oldest_stale_lease_age_seconds") is not None:
        commands.append(f"ingest-status {root_arg} --run-id {run_id_arg}")
    return commands


def ingest_v2_status_payload(
    connection: sqlite3.Connection,
    root: Path,
    row: sqlite3.Row,
    *,
    budget_seconds: int = DEFAULT_RESUMABLE_STEP_BUDGET_SECONDS,
) -> dict[str, object]:
    run_id = str(row["run_id"])
    scope = decode_json_text(row["scope_json"], default={}) or {}
    counts = ingest_v2_status_counts(connection, run_id=run_id)
    unit_type_counts = ingest_v2_unit_type_counts(connection, run_id=run_id)
    leases = ingest_v2_lease_health(connection, row, run_id=run_id)
    artifacts = ingest_v2_artifact_counts(connection, run_id=run_id)
    prepared_order_row = connection.execute(
        """
        SELECT MIN(commit_order) AS next_commit_order
        FROM ingest_work_items
        WHERE run_id = ?
          AND status = 'prepared'
        """,
        (run_id,),
    ).fetchone()
    next_commands = ingest_v2_next_commands(
        root,
        row,
        counts=counts,
        leases=leases,
        artifacts=artifacts,
        budget_seconds=budget_seconds,
    )
    return {
        "run_id": run_id,
        "status": row["status"],
        "phase": row["phase"],
        "scope": list((scope if isinstance(scope, dict) else {}).get("scan_paths") or []),
        "scope_details": scope,
        "budget_recommendation_seconds": int(budget_seconds),
        "counts": {
            "work_items": counts,
            "by_unit_type": unit_type_counts,
        },
        "leases": leases,
        "entity": {
            "graph_stale": bool(row["entity_graph_stale"]),
            "sync_pending": 0,
            "sync_completed": 0,
            "entity_writer_busy": False,
            "policy_changed_during_run": False,
        },
        "artifacts": artifacts,
        "progress": {
            "planning_complete": str(row["phase"]) != "planning",
            "commit_order_next": (
                int(prepared_order_row["next_commit_order"])
                if prepared_order_row is not None and prepared_order_row["next_commit_order"] is not None
                else None
            ),
            "finalize_phase": str(row["phase"]) if str(row["phase"]) == "finalizing" else None,
        },
        "stalled": leases.get("oldest_stale_lease_age_seconds") is not None,
        "last_error": row["error"],
        "next_recommended_commands": next_commands,
    }


def ingest_v2_status_payload_timed(
    connection: sqlite3.Connection,
    root: Path,
    row: sqlite3.Row,
    *,
    budget_seconds: int = DEFAULT_RESUMABLE_STEP_BUDGET_SECONDS,
) -> tuple[dict[str, object], float]:
    started = time.perf_counter()
    payload = ingest_v2_status_payload(connection, root, row, budget_seconds=budget_seconds)
    return payload, ingest_v2_elapsed_ms(started)


def ingest_v2_refresh_conversation_previews_best_effort(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
) -> tuple[int, str | None]:
    connection.execute("SAVEPOINT ingest_v2_conversation_preview_refresh")
    try:
        previews_refreshed = refresh_conversation_previews(connection, paths)
        connection.execute("RELEASE SAVEPOINT ingest_v2_conversation_preview_refresh")
        return previews_refreshed, None
    except PermissionError as exc:
        connection.execute("ROLLBACK TO SAVEPOINT ingest_v2_conversation_preview_refresh")
        connection.execute("RELEASE SAVEPOINT ingest_v2_conversation_preview_refresh")
        return 0, f"{type(exc).__name__}: {exc}"


def ingest_v2_conversation_preview_work_summary(
    connection: sqlite3.Connection,
    *,
    run_id: str,
) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT status, artifact_manifest_json
        FROM ingest_work_items
        WHERE run_id = ?
          AND unit_type = 'conversation_preview'
        """,
        (run_id,),
    ).fetchall()
    summary = {
        "total": 0,
        "committed": 0,
        "failed": 0,
        "refreshed": 0,
    }
    for row in rows:
        summary["total"] += 1
        status = str(row["status"] or "")
        if status == "committed":
            summary["committed"] += 1
            manifest = decode_json_text(row["artifact_manifest_json"], default={}) or {}
            if isinstance(manifest, dict):
                summary["refreshed"] += int(manifest.get("refreshed") or 0)
        elif status == "failed":
            summary["failed"] += 1
    return summary


def ingest_v2_start(
    root: Path,
    *,
    recursive: bool,
    raw_file_types: str | None,
    raw_paths: list[str] | None = None,
    budget_seconds: int | None = None,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    paths = workspace_paths(root)
    ensure_layout(paths)
    scope = ingest_v2_scope_payload(root, recursive=recursive, raw_file_types=raw_file_types, raw_paths=raw_paths)
    with workspace_ingest_session(paths, command_name="ingest-start"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            active_row = active_ingest_v2_run_row(connection)
            if active_row is not None:
                raise RetrieverStructuredError(
                    f"Resumable ingest run {active_row['run_id']} is already active.",
                    ingest_v2_conflict_payload(
                        root,
                        active_row,
                        message="A resumable ingest run is active in this workspace.",
                    ),
                )
            run_id = new_ingest_v2_run_id()
            now = utc_now()
            connection.execute("BEGIN")
            try:
                connection.execute(
                    """
                    INSERT INTO ingest_runs (
                      run_id, scope_json, recursive, raw_file_types, pipeline_schema_version,
                      phase, status, prepare_worker_soft_limit, entity_policy_snapshot_json,
                      created_at, started_at, last_heartbeat_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        compact_json_text(scope),
                        1 if recursive else 0,
                        raw_file_types,
                        INGEST_V2_PIPELINE_SCHEMA_VERSION,
                        "planning",
                        "planning",
                        DEFAULT_WORKER_BACKGROUND_MAX_PARALLEL,
                        "{}",
                        now,
                        now,
                        now,
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            row = require_ingest_v2_run_row(connection, run_id)
            return {
                "ok": True,
                "created": True,
                **ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }
        finally:
            connection.close()


def ingest_v2_status(root: Path, *, run_id: str | None = None, budget_seconds: int | None = None) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        row = require_ingest_v2_run_row(connection, run_id) if run_id else latest_ingest_v2_run_row(connection)
        if row is None:
            return {
                "ok": True,
                "status": "none",
                "phase": None,
                "run_id": None,
                "counts": {"work_items": {status: 0 for status in INGEST_V2_WORK_ITEM_STATUSES}, "by_unit_type": {}},
                "leases": {
                    "oldest_active_lease_age_seconds": None,
                    "oldest_stale_lease_age_seconds": None,
                    "writer_busy": False,
                    "active_prepare_workers": 0,
                    "prepare_worker_soft_limit": DEFAULT_WORKER_BACKGROUND_MAX_PARALLEL,
                },
                "entity": {
                    "graph_stale": False,
                    "sync_pending": 0,
                    "sync_completed": 0,
                    "entity_writer_busy": False,
                    "policy_changed_during_run": False,
                },
                "artifacts": {
                    "orphan_pending_sweep": 0,
                    "oldest_unswept_age_seconds": None,
                },
                "progress": {
                    "planning_complete": False,
                    "commit_order_next": None,
                    "finalize_phase": None,
                },
                "stalled": False,
                "last_error": None,
                "next_recommended_commands": [],
            }
        return {"ok": True, **ingest_v2_status_payload(connection, root, row, budget_seconds=budget)}
    finally:
        connection.close()


def ingest_v2_cancel(root: Path, *, run_id: str, force: bool = False) -> dict[str, object]:
    set_active_workspace_root(root)
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_ingest_session(paths, command_name="ingest-cancel"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            row = require_ingest_v2_run_row(connection, run_id)
            now = utc_now()
            connection.execute("BEGIN")
            try:
                connection.execute(
                    """
                    UPDATE ingest_runs
                    SET cancel_requested_at = COALESCE(cancel_requested_at, ?),
                        status = CASE
                          WHEN status IN ('completed', 'failed') THEN status
                          ELSE 'canceled'
                        END,
                        phase = CASE
                          WHEN status IN ('completed', 'failed') THEN phase
                          ELSE 'canceled'
                        END,
                        completed_at = COALESCE(completed_at, ?),
                        last_heartbeat_at = ?
                    WHERE run_id = ?
                    """,
                    (now, now, now, run_id),
                )
                cursor = connection.execute(
                    """
                    UPDATE ingest_work_items
                    SET status = 'cancelled',
                        updated_at = ?,
                        last_error = COALESCE(last_error, 'Ingest run canceled.')
                    WHERE run_id = ?
                      AND status IN ('pending', 'leased', 'prepared')
                    """,
                    (now, run_id),
                )
                cancelled_items = int(cursor.rowcount or 0)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            updated_row = require_ingest_v2_run_row(connection, run_id)
            return {
                "ok": True,
                "cancel_requested": True,
                "force": bool(force),
                "cancelled_work_items": cancelled_items,
                **ingest_v2_status_payload(connection, root, updated_row),
            }
        finally:
            connection.close()


def ingest_v2_plan_step(
    root: Path,
    *,
    run_id: str,
    budget_seconds: int | None = None,
    schema_applied: bool = False,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    deadline = time.perf_counter() + max(0.1, float(budget) - 0.25)
    paths = workspace_paths(root)
    ensure_layout(paths)
    processed_paths = 0
    planned_loose_files = 0
    planned_production_roots = 0
    planned_production_rows = 0
    unsaved_plan_steps = 0
    cursor_save_ms_values: list[float] = []
    work_item_insert_ms_values: list[float] = []
    with workspace_ingest_session(paths, command_name="ingest-plan-step"):
        connection = connect_db(paths["db_path"])
        try:
            if not schema_applied:
                apply_schema(connection, root)
            row = require_ingest_v2_run_row(connection, run_id)
            if str(row["status"]) in INGEST_V2_TERMINAL_STATUSES or row["cancel_requested_at"] is not None:
                return {
                    "ok": True,
                    "implemented": True,
                    "step": "plan",
                    "processed_paths": 0,
                    "planned_loose_files": 0,
                    "planned_production_roots": 0,
                    "planned_production_rows": 0,
                    "more_planning_remaining": False,
                    "more_work_remaining": False,
                    "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
                }
            if str(row["phase"]) != "planning":
                return {
                    "ok": True,
                    "implemented": True,
                    "step": "plan",
                    "processed_paths": 0,
                    "planned_loose_files": 0,
                    "planned_production_roots": 0,
                    "planned_production_rows": 0,
                    "more_planning_remaining": False,
                    "more_work_remaining": str(row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
                    "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
                }

            cursor = ingest_v2_load_or_create_loose_file_plan_cursor(connection, root, row)
            recursive = bool(row["recursive"])
            allowed_types = parse_file_types(row["raw_file_types"])
            scan_scope = ingest_v2_scan_scope_from_run(root, row)

            while time.perf_counter() < deadline:
                current_mbox_source = cursor.get("current_mbox_source")
                if isinstance(current_mbox_source, dict):
                    if unsaved_plan_steps:
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                        unsaved_plan_steps = 0
                    try:
                        updated_mbox_source, next_commit_order, planned_messages, source_complete = (
                            ingest_v2_plan_current_mbox_source(
                                connection,
                                root,
                                run_id=run_id,
                                current_mbox_source=current_mbox_source,
                                deadline=deadline,
                            )
                        )
                    except Exception as exc:
                        rollback_open_transaction(connection)
                        failures = list(cursor.get("mbox_failures") or [])
                        failures.append(
                            {
                                "source_rel_path": str(current_mbox_source.get("source_rel_path") or ""),
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                        cursor["mbox_failures"] = failures
                        cursor["current_mbox_source"] = None
                        cursor["next_commit_order"] = int(current_mbox_source.get("next_commit_order") or cursor.get("next_commit_order") or 1)
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                        continue
                    cursor["next_commit_order"] = int(next_commit_order)
                    cursor["planned_mbox_messages"] = int(cursor.get("planned_mbox_messages") or 0) + int(planned_messages)
                    cursor["current_mbox_source"] = updated_mbox_source
                    if source_complete:
                        source_rel_path = str(current_mbox_source.get("source_rel_path") or "")
                        planned_sources = list(cursor.get("planned_mbox_sources") or [])
                        if source_rel_path and source_rel_path not in planned_sources:
                            planned_sources.append(source_rel_path)
                            cursor["planned_mbox_sources"] = planned_sources
                    if source_complete or ingest_v2_deadline_remaining_seconds(deadline) < 1.0:
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                    elif isinstance(updated_mbox_source, dict):
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                    if not source_complete:
                        break
                    continue

                current_pst_source = cursor.get("current_pst_source")
                if isinstance(current_pst_source, dict):
                    if unsaved_plan_steps:
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                        unsaved_plan_steps = 0
                    try:
                        updated_pst_source, next_commit_order, planned_messages, source_complete = (
                            ingest_v2_plan_current_pst_source(
                                connection,
                                root,
                                run_id=run_id,
                                current_pst_source=current_pst_source,
                                deadline=deadline,
                            )
                        )
                    except Exception as exc:
                        rollback_open_transaction(connection)
                        failures = list(cursor.get("pst_failures") or [])
                        failures.append(
                            {
                                "source_rel_path": str(current_pst_source.get("source_rel_path") or ""),
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                        cursor["pst_failures"] = failures
                        cursor["current_pst_source"] = None
                        cursor["next_commit_order"] = int(current_pst_source.get("next_commit_order") or cursor.get("next_commit_order") or 1)
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                        continue
                    cursor["next_commit_order"] = int(next_commit_order)
                    cursor["planned_pst_messages"] = int(cursor.get("planned_pst_messages") or 0) + int(planned_messages)
                    cursor["current_pst_source"] = updated_pst_source
                    if source_complete:
                        source_rel_path = str(current_pst_source.get("source_rel_path") or "")
                        planned_sources = list(cursor.get("planned_pst_sources") or [])
                        if source_rel_path and source_rel_path not in planned_sources:
                            planned_sources.append(source_rel_path)
                            cursor["planned_pst_sources"] = planned_sources
                    if source_complete or ingest_v2_deadline_remaining_seconds(deadline) < 1.0:
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                    elif isinstance(updated_pst_source, dict):
                        cursor_save_ms_values.append(
                            ingest_v2_save_planning_cursor_heartbeat(
                                connection,
                                run_id=run_id,
                                cursor=cursor,
                                status="pending",
                            )
                        )
                    if not source_complete:
                        break
                    continue

                pending_slack_export_roots = list(cursor.get("pending_slack_export_roots") or [])
                if pending_slack_export_roots:
                    slack_rel_root = str(pending_slack_export_roots.pop(0))
                    cursor["pending_slack_export_roots"] = pending_slack_export_roots
                    slack_payload = dict(
                        dict(cursor.get("slack_export_roots_by_rel_root") or {}).get(slack_rel_root) or {}
                    )
                    if slack_payload:
                        try:
                            insert_started = time.perf_counter()
                            slack_plan = ingest_v2_plan_slack_export_root(
                                connection,
                                root,
                                run_id=run_id,
                                payload=slack_payload,
                                next_commit_order=int(cursor.get("next_commit_order") or 1),
                            )
                            work_item_insert_ms_values.append(ingest_v2_elapsed_ms(insert_started))
                        except Exception as exc:
                            rollback_open_transaction(connection)
                            failures = list(cursor.get("slack_failures") or [])
                            failures.append(
                                {
                                    "slack_rel_root": slack_rel_root,
                                    "error": f"{type(exc).__name__}: {exc}",
                                }
                            )
                            cursor["slack_failures"] = failures
                            slack_plan = None
                        if slack_plan is not None:
                            cursor["next_commit_order"] = int(slack_plan["next_commit_order"])
                            cursor["planned_slack_conversations"] = (
                                int(cursor.get("planned_slack_conversations") or 0)
                                + int(slack_plan["planned_conversations"] or 0)
                            )
                            cursor["planned_slack_day_documents"] = (
                                int(cursor.get("planned_slack_day_documents") or 0)
                                + int(slack_plan["planned_day_documents"] or 0)
                            )
                            slack_root_payloads = dict(cursor.get("slack_export_roots_by_rel_root") or {})
                            slack_root_payloads[slack_rel_root] = {
                                **slack_payload,
                                "seen_rel_paths": list(slack_plan["seen_rel_paths"]),
                                "planned_conversations": int(slack_plan["planned_conversations"] or 0),
                                "planned_day_documents": int(slack_plan["planned_day_documents"] or 0),
                            }
                            cursor["slack_export_roots_by_rel_root"] = slack_root_payloads
                            planned_roots = list(cursor.get("planned_slack_export_roots") or [])
                            if slack_rel_root not in planned_roots:
                                planned_roots.append(slack_rel_root)
                                cursor["planned_slack_export_roots"] = planned_roots
                    cursor_save_ms_values.append(
                        ingest_v2_save_planning_cursor_heartbeat(
                            connection,
                            run_id=run_id,
                            cursor=cursor,
                            status="pending",
                        )
                    )
                    continue

                pending_gmail_mbox_sources = list(cursor.get("pending_gmail_mbox_sources") or [])
                if pending_gmail_mbox_sources:
                    gmail_source_payload = dict(pending_gmail_mbox_sources.pop(0))
                    cursor["pending_gmail_mbox_sources"] = pending_gmail_mbox_sources
                    source_rel_path = str(gmail_source_payload.get("source_rel_path") or "")
                    if not source_rel_path:
                        continue
                    scanned_mbox_paths = list(cursor.get("scanned_mbox_source_rel_paths") or [])
                    if source_rel_path not in scanned_mbox_paths:
                        scanned_mbox_paths.append(source_rel_path)
                        cursor["scanned_mbox_source_rel_paths"] = scanned_mbox_paths
                    insert_started = time.perf_counter()
                    source_scan_hash = ingest_v2_gmail_mbox_source_scan_hash(
                        ingest_v2_cursor_path(root, source_rel_path),
                        gmail_source_payload,
                    )
                    current_mbox_source, next_commit_order, skipped_source = ingest_v2_begin_mbox_source_plan(
                        connection,
                        root,
                        run_id=run_id,
                        rel_path=source_rel_path,
                        commit_order=int(cursor.get("next_commit_order") or 1),
                        source_plan_kind="gmail",
                        source_scan_hash=source_scan_hash,
                        gmail_source_payload=gmail_source_payload,
                    )
                    work_item_insert_ms_values.append(ingest_v2_elapsed_ms(insert_started))
                    cursor["next_commit_order"] = int(next_commit_order)
                    cursor["planned_gmail_mbox_sources"] = int(cursor.get("planned_gmail_mbox_sources") or 0) + 1
                    if skipped_source:
                        cursor["skipped_mbox_sources"] = int(cursor.get("skipped_mbox_sources") or 0) + 1
                        planned_sources = list(cursor.get("planned_mbox_sources") or [])
                        if source_rel_path not in planned_sources:
                            planned_sources.append(source_rel_path)
                            cursor["planned_mbox_sources"] = planned_sources
                    else:
                        cursor["current_mbox_source"] = current_mbox_source
                    cursor_save_ms_values.append(
                        ingest_v2_save_planning_cursor_heartbeat(
                            connection,
                            run_id=run_id,
                            cursor=cursor,
                            status="pending",
                        )
                    )
                    continue

                pending_production_rel_roots = list(cursor.get("pending_production_rel_roots") or [])
                if pending_production_rel_roots:
                    production_rel_root = str(pending_production_rel_roots.pop(0))
                    cursor["pending_production_rel_roots"] = pending_production_rel_roots
                    signature_payload = dict(
                        dict(cursor.get("production_roots_by_rel_root") or {}).get(production_rel_root) or {}
                    )
                    if signature_payload:
                        try:
                            insert_started = time.perf_counter()
                            production_plan = ingest_v2_plan_production_root(
                                connection,
                                root,
                                run_id=run_id,
                                signature_payload=signature_payload,
                                next_commit_order=int(cursor.get("next_commit_order") or 1),
                            )
                            work_item_insert_ms_values.append(ingest_v2_elapsed_ms(insert_started))
                        except Exception as exc:
                            rollback_open_transaction(connection)
                            failures = list(cursor.get("production_failures") or [])
                            failures.append(
                                {
                                    "production_rel_root": production_rel_root,
                                    "error": f"{type(exc).__name__}: {exc}",
                                }
                            )
                            cursor["production_failures"] = failures
                            production_plan = None
                        if production_plan is None:
                            cursor_save_ms_values.append(
                                ingest_v2_save_planning_cursor_heartbeat(
                                    connection,
                                    run_id=run_id,
                                    cursor=cursor,
                                    status="pending",
                                )
                            )
                            continue
                        cursor["next_commit_order"] = int(production_plan["next_commit_order"])
                        cursor["planned_production_rows"] = (
                            int(cursor.get("planned_production_rows") or 0)
                            + int(production_plan["planned_rows"] or 0)
                        )
                        cursor["planned_production_preview_batches"] = (
                            int(cursor.get("planned_production_preview_batches") or 0)
                            + int(production_plan.get("planned_preview_batches") or 0)
                        )
                        cursor["production_docs_missing_linked_text"] = (
                            int(cursor.get("production_docs_missing_linked_text") or 0)
                            + int(production_plan["docs_missing_linked_text"] or 0)
                        )
                        cursor["production_docs_missing_linked_images"] = (
                            int(cursor.get("production_docs_missing_linked_images") or 0)
                            + int(production_plan["docs_missing_linked_images"] or 0)
                        )
                        cursor["production_docs_missing_linked_natives"] = (
                            int(cursor.get("production_docs_missing_linked_natives") or 0)
                            + int(production_plan["docs_missing_linked_natives"] or 0)
                        )
                        production_root_payloads = dict(cursor.get("production_roots_by_rel_root") or {})
                        production_root_payloads[production_rel_root] = {
                            **signature_payload,
                            "seen_control_numbers": list(production_plan["seen_control_numbers"]),
                            "planned_rows": int(production_plan["planned_rows"] or 0),
                            "planned_preview_batches": int(production_plan.get("planned_preview_batches") or 0),
                            "docs_missing_linked_text": int(production_plan["docs_missing_linked_text"] or 0),
                            "docs_missing_linked_images": int(production_plan["docs_missing_linked_images"] or 0),
                            "docs_missing_linked_natives": int(production_plan["docs_missing_linked_natives"] or 0),
                        }
                        cursor["production_roots_by_rel_root"] = production_root_payloads
                        planned_roots = list(cursor.get("planned_production_roots") or [])
                        if production_rel_root not in planned_roots:
                            planned_roots.append(production_rel_root)
                            cursor["planned_production_roots"] = planned_roots
                            planned_production_roots += 1
                        planned_production_rows += int(production_plan["planned_rows"] or 0)
                    cursor_save_ms_values.append(
                        ingest_v2_save_planning_cursor_heartbeat(
                            connection,
                            run_id=run_id,
                            cursor=cursor,
                            status="pending",
                        )
                    )
                    continue

                pending_paths = list(cursor.get("pending_paths") or [])
                if not pending_paths:
                    break
                rel_path = str(pending_paths.pop(0))
                cursor["pending_paths"] = pending_paths
                cursor["scanned_paths"] = int(cursor.get("scanned_paths") or 0) + 1
                processed_paths += 1

                candidate_path = ingest_v2_cursor_path(root, rel_path)
                if rel_path and ingest_v2_rel_path_excluded(cursor, rel_path):
                    cursor["skipped_excluded_paths"] = int(cursor.get("skipped_excluded_paths") or 0) + 1
                elif not candidate_path.exists():
                    cursor["skipped_missing_paths"] = int(cursor.get("skipped_missing_paths") or 0) + 1
                elif not path_is_at_or_under(candidate_path, root):
                    cursor["skipped_excluded_paths"] = int(cursor.get("skipped_excluded_paths") or 0) + 1
                elif candidate_path.is_dir():
                    child_paths = ingest_v2_planning_child_paths(root, candidate_path, recursive=recursive)
                    cursor["pending_paths"] = ingest_v2_sorted_pending_paths(
                        [*list(cursor.get("pending_paths") or []), *child_paths]
                    )
                    cursor["listed_directories"] = int(cursor.get("listed_directories") or 0) + 1
                elif candidate_path.is_file():
                    if ".retriever" in candidate_path.resolve().relative_to(root.resolve()).parts:
                        cursor["skipped_excluded_paths"] = int(cursor.get("skipped_excluded_paths") or 0) + 1
                    elif not ingest_scan_scope_contains_rel_path(scan_scope, rel_path):
                        cursor["skipped_excluded_paths"] = int(cursor.get("skipped_excluded_paths") or 0) + 1
                    else:
                        file_type = normalize_extension(candidate_path)
                        if not file_type:
                            cursor["skipped_extensionless_files"] = int(cursor.get("skipped_extensionless_files") or 0) + 1
                        elif allowed_types is not None and file_type not in allowed_types:
                            cursor["skipped_filtered_files"] = int(cursor.get("skipped_filtered_files") or 0) + 1
                        elif file_type == PST_SOURCE_KIND:
                            scanned_pst_paths = list(cursor.get("scanned_pst_source_rel_paths") or [])
                            if rel_path not in scanned_pst_paths:
                                scanned_pst_paths.append(rel_path)
                                cursor["scanned_pst_source_rel_paths"] = scanned_pst_paths
                            pst_source_payload = dict(
                                dict(cursor.get("pst_source_payloads_by_rel_path") or {}).get(rel_path) or {}
                            )
                            insert_started = time.perf_counter()
                            current_pst_source, next_commit_order, skipped_source = ingest_v2_begin_pst_source_plan(
                                connection,
                                root,
                                run_id=run_id,
                                rel_path=rel_path,
                                commit_order=int(cursor.get("next_commit_order") or 1),
                                source_payload=pst_source_payload,
                            )
                            work_item_insert_ms_values.append(ingest_v2_elapsed_ms(insert_started))
                            cursor["next_commit_order"] = int(next_commit_order)
                            if skipped_source:
                                cursor["skipped_pst_sources"] = int(cursor.get("skipped_pst_sources") or 0) + 1
                                planned_sources = list(cursor.get("planned_pst_sources") or [])
                                if rel_path not in planned_sources:
                                    planned_sources.append(rel_path)
                                    cursor["planned_pst_sources"] = planned_sources
                            else:
                                cursor["current_pst_source"] = current_pst_source
                        elif file_type == MBOX_SOURCE_KIND:
                            scanned_mbox_paths = list(cursor.get("scanned_mbox_source_rel_paths") or [])
                            if rel_path not in scanned_mbox_paths:
                                scanned_mbox_paths.append(rel_path)
                                cursor["scanned_mbox_source_rel_paths"] = scanned_mbox_paths
                            insert_started = time.perf_counter()
                            current_mbox_source, next_commit_order, skipped_source = ingest_v2_begin_mbox_source_plan(
                                connection,
                                root,
                                run_id=run_id,
                                rel_path=rel_path,
                                commit_order=int(cursor.get("next_commit_order") or 1),
                            )
                            work_item_insert_ms_values.append(ingest_v2_elapsed_ms(insert_started))
                            cursor["next_commit_order"] = int(next_commit_order)
                            if skipped_source:
                                cursor["skipped_mbox_sources"] = int(cursor.get("skipped_mbox_sources") or 0) + 1
                                planned_sources = list(cursor.get("planned_mbox_sources") or [])
                                if rel_path not in planned_sources:
                                    planned_sources.append(rel_path)
                                    cursor["planned_mbox_sources"] = planned_sources
                            else:
                                cursor["current_mbox_source"] = current_mbox_source
                        elif file_type in CONTAINER_SOURCE_FILE_TYPES:
                            cursor["skipped_container_files"] = int(cursor.get("skipped_container_files") or 0) + 1
                        else:
                            file_size, file_mtime_ns = source_file_snapshot(candidate_path)
                            insert_started = time.perf_counter()
                            inserted = ingest_v2_plan_loose_file_item(
                                connection,
                                run_id=run_id,
                                rel_path=rel_path,
                                file_type=file_type,
                                file_size=file_size,
                                file_mtime_ns=file_mtime_ns,
                                commit_order=int(cursor.get("next_commit_order") or 1),
                            )
                            work_item_insert_ms_values.append(ingest_v2_elapsed_ms(insert_started))
                            cursor["next_commit_order"] = int(cursor.get("next_commit_order") or 1) + 1
                            if inserted:
                                planned_loose_files += 1
                                cursor["planned_loose_files"] = int(cursor.get("planned_loose_files") or 0) + 1

                unsaved_plan_steps += 1
                if (
                    unsaved_plan_steps >= INGEST_V2_PLAN_CURSOR_SAVE_INTERVAL
                    or ingest_v2_deadline_remaining_seconds(deadline) < 1.0
                ):
                    cursor_save_ms_values.append(
                        ingest_v2_save_planning_cursor_heartbeat(
                            connection,
                            run_id=run_id,
                            cursor=cursor,
                            status="pending",
                        )
                    )
                    unsaved_plan_steps = 0

            planning_complete = (
                not list(cursor.get("pending_production_rel_roots") or [])
                and not list(cursor.get("pending_slack_export_roots") or [])
                and not list(cursor.get("pending_gmail_mbox_sources") or [])
                and not list(cursor.get("pending_paths") or [])
                and not isinstance(cursor.get("current_mbox_source"), dict)
                and not isinstance(cursor.get("current_pst_source"), dict)
            )
            if not planning_complete and unsaved_plan_steps:
                cursor_save_ms_values.append(
                    ingest_v2_save_planning_cursor_heartbeat(
                        connection,
                        run_id=run_id,
                        cursor=cursor,
                        status="pending",
                    )
                )
                unsaved_plan_steps = 0
            if planning_complete:
                total_work_items = int(
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM ingest_work_items
                        WHERE run_id = ?
                        """,
                        (run_id,),
                    ).fetchone()[0]
                    or 0
                )
                next_phase = "preparing" if total_work_items else "finalizing"
                save_started = time.perf_counter()
                if not connection.in_transaction:
                    connection.execute("BEGIN")
                try:
                    ingest_v2_save_phase_cursor(
                        connection,
                        run_id=run_id,
                        phase="planning",
                        cursor_key="loose_file_scan",
                        cursor=cursor,
                        status="complete",
                    )
                    connection.execute(
                        """
                        UPDATE ingest_runs
                        SET phase = ?,
                            status = ?,
                            last_heartbeat_at = ?
                        WHERE run_id = ?
                          AND cancel_requested_at IS NULL
                        """,
                        (next_phase, next_phase, utc_now(), run_id),
                    )
                    connection.commit()
                    cursor_save_ms_values.append(ingest_v2_elapsed_ms(save_started))
                    unsaved_plan_steps = 0
                except Exception:
                    connection.rollback()
                    raise

            updated_row = require_ingest_v2_run_row(connection, run_id)
            run_payload, status_payload_ms = ingest_v2_status_payload_timed(
                connection,
                root,
                updated_row,
                budget_seconds=budget,
            )
            return {
                "ok": True,
                "implemented": True,
                "step": "plan",
                "processed_paths": processed_paths,
                "planned_loose_files": planned_loose_files,
                "planned_production_roots": planned_production_roots,
                "planned_production_rows": planned_production_rows,
                "cursor": {
                    "pending_paths": len(list(cursor.get("pending_paths") or [])),
                    "pending_production_roots": len(list(cursor.get("pending_production_rel_roots") or [])),
                    "pending_slack_export_roots": len(list(cursor.get("pending_slack_export_roots") or [])),
                    "pending_gmail_mbox_sources": len(list(cursor.get("pending_gmail_mbox_sources") or [])),
                    "scanned_paths": int(cursor.get("scanned_paths") or 0),
                    "planned_loose_files": int(cursor.get("planned_loose_files") or 0),
                    "planned_production_roots": len(list(cursor.get("planned_production_roots") or [])),
                    "planned_production_rows": int(cursor.get("planned_production_rows") or 0),
                    "planned_production_preview_batches": int(cursor.get("planned_production_preview_batches") or 0),
                    "planned_slack_export_roots": list(cursor.get("planned_slack_export_roots") or []),
                    "planned_slack_conversations": int(cursor.get("planned_slack_conversations") or 0),
                    "planned_slack_day_documents": int(cursor.get("planned_slack_day_documents") or 0),
                    "slack_failures": list(cursor.get("slack_failures") or []),
                    "current_mbox_source": (
                        str(dict(cursor.get("current_mbox_source") or {}).get("source_rel_path") or "")
                        if isinstance(cursor.get("current_mbox_source"), dict)
                        else None
                    ),
                    "planned_mbox_sources": list(cursor.get("planned_mbox_sources") or []),
                    "planned_mbox_messages": int(cursor.get("planned_mbox_messages") or 0),
                    "planned_gmail_mbox_sources": int(cursor.get("planned_gmail_mbox_sources") or 0),
                    "skipped_mbox_sources": int(cursor.get("skipped_mbox_sources") or 0),
                    "scanned_mbox_source_rel_paths": list(cursor.get("scanned_mbox_source_rel_paths") or []),
                    "mbox_failures": list(cursor.get("mbox_failures") or []),
                    "current_pst_source": (
                        str(dict(cursor.get("current_pst_source") or {}).get("source_rel_path") or "")
                        if isinstance(cursor.get("current_pst_source"), dict)
                        else None
                    ),
                    "planned_pst_sources": list(cursor.get("planned_pst_sources") or []),
                    "planned_pst_messages": int(cursor.get("planned_pst_messages") or 0),
                    "skipped_pst_sources": int(cursor.get("skipped_pst_sources") or 0),
                    "scanned_pst_source_rel_paths": list(cursor.get("scanned_pst_source_rel_paths") or []),
                    "pst_failures": list(cursor.get("pst_failures") or []),
                    "skipped_production_roots": list(cursor.get("skipped_production_roots") or []),
                    "production_failures": list(cursor.get("production_failures") or []),
                    "production_docs_missing_linked_text": int(cursor.get("production_docs_missing_linked_text") or 0),
                    "production_docs_missing_linked_images": int(cursor.get("production_docs_missing_linked_images") or 0),
                    "production_docs_missing_linked_natives": int(cursor.get("production_docs_missing_linked_natives") or 0),
                    "skipped_container_files": int(cursor.get("skipped_container_files") or 0),
                    "skipped_filtered_files": int(cursor.get("skipped_filtered_files") or 0),
                    "skipped_excluded_paths": int(cursor.get("skipped_excluded_paths") or 0),
                    "special_source_counts": cursor.get("special_source_counts") or {},
                },
                "more_planning_remaining": not planning_complete,
                "more_work_remaining": str(updated_row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
                "timings": {
                    "work_item_insert_ms": ingest_v2_timing_summary(work_item_insert_ms_values),
                    "cursor_save_ms": ingest_v2_timing_summary(cursor_save_ms_values),
                    "status_payload_ms": round(status_payload_ms, 3),
                },
                "run": run_payload,
            }
        finally:
            connection.close()


def ingest_v2_prepare_step(
    root: Path,
    *,
    run_id: str,
    budget_seconds: int | None = None,
    schema_applied: bool = False,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    deadline = time.perf_counter() + max(0.1, float(budget) - 0.25)
    paths = workspace_paths(root)
    ensure_layout(paths)
    worker_id = ingest_v2_worker_id("prepare")
    claimed = 0
    prepared = 0
    deferred_timeout = 0
    released = 0
    stale_reclaimed = 0
    throttled = False
    claim_limit = ingest_v2_prepare_claim_limit()
    prepare_workers = 1
    prepared_entries: list[dict[str, object]] = []
    prepare_ms_values: list[float] = []
    prepare_hash_ms_values: list[float] = []
    prepare_extract_ms_values: list[float] = []
    prepare_chunk_ms_values: list[float] = []
    prepared_serialize_ms_values: list[float] = []
    prepared_write_ms_values: list[float] = []
    prepared_payload_bytes = 0
    connection = connect_db(paths["db_path"])
    try:
        if not schema_applied:
            apply_schema(connection, root)
        row = require_ingest_v2_run_row(connection, run_id)
        if str(row["status"]) in INGEST_V2_TERMINAL_STATUSES or row["cancel_requested_at"] is not None:
            return {
                "ok": True,
                "implemented": True,
                "step": "prepare",
                "worker_id": worker_id,
                "claimed": 0,
                "prepared": 0,
                "deferred_timeout": 0,
                "released": 0,
                "stale_reclaimed": 0,
                "claim_limit": claim_limit,
                "prepare_workers": 0,
                "throttled": False,
                "more_prepare_remaining": False,
                "more_work_remaining": False,
                "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }
        if str(row["phase"]) != "preparing":
            return {
                "ok": True,
                "implemented": True,
                "step": "prepare",
                "worker_id": worker_id,
                "claimed": 0,
                "prepared": 0,
                "deferred_timeout": 0,
                "released": 0,
                "stale_reclaimed": 0,
                "claim_limit": claim_limit,
                "prepare_workers": 0,
                "throttled": False,
                "more_prepare_remaining": str(row["phase"]) == "planning",
                "more_work_remaining": str(row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
                "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }

        stale_reclaimed = ingest_v2_reclaim_stale_prepare_items(connection, run_id=run_id)
        claimed_rows, throttled = ingest_v2_claim_prepare_items(
            connection,
            run_id=run_id,
            worker_id=worker_id,
            limit=claim_limit,
        )
        claimed = len(claimed_rows)
        prepare_workers = max(1, min(ingest_v2_prepare_worker_count_for_rows(claimed_rows), claimed or 1))
        prepared_results = ingest_v2_prepare_claimed_work_items_parallel(
            root,
            claimed_rows,
            deadline=deadline,
            prepare_workers=prepare_workers,
        )

        for prepared_result in prepared_results:
            work_item_id = int(prepared_result["work_item_id"])
            prepared_item = prepared_result.get("prepared_item")
            source_fingerprint = dict(prepared_result.get("source_fingerprint") or {})
            defer_message = prepared_result.get("defer_message")
            payload_kind = str(prepared_result.get("payload_kind") or "loose_file")
            if prepared_item is None:
                if defer_message == "Not enough budget remaining to start prepare.":
                    if ingest_v2_release_prepare_item(
                        connection,
                        run_id=run_id,
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        reason=defer_message,
                    ):
                        released += 1
                else:
                    if ingest_v2_mark_prepare_deferred_timeout(
                        connection,
                        run_id=run_id,
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        message=defer_message or "Prepare could not complete within the step budget.",
                    ):
                        deferred_timeout += 1
                continue
            prepared_item_dict = dict(prepared_item)

            prepare_ms_values.append(float(prepared_item_dict.get("prepare_ms") or 0.0))
            prepare_hash_ms_values.append(float(prepared_item_dict.get("prepare_hash_ms") or 0.0))
            prepare_extract_ms_values.append(float(prepared_item_dict.get("prepare_extract_ms") or 0.0))
            prepare_chunk_ms_values.append(float(prepared_item_dict.get("prepare_chunk_ms") or 0.0))
            prepared_entries.append(
                {
                    "work_item_id": work_item_id,
                    "payload_kind": payload_kind,
                    "prepared_item": prepared_item_dict,
                    "source_fingerprint": source_fingerprint,
                }
            )

        store_result = ingest_v2_store_prepared_items_batch(
            connection,
            run_id=run_id,
            worker_id=worker_id,
            entries=prepared_entries,
        )
        prepared = int(store_result.get("stored") or 0)
        prepared_serialize_ms_values.extend(list(store_result.get("serialize_ms_values") or []))
        prepared_write_ms_values.extend(list(store_result.get("prepared_write_ms_values") or []))
        prepared_payload_bytes = int(store_result.get("payload_bytes") or 0)

        advanced_to_commit = ingest_v2_maybe_advance_after_prepare(connection, run_id=run_id)
        updated_row = require_ingest_v2_run_row(connection, run_id)
        remaining_prepare_items = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM ingest_work_items
                WHERE run_id = ?
                  AND status IN ('pending', 'leased')
                """,
                (run_id,),
            ).fetchone()[0]
            or 0
        )
        run_payload, status_payload_ms = ingest_v2_status_payload_timed(
            connection,
            root,
            updated_row,
            budget_seconds=budget,
        )
        return {
            "ok": True,
            "implemented": True,
            "step": "prepare",
            "worker_id": worker_id,
            "claimed": claimed,
            "prepared": prepared,
            "deferred_timeout": deferred_timeout,
            "released": released,
            "stale_reclaimed": stale_reclaimed,
            "claim_limit": claim_limit,
            "prepare_workers": prepare_workers if claimed else 0,
            "throttled": throttled,
            "advanced_to_commit": advanced_to_commit,
            "more_prepare_remaining": remaining_prepare_items > 0,
            "more_work_remaining": str(updated_row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
            "timings": {
                "prepare_ms": ingest_v2_timing_summary(prepare_ms_values),
                "hash_ms": ingest_v2_timing_summary(prepare_hash_ms_values),
                "extract_ms": ingest_v2_timing_summary(prepare_extract_ms_values),
                "chunk_ms": ingest_v2_timing_summary(prepare_chunk_ms_values),
                "prepared_serialize_ms": ingest_v2_timing_summary(prepared_serialize_ms_values),
                "prepared_write_ms": ingest_v2_timing_summary(prepared_write_ms_values),
                "prepared_payload_bytes": prepared_payload_bytes,
                "status_payload_ms": round(status_payload_ms, 3),
            },
            "run": run_payload,
        }
    finally:
        connection.close()


def ingest_v2_commit_step(
    root: Path,
    *,
    run_id: str,
    budget_seconds: int | None = None,
    max_items: int | None = None,
    schema_applied: bool = False,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    item_limit = None if max_items is None else max(1, int(max_items))
    deadline = time.perf_counter() + max(0.1, float(budget) - 0.25)
    paths = workspace_paths(root)
    ensure_layout(paths)
    writer_id = ingest_v2_worker_id("commit")
    committed = 0
    failed = 0
    stale_reclaimed = 0
    actions: dict[str, int] = {}
    freshness_fallbacks = 0
    writer_busy = False
    connection = connect_db(paths["db_path"])
    lease_acquired = False
    try:
        if not schema_applied:
            apply_schema(connection, root)
        row = require_ingest_v2_run_row(connection, run_id)
        if str(row["status"]) in INGEST_V2_TERMINAL_STATUSES or row["cancel_requested_at"] is not None:
            return {
                "ok": True,
                "implemented": True,
                "step": "commit",
                "writer_id": writer_id,
                "writer_busy": False,
                "committed": 0,
                "failed": 0,
                "stale_reclaimed": 0,
                "actions": {},
                "freshness_fallbacks": 0,
                "advanced_to_finalize": False,
                "more_commit_remaining": False,
                "more_work_remaining": False,
                "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }
        if str(row["phase"]) != "committing":
            return {
                "ok": True,
                "implemented": True,
                "step": "commit",
                "writer_id": writer_id,
                "writer_busy": False,
                "committed": 0,
                "failed": 0,
                "stale_reclaimed": 0,
                "actions": {},
                "freshness_fallbacks": 0,
                "advanced_to_finalize": False,
                "more_commit_remaining": str(row["phase"]) in {"planning", "preparing"},
                "more_work_remaining": str(row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
                "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }

        lease_acquired = ingest_v2_acquire_writer_lease(connection, run_id=run_id, writer_id=writer_id)
        if not lease_acquired:
            writer_busy = True
            updated_row = require_ingest_v2_run_row(connection, run_id)
            return {
                "ok": True,
                "implemented": True,
                "step": "commit",
                "writer_id": writer_id,
                "writer_busy": True,
                "committed": 0,
                "failed": 0,
                "stale_reclaimed": 0,
                "actions": {},
                "freshness_fallbacks": 0,
                "advanced_to_finalize": False,
                "more_commit_remaining": True,
                "more_work_remaining": str(updated_row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
                "run": ingest_v2_status_payload(connection, root, updated_row, budget_seconds=budget),
            }

        stale_reclaimed = ingest_v2_reclaim_stale_commit_items(connection, run_id=run_id)
        run_row = require_ingest_v2_run_row(connection, run_id)
        existing_by_rel, unseen_existing_by_hash = ingest_v2_load_loose_file_commit_state(
            connection,
            root=root,
            run_row=run_row,
        )
        cursor = ingest_v2_load_commit_cursor(connection, run_id=run_id)
        filesystem_dataset_id: int | None = None
        filesystem_dataset_source_id: int | None = None
        mbox_contexts: dict[str, dict[str, object]] = {}
        pst_contexts: dict[str, dict[str, object]] = {}

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

        while (
            ingest_v2_deadline_remaining_seconds(deadline) >= INGEST_V2_COMMIT_MIN_START_SECONDS
            and (item_limit is None or committed + failed < item_limit)
        ):
            claimed_row = ingest_v2_claim_next_commit_item(
                connection,
                run_id=run_id,
                writer_id=writer_id,
            )
            if claimed_row is None:
                break
            work_item_id = int(claimed_row["id"])
            try:
                prepared_item = ingest_v2_prepared_item_from_row(claimed_row)
                payload_kind = str(claimed_row["payload_kind"] or claimed_row["unit_type"] or "")
                if payload_kind == "production_row" or str(claimed_row["unit_type"] or "") == "production_row":
                    prepared_item = ingest_v2_hydrate_prepared_production_item(prepared_item)
                    dataset_id, dataset_source_id, production_id = ingest_v2_ensure_production_context(
                        connection,
                        root,
                        prepared_item,
                    )
                    existing_row = connection.execute(
                        """
                        SELECT *
                        FROM documents
                        WHERE production_id = ?
                          AND control_number = ?
                        ORDER BY id ASC
                        LIMIT 1
                        """,
                        (production_id, str(prepared_item["control_number"])),
                    ).fetchone()
                    commit_result = commit_prepared_production_row(
                        connection,
                        paths,
                        existing_row,
                        prepared_item,
                        dataset_id=dataset_id,
                        dataset_source_id=dataset_source_id,
                        production_id=production_id,
                        before_transaction_commit=lambda commit_connection, result: ingest_v2_commit_production_work_item_hook(
                            commit_connection,
                            run_id=run_id,
                            work_item_id=work_item_id,
                            writer_id=writer_id,
                            cursor=cursor,
                            result=result,
                        ),
                    )
                elif (
                    payload_kind == "production_preview_batch"
                    or str(claimed_row["unit_type"] or "") == "production_preview_batch"
                ):
                    _dataset_id, _dataset_source_id, production_id = ingest_v2_ensure_production_context(
                        connection,
                        root,
                        prepared_item,
                    )
                    commit_result = ingest_v2_commit_production_preview_batch(
                        connection,
                        paths,
                        run_id=run_id,
                        work_item_id=work_item_id,
                        writer_id=writer_id,
                        cursor=cursor,
                        production_id=production_id,
                        prepared_item=prepared_item,
                    )
                elif payload_kind == "slack_conversation" or str(claimed_row["unit_type"] or "") == "slack_conversation":
                    prepare_error = normalize_whitespace(str(prepared_item.get("prepare_error") or "")) or None
                    if prepare_error:
                        raise RetrieverError(prepare_error)
                    conversation_identity = prepared_item.get("conversation_identity")
                    identity_root = ""
                    if isinstance(conversation_identity, list) and len(conversation_identity) > 1:
                        identity_root = str(conversation_identity[1])
                    source_locator = str(prepared_item.get("source_locator") or identity_root)
                    dataset_id, dataset_source_id = ensure_source_backed_dataset(
                        connection,
                        source_kind=SLACK_EXPORT_SOURCE_KIND,
                        source_locator=source_locator,
                        dataset_name=slack_export_dataset_name(source_locator),
                    )
                    connection.commit()
                    existing_by_rel_for_slack = existing_rows_by_rel_path(
                        connection,
                        [str(rel_path) for rel_path in list(prepared_item.get("rel_paths") or [])],
                    )
                    commit_result = commit_prepared_slack_conversation(
                        connection,
                        paths,
                        prepared_item,
                        existing_by_rel_for_slack,
                        dataset_id=int(dataset_id),
                        dataset_source_id=int(dataset_source_id),
                        current_batch=(
                            int(cursor["current_ingestion_batch"])
                            if cursor.get("current_ingestion_batch") is not None
                            else None
                        ),
                        before_transaction_commit=lambda commit_connection, result: ingest_v2_commit_slack_conversation_work_item_hook(
                            commit_connection,
                            run_id=run_id,
                            work_item_id=work_item_id,
                            writer_id=writer_id,
                            cursor=cursor,
                            result=result,
                        ),
                    )
                    if str(commit_result.get("status") or "") == "failed":
                        raise RetrieverError(str(commit_result.get("error") or "Slack conversation commit failed."))
                elif payload_kind == "conversation_preview" or str(claimed_row["unit_type"] or "") == "conversation_preview":
                    prepare_error = normalize_whitespace(str(prepared_item.get("prepare_error") or "")) or None
                    if prepare_error:
                        raise RetrieverError(prepare_error)
                    commit_result = ingest_v2_commit_conversation_preview_work_item(
                        connection,
                        paths,
                        run_id=run_id,
                        work_item_id=work_item_id,
                        writer_id=writer_id,
                        cursor=cursor,
                        prepared_item=prepared_item,
                    )
                elif payload_kind == "mbox_message" or str(claimed_row["unit_type"] or "") == "mbox_message":
                    prepare_error = normalize_whitespace(str(prepared_item.get("prepare_error") or "")) or None
                    if prepare_error:
                        raise RetrieverError(prepare_error)
                    if prepared_item.get("skip"):
                        connection.execute("BEGIN")
                        try:
                            commit_result = {
                                "action": "skipped",
                                "source_kind": MBOX_SOURCE_KIND,
                                "source_plan_kind": str(prepared_item.get("source_plan_kind") or "mbox"),
                                "source_rel_path": str(prepared_item.get("source_rel_path") or ""),
                                "source_item_id": str(prepared_item.get("source_item_id") or ""),
                                "current_ingestion_batch": None,
                                "document_id": None,
                            }
                            ingest_v2_commit_mbox_work_item_hook(
                                connection,
                                run_id=run_id,
                                work_item_id=work_item_id,
                                writer_id=writer_id,
                                cursor=cursor,
                                result=commit_result,
                            )
                            connection.commit()
                        except Exception:
                            connection.rollback()
                            raise
                    else:
                        source_rel_path = str(prepared_item["source_rel_path"])
                        context = ingest_v2_ensure_mbox_commit_context(
                            connection,
                            root,
                            prepared_item=prepared_item,
                            cursor=cursor,
                            contexts=mbox_contexts,
                        )
                        existing_entry = dict(
                            dict(context.get("existing_entries_by_source_item") or {}).get(
                                str(prepared_item["source_item_id"])
                            )
                            or {}
                        )
                        commit_result = commit_prepared_container_message(
                            connection,
                            paths,
                            prepared_item,
                            existing_entry.get("document_row"),
                            existing_entry.get("occurrence_row"),
                            current_ingestion_batch=(
                                int(context["current_ingestion_batch"])
                                if context.get("current_ingestion_batch") is not None
                                else None
                            ),
                            dataset_id=int(context["dataset_id"]),
                            dataset_source_id=(
                                int(context["dataset_source_id"])
                                if context.get("dataset_source_id") is not None
                                else None
                            ),
                            source_kind=MBOX_SOURCE_KIND,
                            source_rel_path=source_rel_path,
                            file_type_override=MBOX_SOURCE_KIND,
                            scan_started_at=str(prepared_item["scan_started_at"]),
                            before_transaction_commit=lambda commit_connection, result: ingest_v2_commit_mbox_work_item_hook(
                                commit_connection,
                                run_id=run_id,
                                work_item_id=work_item_id,
                                writer_id=writer_id,
                                cursor=cursor,
                                result={
                                    **result,
                                    "source_kind": MBOX_SOURCE_KIND,
                                    "source_plan_kind": str(prepared_item.get("source_plan_kind") or "mbox"),
                                    "source_rel_path": source_rel_path,
                                    "source_item_id": str(prepared_item["source_item_id"]),
                                },
                            ),
                        )
                        context["current_ingestion_batch"] = commit_result.get("current_ingestion_batch")
                elif payload_kind == "mbox_source_finalizer" or str(claimed_row["unit_type"] or "") == "mbox_source_finalizer":
                    prepare_error = normalize_whitespace(str(prepared_item.get("prepare_error") or "")) or None
                    if prepare_error:
                        raise RetrieverError(prepare_error)
                    commit_result = ingest_v2_commit_mbox_source_finalizer(
                        connection,
                        paths,
                        run_id=run_id,
                        work_item_id=work_item_id,
                        writer_id=writer_id,
                        cursor=cursor,
                        prepared_item=prepared_item,
                    )
                elif payload_kind == "pst_message" or str(claimed_row["unit_type"] or "") == "pst_message":
                    prepare_error = normalize_whitespace(str(prepared_item.get("prepare_error") or "")) or None
                    if prepare_error:
                        raise RetrieverError(prepare_error)
                    if prepared_item.get("skip"):
                        connection.execute("BEGIN")
                        try:
                            commit_result = {
                                "action": "skipped",
                                "source_kind": PST_SOURCE_KIND,
                                "source_plan_kind": str(prepared_item.get("source_plan_kind") or "pst"),
                                "source_rel_path": str(prepared_item.get("source_rel_path") or ""),
                                "source_item_id": str(prepared_item.get("source_item_id") or ""),
                                "current_ingestion_batch": None,
                                "document_id": None,
                            }
                            ingest_v2_commit_pst_work_item_hook(
                                connection,
                                run_id=run_id,
                                work_item_id=work_item_id,
                                writer_id=writer_id,
                                cursor=cursor,
                                result=commit_result,
                            )
                            connection.commit()
                        except Exception:
                            connection.rollback()
                            raise
                    else:
                        source_rel_path = str(prepared_item["source_rel_path"])
                        context = ingest_v2_ensure_pst_commit_context(
                            connection,
                            root,
                            prepared_item=prepared_item,
                            cursor=cursor,
                            contexts=pst_contexts,
                        )
                        existing_entry = dict(
                            dict(context.get("existing_entries_by_source_item") or {}).get(
                                str(prepared_item["source_item_id"])
                            )
                            or {}
                        )
                        commit_result = commit_prepared_container_message(
                            connection,
                            paths,
                            prepared_item,
                            existing_entry.get("document_row"),
                            existing_entry.get("occurrence_row"),
                            current_ingestion_batch=(
                                int(context["current_ingestion_batch"])
                                if context.get("current_ingestion_batch") is not None
                                else None
                            ),
                            dataset_id=int(context["dataset_id"]),
                            dataset_source_id=(
                                int(context["dataset_source_id"])
                                if context.get("dataset_source_id") is not None
                                else None
                            ),
                            source_kind=PST_SOURCE_KIND,
                            source_rel_path=source_rel_path,
                            file_type_override=PST_SOURCE_KIND,
                            scan_started_at=str(prepared_item["scan_started_at"]),
                            before_transaction_commit=lambda commit_connection, result: ingest_v2_commit_pst_work_item_hook(
                                commit_connection,
                                run_id=run_id,
                                work_item_id=work_item_id,
                                writer_id=writer_id,
                                cursor=cursor,
                                result={
                                    **result,
                                    "source_kind": PST_SOURCE_KIND,
                                    "source_plan_kind": str(prepared_item.get("source_plan_kind") or "pst"),
                                    "source_rel_path": source_rel_path,
                                    "source_item_id": str(prepared_item["source_item_id"]),
                                },
                            ),
                        )
                        context["current_ingestion_batch"] = commit_result.get("current_ingestion_batch")
                elif payload_kind == "pst_source_finalizer" or str(claimed_row["unit_type"] or "") == "pst_source_finalizer":
                    prepare_error = normalize_whitespace(str(prepared_item.get("prepare_error") or "")) or None
                    if prepare_error:
                        raise RetrieverError(prepare_error)
                    commit_result = ingest_v2_commit_pst_source_finalizer(
                        connection,
                        paths,
                        run_id=run_id,
                        work_item_id=work_item_id,
                        writer_id=writer_id,
                        cursor=cursor,
                        prepared_item=prepared_item,
                    )
                else:
                    commit_result = commit_prepared_loose_file(
                        connection,
                        paths,
                        prepared_item,
                        existing_by_rel,
                        unseen_existing_by_hash,
                        ensure_filesystem_dataset,
                        (
                            int(cursor["current_ingestion_batch"])
                            if cursor.get("current_ingestion_batch") is not None
                            else None
                        ),
                        before_transaction_commit=lambda commit_connection, result: ingest_v2_commit_work_item_hook(
                            commit_connection,
                            run_id=run_id,
                            work_item_id=work_item_id,
                            writer_id=writer_id,
                            cursor=cursor,
                            result=result,
                        ),
                    )
                action = str(commit_result.get("action") or "")
                if action == "failed":
                    failed += 1
                    ingest_v2_mark_commit_failed(
                        connection,
                        run_id=run_id,
                        work_item_id=work_item_id,
                        writer_id=writer_id,
                        message=str(commit_result.get("error") or "Commit failed."),
                    )
                else:
                    committed += 1
                    actions[action] = int(actions.get(action) or 0) + 1
                    if bool(commit_result.get("freshness_fallback")):
                        freshness_fallbacks += 1
            except Exception as exc:
                rollback_open_transaction(connection)
                failed += 1
                ingest_v2_mark_commit_failed(
                    connection,
                    run_id=run_id,
                    work_item_id=work_item_id,
                    writer_id=writer_id,
                    message=f"{type(exc).__name__}: {exc}",
                )

        advanced_after_commit = ingest_v2_maybe_advance_after_commit(connection, run_id=run_id)
        if lease_acquired:
            ingest_v2_release_writer_lease(connection, run_id=run_id, writer_id=writer_id)
            lease_acquired = False
        updated_row = require_ingest_v2_run_row(connection, run_id)
        advanced_to_finalize = bool(advanced_after_commit and str(updated_row["phase"]) == "finalizing")
        advanced_to_prepare = bool(advanced_after_commit and str(updated_row["phase"]) == "preparing")
        remaining_commit_items = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM ingest_work_items
                WHERE run_id = ?
                  AND status IN ('prepared', 'committing')
                """,
                (run_id,),
            ).fetchone()[0]
            or 0
        )
        return {
            "ok": True,
            "implemented": True,
            "step": "commit",
            "writer_id": writer_id,
            "writer_busy": writer_busy,
            "committed": committed,
            "failed": failed,
            "stale_reclaimed": stale_reclaimed,
            "actions": actions,
            "freshness_fallbacks": freshness_fallbacks,
            "advanced_to_finalize": advanced_to_finalize,
            "advanced_to_prepare": advanced_to_prepare,
            "more_commit_remaining": remaining_commit_items > 0,
            "more_work_remaining": str(updated_row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
            "run": ingest_v2_status_payload(connection, root, updated_row, budget_seconds=budget),
        }
    finally:
        if lease_acquired:
            try:
                ingest_v2_release_writer_lease(connection, run_id=run_id, writer_id=writer_id)
            except Exception:
                pass
        connection.close()


def ingest_v2_finalize_step(
    root: Path,
    *,
    run_id: str,
    budget_seconds: int | None = None,
    schema_applied: bool = False,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    deadline = time.perf_counter() + max(0.1, float(budget) - 0.25)
    paths = workspace_paths(root)
    ensure_layout(paths)
    stages_completed: list[str] = []
    connection = connect_db(paths["db_path"])
    try:
        if not schema_applied:
            apply_schema(connection, root)
        row = require_ingest_v2_run_row(connection, run_id)
        if str(row["status"]) in INGEST_V2_TERMINAL_STATUSES or row["cancel_requested_at"] is not None:
            return {
                "ok": True,
                "implemented": True,
                "step": "finalize",
                "stages_completed": [],
                "finalization_complete": str(row["status"]) == "completed",
                "more_finalize_remaining": False,
                "more_work_remaining": False,
                "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }
        if str(row["phase"]) != "finalizing":
            return {
                "ok": True,
                "implemented": True,
                "step": "finalize",
                "stages_completed": [],
                "finalization_complete": False,
                "more_finalize_remaining": str(row["phase"]) in {"planning", "preparing", "committing"},
                "more_work_remaining": str(row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
                "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }

        cursor = ingest_v2_load_finalize_cursor(connection, run_id=run_id)
        stage = str(cursor.get("stage") or "missing")
        if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_COMMIT_MIN_START_SECONDS:
            return {
                "ok": True,
                "implemented": True,
                "step": "finalize",
                "stages_completed": [],
                "finalization_complete": False,
                "more_finalize_remaining": True,
                "more_work_remaining": True,
                "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
            }

        if stage == "production":
            pending_production_rel_roots = list(cursor.get("pending_production_rel_roots") or [])
            while (
                pending_production_rel_roots
                and ingest_v2_deadline_remaining_seconds(deadline) >= INGEST_V2_COMMIT_MIN_START_SECONDS
            ):
                production_rel_root = str(pending_production_rel_roots.pop(0))
                cursor["pending_production_rel_roots"] = pending_production_rel_roots
                production_payload = dict(
                    dict(cursor.get("production_roots_by_rel_root") or {}).get(production_rel_root) or {}
                )
                if production_payload:
                    production_stats = ingest_v2_finalize_production_root(
                        connection,
                        paths,
                        root,
                        production_payload=production_payload,
                    )
                    aggregate_stats = cursor.setdefault("production_stats", {})
                    if isinstance(aggregate_stats, dict):
                        for key, value in production_stats.items():
                            aggregate_stats[key] = int(aggregate_stats.get(key) or 0) + int(value)
                    finalized_roots = list(cursor.get("production_finalized_roots") or [])
                    finalized_roots.append(production_rel_root)
                    cursor["production_finalized_roots"] = finalized_roots
                connection.execute("BEGIN")
                try:
                    ingest_v2_save_finalize_cursor(connection, run_id=run_id, cursor=cursor, status="pending")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
            if pending_production_rel_roots:
                updated_row = require_ingest_v2_run_row(connection, run_id)
                return {
                    "ok": True,
                    "implemented": True,
                    "step": "finalize",
                    "stages_completed": stages_completed,
                    "cursor": cursor,
                    "finalization_complete": False,
                    "more_finalize_remaining": True,
                    "more_work_remaining": True,
                    "run": ingest_v2_status_payload(connection, root, updated_row, budget_seconds=budget),
                }
            cursor["stage"] = "missing"
            connection.execute("BEGIN")
            try:
                ingest_v2_save_finalize_cursor(connection, run_id=run_id, cursor=cursor, status="pending")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            if list(cursor.get("production_finalized_roots") or []):
                stages_completed.append("production")
            stage = "missing"

        if stage == "missing":
            scanned_rel_paths = ingest_v2_run_loose_rel_paths(connection, run_id=run_id)
            scan_scope = ingest_v2_scan_scope_from_run(root, row)
            filesystem_missing = mark_missing_documents(connection, scanned_rel_paths, scan_scope=scan_scope)
            scanned_pst_source_rel_paths = ingest_v2_run_pst_source_rel_paths(connection, run_id=run_id)
            pst_sources_missing, pst_documents_missing = mark_missing_pst_documents(
                connection,
                scanned_pst_source_rel_paths,
                scan_scope=scan_scope,
            )
            scanned_mbox_source_rel_paths = ingest_v2_run_mbox_source_rel_paths(connection, run_id=run_id)
            mbox_sources_missing, mbox_documents_missing = mark_missing_mbox_documents(
                connection,
                scanned_mbox_source_rel_paths,
                scan_scope=scan_scope,
            )
            slack_documents_missing = ingest_v2_mark_missing_slack_documents(connection, run_id=run_id)
            cursor["filesystem_missing"] = int(filesystem_missing)
            cursor["pst_sources_missing"] = int(pst_sources_missing)
            cursor["pst_documents_missing"] = int(pst_documents_missing)
            cursor["mbox_sources_missing"] = int(mbox_sources_missing)
            cursor["mbox_documents_missing"] = int(mbox_documents_missing)
            cursor["slack_documents_missing"] = int(slack_documents_missing)
            cursor["stage"] = "conversations"
            connection.execute("BEGIN")
            try:
                ingest_v2_save_finalize_cursor(connection, run_id=run_id, cursor=cursor, status="pending")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            stages_completed.append("missing")
            stage = "conversations"

        if stage == "conversations" and ingest_v2_deadline_remaining_seconds(deadline) >= INGEST_V2_COMMIT_MIN_START_SECONDS:
            connection.execute("BEGIN")
            try:
                conversation_assignment = assign_supported_conversations(connection)
                mbox_documents_redeleted = ingest_v2_redelete_retired_mbox_documents(
                    connection,
                    paths,
                    run_id=run_id,
                )
                pst_documents_redeleted = ingest_v2_redelete_retired_pst_documents(
                    connection,
                    paths,
                    run_id=run_id,
                )
                cursor["conversation_assignment"] = {
                    key: int(value)
                    for key, value in dict(conversation_assignment).items()
                }
                cursor["mbox_documents_redeleted"] = int(mbox_documents_redeleted)
                cursor["pst_documents_redeleted"] = int(pst_documents_redeleted)
                target_conversation_ids = list_active_conversation_ids(connection)
                next_commit_order = int(
                    connection.execute(
                        """
                        SELECT COALESCE(MAX(commit_order), 0) + 1
                        FROM ingest_work_items
                        WHERE run_id = ?
                        """,
                        (run_id,),
                    ).fetchone()[0]
                    or 1
                )
                planned_preview_items = ingest_v2_plan_conversation_preview_items(
                    connection,
                    run_id=run_id,
                    conversation_ids=target_conversation_ids,
                    next_commit_order=next_commit_order,
                )
                cursor["conversation_preview_target_count"] = len(target_conversation_ids)
                cursor["conversation_preview_work_items_planned"] = int(planned_preview_items["planned"])
                if int(planned_preview_items["planned"]):
                    cursor["stage"] = "conversation_previews"
                    next_phase = "preparing"
                else:
                    cursor["conversation_previews_refreshed"] = 0
                    cursor["conversation_preview_failures"] = 0
                    cursor["empty_conversation_preview_dirs_pruned"] = prune_empty_conversation_preview_dirs(paths)
                    cursor["stage"] = "prune"
                    next_phase = "finalizing"
                ingest_v2_save_finalize_cursor(connection, run_id=run_id, cursor=cursor, status="pending")
                connection.execute(
                    """
                    UPDATE ingest_runs
                    SET phase = ?,
                        status = ?,
                        last_heartbeat_at = ?
                    WHERE run_id = ?
                      AND phase = 'finalizing'
                      AND cancel_requested_at IS NULL
                    """,
                    (next_phase, next_phase, utc_now(), run_id),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            stages_completed.append("conversations")
            stage = str(cursor.get("stage") or "prune")

        if (
            stage == "conversation_previews"
            and str(require_ingest_v2_run_row(connection, run_id)["phase"]) == "finalizing"
        ):
            preview_summary = ingest_v2_conversation_preview_work_summary(connection, run_id=run_id)
            connection.execute("BEGIN")
            try:
                cursor["conversation_previews_refreshed"] = int(preview_summary["refreshed"])
                cursor["conversation_preview_failures"] = int(preview_summary["failed"])
                cursor["mbox_documents_redeleted"] = int(cursor.get("mbox_documents_redeleted") or 0) + int(
                    ingest_v2_redelete_retired_mbox_documents(
                        connection,
                        paths,
                        run_id=run_id,
                    )
                )
                cursor["pst_documents_redeleted"] = int(cursor.get("pst_documents_redeleted") or 0) + int(
                    ingest_v2_redelete_retired_pst_documents(
                        connection,
                        paths,
                        run_id=run_id,
                    )
                )
                cursor["empty_conversation_preview_dirs_pruned"] = prune_empty_conversation_preview_dirs(paths)
                cursor["stage"] = "prune"
                ingest_v2_save_finalize_cursor(connection, run_id=run_id, cursor=cursor, status="pending")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            stages_completed.append("conversation_previews")
            stage = "prune"

        if stage == "prune" and ingest_v2_deadline_remaining_seconds(deadline) >= INGEST_V2_COMMIT_MIN_START_SECONDS:
            connection.execute("BEGIN")
            try:
                pruned = prune_unused_filesystem_dataset(connection)
                cursor["pruned_unused_filesystem_dataset"] = bool(pruned)
                cursor["stage"] = "complete"
                ingest_v2_save_finalize_cursor(connection, run_id=run_id, cursor=cursor, status="pending")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            stages_completed.append("prune")
            stage = "complete"

        finalization_complete = stage == "complete"
        if finalization_complete:
            now = utc_now()
            connection.execute("BEGIN")
            try:
                ingest_v2_save_finalize_cursor(connection, run_id=run_id, cursor=cursor, status="complete")
                connection.execute(
                    """
                    UPDATE ingest_runs
                    SET phase = 'completed',
                        status = 'completed',
                        completed_at = COALESCE(completed_at, ?),
                        last_heartbeat_at = ?,
                        error = NULL
                    WHERE run_id = ?
                      AND phase = 'finalizing'
                      AND cancel_requested_at IS NULL
                    """,
                    (now, now, run_id),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            stages_completed.append("complete")

        updated_row = require_ingest_v2_run_row(connection, run_id)
        return {
            "ok": True,
            "implemented": True,
            "step": "finalize",
            "stages_completed": stages_completed,
            "cursor": cursor,
            "finalization_complete": str(updated_row["status"]) == "completed",
            "more_finalize_remaining": str(updated_row["phase"]) == "finalizing",
            "more_work_remaining": str(updated_row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
            "run": ingest_v2_status_payload(connection, root, updated_row, budget_seconds=budget),
        }
    finally:
        connection.close()


def ingest_v2_select_runnable_step(status_payload: dict[str, object]) -> str | None:
    for command in list(status_payload.get("next_recommended_commands") or []):
        try:
            command_name = shlex.split(str(command))[0]
        except (IndexError, ValueError):
            continue
        if command_name == "ingest-run-step":
            continue
        if command_name == "ingest-plan-step":
            return "plan"
        if command_name == "ingest-prepare-step":
            return "prepare"
        if command_name == "ingest-commit-step":
            return "commit"
        if command_name == "ingest-finalize-step":
            return "finalize"
    return None


def ingest_v2_runner_step_budget(remaining_seconds: float) -> int:
    return max(
        1,
        min(
            MAX_RESUMABLE_STEP_BUDGET_SECONDS,
            int(max(1.0, remaining_seconds)),
        ),
    )


def ingest_v2_run_step(
    root: Path,
    *,
    run_id: str | None = None,
    budget_seconds: int | None = None,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    deadline = time.perf_counter() + max(0.1, float(budget) - 0.25)
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    schema_started = time.perf_counter()
    schema_ms = 0.0
    initial_status_payload_ms = 0.0
    stale_reclaimed = {"prepare": 0, "commit": 0}
    try:
        apply_schema(connection, root)
        schema_ms = ingest_v2_elapsed_ms(schema_started)
        row = require_ingest_v2_run_row(connection, run_id) if run_id else latest_ingest_v2_run_row(connection)
        if row is None:
            status_payload = ingest_v2_status(root, run_id=None, budget_seconds=budget)
            run_payload = dict(status_payload)
            run_payload.pop("ok", None)
            return {
                "ok": True,
                "implemented": True,
                "step": "run",
                "executed": False,
                "selected_step": None,
                "executed_steps": [],
                "stale_reclaimed": stale_reclaimed,
                "reason": "no_ingest_run",
                "step_result": None,
                "step_results": [],
                "more_work_remaining": False,
                "run": run_payload,
                "timings": {
                    "schema_ms": round(schema_ms, 3),
                    "initial_status_payload_ms": 0.0,
                    "inner_step_ms": ingest_v2_timing_summary([]),
                },
                "remaining_budget_seconds": round(ingest_v2_deadline_remaining_seconds(deadline), 3),
            }
        resolved_run_id = str(row["run_id"])
        if str(row["status"]) in INGEST_V2_ACTIVE_STATUSES and row["cancel_requested_at"] is None:
            stale_reclaimed = ingest_v2_reclaim_stale_work_items(connection, run_id=resolved_run_id)
            if any(stale_reclaimed.values()):
                row = require_ingest_v2_run_row(connection, resolved_run_id)
        status_payload, initial_status_payload_ms = ingest_v2_status_payload_timed(
            connection,
            root,
            row,
            budget_seconds=budget,
        )
    finally:
        connection.close()

    executed_steps: list[str] = []
    step_results: list[dict[str, object]] = []
    inner_step_ms_values: list[float] = []
    stop_reason: str | None = None
    run_payload = status_payload
    while len(executed_steps) < INGEST_V2_RUN_STEP_MAX_INNER_STEPS:
        remaining_seconds = ingest_v2_deadline_remaining_seconds(deadline)
        if remaining_seconds < INGEST_V2_RUN_STEP_MIN_REMAINING_SECONDS:
            stop_reason = "budget_exhausted"
            break
        selected_step = ingest_v2_select_runnable_step(run_payload)
        if selected_step is None:
            stop_reason = (
                "run_terminal"
                if str(run_payload.get("status")) in INGEST_V2_TERMINAL_STATUSES
                else "no_runnable_step"
            )
            break
        inner_budget = ingest_v2_runner_step_budget(remaining_seconds)
        step_started = time.perf_counter()
        if selected_step == "plan":
            step_result = ingest_v2_plan_step(
                root,
                run_id=resolved_run_id,
                budget_seconds=inner_budget,
                schema_applied=True,
            )
        elif selected_step == "prepare":
            step_result = ingest_v2_prepare_step(
                root,
                run_id=resolved_run_id,
                budget_seconds=inner_budget,
                schema_applied=True,
            )
        elif selected_step == "commit":
            step_result = ingest_v2_commit_step(
                root,
                run_id=resolved_run_id,
                budget_seconds=inner_budget,
                schema_applied=True,
            )
        elif selected_step == "finalize":
            step_result = ingest_v2_finalize_step(
                root,
                run_id=resolved_run_id,
                budget_seconds=inner_budget,
                schema_applied=True,
            )
        else:
            raise RetrieverError(f"Unsupported resumable ingest step selection: {selected_step}")
        inner_step_ms_values.append(ingest_v2_elapsed_ms(step_started))

        executed_steps.append(selected_step)
        step_results.append(step_result)
        if isinstance(step_result.get("run"), dict):
            run_payload = dict(step_result["run"])
        if selected_step == "prepare" and bool(step_result.get("throttled")):
            stop_reason = "prepare_throttled"
            break
        if selected_step == "commit" and bool(step_result.get("writer_busy")):
            stop_reason = "writer_busy"
            break
        if not bool(step_result.get("more_work_remaining")):
            stop_reason = "run_terminal"
            break
    else:
        stop_reason = "max_inner_steps"

    return {
        "ok": True,
        "implemented": True,
        "step": "run",
        "executed": bool(executed_steps),
        "selected_step": executed_steps[0] if executed_steps else None,
        "executed_steps": executed_steps,
        "stale_reclaimed": stale_reclaimed,
        "reason": stop_reason,
        "step_result": step_results[-1] if step_results else None,
        "step_results": step_results,
        "more_work_remaining": str(run_payload.get("status")) not in INGEST_V2_TERMINAL_STATUSES,
        "run": run_payload,
        "timings": {
            "schema_ms": round(schema_ms, 3),
            "initial_status_payload_ms": round(initial_status_payload_ms, 3),
            "inner_step_ms": ingest_v2_timing_summary(inner_step_ms_values),
        },
        "remaining_budget_seconds": round(ingest_v2_deadline_remaining_seconds(deadline), 3),
    }


def ingest_v2_scopes_match(active_scope: object, requested_scope: dict[str, object]) -> bool:
    if not isinstance(active_scope, dict):
        return False
    return compact_json_text(active_scope) == compact_json_text(requested_scope)


def ingest_v2_facade_command(
    root: Path,
    *,
    recursive: bool,
    raw_file_types: str | None,
    raw_paths: list[str] | None,
    budget_seconds: int,
) -> str:
    parts = ["ingest", shlex.quote(str(root))]
    if recursive:
        parts.append("--recursive")
    for raw_path in raw_paths or []:
        parts.extend(["--path", shlex.quote(str(raw_path))])
    if raw_file_types:
        parts.extend(["--file-types", shlex.quote(str(raw_file_types))])
    parts.extend(["--budget-seconds", str(int(budget_seconds))])
    return " ".join(parts)


def ingest_v2_facade_payload(
    *,
    root: Path,
    recursive: bool,
    raw_file_types: str | None,
    raw_paths: list[str] | None,
    budget_seconds: int,
    created: bool,
    resumed: bool,
    run_payload: dict[str, object],
    step_payloads: list[dict[str, object]],
    reason: str,
    mode: str,
) -> dict[str, object]:
    run_id = str(run_payload.get("run_id") or "")
    more_work_remaining = str(run_payload.get("status")) not in INGEST_V2_TERMINAL_STATUSES
    executed_steps: list[str] = []
    for step_payload in step_payloads:
        executed_steps.extend(str(step) for step in list(step_payload.get("executed_steps") or []))
    next_commands: list[str] = []
    if more_work_remaining:
        next_commands.append(
            ingest_v2_facade_command(
                root,
                recursive=recursive,
                raw_file_types=raw_file_types,
                raw_paths=raw_paths,
                budget_seconds=budget_seconds,
            )
        )
    next_commands.extend(str(command) for command in list(run_payload.get("next_recommended_commands") or []))
    return {
        "ok": True,
        "pipeline": INGEST_PIPELINE_V2,
        "mode": mode,
        "created": created,
        "resumed": resumed,
        "run_id": run_id or None,
        "status": run_payload.get("status"),
        "phase": run_payload.get("phase"),
        "reason": reason,
        "executed": bool(executed_steps),
        "executed_steps": executed_steps,
        "step_calls": len(step_payloads),
        "step_results": step_payloads,
        "more_work_remaining": more_work_remaining,
        "run": run_payload,
        "counts": run_payload.get("counts"),
        "next_recommended_commands": next_commands,
    }


def ingest_v2_facade_remaining_budget(deadline: float) -> int | None:
    remaining_seconds = ingest_v2_deadline_remaining_seconds(deadline)
    if remaining_seconds < INGEST_V2_RUN_STEP_MIN_REMAINING_SECONDS:
        return None
    return max(1, min(MAX_RESUMABLE_STEP_BUDGET_SECONDS, int(remaining_seconds)))


def ingest_v2_facade(
    root: Path,
    *,
    recursive: bool,
    raw_file_types: str | None,
    raw_paths: list[str] | None = None,
    budget_seconds: int | None = None,
    run_to_completion: bool = False,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    deadline = time.perf_counter() + max(0.1, float(budget) - 0.25)
    paths = workspace_paths(root)
    ensure_layout(paths)
    requested_scope = ingest_v2_scope_payload(
        root,
        recursive=recursive,
        raw_file_types=raw_file_types,
        raw_paths=raw_paths,
    )
    created = False
    resumed = False
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        active_row = active_ingest_v2_run_row(connection)
        if active_row is not None:
            active_scope = decode_json_text(active_row["scope_json"], default={}) or {}
            if not ingest_v2_scopes_match(active_scope, requested_scope):
                payload = ingest_v2_conflict_payload(
                    root,
                    active_row,
                    message="A resumable ingest run is active with a different scope.",
                )
                payload["active_scope"] = active_scope
                payload["requested_scope"] = requested_scope
                raise RetrieverStructuredError(
                    f"Resumable ingest run {active_row['run_id']} is already active with a different scope.",
                    payload,
                )
            run_id = str(active_row["run_id"])
            resumed = True
            run_payload = ingest_v2_status_payload(connection, root, active_row, budget_seconds=budget)
        else:
            run_id = ""
            run_payload = {}
    finally:
        connection.close()

    if not resumed:
        start_payload = ingest_v2_start(
            root,
            recursive=recursive,
            raw_file_types=raw_file_types,
            raw_paths=raw_paths,
            budget_seconds=budget,
        )
        run_id = str(start_payload["run_id"])
        created = True
        run_payload = {
            key: value
            for key, value in start_payload.items()
            if key not in {"ok", "created"}
        }

    step_payloads: list[dict[str, object]] = []
    reason = "run_terminal" if str(run_payload.get("status")) in INGEST_V2_TERMINAL_STATUSES else "budget_exhausted"
    while str(run_payload.get("status")) not in INGEST_V2_TERMINAL_STATUSES:
        step_budget = budget if run_to_completion else ingest_v2_facade_remaining_budget(deadline)
        if step_budget is None:
            reason = "budget_exhausted"
            break
        step_payload = ingest_v2_run_step(root, run_id=run_id, budget_seconds=step_budget)
        step_payloads.append(step_payload)
        run_payload = dict(step_payload["run"])
        reason = str(step_payload.get("reason") or reason)
        if not run_to_completion:
            break

    return ingest_v2_facade_payload(
        root=root,
        recursive=recursive,
        raw_file_types=raw_file_types,
        raw_paths=raw_paths,
        budget_seconds=budget,
        created=created,
        resumed=resumed,
        run_payload=run_payload,
        step_payloads=step_payloads,
        reason=reason,
        mode="run_to_completion" if run_to_completion else "bounded",
    )


def ingest_v2_step_not_implemented(
    root: Path,
    *,
    run_id: str,
    phase: str,
    budget_seconds: int | None = None,
) -> dict[str, object]:
    set_active_workspace_root(root)
    budget = normalize_resumable_step_budget(budget_seconds)
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        row = require_ingest_v2_run_row(connection, run_id)
        return {
            "ok": True,
            "implemented": False,
            "step": phase,
            "processed": 0,
            "more_work_remaining": str(row["status"]) not in INGEST_V2_TERMINAL_STATUSES,
            "message": "This V2 step command is reserved by the foundation slice and will be implemented in the next ingest slice.",
            "run": ingest_v2_status_payload(connection, root, row, budget_seconds=budget),
        }
    finally:
        connection.close()


def source_file_snapshot(path: Path) -> tuple[int | None, int | None]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None, None
    return stat.st_size, stat.st_mtime_ns


def rollback_open_transaction(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        return
    try:
        connection.rollback()
    except sqlite3.Error:
        return


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
    scan_scope: dict[str, object],
) -> dict[str, object]:
    scan_hash_ms = 0.0
    production_signatures = find_production_root_signatures(root, recursive, connection, scan_scope=scan_scope)
    production_root_paths = [Path(signature["root"]).resolve() for signature in production_signatures]
    slack_export_descriptors = find_scoped_source_roots(
        find_slack_export_roots,
        root,
        recursive,
        scan_scope,
        allowed_types,
    )
    slack_export_root_paths = [Path(descriptor["root"]).resolve() for descriptor in slack_export_descriptors]
    gmail_export_descriptors = find_scoped_source_roots(
        find_gmail_export_roots,
        root,
        recursive,
        scan_scope,
        allowed_types,
    )
    pst_export_descriptors = (
        find_scoped_source_roots(find_pst_export_roots, root, recursive, scan_scope)
        if allowed_types is None or PST_SOURCE_KIND in allowed_types
        else []
    )
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
        for path in collect_files(root, recursive, allowed_types, scan_scope=scan_scope)
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
            rollback_open_transaction(connection)
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
            rollback_open_transaction(connection)
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
                rollback_open_transaction(connection)
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
    scan_scope: dict[str, object] | None = None,
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
        if (
            ingest_scan_scope_contains_rel_path(scan_scope, str(row["rel_path"]))
            and str(row["rel_path"]) not in scanned_rel_paths
            and row["file_hash"]
        ):
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
    default_workers = min(8, os.cpu_count() or 4)
    raw_value = os.environ.get("RETRIEVER_INGEST_CONTAINER_WORKERS")
    if raw_value is None:
        return default_workers
    raw_value = raw_value.strip()
    if not raw_value:
        return default_workers
    try:
        return max(1, int(raw_value))
    except ValueError:
        return default_workers


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
        extract_started = time.perf_counter()
        extracted_payload = extract_document(Path(item["path"]), include_attachments=True)
        prepared_item["prepare_extract_ms"] = ingest_v2_elapsed_ms(extract_started)
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
        prepared_item["prepare_extract_ms"] = 0.0
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
    before_transaction_commit=None,
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
        extracted_for_skip_check = prepared_item.get("extracted_payload")
        if isinstance(extracted_for_skip_check, dict):
            extracted_for_skip_check = apply_manual_locks(existing_row, dict(extracted_for_skip_check))
        if (
            existing_occurrence_row["file_hash"] == file_hash
            and existing_occurrence_row["lifecycle_status"] == ACTIVE_OCCURRENCE_STATUS
            and document_row_has_seeded_text_revisions(existing_row)
            and extracted_payload_matches_document_row(existing_row, extracted_for_skip_check)
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
                result = {
                    "action": "skipped",
                    "current_ingestion_batch": current_ingestion_batch,
                    "freshness_fallback": freshness_fallback,
                    "document_id": int(existing_occurrence_row["document_id"]),
                    "file_hash": file_hash,
                }
                if before_transaction_commit is not None:
                    before_transaction_commit(connection, result)
                connection.commit()
                return result
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
                result = {
                    "action": "new",
                    "current_ingestion_batch": current_ingestion_batch,
                    "freshness_fallback": freshness_fallback,
                    "document_id": int(exact_duplicate_document["id"]),
                    "file_hash": file_hash,
                }
                if before_transaction_commit is not None:
                    before_transaction_commit(connection, result)
                connection.commit()
                return result
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
        result = {
            "action": action,
            "current_ingestion_batch": current_ingestion_batch,
            "freshness_fallback": freshness_fallback,
            "document_id": document_id,
            "file_hash": file_hash,
        }
        if action == "renamed" and existing_occurrence_row is not None:
            result["source_occurrence_id"] = int(existing_occurrence_row["id"])
            result["source_document_id"] = int(existing_occurrence_row["document_id"])
        if before_transaction_commit is not None:
            before_transaction_commit(connection, result)
        connection.commit()
        return result
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
    scan_scope: dict[str, object] | None = None,
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
    filesystem_missing = mark_missing_documents(connection, scanned_rel_paths, scan_scope=scan_scope)
    pst_sources_missing = 0
    pst_documents_missing = 0
    mbox_sources_missing = 0
    mbox_documents_missing = 0
    if allowed_types is None or PST_SOURCE_KIND in allowed_types:
        pst_sources_missing, pst_documents_missing = mark_missing_pst_documents(
            connection,
            scanned_pst_source_rel_paths,
            scan_scope=scan_scope,
        )
    if allowed_types is None or MBOX_SOURCE_KIND in allowed_types:
        mbox_sources_missing, mbox_documents_missing = mark_missing_mbox_documents(
            connection,
            scanned_mbox_source_rel_paths,
            scan_scope=scan_scope,
        )
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
    set_active_workspace_root(root)
    paths = workspace_paths(root)
    ensure_layout(paths)
    total_started = time.perf_counter()
    benchmark_mark("ingest_production_begin")
    with workspace_ingest_session(paths, command_name="ingest-production") as ingest_session:
        connection = connect_db(paths["db_path"])
        try:
            setup_started = time.perf_counter()
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="ingest-production")
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
            session_warnings = list(ingest_session.get("warnings") or [])
            if session_warnings:
                result = {**result, "warnings": [*list(result.get("warnings", [])), *session_warnings]}
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


def ingest(
    root: Path,
    recursive: bool,
    raw_file_types: str | None,
    raw_paths: list[str] | None = None,
) -> dict[str, object]:
    set_active_workspace_root(root)
    paths = workspace_paths(root)
    ensure_layout(paths)
    allowed_types = parse_file_types(raw_file_types)
    scan_scope = build_ingest_scan_scope(root, raw_paths)
    total_started = time.perf_counter()
    benchmark_mark(
        "ingest_begin",
        recursive=recursive,
        file_type_filter_count=(len(allowed_types) if allowed_types is not None else 0),
        scan_paths=list(scan_scope.get("display_paths") or []),
    )
    with workspace_ingest_session(paths, command_name="ingest") as ingest_session:
        connection = connect_db(paths["db_path"])
        try:
            setup_started = time.perf_counter()
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="ingest")
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
            ingest_plan = plan_ingest_work(root, recursive, allowed_types, connection, scan_scope)
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
            warnings = [*list(ingest_session.get("warnings") or []), *list(special_source_state["warnings"])]
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
                scan_scope=scan_scope,
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
                        rollback_open_transaction(connection)
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
                        rollback_open_transaction(connection)
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
                scan_scope=scan_scope,
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
            if not bool(scan_scope.get("is_full_workspace")):
                result["scan_paths"] = list(scan_scope.get("display_paths") or [])
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


def counted_delete(
    connection: sqlite3.Connection,
    *,
    count_sql: str,
    delete_sql: str,
    params: tuple[object, ...] = (),
) -> int:
    row = connection.execute(count_sql, params).fetchone()
    deleted_count = int(row[0] or 0) if row is not None else 0
    connection.execute(delete_sql, params)
    return deleted_count


def reset_auto_entity_graph(connection: sqlite3.Connection) -> dict[str, int]:
    auto_document_links_deleted = counted_delete(
        connection,
        count_sql="SELECT COUNT(*) FROM document_entities WHERE assignment_mode = 'auto'",
        delete_sql="DELETE FROM document_entities WHERE assignment_mode = 'auto'",
    )
    auto_resolution_keys_deleted = counted_delete(
        connection,
        count_sql="""
            SELECT COUNT(*)
            FROM entity_resolution_keys
            WHERE identifier_id IS NULL
               OR identifier_id IN (
                 SELECT id
                 FROM entity_identifiers
                 WHERE COALESCE(source_kind, 'auto') = 'auto'
               )
        """,
        delete_sql="""
            DELETE FROM entity_resolution_keys
            WHERE identifier_id IS NULL
               OR identifier_id IN (
                 SELECT id
                 FROM entity_identifiers
                 WHERE COALESCE(source_kind, 'auto') = 'auto'
               )
        """,
    )
    auto_identifiers_deleted = counted_delete(
        connection,
        count_sql="SELECT COUNT(*) FROM entity_identifiers WHERE COALESCE(source_kind, 'auto') = 'auto'",
        delete_sql="DELETE FROM entity_identifiers WHERE COALESCE(source_kind, 'auto') = 'auto'",
    )
    auto_entities_deleted = counted_delete(
        connection,
        count_sql="""
            SELECT COUNT(*)
            FROM entities
            WHERE entity_origin IN ('observed', 'identified')
              AND display_name_source = 'auto'
              AND canonical_status = 'active'
              AND NOT EXISTS (
                SELECT 1
                FROM entity_identifiers ei
                WHERE ei.entity_id = entities.id
                  AND COALESCE(ei.source_kind, 'auto') != 'auto'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM document_entities de
                WHERE de.entity_id = entities.id
                  AND de.assignment_mode != 'auto'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entity_resolution_keys erk
                WHERE erk.entity_id = entities.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entity_overrides eo
                WHERE eo.source_entity_id = entities.id
                   OR eo.replacement_entity_id = entities.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entity_merge_blocks emb
                WHERE emb.left_entity_id = entities.id
                   OR emb.right_entity_id = entities.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entities merged_child
                WHERE merged_child.merged_into_entity_id = entities.id
              )
        """,
        delete_sql="""
            DELETE FROM entities
            WHERE entity_origin IN ('observed', 'identified')
              AND display_name_source = 'auto'
              AND canonical_status = 'active'
              AND NOT EXISTS (
                SELECT 1
                FROM entity_identifiers ei
                WHERE ei.entity_id = entities.id
                  AND COALESCE(ei.source_kind, 'auto') != 'auto'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM document_entities de
                WHERE de.entity_id = entities.id
                  AND de.assignment_mode != 'auto'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entity_resolution_keys erk
                WHERE erk.entity_id = entities.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entity_overrides eo
                WHERE eo.source_entity_id = entities.id
                   OR eo.replacement_entity_id = entities.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entity_merge_blocks emb
                WHERE emb.left_entity_id = entities.id
                   OR emb.right_entity_id = entities.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM entities merged_child
                WHERE merged_child.merged_into_entity_id = entities.id
              )
        """,
    )
    return {
        "auto_document_links_deleted": auto_document_links_deleted,
        "auto_resolution_keys_deleted": auto_resolution_keys_deleted,
        "auto_identifiers_deleted": auto_identifiers_deleted,
        "auto_entities_deleted": auto_entities_deleted,
    }


def entity_rebuild_document_ids(
    connection: sqlite3.Connection,
    document_ids: list[int] | None,
) -> list[int]:
    if document_ids:
        normalized_ids = list(dict.fromkeys(int(document_id) for document_id in document_ids))
        placeholders = ",".join("?" for _ in normalized_ids)
        rows = connection.execute(
            f"""
            SELECT id
            FROM documents
            WHERE id IN ({placeholders})
            ORDER BY id ASC
            """,
            tuple(normalized_ids),
        ).fetchall()
        found_ids = [int(row["id"]) for row in rows]
        found_id_set = set(found_ids)
        missing_ids = [document_id for document_id in normalized_ids if document_id not in found_id_set]
        if missing_ids:
            raise RetrieverError(f"Unknown document id(s): {', '.join(str(item) for item in missing_ids)}")
        return found_ids
    return [
        int(row["id"])
        for row in connection.execute(
            """
            SELECT id
            FROM documents
            WHERE canonical_status != ?
            ORDER BY id ASC
            """,
            (CANONICAL_STATUS_MERGED,),
        ).fetchall()
    ]


def entity_graph_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "entity_count": int(connection.execute("SELECT COUNT(*) FROM entities").fetchone()[0] or 0),
        "active_entity_count": int(
            connection.execute(
                "SELECT COUNT(*) FROM entities WHERE canonical_status = ?",
                (ENTITY_STATUS_ACTIVE,),
            ).fetchone()[0]
            or 0
        ),
        "document_entity_count": int(connection.execute("SELECT COUNT(*) FROM document_entities").fetchone()[0] or 0),
        "resolution_key_count": int(connection.execute("SELECT COUNT(*) FROM entity_resolution_keys").fetchone()[0] or 0),
    }


ENTITY_REBUILD_ACTIVE_STATUSES = {"resetting", "planning", "rebuilding"}
ENTITY_REBUILD_TERMINAL_STATUSES = {"completed", "canceled", "failed"}
ENTITY_REBUILD_ITEM_STATUSES = ("pending", "leased", "committed", "failed", "cancelled")
ENTITY_REBUILD_RESET_STAGES = (
    "document_entities",
    "resolution_keys",
    "identifiers",
    "entities",
    "complete",
)
ENTITY_REBUILD_LEASE_SECONDS = 45


def new_entity_rebuild_run_id(now: datetime | None = None) -> str:
    return new_ingest_v2_run_id(now)


def entity_rebuild_run_row_by_id(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM entity_rebuild_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()


def latest_entity_rebuild_run_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM entity_rebuild_runs
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()


def active_entity_rebuild_run_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        f"""
        SELECT *
        FROM entity_rebuild_runs
        WHERE status IN ({", ".join("?" for _ in ENTITY_REBUILD_ACTIVE_STATUSES)})
          AND cancel_requested_at IS NULL
        ORDER BY id ASC
        LIMIT 1
        """,
        tuple(sorted(ENTITY_REBUILD_ACTIVE_STATUSES)),
    ).fetchone()


def require_entity_rebuild_run_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
    row = entity_rebuild_run_row_by_id(connection, run_id)
    if row is None:
        raise RetrieverError(f"Unknown entity rebuild run id: {run_id}")
    return row


def entity_rebuild_conflict_payload(root: Path, active_row: sqlite3.Row, *, message: str) -> dict[str, object]:
    run_id = str(active_row["run_id"])
    quoted_root = shlex.quote(str(root))
    quoted_run_id = shlex.quote(run_id)
    return {
        "ok": False,
        "error": "active_entity_rebuild_run",
        "active_run_id": run_id,
        "message": message,
        "status_command": f"rebuild-entities-status {quoted_root} --run-id {quoted_run_id}",
        "cancel_command": f"rebuild-entities-cancel {quoted_root} --run-id {quoted_run_id}",
    }


def raise_if_entity_rebuild_active(connection: sqlite3.Connection, root: Path, *, command_name: str) -> None:
    active_row = active_entity_rebuild_run_row(connection)
    if active_row is None:
        return
    raise RetrieverStructuredError(
        f"{command_name} cannot run while entity rebuild run {active_row['run_id']} is active.",
        entity_rebuild_conflict_payload(
            root,
            active_row,
            message=f"{command_name} cannot run while a resumable entity rebuild is active.",
        ),
    )


def entity_rebuild_status_counts(connection: sqlite3.Connection, *, run_id: str) -> dict[str, int]:
    counts = {status: 0 for status in ENTITY_REBUILD_ITEM_STATUSES}
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM entity_rebuild_items
        WHERE run_id = ?
        GROUP BY status
        """,
        (run_id,),
    ).fetchall()
    for row in rows:
        counts[str(row["status"])] = int(row["count"] or 0)
    return counts


def entity_rebuild_status_payload(
    connection: sqlite3.Connection,
    root: Path,
    row: sqlite3.Row,
    *,
    budget_seconds: int = DEFAULT_RESUMABLE_STEP_BUDGET_SECONDS,
) -> dict[str, object]:
    run_id = str(row["run_id"])
    counts = entity_rebuild_status_counts(connection, run_id=run_id)
    reset_counts = decode_json_text(row["reset_counts_json"], default={}) or {}
    cursor = decode_json_text(row["cursor_json"], default={}) or {}
    totals_row = connection.execute(
        """
        SELECT
          COALESCE(SUM(document_synced), 0) AS documents_synced,
          COALESCE(SUM(auto_links_created), 0) AS auto_links_created
        FROM entity_rebuild_items
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    root_arg = shlex.quote(str(root))
    run_id_arg = shlex.quote(run_id)
    budget_arg = str(int(budget_seconds))
    next_commands: list[str]
    if row["cancel_requested_at"] is not None or str(row["status"]) in ENTITY_REBUILD_TERMINAL_STATUSES:
        next_commands = [f"rebuild-entities-status {root_arg} --run-id {run_id_arg}"]
    else:
        next_commands = [
            f"rebuild-entities-run-step {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}",
        ]
    return {
        "run_id": run_id,
        "mode": str(row["mode"]),
        "phase": str(row["phase"]),
        "status": str(row["status"]),
        "batch_size": int(row["batch_size"] or 0),
        "counts": {"work_items": counts},
        "progress": {
            "reset_stage": str(row["reset_stage"] or ""),
            "reset_counts": reset_counts,
            "cursor": cursor,
            "documents_scanned": sum(counts.values()),
            "documents_synced": int(totals_row["documents_synced"] or 0) if totals_row is not None else 0,
            "auto_links_created": int(totals_row["auto_links_created"] or 0) if totals_row is not None else 0,
        },
        "graph": entity_graph_counts(connection),
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "cancel_requested_at": row["cancel_requested_at"],
        "last_error": row["error"],
        "next_recommended_commands": next_commands,
    }


def rebuild_entities_start(
    root: Path,
    *,
    document_ids: list[int] | None = None,
    batch_size: int = 500,
    budget_seconds: int | None = None,
) -> dict[str, object]:
    budget = normalize_resumable_step_budget(budget_seconds)
    normalized_batch_size = max(1, min(int(batch_size or 500), 5000))
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="rebuild-entities-start"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="rebuild-entities-start")
            active_row = active_entity_rebuild_run_row(connection)
            if active_row is not None:
                raise RetrieverStructuredError(
                    f"Entity rebuild run {active_row['run_id']} is already active.",
                    entity_rebuild_conflict_payload(
                        root,
                        active_row,
                        message="A resumable entity rebuild is active in this workspace.",
                    ),
                )
            selected_document_ids = entity_rebuild_document_ids(connection, document_ids) if document_ids else []
            full_rebuild = not document_ids
            run_id = new_entity_rebuild_run_id()
            now = utc_now()
            phase = "resetting" if full_rebuild else "planning"
            connection.execute("BEGIN")
            try:
                connection.execute(
                    """
                    INSERT INTO entity_rebuild_runs (
                      run_id, mode, phase, status, document_ids_json, batch_size,
                      reset_stage, reset_counts_json, cursor_json,
                      created_at, started_at, last_heartbeat_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        "full" if full_rebuild else "selected",
                        phase,
                        phase,
                        compact_json_text(selected_document_ids),
                        normalized_batch_size,
                        "document_entities",
                        compact_json_text(
                            {
                                "auto_document_links_deleted": 0,
                                "auto_resolution_keys_deleted": 0,
                                "auto_identifiers_deleted": 0,
                                "auto_entities_deleted": 0,
                            }
                        ),
                        compact_json_text({"selected_offset": 0, "last_document_id": 0, "planned": 0}),
                        now,
                        now,
                        now,
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            row = require_entity_rebuild_run_row(connection, run_id)
            return {"ok": True, "created": True, **entity_rebuild_status_payload(connection, root, row, budget_seconds=budget)}
        finally:
            connection.close()


def rebuild_entities_status(root: Path, *, run_id: str | None = None, budget_seconds: int | None = None) -> dict[str, object]:
    budget = normalize_resumable_step_budget(budget_seconds)
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        row = require_entity_rebuild_run_row(connection, run_id) if run_id else latest_entity_rebuild_run_row(connection)
        if row is None:
            return {
                "ok": True,
                "status": "none",
                "phase": None,
                "run_id": None,
                "counts": {"work_items": {status: 0 for status in ENTITY_REBUILD_ITEM_STATUSES}},
                "progress": {
                    "reset_stage": None,
                    "reset_counts": {},
                    "cursor": {},
                    "documents_scanned": 0,
                    "documents_synced": 0,
                    "auto_links_created": 0,
                },
                "graph": entity_graph_counts(connection),
                "next_recommended_commands": [],
            }
        return {"ok": True, **entity_rebuild_status_payload(connection, root, row, budget_seconds=budget)}
    finally:
        connection.close()


def rebuild_entities_cancel(root: Path, *, run_id: str) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="rebuild-entities-cancel"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            row = require_entity_rebuild_run_row(connection, run_id)
            now = utc_now()
            connection.execute("BEGIN")
            try:
                if str(row["status"]) not in ENTITY_REBUILD_TERMINAL_STATUSES:
                    connection.execute(
                        """
                        UPDATE entity_rebuild_items
                        SET status = 'cancelled',
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            updated_at = ?
                        WHERE run_id = ?
                          AND status IN ('pending', 'leased')
                        """,
                        (now, run_id),
                    )
                    connection.execute(
                        """
                        UPDATE entity_rebuild_runs
                        SET phase = 'canceled',
                            status = 'canceled',
                            cancel_requested_at = COALESCE(cancel_requested_at, ?),
                            completed_at = COALESCE(completed_at, ?),
                            last_heartbeat_at = ?
                        WHERE run_id = ?
                        """,
                        (now, now, now, run_id),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            updated_row = require_entity_rebuild_run_row(connection, run_id)
            return {"ok": True, "cancelled": str(updated_row["status"]) == "canceled", **entity_rebuild_status_payload(connection, root, updated_row)}
        finally:
            connection.close()


def entity_rebuild_reset_stage_delete_sql(stage: str) -> tuple[str, str]:
    if stage == "document_entities":
        return (
            "auto_document_links_deleted",
            """
            DELETE FROM document_entities
            WHERE id IN (
              SELECT id
              FROM document_entities
              WHERE assignment_mode = 'auto'
              ORDER BY id ASC
              LIMIT ?
            )
            """,
        )
    if stage == "resolution_keys":
        return (
            "auto_resolution_keys_deleted",
            """
            DELETE FROM entity_resolution_keys
            WHERE id IN (
              SELECT id
              FROM entity_resolution_keys
              WHERE identifier_id IS NULL
                 OR identifier_id IN (
                   SELECT id
                   FROM entity_identifiers
                   WHERE COALESCE(source_kind, 'auto') = 'auto'
                 )
              ORDER BY id ASC
              LIMIT ?
            )
            """,
        )
    if stage == "identifiers":
        return (
            "auto_identifiers_deleted",
            """
            DELETE FROM entity_identifiers
            WHERE id IN (
              SELECT id
              FROM entity_identifiers
              WHERE COALESCE(source_kind, 'auto') = 'auto'
              ORDER BY id ASC
              LIMIT ?
            )
            """,
        )
    if stage == "entities":
        return (
            "auto_entities_deleted",
            """
            DELETE FROM entities
            WHERE id IN (
              SELECT id
              FROM entities
              WHERE entity_origin IN ('observed', 'identified')
                AND display_name_source = 'auto'
                AND canonical_status = 'active'
                AND NOT EXISTS (
                  SELECT 1
                  FROM entity_identifiers ei
                  WHERE ei.entity_id = entities.id
                    AND COALESCE(ei.source_kind, 'auto') != 'auto'
                )
                AND NOT EXISTS (
                  SELECT 1
                  FROM document_entities de
                  WHERE de.entity_id = entities.id
                    AND de.assignment_mode != 'auto'
                )
                AND NOT EXISTS (
                  SELECT 1
                  FROM entity_resolution_keys erk
                  WHERE erk.entity_id = entities.id
                )
                AND NOT EXISTS (
                  SELECT 1
                  FROM entity_overrides eo
                  WHERE eo.source_entity_id = entities.id
                     OR eo.replacement_entity_id = entities.id
                )
                AND NOT EXISTS (
                  SELECT 1
                  FROM entity_merge_blocks emb
                  WHERE emb.left_entity_id = entities.id
                     OR emb.right_entity_id = entities.id
                )
                AND NOT EXISTS (
                  SELECT 1
                  FROM entities merged_child
                  WHERE merged_child.merged_into_entity_id = entities.id
                )
              ORDER BY id ASC
              LIMIT ?
            )
            """,
        )
    raise RetrieverError(f"Unsupported entity rebuild reset stage: {stage}")


def entity_rebuild_next_reset_stage(stage: str) -> str:
    try:
        index = ENTITY_REBUILD_RESET_STAGES.index(stage)
    except ValueError:
        return "complete"
    return ENTITY_REBUILD_RESET_STAGES[min(index + 1, len(ENTITY_REBUILD_RESET_STAGES) - 1)]


def entity_rebuild_reset_step(connection: sqlite3.Connection, *, run_id: str, batch_size: int) -> dict[str, object]:
    row = require_entity_rebuild_run_row(connection, run_id)
    stage = str(row["reset_stage"] or "document_entities")
    counts = decode_json_text(row["reset_counts_json"], default={}) or {}
    if stage == "complete":
        return {"stage": stage, "deleted": 0, "reset_complete": True}
    count_key, delete_sql = entity_rebuild_reset_stage_delete_sql(stage)
    now = utc_now()
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(delete_sql, (max(1, int(batch_size)),))
        deleted = int(cursor.rowcount or 0)
        if deleted:
            counts[count_key] = int(counts.get(count_key) or 0) + deleted
            next_stage = stage
            next_phase = "resetting"
        else:
            next_stage = entity_rebuild_next_reset_stage(stage)
            next_phase = "planning" if next_stage == "complete" else "resetting"
        connection.execute(
            """
            UPDATE entity_rebuild_runs
            SET reset_stage = ?,
                reset_counts_json = ?,
                phase = ?,
                status = ?,
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND status = 'resetting'
              AND cancel_requested_at IS NULL
            """,
            (next_stage, compact_json_text(counts), next_phase, next_phase, now, run_id),
        )
        connection.commit()
        return {
            "stage": stage,
            "deleted": deleted,
            "reset_complete": next_stage == "complete",
            "next_stage": next_stage,
        }
    except Exception:
        connection.rollback()
        raise


def entity_rebuild_plan_step(connection: sqlite3.Connection, *, run_id: str, batch_size: int) -> dict[str, object]:
    row = require_entity_rebuild_run_row(connection, run_id)
    cursor = decode_json_text(row["cursor_json"], default={}) or {}
    now = utc_now()
    planned_ids: list[int] = []
    if str(row["mode"]) == "selected":
        selected_ids = [int(value) for value in list(decode_json_text(row["document_ids_json"], default=[]) or [])]
        offset = int(cursor.get("selected_offset") or 0)
        planned_ids = selected_ids[offset : offset + max(1, int(batch_size))]
        cursor["selected_offset"] = offset + len(planned_ids)
        planning_complete = int(cursor["selected_offset"]) >= len(selected_ids)
    else:
        last_document_id = int(cursor.get("last_document_id") or 0)
        rows = connection.execute(
            """
            SELECT id
            FROM documents
            WHERE canonical_status != ?
              AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (CANONICAL_STATUS_MERGED, last_document_id, max(1, int(batch_size))),
        ).fetchall()
        planned_ids = [int(item["id"]) for item in rows]
        if planned_ids:
            cursor["last_document_id"] = planned_ids[-1]
        planning_complete = len(planned_ids) < max(1, int(batch_size))

    connection.execute("BEGIN")
    try:
        planned = 0
        for ordinal, document_id in enumerate(planned_ids, start=int(cursor.get("planned") or 0) + 1):
            insert_cursor = connection.execute(
                """
                INSERT OR IGNORE INTO entity_rebuild_items (
                  run_id, document_id, ordinal, status, created_at, updated_at
                ) VALUES (?, ?, ?, 'pending', ?, ?)
                """,
                (run_id, document_id, ordinal, now, now),
            )
            planned += int(insert_cursor.rowcount or 0)
        cursor["planned"] = int(cursor.get("planned") or 0) + planned
        if planning_complete:
            item_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM entity_rebuild_items WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                or 0
            )
            next_phase = "rebuilding" if item_count else "completed"
            completed_at = now if not item_count else None
        else:
            next_phase = "planning"
            completed_at = None
        connection.execute(
            """
            UPDATE entity_rebuild_runs
            SET cursor_json = ?,
                phase = ?,
                status = ?,
                completed_at = COALESCE(completed_at, ?),
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND phase = 'planning'
              AND cancel_requested_at IS NULL
            """,
            (compact_json_text(cursor), next_phase, next_phase, completed_at, now, run_id),
        )
        connection.commit()
        return {"planned": planned, "planning_complete": planning_complete, "next_phase": next_phase}
    except Exception:
        connection.rollback()
        raise


def entity_rebuild_reclaim_stale_items(connection: sqlite3.Connection, *, run_id: str) -> int:
    now_dt = datetime.now(timezone.utc)
    now = format_utc_timestamp(now_dt)
    stale_cutoff = format_utc_timestamp(now_dt - timedelta(seconds=ENTITY_REBUILD_LEASE_SECONDS))
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            """
            UPDATE entity_rebuild_items
            SET status = 'pending',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE run_id = ?
              AND status = 'leased'
              AND lease_expires_at IS NOT NULL
              AND (
                lease_expires_at <= ?
                OR updated_at <= ?
              )
            """,
            (now, run_id, now, stale_cutoff),
        )
        reclaimed = int(cursor.rowcount or 0)
        connection.commit()
        return reclaimed
    except Exception:
        connection.rollback()
        raise


def entity_rebuild_claim_items(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    worker_id: str,
    limit: int,
) -> list[sqlite3.Row]:
    now_dt = datetime.now(timezone.utc)
    now = format_utc_timestamp(now_dt)
    lease_expires_at = lease_expiration_after(ENTITY_REBUILD_LEASE_SECONDS, now=now_dt)
    connection.execute("BEGIN IMMEDIATE")
    try:
        row = require_entity_rebuild_run_row(connection, run_id)
        if str(row["phase"]) != "rebuilding" or row["cancel_requested_at"] is not None:
            connection.rollback()
            return []
        claim_rows = connection.execute(
            """
            SELECT id
            FROM entity_rebuild_items
            WHERE run_id = ?
              AND status = 'pending'
            ORDER BY ordinal ASC, id ASC
            LIMIT ?
            """,
            (run_id, max(1, int(limit))),
        ).fetchall()
        claim_ids = [int(item["id"]) for item in claim_rows]
        if claim_ids:
            placeholders = ",".join("?" for _ in claim_ids)
            connection.execute(
                f"""
                UPDATE entity_rebuild_items
                SET status = 'leased',
                    lease_owner = ?,
                    lease_expires_at = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE run_id = ?
                  AND status = 'pending'
                  AND id IN ({placeholders})
                """,
                (worker_id, lease_expires_at, now, run_id, *claim_ids),
            )
        connection.execute(
            "UPDATE entity_rebuild_runs SET last_heartbeat_at = ? WHERE run_id = ?",
            (now, run_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    if not claim_ids:
        return []
    placeholders = ",".join("?" for _ in claim_ids)
    return connection.execute(
        f"""
        SELECT *
        FROM entity_rebuild_items
        WHERE run_id = ?
          AND lease_owner = ?
          AND status = 'leased'
          AND id IN ({placeholders})
        ORDER BY ordinal ASC, id ASC
        """,
        (run_id, worker_id, *claim_ids),
    ).fetchall()


def entity_rebuild_release_items(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    worker_id: str,
    item_ids: list[int],
    reason: str,
) -> int:
    if not item_ids:
        return 0
    now = utc_now()
    placeholders = ",".join("?" for _ in item_ids)
    connection.execute("BEGIN")
    try:
        cursor = connection.execute(
            f"""
            UPDATE entity_rebuild_items
            SET status = 'pending',
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_error = ?,
                updated_at = ?
            WHERE run_id = ?
              AND lease_owner = ?
              AND status = 'leased'
              AND id IN ({placeholders})
            """,
            (reason, now, run_id, worker_id, *item_ids),
        )
        released = int(cursor.rowcount or 0)
        connection.commit()
        return released
    except Exception:
        connection.rollback()
        raise


def entity_rebuild_mark_item_failed(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    item_id: int,
    worker_id: str,
    message: str,
) -> None:
    now = utc_now()
    connection.execute("BEGIN")
    try:
        connection.execute(
            """
            UPDATE entity_rebuild_items
            SET status = 'failed',
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_error = ?,
                updated_at = ?
            WHERE run_id = ?
              AND id = ?
              AND status = 'leased'
              AND lease_owner = ?
            """,
            (message, now, run_id, item_id, worker_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def entity_rebuild_maybe_complete(connection: sqlite3.Connection, *, run_id: str) -> bool:
    row = require_entity_rebuild_run_row(connection, run_id)
    if str(row["phase"]) != "rebuilding" or row["cancel_requested_at"] is not None:
        return False
    remaining = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM entity_rebuild_items
            WHERE run_id = ?
              AND status IN ('pending', 'leased')
            """,
            (run_id,),
        ).fetchone()[0]
        or 0
    )
    if remaining:
        return False
    now = utc_now()
    connection.execute("BEGIN")
    try:
        connection.execute(
            """
            UPDATE entity_rebuild_runs
            SET phase = 'completed',
                status = 'completed',
                completed_at = COALESCE(completed_at, ?),
                last_heartbeat_at = ?
            WHERE run_id = ?
              AND phase = 'rebuilding'
              AND cancel_requested_at IS NULL
            """,
            (now, now, run_id),
        )
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise


def rebuild_entities_run_step(
    root: Path,
    *,
    run_id: str | None = None,
    budget_seconds: int | None = None,
) -> dict[str, object]:
    budget = normalize_resumable_step_budget(budget_seconds)
    deadline = time.perf_counter() + max(0.1, float(budget) - 0.25)
    paths = workspace_paths(root)
    ensure_layout(paths)
    worker_id = ingest_v2_worker_id("entity-rebuild")
    executed_steps: list[str] = []
    step_results: list[dict[str, object]] = []
    with workspace_entity_rebuild_session(paths, command_name="rebuild-entities-run-step"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            row = require_entity_rebuild_run_row(connection, run_id) if run_id else active_entity_rebuild_run_row(connection)
            if row is None and run_id is None:
                row = latest_entity_rebuild_run_row(connection)
            if row is None:
                raise RetrieverError("No entity rebuild run exists in this workspace.")
            run_id = str(row["run_id"])
            if str(row["status"]) in ENTITY_REBUILD_TERMINAL_STATUSES or row["cancel_requested_at"] is not None:
                return {
                    "ok": True,
                    "executed_steps": [],
                    "reason": "run_terminal",
                    "more_work_remaining": False,
                    "run": entity_rebuild_status_payload(connection, root, row, budget_seconds=budget),
                }
            raise_if_ingest_v2_active(connection, root, command_name="rebuild-entities-run-step")
            while ingest_v2_deadline_remaining_seconds(deadline) >= INGEST_V2_RUN_STEP_MIN_REMAINING_SECONDS:
                row = require_entity_rebuild_run_row(connection, run_id)
                phase = str(row["phase"])
                if phase == "resetting":
                    result = entity_rebuild_reset_step(connection, run_id=run_id, batch_size=int(row["batch_size"] or 500))
                    executed_steps.append("reset")
                    step_results.append(result)
                    continue
                if phase == "planning":
                    result = entity_rebuild_plan_step(connection, run_id=run_id, batch_size=int(row["batch_size"] or 500))
                    executed_steps.append("plan")
                    step_results.append(result)
                    continue
                if phase != "rebuilding":
                    break
                reclaimed = entity_rebuild_reclaim_stale_items(connection, run_id=run_id)
                claim_limit = max(1, min(int(row["batch_size"] or 500), 100))
                claimed_rows = entity_rebuild_claim_items(
                    connection,
                    run_id=run_id,
                    worker_id=worker_id,
                    limit=claim_limit,
                )
                if not claimed_rows:
                    completed = entity_rebuild_maybe_complete(connection, run_id=run_id)
                    executed_steps.append("complete" if completed else "rebuild")
                    step_results.append({"claimed": 0, "committed": 0, "failed": 0, "stale_reclaimed": reclaimed})
                    break
                committed = 0
                failed = 0
                processed_ids: set[int] = set()
                for item_row in claimed_rows:
                    item_id = int(item_row["id"])
                    if ingest_v2_deadline_remaining_seconds(deadline) < INGEST_V2_RUN_STEP_MIN_REMAINING_SECONDS:
                        break
                    processed_ids.add(item_id)
                    document_id = int(item_row["document_id"])
                    connection.execute("BEGIN")
                    try:
                        result = refresh_document_from_occurrences(connection, document_id)
                        document_synced = 1 if result.get("canonical_status") == CANONICAL_STATUS_ACTIVE else 0
                        auto_links_created = 0
                        if document_synced:
                            link_row = connection.execute(
                                """
                                SELECT COUNT(*)
                                FROM document_entities
                                WHERE document_id = ?
                                  AND assignment_mode = 'auto'
                                """,
                                (document_id,),
                            ).fetchone()
                            auto_links_created = int(link_row[0] or 0) if link_row is not None else 0
                        now = utc_now()
                        connection.execute(
                            """
                            UPDATE entity_rebuild_items
                            SET status = 'committed',
                                lease_owner = NULL,
                                lease_expires_at = NULL,
                                document_synced = ?,
                                auto_links_created = ?,
                                last_error = NULL,
                                updated_at = ?
                            WHERE run_id = ?
                              AND id = ?
                              AND status = 'leased'
                              AND lease_owner = ?
                            """,
                            (document_synced, auto_links_created, now, run_id, item_id, worker_id),
                        )
                        connection.execute(
                            "UPDATE entity_rebuild_runs SET last_heartbeat_at = ? WHERE run_id = ?",
                            (now, run_id),
                        )
                        connection.commit()
                        committed += 1
                    except Exception as exc:
                        rollback_open_transaction(connection)
                        entity_rebuild_mark_item_failed(
                            connection,
                            run_id=run_id,
                            item_id=item_id,
                            worker_id=worker_id,
                            message=f"{type(exc).__name__}: {exc}",
                        )
                        failed += 1
                unprocessed_ids = [int(item["id"]) for item in claimed_rows if int(item["id"]) not in processed_ids]
                released = entity_rebuild_release_items(
                    connection,
                    run_id=run_id,
                    worker_id=worker_id,
                    item_ids=unprocessed_ids,
                    reason="Released because run-step budget was nearly exhausted.",
                )
                entity_rebuild_maybe_complete(connection, run_id=run_id)
                executed_steps.append("rebuild")
                step_results.append(
                    {
                        "claimed": len(claimed_rows),
                        "committed": committed,
                        "failed": failed,
                        "released": released,
                        "stale_reclaimed": reclaimed,
                    }
                )
            updated_row = require_entity_rebuild_run_row(connection, run_id)
            reason = "run_terminal" if str(updated_row["status"]) in ENTITY_REBUILD_TERMINAL_STATUSES else "budget_exhausted"
            return {
                "ok": True,
                "executed_steps": executed_steps,
                "step_results": step_results,
                "reason": reason,
                "more_work_remaining": str(updated_row["status"]) not in ENTITY_REBUILD_TERMINAL_STATUSES,
                "run": entity_rebuild_status_payload(connection, root, updated_row, budget_seconds=budget),
                "remaining_budget_seconds": round(ingest_v2_deadline_remaining_seconds(deadline), 3),
            }
        finally:
            connection.close()


def rebuild_entities(
    root: Path,
    *,
    document_ids: list[int] | None = None,
    batch_size: int = 500,
) -> dict[str, object]:
    normalized_batch_size = max(1, min(int(batch_size or 500), 5000))
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="rebuild-entities") as rebuild_session:
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="rebuild-entities")
            raise_if_entity_rebuild_active(connection, root, command_name="rebuild-entities")
            ids_to_rebuild = entity_rebuild_document_ids(connection, document_ids)
            full_rebuild = not document_ids
            reset_counts = {
                "auto_document_links_deleted": 0,
                "auto_resolution_keys_deleted": 0,
                "auto_identifiers_deleted": 0,
                "auto_entities_deleted": 0,
            }
            if full_rebuild:
                connection.execute("BEGIN")
                try:
                    reset_counts = reset_auto_entity_graph(connection)
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise

            documents_synced = 0
            auto_links_created = 0
            for offset in range(0, len(ids_to_rebuild), normalized_batch_size):
                batch_document_ids = ids_to_rebuild[offset : offset + normalized_batch_size]
                connection.execute("BEGIN")
                try:
                    for document_id in batch_document_ids:
                        result = refresh_document_from_occurrences(connection, document_id)
                        if result.get("canonical_status") == CANONICAL_STATUS_ACTIVE:
                            documents_synced += 1
                            row = connection.execute(
                                """
                                SELECT COUNT(*)
                                FROM document_entities
                                WHERE document_id = ?
                                  AND assignment_mode = 'auto'
                                """,
                                (document_id,),
                            ).fetchone()
                            auto_links_created += int(row[0] or 0) if row is not None else 0
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise

            return {
                "status": "ok",
                "session_id": rebuild_session["id"],
                "mode": "full" if full_rebuild else "selected",
                "documents_scanned": len(ids_to_rebuild),
                "documents_synced": documents_synced,
                "auto_links_created": auto_links_created,
                "batch_size": normalized_batch_size,
                **reset_counts,
                **entity_graph_counts(connection),
            }
        finally:
            connection.close()


def extract_vault_filename_custodian_email_from_artifact(raw_value: object) -> str | None:
    text = normalize_entity_text(raw_value)
    marker_index = text.rfind("--")
    if marker_index < 0:
        return None
    suffix = text[marker_index + 2 :]
    dash_index = suffix.find("-")
    candidate = suffix[:dash_index] if dash_index >= 0 else suffix
    return normalize_entity_email(candidate)


def active_entity_id_for_email_value(
    connection: sqlite3.Connection,
    email: str,
    *,
    exclude_entity_id: int | None = None,
) -> int | None:
    normalized_email = normalize_entity_email(email)
    if normalized_email is None:
        return None
    resolution_owner = active_entity_id_for_resolution_key(
        connection,
        {
            "identifier_type": "email",
            "normalized_value": normalized_email,
        },
    )
    if resolution_owner is not None and resolution_owner != exclude_entity_id:
        return resolution_owner
    row = connection.execute(
        """
        SELECT e.id
        FROM entities e
        LEFT JOIN entity_identifiers ei
          ON ei.entity_id = e.id
         AND ei.identifier_type = 'email'
         AND ei.normalized_value = ?
        WHERE e.canonical_status = ?
          AND e.id != ?
          AND (e.primary_email = ? OR ei.id IS NOT NULL)
        ORDER BY CASE WHEN e.primary_email = ? THEN 0 ELSE 1 END, e.id ASC
        LIMIT 1
        """,
        (
            normalized_email,
            ENTITY_STATUS_ACTIVE,
            int(exclude_entity_id or 0),
            normalized_email,
            normalized_email,
        ),
    ).fetchone()
    return int(row["id"]) if row is not None else None


def vault_filename_custodian_evidence_for_entity(
    connection: sqlite3.Connection,
    entity_id: int,
) -> dict[str, object]:
    link_rows = connection.execute(
        """
        SELECT id, document_id, evidence_json
        FROM document_entities
        WHERE entity_id = ?
          AND role = 'custodian'
        ORDER BY id ASC
        """,
        (int(entity_id),),
    ).fetchall()
    document_ids: set[int] = set()
    occurrence_ids: set[int] = set()
    raw_values: set[str] = set()
    for row in link_rows:
        document_ids.add(int(row["document_id"]))
        try:
            evidence = json.loads(str(row["evidence_json"] or "{}"))
        except json.JSONDecodeError:
            evidence = {}
        if not isinstance(evidence, dict):
            evidence = {}
        raw_value = normalize_whitespace(str(evidence.get("raw_value") or ""))
        if raw_value:
            raw_values.add(raw_value)
        occurrence_id = evidence.get("occurrence_id")
        if occurrence_id is not None:
            try:
                occurrence_ids.add(int(occurrence_id))
            except (TypeError, ValueError):
                pass
    if occurrence_ids:
        placeholders = ", ".join("?" for _ in occurrence_ids)
        occurrence_rows = connection.execute(
            f"""
            SELECT id, document_id, custodian
            FROM document_occurrences
            WHERE id IN ({placeholders})
            ORDER BY id ASC
            """,
            tuple(sorted(occurrence_ids)),
        ).fetchall()
        for row in occurrence_rows:
            document_ids.add(int(row["document_id"]))
            raw_value = normalize_whitespace(str(row["custodian"] or ""))
            if raw_value:
                raw_values.add(raw_value)
    return {
        "document_ids": sorted(document_ids),
        "occurrence_ids": sorted(occurrence_ids),
        "raw_values": sorted(raw_values),
        "document_link_count": len(link_rows),
    }


def cleaned_email_from_vault_filename_custodian_values(values: list[object]) -> str | None:
    for value in values:
        text = normalize_whitespace(str(value or ""))
        if not text:
            continue
        vault_parts = parse_google_vault_mbox_basename(Path(text).stem)
        if vault_parts is not None:
            return vault_parts["email"]
    for value in values:
        cleaned_email = extract_vault_filename_custodian_email_from_artifact(value)
        if cleaned_email:
            return cleaned_email
    return None


def vault_filename_custodian_candidates(connection: sqlite3.Connection) -> list[dict[str, object]]:
    if not all(table_exists(connection, table_name) for table_name in ("entities", "entity_identifiers", "document_entities")):
        return []
    rows = connection.execute(
        """
        SELECT e.id, e.entity_type, e.display_name, e.primary_email, e.canonical_status
        FROM entities e
        WHERE e.canonical_status = ?
          AND (
            COALESCE(e.primary_email, '') LIKE '%--%@%'
            OR EXISTS (
              SELECT 1
              FROM entity_identifiers ei
              WHERE ei.entity_id = e.id
                AND ei.identifier_type = 'email'
                AND ei.normalized_value LIKE '%--%@%'
            )
          )
        ORDER BY e.id ASC
        """,
        (ENTITY_STATUS_ACTIVE,),
    ).fetchall()
    candidates: list[dict[str, object]] = []
    for row in rows:
        entity_id = int(row["id"])
        identifier_rows = connection.execute(
            """
            SELECT id, identifier_type, display_value, normalized_value
            FROM entity_identifiers
            WHERE entity_id = ?
            ORDER BY id ASC
            """,
            (entity_id,),
        ).fetchall()
        polluted_email_values = [
            normalize_whitespace(str(value or ""))
            for value in [row["primary_email"], *[item["normalized_value"] for item in identifier_rows if item["identifier_type"] == "email"]]
            if normalize_whitespace(str(value or "")) and "--" in normalize_whitespace(str(value or "")) and "@" in normalize_whitespace(str(value or ""))
        ]
        evidence = vault_filename_custodian_evidence_for_entity(connection, entity_id)
        raw_values = [str(value) for value in evidence["raw_values"]]
        cleaned_email = cleaned_email_from_vault_filename_custodian_values([*raw_values, *polluted_email_values])
        if cleaned_email is None:
            continue
        target_entity_id = active_entity_id_for_email_value(
            connection,
            cleaned_email,
            exclude_entity_id=entity_id,
        )
        action = "merge" if target_entity_id is not None else "rewrite"
        candidates.append(
            {
                "entity_id": entity_id,
                "entity_type": row["entity_type"],
                "display_name": row["display_name"],
                "polluted_email": polluted_email_values[0] if polluted_email_values else None,
                "cleaned_email": cleaned_email,
                "target_entity_id": target_entity_id,
                "action": action,
                "document_ids": evidence["document_ids"],
                "document_link_count": evidence["document_link_count"],
                "raw_values": raw_values,
            }
        )
    return candidates


def update_vault_filename_custodian_occurrences(
    connection: sqlite3.Connection,
    *,
    occurrence_ids: list[int],
    raw_values: list[str],
    cleaned_email: str,
) -> dict[str, object]:
    updated_occurrence_ids: set[int] = set()
    for occurrence_id in sorted({int(item) for item in occurrence_ids}):
        connection.execute(
            """
            UPDATE document_occurrences
            SET custodian = ?, updated_at = ?
            WHERE id = ?
              AND COALESCE(custodian, '') != ?
            """,
            (cleaned_email, utc_now(), occurrence_id, cleaned_email),
        )
        if int(connection.execute("SELECT changes()").fetchone()[0] or 0):
            updated_occurrence_ids.add(occurrence_id)
    normalized_raw_values = [normalize_whitespace(str(value or "")) for value in raw_values if normalize_whitespace(str(value or ""))]
    for raw_value in normalized_raw_values:
        matching_rows = connection.execute(
            """
            SELECT id
            FROM document_occurrences
            WHERE custodian = ?
            ORDER BY id ASC
            """,
            (raw_value,),
        ).fetchall()
        matching_occurrence_ids = {int(row["id"]) for row in matching_rows}
        connection.execute(
            """
            UPDATE document_occurrences
            SET custodian = ?, updated_at = ?
            WHERE custodian = ?
            """,
            (cleaned_email, utc_now(), raw_value),
        )
        if int(connection.execute("SELECT changes()").fetchone()[0] or 0):
            updated_occurrence_ids.update(matching_occurrence_ids)
    affected_document_ids = [
        int(row["document_id"])
        for row in connection.execute(
            f"""
            SELECT DISTINCT document_id
            FROM document_occurrences
            WHERE id IN ({', '.join('?' for _ in updated_occurrence_ids)})
            ORDER BY document_id ASC
            """,
            tuple(sorted(updated_occurrence_ids)),
        ).fetchall()
    ] if updated_occurrence_ids else []
    return {
        "updated_occurrence_ids": sorted(updated_occurrence_ids),
        "affected_document_ids": affected_document_ids,
    }


def delete_artifact_name_identifiers_for_entity(connection: sqlite3.Connection, entity_id: int) -> int:
    rows = connection.execute(
        """
        SELECT id, display_value, normalized_value
        FROM entity_identifiers
        WHERE entity_id = ?
          AND identifier_type = 'name'
        ORDER BY id ASC
        """,
        (int(entity_id),),
    ).fetchall()
    deleted = 0
    for row in rows:
        if not (
            entity_name_identifier_looks_like_export_artifact(row["display_value"])
            or entity_name_identifier_looks_like_export_artifact(row["normalized_value"])
        ):
            continue
        connection.execute("DELETE FROM entity_resolution_keys WHERE identifier_id = ?", (int(row["id"]),))
        connection.execute("DELETE FROM entity_identifiers WHERE id = ?", (int(row["id"]),))
        deleted += int(connection.execute("SELECT changes()").fetchone()[0] or 0)
    return deleted


def rewrite_vault_filename_custodian_entity(
    connection: sqlite3.Connection,
    *,
    entity_id: int,
    cleaned_email: str,
) -> dict[str, object]:
    deleted_artifact_names = delete_artifact_name_identifiers_for_entity(connection, entity_id)
    connection.execute(
        """
        DELETE FROM entity_resolution_keys
        WHERE entity_id = ?
          AND key_type = 'email'
          AND normalized_value LIKE '%--%@%'
        """,
        (int(entity_id),),
    )
    deleted_polluted_resolution_keys = int(connection.execute("SELECT changes()").fetchone()[0] or 0)
    connection.execute(
        """
        UPDATE entity_identifiers
        SET display_value = ?,
            normalized_value = ?,
            is_verified = 1,
            updated_at = ?
        WHERE entity_id = ?
          AND identifier_type = 'email'
          AND normalized_value LIKE '%--%@%'
        """,
        (cleaned_email, cleaned_email, utc_now(), int(entity_id)),
    )
    updated_email_identifiers = int(connection.execute("SELECT changes()").fetchone()[0] or 0)
    if updated_email_identifiers == 0:
        ensure_entity_identifier(
            connection,
            entity_id=int(entity_id),
            identifier={
                "identifier_type": "email",
                "display_value": cleaned_email,
                "normalized_value": cleaned_email,
                "is_verified": 1,
            },
        )
        updated_email_identifiers = 1
    email_identifier_rows = connection.execute(
        """
        SELECT *
        FROM entity_identifiers
        WHERE entity_id = ?
          AND identifier_type = 'email'
          AND normalized_value = ?
        ORDER BY id ASC
        """,
        (int(entity_id), cleaned_email),
    ).fetchall()
    created_resolution_keys = 0
    for identifier_row in email_identifier_rows:
        resolution_key_id = ensure_entity_resolution_key(
            connection,
            entity_id=int(entity_id),
            identifier_id=int(identifier_row["id"]),
            identifier={
                "identifier_type": "email",
                "normalized_value": cleaned_email,
            },
        )
        if resolution_key_id is not None:
            created_resolution_keys += 1
    connection.execute(
        """
        UPDATE entities
        SET entity_type = ?,
            display_name = NULL,
            display_name_source = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (ENTITY_TYPE_UNKNOWN, ENTITY_DISPLAY_SOURCE_AUTO, utc_now(), int(entity_id)),
    )
    recompute_entity_caches(connection, int(entity_id))
    return {
        "updated_email_identifiers": updated_email_identifiers,
        "deleted_artifact_name_identifiers": deleted_artifact_names,
        "deleted_polluted_resolution_keys": deleted_polluted_resolution_keys,
        "created_resolution_keys": created_resolution_keys,
    }


def merge_vault_filename_custodian_entity(
    connection: sqlite3.Connection,
    *,
    loser_entity_id: int,
    survivor_entity_id: int,
) -> dict[str, object]:
    if int(loser_entity_id) == int(survivor_entity_id):
        raise RetrieverError("Cannot merge an entity into itself.")
    affected_document_ids = [
        int(row["document_id"])
        for row in connection.execute(
            """
            SELECT DISTINCT document_id
            FROM document_entities
            WHERE entity_id IN (?, ?)
            ORDER BY document_id ASC
            """,
            (int(loser_entity_id), int(survivor_entity_id)),
        ).fetchall()
    ]
    duplicate_link_count = counted_delete(
        connection,
        count_sql="""
            SELECT COUNT(*)
            FROM document_entities
            WHERE entity_id = ?
              AND EXISTS (
                SELECT 1
                FROM document_entities survivor_link
                WHERE survivor_link.document_id = document_entities.document_id
                  AND survivor_link.role = document_entities.role
                  AND survivor_link.entity_id = ?
              )
        """,
        delete_sql="""
            DELETE FROM document_entities
            WHERE entity_id = ?
              AND EXISTS (
                SELECT 1
                FROM document_entities survivor_link
                WHERE survivor_link.document_id = document_entities.document_id
                  AND survivor_link.role = document_entities.role
                  AND survivor_link.entity_id = ?
              )
        """,
        params=(int(loser_entity_id), int(survivor_entity_id)),
    )
    connection.execute(
        """
        UPDATE document_entities
        SET entity_id = ?, updated_at = ?
        WHERE entity_id = ?
        """,
        (int(survivor_entity_id), utc_now(), int(loser_entity_id)),
    )
    moved_link_count = int(connection.execute("SELECT changes()").fetchone()[0] or 0)
    connection.execute(
        """
        UPDATE entity_overrides
        SET replacement_entity_id = ?, updated_at = ?
        WHERE replacement_entity_id = ?
        """,
        (int(survivor_entity_id), utc_now(), int(loser_entity_id)),
    )
    connection.execute(
        """
        UPDATE entity_overrides
        SET source_entity_id = ?, updated_at = ?
        WHERE source_entity_id = ?
        """,
        (int(survivor_entity_id), utc_now(), int(loser_entity_id)),
    )
    deleted_artifact_names = delete_artifact_name_identifiers_for_entity(connection, loser_entity_id)
    connection.execute("DELETE FROM entity_resolution_keys WHERE entity_id = ?", (int(loser_entity_id),))
    deleted_resolution_keys = int(connection.execute("SELECT changes()").fetchone()[0] or 0)
    connection.execute(
        """
        UPDATE entities
        SET canonical_status = ?, merged_into_entity_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (ENTITY_STATUS_MERGED, int(survivor_entity_id), utc_now(), int(loser_entity_id)),
    )
    created_resolution_keys = ensure_manual_email_resolution_keys(connection, int(survivor_entity_id))
    recompute_entity_caches(connection, int(survivor_entity_id))
    refresh_documents_after_entity_graph_change(connection, affected_document_ids)
    return {
        "affected_document_ids": affected_document_ids,
        "moved_document_links": moved_link_count,
        "deduped_document_links": duplicate_link_count,
        "deleted_artifact_name_identifiers": deleted_artifact_names,
        "deleted_polluted_resolution_keys": deleted_resolution_keys,
        "created_resolution_keys": created_resolution_keys,
    }


def purge_vault_filename_custodians(
    root: Path,
    *,
    apply: bool = False,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    command_name = "purge-vault-filename-custodians"
    with workspace_entity_rebuild_session(paths, command_name=command_name):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name=command_name)
            raise_if_entity_rebuild_active(connection, root, command_name=command_name)
            candidates = vault_filename_custodian_candidates(connection)
            if not apply:
                return {
                    "status": "ok",
                    "dry_run": True,
                    "candidate_count": len(candidates),
                    "candidates": candidates,
                }
            applied: list[dict[str, object]] = []
            connection.execute("BEGIN")
            try:
                for candidate in candidates:
                    entity_id = int(candidate["entity_id"])
                    cleaned_email = str(candidate["cleaned_email"])
                    evidence = vault_filename_custodian_evidence_for_entity(connection, entity_id)
                    occurrence_result = update_vault_filename_custodian_occurrences(
                        connection,
                        occurrence_ids=[int(item) for item in evidence["occurrence_ids"]],
                        raw_values=[str(item) for item in evidence["raw_values"]],
                        cleaned_email=cleaned_email,
                    )
                    target_entity_id = candidate.get("target_entity_id")
                    if target_entity_id is not None:
                        action_result = merge_vault_filename_custodian_entity(
                            connection,
                            loser_entity_id=entity_id,
                            survivor_entity_id=int(target_entity_id),
                        )
                        action = "merged"
                    else:
                        action_result = rewrite_vault_filename_custodian_entity(
                            connection,
                            entity_id=entity_id,
                            cleaned_email=cleaned_email,
                        )
                        refresh_documents_after_entity_graph_change(
                            connection,
                            [
                                *[int(item) for item in evidence["document_ids"]],
                                *[int(item) for item in occurrence_result["affected_document_ids"]],
                            ],
                        )
                        action = "rewritten"
                    applied.append(
                        {
                            **candidate,
                            "action": action,
                            **occurrence_result,
                            **action_result,
                        }
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "dry_run": False,
                "candidate_count": len(candidates),
                "applied_count": len(applied),
                "candidates": applied,
                **entity_graph_counts(connection),
            }
        finally:
            connection.close()


def serialize_entity_identifier(row: sqlite3.Row) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": int(row["id"]),
        "identifier_type": row["identifier_type"],
        "display_value": row["display_value"],
        "normalized_value": row["normalized_value"],
        "is_primary": bool(row["is_primary"]),
        "is_verified": bool(row["is_verified"]),
        "source_kind": row["source_kind"],
    }
    for key in (
        "provider",
        "provider_scope",
        "identifier_name",
        "identifier_scope",
        "normalized_full_name",
        "normalized_sort_name",
    ):
        if payload_has_meaningful_value(row[key]):
            payload[key] = row[key]
    return payload


def entity_identifiers_by_entity_id(
    connection: sqlite3.Connection,
    entity_ids: list[int],
) -> dict[int, list[dict[str, object]]]:
    if not entity_ids:
        return {}
    placeholders = ",".join("?" for _ in entity_ids)
    rows = connection.execute(
        f"""
        SELECT *
        FROM entity_identifiers
        WHERE entity_id IN ({placeholders})
        ORDER BY entity_id ASC, is_primary DESC, is_verified DESC, id ASC
        """,
        tuple(entity_ids),
    ).fetchall()
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["entity_id"])].append(serialize_entity_identifier(row))
    return grouped


def serialize_entity_summary(
    row: sqlite3.Row,
    identifiers: list[dict[str, object]],
) -> dict[str, object]:
    roles = sorted({role for role in str(row["roles"] or "").split(",") if role})
    emails = [
        str(identifier["normalized_value"])
        for identifier in identifiers
        if identifier.get("identifier_type") == "email"
    ]
    phones = [
        str(identifier["display_value"])
        for identifier in identifiers
        if identifier.get("identifier_type") == "phone"
    ]
    names = [
        str(identifier["display_value"])
        for identifier in identifiers
        if identifier.get("identifier_type") == "name"
    ]
    return {
        "id": int(row["id"]),
        "label": entity_display_label_from_row(row),
        "entity_type": row["entity_type"],
        "entity_origin": row["entity_origin"],
        "canonical_status": row["canonical_status"],
        "display_name": row["display_name"],
        "primary_email": row["primary_email"],
        "primary_phone": row["primary_phone"],
        "sort_name": row["sort_name"],
        "document_count": int(row["document_count"] or 0),
        "roles": roles,
        "emails": emails,
        "phones": phones,
        "names": names,
    }


ENTITY_LIST_SORT_EXPRESSIONS = {
    "id": "e.id",
    "label": "LOWER(COALESCE(e.sort_name, e.display_name, e.primary_email, e.primary_phone, ''))",
    "display_name": "LOWER(COALESCE(e.display_name, ''))",
    "primary_email": "LOWER(COALESCE(e.primary_email, ''))",
    "primary_phone": "LOWER(COALESCE(e.primary_phone, ''))",
    "sort_name": "LOWER(COALESCE(e.sort_name, ''))",
    "entity_type": "e.entity_type",
    "entity_origin": "e.entity_origin",
    "canonical_status": "e.canonical_status",
    "entity_status": "e.canonical_status",
    "document_count": "document_count",
}


def normalize_entity_list_sort_field(raw_field: str | None) -> str:
    field_name = normalize_inline_whitespace(str(raw_field or "")).lower()
    if field_name == "status":
        field_name = "entity_status"
    if field_name == "type":
        field_name = "entity_type"
    if field_name == "origin":
        field_name = "entity_origin"
    if field_name == "email":
        field_name = "primary_email"
    if field_name not in ENTITY_LIST_SORT_EXPRESSIONS:
        allowed = ", ".join(sorted(ENTITY_LIST_SORT_EXPRESSIONS))
        raise RetrieverError(f"Unsupported entity sort field: {raw_field}. Sortable fields: {allowed}.")
    return field_name


def normalize_entity_list_sort_specs(
    *,
    sort: str | None = None,
    order: str | None = None,
    sort_specs: list[tuple[str, str]] | None = None,
) -> list[tuple[str, str]]:
    if sort_specs is not None:
        normalized_specs: list[tuple[str, str]] = []
        for raw_field, raw_direction in sort_specs:
            field_name = normalize_entity_list_sort_field(raw_field)
            direction = normalize_inline_whitespace(str(raw_direction or "asc")).lower()
            if direction not in {"asc", "desc"}:
                raise RetrieverError("Sort direction must be 'asc' or 'desc'.")
            normalized_specs.append((field_name, direction))
        return normalized_specs or [("document_count", "desc"), ("label", "asc"), ("id", "asc")]
    if sort:
        direction = normalize_inline_whitespace(str(order or "asc")).lower()
        if direction not in {"asc", "desc"}:
            raise RetrieverError("Sort direction must be 'asc' or 'desc'.")
        return [(normalize_entity_list_sort_field(sort), direction)]
    return [("document_count", "desc"), ("label", "asc"), ("id", "asc")]


def entity_list_sort_spec_text(sort_specs: list[tuple[str, str]]) -> str:
    return ", ".join(f"{field_name} {direction}" for field_name, direction in sort_specs)


def entity_list_order_sql(sort_specs: list[tuple[str, str]]) -> str:
    effective_specs = list(sort_specs)
    if not any(field_name == "id" for field_name, _ in effective_specs):
        effective_specs.append(("id", "asc"))
    parts: list[str] = []
    for field_name, direction in effective_specs:
        expression = ENTITY_LIST_SORT_EXPRESSIONS[normalize_entity_list_sort_field(field_name)]
        parts.append(f"{expression} {direction.upper()}")
    return ", ".join(parts)


def list_entities(
    root: Path,
    *,
    query: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sort: str | None = None,
    order: str | None = None,
    sort_specs: list[tuple[str, str]] | None = None,
    include_ignored: bool = False,
) -> dict[str, object]:
    normalized_limit = max(1, min(int(limit or 50), 200))
    normalized_offset = max(0, int(offset or 0))
    normalized_query = normalize_whitespace(str(query or "")).lower()
    normalized_sort_specs = normalize_entity_list_sort_specs(sort=sort, order=order, sort_specs=sort_specs)
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        where_clauses = ["e.canonical_status != ?"] if include_ignored else ["e.canonical_status = ?"]
        params: list[object] = [ENTITY_STATUS_MERGED if include_ignored else ENTITY_STATUS_ACTIVE]
        if normalized_query:
            like_query = f"%{normalized_query}%"
            where_clauses.append(
                """
                (
                  LOWER(COALESCE(e.display_name, '')) LIKE ?
                  OR LOWER(COALESCE(e.primary_email, '')) LIKE ?
                  OR LOWER(COALESCE(e.primary_phone, '')) LIKE ?
                  OR EXISTS (
                    SELECT 1
                    FROM entity_identifiers ei
                    WHERE ei.entity_id = e.id
                      AND (
                        LOWER(COALESCE(ei.display_value, '')) LIKE ?
                        OR LOWER(COALESCE(ei.normalized_value, '')) LIKE ?
                      )
                  )
                )
                """
            )
            params.extend([like_query, like_query, like_query, like_query, like_query])
        where_sql = " AND ".join(where_clauses)
        total_row = connection.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM entities e
            WHERE {where_sql}
            """,
            tuple(params),
        ).fetchone()
        total_entities = int(total_row["total"] if total_row is not None else 0)
        rows = connection.execute(
            f"""
            SELECT e.*,
                   COUNT(DISTINCT de.document_id) AS document_count,
                   GROUP_CONCAT(DISTINCT de.role) AS roles
            FROM entities e
            LEFT JOIN document_entities de ON de.entity_id = e.id
            WHERE {where_sql}
            GROUP BY e.id
            ORDER BY {entity_list_order_sql(normalized_sort_specs)}
            LIMIT ?
            OFFSET ?
            """,
            (*params, normalized_limit, normalized_offset),
        ).fetchall()
        entity_ids = [int(row["id"]) for row in rows]
        identifiers_by_entity = entity_identifiers_by_entity_id(connection, entity_ids)
        entities = [
            serialize_entity_summary(row, identifiers_by_entity.get(int(row["id"]), []))
            for row in rows
        ]
        return {
            "status": "ok",
            "query": normalized_query,
            "limit": normalized_limit,
            "offset": normalized_offset,
            "total_hits": total_entities,
            "total": total_entities,
            "has_more": normalized_offset + len(entities) < total_entities,
            "next_offset": normalized_offset + normalized_limit if normalized_offset + len(entities) < total_entities else None,
            "sort": normalized_sort_specs[0][0],
            "order": normalized_sort_specs[0][1],
            "sort_spec": entity_list_sort_spec_text(normalized_sort_specs),
            "sort_override": serialize_sort_specs(normalized_sort_specs),
            "include_ignored": include_ignored,
            "entities": entities,
            **entity_graph_counts(connection),
        }
    finally:
        connection.close()


def show_entity(root: Path, entity_id: int, *, document_limit: int = 25) -> dict[str, object]:
    normalized_document_limit = max(1, min(int(document_limit or 25), 200))
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        row = connection.execute(
            """
            SELECT e.*,
                   COUNT(DISTINCT de.document_id) AS document_count,
                   GROUP_CONCAT(DISTINCT de.role) AS roles
            FROM entities e
            LEFT JOIN document_entities de ON de.entity_id = e.id
            WHERE e.id = ?
            GROUP BY e.id
            """,
            (int(entity_id),),
        ).fetchone()
        if row is None:
            raise RetrieverError(f"Unknown entity id: {entity_id}")
        identifiers = entity_identifiers_by_entity_id(connection, [int(entity_id)]).get(int(entity_id), [])
        role_counts = [
            {"role": role_row["role"], "document_count": int(role_row["document_count"] or 0)}
            for role_row in connection.execute(
                """
                SELECT role, COUNT(DISTINCT document_id) AS document_count
                FROM document_entities
                WHERE entity_id = ?
                GROUP BY role
                ORDER BY role ASC
                """,
                (int(entity_id),),
            ).fetchall()
        ]
        document_rows = connection.execute(
            """
            SELECT de.role, de.ordinal, de.assignment_mode, de.observed_title,
                   d.id AS document_id, d.control_number, d.rel_path, d.title, d.date_created
            FROM document_entities de
            JOIN documents d ON d.id = de.document_id
            WHERE de.entity_id = ?
            ORDER BY de.role ASC, de.ordinal ASC, d.id ASC
            LIMIT ?
            """,
            (int(entity_id), normalized_document_limit),
        ).fetchall()
        documents = [
            {
                "document_id": int(document_row["document_id"]),
                "role": document_row["role"],
                "ordinal": int(document_row["ordinal"] or 0),
                "assignment_mode": document_row["assignment_mode"],
                "control_number": document_row["control_number"],
                "rel_path": document_row["rel_path"],
                "title": document_row["title"],
                "date_created": document_row["date_created"],
                "observed_title": document_row["observed_title"],
            }
            for document_row in document_rows
        ]
        return {
            "status": "ok",
            "entity": serialize_entity_summary(row, identifiers),
            "identifiers": identifiers,
            "role_counts": role_counts,
            "documents": documents,
            "document_limit": normalized_document_limit,
        }
    finally:
        connection.close()


def entity_payload_by_id(connection: sqlite3.Connection, entity_id: int) -> dict[str, object]:
    row = connection.execute(
        """
        SELECT e.*,
               COUNT(DISTINCT de.document_id) AS document_count,
               GROUP_CONCAT(DISTINCT de.role) AS roles
        FROM entities e
        LEFT JOIN document_entities de ON de.entity_id = e.id
        WHERE e.id = ?
        GROUP BY e.id
        """,
        (int(entity_id),),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown entity id: {entity_id}")
    identifiers = entity_identifiers_by_entity_id(connection, [int(entity_id)]).get(int(entity_id), [])
    return {
        "entity": serialize_entity_summary(row, identifiers),
        "identifiers": identifiers,
    }


def active_entity_row(connection: sqlite3.Connection, entity_id: int) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT *
        FROM entities
        WHERE id = ?
        """,
        (int(entity_id),),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown entity id: {entity_id}")
    if row["canonical_status"] != ENTITY_STATUS_ACTIVE:
        raise RetrieverError(f"Entity {entity_id} is not active; status is {row['canonical_status']}.")
    return row


def normalize_entity_type_arg(raw_entity_type: object) -> str:
    normalized = normalize_entity_lookup_text(raw_entity_type).replace("-", "_").replace(" ", "_")
    aliases = {
        "shared": ENTITY_TYPE_SHARED_MAILBOX,
        "shared_mailbox": ENTITY_TYPE_SHARED_MAILBOX,
        "system": ENTITY_TYPE_SYSTEM_MAILBOX,
        "system_mailbox": ENTITY_TYPE_SYSTEM_MAILBOX,
        "mailbox": ENTITY_TYPE_SHARED_MAILBOX,
    }
    entity_type = aliases.get(normalized, normalized)
    if entity_type not in ENTITY_TYPES:
        raise RetrieverError(
            f"Unsupported entity type: {raw_entity_type}. Supported types: {', '.join(sorted(ENTITY_TYPES))}."
        )
    return entity_type


def parse_manual_handle_identifier_arg(raw_value: object) -> dict[str, object]:
    parts = [part.strip() for part in str(raw_value or "").split(":", 2)]
    if len(parts) != 3:
        raise RetrieverError("Handle identifiers must use provider:scope:handle, e.g. slack:workspace:@jane.")
    provider = normalize_entity_identifier_name(parts[0])
    provider_scope = normalize_entity_lookup_text(parts[1])
    handle = normalize_entity_handle(parts[2])
    if not provider or not provider_scope or not handle:
        raise RetrieverError("Handle identifiers require non-empty provider, scope, and handle values.")
    return {
        "identifier_type": "handle",
        "display_value": parts[2],
        "normalized_value": handle,
        "provider": provider,
        "provider_scope": provider_scope,
        "source_kind": "manual",
    }


def parse_manual_external_id_identifier_arg(raw_value: object) -> dict[str, object]:
    parts = [part.strip() for part in str(raw_value or "").split(":", 2)]
    if len(parts) == 2:
        raw_name, raw_value_part = parts
        raw_scope = None
    elif len(parts) == 3:
        raw_name, raw_scope, raw_value_part = parts
    else:
        raise RetrieverError("External identifiers must use name:value or name:scope:value.")
    identifier_name = normalize_entity_identifier_name(raw_name)
    identifier_scope = normalize_entity_lookup_text(raw_scope) if raw_scope else None
    normalized_value = normalize_entity_lookup_text(raw_value_part)
    if not identifier_name or not normalized_value:
        raise RetrieverError("External identifiers require non-empty name and value fields.")
    return {
        "identifier_type": "external_id",
        "display_value": raw_value_part,
        "normalized_value": normalized_value,
        "identifier_name": identifier_name,
        "identifier_scope": identifier_scope,
        "source_kind": "manual",
    }


def manual_entity_identifier_payloads(
    *,
    emails: list[str] | None = None,
    phones: list[str] | None = None,
    names: list[str] | None = None,
    handles: list[str] | None = None,
    external_ids: list[str] | None = None,
) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for index, raw_email in enumerate(emails or []):
        email = normalize_entity_email(raw_email)
        if not email:
            raise RetrieverError(f"Invalid entity email identifier: {raw_email}")
        payloads.append(
            {
                "identifier_type": "email",
                "display_value": email,
                "normalized_value": email,
                "is_primary": 1 if index == 0 else 0,
                "is_verified": 1,
                "source_kind": "manual",
            }
        )
    for index, raw_phone in enumerate(phones or []):
        phone = normalize_entity_phone(raw_phone)
        if phone is None:
            raise RetrieverError(f"Invalid entity phone identifier: {raw_phone}")
        payloads.append(
            {
                "identifier_type": "phone",
                "display_value": phone["display_value"],
                "normalized_value": phone["normalized_value"],
                "parsed_phone_json": json.dumps(phone["parsed_phone"], ensure_ascii=True, sort_keys=True),
                "is_primary": 1 if index == 0 else 0,
                "source_kind": "manual",
            }
        )
    for index, raw_name in enumerate(names or []):
        parsed_name = parse_entity_name(raw_name)
        if parsed_name is None:
            raise RetrieverError(f"Invalid entity name identifier: {raw_name}")
        payloads.append(
            {
                "identifier_type": "name",
                "display_value": parsed_name["display_value"],
                "normalized_value": parsed_name["normalized_value"],
                "parsed_name_json": json.dumps(parsed_name["parsed_name"], ensure_ascii=True, sort_keys=True),
                "normalized_full_name": parsed_name["normalized_full_name"],
                "normalized_sort_name": parsed_name["normalized_sort_name"],
                "is_primary": 1 if index == 0 or parsed_name["is_full_name"] else 0,
                "source_kind": "manual",
            }
        )
    payloads.extend(parse_manual_handle_identifier_arg(raw_handle) for raw_handle in handles or [])
    payloads.extend(parse_manual_external_id_identifier_arg(raw_external_id) for raw_external_id in external_ids or [])

    deduped_payloads: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for payload in payloads:
        key = entity_candidate_identifier_key(payload)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped_payloads.append(payload)
    return deduped_payloads


def manual_identifier_error_label(identifier: dict[str, object]) -> str:
    identifier_type = str(identifier.get("identifier_type") or "identifier")
    display_value = str(identifier.get("display_value") or identifier.get("normalized_value") or "")
    if identifier_type == "external_id" and identifier.get("identifier_name"):
        return f"{identifier.get('identifier_name')}:{display_value}"
    if identifier_type == "handle" and identifier.get("provider") and identifier.get("provider_scope"):
        return f"{identifier.get('provider')}:{identifier.get('provider_scope')}:{display_value}"
    return f"{identifier_type}:{display_value}"


def assert_manual_resolution_identifiers_available(
    connection: sqlite3.Connection,
    identifiers: list[dict[str, object]],
    *,
    entity_id: int | None = None,
) -> None:
    for identifier in identifiers:
        if identifier.get("identifier_type") not in {"email", "handle", "external_id"}:
            continue
        owner_entity_id = active_entity_id_for_resolution_key(connection, identifier)
        if owner_entity_id is not None and (entity_id is None or owner_entity_id != int(entity_id)):
            raise RetrieverError(
                f"Identifier {manual_identifier_error_label(identifier)!r} already resolves to entity {owner_entity_id}."
            )


def ensure_manual_entity_identifiers(
    connection: sqlite3.Connection,
    *,
    entity_id: int,
    identifiers: list[dict[str, object]],
) -> dict[str, object]:
    identifier_ids: list[int] = []
    created_count = 0
    existing_count = 0
    for identifier in identifiers:
        before_count = int(connection.execute("SELECT COUNT(*) FROM entity_identifiers").fetchone()[0] or 0)
        identifier_id = ensure_entity_identifier(connection, entity_id=int(entity_id), identifier=identifier)
        after_count = int(connection.execute("SELECT COUNT(*) FROM entity_identifiers").fetchone()[0] or 0)
        identifier_ids.append(identifier_id)
        if after_count > before_count:
            created_count += 1
        else:
            existing_count += 1
    return {
        "identifier_ids": identifier_ids,
        "created_identifier_count": created_count,
        "existing_identifier_count": existing_count,
    }


def create_entity(
    root: Path,
    *,
    entity_type: str = ENTITY_TYPE_PERSON,
    display_name: str | None = None,
    notes: str | None = None,
    emails: list[str] | None = None,
    phones: list[str] | None = None,
    names: list[str] | None = None,
    handles: list[str] | None = None,
    external_ids: list[str] | None = None,
) -> dict[str, object]:
    normalized_entity_type = normalize_entity_type_arg(entity_type)
    normalized_display_name = normalize_whitespace(str(display_name or "")) or None
    normalized_notes = normalize_whitespace(str(notes or "")) or None
    identifiers = manual_entity_identifier_payloads(
        emails=emails,
        phones=phones,
        names=names,
        handles=handles,
        external_ids=external_ids,
    )
    if normalized_display_name is None and not identifiers:
        raise RetrieverError("create-entity requires --display-name or at least one identifier.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="create-entity"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="create-entity")
            raise_if_entity_rebuild_active(connection, root, command_name="create-entity")
            connection.execute("BEGIN")
            try:
                assert_manual_resolution_identifiers_available(connection, identifiers)
                connection.execute(
                    """
                    INSERT INTO entities (
                      entity_type, display_name, notes, display_name_source, entity_origin,
                      canonical_status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_entity_type,
                        normalized_display_name,
                        normalized_notes,
                        ENTITY_DISPLAY_SOURCE_MANUAL if normalized_display_name else ENTITY_DISPLAY_SOURCE_AUTO,
                        ENTITY_ORIGIN_MANUAL,
                        ENTITY_STATUS_ACTIVE,
                        utc_now(),
                        utc_now(),
                    ),
                )
                entity_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                identifier_counts = ensure_manual_entity_identifiers(
                    connection,
                    entity_id=entity_id,
                    identifiers=identifiers,
                )
                created_resolution_keys = ensure_manual_email_resolution_keys(connection, entity_id)
                recompute_entity_caches(connection, entity_id)
                payload = entity_payload_by_id(connection, entity_id)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "created": True,
                "entity_id": entity_id,
                "created_resolution_keys": created_resolution_keys,
                **identifier_counts,
                **payload,
            }
        finally:
            connection.close()


def edit_entity(
    root: Path,
    entity_id: int,
    *,
    entity_type: str | None = None,
    display_name: str | None = None,
    clear_display_name: bool = False,
    notes: str | None = None,
    clear_notes: bool = False,
    add_emails: list[str] | None = None,
    add_phones: list[str] | None = None,
    add_names: list[str] | None = None,
    add_handles: list[str] | None = None,
    add_external_ids: list[str] | None = None,
) -> dict[str, object]:
    if clear_display_name and display_name is not None:
        raise RetrieverError("Use either --display-name or --clear-display-name, not both.")
    if clear_notes and notes is not None:
        raise RetrieverError("Use either --notes or --clear-notes, not both.")
    normalized_entity_type = normalize_entity_type_arg(entity_type) if entity_type is not None else None
    normalized_display_name = normalize_whitespace(str(display_name or "")) if display_name is not None else None
    normalized_notes = normalize_whitespace(str(notes or "")) if notes is not None else None
    identifiers = manual_entity_identifier_payloads(
        emails=add_emails,
        phones=add_phones,
        names=add_names,
        handles=add_handles,
        external_ids=add_external_ids,
    )
    has_entity_update = (
        normalized_entity_type is not None
        or display_name is not None
        or clear_display_name
        or notes is not None
        or clear_notes
    )
    if not has_entity_update and not identifiers:
        raise RetrieverError("edit-entity requires an entity field change or at least one added identifier.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="edit-entity"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="edit-entity")
            raise_if_entity_rebuild_active(connection, root, command_name="edit-entity")
            active_entity_row(connection, entity_id)
            affected_document_ids = [
                int(row["document_id"])
                for row in connection.execute(
                    """
                    SELECT DISTINCT document_id
                    FROM document_entities
                    WHERE entity_id = ?
                    ORDER BY document_id ASC
                    """,
                    (int(entity_id),),
                ).fetchall()
            ]
            connection.execute("BEGIN")
            try:
                assert_manual_resolution_identifiers_available(connection, identifiers, entity_id=int(entity_id))
                update_values: dict[str, object] = {
                    "entity_origin": ENTITY_ORIGIN_MANUAL,
                    "updated_at": utc_now(),
                }
                if normalized_entity_type is not None:
                    update_values["entity_type"] = normalized_entity_type
                if display_name is not None:
                    update_values["display_name"] = normalized_display_name or None
                    update_values["display_name_source"] = ENTITY_DISPLAY_SOURCE_MANUAL
                elif clear_display_name:
                    update_values["display_name"] = None
                    update_values["display_name_source"] = ENTITY_DISPLAY_SOURCE_AUTO
                if notes is not None:
                    update_values["notes"] = normalized_notes or None
                elif clear_notes:
                    update_values["notes"] = None
                set_clause = ", ".join(f"{quote_identifier(column)} = ?" for column in update_values)
                connection.execute(
                    f"""
                    UPDATE entities
                    SET {set_clause}
                    WHERE id = ?
                    """,
                    [*update_values.values(), int(entity_id)],
                )
                identifier_counts = ensure_manual_entity_identifiers(
                    connection,
                    entity_id=int(entity_id),
                    identifiers=identifiers,
                )
                created_resolution_keys = ensure_manual_email_resolution_keys(connection, int(entity_id))
                recompute_entity_caches(connection, int(entity_id))
                refresh_documents_after_entity_graph_change(connection, affected_document_ids)
                payload = entity_payload_by_id(connection, int(entity_id))
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "entity_id": int(entity_id),
                "created_resolution_keys": created_resolution_keys,
                "affected_document_ids": affected_document_ids,
                **identifier_counts,
                **payload,
            }
        finally:
            connection.close()


def entity_pair_key(left_entity_id: int, right_entity_id: int) -> tuple[int, int]:
    left = int(left_entity_id)
    right = int(right_entity_id)
    if left == right:
        raise RetrieverError("Entity pair requires two distinct entity ids.")
    return (left, right) if left < right else (right, left)


def entity_merge_block_exists(connection: sqlite3.Connection, left_entity_id: int, right_entity_id: int) -> bool:
    left, right = entity_pair_key(left_entity_id, right_entity_id)
    row = connection.execute(
        """
        SELECT 1
        FROM entity_merge_blocks
        WHERE left_entity_id = ?
          AND right_entity_id = ?
        LIMIT 1
        """,
        (left, right),
    ).fetchone()
    return row is not None


def block_entity_merge(
    root: Path,
    left_entity_id: int,
    right_entity_id: int,
    *,
    reason: str | None = None,
) -> dict[str, object]:
    left, right = entity_pair_key(left_entity_id, right_entity_id)
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        raise_if_ingest_v2_active(connection, root, command_name="block-entity-merge")
        active_entity_row(connection, left)
        active_entity_row(connection, right)
        normalized_reason = normalize_whitespace(str(reason or "")) or None
        connection.execute("BEGIN")
        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO entity_merge_blocks (
                  left_entity_id, right_entity_id, reason, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (left, right, normalized_reason, utc_now()),
            )
            inserted = connection.execute("SELECT changes()").fetchone()[0]
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {
            "status": "ok",
            "left_entity_id": left,
            "right_entity_id": right,
            "created": bool(inserted),
            "reason": normalized_reason,
        }
    finally:
        connection.close()


def entity_profile_from_summary(entity: dict[str, object], identifiers: list[dict[str, object]]) -> dict[str, object]:
    emails = {
        normalize_entity_email(identifier.get("normalized_value") or identifier.get("display_value"))
        for identifier in identifiers
        if identifier.get("identifier_type") == "email"
    }
    emails = {email for email in emails if email}
    email_locals = {email.split("@", 1)[0] for email in emails}
    email_domains = {email.split("@", 1)[1] for email in emails}
    full_names = {
        normalize_entity_lookup_text(identifier.get("normalized_full_name") or identifier.get("display_value"))
        for identifier in identifiers
        if identifier.get("identifier_type") == "name"
    }
    full_names = {name for name in full_names if name and len(name.split()) >= 2}
    sort_names = {
        normalize_entity_lookup_text(identifier.get("normalized_sort_name"))
        for identifier in identifiers
        if identifier.get("identifier_type") == "name"
    }
    sort_names = {name for name in sort_names if name}
    phones = {
        normalize_entity_lookup_text(identifier.get("normalized_value") or identifier.get("display_value"))
        for identifier in identifiers
        if identifier.get("identifier_type") == "phone"
    }
    phones = {phone for phone in phones if phone}
    handles = {
        (
            normalize_entity_lookup_text(identifier.get("provider")),
            normalize_entity_lookup_text(identifier.get("provider_scope")),
            normalize_entity_lookup_text(identifier.get("normalized_value")),
        )
        for identifier in identifiers
        if identifier.get("identifier_type") == "handle"
    }
    handles = {handle for handle in handles if handle[2]}
    label = normalize_entity_lookup_text(entity.get("label"))
    display_name = normalize_entity_lookup_text(entity.get("display_name"))
    if display_name and len(display_name.split()) >= 2:
        full_names.add(display_name)
    if label and len(label.split()) >= 2 and "@" not in label:
        full_names.add(label)
    return {
        "emails": emails,
        "email_locals": email_locals,
        "email_domains": email_domains,
        "full_names": full_names,
        "sort_names": sort_names,
        "phones": phones,
        "handles": handles,
        "label": label,
    }


def name_initial_family_pairs(full_names: set[str]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for name in full_names:
        parts = name.split()
        if len(parts) < 2:
            continue
        pairs.add((parts[-1], parts[0][:1]))
    return pairs


def entity_similarity_reasons(left_profile: dict[str, object], right_profile: dict[str, object]) -> list[dict[str, object]]:
    reasons: list[dict[str, object]] = []
    left_emails = set(left_profile["emails"])  # type: ignore[arg-type]
    right_emails = set(right_profile["emails"])  # type: ignore[arg-type]
    if left_emails & right_emails:
        reasons.append({"kind": "exact_email", "score": 100, "value": sorted(left_emails & right_emails)[0]})
    left_full_names = set(left_profile["full_names"])  # type: ignore[arg-type]
    right_full_names = set(right_profile["full_names"])  # type: ignore[arg-type]
    if left_full_names & right_full_names:
        reasons.append({"kind": "exact_full_name", "score": 80, "value": sorted(left_full_names & right_full_names)[0]})
    left_sort_names = set(left_profile["sort_names"])  # type: ignore[arg-type]
    right_sort_names = set(right_profile["sort_names"])  # type: ignore[arg-type]
    if left_sort_names & right_sort_names:
        reasons.append({"kind": "exact_sort_name", "score": 80, "value": sorted(left_sort_names & right_sort_names)[0]})
    left_phones = set(left_profile["phones"])  # type: ignore[arg-type]
    right_phones = set(right_profile["phones"])  # type: ignore[arg-type]
    if left_phones & right_phones:
        reasons.append({"kind": "same_phone", "score": 70, "value": sorted(left_phones & right_phones)[0]})
    left_handles = set(left_profile["handles"])  # type: ignore[arg-type]
    right_handles = set(right_profile["handles"])  # type: ignore[arg-type]
    shared_handles = left_handles & right_handles
    if shared_handles:
        handle = sorted(shared_handles)[0]
        reasons.append({"kind": "same_handle", "score": 65, "value": handle[2]})
    left_initial_pairs = name_initial_family_pairs(left_full_names)
    right_initial_pairs = name_initial_family_pairs(right_full_names)
    if left_initial_pairs & right_initial_pairs:
        family, initial = sorted(left_initial_pairs & right_initial_pairs)[0]
        reasons.append({"kind": "family_name_and_initial", "score": 45, "value": f"{family}, {initial}"})
    left_email_locals = set(left_profile["email_locals"])  # type: ignore[arg-type]
    right_email_locals = set(right_profile["email_locals"])  # type: ignore[arg-type]
    if left_email_locals & right_email_locals:
        reasons.append({"kind": "same_email_local_part", "score": 40, "value": sorted(left_email_locals & right_email_locals)[0]})
    left_email_domains = set(left_profile["email_domains"])  # type: ignore[arg-type]
    right_email_domains = set(right_profile["email_domains"])  # type: ignore[arg-type]
    if left_email_domains & right_email_domains and (left_full_names & right_full_names or left_initial_pairs & right_initial_pairs):
        reasons.append({"kind": "same_email_domain", "score": 20, "value": sorted(left_email_domains & right_email_domains)[0]})
    return reasons


def load_active_entity_summaries(
    connection: sqlite3.Connection,
    *,
    query: str | None = None,
    limit: int = 500,
) -> tuple[list[dict[str, object]], dict[int, list[dict[str, object]]]]:
    normalized_limit = max(1, min(int(limit or 500), 5000))
    normalized_query = normalize_whitespace(str(query or "")).lower()
    where_clauses = ["e.canonical_status = ?"]
    params: list[object] = [ENTITY_STATUS_ACTIVE]
    if normalized_query:
        like_query = f"%{normalized_query}%"
        where_clauses.append(
            """
            (
              LOWER(COALESCE(e.display_name, '')) LIKE ?
              OR LOWER(COALESCE(e.primary_email, '')) LIKE ?
              OR LOWER(COALESCE(e.primary_phone, '')) LIKE ?
              OR EXISTS (
                SELECT 1
                FROM entity_identifiers ei
                WHERE ei.entity_id = e.id
                  AND (
                    LOWER(COALESCE(ei.display_value, '')) LIKE ?
                    OR LOWER(COALESCE(ei.normalized_value, '')) LIKE ?
                  )
              )
            )
            """
        )
        params.extend([like_query, like_query, like_query, like_query, like_query])
    rows = connection.execute(
        f"""
        SELECT e.*,
               COUNT(DISTINCT de.document_id) AS document_count,
               GROUP_CONCAT(DISTINCT de.role) AS roles
        FROM entities e
        LEFT JOIN document_entities de ON de.entity_id = e.id
        WHERE {' AND '.join(where_clauses)}
        GROUP BY e.id
        ORDER BY document_count DESC,
                 COALESCE(e.sort_name, e.display_name, e.primary_email, e.primary_phone, '') ASC,
                 e.id ASC
        LIMIT ?
        """,
        (*params, normalized_limit),
    ).fetchall()
    entity_ids = [int(row["id"]) for row in rows]
    identifiers_by_entity = entity_identifiers_by_entity_id(connection, entity_ids)
    return (
        [serialize_entity_summary(row, identifiers_by_entity.get(int(row["id"]), [])) for row in rows],
        identifiers_by_entity,
    )


def similar_entities(root: Path, entity_id: int, *, limit: int = 25) -> dict[str, object]:
    normalized_limit = max(1, min(int(limit or 25), 200))
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        active_entity_row(connection, entity_id)
        entities, identifiers_by_entity = load_active_entity_summaries(connection, limit=5000)
        summaries_by_id = {int(entity["id"]): entity for entity in entities}
        target = summaries_by_id.get(int(entity_id))
        if target is None:
            raise RetrieverError(f"Entity {entity_id} is not active.")
        profiles = {
            int(entity["id"]): entity_profile_from_summary(
                entity,
                identifiers_by_entity.get(int(entity["id"]), []),
            )
            for entity in entities
        }
        target_profile = profiles[int(entity_id)]
        suggestions: list[dict[str, object]] = []
        for candidate in entities:
            candidate_id = int(candidate["id"])
            if candidate_id == int(entity_id):
                continue
            if entity_merge_block_exists(connection, int(entity_id), candidate_id):
                continue
            reasons = entity_similarity_reasons(target_profile, profiles[candidate_id])
            if not reasons:
                continue
            score = sum(int(reason["score"]) for reason in reasons)
            suggestions.append(
                {
                    "entity": candidate,
                    "score": score,
                    "reasons": reasons,
                }
            )
        suggestions.sort(
            key=lambda item: (
                -int(item["score"]),
                -int(item["entity"]["document_count"]),  # type: ignore[index]
                str(item["entity"]["label"]),  # type: ignore[index]
                int(item["entity"]["id"]),  # type: ignore[index]
            )
        )
        return {
            "status": "ok",
            "entity": target,
            "suggestions": suggestions[:normalized_limit],
            "limit": normalized_limit,
        }
    finally:
        connection.close()


def resolution_key_matches_row(connection: sqlite3.Connection, row: sqlite3.Row, *, exclude_id: int | None = None) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM entity_resolution_keys
        WHERE key_type = ?
          AND COALESCE(provider, '') = COALESCE(?, '')
          AND COALESCE(provider_scope, '') = COALESCE(?, '')
          AND COALESCE(identifier_name, '') = COALESCE(?, '')
          AND COALESCE(identifier_scope, '') = COALESCE(?, '')
          AND normalized_value = ?
          AND (? IS NULL OR id != ?)
        ORDER BY id ASC
        LIMIT 1
        """,
        (
            row["key_type"],
            row["provider"],
            row["provider_scope"],
            row["identifier_name"],
            row["identifier_scope"],
            row["normalized_value"],
            exclude_id,
            exclude_id,
        ),
    ).fetchone()


def matching_survivor_identifier_id(
    connection: sqlite3.Connection,
    *,
    survivor_entity_id: int,
    identifier_row: sqlite3.Row,
) -> int | None:
    row = connection.execute(
        """
        SELECT id
        FROM entity_identifiers
        WHERE entity_id = ?
          AND identifier_type = ?
          AND normalized_value = ?
          AND COALESCE(provider, '') = COALESCE(?, '')
          AND COALESCE(provider_scope, '') = COALESCE(?, '')
          AND COALESCE(identifier_name, '') = COALESCE(?, '')
          AND COALESCE(identifier_scope, '') = COALESCE(?, '')
        ORDER BY id ASC
        LIMIT 1
        """,
        (
            survivor_entity_id,
            identifier_row["identifier_type"],
            identifier_row["normalized_value"],
            identifier_row["provider"],
            identifier_row["provider_scope"],
            identifier_row["identifier_name"],
            identifier_row["identifier_scope"],
        ),
    ).fetchone()
    return int(row["id"]) if row is not None else None


def identifier_payload_from_row(row: sqlite3.Row) -> dict[str, object]:
    return {
        "identifier_type": row["identifier_type"],
        "display_value": row["display_value"],
        "normalized_value": row["normalized_value"],
        "provider": row["provider"],
        "provider_scope": row["provider_scope"],
        "identifier_name": row["identifier_name"],
        "identifier_scope": row["identifier_scope"],
        "parsed_name_json": row["parsed_name_json"],
        "parsed_phone_json": row["parsed_phone_json"],
        "normalized_full_name": row["normalized_full_name"],
        "normalized_sort_name": row["normalized_sort_name"],
        "is_primary": row["is_primary"],
        "is_verified": row["is_verified"],
        "source_kind": row["source_kind"],
    }


def move_loser_identifiers_to_survivor(
    connection: sqlite3.Connection,
    *,
    loser_entity_id: int,
    survivor_entity_id: int,
) -> dict[str, int]:
    moved_identifiers = 0
    deduped_identifiers = 0
    moved_resolution_keys = 0
    deleted_resolution_keys = 0
    identifier_rows = connection.execute(
        """
        SELECT *
        FROM entity_identifiers
        WHERE entity_id = ?
        ORDER BY id ASC
        """,
        (loser_entity_id,),
    ).fetchall()
    for identifier_row in identifier_rows:
        loser_identifier_id = int(identifier_row["id"])
        target_identifier_id = matching_survivor_identifier_id(
            connection,
            survivor_entity_id=survivor_entity_id,
            identifier_row=identifier_row,
        )
        if target_identifier_id is None:
            connection.execute(
                """
                UPDATE entity_identifiers
                SET entity_id = ?, source_kind = 'manual', updated_at = ?
                WHERE id = ?
                """,
                (survivor_entity_id, utc_now(), loser_identifier_id),
            )
            target_identifier_id = loser_identifier_id
            moved_identifiers += 1
        else:
            connection.execute(
                """
                UPDATE entity_identifiers
                SET source_kind = 'manual', updated_at = ?
                WHERE id = ?
                """,
                (utc_now(), target_identifier_id),
            )
            deduped_identifiers += 1

        key_rows = connection.execute(
            """
            SELECT *
            FROM entity_resolution_keys
            WHERE identifier_id = ?
            ORDER BY id ASC
            """,
            (loser_identifier_id,),
        ).fetchall()
        for key_row in key_rows:
            existing_key = resolution_key_matches_row(connection, key_row, exclude_id=int(key_row["id"]))
            if existing_key is not None:
                connection.execute("DELETE FROM entity_resolution_keys WHERE id = ?", (int(key_row["id"]),))
                deleted_resolution_keys += 1
            else:
                connection.execute(
                    """
                    UPDATE entity_resolution_keys
                    SET entity_id = ?, identifier_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (survivor_entity_id, target_identifier_id, utc_now(), int(key_row["id"])),
                )
                moved_resolution_keys += 1

        if target_identifier_id != loser_identifier_id:
            connection.execute("DELETE FROM entity_identifiers WHERE id = ?", (loser_identifier_id,))

    for key_row in connection.execute(
        """
        SELECT *
        FROM entity_resolution_keys
        WHERE entity_id = ?
        ORDER BY id ASC
        """,
        (loser_entity_id,),
    ).fetchall():
        existing_key = resolution_key_matches_row(connection, key_row, exclude_id=int(key_row["id"]))
        if existing_key is not None:
            connection.execute("DELETE FROM entity_resolution_keys WHERE id = ?", (int(key_row["id"]),))
            deleted_resolution_keys += 1
        else:
            connection.execute(
                """
                UPDATE entity_resolution_keys
                SET entity_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (survivor_entity_id, utc_now(), int(key_row["id"])),
            )
            moved_resolution_keys += 1

    connection.execute(
        """
        UPDATE entity_identifiers
        SET source_kind = 'manual', updated_at = ?
        WHERE entity_id = ?
        """,
        (utc_now(), survivor_entity_id),
    )
    return {
        "moved_identifiers": moved_identifiers,
        "deduped_identifiers": deduped_identifiers,
        "moved_resolution_keys": moved_resolution_keys,
        "deleted_resolution_keys": deleted_resolution_keys,
    }


def ensure_manual_email_resolution_keys(connection: sqlite3.Connection, entity_id: int) -> int:
    created = 0
    for identifier_row in connection.execute(
        """
        SELECT *
        FROM entity_identifiers
        WHERE entity_id = ?
          AND identifier_type IN ('email', 'handle', 'external_id')
        ORDER BY id ASC
        """,
        (entity_id,),
    ).fetchall():
        before = connection.execute("SELECT COUNT(*) FROM entity_resolution_keys").fetchone()[0]
        ensure_entity_resolution_key(
            connection,
            entity_id=entity_id,
            identifier_id=int(identifier_row["id"]),
            identifier=identifier_payload_from_row(identifier_row),
        )
        after = connection.execute("SELECT COUNT(*) FROM entity_resolution_keys").fetchone()[0]
        created += max(0, int(after or 0) - int(before or 0))
    return created


def refresh_documents_after_entity_graph_change(connection: sqlite3.Connection, document_ids: list[int]) -> None:
    for document_id in sorted(set(int(item) for item in document_ids)):
        rebuild_document_entity_caches(connection, document_id)
        refresh_documents_fts_row(connection, document_id)


def merge_entities(
    root: Path,
    source_entity_id: int,
    target_entity_id: int,
    *,
    force: bool = False,
    reason: str | None = None,
) -> dict[str, object]:
    loser_entity_id = int(source_entity_id)
    survivor_entity_id = int(target_entity_id)
    if loser_entity_id == survivor_entity_id:
        raise RetrieverError("Cannot merge an entity into itself.")
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="merge-entities"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="merge-entities")
            raise_if_entity_rebuild_active(connection, root, command_name="merge-entities")
            loser_row = active_entity_row(connection, loser_entity_id)
            survivor_row = active_entity_row(connection, survivor_entity_id)
            if entity_merge_block_exists(connection, loser_entity_id, survivor_entity_id) and not force:
                raise RetrieverError("These entities have a merge block. Pass --force to merge anyway.")
            affected_document_ids = [
                int(row["document_id"])
                for row in connection.execute(
                    """
                    SELECT DISTINCT document_id
                    FROM document_entities
                    WHERE entity_id IN (?, ?)
                    ORDER BY document_id ASC
                    """,
                    (loser_entity_id, survivor_entity_id),
                ).fetchall()
            ]
            connection.execute("BEGIN")
            try:
                identifier_counts = move_loser_identifiers_to_survivor(
                    connection,
                    loser_entity_id=loser_entity_id,
                    survivor_entity_id=survivor_entity_id,
                )
                duplicate_link_count = counted_delete(
                    connection,
                    count_sql="""
                        SELECT COUNT(*)
                        FROM document_entities
                        WHERE entity_id = ?
                          AND EXISTS (
                            SELECT 1
                            FROM document_entities survivor_link
                            WHERE survivor_link.document_id = document_entities.document_id
                              AND survivor_link.role = document_entities.role
                              AND survivor_link.entity_id = ?
                          )
                    """,
                    delete_sql="""
                        DELETE FROM document_entities
                        WHERE entity_id = ?
                          AND EXISTS (
                            SELECT 1
                            FROM document_entities survivor_link
                            WHERE survivor_link.document_id = document_entities.document_id
                              AND survivor_link.role = document_entities.role
                              AND survivor_link.entity_id = ?
                          )
                    """,
                    params=(loser_entity_id, survivor_entity_id),
                )
                connection.execute(
                    """
                    UPDATE document_entities
                    SET entity_id = ?, updated_at = ?
                    WHERE entity_id = ?
                    """,
                    (survivor_entity_id, utc_now(), loser_entity_id),
                )
                moved_link_count = int(connection.execute("SELECT changes()").fetchone()[0] or 0)
                connection.execute(
                    """
                    UPDATE entity_overrides
                    SET replacement_entity_id = ?, updated_at = ?
                    WHERE replacement_entity_id = ?
                    """,
                    (survivor_entity_id, utc_now(), loser_entity_id),
                )
                connection.execute(
                    """
                    UPDATE entity_overrides
                    SET source_entity_id = ?, updated_at = ?
                    WHERE source_entity_id = ?
                    """,
                    (survivor_entity_id, utc_now(), loser_entity_id),
                )
                connection.execute(
                    """
                    UPDATE entities
                    SET entity_origin = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (ENTITY_ORIGIN_MANUAL, utc_now(), survivor_entity_id),
                )
                connection.execute(
                    """
                    UPDATE entities
                    SET canonical_status = ?, merged_into_entity_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (ENTITY_STATUS_MERGED, survivor_entity_id, utc_now(), loser_entity_id),
                )
                created_resolution_keys = ensure_manual_email_resolution_keys(connection, survivor_entity_id)
                recompute_entity_caches(connection, survivor_entity_id)
                refresh_documents_after_entity_graph_change(connection, affected_document_ids)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "source_entity_id": loser_entity_id,
                "target_entity_id": survivor_entity_id,
                "source_label": entity_display_label_from_row(loser_row),
                "target_label": entity_display_label_from_row(survivor_row),
                "force": bool(force),
                "reason": normalize_whitespace(str(reason or "")) or None,
                "affected_document_ids": affected_document_ids,
                "moved_document_links": moved_link_count,
                "deduped_document_links": duplicate_link_count,
                "created_resolution_keys": created_resolution_keys,
                **identifier_counts,
            }
        finally:
            connection.close()


def ignore_override_keys_for_entity(connection: sqlite3.Connection, entity_id: int) -> list[dict[str, object]]:
    keys: dict[tuple[str | None, str | None, str | None], dict[str, object]] = {}
    rows = connection.execute(
        """
        SELECT role, evidence_json
        FROM document_entities
        WHERE entity_id = ?
        ORDER BY id ASC
        """,
        (int(entity_id),),
    ).fetchall()
    for row in rows:
        try:
            evidence = json.loads(str(row["evidence_json"] or "{}"))
        except json.JSONDecodeError:
            evidence = {}
        if not isinstance(evidence, dict):
            evidence = {}
        role = str(row["role"] or "")
        raw_value = normalize_whitespace(str(evidence.get("raw_value") or ""))
        candidate_key = normalize_whitespace(str(evidence.get("normalized_candidate_key") or ""))
        if candidate_key:
            keys[(role, candidate_key, raw_value or None)] = {
                "role": role,
                "normalized_candidate_key": candidate_key,
                "source_hint": raw_value or None,
            }
        if raw_value:
            for candidate in parse_entity_candidates(raw_value, role=role):
                parsed_key = normalize_whitespace(str(candidate.get("normalized_candidate_key") or ""))
                if parsed_key:
                    keys[(role, parsed_key, raw_value)] = {
                        "role": role,
                        "normalized_candidate_key": parsed_key,
                        "source_hint": raw_value,
                    }
    return list(keys.values())


def ignore_entity(root: Path, entity_id: int, *, reason: str | None = None) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="ignore-entity"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="ignore-entity")
            raise_if_entity_rebuild_active(connection, root, command_name="ignore-entity")
            entity_row = active_entity_row(connection, entity_id)
            affected_document_ids = [
                int(row["document_id"])
                for row in connection.execute(
                    """
                    SELECT DISTINCT document_id
                    FROM document_entities
                    WHERE entity_id = ?
                    ORDER BY document_id ASC
                    """,
                    (int(entity_id),),
                ).fetchall()
            ]
            override_keys = ignore_override_keys_for_entity(connection, int(entity_id))
            normalized_reason = normalize_whitespace(str(reason or "")) or None
            connection.execute("BEGIN")
            try:
                for override in override_keys:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO entity_overrides (
                          scope_type, scope_id, role, source_entity_id,
                          normalized_candidate_key, replacement_entity_id,
                          override_effect, source_hint, reason, created_at, updated_at
                        ) VALUES ('global', NULL, ?, ?, ?, NULL, 'ignore', ?, ?, ?, ?)
                        """,
                        (
                            override.get("role"),
                            int(entity_id),
                            override.get("normalized_candidate_key"),
                            override.get("source_hint"),
                            normalized_reason,
                            utc_now(),
                            utc_now(),
                        ),
                    )
                resolution_keys_deleted = counted_delete(
                    connection,
                    count_sql="SELECT COUNT(*) FROM entity_resolution_keys WHERE entity_id = ?",
                    delete_sql="DELETE FROM entity_resolution_keys WHERE entity_id = ?",
                    params=(int(entity_id),),
                )
                document_links_deleted = counted_delete(
                    connection,
                    count_sql="SELECT COUNT(*) FROM document_entities WHERE entity_id = ?",
                    delete_sql="DELETE FROM document_entities WHERE entity_id = ?",
                    params=(int(entity_id),),
                )
                connection.execute(
                    """
                    UPDATE entities
                    SET canonical_status = ?, merged_into_entity_id = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (ENTITY_STATUS_IGNORED, utc_now(), int(entity_id)),
                )
                refresh_documents_after_entity_graph_change(connection, affected_document_ids)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "entity_id": int(entity_id),
                "label": entity_display_label_from_row(entity_row),
                "reason": normalized_reason,
                "affected_document_ids": affected_document_ids,
                "override_count": len(override_keys),
                "document_links_deleted": document_links_deleted,
                "resolution_keys_deleted": resolution_keys_deleted,
            }
        finally:
            connection.close()


def normalize_document_entity_role(raw_role: object) -> str:
    normalized = normalize_entity_lookup_text(raw_role).replace(" ", "_")
    aliases = {
        "participants": "participant",
        "recipients": "recipient",
        "authors": "author",
        "custodians": "custodian",
    }
    role = aliases.get(normalized, normalized)
    if role not in DOCUMENT_ENTITY_ROLES:
        raise RetrieverError(f"Unsupported entity role: {raw_role}. Supported roles: {', '.join(sorted(DOCUMENT_ENTITY_ROLES))}.")
    return role


def ensure_document_row(connection: sqlite3.Connection, document_id: int) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE id = ?
        """,
        (int(document_id),),
    ).fetchone()
    if row is None:
        raise RetrieverError(f"Unknown document id: {document_id}")
    return row


def next_document_entity_ordinal(connection: sqlite3.Connection, document_id: int, role: str) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(ordinal), -1) + 1 AS next_ordinal
        FROM document_entities
        WHERE document_id = ?
          AND role = ?
        """,
        (int(document_id), role),
    ).fetchone()
    return int(row["next_ordinal"] or 0) if row is not None else 0


def assign_entity(
    root: Path,
    *,
    document_id: int,
    role: str,
    entity_id: int,
    reason: str | None = None,
) -> dict[str, object]:
    normalized_role = normalize_document_entity_role(role)
    normalized_reason = normalize_whitespace(str(reason or "")) or None
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="assign-entity"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="assign-entity")
            raise_if_entity_rebuild_active(connection, root, command_name="assign-entity")
            ensure_document_row(connection, document_id)
            entity_row = active_entity_row(connection, entity_id)
            connection.execute("BEGIN")
            try:
                existing_row = connection.execute(
                    """
                    SELECT *
                    FROM document_entities
                    WHERE document_id = ?
                      AND role = ?
                      AND entity_id = ?
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (int(document_id), normalized_role, int(entity_id)),
                ).fetchone()
                evidence = json.dumps(
                    {"source": "manual_assignment", "reason": normalized_reason},
                    ensure_ascii=True,
                    sort_keys=True,
                )
                if existing_row is not None:
                    connection.execute(
                        """
                        UPDATE document_entities
                        SET assignment_mode = 'manual', evidence_json = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (evidence, utc_now(), int(existing_row["id"])),
                    )
                    created = False
                else:
                    connection.execute(
                        """
                        INSERT INTO document_entities (
                          document_id, entity_id, role, ordinal, assignment_mode,
                          observed_title, evidence_json, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, 'manual', NULL, ?, ?, ?)
                        """,
                        (
                            int(document_id),
                            int(entity_id),
                            normalized_role,
                            next_document_entity_ordinal(connection, int(document_id), normalized_role),
                            evidence,
                            utc_now(),
                            utc_now(),
                        ),
                    )
                    created = True
                refresh_documents_after_entity_graph_change(connection, [int(document_id)])
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "document_id": int(document_id),
                "role": normalized_role,
                "entity_id": int(entity_id),
                "label": entity_display_label_from_row(entity_row),
                "created": created,
                "reason": normalized_reason,
            }
        finally:
            connection.close()


def document_override_from_link(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    role: str,
    source_entity_id: int,
    override_effect: str,
    replacement_entity_id: int | None = None,
    evidence_json: object = None,
    reason: str | None = None,
) -> int:
    evidence: dict[str, object] = {}
    if isinstance(evidence_json, str):
        try:
            parsed = json.loads(evidence_json or "{}")
            if isinstance(parsed, dict):
                evidence = parsed
        except json.JSONDecodeError:
            evidence = {}
    elif isinstance(evidence_json, dict):
        evidence = evidence_json
    normalized_candidate_key = normalize_whitespace(str(evidence.get("normalized_candidate_key") or "")) or None
    source_hint = normalize_whitespace(str(evidence.get("raw_value") or "")) or None
    connection.execute(
        """
        INSERT OR IGNORE INTO entity_overrides (
          scope_type, scope_id, role, source_entity_id,
          normalized_candidate_key, replacement_entity_id,
          override_effect, source_hint, reason, created_at, updated_at
        ) VALUES ('document', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(document_id),
            role,
            int(source_entity_id),
            normalized_candidate_key,
            replacement_entity_id,
            override_effect,
            source_hint,
            reason,
            utc_now(),
            utc_now(),
        ),
    )
    return int(connection.execute("SELECT changes()").fetchone()[0] or 0)


def unassign_entity(
    root: Path,
    *,
    document_id: int,
    role: str,
    entity_id: int,
    reason: str | None = None,
) -> dict[str, object]:
    normalized_role = normalize_document_entity_role(role)
    normalized_reason = normalize_whitespace(str(reason or "")) or None
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="unassign-entity"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="unassign-entity")
            raise_if_entity_rebuild_active(connection, root, command_name="unassign-entity")
            ensure_document_row(connection, document_id)
            active_entity_row(connection, entity_id)
            link_rows = connection.execute(
                """
                SELECT *
                FROM document_entities
                WHERE document_id = ?
                  AND role = ?
                  AND entity_id = ?
                ORDER BY id ASC
                """,
                (int(document_id), normalized_role, int(entity_id)),
            ).fetchall()
            if not link_rows:
                raise RetrieverError(
                    f"Document {document_id} has no {normalized_role} link for entity {entity_id}."
                )
            manual_links_removed = 0
            auto_links_removed = 0
            overrides_created = 0
            connection.execute("BEGIN")
            try:
                for link_row in link_rows:
                    if link_row["assignment_mode"] == "manual":
                        connection.execute("DELETE FROM document_entities WHERE id = ?", (int(link_row["id"]),))
                        manual_links_removed += 1
                    else:
                        overrides_created += document_override_from_link(
                            connection,
                            document_id=int(document_id),
                            role=normalized_role,
                            source_entity_id=int(entity_id),
                            override_effect="remove",
                            evidence_json=link_row["evidence_json"],
                            reason=normalized_reason,
                        )
                        connection.execute("DELETE FROM document_entities WHERE id = ?", (int(link_row["id"]),))
                        auto_links_removed += 1
                refresh_documents_after_entity_graph_change(connection, [int(document_id)])
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "document_id": int(document_id),
                "role": normalized_role,
                "entity_id": int(entity_id),
                "manual_links_removed": manual_links_removed,
                "auto_links_removed": auto_links_removed,
                "overrides_created": overrides_created,
                "reason": normalized_reason,
            }
        finally:
            connection.close()


def create_split_target_entity(
    connection: sqlite3.Connection,
    *,
    source_row: sqlite3.Row,
    display_name: str | None,
) -> int:
    normalized_display_name = normalize_whitespace(str(display_name or "")) or None
    connection.execute(
        """
        INSERT INTO entities (
          entity_type, display_name, display_name_source, entity_origin,
          canonical_status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_row["entity_type"],
            normalized_display_name,
            ENTITY_DISPLAY_SOURCE_MANUAL if normalized_display_name else ENTITY_DISPLAY_SOURCE_AUTO,
            ENTITY_ORIGIN_MANUAL,
            ENTITY_STATUS_ACTIVE,
            utc_now(),
            utc_now(),
        ),
    )
    return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])


def selected_identifier_rows_for_split(
    connection: sqlite3.Connection,
    *,
    source_entity_id: int,
    identifier_ids: list[int] | None,
) -> list[sqlite3.Row]:
    normalized_ids = list(dict.fromkeys(int(identifier_id) for identifier_id in identifier_ids or []))
    if not normalized_ids:
        return []
    placeholders = ", ".join("?" for _ in normalized_ids)
    rows = connection.execute(
        f"""
        SELECT *
        FROM entity_identifiers
        WHERE id IN ({placeholders})
          AND entity_id = ?
        ORDER BY id ASC
        """,
        [*normalized_ids, int(source_entity_id)],
    ).fetchall()
    found_ids = {int(row["id"]) for row in rows}
    missing_ids = [identifier_id for identifier_id in normalized_ids if identifier_id not in found_ids]
    if missing_ids:
        raise RetrieverError(
            f"Identifier id(s) do not belong to entity {source_entity_id}: {', '.join(str(item) for item in missing_ids)}"
        )
    return rows


def move_selected_identifiers_to_target(
    connection: sqlite3.Connection,
    *,
    source_entity_id: int,
    target_entity_id: int,
    identifier_rows: list[sqlite3.Row],
) -> dict[str, int]:
    moved_identifiers = 0
    deduped_identifiers = 0
    moved_resolution_keys = 0
    deleted_resolution_keys = 0
    for identifier_row in identifier_rows:
        source_identifier_id = int(identifier_row["id"])
        target_identifier_id = matching_survivor_identifier_id(
            connection,
            survivor_entity_id=int(target_entity_id),
            identifier_row=identifier_row,
        )
        if target_identifier_id is None:
            connection.execute(
                """
                UPDATE entity_identifiers
                SET entity_id = ?, source_kind = 'manual', updated_at = ?
                WHERE id = ?
                """,
                (int(target_entity_id), utc_now(), source_identifier_id),
            )
            target_identifier_id = source_identifier_id
            moved_identifiers += 1
        else:
            deduped_identifiers += 1
        for key_row in connection.execute(
            """
            SELECT *
            FROM entity_resolution_keys
            WHERE identifier_id = ?
               OR entity_id = ?
                  AND key_type = ?
                  AND normalized_value = ?
                  AND COALESCE(provider, '') = COALESCE(?, '')
                  AND COALESCE(provider_scope, '') = COALESCE(?, '')
                  AND COALESCE(identifier_name, '') = COALESCE(?, '')
                  AND COALESCE(identifier_scope, '') = COALESCE(?, '')
            ORDER BY id ASC
            """,
            (
                source_identifier_id,
                int(source_entity_id),
                identifier_row["identifier_type"],
                identifier_row["normalized_value"],
                identifier_row["provider"],
                identifier_row["provider_scope"],
                identifier_row["identifier_name"],
                identifier_row["identifier_scope"],
            ),
        ).fetchall():
            existing_key = resolution_key_matches_row(connection, key_row, exclude_id=int(key_row["id"]))
            if existing_key is not None:
                connection.execute("DELETE FROM entity_resolution_keys WHERE id = ?", (int(key_row["id"]),))
                deleted_resolution_keys += 1
            else:
                connection.execute(
                    """
                    UPDATE entity_resolution_keys
                    SET entity_id = ?, identifier_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (int(target_entity_id), target_identifier_id, utc_now(), int(key_row["id"])),
                )
                moved_resolution_keys += 1
        if target_identifier_id != source_identifier_id:
            connection.execute("DELETE FROM entity_identifiers WHERE id = ?", (source_identifier_id,))
    return {
        "moved_identifiers": moved_identifiers,
        "deduped_identifiers": deduped_identifiers,
        "moved_resolution_keys": moved_resolution_keys,
        "deleted_resolution_keys": deleted_resolution_keys,
    }


def selected_document_entity_rows_for_split(
    connection: sqlite3.Connection,
    *,
    source_entity_id: int,
    document_ids: list[int] | None,
    roles: list[str] | None,
) -> list[sqlite3.Row]:
    normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in document_ids or []))
    normalized_roles = [normalize_document_entity_role(role) for role in roles or []]
    if not normalized_document_ids:
        return []
    for document_id in normalized_document_ids:
        ensure_document_row(connection, document_id)
    params: list[object] = [int(source_entity_id), *normalized_document_ids]
    role_clause = ""
    if normalized_roles:
        role_clause = f"AND role IN ({', '.join('?' for _ in normalized_roles)})"
        params.extend(normalized_roles)
    rows = connection.execute(
        f"""
        SELECT *
        FROM document_entities
        WHERE entity_id = ?
          AND document_id IN ({', '.join('?' for _ in normalized_document_ids)})
          {role_clause}
        ORDER BY document_id ASC, role ASC, id ASC
        """,
        params,
    ).fetchall()
    if not rows:
        raise RetrieverError("No matching document/entity links found to split.")
    return rows


def split_entity(
    root: Path,
    source_entity_id: int,
    *,
    target_entity_id: int | None = None,
    identifier_ids: list[int] | None = None,
    document_ids: list[int] | None = None,
    roles: list[str] | None = None,
    display_name: str | None = None,
    reason: str | None = None,
    block_merge: bool = True,
) -> dict[str, object]:
    if not identifier_ids and not document_ids:
        raise RetrieverError("split-entity requires at least one --identifier-id or --doc-id.")
    normalized_reason = normalize_whitespace(str(reason or "")) or None
    paths = workspace_paths(root)
    ensure_layout(paths)
    with workspace_entity_rebuild_session(paths, command_name="split-entity"):
        connection = connect_db(paths["db_path"])
        try:
            apply_schema(connection, root)
            raise_if_ingest_v2_active(connection, root, command_name="split-entity")
            raise_if_entity_rebuild_active(connection, root, command_name="split-entity")
            source_row = active_entity_row(connection, source_entity_id)
            identifier_rows = selected_identifier_rows_for_split(
                connection,
                source_entity_id=int(source_entity_id),
                identifier_ids=identifier_ids,
            )
            document_link_rows = selected_document_entity_rows_for_split(
                connection,
                source_entity_id=int(source_entity_id),
                document_ids=document_ids,
                roles=roles,
            )
            connection.execute("BEGIN")
            try:
                created_target = target_entity_id is None
                if target_entity_id is None:
                    target_id = create_split_target_entity(
                        connection,
                        source_row=source_row,
                        display_name=display_name,
                    )
                else:
                    target_id = int(target_entity_id)
                    active_entity_row(connection, target_id)
                identifier_counts = move_selected_identifiers_to_target(
                    connection,
                    source_entity_id=int(source_entity_id),
                    target_entity_id=target_id,
                    identifier_rows=identifier_rows,
                )
                moved_links = 0
                deduped_links = 0
                overrides_created = 0
                affected_document_ids: list[int] = []
                for link_row in document_link_rows:
                    document_id = int(link_row["document_id"])
                    role = str(link_row["role"])
                    affected_document_ids.append(document_id)
                    if link_row["assignment_mode"] == "auto":
                        overrides_created += document_override_from_link(
                            connection,
                            document_id=document_id,
                            role=role,
                            source_entity_id=int(source_entity_id),
                            override_effect="replace",
                            replacement_entity_id=target_id,
                            evidence_json=link_row["evidence_json"],
                            reason=normalized_reason,
                        )
                    existing_target_link = connection.execute(
                        """
                        SELECT id
                        FROM document_entities
                        WHERE document_id = ?
                          AND role = ?
                          AND entity_id = ?
                        ORDER BY id ASC
                        LIMIT 1
                        """,
                        (document_id, role, target_id),
                    ).fetchone()
                    if existing_target_link is not None:
                        connection.execute("DELETE FROM document_entities WHERE id = ?", (int(link_row["id"]),))
                        deduped_links += 1
                    else:
                        connection.execute(
                            """
                            UPDATE document_entities
                            SET entity_id = ?, assignment_mode = 'manual', updated_at = ?
                            WHERE id = ?
                            """,
                            (target_id, utc_now(), int(link_row["id"])),
                        )
                        moved_links += 1
                if block_merge:
                    left, right = entity_pair_key(int(source_entity_id), target_id)
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO entity_merge_blocks (
                          left_entity_id, right_entity_id, reason, created_at
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (left, right, normalized_reason or "entity split", utc_now()),
                    )
                recompute_entity_caches(connection, int(source_entity_id))
                recompute_entity_caches(connection, target_id)
                refresh_documents_after_entity_graph_change(connection, affected_document_ids)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "status": "ok",
                "source_entity_id": int(source_entity_id),
                "target_entity_id": target_id,
                "created_target": created_target,
                "display_name": normalize_whitespace(str(display_name or "")) or None,
                "reason": normalized_reason,
                "block_merge": bool(block_merge),
                "moved_document_links": moved_links,
                "deduped_document_links": deduped_links,
                "overrides_created": overrides_created,
                "affected_document_ids": sorted(set(affected_document_ids)),
                **identifier_counts,
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
            "datasets": list_dataset_summaries(connection, root=root),
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
            "dataset": dataset_summary_by_id(connection, dataset_id, root=root),
        }
    finally:
        connection.close()


def normalize_dataset_policy_bool(raw_value: object, *, field_name: str) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    text = normalize_entity_lookup_text(raw_value)
    if text in {"1", "true", "yes", "y", "on", "enable", "enabled"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disable", "disabled"}:
        return False
    raise RetrieverError(f"Invalid boolean value for {field_name}: {raw_value!r}")


def normalize_dataset_external_id_policy_names(raw_names: list[str] | None) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for raw_name in raw_names or []:
        name = normalize_entity_identifier_name(raw_name)
        if not name:
            raise RetrieverError(f"Invalid external-id merge policy name: {raw_name!r}")
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def show_dataset_policy(
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
        raise_if_ingest_v2_active(connection, root, command_name="set-dataset-policy")
        dataset_row = resolve_dataset_row(connection, dataset_id=dataset_id, dataset_name=dataset_name)
        return {
            "status": "ok",
            "dataset": dataset_summary_by_id(connection, int(dataset_row["id"]), root=root),
            "merge_policy": dataset_merge_policy_payload_from_row(dataset_row),
        }
    finally:
        connection.close()


def set_dataset_policy(
    root: Path,
    *,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
    allow_auto_merge: object | None = None,
    email_auto_merge: object | None = None,
    handle_auto_merge: object | None = None,
    phone_auto_merge: object | None = None,
    name_auto_merge: object | None = None,
    external_id_auto_merge_names: list[str] | None = None,
    clear_external_id_auto_merge_names: bool = False,
) -> dict[str, object]:
    if clear_external_id_auto_merge_names and external_id_auto_merge_names is not None:
        raise RetrieverError("Use either --external-id-auto-merge-name or --clear-external-id-auto-merge-names, not both.")
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        raise_if_ingest_v2_active(connection, root, command_name="set-dataset-policy")
        raise_if_entity_rebuild_active(connection, root, command_name="set-dataset-policy")
        dataset_row = resolve_dataset_row(connection, dataset_id=dataset_id, dataset_name=dataset_name)
        if normalize_whitespace(str(dataset_row["source_kind"] or "")).lower() == MANUAL_DATASET_SOURCE_KIND:
            raise RetrieverError("Dataset merge policy controls apply only to source-backed datasets.")
        before_policy = dataset_merge_policy_payload_from_row(dataset_row)
        updates: dict[str, object] = {}
        for column_name, raw_value in (
            ("allow_auto_merge", allow_auto_merge),
            ("email_auto_merge", email_auto_merge),
            ("handle_auto_merge", handle_auto_merge),
            ("phone_auto_merge", phone_auto_merge),
            ("name_auto_merge", name_auto_merge),
        ):
            if raw_value is None:
                continue
            updates[column_name] = 1 if normalize_dataset_policy_bool(raw_value, field_name=column_name) else 0
        if external_id_auto_merge_names is not None:
            updates["external_id_auto_merge_names_json"] = json.dumps(
                normalize_dataset_external_id_policy_names(external_id_auto_merge_names),
                ensure_ascii=True,
                sort_keys=True,
            )
        elif clear_external_id_auto_merge_names:
            updates["external_id_auto_merge_names_json"] = "[]"
        if not updates:
            raise RetrieverError("set-dataset-policy requires at least one policy flag.")

        connection.execute("BEGIN")
        try:
            updates["updated_at"] = utc_now()
            set_clause = ", ".join(f"{quote_identifier(column)} = ?" for column in updates)
            connection.execute(
                f"""
                UPDATE datasets
                SET {set_clause}
                WHERE id = ?
                """,
                [*updates.values(), int(dataset_row["id"])],
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        updated_row = get_dataset_row_by_id(connection, int(dataset_row["id"]))
        assert updated_row is not None
        after_policy = dataset_merge_policy_payload_from_row(updated_row)
        return {
            "status": "ok",
            "dataset": dataset_summary_by_id(connection, int(updated_row["id"]), root=root),
            "before_merge_policy": before_policy,
            "merge_policy": after_policy,
            "changed": before_policy != after_policy,
            "rebuild_recommended": before_policy != after_policy,
            "rebuild_command": f"rebuild-entities {shlex.quote(str(root))}",
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
            "dataset": dataset_summary_by_id(connection, int(dataset_row["id"]), root=root),
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
            "dataset": dataset_summary_by_id(connection, int(dataset_row["id"]), root=root),
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
            result = delete_dataset_row(connection, int(dataset_row["id"]), root=root)
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
            renamed_summary = rename_dataset_row(connection, int(dataset_row["id"]), new_name, root=root)
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
    stale_after_seconds: int | None = None,
    launch_mode: str = "inline",
    worker_task_id: str | None = None,
    max_batches: int | None = None,
) -> dict[str, object]:
    if limit < 1:
        raise RetrieverError("Claim limit must be >= 1.")
    effective_stale_after_seconds = (
        int(stale_after_seconds)
        if stale_after_seconds is not None
        else default_run_item_claim_stale_seconds_for_launch_mode(launch_mode)
    )
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
                stale_after_seconds=effective_stale_after_seconds,
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
                "stale_after_seconds": effective_stale_after_seconds,
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
    stale_after_seconds: int | None = None,
    launch_mode: str = "inline",
    worker_task_id: str | None = None,
    max_batches: int | None = None,
    budget_seconds: int | None = None,
) -> dict[str, object]:
    if limit is not None and limit < 1:
        raise RetrieverError("Claim limit must be >= 1.")
    budget = normalize_resumable_step_budget(budget_seconds) if budget_seconds is not None else None
    effective_stale_after_seconds = (
        int(stale_after_seconds)
        if stale_after_seconds is not None
        else default_run_item_claim_stale_seconds_for_launch_mode(launch_mode)
    )
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
            if budget is not None and budget < RUN_JOB_MIN_SECONDS_TO_CLAIM:
                effective_limit = 0
            claimed_rows: list[sqlite3.Row] = []
            batch_payloads: list[dict[str, object]] = []

            if initial_worker_payload["next_action"] == "claim" and effective_limit > 0:
                claimed_rows = claim_run_item_rows(
                    connection,
                    run_id=run_id,
                    claimed_by=claimed_by,
                    limit=effective_limit,
                    stale_after_seconds=effective_stale_after_seconds,
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
            if budget is not None:
                worker_payload["budget_seconds"] = budget
                worker_payload["minimum_seconds_to_claim"] = RUN_JOB_MIN_SECONDS_TO_CLAIM
                if budget < RUN_JOB_MIN_SECONDS_TO_CLAIM and worker_payload["next_action"] == "claim":
                    worker_payload["next_action"] = "stop"
                    worker_payload["stop_reason"] = "budget_exhausted"
            if batch_payloads:
                worker_payload["next_action"] = "process_batch"
                worker_payload["stop_reason"] = None
            elif worker_payload["next_action"] == "claim":
                worker_payload["next_action"] = "stop"
                worker_payload["stop_reason"] = "no_claimable_items"
            worker_payload["prepared_batch_size"] = len(batch_payloads)
            current_run_payload["next_recommended_commands"] = run_job_next_recommended_commands(
                root,
                run_payload=current_run_payload,
                budget_seconds=budget if budget is not None else DEFAULT_RESUMABLE_STEP_BUDGET_SECONDS,
                claimed_by=claimed_by,
            )

            payload = {
                "status": "ok",
                "run": current_run_payload,
                "worker": worker_payload,
                "claimed_by": normalize_whitespace(claimed_by),
                "requested_limit": effective_limit,
                "stale_after_seconds": effective_stale_after_seconds,
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


def run_job_default_claimed_by(run_id: int) -> str:
    return f"cowork-run-{int(run_id)}"


def run_job_next_recommended_commands(
    root: Path,
    *,
    run_payload: dict[str, object],
    budget_seconds: int,
    claimed_by: str | None = None,
) -> list[str]:
    run_id = int(run_payload["id"])
    root_arg = shlex.quote(str(root))
    run_id_arg = str(run_id)
    budget_arg = str(int(budget_seconds))
    claimed_by_arg = shlex.quote(normalize_whitespace(claimed_by or run_job_default_claimed_by(run_id)))
    if str(run_payload.get("status") or "") in {"completed", "failed", "canceled"}:
        return [f"run-status {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}"]

    worker_payload = dict(run_payload.get("worker") or {})
    next_action = str(worker_payload.get("next_action") or "")
    commands: list[str] = []
    if next_action == "finalize_ocr":
        commands.append(f"finalize-ocr-run {root_arg} --run-id {run_id_arg}")
    elif next_action == "finalize_image_description":
        commands.append(f"finalize-image-description-run {root_arg} --run-id {run_id_arg}")
    elif next_action in {"claim", "stop"}:
        run_item_counts = dict(run_payload.get("run_item_counts") or {})
        if (
            int(worker_payload.get("outstanding_items", 0) or 0) > 0
            or int(run_item_counts.get("pending", 0) or 0) > 0
            or int(run_item_counts.get("running", 0) or 0) > 0
        ):
            commands.append(
                "prepare-run-batch "
                f"{root_arg} --run-id {run_id_arg} --claimed-by {claimed_by_arg} "
                f"--budget-seconds {budget_arg} --stale-seconds {DEFAULT_COWORK_RUN_ITEM_CLAIM_STALE_SECONDS}"
            )
    if commands:
        commands.insert(
            0,
            "run-job-step "
            f"{root_arg} --run-id {run_id_arg} --claimed-by {claimed_by_arg} --budget-seconds {budget_arg}",
        )
    commands.append(f"run-status {root_arg} --run-id {run_id_arg} --budget-seconds {budget_arg}")
    return commands


def run_status(root: Path, *, run_id: int, budget_seconds: int | None = None) -> dict[str, object]:
    budget = (
        normalize_resumable_step_budget(budget_seconds)
        if budget_seconds is not None
        else DEFAULT_RESUMABLE_STEP_BUDGET_SECONDS
    )
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        run_payload = run_status_by_id(connection, run_id)
        run_payload["next_recommended_commands"] = run_job_next_recommended_commands(
            root,
            run_payload=run_payload,
            budget_seconds=budget,
        )
        return {
            "status": "ok",
            "run": run_payload,
        }
    finally:
        connection.close()


def run_job_step(
    root: Path,
    *,
    run_id: int,
    claimed_by: str | None = None,
    budget_seconds: int | None = None,
    limit: int | None = None,
    launch_mode: str = "inline",
    worker_task_id: str | None = None,
    max_batches: int | None = None,
    stale_after_seconds: int | None = None,
) -> dict[str, object]:
    budget = normalize_resumable_step_budget(budget_seconds)
    normalized_claimed_by = normalize_whitespace(claimed_by or run_job_default_claimed_by(run_id))
    if not normalized_claimed_by:
        raise RetrieverError("claimed_by cannot be empty.")
    status_payload = run_status(root, run_id=run_id, budget_seconds=budget)
    run_payload = dict(status_payload["run"])
    worker_payload = dict(run_payload.get("worker") or {})
    next_action = str(worker_payload.get("next_action") or "")
    if str(run_payload.get("status") or "") in {"completed", "failed", "canceled"}:
        return {
            "status": "ok",
            "step": "run-job",
            "run_id": run_id,
            "claimed_by": normalized_claimed_by,
            "budget_seconds": budget,
            "executed": False,
            "executed_step": None,
            "reason": "run_terminal",
            "batch": [],
            "step_result": None,
            "run": run_payload,
            "more_work_remaining": False,
            "next_recommended_commands": run_payload.get("next_recommended_commands", []),
        }
    if budget < RUN_JOB_MIN_SECONDS_TO_CLAIM and next_action == "claim":
        run_payload["next_recommended_commands"] = run_job_next_recommended_commands(
            root,
            run_payload=run_payload,
            budget_seconds=budget,
            claimed_by=normalized_claimed_by,
        )
        return {
            "status": "ok",
            "step": "run-job",
            "run_id": run_id,
            "claimed_by": normalized_claimed_by,
            "budget_seconds": budget,
            "executed": False,
            "executed_step": None,
            "reason": "budget_exhausted",
            "batch": [],
            "step_result": None,
            "run": run_payload,
            "more_work_remaining": True,
            "next_recommended_commands": run_payload["next_recommended_commands"],
        }

    executed_step: str | None = None
    step_result: dict[str, object] | None = None
    if next_action == "finalize_ocr":
        executed_step = "finalize_ocr"
        step_result = finalize_ocr_run(root, run_id=run_id)
    elif next_action == "finalize_image_description":
        executed_step = "finalize_image_description"
        step_result = finalize_image_description_run(root, run_id=run_id)
    elif next_action == "claim":
        executed_step = "prepare_run_batch"
        step_result = prepare_run_batch(
            root,
            run_id=run_id,
            claimed_by=normalized_claimed_by,
            limit=limit,
            stale_after_seconds=(
                stale_after_seconds
                if stale_after_seconds is not None
                else DEFAULT_COWORK_RUN_ITEM_CLAIM_STALE_SECONDS
            ),
            launch_mode=launch_mode,
            worker_task_id=worker_task_id,
            max_batches=max_batches,
            budget_seconds=budget,
        )
    else:
        run_payload["next_recommended_commands"] = run_job_next_recommended_commands(
            root,
            run_payload=run_payload,
            budget_seconds=budget,
            claimed_by=normalized_claimed_by,
        )
        return {
            "status": "ok",
            "step": "run-job",
            "run_id": run_id,
            "claimed_by": normalized_claimed_by,
            "budget_seconds": budget,
            "executed": False,
            "executed_step": None,
            "reason": next_action or "stop",
            "batch": [],
            "step_result": None,
            "run": run_payload,
            "more_work_remaining": bool(run_payload.get("status") not in {"completed", "failed", "canceled"}),
            "next_recommended_commands": run_payload["next_recommended_commands"],
        }

    updated_run_payload = dict(
        (step_result or {}).get("run")
        or run_status(root, run_id=run_id, budget_seconds=budget)["run"]
    )
    updated_run_payload["next_recommended_commands"] = run_job_next_recommended_commands(
        root,
        run_payload=updated_run_payload,
        budget_seconds=budget,
        claimed_by=normalized_claimed_by,
    )
    batch_payload = list((step_result or {}).get("batch") or [])
    return {
        "status": "ok",
        "step": "run-job",
        "run_id": run_id,
        "claimed_by": normalized_claimed_by,
        "budget_seconds": budget,
        "executed": True,
        "executed_step": executed_step,
        "reason": (
            "batch_ready"
            if batch_payload
            else (
                "run_terminal"
                if str(updated_run_payload.get("status") or "") in {"completed", "failed", "canceled"}
                else "step_complete"
            )
        ),
        "batch": batch_payload,
        "worker": (step_result or {}).get("worker"),
        "step_result": step_result,
        "run": updated_run_payload,
        "more_work_remaining": bool(
            str(updated_run_payload.get("status") or "") not in {"completed", "failed", "canceled"}
        ),
        "next_recommended_commands": updated_run_payload["next_recommended_commands"],
    }


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


def normalize_delete_path_prefixes(raw_path_prefixes: list[str] | None) -> list[str]:
    normalized_prefixes: list[str] = []
    for raw_path_prefix in raw_path_prefixes or []:
        candidate = str(raw_path_prefix or "").strip()
        if candidate.startswith("./"):
            candidate = candidate[2:]
        candidate = candidate.rstrip("/")
        if candidate:
            normalized_prefixes.append(candidate)
    return list(dict.fromkeys(normalized_prefixes))


def sql_text_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def path_prefix_scope_expression(path_prefixes: list[str]) -> str | None:
    clauses: list[str] = []
    for path_prefix in normalize_delete_path_prefixes(path_prefixes):
        clauses.append(
            f"(rel_path = {sql_text_literal(path_prefix)} OR rel_path LIKE {sql_text_literal(path_prefix + '/%')})"
        )
    if not clauses:
        return None
    return " OR ".join(clauses)


def raw_filters_include_occurrence_scope(
    connection: sqlite3.Connection,
    raw_filters: list[list[str]] | None,
) -> bool:
    if not raw_filters:
        return False
    if uses_legacy_tuple_filters(raw_filters):
        for raw_filter in parse_filter_args(raw_filters):
            field_def = resolve_field_definition(connection, str(raw_filter["field_name"]))
            if str(field_def["field_name"]) in OCCURRENCE_FILTER_FIELDS:
                return True
        return False

    pattern = re.compile(
        r"\b(?:"
        + "|".join(re.escape(field_name) for field_name in sorted(OCCURRENCE_FILTER_FIELDS, key=len, reverse=True))
        + r")\b",
        re.IGNORECASE,
    )
    return any(
        pattern.search(expression) is not None
        for expression in normalize_sql_filter_expressions(raw_filters)
    )


def raw_filter_item_includes_occurrence_scope(
    connection: sqlite3.Connection,
    raw_filter_item: object,
) -> bool:
    if not isinstance(raw_filter_item, (list, tuple)):
        return False
    if len(raw_filter_item) >= 2:
        operator = normalize_inline_whitespace(str(raw_filter_item[1] or "")).lower()
        if operator in {"eq", "neq", "gt", "gte", "lt", "lte", "contains", "is-null", "not-null"}:
            field_def = resolve_field_definition(connection, str(raw_filter_item[0]))
            return str(field_def["field_name"]) in OCCURRENCE_FILTER_FIELDS
    expression = " ".join(str(part) for part in raw_filter_item if normalize_inline_whitespace(str(part or "")))
    if not expression:
        return False
    pattern = re.compile(
        r"\b(?:"
        + "|".join(re.escape(field_name) for field_name in sorted(OCCURRENCE_FILTER_FIELDS, key=len, reverse=True))
        + r")\b",
        re.IGNORECASE,
    )
    return pattern.search(expression) is not None


def document_only_raw_filters(
    connection: sqlite3.Connection,
    raw_filters: list[list[str]] | None,
) -> list[list[str]]:
    document_filters: list[list[str]] = []
    for raw_filter_item in raw_filters or []:
        if raw_filter_item_includes_occurrence_scope(connection, raw_filter_item):
            continue
        document_filters.append(list(raw_filter_item))
    return document_filters


def document_ids_matching_occurrence_filters(
    connection: sqlite3.Connection,
    raw_filters: list[list[str]],
) -> list[int]:
    occurrence_scope_clauses, occurrence_scope_params = build_occurrence_scope_filters(connection, raw_filters)
    rows = connection.execute(
        f"""
        SELECT DISTINCT d.id
        FROM document_occurrences o
        JOIN documents d ON d.id = o.document_id
        WHERE d.lifecycle_status NOT IN ('missing', 'deleted')
          AND {' AND '.join(occurrence_scope_clauses)}
        ORDER BY d.id ASC
        """,
        occurrence_scope_params,
    ).fetchall()
    return [int(row["id"]) for row in rows]


def fetch_deletable_document_rows_by_ids(
    connection: sqlite3.Connection,
    document_ids: list[int],
) -> list[sqlite3.Row]:
    normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in document_ids))
    if not normalized_document_ids:
        return []
    placeholders = ", ".join("?" for _ in normalized_document_ids)
    rows = connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE id IN ({placeholders})
        """,
        normalized_document_ids,
    ).fetchall()
    rows_by_id = {int(row["id"]): row for row in rows}
    missing_ids: list[int] = []
    lifecycle_hidden: list[str] = []
    visible_rows_by_id: dict[int, sqlite3.Row] = {}
    for document_id in normalized_document_ids:
        row = rows_by_id.get(document_id)
        if row is None:
            missing_ids.append(document_id)
            continue
        if row["lifecycle_status"] in {"missing", "deleted"}:
            lifecycle_hidden.append(f"{document_id} ({row['lifecycle_status']})")
            continue
        visible_rows_by_id[document_id] = row

    errors: list[str] = []
    if missing_ids:
        errors.append(
            "Unknown document id" + ("" if len(missing_ids) == 1 else "s") + f": {', '.join(str(document_id) for document_id in missing_ids)}"
        )
    if lifecycle_hidden:
        errors.append(
            "Document id"
            + ("" if len(lifecycle_hidden) == 1 else "s")
            + " not deletable due to lifecycle_status: "
            + ", ".join(lifecycle_hidden)
        )
    if errors:
        raise RetrieverError(" ".join(errors))
    return [visible_rows_by_id[document_id] for document_id in normalized_document_ids]


def sample_documents_for_delete(
    connection: sqlite3.Connection,
    document_ids: list[int],
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
          rel_path,
          file_name,
          title
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
                "rel_path": row["rel_path"],
                "file_name": row["file_name"],
                "title": row["title"],
            }
        )
    return sample_rows


def scoped_active_occurrence_rows_for_document(
    connection: sqlite3.Connection,
    document_id: int,
    occurrence_scope_clauses: list[str],
    occurrence_scope_params: list[object],
) -> list[sqlite3.Row]:
    return connection.execute(
        f"""
        SELECT o.*
        FROM document_occurrences o
        JOIN documents d ON d.id = o.document_id
        WHERE o.document_id = ?
          AND {' AND '.join(occurrence_scope_clauses)}
        ORDER BY o.id ASC
        """,
        [document_id, *occurrence_scope_params],
    ).fetchall()


def collect_occurrence_rows_for_delete_plan(
    connection: sqlite3.Connection,
    *,
    root_occurrence_ids: set[int],
    direct_occurrence_ids: set[int],
) -> list[sqlite3.Row]:
    rows_by_id: dict[int, sqlite3.Row] = {}

    normalized_root_ids = sorted(int(occurrence_id) for occurrence_id in root_occurrence_ids)
    if normalized_root_ids:
        placeholders = ", ".join("?" for _ in normalized_root_ids)
        root_rows = connection.execute(
            f"""
            SELECT *
            FROM document_occurrences
            WHERE id IN ({placeholders}) OR parent_occurrence_id IN ({placeholders})
            ORDER BY id ASC
            """,
            [*normalized_root_ids, *normalized_root_ids],
        ).fetchall()
        for row in root_rows:
            rows_by_id[int(row["id"])] = row

    normalized_direct_ids = sorted(int(occurrence_id) for occurrence_id in direct_occurrence_ids)
    if normalized_direct_ids:
        placeholders = ", ".join("?" for _ in normalized_direct_ids)
        direct_rows = connection.execute(
            f"""
            SELECT *
            FROM document_occurrences
            WHERE id IN ({placeholders})
            ORDER BY id ASC
            """,
            normalized_direct_ids,
        ).fetchall()
        for row in direct_rows:
            rows_by_id[int(row["id"])] = row

    return [rows_by_id[occurrence_id] for occurrence_id in sorted(rows_by_id)]


def delete_documents_with_no_active_occurrences(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    document_ids: list[int] | set[int],
    *,
    deleted_at: str,
) -> list[int]:
    deleted_document_ids: list[int] = []
    for document_id in sorted({int(value) for value in document_ids}):
        remaining_row = connection.execute(
            """
            SELECT 1
            FROM document_occurrences
            WHERE document_id = ?
              AND lifecycle_status = 'active'
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()
        if remaining_row is not None:
            continue
        document_row = connection.execute(
            "SELECT * FROM documents WHERE id = ?",
            (document_id,),
        ).fetchone()
        if document_row is None or document_row["lifecycle_status"] == "deleted":
            continue
        cleanup_document_artifacts(paths, connection, document_row)
        delete_document_related_rows(connection, document_id)
        connection.execute(
            """
            UPDATE documents
            SET dataset_id = NULL, lifecycle_status = 'deleted', updated_at = ?
            WHERE id = ?
            """,
            (deleted_at, document_id),
        )
        deleted_document_ids.append(document_id)
    return deleted_document_ids


def delete_dataset_memberships_for_documents(
    connection: sqlite3.Connection,
    document_ids: list[int] | set[int],
) -> int:
    normalized_document_ids = sorted({int(document_id) for document_id in document_ids})
    if not normalized_document_ids:
        return 0
    placeholders = ", ".join("?" for _ in normalized_document_ids)
    cursor = connection.execute(
        f"""
        DELETE FROM dataset_documents
        WHERE document_id IN ({placeholders})
        """,
        normalized_document_ids,
    )
    return int(cursor.rowcount or 0)


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


def filter_conversation_ids_with_missing_preview_artifacts(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    conversation_ids: list[int],
) -> list[int]:
    normalized_conversation_ids = sorted(dict.fromkeys(int(conversation_id) for conversation_id in conversation_ids))
    if not normalized_conversation_ids:
        return []
    rows = connection.execute(
        f"""
        SELECT
          d.conversation_id,
          d.id AS document_id,
          dp.rel_preview_path,
          dp.preview_type
        FROM documents d
        LEFT JOIN document_previews dp ON dp.document_id = d.id
        WHERE d.conversation_id IN ({", ".join("?" for _ in normalized_conversation_ids)})
          AND d.lifecycle_status NOT IN ('missing', 'deleted')
          AND COALESCE(d.child_document_kind, '') != ?
        ORDER BY d.conversation_id ASC, d.id ASC, dp.ordinal ASC, dp.id ASC
        """,
        (*normalized_conversation_ids, CHILD_DOCUMENT_KIND_ATTACHMENT),
    ).fetchall()
    missing_conversation_ids: set[int] = set()
    for row in rows:
        conversation_id = int(row["conversation_id"])
        rel_preview_path = row["rel_preview_path"]
        if rel_preview_path is None:
            missing_conversation_ids.add(conversation_id)
            continue
        if normalize_whitespace(str(row["preview_type"] or "")).lower() == "native":
            continue
        if not (paths["state_dir"] / str(rel_preview_path)).exists():
            missing_conversation_ids.add(conversation_id)
    return [
        conversation_id
        for conversation_id in normalized_conversation_ids
        if conversation_id in missing_conversation_ids
    ]


def refresh_generated_previews(
    root: Path,
    *,
    scope: str = "conversations",
    conversation_ids: list[int] | None = None,
    document_ids: list[int] | None = None,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
    missing_only: bool = False,
    from_source: bool = False,
) -> dict[str, object]:
    normalized_scope = normalize_whitespace(str(scope or "conversations")).lower()
    if normalized_scope not in {"conversations"}:
        raise RetrieverError(
            "refresh-previews currently supports --scope conversations. "
            "Document/all preview refresh will be added as a separate safe repair path."
        )
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
        candidate_conversation_ids = list(target_conversation_ids)
        if missing_only:
            target_conversation_ids = filter_conversation_ids_with_missing_preview_artifacts(
                connection,
                paths,
                target_conversation_ids,
            )
        connection.execute("BEGIN")
        try:
            refreshed = refresh_conversation_previews(connection, paths, target_conversation_ids)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        empty_dirs_pruned = prune_empty_conversation_preview_dirs(paths)

        result: dict[str, object] = {
            "status": "ok",
            "scope": normalized_scope,
            "missing_only": bool(missing_only),
            "from_source_requested": bool(from_source),
            "refreshed_conversations": int(refreshed),
            "empty_conversation_preview_dirs_pruned": int(empty_dirs_pruned),
        }
        if from_source:
            result["from_source_mode"] = "not_applicable_for_conversation_previews"
            result["from_source_note"] = (
                "Conversation previews are regenerated from stored document text and metadata; "
                "use ingest to reparse original source files."
            )
        if missing_only:
            result["candidate_conversations"] = len(candidate_conversation_ids)
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


def rebuild_conversations(
    root: Path,
    *,
    conversation_ids: list[int] | None = None,
    document_ids: list[int] | None = None,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
    batch_size: int = 50,
) -> dict[str, object]:
    normalized_batch_size = max(1, min(int(batch_size or 50), 1000))
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        raise_if_ingest_v2_active(connection, root, command_name="rebuild-conversations")
        connection.execute("BEGIN")
        try:
            assignment = assign_supported_conversations(connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        target_conversation_ids, dataset_summary = resolve_conversation_preview_refresh_ids(
            connection,
            conversation_ids=conversation_ids,
            document_ids=document_ids,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
        )
        refreshed = 0
        for offset in range(0, len(target_conversation_ids), normalized_batch_size):
            batch_conversation_ids = target_conversation_ids[offset : offset + normalized_batch_size]
            connection.execute("BEGIN")
            try:
                refreshed += refresh_conversation_previews(connection, paths, batch_conversation_ids)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        empty_dirs_pruned = prune_empty_conversation_preview_dirs(paths)
        result: dict[str, object] = {
            "status": "ok",
            "assignment": {key: int(value) for key, value in dict(assignment).items()},
            "target_conversations": len(target_conversation_ids),
            "refreshed_conversations": int(refreshed),
            "batch_size": normalized_batch_size,
            "empty_conversation_preview_dirs_pruned": int(empty_dirs_pruned),
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


def delete_docs(
    root: Path,
    *,
    document_ids: list[int] | None = None,
    query: str = "",
    raw_bates: str | None = None,
    raw_filters: list[list[str]] | None = None,
    dataset_names: list[str] | None = None,
    from_run_id: int | None = None,
    select_from_scope: bool = False,
    path_prefixes: list[str] | None = None,
    dry_run: bool = False,
    confirm: bool = False,
) -> dict[str, object]:
    normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in (document_ids or [])))
    normalized_path_prefixes = normalize_delete_path_prefixes(path_prefixes)
    selector_inputs_present = bool(
        query.strip()
        or raw_bates
        or raw_filters
        or dataset_names
        or from_run_id is not None
        or normalized_path_prefixes
    )
    if normalized_document_ids and (selector_inputs_present or select_from_scope):
        raise RetrieverError("delete-docs accepts either --doc-id selectors or query/filter/scope selectors, not both.")
    if dry_run and confirm:
        raise RetrieverError("Choose either --dry-run or --confirm, not both.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)

        effective_raw_filters = list(raw_filters or [])
        path_expression = path_prefix_scope_expression(normalized_path_prefixes)
        if path_expression:
            effective_raw_filters.append([path_expression])
        occurrence_scoped = bool(normalized_path_prefixes) or raw_filters_include_occurrence_scope(
            connection,
            effective_raw_filters,
        )

        if normalized_document_ids:
            target_rows = fetch_deletable_document_rows_by_ids(connection, normalized_document_ids)
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
                raw_filters=effective_raw_filters,
                dataset_names=dataset_names,
                from_run_id=from_run_id,
                select_from_scope=select_from_scope,
            )
            if not scope_run_selector_has_inputs(selector):
                raise RetrieverError(
                    "No document selection active. Provide --doc-id, --path, or at least one of --keyword, --filter, --bates, --dataset, --from-run-id, or --select-from-scope."
                )
            if occurrence_scoped:
                occurrence_document_ids = document_ids_matching_occurrence_filters(connection, effective_raw_filters)
                base_selector = build_effective_scope_selector(
                    connection,
                    paths,
                    query=query,
                    raw_bates=raw_bates,
                    raw_filters=document_only_raw_filters(connection, effective_raw_filters),
                    dataset_names=dataset_names,
                    from_run_id=from_run_id,
                    select_from_scope=select_from_scope,
                )
                if scope_run_selector_has_inputs(base_selector):
                    base_document_ids, _, _ = resolve_seed_documents_for_scope_selector(connection, base_selector)
                    allowed_document_ids = {int(document_id) for document_id in base_document_ids}
                    scope_document_ids = [
                        document_id
                        for document_id in occurrence_document_ids
                        if document_id in allowed_document_ids
                    ]
                else:
                    scope_document_ids = occurrence_document_ids
            else:
                scope_document_ids, _, _ = resolve_seed_documents_for_scope_selector(connection, selector)
            target_rows = fetch_deletable_document_rows_by_ids(connection, scope_document_ids)
            target_document_ids = [int(row["id"]) for row in target_rows]
            selector_payload = {
                "mode": "scope_search",
                "scope": selector,
            }
            selected_from_scope = bool(select_from_scope)

        occurrence_scope_clauses: list[str] = []
        occurrence_scope_params: list[object] = []
        if occurrence_scoped:
            occurrence_scope_clauses, occurrence_scope_params = build_occurrence_scope_filters(
                connection,
                effective_raw_filters,
            )

        root_occurrence_ids: set[int] = set()
        direct_occurrence_ids: set[int] = set()
        preview_refresh_document_ids: set[int] = set()

        for row in target_rows:
            document_id = int(row["id"])
            if occurrence_scoped:
                occurrence_rows = scoped_active_occurrence_rows_for_document(
                    connection,
                    document_id,
                    occurrence_scope_clauses,
                    occurrence_scope_params,
                )
            else:
                occurrence_rows = active_occurrence_rows_for_document(connection, document_id)
            if not occurrence_rows:
                continue

            if row["parent_document_id"] is None:
                root_occurrence_ids.update(int(occurrence_row["id"]) for occurrence_row in occurrence_rows)
                preview_refresh_document_ids.add(document_id)
            else:
                direct_occurrence_ids.update(int(occurrence_row["id"]) for occurrence_row in occurrence_rows)
                preview_refresh_document_ids.add(int(row["parent_document_id"]))

        targeted_occurrence_rows = collect_occurrence_rows_for_delete_plan(
            connection,
            root_occurrence_ids=root_occurrence_ids,
            direct_occurrence_ids=direct_occurrence_ids,
        )
        targeted_occurrence_ids = [int(row["id"]) for row in targeted_occurrence_rows]
        affected_document_ids = sorted({int(row["document_id"]) for row in targeted_occurrence_rows})

        preview_payload = {
            "selector": selector_payload,
            "selected_from_scope": selected_from_scope,
            "occurrence_scoped": occurrence_scoped,
            "path_prefixes": normalized_path_prefixes,
            "matched_document_count": len(target_document_ids),
            "document_ids": target_document_ids,
            "affected_document_ids": affected_document_ids,
            "would_delete_occurrences": len(targeted_occurrence_ids),
            "would_touch_documents": len(affected_document_ids),
            "sample": sample_documents_for_delete(connection, target_document_ids),
        }
        if not targeted_occurrence_ids:
            return {
                "status": "ok",
                **preview_payload,
                "deleted_occurrences": 0,
                "deleted_document_ids": [],
                "deleted_documents": 0,
                "retained_document_ids": [],
                "retained_documents": 0,
                "dataset_memberships_removed": 0,
                "attachment_preview_updates": 0,
                "assignment_summary": {"documents_assigned": 0, "conversations_created": 0},
            }

        if dry_run:
            return {"status": "ok", "dry_run": True, **preview_payload}
        if not confirm:
            return {"status": "confirm_required", **preview_payload}

        connection.execute("BEGIN")
        try:
            deleted_at = utc_now()
            placeholders = ", ".join("?" for _ in targeted_occurrence_ids)
            deleted_occurrence_cursor = connection.execute(
                f"""
                UPDATE document_occurrences
                SET lifecycle_status = 'deleted', updated_at = ?
                WHERE lifecycle_status != 'deleted'
                  AND id IN ({placeholders})
                """,
                [deleted_at, *targeted_occurrence_ids],
            )

            for document_id in affected_document_ids:
                refresh_source_backed_dataset_memberships_for_document(connection, document_id)
                refresh_document_from_occurrences(connection, document_id)

            deleted_document_ids = delete_documents_with_no_active_occurrences(
                connection,
                paths,
                affected_document_ids,
                deleted_at=deleted_at,
            )
            dataset_memberships_removed = delete_dataset_memberships_for_documents(connection, deleted_document_ids)
            assignment_summary = reassign_conversations_and_refresh_previews(connection, paths)

            attachment_preview_updates = 0
            for preview_document_id in sorted(preview_refresh_document_ids):
                preview_document_row = connection.execute(
                    """
                    SELECT lifecycle_status
                    FROM documents
                    WHERE id = ?
                    """,
                    (preview_document_id,),
                ).fetchone()
                if preview_document_row is None or preview_document_row["lifecycle_status"] in {"missing", "deleted"}:
                    continue
                attachment_preview_updates += sync_document_attachment_preview_links(
                    connection,
                    paths,
                    preview_document_id,
                )

            connection.commit()
        except Exception:
            connection.rollback()
            raise

        deleted_document_id_set = {int(document_id) for document_id in deleted_document_ids}
        retained_document_ids = [
            document_id for document_id in affected_document_ids if document_id not in deleted_document_id_set
        ]
        return {
            "status": "ok",
            **preview_payload,
            "deleted_occurrences": int(deleted_occurrence_cursor.rowcount or 0),
            "deleted_document_ids": deleted_document_ids,
            "deleted_documents": len(deleted_document_ids),
            "retained_document_ids": retained_document_ids,
            "retained_documents": len(retained_document_ids),
            "dataset_memberships_removed": dataset_memberships_removed,
            "attachment_preview_updates": attachment_preview_updates,
            "assignment_summary": assignment_summary,
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
