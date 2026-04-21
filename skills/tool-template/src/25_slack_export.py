SLACK_EXPORT_DAY_FILE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\.json$")
SLACK_EXPORT_CONVERSATION_INDEX_FILES = {
    "channels.json": "public_channel",
    "groups.json": "private_channel",
    "dms.json": "dm",
    "mpims.json": "mpim",
}
SLACK_EXPORT_AUXILIARY_FILES = {
    "users.json",
    "canvases.json",
    "integration_logs.json",
}
SLACK_EXPORT_METADATA_FILES = set(SLACK_EXPORT_CONVERSATION_INDEX_FILES) | SLACK_EXPORT_AUXILIARY_FILES


def is_slack_export_day_file(path: Path) -> bool:
    return path.is_file() and SLACK_EXPORT_DAY_FILE_PATTERN.fullmatch(path.name) is not None


def iter_slack_export_day_files(export_root: Path) -> list[Path]:
    day_files: list[Path] = []
    try:
        children = sorted(export_root.iterdir())
    except OSError:
        return []
    for child in children:
        if not child.is_dir() or child.name == ".retriever":
            continue
        try:
            for item in sorted(child.iterdir()):
                if is_slack_export_day_file(item):
                    day_files.append(item)
        except OSError:
            continue
    return day_files


def slack_export_day_file_sample_valid(path: Path) -> bool:
    try:
        decoded, _, _ = decode_bytes(path.read_bytes())
    except OSError:
        return False
    return extract_slack_chat_json_payload(path, decoded) is not None


def detect_slack_export_root(candidate_root: Path) -> dict[str, object] | None:
    if not candidate_root.is_dir() or ".retriever" in candidate_root.parts:
        return None
    if not (candidate_root / "users.json").exists():
        return None
    index_files = [
        file_name
        for file_name in sorted(SLACK_EXPORT_CONVERSATION_INDEX_FILES)
        if (candidate_root / file_name).exists()
    ]
    if not index_files:
        return None
    day_files = iter_slack_export_day_files(candidate_root)
    if not day_files:
        return None
    sample_valid = any(slack_export_day_file_sample_valid(path) for path in day_files[: min(3, len(day_files))])
    if not sample_valid:
        return None
    return {
        "root": candidate_root,
        "index_files": index_files,
        "day_files": day_files,
    }


def find_slack_export_roots(
    root: Path,
    recursive: bool,
    allowed_file_types: set[str] | None,
) -> list[dict[str, object]]:
    if allowed_file_types is not None and "json" not in allowed_file_types:
        return []

    candidates: set[Path] = set()
    if recursive:
        try:
            for users_path in root.rglob("users.json"):
                if ".retriever" in users_path.parts:
                    continue
                candidates.add(users_path.parent)
        except OSError:
            return []
    else:
        if (root / "users.json").exists():
            candidates.add(root)

    descriptors: list[dict[str, object]] = []
    accepted_roots: list[Path] = []
    for candidate in sorted(candidates, key=lambda path: (len(path.parts), path.as_posix())):
        if any(parent == candidate or parent in candidate.parents for parent in accepted_roots):
            continue
        descriptor = detect_slack_export_root(candidate)
        if descriptor is None:
            continue
        descriptors.append(descriptor)
        accepted_roots.append(candidate)
    return descriptors


def slack_conversation_display_name(folder_name: str, conversation_type: str) -> str:
    normalized = normalize_whitespace(folder_name)
    if conversation_type in {"public_channel", "private_channel"} and normalized:
        return normalized if normalized.startswith("#") else f"#{normalized}"
    return normalized or "Slack conversation"


def load_slack_export_conversation_directory(export_root: Path) -> dict[str, dict[str, str]]:
    directory: dict[str, dict[str, str]] = {}
    for file_name, conversation_type in SLACK_EXPORT_CONVERSATION_INDEX_FILES.items():
        path = export_root / file_name
        if not path.exists():
            continue
        try:
            raw_items = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(raw_items, list):
            continue
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            folder_name = choose_slack_text(item.get("name"), item.get("id"))
            if not folder_name:
                continue
            folder_key = normalize_whitespace(folder_name).lower()
            if not folder_key:
                continue
            conversation_key = choose_slack_text(item.get("id"), folder_name) or folder_name
            display_name = slack_conversation_display_name(folder_name, conversation_type)
            directory[folder_key] = {
                "conversation_key": conversation_key,
                "conversation_type": conversation_type,
                "display_name": display_name,
            }
    return directory


def slack_conversation_metadata_for_folder(
    folder_name: str,
    directory: dict[str, dict[str, str]],
) -> dict[str, str]:
    folder_key = normalize_whitespace(folder_name).lower()
    if folder_key in directory:
        return directory[folder_key]
    return {
        "conversation_key": normalize_whitespace(folder_name) or folder_name,
        "conversation_type": "channel",
        "display_name": slack_conversation_display_name(folder_name, "channel"),
    }


def slack_reply_thread_rel_path(conversation_key: str, thread_ts: str) -> str:
    conversation_segment = re.sub(r"[^A-Za-z0-9._-]+", "_", normalize_whitespace(conversation_key) or "conversation")
    thread_segment = re.sub(r"[^A-Za-z0-9._-]+", "_", normalize_whitespace(thread_ts) or "thread")
    return (
        Path("_retriever")
        / "logical"
        / "slack"
        / conversation_segment
        / "threads"
        / f"{thread_segment}.slackthread"
    ).as_posix()


def slack_day_document_title(display_name: str, day_file: Path) -> str:
    day_token = normalize_whitespace(day_file.stem)
    try:
        day_label = date.fromisoformat(day_token).strftime("%b %d, %Y").replace(" 0", " ")
    except ValueError:
        day_label = day_token
    normalized_display_name = normalize_whitespace(display_name)
    if normalized_display_name and day_label:
        return f"{normalized_display_name} - {day_label}"
    return normalized_display_name or day_label or "Slack conversation"


def slack_reply_thread_title(display_name: str, root_timestamp: str | None) -> str:
    timestamp_label = format_chat_preview_timestamp(root_timestamp)
    normalized_display_name = normalize_whitespace(display_name) or "Slack conversation"
    if timestamp_label:
        return f"{normalized_display_name} - thread from {timestamp_label}"
    return f"{normalized_display_name} - thread"


def slack_timestamp_sort_key(value: object) -> tuple[float, str]:
    raw = normalize_whitespace(str(value or ""))
    try:
        return (float(raw), raw)
    except (TypeError, ValueError):
        return (float("inf"), raw)


def slack_message_has_thread(raw_message: dict[str, object], ts_raw: str, thread_ts_raw: str | None) -> bool:
    if thread_ts_raw and thread_ts_raw == ts_raw:
        return True
    for key in ("reply_count", "reply_users_count"):
        try:
            if int(raw_message.get(key) or 0) > 0:
                return True
        except (TypeError, ValueError):
            continue
    if raw_message.get("latest_reply") is not None:
        return True
    replies = raw_message.get("replies")
    return isinstance(replies, list) and bool(replies)


def parse_slack_int(value: object) -> int:
    normalized = normalize_whitespace(str(value or ""))
    if not normalized:
        return 0
    try:
        return int(normalized)
    except ValueError:
        return 0


def load_slack_day_messages(
    day_file: Path,
    *,
    rel_path: str,
    user_directory: dict[str, dict[str, str | None]],
) -> list[dict[str, object]]:
    try:
        raw_value = json.loads(day_file.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    candidate_items = raw_value.get("messages") if isinstance(raw_value, dict) else raw_value
    if not isinstance(candidate_items, list):
        return []

    messages: list[dict[str, object]] = []
    for ordinal, item in enumerate(candidate_items):
        if not isinstance(item, dict):
            continue
        if normalize_whitespace(str(item.get("type") or "")).lower() != "message":
            continue
        ts_raw = choose_slack_text(item.get("ts"))
        if not ts_raw:
            continue
        body = render_slack_text(choose_slack_text(item.get("text")) or "", user_directory)
        if not body:
            continue
        actor_info = slack_message_actor_info(item, user_directory)
        speaker = actor_info.get("speaker_name") or "Slack message"
        timestamp = normalize_slack_timestamp(ts_raw)
        thread_ts_raw = choose_slack_text(item.get("thread_ts"))
        is_reply = bool(thread_ts_raw and thread_ts_raw != ts_raw)
        messages.append(
            {
                "avatar_color": actor_info.get("avatar_color"),
                "speaker": speaker,
                "body": body,
                "timestamp": timestamp,
                "timestamp_label": format_chat_preview_timestamp(timestamp),
                "avatar_label": chat_avatar_initials(speaker),
                "ts": ts_raw,
                "thread_ts": thread_ts_raw,
                "reply_count": parse_slack_int(item.get("reply_count")),
                "is_reply": is_reply,
                "is_thread_root": (not is_reply) and slack_message_has_thread(item, ts_raw, thread_ts_raw),
                "day_rel_path": rel_path,
                "day_file_name": day_file.name,
                "ordinal": ordinal,
            }
        )
    return sorted(
        messages,
        key=lambda message: (
            slack_timestamp_sort_key(message.get("ts")),
            int(message.get("ordinal") or 0),
        ),
    )


def build_slack_transcript_components(messages: list[dict[str, object]]) -> dict[str, object]:
    participants: list[str] = []
    seen_participants: set[str] = set()
    transcript_lines: list[str] = []
    chat_entries: list[dict[str, object]] = []
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    timestamped_message_count = 0

    for message in sorted(messages, key=lambda item: (slack_timestamp_sort_key(item.get("ts")), int(item.get("ordinal") or 0))):
        speaker = normalize_whitespace(str(message.get("speaker") or "")) or "Slack message"
        normalized_speaker = speaker.lower()
        if normalized_speaker not in seen_participants:
            seen_participants.add(normalized_speaker)
            participants.append(speaker)
        body = normalize_whitespace(str(message.get("body") or ""))
        timestamp = normalize_whitespace(str(message.get("timestamp") or "")) or None
        if timestamp:
            timestamped_message_count += 1
            if first_timestamp is None:
                first_timestamp = timestamp
            last_timestamp = timestamp
            transcript_lines.append(f"[{timestamp}] {speaker}: {body}")
        else:
            transcript_lines.append(f"{speaker}: {body}")
        chat_entries.append(
            {
                "avatar_color": message.get("avatar_color"),
                "speaker": speaker,
                "body": body,
                "timestamp": timestamp,
                "timestamp_label": message.get("timestamp_label"),
                "avatar_label": message.get("avatar_label"),
            }
        )

    return {
        "participants": ", ".join(participants) or None,
        "text_content": normalize_whitespace("\n".join(transcript_lines)),
        "chat_entries": chat_entries,
        "date_created": first_timestamp,
        "date_modified": last_timestamp if last_timestamp and last_timestamp != first_timestamp else None,
        "message_count": len(chat_entries),
        "timestamped_message_count": timestamped_message_count,
    }


def slack_root_message_key(conversation_key: str, thread_ts: str) -> str:
    return f"{normalize_whitespace(conversation_key) or conversation_key}:{normalize_whitespace(thread_ts) or thread_ts}"


def slack_preview_file_name(file_name: str) -> str:
    return f"{Path(file_name).stem}.html"


def build_slack_chat_payload(
    *,
    title: str,
    preview_file_name: str,
    messages: list[dict[str, object]],
    placeholder_text: str | None = None,
) -> dict[str, object]:
    components = build_slack_transcript_components(messages)
    text_body = str(components["text_content"] or "")
    if not text_body and placeholder_text:
        text_body = normalize_whitespace(placeholder_text)
    chat_metadata = {
        "author": None,
        "participants": components["participants"],
        "date_created": components["date_created"],
        "date_modified": components["date_modified"],
        "title": title,
        "message_count": components["message_count"],
        "timestamped_message_count": components["timestamped_message_count"],
    }
    return build_chat_extracted_payload(
        title=title,
        author=None,
        date_created=components["date_created"],
        text_body=text_body,
        html_body=None,
        attachments=[],
        preview_file_name=preview_file_name,
        chat_metadata=chat_metadata,
        chat_entries=list(components["chat_entries"]),
    )


def build_slack_day_record(
    day_file: Path,
    *,
    rel_path: str,
    conversation_meta: dict[str, str],
    user_directory: dict[str, dict[str, str | None]],
) -> dict[str, object]:
    return {
        "day_file": day_file,
        "rel_path": rel_path,
        "folder_name": day_file.parent.name,
        "conversation_key": conversation_meta["conversation_key"],
        "conversation_type": conversation_meta["conversation_type"],
        "display_name": conversation_meta["display_name"],
        "title": slack_day_document_title(conversation_meta["display_name"], day_file),
        "messages": load_slack_day_messages(
            day_file,
            rel_path=rel_path,
            user_directory=user_directory,
        ),
    }


def build_slack_thread_document_plans(
    conversation_key: str,
    day_records: list[dict[str, object]],
) -> tuple[list[dict[str, object]], set[str]]:
    day_records_by_rel = {
        str(record["rel_path"]): record
        for record in day_records
    }
    root_messages_by_ts: dict[str, dict[str, object]] = {}
    reply_messages_by_thread_ts: dict[str, list[dict[str, object]]] = defaultdict(list)
    candidate_thread_timestamps: set[str] = set()

    for day_record in day_records:
        for message in list(day_record.get("messages", [])):
            ts_raw = normalize_whitespace(str(message.get("ts") or ""))
            if not ts_raw:
                continue
            if bool(message.get("is_reply")):
                thread_ts = normalize_whitespace(str(message.get("thread_ts") or ""))
                if not thread_ts:
                    continue
                reply_messages_by_thread_ts[thread_ts].append(message)
                candidate_thread_timestamps.add(thread_ts)
                continue
            root_messages_by_ts[ts_raw] = message
            if bool(message.get("is_thread_root")):
                candidate_thread_timestamps.add(ts_raw)

    thread_plans: list[dict[str, object]] = []
    materialized_thread_roots: set[str] = set()
    for thread_ts in sorted(candidate_thread_timestamps, key=slack_timestamp_sort_key):
        root_message = root_messages_by_ts.get(thread_ts)
        if root_message is None:
            continue
        root_day_rel_path = str(root_message["day_rel_path"])
        root_day_record = day_records_by_rel[root_day_rel_path]
        replies = sorted(
            reply_messages_by_thread_ts.get(thread_ts, []),
            key=lambda message: (
                slack_timestamp_sort_key(message.get("ts")),
                int(message.get("ordinal") or 0),
            ),
        )
        materialized_thread_roots.add(thread_ts)
        file_name = f"{thread_ts}.slackthread"
        source_parts = [
            {
                "part_kind": "slack_thread_root_day",
                "rel_source_path": root_day_rel_path,
                "ordinal": 0,
                "label": root_day_record["title"],
            }
        ]
        seen_reply_days: set[str] = set()
        part_ordinal = 1
        for reply in replies:
            reply_day_rel_path = normalize_whitespace(str(reply.get("day_rel_path") or ""))
            if not reply_day_rel_path or reply_day_rel_path == root_day_rel_path or reply_day_rel_path in seen_reply_days:
                continue
            seen_reply_days.add(reply_day_rel_path)
            reply_day_record = day_records_by_rel.get(reply_day_rel_path)
            source_parts.append(
                {
                    "part_kind": "slack_thread_reply_day",
                    "rel_source_path": reply_day_rel_path,
                    "ordinal": part_ordinal,
                    "label": reply_day_record["title"] if reply_day_record is not None else Path(reply_day_rel_path).name,
                }
            )
            part_ordinal += 1
        thread_plans.append(
            {
                "rel_path": slack_reply_thread_rel_path(conversation_key, thread_ts),
                "file_name": file_name,
                "preview_file_name": slack_preview_file_name(file_name),
                "title": slack_reply_thread_title(
                    str(root_day_record["display_name"]),
                    str(root_message.get("timestamp") or ""),
                ),
                "messages": [root_message, *replies],
                "parent_rel_path": root_day_rel_path,
                "source_rel_path": root_day_rel_path,
                "source_item_id": thread_ts,
                "source_folder_path": str(root_day_record["folder_name"]),
                "root_message_key": slack_root_message_key(conversation_key, thread_ts),
                "source_parts": source_parts,
            }
        )
    return thread_plans, materialized_thread_roots


def build_slack_day_document_plan(
    day_record: dict[str, object],
    *,
    materialized_thread_roots: set[str],
) -> dict[str, object]:
    visible_messages: list[dict[str, object]] = []
    for message in list(day_record.get("messages", [])):
        if bool(message.get("is_reply")):
            thread_ts = normalize_whitespace(str(message.get("thread_ts") or ""))
            if thread_ts and thread_ts in materialized_thread_roots:
                continue
        visible_messages.append(message)
    file_name = str(day_record["day_file"].name)
    return {
        "rel_path": str(day_record["rel_path"]),
        "file_name": file_name,
        "preview_file_name": slack_preview_file_name(file_name),
        "title": str(day_record["title"]),
        "messages": visible_messages,
        "source_rel_path": str(day_record["rel_path"]),
        "source_item_id": None,
        "source_folder_path": str(day_record["folder_name"]),
        "root_message_key": None,
        "source_parts": [
            {
                "part_kind": "slack_day_file",
                "rel_source_path": str(day_record["rel_path"]),
                "ordinal": 0,
                "label": day_record["title"],
            }
        ],
    }


def remove_source_dataset_membership_for_document(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    source_kind: str,
    source_locator: str,
) -> int:
    dataset_source_row = get_dataset_source_row(
        connection,
        source_kind=source_kind,
        source_locator=source_locator,
    )
    if dataset_source_row is None:
        return 0
    cursor = connection.execute(
        """
        DELETE FROM dataset_documents
        WHERE document_id = ?
          AND dataset_source_id = ?
        """,
        (document_id, int(dataset_source_row["id"])),
    )
    refresh_document_dataset_cache(connection, document_id)
    return int(cursor.rowcount or 0)


def existing_rows_by_rel_path(
    connection: sqlite3.Connection,
    rel_paths: list[str],
) -> dict[str, sqlite3.Row]:
    if not rel_paths:
        return {}
    placeholders = ", ".join("?" for _ in rel_paths)
    rows = connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE rel_path IN ({placeholders})
        ORDER BY id ASC
        """,
        rel_paths,
    ).fetchall()
    return {str(row["rel_path"]): row for row in rows}


def mark_missing_slack_export_documents(
    connection: sqlite3.Connection,
    *,
    dataset_id: int,
    seen_rel_paths: set[str],
) -> int:
    rows = connection.execute(
        """
        SELECT d.id, d.rel_path, d.lifecycle_status
        FROM documents d
        JOIN dataset_documents dd ON dd.document_id = d.id
        WHERE d.source_kind = ?
          AND dd.dataset_id = ?
          AND d.lifecycle_status != 'deleted'
        ORDER BY d.id ASC
        """,
        (SLACK_EXPORT_SOURCE_KIND, dataset_id),
    ).fetchall()
    missing_ids = [
        int(row["id"])
        for row in rows
        if normalize_whitespace(str(row["rel_path"] or "")) not in seen_rel_paths
        and row["lifecycle_status"] != "missing"
    ]
    if not missing_ids:
        return 0
    now = utc_now()
    placeholders = ", ".join("?" for _ in missing_ids)
    connection.execute(
        f"""
        UPDATE documents
        SET lifecycle_status = 'missing', updated_at = ?
        WHERE lifecycle_status != 'deleted'
          AND id IN ({placeholders})
        """,
        [now, *missing_ids],
    )
    return len(missing_ids)


def ingest_slack_export_root(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    export_root: Path,
    *,
    ingestion_batch_number: int | None = None,
) -> dict[str, object]:
    root = paths["root"]
    rel_root = relative_document_path(root, export_root)
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=SLACK_EXPORT_SOURCE_KIND,
        source_locator=rel_root,
        dataset_name=slack_export_dataset_name(rel_root),
    )
    connection.commit()
    conversation_directory = load_slack_export_conversation_directory(export_root)
    user_directory = load_slack_user_directory(export_root)
    day_files = iter_slack_export_day_files(export_root)
    stats = {
        "new": 0,
        "updated": 0,
        "missing": 0,
        "failed": 0,
        "scanned_day_files": len(day_files),
        "conversations": 0,
    }
    failures: list[dict[str, str]] = []
    seen_conversation_keys: set[tuple[str, str, str]] = set()
    current_batch = ingestion_batch_number
    conversation_day_records: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for day_file in day_files:
        rel_path = relative_document_path(root, day_file)
        folder_name = day_file.parent.name
        conversation_meta = slack_conversation_metadata_for_folder(folder_name, conversation_directory)
        conversation_identity = (
            SLACK_EXPORT_SOURCE_KIND,
            rel_root,
            conversation_meta["conversation_key"],
        )
        conversation_day_records[conversation_identity].append(
            build_slack_day_record(
                day_file,
                rel_path=rel_path,
                conversation_meta=conversation_meta,
                user_directory=user_directory,
            )
        )
        if conversation_identity not in seen_conversation_keys:
            seen_conversation_keys.add(conversation_identity)
            stats["conversations"] += 1

    planned_documents: list[dict[str, object]] = []
    for conversation_identity, records in conversation_day_records.items():
        ordered_records = sorted(records, key=lambda record: (str(record["rel_path"]).lower(), str(record["rel_path"])))
        thread_plans, materialized_thread_roots = build_slack_thread_document_plans(
            conversation_identity[2],
            ordered_records,
        )
        for day_record in ordered_records:
            planned_documents.append(
                {
                    "conversation_identity": conversation_identity,
                    "kind": "day",
                    "plan": build_slack_day_document_plan(
                        day_record,
                        materialized_thread_roots=materialized_thread_roots,
                    ),
                    "day_file": day_record["day_file"],
                    "display_name": day_record["display_name"],
                    "conversation_type": day_record["conversation_type"],
                }
            )
        for thread_plan in thread_plans:
            planned_documents.append(
                {
                    "conversation_identity": conversation_identity,
                    "kind": "reply_thread",
                    "plan": thread_plan,
                    "day_file": None,
                    "display_name": ordered_records[0]["display_name"],
                    "conversation_type": ordered_records[0]["conversation_type"],
                }
            )

    existing_by_rel = existing_rows_by_rel_path(
        connection,
        [str(item["plan"]["rel_path"]) for item in planned_documents],
    )
    seen_rel_paths: set[str] = set()
    parent_state_by_rel: dict[str, dict[str, int | str | None]] = {}

    for conversation_identity, records in sorted(conversation_day_records.items()):
        ordered_records = sorted(records, key=lambda record: (str(record["rel_path"]).lower(), str(record["rel_path"])))
        thread_plans, materialized_thread_roots = build_slack_thread_document_plans(
            conversation_identity[2],
            ordered_records,
        )
        day_plans = [
            build_slack_day_document_plan(
                day_record,
                materialized_thread_roots=materialized_thread_roots,
            )
            for day_record in ordered_records
        ]

        connection.execute("BEGIN")
        try:
            conversation_id = upsert_conversation_row(
                connection,
                source_kind=SLACK_EXPORT_SOURCE_KIND,
                source_locator=rel_root,
                conversation_key=conversation_identity[2],
                conversation_type=str(ordered_records[0]["conversation_type"]),
                display_name=str(ordered_records[0]["display_name"]),
            )
            for day_record, plan in zip(ordered_records, day_plans):
                rel_path = str(plan["rel_path"])
                seen_rel_paths.add(rel_path)
                existing_row = existing_by_rel.get(rel_path)
                extracted_payload = build_slack_chat_payload(
                    title=str(plan["title"]),
                    preview_file_name=str(plan["preview_file_name"]),
                    messages=list(plan["messages"]),
                )
                extracted = apply_manual_locks(existing_row, extracted_payload)
                if existing_row is None:
                    if current_batch is None:
                        current_batch = allocate_ingestion_batch_number(connection)
                    control_number_batch = current_batch
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
                    Path(day_record["day_file"]),
                    existing_row,
                    extracted,
                    file_name=str(plan["file_name"]),
                    parent_document_id=None,
                    control_number=control_number,
                    dataset_id=dataset_id,
                    conversation_id=conversation_id,
                    control_number_batch=control_number_batch,
                    control_number_family_sequence=control_number_family_sequence,
                    control_number_attachment_sequence=control_number_attachment_sequence,
                    source_kind=SLACK_EXPORT_SOURCE_KIND,
                    source_rel_path=str(plan["source_rel_path"]),
                    source_item_id=plan["source_item_id"],
                    source_folder_path=str(plan["source_folder_path"]),
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
                remove_source_dataset_membership_for_document(
                    connection,
                    document_id=document_id,
                    source_kind=FILESYSTEM_SOURCE_KIND,
                    source_locator=filesystem_dataset_locator(),
                )
                preview_rows = write_preview_artifacts(paths, rel_path, list(extracted.get("preview_artifacts", [])))
                chunks = extracted_search_chunks(extracted)
                replace_document_related_rows(connection, document_id, extracted | {"file_name": str(plan["file_name"])}, chunks, preview_rows)
                replace_document_source_parts(
                    connection,
                    document_id,
                    list(plan["source_parts"]),
                )
                parent_state_by_rel[rel_path] = {
                    "document_id": document_id,
                    "control_number_batch": control_number_batch,
                    "control_number_family_sequence": control_number_family_sequence,
                }
                if existing_row is None:
                    stats["new"] += 1
                else:
                    stats["updated"] += 1

            for thread_plan in thread_plans:
                rel_path = str(thread_plan["rel_path"])
                seen_rel_paths.add(rel_path)
                existing_row = existing_by_rel.get(rel_path)
                parent_state = parent_state_by_rel[str(thread_plan["parent_rel_path"])]
                parent_document_id = int(parent_state["document_id"])
                extracted_payload = build_slack_chat_payload(
                    title=str(thread_plan["title"]),
                    preview_file_name=str(thread_plan["preview_file_name"]),
                    messages=list(thread_plan["messages"]),
                )
                extracted = apply_manual_locks(existing_row, extracted_payload)
                if existing_row is None:
                    control_number_batch = int(parent_state["control_number_batch"])
                    control_number_family_sequence = int(parent_state["control_number_family_sequence"])
                    control_number_attachment_sequence = next_attachment_sequence(connection, parent_document_id)
                    control_number = format_control_number(
                        control_number_batch,
                        control_number_family_sequence,
                        control_number_attachment_sequence,
                    )
                else:
                    control_number_batch = int(existing_row["control_number_batch"])
                    control_number_family_sequence = int(existing_row["control_number_family_sequence"])
                    control_number_attachment_sequence = int(existing_row["control_number_attachment_sequence"])
                    control_number = str(existing_row["control_number"])
                    cleanup_document_artifacts(paths, connection, existing_row)
                document_id = upsert_document_row(
                    connection,
                    rel_path,
                    None,
                    existing_row,
                    extracted,
                    file_name=str(thread_plan["file_name"]),
                    parent_document_id=parent_document_id,
                    child_document_kind=CHILD_DOCUMENT_KIND_REPLY_THREAD,
                    control_number=control_number,
                    dataset_id=dataset_id,
                    conversation_id=conversation_id,
                    control_number_batch=control_number_batch,
                    control_number_family_sequence=control_number_family_sequence,
                    control_number_attachment_sequence=control_number_attachment_sequence,
                    root_message_key=str(thread_plan["root_message_key"]),
                    source_kind=SLACK_EXPORT_SOURCE_KIND,
                    source_rel_path=str(thread_plan["source_rel_path"]),
                    source_item_id=str(thread_plan["source_item_id"]),
                    source_folder_path=str(thread_plan["source_folder_path"]),
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
                remove_source_dataset_membership_for_document(
                    connection,
                    document_id=document_id,
                    source_kind=FILESYSTEM_SOURCE_KIND,
                    source_locator=filesystem_dataset_locator(),
                )
                preview_rows = write_preview_artifacts(paths, rel_path, list(extracted.get("preview_artifacts", [])))
                chunks = extracted_search_chunks(extracted)
                replace_document_related_rows(connection, document_id, extracted | {"file_name": str(thread_plan["file_name"])}, chunks, preview_rows)
                replace_document_source_parts(
                    connection,
                    document_id,
                    list(thread_plan["source_parts"]),
                )
                if existing_row is None:
                    stats["new"] += 1
                else:
                    stats["updated"] += 1
            connection.commit()
        except Exception as exc:
            connection.rollback()
            stats["failed"] += 1
            failures.append(
                {
                    "rel_path": ", ".join(str(plan["rel_path"]) for plan in [*day_plans, *thread_plans]),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    connection.execute("BEGIN")
    try:
        stats["missing"] = mark_missing_slack_export_documents(
            connection,
            dataset_id=dataset_id,
            seen_rel_paths=seen_rel_paths,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return {
        **stats,
        "dataset_id": dataset_id,
        "dataset_source_id": dataset_source_id,
        "source_locator": rel_root,
        "ingestion_batch_number": current_batch,
        "failures": failures,
    }
