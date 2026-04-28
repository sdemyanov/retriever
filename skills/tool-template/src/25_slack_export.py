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


def slack_actor_entity_hint(
    actor_info: dict[str, str | None],
    *,
    identifier_scope: str | None,
) -> dict[str, object] | None:
    slack_user_id = normalize_whitespace(str(actor_info.get("slack_user_id") or ""))
    speaker_name = normalize_entity_text(actor_info.get("speaker_name") or "")
    if not slack_user_id or not speaker_name:
        return None
    identifier: dict[str, object] = {
        "identifier_type": "external_id",
        "identifier_name": "slack_user_id",
        "display_value": slack_user_id,
        "normalized_value": normalize_entity_lookup_text(slack_user_id),
        "is_verified": 1,
    }
    normalized_scope = normalize_entity_lookup_text(identifier_scope or "")
    if normalized_scope:
        identifier["identifier_scope"] = normalized_scope
    return {
        "display_value": speaker_name,
        "identifiers": [identifier],
    }


def load_slack_day_messages(
    day_file: Path,
    *,
    rel_path: str,
    user_directory: dict[str, dict[str, str | None]],
    entity_identifier_scope: str | None = None,
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
        entity_hint = slack_actor_entity_hint(
            actor_info,
            identifier_scope=entity_identifier_scope,
        )
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
                "entity_hint": entity_hint,
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
    participant_entity_hints: list[dict[str, object]] = []
    seen_participant_hint_keys: set[str] = set()
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
        entity_hint = message.get("entity_hint")
        if isinstance(entity_hint, dict):
            hint_key = normalize_entity_lookup_text(entity_hint.get("display_value") or "")
            for identifier in list(entity_hint.get("identifiers") or []):
                if isinstance(identifier, dict) and identifier.get("identifier_type") == "external_id":
                    hint_key = "|".join(
                        [
                            "external_id",
                            normalize_entity_lookup_text(identifier.get("identifier_name") or ""),
                            normalize_entity_lookup_text(identifier.get("identifier_scope") or ""),
                            normalize_entity_lookup_text(identifier.get("normalized_value") or identifier.get("display_value") or ""),
                        ]
                    )
                    break
            if hint_key and hint_key not in seen_participant_hint_keys:
                seen_participant_hint_keys.add(hint_key)
                participant_entity_hints.append(entity_hint)
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
        "entity_hints": {"participants": participant_entity_hints} if participant_entity_hints else {},
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
    payload = build_chat_extracted_payload(
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
    if components.get("entity_hints"):
        payload["entity_hints"] = components["entity_hints"]
    return payload


def build_slack_day_record(
    day_file: Path,
    *,
    rel_path: str,
    conversation_meta: dict[str, str],
    user_directory: dict[str, dict[str, str | None]],
    entity_identifier_scope: str | None = None,
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
            entity_identifier_scope=entity_identifier_scope,
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


def plan_slack_export_conversations(
    root: Path,
    export_root: Path,
    *,
    conversation_directory: dict[str, dict[str, str]],
    user_directory: dict[str, dict[str, str | None]],
    day_files: list[Path],
) -> list[dict[str, object]]:
    rel_root = relative_document_path(root, export_root)
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
                entity_identifier_scope=rel_root,
            )
        )

    conversation_plans: list[dict[str, object]] = []
    for conversation_identity, records in sorted(conversation_day_records.items()):
        ordered_records = sorted(records, key=lambda record: (str(record["rel_path"]).lower(), str(record["rel_path"])))
        if not ordered_records:
            continue
        thread_plans, materialized_thread_roots = build_slack_thread_document_plans(
            conversation_identity[2],
            ordered_records,
        )
        day_documents: list[dict[str, object]] = []
        rel_paths: list[str] = []
        for day_record in ordered_records:
            day_plan = build_slack_day_document_plan(
                day_record,
                materialized_thread_roots=materialized_thread_roots,
            )
            day_documents.append(
                {
                    "kind": "day",
                    "plan": day_plan,
                    "source_path": Path(day_record["day_file"]),
                }
            )
            rel_paths.append(str(day_plan["rel_path"]))
        thread_documents: list[dict[str, object]] = []
        for thread_plan in thread_plans:
            thread_documents.append(
                {
                    "kind": "reply_thread",
                    "plan": thread_plan,
                }
            )
            rel_paths.append(str(thread_plan["rel_path"]))
        conversation_plans.append(
            {
                "conversation_identity": conversation_identity,
                "conversation_key": conversation_identity[2],
                "conversation_type": str(ordered_records[0]["conversation_type"]),
                "display_name": str(ordered_records[0]["display_name"]),
                "day_documents": day_documents,
                "thread_documents": thread_documents,
                "rel_paths": rel_paths,
            }
        )
    return conversation_plans


def prepare_slack_document_plan(document_plan: dict[str, object]) -> dict[str, object]:
    prepared_document = dict(document_plan)
    prepared_plan = dict(document_plan.get("plan") or {})
    prepare_started = time.perf_counter()
    try:
        extracted_payload = build_slack_chat_payload(
            title=str(prepared_plan["title"]),
            preview_file_name=str(prepared_plan["preview_file_name"]),
            messages=list(prepared_plan.get("messages", [])),
        )
        chunk_started = time.perf_counter()
        prepared_chunks = extracted_search_chunks(extracted_payload)
        prepared_document["plan"] = prepared_plan
        prepared_document["extracted_payload"] = extracted_payload
        prepared_document["prepared_chunks"] = prepared_chunks
        prepared_document["prepare_chunk_ms"] = (time.perf_counter() - chunk_started) * 1000.0
        prepared_document["prepare_error"] = None
    except Exception as exc:
        prepared_document["plan"] = prepared_plan
        prepared_document["extracted_payload"] = None
        prepared_document["prepared_chunks"] = []
        prepared_document["prepare_chunk_ms"] = 0.0
        prepared_document["prepare_error"] = f"{type(exc).__name__}: {exc}"
    prepared_document["prepare_ms"] = (time.perf_counter() - prepare_started) * 1000.0
    return prepared_document


def prepare_slack_conversation_plan(conversation_plan: dict[str, object]) -> dict[str, object]:
    prepared_conversation = dict(conversation_plan)
    prepare_started = time.perf_counter()
    prepared_day_documents: list[dict[str, object]] = []
    prepared_thread_documents: list[dict[str, object]] = []
    prepare_error: str | None = None
    prepare_chunk_ms = 0.0
    for document_plan in list(conversation_plan.get("day_documents") or []):
        prepared_document = prepare_slack_document_plan(document_plan)
        prepared_day_documents.append(prepared_document)
        prepare_chunk_ms += float(prepared_document.get("prepare_chunk_ms") or 0.0)
        if prepare_error is None and prepared_document.get("prepare_error"):
            prepare_error = f"{prepared_document['plan'].get('rel_path')}: {prepared_document['prepare_error']}"
    for document_plan in list(conversation_plan.get("thread_documents") or []):
        prepared_document = prepare_slack_document_plan(document_plan)
        prepared_thread_documents.append(prepared_document)
        prepare_chunk_ms += float(prepared_document.get("prepare_chunk_ms") or 0.0)
        if prepare_error is None and prepared_document.get("prepare_error"):
            prepare_error = f"{prepared_document['plan'].get('rel_path')}: {prepared_document['prepare_error']}"
    prepared_conversation["day_documents"] = prepared_day_documents
    prepared_conversation["thread_documents"] = prepared_thread_documents
    prepared_conversation["prepare_chunk_ms"] = prepare_chunk_ms
    prepared_conversation["prepare_error"] = prepare_error
    prepared_conversation["prepare_ms"] = (time.perf_counter() - prepare_started) * 1000.0
    return prepared_conversation


def iter_prepared_slack_conversation_plans(
    conversation_plans: list[dict[str, object]],
    staging_root: Path | None = None,
) -> Iterator[tuple[dict[str, object], float]]:
    effective_staging_root = staging_root
    if staging_root is not None and conversation_plans:
        effective_staging_root = (
            Path(staging_root)
            / "slack"
            / sanitize_storage_filename(str(conversation_plans[0]["conversation_identity"][1]))
        )
    yield from iter_staged_prepared_items(
        conversation_plans,
        prepare_item=prepare_slack_conversation_plan,
        config_benchmark_name="ingest_slack_prepare_config",
        queue_done_benchmark_name="ingest_slack_prepare_queue_done",
        spill_subdir_name="prepared-slack-conversations",
        staging_root=effective_staging_root,
    )


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


def commit_prepared_slack_conversation(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    prepared_conversation: dict[str, object],
    existing_by_rel: dict[str, sqlite3.Row],
    *,
    dataset_id: int,
    dataset_source_id: int,
    current_batch: int | None,
    before_transaction_commit=None,
) -> dict[str, object]:
    prepare_error = prepared_conversation.get("prepare_error")
    if prepare_error:
        return {
            "status": "failed",
            "action": "failed",
            "current_batch": current_batch,
            "rel_paths": list(prepared_conversation.get("rel_paths") or []),
            "error": str(prepare_error),
        }

    rel_paths = list(prepared_conversation.get("rel_paths") or [])
    affected_document_ids: list[int] = []
    parent_state_by_rel: dict[str, dict[str, int]] = {}
    connection.execute("BEGIN")
    try:
        conversation_id = upsert_conversation_row(
            connection,
            source_kind=SLACK_EXPORT_SOURCE_KIND,
            source_locator=str(prepared_conversation["conversation_identity"][1]),
            conversation_key=str(prepared_conversation["conversation_key"]),
            conversation_type=str(prepared_conversation["conversation_type"]),
            display_name=str(prepared_conversation["display_name"]),
        )
        new_count = 0
        updated_count = 0

        for prepared_document in list(prepared_conversation.get("day_documents") or []):
            plan = dict(prepared_document.get("plan") or {})
            rel_path = str(plan["rel_path"])
            existing_row = existing_by_rel.get(rel_path)
            extracted = apply_manual_locks(existing_row, dict(prepared_document.get("extracted_payload") or {}))
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
                Path(prepared_document["source_path"]),
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
            affected_document_ids.append(int(document_id))
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
            replace_document_related_rows(
                connection,
                document_id,
                extracted | {"file_name": str(plan["file_name"])},
                list(prepared_document.get("prepared_chunks", [])),
                preview_rows,
            )
            replace_document_source_parts(
                connection,
                document_id,
                list(plan["source_parts"]),
            )
            parent_state_by_rel[rel_path] = {
                "document_id": document_id,
                "control_number_batch": int(control_number_batch),
                "control_number_family_sequence": int(control_number_family_sequence),
            }
            if existing_row is None:
                new_count += 1
            else:
                updated_count += 1

        for prepared_document in list(prepared_conversation.get("thread_documents") or []):
            plan = dict(prepared_document.get("plan") or {})
            rel_path = str(plan["rel_path"])
            existing_row = existing_by_rel.get(rel_path)
            parent_state = parent_state_by_rel[str(plan["parent_rel_path"])]
            parent_document_id = int(parent_state["document_id"])
            extracted = apply_manual_locks(existing_row, dict(prepared_document.get("extracted_payload") or {}))
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
                file_name=str(plan["file_name"]),
                parent_document_id=parent_document_id,
                child_document_kind=CHILD_DOCUMENT_KIND_REPLY_THREAD,
                control_number=control_number,
                dataset_id=dataset_id,
                conversation_id=conversation_id,
                control_number_batch=control_number_batch,
                control_number_family_sequence=control_number_family_sequence,
                control_number_attachment_sequence=control_number_attachment_sequence,
                root_message_key=str(plan["root_message_key"]),
                source_kind=SLACK_EXPORT_SOURCE_KIND,
                source_rel_path=str(plan["source_rel_path"]),
                source_item_id=str(plan["source_item_id"]),
                source_folder_path=str(plan["source_folder_path"]),
            )
            affected_document_ids.append(int(document_id))
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
            replace_document_related_rows(
                connection,
                document_id,
                extracted | {"file_name": str(plan["file_name"])},
                list(prepared_document.get("prepared_chunks", [])),
                preview_rows,
            )
            replace_document_source_parts(
                connection,
                document_id,
                list(plan["source_parts"]),
            )
            if existing_row is None:
                new_count += 1
            else:
                updated_count += 1
        result = {
            "status": "ok",
            "action": "committed",
            "current_batch": current_batch,
            "new": new_count,
            "updated": updated_count,
            "affected_document_ids": affected_document_ids,
            "rel_paths": rel_paths,
            "source_locator": str(prepared_conversation["conversation_identity"][1]),
            "conversation_key": str(prepared_conversation["conversation_key"]),
        }
        if before_transaction_commit is not None:
            before_transaction_commit(connection, result)
        connection.commit()
        return result
    except Exception as exc:
        connection.rollback()
        return {
            "status": "failed",
            "action": "failed",
            "current_batch": current_batch,
            "rel_paths": rel_paths,
            "error": f"{type(exc).__name__}: {exc}",
        }


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
    staging_root: Path | None = None,
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
    current_batch = ingestion_batch_number
    conversation_plans = plan_slack_export_conversations(
        root,
        export_root,
        conversation_directory=conversation_directory,
        user_directory=user_directory,
        day_files=day_files,
    )
    stats["conversations"] = len(conversation_plans)
    existing_by_rel = existing_rows_by_rel_path(
        connection,
        [rel_path for conversation_plan in conversation_plans for rel_path in list(conversation_plan.get("rel_paths") or [])],
    )
    seen_rel_paths: set[str] = {
        rel_path
        for conversation_plan in conversation_plans
        for rel_path in list(conversation_plan.get("rel_paths") or [])
    }
    prepare_ms = 0.0
    prepare_chunk_ms = 0.0
    prepare_wait_ms = 0.0
    commit_ms = 0.0
    conversation_loop_started = time.perf_counter()
    for prepared_conversation, wait_ms in iter_prepared_slack_conversation_plans(
        conversation_plans,
        staging_root=staging_root,
    ):
        prepare_wait_ms += wait_ms
        prepare_ms += float(prepared_conversation.get("prepare_ms") or 0.0)
        prepare_chunk_ms += float(prepared_conversation.get("prepare_chunk_ms") or 0.0)
        commit_started = time.perf_counter()
        commit_result = commit_prepared_slack_conversation(
            connection,
            paths,
            prepared_conversation,
            existing_by_rel,
            dataset_id=dataset_id,
            dataset_source_id=dataset_source_id,
            current_batch=current_batch,
        )
        commit_ms += (time.perf_counter() - commit_started) * 1000.0
        current_batch = commit_result["current_batch"]
        if commit_result["status"] == "failed":
            stats["failed"] += 1
            failures.append(
                {
                    "rel_path": ", ".join(commit_result.get("rel_paths") or list(prepared_conversation.get("rel_paths") or [])),
                    "error": str(commit_result.get("error") or "Unknown slack export ingest failure."),
                }
            )
            continue
        stats["new"] += int(commit_result["new"])
        stats["updated"] += int(commit_result["updated"])
    benchmark_mark(
        "ingest_slack_conversations_done",
        conversation_count=len(conversation_plans),
        conversation_loop_ms=round((time.perf_counter() - conversation_loop_started) * 1000.0, 3),
        prepare_ms=round(prepare_ms, 3),
        prepare_chunk_ms=round(prepare_chunk_ms, 3),
        prepare_wait_ms=round(prepare_wait_ms, 3),
        commit_ms=round(commit_ms, 3),
        new=stats["new"],
        updated=stats["updated"],
        failed=stats["failed"],
    )

    missing_started = time.perf_counter()
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
    benchmark_mark(
        "ingest_slack_missing_done",
        missing_ms=round((time.perf_counter() - missing_started) * 1000.0, 3),
        missing=stats["missing"],
    )

    return {
        **stats,
        "dataset_id": dataset_id,
        "dataset_source_id": dataset_source_id,
        "source_locator": rel_root,
        "ingestion_batch_number": current_batch,
        "failures": failures,
    }


GMAIL_EXPORT_ARCHIVE_BROWSER_FILE = "archive_browser.html"
GMAIL_EXPORT_DRIVE_FOLDER_PATTERN = re.compile(r"_Drive_Link_Export_\d+$", re.IGNORECASE)
GMAIL_EXPORT_FILE_DRIVE_ID_PATTERN = re.compile(r"^(?P<base>.+)_(?P<drive_id>[A-Za-z0-9_-]{10,})$")
GMAIL_METADATA_REQUIRED_HEADERS = {
    "Rfc822MessageId",
    "GmailMessageId",
    "Labels",
    "Account",
    "Subject",
}
GMAIL_DRIVE_LINKS_REQUIRED_HEADERS = {
    "Rfc822MessageId",
    "DriveUrl",
    "DriveItemId",
}
PST_EXPORT_STANDARD_RESULTS_REQUIRED_HEADERS = {
    "Document ID",
    "Item Identity",
    "Target Path",
}
PST_EXPORT_ARCHIVE_RESULTS_REQUIRED_HEADERS = {
    "Document ID",
    "Export Item Id",
    "Export Item Path",
}
PST_EXPORT_RESULTS_FILE_PATTERN = re.compile(r"^results\.csv$", re.IGNORECASE)
PST_EXPORT_ARCHIVE_RESULTS_FILE_PATTERN = re.compile(r"^export_results.*\.csv$", re.IGNORECASE)
PST_EXPORT_SUMMARY_FILE_PATTERN = re.compile(r"^export summary .+\.csv$", re.IGNORECASE)
PST_EXPORT_MANIFEST_FILE_PATTERN = re.compile(r"^manifest\.xml$", re.IGNORECASE)
PST_EXPORT_TRACE_LOG_FILE_PATTERN = re.compile(r"^trace\.log$", re.IGNORECASE)


def load_normalized_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    try:
        decoded, _, _ = decode_bytes(path.read_bytes())
    except OSError:
        return [], []
    try:
        reader = csv.DictReader(io.StringIO(decoded))
    except csv.Error:
        return [], []
    headers = [normalize_inline_whitespace(str(header or "")) for header in (reader.fieldnames or []) if header]
    rows: list[dict[str, str]] = []
    for raw_row in reader:
        if not isinstance(raw_row, dict):
            continue
        normalized_row: dict[str, str] = {}
        for key, value in raw_row.items():
            normalized_key = normalize_inline_whitespace(str(key or ""))
            if not normalized_key:
                continue
            normalized_row[normalized_key] = normalize_whitespace(str(value or "")) if value is not None else ""
        if normalized_row:
            rows.append(normalized_row)
    return headers, rows


def csv_has_required_headers(path: Path, required_headers: set[str]) -> bool:
    headers, _ = load_normalized_csv_rows(path)
    return required_headers.issubset(set(headers))


def load_gmail_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    return load_normalized_csv_rows(path)


def gmail_csv_has_required_headers(path: Path, required_headers: set[str]) -> bool:
    return csv_has_required_headers(path, required_headers)


def gmail_metadata_csv_valid(path: Path) -> bool:
    return path.is_file() and gmail_csv_has_required_headers(path, GMAIL_METADATA_REQUIRED_HEADERS)


def gmail_drive_links_csv_valid(path: Path) -> bool:
    return path.is_file() and gmail_csv_has_required_headers(path, GMAIL_DRIVE_LINKS_REQUIRED_HEADERS)


def gmail_csv_list_values(value: object) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for raw_part in str(value or "").split(","):
        normalized = normalize_whitespace(raw_part)
        if not normalized or normalized in seen:
            continue
        values.append(normalized)
        seen.add(normalized)
    return values


def gmail_normalized_message_lookup_key(value: object) -> str | None:
    return normalize_email_message_id(value)


def gmail_normalized_drive_item_id(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or ""))
    return normalized or None


def gmail_drive_item_id_from_export_file_name(path: Path) -> str | None:
    stem = normalize_whitespace(path.stem)
    if "_" not in stem:
        return None
    match = GMAIL_EXPORT_FILE_DRIVE_ID_PATTERN.fullmatch(stem)
    if match is None:
        return None
    return gmail_normalized_drive_item_id(match.group("drive_id"))


def gmail_drive_export_files(export_dirs: list[Path]) -> list[Path]:
    files: list[Path] = []
    for export_dir in export_dirs:
        try:
            iterator = export_dir.rglob("*")
        except OSError:
            continue
        for path in iterator:
            if path.is_dir() or ".retriever" in path.parts:
                continue
            file_type = normalize_extension(path)
            if not file_type or file_type not in SUPPORTED_FILE_TYPES:
                continue
            files.append(path)
    return sorted(files)


def parse_gmail_metadata_csv(paths: list[Path]) -> dict[str, dict[str, object]]:
    metadata_by_message_id: dict[str, dict[str, object]] = {}
    for path in paths:
        _, rows = load_gmail_csv_rows(path)
        for row in rows:
            message_id = gmail_normalized_message_lookup_key(row.get("Rfc822MessageId"))
            if not message_id:
                continue
            recipients = ", ".join(
                part
                for part in [row.get("To"), row.get("CC"), row.get("BCC")]
                if normalize_whitespace(str(part or ""))
            )
            metadata_by_message_id[message_id] = {
                "account": normalize_whitespace(str(row.get("Account") or "")) or None,
                "bcc": normalize_whitespace(str(row.get("BCC") or "")) or None,
                "cc": normalize_whitespace(str(row.get("CC") or "")) or None,
                "date_received": normalize_date_field_value(row.get("DateReceived")),
                "date_sent": normalize_date_field_value(row.get("DateSent")),
                "file_name": normalize_whitespace(str(row.get("FileName") or "")) or None,
                "from": normalize_whitespace(str(row.get("From") or "")) or None,
                "gmail_message_id": gmail_normalized_drive_item_id(row.get("GmailMessageId")),
                "labels": gmail_csv_list_values(row.get("Labels")),
                "subject": normalize_generated_document_title(row.get("Subject")),
                "threaded_message_count": normalize_whitespace(str(row.get("ThreadedMessageCount") or "")) or None,
                "to": normalize_whitespace(str(row.get("To") or "")) or None,
                "recipients": recipients or None,
            }
    return metadata_by_message_id


def parse_gmail_drive_links_csv(paths: list[Path]) -> dict[str, list[dict[str, str]]]:
    links_by_message_id: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for path in paths:
        _, rows = load_gmail_csv_rows(path)
        for row in rows:
            message_id = gmail_normalized_message_lookup_key(row.get("Rfc822MessageId"))
            drive_item_id = gmail_normalized_drive_item_id(row.get("DriveItemId"))
            if not message_id or not drive_item_id:
                continue
            drive_url = html.unescape(normalize_whitespace(str(row.get("DriveUrl") or ""))) or ""
            marker = (drive_item_id, drive_url)
            if marker in seen[message_id]:
                continue
            links_by_message_id[message_id].append(
                {
                    "drive_item_id": drive_item_id,
                    "drive_url": drive_url,
                }
            )
            seen[message_id].add(marker)
    return links_by_message_id


def parse_gmail_drive_export_metadata(
    paths: list[Path],
    *,
    file_paths_by_name: dict[str, Path],
) -> dict[str, dict[str, object]]:
    metadata_by_drive_item_id: dict[str, dict[str, object]] = {}
    for path in paths:
        try:
            root = parse_xml_document(path.read_bytes())
        except (OSError, ET.ParseError):
            continue
        for document in root.findall(".//Document"):
            drive_item_id = gmail_normalized_drive_item_id(document.attrib.get("DocID"))
            if not drive_item_id:
                continue
            tag_values: dict[str, str] = {}
            for tag in document.findall("./Tags/Tag"):
                tag_name = normalize_inline_whitespace(str(tag.attrib.get("TagName") or ""))
                if not tag_name:
                    continue
                tag_values[tag_name] = normalize_whitespace(str(tag.attrib.get("TagValue") or ""))
            external_file = document.find(".//ExternalFile")
            file_name = normalize_whitespace(str(external_file.attrib.get("FileName") or "")) if external_file is not None else ""
            file_path = file_paths_by_name.get(file_name)
            metadata_by_drive_item_id[drive_item_id] = {
                "author": normalize_whitespace(str(tag_values.get("#Author") or "")) or None,
                "collaborators": gmail_csv_list_values(tag_values.get("Collaborators")),
                "date_created": normalize_date_field_value(tag_values.get("#DateCreated")),
                "date_modified": normalize_date_field_value(tag_values.get("#DateModified")),
                "document_type": normalize_whitespace(str(tag_values.get("DocumentType") or "")) or None,
                "drive_item_id": drive_item_id,
                "file_name": file_name or (file_path.name if file_path is not None else None),
                "file_path": file_path,
                "others": gmail_csv_list_values(tag_values.get("Others")),
                "source_hash": normalize_whitespace(str(tag_values.get("SourceHash") or "")) or None,
                "title": normalize_generated_document_title(tag_values.get("#Title")) or None,
                "viewers": gmail_csv_list_values(tag_values.get("Viewers")),
            }
    return metadata_by_drive_item_id


def parse_gmail_drive_export_errors(paths: list[Path]) -> dict[str, str]:
    errors_by_drive_item_id: dict[str, str] = {}
    for path in paths:
        _, rows = load_gmail_csv_rows(path)
        for row in rows:
            drive_item_id = (
                gmail_normalized_drive_item_id(row.get("Drive Document ID"))
                or gmail_normalized_drive_item_id(row.get("Document ID"))
            )
            if not drive_item_id:
                continue
            description = normalize_whitespace(str(row.get("Error Description") or ""))
            if description:
                errors_by_drive_item_id[drive_item_id] = description
    return errors_by_drive_item_id


def gmail_drive_document_participants(record: dict[str, object]) -> list[str]:
    values: list[str] = []
    for value in [
        record.get("author"),
        *list(record.get("collaborators") or []),
        *list(record.get("viewers") or []),
        *list(record.get("others") or []),
    ]:
        normalized = normalize_whitespace(str(value or ""))
        if normalized:
            values.append(normalized)
    return sorted_unique_display_names(values)


def gmail_drive_document_title(record: dict[str, object]) -> str | None:
    for value in (
        record.get("title"),
        record.get("file_name"),
    ):
        normalized = normalize_generated_document_title(value)
        if normalized:
            return normalized
    return None


def gmail_drive_document_link_summary(record: dict[str, object]) -> dict[str, object]:
    return {
        "author": normalize_whitespace(str(record.get("author") or "")) or None,
        "drive_item_id": gmail_normalized_drive_item_id(record.get("drive_item_id")),
        "error": normalize_whitespace(str(record.get("error") or "")) or None,
        "file_name": normalize_whitespace(str(record.get("file_name") or "")) or None,
        "title": gmail_drive_document_title(record),
    }


def gmail_drive_attachment_payload(record: dict[str, object]) -> dict[str, object] | None:
    file_path = record.get("file_path")
    if not isinstance(file_path, Path) or not file_path.exists() or file_path.is_dir():
        return None
    try:
        payload = file_path.read_bytes()
    except OSError:
        return None
    file_name = normalize_whitespace(str(record.get("file_name") or "")) or file_path.name
    return {
        "file_name": file_name,
        "payload": payload,
        "file_hash": sha256_bytes(payload),
        "gmail_drive_record": dict(record),
    }


def gmail_drive_record_preference_key(record: dict[str, object]) -> tuple[int, int, int, int, int, int]:
    drive_item_id = normalize_whitespace(str(record.get("drive_item_id") or ""))
    return (
        1 if list(record.get("linked_message_ids") or []) else 0,
        1 if gmail_drive_document_title(record) else 0,
        1 if normalize_whitespace(str(record.get("author") or "")) else 0,
        1 if normalize_whitespace(str(record.get("date_created") or "")) else 0,
        1 if normalize_whitespace(str(record.get("date_modified") or "")) else 0,
        len(drive_item_id),
    )


def append_extracted_search_context(
    extracted: dict[str, object],
    extra_sections: list[str],
) -> dict[str, object]:
    normalized_sections = [
        normalize_whitespace(section)
        for section in extra_sections
        if normalize_whitespace(section)
    ]
    if not normalized_sections:
        return dict(extracted)
    merged = dict(extracted)
    chunks = list(extracted_search_chunks(merged))
    next_chunk_index = max((int(chunk["chunk_index"]) for chunk in chunks), default=-1) + 1
    for section in normalized_sections:
        chunks.append(
            {
                "chunk_index": next_chunk_index,
                "char_start": 0,
                "char_end": len(section),
                "token_estimate": token_estimate(section),
                "text_content": section,
            }
        )
        next_chunk_index += 1
    merged["chunks"] = chunks
    if merged.get("text_status") == "empty":
        merged["text_status"] = "ok"
    return merged


def gmail_append_search_context(
    extracted: dict[str, object],
    extra_sections: list[str],
) -> dict[str, object]:
    return append_extracted_search_context(extracted, extra_sections)


def gmail_email_metadata_search_text(
    message_metadata: dict[str, object] | None,
    linked_drive_records: list[dict[str, object]],
) -> str | None:
    lines: list[str] = []
    if message_metadata:
        lines.append("Gmail export metadata")
        account = normalize_whitespace(str(message_metadata.get("account") or ""))
        if account:
            lines.append(f"Account: {account}")
        gmail_message_id = normalize_whitespace(str(message_metadata.get("gmail_message_id") or ""))
        if gmail_message_id:
            lines.append(f"Gmail message ID: {gmail_message_id}")
        labels = [label for label in list(message_metadata.get("labels") or []) if normalize_whitespace(str(label or ""))]
        if labels:
            lines.append(f"Labels: {', '.join(labels)}")
        date_received = normalize_whitespace(str(message_metadata.get("date_received") or ""))
        if date_received:
            lines.append(f"Date received: {date_received}")
        threaded_count = normalize_whitespace(str(message_metadata.get("threaded_message_count") or ""))
        if threaded_count:
            lines.append(f"Threaded message count: {threaded_count}")
    if linked_drive_records:
        lines.append("Linked Google Drive items")
        for record in linked_drive_records[:20]:
            title = normalize_whitespace(str(record.get("title") or record.get("file_name") or record.get("drive_item_id") or ""))
            if not title:
                continue
            details = [
                f"Drive ID: {record['drive_item_id']}"
                for _ in [record.get("drive_item_id")]
                if normalize_whitespace(str(record.get("drive_item_id") or ""))
            ]
            author = normalize_whitespace(str(record.get("author") or ""))
            if author:
                details.append(f"Author: {author}")
            error_text = normalize_whitespace(str(record.get("error") or ""))
            if error_text:
                details.append(f"Export error: {error_text}")
            lines.append(f"- {title}" + (f" ({'; '.join(details)})" if details else ""))
    normalized = normalize_whitespace("\n".join(lines))
    return normalized or None


def gmail_drive_document_search_text(record: dict[str, object]) -> str | None:
    lines: list[str] = ["Gmail Drive export metadata"]
    drive_item_id = normalize_whitespace(str(record.get("drive_item_id") or ""))
    if drive_item_id:
        lines.append(f"Drive item ID: {drive_item_id}")
    title = gmail_drive_document_title(record)
    if title:
        lines.append(f"Title: {title}")
    author = normalize_whitespace(str(record.get("author") or ""))
    if author:
        lines.append(f"Author: {author}")
    collaborators = [value for value in list(record.get("collaborators") or []) if normalize_whitespace(str(value or ""))]
    if collaborators:
        lines.append(f"Collaborators: {', '.join(collaborators)}")
    viewers = [value for value in list(record.get("viewers") or []) if normalize_whitespace(str(value or ""))]
    if viewers:
        lines.append(f"Viewers: {', '.join(viewers)}")
    others = [value for value in list(record.get("others") or []) if normalize_whitespace(str(value or ""))]
    if others:
        lines.append(f"Others: {', '.join(others)}")
    linked_subjects = [
        normalize_generated_document_title(subject)
        for subject in list(record.get("linked_subjects") or [])
        if normalize_generated_document_title(subject)
    ]
    if linked_subjects:
        lines.append("Linked from Gmail messages")
        for subject in linked_subjects[:20]:
            lines.append(f"- {subject}")
    error_text = normalize_whitespace(str(record.get("error") or ""))
    if error_text:
        lines.append(f"Export error: {error_text}")
    normalized = normalize_whitespace("\n".join(lines))
    return normalized or None


def apply_gmail_email_export_metadata(
    extracted: dict[str, object],
    *,
    message_metadata: dict[str, object] | None,
    linked_drive_records: list[dict[str, object]],
) -> dict[str, object]:
    enriched = dict(extracted)
    if message_metadata:
        if not normalize_whitespace(str(enriched.get("date_created") or "")) and message_metadata.get("date_sent"):
            enriched["date_created"] = message_metadata.get("date_sent")
        subject = normalize_generated_document_title(message_metadata.get("subject"))
        if subject and not normalize_whitespace(str(enriched.get("title") or "")):
            enriched["title"] = subject
        if subject and not normalize_whitespace(str(enriched.get("subject") or "")):
            enriched["subject"] = subject
        author = normalize_whitespace(str(message_metadata.get("from") or ""))
        if author and not normalize_whitespace(str(enriched.get("author") or "")):
            enriched["author"] = author
        recipients = normalize_whitespace(str(message_metadata.get("recipients") or ""))
        if recipients and not normalize_whitespace(str(enriched.get("recipients") or "")):
            enriched["recipients"] = recipients
    search_text = gmail_email_metadata_search_text(message_metadata, linked_drive_records)
    return gmail_append_search_context(enriched, [search_text] if search_text else [])


def apply_gmail_drive_export_metadata(
    extracted: dict[str, object],
    *,
    drive_record: dict[str, object],
) -> dict[str, object]:
    enriched = dict(extracted)
    title = gmail_drive_document_title(drive_record)
    if title:
        enriched["title"] = title
    author = normalize_whitespace(str(drive_record.get("author") or ""))
    if author:
        enriched["author"] = author
    date_created = normalize_whitespace(str(drive_record.get("date_created") or ""))
    if date_created:
        enriched["date_created"] = date_created
    date_modified = normalize_whitespace(str(drive_record.get("date_modified") or ""))
    if date_modified:
        enriched["date_modified"] = date_modified
    participants = gmail_drive_document_participants(drive_record)
    if participants:
        enriched["participants"] = "; ".join(participants)
    search_text = gmail_drive_document_search_text(drive_record)
    return gmail_append_search_context(enriched, [search_text] if search_text else [])


def gmail_enriched_message_file_hash(
    base_hash: object,
    *,
    message_metadata: dict[str, object] | None,
    linked_drive_records: list[dict[str, object]],
    linked_drive_attachment_records: list[dict[str, object]] | None = None,
) -> str:
    return sha256_json_value(
        {
            "base_hash": normalize_whitespace(str(base_hash or "")) or None,
            "message_metadata": message_metadata or {},
            "linked_drive_records": linked_drive_records,
            "linked_drive_attachments": [
                {
                    "drive_item_id": gmail_normalized_drive_item_id(record.get("drive_item_id")),
                    "file_name": normalize_whitespace(str(record.get("file_name") or "")) or None,
                    "file_hash": (
                        gmail_drive_document_file_hash(file_path, record)
                        if isinstance(file_path := record.get("file_path"), Path) and file_path.exists()
                        else None
                    ),
                    "title": gmail_drive_document_title(record),
                }
                for record in list(linked_drive_attachment_records or [])
            ],
        }
    )


def gmail_drive_document_file_hash(path: Path, drive_record: dict[str, object]) -> str:
    return sha256_json_value(
        {
            "file_hash": sha256_file(path),
            "drive_record": {
                "author": drive_record.get("author"),
                "collaborators": list(drive_record.get("collaborators") or []),
                "date_created": drive_record.get("date_created"),
                "date_modified": drive_record.get("date_modified"),
                "drive_item_id": drive_record.get("drive_item_id"),
                "error": drive_record.get("error"),
                "linked_message_ids": list(drive_record.get("linked_message_ids") or []),
                "linked_subjects": list(drive_record.get("linked_subjects") or []),
                "others": list(drive_record.get("others") or []),
                "title": drive_record.get("title"),
                "viewers": list(drive_record.get("viewers") or []),
            },
        }
    )


def pst_export_results_csv_valid(path: Path) -> bool:
    if not path.is_file():
        return False
    if PST_EXPORT_RESULTS_FILE_PATTERN.match(path.name):
        return csv_has_required_headers(path, PST_EXPORT_STANDARD_RESULTS_REQUIRED_HEADERS)
    if PST_EXPORT_ARCHIVE_RESULTS_FILE_PATTERN.match(path.name):
        return csv_has_required_headers(path, PST_EXPORT_ARCHIVE_RESULTS_REQUIRED_HEADERS)
    return False


def pst_export_summary_csv_valid(path: Path) -> bool:
    return path.is_file() and PST_EXPORT_SUMMARY_FILE_PATTERN.match(path.name) is not None


def pst_export_manifest_xml_valid(path: Path) -> bool:
    return path.is_file() and PST_EXPORT_MANIFEST_FILE_PATTERN.match(path.name) is not None


def pst_export_trace_log_valid(path: Path) -> bool:
    return path.is_file() and PST_EXPORT_TRACE_LOG_FILE_PATTERN.match(path.name) is not None


def pst_export_normalized_text(value: object) -> str | None:
    normalized = normalize_whitespace(html.unescape(str(value or "")))
    return normalized or None


def pst_export_match_text_key(value: object) -> str | None:
    normalized = normalize_generated_document_title(value) or pst_export_normalized_text(value)
    return normalized.casefold() if normalized else None


def pst_export_row_value(row: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        if key not in row:
            continue
        normalized = pst_export_normalized_text(row.get(key))
        if normalized:
            return normalized
    return None


def pst_export_path_parts(value: object) -> list[str]:
    normalized = pst_export_normalized_text(value)
    if not normalized:
        return []
    return [part.strip() for part in re.split(r"[\\/]+", normalized) if part.strip()]


def pst_export_folder_match_part(value: object) -> str | None:
    normalized = pst_export_normalized_text(value)
    if not normalized:
        return None
    normalized = normalize_whitespace(re.sub(r"[-_]+", " ", normalized.casefold()))
    return normalized or None


def pst_export_folder_match_parts(value: object) -> tuple[str, ...]:
    parts = pst_export_path_parts(value)
    if not parts:
        return ()
    pst_index = next((index for index, part in enumerate(parts) if part.lower().endswith(".pst")), None)
    if pst_index is not None:
        parts = parts[pst_index + 1 :]
        if parts:
            parts = parts[:-1]
    normalized_parts = [
        normalized_part
        for normalized_part in (pst_export_folder_match_part(part) for part in parts)
        if normalized_part
    ]
    return tuple(normalized_parts)


def pst_export_folder_parts_match(
    message_folder_parts: tuple[str, ...],
    candidate_folder_parts: tuple[str, ...],
) -> bool:
    if not message_folder_parts or not candidate_folder_parts:
        return False
    if len(message_folder_parts) >= len(candidate_folder_parts):
        return tuple(message_folder_parts[-len(candidate_folder_parts) :]) == tuple(candidate_folder_parts)
    return tuple(candidate_folder_parts[-len(message_folder_parts) :]) == tuple(message_folder_parts)


def pst_export_resolve_pst_path(
    candidate_root: Path,
    raw_path: object,
) -> tuple[Path | None, bool]:
    parts = pst_export_path_parts(raw_path)
    pst_index = next((index for index, part in enumerate(parts) if part.lower().endswith(".pst")), None)
    if pst_index is None:
        return None, False
    has_message_component = pst_index < len(parts) - 1
    pst_name = parts[pst_index]
    candidate_part_sets: list[list[str]] = []
    if parts and parts[0].lower() == "exchange":
        candidate_part_sets.append(parts[: pst_index + 1])
    candidate_part_sets.extend((["Exchange", pst_name], [pst_name]))
    seen: set[tuple[str, ...]] = set()
    for rel_parts in candidate_part_sets:
        marker = tuple(part.lower() for part in rel_parts)
        if marker in seen:
            continue
        seen.add(marker)
        resolved = resolve_case_insensitive_relative_path(candidate_root, rel_parts)
        if resolved is None or not resolved.exists() or resolved.is_dir():
            continue
        if normalize_extension(resolved) != PST_SOURCE_KIND:
            continue
        return resolved.resolve(), has_message_component
    return None, has_message_component


def pst_export_combined_recipients(row: dict[str, object]) -> str | None:
    recipients: list[str] = []
    for keys in (
        ("To - Expanded", "To – Expanded", "Recipients in To line"),
        ("CC - Expanded", "CC – Expanded", "Recipients in Cc line"),
        ("BCC - Expanded", "BCC – Expanded", "Recipients in Bcc line"),
    ):
        value = pst_export_row_value(row, *keys)
        if value:
            recipients.append(value)
    if not recipients:
        return None
    return ", ".join(recipients)


def pst_export_manifest_recipients(tag_values: dict[str, str]) -> str | None:
    recipients = [
        value
        for value in (
            pst_export_normalized_text(tag_values.get("#To")),
            pst_export_normalized_text(tag_values.get("#CC")),
            pst_export_normalized_text(tag_values.get("#BCC")),
        )
        if value
    ]
    if not recipients:
        return None
    return ", ".join(recipients)


def pst_export_merge_message_metadata(
    existing: dict[str, object] | None,
    updates: dict[str, object],
) -> dict[str, object]:
    merged = dict(existing or {})
    for key, value in updates.items():
        if value in (None, "", [], {}):
            continue
        current = merged.get(key)
        if current in (None, "", [], {}):
            merged[key] = value
    return merged


def parse_pst_export_results_csv(
    paths: list[Path],
    *,
    candidate_root: Path,
) -> dict[str, dict[str, dict[str, object]]]:
    metadata_by_pst_path: dict[str, dict[str, dict[str, object]]] = defaultdict(dict)
    for path in paths:
        headers, rows = load_normalized_csv_rows(path)
        header_set = set(headers)
        standard_format = PST_EXPORT_STANDARD_RESULTS_REQUIRED_HEADERS.issubset(header_set)
        archive_format = PST_EXPORT_ARCHIVE_RESULTS_REQUIRED_HEADERS.issubset(header_set)
        if not standard_format and not archive_format:
            continue
        for row in rows:
            source_item_id = normalize_pst_identifier(
                row.get("Export Item Id") if archive_format else row.get("Item Identity")
            )
            if not source_item_id:
                continue
            resolved_pst_path, has_message_component = pst_export_resolve_pst_path(
                candidate_root,
                row.get("Export Item Path") if archive_format else row.get("Target Path"),
            )
            if resolved_pst_path is None or not has_message_component:
                continue
            metadata_updates = {
                "author": pst_export_row_value(row, "Sender or Created by"),
                "compliance_tag": pst_export_row_value(row, "Compliance Tag"),
                "custodian": pst_export_row_value(row, "Location Name"),
                "date_created": normalize_datetime(
                    pst_export_row_value(row, "Sent", "Received or Created")
                ),
                "date_modified": normalize_datetime(pst_export_row_value(row, "Modified Date")),
                "decode_status": pst_export_row_value(row, "Decode Status"),
                "document_path": pst_export_row_value(row, "Document Path"),
                "export_document_id": pst_export_row_value(row, "Document ID"),
                "export_item_id": pst_export_row_value(row, "Export Item Id", "ExportItem Id"),
                "export_item_path": pst_export_row_value(row, "Export Item Path"),
                "has_attachments": pst_export_row_value(row, "Has Attachments"),
                "item_identity": pst_export_row_value(row, "Item Identity"),
                "internet_message_id": pst_export_row_value(row, "Internet Message Id"),
                "location": pst_export_row_value(row, "Location"),
                "location_name": pst_export_row_value(row, "Location Name"),
                "modern_attachment_embedded_urls": pst_export_row_value(row, "Modern Attachment Embedded Urls"),
                "original_path": pst_export_row_value(row, "Original Path"),
                "preservation_original_url": pst_export_row_value(row, "Preservation Original Url"),
                "recipients": pst_export_combined_recipients(row),
                "retention_url": pst_export_row_value(row, "Retention Url"),
                "sidecar_source_item_id": source_item_id,
                "subject": normalize_generated_document_title(pst_export_row_value(row, "Subject or Title")),
                "target_path": pst_export_row_value(row, "Target Path"),
                "teams_metadata": pst_export_row_value(row, "Teams Metadata"),
            }
            pst_key = resolved_pst_path.as_posix()
            existing = metadata_by_pst_path[pst_key].get(source_item_id)
            metadata_by_pst_path[pst_key][source_item_id] = pst_export_merge_message_metadata(existing, metadata_updates)
    return {key: dict(value) for key, value in metadata_by_pst_path.items()}


def parse_pst_export_manifest_xml(
    paths: list[Path],
    *,
    candidate_root: Path,
) -> dict[str, dict[str, dict[str, object]]]:
    metadata_by_pst_path: dict[str, dict[str, dict[str, object]]] = defaultdict(dict)
    for path in paths:
        try:
            root = parse_xml_document(path.read_bytes())
        except (OSError, ET.ParseError):
            continue
        for document in root.iter():
            if xml_local_name(document.tag) != "Document":
                continue
            source_item_id = normalize_pst_identifier(document.attrib.get("DocID"))
            if not source_item_id:
                continue
            tag_values: dict[str, str] = {}
            location_custodian = None
            location_uri = None
            for node in document.iter():
                local_name = xml_local_name(node.tag)
                if local_name == "Tag":
                    tag_name = normalize_inline_whitespace(str(node.attrib.get("TagName") or ""))
                    if not tag_name:
                        continue
                    normalized_value = pst_export_normalized_text(node.attrib.get("TagValue"))
                    if normalized_value:
                        tag_values[tag_name] = normalized_value
                    continue
                if local_name == "Custodian":
                    location_custodian = pst_export_normalized_text(node.text)
                    continue
                if local_name == "LocationURI":
                    location_uri = pst_export_normalized_text(node.text)
            resolved_pst_path, has_message_component = pst_export_resolve_pst_path(
                candidate_root,
                tag_values.get("TargetPath"),
            )
            if resolved_pst_path is None or not has_message_component:
                continue
            metadata_updates = {
                "author": pst_export_normalized_text(tag_values.get("#From")),
                "custodian": pst_export_normalized_text(tag_values.get("#Source")) or location_custodian,
                "date_created": normalize_datetime(
                    tag_values.get("#DateSent")
                    or tag_values.get("#DateReceived")
                    or tag_values.get("#CreatedOn")
                ),
                "has_attachments": pst_export_normalized_text(tag_values.get("#HasAttachments")),
                "location": location_custodian or location_uri,
                "location_name": pst_export_normalized_text(tag_values.get("#Source")),
                "manifest_doc_id": source_item_id,
                "original_url": pst_export_normalized_text(tag_values.get("#OriginalUrl")),
                "recipients": pst_export_manifest_recipients(tag_values),
                "sidecar_source_item_id": source_item_id,
                "subject": normalize_generated_document_title(tag_values.get("#Subject")),
                "target_path": pst_export_normalized_text(tag_values.get("TargetPath")),
            }
            pst_key = resolved_pst_path.as_posix()
            existing = metadata_by_pst_path[pst_key].get(source_item_id)
            metadata_by_pst_path[pst_key][source_item_id] = pst_export_merge_message_metadata(existing, metadata_updates)
    return {key: dict(value) for key, value in metadata_by_pst_path.items()}


def build_pst_export_message_match_records(
    message_metadata_by_pst_path: dict[str, dict[str, dict[str, object]]],
) -> dict[str, list[dict[str, object]]]:
    records_by_pst_path: dict[str, list[dict[str, object]]] = {}
    for pst_path, metadata_by_source_item in message_metadata_by_pst_path.items():
        records: list[dict[str, object]] = []
        for sidecar_source_item_id, message_metadata in sorted(metadata_by_source_item.items()):
            exact_match_ids: list[str] = []
            seen_exact_match_ids: set[str] = set()
            for candidate in (
                sidecar_source_item_id,
                message_metadata.get("sidecar_source_item_id"),
                message_metadata.get("item_identity"),
                message_metadata.get("manifest_doc_id"),
                message_metadata.get("export_item_id"),
                message_metadata.get("export_document_id"),
            ):
                normalized_candidate = normalize_pst_identifier(candidate) or pst_export_normalized_text(candidate)
                if not normalized_candidate or normalized_candidate in seen_exact_match_ids:
                    continue
                seen_exact_match_ids.add(normalized_candidate)
                exact_match_ids.append(normalized_candidate)

            folder_match_paths: list[tuple[str, ...]] = []
            seen_folder_match_paths: set[tuple[str, ...]] = set()
            for raw_path in (
                message_metadata.get("target_path"),
                message_metadata.get("original_path"),
                message_metadata.get("document_path"),
                message_metadata.get("export_item_path"),
            ):
                folder_match_parts = pst_export_folder_match_parts(raw_path)
                if not folder_match_parts or folder_match_parts in seen_folder_match_paths:
                    continue
                seen_folder_match_paths.add(folder_match_parts)
                folder_match_paths.append(folder_match_parts)

            records.append(
                {
                    "date_created": normalize_datetime(message_metadata.get("date_created")),
                    "exact_match_ids": exact_match_ids,
                    "folder_match_paths": folder_match_paths,
                    "internet_message_id": normalize_email_message_id(message_metadata.get("internet_message_id")),
                    "message_metadata": dict(message_metadata),
                    "subject_key": pst_export_match_text_key(message_metadata.get("subject")),
                }
            )
        records_by_pst_path[pst_path] = records
    return records_by_pst_path


def pst_export_message_search_text(message_metadata: dict[str, object] | None) -> str | None:
    if not message_metadata:
        return None
    lines = ["PST export metadata"]
    for label, key in (
        ("Sidecar source item ID", "sidecar_source_item_id"),
        ("Export document ID", "export_document_id"),
        ("Export item ID", "export_item_id"),
        ("Item identity", "item_identity"),
        ("Location name", "location_name"),
        ("Location", "location"),
        ("Export target path", "target_path"),
        ("Export item path", "export_item_path"),
        ("Document path", "document_path"),
        ("Original path", "original_path"),
        ("Original URL", "original_url"),
        ("Preservation original URL", "preservation_original_url"),
        ("Retention URL", "retention_url"),
        ("Internet message ID", "internet_message_id"),
        ("Modern attachment embedded URLs", "modern_attachment_embedded_urls"),
        ("Teams metadata", "teams_metadata"),
        ("Compliance tag", "compliance_tag"),
        ("Decode status", "decode_status"),
    ):
        value = pst_export_normalized_text(message_metadata.get(key))
        if value:
            lines.append(f"{label}: {value}")
    normalized = normalize_whitespace("\n".join(lines))
    return normalized or None


def pst_export_sidecar_custodian_candidate(message_metadata: dict[str, object] | None) -> str | None:
    if not message_metadata:
        return None
    for key in ("custodian", "location_name"):
        candidate = pst_export_normalized_text(message_metadata.get(key))
        if candidate:
            return candidate
    return None


def pst_export_custodian_entity_hint(
    message_metadata: dict[str, object] | None,
    *,
    identifier_scope: str | None = None,
) -> dict[str, object] | None:
    custodian = pst_export_sidecar_custodian_candidate(message_metadata)
    location = pst_export_normalized_text(dict(message_metadata or {}).get("location"))
    if not custodian or not location:
        return None
    if normalize_entity_lookup_text(custodian) == normalize_entity_lookup_text(location):
        return None
    identifier: dict[str, object] = {
        "identifier_type": "external_id",
        "identifier_name": "pst_location",
        "display_value": location,
        "normalized_value": normalize_entity_lookup_text(location),
        "is_verified": 1,
    }
    normalized_scope = normalize_entity_lookup_text(identifier_scope or "")
    if normalized_scope:
        identifier["identifier_scope"] = normalized_scope
    return {
        "display_value": custodian,
        "identifiers": [identifier],
    }


def merge_entity_hint_payload(
    existing_hints: object,
    *,
    role: str,
    hint: dict[str, object] | None,
) -> dict[str, object]:
    hints = dict(existing_hints) if isinstance(existing_hints, dict) else {}
    if hint is None:
        return hints
    role_hints = [
        item
        for item in list(hints.get(role) or [])
        if isinstance(item, dict)
    ]
    hint_key = normalize_entity_lookup_text(hint.get("display_value") or "")
    for identifier in list(hint.get("identifiers") or []):
        if isinstance(identifier, dict):
            hint_key = entity_candidate_identifier_key(identifier)
            break
    if hint_key:
        for item in role_hints:
            item_key = normalize_entity_lookup_text(item.get("display_value") or "")
            for identifier in list(item.get("identifiers") or []):
                if isinstance(identifier, dict):
                    item_key = entity_candidate_identifier_key(identifier)
                    break
            if item_key == hint_key:
                return hints
    role_hints.append(hint)
    hints[role] = role_hints
    return hints


def select_pst_export_message_metadata(
    normalized_message: dict[str, object],
    *,
    exact_metadata_by_source_item: dict[str, dict[str, object]] | None = None,
    message_match_records: list[dict[str, object]] | None = None,
) -> dict[str, object] | None:
    source_item_id = normalize_pst_identifier(normalized_message.get("source_item_id"))
    if source_item_id:
        exact_match = dict(exact_metadata_by_source_item or {}).get(source_item_id)
        if exact_match:
            return dict(exact_match)

    records = list(message_match_records or [])
    if not records:
        return None

    extracted = dict(normalized_message.get("extracted") or {})
    email_threading = dict(extracted.get("email_threading") or {})
    message_id = normalize_email_message_id(email_threading.get("message_id"))
    folder_match_parts = pst_export_folder_match_parts(normalized_message.get("source_folder_path"))
    date_created = normalize_datetime(extracted.get("date_created"))
    subject_key = pst_export_match_text_key(extracted.get("subject") or extracted.get("title"))

    candidates = records
    if source_item_id:
        exact_id_matches = [
            record
            for record in candidates
            if source_item_id in list(record.get("exact_match_ids") or [])
        ]
        if len(exact_id_matches) == 1:
            return dict(exact_id_matches[0]["message_metadata"])
        if exact_id_matches:
            candidates = exact_id_matches

    if message_id:
        message_id_matches = [
            record
            for record in candidates
            if normalize_email_message_id(record.get("internet_message_id")) == message_id
        ]
        if len(message_id_matches) == 1:
            return dict(message_id_matches[0]["message_metadata"])
        if message_id_matches:
            candidates = message_id_matches

    if folder_match_parts:
        folder_matches = [
            record
            for record in candidates
            if any(
                pst_export_folder_parts_match(folder_match_parts, tuple(candidate_parts))
                for candidate_parts in list(record.get("folder_match_paths") or [])
            )
        ]
        if len(folder_matches) == 1:
            return dict(folder_matches[0]["message_metadata"])
        if folder_matches:
            candidates = folder_matches

    if date_created:
        date_matches = [
            record
            for record in candidates
            if normalize_datetime(record.get("date_created")) == date_created
        ]
        if len(date_matches) == 1:
            return dict(date_matches[0]["message_metadata"])
        if date_matches:
            candidates = date_matches

    if subject_key:
        subject_matches = [
            record
            for record in candidates
            if pst_export_match_text_key(record.get("subject_key")) == subject_key
            or pst_export_match_text_key(dict(record.get("message_metadata") or {}).get("subject")) == subject_key
        ]
        if len(subject_matches) == 1:
            return dict(subject_matches[0]["message_metadata"])
        if subject_matches:
            candidates = subject_matches

    if len(candidates) == 1:
        return dict(candidates[0]["message_metadata"])
    return None


def apply_pst_export_message_metadata(
    extracted: dict[str, object],
    *,
    message_metadata: dict[str, object] | None,
    identifier_scope: str | None = None,
) -> dict[str, object]:
    if not message_metadata:
        return dict(extracted)
    enriched = dict(extracted)
    if not normalize_whitespace(str(enriched.get("date_created") or "")) and message_metadata.get("date_created"):
        enriched["date_created"] = message_metadata.get("date_created")
    if not normalize_whitespace(str(enriched.get("date_modified") or "")) and message_metadata.get("date_modified"):
        enriched["date_modified"] = message_metadata.get("date_modified")
    subject = normalize_generated_document_title(message_metadata.get("subject"))
    if subject and not normalize_whitespace(str(enriched.get("title") or "")):
        enriched["title"] = subject
    if subject and not normalize_whitespace(str(enriched.get("subject") or "")):
        enriched["subject"] = subject
    author = pst_export_normalized_text(message_metadata.get("author"))
    if author and not normalize_whitespace(str(enriched.get("author") or "")):
        enriched["author"] = author
    recipients = pst_export_normalized_text(message_metadata.get("recipients"))
    if recipients and not normalize_whitespace(str(enriched.get("recipients") or "")):
        enriched["recipients"] = recipients
    custodian_candidate = pst_export_sidecar_custodian_candidate(message_metadata)
    current_custodian = normalize_whitespace(str(enriched.get("custodian") or ""))
    if custodian_candidate and (not current_custodian or ("@" in custodian_candidate and "@" not in current_custodian)):
        enriched["custodian"] = custodian_candidate
    custodian_entity_hint = pst_export_custodian_entity_hint(
        message_metadata,
        identifier_scope=identifier_scope,
    )
    if custodian_entity_hint is not None:
        enriched["entity_hints"] = merge_entity_hint_payload(
            enriched.get("entity_hints"),
            role="custodian",
            hint=custodian_entity_hint,
        )
    if normalize_whitespace(str(enriched.get("content_type") or "")).lower() == "email":
        email_threading = dict(enriched.get("email_threading") or {})
        if not normalize_email_message_id(email_threading.get("message_id")):
            internet_message_id = pst_export_normalized_text(message_metadata.get("internet_message_id"))
            if internet_message_id:
                email_threading["message_id"] = internet_message_id
        enriched["email_threading"] = email_threading
    search_text = pst_export_message_search_text(message_metadata)
    return append_extracted_search_context(enriched, [search_text] if search_text else [])


def pst_export_enriched_message_file_hash(
    base_hash: object,
    *,
    message_metadata: dict[str, object] | None,
) -> str:
    return sha256_json_value(
        {
            "base_hash": normalize_whitespace(str(base_hash or "")) or None,
            "message_metadata": message_metadata or {},
        }
    )


def detect_pst_export_root(candidate_root: Path) -> dict[str, object] | None:
    if not candidate_root.is_dir() or ".retriever" in candidate_root.parts:
        return None
    try:
        pst_paths = sorted(
            path
            for path in candidate_root.rglob("*.pst")
            if ".retriever" not in path.parts
        )
        csv_paths = sorted(
            path
            for path in candidate_root.rglob("*.csv")
            if ".retriever" not in path.parts
        )
        xml_paths = sorted(
            path
            for path in candidate_root.rglob("*.xml")
            if ".retriever" not in path.parts
        )
        log_paths = sorted(
            path
            for path in candidate_root.rglob("*.log")
            if ".retriever" not in path.parts
        )
    except OSError:
        return None
    if not pst_paths:
        return None

    results_csv_paths = [path for path in csv_paths if pst_export_results_csv_valid(path)]
    summary_csv_paths = [path for path in csv_paths if pst_export_summary_csv_valid(path)]
    manifest_paths = [path for path in xml_paths if pst_export_manifest_xml_valid(path)]
    trace_log_paths = [path for path in log_paths if pst_export_trace_log_valid(path)]
    if not (results_csv_paths or summary_csv_paths or manifest_paths or trace_log_paths):
        return None

    message_metadata_by_pst_path = parse_pst_export_results_csv(results_csv_paths, candidate_root=candidate_root)
    manifest_metadata_by_pst_path = parse_pst_export_manifest_xml(manifest_paths, candidate_root=candidate_root)
    for pst_path, records in manifest_metadata_by_pst_path.items():
        target_records = message_metadata_by_pst_path.setdefault(pst_path, {})
        for source_item_id, metadata in records.items():
            target_records[source_item_id] = pst_export_merge_message_metadata(
                target_records.get(source_item_id),
                metadata,
            )
    message_match_records_by_pst_path = build_pst_export_message_match_records(message_metadata_by_pst_path)

    owned_paths = {
        *(path.resolve() for path in results_csv_paths),
        *(path.resolve() for path in summary_csv_paths),
        *(path.resolve() for path in manifest_paths),
        *(path.resolve() for path in trace_log_paths),
    }
    message_sidecar_paths = [*results_csv_paths, *manifest_paths]
    message_sidecar_hash = (
        sha256_json_value(
            {
                "files": {
                    path.resolve().as_posix(): sha256_file(path)
                    for path in sorted(message_sidecar_paths, key=lambda item: item.resolve().as_posix())
                }
            }
        )
        if message_sidecar_paths
        else None
    )
    return {
        "message_metadata_by_pst_path": message_metadata_by_pst_path,
        "message_match_records_by_pst_path": message_match_records_by_pst_path,
        "message_sidecar_hash": message_sidecar_hash,
        "owned_paths": owned_paths,
        "pst_paths": [path.resolve() for path in pst_paths],
        "root": candidate_root,
    }


def find_pst_export_roots(
    root: Path,
    recursive: bool,
) -> list[dict[str, object]]:
    candidates: set[Path] = set()
    if recursive:
        try:
            for csv_path in root.rglob("*.csv"):
                if ".retriever" in csv_path.parts:
                    continue
                if pst_export_results_csv_valid(csv_path) or pst_export_summary_csv_valid(csv_path):
                    candidates.add(csv_path.parent)
            for xml_path in root.rglob("*.xml"):
                if ".retriever" in xml_path.parts:
                    continue
                if pst_export_manifest_xml_valid(xml_path):
                    candidates.add(xml_path.parent)
            for log_path in root.rglob("*.log"):
                if ".retriever" in log_path.parts:
                    continue
                if pst_export_trace_log_valid(log_path):
                    candidates.add(log_path.parent)
        except OSError:
            return []
    else:
        candidates.add(root)

    descriptors: list[dict[str, object]] = []
    accepted_roots: list[Path] = []
    for candidate in sorted(candidates, key=lambda path: (len(path.parts), path.as_posix())):
        if any(parent == candidate or parent in candidate.parents for parent in accepted_roots):
            continue
        descriptor = detect_pst_export_root(candidate)
        if descriptor is None:
            continue
        descriptors.append(descriptor)
        accepted_roots.append(candidate)
    return descriptors


def detect_gmail_export_root(candidate_root: Path) -> dict[str, object] | None:
    if not candidate_root.is_dir() or ".retriever" in candidate_root.parts:
        return None
    archive_browser_path = candidate_root / GMAIL_EXPORT_ARCHIVE_BROWSER_FILE
    try:
        mbox_paths = sorted(
            path
            for path in candidate_root.rglob("*.mbox")
            if ".retriever" not in path.parts
        )
    except OSError:
        return None
    if not mbox_paths:
        return None

    try:
        metadata_csv_paths = sorted(path for path in candidate_root.rglob("*.csv") if gmail_metadata_csv_valid(path))
        drive_links_paths = sorted(path for path in candidate_root.rglob("*.csv") if gmail_drive_links_csv_valid(path))
        drive_export_dirs = sorted(
            path
            for path in candidate_root.rglob("*")
            if path.is_dir() and GMAIL_EXPORT_DRIVE_FOLDER_PATTERN.search(path.name)
        )
        drive_export_metadata_paths = sorted(
            path
            for path in candidate_root.rglob("*-metadata.xml")
            if "Drive_Link_Export" in path.name
        )
        drive_export_error_paths = sorted(
            path
            for path in candidate_root.rglob("*-errors.csv")
            if "Drive_Link_Export" in path.name
        )
        auxiliary_metadata_xml_paths = sorted(
            path
            for path in candidate_root.rglob("*-metadata.xml")
            if "Drive_Link_Export" not in path.name
        )
        auxiliary_error_xml_paths = sorted(path for path in candidate_root.rglob("*-errors.xml"))
        result_count_paths = sorted(path for path in candidate_root.rglob("*-result-counts.csv"))
        md5_paths = sorted(path for path in candidate_root.rglob("*.md5"))
    except OSError:
        return None

    if not (
        archive_browser_path.exists()
        or metadata_csv_paths
        or drive_links_paths
        or drive_export_dirs
        or drive_export_metadata_paths
    ):
        return None

    email_metadata_by_message_id = parse_gmail_metadata_csv(metadata_csv_paths)
    drive_links_by_message_id = parse_gmail_drive_links_csv(drive_links_paths)
    drive_export_files = gmail_drive_export_files(drive_export_dirs)
    file_paths_by_name = {path.name: path for path in drive_export_files}
    drive_records_by_item_id: dict[str, dict[str, object]] = {}
    drive_documents: list[dict[str, object]] = []
    for export_path in drive_export_files:
        drive_item_id = gmail_normalized_drive_item_id(gmail_drive_item_id_from_export_file_name(export_path))
        record = {
            "author": None,
            "collaborators": [],
            "date_created": None,
            "date_modified": None,
            "document_type": None,
            "drive_item_id": drive_item_id,
            "error": None,
            "file_name": export_path.name,
            "file_path": export_path,
            "linked_message_ids": [],
            "linked_subjects": [],
            "others": [],
            "source_hash": None,
            "title": normalize_generated_document_title(export_path.stem.rsplit("_", 1)[0]) or None,
            "viewers": [],
        }
        drive_documents.append(record)
        if drive_item_id:
            drive_records_by_item_id[drive_item_id] = record

    parsed_drive_metadata = parse_gmail_drive_export_metadata(
        drive_export_metadata_paths,
        file_paths_by_name=file_paths_by_name,
    )
    for drive_item_id, metadata in parsed_drive_metadata.items():
        record = drive_records_by_item_id.get(drive_item_id)
        if record is None:
            record = {
                "author": None,
                "collaborators": [],
                "date_created": None,
                "date_modified": None,
                "document_type": None,
                "drive_item_id": drive_item_id,
                "error": None,
                "file_name": metadata.get("file_name"),
                "file_path": metadata.get("file_path"),
                "linked_message_ids": [],
                "linked_subjects": [],
                "others": [],
                "source_hash": None,
                "title": None,
                "viewers": [],
            }
            drive_records_by_item_id[drive_item_id] = record
            if record.get("file_path") is not None:
                drive_documents.append(record)
        record.update(
            {
                "author": metadata.get("author"),
                "collaborators": list(metadata.get("collaborators") or []),
                "date_created": metadata.get("date_created"),
                "date_modified": metadata.get("date_modified"),
                "document_type": metadata.get("document_type"),
                "file_name": metadata.get("file_name"),
                "file_path": metadata.get("file_path"),
                "others": list(metadata.get("others") or []),
                "source_hash": metadata.get("source_hash"),
                "title": metadata.get("title"),
                "viewers": list(metadata.get("viewers") or []),
            }
        )

    drive_errors_by_item_id = parse_gmail_drive_export_errors(drive_export_error_paths)
    linked_drive_records_by_message_id: dict[str, list[dict[str, object]]] = defaultdict(list)
    linked_drive_attachment_records_by_message_id: dict[str, list[dict[str, object]]] = defaultdict(list)
    for message_id, link_rows in drive_links_by_message_id.items():
        seen_drive_items: set[str] = set()
        for link_row in link_rows:
            drive_item_id = gmail_normalized_drive_item_id(link_row.get("drive_item_id"))
            if not drive_item_id or drive_item_id in seen_drive_items:
                continue
            seen_drive_items.add(drive_item_id)
            drive_record = drive_records_by_item_id.get(drive_item_id)
            summary = gmail_drive_document_link_summary(
                {
                    **dict(drive_record or {}),
                    "drive_item_id": drive_item_id,
                    "error": drive_errors_by_item_id.get(drive_item_id),
                }
            )
            linked_drive_records_by_message_id[message_id].append(summary)
            if drive_record is not None:
                if message_id not in drive_record["linked_message_ids"]:
                    drive_record["linked_message_ids"].append(message_id)
                subject = normalize_generated_document_title(
                    (email_metadata_by_message_id.get(message_id) or {}).get("subject")
                )
                if subject and subject not in drive_record["linked_subjects"]:
                    drive_record["linked_subjects"].append(subject)
                drive_record["error"] = drive_errors_by_item_id.get(drive_item_id)
                if isinstance(drive_record.get("file_path"), Path):
                    linked_drive_attachment_records_by_message_id[message_id].append(drive_record)

    deduplicated_drive_documents: dict[str, dict[str, object]] = {}
    for record in drive_documents:
        file_path = record.get("file_path")
        drive_item_id = gmail_normalized_drive_item_id(record.get("drive_item_id"))
        if isinstance(file_path, Path):
            key = f"path:{file_path.resolve().as_posix()}"
        elif drive_item_id:
            key = f"drive:{drive_item_id}"
        else:
            key = f"file:{normalize_whitespace(str(record.get('file_name') or ''))}"
        current = deduplicated_drive_documents.get(key)
        if current is None or gmail_drive_record_preference_key(record) > gmail_drive_record_preference_key(current):
            deduplicated_drive_documents[key] = record
    drive_documents = sorted(
        deduplicated_drive_documents.values(),
        key=lambda record: (
            str(record.get("file_path") or ""),
            str(record.get("file_name") or ""),
            str(record.get("drive_item_id") or ""),
        ),
    )

    owned_paths = {
        *(path.resolve() for path in mbox_paths),
        *(path.resolve() for path in metadata_csv_paths),
        *(path.resolve() for path in drive_links_paths),
        *(path.resolve() for path in auxiliary_metadata_xml_paths),
        *(path.resolve() for path in auxiliary_error_xml_paths),
        *(path.resolve() for path in drive_export_metadata_paths),
        *(path.resolve() for path in drive_export_error_paths),
        *(path.resolve() for path in result_count_paths),
        *(path.resolve() for path in md5_paths),
        *(path.resolve() for path in drive_export_files),
    }
    if archive_browser_path.exists():
        owned_paths.add(archive_browser_path.resolve())

    message_sidecar_paths = [
        *metadata_csv_paths,
        *drive_links_paths,
        *drive_export_metadata_paths,
        *drive_export_error_paths,
        *drive_export_files,
    ]
    message_sidecar_hash = sha256_json_value(
        {
            "files": {
                path.resolve().as_posix(): sha256_file(path)
                for path in sorted(message_sidecar_paths, key=lambda item: item.resolve().as_posix())
            }
        }
    )

    return {
        "drive_documents": drive_documents,
        "drive_records_by_item_id": drive_records_by_item_id,
        "email_metadata_by_message_id": email_metadata_by_message_id,
        "linked_drive_attachment_records_by_message_id": linked_drive_attachment_records_by_message_id,
        "linked_drive_records_by_message_id": linked_drive_records_by_message_id,
        "mbox_paths": mbox_paths,
        "message_sidecar_hash": message_sidecar_hash,
        "owned_paths": owned_paths,
        "root": candidate_root,
    }


def find_gmail_export_roots(
    root: Path,
    recursive: bool,
    allowed_file_types: set[str] | None,
) -> list[dict[str, object]]:
    candidates: set[Path] = set()
    if recursive:
        try:
            for archive_browser_path in root.rglob(GMAIL_EXPORT_ARCHIVE_BROWSER_FILE):
                if ".retriever" in archive_browser_path.parts:
                    continue
                candidates.add(archive_browser_path.parent)
            for metadata_path in root.rglob("*-metadata.csv"):
                if ".retriever" in metadata_path.parts:
                    continue
                candidates.add(metadata_path.parent)
            for drive_links_path in root.rglob("*-drive-links.csv"):
                if ".retriever" in drive_links_path.parts:
                    continue
                candidates.add(drive_links_path.parent)
            for export_dir in root.rglob("*"):
                if not export_dir.is_dir() or ".retriever" in export_dir.parts:
                    continue
                if GMAIL_EXPORT_DRIVE_FOLDER_PATTERN.search(export_dir.name):
                    candidates.add(export_dir.parent)
        except OSError:
            return []
    else:
        candidates.add(root)

    descriptors: list[dict[str, object]] = []
    accepted_roots: list[Path] = []
    for candidate in sorted(candidates, key=lambda path: (len(path.parts), path.as_posix())):
        if any(parent == candidate or parent in candidate.parents for parent in accepted_roots):
            continue
        descriptor = detect_gmail_export_root(candidate)
        if descriptor is None:
            continue
        descriptors.append(descriptor)
        accepted_roots.append(candidate)
    return descriptors
