def preview_base_path_for_rel_path(rel_path: str) -> Path:
    source_rel_path = container_source_rel_path_from_message_rel_path(rel_path)
    if source_rel_path is not None:
        return Path("previews") / Path(source_rel_path) / "messages"
    base = Path(rel_path)
    if base.parts and base.parts[0] == INTERNAL_REL_PATH_PREFIX:
        base = Path(*base.parts[1:])
    if base.parts and base.parts[0] == "previews":
        return base.parent
    return Path("previews") / base.parent


def production_source_part_targets(
    paths: dict[str, Path],
    connection: sqlite3.Connection,
    row: sqlite3.Row | None,
) -> list[dict[str, object]]:
    if row is None or row["source_kind"] != PRODUCTION_SOURCE_KIND:
        return []
    source_rows = connection.execute(
        """
        SELECT part_kind, rel_source_path, ordinal, label
        FROM document_source_parts
        WHERE document_id = ?
        ORDER BY
          CASE part_kind WHEN 'native' THEN 0 WHEN 'image' THEN 1 ELSE 2 END,
          ordinal ASC,
          id ASC
        """,
        (row["id"],),
    ).fetchall()
    targets: list[dict[str, object]] = []
    for source_row in source_rows:
        rel_source_path = str(source_row["rel_source_path"])
        abs_path = paths["root"] / rel_source_path
        if not abs_path.exists():
            continue
        part_kind = str(source_row["part_kind"])
        preview_type = "native"
        if part_kind == "image":
            preview_type = "image"
        targets.append(
            {
                "rel_path": rel_source_path,
                "abs_path": str(abs_path),
                "preview_type": preview_type,
                "label": source_row["label"],
                "ordinal": int(source_row["ordinal"]),
            }
        )
    return targets


def document_native_target(paths: dict[str, Path], row: sqlite3.Row | None) -> dict[str, object] | None:
    if row is None:
        return None
    rel_path = str(row["rel_path"])
    abs_path = document_absolute_path(paths, rel_path)
    if not abs_path.exists():
        return None
    return {
        "rel_path": rel_path,
        "abs_path": str(abs_path),
        "preview_type": "native",
        "label": None,
        "ordinal": 0,
    }


def document_prefers_native_primary_preview(row: sqlite3.Row | None) -> bool:
    if row is None:
        return False
    if normalize_whitespace(str(row["content_type"] or "")) != "Chat":
        return False
    file_type = normalize_whitespace(str(row["file_type"] or "")).lower()
    if not file_type:
        file_type = normalize_extension(Path(str(row["file_name"] or row["rel_path"] or "")))
    return file_type in {"pdf", "docx", "rtf"}


def build_preview_target_payload(
    *,
    rel_path: str,
    abs_path: str,
    preview_type: str,
    label: str | None,
    ordinal: int,
    target_fragment: object = None,
) -> dict[str, object]:
    normalized_fragment = normalize_whitespace(str(target_fragment or "")) or None
    return {
        "rel_path": append_preview_fragment(rel_path, normalized_fragment),
        "abs_path": append_preview_fragment(abs_path, normalized_fragment),
        "file_rel_path": rel_path,
        "file_abs_path": abs_path,
        "preview_type": preview_type,
        "label": label,
        "ordinal": ordinal,
        "target_fragment": normalized_fragment,
    }


def preview_target_payload_from_preview_row(paths: dict[str, Path], preview_row: sqlite3.Row) -> dict[str, object]:
    rel_preview = str(Path(INTERNAL_REL_PATH_PREFIX) / preview_row["rel_preview_path"])
    abs_preview = str(paths["state_dir"] / preview_row["rel_preview_path"])
    return build_preview_target_payload(
        rel_path=rel_preview,
        abs_path=abs_preview,
        preview_type=str(preview_row["preview_type"]),
        label=(str(preview_row["label"]) if preview_row["label"] is not None else None),
        ordinal=int(preview_row["ordinal"]),
        target_fragment=preview_row["target_fragment"] if "target_fragment" in preview_row.keys() else None,
    )


def preview_rows_use_conversation_navigation(preview_rows: list[sqlite3.Row]) -> bool:
    return bool(preview_rows) and is_conversation_preview_rel_path(preview_rows[0]["rel_preview_path"])


def ordered_preview_rows_for_document(
    document_row: sqlite3.Row | None,
    preview_rows: list[sqlite3.Row],
) -> list[sqlite3.Row]:
    if not preview_rows:
        return []
    if document_row is None or not preview_rows_use_conversation_navigation(preview_rows):
        return list(preview_rows)
    if normalize_whitespace(str(document_row["content_type"] or "")) == "Chat":
        return list(preview_rows)
    for preview_row in preview_rows:
        if not is_conversation_preview_rel_path(preview_row["rel_preview_path"]):
            return [preview_row, *[row for row in preview_rows if row is not preview_row]]
    return list(preview_rows)


def default_preview_target(paths: dict[str, Path], row: sqlite3.Row, connection: sqlite3.Connection) -> dict[str, object]:
    preview_rows = connection.execute(
        """
        SELECT rel_preview_path, preview_type, target_fragment, label, ordinal
        FROM document_previews
        WHERE document_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (row["id"],),
    ).fetchall()
    ordered_preview_rows = ordered_preview_rows_for_document(row, preview_rows)
    native_target = document_native_target(paths, row)
    if (
        ordered_preview_rows
        and not preview_rows_use_conversation_navigation(ordered_preview_rows)
        and document_prefers_native_primary_preview(row)
        and native_target is not None
    ):
        return build_preview_target_payload(
            rel_path=str(native_target["rel_path"]),
            abs_path=str(native_target["abs_path"]),
            preview_type=str(native_target["preview_type"]),
            label=(str(native_target["label"]) if native_target["label"] is not None else None),
            ordinal=int(native_target["ordinal"]),
        )
    if ordered_preview_rows:
        return preview_target_payload_from_preview_row(paths, ordered_preview_rows[0])
    source_targets = production_source_part_targets(paths, connection, row)
    if source_targets:
        return build_preview_target_payload(
            rel_path=str(source_targets[0]["rel_path"]),
            abs_path=str(source_targets[0]["abs_path"]),
            preview_type=str(source_targets[0]["preview_type"]),
            label=(str(source_targets[0]["label"]) if source_targets[0]["label"] is not None else None),
            ordinal=int(source_targets[0]["ordinal"]),
        )
    rel_path = row["rel_path"]
    return build_preview_target_payload(
        rel_path=str(rel_path),
        abs_path=str(document_absolute_path(paths, rel_path)),
        preview_type="native",
        label=None,
        ordinal=0,
    )


def collect_preview_targets(paths: dict[str, Path], document_id: int, rel_path: str, connection: sqlite3.Connection) -> list[dict[str, object]]:
    document_row = connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
    preview_rows = connection.execute(
        """
        SELECT rel_preview_path, preview_type, target_fragment, label, ordinal
        FROM document_previews
        WHERE document_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (document_id,),
    ).fetchall()
    if not preview_rows:
        native_target = document_native_target(paths, document_row)
        source_targets = production_source_part_targets(paths, connection, document_row)
        if native_target is not None and document_prefers_native_primary_preview(document_row):
            targets = [native_target]
            for target in source_targets:
                if target["rel_path"] not in {existing["rel_path"] for existing in targets}:
                    targets.append(target)
            return targets
        if source_targets:
            return source_targets
        if native_target is not None:
            return [native_target]
        abs_path = document_absolute_path(paths, rel_path)
        return [
            build_preview_target_payload(
                rel_path=rel_path,
                abs_path=str(abs_path),
                preview_type="native",
                label=None,
                ordinal=0,
            )
        ]

    ordered_preview_rows = ordered_preview_rows_for_document(document_row, preview_rows)
    targets: list[dict[str, object]] = []
    if (
        ordered_preview_rows
        and not preview_rows_use_conversation_navigation(ordered_preview_rows)
        and document_prefers_native_primary_preview(document_row)
    ):
        native_target = document_native_target(paths, document_row)
        if native_target is not None:
            targets.append(
                build_preview_target_payload(
                    rel_path=str(native_target["rel_path"]),
                    abs_path=str(native_target["abs_path"]),
                    preview_type=str(native_target["preview_type"]),
                    label=(str(native_target["label"]) if native_target["label"] is not None else None),
                    ordinal=int(native_target["ordinal"]),
                )
            )
    for preview_row in ordered_preview_rows:
        targets.append(preview_target_payload_from_preview_row(paths, preview_row))
    source_targets = production_source_part_targets(paths, connection, document_row)
    for target in source_targets:
        if target["rel_path"] not in {existing["rel_path"] for existing in targets}:
            targets.append(
                build_preview_target_payload(
                    rel_path=str(target["rel_path"]),
                    abs_path=str(target["abs_path"]),
                    preview_type=str(target["preview_type"]),
                    label=(str(target["label"]) if target["label"] is not None else None),
                    ordinal=int(target["ordinal"]),
                )
            )
    return targets


def token_estimate(text: str) -> int:
    return max(1, len(text) // 4)


def chunk_text(text: str, max_chars: int = CHUNK_TARGET_CHARS, overlap: int = CHUNK_OVERLAP_CHARS) -> list[dict[str, object]]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []

    chunks: list[dict[str, object]] = []
    start = 0
    length = len(normalized)
    chunk_index = 0
    while start < length:
        end = min(length, start + max_chars)
        if end < length:
            preferred_break = normalized.rfind("\n", max(start + 400, end - 500), end)
            if preferred_break <= start:
                preferred_break = normalized.rfind(" ", max(start + 400, end - 250), end)
            if preferred_break > start:
                end = preferred_break
        chunk_text_value = normalized[start:end].strip()
        if chunk_text_value:
            chunk_start = normalized.find(chunk_text_value, start, end + len(chunk_text_value))
            chunk_end = chunk_start + len(chunk_text_value)
            chunks.append(
                {
                    "chunk_index": chunk_index,
                    "char_start": chunk_start,
                    "char_end": chunk_end,
                    "token_estimate": token_estimate(chunk_text_value),
                    "text_content": chunk_text_value,
                }
            )
            chunk_index += 1
        if end >= length:
            break
        start = max(end - overlap, start + 1)
    return chunks


def extracted_search_chunks(extracted: dict[str, object]) -> list[dict[str, object]]:
    raw_chunks = extracted.get("chunks")
    if isinstance(raw_chunks, list):
        normalized_chunks: list[dict[str, object]] = []
        for index, raw_chunk in enumerate(raw_chunks):
            if not isinstance(raw_chunk, dict):
                continue
            text_content = normalize_whitespace(str(raw_chunk.get("text_content") or ""))
            if not text_content:
                continue
            normalized_chunks.append(
                {
                    "chunk_index": int(raw_chunk.get("chunk_index", index)),
                    "char_start": int(raw_chunk.get("char_start", 0)),
                    "char_end": int(raw_chunk.get("char_end", len(text_content))),
                    "token_estimate": int(raw_chunk.get("token_estimate", token_estimate(text_content))),
                    "text_content": text_content,
                }
            )
        return normalized_chunks
    return chunk_text(str(extracted.get("text_content") or ""))


SPREADSHEET_TYPE_SAMPLE_LIMIT = 50
SPREADSHEET_MAX_COLUMNS_PER_SHEET = 100
SPREADSHEET_MAX_COMMENTS_PER_SHEET = 200
SPREADSHEET_MAX_HYPERLINKS_PER_WORKBOOK = 200
SPREADSHEET_MAX_NAMED_RANGES = 500
SPREADSHEET_MAX_ENUM_VALUES = 64
SPREADSHEET_MAX_PARTICIPANTS = 32
SPREADSHEET_MAX_SUMMARY_CHARS = 64 * 1024
SPREADSHEET_XLSX_READ_ONLY_FALLBACK_BYTES = 50 * 1024 * 1024


SLACK_USER_DIRECTORY_CACHE: dict[str, dict[str, dict[str, str | None]]] = {}
SLACK_USER_MENTION_PATTERN = re.compile(r"<@([A-Z0-9]+)(?:\|[^>]+)?>")
SLACK_CHANNEL_MENTION_PATTERN = re.compile(r"<#([A-Z0-9]+)(?:\|([^>]+))?>")
SLACK_SPECIAL_MENTION_PATTERN = re.compile(r"<!([a-z]+)(?:\|[^>]+)?>")
SLACK_NAMED_LINK_PATTERN = re.compile(r"<([^>|]+)\|([^>]+)>")
SLACK_BARE_LINK_PATTERN = re.compile(r"<(https?://[^>]+)>")
SLACK_EMOJI_SHORTCODE_PATTERN = re.compile(r":([a-z0-9_+\-]+):")
SLACK_EMOJI_NAME_ALIASES = {
    "+1": "THUMBS UP SIGN",
    "-1": "THUMBS DOWN SIGN",
    "100": "HUNDRED POINTS SYMBOL",
    "christmas_tree": "🎄",
    "clap": "CLAPPING HANDS SIGN",
    "eyes": "EYES",
    "fire": "FIRE",
    "grin": "GRINNING FACE WITH SMILING EYES",
    "heart": "HEAVY BLACK HEART",
    "joy": "FACE WITH TEARS OF JOY",
    "ok_hand": "OK HAND SIGN",
    "partying_face": "🥳",
    "pray": "PERSON WITH FOLDED HANDS",
    "rocket": "ROCKET",
    "smile": "SMILING FACE WITH OPEN MOUTH",
    "sob": "LOUDLY CRYING FACE",
    "tada": "PARTY POPPER",
    "thinking_face": "THINKING FACE",
    "thumbsdown": "THUMBS DOWN SIGN",
    "thumbsup": "THUMBS UP SIGN",
    "warning": "WARNING SIGN",
    "wave": "WAVING HAND SIGN",
    "white_check_mark": "WHITE HEAVY CHECK MARK",
}


def choose_slack_text(*values: object) -> str | None:
    for value in values:
        normalized = normalize_whitespace(str(value or ""))
        if normalized:
            return normalized
    return None


def replace_slack_emoji_shortcodes(text: str) -> str:
    def replace_match(match: re.Match[str]) -> str:
        token = match.group(1).lower()
        candidate_names = [
            SLACK_EMOJI_NAME_ALIASES.get(token),
            token.replace("_", " ").replace("-", " ").upper(),
        ]
        for candidate_name in candidate_names:
            if not candidate_name:
                continue
            if any(ord(character) > 127 for character in candidate_name):
                return candidate_name
            try:
                return unicodedata.lookup(candidate_name)
            except KeyError:
                continue
        return match.group(0)

    return SLACK_EMOJI_SHORTCODE_PATTERN.sub(replace_match, text)


def normalize_slack_user_info(
    user_record: dict[str, object] | None = None,
    inline_profile: dict[str, object] | None = None,
) -> dict[str, str | None]:
    record = user_record or {}
    profile = dict(record.get("profile") if isinstance(record.get("profile"), dict) else {})
    if inline_profile:
        for key, value in inline_profile.items():
            if value not in (None, ""):
                profile[key] = value
    first_name = choose_slack_text(profile.get("first_name"), record.get("first_name"))
    last_name = choose_slack_text(profile.get("last_name"), record.get("last_name"))
    combined_name = " ".join(part for part in [first_name, last_name] if part)
    speaker_name = choose_slack_text(
        profile.get("real_name"),
        record.get("real_name"),
        profile.get("display_name"),
        combined_name,
        profile.get("name"),
        record.get("name"),
    )
    mention_name = choose_slack_text(
        profile.get("display_name"),
        first_name,
        profile.get("name"),
        record.get("name"),
        speaker_name,
    )
    return {
        "avatar_color": choose_slack_text(profile.get("color"), record.get("color")),
        "speaker_name": speaker_name,
        "mention_name": mention_name,
    }


def slack_export_root_for_path(path: Path) -> Path | None:
    current = path.parent
    while True:
        if (current / "users.json").exists():
            return current
        if current.parent == current:
            return None
        current = current.parent


def load_slack_user_directory(export_root: Path | None) -> dict[str, dict[str, str | None]]:
    if export_root is None:
        return {}
    cache_key = str(export_root)
    cached = SLACK_USER_DIRECTORY_CACHE.get(cache_key)
    if cached is not None:
        return cached
    users_path = export_root / "users.json"
    directory: dict[str, dict[str, str | None]] = {}
    try:
        raw_users = json.loads(users_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        raw_users = None
    if isinstance(raw_users, list):
        for item in raw_users:
            if not isinstance(item, dict):
                continue
            user_id = choose_slack_text(item.get("id"))
            if not user_id:
                continue
            directory[user_id] = normalize_slack_user_info(item)
    SLACK_USER_DIRECTORY_CACHE[cache_key] = directory
    return directory


def resolve_slack_user_info(
    user_id: str | None,
    user_directory: dict[str, dict[str, str | None]],
    inline_profile: dict[str, object] | None = None,
) -> dict[str, str | None]:
    resolved = dict(user_directory.get(user_id or "", {}))
    inline_info = normalize_slack_user_info({}, inline_profile) if inline_profile else {}
    for key, value in inline_info.items():
        if value:
            resolved[key] = value
    if user_id and not resolved.get("speaker_name"):
        resolved["speaker_name"] = user_id
    if user_id and not resolved.get("mention_name"):
        resolved["mention_name"] = user_id
    if user_id:
        resolved["slack_user_id"] = user_id
    return resolved


def format_slack_document_title(path: Path) -> str:
    channel_name = normalize_whitespace(path.parent.name)
    if channel_name and not channel_name.startswith("#"):
        channel_name = f"#{channel_name}"
    day_token = normalize_whitespace(path.stem)
    try:
        day_label = date.fromisoformat(day_token).strftime("%b %d, %Y").replace(" 0", " ")
    except ValueError:
        day_label = day_token
    if channel_name and day_label:
        return f"{channel_name} - {day_label}"
    return channel_name or day_label or "Slack conversation"


def normalize_slack_timestamp(value: object) -> str | None:
    raw = normalize_whitespace(str(value or ""))
    if not raw:
        return None
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def render_slack_text(text: str, user_directory: dict[str, dict[str, str | None]]) -> str:
    rendered = str(text or "")
    rendered = SLACK_USER_MENTION_PATTERN.sub(
        lambda match: f"@{resolve_slack_user_info(match.group(1), user_directory).get('mention_name') or match.group(1)}",
        rendered,
    )
    rendered = SLACK_CHANNEL_MENTION_PATTERN.sub(lambda match: f"#{match.group(2) or match.group(1)}", rendered)
    rendered = SLACK_SPECIAL_MENTION_PATTERN.sub(lambda match: f"@{match.group(1)}", rendered)
    rendered = SLACK_NAMED_LINK_PATTERN.sub(lambda match: match.group(2), rendered)
    rendered = SLACK_BARE_LINK_PATTERN.sub(lambda match: match.group(1), rendered)
    return normalize_whitespace(replace_slack_emoji_shortcodes(rendered))


def slack_message_actor_info(
    message: dict[str, object],
    user_directory: dict[str, dict[str, str | None]],
) -> dict[str, str | None]:
    user_id = choose_slack_text(message.get("user"))
    user_profile = message.get("user_profile") if isinstance(message.get("user_profile"), dict) else None
    if user_id:
        return resolve_slack_user_info(user_id, user_directory, user_profile)
    bot_profile = message.get("bot_profile") if isinstance(message.get("bot_profile"), dict) else None
    if bot_profile:
        actor_info = normalize_slack_user_info({}, bot_profile)
        if not actor_info.get("speaker_name"):
            actor_info["speaker_name"] = "Slack bot"
        return actor_info
    speaker_name = (
        choose_slack_text(message.get("username"))
        or choose_slack_text(message.get("subtype"))
        or "Slack message"
    )
    return {
        "avatar_color": None,
        "mention_name": speaker_name,
        "speaker_name": speaker_name,
    }


def iter_slack_export_entries(
    raw_value: object,
    user_directory: dict[str, dict[str, str | None]],
) -> list[dict[str, object]]:
    candidate_items = raw_value.get("messages") if isinstance(raw_value, dict) else raw_value
    if not isinstance(candidate_items, list):
        return []

    entries: list[dict[str, object]] = []
    for item in candidate_items:
        if not isinstance(item, dict):
            continue
        if normalize_whitespace(str(item.get("type") or "")).lower() != "message":
            continue
        body = render_slack_text(choose_slack_text(item.get("text")) or "", user_directory)
        if not body:
            continue
        actor_info = slack_message_actor_info(item, user_directory)
        speaker = actor_info.get("speaker_name") or "Slack message"
        timestamp = normalize_slack_timestamp(item.get("ts"))
        entries.append(
            {
                "avatar_color": actor_info.get("avatar_color"),
                "speaker": speaker,
                "body": body,
                "timestamp": timestamp,
                "timestamp_label": format_chat_preview_timestamp(timestamp),
                "avatar_label": chat_avatar_initials(speaker),
            }
        )
    return entries


def extract_slack_chat_json_payload(path: Path, decoded_text: str) -> dict[str, object] | None:
    try:
        raw_value = json.loads(decoded_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None

    candidate_items = raw_value.get("messages") if isinstance(raw_value, dict) else raw_value
    if not isinstance(candidate_items, list):
        return None

    message_like_count = sum(
        1
        for item in candidate_items
        if isinstance(item, dict)
        and normalize_whitespace(str(item.get("type") or "")).lower() == "message"
        and item.get("ts") is not None
        and any(item.get(key) is not None for key in ("text", "user", "user_profile", "subtype"))
    )
    if message_like_count == 0:
        return None

    user_directory = load_slack_user_directory(slack_export_root_for_path(path))
    entries = iter_slack_export_entries(raw_value, user_directory)
    if not entries:
        return None

    participants: list[str] = []
    seen: set[str] = set()
    first_body: str | None = None
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    timestamped_matches = 0
    transcript_lines: list[str] = []

    for entry in entries:
        speaker = normalize_whitespace(str(entry.get("speaker") or "")) or "Unknown"
        key = speaker.lower()
        if key not in seen:
            seen.add(key)
            participants.append(speaker)
        body = str(entry.get("body") or "").strip()
        if first_body is None and body:
            first_body = body
        timestamp = entry.get("timestamp")
        if isinstance(timestamp, str) and timestamp:
            timestamped_matches += 1
            if first_timestamp is None:
                first_timestamp = timestamp
            last_timestamp = timestamp
            transcript_lines.append(f"[{timestamp}] {speaker}: {body}")
        else:
            transcript_lines.append(f"{speaker}: {body}")

    return {
        "text_content": normalize_whitespace("\n".join(transcript_lines)),
        "chat_metadata": {
            "author": None,
            "participants": ", ".join(participants) or None,
            "date_created": first_timestamp,
            "date_modified": last_timestamp if last_timestamp and last_timestamp != first_timestamp else None,
            "title": format_slack_document_title(path),
            "message_count": len(entries),
            "timestamped_message_count": timestamped_matches,
        },
        "chat_entries": entries,
    }


def extract_plain_text_file(path: Path) -> dict[str, object]:
    decoded, text_status, _ = decode_bytes(path.read_bytes())
    file_type = normalize_extension(path)
    structured_chat = extract_slack_chat_json_payload(path, decoded) if file_type == "json" else None
    text_content = (
        str(structured_chat["text_content"])
        if structured_chat and structured_chat.get("text_content")
        else (strip_html_tags(decoded) if file_type in {"htm", "html"} else normalize_whitespace(decoded))
    )
    email_headers = {} if structured_chat else extract_email_like_headers(text_content)
    chat_metadata = (
        dict(structured_chat["chat_metadata"])
        if structured_chat and isinstance(structured_chat.get("chat_metadata"), dict)
        else extract_chat_transcript_metadata(text_content)
    )
    chat_entries = structured_chat.get("chat_entries") if structured_chat and isinstance(structured_chat.get("chat_entries"), list) else None
    participants = extract_email_chain_participants(
        text_content,
        [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
    ) or (str(chat_metadata["participants"]) if chat_metadata and chat_metadata.get("participants") else None) or extract_chat_participants(text_content)
    title = email_headers.get("title") if email_headers else (str(chat_metadata["title"]) if chat_metadata and chat_metadata.get("title") else None)
    if title is None and file_type in {"md", "txt"} and text_content:
        title = text_content.splitlines()[0][:200]
    resolved_author = None if chat_metadata else (email_headers.get("author") if email_headers else None)
    preview_artifacts = (
        build_chat_preview_artifacts(
            title=title,
            author=resolved_author,
            participants=participants,
            date_created=(email_headers.get("date_created") if email_headers else None)
            or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None),
            date_modified=str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None,
            text_body=text_content,
            preview_file_name=f"{path.name}.html",
            chat_metadata=chat_metadata,
            chat_entries=chat_entries,
        )
        if chat_metadata
        else []
    )
    return {
        "page_count": None,
        "author": resolved_author,
        "content_type": determine_content_type(path, text_content, email_headers=email_headers, chat_metadata=chat_metadata),
        "date_created": (email_headers.get("date_created") if email_headers else None) or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None),
        "date_modified": str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None,
        "participants": participants,
        "title": title,
        "subject": email_headers.get("subject") if email_headers else None,
        "recipients": email_headers.get("recipients") if email_headers else None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else text_status,
        "preview_artifacts": preview_artifacts,
    }


def extract_native_preview_only_file(path: Path, explicit_content_type: str | None = None) -> dict[str, object]:
    return {
        "page_count": None,
        "author": None,
        "content_type": explicit_content_type or determine_content_type(path, ""),
        "date_created": None,
        "date_modified": None,
        "participants": None,
        "title": None,
        "subject": None,
        "recipients": None,
        "text_content": "",
        "text_status": "empty",
        "preview_artifacts": [],
    }


def extract_rtf_file(path: Path) -> dict[str, object]:
    rtf_to_text_fn = dependency_guard("rtf_to_text", "striprtf", "rtf")
    decoded, text_status, _ = decode_bytes(path.read_bytes())
    text_content = normalize_whitespace(rtf_to_text_fn(decoded))
    email_headers = extract_email_like_headers(text_content)
    chat_metadata = extract_chat_transcript_metadata(text_content)
    participants = extract_email_chain_participants(
        text_content,
        [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
    ) or (str(chat_metadata["participants"]) if chat_metadata and chat_metadata.get("participants") else None) or extract_chat_participants(text_content)
    title = email_headers.get("title") if email_headers else (str(chat_metadata["title"]) if chat_metadata and chat_metadata.get("title") else None)
    if title is None and text_content:
        title = text_content.splitlines()[0][:200]
    resolved_author = None if chat_metadata else (email_headers.get("author") if email_headers else None)
    preview_artifacts = (
        build_chat_preview_artifacts(
            title=title,
            author=resolved_author,
            participants=participants,
            date_created=(email_headers.get("date_created") if email_headers else None)
            or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None),
            date_modified=str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None,
            text_body=text_content,
            preview_file_name=f"{path.name}.html",
            chat_metadata=chat_metadata,
            label="text",
        )
        if chat_metadata
        else [
            {
                "file_name": f"{path.name}.html",
                "preview_type": "html",
                "label": "text",
                "ordinal": 0,
                "content": build_html_preview(
                    {},
                    body_text=text_content,
                    document_title=title or path.stem or path.name,
                ),
            }
        ]
    )
    return {
        "page_count": None,
        "author": resolved_author,
        "content_type": determine_content_type(
            path,
            text_content,
            email_headers=email_headers,
            chat_metadata=chat_metadata,
            explicit_content_type="E-Doc",
        ),
        "date_created": (email_headers.get("date_created") if email_headers else None) or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None),
        "date_modified": str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None,
        "participants": participants,
        "title": title,
        "subject": email_headers.get("subject") if email_headers else None,
        "recipients": email_headers.get("recipients") if email_headers else None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else text_status,
        "preview_artifacts": preview_artifacts,
    }


def extract_pdf_file(path: Path) -> dict[str, object]:
    pdfplumber_module = dependency_guard("pdfplumber", "pdfplumber", "pdf")
    with pdfplumber_module.open(path) as pdf:  # type: ignore[union-attr]
        metadata = pdf.metadata or {}
        texts = [(page.extract_text() or "").strip() for page in pdf.pages]
        text_content = normalize_whitespace("\n\n".join(part for part in texts if part))
        email_headers = extract_email_like_headers(texts[0] if texts else "")
        chat_metadata = extract_chat_transcript_metadata(text_content)
        participants = extract_email_chain_participants(
            text_content,
            [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
        ) or (str(chat_metadata["participants"]) if chat_metadata and chat_metadata.get("participants") else None) or extract_chat_participants(text_content)
        resolved_author = (
            None
            if chat_metadata
            else (email_headers.get("author") if email_headers else None) or metadata.get("Author")
        )
        preview_artifacts = (
            build_chat_preview_artifacts(
                title=(email_headers.get("title") if email_headers else None)
                or (str(chat_metadata["title"]) if chat_metadata and chat_metadata.get("title") else None)
                or metadata.get("Title"),
                author=resolved_author,
                participants=participants,
                date_created=(email_headers.get("date_created") if email_headers else None)
                or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None)
                or normalize_datetime(metadata.get("CreationDate")),
                date_modified=(str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None)
                or normalize_datetime(metadata.get("ModDate")),
                text_body=text_content,
                preview_file_name=f"{path.name}.html",
                chat_metadata=chat_metadata,
            )
            if chat_metadata
            else []
        )
        return {
            "page_count": len(pdf.pages),
            "author": resolved_author,
            "content_type": determine_content_type(
                path,
                text_content,
                email_headers=email_headers,
                chat_metadata=chat_metadata,
                explicit_content_type="E-Doc",
            ),
            "date_created": (email_headers.get("date_created") if email_headers else None)
            or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None)
            or normalize_datetime(metadata.get("CreationDate")),
            "date_modified": (str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None)
            or normalize_datetime(metadata.get("ModDate")),
            "participants": participants,
            "title": (email_headers.get("title") if email_headers else None)
            or (str(chat_metadata["title"]) if chat_metadata and chat_metadata.get("title") else None)
            or metadata.get("Title"),
            "subject": (email_headers.get("subject") if email_headers else None) or metadata.get("Subject"),
            "recipients": email_headers.get("recipients") if email_headers else None,
            "text_content": text_content,
            "text_status": "empty" if not text_content else "ok",
            "preview_artifacts": preview_artifacts,
        }


def extract_docx_file(path: Path) -> dict[str, object]:
    docx_document_cls = dependency_guard("DocxDocument", "python-docx", "docx")
    document = docx_document_cls(path)  # type: ignore[operator]
    text_content = normalize_whitespace("\n\n".join(paragraph.text for paragraph in document.paragraphs if paragraph.text))
    props = document.core_properties
    email_headers = extract_email_like_headers(text_content)
    chat_metadata = extract_chat_transcript_metadata(text_content)
    participants = extract_email_chain_participants(
        text_content,
        [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
    ) or (str(chat_metadata["participants"]) if chat_metadata and chat_metadata.get("participants") else None) or extract_chat_participants(text_content)
    resolved_author = (
        None
        if chat_metadata
        else (email_headers.get("author") if email_headers else None) or props.author or None
    )
    preview_artifacts = (
        build_chat_preview_artifacts(
            title=(email_headers.get("title") if email_headers else None)
            or (str(chat_metadata["title"]) if chat_metadata and chat_metadata.get("title") else None)
            or props.title
            or None,
            author=resolved_author,
            participants=participants,
            date_created=(email_headers.get("date_created") if email_headers else None)
            or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None)
            or normalize_datetime(props.created),
            date_modified=(str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None)
            or normalize_datetime(props.modified),
            text_body=text_content,
            preview_file_name=f"{path.name}.html",
            chat_metadata=chat_metadata,
        )
        if chat_metadata
        else []
    )
    return {
        "page_count": None,
        "author": resolved_author,
        "content_type": determine_content_type(
            path,
            text_content,
            email_headers=email_headers,
            chat_metadata=chat_metadata,
            explicit_content_type="E-Doc",
        ),
        "date_created": (email_headers.get("date_created") if email_headers else None)
        or (str(chat_metadata["date_created"]) if chat_metadata and chat_metadata.get("date_created") else None)
        or normalize_datetime(props.created),
        "date_modified": (str(chat_metadata["date_modified"]) if chat_metadata and chat_metadata.get("date_modified") else None)
        or normalize_datetime(props.modified),
        "participants": participants,
        "title": (email_headers.get("title") if email_headers else None)
        or (str(chat_metadata["title"]) if chat_metadata and chat_metadata.get("title") else None)
        or props.title
        or None,
        "subject": (email_headers.get("subject") if email_headers else None) or props.subject or None,
        "recipients": email_headers.get("recipients") if email_headers else None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else "ok",
        "preview_artifacts": preview_artifacts,
    }


def stringify_spreadsheet_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return normalize_datetime(value) or value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return value


def spreadsheet_column_label(column_index: int) -> str:
    index = max(1, int(column_index))
    letters: list[str] = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(ord("A") + remainder))
    return "".join(reversed(letters))


def spreadsheet_text_value(value: object) -> str:
    rendered = stringify_spreadsheet_value(value)
    if rendered == "":
        return ""
    return normalize_inline_whitespace(str(rendered))


def spreadsheet_value_is_present(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def spreadsheet_row_last_nonempty_index(values: list[object]) -> int:
    for index in range(len(values), 0, -1):
        if spreadsheet_value_is_present(values[index - 1]):
            return index
    return 0


def normalize_spreadsheet_headers(raw_headers: list[object], column_count: int) -> list[str]:
    headers: list[str] = []
    seen: dict[str, int] = {}
    for column_index in range(1, column_count + 1):
        raw_value = raw_headers[column_index - 1] if column_index - 1 < len(raw_headers) else None
        base = spreadsheet_text_value(raw_value) or f"Column {spreadsheet_column_label(column_index)}"
        key = base.casefold()
        seen[key] = seen.get(key, 0) + 1
        headers.append(base if seen[key] == 1 else f"{base} ({seen[key]})")
    return headers


def append_spreadsheet_participants(
    participants: list[str],
    seen: set[str],
    raw_values: list[str | None],
) -> None:
    if len(participants) >= SPREADSHEET_MAX_PARTICIPANTS:
        return
    append_unique_participants(participants, seen, raw_values)
    if len(participants) > SPREADSHEET_MAX_PARTICIPANTS:
        del participants[SPREADSHEET_MAX_PARTICIPANTS:]


def spreadsheet_number_format_hint(number_formats: list[str]) -> str | None:
    for raw_format in number_formats:
        normalized = str(raw_format or "").strip().lower()
        if not normalized or normalized == "general":
            continue
        if "%" in normalized:
            return "percent"
        if "[$" in normalized or any(symbol in normalized for symbol in ("$", "€", "£", "¥", "₹")):
            return "currency"
        if any(token in normalized for token in ("yy", "dd", "mmm", "am/pm")):
            if any(token in normalized for token in ("h", "ss", "am/pm")):
                return "datetime"
            return "date"
    return None


def spreadsheet_string_shape(text: str) -> str:
    normalized = text.strip()
    lowered = normalized.lower()
    if not normalized:
        return "string"
    if lowered in {"true", "false", "yes", "no", "y", "n"}:
        return "boolean"
    if re.fullmatch(r"[$€£¥₹]\s*[-+]?\d[\d,]*(?:\.\d+)?", normalized):
        return "currency"
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?%", normalized):
        return "percent"
    if re.fullmatch(r"[-+]?\d+", normalized):
        return "integer"
    if re.fullmatch(r"[-+]?(?:\d+\.\d*|\d*\.\d+)(?:[eE][-+]?\d+)?", normalized) or re.fullmatch(
        r"[-+]?\d+[eE][-+]?\d+",
        normalized,
    ):
        return "number"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?", normalized):
        return "datetime"
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?", normalized, re.IGNORECASE):
        return "datetime"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        return "date"
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", normalized):
        return "date"
    return "string"


def infer_spreadsheet_value_kind(value: object) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, date):
        return "date"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "integer" if value.is_integer() else "number"
    if isinstance(value, str):
        return spreadsheet_string_shape(value)
    return "string"


def infer_spreadsheet_column_type(
    samples: list[object],
    number_formats: list[str],
    *,
    force_enum: bool = False,
) -> str:
    if force_enum:
        return "enum"
    format_hint = spreadsheet_number_format_hint(number_formats)
    if format_hint is not None:
        return format_hint
    sample_kinds = {infer_spreadsheet_value_kind(value) for value in samples if spreadsheet_value_is_present(value)}
    if not sample_kinds:
        return "string"
    if sample_kinds <= {"boolean"}:
        return "boolean"
    if sample_kinds <= {"integer"}:
        return "integer"
    if sample_kinds <= {"integer", "number"}:
        return "number" if "number" in sample_kinds else "integer"
    if sample_kinds <= {"date"}:
        return "date"
    if sample_kinds <= {"date", "datetime"}:
        return "datetime" if "datetime" in sample_kinds else "date"
    if "currency" in sample_kinds and sample_kinds <= {"currency", "integer", "number"}:
        return "currency"
    if "percent" in sample_kinds and sample_kinds <= {"percent", "integer", "number"}:
        return "percent"
    return "string"


def build_spreadsheet_sheet_section(
    sheet_name: str,
    column_descriptors: list[str],
    *,
    hidden_column_count: int = 0,
    comment_lines: list[str] | None = None,
    comment_overflow: int = 0,
    validation_lines: list[str] | None = None,
    hyperlink_lines: list[str] | None = None,
    chart_lines: list[str] | None = None,
) -> str:
    lines = [f"Sheet: {sheet_name}"]
    if column_descriptors:
        lines.append(f"Columns: {', '.join(column_descriptors)}")
    else:
        lines.append("Columns: none detected")
    if hidden_column_count > 0:
        lines.append(f"Columns truncated: {hidden_column_count} more columns not listed.")
    if comment_lines:
        lines.append("Comments:")
        lines.extend(comment_lines)
    if comment_overflow > 0:
        lines.append(f"Comments truncated: {comment_overflow} more comments not listed.")
    if validation_lines:
        lines.append("Validations:")
        lines.extend(validation_lines)
    if hyperlink_lines:
        lines.append("Hyperlinks:")
        lines.extend(hyperlink_lines)
    if chart_lines:
        lines.append("Charts:")
        lines.extend(chart_lines)
    return normalize_whitespace("\n".join(lines))


def build_spreadsheet_header_section(
    *,
    title: str,
    sheet_names: list[str],
    author: str | None,
    subject: str | None,
    participants: str | None,
    parse_note: str | None = None,
) -> str:
    lines = [
        f"Workbook: {title}",
        f"Sheets: {', '.join(sheet_names) if sheet_names else title}",
    ]
    if author:
        lines.append(f"Author: {author}")
    if subject:
        lines.append(f"Subject: {subject}")
    if participants:
        lines.append(f"Participants: {participants}")
    if parse_note:
        lines.append(parse_note)
    return normalize_whitespace("\n".join(lines))


def build_structural_summary_from_sections(sections: list[str]) -> tuple[str, list[dict[str, object]]]:
    normalized_sections = [normalize_whitespace(section) for section in sections if normalize_whitespace(section)]
    if not normalized_sections:
        return "", []

    kept_sections: list[str] = []
    truncation_note = "Structural summary truncated."
    for section in normalized_sections:
        candidate_text = normalize_whitespace("\n\n".join([*kept_sections, section]))
        if len(candidate_text) <= SPREADSHEET_MAX_SUMMARY_CHARS:
            kept_sections.append(section)
            continue
        candidate_with_note = normalize_whitespace("\n\n".join([*kept_sections, truncation_note]))
        while kept_sections and len(candidate_with_note) > SPREADSHEET_MAX_SUMMARY_CHARS:
            kept_sections.pop()
            candidate_with_note = normalize_whitespace("\n\n".join([*kept_sections, truncation_note]))
        if len(candidate_with_note) <= SPREADSHEET_MAX_SUMMARY_CHARS:
            kept_sections.append(truncation_note)
        else:
            kept_sections = [truncation_note[:SPREADSHEET_MAX_SUMMARY_CHARS]]
        break

    text_content = normalize_whitespace("\n\n".join(kept_sections))
    chunks: list[dict[str, object]] = []
    chunk_index = 0
    char_cursor = 0
    for section_index, section in enumerate(kept_sections):
        if section_index > 0:
            char_cursor += 2
        for section_chunk in chunk_text(section):
            chunks.append(
                {
                    "chunk_index": chunk_index,
                    "char_start": char_cursor + int(section_chunk["char_start"]),
                    "char_end": char_cursor + int(section_chunk["char_end"]),
                    "token_estimate": int(section_chunk["token_estimate"]),
                    "text_content": str(section_chunk["text_content"]),
                }
            )
            chunk_index += 1
        char_cursor += len(section)
    return text_content, chunks


def parse_spreadsheet_literal_list(formula: str) -> list[str]:
    inner = formula[1:-1]
    try:
        values = next(csv.reader([inner]))
    except Exception:
        values = [part.strip() for part in inner.split(",")]
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = spreadsheet_text_value(value)
        if not normalized:
            continue
        key = normalized.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(normalized)
        if len(deduped) >= SPREADSHEET_MAX_ENUM_VALUES:
            break
    return deduped


def flatten_openpyxl_range_cells(range_cells: object) -> list[object]:
    if range_cells is None:
        return []
    if isinstance(range_cells, tuple):
        flattened: list[object] = []
        for item in range_cells:
            flattened.extend(flatten_openpyxl_range_cells(item))
        return flattened
    return [range_cells]


def resolve_openpyxl_reference_values(workbook: object, reference: object) -> list[str]:
    normalized_reference = normalize_inline_whitespace(str(reference or "")).lstrip("=")
    if not normalized_reference:
        return []

    values: list[str] = []
    if normalized_reference.startswith('"') and normalized_reference.endswith('"'):
        return parse_spreadsheet_literal_list(normalized_reference)

    destinations: list[tuple[str, str]] = []
    direct_match = re.fullmatch(r"(?:'((?:[^']|'')+)'|([^!]+))!(.+)", normalized_reference)
    if direct_match:
        sheet_name = direct_match.group(1) or direct_match.group(2) or ""
        destinations.append((sheet_name.replace("''", "'"), direct_match.group(3)))
    else:
        defined_name = getattr(workbook, "defined_names", {}).get(normalized_reference)
        if defined_name is not None:
            try:
                destinations.extend(list(defined_name.destinations))
            except Exception:
                pass

    seen: set[str] = set()
    for sheet_name, range_ref in destinations:
        try:
            sheet = workbook[sheet_name]
            resolved_cells = flatten_openpyxl_range_cells(sheet[range_ref])
        except Exception:
            continue
        for cell in resolved_cells:
            normalized_value = spreadsheet_text_value(getattr(cell, "value", None))
            if not normalized_value:
                continue
            key = normalized_value.casefold()
            if key not in seen:
                seen.add(key)
                values.append(normalized_value)
            if len(values) >= SPREADSHEET_MAX_ENUM_VALUES:
                return values
    return values


def extract_openpyxl_chart_text(workbook: object, title: object) -> str | None:
    if title is None:
        return None
    if isinstance(title, str):
        return spreadsheet_text_value(title) or None
    tx = getattr(title, "tx", None)
    rich = getattr(tx, "rich", None) or getattr(title, "rich", None)
    if rich is not None:
        parts: list[str] = []
        for paragraph in getattr(rich, "p", []) or []:
            for run in getattr(paragraph, "r", []) or []:
                text_value = spreadsheet_text_value(getattr(run, "t", None))
                if text_value:
                    parts.append(text_value)
        if parts:
            return " ".join(parts)
    str_ref = getattr(tx, "strRef", None) or getattr(title, "strRef", None)
    formula = getattr(str_ref, "f", None)
    if formula:
        values = resolve_openpyxl_reference_values(workbook, formula)
        if values:
            return values[0]
    return None


def extract_openpyxl_chart_lines(workbook: object, sheet: object) -> list[str]:
    lines: list[str] = []
    for chart in getattr(sheet, "_charts", []) or []:
        title = extract_openpyxl_chart_text(workbook, getattr(chart, "title", None))
        x_axis = extract_openpyxl_chart_text(workbook, getattr(getattr(chart, "x_axis", None), "title", None))
        y_axis = extract_openpyxl_chart_text(workbook, getattr(getattr(chart, "y_axis", None), "title", None))
        parts: list[str] = []
        if title:
            parts.append(title)
        if x_axis:
            parts.append(f"X axis: {x_axis}")
        if y_axis:
            parts.append(f"Y axis: {y_axis}")
        if parts:
            lines.append(f"- {'; '.join(parts)}")
    return lines


def openpyxl_validation_map(workbook: object, sheet: object, headers: list[str], header_row_index: int | None) -> dict[int, list[str]]:
    validation_map: dict[int, list[str]] = {}
    if header_row_index is None:
        return validation_map
    data_validations = getattr(getattr(sheet, "data_validations", None), "dataValidation", None)
    if not data_validations:
        return validation_map

    for validation in data_validations:
        if str(getattr(validation, "type", "")).lower() != "list":
            continue
        formula = str(getattr(validation, "formula1", "") or "").strip()
        if not formula:
            continue
        values = resolve_openpyxl_reference_values(workbook, formula)
        if not values:
            continue
        try:
            ranges = list(getattr(getattr(validation, "cells", None), "ranges", []) or [])
        except Exception:
            ranges = []
        covered_columns: set[int] = set()
        for cell_range in ranges:
            if getattr(cell_range, "max_row", 0) and int(cell_range.max_row) < header_row_index:
                continue
            for column_index in range(int(cell_range.min_col), int(cell_range.max_col) + 1):
                covered_columns.add(column_index)
        for column_index in sorted(covered_columns):
            validation_map[column_index] = values[:SPREADSHEET_MAX_ENUM_VALUES]
    return validation_map


def extract_openpyxl_named_ranges(workbook: object) -> list[str]:
    lines: list[str] = []
    for name, defined_name in getattr(workbook, "defined_names", {}).items():
        normalized_name = spreadsheet_text_value(name)
        if not normalized_name:
            continue
        lowered = normalized_name.casefold()
        if lowered in {"_xlnm.print_area", "_xlnm.print_titles"}:
            continue
        target = spreadsheet_text_value(getattr(defined_name, "attr_text", None))
        if not target:
            try:
                destinations = [f"{sheet_name}!{range_ref}" for sheet_name, range_ref in defined_name.destinations]
            except Exception:
                destinations = []
            target = ", ".join(destinations)
        if not target:
            continue
        lines.append(f"- {normalized_name} -> {target}")
        if len(lines) >= SPREADSHEET_MAX_NAMED_RANGES:
            break
    return lines


def scan_openpyxl_sheet_surface(
    workbook: object,
    sheet: object,
    *,
    read_only: bool,
    workbook_hyperlinks_seen: set[str],
    participants: list[str],
    participant_seen: set[str],
) -> dict[str, object]:
    output = io.StringIO()
    writer = csv.writer(output)
    header_row_index: int | None = None
    raw_headers: list[object] = []
    observed_columns = 0
    column_samples: dict[int, list[object]] = defaultdict(list)
    column_formats: dict[int, list[str]] = defaultdict(list)
    comment_lines: list[str] = []
    comment_overflow = 0
    hyperlink_lines: list[str] = []

    for row_index, row in enumerate(sheet.iter_rows(), start=1):
        row_values = [getattr(cell, "value", None) for cell in row]
        writer.writerow([stringify_spreadsheet_value(value) for value in row_values])
        last_nonempty = spreadsheet_row_last_nonempty_index(row_values)

        if header_row_index is None:
            if last_nonempty == 0:
                pass
            else:
                header_row_index = row_index
                raw_headers = row_values[:last_nonempty]
                observed_columns = max(observed_columns, last_nonempty)
        else:
            observed_columns = max(observed_columns, last_nonempty)
            if last_nonempty > 0:
                for column_index, cell in enumerate(row[:last_nonempty], start=1):
                    value = getattr(cell, "value", None)
                    if not spreadsheet_value_is_present(value):
                        continue
                    if len(column_samples[column_index]) < SPREADSHEET_TYPE_SAMPLE_LIMIT:
                        column_samples[column_index].append(value)
                    number_format = spreadsheet_text_value(getattr(cell, "number_format", None))
                    if number_format and len(column_formats[column_index]) < SPREADSHEET_TYPE_SAMPLE_LIMIT:
                        column_formats[column_index].append(number_format)

        if read_only:
            continue

        for cell in row:
            comment = getattr(cell, "comment", None)
            if comment is not None:
                author = spreadsheet_text_value(getattr(comment, "author", None)) or "Unknown"
                body = normalize_whitespace(str(getattr(comment, "text", "") or ""))
                if body:
                    append_spreadsheet_participants(participants, participant_seen, [author])
                    if len(comment_lines) < SPREADSHEET_MAX_COMMENTS_PER_SHEET:
                        comment_lines.append(f"- {author}: {body}")
                    else:
                        comment_overflow += 1

            hyperlink = getattr(cell, "hyperlink", None)
            target = spreadsheet_text_value(getattr(hyperlink, "target", None) or getattr(hyperlink, "location", None))
            if not target:
                continue
            display = spreadsheet_text_value(getattr(cell, "value", None))
            rendered = target if not display or display == target else f"{target} ({display})"
            hyperlink_key = rendered.casefold()
            if hyperlink_key in workbook_hyperlinks_seen:
                continue
            if len(workbook_hyperlinks_seen) >= SPREADSHEET_MAX_HYPERLINKS_PER_WORKBOOK:
                continue
            workbook_hyperlinks_seen.add(hyperlink_key)
            hyperlink_lines.append(f"- {rendered}")

    headers = normalize_spreadsheet_headers(raw_headers, observed_columns)
    validation_map = {} if read_only else openpyxl_validation_map(workbook, sheet, headers, header_row_index)
    column_descriptors: list[str] = []
    visible_column_count = min(len(headers), SPREADSHEET_MAX_COLUMNS_PER_SHEET)
    for column_index in range(1, visible_column_count + 1):
        header = headers[column_index - 1]
        column_type = infer_spreadsheet_column_type(
            column_samples.get(column_index, []),
            column_formats.get(column_index, []),
            force_enum=column_index in validation_map,
        )
        column_descriptors.append(f"{header} ({column_type})")

    validation_lines: list[str] = []
    for column_index in sorted(validation_map):
        if column_index < 1 or column_index > len(headers):
            continue
        values = validation_map[column_index]
        validation_lines.append(f"- {headers[column_index - 1]} ∈ {{{', '.join(values)}}}")

    chart_lines = [] if read_only else extract_openpyxl_chart_lines(workbook, sheet)
    return {
        "sheet_name": str(sheet.title),
        "preview_csv": output.getvalue(),
        "column_descriptors": column_descriptors,
        "hidden_column_count": max(0, len(headers) - SPREADSHEET_MAX_COLUMNS_PER_SHEET),
        "comment_lines": comment_lines,
        "comment_overflow": comment_overflow,
        "validation_lines": validation_lines,
        "hyperlink_lines": hyperlink_lines,
        "chart_lines": chart_lines,
    }


def xls_cell_number_format(workbook: object, cell: object) -> str | None:
    try:
        xf = workbook.xf_list[cell.xf_index]
        format_obj = workbook.format_map.get(xf.format_key)
    except Exception:
        return None
    if format_obj is None:
        return None
    return spreadsheet_text_value(getattr(format_obj, "format_str", None)) or None


def extract_xls_named_ranges(workbook: object) -> list[str]:
    lines: list[str] = []
    for name_obj in getattr(workbook, "name_obj_list", []) or []:
        name = spreadsheet_text_value(getattr(name_obj, "name", None))
        if not name:
            continue
        lowered = name.casefold()
        if lowered in {"_xlnm.print_area", "_xlnm.print_titles"}:
            continue
        target = spreadsheet_text_value(getattr(name_obj, "formula_text", None) or getattr(name_obj, "raw_formula", None))
        if not target:
            continue
        lines.append(f"- {name} -> {target}")
        if len(lines) >= SPREADSHEET_MAX_NAMED_RANGES:
            break
    return lines


def scan_xls_sheet_surface(
    workbook: object,
    sheet: object,
    *,
    workbook_hyperlinks_seen: set[str],
    participants: list[str],
    participant_seen: set[str],
) -> dict[str, object]:
    output = io.StringIO()
    writer = csv.writer(output)
    header_row_index: int | None = None
    raw_headers: list[object] = []
    observed_columns = 0
    column_samples: dict[int, list[object]] = defaultdict(list)
    column_formats: dict[int, list[str]] = defaultdict(list)

    for row_index in range(sheet.nrows):
        row = sheet.row(row_index)
        row_values = [cell.value for cell in row]
        writer.writerow([stringify_spreadsheet_value(value) for value in row_values])
        last_nonempty = spreadsheet_row_last_nonempty_index(row_values)

        if header_row_index is None:
            if last_nonempty == 0:
                continue
            header_row_index = row_index + 1
            raw_headers = row_values[:last_nonempty]
            observed_columns = max(observed_columns, last_nonempty)
            continue

        observed_columns = max(observed_columns, last_nonempty)
        if last_nonempty == 0:
            continue
        for column_index, cell in enumerate(row[:last_nonempty], start=1):
            if not spreadsheet_value_is_present(cell.value):
                continue
            if len(column_samples[column_index]) < SPREADSHEET_TYPE_SAMPLE_LIMIT:
                column_samples[column_index].append(cell.value)
            number_format = xls_cell_number_format(workbook, cell)
            if number_format and len(column_formats[column_index]) < SPREADSHEET_TYPE_SAMPLE_LIMIT:
                column_formats[column_index].append(number_format)

    headers = normalize_spreadsheet_headers(raw_headers, observed_columns)
    column_descriptors: list[str] = []
    visible_column_count = min(len(headers), SPREADSHEET_MAX_COLUMNS_PER_SHEET)
    for column_index in range(1, visible_column_count + 1):
        header = headers[column_index - 1]
        column_type = infer_spreadsheet_column_type(column_samples.get(column_index, []), column_formats.get(column_index, []))
        column_descriptors.append(f"{header} ({column_type})")

    comment_lines: list[str] = []
    comment_overflow = 0
    for note in getattr(sheet, "cell_note_map", {}).values():
        author = spreadsheet_text_value(getattr(note, "author", None)) or "Unknown"
        body = normalize_whitespace(str(getattr(note, "text", "") or ""))
        if not body:
            continue
        append_spreadsheet_participants(participants, participant_seen, [author])
        if len(comment_lines) < SPREADSHEET_MAX_COMMENTS_PER_SHEET:
            comment_lines.append(f"- {author}: {body}")
        else:
            comment_overflow += 1

    hyperlink_lines: list[str] = []
    hyperlink_candidates = list(getattr(sheet, "hyperlink_list", []) or [])
    hyperlink_candidates.extend(list(getattr(getattr(sheet, "hyperlink_map", {}), "values", lambda: [])()))
    for hyperlink in hyperlink_candidates:
        target = spreadsheet_text_value(
            getattr(hyperlink, "url_or_path", None) or getattr(hyperlink, "target", None) or getattr(hyperlink, "url", None)
        )
        if not target:
            continue
        display = spreadsheet_text_value(getattr(hyperlink, "desc", None) or getattr(hyperlink, "description", None))
        rendered = target if not display or display == target else f"{target} ({display})"
        hyperlink_key = rendered.casefold()
        if hyperlink_key in workbook_hyperlinks_seen:
            continue
        if len(workbook_hyperlinks_seen) >= SPREADSHEET_MAX_HYPERLINKS_PER_WORKBOOK:
            continue
        workbook_hyperlinks_seen.add(hyperlink_key)
        hyperlink_lines.append(f"- {rendered}")

    return {
        "sheet_name": str(sheet.name),
        "preview_csv": output.getvalue(),
        "column_descriptors": column_descriptors,
        "hidden_column_count": max(0, len(headers) - SPREADSHEET_MAX_COLUMNS_PER_SHEET),
        "comment_lines": comment_lines,
        "comment_overflow": comment_overflow,
        "validation_lines": [],
        "hyperlink_lines": hyperlink_lines,
        "chart_lines": [],
    }


def extract_xlsx_file(path: Path) -> dict[str, object]:
    openpyxl_module = dependency_guard("openpyxl", "openpyxl", "xlsx")
    read_only = path.stat().st_size > SPREADSHEET_XLSX_READ_ONLY_FALLBACK_BYTES
    workbook = openpyxl_module.load_workbook(path, read_only=read_only, data_only=True)  # type: ignore[union-attr]
    preview_artifacts: list[dict[str, object]] = []
    sections: list[str] = []
    workbook_hyperlinks_seen: set[str] = set()
    participants: list[str] = []
    participant_seen: set[str] = set()
    try:
        props = getattr(workbook, "properties", None)
        author = spreadsheet_text_value(getattr(props, "creator", None)) or None
        title = spreadsheet_text_value(getattr(props, "title", None)) or path.stem
        subject = spreadsheet_text_value(getattr(props, "subject", None)) or None
        last_modified_by = spreadsheet_text_value(getattr(props, "lastModifiedBy", None)) or None
        append_spreadsheet_participants(participants, participant_seen, [author, last_modified_by])

        sheet_names = [str(sheet.title) for sheet in workbook.worksheets]
        sections.append(
            build_spreadsheet_header_section(
                title=title,
                sheet_names=sheet_names,
                author=author,
                subject=subject,
                participants=None,
                parse_note=(
                    "Parse note: large-workbook read-only fallback; workbook properties and named ranges are preserved, "
                    "but comments, hyperlinks, validations, charts, and other sheet-level details may be incomplete."
                    if read_only
                    else None
                ),
            )
        )

        for ordinal, sheet in enumerate(workbook.worksheets):
            surface = scan_openpyxl_sheet_surface(
                workbook,
                sheet,
                read_only=read_only,
                workbook_hyperlinks_seen=workbook_hyperlinks_seen,
                participants=participants,
                participant_seen=participant_seen,
            )
            preview_artifacts.append(
                {
                    "file_name": f"{path.name}.{slugify(str(surface['sheet_name']))}.csv",
                    "preview_type": "csv",
                    "label": surface["sheet_name"],
                    "ordinal": ordinal,
                    "content": surface["preview_csv"],
                }
            )
            sections.append(
                build_spreadsheet_sheet_section(
                    str(surface["sheet_name"]),
                    list(surface["column_descriptors"]),
                    hidden_column_count=int(surface["hidden_column_count"]),
                    comment_lines=list(surface["comment_lines"]),
                    comment_overflow=int(surface["comment_overflow"]),
                    validation_lines=list(surface["validation_lines"]),
                    hyperlink_lines=list(surface["hyperlink_lines"]),
                    chart_lines=list(surface["chart_lines"]),
                )
            )

        named_ranges = extract_openpyxl_named_ranges(workbook)
        if named_ranges:
            named_range_section_lines = ["Named ranges:"]
            named_range_section_lines.extend(named_ranges)
            if len(named_ranges) >= SPREADSHEET_MAX_NAMED_RANGES:
                named_range_section_lines.append("Named ranges truncated: more named ranges not listed.")
            sections.append(normalize_whitespace("\n".join(named_range_section_lines)))
    finally:
        workbook.close()

    participants_text = ", ".join(participants) or None
    if participants_text:
        sections[0] = build_spreadsheet_header_section(
            title=title,
            sheet_names=sheet_names,
            author=author,
            subject=subject,
            participants=participants_text,
            parse_note=(
                "Parse note: large-workbook read-only fallback; workbook properties and named ranges are preserved, "
                "but comments, hyperlinks, validations, charts, and other sheet-level details may be incomplete."
                if read_only
                else None
            ),
        )
    text_content, chunks = build_structural_summary_from_sections(sections)
    return {
        "page_count": len(preview_artifacts),
        "author": author,
        "content_type": "Spreadsheet / Table",
        "date_created": normalize_datetime(getattr(props, "created", None)),
        "date_modified": normalize_datetime(getattr(props, "modified", None)),
        "participants": participants_text,
        "title": title,
        "subject": subject,
        "recipients": None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else "ok",
        "chunks": chunks,
        "preview_artifacts": preview_artifacts,
    }


def extract_xls_file(path: Path) -> dict[str, object]:
    xlrd_module = dependency_guard("xlrd", "xlrd", "xls")
    workbook = xlrd_module.open_workbook(path, formatting_info=True)  # type: ignore[union-attr]
    preview_artifacts: list[dict[str, object]] = []
    sections: list[str] = []
    workbook_hyperlinks_seen: set[str] = set()
    participants: list[str] = []
    participant_seen: set[str] = set()

    author = spreadsheet_text_value(getattr(workbook, "user_name", None)) or None
    append_spreadsheet_participants(participants, participant_seen, [author])
    sheet_names = [str(sheet.name) for sheet in workbook.sheets()]
    sections.append(
        build_spreadsheet_header_section(
            title=path.stem,
            sheet_names=sheet_names,
            author=author,
            subject=None,
            participants=None,
        )
    )

    for ordinal, sheet in enumerate(workbook.sheets()):
        surface = scan_xls_sheet_surface(
            workbook,
            sheet,
            workbook_hyperlinks_seen=workbook_hyperlinks_seen,
            participants=participants,
            participant_seen=participant_seen,
        )
        preview_artifacts.append(
            {
                "file_name": f"{path.name}.{slugify(str(surface['sheet_name']))}.csv",
                "preview_type": "csv",
                "label": surface["sheet_name"],
                "ordinal": ordinal,
                "content": surface["preview_csv"],
            }
        )
        sections.append(
            build_spreadsheet_sheet_section(
                str(surface["sheet_name"]),
                list(surface["column_descriptors"]),
                hidden_column_count=int(surface["hidden_column_count"]),
                comment_lines=list(surface["comment_lines"]),
                comment_overflow=int(surface["comment_overflow"]),
                validation_lines=list(surface["validation_lines"]),
                hyperlink_lines=list(surface["hyperlink_lines"]),
                chart_lines=list(surface["chart_lines"]),
            )
        )

    named_ranges = extract_xls_named_ranges(workbook)
    if named_ranges:
        named_range_section_lines = ["Named ranges:"]
        named_range_section_lines.extend(named_ranges)
        if len(named_ranges) >= SPREADSHEET_MAX_NAMED_RANGES:
            named_range_section_lines.append("Named ranges truncated: more named ranges not listed.")
        sections.append(normalize_whitespace("\n".join(named_range_section_lines)))

    participants_text = ", ".join(participants) or None
    if participants_text:
        sections[0] = build_spreadsheet_header_section(
            title=path.stem,
            sheet_names=sheet_names,
            author=author,
            subject=None,
            participants=participants_text,
        )

    text_content, chunks = build_structural_summary_from_sections(sections)
    return {
        "page_count": len(preview_artifacts),
        "author": author,
        "content_type": "Spreadsheet / Table",
        "date_created": None,
        "date_modified": None,
        "participants": participants_text,
        "title": path.stem,
        "subject": None,
        "recipients": None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else "ok",
        "chunks": chunks,
        "preview_artifacts": preview_artifacts,
    }


def parse_csv_rows_for_summary(path: Path) -> tuple[list[list[str]], str]:
    decoded, text_status, _ = decode_bytes(path.read_bytes())
    normalized_text = decoded.lstrip("\ufeff")
    rows_source = normalized_text
    delimiter: str | None = None
    first_line, _, remainder = normalized_text.partition("\n")
    directive_match = re.fullmatch(r"\s*sep=(.)\s*", first_line)
    if directive_match is not None:
        delimiter = directive_match.group(1)
        rows_source = remainder

    if delimiter is None:
        sample = rows_source[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
            reader = csv.reader(io.StringIO(rows_source), dialect)
        except csv.Error:
            reader = csv.reader(io.StringIO(rows_source), csv.excel)
    else:
        reader = csv.reader(io.StringIO(rows_source), delimiter=delimiter)

    rows = [[field.strip().strip("\ufeff") for field in row] for row in reader if any(field.strip() for field in row)]
    return rows, text_status


def csv_first_row_looks_like_header(row: list[str]) -> bool:
    nonempty = [field.strip() for field in row if field.strip()]
    if not nonempty:
        return False
    if len({value.casefold() for value in nonempty}) != len(nonempty):
        return False
    if any(len(value) > 120 for value in nonempty):
        return False
    numeric_like = sum(1 for value in nonempty if spreadsheet_string_shape(value) in {"integer", "number", "currency", "percent", "date", "datetime"})
    return numeric_like < len(nonempty)


def extract_csv_file(path: Path) -> dict[str, object]:
    rows, text_status = parse_csv_rows_for_summary(path)
    title = path.stem
    if not rows:
        sections = [
            build_spreadsheet_header_section(
                title=title,
                sheet_names=[title],
                author=None,
                subject=None,
                participants=None,
            ),
            build_spreadsheet_sheet_section(title, []),
        ]
        text_content, chunks = build_structural_summary_from_sections(sections)
        return {
            "page_count": 1,
            "author": None,
            "content_type": "Spreadsheet / Table",
            "date_created": None,
            "date_modified": None,
            "participants": None,
            "title": title,
            "subject": None,
            "recipients": None,
            "text_content": text_content,
            "text_status": "empty" if not text_content else text_status,
            "chunks": chunks,
            "preview_artifacts": [],
        }

    has_headers = csv_first_row_looks_like_header(rows[0])
    header_row = rows[0] if has_headers else []
    data_rows = rows[1:] if has_headers else rows
    column_count = max((len(row) for row in rows), default=0)
    headers = normalize_spreadsheet_headers(header_row, column_count)

    column_samples: dict[int, list[object]] = defaultdict(list)
    for row in data_rows:
        for column_index in range(1, min(len(row), column_count) + 1):
            value = row[column_index - 1]
            if not spreadsheet_value_is_present(value):
                continue
            if len(column_samples[column_index]) < SPREADSHEET_TYPE_SAMPLE_LIMIT:
                column_samples[column_index].append(value)

    column_descriptors: list[str] = []
    visible_column_count = min(len(headers), SPREADSHEET_MAX_COLUMNS_PER_SHEET)
    for column_index in range(1, visible_column_count + 1):
        column_descriptors.append(
            f"{headers[column_index - 1]} ({infer_spreadsheet_column_type(column_samples.get(column_index, []), [])})"
        )

    sections = [
        build_spreadsheet_header_section(
            title=title,
            sheet_names=[title],
            author=None,
            subject=None,
            participants=None,
        ),
        build_spreadsheet_sheet_section(
            title,
            column_descriptors,
            hidden_column_count=max(0, len(headers) - SPREADSHEET_MAX_COLUMNS_PER_SHEET),
        ),
    ]
    text_content, chunks = build_structural_summary_from_sections(sections)
    return {
        "page_count": 1,
        "author": None,
        "content_type": "Spreadsheet / Table",
        "date_created": None,
        "date_modified": None,
        "participants": None,
        "title": title,
        "subject": None,
        "recipients": None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else text_status,
        "chunks": chunks,
        "preview_artifacts": [],
    }


def normalize_attachment_filename(
    file_name: str | None,
    ordinal: int,
    *,
    payload: bytes | None = None,
    content_type: object = None,
    preferred_extension: object = None,
) -> str:
    normalized = normalize_whitespace(file_name or "")
    detected_file_type = infer_attachment_file_type(
        file_name=normalized or None,
        payload=payload,
        content_type=content_type,
        preferred_extension=preferred_extension,
    )
    if normalized:
        current_file_type = normalize_file_type_name(Path(normalized).suffix.lower().lstrip("."))
        if (
            current_file_type == "bin"
            and re.fullmatch(rf"attachment-{ordinal:03d}\.bin", normalized, re.IGNORECASE)
            and detected_file_type
            and detected_file_type != "bin"
        ):
            return f"attachment-{ordinal:03d}.{detected_file_type}"
        if current_file_type:
            return normalized
        if detected_file_type:
            return f"{normalized}.{detected_file_type}"
        return normalized
    return f"attachment-{ordinal:03d}.{detected_file_type or 'bin'}"


def coerce_email_part_payload(part: object) -> bytes | None:
    try:
        payload = part.get_payload(decode=True)
    except Exception:
        payload = None
    if isinstance(payload, bytes):
        return payload
    try:
        content = part.get_content()
    except Exception:
        content = None
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        charset = None
        try:
            charset = part.get_content_charset()
        except Exception:
            charset = None
        return content.encode(charset or "utf-8", errors="replace")
    if hasattr(content, "as_bytes"):
        try:
            return content.as_bytes(policy=policy.default)
        except Exception:
            return None
    return None


def extract_eml_attachments(message: object) -> list[dict[str, object]]:
    attachments: list[dict[str, object]] = []
    ordinal = 1
    for part in message.walk():
        if part.is_multipart():
            continue
        disposition = (part.get_content_disposition() or "").lower()
        file_name = part.get_filename()
        content_type = normalize_mime_type(part.get_content_type())
        content_id = normalize_content_id(part.get("Content-ID"))
        is_inline = disposition == "inline" or (content_id is not None and disposition != "attachment")
        if disposition != "attachment" and not file_name and not content_id:
            continue
        payload = coerce_email_part_payload(part)
        if payload is None:
            continue
        attachments.append(
            {
                "file_name": normalize_attachment_filename(
                    file_name,
                    ordinal,
                    payload=payload,
                    content_type=content_type,
                ),
                "ordinal": ordinal,
                "payload": payload,
                "file_hash": sha256_bytes(payload),
                "content_type": content_type,
                "content_id": content_id,
                "is_inline": is_inline,
            }
        )
        ordinal += 1
    return attachments


def extract_msg_attachment_payload(attachment: object) -> bytes | None:
    data = getattr(attachment, "data", None)
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode("utf-8", errors="replace")
    if hasattr(data, "as_bytes"):
        try:
            return data.as_bytes(policy=policy.default)
        except Exception:
            return None
    try:
        with tempfile.TemporaryDirectory(prefix="retriever-msg-attachment-") as tempdir:
            saved = attachment.save(customPath=tempdir, extractEmbedded=True)
            if isinstance(saved, tuple) and len(saved) >= 2 and saved[1]:
                saved_path = Path(str(saved[1]))
                if saved_path.exists():
                    return saved_path.read_bytes()
    except Exception:
        return None
    return None


def extract_msg_attachment_content_id(attachment: object) -> str | None:
    for attr_name in ("cid", "contentId", "content_id"):
        raw = getattr(attachment, attr_name, None)
        normalized = normalize_content_id(raw)
        if normalized:
            return normalized
    return None


def extract_msg_attachment_content_type(attachment: object) -> str | None:
    for attr_name in ("mimeTag", "mime_tag", "mimetype", "mime_type", "contentType", "content_type"):
        normalized = normalize_mime_type(getattr(attachment, attr_name, None))
        if normalized:
            return normalized
    return None


def extract_msg_attachments(message: object) -> list[dict[str, object]]:
    attachments: list[dict[str, object]] = []
    ordinal = 1
    for attachment in getattr(message, "attachments", []):
        if getattr(attachment, "hidden", False):
            continue
        try:
            file_name = attachment.getFilename()
        except Exception:
            file_name = None
        content_type = extract_msg_attachment_content_type(attachment)
        payload = extract_msg_attachment_payload(attachment)
        if payload is None:
            continue
        attachments.append(
            {
                "file_name": normalize_attachment_filename(
                    file_name,
                    ordinal,
                    payload=payload,
                    content_type=content_type,
                ),
                "ordinal": ordinal,
                "payload": payload,
                "file_hash": sha256_bytes(payload),
                "content_type": content_type,
                "content_id": extract_msg_attachment_content_id(attachment),
            }
        )
        ordinal += 1
    return attachments


def build_email_extracted_payload(
    *,
    subject: str | None,
    author: str | None,
    recipients: str | None,
    date_created: str | None,
    text_body: str | None,
    html_body: str | None,
    attachments: list[dict[str, object]] | None,
    preview_file_name: str,
    email_threading: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized_html = None if html_body is None else str(html_body)
    normalized_text = normalize_whitespace(str(text_body or ""))
    if not normalized_text and normalized_html:
        normalized_text = strip_html_tags(normalized_html)
    normalized_text = normalize_whitespace(normalized_text)
    resolved_subject = normalize_generated_document_title(subject)
    participants = extract_email_chain_participants(
        normalized_text,
        [author, recipients or None],
    )
    normalized_attachments = list(attachments or [])
    preview_html_body = inline_cid_references_in_html(normalized_html, normalized_attachments)
    document_attachments = filter_html_preview_embedded_image_attachments(
        normalized_html,
        normalized_attachments,
    )
    calendar_invites, document_attachments = partition_calendar_invite_attachments(document_attachments)
    if calendar_invites:
        merged_participants: list[str] = []
        seen_participants: set[str] = set()
        append_unique_participants(merged_participants, seen_participants, [participants])
        for invite in calendar_invites:
            append_unique_participants(
                merged_participants,
                seen_participants,
                [invite.get("organizer"), invite.get("attendees")],
            )
        participants = ", ".join(merged_participants) or None
    calendar_invite_search_text = build_calendar_invite_search_text(calendar_invites)
    indexed_text_content = "\n\n".join(
        part
        for part in (normalized_text, calendar_invite_search_text)
        if part
    )
    preview = build_email_message_preview_html(
        {
            "id": 0,
            "author": author,
            "recipients": recipients or None,
            "date_created": date_created,
            "subject": resolved_subject,
            "title": resolved_subject,
            "text_content": indexed_text_content,
            "standalone_preview_body_html": preview_html_body,
        },
        body_html=preview_html_body,
    )
    return {
        "page_count": 1,
        "author": author,
        "content_type": "Email",
        "date_created": date_created,
        "date_modified": None,
        "participants": participants,
        "title": resolved_subject,
        "subject": resolved_subject,
        "recipients": recipients or None,
        "text_content": indexed_text_content,
        "text_status": "empty" if not indexed_text_content else "ok",
        "attachments": document_attachments,
        "email_threading": dict(email_threading or {}),
        "preview_artifacts": [
            {
                "file_name": preview_file_name,
                "preview_type": "html",
                "label": "message",
                "ordinal": 0,
                "content": preview,
            }
        ],
    }


def parse_email_headers_only(header_text: object) -> object | None:
    normalized = str(header_text or "")
    if not normalize_whitespace(normalized):
        return None
    payload = normalized.replace("\r\n", "\n").replace("\r", "\n")
    if "\n\n" not in payload:
        payload = payload.rstrip("\n") + "\n\n"
    try:
        return BytesParser(policy=policy.default).parsebytes(payload.encode("utf-8", errors="replace"), headersonly=True)
    except Exception:
        return None


def email_header_value(header_mapping: object, key: str) -> str | None:
    target = normalize_whitespace(key).lower()
    if isinstance(header_mapping, dict):
        for raw_key, raw_value in header_mapping.items():
            if normalize_whitespace(str(raw_key or "")).lower() != target:
                continue
            normalized = normalize_whitespace(str(raw_value or ""))
            return normalized or None
    return None


def extract_email_recipients_from_headers(header_text: object) -> str | None:
    parsed_headers = parse_email_headers_only(header_text)
    if parsed_headers is None:
        return None
    recipients: list[str] = []
    seen: set[str] = set()
    append_unique_participants(
        recipients,
        seen,
        [
            normalize_whitespace(str(parsed_headers.get("To") or "")) or None,
            normalize_whitespace(str(parsed_headers.get("Cc") or "")) or None,
            normalize_whitespace(str(parsed_headers.get("Bcc") or "")) or None,
        ],
    )
    return ", ".join(recipients) or None


def build_email_threading_payload(
    *,
    subject: object,
    message_id: object = None,
    in_reply_to: object = None,
    references: object = None,
    conversation_index: object = None,
    conversation_topic: object = None,
) -> dict[str, object]:
    normalized_conversation_topic = normalize_email_thread_subject(conversation_topic)
    normalized_subject = normalize_email_thread_subject(subject or conversation_topic)
    return {
        "message_id": normalize_email_message_id(message_id),
        "in_reply_to": normalize_email_message_id(in_reply_to),
        "references": extract_email_message_ids(references),
        "conversation_index": normalize_whitespace(str(conversation_index or "")) or None,
        "conversation_topic": normalized_conversation_topic,
        "normalized_subject": normalized_subject,
    }


def extract_parsed_email_threading(message: object) -> dict[str, object]:
    return build_email_threading_payload(
        subject=message.get("Subject"),
        message_id=message.get("Message-ID") or message.get("Message-Id"),
        in_reply_to=message.get("In-Reply-To"),
        references=message.get("References"),
        conversation_index=message.get("Conversation-Index"),
        conversation_topic=message.get("Conversation-Topic"),
    )


def extract_msg_threading(message: object, subject: object) -> dict[str, object]:
    header_mapping = getattr(message, "headerDict", None)
    parsed_headers = parse_email_headers_only(
        getattr(message, "headerText", None) or getattr(message, "header", None)
    )

    def _value(header_name: str) -> str | None:
        mapped = email_header_value(header_mapping, header_name)
        if mapped:
            return mapped
        if parsed_headers is not None:
            normalized = normalize_whitespace(str(parsed_headers.get(header_name) or ""))
            if normalized:
                return normalized
        return None

    return build_email_threading_payload(
        subject=subject,
        message_id=_value("Message-ID"),
        in_reply_to=_value("In-Reply-To"),
        references=_value("References"),
        conversation_index=_value("Conversation-Index"),
        conversation_topic=_value("Conversation-Topic"),
    )


def extract_transport_header_threading(
    transport_headers: object,
    *,
    subject: object,
    conversation_topic: object = None,
) -> dict[str, object]:
    parsed_headers = parse_email_headers_only(transport_headers)
    return build_email_threading_payload(
        subject=subject,
        message_id=parsed_headers.get("Message-ID") if parsed_headers is not None else None,
        in_reply_to=parsed_headers.get("In-Reply-To") if parsed_headers is not None else None,
        references=parsed_headers.get("References") if parsed_headers is not None else None,
        conversation_index=parsed_headers.get("Conversation-Index") if parsed_headers is not None else None,
        conversation_topic=conversation_topic or (parsed_headers.get("Conversation-Topic") if parsed_headers is not None else None),
    )


def build_chat_preview_artifacts(
    *,
    title: str | None,
    author: str | None,
    participants: str | None,
    date_created: str | None,
    date_modified: str | None,
    text_body: str | None,
    preview_file_name: str,
    chat_metadata: dict[str, object] | None = None,
    chat_entries: list[dict[str, object]] | None = None,
    label: str = "conversation",
) -> list[dict[str, object]]:
    normalized_text = normalize_whitespace(str(text_body or ""))
    metadata = chat_metadata or extract_chat_transcript_metadata(normalized_text) or {}
    headers = {
        "Author": author or "",
        "Participants": participants or "",
        "Started": format_chat_preview_timestamp(date_created) or date_created or "",
        "Updated": format_chat_preview_timestamp(date_modified) or date_modified or "",
        "Title": title or "",
        "Messages": str(metadata["message_count"]) if metadata.get("message_count") is not None else "",
    }
    preview = build_chat_preview_html(
        headers,
        normalized_text,
        document_title=title or "Retriever Chat Preview",
        entries=chat_entries,
    )
    return [
        {
            "file_name": preview_file_name,
            "preview_type": "html",
            "label": label,
            "ordinal": 0,
            "content": preview,
        }
    ]


def build_chat_extracted_payload(
    *,
    title: str | None,
    author: str | None,
    date_created: str | None,
    text_body: str | None,
    html_body: str | None,
    attachments: list[dict[str, object]] | None,
    preview_file_name: str,
    chat_metadata: dict[str, object] | None = None,
    chat_entries: list[dict[str, object]] | None = None,
    chat_threading: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized_html = None if html_body is None else str(html_body)
    normalized_text = normalize_whitespace(str(text_body or ""))
    if not normalized_text and normalized_html:
        normalized_text = strip_html_tags(normalized_html)
    normalized_text = normalize_whitespace(normalized_text)
    metadata = chat_metadata or extract_chat_transcript_metadata(normalized_text) or {}
    participants = (
        str(metadata["participants"])
        if metadata.get("participants")
        else extract_chat_participants(normalized_text)
    )
    resolved_date_created = str(metadata["date_created"]) if metadata.get("date_created") else date_created
    resolved_date_modified = str(metadata["date_modified"]) if metadata.get("date_modified") else None
    metadata_title = normalize_generated_document_title(str(metadata["title"])) if metadata.get("title") else None
    resolved_title = normalize_generated_document_title(title) or metadata_title
    return {
        "page_count": 1,
        "author": None,
        "content_type": "Chat",
        "date_created": resolved_date_created,
        "date_modified": resolved_date_modified,
        "participants": participants,
        "title": resolved_title,
        "subject": None,
        "recipients": None,
        "text_content": normalized_text,
        "text_status": "empty" if not normalized_text else "ok",
        "chat_threading": dict(chat_threading or {}),
        "attachments": list(attachments or []),
        "preview_artifacts": build_chat_preview_artifacts(
            title=resolved_title,
            author=None,
            participants=participants,
            date_created=resolved_date_created,
            date_modified=resolved_date_modified,
            text_body=normalized_text,
            preview_file_name=preview_file_name,
            chat_metadata=metadata,
            chat_entries=chat_entries,
        ),
    }


def build_calendar_extracted_payload(
    *,
    subject: str | None,
    author: str | None,
    recipients: str | None,
    date_created: str | None,
    text_body: str | None,
    html_body: str | None,
    attachments: list[dict[str, object]] | None,
    preview_file_name: str,
) -> dict[str, object]:
    normalized_html = None if html_body is None else str(html_body)
    normalized_text = normalize_whitespace(str(text_body or ""))
    if not normalized_text and normalized_html:
        normalized_text = strip_html_tags(normalized_html)
    normalized_text = normalize_whitespace(normalized_text)
    resolved_subject = normalize_generated_document_title(subject)
    normalized_attachments = list(attachments or [])
    preview_html_body = inline_cid_references_in_html(normalized_html, normalized_attachments)
    preview_title = resolved_subject or "Retriever Calendar Preview"
    preview = build_html_preview(
        {
            "Organizer": author or "",
            "Date": format_chat_preview_timestamp(date_created) or date_created or "",
            "Title": resolved_subject or "",
            "Attendees": recipients or "",
        },
        body_html=preview_html_body,
        body_text=normalized_text,
        document_title=preview_title,
    )
    return {
        "page_count": 1,
        "author": author,
        "content_type": "Calendar",
        "date_created": date_created,
        "date_modified": None,
        "participants": author or None,
        "title": resolved_subject,
        "subject": resolved_subject,
        "recipients": recipients or None,
        "text_content": normalized_text,
        "text_status": "empty" if not normalized_text else "ok",
        "attachments": filter_html_preview_embedded_image_attachments(
            normalized_html,
            normalized_attachments,
        ),
        "preview_artifacts": [
            {
                "file_name": preview_file_name,
                "preview_type": "html",
                "label": "calendar",
                "ordinal": 0,
                "content": preview,
            }
        ],
    }


def normalize_pst_folder_marker(folder_path: object) -> str:
    normalized = normalize_whitespace(str(folder_path or "")).strip().strip("/")
    if not normalized:
        return "/"
    return f"/{normalized.lower()}/"


def pst_folder_path_contains(folder_path: object, marker: str) -> bool:
    return marker in normalize_pst_folder_marker(folder_path)


def infer_pst_chat_title(subject: str | None, text_body: str | None) -> str | None:
    normalized_subject = normalize_generated_document_title(subject)
    if normalized_subject:
        return normalized_subject
    normalized_text = normalize_whitespace(str(text_body or ""))
    if not normalized_text:
        return None
    if len(normalized_text) <= 120:
        return normalized_text
    return normalized_text[:117].rstrip() + "..."


def synthesize_pst_chat_entries(
    *,
    author: str | None,
    date_created: str | None,
    text_body: str | None,
    chat_metadata: dict[str, object] | None,
) -> list[dict[str, object]] | None:
    if chat_metadata:
        try:
            if int(chat_metadata.get("message_count") or 0) > 1:
                return None
        except Exception:
            pass
    speaker = normalize_whitespace(str(author or ""))
    body = normalize_whitespace(str(text_body or ""))
    if not speaker or not body:
        return None
    return [
        {
            "speaker": speaker,
            "body": body,
            "timestamp": date_created,
            "timestamp_label": format_chat_preview_timestamp(date_created),
            "avatar_label": chat_avatar_initials(speaker),
        }
    ]


def synthesize_pst_chat_metadata(
    *,
    subject: str | None,
    author: str | None,
    date_created: str | None,
    text_body: str | None,
    chat_metadata: dict[str, object] | None,
    chat_entries: list[dict[str, object]] | None,
    preferred_title: str | None = None,
    preferred_participants: str | None = None,
) -> dict[str, object] | None:
    metadata = dict(chat_metadata or {})
    resolved_title = infer_pst_chat_title(subject, text_body)
    if preferred_title:
        metadata["title"] = preferred_title
    elif resolved_title and not metadata.get("title"):
        metadata["title"] = resolved_title
    normalized_author = normalize_whitespace(str(author or ""))
    if preferred_participants:
        metadata["participants"] = preferred_participants
    elif normalized_author and not metadata.get("participants"):
        metadata["participants"] = normalized_author
    if date_created and not metadata.get("date_created"):
        metadata["date_created"] = date_created
    if date_created and not metadata.get("date_modified") and chat_entries is not None:
        metadata["date_modified"] = date_created
    if chat_entries is not None and not metadata.get("message_count"):
        metadata["message_count"] = len(chat_entries)
    return metadata or None


def classify_pst_message_kind(message_dict: dict[str, object], chat_metadata: dict[str, object] | None) -> str:
    folder_path = message_dict.get("folder_path")
    if pst_folder_path_contains(folder_path, "/substratefiles/spools/"):
        return "skip"
    if pst_folder_path_contains(folder_path, "/skypespacesdata/teamsmeetings/"):
        return "skip"
    if pst_folder_path_contains(folder_path, "/teamsmessagesdata/"):
        return "chat"
    if pst_folder_path_contains(folder_path, "/conversation history/"):
        return "chat"
    message_class = normalize_whitespace(
        str(message_dict.get("message_class") or message_dict.get("item_class") or "")
    )
    lowered_class = message_class.lower() if message_class else ""
    if lowered_class.startswith("ipm.appointment") or "appointment" in lowered_class:
        return "calendar"
    if any(token in lowered_class for token in ("conversation", "chat", "im")):
        return "chat"
    if pst_folder_path_contains(folder_path, "/calendar/"):
        return "calendar"
    if not chat_metadata:
        return "email"
    recipients = normalize_whitespace(str(message_dict.get("recipients") or ""))
    return "chat" if not recipients else "email"


def extract_parsed_email_message(
    message: object,
    *,
    include_attachments: bool = True,
    preview_file_name: str = "message.html",
) -> dict[str, object]:
    subject = message.get("Subject")
    author = message.get("From")
    recipients = ", ".join(part for part in [message.get("To"), message.get("Cc"), message.get("Bcc")] if part)
    date_created = normalize_datetime(message.get("Date"))

    plain_body = message.get_body(preferencelist=("plain",))
    html_body = message.get_body(preferencelist=("html",))
    text_body = None
    html_content = None
    if plain_body is not None:
        content = plain_body.get_content()
        text_body = normalize_whitespace(str(content))
    if html_body is not None:
        content = html_body.get_content()
        html_content = str(content)
        if text_body is None:
            text_body = strip_html_tags(html_content)
    if text_body is None:
        content = message.get_content()
        if isinstance(content, bytes):
            text_body, _, _ = decode_bytes(content)
        else:
            text_body = normalize_whitespace(str(content))
    email_threading = extract_parsed_email_threading(message)
    return build_email_extracted_payload(
        subject=subject or None,
        author=author or None,
        recipients=recipients or None,
        date_created=date_created,
        text_body=text_body,
        html_body=html_content,
        attachments=extract_eml_attachments(message) if include_attachments else [],
        preview_file_name=preview_file_name,
        email_threading=email_threading,
    )


def parse_email_message(
    data: bytes,
    *,
    include_attachments: bool = True,
    preview_file_name: str = "message.html",
) -> dict[str, object]:
    message = BytesParser(policy=policy.default).parsebytes(data)
    return extract_parsed_email_message(
        message,
        include_attachments=include_attachments,
        preview_file_name=preview_file_name,
    )


def extract_eml_file(path: Path, include_attachments: bool = True) -> dict[str, object]:
    return parse_email_message(
        path.read_bytes(),
        include_attachments=include_attachments,
        preview_file_name=f"{path.name}.html",
    )


def extract_msg_file(path: Path, include_attachments: bool = True) -> dict[str, object]:
    extract_msg_module = dependency_guard("extract_msg", "extract-msg", "msg")
    message = extract_msg_module.Message(str(path))  # type: ignore[union-attr]
    try:
        subject = message.subject or None
        author = message.sender or None
        recipients = ", ".join(part for part in [message.to, message.cc, message.bcc] if part)
        html_body = None
        if getattr(message, "htmlBody", None):
            body_value = message.htmlBody
            html_body = body_value.decode("utf-8", errors="replace") if isinstance(body_value, bytes) else str(body_value)
        text_body = normalize_whitespace(str(message.body or "")) or (strip_html_tags(html_body) if html_body else "")
        email_threading = extract_msg_threading(message, subject)
        return build_email_extracted_payload(
            subject=subject,
            author=author,
            recipients=recipients or None,
            date_created=normalize_datetime(message.date),
            text_body=text_body,
            html_body=html_body,
            attachments=extract_msg_attachments(message) if include_attachments else [],
            preview_file_name=f"{path.name}.html",
            email_threading=email_threading,
        )
    finally:
        try:
            message.close()
        except Exception:
            pass


def iter_mbox_messages(path: Path):
    archive = mailbox.mbox(str(path), factory=mailbox.mboxMessage, create=False)
    duplicate_counts: dict[str, int] = defaultdict(int)
    try:
        for _, raw_message in archive.iteritems():
            payload_bytes = raw_message.as_bytes(policy=policy.default, unixfrom=False)
            payload_hash = sha256_bytes(payload_bytes)
            parsed_message = BytesParser(policy=policy.default).parsebytes(payload_bytes)
            explicit_source_item_id = normalize_whitespace(
                str(parsed_message.get("Message-ID") or parsed_message.get("Message-Id") or "")
            ) or None
            base_source_item_id = explicit_source_item_id or f"mbox-hash:{payload_hash}"
            duplicate_counts[base_source_item_id] += 1
            occurrence = duplicate_counts[base_source_item_id]
            stable_source_item_id = base_source_item_id if occurrence == 1 else f"{base_source_item_id}#{occurrence}"
            yield {
                "source_item_id": stable_source_item_id,
                "payload_hash": payload_hash,
                "parsed_message": parsed_message,
            }
    finally:
        try:
            archive.close()
        except Exception:
            pass


def container_message_payload_hash(message_payload: dict[str, object]) -> str:
    attachment_manifest: list[dict[str, object]] = []
    for ordinal, attachment in enumerate(list(message_payload.get("attachments") or []), start=1):
        raw_name = attachment.get("file_name") if isinstance(attachment, dict) else None
        payload = attachment.get("payload") if isinstance(attachment, dict) else None
        file_hash = attachment.get("file_hash") if isinstance(attachment, dict) else None
        if file_hash is None and isinstance(payload, bytes):
            file_hash = sha256_bytes(payload)
        attachment_manifest.append(
            {
                "file_name": normalize_attachment_filename(raw_name if isinstance(raw_name, str) else None, ordinal),
                "ordinal": int(attachment.get("ordinal", ordinal)) if isinstance(attachment, dict) else ordinal,
                "file_hash": file_hash,
                "payload_size": len(payload) if isinstance(payload, bytes) else None,
            }
        )
    return sha256_json_value(
        {
            "subject": message_payload.get("subject"),
            "author": message_payload.get("author"),
            "recipients": message_payload.get("recipients"),
            "date_created": message_payload.get("date_created"),
            "text_body": normalize_whitespace(str(message_payload.get("text_body") or "")),
            "html_body": normalize_whitespace(strip_html_tags(str(message_payload.get("html_body") or ""))),
            "attachments": attachment_manifest,
        }
    )


def normalize_mbox_message(source_rel_path: str, message_dict: dict[str, object]) -> dict[str, object]:
    parsed_message = message_dict.get("parsed_message")
    if parsed_message is None:
        raise RetrieverError(f"MBOX message is missing a parsed message payload in {source_rel_path}")
    source_item_id = normalize_source_item_id(message_dict.get("source_item_id") or parsed_message.get("Message-ID"))
    extracted = extract_parsed_email_message(
        parsed_message,
        include_attachments=True,
        preview_file_name=mbox_preview_file_name(source_item_id),
    )
    payload_hash = normalize_whitespace(str(message_dict.get("payload_hash") or "")) or None
    return {
        "rel_path": mbox_message_rel_path(source_rel_path, source_item_id),
        "file_name": mbox_message_file_name(source_item_id),
        "file_hash": payload_hash
        or container_message_payload_hash(
            {
                "subject": extracted.get("subject"),
                "author": extracted.get("author"),
                "recipients": extracted.get("recipients"),
                "date_created": extracted.get("date_created"),
                "text_body": extracted.get("text_content"),
                "attachments": extracted.get("attachments"),
            }
        ),
        "source_rel_path": source_rel_path,
        "source_item_id": source_item_id,
        "source_folder_path": None,
        "extracted": extracted,
    }


def coerce_pst_attachment_payload(attachment: object) -> bytes | None:
    if attachment is None:
        return None
    if isinstance(attachment, bytes):
        return attachment
    if isinstance(attachment, str):
        return attachment.encode("utf-8", errors="replace")
    if isinstance(attachment, dict):
        for key in ("payload", "data", "content"):
            if key in attachment:
                return coerce_pst_attachment_payload(attachment.get(key))
    for attr_name in ("data", "payload", "content"):
        value = getattr(attachment, attr_name, None)
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8", errors="replace")
    for method_name in ("read_buffer", "read_data", "get_data"):
        method = getattr(attachment, method_name, None)
        if not callable(method):
            continue
        try:
            size = getattr(attachment, "size", None)
            if method_name == "read_buffer" and isinstance(size, int) and size > 0:
                payload = method(size)
            else:
                payload = method()
        except Exception:
            continue
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, str):
            return payload.encode("utf-8", errors="replace")
    return None


PST_PROP_DISPLAY_NAME = 0x3001
PST_PROP_ATTACH_EXTENSION = 0x3703
PST_PROP_ATTACH_FILENAME = 0x3704
PST_PROP_ATTACH_LONG_FILENAME = 0x3707
PST_PROP_ATTACH_MIME_TAG = 0x370E
PST_PROP_ATTACH_CONTENT_ID = 0x3712
PST_DEBUG_SCOPE_NAME_PATTERN = re.compile(
    r"(team|teams|skype|conversation|thread|chat|channel|space|group|participant|member|roster)",
    re.IGNORECASE,
)
PST_DEBUG_GUID_VALUE_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
PST_DEBUG_MAX_TEXT_VALUE_CHARS = 512
PST_DEBUG_MAX_HEX_PREVIEW_BYTES = 32


def normalize_pst_identifier(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = bytes(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (int, float)):
        return str(int(value))
    normalized = normalize_whitespace(str(value))
    return normalized or None


def iter_pst_collection(
    owner: object,
    *,
    list_attrs: tuple[str, ...],
    count_getter_pairs: tuple[tuple[str, str], ...],
):
    for attr_name in list_attrs:
        value = getattr(owner, attr_name, None)
        if value is None:
            continue
        try:
            for item in value:
                yield item
            return
        except TypeError:
            pass
    for count_name, getter_name in count_getter_pairs:
        try:
            count = int(getattr(owner, count_name))
        except Exception:
            continue
        getter = getattr(owner, getter_name, None)
        if not callable(getter):
            continue
        for index in range(count):
            try:
                yield getter(index)
            except Exception:
                continue
        return


def pst_message_folder_path(raw_value: object) -> str | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, (list, tuple)):
        parts = [normalize_whitespace(str(part)) for part in raw_value if normalize_whitespace(str(part))]
        return "/".join(parts) or None
    normalized = normalize_whitespace(str(raw_value).replace("\\", "/"))
    normalized = re.sub(r"/+", "/", normalized).strip("/")
    return normalized or None


def normalize_pst_chat_thread_id(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or ""))
    if not normalized or not normalized.startswith("19:"):
        return None
    suffix_index = normalized.lower().find(";messageid=")
    if suffix_index >= 0:
        normalized = normalized[:suffix_index]
    return normalized or None


def parse_pst_json_object(raw_value: object) -> dict[str, object] | None:
    normalized = normalize_whitespace(str(raw_value or ""))
    if not normalized.startswith("{") or not normalized.endswith("}"):
        return None
    try:
        parsed = json.loads(normalized)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def pst_chat_participant_display_name(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    normalized_name = normalize_participant_token(payload.get("Name"))
    if normalized_name:
        return normalized_name
    normalized_email = normalize_participant_token(payload.get("EmailAddress"))
    if normalized_email:
        return normalized_email.lower()
    return None


def pst_chat_participant_names_from_payload(payload: object) -> list[str]:
    if not isinstance(payload, dict):
        return []
    participant_names: list[object] = []
    sender_display_name = pst_chat_participant_display_name(payload.get("Sender"))
    if sender_display_name:
        participant_names.append(sender_display_name)
    recipients = payload.get("Recipients")
    if isinstance(recipients, list):
        for recipient in recipients:
            recipient_display_name = pst_chat_participant_display_name(recipient)
            if recipient_display_name:
                participant_names.append(recipient_display_name)
    return sorted_unique_display_names(participant_names)


def pst_message_may_have_chat_threading(folder_path: object, message_class: object) -> bool:
    if pst_folder_path_contains(folder_path, "/skypespacesdata/teamsmeetings/"):
        return False
    if pst_folder_path_contains(folder_path, "/teamsmessagesdata/"):
        return True
    if pst_folder_path_contains(folder_path, "/conversation history/"):
        return True
    normalized_message_class = normalize_whitespace(str(message_class or "")).lower()
    return "skypeteams" in normalized_message_class or "microsoft.conversation" in normalized_message_class


def extract_pst_chat_threading(message: object) -> dict[str, object] | None:
    thread_id = None
    message_id = None
    parent_message_id = None
    thread_type = None
    participant_names: list[object] = []

    for record_set in pst_record_sets(message):
        for entry in pst_record_entries(record_set):
            decoded_value = decode_pst_record_entry_value(entry)
            normalized_value = normalize_whitespace(str(decoded_value or ""))
            if not normalized_value:
                continue

            candidate_thread_id = normalize_pst_chat_thread_id(normalized_value)
            if candidate_thread_id and thread_id is None:
                thread_id = candidate_thread_id
            if message_id is None and normalized_value.startswith("19:") and ";messageid=" in normalized_value.lower():
                message_id = normalize_pst_identifier(normalized_value.rsplit("=", 1)[-1])

            parsed_payload = parse_pst_json_object(normalized_value)
            if parsed_payload is None:
                continue

            parsed_thread_id = normalize_pst_chat_thread_id(parsed_payload.get("ThreadId"))
            if parsed_thread_id:
                thread_id = parsed_thread_id
            parsed_message_id = normalize_pst_identifier(parsed_payload.get("MessageId"))
            if parsed_message_id:
                message_id = parsed_message_id
            parsed_parent_message_id = normalize_pst_identifier(parsed_payload.get("ParentMessageId"))
            if parsed_parent_message_id:
                parent_message_id = parsed_parent_message_id
            normalized_thread_type = normalize_whitespace(str(parsed_payload.get("ThreadType") or "")).lower()
            if normalized_thread_type:
                thread_type = normalized_thread_type
            participant_names.extend(pst_chat_participant_names_from_payload(parsed_payload))

    normalized_participants = sorted_unique_display_names(participant_names)
    if not any((thread_id, message_id, parent_message_id, thread_type, normalized_participants)):
        return None
    return {
        "thread_id": thread_id,
        "message_id": message_id,
        "parent_message_id": parent_message_id,
        "thread_type": thread_type,
        "participants": normalized_participants,
    }


def pst_message_author(message: object) -> str | None:
    sender = normalize_whitespace(str(getattr(message, "sender_name", "") or getattr(message, "sender", "") or ""))
    sender_email = normalize_whitespace(str(getattr(message, "sender_email_address", "") or ""))
    if sender and sender_email:
        return f"{sender} <{sender_email}>"
    return sender or sender_email or None


def pst_message_recipients(message: object, transport_headers: object = None) -> str | None:
    parts = [
        normalize_whitespace(str(getattr(message, "display_to", "") or "")),
        normalize_whitespace(str(getattr(message, "display_cc", "") or "")),
        normalize_whitespace(str(getattr(message, "display_bcc", "") or "")),
    ]
    recipients = ", ".join(part for part in parts if part)
    if recipients:
        return recipients
    return extract_email_recipients_from_headers(transport_headers)


def pst_attachment_record_entry_text(attachment: object, *entry_types: int) -> str | None:
    ordered_types = [int(entry_type) for entry_type in entry_types]
    if not ordered_types:
        return None
    found_values: dict[int, str] = {}
    for record_set in pst_record_sets(attachment):
        for entry in pst_record_entries(record_set):
            try:
                entry_type = int(getattr(entry, "entry_type", 0) or 0)
            except Exception:
                continue
            if entry_type not in ordered_types or entry_type in found_values:
                continue
            decoded = decode_pst_record_entry_value(entry)
            if decoded:
                found_values[entry_type] = decoded
    for entry_type in ordered_types:
        if entry_type in found_values:
            return found_values[entry_type]
    return None


def pst_attachment_declared_file_name(attachment: object) -> str | None:
    raw_name = None
    if isinstance(attachment, dict):
        raw_name = attachment.get("file_name") or attachment.get("name") or attachment.get("filename")
    else:
        for attr_name in ("long_filename", "filename", "name", "display_name"):
            value = getattr(attachment, attr_name, None)
            if value:
                raw_name = value
                break
        if raw_name is None:
            raw_name = pst_attachment_record_entry_text(
                attachment,
                PST_PROP_ATTACH_LONG_FILENAME,
                PST_PROP_ATTACH_FILENAME,
                PST_PROP_DISPLAY_NAME,
            )
    normalized = normalize_whitespace(raw_name if isinstance(raw_name, str) else "")
    if normalized:
        return normalized
    return None


def pst_attachment_extension(attachment: object) -> str | None:
    if isinstance(attachment, dict):
        for key in ("preferred_extension", "extension", "file_type"):
            normalized = normalize_file_type_name(attachment.get(key))
            if normalized:
                return normalized
        return None
    for attr_name in ("extension", "attach_extension"):
        normalized = normalize_file_type_name(getattr(attachment, attr_name, None))
        if normalized:
            return normalized
    return normalize_file_type_name(pst_attachment_record_entry_text(attachment, PST_PROP_ATTACH_EXTENSION))


def pst_attachment_content_type(attachment: object) -> str | None:
    if isinstance(attachment, dict):
        return normalize_mime_type(attachment.get("content_type") or attachment.get("mime_type"))
    for attr_name in ("mime_tag", "attach_mime_tag", "mime_type", "content_type"):
        normalized = normalize_mime_type(getattr(attachment, attr_name, None))
        if normalized:
            return normalized
    return normalize_mime_type(pst_attachment_record_entry_text(attachment, PST_PROP_ATTACH_MIME_TAG))


def pst_attachment_file_name(
    attachment: object,
    ordinal: int,
    *,
    payload: bytes | None = None,
) -> str:
    raw_name = pst_attachment_declared_file_name(attachment)
    return normalize_attachment_filename(
        raw_name,
        ordinal,
        payload=payload,
        content_type=pst_attachment_content_type(attachment),
        preferred_extension=pst_attachment_extension(attachment),
    )


def pst_message_html_body(message: object) -> str | None:
    for attr_name in ("html_body", "htmlBody"):
        value = getattr(message, attr_name, None)
        if value is None:
            continue
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)
    return None


def pst_message_text_body(message: object, html_body: str | None) -> str | None:
    for attr_name in ("plain_text_body", "body", "text_body"):
        value = getattr(message, attr_name, None)
        if value is None:
            continue
        if isinstance(value, bytes):
            decoded, _, _ = decode_bytes(value)
            text = normalize_whitespace(decoded)
        else:
            text = normalize_whitespace(str(value))
        if text:
            return text
    if html_body:
        return strip_html_tags(html_body)
    return None


def pst_message_transport_headers(message: object) -> str | None:
    for attr_name in ("transport_headers",):
        value = getattr(message, attr_name, None)
        if value is None:
            continue
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        normalized = str(value)
        if normalize_whitespace(normalized):
            return normalized
    for method_name in ("get_transport_headers",):
        method = getattr(message, method_name, None)
        if not callable(method):
            continue
        try:
            value = method()
        except Exception:
            continue
        if value is None:
            continue
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        normalized = str(value)
        if normalize_whitespace(normalized):
            return normalized
    return None


def pst_message_conversation_topic(message: object) -> str | None:
    for attr_name in ("conversation_topic",):
        value = getattr(message, attr_name, None)
        normalized = normalize_whitespace(str(value or ""))
        if normalized:
            return normalized
    for method_name in ("get_conversation_topic",):
        method = getattr(message, method_name, None)
        if not callable(method):
            continue
        try:
            value = method()
        except Exception:
            continue
        normalized = normalize_whitespace(str(value or ""))
        if normalized:
            return normalized
    return None


def pst_record_sets(owner: object):
    yield from iter_pst_collection(
        owner,
        list_attrs=("record_sets",),
        count_getter_pairs=(("number_of_record_sets", "get_record_set"),),
    )


def pst_record_entries(record_set: object):
    yield from iter_pst_collection(
        record_set,
        list_attrs=("entries", "record_entries"),
        count_getter_pairs=(("number_of_entries", "get_entry"), ("number_of_record_entries", "get_record_entry")),
    )


def pst_debug_value_preview(raw_value: object, *, max_chars: int = PST_DEBUG_MAX_TEXT_VALUE_CHARS) -> str | None:
    normalized = normalize_whitespace(str(raw_value or ""))
    if not normalized:
        return None
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3].rstrip() + "..."


def pst_debug_record_entry_payload(entry: object, *, entry_index: int) -> dict[str, object]:
    payload: dict[str, object] = {"entry_index": int(entry_index)}
    for field_name in (
        "identifier",
        "name",
        "entry_name",
        "property_name",
        "named_property_name",
        "guid",
        "property_guid",
        "property_set_guid",
        "named_property_guid",
    ):
        normalized = pst_debug_value_preview(getattr(entry, field_name, None))
        if normalized:
            payload[field_name] = normalized
    try:
        entry_type = int(getattr(entry, "entry_type", 0) or 0)
    except Exception:
        entry_type = None
    if entry_type is not None:
        payload["entry_type"] = entry_type
        payload["entry_type_hex"] = f"0x{entry_type:04X}"
    try:
        value_type = int(getattr(entry, "value_type", 0) or 0)
    except Exception:
        value_type = None
    if value_type is not None:
        payload["value_type"] = value_type
        payload["value_type_hex"] = f"0x{value_type:04X}"
    data = getattr(entry, "data", None)
    if isinstance(data, (bytes, bytearray)):
        payload["byte_length"] = len(data)
        payload["hex_preview"] = bytes(data[:PST_DEBUG_MAX_HEX_PREVIEW_BYTES]).hex()
    decoded_value = decode_pst_record_entry_value(entry)
    if decoded_value:
        payload["decoded_value"] = pst_debug_value_preview(decoded_value)
    return payload


def pst_debug_entry_scope_candidate(payload: dict[str, object]) -> str | None:
    decoded_value = normalize_whitespace(str(payload.get("decoded_value") or ""))
    if not decoded_value or len(decoded_value) > PST_DEBUG_MAX_TEXT_VALUE_CHARS:
        return None
    if decoded_value.lower().startswith("19:"):
        return decoded_value
    if PST_DEBUG_GUID_VALUE_PATTERN.search(decoded_value):
        return decoded_value
    name_blob = " ".join(
        normalize_whitespace(str(payload.get(field_name) or ""))
        for field_name in (
            "name",
            "entry_name",
            "property_name",
            "named_property_name",
        )
    )
    if PST_DEBUG_SCOPE_NAME_PATTERN.search(name_blob):
        return decoded_value
    return None


def pst_debug_interesting_entry(payload: dict[str, object]) -> bool:
    candidate = pst_debug_entry_scope_candidate(payload)
    if candidate:
        return True
    name_blob = " ".join(
        normalize_whitespace(str(payload.get(field_name) or ""))
        for field_name in (
            "name",
            "entry_name",
            "property_name",
            "named_property_name",
        )
    )
    if PST_DEBUG_SCOPE_NAME_PATTERN.search(name_blob):
        return True
    decoded_value = normalize_whitespace(str(payload.get("decoded_value") or ""))
    return bool(decoded_value and decoded_value.lower().startswith("https://teams.microsoft.com/"))


def pst_debug_record_sets_payloads(
    owner: object,
    *,
    max_record_entries: int = 128,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[str]]:
    record_set_payloads: list[dict[str, object]] = []
    interesting_entries: list[dict[str, object]] = []
    candidate_scope_values: list[str] = []
    seen_candidates: set[str] = set()
    for record_set_index, record_set in enumerate(pst_record_sets(owner), start=1):
        entry_payloads: list[dict[str, object]] = []
        entry_total = 0
        truncated = False
        for entry_index, entry in enumerate(pst_record_entries(record_set), start=1):
            entry_total += 1
            if entry_index > max_record_entries:
                truncated = True
                continue
            entry_payload = pst_debug_record_entry_payload(entry, entry_index=entry_index)
            entry_payloads.append(entry_payload)
            if pst_debug_interesting_entry(entry_payload):
                interesting_entry = {
                    "record_set_index": int(record_set_index),
                    **entry_payload,
                }
                interesting_entries.append(interesting_entry)
            candidate = pst_debug_entry_scope_candidate(entry_payload)
            if candidate and candidate not in seen_candidates:
                seen_candidates.add(candidate)
                candidate_scope_values.append(candidate)
        record_set_payloads.append(
            {
                "record_set_index": int(record_set_index),
                "entry_count": int(entry_total),
                "entries_truncated": truncated,
                "entries": entry_payloads,
            }
        )
    return record_set_payloads, interesting_entries, candidate_scope_values


def iter_pst_raw_messages(
    path: Path,
    *,
    include_debug_record_sets: bool = False,
    max_record_entries: int = 128,
):
    pypff_module = dependency_guard("pypff", "libpff-python", "pst")

    def _iter_folder(folder: object, ancestors: list[str]):
        folder_name = normalize_whitespace(str(getattr(folder, "name", "") or ""))
        current_ancestors = [*ancestors]
        if folder_name:
            current_ancestors.append(folder_name)
        folder_path = pst_message_folder_path(current_ancestors)

        for message in iter_pst_collection(
            folder,
            list_attrs=("sub_messages", "messages"),
            count_getter_pairs=(("number_of_sub_messages", "get_sub_message"), ("number_of_messages", "get_message")),
        ):
            source_item_id = None
            for attr_name in ("entry_identifier", "entry_identifier_string", "record_key", "search_key", "identifier"):
                source_item_id = normalize_pst_identifier(getattr(message, attr_name, None))
                if source_item_id:
                    break
            if not source_item_id:
                raise RetrieverError(f"PST message is missing a stable item identifier in {path}")

            html_body = pst_message_html_body(message)
            attachments: list[dict[str, object]] = []
            for ordinal, attachment in enumerate(
                iter_pst_collection(
                    message,
                    list_attrs=("attachments",),
                    count_getter_pairs=(("number_of_attachments", "get_attachment"),),
                ),
                start=1,
            ):
                payload = coerce_pst_attachment_payload(attachment)
                if payload is None:
                    continue
                content_type = pst_attachment_content_type(attachment)
                attachments.append(
                    {
                        "file_name": pst_attachment_file_name(attachment, ordinal, payload=payload),
                        "ordinal": ordinal,
                        "payload": payload,
                        "file_hash": sha256_bytes(payload),
                        "content_type": content_type,
                        "content_id": pst_attachment_content_id(attachment),
                    }
                )

            message_class = normalize_whitespace(str(getattr(message, "message_class", "") or "")) or None
            chat_threading = (
                extract_pst_chat_threading(message)
                if pst_message_may_have_chat_threading(folder_path, message_class)
                else None
            )

            debug_record_sets: list[dict[str, object]] = []
            debug_interesting_entries: list[dict[str, object]] = []
            debug_candidate_scope_values: list[str] = []
            if include_debug_record_sets:
                (
                    debug_record_sets,
                    debug_interesting_entries,
                    debug_candidate_scope_values,
                ) = pst_debug_record_sets_payloads(
                    message,
                    max_record_entries=max_record_entries,
                )

            transport_headers = pst_message_transport_headers(message)

            yield {
                "source_item_id": source_item_id,
                "folder_path": folder_path,
                "message_class": message_class,
                "subject": normalize_whitespace(str(getattr(message, "subject", "") or "")) or None,
                "conversation_topic": pst_message_conversation_topic(message),
                "transport_headers": transport_headers,
                "author": pst_message_author(message),
                "recipients": pst_message_recipients(message, transport_headers),
                "date_created": normalize_datetime(
                    getattr(message, "delivery_time", None)
                    or getattr(message, "client_submit_time", None)
                    or getattr(message, "creation_time", None)
                ),
                "text_body": pst_message_text_body(message, html_body),
                "html_body": html_body,
                "attachments": attachments,
                "chat_threading": chat_threading,
                "high_level_identifiers": {
                    attr_name: normalize_pst_identifier(getattr(message, attr_name, None))
                    for attr_name in ("entry_identifier", "entry_identifier_string", "record_key", "search_key", "identifier")
                    if normalize_pst_identifier(getattr(message, attr_name, None))
                },
                "debug_record_sets": debug_record_sets,
                "debug_interesting_entries": debug_interesting_entries,
                "debug_candidate_scope_values": debug_candidate_scope_values,
            }

        for child_folder in iter_pst_collection(
            folder,
            list_attrs=("sub_folders", "folders"),
            count_getter_pairs=(("number_of_sub_folders", "get_sub_folder"), ("number_of_folders", "get_folder")),
        ):
            yield from _iter_folder(child_folder, current_ancestors)

    pst_file = pypff_module.file()  # type: ignore[union-attr]
    pst_file.open(str(path))
    try:
        root_folder = pst_file.get_root_folder()
        yield from _iter_folder(root_folder, [])
    finally:
        try:
            pst_file.close()
        except Exception:
            pass


def iter_pst_debug_messages(
    path: Path,
    *,
    max_record_entries: int = 128,
):
    for message_dict in iter_pst_raw_messages(
        path,
        include_debug_record_sets=True,
        max_record_entries=max_record_entries,
    ):
        chat_text = str(message_dict.get("text_body") or "") or (
            strip_html_tags(str(message_dict.get("html_body") or ""))
            if message_dict.get("html_body")
            else ""
        )
        chat_metadata = extract_chat_transcript_metadata(chat_text)
        message_kind = classify_pst_message_kind(message_dict, chat_metadata)
        payload = {
            "source_item_id": message_dict["source_item_id"],
            "folder_path": message_dict.get("folder_path"),
            "message_kind": message_kind,
            "message_class": message_dict.get("message_class"),
            "subject": message_dict.get("subject"),
            "conversation_topic": message_dict.get("conversation_topic"),
            "author": message_dict.get("author"),
            "recipients": message_dict.get("recipients"),
            "date_created": message_dict.get("date_created"),
            "high_level_identifiers": dict(message_dict.get("high_level_identifiers") or {}),
            "candidate_scope_values": list(message_dict.get("debug_candidate_scope_values") or []),
            "interesting_properties": list(message_dict.get("debug_interesting_entries") or []),
            "record_sets": list(message_dict.get("debug_record_sets") or []),
        }
        if chat_metadata:
            payload["chat_metadata"] = {
                key: chat_metadata[key]
                for key in ("title", "participants", "date_created", "date_modified", "message_count")
                if key in chat_metadata and chat_metadata.get(key) not in (None, "")
            }
        yield payload


def decode_pst_record_entry_value(entry: object) -> str | None:
    data = getattr(entry, "data", None)
    if isinstance(data, (bytes, bytearray)):
        value_type = None
        try:
            value_type = int(getattr(entry, "value_type", 0) or 0)
        except Exception:
            value_type = None
        raw = bytes(data)
        try:
            if value_type == 0x001F:
                decoded = raw.decode("utf-16-le", errors="replace")
            elif value_type == 0x001E:
                decoded = raw.decode("utf-8", errors="replace")
            else:
                try:
                    decoded = raw.decode("utf-16-le")
                except UnicodeDecodeError:
                    decoded = raw.decode("utf-8", errors="replace")
        except Exception:
            decoded = raw.decode("utf-8", errors="replace")
        return decoded.rstrip("\x00").strip() or None
    for method_name in ("get_data_as_string",):
        method = getattr(entry, method_name, None)
        if callable(method):
            try:
                value = method()
            except Exception:
                continue
            if value:
                text = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
                text = text.rstrip("\x00").strip()
                if text:
                    return text
    if data is not None:
        text = str(data).rstrip("\x00").strip()
        return text or None
    return None


def pst_attachment_content_id(attachment: object) -> str | None:
    for attr_name in ("content_id", "attach_content_id", "long_content_id"):
        direct = getattr(attachment, attr_name, None)
        normalized = normalize_content_id(direct)
        if normalized:
            return normalized
    record_sets = getattr(attachment, "record_sets", None)
    if record_sets is None:
        return None
    try:
        record_sets_iter = list(record_sets)
    except Exception:
        return None
    for record_set in record_sets_iter:
        for entries_attr in ("entries", "record_entries"):
            entries = getattr(record_set, entries_attr, None)
            if entries is None:
                continue
            try:
                entries_iter = list(entries)
            except Exception:
                continue
            for entry in entries_iter:
                try:
                    entry_type = int(getattr(entry, "entry_type", 0) or 0)
                except Exception:
                    continue
                if entry_type != PST_PROP_ATTACH_CONTENT_ID:
                    continue
                decoded = decode_pst_record_entry_value(entry)
                normalized = normalize_content_id(decoded)
                if normalized:
                    return normalized
            break
    return None


def iter_pst_messages(path: Path):
    for payload in iter_pst_raw_messages(path):
        yield {
            "source_item_id": payload["source_item_id"],
            "folder_path": payload.get("folder_path"),
            "message_class": payload.get("message_class"),
            "subject": payload.get("subject"),
            "conversation_topic": payload.get("conversation_topic"),
            "transport_headers": payload.get("transport_headers"),
            "author": payload.get("author"),
            "recipients": payload.get("recipients"),
            "date_created": payload.get("date_created"),
            "text_body": payload.get("text_body"),
            "html_body": payload.get("html_body"),
            "chat_threading": payload.get("chat_threading"),
            "attachments": list(payload.get("attachments") or []),
        }


def normalize_pst_message(source_rel_path: str, message_dict: dict[str, object]) -> dict[str, object] | None:
    source_item_id = normalize_source_item_id(
        message_dict.get("source_item_id")
        or message_dict.get("entry_identifier")
        or message_dict.get("identifier")
    )
    normalized_attachments: list[dict[str, object]] = []
    for ordinal, raw_attachment in enumerate(list(message_dict.get("attachments") or []), start=1):
        raw_name = None
        raw_content_type: object = None
        raw_extension: object = None
        raw_content_id: object = None
        if isinstance(raw_attachment, dict):
            raw_name = raw_attachment.get("file_name") or raw_attachment.get("name") or raw_attachment.get("filename")
            raw_content_type = raw_attachment.get("content_type") or raw_attachment.get("mime_type")
            raw_extension = raw_attachment.get("preferred_extension") or raw_attachment.get("extension") or raw_attachment.get("file_type")
            raw_content_id = raw_attachment.get("content_id")
        else:
            raw_name = pst_attachment_declared_file_name(raw_attachment)
            raw_content_type = pst_attachment_content_type(raw_attachment)
            raw_extension = pst_attachment_extension(raw_attachment)
            raw_content_id = pst_attachment_content_id(raw_attachment)
        payload = coerce_pst_attachment_payload(raw_attachment)
        if payload is None:
            continue
        attachment_ordinal = ordinal
        if isinstance(raw_attachment, dict):
            try:
                attachment_ordinal = int(raw_attachment.get("ordinal", ordinal))
            except Exception:
                attachment_ordinal = ordinal
        normalized_attachments.append(
            {
                "file_name": normalize_attachment_filename(
                    raw_name if isinstance(raw_name, str) else None,
                    attachment_ordinal,
                    payload=payload,
                    content_type=raw_content_type,
                    preferred_extension=raw_extension,
                ),
                "ordinal": attachment_ordinal,
                "payload": payload,
                "file_hash": sha256_bytes(payload),
                "content_type": normalize_mime_type(raw_content_type),
                "content_id": normalize_content_id(raw_content_id),
            }
        )

    html_body = message_dict.get("html_body")
    normalized_subject = normalize_whitespace(str(message_dict.get("subject") or "")) or None
    normalized_author = normalize_whitespace(str(message_dict.get("author") or "")) or None
    normalized_recipients = normalize_whitespace(str(message_dict.get("recipients") or "")) or None
    if normalized_recipients is None:
        normalized_recipients = extract_email_recipients_from_headers(message_dict.get("transport_headers"))
    normalized_date_created = normalize_datetime(message_dict.get("date_created"))
    normalized_text_body = None if message_dict.get("text_body") is None else str(message_dict.get("text_body") or "")
    normalized_html_body = None if html_body is None else str(html_body)
    chat_text = normalized_text_body or (strip_html_tags(normalized_html_body) if normalized_html_body else "")
    chat_metadata = extract_chat_transcript_metadata(chat_text)
    message_kind = classify_pst_message_kind(message_dict, chat_metadata)
    if message_kind == "skip":
        return None
    if message_kind == "chat":
        raw_chat_threading = message_dict.get("chat_threading")
        chat_threading = dict(raw_chat_threading) if isinstance(raw_chat_threading, dict) else {}
        chat_thread_participants = sorted_unique_display_names(normalize_string_list(chat_threading.get("participants")))
        preferred_participants = render_display_name_list(chat_thread_participants)
        preferred_title = None
        if normalize_whitespace(str(chat_threading.get("thread_type") or "")).lower() == "chat":
            preferred_title = render_display_name_title(chat_thread_participants, max_names=4)
        chat_entries = synthesize_pst_chat_entries(
            author=normalized_author,
            date_created=normalized_date_created,
            text_body=chat_text,
            chat_metadata=chat_metadata,
        )
        resolved_chat_metadata = synthesize_pst_chat_metadata(
            subject=normalized_subject,
            author=normalized_author,
            date_created=normalized_date_created,
            text_body=chat_text,
            chat_metadata=chat_metadata,
            chat_entries=chat_entries,
            preferred_title=preferred_title,
            preferred_participants=preferred_participants,
        )
        extracted = build_chat_extracted_payload(
            title=preferred_title or normalized_subject,
            author=normalized_author,
            date_created=normalized_date_created,
            text_body=normalized_text_body,
            html_body=normalized_html_body,
            attachments=normalized_attachments,
            preview_file_name=pst_preview_file_name(source_item_id),
            chat_metadata=resolved_chat_metadata,
            chat_entries=chat_entries,
            chat_threading=chat_threading,
        )
    elif message_kind == "calendar":
        extracted = build_calendar_extracted_payload(
            subject=normalized_subject,
            author=normalized_author,
            recipients=normalized_recipients,
            date_created=normalized_date_created,
            text_body=normalized_text_body,
            html_body=normalized_html_body,
            attachments=normalized_attachments,
            preview_file_name=pst_preview_file_name(source_item_id),
        )
    else:
        email_threading = extract_transport_header_threading(
            message_dict.get("transport_headers"),
            subject=normalized_subject,
            conversation_topic=message_dict.get("conversation_topic"),
        )
        extracted = build_email_extracted_payload(
            subject=normalized_subject,
            author=normalized_author,
            recipients=normalized_recipients,
            date_created=normalized_date_created,
            text_body=normalized_text_body,
            html_body=normalized_html_body,
            attachments=normalized_attachments,
            preview_file_name=pst_preview_file_name(source_item_id),
            email_threading=email_threading,
        )
    return {
        "rel_path": pst_message_rel_path(source_rel_path, source_item_id),
        "file_name": pst_message_file_name(source_item_id),
        "file_hash": container_message_payload_hash(
            {
                "subject": extracted.get("subject"),
                "author": extracted.get("author"),
                "recipients": extracted.get("recipients"),
                "date_created": extracted.get("date_created"),
                "text_body": extracted.get("text_content"),
                "html_body": html_body,
                "attachments": normalized_attachments,
            }
        ),
        "source_rel_path": source_rel_path,
        "source_item_id": source_item_id,
        "source_folder_path": pst_message_folder_path(message_dict.get("folder_path")),
        "extracted": extracted,
    }


def extract_document(path: Path, include_attachments: bool = True) -> dict[str, object]:
    file_type = normalize_extension(path)
    if file_type not in SUPPORTED_FILE_TYPES:
        raise RetrieverError(f"Unsupported file type: .{file_type or '(none)'}")
    if file_type == "csv":
        return extract_csv_file(path)
    if file_type in TEXT_FILE_TYPES:
        return extract_plain_text_file(path)
    if file_type in IMAGE_NATIVE_PREVIEW_FILE_TYPES:
        return extract_native_preview_only_file(path, explicit_content_type="Image")
    if file_type == "rtf":
        return extract_rtf_file(path)
    if file_type == "pdf":
        return extract_pdf_file(path)
    if file_type == "docx":
        return extract_docx_file(path)
    if file_type == "pptx":
        return extract_pptx_file(path)
    if file_type == "xls":
        return extract_xls_file(path)
    if file_type == "xlsx":
        return extract_xlsx_file(path)
    if file_type == "eml":
        return extract_eml_file(path, include_attachments=include_attachments)
    if file_type == "msg":
        return extract_msg_file(path, include_attachments=include_attachments)
    if file_type == "mbox":
        raise RetrieverError("MBOX sources must be ingested through the container ingest pipeline.")
    if file_type == "pst":
        raise RetrieverError("PST sources must be ingested through the container ingest pipeline.")
    raise RetrieverError(f"Unsupported file type: .{file_type}")
