def preview_base_path_for_rel_path(rel_path: str) -> Path:
    source_rel_path = container_source_rel_path_from_message_rel_path(rel_path)
    if source_rel_path is not None:
        return Path("previews") / Path(source_rel_path) / "messages"
    base = Path(rel_path)
    if base.parts and base.parts[0] == ".retriever":
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
    abs_path = paths["root"] / rel_path
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


def default_preview_target(paths: dict[str, Path], row: sqlite3.Row, connection: sqlite3.Connection) -> tuple[str, str]:
    preview_rows = connection.execute(
        """
        SELECT rel_preview_path, preview_type, label
        FROM document_previews
        WHERE document_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (row["id"],),
    ).fetchall()
    native_target = document_native_target(paths, row)
    if preview_rows and document_prefers_native_primary_preview(row) and native_target is not None:
        return str(native_target["rel_path"]), str(native_target["abs_path"])
    if preview_rows:
        rel_preview = preview_rows[0]["rel_preview_path"]
        rel_path = str(Path(".retriever") / rel_preview)
        abs_path = str(paths["state_dir"] / rel_preview)
        return rel_path, abs_path
    source_targets = production_source_part_targets(paths, connection, row)
    if source_targets:
        return str(source_targets[0]["rel_path"]), str(source_targets[0]["abs_path"])
    rel_path = row["rel_path"]
    return rel_path, str(paths["root"] / rel_path)


def collect_preview_targets(paths: dict[str, Path], document_id: int, rel_path: str, connection: sqlite3.Connection) -> list[dict[str, object]]:
    document_row = connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
    preview_rows = connection.execute(
        """
        SELECT rel_preview_path, preview_type, label, ordinal
        FROM document_previews
        WHERE document_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (document_id,),
    ).fetchall()
    if not preview_rows:
        abs_path = paths["root"] / rel_path
        return [
            {
                "rel_path": rel_path,
                "abs_path": str(abs_path),
                "preview_type": "native",
                "label": None,
                "ordinal": 0,
            }
        ]

    targets: list[dict[str, object]] = []
    if document_prefers_native_primary_preview(document_row):
        native_target = document_native_target(paths, document_row)
        if native_target is not None:
            targets.append(native_target)
    for preview_row in preview_rows:
        rel_preview = str(Path(".retriever") / preview_row["rel_preview_path"])
        targets.append(
            {
                "rel_path": rel_preview,
                "abs_path": str(paths["state_dir"] / preview_row["rel_preview_path"]),
                "preview_type": preview_row["preview_type"],
                "label": preview_row["label"],
                "ordinal": preview_row["ordinal"],
            }
        )
    source_targets = production_source_part_targets(paths, connection, document_row)
    for target in source_targets:
        if target["rel_path"] not in {existing["rel_path"] for existing in targets}:
            targets.append(target)
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
    dependency_guard(rtf_to_text, "striprtf", "rtf")
    decoded, text_status, _ = decode_bytes(path.read_bytes())
    text_content = normalize_whitespace(rtf_to_text(decoded))
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
                "content": build_html_preview({}, body_text=text_content),
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
    dependency_guard(pdfplumber, "pdfplumber", "pdf")
    with pdfplumber.open(path) as pdf:  # type: ignore[union-attr]
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
    dependency_guard(DocxDocument, "python-docx", "docx")
    document = DocxDocument(path)  # type: ignore[operator]
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


def sheet_to_csv(sheet: object) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    for row in sheet.iter_rows(values_only=True):
        writer.writerow([stringify_spreadsheet_value(value) for value in row])
    return output.getvalue()


def stringify_spreadsheet_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return value


def extract_xlsx_file(path: Path) -> dict[str, object]:
    dependency_guard(openpyxl, "openpyxl", "xlsx")
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)  # type: ignore[union-attr]
    preview_artifacts: list[dict[str, object]] = []
    sheet_texts: list[str] = []
    try:
        for ordinal, sheet in enumerate(workbook.worksheets):
            csv_text = sheet_to_csv(sheet)
            preview_artifacts.append(
                {
                    "file_name": f"{path.name}.{slugify(sheet.title)}.csv",
                    "preview_type": "csv",
                    "label": sheet.title,
                    "ordinal": ordinal,
                    "content": csv_text,
                }
            )
            text_block = normalize_whitespace(csv_text)
            if text_block:
                sheet_texts.append(f"Sheet: {sheet.title}\n{text_block}")
    finally:
        workbook.close()
    text_content = normalize_whitespace("\n\n".join(sheet_texts))
    return {
        "page_count": len(preview_artifacts),
        "author": None,
        "content_type": "Spreadsheet / Table",
        "date_created": None,
        "date_modified": None,
        "participants": None,
        "title": path.stem,
        "subject": None,
        "recipients": None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else "ok",
        "preview_artifacts": preview_artifacts,
    }


def xls_sheet_to_csv(sheet: object) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    for row_idx in range(sheet.nrows):
        writer.writerow([stringify_spreadsheet_value(sheet.cell_value(row_idx, col_idx)) for col_idx in range(sheet.ncols)])
    return output.getvalue()


def extract_xls_file(path: Path) -> dict[str, object]:
    dependency_guard(xlrd, "xlrd", "xls")
    workbook = xlrd.open_workbook(path)  # type: ignore[union-attr]
    preview_artifacts: list[dict[str, object]] = []
    sheet_texts: list[str] = []
    for ordinal, sheet in enumerate(workbook.sheets()):
        csv_text = xls_sheet_to_csv(sheet)
        preview_artifacts.append(
            {
                "file_name": f"{path.name}.{slugify(sheet.name)}.csv",
                "preview_type": "csv",
                "label": sheet.name,
                "ordinal": ordinal,
                "content": csv_text,
            }
        )
        text_block = normalize_whitespace(csv_text)
        if text_block:
            sheet_texts.append(f"Sheet: {sheet.name}\n{text_block}")
    text_content = normalize_whitespace("\n\n".join(sheet_texts))
    return {
        "page_count": len(preview_artifacts),
        "author": None,
        "content_type": "Spreadsheet / Table",
        "date_created": None,
        "date_modified": None,
        "participants": None,
        "title": path.stem,
        "subject": None,
        "recipients": None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else "ok",
        "preview_artifacts": preview_artifacts,
    }


def normalize_attachment_filename(file_name: str | None, ordinal: int) -> str:
    normalized = normalize_whitespace(file_name or "")
    if normalized:
        return normalized
    return f"attachment-{ordinal:03d}.bin"


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
        content_id = normalize_content_id(part.get("Content-ID"))
        is_inline = disposition == "inline" or (content_id is not None and disposition != "attachment")
        if disposition != "attachment" and not file_name and not content_id:
            continue
        payload = coerce_email_part_payload(part)
        if payload is None:
            continue
        attachments.append(
            {
                "file_name": normalize_attachment_filename(file_name, ordinal),
                "ordinal": ordinal,
                "payload": payload,
                "file_hash": sha256_bytes(payload),
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
        payload = extract_msg_attachment_payload(attachment)
        if payload is None:
            continue
        attachments.append(
            {
                "file_name": normalize_attachment_filename(file_name, ordinal),
                "ordinal": ordinal,
                "payload": payload,
                "file_hash": sha256_bytes(payload),
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
) -> dict[str, object]:
    normalized_html = None if html_body is None else str(html_body)
    normalized_text = normalize_whitespace(str(text_body or ""))
    if not normalized_text and normalized_html:
        normalized_text = strip_html_tags(normalized_html)
    normalized_text = normalize_whitespace(normalized_text)
    participants = extract_email_chain_participants(
        normalized_text,
        [author, recipients or None],
    )
    preview_html_body = inline_cid_references_in_html(normalized_html, attachments)
    preview = build_html_preview(
        {
            "From": author or "",
            "To": recipients or "",
            "Date": format_chat_preview_timestamp(date_created) or date_created or "",
            "Subject": subject or "",
        },
        body_html=preview_html_body,
        body_text=normalized_text,
    )
    return {
        "page_count": 1,
        "author": author,
        "content_type": "Email",
        "date_created": date_created,
        "date_modified": None,
        "participants": participants,
        "title": subject or None,
        "subject": subject or None,
        "recipients": recipients or None,
        "text_content": normalized_text,
        "text_status": "empty" if not normalized_text else "ok",
        "attachments": list(attachments or []),
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
    resolved_title = title or (str(metadata["title"]) if metadata.get("title") else None)
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
    preview_html_body = inline_cid_references_in_html(normalized_html, attachments)
    preview = build_html_preview(
        {
            "Organizer": author or "",
            "Date": format_chat_preview_timestamp(date_created) or date_created or "",
            "Title": subject or "",
            "Attendees": recipients or "",
        },
        body_html=preview_html_body,
        body_text=normalized_text,
        document_title=subject or "Retriever Calendar Preview",
        heading="Retriever Calendar Preview",
    )
    return {
        "page_count": 1,
        "author": author,
        "content_type": "Calendar",
        "date_created": date_created,
        "date_modified": None,
        "participants": author or None,
        "title": subject or None,
        "subject": subject or None,
        "recipients": recipients or None,
        "text_content": normalized_text,
        "text_status": "empty" if not normalized_text else "ok",
        "attachments": list(attachments or []),
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
    normalized_subject = normalize_whitespace(str(subject or ""))
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
) -> dict[str, object] | None:
    metadata = dict(chat_metadata or {})
    resolved_title = infer_pst_chat_title(subject, text_body)
    if resolved_title and not metadata.get("title"):
        metadata["title"] = resolved_title
    normalized_author = normalize_whitespace(str(author or ""))
    if normalized_author and not metadata.get("participants"):
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


def parse_email_message(
    data: bytes,
    *,
    include_attachments: bool = True,
    preview_file_name: str = "message.html",
) -> dict[str, object]:
    message = BytesParser(policy=policy.default).parsebytes(data)
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
    return build_email_extracted_payload(
        subject=subject or None,
        author=author or None,
        recipients=recipients or None,
        date_created=date_created,
        text_body=text_body,
        html_body=html_content,
        attachments=extract_eml_attachments(message) if include_attachments else [],
        preview_file_name=preview_file_name,
    )


def extract_eml_file(path: Path, include_attachments: bool = True) -> dict[str, object]:
    return parse_email_message(
        path.read_bytes(),
        include_attachments=include_attachments,
        preview_file_name=f"{path.name}.html",
    )


def extract_msg_file(path: Path, include_attachments: bool = True) -> dict[str, object]:
    dependency_guard(extract_msg, "extract-msg", "msg")
    message = extract_msg.Message(str(path))  # type: ignore[union-attr]
    try:
        subject = message.subject or None
        author = message.sender or None
        recipients = ", ".join(part for part in [message.to, message.cc, message.bcc] if part)
        html_body = None
        if getattr(message, "htmlBody", None):
            body_value = message.htmlBody
            html_body = body_value.decode("utf-8", errors="replace") if isinstance(body_value, bytes) else str(body_value)
        text_body = normalize_whitespace(str(message.body or "")) or (strip_html_tags(html_body) if html_body else "")
        return build_email_extracted_payload(
            subject=subject,
            author=author,
            recipients=recipients or None,
            date_created=normalize_datetime(message.date),
            text_body=text_body,
            html_body=html_body,
            attachments=extract_msg_attachments(message) if include_attachments else [],
            preview_file_name=f"{path.name}.html",
        )
    finally:
        try:
            message.close()
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


PST_PROP_ATTACH_CONTENT_ID = 0x3712


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


def pst_message_folder_path(raw_value: object) -> str | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, (list, tuple)):
        parts = [normalize_whitespace(str(part)) for part in raw_value if normalize_whitespace(str(part))]
        return "/".join(parts) or None
    normalized = normalize_whitespace(str(raw_value).replace("\\", "/"))
    normalized = re.sub(r"/+", "/", normalized).strip("/")
    return normalized or None


def iter_pst_messages(path: Path):
    dependency_guard(pypff, "libpff-python", "pst")

    def _normalize_pst_identifier(value: object) -> str | None:
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

    def _iter_collection(
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

    def _message_author(message: object) -> str | None:
        sender = normalize_whitespace(str(getattr(message, "sender_name", "") or getattr(message, "sender", "") or ""))
        sender_email = normalize_whitespace(str(getattr(message, "sender_email_address", "") or ""))
        if sender and sender_email:
            return f"{sender} <{sender_email}>"
        return sender or sender_email or None

    def _message_recipients(message: object) -> str | None:
        parts = [
            normalize_whitespace(str(getattr(message, "display_to", "") or "")),
            normalize_whitespace(str(getattr(message, "display_cc", "") or "")),
            normalize_whitespace(str(getattr(message, "display_bcc", "") or "")),
        ]
        recipients = ", ".join(part for part in parts if part)
        return recipients or None

    def _attachment_file_name(attachment: object, ordinal: int) -> str:
        raw_name = None
        if isinstance(attachment, dict):
            raw_name = attachment.get("file_name") or attachment.get("name") or attachment.get("filename")
        else:
            for attr_name in ("name", "filename", "long_filename"):
                value = getattr(attachment, attr_name, None)
                if value:
                    raw_name = value
                    break
        return normalize_attachment_filename(raw_name if isinstance(raw_name, str) else None, ordinal)

    def _message_html_body(message: object) -> str | None:
        for attr_name in ("html_body", "htmlBody"):
            value = getattr(message, attr_name, None)
            if value is None:
                continue
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="replace")
            return str(value)
        return None

    def _message_text_body(message: object, html_body: str | None) -> str | None:
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

    def _iter_folder(folder: object, ancestors: list[str]):
        folder_name = normalize_whitespace(str(getattr(folder, "name", "") or ""))
        current_ancestors = [*ancestors]
        if folder_name:
            current_ancestors.append(folder_name)
        folder_path = pst_message_folder_path(current_ancestors)

        for message in _iter_collection(
            folder,
            list_attrs=("sub_messages", "messages"),
            count_getter_pairs=(("number_of_sub_messages", "get_sub_message"), ("number_of_messages", "get_message")),
        ):
            source_item_id = None
            for attr_name in ("entry_identifier", "entry_identifier_string", "record_key", "search_key", "identifier"):
                source_item_id = _normalize_pst_identifier(getattr(message, attr_name, None))
                if source_item_id:
                    break
            if not source_item_id:
                raise RetrieverError(f"PST message is missing a stable item identifier in {path}")

            html_body = _message_html_body(message)
            attachments: list[dict[str, object]] = []
            for ordinal, attachment in enumerate(
                _iter_collection(
                    message,
                    list_attrs=("attachments",),
                    count_getter_pairs=(("number_of_attachments", "get_attachment"),),
                ),
                start=1,
            ):
                payload = coerce_pst_attachment_payload(attachment)
                if payload is None:
                    continue
                attachments.append(
                    {
                        "file_name": _attachment_file_name(attachment, ordinal),
                        "ordinal": ordinal,
                        "payload": payload,
                        "file_hash": sha256_bytes(payload),
                        "content_id": pst_attachment_content_id(attachment),
                    }
                )

            yield {
                "source_item_id": source_item_id,
                "folder_path": folder_path,
                "message_class": normalize_whitespace(str(getattr(message, "message_class", "") or "")) or None,
                "subject": normalize_whitespace(str(getattr(message, "subject", "") or "")) or None,
                "author": _message_author(message),
                "recipients": _message_recipients(message),
                "date_created": normalize_datetime(
                    getattr(message, "delivery_time", None)
                    or getattr(message, "client_submit_time", None)
                    or getattr(message, "creation_time", None)
                ),
                "text_body": _message_text_body(message, html_body),
                "html_body": html_body,
                "attachments": attachments,
            }

        for child_folder in _iter_collection(
            folder,
            list_attrs=("sub_folders", "folders"),
            count_getter_pairs=(("number_of_sub_folders", "get_sub_folder"), ("number_of_folders", "get_folder")),
        ):
            yield from _iter_folder(child_folder, current_ancestors)

    pst_file = pypff.file()  # type: ignore[union-attr]
    pst_file.open(str(path))
    try:
        root_folder = pst_file.get_root_folder()
        yield from _iter_folder(root_folder, [])
    finally:
        try:
            pst_file.close()
        except Exception:
            pass


def normalize_pst_message(source_rel_path: str, message_dict: dict[str, object]) -> dict[str, object] | None:
    source_item_id = normalize_source_item_id(
        message_dict.get("source_item_id")
        or message_dict.get("entry_identifier")
        or message_dict.get("identifier")
    )
    normalized_attachments: list[dict[str, object]] = []
    for ordinal, raw_attachment in enumerate(list(message_dict.get("attachments") or []), start=1):
        raw_name = None
        raw_content_id: object = None
        if isinstance(raw_attachment, dict):
            raw_name = raw_attachment.get("file_name") or raw_attachment.get("name") or raw_attachment.get("filename")
            raw_content_id = raw_attachment.get("content_id")
        else:
            for attr_name in ("name", "filename", "long_filename"):
                value = getattr(raw_attachment, attr_name, None)
                if value:
                    raw_name = value
                    break
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
                "file_name": normalize_attachment_filename(raw_name if isinstance(raw_name, str) else None, attachment_ordinal),
                "ordinal": attachment_ordinal,
                "payload": payload,
                "file_hash": sha256_bytes(payload),
                "content_id": normalize_content_id(raw_content_id),
            }
        )

    html_body = message_dict.get("html_body")
    normalized_subject = normalize_whitespace(str(message_dict.get("subject") or "")) or None
    normalized_author = normalize_whitespace(str(message_dict.get("author") or "")) or None
    normalized_recipients = normalize_whitespace(str(message_dict.get("recipients") or "")) or None
    normalized_date_created = normalize_datetime(message_dict.get("date_created"))
    normalized_text_body = None if message_dict.get("text_body") is None else str(message_dict.get("text_body") or "")
    normalized_html_body = None if html_body is None else str(html_body)
    chat_text = normalized_text_body or (strip_html_tags(normalized_html_body) if normalized_html_body else "")
    chat_metadata = extract_chat_transcript_metadata(chat_text)
    message_kind = classify_pst_message_kind(message_dict, chat_metadata)
    if message_kind == "skip":
        return None
    if message_kind == "chat":
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
        )
        extracted = build_chat_extracted_payload(
            title=normalized_subject,
            author=normalized_author,
            date_created=normalized_date_created,
            text_body=normalized_text_body,
            html_body=normalized_html_body,
            attachments=normalized_attachments,
            preview_file_name=pst_preview_file_name(source_item_id),
            chat_metadata=resolved_chat_metadata,
            chat_entries=chat_entries,
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
        extracted = build_email_extracted_payload(
            subject=normalized_subject,
            author=normalized_author,
            recipients=normalized_recipients,
            date_created=normalized_date_created,
            text_body=normalized_text_body,
            html_body=normalized_html_body,
            attachments=normalized_attachments,
            preview_file_name=pst_preview_file_name(source_item_id),
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
    if file_type == "pst":
        raise RetrieverError("PST sources must be ingested through the container ingest pipeline.")
    raise RetrieverError(f"Unsupported file type: .{file_type}")
