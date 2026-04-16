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
    for row in preview_rows:
        rel_preview = str(Path(".retriever") / row["rel_preview_path"])
        targets.append(
            {
                "rel_path": rel_preview,
                "abs_path": str(paths["state_dir"] / row["rel_preview_path"]),
                "preview_type": row["preview_type"],
                "label": row["label"],
                "ordinal": row["ordinal"],
            }
        )
    source_targets = production_source_part_targets(paths, connection, connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone())
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


def extract_plain_text_file(path: Path) -> dict[str, object]:
    decoded, text_status, _ = decode_bytes(path.read_bytes())
    file_type = normalize_extension(path)
    text_content = strip_html_tags(decoded) if file_type in {"htm", "html"} else normalize_whitespace(decoded)
    email_headers = extract_email_like_headers(text_content)
    participants = extract_email_chain_participants(
        text_content,
        [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
    ) or extract_chat_participants(text_content)
    title = email_headers.get("title") if email_headers else None
    if title is None and file_type in {"md", "txt"} and text_content:
        title = text_content.splitlines()[0][:200]
    return {
        "page_count": None,
        "author": email_headers.get("author") if email_headers else None,
        "content_type": determine_content_type(path, text_content, email_headers=email_headers),
        "date_created": email_headers.get("date_created") if email_headers else None,
        "date_modified": None,
        "participants": participants,
        "title": title,
        "subject": email_headers.get("subject") if email_headers else None,
        "recipients": email_headers.get("recipients") if email_headers else None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else text_status,
        "preview_artifacts": [],
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
    participants = extract_email_chain_participants(
        text_content,
        [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
    ) or extract_chat_participants(text_content)
    title = email_headers.get("title") if email_headers else None
    if title is None and text_content:
        title = text_content.splitlines()[0][:200]
    preview = build_html_preview(
        {},
        body_text=text_content,
    )
    return {
        "page_count": None,
        "author": email_headers.get("author") if email_headers else None,
        "content_type": determine_content_type(
            path,
            text_content,
            email_headers=email_headers,
            explicit_content_type="E-Doc",
        ),
        "date_created": email_headers.get("date_created") if email_headers else None,
        "date_modified": None,
        "participants": participants,
        "title": title,
        "subject": email_headers.get("subject") if email_headers else None,
        "recipients": email_headers.get("recipients") if email_headers else None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else text_status,
        "preview_artifacts": [
            {
                "file_name": f"{path.name}.html",
                "preview_type": "html",
                "label": "text",
                "ordinal": 0,
                "content": preview,
            }
        ],
    }


def extract_pdf_file(path: Path) -> dict[str, object]:
    dependency_guard(pdfplumber, "pdfplumber", "pdf")
    with pdfplumber.open(path) as pdf:  # type: ignore[union-attr]
        metadata = pdf.metadata or {}
        texts = [(page.extract_text() or "").strip() for page in pdf.pages]
        text_content = normalize_whitespace("\n\n".join(part for part in texts if part))
        email_headers = extract_email_like_headers(texts[0] if texts else "")
        participants = extract_email_chain_participants(
            text_content,
            [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
        ) or extract_chat_participants(text_content)
        return {
            "page_count": len(pdf.pages),
            "author": (email_headers.get("author") if email_headers else None) or metadata.get("Author"),
            "content_type": determine_content_type(
                path,
                text_content,
                email_headers=email_headers,
                explicit_content_type="E-Doc",
            ),
            "date_created": (email_headers.get("date_created") if email_headers else None)
            or normalize_datetime(metadata.get("CreationDate")),
            "date_modified": normalize_datetime(metadata.get("ModDate")),
            "participants": participants,
            "title": (email_headers.get("title") if email_headers else None) or metadata.get("Title"),
            "subject": (email_headers.get("subject") if email_headers else None) or metadata.get("Subject"),
            "recipients": email_headers.get("recipients") if email_headers else None,
            "text_content": text_content,
            "text_status": "empty" if not text_content else "ok",
            "preview_artifacts": [],
        }


def extract_docx_file(path: Path) -> dict[str, object]:
    dependency_guard(DocxDocument, "python-docx", "docx")
    document = DocxDocument(path)  # type: ignore[operator]
    text_content = normalize_whitespace("\n\n".join(paragraph.text for paragraph in document.paragraphs if paragraph.text))
    props = document.core_properties
    email_headers = extract_email_like_headers(text_content)
    participants = extract_email_chain_participants(
        text_content,
        [email_headers.get("author"), email_headers.get("recipients")] if email_headers else None,
    ) or extract_chat_participants(text_content)
    return {
        "page_count": None,
        "author": (email_headers.get("author") if email_headers else None) or props.author or None,
        "content_type": determine_content_type(
            path,
            text_content,
            email_headers=email_headers,
            explicit_content_type="E-Doc",
        ),
        "date_created": (email_headers.get("date_created") if email_headers else None) or normalize_datetime(props.created),
        "date_modified": normalize_datetime(props.modified),
        "participants": participants,
        "title": (email_headers.get("title") if email_headers else None) or props.title or None,
        "subject": (email_headers.get("subject") if email_headers else None) or props.subject or None,
        "recipients": email_headers.get("recipients") if email_headers else None,
        "text_content": text_content,
        "text_status": "empty" if not text_content else "ok",
        "preview_artifacts": [],
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
        if disposition != "attachment" and not file_name:
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
    preview = build_html_preview(
        {
            "From": author or "",
            "To": recipients or "",
            "Date": date_created or "",
            "Subject": subject or "",
        },
        body_html=normalized_html,
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
                    }
                )

            yield {
                "source_item_id": source_item_id,
                "folder_path": folder_path,
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


def normalize_pst_message(source_rel_path: str, message_dict: dict[str, object]) -> dict[str, object]:
    source_item_id = normalize_source_item_id(
        message_dict.get("source_item_id")
        or message_dict.get("entry_identifier")
        or message_dict.get("identifier")
    )
    normalized_attachments: list[dict[str, object]] = []
    for ordinal, raw_attachment in enumerate(list(message_dict.get("attachments") or []), start=1):
        raw_name = None
        if isinstance(raw_attachment, dict):
            raw_name = raw_attachment.get("file_name") or raw_attachment.get("name") or raw_attachment.get("filename")
        else:
            for attr_name in ("name", "filename", "long_filename"):
                value = getattr(raw_attachment, attr_name, None)
                if value:
                    raw_name = value
                    break
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
            }
        )

    html_body = message_dict.get("html_body")
    extracted = build_email_extracted_payload(
        subject=normalize_whitespace(str(message_dict.get("subject") or "")) or None,
        author=normalize_whitespace(str(message_dict.get("author") or "")) or None,
        recipients=normalize_whitespace(str(message_dict.get("recipients") or "")) or None,
        date_created=normalize_datetime(message_dict.get("date_created")),
        text_body=None if message_dict.get("text_body") is None else str(message_dict.get("text_body") or ""),
        html_body=None if html_body is None else str(html_body),
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
