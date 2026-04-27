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
    raise RetrieverError(f"Unsupported file type: .{file_type}")


def relative_document_path(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def collect_files(root: Path, recursive: bool, allowed_file_types: set[str] | None) -> list[Path]:
    iterator = root.rglob("*") if recursive else root.iterdir()
    files: list[Path] = []
    for path in iterator:
        if path.is_dir():
            if path.name == ".retriever":
                continue
            if not recursive:
                continue
            continue
        if ".retriever" in path.parts:
            continue
        file_type = normalize_extension(path)
        if allowed_file_types and file_type not in allowed_file_types:
            continue
        if not file_type:
            continue
        files.append(path)
    return sorted(files)


def normalize_production_header(raw_header: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", raw_header.strip().lower()).strip()
    squashed = normalized.replace(" ", "")
    return PRODUCTION_DAT_HEADER_ALIASES.get(normalized) or PRODUCTION_DAT_HEADER_ALIASES.get(squashed) or normalized.replace(" ", "_")


def parse_concordance_rows(raw_bytes: bytes) -> list[list[str]]:
    text = raw_bytes.decode("latin-1")
    reader = csv.reader(io.StringIO(text), delimiter="\x14", quotechar="\xfe")
    return [[field.strip().strip("\ufeff") for field in row] for row in reader if any(field.strip() for field in row)]


def parse_generic_delimited_rows(raw_bytes: bytes) -> list[list[str]]:
    decoded, _, _ = decode_bytes(raw_bytes)
    sample = decoded[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(decoded), dialect)
    return [[field.strip().strip("\ufeff") for field in row] for row in reader if any(field.strip() for field in row)]


def parse_production_metadata_load(path: Path) -> dict[str, object]:
    raw_bytes = path.read_bytes()
    rows = parse_concordance_rows(raw_bytes) if b"\x14" in raw_bytes else parse_generic_delimited_rows(raw_bytes)
    if not rows:
        raise RetrieverError(f"Production metadata load file is empty: {path}")
    headers = [normalize_production_header(header) for header in rows[0]]
    if "begin_bates" not in headers or "end_bates" not in headers:
        raise RetrieverError(f"Production metadata load file missing Bates headers: {path}")
    records: list[dict[str, str]] = []
    for values in rows[1:]:
        record: dict[str, str] = {}
        for index, header in enumerate(headers):
            if index >= len(values):
                continue
            value = values[index].strip()
            if value:
                record[header] = value
        if not record:
            continue
        if "text_path" not in record:
            for key, value in list(record.items()):
                if "text" in key and normalize_extension(Path(value)) == "txt":
                    record["text_path"] = value
                    break
        if "native_path" not in record:
            for key, value in list(record.items()):
                if ("file" in key or "native" in key or "path" in key) and normalize_extension(Path(value)) not in {"", "txt"}:
                    record["native_path"] = value
                    break
        if record.get("begin_bates"):
            records.append(record)
    return {"headers": headers, "rows": records}


def parse_production_image_load(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    decoded, _, _ = decode_bytes(path.read_bytes())
    reader = csv.reader(io.StringIO(decoded))
    rows: list[dict[str, str]] = []
    for values in reader:
        if len(values) < 3:
            continue
        page_bates = values[0].strip()
        image_path = values[2].strip()
        if not page_bates or not image_path:
            continue
        rows.append(
            {
                "page_bates": page_bates,
                "volume_name": values[1].strip() if len(values) > 1 else "",
                "image_path": image_path,
                "is_first_page": values[3].strip().upper() == "Y" if len(values) > 3 else False,
            }
        )
    return rows


def find_production_load_file(candidate_root: Path, extension: str) -> Path | None:
    preferred_dir = candidate_root / "DATA"
    candidates: list[Path] = []
    if preferred_dir.is_dir():
        candidates.extend(sorted(preferred_dir.glob(f"*.{extension}")))
    candidates.extend(sorted(path for path in candidate_root.glob(f"*.{extension}") if path not in candidates))
    return candidates[0] if candidates else None


def production_signature_for_root(workspace_root: Path, candidate_root: Path) -> dict[str, object] | None:
    workspace_root = workspace_root.resolve()
    candidate_root = candidate_root.resolve()
    metadata_load_path = find_production_load_file(candidate_root, "dat")
    if metadata_load_path is None:
        return None
    has_payload_dirs = any((candidate_root / name).is_dir() for name in ("TEXT", "IMAGES", "NATIVES"))
    if not has_payload_dirs:
        return None
    try:
        metadata = parse_production_metadata_load(metadata_load_path)
    except Exception:
        return None
    headers = set(metadata["headers"])
    if not {"begin_bates", "end_bates"}.issubset(headers):
        return None
    image_load_path = find_production_load_file(candidate_root, "opt")
    return {
        "root": candidate_root,
        "rel_root": candidate_root.relative_to(workspace_root).as_posix(),
        "production_name": candidate_root.name,
        "metadata_load_path": metadata_load_path,
        "image_load_path": image_load_path,
        "source_type": "concordance-dat-opt" if image_load_path else "concordance-dat",
    }


def find_production_root_signatures(
    workspace_root: Path,
    recursive: bool,
    connection: sqlite3.Connection | None = None,
) -> list[dict[str, object]]:
    signatures: dict[str, dict[str, object]] = {}
    if connection is not None and table_exists(connection, "productions"):
        rows = connection.execute("SELECT rel_root, production_name, metadata_load_rel_path, image_load_rel_path, source_type FROM productions").fetchall()
        for row in rows:
            candidate_root = workspace_root / row["rel_root"]
            if candidate_root.exists():
                signatures[row["rel_root"]] = {
                    "root": candidate_root,
                    "rel_root": row["rel_root"],
                    "production_name": row["production_name"],
                    "metadata_load_path": workspace_root / row["metadata_load_rel_path"],
                    "image_load_path": (workspace_root / row["image_load_rel_path"]) if row["image_load_rel_path"] else None,
                    "source_type": row["source_type"],
                }

    dat_paths = list(workspace_root.rglob("*.dat")) if recursive else list(workspace_root.glob("*.dat"))
    if not recursive:
        for child in workspace_root.iterdir():
            if child.is_dir() and child.name != ".retriever":
                data_dir = child / "DATA"
                if data_dir.is_dir():
                    dat_paths.extend(sorted(data_dir.glob("*.dat")))
    for dat_path in dat_paths:
        if ".retriever" in dat_path.parts:
            continue
        candidate_root = dat_path.parent.parent if dat_path.parent.name.lower() == "data" else dat_path.parent
        if candidate_root == workspace_root / ".retriever":
            continue
        try:
            signature = production_signature_for_root(workspace_root, candidate_root)
        except Exception:
            signature = None
        if signature is not None:
            signatures[str(signature["rel_root"])] = signature
    return [signatures[key] for key in sorted(signatures)]


def normalize_source_reference(raw_value: str) -> list[str]:
    normalized = raw_value.strip().strip('"').strip("'").replace("\\", "/")
    normalized = re.sub(r"^[.]/+", "", normalized)
    normalized = normalized.lstrip("/")
    return [part for part in normalized.split("/") if part and part != "."]


def source_reference_suffixes(parts: list[str]) -> list[list[str]]:
    seen: set[tuple[str, ...]] = set()
    suffixes: list[list[str]] = []
    for index in range(len(parts)):
        suffix = tuple(parts[index:])
        if not suffix or suffix in seen:
            continue
        seen.add(suffix)
        suffixes.append(list(suffix))
    return suffixes


def resolve_case_insensitive_relative_path(base_dir: Path, parts: list[str]) -> Path | None:
    current = base_dir
    for part in parts:
        if not current.is_dir():
            return None
        entries = {child.name.lower(): child for child in current.iterdir()}
        candidate = entries.get(part.lower())
        if candidate is None:
            return None
        current = candidate
    return current


def resolve_production_source_path(workspace_root: Path, production_root: Path, raw_value: str | None) -> Path | None:
    if raw_value is None:
        return None
    stripped = raw_value.strip().strip('"').strip("'")
    if not stripped:
        return None
    raw_path = Path(stripped)
    if raw_path.is_absolute():
        try:
            absolute = raw_path.resolve()
        except Exception:
            absolute = raw_path
        return absolute if absolute.exists() else None

    parts = normalize_source_reference(stripped)
    suffixes = source_reference_suffixes(parts)
    if not suffixes:
        return None

    # Preserve existing literal resolution before we attempt any fallback prefix stripping.
    literal_parts = suffixes[0]
    resolved = resolve_case_insensitive_relative_path(production_root, literal_parts)
    if resolved is not None:
        return resolved
    resolved = resolve_case_insensitive_relative_path(workspace_root, literal_parts)
    if resolved is not None:
        return resolved

    for candidate_parts in suffixes[1:]:
        resolved = resolve_case_insensitive_relative_path(production_root, candidate_parts)
        if resolved is not None:
            return resolved
    for candidate_parts in suffixes[1:]:
        resolved = resolve_case_insensitive_relative_path(workspace_root, candidate_parts)
        if resolved is not None:
            return resolved
    return None


def production_logical_rel_path(production_rel_root: str, control_number: str) -> str:
    production_slug = sanitize_storage_filename(Path(production_rel_root).name)
    control_slug = sanitize_storage_filename(control_number)
    return Path(INTERNAL_REL_PATH_PREFIX) / "productions" / production_slug / "documents" / f"{control_slug}.logical"


def infer_production_title(control_number: str, text_content: str, native_path: Path | None) -> str:
    for line in text_content.splitlines():
        candidate = normalize_whitespace(line)
        if candidate:
            if re.match(r"^(From|To|Cc|Bcc|Sent|Date|Subject):\s*", candidate, flags=re.IGNORECASE):
                continue
            return normalize_generated_document_title(candidate[:220]) or candidate[:220]
    if native_path is not None:
        return normalize_generated_document_title(native_path.stem or native_path.name) or native_path.stem or native_path.name
    return control_number


def build_production_preview_html(
    *,
    document_title: str,
    control_number: str,
    production_name: str,
    begin_bates: str,
    end_bates: str,
    begin_attachment: str | None,
    end_attachment: str | None,
    text_content: str,
    page_images: list[dict[str, object]],
) -> str:
    headers = {
        passive_field_label("production_name"): production_name,
        passive_field_label("control_number"): control_number,
        passive_field_label("begin_bates"): begin_bates,
        passive_field_label("end_bates"): end_bates,
        passive_field_label("begin_attachment"): begin_attachment or "",
        passive_field_label("end_attachment"): end_attachment or "",
    }
    sections: list[str] = []
    if text_content.strip():
        sections.append(
            "<section><h2>Linked Text</h2><pre>"
            + html.escape(text_content)
            + "</pre></section>"
        )
    if page_images:
        image_sections: list[str] = ["<section><h2>Produced Pages</h2>"]
        for image in page_images:
            label = html.escape(str(image["label"]))
            src = html.escape(str(image["src"]))
            image_sections.append(f'<figure><figcaption>{label}</figcaption><img src="{src}" alt="{label}"/></figure>')
        image_sections.append("</section>")
        sections.append("".join(image_sections))
    body_html = "".join(sections) or "<p>No linked text or page images were available for this production document.</p>"
    head_html = """
<style>
body { font-family: Georgia, serif; margin: 2rem auto; max-width: 960px; line-height: 1.5; color: #1f2933; }
table { border-collapse: collapse; width: 100%; margin-bottom: 1.5rem; }
th, td { text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid #d8dee4; vertical-align: top; }
figure { margin: 1.25rem 0; }
img { max-width: 100%; height: auto; border: 1px solid #d8dee4; background: #fff; }
pre { white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #d8dee4; padding: 0.75rem; }
</style>
""".strip()
    return build_html_preview(headers, body_html=body_html, document_title=document_title, head_html=head_html)


def attachment_preview_link_label(row: sqlite3.Row) -> str:
    file_name = normalize_whitespace(str(row["file_name"] or ""))
    title = normalize_generated_document_title(row["title"])
    control_number = normalize_whitespace(str(row["control_number"] or ""))
    if file_name and not file_name.lower().endswith(".logical"):
        return file_name
    if title:
        return title
    if file_name:
        return file_name
    return control_number or "Attachment"


def relative_preview_href(path: Path, parent_preview_path: Path, target_fragment: object = None) -> str:
    href = urllib_request.pathname2url(
        os.path.relpath(str(path), start=str(parent_preview_path.parent))
    )
    return append_preview_fragment(href, target_fragment)


def parse_calendar_attachment_metadata(paths: dict[str, Path], child_row: sqlite3.Row) -> dict[str, object] | None:
    content_type = normalize_whitespace(str(child_row["content_type"] or ""))
    file_type = normalize_whitespace(str(child_row["file_type"] or "")).lower()
    if not file_type:
        file_type = normalize_extension(Path(str(child_row["file_name"] or child_row["rel_path"] or "")))
    if content_type != "Calendar" and file_type not in ICALENDAR_FILE_TYPES:
        return None
    rel_path = normalize_whitespace(str(child_row["rel_path"] or ""))
    if not rel_path:
        return None
    attachment_path = document_absolute_path(paths, rel_path)
    if not attachment_path.exists():
        return None
    try:
        decoded, _, _ = decode_bytes(attachment_path.read_bytes())
    except OSError:
        return None
    metadata = parse_icalendar_event_metadata(decoded)
    if not metadata:
        return None
    if not any(metadata.get(key) for key in ("summary", "when", "organizer", "attendees_display", "conference_url")):
        return None
    return metadata


def build_calendar_attachment_preview_link(
    *,
    paths: dict[str, Path],
    child_row: sqlite3.Row,
    child_preview_path: Path,
    parent_preview_path: Path,
) -> dict[str, str] | None:
    metadata = parse_calendar_attachment_metadata(paths, child_row)
    if metadata is None:
        return None
    title = (
        normalize_generated_document_title(metadata.get("summary"))
        or normalize_generated_document_title(child_row["title"])
        or attachment_preview_link_label(child_row)
    )
    organizer = metadata.get("organizer") or normalize_whitespace(str(child_row["author"] or "")) or None
    attendees = metadata.get("attendees_display") or normalize_whitespace(str(child_row["recipients"] or "")) or None
    return {
        "kind": "calendar_invite",
        "href": relative_preview_href(child_preview_path, parent_preview_path),
        "label": attachment_preview_link_label(child_row),
        "detail": normalize_whitespace(str(child_row["control_number"] or "")),
        "title": str(title or "Calendar invite"),
        "when": str(metadata.get("when") or ""),
        "organizer": str(organizer or ""),
        "attendees": str(attendees or ""),
        "location": str(metadata.get("location") or ""),
        "join_href": str(metadata.get("conference_url") or ""),
        "status": str(summarize_icalendar_invite_status(metadata) or ""),
    }


def build_document_attachment_preview_links(
    paths: dict[str, Path],
    connection: sqlite3.Connection,
    parent_preview_path: Path,
    child_rows: list[sqlite3.Row],
) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for child_row in child_rows:
        child_preview_target = default_preview_target(paths, child_row, connection)
        child_preview_abs_path = str(
            child_preview_target.get("file_abs_path")
            or child_preview_target.get("abs_path")
            or ""
        ).split("#", 1)[0]
        child_preview_path = Path(child_preview_abs_path)
        if not child_preview_path.exists():
            continue
        calendar_link = build_calendar_attachment_preview_link(
            paths=paths,
            child_row=child_row,
            child_preview_path=child_preview_path,
            parent_preview_path=parent_preview_path,
        )
        if calendar_link is not None:
            links.append(calendar_link)
            continue
        detail = normalize_whitespace(str(child_row["control_number"] or ""))
        links.append(
            {
                "href": relative_preview_href(child_preview_path, parent_preview_path),
                "label": attachment_preview_link_label(child_row),
                "detail": detail,
            }
        )
    return links


def conversation_attachment_links_by_document_id(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    segment_preview_path: Path,
    documents: list[dict[str, object]],
) -> dict[int, list[dict[str, str]]]:
    parent_ids = [
        int(document["id"])
        for document in documents
        if document.get("parent_document_id") is None
    ]
    if not parent_ids:
        return {}
    child_rows = connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE parent_document_id IN ({", ".join("?" for _ in parent_ids)})
          AND lifecycle_status NOT IN ('missing', 'deleted')
          AND COALESCE(child_document_kind, ?) = ?
        ORDER BY
          parent_document_id ASC,
          CASE WHEN control_number_attachment_sequence IS NULL THEN 1 ELSE 0 END ASC,
          control_number_attachment_sequence ASC,
          control_number ASC,
          id ASC
        """,
        (*parent_ids, CHILD_DOCUMENT_KIND_ATTACHMENT, CHILD_DOCUMENT_KIND_ATTACHMENT),
    ).fetchall()
    child_rows_by_parent_id: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for child_row in child_rows:
        child_rows_by_parent_id[int(child_row["parent_document_id"])].append(child_row)
    return {
        parent_id: build_document_attachment_preview_links(
            paths,
            connection,
            segment_preview_path,
            rows,
        )
        for parent_id, rows in child_rows_by_parent_id.items()
    }


def sync_document_attachment_preview_links(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    document_id: int,
) -> int:
    preview_rows = connection.execute(
        """
        SELECT rel_preview_path
        FROM document_previews
        WHERE document_id = ? AND preview_type = 'html'
        ORDER BY ordinal ASC, id ASC
        """,
        (document_id,),
    ).fetchall()
    if not preview_rows:
        return 0

    child_rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE parent_document_id = ?
          AND lifecycle_status NOT IN ('missing', 'deleted')
          AND COALESCE(child_document_kind, ?) = ?
        ORDER BY
          CASE WHEN control_number_attachment_sequence IS NULL THEN 1 ELSE 0 END ASC,
          control_number_attachment_sequence ASC,
          control_number ASC,
          id ASC
        """,
        (document_id, CHILD_DOCUMENT_KIND_ATTACHMENT, CHILD_DOCUMENT_KIND_ATTACHMENT),
    ).fetchall()

    updated = 0
    for preview_row in preview_rows:
        preview_path = paths["state_dir"] / preview_row["rel_preview_path"]
        if not preview_path.exists():
            continue
        current_html = preview_path.read_text(encoding="utf-8")
        links = build_document_attachment_preview_links(paths, connection, preview_path, child_rows)
        updated_html = inject_html_preview_attachment_links(current_html, links)
        if updated_html == current_html:
            continue
        preview_path.write_text(updated_html, encoding="utf-8")
        updated += 1
    return updated


def regenerate_production_preview_for_document(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    document_id: int,
    text_content: str,
) -> dict[str, object]:
    """Rebuild the synthesized HTML preview for a production document.

    Returns a status payload. Never raises on expected skips (non-production
    doc, no HTML preview row, missing production row). Unexpected I/O errors
    propagate and should be handled by the caller as a best-effort regen.
    """
    document_row = connection.execute(
        """
        SELECT id, control_number, production_id, begin_bates, end_bates,
               begin_attachment, end_attachment, title
        FROM documents
        WHERE id = ?
        """,
        (document_id,),
    ).fetchone()
    if document_row is None:
        return {"status": "skipped", "reason": "unknown_document"}
    if document_row["production_id"] is None:
        return {"status": "skipped", "reason": "not_production"}

    preview_row = connection.execute(
        """
        SELECT rel_preview_path
        FROM document_previews
        WHERE document_id = ? AND preview_type = 'html'
        ORDER BY ordinal
        LIMIT 1
        """,
        (document_id,),
    ).fetchone()
    if preview_row is None:
        return {"status": "skipped", "reason": "no_html_preview"}

    production_row = connection.execute(
        "SELECT production_name FROM productions WHERE id = ?",
        (document_row["production_id"],),
    ).fetchone()
    if production_row is None:
        return {"status": "skipped", "reason": "missing_production_row"}

    image_rows = connection.execute(
        """
        SELECT ordinal, label, rel_source_path
        FROM document_source_parts
        WHERE document_id = ? AND part_kind = 'image'
        ORDER BY ordinal
        """,
        (document_id,),
    ).fetchall()

    page_images: list[dict[str, object]] = []
    for index, row in enumerate(image_rows, start=1):
        abs_image = paths["root"] / row["rel_source_path"]
        if not abs_image.exists():
            continue
        data_url = image_path_data_url(abs_image)
        if data_url is None:
            continue
        page_images.append(
            {
                "label": row["label"] or f"Page {index}",
                "src": data_url,
            }
        )

    resolved_title = document_row["title"] or document_row["control_number"]
    html_body = build_production_preview_html(
        document_title=resolved_title,
        control_number=document_row["control_number"],
        production_name=production_row["production_name"],
        begin_bates=document_row["begin_bates"] or "",
        end_bates=document_row["end_bates"] or "",
        begin_attachment=document_row["begin_attachment"],
        end_attachment=document_row["end_attachment"],
        text_content=text_content,
        page_images=page_images,
    )

    preview_path = paths["state_dir"] / preview_row["rel_preview_path"]
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text(html_body, encoding="utf-8")
    sync_document_attachment_preview_links(connection, paths, document_id)
    return {
        "status": "ok",
        "rel_preview_path": preview_row["rel_preview_path"],
        "page_images": len(page_images),
        "text_chars": len(text_content),
    }


def replace_document_related_rows(
    connection: sqlite3.Connection,
    document_id: int,
    metadata_values: dict[str, object],
    chunks: list[dict[str, object]],
    preview_rows: list[dict[str, object]],
) -> None:
    delete_document_related_rows(connection, document_id)
    row = connection.execute(
        "SELECT custodians_json FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    custodian_text = document_custodian_display_text_from_row(row)

    insert_document_preview_rows(connection, document_id, preview_rows)

    replace_document_chunks(connection, document_id, chunks)

    connection.execute(
        """
        INSERT INTO documents_fts (document_id, file_name, title, subject, author, custodian, participants, recipients)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            document_id,
            metadata_values["file_name"],
            metadata_values["title"],
            metadata_values["subject"],
            metadata_values["author"],
            custodian_text,
            metadata_values["participants"],
            metadata_values["recipients"],
        ),
    )


def insert_document_preview_rows(
    connection: sqlite3.Connection,
    document_id: int,
    preview_rows: list[dict[str, object]],
) -> None:
    if not preview_rows:
        return
    connection.executemany(
        """
        INSERT INTO document_previews (
          document_id, rel_preview_path, preview_type, target_fragment, label, ordinal, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                document_id,
                row["rel_preview_path"],
                row["preview_type"],
                row.get("target_fragment"),
                row.get("label"),
                row["ordinal"],
                row["created_at"],
            )
            for row in preview_rows
        ],
    )


def replace_document_preview_rows(
    connection: sqlite3.Connection,
    document_id: int,
    preview_rows: list[dict[str, object]],
) -> None:
    connection.execute("DELETE FROM document_previews WHERE document_id = ?", (document_id,))
    insert_document_preview_rows(connection, document_id, preview_rows)


def replace_document_source_parts(
    connection: sqlite3.Connection,
    document_id: int,
    source_parts: list[dict[str, object]],
) -> None:
    connection.execute("DELETE FROM document_source_parts WHERE document_id = ?", (document_id,))
    if not source_parts:
        return
    connection.executemany(
        """
        INSERT INTO document_source_parts (
          document_id, part_kind, rel_source_path, ordinal, label, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                document_id,
                row["part_kind"],
                row["rel_source_path"],
                row.get("ordinal", 0),
                row.get("label"),
                row.get("created_at", utc_now()),
            )
            for row in source_parts
        ],
    )


def replace_document_chunks(
    connection: sqlite3.Connection,
    document_id: int,
    chunks: list[dict[str, object]],
) -> None:
    connection.execute("DELETE FROM document_chunks WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM chunks_fts WHERE document_id = ?", (document_id,))
    if not chunks:
        return
    connection.executemany(
        """
        INSERT INTO document_chunks (
          document_id, chunk_index, char_start, char_end, token_estimate, text_content
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                document_id,
                chunk["chunk_index"],
                chunk["char_start"],
                chunk["char_end"],
                chunk["token_estimate"],
                chunk["text_content"],
            )
            for chunk in chunks
        ],
    )
    chunk_rows = connection.execute(
        """
        SELECT id, document_id, text_content
        FROM document_chunks
        WHERE document_id = ?
        ORDER BY chunk_index ASC
        """,
        (document_id,),
    ).fetchall()
    connection.executemany(
        """
        INSERT INTO chunks_fts (chunk_id, document_id, text_content)
        VALUES (?, ?, ?)
        """,
        [(row["id"], row["document_id"], row["text_content"]) for row in chunk_rows],
    )


def delete_document_related_rows(connection: sqlite3.Connection, document_id: int) -> None:
    connection.execute("DELETE FROM document_chunks WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM chunks_fts WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM document_previews WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM document_source_parts WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM documents_fts WHERE document_id = ?", (document_id,))


def cleanup_unreferenced_preview_files(
    paths: dict[str, Path],
    connection: sqlite3.Connection,
    rel_preview_paths: list[str] | set[str] | tuple[str, ...],
) -> None:
    seen: set[str] = set()
    for rel_preview_path in rel_preview_paths:
        normalized_rel_path = normalize_whitespace(str(rel_preview_path or ""))
        if not normalized_rel_path or normalized_rel_path in seen:
            continue
        seen.add(normalized_rel_path)
        referenced_elsewhere = connection.execute(
            """
            SELECT 1
            FROM document_previews
            WHERE rel_preview_path = ?
            LIMIT 1
            """,
            (normalized_rel_path,),
        ).fetchone()
        if referenced_elsewhere is None:
            remove_file_if_exists(paths["state_dir"] / normalized_rel_path)


def refresh_documents_fts_row(connection: sqlite3.Connection, document_id: int) -> None:
    connection.execute("DELETE FROM documents_fts WHERE document_id = ?", (document_id,))
    row = connection.execute(
        """
        SELECT id, file_name, title, subject, author, custodians_json, participants, recipients
        FROM documents
        WHERE id = ?
        """,
        (document_id,),
    ).fetchone()
    if row is None:
        return
    connection.execute(
        """
        INSERT INTO documents_fts (document_id, file_name, title, subject, author, custodian, participants, recipients)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["id"],
            row["file_name"],
            row["title"],
            row["subject"],
            row["author"],
            document_custodian_display_text_from_row(row),
            row["participants"],
            row["recipients"],
        ),
    )


def write_preview_artifacts(
    paths: dict[str, Path], rel_path: str, preview_artifacts: list[dict[str, object]]
) -> list[dict[str, object]]:
    preview_base = preview_base_path_for_rel_path(rel_path)
    preview_rows: list[dict[str, object]] = []
    for artifact in preview_artifacts:
        preview_rel_path = preview_base / str(artifact["file_name"])
        absolute_path = paths["state_dir"] / preview_rel_path
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        absolute_path.write_text(str(artifact["content"]), encoding="utf-8")
        preview_rows.append(
            {
                "rel_preview_path": preview_rel_path.as_posix(),
                "preview_type": artifact["preview_type"],
                "target_fragment": artifact.get("target_fragment"),
                "label": artifact.get("label"),
                "ordinal": int(artifact.get("ordinal", 0)),
                "created_at": utc_now(),
            }
        )
    return preview_rows


def write_attachment_blob(
    paths: dict[str, Path],
    parent_rel_path: str,
    control_number: str,
    file_name: str,
    payload: bytes,
) -> tuple[str, Path]:
    storage_name = sanitize_storage_filename(file_name)
    source_rel_path = container_source_rel_path_from_message_rel_path(parent_rel_path)
    if source_rel_path is not None:
        preview_rel_path = Path("previews") / Path(source_rel_path) / "attachments" / control_number / storage_name
    else:
        preview_rel_path = Path("previews") / Path(parent_rel_path) / "attachments" / control_number / storage_name
    absolute_path = paths["state_dir"] / preview_rel_path
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_bytes(payload)
    rel_path = Path(INTERNAL_REL_PATH_PREFIX) / preview_rel_path
    return rel_path.as_posix(), absolute_path


def remove_file_if_exists(path: Path) -> None:
    try:
        if path.is_file() or path.is_symlink():
            path.unlink()
    except FileNotFoundError:
        return


def cleanup_document_artifacts(
    paths: dict[str, Path],
    connection: sqlite3.Connection,
    row: sqlite3.Row | None,
) -> None:
    if row is None:
        return
    preview_rows = connection.execute(
        """
        SELECT rel_preview_path
        FROM document_previews
        WHERE document_id = ?
        """,
        (row["id"],),
    ).fetchall()
    for preview_row in preview_rows:
        referenced_elsewhere = connection.execute(
            """
            SELECT 1
            FROM document_previews
            WHERE rel_preview_path = ?
              AND document_id != ?
            LIMIT 1
            """,
            (preview_row["rel_preview_path"], row["id"]),
        ).fetchone()
        if referenced_elsewhere is None:
            remove_file_if_exists(paths["state_dir"] / preview_row["rel_preview_path"])
    if is_internal_rel_path(row["rel_path"]):
        remove_file_if_exists(document_absolute_path(paths, row["rel_path"]))


def parse_file_types(raw_value: str | None) -> set[str] | None:
    if raw_value is None:
        return None
    parts = {part.strip().lower() for part in raw_value.split(",") if part.strip()}
    invalid = sorted(parts - SUPPORTED_FILE_TYPES)
    if invalid:
        raise RetrieverError(f"Unsupported file type filters: {', '.join(invalid)}")
    return parts


def apply_manual_locks(existing_row: sqlite3.Row | None, extracted: dict[str, object]) -> dict[str, object]:
    if existing_row is None:
        return extracted
    locked_fields = set(normalize_string_list(existing_row[MANUAL_FIELD_LOCKS_COLUMN]))
    merged = dict(extracted)
    for field_name in locked_fields:
        if field_name in EDITABLE_BUILTIN_FIELDS:
            merged[field_name] = existing_row[field_name]
    return merged


def build_fallback_extract(path: Path) -> dict[str, object]:
    return {
        "page_count": None,
        "author": None,
        "content_type": infer_content_type_from_extension(normalize_extension(path)),
        "custodian": None,
        "date_created": None,
        "date_modified": None,
        "participants": None,
        "title": path.stem or path.name,
        "subject": None,
        "recipients": None,
        "text_content": "",
        "text_status": "empty",
        "preview_artifacts": [],
        "attachments": [],
    }


def upsert_document_row(
    connection: sqlite3.Connection,
    rel_path: str,
    source_path: Path | None,
    existing_row: sqlite3.Row | None,
    extracted: dict[str, object],
    *,
    existing_occurrence_row: sqlite3.Row | None = None,
    file_name: str,
    parent_document_id: int | None,
    child_document_kind: str | None = None,
    control_number: str,
    dataset_id: int | None,
    conversation_id: int | None = None,
    conversation_assignment_mode: str | None = None,
    control_number_batch: int | None,
    control_number_family_sequence: int | None,
    control_number_attachment_sequence: int | None,
    root_message_key: str | None = None,
    source_kind: str | None = None,
    source_rel_path: str | None = None,
    source_item_id: str | None = None,
    source_folder_path: str | None = None,
    custodian_override: str | None = None,
    production_id: int | None = None,
    begin_bates: str | None = None,
    end_bates: str | None = None,
    begin_attachment: str | None = None,
    end_attachment: str | None = None,
    file_type_override: str | None = None,
    file_size_override: int | None = None,
    file_hash_override: str | None = None,
    ingested_at_override: str | None = None,
    last_seen_at_override: str | None = None,
    updated_at_override: str | None = None,
) -> int:
    now = utc_now()
    content_hash = sha256_text(str(extracted["text_content"] or ""))
    file_hash = file_hash_override if file_hash_override is not None else (sha256_file(source_path) if source_path is not None else None)
    file_size = file_size_override if file_size_override is not None else (source_path.stat().st_size if source_path is not None and source_path.exists() else None)
    file_type = file_type_override or normalize_extension(Path(file_name)) or (normalize_extension(source_path) if source_path is not None else None)
    effective_child_kind = effective_child_document_kind(
        parent_document_id=parent_document_id,
        child_document_kind=(
            child_document_kind
            if child_document_kind is not None
            else existing_row["child_document_kind"]
            if existing_row is not None and "child_document_kind" in existing_row.keys()
            else None
        ),
    )
    effective_source_kind = source_kind or (
        EMAIL_ATTACHMENT_SOURCE_KIND
        if effective_child_kind == CHILD_DOCUMENT_KIND_ATTACHMENT
        else FILESYSTEM_SOURCE_KIND
    )
    effective_source_rel_path = source_rel_path
    if effective_source_rel_path is None:
        if existing_row is not None and existing_row["source_rel_path"] is not None:
            effective_source_rel_path = existing_row["source_rel_path"]
        else:
            effective_source_rel_path = rel_path
    effective_dataset_id = dataset_id
    if effective_dataset_id is None and existing_row is not None and existing_row["dataset_id"] is not None:
        effective_dataset_id = int(existing_row["dataset_id"])
    normalized_extracted_content_type = normalize_whitespace(str(extracted.get("content_type") or "")).lower()
    should_preserve_existing_conversation = (
        effective_child_kind is not None
        or normalized_extracted_content_type in {"email", "chat"}
        or conversation_id is not None
    )
    effective_conversation_id = conversation_id
    if (
        effective_conversation_id is None
        and should_preserve_existing_conversation
        and existing_row is not None
        and "conversation_id" in existing_row.keys()
    ):
        if existing_row["conversation_id"] is not None:
            effective_conversation_id = int(existing_row["conversation_id"])
    effective_conversation_assignment = effective_conversation_assignment_mode(
        conversation_assignment_mode
        if conversation_assignment_mode is not None
        else existing_row["conversation_assignment_mode"]
        if existing_row is not None and "conversation_assignment_mode" in existing_row.keys()
        else None
    )
    if not should_preserve_existing_conversation:
        effective_conversation_assignment = CONVERSATION_ASSIGNMENT_MODE_AUTO
    effective_root_message_key = root_message_key
    if effective_root_message_key is None and existing_row is not None and "root_message_key" in existing_row.keys():
        effective_root_message_key = existing_row["root_message_key"]
    custodian = extracted.get("custodian")
    if custodian is None:
        custodian = infer_source_custodian(
            source_kind=effective_source_kind,
            source_rel_path=source_rel_path,
            parent_custodian=custodian_override,
        )
    common_values = {
        "control_number": control_number,
        "canonical_kind": canonical_kind_from_metadata(
            extracted_content_type=extracted.get("content_type"),
            file_type=file_type,
            source_kind=effective_source_kind,
        ),
        "canonical_status": CANONICAL_STATUS_ACTIVE,
        "conversation_id": effective_conversation_id,
        "conversation_assignment_mode": effective_conversation_assignment,
        "dataset_id": effective_dataset_id,
        "parent_document_id": parent_document_id,
        "child_document_kind": effective_child_kind,
        "source_kind": effective_source_kind,
        "source_rel_path": effective_source_rel_path,
        "source_item_id": source_item_id,
        "root_message_key": effective_root_message_key,
        "source_folder_path": source_folder_path,
        "production_id": production_id,
        "begin_bates": begin_bates,
        "end_bates": end_bates,
        "begin_attachment": begin_attachment,
        "end_attachment": end_attachment,
        "rel_path": rel_path,
        "file_name": file_name,
        "file_type": file_type,
        "file_size": file_size,
        "page_count": extracted.get("page_count"),
        "author": extracted.get("author"),
        "content_type": extracted.get("content_type"),
        "date_created": extracted.get("date_created"),
        "date_modified": extracted.get("date_modified"),
        "participants": extracted.get("participants"),
        "title": extracted.get("title"),
        "subject": extracted.get("subject"),
        "recipients": extracted.get("recipients"),
        "file_hash": file_hash,
        "content_hash": content_hash,
        "text_status": extracted.get("text_status", "ok"),
        "lifecycle_status": "active",
        "ingested_at": ingested_at_override or now,
        "last_seen_at": last_seen_at_override or now,
        "updated_at": updated_at_override or now,
        "control_number_batch": control_number_batch,
        "control_number_family_sequence": control_number_family_sequence,
        "control_number_attachment_sequence": control_number_attachment_sequence,
    }
    if existing_row is None:
        connection.execute(
            """
            INSERT INTO documents (
              control_number, canonical_kind, canonical_status, merged_into_document_id,
              conversation_id, conversation_assignment_mode, dataset_id, parent_document_id, child_document_kind,
              source_kind, source_rel_path, source_item_id, root_message_key, source_folder_path,
              production_id, begin_bates, end_bates, begin_attachment, end_attachment,
              rel_path, file_name, file_type, file_size, page_count, author, date_created,
              content_type, date_modified, title, subject, participants, recipients, manual_field_locks_json, file_hash,
              content_hash, text_status, lifecycle_status, ingested_at, last_seen_at, updated_at,
              control_number_batch, control_number_family_sequence, control_number_attachment_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                common_values["control_number"],
                common_values["canonical_kind"],
                common_values["canonical_status"],
                None,
                common_values["conversation_id"],
                common_values["conversation_assignment_mode"],
                common_values["dataset_id"],
                common_values["parent_document_id"],
                common_values["child_document_kind"],
                common_values["source_kind"],
                common_values["source_rel_path"],
                common_values["source_item_id"],
                common_values["root_message_key"],
                common_values["source_folder_path"],
                common_values["production_id"],
                common_values["begin_bates"],
                common_values["end_bates"],
                common_values["begin_attachment"],
                common_values["end_attachment"],
                common_values["rel_path"],
                common_values["file_name"],
                common_values["file_type"],
                common_values["file_size"],
                common_values["page_count"],
                common_values["author"],
                common_values["date_created"],
                common_values["content_type"],
                common_values["date_modified"],
                common_values["title"],
                common_values["subject"],
                common_values["participants"],
                common_values["recipients"],
                "[]",
                common_values["file_hash"],
                common_values["content_hash"],
                common_values["text_status"],
                common_values["lifecycle_status"],
                common_values["ingested_at"],
                common_values["last_seen_at"],
                common_values["updated_at"],
                common_values["control_number_batch"],
                common_values["control_number_family_sequence"],
                common_values["control_number_attachment_sequence"],
            ),
        )
        document_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    else:
        locked_value = existing_row[MANUAL_FIELD_LOCKS_COLUMN] or "[]"
        connection.execute(
            """
            UPDATE documents
            SET control_number = ?, canonical_kind = ?, canonical_status = ?, merged_into_document_id = NULL,
                conversation_id = ?, conversation_assignment_mode = ?, dataset_id = ?, parent_document_id = ?, child_document_kind = ?,
                source_kind = ?, source_rel_path = ?, source_item_id = ?, root_message_key = ?, source_folder_path = ?,
                production_id = ?, begin_bates = ?, end_bates = ?, begin_attachment = ?, end_attachment = ?,
                rel_path = ?, file_name = ?, file_type = ?, file_size = ?, page_count = ?,
                author = ?, content_type = ?, date_created = ?, date_modified = ?, title = ?, subject = ?,
                participants = ?, recipients = ?, file_hash = ?, content_hash = ?, text_status = ?, lifecycle_status = ?,
                ingested_at = ?, last_seen_at = ?, updated_at = ?, manual_field_locks_json = ?,
                control_number_batch = ?, control_number_family_sequence = ?, control_number_attachment_sequence = ?
            WHERE id = ?
            """,
            (
                common_values["control_number"],
                common_values["canonical_kind"],
                common_values["canonical_status"],
                common_values["conversation_id"],
                common_values["conversation_assignment_mode"],
                common_values["dataset_id"],
                common_values["parent_document_id"],
                common_values["child_document_kind"],
                common_values["source_kind"],
                common_values["source_rel_path"],
                common_values["source_item_id"],
                common_values["root_message_key"],
                common_values["source_folder_path"],
                common_values["production_id"],
                common_values["begin_bates"],
                common_values["end_bates"],
                common_values["begin_attachment"],
                common_values["end_attachment"],
                common_values["rel_path"],
                common_values["file_name"],
                common_values["file_type"],
                common_values["file_size"],
                common_values["page_count"],
                common_values["author"],
                common_values["content_type"],
                common_values["date_created"],
                common_values["date_modified"],
                common_values["title"],
                common_values["subject"],
                common_values["participants"],
                common_values["recipients"],
                common_values["file_hash"],
                common_values["content_hash"],
                common_values["text_status"],
                common_values["lifecycle_status"],
                common_values["ingested_at"],
                common_values["last_seen_at"],
                common_values["updated_at"],
                locked_value,
                common_values["control_number_batch"],
                common_values["control_number_family_sequence"],
                common_values["control_number_attachment_sequence"],
                existing_row["id"],
            ),
        )
        document_id = int(existing_row["id"])

    effective_existing_occurrence = existing_occurrence_row
    if effective_existing_occurrence is None and existing_row is not None:
        effective_existing_occurrence = find_active_occurrence_by_source_identity(
            connection,
            source_kind=effective_source_kind,
            custodian=custodian,
            source_rel_path=effective_source_rel_path,
            source_item_id=source_item_id,
        )
        if effective_existing_occurrence is None:
            effective_existing_occurrence = connection.execute(
                """
                SELECT *
                FROM document_occurrences
                WHERE document_id = ?
                  AND rel_path = ?
                  AND lifecycle_status = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (document_id, rel_path, ACTIVE_OCCURRENCE_STATUS),
            ).fetchone()

    parent_occurrence_id = None
    if parent_document_id is not None:
        parent_occurrence = select_preferred_occurrence(active_occurrence_rows_for_document(connection, int(parent_document_id)))
        if parent_occurrence is not None:
            parent_occurrence_id = int(parent_occurrence["id"])

    upsert_document_occurrence(
        connection,
        document_id=document_id,
        existing_occurrence_id=(int(effective_existing_occurrence["id"]) if effective_existing_occurrence is not None else None),
        parent_occurrence_id=parent_occurrence_id,
        occurrence_control_number=control_number,
        source_kind=effective_source_kind,
        source_rel_path=effective_source_rel_path,
        source_item_id=source_item_id,
        source_folder_path=source_folder_path,
        production_id=production_id,
        begin_bates=begin_bates,
        end_bates=end_bates,
        begin_attachment=begin_attachment,
        end_attachment=end_attachment,
        rel_path=rel_path,
        file_name=file_name,
        file_type=file_type,
        mime_type=None,
        file_size=file_size,
        file_hash=file_hash,
        custodian=custodian,
        fs_created_at=None,
        fs_modified_at=None,
        extracted=extracted,
        has_preview=bool(extracted.get("preview_artifacts")),
        text_status=str(common_values["text_status"]),
        ingested_at=str(common_values["ingested_at"]),
        last_seen_at=str(common_values["last_seen_at"]),
        updated_at=str(common_values["updated_at"]),
    )
    if parent_document_id is None and effective_source_kind in {FILESYSTEM_SOURCE_KIND, PST_SOURCE_KIND, MBOX_SOURCE_KIND}:
        bind_document_dedupe_key(
            connection,
            basis="file_hash",
            key_value=file_hash,
            document_id=document_id,
        )
    refresh_source_backed_dataset_memberships_for_document(connection, document_id)
    refresh_document_from_occurrences(connection, document_id)
    return document_id


def attach_occurrence_to_existing_document(
    connection: sqlite3.Connection,
    document_row: sqlite3.Row,
    *,
    existing_occurrence_row: sqlite3.Row | None,
    rel_path: str,
    file_name: str,
    file_type: str | None,
    file_size: int | None,
    file_hash: str | None,
    source_kind: str,
    source_rel_path: str,
    source_item_id: str | None,
    source_folder_path: str | None,
    custodian: str | None,
    production_id: int | None = None,
    begin_bates: str | None = None,
    end_bates: str | None = None,
    begin_attachment: str | None = None,
    end_attachment: str | None = None,
    parent_document_id: int | None = None,
    parent_occurrence_id: int | None = None,
    occurrence_control_number: str | None = None,
    ingested_at: str | None = None,
    last_seen_at: str | None = None,
    updated_at: str | None = None,
) -> int:
    resolved_parent_occurrence_id = parent_occurrence_id
    if resolved_parent_occurrence_id is None and parent_document_id is not None:
        parent_occurrence = select_preferred_occurrence(active_occurrence_rows_for_document(connection, parent_document_id))
        if parent_occurrence is not None:
            resolved_parent_occurrence_id = int(parent_occurrence["id"])
    preview_exists = connection.execute(
        "SELECT 1 FROM document_previews WHERE document_id = ? LIMIT 1",
        (document_row["id"],),
    ).fetchone() is not None
    occurrence_id = upsert_document_occurrence(
        connection,
        document_id=int(document_row["id"]),
        existing_occurrence_id=(int(existing_occurrence_row["id"]) if existing_occurrence_row is not None else None),
        parent_occurrence_id=resolved_parent_occurrence_id,
        occurrence_control_number=occurrence_control_number or document_row["control_number"],
        source_kind=source_kind,
        source_rel_path=source_rel_path,
        source_item_id=source_item_id,
        source_folder_path=source_folder_path,
        production_id=production_id,
        begin_bates=begin_bates,
        end_bates=end_bates,
        begin_attachment=begin_attachment,
        end_attachment=end_attachment,
        rel_path=rel_path,
        file_name=file_name,
        file_type=file_type,
        mime_type=None,
        file_size=file_size,
        file_hash=file_hash,
        custodian=custodian,
        fs_created_at=None,
        fs_modified_at=None,
        extracted={
            "author": document_row["author"],
            "title": document_row["title"],
            "subject": document_row["subject"],
            "participants": document_row["participants"],
            "recipients": document_row["recipients"],
            "date_created": document_row["date_created"],
            "date_modified": document_row["date_modified"],
            "content_type": document_row["content_type"],
        },
        has_preview=preview_exists,
        text_status=str(document_row["text_status"] or "ok"),
        ingested_at=ingested_at or str(document_row["ingested_at"] or utc_now()),
        last_seen_at=last_seen_at or str(document_row["last_seen_at"] or document_row["ingested_at"] or utc_now()),
        updated_at=updated_at or str(document_row["updated_at"] or utc_now()),
    )
    if parent_document_id is None and source_kind in {FILESYSTEM_SOURCE_KIND, PST_SOURCE_KIND, MBOX_SOURCE_KIND}:
        bind_document_dedupe_key(
            connection,
            basis="file_hash",
            key_value=file_hash,
            document_id=int(document_row["id"]),
        )
    refresh_source_backed_dataset_memberships_for_document(connection, int(document_row["id"]))
    refresh_document_from_occurrences(connection, int(document_row["id"]))
    return occurrence_id


def clone_duplicate_family_child_occurrences(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    parent_document_id: int,
    parent_occurrence_id: int,
    parent_rel_path: str,
    custodian: str | None,
    ingested_at: str | None = None,
    last_seen_at: str | None = None,
    updated_at: str | None = None,
) -> int:
    child_rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE parent_document_id = ?
          AND lifecycle_status != 'deleted'
        ORDER BY
          CASE WHEN control_number_attachment_sequence IS NULL THEN 1 ELSE 0 END ASC,
          control_number_attachment_sequence ASC,
          id ASC
        """,
        (parent_document_id,),
    ).fetchall()
    cloned_count = 0
    for child_row in child_rows:
        child_document_id = int(child_row["id"])
        preferred_occurrence = select_preferred_occurrence(
            active_occurrence_rows_for_document(connection, child_document_id)
        )
        child_source_rel_path = (
            str(preferred_occurrence["rel_path"])
            if preferred_occurrence is not None and preferred_occurrence["rel_path"] is not None
            else str(child_row["rel_path"])
        )
        child_source_path = document_absolute_path(paths, child_source_rel_path)
        if not child_source_path.exists():
            raise RetrieverError(
                f"Could not clone duplicate family child {child_document_id}: missing source artifact {child_source_rel_path!r}."
            )
        cloned_rel_path, cloned_path = write_attachment_blob(
            paths,
            parent_rel_path,
            str(child_row["control_number"]),
            str(child_row["file_name"]),
            child_source_path.read_bytes(),
        )
        attach_occurrence_to_existing_document(
            connection,
            child_row,
            existing_occurrence_row=None,
            rel_path=cloned_rel_path,
            file_name=str(child_row["file_name"]),
            file_type=(
                str(preferred_occurrence["file_type"])
                if preferred_occurrence is not None and preferred_occurrence["file_type"] is not None
                else child_row["file_type"]
            ),
            file_size=(
                int(preferred_occurrence["file_size"])
                if preferred_occurrence is not None and preferred_occurrence["file_size"] is not None
                else cloned_path.stat().st_size
            ),
            file_hash=(
                str(preferred_occurrence["file_hash"])
                if preferred_occurrence is not None and preferred_occurrence["file_hash"] is not None
                else str(child_row["file_hash"])
                if child_row["file_hash"] is not None
                else sha256_file(cloned_path)
            ),
            source_kind=(
                str(preferred_occurrence["source_kind"])
                if preferred_occurrence is not None and preferred_occurrence["source_kind"] is not None
                else str(child_row["source_kind"])
            ),
            source_rel_path=cloned_rel_path,
            source_item_id=(
                str(preferred_occurrence["source_item_id"])
                if preferred_occurrence is not None and preferred_occurrence["source_item_id"] is not None
                else str(child_row["source_item_id"])
                if child_row["source_item_id"] is not None
                else None
            ),
            source_folder_path=(
                str(preferred_occurrence["source_folder_path"])
                if preferred_occurrence is not None and preferred_occurrence["source_folder_path"] is not None
                else str(child_row["source_folder_path"])
                if child_row["source_folder_path"] is not None
                else None
            ),
            custodian=custodian,
            parent_document_id=parent_document_id,
            parent_occurrence_id=parent_occurrence_id,
            occurrence_control_number=str(child_row["control_number"] or ""),
            ingested_at=ingested_at,
            last_seen_at=last_seen_at,
            updated_at=updated_at,
        )
        cloned_count += 1
    return cloned_count


def mark_seen_without_reingest(
    connection: sqlite3.Connection,
    occurrence_row: sqlite3.Row,
    *,
    dataset_id: int | None = None,
    dataset_source_id: int | None = None,
) -> None:
    now = utc_now()
    connection.execute(
        """
        UPDATE document_occurrences
        SET lifecycle_status = 'active', last_seen_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (now, now, occurrence_row["id"]),
    )
    connection.execute(
        """
        UPDATE document_occurrences
        SET lifecycle_status = 'active', last_seen_at = ?, updated_at = ?
        WHERE parent_occurrence_id = ? AND lifecycle_status != 'deleted'
        """,
        (now, now, occurrence_row["id"]),
    )
    if dataset_id is not None:
        ensure_dataset_document_membership(
            connection,
            dataset_id=dataset_id,
            document_id=int(occurrence_row["document_id"]),
            dataset_source_id=dataset_source_id,
        )
        child_rows = connection.execute(
            """
            SELECT DISTINCT document_id
            FROM document_occurrences
            WHERE parent_occurrence_id = ?
              AND lifecycle_status != 'deleted'
            ORDER BY document_id ASC
            """,
            (occurrence_row["id"],),
        ).fetchall()
        for child_row in child_rows:
            ensure_dataset_document_membership(
                connection,
                dataset_id=dataset_id,
                document_id=int(child_row["document_id"]),
                dataset_source_id=dataset_source_id,
            )
            refresh_source_backed_dataset_memberships_for_document(connection, int(child_row["document_id"]))
            refresh_document_from_occurrences(connection, int(child_row["document_id"]))
    refresh_source_backed_dataset_memberships_for_document(connection, int(occurrence_row["document_id"]))
    refresh_document_from_occurrences(connection, int(occurrence_row["document_id"]))


def replace_document_email_threading_row(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    email_threading: object,
) -> None:
    if not isinstance(email_threading, dict):
        connection.execute("DELETE FROM document_email_threading WHERE document_id = ?", (document_id,))
        return
    references = [
        value
        for value in normalize_string_list(email_threading.get("references"))
        if normalize_whitespace(value)
    ]
    normalized_payload = {
        "message_id": normalize_email_message_id(email_threading.get("message_id")),
        "in_reply_to": normalize_email_message_id(email_threading.get("in_reply_to")),
        "references_json": json.dumps(references),
        "conversation_index": normalize_whitespace(str(email_threading.get("conversation_index") or "")) or None,
        "conversation_topic": normalize_email_thread_subject(email_threading.get("conversation_topic")),
        "normalized_subject": normalize_email_thread_subject(email_threading.get("normalized_subject")),
        "updated_at": utc_now(),
    }
    if not any(
        (
            normalized_payload["message_id"],
            normalized_payload["in_reply_to"],
            references,
            normalized_payload["conversation_index"],
            normalized_payload["conversation_topic"],
            normalized_payload["normalized_subject"],
        )
    ):
        connection.execute("DELETE FROM document_email_threading WHERE document_id = ?", (document_id,))
        return
    connection.execute(
        """
        INSERT INTO document_email_threading (
          document_id, message_id, in_reply_to, references_json,
          conversation_index, conversation_topic, normalized_subject, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(document_id) DO UPDATE SET
          message_id = excluded.message_id,
          in_reply_to = excluded.in_reply_to,
          references_json = excluded.references_json,
          conversation_index = excluded.conversation_index,
          conversation_topic = excluded.conversation_topic,
          normalized_subject = excluded.normalized_subject,
          updated_at = excluded.updated_at
        """,
        (
            document_id,
            normalized_payload["message_id"],
            normalized_payload["in_reply_to"],
            normalized_payload["references_json"],
            normalized_payload["conversation_index"],
            normalized_payload["conversation_topic"],
            normalized_payload["normalized_subject"],
            normalized_payload["updated_at"],
        ),
    )


def replace_document_chat_threading_row(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    chat_threading: object,
) -> None:
    if not isinstance(chat_threading, dict):
        connection.execute("DELETE FROM document_chat_threading WHERE document_id = ?", (document_id,))
        return
    participant_names = sorted_unique_display_names(normalize_string_list(chat_threading.get("participants")))
    normalized_payload = {
        "thread_id": normalize_pst_chat_thread_id(chat_threading.get("thread_id")),
        "message_id": normalize_pst_identifier(chat_threading.get("message_id")),
        "parent_message_id": normalize_pst_identifier(chat_threading.get("parent_message_id")),
        "thread_type": normalize_whitespace(str(chat_threading.get("thread_type") or "")).lower() or None,
        "participants_json": json.dumps(participant_names),
        "updated_at": utc_now(),
    }
    if not any(
        (
            normalized_payload["thread_id"],
            normalized_payload["message_id"],
            normalized_payload["parent_message_id"],
            normalized_payload["thread_type"],
            participant_names,
        )
    ):
        connection.execute("DELETE FROM document_chat_threading WHERE document_id = ?", (document_id,))
        return
    connection.execute(
        """
        INSERT INTO document_chat_threading (
          document_id, thread_id, message_id, parent_message_id, thread_type, participants_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(document_id) DO UPDATE SET
          thread_id = excluded.thread_id,
          message_id = excluded.message_id,
          parent_message_id = excluded.parent_message_id,
          thread_type = excluded.thread_type,
          participants_json = excluded.participants_json,
          updated_at = excluded.updated_at
        """,
        (
            document_id,
            normalized_payload["thread_id"],
            normalized_payload["message_id"],
            normalized_payload["parent_message_id"],
            normalized_payload["thread_type"],
            normalized_payload["participants_json"],
            normalized_payload["updated_at"],
        ),
    )


def document_row_has_email_threading(connection: sqlite3.Connection, row: sqlite3.Row | None) -> bool:
    if row is None:
        return False
    file_type = normalize_whitespace(str(row["file_type"] or "")).lower() if "file_type" in row.keys() else ""
    if file_type not in {"eml", "msg"}:
        return True
    signal_row = connection.execute(
        """
        SELECT 1
        FROM document_email_threading
        WHERE document_id = ?
        LIMIT 1
        """,
        (int(row["id"]),),
    ).fetchone()
    return signal_row is not None


def container_email_documents_missing_threading(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM documents d
        LEFT JOIN document_email_threading det ON det.document_id = d.id
        LEFT JOIN document_chat_threading dct ON dct.document_id = d.id
        WHERE d.source_kind = ?
          AND d.source_rel_path = ?
          AND d.parent_document_id IS NULL
          AND d.lifecycle_status != 'deleted'
          AND (
            (d.content_type = 'Email' AND det.document_id IS NULL)
            OR (d.content_type = 'Chat' AND dct.document_id IS NULL)
          )
        LIMIT 1
        """,
        (source_kind, source_rel_path),
    ).fetchone()
    return row is not None


def list_email_conversation_documents(connection: sqlite3.Connection) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT
          d.id,
          d.control_number,
          d.conversation_id,
          d.conversation_assignment_mode,
          d.source_kind,
          d.source_rel_path,
          d.source_folder_path,
          d.file_type,
          d.author,
          d.recipients,
          d.subject,
          d.title,
          d.date_created,
          d.custodians_json,
          det.message_id,
          det.in_reply_to,
          det.references_json,
          det.conversation_index,
          det.conversation_topic,
          det.normalized_subject
        FROM documents d
        LEFT JOIN document_email_threading det ON det.document_id = d.id
        WHERE d.parent_document_id IS NULL
          AND d.content_type = 'Email'
          AND d.lifecycle_status NOT IN ('missing', 'deleted')
        ORDER BY
          CASE WHEN d.date_created IS NULL OR TRIM(d.date_created) = '' THEN 1 ELSE 0 END ASC,
          d.date_created ASC,
          d.id ASC
        """
    ).fetchall()
    documents: list[dict[str, object]] = []
    for row in rows:
        references = normalize_string_list(row["references_json"])
        normalized_subject = normalize_email_thread_subject(
            row["normalized_subject"] or row["subject"] or row["conversation_topic"]
        )
        custodians = parse_document_custodians_json(row["custodians_json"])
        documents.append(
            {
                "id": int(row["id"]),
                "control_number": row["control_number"],
                "existing_conversation_id": int(row["conversation_id"]) if row["conversation_id"] is not None else None,
                "assignment_mode": effective_conversation_assignment_mode(row["conversation_assignment_mode"]),
                "source_kind": normalize_whitespace(str(row["source_kind"] or "")).lower(),
                "source_rel_path": normalize_whitespace(str(row["source_rel_path"] or "")) or None,
                "source_folder_path": normalize_whitespace(str(row["source_folder_path"] or "")) or None,
                "file_type": normalize_whitespace(str(row["file_type"] or "")).lower() or None,
                "author": normalize_whitespace(str(row["author"] or "")) or None,
                "recipients": normalize_whitespace(str(row["recipients"] or "")) or None,
                "subject": normalize_whitespace(str(row["subject"] or "")) or None,
                "title": normalize_whitespace(str(row["title"] or "")) or None,
                "date_created": normalize_datetime(row["date_created"]),
                "custodians": custodians,
                "message_id": normalize_email_message_id(row["message_id"]),
                "in_reply_to": normalize_email_message_id(row["in_reply_to"]),
                "references": references,
                "conversation_index_root": normalize_email_conversation_index_root(row["conversation_index"]),
                "conversation_topic": normalize_email_thread_subject(row["conversation_topic"]),
                "normalized_subject": normalized_subject,
                "participant_keys": email_participant_keys(row["author"], row["recipients"]),
                "heuristic_scope": email_heuristic_scope_key(row["source_kind"], row["source_rel_path"]),
            }
        )
    return documents


def create_email_conversation_cluster(*, manual_conversation_id: int | None = None) -> dict[str, object]:
    return {
        "documents": [],
        "manual_conversation_id": manual_conversation_id,
        "message_ids": set(),
        "conversation_index_roots": set(),
        "conversation_topics": set(),
        "normalized_subjects": set(),
        "participant_keys": set(),
        "custodians": set(),
        "heuristic_scopes": set(),
        "latest_date": None,
    }


def add_document_to_email_cluster(
    cluster: dict[str, object],
    document: dict[str, object],
    *,
    cluster_id: int,
    message_id_index: dict[str, set[int]],
    conversation_index_root_index: dict[str, set[int]],
    conversation_topic_index: dict[str, set[int]],
    heuristic_subject_index: dict[tuple[str, str], set[int]],
) -> None:
    cluster["documents"].append(document)
    message_id = document.get("message_id")
    if message_id:
        cluster["message_ids"].add(message_id)
        message_id_index.setdefault(str(message_id), set()).add(cluster_id)
    conversation_index_root = document.get("conversation_index_root")
    if conversation_index_root:
        cluster["conversation_index_roots"].add(conversation_index_root)
        conversation_index_root_index.setdefault(str(conversation_index_root), set()).add(cluster_id)
    conversation_topic = document.get("conversation_topic")
    if conversation_topic:
        cluster["conversation_topics"].add(conversation_topic)
        conversation_topic_index.setdefault(str(conversation_topic), set()).add(cluster_id)
    normalized_subject = document.get("normalized_subject")
    heuristic_scope = document.get("heuristic_scope")
    if normalized_subject:
        cluster["normalized_subjects"].add(normalized_subject)
    if heuristic_scope:
        cluster["heuristic_scopes"].add(heuristic_scope)
    if normalized_subject and heuristic_scope:
        heuristic_subject_index.setdefault((str(heuristic_scope), str(normalized_subject)), set()).add(cluster_id)
    participant_keys = set(document.get("participant_keys") or [])
    cluster["participant_keys"].update(participant_keys)
    cluster["custodians"].update(normalize_custodian_values(set(document.get("custodians") or [])))
    document_date = normalize_datetime(document.get("date_created"))
    if document_date and (cluster["latest_date"] is None or str(cluster["latest_date"]) < document_date):
        cluster["latest_date"] = document_date


def choose_unique_cluster(cluster_ids: set[int]) -> int | None:
    if len(cluster_ids) == 1:
        return next(iter(cluster_ids))
    return None


def choose_reference_cluster(document: dict[str, object], message_id_index: dict[str, set[int]]) -> int | None:
    references = list(document.get("references") or [])
    if not references:
        return None
    candidate_cluster_ids: set[int] = set()
    for reference_id in references:
        candidate_cluster_ids.update(message_id_index.get(str(reference_id), set()))
    if not candidate_cluster_ids:
        return None
    scores: dict[int, int] = {}
    for cluster_id in candidate_cluster_ids:
        suffix_length = 0
        for reference_id in reversed(references):
            if cluster_id in message_id_index.get(str(reference_id), set()):
                suffix_length += 1
            elif suffix_length > 0:
                break
        if suffix_length > 0:
            scores[cluster_id] = suffix_length
    if not scores:
        return None
    best_score = max(scores.values())
    best_cluster_ids = [cluster_id for cluster_id, score in scores.items() if score == best_score]
    if len(best_cluster_ids) != 1:
        return None
    return best_cluster_ids[0]


def choose_outlook_cluster(
    document: dict[str, object],
    conversation_index_root_index: dict[str, set[int]],
    conversation_topic_index: dict[str, set[int]],
) -> int | None:
    conversation_index_root = document.get("conversation_index_root")
    conversation_topic = document.get("conversation_topic")
    if not conversation_index_root:
        return None
    root_candidates = (
        set(conversation_index_root_index.get(str(conversation_index_root), set()))
        if conversation_index_root
        else set()
    )
    if not root_candidates:
        return None
    if conversation_topic:
        topic_candidates = set(conversation_topic_index.get(str(conversation_topic), set()))
        intersection = root_candidates & topic_candidates
        chosen = choose_unique_cluster(intersection)
        if chosen is not None:
            return chosen
    return choose_unique_cluster(root_candidates)


def email_document_has_strong_threading_signals(document: dict[str, object]) -> bool:
    if document.get("message_id"):
        return True
    if document.get("in_reply_to"):
        return True
    if list(document.get("references") or []):
        return True
    if document.get("conversation_index_root"):
        return True
    return False


def email_document_allows_heuristic_fallback(document: dict[str, object]) -> bool:
    source_kind = normalize_whitespace(str(document.get("source_kind") or "")).lower()
    return source_kind in {FILESYSTEM_SOURCE_KIND, PRODUCTION_SOURCE_KIND}


def email_document_can_use_heuristic_fallback(document: dict[str, object]) -> bool:
    return (
        email_document_allows_heuristic_fallback(document)
        and not email_document_has_strong_threading_signals(document)
    )


def choose_heuristic_cluster(
    document: dict[str, object],
    clusters: list[dict[str, object]],
    heuristic_subject_index: dict[tuple[str, str], set[int]],
) -> int | None:
    normalized_subject = document.get("normalized_subject")
    heuristic_scope = document.get("heuristic_scope")
    if not normalized_subject or not heuristic_scope:
        return None
    candidate_cluster_ids = heuristic_subject_index.get((str(heuristic_scope), str(normalized_subject)), set())
    if not candidate_cluster_ids:
        return None
    participant_keys = set(document.get("participant_keys") or [])
    document_date = normalize_datetime(document.get("date_created"))
    document_custodians = set(normalize_custodian_values(set(document.get("custodians") or [])))
    scored_candidates: list[tuple[int, str, int]] = []
    for cluster_id in candidate_cluster_ids:
        cluster = clusters[cluster_id]
        cluster_participants = set(cluster["participant_keys"])
        overlap = len(cluster_participants & participant_keys)
        if overlap <= 0:
            continue
        cluster_custodians = set(cluster["custodians"])
        if document_custodians and cluster_custodians and document_custodians.isdisjoint(cluster_custodians):
            continue
        latest_date = normalize_datetime(cluster["latest_date"])
        if document_date and latest_date and latest_date > document_date:
            continue
        scored_candidates.append((overlap, latest_date or "", cluster_id))
    if not scored_candidates:
        return None
    scored_candidates.sort(key=lambda item: (item[0], item[1], -item[2]), reverse=True)
    best_candidate = scored_candidates[0]
    if len(scored_candidates) > 1 and scored_candidates[1][:2] == best_candidate[:2]:
        return None
    return best_candidate[2]


def derive_email_conversation_key(cluster: dict[str, object]) -> str:
    message_ids = sorted(str(value) for value in cluster["message_ids"] if normalize_whitespace(str(value)))
    if message_ids:
        return f"rfc:{message_ids[0]}"
    conversation_index_roots = sorted(
        str(value)
        for value in cluster["conversation_index_roots"]
        if normalize_whitespace(str(value))
    )
    conversation_topics = sorted(
        str(value)
        for value in cluster["conversation_topics"]
        if normalize_whitespace(str(value))
    )
    if conversation_index_roots and conversation_topics:
        return f"outlook:{conversation_topics[0]}:{conversation_index_roots[0]}"
    if conversation_index_roots:
        return f"outlook_index:{conversation_index_roots[0]}"
    normalized_subjects = sorted(
        str(value)
        for value in cluster["normalized_subjects"]
        if normalize_whitespace(str(value))
    )
    heuristic_scopes = sorted(
        str(value)
        for value in cluster["heuristic_scopes"]
        if normalize_whitespace(str(value))
    )
    heuristic_signature = sha256_json_value(
        {
            "scope": heuristic_scopes[0] if heuristic_scopes else filesystem_dataset_locator(),
            "subject": normalized_subjects[0] if normalized_subjects else "",
            "participants": sorted(str(value) for value in cluster["participant_keys"]),
        }
    )
    return f"heuristic:{heuristic_signature[:24]}"


def derive_email_conversation_display_name(cluster: dict[str, object]) -> str:
    for document in list(cluster["documents"]):
        subject = normalize_email_thread_subject(document.get("subject"), preserve_case=True)
        if subject:
            return subject
        title = normalize_email_thread_subject(document.get("title"), preserve_case=True)
        if title:
            return title
    for conversation_topic in sorted(
        str(value)
        for value in cluster["conversation_topics"]
        if normalize_whitespace(str(value))
    ):
        display_name = normalize_email_thread_subject(conversation_topic, preserve_case=True)
        if display_name:
            return display_name
    return "Email conversation"


def sync_child_document_conversations(
    connection: sqlite3.Connection,
    *,
    parent_ids: set[int] | None = None,
    parent_content_types: set[str] | None = None,
    parent_source_kinds: set[str] | None = None,
) -> int:
    clauses = [
        "parent.lifecycle_status NOT IN ('missing', 'deleted')",
        "child.lifecycle_status NOT IN ('missing', 'deleted')",
    ]
    parameters: list[object] = []

    normalized_parent_ids = {int(value) for value in (parent_ids or set())}
    normalized_parent_content_types = sorted(
        normalize_whitespace(str(value or "")) for value in (parent_content_types or set()) if normalize_whitespace(str(value or ""))
    )
    normalized_parent_source_kinds = sorted(
        normalize_whitespace(str(value or "")).lower()
        for value in (parent_source_kinds or set())
        if normalize_whitespace(str(value or ""))
    )

    if normalized_parent_ids:
        placeholders = ", ".join("?" for _ in normalized_parent_ids)
        clauses.append(f"parent.id IN ({placeholders})")
        parameters.extend(sorted(normalized_parent_ids))
    if normalized_parent_content_types:
        placeholders = ", ".join("?" for _ in normalized_parent_content_types)
        clauses.append(f"parent.content_type IN ({placeholders})")
        parameters.extend(normalized_parent_content_types)
    if normalized_parent_source_kinds:
        placeholders = ", ".join("?" for _ in normalized_parent_source_kinds)
        clauses.append(f"parent.source_kind IN ({placeholders})")
        parameters.extend(normalized_parent_source_kinds)

    child_rows = connection.execute(
        f"""
        SELECT
          child.id,
          child.conversation_id AS child_conversation_id,
          child.conversation_assignment_mode AS child_conversation_assignment_mode,
          parent.conversation_id AS parent_conversation_id,
          parent.conversation_assignment_mode AS parent_conversation_assignment_mode
        FROM documents child
        JOIN documents parent ON parent.id = child.parent_document_id
        WHERE {' AND '.join(clauses)}
        ORDER BY child.id ASC
        """,
        tuple(parameters),
    ).fetchall()
    updated = 0
    now = utc_now()
    for row in child_rows:
        parent_conversation_id = int(row["parent_conversation_id"]) if row["parent_conversation_id"] is not None else None
        parent_mode = effective_conversation_assignment_mode(row["parent_conversation_assignment_mode"])
        child_conversation_id = int(row["child_conversation_id"]) if row["child_conversation_id"] is not None else None
        child_mode = effective_conversation_assignment_mode(row["child_conversation_assignment_mode"])
        if child_conversation_id == parent_conversation_id and child_mode == parent_mode:
            continue
        connection.execute(
            """
            UPDATE documents
            SET conversation_id = ?, conversation_assignment_mode = ?, updated_at = ?
            WHERE id = ?
            """,
            (parent_conversation_id, parent_mode, now, int(row["id"])),
        )
        updated += 1
    return updated


def assign_email_conversations(connection: sqlite3.Connection) -> dict[str, int]:
    documents = list_email_conversation_documents(connection)
    if not documents:
        return {
            "email_conversations": 0,
            "email_documents_reassigned": 0,
            "email_child_documents_updated": 0,
        }

    clusters: list[dict[str, object]] = []
    message_id_index: dict[str, set[int]] = {}
    conversation_index_root_index: dict[str, set[int]] = {}
    conversation_topic_index: dict[str, set[int]] = {}
    heuristic_subject_index: dict[tuple[str, str], set[int]] = {}

    manual_cluster_ids_by_conversation_id: dict[int, int] = {}
    for document in documents:
        if document["assignment_mode"] != CONVERSATION_ASSIGNMENT_MODE_MANUAL:
            continue
        existing_conversation_id = document.get("existing_conversation_id")
        if existing_conversation_id is None:
            continue
        cluster_id = manual_cluster_ids_by_conversation_id.get(int(existing_conversation_id))
        if cluster_id is None:
            cluster_id = len(clusters)
            manual_cluster_ids_by_conversation_id[int(existing_conversation_id)] = cluster_id
            clusters.append(create_email_conversation_cluster(manual_conversation_id=int(existing_conversation_id)))
        add_document_to_email_cluster(
            clusters[cluster_id],
            document,
            cluster_id=cluster_id,
            message_id_index=message_id_index,
            conversation_index_root_index=conversation_index_root_index,
            conversation_topic_index=conversation_topic_index,
            heuristic_subject_index=heuristic_subject_index,
        )

    auto_documents = [document for document in documents if document["assignment_mode"] != CONVERSATION_ASSIGNMENT_MODE_MANUAL]
    for document in auto_documents:
        cluster_id = None
        if document.get("message_id"):
            cluster_id = choose_unique_cluster(message_id_index.get(str(document["message_id"]), set()))
        if cluster_id is None and document.get("in_reply_to"):
            cluster_id = choose_unique_cluster(message_id_index.get(str(document["in_reply_to"]), set()))
        if cluster_id is None:
            cluster_id = choose_reference_cluster(document, message_id_index)
        if cluster_id is None:
            cluster_id = choose_outlook_cluster(document, conversation_index_root_index, conversation_topic_index)
        if cluster_id is None and email_document_can_use_heuristic_fallback(document):
            cluster_id = choose_heuristic_cluster(document, clusters, heuristic_subject_index)
        if cluster_id is None:
            cluster_id = len(clusters)
            clusters.append(create_email_conversation_cluster())
        add_document_to_email_cluster(
            clusters[cluster_id],
            document,
            cluster_id=cluster_id,
            message_id_index=message_id_index,
            conversation_index_root_index=conversation_index_root_index,
            conversation_topic_index=conversation_topic_index,
            heuristic_subject_index=heuristic_subject_index,
        )

    conversation_id_by_document_id: dict[int, int] = {}
    unique_conversation_ids: set[int] = set()
    for cluster in clusters:
        manual_conversation_id = cluster["manual_conversation_id"]
        if manual_conversation_id is not None:
            conversation_id = int(manual_conversation_id)
        else:
            conversation_id = upsert_conversation_row(
                connection,
                source_kind=EMAIL_CONVERSATION_SOURCE_KIND,
                source_locator=filesystem_dataset_locator(),
                conversation_key=derive_email_conversation_key(cluster),
                conversation_type="email",
                display_name=derive_email_conversation_display_name(cluster),
            )
        unique_conversation_ids.add(conversation_id)
        for document in list(cluster["documents"]):
            conversation_id_by_document_id[int(document["id"])] = conversation_id

    documents_reassigned = 0
    now = utc_now()
    for document in auto_documents:
        target_conversation_id = conversation_id_by_document_id.get(int(document["id"]))
        if target_conversation_id is None:
            continue
        if (
            document.get("existing_conversation_id") == target_conversation_id
            and document.get("assignment_mode") == CONVERSATION_ASSIGNMENT_MODE_AUTO
        ):
            continue
        connection.execute(
            """
            UPDATE documents
            SET conversation_id = ?, conversation_assignment_mode = ?, updated_at = ?
            WHERE id = ?
              AND COALESCE(conversation_assignment_mode, ?) != ?
            """,
            (
                target_conversation_id,
                CONVERSATION_ASSIGNMENT_MODE_AUTO,
                now,
                int(document["id"]),
                CONVERSATION_ASSIGNMENT_MODE_AUTO,
                CONVERSATION_ASSIGNMENT_MODE_MANUAL,
            ),
        )
        documents_reassigned += 1

    child_documents_updated = sync_child_document_conversations(connection, parent_content_types={"Email"})

    return {
        "email_conversations": len(unique_conversation_ids),
        "email_documents_reassigned": documents_reassigned,
        "email_child_documents_updated": child_documents_updated,
    }


def list_pst_chat_conversation_documents(connection: sqlite3.Connection) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT
          d.id,
          d.control_number,
          d.conversation_id,
          d.conversation_assignment_mode,
          d.manual_field_locks_json,
          d.source_kind,
          d.source_rel_path,
          d.source_item_id,
          d.source_folder_path,
          d.file_type,
          d.title,
          d.participants,
          d.date_created,
          d.custodians_json,
          dct.thread_id,
          dct.thread_type,
          dct.participants_json
        FROM documents d
        LEFT JOIN document_chat_threading dct ON dct.document_id = d.id
        WHERE d.parent_document_id IS NULL
          AND d.source_kind = ?
          AND d.content_type = 'Chat'
          AND d.lifecycle_status NOT IN ('missing', 'deleted')
        ORDER BY
          CASE WHEN d.date_created IS NULL OR TRIM(d.date_created) = '' THEN 1 ELSE 0 END ASC,
          d.date_created ASC,
          d.id ASC
        """,
        (PST_SOURCE_KIND,),
    ).fetchall()
    documents: list[dict[str, object]] = []
    for row in rows:
        participant_names = sorted_unique_display_names(
            [
                *normalize_string_list(row["participants_json"]),
                *[
                    normalize_whitespace(part)
                    for part in str(row["participants"] or "").split(",")
                    if normalize_whitespace(part)
                ],
            ]
        )
        documents.append(
            {
                "id": int(row["id"]),
                "control_number": row["control_number"],
                "existing_conversation_id": int(row["conversation_id"]) if row["conversation_id"] is not None else None,
                "assignment_mode": effective_conversation_assignment_mode(row["conversation_assignment_mode"]),
                "locked_fields": set(normalize_string_list(row[MANUAL_FIELD_LOCKS_COLUMN])),
                "source_kind": normalize_whitespace(str(row["source_kind"] or "")).lower(),
                "source_rel_path": normalize_whitespace(str(row["source_rel_path"] or "")) or None,
                "source_item_id": normalize_whitespace(str(row["source_item_id"] or "")) or None,
                "source_folder_path": normalize_whitespace(str(row["source_folder_path"] or "")) or None,
                "file_type": normalize_whitespace(str(row["file_type"] or "")).lower() or None,
                "title": normalize_whitespace(str(row["title"] or "")) or None,
                "participants": normalize_whitespace(str(row["participants"] or "")) or None,
                "participant_names": participant_names,
                "date_created": normalize_datetime(row["date_created"]),
                "custodians": parse_document_custodians_json(row["custodians_json"]),
                "thread_id": normalize_pst_chat_thread_id(row["thread_id"]),
                "thread_type": normalize_whitespace(str(row["thread_type"] or "")).lower() or None,
            }
        )
    return documents


def create_pst_chat_conversation_cluster(*, manual_conversation_id: int | None = None) -> dict[str, object]:
    return {
        "documents": [],
        "manual_conversation_id": manual_conversation_id,
        "source_rel_paths": set(),
        "source_folder_paths": set(),
        "thread_ids": set(),
        "thread_types": set(),
        "participant_names": set(),
        "titles": set(),
        "participants": set(),
    }


def add_document_to_pst_chat_cluster(cluster: dict[str, object], document: dict[str, object]) -> None:
    cluster["documents"].append(document)
    source_rel_path = document.get("source_rel_path")
    if source_rel_path:
        cluster["source_rel_paths"].add(source_rel_path)
    source_folder_path = document.get("source_folder_path")
    if source_folder_path:
        cluster["source_folder_paths"].add(source_folder_path)
    thread_id = document.get("thread_id")
    if thread_id:
        cluster["thread_ids"].add(thread_id)
    thread_type = document.get("thread_type")
    if thread_type:
        cluster["thread_types"].add(thread_type)
    for participant_name in list(document.get("participant_names") or []):
        if normalize_whitespace(str(participant_name)):
            cluster["participant_names"].add(str(participant_name))
    title = document.get("title")
    if title:
        cluster["titles"].add(title)
    participants = document.get("participants")
    if participants:
        cluster["participants"].add(participants)


def pst_chat_cluster_scope_key(document: dict[str, object]) -> tuple[str, str]:
    source_rel_path = normalize_whitespace(str(document.get("source_rel_path") or ""))
    thread_id = normalize_pst_chat_thread_id(document.get("thread_id"))
    if thread_id:
        return (source_rel_path, f"thread:{thread_id.lower()}")
    source_folder_path = normalize_whitespace(str(document.get("source_folder_path") or ""))
    if source_folder_path:
        return (source_rel_path, f"folder:{source_folder_path.lower()}")
    source_item_id = normalize_whitespace(str(document.get("source_item_id") or ""))
    if source_item_id:
        return (source_rel_path, f"item:{source_item_id.lower()}")
    return (source_rel_path, f"doc:{int(document['id'])}")


def derive_pst_chat_conversation_key(cluster: dict[str, object]) -> str:
    thread_ids = sorted(
        normalize_pst_chat_thread_id(value).lower()
        for value in cluster["thread_ids"]
        if normalize_pst_chat_thread_id(value)
    )
    if thread_ids:
        return f"thread:{thread_ids[0]}"
    source_folder_paths = sorted(
        str(value).lower()
        for value in cluster["source_folder_paths"]
        if normalize_whitespace(str(value))
    )
    if source_folder_paths:
        return f"folder:{source_folder_paths[0]}"
    source_item_ids = sorted(
        normalize_whitespace(str(document.get("source_item_id") or "")).lower()
        for document in list(cluster["documents"])
        if normalize_whitespace(str(document.get("source_item_id") or ""))
    )
    if source_item_ids:
        return f"item:{source_item_ids[0]}"
    return f"doc:{int(cluster['documents'][0]['id'])}"


def derive_pst_chat_conversation_display_name(cluster: dict[str, object]) -> str:
    generic_folder_names = {"conversation history", "teamsmessagesdata", "teamsmeetings"}
    participant_summary = render_display_name_title(list(cluster["participant_names"]), max_names=4)
    if "chat" in cluster["thread_types"] and participant_summary:
        return participant_summary
    source_folder_paths = sorted(
        str(value)
        for value in cluster["source_folder_paths"]
        if normalize_whitespace(str(value))
    )
    for folder_path in source_folder_paths:
        leaf_name = normalize_whitespace(str(folder_path).split("/")[-1])
        if leaf_name and leaf_name.lower() not in generic_folder_names:
            return leaf_name
    if participant_summary:
        return participant_summary
    for document in list(cluster["documents"]):
        title = normalize_whitespace(str(document.get("title") or ""))
        if title:
            return title
    for folder_path in source_folder_paths:
        leaf_name = normalize_whitespace(str(folder_path).split("/")[-1])
        if leaf_name:
            return leaf_name
    for participants in sorted(str(value) for value in cluster["participants"] if normalize_whitespace(str(value))):
        if participants:
            return participants
    return "Chat conversation"


def assign_pst_chat_conversations(connection: sqlite3.Connection) -> dict[str, int]:
    documents = list_pst_chat_conversation_documents(connection)
    if not documents:
        return {
            "pst_chat_conversations": 0,
            "pst_chat_documents_reassigned": 0,
            "pst_chat_child_documents_updated": 0,
        }

    clusters: list[dict[str, object]] = []
    manual_cluster_ids_by_conversation_id: dict[int, int] = {}
    auto_cluster_ids_by_scope: dict[tuple[str, str], int] = {}

    for document in documents:
        if document["assignment_mode"] != CONVERSATION_ASSIGNMENT_MODE_MANUAL:
            continue
        existing_conversation_id = document.get("existing_conversation_id")
        if existing_conversation_id is None:
            continue
        cluster_id = manual_cluster_ids_by_conversation_id.get(int(existing_conversation_id))
        if cluster_id is None:
            cluster_id = len(clusters)
            manual_cluster_ids_by_conversation_id[int(existing_conversation_id)] = cluster_id
            clusters.append(create_pst_chat_conversation_cluster(manual_conversation_id=int(existing_conversation_id)))
        add_document_to_pst_chat_cluster(clusters[cluster_id], document)

    auto_documents = [document for document in documents if document["assignment_mode"] != CONVERSATION_ASSIGNMENT_MODE_MANUAL]
    for document in auto_documents:
        scope_key = pst_chat_cluster_scope_key(document)
        cluster_id = auto_cluster_ids_by_scope.get(scope_key)
        if cluster_id is None:
            cluster_id = len(clusters)
            auto_cluster_ids_by_scope[scope_key] = cluster_id
            clusters.append(create_pst_chat_conversation_cluster())
        add_document_to_pst_chat_cluster(clusters[cluster_id], document)

    conversation_id_by_document_id: dict[int, int] = {}
    unique_conversation_ids: set[int] = set()
    for cluster in clusters:
        manual_conversation_id = cluster["manual_conversation_id"]
        if manual_conversation_id is not None:
            conversation_id = int(manual_conversation_id)
        else:
            source_locator = next(
                (
                    str(value)
                    for value in sorted(cluster["source_rel_paths"])
                    if normalize_whitespace(str(value))
                ),
                None,
            ) or filesystem_dataset_locator()
            conversation_id = upsert_conversation_row(
                connection,
                source_kind=PST_SOURCE_KIND,
                source_locator=source_locator,
                conversation_key=derive_pst_chat_conversation_key(cluster),
                conversation_type="chat",
                display_name=derive_pst_chat_conversation_display_name(cluster),
            )
        unique_conversation_ids.add(conversation_id)
        for document in list(cluster["documents"]):
            conversation_id_by_document_id[int(document["id"])] = conversation_id

    documents_reassigned = 0
    now = utc_now()
    for document in auto_documents:
        target_conversation_id = conversation_id_by_document_id.get(int(document["id"]))
        if target_conversation_id is None:
            continue
        if (
            document.get("existing_conversation_id") == target_conversation_id
            and document.get("assignment_mode") == CONVERSATION_ASSIGNMENT_MODE_AUTO
        ):
            continue
        connection.execute(
            """
            UPDATE documents
            SET conversation_id = ?, conversation_assignment_mode = ?, updated_at = ?
            WHERE id = ?
              AND COALESCE(conversation_assignment_mode, ?) != ?
            """,
            (
                target_conversation_id,
                CONVERSATION_ASSIGNMENT_MODE_AUTO,
                now,
                int(document["id"]),
                CONVERSATION_ASSIGNMENT_MODE_AUTO,
                CONVERSATION_ASSIGNMENT_MODE_MANUAL,
            ),
        )
        documents_reassigned += 1

    child_documents_updated = sync_child_document_conversations(
        connection,
        parent_content_types={"Chat"},
        parent_source_kinds={PST_SOURCE_KIND},
    )
    return {
        "pst_chat_conversations": len(unique_conversation_ids),
        "pst_chat_documents_reassigned": documents_reassigned,
        "pst_chat_child_documents_updated": child_documents_updated,
    }


def clear_unsupported_conversation_assignments(connection: sqlite3.Connection) -> int:
    cursor = connection.execute(
        """
        UPDATE documents
        SET conversation_id = NULL,
            conversation_assignment_mode = ?,
            updated_at = ?
        WHERE parent_document_id IS NULL
          AND conversation_id IS NOT NULL
          AND lifecycle_status NOT IN ('missing', 'deleted')
          AND LOWER(TRIM(COALESCE(content_type, ''))) NOT IN ('email', 'chat')
        """,
        (CONVERSATION_ASSIGNMENT_MODE_AUTO, utc_now()),
    )
    return int(cursor.rowcount or 0)


def assign_supported_conversations(connection: sqlite3.Connection) -> dict[str, int]:
    clear_unsupported_conversation_assignments(connection)
    email_assignment = assign_email_conversations(connection)
    pst_chat_assignment = assign_pst_chat_conversations(connection)
    return {
        **email_assignment,
        **pst_chat_assignment,
    }


def list_active_conversation_ids(connection: sqlite3.Connection) -> list[int]:
    rows = connection.execute(
        """
        SELECT DISTINCT conversation_id
        FROM documents
        WHERE conversation_id IS NOT NULL
          AND lifecycle_status NOT IN ('missing', 'deleted')
          AND COALESCE(child_document_kind, '') != ?
        ORDER BY conversation_id ASC
        """,
        (CHILD_DOCUMENT_KIND_ATTACHMENT,),
    ).fetchall()
    return [int(row["conversation_id"]) for row in rows if row["conversation_id"] is not None]


def conversation_preview_date_hint(value: object) -> str | None:
    candidate = Path(str(value or "")).stem
    if not candidate:
        return None
    try:
        return f"{date.fromisoformat(candidate).isoformat()}T00:00:00Z"
    except ValueError:
        return None


def conversation_preview_primary_timestamp(document: dict[str, object]) -> str | None:
    return (
        normalize_datetime(document.get("date_created"))
        or normalize_datetime(document.get("date_modified"))
        or conversation_preview_date_hint(document.get("source_rel_path"))
        or conversation_preview_date_hint(document.get("rel_path"))
    )


def conversation_preview_sort_key(document: dict[str, object]) -> tuple[str, int, str, int]:
    return (
        conversation_preview_primary_timestamp(document) or "9999-12-31T23:59:59Z",
        0 if document.get("parent_document_id") is None else 1,
        normalize_whitespace(str(document.get("control_number") or "")),
        int(document["id"]),
    )


def conversation_preview_segment_mode(conversation_type: object, documents: list[dict[str, object]]) -> str:
    if normalize_whitespace(str(conversation_type or "")).lower() != "email":
        return "monthly"
    total_chars = sum(len(str(document.get("text_content") or "")) for document in documents)
    if total_chars > CONVERSATION_PREVIEW_MAX_CHARS:
        return "yearly"
    return "single"


def conversation_preview_segment_key(document: dict[str, object], *, segment_mode: str) -> str:
    if segment_mode == "single":
        return "all"
    timestamp = conversation_preview_primary_timestamp(document)
    if not timestamp:
        return "undated"
    if segment_mode == "yearly":
        return timestamp[:4]
    return timestamp[:7]


def conversation_preview_segment_label(segment_key: str, *, segment_mode: str) -> str:
    if segment_key == "all":
        return "All messages"
    if segment_key == "undated":
        return "Undated"
    if segment_mode == "yearly":
        return segment_key
    try:
        return date.fromisoformat(f"{segment_key}-01").strftime("%B %Y")
    except ValueError:
        return segment_key


def conversation_preview_writes_aggregate_artifacts(
    conversation_type: object,
    documents: list[dict[str, object]],
    *,
    segment_mode: str,
) -> bool:
    _ = (conversation_type, segment_mode)
    return len(documents) > 1


def conversation_preview_document_heading(document: dict[str, object]) -> str:
    for value in (
        document.get("title"),
        document.get("subject"),
        document.get("file_name"),
        document.get("control_number"),
    ):
        normalized = normalize_whitespace(str(value or ""))
        if normalized:
            return normalized
    return f"Document {int(document['id'])}"


def conversation_preview_document_kind(document: dict[str, object]) -> str:
    child_kind = normalize_whitespace(str(document.get("child_document_kind") or "")).lower()
    content_type = normalize_whitespace(str(document.get("content_type") or "Document"))
    if child_kind == CHILD_DOCUMENT_KIND_REPLY_THREAD:
        return "Reply thread"
    if content_type == "Email":
        return "Email message"
    if content_type == "Chat":
        return "Conversation document"
    return content_type or "Document"


def conversation_preview_participants(documents: list[dict[str, object]]) -> str | None:
    participants: list[str] = []
    seen: set[str] = set()
    for document in documents:
        append_unique_participants(
            participants,
            seen,
            [
                normalize_whitespace(str(document.get("participants") or "")) or None,
                normalize_whitespace(str(document.get("author") or "")) or None,
                normalize_whitespace(str(document.get("recipients") or "")) or None,
            ],
        )
    return ", ".join(participants) or None


def conversation_preview_bounds(documents: list[dict[str, object]]) -> tuple[str | None, str | None]:
    timestamps = sorted(
        timestamp
        for timestamp in (
            conversation_preview_primary_timestamp(document)
            for document in documents
        )
        if timestamp
    )
    if not timestamps:
        return None, None
    return timestamps[0], timestamps[-1]


EMAIL_PREVIEW_BODY_SOURCE_PATTERN = re.compile(
    r"<template\b[^>]*data-retriever-email-body-source\b[^>]*>(.*?)</template>",
    flags=re.IGNORECASE | re.DOTALL,
)
EMAIL_QUOTED_REPLY_SEPARATOR_PATTERN = re.compile(
    r"^(?:On .+ wrote:|Begin forwarded message:|-{2,}\s*Original Message\s*-{2,}|-{2,}\s*Forwarded message\s*-{2,})$",
    flags=re.IGNORECASE,
)
EMAIL_QUOTED_REPLY_HEADER_PATTERN = re.compile(
    r"^(?:From|Sent|To|Cc|Bcc|Subject|Date):\s+",
    flags=re.IGNORECASE,
)
EMAIL_HTML_QUOTED_REPLY_START_PATTERNS = (
    re.compile(
        r'(?is)<(?:div|section|table|blockquote)\b[^>]*class\s*=\s*(["\'])[^"\']*\bgmail_(?:quote|attr)\b[^"\']*\1'
    ),
    re.compile(r'(?is)<blockquote\b[^>]*type\s*=\s*(["\'])cite\1[^>]*>'),
    re.compile(
        r"(?is)<(?:div|p|span|font|td)\b[^>]*>\s*"
        r"(?:On .+ wrote:|Begin forwarded message:|-{2,}\s*(?:Original|Forwarded) Message\s*-{2,})"
    ),
    re.compile(r"(?is)<blockquote\b"),
)


class EmailPreviewBodyHTMLExtractor(HTMLParser):
    def __init__(self, *, require_selected_card: bool) -> None:
        super().__init__(convert_charrefs=False)
        self.require_selected_card = require_selected_card
        self.card_depth: int | None = None
        self.body_depth: int | None = None
        self.parts: list[str] = []
        self.extracted_html: str | None = None

    @staticmethod
    def classes(attrs: list[tuple[str, str | None]]) -> set[str]:
        class_value = next(
            (
                str(value or "")
                for key, value in attrs
                if normalize_whitespace(str(key or "")).lower() == "class"
            ),
            "",
        )
        return {token for token in normalize_whitespace(class_value).split(" ") if token}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.extracted_html is not None:
            return
        if self.body_depth is not None:
            self.parts.append(self.get_starttag_text())
            self.body_depth += 1
            return
        classes = self.classes(attrs)
        if self.card_depth is None:
            if tag == "article" and "gmail-message-card" in classes and (
                not self.require_selected_card or "gmail-message-card--selected" in classes
            ):
                self.card_depth = 1
            return
        self.card_depth += 1
        if tag == "div" and "gmail-message-body" in classes:
            self.body_depth = 0

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.body_depth is not None:
            self.parts.append(self.get_starttag_text())

    def handle_endtag(self, tag: str) -> None:
        if self.extracted_html is None and self.body_depth is not None:
            if self.body_depth == 0:
                extracted = "".join(self.parts).strip()
                self.extracted_html = extracted or None
                self.parts.clear()
                self.body_depth = None
            else:
                self.parts.append(f"</{tag}>")
                self.body_depth -= 1
        if self.card_depth is not None:
            self.card_depth -= 1
            if self.card_depth <= 0:
                self.card_depth = None

    def handle_data(self, data: str) -> None:
        if self.body_depth is not None:
            self.parts.append(data)

    def handle_entityref(self, name: str) -> None:
        if self.body_depth is not None:
            self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self.body_depth is not None:
            self.parts.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        if self.body_depth is not None:
            self.parts.append(f"<!--{data}-->")

    def handle_decl(self, decl: str) -> None:
        if self.body_depth is not None:
            self.parts.append(f"<!{decl}>")

    def handle_pi(self, data: str) -> None:
        if self.body_depth is not None:
            self.parts.append(f"<?{data}>")


def extract_visible_email_preview_body_html(preview_html: str) -> str | None:
    for require_selected_card in (True, False):
        parser = EmailPreviewBodyHTMLExtractor(require_selected_card=require_selected_card)
        parser.feed(preview_html)
        parser.close()
        if parser.extracted_html:
            return parser.extracted_html
    return None


def build_email_preview_head_html() -> str:
    return (
        "<style>"
        "body { margin: 0; background: #f6f8fc; color: #202124; font-family: Google Sans, Roboto, Arial, sans-serif; }"
        ".gmail-thread-page { max-width: 1120px; margin: 0 auto; padding: 24px 18px 44px; }"
        ".gmail-thread-header { margin-bottom: 1rem; }"
        ".gmail-thread-kicker { margin: 0 0 0.45rem; color: #5f6368; font-size: 0.76rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; }"
        ".gmail-thread-title { margin: 0; font-size: clamp(1.85rem, 3.2vw, 2.7rem); line-height: 1.08; font-weight: 500; color: #202124; }"
        ".gmail-thread-summary { display: flex; flex-wrap: wrap; gap: 0.55rem; margin-top: 0.95rem; }"
        ".gmail-thread-pill { display: inline-flex; align-items: center; padding: 0.38rem 0.72rem; border-radius: 999px; background: #ffffff; border: 1px solid #dde3eb; color: #5f6368; font-size: 0.92rem; line-height: 1.2; }"
        ".gmail-thread-pill--active { background: #e8f0fe; border-color: #aecbfa; color: #1a73e8; }"
        ".gmail-thread-messages { display: grid; gap: 1rem; }"
        ".gmail-message-card { display: flex; gap: 0.95rem; align-items: flex-start; padding: 1.08rem 1.15rem 1.15rem; background: #ffffff; border: 1px solid #e0e3e7; border-radius: 20px; box-shadow: 0 1px 2px rgba(60, 64, 67, 0.1); }"
        ".gmail-message-card--selected { border-color: #aecbfa; box-shadow: 0 10px 26px rgba(26, 115, 232, 0.12); }"
        ".gmail-message-main { min-width: 0; flex: 1 1 auto; }"
        ".gmail-message-header { display: flex; justify-content: space-between; gap: 1rem; align-items: flex-start; margin-bottom: 0.9rem; }"
        ".gmail-message-header-main { min-width: 0; }"
        ".gmail-message-author-line { display: flex; flex-wrap: wrap; gap: 0.35rem; align-items: baseline; }"
        ".gmail-message-author { font-size: 1.03rem; font-weight: 700; color: #202124; }"
        ".gmail-message-address, .gmail-message-recipient-line, .gmail-message-time { color: #5f6368; }"
        ".gmail-message-address { font-size: 0.95rem; }"
        ".gmail-message-recipient-line { margin-top: 0.2rem; font-size: 0.94rem; line-height: 1.4; }"
        ".gmail-message-time { font-size: 0.92rem; white-space: nowrap; }"
        ".gmail-message-body { color: #202124; line-height: 1.55; min-width: 0; }"
        ".gmail-message-rendered-html { min-width: 0; }"
        ".gmail-message-rendered-html img { max-width: 100%; height: auto; border-radius: 12px; }"
        ".gmail-message-rendered-html table { max-width: 100%; }"
        ".gmail-message-rendered-html a, .retriever-attachments a { color: #1a73e8; text-decoration: none; }"
        ".gmail-message-rendered-html a:hover, .retriever-attachments a:hover { text-decoration: underline; }"
        ".gmail-message-rendered-html .gmail_quote, .gmail-message-rendered-html blockquote[type='cite'], .gmail-message-rendered-html blockquote { margin: 1rem 0 0; padding-left: 0.9rem; border-left: 3px solid #d8dde3; color: #5f6368; }"
        ".gmail-message-plain { white-space: pre-wrap; word-break: break-word; font: inherit; }"
        ".gmail-message-quoted { margin-top: 1rem; }"
        ".gmail-message-quoted summary { cursor: pointer; color: #5f6368; font-weight: 500; }"
        ".gmail-message-quoted pre { white-space: pre-wrap; word-break: break-word; margin: 0.72rem 0 0; padding: 0.85rem 1rem; background: #f8fafc; border: 1px solid #e4e7eb; border-radius: 14px; font: inherit; color: #5f6368; }"
        ".retriever-calendar-invites { margin: 0 0 1rem; display: grid; gap: 0.8rem; }"
        ".retriever-calendar-invite { padding: 0.95rem 1rem; border: 1px solid #c6dafc; border-radius: 16px; background: linear-gradient(180deg, #eef4ff 0%, #f7fbff 100%); }"
        ".retriever-calendar-invite-header { display: flex; justify-content: space-between; gap: 0.75rem; align-items: flex-start; }"
        ".retriever-calendar-invite-kicker { margin: 0 0 0.25rem; color: #4a6488; font-size: 0.76rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; }"
        ".retriever-calendar-invite-title { margin: 0; font-size: 1rem; line-height: 1.3; }"
        ".retriever-calendar-invite-title a { color: #174ea6; text-decoration: none; }"
        ".retriever-calendar-invite-title a:hover { text-decoration: underline; }"
        ".retriever-calendar-invite-detail { margin: 0; color: #5f6368; font-size: 0.84rem; white-space: nowrap; }"
        ".retriever-calendar-invite-meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 0.6rem 0.75rem; margin: 0.85rem 0 0; }"
        ".retriever-calendar-invite-meta div { background: rgba(255,255,255,0.82); border: 1px solid #d7e6ff; border-radius: 12px; padding: 0.6rem 0.7rem; }"
        ".retriever-calendar-invite-meta dt { font-size: 0.78rem; font-weight: 700; color: #516072; margin-bottom: 0.18rem; letter-spacing: 0.04em; text-transform: uppercase; }"
        ".retriever-calendar-invite-meta dd { margin: 0; }"
        ".retriever-calendar-invite-meta a { color: #174ea6; text-decoration: none; word-break: break-all; }"
        ".retriever-calendar-invite-meta a:hover { text-decoration: underline; }"
        ".retriever-attachments { margin-top: 1rem; padding-top: 0.8rem; border-top: 1px solid #eceff3; }"
        ".retriever-attachments h2 { margin: 0 0 0.5rem; font-size: 0.86rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; color: #5f6368; }"
        ".retriever-attachments ul { margin: 0; padding-left: 1.1rem; }"
        ".retriever-attachments li { margin: 0.26rem 0; }"
        ".chat-avatar-svg { width: 3rem; height: 3rem; flex: 0 0 auto; display: block; }"
        "@media (max-width: 720px) {"
        ".gmail-thread-page { padding: 18px 12px 30px; }"
        ".gmail-message-card { padding: 0.95rem; }"
        ".gmail-message-header { flex-direction: column; gap: 0.45rem; }"
        ".gmail-message-time { white-space: normal; }"
        "}"
        "</style>"
    )


def email_preview_document_id(document: dict[str, object]) -> int | None:
    try:
        raw_value = document.get("id")
        if raw_value is None:
            return None
        value = int(raw_value)
        return value if value > 0 else None
    except (TypeError, ValueError):
        return None


def normalize_email_preview_address(value: object) -> tuple[str | None, str | None]:
    normalized = normalize_whitespace(str(value or ""))
    if not normalized:
        return None, None
    addresses = getaddresses([normalized])
    if addresses:
        name, address = addresses[0]
        normalized_name = normalize_whitespace(name)
        normalized_address = normalize_whitespace(address)
        if normalized_name or normalized_address:
            return normalized_name or normalized_address or None, normalized_address or None
    return normalized, None


def format_email_preview_person(value: object) -> str | None:
    name, address = normalize_email_preview_address(value)
    if name and address and name != address:
        return f"{name} <{address}>"
    return name or address


def summarize_email_preview_recipients(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or ""))
    if not normalized:
        return None
    addresses = getaddresses([normalized])
    formatted_addresses = [
        formatted
        for formatted in (format_email_preview_person(f"{name} <{address}>") if address else normalize_whitespace(name) for name, address in addresses)
        if formatted
    ]
    if formatted_addresses:
        return f"to {', '.join(formatted_addresses)}"
    return f"to {normalized}"


def split_email_preview_text_content(text_content: str) -> tuple[str, str | None]:
    normalized_text = text_content.replace("\r\n", "\n").replace("\r", "\n").strip("\n")
    if not normalized_text:
        return "", None
    lines = normalized_text.split("\n")
    has_visible_content = False
    for index, raw_line in enumerate(lines):
        stripped = raw_line.strip()
        if not stripped:
            continue
        if has_visible_content:
            if raw_line.lstrip().startswith(">") or EMAIL_QUOTED_REPLY_SEPARATOR_PATTERN.match(stripped):
                visible_text = "\n".join(lines[:index]).strip()
                quoted_text = "\n".join(lines[index:]).strip()
                if visible_text and quoted_text:
                    return visible_text, quoted_text
            if index > 0 and not lines[index - 1].strip() and EMAIL_QUOTED_REPLY_HEADER_PATTERN.match(stripped):
                visible_text = "\n".join(lines[:index]).strip()
                quoted_text = "\n".join(lines[index:]).strip()
                if visible_text and quoted_text:
                    return visible_text, quoted_text
        has_visible_content = True
    return normalized_text, None


def email_html_has_visible_content(html_content: str | None) -> bool:
    return bool(normalize_whitespace(strip_html_tags(str(html_content or ""))))


def strip_email_reply_history_html(html_content: str | None) -> str | None:
    normalized_html = str(html_content or "").strip()
    if not normalized_html:
        return None
    split_index = min(
        (
            match.start()
            for pattern in EMAIL_HTML_QUOTED_REPLY_START_PATTERNS
            if (match := pattern.search(normalized_html)) is not None
        ),
        default=None,
    )
    if split_index is None:
        return normalized_html
    candidate = re.sub(r"(?is)(?:<br\s*/?>|\s|&nbsp;)+$", "", normalized_html[:split_index]).strip()
    return candidate if email_html_has_visible_content(candidate) else None


def build_email_message_body_content_html(
    document: dict[str, object],
    *,
    body_html: str | None = None,
    strip_quoted_history: bool = False,
) -> str:
    text_content, calendar_invites = extract_calendar_invites_from_text_content(
        str(document.get("text_content") or "")
    )
    visible_text, quoted_text = split_email_preview_text_content(text_content)
    calendar_invites_html = render_html_preview_calendar_invite_cards(calendar_invites)
    preferred_body_html = body_html
    if preferred_body_html is None:
        stored_body_html = document.get("standalone_preview_body_html")
        preferred_body_html = (
            str(stored_body_html)
            if isinstance(stored_body_html, str) and stored_body_html.strip()
            else None
        )
    if preferred_body_html:
        normalized_html = preferred_body_html.strip()
        if strip_quoted_history:
            stripped_html = strip_email_reply_history_html(normalized_html)
            if stripped_html is not None:
                normalized_html = stripped_html
            elif quoted_text and visible_text:
                normalized_html = ""
        normalized_html = HTML_PREVIEW_CALENDAR_INVITES_PATTERN.sub("", normalized_html).strip()
        if normalized_html and any(
            token in normalized_html
            for token in ('class="gmail-message-rendered-html"', 'class="gmail-message-plain"', 'class="gmail-message-quoted"')
        ):
            return f"{calendar_invites_html}{normalized_html}" if calendar_invites_html else normalized_html
        normalized_html = re.sub(r"(?is)<!doctype[^>]*>\s*", "", normalized_html).strip()
        if normalized_html:
            rendered_html = f'<div class="gmail-message-rendered-html">{normalized_html}</div>'
            return f"{calendar_invites_html}{rendered_html}" if calendar_invites_html else rendered_html
    body_parts: list[str] = []
    if calendar_invites_html:
        body_parts.append(calendar_invites_html)
    plain_text = visible_text or ("No extracted text available." if not body_parts else "")
    if plain_text:
        body_parts.append(
            f'<div class="gmail-message-plain">{html.escape(plain_text)}</div>'
        )
    elif not body_parts:
        body_parts.append(
            '<div class="gmail-message-plain">No extracted text available.</div>'
        )
    if quoted_text and not strip_quoted_history:
        body_parts.append(
            "<details class=\"gmail-message-quoted\">"
            "<summary>Quoted text</summary>"
            f"<pre>{html.escape(quoted_text)}</pre>"
            "</details>"
        )
    return "".join(body_parts)


def build_email_message_card_html(
    document: dict[str, object],
    *,
    body_html: str | None = None,
    selected: bool = False,
    attachment_links: list[dict[str, str]] | None = None,
    strip_quoted_history: bool = False,
) -> str:
    author_name, author_email = normalize_email_preview_address(document.get("author"))
    author_label = author_name or author_email or "Unknown sender"
    author_email_html = (
        f'<span class="gmail-message-address">&lt;{html.escape(author_email)}&gt;</span>'
        if author_email and author_email != author_label
        else ""
    )
    recipients_line = summarize_email_preview_recipients(document.get("recipients"))
    timestamp_label = format_chat_preview_timestamp(conversation_preview_primary_timestamp(document)) or ""
    avatar_background, avatar_foreground = chat_avatar_colors(author_label)
    avatar_html = build_chat_avatar_svg(
        chat_avatar_initials(author_label),
        avatar_background,
        avatar_foreground,
        author_label,
    )
    message_body_html = build_email_message_body_content_html(
        document,
        body_html=body_html,
        strip_quoted_history=strip_quoted_history,
    )
    attachment_links_html = render_html_preview_attachment_links(attachment_links or [])
    card_classes = "gmail-message-card gmail-message-card--selected" if selected else "gmail-message-card"
    anchor = conversation_preview_anchor(document_id) if (document_id := email_preview_document_id(document)) is not None else None
    anchor_attr = f' id="{html.escape(anchor)}"' if anchor else ""
    recipient_line_html = (
        f'<div class="gmail-message-recipient-line">{html.escape(recipients_line)}</div>'
        if recipients_line
        else ""
    )
    time_html = f'<div class="gmail-message-time">{html.escape(timestamp_label)}</div>' if timestamp_label else ""
    return (
        f'<article class="{card_classes}"{anchor_attr}>'
        f"{avatar_html}"
        '<div class="gmail-message-main">'
        '<header class="gmail-message-header">'
        '<div class="gmail-message-header-main">'
        '<div class="gmail-message-author-line">'
        f'<span class="gmail-message-author">{html.escape(author_label)}</span>'
        f"{author_email_html}"
        "</div>"
        f"{recipient_line_html}"
        "</div>"
        f"{time_html}"
        "</header>"
        f'<div class="gmail-message-body">{message_body_html}</div>'
        f"{attachment_links_html}"
        "</div>"
        "</article>"
    )


def build_email_thread_summary_html(
    documents: list[dict[str, object]],
    *,
    summary_documents: list[dict[str, object]] | None = None,
    position_index: int | None = None,
    segment_label: str | None = None,
    segment_count: int | None = None,
) -> str:
    summary_scope = summary_documents if summary_documents is not None else documents
    if not summary_scope:
        return ""
    total_messages = len(summary_scope)
    if total_messages <= 1 and not (segment_label and (segment_count or 0) > 1):
        return ""
    started_at, last_message_at = conversation_preview_bounds(summary_scope)
    participants = conversation_preview_participants(summary_scope)
    pills: list[tuple[str, bool]] = []
    if segment_label and (segment_count or 0) > 1:
        pills.append((segment_label, False))
    if total_messages > 1:
        pills.append((f"{total_messages} messages", False))
    started_label = format_chat_preview_timestamp(started_at) or started_at or ""
    if started_label:
        pills.append((f"Created {started_label}", False))
    last_message_label = format_chat_preview_timestamp(last_message_at) or last_message_at or ""
    if last_message_label and last_message_label != started_label:
        pills.append((f"Last modified {last_message_label}", False))
    if participants:
        pills.append((f"Participants {participants}", False))
    if position_index is not None and total_messages > 1:
        pills.append((f"Viewing message {position_index} of {total_messages}", True))
    if not pills:
        return ""
    pills_html = "".join(
        f'<span class="gmail-thread-pill{" gmail-thread-pill--active" if is_active else ""}">{html.escape(label)}</span>'
        for label, is_active in pills
        if label
    )
    return f'<div class="gmail-thread-summary">{pills_html}</div>' if pills_html else ""


def build_email_thread_title_html(
    thread_title: str,
    *,
    thread_link_href: str | None = None,
    thread_position_label: str | None = None,
) -> str:
    if not thread_link_href and not thread_position_label:
        return html.escape(thread_title)
    title_html = (
        f'<a class="gmail-thread-title-link" href="{html.escape(thread_link_href)}">{html.escape(thread_title)}</a>'
        if thread_link_href
        else f'<span class="gmail-thread-title-text">{html.escape(thread_title)}</span>'
    )
    if thread_position_label:
        title_html += f'<span class="gmail-thread-title-meta">({html.escape(thread_position_label)})</span>'
    return title_html


def build_email_thread_preview_html(
    *,
    thread_title: str,
    documents: list[dict[str, object]],
    summary_documents: list[dict[str, object]] | None = None,
    page_title: str,
    selected_document_id: int | None = None,
    position_index: int | None = None,
    segment_label: str | None = None,
    segment_count: int | None = None,
    attachment_links_by_document_id: dict[int, list[dict[str, str]]] | None = None,
    body_source_document: dict[str, object] | None = None,
    body_source_html: str | None = None,
    thread_link_href: str | None = None,
    thread_position_label: str | None = None,
    strip_quoted_history: bool = False,
    newest_first: bool = False,
) -> str:
    selected_card_script = ""
    if selected_document_id is not None:
        selected_card_script = (
            "<script>"
            "window.addEventListener('load', function () {"
            "var selected = document.querySelector('.gmail-message-card--selected');"
            "if (!selected) { return; }"
            "selected.scrollIntoView({block: 'start'});"
            "});"
            "</script>"
        )
    summary_html = build_email_thread_summary_html(
        documents,
        summary_documents=summary_documents,
        position_index=position_index,
        segment_label=segment_label,
        segment_count=segment_count,
    )
    title_html = build_email_thread_title_html(
        thread_title,
        thread_link_href=thread_link_href,
        thread_position_label=thread_position_label,
    )
    render_documents = list(reversed(documents)) if newest_first else list(documents)
    message_cards = []
    for document in render_documents:
        document_id = email_preview_document_id(document)
        message_cards.append(
            build_email_message_card_html(
                document,
                selected=(selected_document_id is not None and document_id == selected_document_id),
                attachment_links=(
                    (attachment_links_by_document_id or {}).get(document_id, [])
                    if document_id is not None
                    else None
                ),
                strip_quoted_history=strip_quoted_history,
            )
        )
    header_kicker = (
        f'<p class="gmail-thread-kicker">{html.escape(segment_label)}</p>'
        if segment_label and (segment_count or 0) > 1
        else ""
    )
    return (
        "<!DOCTYPE html>"
        "<html><head>"
        '<meta charset="utf-8"/>'
        f"<title>{html.escape(page_title)}</title>"
        f"{build_email_preview_head_html()}"
        "</head><body>"
        '<main class="gmail-thread-page">'
        '<header class="gmail-thread-header">'
        f"{header_kicker}"
        f'<h1 class="gmail-thread-title">{title_html}</h1>'
        f"{summary_html}"
        "</header>"
        f'<section class="gmail-thread-messages">{"".join(message_cards)}</section>'
        "</main>"
        f"{selected_card_script}"
        "</body></html>"
    )


def build_email_message_preview_html(
    document: dict[str, object],
    *,
    body_html: str | None,
    conversation_row: sqlite3.Row | None = None,
    conversation_documents: list[dict[str, object]] | None = None,
    position_index: int | None = None,
    thread_link_href: str | None = None,
    attachment_links: list[dict[str, str]] | None = None,
) -> str:
    document_title = conversation_preview_document_heading(document) or "Retriever Email Preview"
    document_id = email_preview_document_id(document)
    attachment_links_by_document_id = (
        {document_id: attachment_links}
        if document_id is not None and attachment_links
        else None
    )
    if conversation_row is not None and conversation_documents is not None and len(conversation_documents) > 1:
        email_documents = [
            item
            for item in conversation_documents
            if normalize_whitespace(str(item.get("content_type") or "")).lower() == "email"
        ]
        selected_position = position_index
        if selected_position is None and document_id is not None:
            for index, candidate in enumerate(email_documents, start=1):
                if email_preview_document_id(candidate) == document_id:
                    selected_position = index
                    break
        if selected_position is None:
            selected_rel_path = normalize_whitespace(str(document.get("rel_path") or ""))
            for index, candidate in enumerate(email_documents, start=1):
                if normalize_whitespace(str(candidate.get("rel_path") or "")) == selected_rel_path:
                    selected_position = index
                    break
        timeline_documents = (
            email_documents[:selected_position]
            if selected_position is not None and selected_position > 0
            else [document]
        )
        thread_title = (
            normalize_whitespace(str(conversation_row["display_name"] or ""))
            or normalize_generated_document_title(document.get("subject") or document.get("title"))
            or document_title
        )
        thread_message_count = len(email_documents)
        thread_position_label = (
            f"{selected_position}/{thread_message_count} in thread"
            if selected_position is not None and thread_message_count > 1
            else None
        )
        return build_email_thread_preview_html(
            thread_title=thread_title,
            documents=timeline_documents,
            summary_documents=email_documents,
            page_title=(
                f"{thread_title} ({thread_position_label})"
                if thread_position_label
                else thread_title
            ),
            selected_document_id=document_id,
            position_index=selected_position,
            body_source_document=document,
            body_source_html=body_html,
            thread_link_href=thread_link_href,
            thread_position_label=thread_position_label,
            attachment_links_by_document_id=attachment_links_by_document_id,
            strip_quoted_history=True,
            newest_first=True,
        )
    thread_title = normalize_generated_document_title(document.get("subject") or document.get("title")) or document_title
    return build_email_thread_preview_html(
        thread_title=thread_title,
        documents=[document],
        page_title=document_title,
        selected_document_id=document_id,
        body_source_document=document,
        body_source_html=body_html,
        attachment_links_by_document_id=attachment_links_by_document_id,
    )


def default_email_message_preview_rel_path(document: dict[str, object]) -> str | None:
    rel_path = normalize_whitespace(str(document.get("rel_path") or ""))
    if not rel_path:
        return None
    preview_base = preview_base_path_for_rel_path(rel_path)
    source_kind = normalize_whitespace(str(document.get("source_kind") or "")).lower()
    source_item_id = normalize_whitespace(str(document.get("source_item_id") or ""))
    if source_kind in {PST_SOURCE_KIND, MBOX_SOURCE_KIND} and source_item_id:
        preview_file_name = container_preview_file_name(source_item_id)
    else:
        preview_file_name = f"{Path(rel_path).name}.html"
    return (preview_base / preview_file_name).as_posix()


def rewrite_preserved_email_message_preview(
    paths: dict[str, Path],
    *,
    document: dict[str, object],
    preview_rows: list[dict[str, object]],
    conversation_row: sqlite3.Row | None = None,
    conversation_documents: list[dict[str, object]] | None = None,
    position_index: int | None = None,
    thread_rel_path: str | None = None,
    attachment_links: list[dict[str, str]] | None = None,
) -> None:
    preferred_rows = sorted(
        preview_rows,
        key=lambda row: (
            0 if normalize_whitespace(str(row.get("label") or "")).lower() == "message" else 1,
            int(row.get("ordinal", 0)),
        ),
    )
    target_row = next(
        (
            row
            for row in preferred_rows
            if normalize_whitespace(str(row.get("preview_type") or "")).lower() == "html"
        ),
        None,
    )
    if target_row is None:
        synthesized_rel_path = default_email_message_preview_rel_path(document)
        if synthesized_rel_path is None:
            return
        target_row = {
            "rel_preview_path": synthesized_rel_path,
            "preview_type": "html",
            "target_fragment": None,
            "label": "message",
            "ordinal": 0,
        }
        preview_rows.insert(0, target_row)
    else:
        preview_rows[:] = [target_row, *[row for row in preview_rows if row is not target_row]]
        if not normalize_whitespace(str(target_row.get("label") or "")):
            target_row["label"] = "message"
    preview_path = paths["state_dir"] / str(target_row["rel_preview_path"])
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    thread_link_href = None
    if thread_rel_path:
        thread_path = paths["state_dir"] / thread_rel_path
        if thread_path.exists():
            thread_link_href = relative_preview_href(thread_path, preview_path)
    body_html = document.get("standalone_preview_body_html")
    preview_path.write_text(
        build_email_message_preview_html(
            document,
            body_html=(str(body_html) if isinstance(body_html, str) and body_html.strip() else None),
            conversation_row=conversation_row,
            conversation_documents=conversation_documents,
            position_index=position_index,
            thread_link_href=thread_link_href,
            attachment_links=attachment_links,
        ),
        encoding="utf-8",
    )


def extract_standalone_preview_body_html(preview_html: str) -> str | None:
    cleaned_preview_html = HTML_PREVIEW_ATTACHMENT_LINKS_PATTERN.sub("", preview_html or "")
    cleaned_preview_html = HTML_PREVIEW_CALENDAR_INVITES_PATTERN.sub("", cleaned_preview_html)
    visible_body_html = extract_visible_email_preview_body_html(cleaned_preview_html)
    if visible_body_html is not None:
        return visible_body_html
    source_match = EMAIL_PREVIEW_BODY_SOURCE_PATTERN.search(cleaned_preview_html)
    if source_match is not None:
        normalized_source = source_match.group(1).strip()
        return normalized_source or None
    body_match = re.search(
        r"<body\b[^>]*>(.*)</body>\s*</html>\s*$",
        cleaned_preview_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if body_match is None:
        return None
    body_html = re.sub(
        r"^\s*<h1\b[^>]*>.*?</h1>\s*",
        "",
        body_match.group(1),
        count=1,
        flags=re.IGNORECASE | re.DOTALL,
    )
    body_html = re.sub(
        r"^\s*<table\b[^>]*>.*?</table>\s*<hr\s*/?>\s*",
        "",
        body_html,
        count=1,
        flags=re.IGNORECASE | re.DOTALL,
    )
    normalized = body_html.strip()
    return normalized or None


def load_preserved_preview_rows_by_document_id(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    document_ids: list[int],
) -> dict[int, list[dict[str, object]]]:
    if not document_ids:
        return {}
    rows = connection.execute(
        f"""
        SELECT document_id, rel_preview_path, preview_type, target_fragment, label, ordinal
        FROM document_previews
        WHERE document_id IN ({", ".join("?" for _ in document_ids)})
        ORDER BY document_id ASC, ordinal ASC, id ASC
        """,
        document_ids,
    ).fetchall()
    preview_rows_by_document_id: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        rel_preview_path = normalize_whitespace(str(row["rel_preview_path"] or ""))
        if not rel_preview_path or is_conversation_preview_rel_path(rel_preview_path):
            continue
        if not (paths["state_dir"] / rel_preview_path).exists():
            continue
        preview_rows_by_document_id[int(row["document_id"])].append(
            {
                "rel_preview_path": rel_preview_path,
                "preview_type": str(row["preview_type"]),
                "target_fragment": row["target_fragment"],
                "label": (str(row["label"]) if row["label"] is not None else None),
                "ordinal": int(row["ordinal"]),
            }
        )
    return dict(preview_rows_by_document_id)


def load_document_preview_body_html(
    paths: dict[str, Path],
    preview_rows: list[dict[str, object]],
) -> str | None:
    preferred_rows = sorted(
        preview_rows,
        key=lambda row: (
            0 if normalize_whitespace(str(row.get("label") or "")).lower() == "message" else 1,
            int(row.get("ordinal", 0)),
        ),
    )
    for preview_row in preferred_rows:
        if normalize_whitespace(str(preview_row.get("preview_type") or "")).lower() != "html":
            continue
        preview_path = paths["state_dir"] / str(preview_row["rel_preview_path"])
        if not preview_path.exists():
            continue
        body_html = extract_standalone_preview_body_html(
            preview_path.read_text(encoding="utf-8")
        )
        if body_html:
            return body_html
    return None


def rebase_preserved_preview_rows(
    preview_rows: list[dict[str, object]],
    *,
    start_ordinal: int,
    created_at: str,
) -> list[dict[str, object]]:
    rebased_rows: list[dict[str, object]] = []
    for index, preview_row in enumerate(preview_rows):
        rebased_rows.append(
            {
                "rel_preview_path": str(preview_row["rel_preview_path"]),
                "preview_type": str(preview_row["preview_type"]),
                "target_fragment": preview_row.get("target_fragment"),
                "label": preview_row.get("label"),
                "ordinal": start_ordinal + index,
                "created_at": created_at,
            }
        )
    return rebased_rows


def render_conversation_chat_body_html(text_content: str) -> str:
    chat_entries = iter_chat_transcript_entries(text_content, max_lines=4000)
    if not chat_entries:
        return f"<pre>{html.escape(text_content)}</pre>"
    rendered_entries: list[str] = []
    for entry in chat_entries:
        speaker = normalize_whitespace(str(entry.get("speaker") or "")) or "Unknown"
        body = normalize_whitespace(str(entry.get("body") or ""))
        if not body:
            continue
        timestamp_label = format_chat_preview_timestamp(entry.get("timestamp")) or ""
        timestamp_html = f'<span class="conversation-chat-time">[{html.escape(timestamp_label)}]</span>' if timestamp_label else ""
        avatar_label = chat_avatar_initials(speaker)
        avatar_background, avatar_foreground = chat_avatar_colors(speaker)
        avatar_html = build_chat_avatar_svg(avatar_label, avatar_background, avatar_foreground, speaker)
        rendered_entries.append(
            "<article class=\"conversation-chat-message\">"
            f"{avatar_html}"
            "<div class=\"conversation-chat-main\">"
            "<div class=\"conversation-chat-meta\">"
            f"<span class=\"conversation-chat-speaker\">{html.escape(speaker)}</span>"
            f"{timestamp_html}"
            "</div>"
            f"<div class=\"conversation-chat-body\">{html.escape(body)}</div>"
            "</div>"
            "</article>"
        )
    if not rendered_entries:
        return f"<pre>{html.escape(text_content)}</pre>"
    return (
        "<div class=\"conversation-chat-transcript\">"
        f"{''.join(rendered_entries)}"
        "</div>"
        "<details class=\"conversation-raw-text\">"
        "<summary>Full transcript</summary>"
        f"<pre>{html.escape(text_content)}</pre>"
        "</details>"
    )


def render_conversation_document_section(
    document: dict[str, object],
    *,
    current_segment_href: str,
    doc_target_hrefs: dict[int, str],
    attachment_links_by_document_id: dict[int, list[dict[str, str]]] | None = None,
) -> str:
    document_id = int(document["id"])
    anchor = conversation_preview_anchor(document_id)
    heading = conversation_preview_document_heading(document)
    kind_label = conversation_preview_document_kind(document)
    timestamp_label = format_chat_preview_timestamp(conversation_preview_primary_timestamp(document)) or ""
    text_content = str(document.get("text_content") or "")
    standalone_preview_body_html = document.get("standalone_preview_body_html")
    content_type = normalize_whitespace(str(document.get("content_type") or ""))
    metadata_pairs: list[tuple[str, object]] = [
        ("Control number", document.get("control_number")),
        ("Created", timestamp_label),
    ]
    if content_type == "Email":
        metadata_pairs.extend(
            [
                ("Author", document.get("author")),
                ("Recipients", document.get("recipients")),
            ]
        )
    else:
        metadata_pairs.extend(
            [
                ("Participants", document.get("participants")),
                ("From", document.get("author")),
                ("To", document.get("recipients")),
            ]
        )
    metadata_items: list[str] = []
    for label, value in metadata_pairs:
        normalized = normalize_whitespace(str(value or ""))
        if not normalized:
            continue
        metadata_items.append(
            f"<div><dt>{html.escape(label)}</dt><dd>{html.escape(normalized)}</dd></div>"
        )
    parent_document_id = document.get("parent_document_id")
    if parent_document_id is not None:
        parent_id = int(parent_document_id)
        parent_href = doc_target_hrefs.get(parent_id)
        parent_label = normalize_whitespace(
            str(document.get("parent_control_number") or document.get("parent_title") or f"Document {parent_id}")
        )
        if parent_href and parent_label:
            metadata_items.append(
                "<div><dt>Contained in</dt>"
                f"<dd><a href=\"{html.escape(parent_href)}\">{html.escape(parent_label)}</a></dd></div>"
            )
    body_html = (
        render_conversation_chat_body_html(text_content)
        if normalize_whitespace(str(document.get("content_type") or "")) == "Chat"
        else str(standalone_preview_body_html)
        if isinstance(standalone_preview_body_html, str) and standalone_preview_body_html.strip()
        else f"<pre>{html.escape(text_content or 'No extracted text available.')}</pre>"
    )
    attachment_links_html = ""
    if attachment_links_by_document_id:
        attachment_links = attachment_links_by_document_id.get(document_id) or []
        attachment_links_html = render_html_preview_attachment_links(attachment_links)
    metadata_html = (
        f'<dl class="conversation-document-meta">{"".join(metadata_items)}</dl>'
        if metadata_items
        else ""
    )
    # Permalinks are intentionally omitted: Cowork's preview iframe blocks link
    # clicks, so a fragment anchor here wouldn't actually jump anywhere useful.
    _ = current_segment_href
    return "".join(
        [
            f'<article class="conversation-document" id="{html.escape(anchor)}">',
            '<header class="conversation-document-header">',
            "<div>",
            f'<div class="conversation-document-kind">{html.escape(kind_label)}</div>',
            f"<h2>{html.escape(heading)}</h2>",
            "</div>",
            "</header>",
            metadata_html,
            f'<div class="conversation-document-body">{body_html}</div>',
            attachment_links_html,
            "</article>",
        ]
    )

def build_conversation_preview_head_html() -> str:
    return (
        "<style>"
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: linear-gradient(180deg, #eef3f8 0%, #f7fafc 100%); color: #122033; }"
        "main { max-width: 1040px; margin: 0 auto; padding: 28px 20px 44px; }"
        "body > h1 { padding-left: 0.75rem; }"
        "body > table { border-collapse: collapse; width: calc(100% - 1.5rem); margin: 0 0.75rem 1.25rem; background: rgba(255,255,255,0.88); border: 1px solid #d7e0ea; border-radius: 16px; overflow: hidden; }"
        "body > table th, body > table td { text-align: left; padding: 0.55rem 0.75rem; border-bottom: 1px solid #e3e8ef; vertical-align: top; }"
        "body > table th { width: 12rem; color: #516072; font-weight: 600; }"
        ".conversation-nav, .conversation-segments { display: grid; gap: 0.9rem; }"
        ".conversation-segment-card, .conversation-document { background: rgba(255,255,255,0.94); border: 1px solid #d7e0ea; border-radius: 18px; box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06); }"
        ".conversation-segment-card { padding: 1rem 1.1rem; }"
        ".conversation-segment-card h2 { margin: 0 0 0.35rem; font-size: 1.05rem; }"
        ".conversation-segment-card p { margin: 0 0 0.65rem; color: #516072; }"
        ".conversation-segment-card ul { margin: 0; padding-left: 1.15rem; }"
        ".conversation-segment-card li { margin: 0.28rem 0; }"
        ".conversation-segment-card a, .conversation-nav a, .conversation-document-meta a, .retriever-attachments a { color: #0b63ce; text-decoration: none; }"
        ".conversation-nav { margin-bottom: 1rem; }"
        ".conversation-nav-links { display: flex; flex-wrap: wrap; gap: 0.75rem; align-items: center; color: #516072; }"
        ".conversation-full-segments, .conversation-segment-group { display: grid; gap: 0.9rem; }"
        ".conversation-segment-banner { background: rgba(255,255,255,0.9); border: 1px solid #d7e0ea; border-radius: 18px; padding: 1rem 1.1rem; box-shadow: 0 10px 28px rgba(15, 23, 42, 0.04); }"
        ".conversation-segment-banner-kicker { margin: 0 0 0.35rem; color: #607080; font-size: 0.78rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; }"
        ".conversation-segment-banner-title { margin: 0; font-size: 1.2rem; line-height: 1.2; }"
        ".conversation-segment-banner-meta { margin: 0.45rem 0 0; color: #516072; }"
        ".conversation-document { padding: 1.1rem 1.15rem 1.15rem; margin-bottom: 1rem; scroll-margin-top: 1rem; }"
        ".conversation-document-header { display: flex; justify-content: space-between; gap: 1rem; align-items: flex-start; margin-bottom: 0.9rem; }"
        ".conversation-document-header h2 { margin: 0.2rem 0 0; font-size: 1.15rem; }"
        ".conversation-document-kind { font-size: 0.82rem; letter-spacing: 0.06em; text-transform: uppercase; color: #516072; }"
        ".conversation-document-meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 0.7rem 1rem; margin: 0 0 1rem; }"
        ".conversation-document-meta div { background: #f8fafc; border: 1px solid #e3e8ef; border-radius: 14px; padding: 0.65rem 0.75rem; }"
        ".conversation-document-meta dt { font-size: 0.8rem; font-weight: 600; color: #607080; margin-bottom: 0.18rem; }"
        ".conversation-document-meta dd { margin: 0; }"
        ".retriever-calendar-invites { margin: 0 0 1rem; display: grid; gap: 0.8rem; }"
        ".retriever-calendar-invite { padding: 0.95rem 1rem; border: 1px solid #cfe0fb; border-radius: 16px; background: linear-gradient(180deg, #edf4ff 0%, #f8fbff 100%); }"
        ".retriever-calendar-invite-header { display: flex; justify-content: space-between; gap: 0.75rem; align-items: flex-start; }"
        ".retriever-calendar-invite-kicker { margin: 0 0 0.25rem; color: #4a6488; font-size: 0.76rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; }"
        ".retriever-calendar-invite-title { margin: 0; font-size: 1rem; line-height: 1.3; }"
        ".retriever-calendar-invite-title a { color: #174ea6; text-decoration: none; }"
        ".retriever-calendar-invite-title a:hover { text-decoration: underline; }"
        ".retriever-calendar-invite-detail { margin: 0; color: #5f6368; font-size: 0.84rem; white-space: nowrap; }"
        ".retriever-calendar-invite-meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 0.6rem 0.75rem; margin: 0.85rem 0 0; }"
        ".retriever-calendar-invite-meta div { background: rgba(255,255,255,0.82); border: 1px solid #d7e6ff; border-radius: 12px; padding: 0.6rem 0.7rem; }"
        ".retriever-calendar-invite-meta dt { font-size: 0.78rem; font-weight: 700; color: #516072; margin-bottom: 0.18rem; letter-spacing: 0.04em; text-transform: uppercase; }"
        ".retriever-calendar-invite-meta dd { margin: 0; }"
        ".retriever-calendar-invite-meta a { color: #174ea6; text-decoration: none; word-break: break-all; }"
        ".retriever-calendar-invite-meta a:hover { text-decoration: underline; }"
        ".conversation-document-body pre { white-space: pre-wrap; word-break: break-word; margin: 0; background: #f8fafc; border: 1px solid #d7e0ea; border-radius: 14px; padding: 0.9rem 1rem; font-family: inherit; font-size: 1rem; }"
        ".conversation-chat-transcript { display: grid; gap: 0.75rem; }"
        ".conversation-chat-message { display: flex; gap: 0.75rem; align-items: flex-start; border: 1px solid #d0d7de; border-radius: 14px; padding: 0.85rem 0.95rem; background: #f6f8fa; }"
        ".chat-avatar-svg { width: 2.5rem; height: 2.5rem; flex: 0 0 auto; display: block; }"
        ".conversation-chat-main { min-width: 0; flex: 1 1 auto; }"
        ".conversation-chat-meta { display: flex; gap: 0.55rem; align-items: baseline; margin-bottom: 0.25rem; flex-wrap: wrap; }"
        ".conversation-chat-speaker { font-weight: 600; color: #0969da; }"
        ".conversation-chat-time { color: #57606a; font-size: 0.9rem; }"
        ".conversation-chat-body { white-space: pre-wrap; line-height: 1.45; font-size: 1rem; }"
        ".conversation-raw-text { margin-top: 0.85rem; }"
        ".conversation-raw-text summary { cursor: pointer; color: #516072; }"
        "</style>"
    )


def conversation_segment_position_label(segment_index: int, segment_count: int) -> str | None:
    if segment_count <= 1:
        return None
    return f"Segment {segment_index + 1} of {segment_count}"


def build_conversation_segment_banner_html(
    *,
    segment_label: str,
    segment_index: int,
    segment_count: int,
    document_count: int,
) -> str:
    position_label = conversation_segment_position_label(segment_index, segment_count)
    if position_label is None:
        return ""
    document_label = f"{document_count} document{'s' if document_count != 1 else ''} in this segment"
    return (
        '<section class="conversation-segment-banner">'
        f'<p class="conversation-segment-banner-kicker">{html.escape(position_label)}</p>'
        f'<h2 class="conversation-segment-banner-title">{html.escape(segment_label)}</h2>'
        f'<p class="conversation-segment-banner-meta">{html.escape(document_label)}</p>'
        "</section>"
    )


def build_conversation_toc_html(
    conversation_row: sqlite3.Row,
    *,
    documents: list[dict[str, object]],
    segment_items: list[dict[str, object]],
) -> str:
    headers = {
        "Conversation": normalize_whitespace(str(conversation_row["display_name"] or "")) or f"Conversation {int(conversation_row['id'])}",
        "Type": normalize_whitespace(str(conversation_row["conversation_type"] or "")),
        "Documents": str(len(documents)),
        "Segments": str(len(segment_items)),
    }
    cards: list[str] = []
    for segment in segment_items:
        doc_entries = "".join(
            f"<li>{html.escape(conversation_preview_document_heading(document))}</li>"
            for document in segment["documents"]
        )
        cards.append(
            "<section class=\"conversation-segment-card\">"
            f"<h2>{html.escape(str(segment['label']))}</h2>"
            f"<p>{len(segment['documents'])} document{'s' if len(segment['documents']) != 1 else ''}</p>"
            f"{'<ul>' + doc_entries + '</ul>' if doc_entries else ''}"
            "</section>"
        )
    return build_html_preview(
        headers,
        body_html=f"<main><div class=\"conversation-segments\">{''.join(cards)}</div></main>",
        document_title=headers["Conversation"] or "Conversation",
        head_html=build_conversation_preview_head_html(),
        heading="Conversation Contents",
    )


def build_conversation_segment_html(
    conversation_row: sqlite3.Row,
    *,
    segment_label: str,
    segment_index: int,
    segment_count: int,
    segment_items: list[dict[str, object]],
    current_segment_rel_path: str,
    doc_target_hrefs: dict[int, str],
    attachment_links_by_document_id: dict[int, list[dict[str, str]]] | None = None,
) -> str:
    current_file_name = Path(current_segment_rel_path).name
    segment_documents = segment_items[segment_index]["documents"]
    if normalize_whitespace(str(conversation_row["conversation_type"] or "")).lower() == "email":
        thread_title = (
            normalize_whitespace(str(conversation_row["display_name"] or ""))
            or f"Conversation {int(conversation_row['id'])}"
        )
        return build_email_thread_preview_html(
            thread_title=thread_title,
            documents=segment_documents,
            page_title=f"{thread_title} - {segment_label}",
            segment_label=(
                f"{segment_label} ({conversation_segment_position_label(segment_index, segment_count)})"
                if conversation_segment_position_label(segment_index, segment_count)
                else segment_label
            ),
            segment_count=segment_count,
            attachment_links_by_document_id=attachment_links_by_document_id,
            strip_quoted_history=True,
        )
    headers = {
        "Conversation": normalize_whitespace(str(conversation_row["display_name"] or "")) or f"Conversation {int(conversation_row['id'])}",
        "Type": normalize_whitespace(str(conversation_row["conversation_type"] or "")),
        "Segment": segment_label,
        "Documents": str(len(segment_documents)),
    }
    sections = "".join(
        render_conversation_document_section(
            document,
            current_segment_href=current_file_name,
            doc_target_hrefs=doc_target_hrefs,
            attachment_links_by_document_id=attachment_links_by_document_id,
        )
        for document in segment_documents
    )
    segment_banner_html = build_conversation_segment_banner_html(
        segment_label=segment_label,
        segment_index=segment_index,
        segment_count=segment_count,
        document_count=len(segment_documents),
    )
    return build_html_preview(
        headers,
        body_html=f"<main>{segment_banner_html}{sections}</main>",
        document_title=f"{headers['Conversation']} - {segment_label}",
        head_html=build_conversation_preview_head_html(),
        heading=segment_label,
    )


def build_conversation_entry_html(
    conversation_row: sqlite3.Row,
    *,
    document: dict[str, object],
    document_heading: str,
    segment_label: str | None = None,
    attachment_links: list[dict[str, str]] | None = None,
    position_index: int | None = None,
    total_count: int | None = None,
) -> str:
    conversation_name = (
        normalize_whitespace(str(conversation_row["display_name"] or ""))
        or f"Conversation {int(conversation_row['id'])}"
    )
    document_id = int(document["id"])
    section_html = render_conversation_document_section(
        document,
        current_segment_href="entry",
        doc_target_hrefs={document_id: f"#{conversation_preview_anchor(document_id)}"},
        attachment_links_by_document_id=(
            {document_id: attachment_links}
            if attachment_links
            else None
        ),
    )
    headers: dict[str, str] = {
        "Conversation": conversation_name,
        "Type": normalize_whitespace(str(conversation_row["conversation_type"] or "")),
    }
    if segment_label:
        headers["Segment"] = segment_label
    if position_index is not None and total_count is not None:
        headers["Document"] = f"{position_index} of {total_count}"
    return build_html_preview(
        headers,
        body_html=f"<main>{section_html}</main>",
        document_title=document_heading or conversation_name or "Conversation document",
        head_html=build_conversation_preview_head_html(),
        heading=document_heading or "Conversation document",
    )


def build_email_conversation_full_html(
    conversation_row: sqlite3.Row,
    *,
    documents: list[dict[str, object]],
    segment_items: list[dict[str, object]],
    attachment_links_by_document_id: dict[int, list[dict[str, str]]] | None = None,
) -> str:
    thread_title = (
        normalize_whitespace(str(conversation_row["display_name"] or ""))
        or f"Conversation {int(conversation_row['id'])}"
    )
    if len(segment_items) <= 1:
        return build_email_thread_preview_html(
            thread_title=thread_title,
            documents=documents,
            page_title=thread_title,
            attachment_links_by_document_id=attachment_links_by_document_id,
            strip_quoted_history=True,
        )
    segment_sections: list[str] = []
    for index, segment in enumerate(segment_items):
        segment_documents = list(segment["documents"])
        message_cards = "".join(
            build_email_message_card_html(
                document,
                attachment_links=(attachment_links_by_document_id or {}).get(int(document["id"]), []),
                strip_quoted_history=True,
            )
            for document in segment_documents
        )
        segment_sections.append(
            '<section class="gmail-thread-segment">'
            '<header class="gmail-thread-segment-header">'
            f'<p class="gmail-thread-kicker">{html.escape(conversation_segment_position_label(index, len(segment_items)) or "")}</p>'
            f'<h2 class="gmail-thread-segment-title">{html.escape(str(segment["label"]))}</h2>'
            f"{build_email_thread_summary_html(segment_documents)}"
            "</header>"
            f'<div class="gmail-thread-messages">{message_cards}</div>'
            "</section>"
        )
    return (
        "<!DOCTYPE html>"
        "<html><head>"
        '<meta charset="utf-8"/>'
        f"<title>{html.escape(thread_title)}</title>"
        f"{build_email_preview_head_html()}"
        "<style>"
        ".gmail-thread-segments { display: grid; gap: 1.25rem; margin-top: 1.25rem; }"
        ".gmail-thread-segment { display: grid; gap: 0.85rem; padding-top: 0.2rem; }"
        ".gmail-thread-segment + .gmail-thread-segment { border-top: 1px solid #e0e3e7; padding-top: 1.35rem; }"
        ".gmail-thread-segment-header { margin-bottom: 0.15rem; }"
        ".gmail-thread-segment-title { margin: 0; font-size: 1.28rem; line-height: 1.15; font-weight: 500; color: #202124; }"
        "</style>"
        "</head><body>"
        '<main class="gmail-thread-page">'
        '<header class="gmail-thread-header">'
        f'<h1 class="gmail-thread-title">{html.escape(thread_title)}</h1>'
        f"{build_email_thread_summary_html(documents)}"
        "</header>"
        f'<section class="gmail-thread-segments">{"".join(segment_sections)}</section>'
        "</main>"
        "</body></html>"
    )


def build_conversation_full_html(
    conversation_row: sqlite3.Row,
    *,
    documents: list[dict[str, object]],
    segment_items: list[dict[str, object]],
    attachment_links_by_document_id: dict[int, list[dict[str, str]]] | None = None,
) -> str:
    if normalize_whitespace(str(conversation_row["conversation_type"] or "")).lower() == "email":
        return build_email_conversation_full_html(
            conversation_row,
            documents=documents,
            segment_items=segment_items,
            attachment_links_by_document_id=attachment_links_by_document_id,
        )
    conversation_name = (
        normalize_whitespace(str(conversation_row["display_name"] or ""))
        or f"Conversation {int(conversation_row['id'])}"
    )
    headers = {
        "Conversation": conversation_name,
        "Type": normalize_whitespace(str(conversation_row["conversation_type"] or "")),
        "Documents": str(len(documents)),
        "Segments": str(len(segment_items)),
    }
    doc_target_hrefs = {
        int(document["id"]): f"#{conversation_preview_anchor(int(document['id']))}"
        for document in documents
    }
    segment_sections = "".join(
        (
            '<section class="conversation-segment-group">'
            f"{build_conversation_segment_banner_html(segment_label=str(segment['label']), segment_index=index, segment_count=len(segment_items), document_count=len(segment['documents']))}"
            + "".join(
                render_conversation_document_section(
                    document,
                    current_segment_href="conversation",
                    doc_target_hrefs=doc_target_hrefs,
                    attachment_links_by_document_id=attachment_links_by_document_id,
                )
                for document in segment["documents"]
            )
            + "</section>"
        )
        for index, segment in enumerate(segment_items)
    )
    return build_html_preview(
        headers,
        body_html=f'<main><div class="conversation-full-segments">{segment_sections}</div></main>',
        document_title=conversation_name,
        head_html=build_conversation_preview_head_html(),
        heading=conversation_name,
    )


def conversation_document_uses_entry_preview(document: dict[str, object]) -> bool:
    return normalize_whitespace(str(document.get("content_type") or "")).lower() == "chat"


def load_document_preview_text(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    document_id: int,
    storage_rel_path: object,
) -> str:
    text_content = read_text_revision_body(paths, str(storage_rel_path or "") or None)
    if text_content is not None:
        return text_content
    chunk_rows = connection.execute(
        """
        SELECT text_content
        FROM document_chunks
        WHERE document_id = ?
        ORDER BY chunk_index ASC
        """,
        (document_id,),
    ).fetchall()
    return "\n".join(str(row["text_content"] or "") for row in chunk_rows)


def load_preview_documents(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    document_ids: list[int] | None = None,
    conversation_id: int | None = None,
    include_attachment_children: bool = False,
    require_dataset_membership: bool = False,
) -> list[dict[str, object]]:
    if document_ids is not None and conversation_id is not None:
        raise RetrieverError("load_preview_documents accepts either document_ids or conversation_id, not both.")
    if document_ids is None and conversation_id is None:
        raise RetrieverError("load_preview_documents requires document_ids or conversation_id.")

    where_clauses = ["d.lifecycle_status NOT IN ('missing', 'deleted')"]
    parameters: list[object] = []
    if document_ids is not None:
        normalized_document_ids = [int(document_id) for document_id in document_ids]
        if not normalized_document_ids:
            return []
        where_clauses.append(f"d.id IN ({', '.join('?' for _ in normalized_document_ids)})")
        parameters.extend(normalized_document_ids)
    else:
        where_clauses.append("d.conversation_id = ?")
        parameters.append(int(conversation_id))
    if not include_attachment_children:
        where_clauses.append("COALESCE(d.child_document_kind, '') != ?")
        parameters.append(CHILD_DOCUMENT_KIND_ATTACHMENT)
    if require_dataset_membership:
        where_clauses.append(
            "EXISTS (SELECT 1 FROM dataset_documents dd WHERE dd.document_id = d.id)"
        )

    rows = connection.execute(
        f"""
        SELECT
          d.id,
          d.rel_path,
          d.control_number,
          d.parent_document_id,
          d.child_document_kind,
          d.file_name,
          d.file_type,
          d.content_type,
          d.date_created,
          d.date_modified,
          d.title,
          d.subject,
          d.author,
          d.participants,
          d.recipients,
          d.source_kind,
          d.source_rel_path,
          d.source_item_id,
          d.source_folder_path,
          d.root_message_key,
          d.conversation_id,
          parent.control_number AS parent_control_number,
          parent.title AS parent_title,
          tr.storage_rel_path AS source_text_storage_rel_path
        FROM documents d
        LEFT JOIN documents parent ON parent.id = d.parent_document_id
        LEFT JOIN text_revisions tr ON tr.id = d.source_text_revision_id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY d.id ASC
        """,
        tuple(parameters),
    ).fetchall()

    documents: list[dict[str, object]] = []
    for row in rows:
        documents.append(
            {
                key: row[key]
                for key in row.keys()
            }
            | {
                "text_content": load_document_preview_text(
                    connection,
                    paths,
                    document_id=int(row["id"]),
                    storage_rel_path=row["source_text_storage_rel_path"],
                )
            }
        )
    preserved_preview_rows_by_document_id = load_preserved_preview_rows_by_document_id(
        connection,
        paths,
        [int(document["id"]) for document in documents],
    )
    for document in documents:
        if normalize_whitespace(str(document.get("content_type") or "")).lower() == "chat":
            continue
        document["standalone_preview_body_html"] = load_document_preview_body_html(
            paths,
            preserved_preview_rows_by_document_id.get(int(document["id"]), []),
        )
    documents.sort(key=conversation_preview_sort_key)
    return documents


def refresh_conversation_previews(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    conversation_ids: list[int] | None = None,
) -> int:
    target_conversation_ids = (
        sorted(dict.fromkeys(int(conversation_id) for conversation_id in conversation_ids))
        if conversation_ids is not None
        else list_active_conversation_ids(connection)
    )
    refreshed = 0
    for conversation_id in target_conversation_ids:
        conversation_row = connection.execute(
            "SELECT * FROM conversations WHERE id = ?",
            (conversation_id,),
        ).fetchone()
        if conversation_row is None:
            continue
        documents = load_preview_documents(connection, paths, conversation_id=conversation_id)
        if not documents:
            continue
        segment_mode = conversation_preview_segment_mode(conversation_row["conversation_type"], documents)
        writes_aggregate_artifacts = conversation_preview_writes_aggregate_artifacts(
            conversation_row["conversation_type"],
            documents,
            segment_mode=segment_mode,
        )
        segment_documents: dict[str, list[dict[str, object]]] = defaultdict(list)
        for document in documents:
            segment_documents[conversation_preview_segment_key(document, segment_mode=segment_mode)].append(document)
        segment_items = [
            {
                "segment_key": segment_key,
                "label": conversation_preview_segment_label(segment_key, segment_mode=segment_mode),
                "segment_rel_path": conversation_preview_segment_rel_path(conversation_id, segment_key),
                "documents": sorted(items, key=conversation_preview_sort_key),
            }
            for segment_key, items in sorted(segment_documents.items(), key=lambda item: item[0])
        ]
        document_ids = [int(document["id"]) for document in documents]
        preserved_preview_rows_by_document_id = load_preserved_preview_rows_by_document_id(
            connection,
            paths,
            document_ids,
        )
        writes_segment_artifacts = writes_aggregate_artifacts and len(segment_items) > 1
        entry_document_ids = [
            int(document["id"])
            for document in documents
            if conversation_document_uses_entry_preview(document)
        ]
        entry_position_by_document_id = {
            document_id: index + 1
            for index, document_id in enumerate(entry_document_ids)
        }
        toc_rel_path = conversation_preview_toc_rel_path(conversation_id)
        full_rel_path = conversation_preview_full_rel_path(conversation_id)
        full_preview_existed_before = (paths["state_dir"] / full_rel_path).exists()
        def aggregate_preview_rel_path(segment: dict[str, object]) -> str:
            return str(segment["segment_rel_path"]) if writes_segment_artifacts else full_rel_path
        doc_target_hrefs = {
            int(document["id"]): f"{Path(aggregate_preview_rel_path(segment)).name}#{conversation_preview_anchor(int(document['id']))}"
            for segment in segment_items
            for document in segment["documents"]
        }
        email_message_position_by_document_id = {
            int(document["id"]): index + 1
            for index, document in enumerate(
                [document for document in documents if normalize_whitespace(str(document.get("content_type") or "")).lower() == "email"]
            )
        }
        if writes_aggregate_artifacts:
            full_abs_path = paths["state_dir"] / full_rel_path
            full_abs_path.parent.mkdir(parents=True, exist_ok=True)
            full_attachment_links = conversation_attachment_links_by_document_id(
                connection,
                paths,
                segment_preview_path=full_abs_path,
                documents=documents,
            )
            full_abs_path.write_text(
                build_conversation_full_html(
                    conversation_row,
                    documents=documents,
                    segment_items=segment_items,
                    attachment_links_by_document_id=full_attachment_links,
                ),
                encoding="utf-8",
            )
            if writes_segment_artifacts:
                toc_abs_path = paths["state_dir"] / toc_rel_path
                toc_abs_path.parent.mkdir(parents=True, exist_ok=True)
                toc_abs_path.write_text(
                    build_conversation_toc_html(
                        conversation_row,
                        documents=documents,
                        segment_items=segment_items,
                    ),
                    encoding="utf-8",
                )
                for index, segment in enumerate(segment_items):
                    segment_abs_path = paths["state_dir"] / str(segment["segment_rel_path"])
                    segment_abs_path.parent.mkdir(parents=True, exist_ok=True)
                    attachment_links = conversation_attachment_links_by_document_id(
                        connection,
                        paths,
                        segment_preview_path=segment_abs_path,
                        documents=list(segment["documents"]),
                    )
                    segment_abs_path.write_text(
                        build_conversation_segment_html(
                            conversation_row,
                            segment_label=str(segment["label"]),
                            segment_index=index,
                            segment_count=len(segment_items),
                            segment_items=segment_items,
                            current_segment_rel_path=str(segment["segment_rel_path"]),
                            doc_target_hrefs=doc_target_hrefs,
                            attachment_links_by_document_id=attachment_links,
                        ),
                        encoding="utf-8",
                    )
        created_at = utc_now()
        document_ids = [int(document["id"]) for document in documents]
        previous_preview_paths = [
            str(row["rel_preview_path"])
            for row in connection.execute(
                f"""
                SELECT DISTINCT rel_preview_path
                FROM document_previews
                WHERE document_id IN ({", ".join("?" for _ in document_ids)})
                """,
                document_ids,
            ).fetchall()
        ]
        if full_preview_existed_before:
            previous_preview_paths.append(full_rel_path)
        for segment in segment_items:
            segment_rel_path = aggregate_preview_rel_path(segment)
            segment_abs_path = paths["state_dir"] / segment_rel_path
            entry_attachment_links = conversation_attachment_links_by_document_id(
                connection,
                paths,
                segment_preview_path=segment_abs_path,
                documents=list(segment["documents"]),
            )
            for document in segment["documents"]:
                document_id = int(document["id"])
                preserved_preview_rows = preserved_preview_rows_by_document_id.get(document_id, [])
                if normalize_whitespace(str(document.get("content_type") or "")).lower() == "email":
                    rewrite_preserved_email_message_preview(
                        paths,
                        document=document,
                        preview_rows=preserved_preview_rows,
                        conversation_row=conversation_row,
                        conversation_documents=documents,
                        position_index=(
                            email_message_position_by_document_id.get(document_id)
                            if len(documents) > 1
                            else None
                        ),
                        thread_rel_path=(
                            segment_rel_path
                            if writes_aggregate_artifacts
                            else None
                        ),
                        attachment_links=entry_attachment_links.get(document_id) or [],
                    )
                preview_rows: list[dict[str, object]] = []
                if conversation_document_uses_entry_preview(document):
                    entry_rel_path = conversation_preview_entry_rel_path(conversation_id, document_id)
                    entry_abs_path = paths["state_dir"] / entry_rel_path
                    entry_abs_path.parent.mkdir(parents=True, exist_ok=True)
                    entry_abs_path.write_text(
                        build_conversation_entry_html(
                            conversation_row,
                            document=document,
                            document_heading=conversation_preview_document_heading(document),
                            segment_label=str(segment["label"]),
                            attachment_links=entry_attachment_links.get(document_id) or [],
                            position_index=entry_position_by_document_id.get(document_id),
                            total_count=len(entry_document_ids),
                        ),
                        encoding="utf-8",
                    )
                    preview_rows.append(
                        {
                            "rel_preview_path": entry_rel_path,
                            "preview_type": "html",
                            "target_fragment": None,
                            "label": "entry",
                            "ordinal": 0,
                            "created_at": created_at,
                        }
                    )
                    if writes_aggregate_artifacts:
                        segment_ordinal = len(preview_rows)
                        preview_rows.append(
                            {
                                "rel_preview_path": segment_rel_path,
                                "preview_type": "html",
                                "target_fragment": conversation_preview_anchor(int(document["id"])),
                                "label": "segment",
                                "ordinal": segment_ordinal,
                                "created_at": created_at,
                            }
                        )
                        if writes_segment_artifacts:
                            preview_rows.append(
                                {
                                    "rel_preview_path": toc_rel_path,
                                    "preview_type": "html",
                                    "target_fragment": None,
                                    "label": "contents",
                                    "ordinal": len(preview_rows),
                                    "created_at": created_at,
                                }
                            )
                    rebased_preview_rows = rebase_preserved_preview_rows(
                        preserved_preview_rows,
                        start_ordinal=len(preview_rows),
                        created_at=created_at,
                    )
                else:
                    rebased_preview_rows = rebase_preserved_preview_rows(
                        preserved_preview_rows,
                        start_ordinal=0,
                        created_at=created_at,
                    )
                    preview_rows.extend(rebased_preview_rows)
                    if writes_aggregate_artifacts:
                        preview_rows.append(
                            {
                                "rel_preview_path": segment_rel_path,
                                "preview_type": "html",
                                "target_fragment": conversation_preview_anchor(int(document["id"])),
                                "label": "segment",
                                "ordinal": len(preview_rows),
                                "created_at": created_at,
                            }
                        )
                        if writes_segment_artifacts:
                            preview_rows.append(
                                {
                                    "rel_preview_path": toc_rel_path,
                                    "preview_type": "html",
                                    "target_fragment": None,
                                    "label": "contents",
                                    "ordinal": len(preview_rows),
                                    "created_at": created_at,
                                }
                            )
                replace_document_preview_rows(
                    connection,
                    int(document["id"]),
                    preview_rows if not conversation_document_uses_entry_preview(document) else [*preview_rows, *rebased_preview_rows],
                )
        cleanup_unreferenced_preview_files(paths, connection, previous_preview_paths)
        refreshed += 1
    return refreshed


def mark_missing_documents(connection: sqlite3.Connection, scanned_rel_paths: set[str]) -> int:
    occurrence_rows = connection.execute(
        """
        SELECT id, document_id, rel_path, lifecycle_status
        FROM document_occurrences
        WHERE parent_occurrence_id IS NULL
          AND source_kind = ?
          AND lifecycle_status != 'deleted'
        """
    , (FILESYSTEM_SOURCE_KIND,)).fetchall()
    missing_occurrence_ids = [
        int(row["id"])
        for row in occurrence_rows
        if row["rel_path"] not in scanned_rel_paths and row["lifecycle_status"] != "missing"
    ]
    if not missing_occurrence_ids:
        return 0
    now = utc_now()
    placeholders = ", ".join("?" for _ in missing_occurrence_ids)
    connection.execute(
        f"""
        UPDATE document_occurrences
        SET lifecycle_status = 'missing', updated_at = ?
        WHERE lifecycle_status != 'deleted'
          AND (id IN ({placeholders}) OR parent_occurrence_id IN ({placeholders}))
        """,
        [now, *missing_occurrence_ids, *missing_occurrence_ids],
    )
    affected_document_ids = {
        int(row["document_id"])
        for row in connection.execute(
            f"""
            SELECT DISTINCT document_id
            FROM document_occurrences
            WHERE id IN ({placeholders}) OR parent_occurrence_id IN ({placeholders})
            """,
            [*missing_occurrence_ids, *missing_occurrence_ids],
        ).fetchall()
    }
    for document_id in affected_document_ids:
        refresh_source_backed_dataset_memberships_for_document(connection, document_id)
        refresh_document_from_occurrences(connection, document_id)
    connection.commit()
    return len(missing_occurrence_ids)


def select_attachment_match_candidate(
    buckets: dict[object, list[sqlite3.Row]],
    key: object,
    matched_ids: set[int],
) -> sqlite3.Row | None:
    candidates = buckets.get(key) or []
    while candidates and int(candidates[0]["id"]) in matched_ids:
        candidates.pop(0)
    if not candidates:
        return None
    candidate = candidates.pop(0)
    matched_ids.add(int(candidate["id"]))
    return candidate


def match_attachment_rows(
    existing_rows: list[sqlite3.Row],
    attachments: list[dict[str, object]],
) -> tuple[list[tuple[dict[str, object], sqlite3.Row | None]], list[sqlite3.Row]]:
    candidate_rows = [row for row in existing_rows if row["lifecycle_status"] != "deleted"]
    by_name_hash: dict[tuple[str | None, str], list[sqlite3.Row]] = defaultdict(list)
    by_hash: dict[str | None, list[sqlite3.Row]] = defaultdict(list)
    for row in candidate_rows:
        file_hash = row["file_hash"]
        file_name = str(row["file_name"] or "").lower()
        by_name_hash[(file_hash, file_name)].append(row)
        by_hash[file_hash].append(row)

    matched_ids: set[int] = set()
    matches: list[tuple[dict[str, object], sqlite3.Row | None]] = []
    for attachment in attachments:
        file_hash = attachment.get("file_hash")
        file_name = str(attachment.get("file_name") or "").lower()
        existing_row = select_attachment_match_candidate(by_name_hash, (file_hash, file_name), matched_ids)
        if existing_row is None:
            existing_row = select_attachment_match_candidate(by_hash, file_hash, matched_ids)
        matches.append((attachment, existing_row))

    removed_rows = [row for row in candidate_rows if int(row["id"]) not in matched_ids]
    return matches, removed_rows


def extract_attachment_document(path: Path) -> dict[str, object]:
    try:
        return extract_document(path, include_attachments=False)
    except Exception:
        return build_fallback_extract(path)


def extract_attachment_document_with_overrides(
    path: Path,
    attachment: dict[str, object],
) -> dict[str, object]:
    extracted = extract_attachment_document(path)
    drive_record = attachment.get("gmail_drive_record")
    if isinstance(drive_record, dict):
        extracted = apply_gmail_drive_export_metadata(
            dict(extracted),
            drive_record=dict(drive_record),
        )
    return extracted


def reconcile_attachment_documents(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    parent_document_id: int,
    parent_rel_path: str,
    control_number_batch: int,
    control_number_family_sequence: int,
    attachments: list[dict[str, object]],
    dataset_memberships: list[tuple[int, int | None]] | None = None,
) -> None:
    parent_row = connection.execute(
        "SELECT dataset_id, conversation_id, conversation_assignment_mode FROM documents WHERE id = ?",
        (parent_document_id,),
    ).fetchone()
    parent_occurrence = select_preferred_occurrence(active_occurrence_rows_for_document(connection, parent_document_id))
    parent_custodian = normalize_whitespace(str(parent_occurrence["custodian"] or "")) or None if parent_occurrence is not None else None
    parent_dataset_id = int(parent_row["dataset_id"]) if parent_row is not None and parent_row["dataset_id"] is not None else None
    parent_conversation_id = int(parent_row["conversation_id"]) if parent_row is not None and parent_row["conversation_id"] is not None else None
    parent_conversation_assignment_mode = (
        str(parent_row["conversation_assignment_mode"])
        if parent_row is not None and parent_row["conversation_assignment_mode"] is not None
        else CONVERSATION_ASSIGNMENT_MODE_AUTO
    )
    existing_rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE parent_document_id = ?
        ORDER BY id ASC
        """,
        (parent_document_id,),
    ).fetchall()
    active_occurrences_by_document_id: dict[int, sqlite3.Row] = {}
    if existing_rows:
        existing_document_ids = [int(row["id"]) for row in existing_rows]
        placeholders = ", ".join("?" for _ in existing_document_ids)
        occurrence_rows = connection.execute(
            f"""
            SELECT *
            FROM document_occurrences
            WHERE document_id IN ({placeholders})
              AND lifecycle_status != 'deleted'
            ORDER BY id ASC
            """,
            existing_document_ids,
        ).fetchall()
        grouped_occurrences: dict[int, list[sqlite3.Row]] = defaultdict(list)
        for occurrence_row in occurrence_rows:
            grouped_occurrences[int(occurrence_row["document_id"])].append(occurrence_row)
        active_occurrences_by_document_id = {
            document_id: select_preferred_occurrence(rows) or rows[0]
            for document_id, rows in grouped_occurrences.items()
            if rows
        }
    matches, removed_rows = match_attachment_rows(existing_rows, attachments)
    next_new_attachment_sequence = next_attachment_sequence(connection, parent_document_id)

    for attachment, existing_row in matches:
        if existing_row is None:
            attachment_sequence = next_new_attachment_sequence
            next_new_attachment_sequence += 1
            control_number = format_control_number(control_number_batch, control_number_family_sequence, attachment_sequence)
        else:
            attachment_sequence = int(existing_row["control_number_attachment_sequence"])
            control_number = str(existing_row["control_number"])
            cleanup_document_artifacts(paths, connection, existing_row)

        child_rel_path, child_path = write_attachment_blob(
            paths,
            parent_rel_path,
            control_number,
            str(attachment["file_name"]),
            bytes(attachment["payload"]),
        )
        extracted = apply_manual_locks(
            existing_row,
            extract_attachment_document_with_overrides(child_path, attachment),
        )
        document_id = upsert_document_row(
            connection,
            child_rel_path,
            child_path,
            existing_row,
            extracted,
            existing_occurrence_row=(
                active_occurrences_by_document_id.get(int(existing_row["id"]))
                if existing_row is not None
                else None
            ),
            file_name=str(attachment["file_name"]),
            parent_document_id=parent_document_id,
            control_number=control_number,
            dataset_id=parent_dataset_id,
            conversation_id=parent_conversation_id,
            conversation_assignment_mode=parent_conversation_assignment_mode,
            control_number_batch=control_number_batch,
            control_number_family_sequence=control_number_family_sequence,
            control_number_attachment_sequence=attachment_sequence,
            custodian_override=parent_custodian,
        )
        seed_source_text_revision_for_document(
            connection,
            paths,
            document_id=document_id,
            extracted=extracted,
            existing_row=existing_row,
        )
        preview_rows = write_preview_artifacts(paths, child_rel_path, list(extracted.get("preview_artifacts", [])))
        chunks = extracted_search_chunks(extracted)
        replace_document_related_rows(
            connection,
            document_id,
            extracted | {"file_name": str(attachment["file_name"])},
            chunks,
            preview_rows,
        )
        for membership_dataset_id, membership_source_id in dataset_memberships or []:
            ensure_dataset_document_membership(
                connection,
                dataset_id=membership_dataset_id,
                document_id=document_id,
                dataset_source_id=membership_source_id,
            )

    for row in removed_rows:
        cleanup_document_artifacts(paths, connection, row)
        delete_document_related_rows(connection, int(row["id"]))
        connection.execute(
            """
            UPDATE documents
            SET lifecycle_status = 'deleted', updated_at = ?
            WHERE id = ?
            """,
            (utc_now(), row["id"]),
        )
        connection.execute(
            """
            UPDATE document_occurrences
            SET lifecycle_status = 'deleted', updated_at = ?
            WHERE document_id = ?
            """,
            (utc_now(), row["id"]),
        )
    sync_document_attachment_preview_links(connection, paths, parent_document_id)


def get_container_source_row(
    connection: sqlite3.Connection,
    source_kind: str,
    source_rel_path: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM container_sources
        WHERE source_kind = ? AND source_rel_path = ?
        """,
        (source_kind, source_rel_path),
    ).fetchone()


def container_source_scan_completed(row: sqlite3.Row | None) -> bool:
    if row is None:
        return False
    started_at = parse_utc_timestamp(row["last_scan_started_at"])
    completed_at = parse_utc_timestamp(row["last_scan_completed_at"])
    return started_at is not None and completed_at is not None and completed_at >= started_at


def write_container_source_scan_started(
    connection: sqlite3.Connection,
    *,
    dataset_id: int | None,
    source_kind: str,
    source_rel_path: str,
    file_size: int | None,
    file_mtime: str | None,
    file_hash: str | None,
    scan_started_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO container_sources (
          dataset_id, source_kind, source_rel_path, file_size, file_mtime, file_hash, last_scan_started_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_rel_path) DO UPDATE SET
          dataset_id = excluded.dataset_id,
          source_kind = excluded.source_kind,
          file_size = excluded.file_size,
          file_mtime = excluded.file_mtime,
          file_hash = excluded.file_hash,
          last_scan_started_at = excluded.last_scan_started_at
        """,
        (dataset_id, source_kind, source_rel_path, file_size, file_mtime, file_hash, scan_started_at),
    )


def write_container_source_scan_completed(
    connection: sqlite3.Connection,
    *,
    dataset_id: int | None,
    source_kind: str,
    source_rel_path: str,
    file_size: int | None,
    file_mtime: str | None,
    file_hash: str | None,
    message_count: int,
    scan_started_at: str,
    scan_completed_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO container_sources (
          dataset_id, source_kind, source_rel_path, file_size, file_mtime, file_hash, message_count,
          last_scan_started_at, last_scan_completed_at, last_ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_rel_path) DO UPDATE SET
          dataset_id = excluded.dataset_id,
          source_kind = excluded.source_kind,
          file_size = excluded.file_size,
          file_mtime = excluded.file_mtime,
          file_hash = excluded.file_hash,
          message_count = excluded.message_count,
          last_scan_started_at = excluded.last_scan_started_at,
          last_scan_completed_at = excluded.last_scan_completed_at,
          last_ingested_at = excluded.last_ingested_at
        """,
        (
            dataset_id,
            source_kind,
            source_rel_path,
            file_size,
            file_mtime,
            file_hash,
            message_count,
            scan_started_at,
            scan_completed_at,
            scan_completed_at,
        ),
    )


def mark_container_source_documents_active(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
    seen_at: str,
) -> int:
    parent_rows = connection.execute(
        """
        SELECT id
        FROM documents
        WHERE parent_document_id IS NULL
          AND source_kind = ?
          AND source_rel_path = ?
          AND lifecycle_status != 'deleted'
        """,
        (source_kind, source_rel_path),
    ).fetchall()
    parent_ids = [int(row["id"]) for row in parent_rows]
    if not parent_ids:
        return 0
    placeholders = ", ".join("?" for _ in parent_ids)
    cursor = connection.execute(
        f"""
        UPDATE documents
        SET lifecycle_status = 'active', last_seen_at = ?, updated_at = ?
        WHERE lifecycle_status != 'deleted'
          AND (id IN ({placeholders}) OR parent_document_id IN ({placeholders}))
        """,
        [seen_at, seen_at, *parent_ids, *parent_ids],
    )
    return int(cursor.rowcount or 0)


def assign_dataset_to_container_documents(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
    dataset_id: int,
    dataset_source_id: int | None = None,
) -> None:
    connection.execute(
        """
        UPDATE documents
        SET dataset_id = ?
        WHERE (source_kind = ? AND source_rel_path = ?)
           OR parent_document_id IN (
                SELECT id
                FROM documents
                WHERE source_kind = ? AND source_rel_path = ?
           )
        """,
        (dataset_id, source_kind, source_rel_path, source_kind, source_rel_path),
    )
    parent_rows = connection.execute(
        """
        SELECT id
        FROM documents
        WHERE parent_document_id IS NULL
          AND source_kind = ?
          AND source_rel_path = ?
        ORDER BY id ASC
        """,
        (source_kind, source_rel_path),
    ).fetchall()
    for parent_row in parent_rows:
        ensure_dataset_document_membership(
            connection,
            dataset_id=dataset_id,
            document_id=int(parent_row["id"]),
            dataset_source_id=dataset_source_id,
        )
        child_rows = connection.execute(
            """
            SELECT id
            FROM documents
            WHERE parent_document_id = ?
            ORDER BY id ASC
            """,
            (parent_row["id"],),
        ).fetchall()
        for child_row in child_rows:
            ensure_dataset_document_membership(
                connection,
                dataset_id=dataset_id,
                document_id=int(child_row["id"]),
                dataset_source_id=dataset_source_id,
            )


def existing_container_rows_by_source_item(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
) -> dict[str, sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE parent_document_id IS NULL
          AND source_kind = ?
          AND source_rel_path = ?
          AND source_item_id IS NOT NULL
        ORDER BY id ASC
        """,
        (source_kind, source_rel_path),
    ).fetchall()
    return {str(row["source_item_id"]): row for row in rows}


def retire_unseen_container_messages(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    source_kind: str,
    source_rel_path: str,
    scan_started_at: str,
) -> int:
    parent_rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE parent_document_id IS NULL
          AND source_kind = ?
          AND source_rel_path = ?
          AND lifecycle_status != 'deleted'
          AND (last_seen_at IS NULL OR last_seen_at != ?)
        ORDER BY id ASC
        """,
        (source_kind, source_rel_path, scan_started_at),
    ).fetchall()
    if not parent_rows:
        return 0

    now = utc_now()
    for parent_row in parent_rows:
        child_rows = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE parent_document_id = ?
            ORDER BY id ASC
            """,
            (parent_row["id"],),
        ).fetchall()
        for child_row in child_rows:
            cleanup_document_artifacts(paths, connection, child_row)
            delete_document_related_rows(connection, int(child_row["id"]))
        cleanup_document_artifacts(paths, connection, parent_row)
        delete_document_related_rows(connection, int(parent_row["id"]))
        child_ids = [int(child_row["id"]) for child_row in child_rows]
        related_ids = [int(parent_row["id"]), *child_ids]
        placeholders = ", ".join("?" for _ in related_ids)
        connection.execute(
            f"""
            UPDATE documents
            SET lifecycle_status = 'deleted', updated_at = ?
            WHERE id IN ({placeholders})
            """,
            [now, *related_ids],
        )
    return len(parent_rows)


def mark_missing_container_documents(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    scanned_source_rel_paths: set[str],
) -> tuple[int, int]:
    source_rows = connection.execute(
        """
        SELECT source_rel_path
        FROM container_sources
        WHERE source_kind = ?
        ORDER BY source_rel_path ASC
        """,
        (source_kind,),
    ).fetchall()
    sources_missing = 0
    documents_missing = 0
    now = utc_now()
    for source_row in source_rows:
        source_rel_path = str(source_row["source_rel_path"])
        if source_rel_path in scanned_source_rel_paths:
            continue
        parent_rows = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE parent_document_id IS NULL
              AND source_kind = ?
              AND source_rel_path = ?
              AND lifecycle_status NOT IN ('missing', 'deleted')
            ORDER BY id ASC
            """,
            (source_kind, source_rel_path),
        ).fetchall()
        if not parent_rows:
            continue
        parent_ids = [int(row["id"]) for row in parent_rows]
        child_rows: list[sqlite3.Row] = []
        if parent_ids:
            placeholders = ", ".join("?" for _ in parent_ids)
            child_rows = connection.execute(
                f"""
                SELECT *
                FROM documents
                WHERE parent_document_id IN ({placeholders})
                  AND lifecycle_status NOT IN ('missing', 'deleted')
                ORDER BY id ASC
                """,
                parent_ids,
            ).fetchall()
        related_ids = [*parent_ids, *[int(row["id"]) for row in child_rows]]
        if not related_ids:
            continue
        placeholders = ", ".join("?" for _ in related_ids)
        connection.execute(
            f"""
            UPDATE documents
            SET lifecycle_status = 'missing', updated_at = ?
            WHERE id IN ({placeholders})
            """,
            [now, *related_ids],
        )
        sources_missing += 1
        documents_missing += len(related_ids)
    if sources_missing or documents_missing:
        connection.commit()
    return sources_missing, documents_missing


def prepare_container_message_item(
    source_rel_path: str,
    raw_message: dict[str, object],
    normalize_message,
) -> dict[str, object]:
    prepare_started = time.perf_counter()
    normalized = normalize_message(source_rel_path, raw_message)
    if normalized is None:
        return {
            "skip": True,
            "source_item_id": normalize_source_item_id(raw_message.get("source_item_id")),
            "prepare_ms": (time.perf_counter() - prepare_started) * 1000.0,
            "prepare_chunk_ms": 0.0,
        }
    extracted_payload = dict(normalized["extracted"])
    attachments = list(extracted_payload.get("attachments", []))
    extracted_payload.pop("attachments", None)
    chunk_started = time.perf_counter()
    prepared_chunks = extracted_search_chunks(extracted_payload)
    return {
        "skip": False,
        "rel_path": str(normalized["rel_path"]),
        "file_name": str(normalized["file_name"]),
        "file_hash": normalized.get("file_hash"),
        "source_item_id": str(normalized["source_item_id"]),
        "source_folder_path": (
            str(normalized["source_folder_path"])
            if normalized.get("source_folder_path") is not None
            else None
        ),
        "extracted_payload": extracted_payload,
        "attachments": attachments,
        "prepared_chunks": prepared_chunks,
        "prepare_ms": (time.perf_counter() - prepare_started) * 1000.0,
        "prepare_chunk_ms": (time.perf_counter() - chunk_started) * 1000.0,
    }


def iter_prepared_container_message_items(
    *,
    source_kind: str,
    source_rel_path: str,
    raw_messages: Iterator[dict[str, object]],
    normalize_message,
    staging_root: Path | None = None,
) -> Iterator[tuple[dict[str, object], float]]:
    effective_staging_root = staging_root
    if staging_root is not None:
        effective_staging_root = (
            Path(staging_root)
            / "container"
            / sanitize_storage_filename(source_kind)
            / sanitize_storage_filename(source_rel_path)
        )
    yield from iter_staged_prepared_items(
        raw_messages,
        prepare_item=lambda raw_message: prepare_container_message_item(
            source_rel_path,
            raw_message,
            normalize_message,
        ),
        config_benchmark_name="ingest_container_prepare_config",
        queue_done_benchmark_name="ingest_container_prepare_queue_done",
        spill_subdir_name="prepared-container",
        staging_root=effective_staging_root,
        prepare_workers=ingest_container_prepare_worker_count(),
    )


def commit_prepared_container_message(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    prepared_item: dict[str, object],
    existing_row: sqlite3.Row | None,
    *,
    current_ingestion_batch: int | None,
    dataset_id: int,
    dataset_source_id: int | None,
    source_kind: str,
    source_rel_path: str,
    file_type_override: str,
    scan_started_at: str,
) -> dict[str, object]:
    connection.execute("BEGIN")
    try:
        extracted = apply_manual_locks(existing_row, dict(prepared_item["extracted_payload"] or {}))
        attachments = list(prepared_item.get("attachments") or [])
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
            str(prepared_item["rel_path"]),
            None,
            existing_row,
            extracted,
            file_name=str(prepared_item["file_name"]),
            parent_document_id=None,
            control_number=control_number,
            dataset_id=dataset_id,
            control_number_batch=control_number_batch,
            control_number_family_sequence=control_number_family_sequence,
            control_number_attachment_sequence=control_number_attachment_sequence,
            source_kind=source_kind,
            source_rel_path=source_rel_path,
            source_item_id=str(prepared_item["source_item_id"]),
            source_folder_path=prepared_item.get("source_folder_path"),
            file_type_override=file_type_override,
            file_size_override=None,
            file_hash_override=(
                str(prepared_item["file_hash"])
                if prepared_item.get("file_hash") is not None
                else None
            ),
            ingested_at_override=scan_started_at,
            last_seen_at_override=scan_started_at,
            updated_at_override=scan_started_at,
        )
        replace_document_email_threading_row(
            connection,
            document_id=document_id,
            email_threading=extracted.get("email_threading"),
        )
        replace_document_chat_threading_row(
            connection,
            document_id=document_id,
            chat_threading=extracted.get("chat_threading"),
        )
        seed_source_text_revision_for_document(
            connection,
            paths,
            document_id=document_id,
            extracted=extracted,
            existing_row=existing_row,
            created_at=scan_started_at,
        )
        preview_rows = write_preview_artifacts(
            paths,
            str(prepared_item["rel_path"]),
            list(extracted.get("preview_artifacts", [])),
        )
        replace_document_related_rows(
            connection,
            document_id,
            extracted | {"file_name": str(prepared_item["file_name"])},
            list(prepared_item.get("prepared_chunks") or []),
            preview_rows,
        )
        ensure_dataset_document_membership(
            connection,
            dataset_id=dataset_id,
            document_id=document_id,
            dataset_source_id=dataset_source_id,
        )
        reconcile_attachment_documents(
            connection,
            paths,
            document_id,
            str(prepared_item["rel_path"]),
            control_number_batch,
            control_number_family_sequence,
            attachments,
            [(dataset_id, dataset_source_id)],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return {
        "action": "new" if existing_row is None else "updated",
        "current_ingestion_batch": current_ingestion_batch,
    }


def ingest_container_source(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    path: Path,
    source_rel_path: str,
    *,
    source_kind: str,
    scan_hash_salt: str,
    dataset_name: str,
    iter_messages,
    normalize_message,
    file_type_override: str,
    source_scan_hash_override: str | None = None,
    staging_root: Path | None = None,
) -> dict[str, object]:
    source_scan_hash = normalize_whitespace(str(source_scan_hash_override or "")) or sha256_text(
        f"{scan_hash_salt}:{sha256_file(path) or ''}"
    )
    transaction_was_open = connection.in_transaction
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=source_kind,
        source_locator=source_rel_path,
        dataset_name=dataset_name,
    )
    if not transaction_was_open and connection.in_transaction:
        # Source-backed dataset repair may create or reattach dataset metadata
        # for legacy workspaces. Flush that implicit transaction before the
        # per-source BEGIN blocks below.
        connection.commit()
    existing_source = get_container_source_row(connection, source_kind, source_rel_path)
    file_size = file_size_bytes(path)
    file_mtime = file_mtime_timestamp(path)
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
            source_kind=source_kind,
            source_rel_path=source_rel_path,
        )
    ):
        same_size = existing_source["file_size"] == file_size
        same_mtime = existing_source["file_mtime"] == file_mtime
        file_hash = source_scan_hash
        if (
            same_size
            and same_mtime
            and existing_source["file_hash"] == source_scan_hash
            and not container_email_documents_missing_threading(
                connection,
                source_kind=source_kind,
                source_rel_path=source_rel_path,
            )
        ):
            message_count = int(existing_source["message_count"] or 0)
            if message_count == 0:
                row = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM documents
                    WHERE parent_document_id IS NULL
                      AND source_kind = ?
                      AND source_rel_path = ?
                      AND lifecycle_status != 'deleted'
                    """,
                    (source_kind, source_rel_path),
                ).fetchone()
                message_count = int(row["count"] or 0)
            connection.execute("BEGIN")
            try:
                mark_container_source_documents_active(
                    connection,
                    source_kind=source_kind,
                    source_rel_path=source_rel_path,
                    seen_at=scan_started_at,
                )
                assign_dataset_to_container_documents(
                    connection,
                    source_kind=source_kind,
                    source_rel_path=source_rel_path,
                    dataset_id=dataset_id,
                    dataset_source_id=dataset_source_id,
                )
                write_container_source_scan_completed(
                    connection,
                    dataset_id=dataset_id,
                    source_kind=source_kind,
                    source_rel_path=source_rel_path,
                    file_size=file_size,
                    file_mtime=file_mtime,
                    file_hash=file_hash,
                    message_count=message_count,
                    scan_started_at=scan_started_at,
                    scan_completed_at=scan_started_at,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "action": "skipped",
                "container_sources_skipped": 1,
                "container_messages_created": 0,
                "container_messages_updated": 0,
                "container_messages_deleted": 0,
                "container_prepare_ms": 0.0,
                "container_chunk_ms": 0.0,
                "container_prepare_wait_ms": 0.0,
                "container_commit_ms": 0.0,
            }
        if (
            same_size
            and existing_source["file_hash"]
            and existing_source["file_hash"] == source_scan_hash
            and not container_email_documents_missing_threading(
                connection,
                source_kind=source_kind,
                source_rel_path=source_rel_path,
            )
        ):
            message_count = int(existing_source["message_count"] or 0)
            connection.execute("BEGIN")
            try:
                mark_container_source_documents_active(
                    connection,
                    source_kind=source_kind,
                    source_rel_path=source_rel_path,
                    seen_at=scan_started_at,
                )
                assign_dataset_to_container_documents(
                    connection,
                    source_kind=source_kind,
                    source_rel_path=source_rel_path,
                    dataset_id=dataset_id,
                    dataset_source_id=dataset_source_id,
                )
                write_container_source_scan_completed(
                    connection,
                    dataset_id=dataset_id,
                    source_kind=source_kind,
                    source_rel_path=source_rel_path,
                    file_size=file_size,
                    file_mtime=file_mtime,
                    file_hash=file_hash,
                    message_count=message_count,
                    scan_started_at=scan_started_at,
                    scan_completed_at=scan_started_at,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return {
                "action": "skipped",
                "container_sources_skipped": 1,
                "container_messages_created": 0,
                "container_messages_updated": 0,
                "container_messages_deleted": 0,
                "container_prepare_ms": 0.0,
                "container_chunk_ms": 0.0,
                "container_prepare_wait_ms": 0.0,
                "container_commit_ms": 0.0,
            }
    else:
        file_hash = source_scan_hash

    write_container_source_scan_started(
        connection,
        dataset_id=dataset_id,
        source_kind=source_kind,
        source_rel_path=source_rel_path,
        file_size=file_size,
        file_mtime=file_mtime,
        file_hash=file_hash,
        scan_started_at=scan_started_at,
    )
    connection.commit()

    existing_rows_by_source_item = existing_container_rows_by_source_item(
        connection,
        source_kind=source_kind,
        source_rel_path=source_rel_path,
    )
    current_ingestion_batch: int | None = None
    container_messages_created = 0
    container_messages_updated = 0
    message_count = 0
    container_prepare_ms = 0.0
    container_chunk_ms = 0.0
    container_prepare_wait_ms = 0.0
    container_commit_ms = 0.0

    for prepared_item, wait_ms in iter_prepared_container_message_items(
        source_kind=source_kind,
        source_rel_path=source_rel_path,
        raw_messages=iter_messages(path),
        normalize_message=normalize_message,
        staging_root=staging_root,
    ):
        container_prepare_wait_ms += wait_ms
        container_prepare_ms += float(prepared_item["prepare_ms"])
        container_chunk_ms += float(prepared_item.get("prepare_chunk_ms") or 0.0)
        if prepared_item.get("skip"):
            continue
        message_count += 1
        existing_row = existing_rows_by_source_item.get(str(prepared_item["source_item_id"]))
        commit_started = time.perf_counter()
        commit_result = commit_prepared_container_message(
            connection,
            paths,
            prepared_item,
            existing_row,
            current_ingestion_batch=current_ingestion_batch,
            dataset_id=dataset_id,
            dataset_source_id=dataset_source_id,
            source_kind=source_kind,
            source_rel_path=source_rel_path,
            file_type_override=file_type_override,
            scan_started_at=scan_started_at,
        )
        container_commit_ms += (time.perf_counter() - commit_started) * 1000.0
        current_ingestion_batch = commit_result["current_ingestion_batch"]
        if str(commit_result["action"]) == "new":
            container_messages_created += 1
        else:
            container_messages_updated += 1

    connection.execute("BEGIN")
    try:
        container_messages_deleted = retire_unseen_container_messages(
            connection,
            paths,
            source_kind=source_kind,
            source_rel_path=source_rel_path,
            scan_started_at=scan_started_at,
        )
        scan_completed_at = next_monotonic_utc_timestamp([scan_started_at])
        write_container_source_scan_completed(
            connection,
            dataset_id=dataset_id,
            source_kind=source_kind,
            source_rel_path=source_rel_path,
            file_size=file_size,
            file_mtime=file_mtime,
            file_hash=file_hash,
            message_count=message_count,
            scan_started_at=scan_started_at,
            scan_completed_at=scan_completed_at,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return {
        "action": "new" if existing_source is None else "updated",
        "container_sources_skipped": 0,
        "container_messages_created": container_messages_created,
        "container_messages_updated": container_messages_updated,
        "container_messages_deleted": container_messages_deleted,
        "container_prepare_ms": container_prepare_ms,
        "container_chunk_ms": container_chunk_ms,
        "container_prepare_wait_ms": container_prepare_wait_ms,
        "container_commit_ms": container_commit_ms,
    }


def mark_missing_pst_documents(
    connection: sqlite3.Connection,
    scanned_source_rel_paths: set[str],
) -> tuple[int, int]:
    return mark_missing_container_documents(
        connection,
        source_kind=PST_SOURCE_KIND,
        scanned_source_rel_paths=scanned_source_rel_paths,
    )


def mark_missing_mbox_documents(
    connection: sqlite3.Connection,
    scanned_source_rel_paths: set[str],
) -> tuple[int, int]:
    return mark_missing_container_documents(
        connection,
        source_kind=MBOX_SOURCE_KIND,
        scanned_source_rel_paths=scanned_source_rel_paths,
    )


def ingest_pst_source(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    path: Path,
    source_rel_path: str,
    *,
    message_metadata_by_source_item: dict[str, dict[str, object]] | None = None,
    message_match_records: list[dict[str, object]] | None = None,
    message_sidecar_hash: str | None = None,
    staging_root: Path | None = None,
) -> dict[str, object]:
    normalized_message_metadata = {
        str(key): dict(value)
        for key, value in dict(message_metadata_by_source_item or {}).items()
    }
    normalized_message_match_records = [
        dict(record)
        for record in list(message_match_records or [])
        if isinstance(record, dict)
    ]

    def normalize_enriched_pst_message(
        source_rel_path_for_message: str,
        message_dict: dict[str, object],
    ) -> dict[str, object] | None:
        normalized = normalize_pst_message(source_rel_path_for_message, message_dict)
        if normalized is None:
            return None
        message_metadata = select_pst_export_message_metadata(
            normalized,
            exact_metadata_by_source_item=normalized_message_metadata,
            message_match_records=normalized_message_match_records,
        )
        if not message_metadata:
            return normalized
        enriched = dict(normalized)
        enriched["extracted"] = apply_pst_export_message_metadata(
            dict(normalized["extracted"]),
            message_metadata=message_metadata,
        )
        enriched["file_hash"] = pst_export_enriched_message_file_hash(
            normalized.get("file_hash"),
            message_metadata=message_metadata,
        )
        return enriched

    pst_scan_hash_override = (
        sha256_json_value(
            {
                "pst_hash": sha256_file(path),
                "message_sidecar_hash": message_sidecar_hash,
                "sidecar_match_version": "pst-export-sidecar-v2",
                "source_rel_path": source_rel_path,
            }
        )
        if normalize_whitespace(str(message_sidecar_hash or ""))
        else None
    )
    # Salt the scan fingerprint so unchanged PSTs get one corrective reparse when
    # container-routing rules change (for example, when Teams/system folders are reclassified
    # or attachment naming/type inference improves for unnamed blobs).
    result = ingest_container_source(
        connection,
        paths,
        path,
        source_rel_path,
        source_kind=PST_SOURCE_KIND,
        scan_hash_salt="pst-ingest-v4",
        dataset_name=pst_dataset_name(source_rel_path),
        iter_messages=iter_pst_messages,
        normalize_message=normalize_enriched_pst_message,
        file_type_override=PST_SOURCE_KIND,
        source_scan_hash_override=pst_scan_hash_override,
        staging_root=staging_root,
    )
    return {
        "action": result["action"],
        "pst_sources_skipped": result["container_sources_skipped"],
        "pst_messages_created": result["container_messages_created"],
        "pst_messages_updated": result["container_messages_updated"],
        "pst_messages_deleted": result["container_messages_deleted"],
        "pst_prepare_ms": result["container_prepare_ms"],
        "pst_chunk_ms": result["container_chunk_ms"],
        "pst_prepare_wait_ms": result["container_prepare_wait_ms"],
        "pst_commit_ms": result["container_commit_ms"],
    }


def ingest_mbox_source(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    path: Path,
    source_rel_path: str,
    staging_root: Path | None = None,
) -> dict[str, object]:
    result = ingest_container_source(
        connection,
        paths,
        path,
        source_rel_path,
        source_kind=MBOX_SOURCE_KIND,
        scan_hash_salt="mbox-ingest-v1",
        dataset_name=mbox_dataset_name(source_rel_path),
        iter_messages=iter_mbox_messages,
        normalize_message=normalize_mbox_message,
        file_type_override=MBOX_SOURCE_KIND,
        staging_root=staging_root,
    )
    return {
        "action": result["action"],
        "mbox_sources_skipped": result["container_sources_skipped"],
        "mbox_messages_created": result["container_messages_created"],
        "mbox_messages_updated": result["container_messages_updated"],
        "mbox_messages_deleted": result["container_messages_deleted"],
        "mbox_prepare_ms": result["container_prepare_ms"],
        "mbox_chunk_ms": result["container_chunk_ms"],
        "mbox_prepare_wait_ms": result["container_prepare_wait_ms"],
        "mbox_commit_ms": result["container_commit_ms"],
    }


def remove_auto_filesystem_dataset_membership(
    connection: sqlite3.Connection,
    *,
    document_id: int,
) -> None:
    filesystem_source_row = get_dataset_source_row(
        connection,
        source_kind=FILESYSTEM_SOURCE_KIND,
        source_locator=filesystem_dataset_locator(),
    )
    if filesystem_source_row is None:
        return
    connection.execute(
        """
        DELETE FROM dataset_documents
        WHERE document_id = ?
          AND dataset_source_id = ?
        """,
        (document_id, int(filesystem_source_row["id"])),
    )


def retire_standalone_filesystem_documents_by_rel_paths(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    rel_paths: set[str],
) -> int:
    normalized_rel_paths = sorted(
        {
            normalize_whitespace(str(rel_path or ""))
            for rel_path in rel_paths
            if normalize_whitespace(str(rel_path or ""))
        }
    )
    if not normalized_rel_paths:
        return 0
    placeholders = ", ".join("?" for _ in normalized_rel_paths)
    rows = connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE parent_document_id IS NULL
          AND rel_path IN ({placeholders})
          AND COALESCE(source_kind, ?) = ?
          AND lifecycle_status != 'deleted'
        ORDER BY id ASC
        """,
        [*normalized_rel_paths, FILESYSTEM_SOURCE_KIND, FILESYSTEM_SOURCE_KIND],
    ).fetchall()
    retired = 0
    now = utc_now()
    for row in rows:
        child_rows = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE parent_document_id = ?
            ORDER BY id ASC
            """,
            (row["id"],),
        ).fetchall()
        for child_row in child_rows:
            cleanup_document_artifacts(paths, connection, child_row)
            delete_document_related_rows(connection, int(child_row["id"]))
        cleanup_document_artifacts(paths, connection, row)
        delete_document_related_rows(connection, int(row["id"]))
        related_ids = [int(row["id"]), *[int(child_row["id"]) for child_row in child_rows]]
        related_placeholders = ", ".join("?" for _ in related_ids)
        connection.execute(
            f"""
            UPDATE documents
            SET lifecycle_status = 'deleted', updated_at = ?
            WHERE id IN ({related_placeholders})
            """,
            [now, *related_ids],
        )
        retired += 1
    return retired


def ingest_gmail_export_root(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    root: Path,
    descriptor: dict[str, object],
    allowed_file_types: set[str] | None = None,
    staging_root: Path | None = None,
) -> dict[str, object]:
    all_mbox_paths = [Path(path) for path in list(descriptor.get("mbox_paths") or [])]
    if not all_mbox_paths:
        return {
            "new": 0,
            "updated": 0,
            "failed": 0,
            "scanned_files": 0,
            "mbox_sources_skipped": 0,
            "mbox_messages_created": 0,
            "mbox_messages_updated": 0,
            "mbox_messages_deleted": 0,
            "gmail_linked_documents_created": 0,
            "gmail_linked_documents_updated": 0,
            "scanned_filesystem_rel_paths": [],
            "scanned_mbox_source_rel_paths": [],
            "failures": [],
        }

    include_mbox_sources = allowed_file_types is None or MBOX_SOURCE_KIND in allowed_file_types
    selected_drive_file_types = (
        None
        if allowed_file_types is None
        else {file_type for file_type in allowed_file_types if file_type != MBOX_SOURCE_KIND}
    )
    mbox_paths = list(all_mbox_paths) if include_mbox_sources else []
    drive_documents: list[dict[str, object]] = []
    for raw_drive_record in list(descriptor.get("drive_documents") or []):
        drive_record = dict(raw_drive_record)
        file_path_value = drive_record.get("file_path")
        if not isinstance(file_path_value, Path):
            continue
        if include_mbox_sources and list(drive_record.get("linked_message_ids") or []):
            continue
        if (
            selected_drive_file_types is not None
            and normalize_extension(file_path_value) not in selected_drive_file_types
        ):
            continue
        drive_documents.append(drive_record)

    if not mbox_paths and not drive_documents:
        return {
            "new": 0,
            "updated": 0,
            "failed": 0,
            "scanned_files": 0,
            "mbox_sources_skipped": 0,
            "mbox_messages_created": 0,
            "mbox_messages_updated": 0,
            "mbox_messages_deleted": 0,
            "gmail_linked_documents_created": 0,
            "gmail_linked_documents_updated": 0,
            "scanned_filesystem_rel_paths": [],
            "scanned_mbox_source_rel_paths": [],
            "failures": [],
        }

    primary_source_rel_path = relative_document_path(root, all_mbox_paths[0])
    transaction_was_open = connection.in_transaction
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=MBOX_SOURCE_KIND,
        source_locator=primary_source_rel_path,
        dataset_name=mbox_dataset_name(primary_source_rel_path),
    )
    if not transaction_was_open and connection.in_transaction:
        # Gmail exports can repair legacy dataset-source rows before any of the
        # explicit per-source BEGIN blocks run.
        connection.commit()
    email_metadata_by_message_id = {
        str(key): dict(value)
        for key, value in dict(descriptor.get("email_metadata_by_message_id") or {}).items()
    }
    linked_drive_attachment_records_by_message_id = {
        str(key): [dict(item) for item in list(value)]
        for key, value in dict(descriptor.get("linked_drive_attachment_records_by_message_id") or {}).items()
    }
    linked_drive_records_by_message_id = {
        str(key): [dict(item) for item in list(value)]
        for key, value in dict(descriptor.get("linked_drive_records_by_message_id") or {}).items()
    }
    message_sidecar_hash = normalize_whitespace(str(descriptor.get("message_sidecar_hash") or "")) or None

    stats = {
        "new": 0,
        "updated": 0,
        "failed": 0,
        "scanned_files": 0,
        "mbox_sources_skipped": 0,
        "mbox_messages_created": 0,
        "mbox_messages_updated": 0,
        "mbox_messages_deleted": 0,
        "gmail_linked_documents_created": 0,
        "gmail_linked_documents_updated": 0,
    }
    failures: list[dict[str, str]] = []
    scanned_filesystem_rel_paths: set[str] = set()
    scanned_mbox_source_rel_paths: set[str] = set()
    current_ingestion_batch: int | None = None
    gmail_staging_root = None
    if staging_root is not None:
        gmail_staging_root = (
            Path(staging_root)
            / "gmail"
            / sanitize_storage_filename(relative_document_path(root, Path(descriptor["root"])))
        )

    for mbox_path in mbox_paths:
        source_rel_path = relative_document_path(root, mbox_path)
        scanned_mbox_source_rel_paths.add(source_rel_path)

        def normalize_gmail_message(source_rel_path_for_message: str, message_dict: dict[str, object]) -> dict[str, object]:
            normalized = normalize_mbox_message(source_rel_path_for_message, message_dict)
            message_id = gmail_normalized_message_lookup_key(
                message_dict.get("source_item_id") or normalized.get("source_item_id")
            )
            linked_drive_attachment_records = (
                list(linked_drive_attachment_records_by_message_id.get(message_id, []))
                if message_id is not None
                else []
            )
            linked_drive_records = (
                list(linked_drive_records_by_message_id.get(message_id, []))
                if message_id is not None
                else []
            )
            extracted = apply_gmail_email_export_metadata(
                dict(normalized["extracted"]),
                message_metadata=(email_metadata_by_message_id.get(message_id) if message_id is not None else None),
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
                message_metadata=(email_metadata_by_message_id.get(message_id) if message_id is not None else None),
                linked_drive_records=linked_drive_records,
                linked_drive_attachment_records=linked_drive_attachment_records,
            )
            return normalized

        mbox_scan_hash_override = sha256_json_value(
            {
                "mbox_hash": sha256_file(mbox_path),
                "message_sidecar_hash": message_sidecar_hash,
                "source_rel_path": source_rel_path,
            }
        )
        result = ingest_container_source(
            connection,
            paths,
            mbox_path,
            source_rel_path,
            source_kind=MBOX_SOURCE_KIND,
            scan_hash_salt="mbox-ingest-v2-gmail",
            dataset_name=mbox_dataset_name(source_rel_path),
            iter_messages=iter_mbox_messages,
            normalize_message=normalize_gmail_message,
            file_type_override=MBOX_SOURCE_KIND,
            source_scan_hash_override=mbox_scan_hash_override,
            staging_root=gmail_staging_root,
        )
        if result["action"] == "new":
            stats["new"] += 1
        elif result["action"] == "updated":
            stats["updated"] += 1
        stats["scanned_files"] += 1
        stats["mbox_sources_skipped"] += int(result["container_sources_skipped"])
        stats["mbox_messages_created"] += int(result["container_messages_created"])
        stats["mbox_messages_updated"] += int(result["container_messages_updated"])
        stats["mbox_messages_deleted"] += int(result["container_messages_deleted"])

    if include_mbox_sources:
        linked_drive_rel_paths = {
            relative_document_path(root, file_path)
            for records in linked_drive_attachment_records_by_message_id.values()
            for record in records
            if isinstance(file_path := record.get("file_path"), Path) and file_path.exists()
        }
        if linked_drive_rel_paths:
            connection.execute("BEGIN")
            try:
                retire_standalone_filesystem_documents_by_rel_paths(
                    connection,
                    paths,
                    rel_paths=linked_drive_rel_paths,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    for drive_record in drive_documents:
        file_path_value = drive_record.get("file_path")
        if not isinstance(file_path_value, Path) or not file_path_value.exists():
            continue
        rel_path = relative_document_path(root, file_path_value)
        scanned_filesystem_rel_paths.add(rel_path)
        existing_row = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE parent_document_id IS NULL
              AND rel_path = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (rel_path,),
        ).fetchone()
        connection.execute("BEGIN")
        try:
            extracted_payload = extract_document(file_path_value, include_attachments=True)
            attachments = list(extracted_payload.get("attachments", []))
            extracted_payload.pop("attachments", None)
            extracted_payload = apply_gmail_drive_export_metadata(
                dict(extracted_payload),
                drive_record=drive_record,
            )
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
                file_path_value,
                existing_row,
                extracted,
                file_name=file_path_value.name,
                parent_document_id=None,
                control_number=control_number,
                dataset_id=dataset_id,
                control_number_batch=control_number_batch,
                control_number_family_sequence=control_number_family_sequence,
                control_number_attachment_sequence=control_number_attachment_sequence,
                source_kind=FILESYSTEM_SOURCE_KIND,
                file_hash_override=gmail_drive_document_file_hash(file_path_value, drive_record),
            )
            replace_document_email_threading_row(
                connection,
                document_id=document_id,
                email_threading=extracted.get("email_threading"),
            )
            replace_document_chat_threading_row(
                connection,
                document_id=document_id,
                chat_threading=extracted.get("chat_threading"),
            )
            seed_source_text_revision_for_document(
                connection,
                paths,
                document_id=document_id,
                extracted=extracted,
                existing_row=existing_row,
            )
            preview_rows = write_preview_artifacts(paths, rel_path, list(extracted.get("preview_artifacts", [])))
            chunks = extracted_search_chunks(extracted)
            replace_document_related_rows(
                connection,
                document_id,
                extracted | {"file_name": file_path_value.name},
                chunks,
                preview_rows,
            )
            remove_auto_filesystem_dataset_membership(connection, document_id=document_id)
            ensure_dataset_document_membership(
                connection,
                dataset_id=dataset_id,
                document_id=document_id,
                dataset_source_id=dataset_source_id,
            )
            reconcile_attachment_documents(
                connection,
                paths,
                document_id,
                rel_path,
                control_number_batch,
                control_number_family_sequence,
                attachments,
                [(dataset_id, dataset_source_id)],
            )
            connection.commit()
            stats["scanned_files"] += 1
            if existing_row is None:
                stats["new"] += 1
                stats["gmail_linked_documents_created"] += 1
            else:
                stats["updated"] += 1
                stats["gmail_linked_documents_updated"] += 1
        except Exception as exc:
            connection.rollback()
            stats["failed"] += 1
            failures.append(
                {
                    "rel_path": rel_path,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    return {
        **stats,
        "scanned_filesystem_rel_paths": sorted(scanned_filesystem_rel_paths),
        "scanned_mbox_source_rel_paths": sorted(scanned_mbox_source_rel_paths),
        "failures": failures,
    }


def upsert_production_row(
    connection: sqlite3.Connection,
    *,
    dataset_id: int | None,
    rel_root: str,
    production_name: str,
    metadata_load_rel_path: str,
    image_load_rel_path: str | None,
    source_type: str,
) -> int:
    now = utc_now()
    existing_row = connection.execute(
        """
        SELECT id
        FROM productions
        WHERE rel_root = ?
        """,
        (rel_root,),
    ).fetchone()
    if existing_row is None:
        connection.execute(
            """
            INSERT INTO productions (
              dataset_id, rel_root, production_name, metadata_load_rel_path, image_load_rel_path, source_type, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (dataset_id, rel_root, production_name, metadata_load_rel_path, image_load_rel_path, source_type, now, now),
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    connection.execute(
        """
        UPDATE productions
        SET dataset_id = ?, production_name = ?, metadata_load_rel_path = ?, image_load_rel_path = ?, source_type = ?, updated_at = ?
        WHERE id = ?
        """,
        (dataset_id, production_name, metadata_load_rel_path, image_load_rel_path, source_type, now, existing_row["id"]),
    )
    return int(existing_row["id"])


def production_previewable_native(path: Path | None) -> Path | None:
    if path is None or not path.exists():
        return None
    file_type = normalize_extension(path)
    if file_type == "pdf":
        return path
    if file_type in SUPPORTED_FILE_TYPES:
        return path
    return None


def production_source_parts(
    workspace_root: Path,
    *,
    text_path: Path | None,
    image_paths: list[Path],
    native_path: Path | None,
) -> list[dict[str, object]]:
    parts: list[dict[str, object]] = []
    if text_path is not None and text_path.exists():
        parts.append(
            {
                "part_kind": "text",
                "rel_source_path": relative_document_path(workspace_root, text_path),
                "ordinal": 0,
                "label": "Linked text",
                "created_at": utc_now(),
            }
        )
    for index, image_path in enumerate(image_paths, start=1):
        if not image_path.exists():
            continue
        parts.append(
            {
                "part_kind": "image",
                "rel_source_path": relative_document_path(workspace_root, image_path),
                "ordinal": index,
                "label": f"Page {index}",
                "created_at": utc_now(),
            }
        )
    if native_path is not None and native_path.exists():
        parts.append(
            {
                "part_kind": "native",
                "rel_source_path": relative_document_path(workspace_root, native_path),
                "ordinal": 0,
                "label": native_path.name,
                "created_at": utc_now(),
            }
        )
    return parts


def build_production_extracted_payload(
    workspace_root: Path,
    *,
    production_name: str,
    control_number: str,
    begin_bates: str,
    end_bates: str,
    begin_attachment: str | None,
    end_attachment: str | None,
    text_path: Path | None,
    image_paths: list[Path],
    native_path: Path | None,
) -> dict[str, object]:
    text_content = ""
    text_status = "empty"
    if text_path is not None and text_path.exists():
        text_content, text_status, _ = decode_bytes(text_path.read_bytes())
        text_content = normalize_whitespace(text_content)
        if not text_content:
            text_status = "empty"
    email_headers = extract_email_like_headers(text_content)
    author = email_headers.get("author") if email_headers else None
    recipients = email_headers.get("recipients") if email_headers else None
    subject = email_headers.get("subject") if email_headers else None
    participants = extract_email_chain_participants(
        text_content,
        [author, recipients] if email_headers else None,
    ) or extract_chat_participants(text_content)
    file_type = normalize_extension(native_path) if native_path is not None else ""
    preferred_native = production_previewable_native(native_path)
    image_content_type = infer_content_type_from_extension(normalize_extension(image_paths[0])) if image_paths else None
    fallback_content_type = infer_content_type_from_extension(file_type) or image_content_type or "E-Doc"
    content_type_path = text_path or preferred_native or native_path or (image_paths[0] if image_paths else Path(f"{control_number}.txt"))
    content_type = (
        determine_content_type(
            content_type_path,
            text_content,
            email_headers=email_headers or None,
            explicit_content_type=fallback_content_type,
        )
        or fallback_content_type
    )
    page_images: list[dict[str, object]] = []
    for index, image_path in enumerate(image_paths, start=1):
        if not image_path.exists():
            continue
        data_url = image_path_data_url(image_path)
        if data_url is None:
            continue
        page_images.append({"label": f"Page {index}", "src": data_url})
    resolved_title = (email_headers.get("title") if email_headers else None) or infer_production_title(control_number, text_content, native_path)
    preview_artifacts: list[dict[str, object]] = []
    if preferred_native is None:
        preview_artifacts.append(
            {
                "file_name": f"{sanitize_storage_filename(control_number)}.html",
                "preview_type": "html",
                "label": "production",
                "ordinal": 0,
                "content": build_production_preview_html(
                    document_title=resolved_title,
                    control_number=control_number,
                    production_name=production_name,
                    begin_bates=begin_bates,
                    end_bates=end_bates,
                    begin_attachment=begin_attachment,
                    end_attachment=end_attachment,
                    text_content=text_content,
                    page_images=page_images,
                ),
            }
        )
    return {
        "page_count": len(image_paths) or None,
        "author": author,
        "content_type": content_type,
        "date_created": email_headers.get("date_created") if email_headers else None,
        "date_modified": None,
        "participants": participants,
        "title": resolved_title,
        "subject": subject,
        "recipients": recipients,
        "text_content": text_content,
        "text_status": text_status,
        "preview_artifacts": preview_artifacts,
        "preferred_native": preferred_native,
    }


def production_document_file_size(text_path: Path | None, image_paths: list[Path], native_path: Path | None) -> int | None:
    total = 0
    found = False
    for path in [text_path, native_path, *image_paths]:
        if path is None or not path.exists():
            continue
        total += path.stat().st_size
        found = True
    return total if found else None


def plan_production_record_work(
    workspace_root: Path,
    resolved_production_root: Path,
    signature: dict[str, object],
    metadata_rows: list[dict[str, str]],
    resolved_image_rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], set[str]]:
    plans: list[dict[str, object]] = []
    seen_control_numbers: set[str] = set()
    for record in metadata_rows:
        begin_bates = str(record.get("begin_bates") or "").strip()
        end_bates = str(record.get("end_bates") or begin_bates).strip()
        if not begin_bates:
            continue
        control_number = begin_bates
        seen_control_numbers.add(control_number)
        text_path = resolve_production_source_path(workspace_root, resolved_production_root, record.get("text_path"))
        native_path = resolve_production_source_path(workspace_root, resolved_production_root, record.get("native_path"))
        matching_image_paths = [
            Path(image_row["resolved_path"])
            for image_row in resolved_image_rows
            if image_row.get("resolved_path") is not None
            and bates_inclusive_contains(begin_bates, end_bates, image_row["page_bates"])
        ]
        plans.append(
            {
                "production_name": str(signature["production_name"]),
                "production_rel_root": str(signature["rel_root"]),
                "control_number": control_number,
                "begin_bates": begin_bates,
                "end_bates": end_bates,
                "begin_attachment": record.get("begin_attachment"),
                "end_attachment": record.get("end_attachment"),
                "text_path": text_path,
                "native_path": native_path,
                "matching_image_paths": matching_image_paths,
                "missing_linked_text": bool(record.get("text_path") and (text_path is None or not text_path.exists())),
                "missing_linked_images": bool(resolved_image_rows and not matching_image_paths),
                "missing_linked_natives": bool(record.get("native_path") and (native_path is None or not native_path.exists())),
            }
        )
    return plans, seen_control_numbers


def prepare_production_row_plan(
    workspace_root: Path,
    prepared_plan: dict[str, object],
) -> dict[str, object]:
    prepared_item = dict(prepared_plan)
    prepare_started = time.perf_counter()
    try:
        text_path = Path(prepared_plan["text_path"]) if prepared_plan.get("text_path") is not None else None
        native_path = Path(prepared_plan["native_path"]) if prepared_plan.get("native_path") is not None else None
        matching_image_paths = [Path(path) for path in list(prepared_plan.get("matching_image_paths") or [])]
        available_text_path = text_path if text_path is not None and text_path.exists() else None
        available_native_path = native_path if native_path is not None and native_path.exists() else None
        extracted_payload = build_production_extracted_payload(
            workspace_root,
            production_name=str(prepared_plan["production_name"]),
            control_number=str(prepared_plan["control_number"]),
            begin_bates=str(prepared_plan["begin_bates"]),
            end_bates=str(prepared_plan["end_bates"]),
            begin_attachment=prepared_plan.get("begin_attachment"),
            end_attachment=prepared_plan.get("end_attachment"),
            text_path=available_text_path,
            image_paths=matching_image_paths,
            native_path=available_native_path,
        )
        preferred_native = extracted_payload.pop("preferred_native", None)
        source_parts = production_source_parts(
            workspace_root,
            text_path=available_text_path,
            image_paths=matching_image_paths,
            native_path=available_native_path,
        )
        chunk_started = time.perf_counter()
        prepared_chunks = chunk_text(str(extracted_payload.get("text_content") or ""))
        rel_path = production_logical_rel_path(str(prepared_plan["production_rel_root"]), str(prepared_plan["control_number"])).as_posix()
        file_name = (
            (preferred_native.name if isinstance(preferred_native, Path) else None)
            or (available_native_path.name if available_native_path is not None else None)
            or f"{prepared_plan['control_number']}.production"
        )
        preferred_source_path = (
            preferred_native
            if isinstance(preferred_native, Path)
            else available_text_path
        )
        prepared_item["extracted_payload"] = extracted_payload
        prepared_item["preferred_native"] = preferred_native
        prepared_item["preferred_source_path"] = preferred_source_path
        prepared_item["source_parts"] = source_parts
        prepared_item["prepared_chunks"] = prepared_chunks
        prepared_item["prepare_chunk_ms"] = (time.perf_counter() - chunk_started) * 1000.0
        prepared_item["rel_path"] = rel_path
        prepared_item["file_name"] = file_name
        prepared_item["available_text_path"] = available_text_path
        prepared_item["available_native_path"] = available_native_path
        prepared_item["matching_image_paths"] = matching_image_paths
        prepared_item["file_type_override"] = (
            normalize_extension(preferred_native)
            if isinstance(preferred_native, Path)
            else (normalize_extension(native_path) if native_path is not None else None)
        )
        prepared_item["file_size_override"] = production_document_file_size(
            available_text_path,
            matching_image_paths,
            available_native_path,
        )
        prepared_item["file_hash_override"] = (
            sha256_file(preferred_native)
            if isinstance(preferred_native, Path)
            else (sha256_file(available_text_path) if available_text_path is not None else None)
        )
        prepared_item["prepare_error"] = None
    except Exception as exc:
        prepared_item["extracted_payload"] = None
        prepared_item["preferred_native"] = None
        prepared_item["preferred_source_path"] = None
        prepared_item["source_parts"] = []
        prepared_item["prepared_chunks"] = []
        prepared_item["prepare_chunk_ms"] = 0.0
        prepared_item["rel_path"] = production_logical_rel_path(
            str(prepared_plan["production_rel_root"]),
            str(prepared_plan["control_number"]),
        ).as_posix()
        prepared_item["file_name"] = f"{prepared_plan['control_number']}.production"
        prepared_item["available_text_path"] = None
        prepared_item["available_native_path"] = None
        prepared_item["matching_image_paths"] = []
        prepared_item["file_type_override"] = None
        prepared_item["file_size_override"] = None
        prepared_item["file_hash_override"] = None
        prepared_item["prepare_error"] = f"{type(exc).__name__}: {exc}"
    prepared_item["prepare_ms"] = (time.perf_counter() - prepare_started) * 1000.0
    return prepared_item


def iter_prepared_production_row_plans(
    workspace_root: Path,
    production_row_plans: list[dict[str, object]],
    staging_root: Path | None = None,
) -> Iterator[tuple[dict[str, object], float]]:
    effective_staging_root = staging_root
    if staging_root is not None and production_row_plans:
        effective_staging_root = (
            Path(staging_root)
            / "production"
            / sanitize_storage_filename(str(production_row_plans[0]["production_rel_root"]))
        )
    yield from iter_staged_prepared_items(
        production_row_plans,
        prepare_item=lambda plan: prepare_production_row_plan(workspace_root, plan),
        config_benchmark_name="ingest_production_prepare_config",
        queue_done_benchmark_name="ingest_production_prepare_queue_done",
        spill_subdir_name="prepared-production",
        staging_root=effective_staging_root,
    )


def commit_prepared_production_row(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    existing_row: sqlite3.Row | None,
    prepared_item: dict[str, object],
    *,
    dataset_id: int,
    dataset_source_id: int,
    production_id: int,
) -> dict[str, object]:
    control_number = str(prepared_item["control_number"])
    prepare_error = prepared_item.get("prepare_error")
    if prepare_error:
        return {
            "action": "failed",
            "control_number": control_number,
            "error": str(prepare_error),
            "page_images_linked": 0,
        }

    connection.execute("BEGIN")
    try:
        existing_signature = existing_production_row_signature(connection, existing_row)
        extracted = apply_manual_locks(existing_row, dict(prepared_item["extracted_payload"] or {}))
        desired_signature = production_row_signature(
            existing_row,
            rel_path=str(prepared_item["rel_path"]),
            file_name=str(prepared_item["file_name"]),
            source_kind=PRODUCTION_SOURCE_KIND,
            production_id=production_id,
            begin_bates=str(prepared_item["begin_bates"]),
            end_bates=str(prepared_item["end_bates"]),
            begin_attachment=prepared_item.get("begin_attachment"),
            end_attachment=prepared_item.get("end_attachment"),
            extracted=extracted,
            source_parts=list(prepared_item.get("source_parts", [])),
        )
        if existing_row is not None:
            cleanup_document_artifacts(paths, connection, existing_row)
        document_id = upsert_document_row(
            connection,
            str(prepared_item["rel_path"]),
            (
                prepared_item["preferred_source_path"]
                if isinstance(prepared_item.get("preferred_source_path"), Path)
                else None
            ),
            existing_row,
            extracted,
            file_name=str(prepared_item["file_name"]),
            parent_document_id=None,
            control_number=control_number,
            dataset_id=dataset_id,
            control_number_batch=None,
            control_number_family_sequence=None,
            control_number_attachment_sequence=None,
            source_kind=PRODUCTION_SOURCE_KIND,
            production_id=production_id,
            begin_bates=str(prepared_item["begin_bates"]),
            end_bates=str(prepared_item["end_bates"]),
            begin_attachment=prepared_item.get("begin_attachment"),
            end_attachment=prepared_item.get("end_attachment"),
            file_type_override=prepared_item.get("file_type_override"),
            file_size_override=prepared_item.get("file_size_override"),
            file_hash_override=prepared_item.get("file_hash_override"),
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
        preview_rows = write_preview_artifacts(paths, str(prepared_item["rel_path"]), list(extracted.get("preview_artifacts", [])))
        replace_document_related_rows(
            connection,
            document_id,
            extracted | {"file_name": str(prepared_item["file_name"])},
            list(prepared_item.get("prepared_chunks", [])),
            preview_rows,
        )
        replace_document_source_parts(connection, document_id, list(prepared_item.get("source_parts", [])))
        connection.commit()
        if existing_row is None:
            action = "created"
        elif existing_row["lifecycle_status"] == "active" and existing_signature == desired_signature:
            action = "unchanged"
        else:
            action = "updated"
        return {
            "action": action,
            "control_number": control_number,
            "page_images_linked": len(list(prepared_item.get("matching_image_paths", []))),
        }
    except Exception as exc:
        connection.rollback()
        return {
            "action": "failed",
            "control_number": control_number,
            "error": f"{type(exc).__name__}: {exc}",
            "page_images_linked": 0,
        }


def update_production_family_relationships(connection: sqlite3.Connection, production_id: int) -> int:
    rows = connection.execute(
        """
        SELECT id, control_number, begin_bates, end_bates, begin_attachment, end_attachment, parent_document_id
        FROM documents
        WHERE production_id = ?
          AND lifecycle_status != 'deleted'
        ORDER BY begin_bates ASC, control_number ASC, id ASC
        """,
        (production_id,),
    ).fetchall()
    parents: list[sqlite3.Row] = []
    for row in rows:
        if row["begin_attachment"] and row["end_attachment"] and row["control_number"] == row["begin_attachment"]:
            parents.append(row)
    updated = 0
    for row in rows:
        desired_parent_id = None
        row_begin = row["begin_bates"] or row["control_number"]
        row_end = row["end_bates"] or row["control_number"]
        for parent_row in parents:
            if int(parent_row["id"]) == int(row["id"]):
                continue
            if bates_ranges_overlap(parent_row["begin_attachment"], parent_row["end_attachment"], row_begin, row_end):
                desired_parent_id = int(parent_row["id"])
                break
        if row["parent_document_id"] == desired_parent_id:
            continue
        connection.execute(
            "UPDATE documents SET parent_document_id = ?, updated_at = ? WHERE id = ?",
            (desired_parent_id, utc_now(), row["id"]),
        )
        updated += 1
    return updated


def ingest_resolved_production_root(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    workspace_root: Path,
    resolved_production_root: Path,
    staging_root: Path | None = None,
) -> dict[str, object]:
    workspace_root = workspace_root.resolve()
    resolved_production_root = resolved_production_root.resolve()
    signature = production_signature_for_root(workspace_root, resolved_production_root)
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
        metadata_load_rel_path=relative_document_path(workspace_root, metadata_load_path),
        image_load_rel_path=relative_document_path(workspace_root, image_load_path) if image_load_path is not None else None,
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

    resolved_image_rows: list[dict[str, object]] = []
    for image_row in image_rows:
        resolved_path = resolve_production_source_path(workspace_root, resolved_production_root, image_row["image_path"])
        resolved_image_rows.append({**image_row, "resolved_path": resolved_path})
    production_row_plans, seen_control_numbers = plan_production_record_work(
        workspace_root,
        resolved_production_root,
        signature,
        list(metadata["rows"]),
        resolved_image_rows,
    )

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
    stats["docs_missing_linked_text"] = sum(int(plan["missing_linked_text"]) for plan in production_row_plans)
    stats["docs_missing_linked_images"] = sum(int(plan["missing_linked_images"]) for plan in production_row_plans)
    stats["docs_missing_linked_natives"] = sum(int(plan["missing_linked_natives"]) for plan in production_row_plans)

    prepare_ms = 0.0
    prepare_chunk_ms = 0.0
    prepare_wait_ms = 0.0
    commit_ms = 0.0
    row_loop_started = time.perf_counter()
    for prepared_item, wait_ms in iter_prepared_production_row_plans(
        workspace_root,
        production_row_plans,
        staging_root=staging_root,
    ):
        prepare_wait_ms += wait_ms
        prepare_ms += float(prepared_item.get("prepare_ms") or 0.0)
        prepare_chunk_ms += float(prepared_item.get("prepare_chunk_ms") or 0.0)
        control_number = str(prepared_item["control_number"])
        existing_row = existing_by_control_number.get(control_number)
        commit_started = time.perf_counter()
        commit_result = commit_prepared_production_row(
            connection,
            paths,
            existing_row,
            prepared_item,
            dataset_id=dataset_id,
            dataset_source_id=dataset_source_id,
            production_id=production_id,
        )
        commit_ms += (time.perf_counter() - commit_started) * 1000.0
        action = str(commit_result["action"])
        if action == "failed":
            failures.append(
                {
                    "control_number": control_number,
                    "error": str(commit_result.get("error") or "Unknown production ingest failure."),
                }
            )
            continue
        stats[action] += 1
        stats["page_images_linked"] += int(commit_result["page_images_linked"])
    benchmark_mark(
        "ingest_production_rows_done",
        row_count=len(production_row_plans),
        row_loop_ms=round((time.perf_counter() - row_loop_started) * 1000.0, 3),
        prepare_ms=round(prepare_ms, 3),
        prepare_chunk_ms=round(prepare_chunk_ms, 3),
        prepare_wait_ms=round(prepare_wait_ms, 3),
        commit_ms=round(commit_ms, 3),
        created=stats["created"],
        updated=stats["updated"],
        unchanged=stats["unchanged"],
        failed=len(failures),
    )

    retire_started = time.perf_counter()
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
    benchmark_mark(
        "ingest_production_retire_done",
        retire_ms=round((time.perf_counter() - retire_started) * 1000.0, 3),
        retired=stats["retired"],
    )

    finalize_started = time.perf_counter()
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
    benchmark_mark(
        "ingest_production_finalize_done",
        finalize_ms=round((time.perf_counter() - finalize_started) * 1000.0, 3),
        parent_link_updates=parent_link_updates,
        attachment_preview_updates=attachment_preview_updates,
        families_reconstructed=stats["families_reconstructed"],
    )

    return {
        "status": "ok",
        "workspace_root": str(workspace_root),
        "production_root": str(resolved_production_root),
        "production_rel_root": str(signature["rel_root"]),
        "production_name": str(signature["production_name"]),
        "production_id": production_id,
        "metadata_load_rel_path": relative_document_path(workspace_root, metadata_load_path),
        "image_load_rel_path": relative_document_path(workspace_root, image_load_path) if image_load_path is not None else None,
        "tool_version": TOOL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "failures": failures,
        **stats,
    }
