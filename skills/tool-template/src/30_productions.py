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
    return Path(".retriever") / "productions" / production_slug / "documents" / f"{control_slug}.logical"


def infer_production_title(control_number: str, text_content: str, native_path: Path | None) -> str:
    for line in text_content.splitlines():
        candidate = normalize_whitespace(line)
        if candidate:
            if re.match(r"^(From|To|Cc|Bcc|Sent|Date|Subject):\s*", candidate, flags=re.IGNORECASE):
                continue
            return candidate[:220]
    if native_path is not None:
        return native_path.stem or native_path.name
    return control_number


def build_production_preview_html(
    *,
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
        "Production": production_name,
        "Control Number": control_number,
        "Begin Bates": begin_bates,
        "End Bates": end_bates,
        "Begin Attachment": begin_attachment or "",
        "End Attachment": end_attachment or "",
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
    return build_html_preview(headers, body_html=body_html, document_title=f"{control_number} Preview", head_html=head_html)


def replace_document_related_rows(
    connection: sqlite3.Connection,
    document_id: int,
    metadata_values: dict[str, object],
    chunks: list[dict[str, object]],
    preview_rows: list[dict[str, object]],
) -> None:
    delete_document_related_rows(connection, document_id)

    if preview_rows:
        connection.executemany(
            """
            INSERT INTO document_previews (
              document_id, rel_preview_path, preview_type, label, ordinal, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    document_id,
                    row["rel_preview_path"],
                    row["preview_type"],
                    row["label"],
                    row["ordinal"],
                    row["created_at"],
                )
                for row in preview_rows
            ],
        )

    if chunks:
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

    connection.execute(
        """
        INSERT INTO documents_fts (document_id, file_name, title, subject, author, participants, recipients)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            document_id,
            metadata_values["file_name"],
            metadata_values["title"],
            metadata_values["subject"],
            metadata_values["author"],
            metadata_values["participants"],
            metadata_values["recipients"],
        ),
    )


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


def delete_document_related_rows(connection: sqlite3.Connection, document_id: int) -> None:
    connection.execute("DELETE FROM document_chunks WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM chunks_fts WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM document_previews WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM document_source_parts WHERE document_id = ?", (document_id,))
    connection.execute("DELETE FROM documents_fts WHERE document_id = ?", (document_id,))


def refresh_documents_fts_row(connection: sqlite3.Connection, document_id: int) -> None:
    connection.execute("DELETE FROM documents_fts WHERE document_id = ?", (document_id,))
    row = connection.execute(
        """
        SELECT id, file_name, title, subject, author, participants, recipients
        FROM documents
        WHERE id = ?
        """,
        (document_id,),
    ).fetchone()
    if row is None:
        return
    connection.execute(
        """
        INSERT INTO documents_fts (document_id, file_name, title, subject, author, participants, recipients)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["id"],
            row["file_name"],
            row["title"],
            row["subject"],
            row["author"],
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
    rel_path = Path(".retriever") / preview_rel_path
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
        remove_file_if_exists(paths["state_dir"] / preview_row["rel_preview_path"])
    if is_internal_rel_path(row["rel_path"]):
        remove_file_if_exists(paths["root"] / row["rel_path"])


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
    file_name: str,
    parent_document_id: int | None,
    control_number: str,
    control_number_batch: int | None,
    control_number_family_sequence: int | None,
    control_number_attachment_sequence: int | None,
    source_kind: str | None = None,
    source_rel_path: str | None = None,
    source_item_id: str | None = None,
    source_folder_path: str | None = None,
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
    common_values = {
        "control_number": control_number,
        "parent_document_id": parent_document_id,
        "source_kind": source_kind or (EMAIL_ATTACHMENT_SOURCE_KIND if parent_document_id is not None else FILESYSTEM_SOURCE_KIND),
        "source_rel_path": source_rel_path,
        "source_item_id": source_item_id,
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
              control_number, parent_document_id, source_kind, source_rel_path, source_item_id, source_folder_path,
              production_id, begin_bates, end_bates, begin_attachment, end_attachment,
              rel_path, file_name, file_type, file_size, page_count, author, date_created,
              content_type, date_modified, title, subject, participants, recipients, manual_field_locks_json, file_hash,
              content_hash, text_status, lifecycle_status, ingested_at, last_seen_at, updated_at,
              control_number_batch, control_number_family_sequence, control_number_attachment_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                common_values["control_number"],
                common_values["parent_document_id"],
                common_values["source_kind"],
                common_values["source_rel_path"],
                common_values["source_item_id"],
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
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    locked_value = existing_row[MANUAL_FIELD_LOCKS_COLUMN] or "[]"
    connection.execute(
        """
        UPDATE documents
        SET control_number = ?, parent_document_id = ?, source_kind = ?, source_rel_path = ?, source_item_id = ?, source_folder_path = ?,
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
            common_values["parent_document_id"],
            common_values["source_kind"],
            common_values["source_rel_path"],
            common_values["source_item_id"],
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
    return int(existing_row["id"])


def mark_seen_without_reingest(connection: sqlite3.Connection, row: sqlite3.Row) -> None:
    now = utc_now()
    connection.execute(
        """
        UPDATE documents
        SET lifecycle_status = 'active', last_seen_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (now, now, row["id"]),
    )
    connection.execute(
        """
        UPDATE documents
        SET lifecycle_status = 'active', last_seen_at = ?, updated_at = ?
        WHERE parent_document_id = ? AND lifecycle_status != 'deleted'
        """,
        (now, now, row["id"]),
    )


def mark_missing_documents(connection: sqlite3.Connection, scanned_rel_paths: set[str]) -> int:
    rows = connection.execute(
        """
        SELECT id, rel_path, lifecycle_status
        FROM documents
        WHERE parent_document_id IS NULL
          AND COALESCE(source_kind, ?) = ?
          AND lifecycle_status != 'deleted'
        """
    , (FILESYSTEM_SOURCE_KIND, FILESYSTEM_SOURCE_KIND)).fetchall()
    missing_ids = [row["id"] for row in rows if row["rel_path"] not in scanned_rel_paths and row["lifecycle_status"] != "missing"]
    if not missing_ids:
        return 0
    now = utc_now()
    placeholders = ", ".join("?" for _ in missing_ids)
    connection.execute(
        f"""
        UPDATE documents
        SET lifecycle_status = 'missing', updated_at = ?
        WHERE lifecycle_status != 'deleted'
          AND (id IN ({placeholders}) OR parent_document_id IN ({placeholders}))
        """,
        [now, *missing_ids, *missing_ids],
    )
    connection.commit()
    return len(missing_ids)


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


def reconcile_attachment_documents(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    parent_document_id: int,
    parent_rel_path: str,
    control_number_batch: int,
    control_number_family_sequence: int,
    attachments: list[dict[str, object]],
) -> None:
    existing_rows = connection.execute(
        """
        SELECT *
        FROM documents
        WHERE parent_document_id = ?
        ORDER BY id ASC
        """,
        (parent_document_id,),
    ).fetchall()
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
        extracted = apply_manual_locks(existing_row, extract_attachment_document(child_path))
        document_id = upsert_document_row(
            connection,
            child_rel_path,
            child_path,
            existing_row,
            extracted,
            file_name=str(attachment["file_name"]),
            parent_document_id=parent_document_id,
            control_number=control_number,
            control_number_batch=control_number_batch,
            control_number_family_sequence=control_number_family_sequence,
            control_number_attachment_sequence=attachment_sequence,
        )
        preview_rows = write_preview_artifacts(paths, child_rel_path, list(extracted.get("preview_artifacts", [])))
        chunks = chunk_text(str(extracted.get("text_content") or ""))
        replace_document_related_rows(
            connection,
            document_id,
            extracted | {"file_name": str(attachment["file_name"])},
            chunks,
            preview_rows,
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
          source_kind, source_rel_path, file_size, file_mtime, file_hash, last_scan_started_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_rel_path) DO UPDATE SET
          source_kind = excluded.source_kind,
          file_size = excluded.file_size,
          file_mtime = excluded.file_mtime,
          file_hash = excluded.file_hash,
          last_scan_started_at = excluded.last_scan_started_at
        """,
        (source_kind, source_rel_path, file_size, file_mtime, file_hash, scan_started_at),
    )


def write_container_source_scan_completed(
    connection: sqlite3.Connection,
    *,
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
          source_kind, source_rel_path, file_size, file_mtime, file_hash, message_count,
          last_scan_started_at, last_scan_completed_at, last_ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_rel_path) DO UPDATE SET
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


def existing_pst_rows_by_source_item(
    connection: sqlite3.Connection,
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
        (PST_SOURCE_KIND, source_rel_path),
    ).fetchall()
    return {str(row["source_item_id"]): row for row in rows}


def retire_pst_unseen_messages(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
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
        (PST_SOURCE_KIND, source_rel_path, scan_started_at),
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


def mark_missing_pst_documents(
    connection: sqlite3.Connection,
    scanned_source_rel_paths: set[str],
) -> tuple[int, int]:
    source_rows = connection.execute(
        """
        SELECT source_rel_path
        FROM container_sources
        WHERE source_kind = ?
        ORDER BY source_rel_path ASC
        """,
        (PST_SOURCE_KIND,),
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
            (PST_SOURCE_KIND, source_rel_path),
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


def ingest_pst_source(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    path: Path,
    source_rel_path: str,
) -> dict[str, object]:
    existing_source = get_container_source_row(connection, PST_SOURCE_KIND, source_rel_path)
    file_size = file_size_bytes(path)
    file_mtime = file_mtime_timestamp(path)
    scan_started_at = next_monotonic_utc_timestamp(
        [
            existing_source["last_scan_started_at"] if existing_source is not None else None,
            existing_source["last_scan_completed_at"] if existing_source is not None else None,
        ]
    )

    if existing_source is not None and container_source_scan_completed(existing_source):
        same_size = existing_source["file_size"] == file_size
        same_mtime = existing_source["file_mtime"] == file_mtime
        file_hash = existing_source["file_hash"]
        if same_size and same_mtime:
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
                    (PST_SOURCE_KIND, source_rel_path),
                ).fetchone()
                message_count = int(row["count"] or 0)
            connection.execute("BEGIN")
            try:
                mark_container_source_documents_active(
                    connection,
                    source_kind=PST_SOURCE_KIND,
                    source_rel_path=source_rel_path,
                    seen_at=scan_started_at,
                )
                write_container_source_scan_completed(
                    connection,
                    source_kind=PST_SOURCE_KIND,
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
                "pst_sources_skipped": 1,
                "pst_messages_created": 0,
                "pst_messages_updated": 0,
                "pst_messages_deleted": 0,
            }

        file_hash = sha256_file(path)
        if same_size and existing_source["file_hash"] and existing_source["file_hash"] == file_hash:
            message_count = int(existing_source["message_count"] or 0)
            connection.execute("BEGIN")
            try:
                mark_container_source_documents_active(
                    connection,
                    source_kind=PST_SOURCE_KIND,
                    source_rel_path=source_rel_path,
                    seen_at=scan_started_at,
                )
                write_container_source_scan_completed(
                    connection,
                    source_kind=PST_SOURCE_KIND,
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
                "pst_sources_skipped": 1,
                "pst_messages_created": 0,
                "pst_messages_updated": 0,
                "pst_messages_deleted": 0,
            }
    else:
        file_hash = sha256_file(path)

    write_container_source_scan_started(
        connection,
        source_kind=PST_SOURCE_KIND,
        source_rel_path=source_rel_path,
        file_size=file_size,
        file_mtime=file_mtime,
        file_hash=file_hash,
        scan_started_at=scan_started_at,
    )
    connection.commit()

    existing_rows_by_source_item = existing_pst_rows_by_source_item(connection, source_rel_path)
    current_ingestion_batch: int | None = None
    pst_messages_created = 0
    pst_messages_updated = 0
    message_count = 0

    for raw_message in iter_pst_messages(path):
        normalized = normalize_pst_message(source_rel_path, raw_message)
        message_count += 1
        existing_row = existing_rows_by_source_item.get(str(normalized["source_item_id"]))
        connection.execute("BEGIN")
        try:
            extracted = apply_manual_locks(existing_row, dict(normalized["extracted"]))
            attachments = list(extracted.get("attachments", []))
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
                str(normalized["rel_path"]),
                None,
                existing_row,
                extracted,
                file_name=str(normalized["file_name"]),
                parent_document_id=None,
                control_number=control_number,
                control_number_batch=control_number_batch,
                control_number_family_sequence=control_number_family_sequence,
                control_number_attachment_sequence=control_number_attachment_sequence,
                source_kind=PST_SOURCE_KIND,
                source_rel_path=source_rel_path,
                source_item_id=str(normalized["source_item_id"]),
                source_folder_path=(
                    str(normalized["source_folder_path"])
                    if normalized.get("source_folder_path") is not None
                    else None
                ),
                file_type_override=PST_SOURCE_KIND,
                file_size_override=None,
                file_hash_override=str(normalized["file_hash"]),
                ingested_at_override=scan_started_at,
                last_seen_at_override=scan_started_at,
                updated_at_override=scan_started_at,
            )
            preview_rows = write_preview_artifacts(paths, str(normalized["rel_path"]), list(extracted.get("preview_artifacts", [])))
            chunks = chunk_text(str(extracted.get("text_content") or ""))
            replace_document_related_rows(
                connection,
                document_id,
                extracted | {"file_name": str(normalized["file_name"])},
                chunks,
                preview_rows,
            )
            reconcile_attachment_documents(
                connection,
                paths,
                document_id,
                str(normalized["rel_path"]),
                control_number_batch,
                control_number_family_sequence,
                attachments,
            )
            connection.commit()
            if existing_row is None:
                pst_messages_created += 1
            else:
                pst_messages_updated += 1
        except Exception:
            connection.rollback()
            raise

    connection.execute("BEGIN")
    try:
        pst_messages_deleted = retire_pst_unseen_messages(connection, paths, source_rel_path, scan_started_at)
        scan_completed_at = next_monotonic_utc_timestamp([scan_started_at])
        write_container_source_scan_completed(
            connection,
            source_kind=PST_SOURCE_KIND,
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
        "pst_sources_skipped": 0,
        "pst_messages_created": pst_messages_created,
        "pst_messages_updated": pst_messages_updated,
        "pst_messages_deleted": pst_messages_deleted,
    }


def upsert_production_row(
    connection: sqlite3.Connection,
    *,
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
              rel_root, production_name, metadata_load_rel_path, image_load_rel_path, source_type, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (rel_root, production_name, metadata_load_rel_path, image_load_rel_path, source_type, now, now),
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    connection.execute(
        """
        UPDATE productions
        SET production_name = ?, metadata_load_rel_path = ?, image_load_rel_path = ?, source_type = ?, updated_at = ?
        WHERE id = ?
        """,
        (production_name, metadata_load_rel_path, image_load_rel_path, source_type, now, existing_row["id"]),
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
    preview_artifacts: list[dict[str, object]] = []
    if preferred_native is None:
        preview_artifacts.append(
            {
                "file_name": f"{sanitize_storage_filename(control_number)}.html",
                "preview_type": "html",
                "label": "production",
                "ordinal": 0,
                "content": build_production_preview_html(
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
        "title": (email_headers.get("title") if email_headers else None) or infer_production_title(control_number, text_content, native_path),
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
