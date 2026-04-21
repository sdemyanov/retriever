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
        relative_href = urllib_request.pathname2url(
            os.path.relpath(str(child_preview_path), start=str(parent_preview_path.parent))
        )
        detail = normalize_whitespace(str(child_row["control_number"] or ""))
        links.append(
            {
                "href": relative_href,
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
    custodian = metadata_values.get("custodian")
    if custodian is None:
        row = connection.execute(
            "SELECT custodian FROM documents WHERE id = ?",
            (document_id,),
        ).fetchone()
        custodian = row["custodian"] if row is not None else None

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
            custodian,
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
        SELECT id, file_name, title, subject, author, custodian, participants, recipients
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
            row["custodian"],
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
    effective_dataset_id = dataset_id
    if effective_dataset_id is None and existing_row is not None and existing_row["dataset_id"] is not None:
        effective_dataset_id = int(existing_row["dataset_id"])
    effective_conversation_id = conversation_id
    if effective_conversation_id is None and existing_row is not None and "conversation_id" in existing_row.keys():
        if existing_row["conversation_id"] is not None:
            effective_conversation_id = int(existing_row["conversation_id"])
    effective_conversation_assignment = effective_conversation_assignment_mode(
        conversation_assignment_mode
        if conversation_assignment_mode is not None
        else existing_row["conversation_assignment_mode"]
        if existing_row is not None and "conversation_assignment_mode" in existing_row.keys()
        else None
    )
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
        "conversation_id": effective_conversation_id,
        "conversation_assignment_mode": effective_conversation_assignment,
        "dataset_id": effective_dataset_id,
        "parent_document_id": parent_document_id,
        "child_document_kind": effective_child_kind,
        "source_kind": effective_source_kind,
        "source_rel_path": source_rel_path,
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
        "custodian": custodian,
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
              control_number, conversation_id, conversation_assignment_mode, dataset_id, parent_document_id, child_document_kind,
              source_kind, source_rel_path, source_item_id, root_message_key, source_folder_path,
              production_id, begin_bates, end_bates, begin_attachment, end_attachment,
              rel_path, file_name, file_type, file_size, page_count, author, custodian, date_created,
              content_type, date_modified, title, subject, participants, recipients, manual_field_locks_json, file_hash,
              content_hash, text_status, lifecycle_status, ingested_at, last_seen_at, updated_at,
              control_number_batch, control_number_family_sequence, control_number_attachment_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                common_values["control_number"],
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
                common_values["custodian"],
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
        SET control_number = ?, conversation_id = ?, conversation_assignment_mode = ?, dataset_id = ?, parent_document_id = ?, child_document_kind = ?,
            source_kind = ?, source_rel_path = ?, source_item_id = ?, root_message_key = ?, source_folder_path = ?,
            production_id = ?, begin_bates = ?, end_bates = ?, begin_attachment = ?, end_attachment = ?,
            rel_path = ?, file_name = ?, file_type = ?, file_size = ?, page_count = ?,
            author = ?, custodian = ?, content_type = ?, date_created = ?, date_modified = ?, title = ?, subject = ?,
            participants = ?, recipients = ?, file_hash = ?, content_hash = ?, text_status = ?, lifecycle_status = ?,
            ingested_at = ?, last_seen_at = ?, updated_at = ?, manual_field_locks_json = ?,
            control_number_batch = ?, control_number_family_sequence = ?, control_number_attachment_sequence = ?
        WHERE id = ?
        """,
        (
            common_values["control_number"],
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
            common_values["custodian"],
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


def mark_seen_without_reingest(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    dataset_id: int | None = None,
    dataset_source_id: int | None = None,
) -> None:
    now = utc_now()
    connection.execute(
        """
        UPDATE documents
        SET dataset_id = COALESCE(?, dataset_id), lifecycle_status = 'active', last_seen_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (dataset_id, now, now, row["id"]),
    )
    connection.execute(
        """
        UPDATE documents
        SET dataset_id = COALESCE(?, dataset_id), lifecycle_status = 'active', last_seen_at = ?, updated_at = ?
        WHERE parent_document_id = ? AND lifecycle_status != 'deleted'
        """,
        (dataset_id, now, now, row["id"]),
    )
    if dataset_id is not None:
        ensure_dataset_document_membership(
            connection,
            dataset_id=dataset_id,
            document_id=int(row["id"]),
            dataset_source_id=dataset_source_id,
        )
        child_rows = connection.execute(
            """
            SELECT id
            FROM documents
            WHERE parent_document_id = ? AND lifecycle_status != 'deleted'
            ORDER BY id ASC
            """,
            (row["id"],),
        ).fetchall()
        for child_row in child_rows:
            ensure_dataset_document_membership(
                connection,
                dataset_id=dataset_id,
                document_id=int(child_row["id"]),
                dataset_source_id=dataset_source_id,
            )


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
        WHERE d.source_kind = ?
          AND d.source_rel_path = ?
          AND d.parent_document_id IS NULL
          AND d.content_type = 'Email'
          AND d.lifecycle_status != 'deleted'
          AND det.document_id IS NULL
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
          d.custodian,
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
                "custodian": normalize_whitespace(str(row["custodian"] or "")) or None,
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
    custodian = document.get("custodian")
    if custodian:
        cluster["custodians"].add(custodian)
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
    root_candidates = (
        set(conversation_index_root_index.get(str(conversation_index_root), set()))
        if conversation_index_root
        else set()
    )
    topic_candidates = (
        set(conversation_topic_index.get(str(conversation_topic), set()))
        if conversation_topic
        else set()
    )
    if root_candidates and topic_candidates:
        intersection = root_candidates & topic_candidates
        chosen = choose_unique_cluster(intersection)
        if chosen is not None:
            return chosen
    chosen = choose_unique_cluster(root_candidates)
    if chosen is not None:
        return chosen
    return choose_unique_cluster(topic_candidates)


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
    document_custodian = document.get("custodian")
    scored_candidates: list[tuple[int, str, int]] = []
    for cluster_id in candidate_cluster_ids:
        cluster = clusters[cluster_id]
        cluster_participants = set(cluster["participant_keys"])
        overlap = len(cluster_participants & participant_keys)
        if overlap <= 0:
            continue
        cluster_custodians = set(cluster["custodians"])
        if document_custodian and cluster_custodians and any(
            custodian != document_custodian for custodian in cluster_custodians
        ):
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
    if conversation_topics:
        return f"outlook_topic:{conversation_topics[0]}"
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
        if cluster_id is None:
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
          d.custodian,
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
                "custodian": normalize_whitespace(str(row["custodian"] or "")) or None,
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
    participant_summary = render_display_name_list(list(cluster["participant_names"]), max_names=4)
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


def assign_supported_conversations(connection: sqlite3.Connection) -> dict[str, int]:
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
    metadata_items: list[str] = []
    for label, value in (
        ("Control number", document.get("control_number")),
        ("Created", timestamp_label),
        ("Participants", document.get("participants")),
        ("From", document.get("author")),
        ("To", document.get("recipients")),
        ("Source", document.get("source_rel_path")),
    ):
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
    header_actions = (
        f"<a class=\"conversation-permalink\" href=\"#{html.escape(anchor)}\">Permalink</a>"
        if current_segment_href
        else ""
    )
    return "".join(
        [
            f'<article class="conversation-document" id="{html.escape(anchor)}">',
            '<header class="conversation-document-header">',
            "<div>",
            f'<div class="conversation-document-kind">{html.escape(kind_label)}</div>',
            f"<h2>{html.escape(heading)}</h2>",
            "</div>",
            header_actions,
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
        "table { border-collapse: collapse; width: 100%; margin-bottom: 1.25rem; background: rgba(255,255,255,0.88); border: 1px solid #d7e0ea; border-radius: 16px; overflow: hidden; }"
        "th, td { text-align: left; padding: 0.55rem 0.75rem; border-bottom: 1px solid #e3e8ef; vertical-align: top; }"
        "th { width: 12rem; color: #516072; font-weight: 600; }"
        ".conversation-nav, .conversation-segments { display: grid; gap: 0.9rem; }"
        ".conversation-segment-card, .conversation-document { background: rgba(255,255,255,0.94); border: 1px solid #d7e0ea; border-radius: 18px; box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06); }"
        ".conversation-segment-card { padding: 1rem 1.1rem; }"
        ".conversation-segment-card h2 { margin: 0 0 0.35rem; font-size: 1.05rem; }"
        ".conversation-segment-card p { margin: 0 0 0.65rem; color: #516072; }"
        ".conversation-segment-card ul { margin: 0; padding-left: 1.15rem; }"
        ".conversation-segment-card li { margin: 0.28rem 0; }"
        ".conversation-segment-card a, .conversation-nav a, .conversation-document a { color: #0b63ce; text-decoration: none; }"
        ".conversation-nav { margin-bottom: 1rem; }"
        ".conversation-nav-links { display: flex; flex-wrap: wrap; gap: 0.75rem; align-items: center; color: #516072; }"
        ".conversation-document { padding: 1.1rem 1.15rem 1.15rem; margin-bottom: 1rem; scroll-margin-top: 1rem; }"
        ".conversation-document-header { display: flex; justify-content: space-between; gap: 1rem; align-items: flex-start; margin-bottom: 0.9rem; }"
        ".conversation-document-header h2 { margin: 0.2rem 0 0; font-size: 1.15rem; }"
        ".conversation-document-kind { font-size: 0.82rem; letter-spacing: 0.06em; text-transform: uppercase; color: #516072; }"
        ".conversation-permalink { white-space: nowrap; font-size: 0.92rem; }"
        ".conversation-document-meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 0.7rem 1rem; margin: 0 0 1rem; }"
        ".conversation-document-meta div { background: #f8fafc; border: 1px solid #e3e8ef; border-radius: 14px; padding: 0.65rem 0.75rem; }"
        ".conversation-document-meta dt { font-size: 0.8rem; font-weight: 600; color: #607080; margin-bottom: 0.18rem; }"
        ".conversation-document-meta dd { margin: 0; }"
        ".conversation-document-body pre { white-space: pre-wrap; word-break: break-word; margin: 0; background: #f8fafc; border: 1px solid #d7e0ea; border-radius: 14px; padding: 0.9rem 1rem; }"
        ".conversation-chat-transcript { display: grid; gap: 0.75rem; }"
        ".conversation-chat-message { display: flex; gap: 0.75rem; align-items: flex-start; border: 1px solid #d0d7de; border-radius: 14px; padding: 0.85rem 0.95rem; background: #f6f8fa; }"
        ".conversation-chat-main { min-width: 0; flex: 1 1 auto; }"
        ".conversation-chat-meta { display: flex; gap: 0.55rem; align-items: baseline; margin-bottom: 0.25rem; flex-wrap: wrap; }"
        ".conversation-chat-speaker { font-weight: 600; color: #0969da; }"
        ".conversation-chat-time { color: #57606a; font-size: 0.9rem; }"
        ".conversation-chat-body { white-space: pre-wrap; line-height: 1.45; }"
        ".conversation-raw-text { margin-top: 0.85rem; }"
        ".conversation-raw-text summary { cursor: pointer; color: #516072; }"
        "</style>"
    )


def build_conversation_toc_html(
    conversation_row: sqlite3.Row,
    *,
    documents: list[dict[str, object]],
    segment_items: list[dict[str, object]],
    doc_target_hrefs: dict[int, str],
) -> str:
    headers = {
        "Conversation": normalize_whitespace(str(conversation_row["display_name"] or "")) or f"Conversation {int(conversation_row['id'])}",
        "Type": normalize_whitespace(str(conversation_row["conversation_type"] or "")),
        "Documents": str(len(documents)),
        "Segments": str(len(segment_items)),
    }
    cards: list[str] = []
    for segment in segment_items:
        doc_links = "".join(
            f"<li><a href=\"{html.escape(doc_target_hrefs[int(document['id'])])}\">{html.escape(conversation_preview_document_heading(document))}</a></li>"
            for document in segment["documents"]
        )
        cards.append(
            "<section class=\"conversation-segment-card\">"
            f"<h2><a href=\"{html.escape(Path(str(segment['segment_rel_path'])).name)}\">{html.escape(str(segment['label']))}</a></h2>"
            f"<p>{len(segment['documents'])} document{'s' if len(segment['documents']) != 1 else ''}</p>"
            f"{'<ul>' + doc_links + '</ul>' if doc_links else ''}"
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
    previous_link = Path(str(segment_items[segment_index - 1]["segment_rel_path"])).name if segment_index > 0 else None
    next_link = Path(str(segment_items[segment_index + 1]["segment_rel_path"])).name if segment_index + 1 < segment_count else None
    headers = {
        "Conversation": normalize_whitespace(str(conversation_row["display_name"] or "")) or f"Conversation {int(conversation_row['id'])}",
        "Type": normalize_whitespace(str(conversation_row["conversation_type"] or "")),
        "Segment": segment_label,
        "Documents": str(len(segment_items[segment_index]["documents"])),
    }
    nav_links = ["<a href=\"index.html\">Contents</a>"]
    if previous_link:
        nav_links.append(f"<a href=\"{html.escape(previous_link)}\">Previous segment</a>")
    if next_link:
        nav_links.append(f"<a href=\"{html.escape(next_link)}\">Next segment</a>")
    sections = "".join(
        render_conversation_document_section(
            document,
            current_segment_href=current_file_name,
            doc_target_hrefs=doc_target_hrefs,
            attachment_links_by_document_id=attachment_links_by_document_id,
        )
        for document in segment_items[segment_index]["documents"]
    )
    return build_html_preview(
        headers,
        body_html=(
            "<main>"
            "<div class=\"conversation-nav\">"
            f"<div class=\"conversation-nav-links\">{' | '.join(nav_links)}</div>"
            "</div>"
            f"{sections}"
            "</main>"
        ),
        document_title=f"{headers['Conversation']} - {segment_label}",
        head_html=build_conversation_preview_head_html(),
        heading=segment_label,
    )


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
        doc_target_hrefs = {
            int(document["id"]): f"{Path(str(segment['segment_rel_path'])).name}#{conversation_preview_anchor(int(document['id']))}"
            for segment in segment_items
            for document in segment["documents"]
        }
        toc_rel_path = conversation_preview_toc_rel_path(conversation_id)
        toc_abs_path = paths["state_dir"] / toc_rel_path
        toc_abs_path.parent.mkdir(parents=True, exist_ok=True)
        toc_abs_path.write_text(
            build_conversation_toc_html(
                conversation_row,
                documents=documents,
                segment_items=segment_items,
                doc_target_hrefs=doc_target_hrefs,
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
        for segment in segment_items:
            segment["preview_row_template"] = {
                "rel_preview_path": str(segment["segment_rel_path"]),
                "preview_type": "html",
                "label": None,
                "ordinal": 0,
                "created_at": created_at,
            }
        toc_row = {
            "rel_preview_path": toc_rel_path,
            "preview_type": "html",
            "target_fragment": None,
            "label": "contents",
            "ordinal": 1,
            "created_at": created_at,
        }
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
        for segment in segment_items:
            for document in segment["documents"]:
                replace_document_preview_rows(
                    connection,
                    int(document["id"]),
                    [
                        segment["preview_row_template"] | {
                            "target_fragment": conversation_preview_anchor(int(document["id"])),
                        },
                        toc_row,
                    ],
                )
        cleanup_unreferenced_preview_files(paths, connection, previous_preview_paths)
        refreshed += 1
    return refreshed


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
    dataset_memberships: list[tuple[int, int | None]] | None = None,
) -> None:
    parent_row = connection.execute(
        "SELECT custodian, dataset_id, conversation_id, conversation_assignment_mode FROM documents WHERE id = ?",
        (parent_document_id,),
    ).fetchone()
    parent_custodian = (
        normalize_whitespace(str(parent_row["custodian"] or "")) if parent_row is not None else ""
    ) or None
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
        chunks = chunk_text(str(extracted.get("text_content") or ""))
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
) -> dict[str, object]:
    source_scan_hash = sha256_text(f"{scan_hash_salt}:{sha256_file(path) or ''}")
    dataset_id, dataset_source_id = ensure_source_backed_dataset(
        connection,
        source_kind=source_kind,
        source_locator=source_rel_path,
        dataset_name=dataset_name,
    )
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

    for raw_message in iter_messages(path):
        normalized = normalize_message(source_rel_path, raw_message)
        if normalized is None:
            continue
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
                dataset_id=dataset_id,
                control_number_batch=control_number_batch,
                control_number_family_sequence=control_number_family_sequence,
                control_number_attachment_sequence=control_number_attachment_sequence,
                source_kind=source_kind,
                source_rel_path=source_rel_path,
                source_item_id=str(normalized["source_item_id"]),
                source_folder_path=(
                    str(normalized["source_folder_path"])
                    if normalized.get("source_folder_path") is not None
                    else None
                ),
                file_type_override=file_type_override,
                file_size_override=None,
                file_hash_override=(
                    str(normalized["file_hash"])
                    if normalized.get("file_hash") is not None
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
            preview_rows = write_preview_artifacts(paths, str(normalized["rel_path"]), list(extracted.get("preview_artifacts", [])))
            chunks = chunk_text(str(extracted.get("text_content") or ""))
            replace_document_related_rows(
                connection,
                document_id,
                extracted | {"file_name": str(normalized["file_name"])},
                chunks,
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
                str(normalized["rel_path"]),
                control_number_batch,
                control_number_family_sequence,
                attachments,
                [(dataset_id, dataset_source_id)],
            )
            connection.commit()
            if existing_row is None:
                container_messages_created += 1
            else:
                container_messages_updated += 1
        except Exception:
            connection.rollback()
            raise

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
) -> dict[str, object]:
    # Salt the scan fingerprint so unchanged PSTs get one corrective reparse when
    # container-routing rules change (for example, when Teams/system folders are reclassified).
    result = ingest_container_source(
        connection,
        paths,
        path,
        source_rel_path,
        source_kind=PST_SOURCE_KIND,
        scan_hash_salt="pst-ingest-v3",
        dataset_name=pst_dataset_name(source_rel_path),
        iter_messages=iter_pst_messages,
        normalize_message=normalize_pst_message,
        file_type_override=PST_SOURCE_KIND,
    )
    return {
        "action": result["action"],
        "pst_sources_skipped": result["container_sources_skipped"],
        "pst_messages_created": result["container_messages_created"],
        "pst_messages_updated": result["container_messages_updated"],
        "pst_messages_deleted": result["container_messages_deleted"],
    }


def ingest_mbox_source(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    path: Path,
    source_rel_path: str,
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
    )
    return {
        "action": result["action"],
        "mbox_sources_skipped": result["container_sources_skipped"],
        "mbox_messages_created": result["container_messages_created"],
        "mbox_messages_updated": result["container_messages_updated"],
        "mbox_messages_deleted": result["container_messages_deleted"],
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
