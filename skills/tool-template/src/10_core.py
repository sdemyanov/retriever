def build_content_type_by_extension() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for content_type, raw_extensions in CONTENT_TYPE_EXTENSION_GROUPS:
        for extension in raw_extensions.split():
            normalized_extension = extension.strip().lower().strip(",")
            if normalized_extension:
                mapping.setdefault(normalized_extension, content_type)
    return mapping


CONTENT_TYPE_BY_EXTENSION = build_content_type_by_extension()

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS workspace_meta (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      schema_version INTEGER NOT NULL,
      tool_version TEXT NOT NULL,
      requirements_version TEXT NOT NULL,
      template_source TEXT NOT NULL,
      template_sha256 TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS documents (
      id INTEGER PRIMARY KEY,
      control_number TEXT UNIQUE,
      parent_document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
      source_kind TEXT,
      source_rel_path TEXT,
      source_item_id TEXT,
      source_folder_path TEXT,
      production_id INTEGER REFERENCES productions(id) ON DELETE SET NULL,
      begin_bates TEXT,
      end_bates TEXT,
      begin_attachment TEXT,
      end_attachment TEXT,
      rel_path TEXT NOT NULL UNIQUE,
      file_name TEXT NOT NULL,
      file_type TEXT,
      file_size INTEGER,
      page_count INTEGER,
      author TEXT,
      content_type TEXT,
      date_created TEXT,
      date_modified TEXT,
      title TEXT,
      subject TEXT,
      participants TEXT,
      recipients TEXT,
      manual_field_locks_json TEXT NOT NULL DEFAULT '[]',
      file_hash TEXT,
      content_hash TEXT,
      text_status TEXT NOT NULL DEFAULT 'ok',
      lifecycle_status TEXT NOT NULL DEFAULT 'active',
      ingested_at TEXT,
      last_seen_at TEXT,
      updated_at TEXT,
      control_number_batch INTEGER,
      control_number_family_sequence INTEGER,
      control_number_attachment_sequence INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS productions (
      id INTEGER PRIMARY KEY,
      rel_root TEXT NOT NULL UNIQUE,
      production_name TEXT NOT NULL,
      metadata_load_rel_path TEXT NOT NULL,
      image_load_rel_path TEXT,
      source_type TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_source_parts (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      part_kind TEXT NOT NULL,
      rel_source_path TEXT NOT NULL,
      ordinal INTEGER NOT NULL DEFAULT 0,
      label TEXT,
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS container_sources (
      id INTEGER PRIMARY KEY,
      source_kind TEXT NOT NULL,
      source_rel_path TEXT NOT NULL UNIQUE,
      file_size INTEGER,
      file_mtime TEXT,
      file_hash TEXT,
      message_count INTEGER,
      last_scan_started_at TEXT,
      last_scan_completed_at TEXT,
      last_ingested_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_previews (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      rel_preview_path TEXT NOT NULL,
      preview_type TEXT NOT NULL,
      label TEXT,
      ordinal INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_chunks (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      chunk_index INTEGER NOT NULL,
      char_start INTEGER NOT NULL,
      char_end INTEGER NOT NULL,
      token_estimate INTEGER,
      text_content TEXT NOT NULL,
      UNIQUE(document_id, chunk_index)
    )
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
      document_id UNINDEXED,
      file_name,
      title,
      subject,
      author,
      participants,
      recipients
    )
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
      chunk_id UNINDEXED,
      document_id UNINDEXED,
      text_content
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS custom_fields_registry (
      id INTEGER PRIMARY KEY,
      field_name TEXT NOT NULL UNIQUE,
      field_type TEXT NOT NULL,
      instruction TEXT,
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS control_number_batches (
      batch_number INTEGER PRIMARY KEY,
      next_family_sequence INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_documents_file_hash ON documents(file_hash)",
    "CREATE INDEX IF NOT EXISTS idx_documents_content_hash ON documents(content_hash)",
    "CREATE INDEX IF NOT EXISTS idx_documents_lifecycle_status ON documents(lifecycle_status)",
    "CREATE INDEX IF NOT EXISTS idx_document_source_parts_document_id ON document_source_parts(document_id, part_kind, ordinal)",
    "CREATE INDEX IF NOT EXISTS idx_previews_document_id ON document_previews(document_id, ordinal)",
    "CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON document_chunks(document_id, chunk_index)",
]


class RetrieverError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc_timestamp(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc)


def format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def next_monotonic_utc_timestamp(previous_values: list[object]) -> str:
    candidate = datetime.now(timezone.utc).replace(microsecond=0)
    parsed_values = [parsed for parsed in (parse_utc_timestamp(value) for value in previous_values) if parsed is not None]
    if parsed_values:
        latest = max(parsed_values)
        if candidate <= latest:
            candidate = latest + timedelta(seconds=1)
    return format_utc_timestamp(candidate)


def sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json_value(value: object) -> str:
    return sha256_text(json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


def run_command(command: list[str]) -> tuple[bool, str]:
    try:
        completed = subprocess.run(command, check=True, capture_output=True, text=True)
        return True, (completed.stdout or completed.stderr).strip()
    except Exception as exc:  # pragma: no cover - shell probe
        return False, f"{type(exc).__name__}: {exc}"


def workspace_paths(root: Path) -> dict[str, Path]:
    state_dir = root / ".retriever"
    return {
        "root": root,
        "state_dir": state_dir,
        "db_path": state_dir / "retriever.db",
        "previews_dir": state_dir / "previews",
        "bin_dir": state_dir / "bin",
        "tool_path": state_dir / "bin" / "retriever_tools.py",
        "backups_dir": state_dir / "bin" / "backups",
        "jobs_dir": state_dir / "jobs",
        "logs_dir": state_dir / "logs",
        "runtime_path": state_dir / "runtime.json",
    }


def ensure_layout(paths: dict[str, Path]) -> None:
    paths["state_dir"].mkdir(parents=True, exist_ok=True)
    for key in ("previews_dir", "bin_dir", "backups_dir", "jobs_dir", "logs_dir"):
        paths[key].mkdir(parents=True, exist_ok=True)


def sqlite_artifact_paths(db_path: Path) -> list[Path]:
    return [
        db_path,
        Path(f"{db_path}-journal"),
        Path(f"{db_path}-wal"),
        Path(f"{db_path}-shm"),
    ]


def stale_sqlite_artifact_paths(db_path: Path) -> list[Path]:
    db_exists = db_path.exists()
    sidecars = [path for path in sqlite_artifact_paths(db_path)[1:] if path.exists()]
    if db_exists:
        try:
            if db_path.stat().st_size == 0:
                return [db_path, *sidecars]
        except OSError:
            return [db_path, *sidecars]
        return []
    return sidecars


def remove_stale_sqlite_artifacts(db_path: Path) -> list[str]:
    removed: list[str] = []
    for path in stale_sqlite_artifact_paths(db_path):
        try:
            path.unlink()
            removed.append(str(path))
        except FileNotFoundError:
            continue
    return removed


def current_journal_mode(connection: sqlite3.Connection) -> str | None:
    row = connection.execute("PRAGMA journal_mode").fetchone()
    if row is None or row[0] in (None, ""):
        return None
    return str(row[0]).lower()


def set_journal_mode(connection: sqlite3.Connection, journal_mode: str) -> str | None:
    row = connection.execute(f"PRAGMA journal_mode = {journal_mode}").fetchone()
    if row is None or row[0] in (None, ""):
        return None
    return str(row[0]).lower()


def connect_db(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    wal_error: sqlite3.DatabaseError | None = None
    journal_mode = None
    try:
        journal_mode = set_journal_mode(connection, "WAL")
    except sqlite3.DatabaseError as exc:
        wal_error = exc
    if journal_mode != "wal":
        try:
            journal_mode = set_journal_mode(connection, "DELETE")
        except sqlite3.DatabaseError as exc:
            connection.close()
            if wal_error is None:
                raise RetrieverError(
                    f"Unable to configure SQLite journal mode for {db_path}: "
                    f"DELETE failed with {type(exc).__name__}: {exc}"
                ) from exc
            raise RetrieverError(
                f"Unable to configure SQLite journal mode for {db_path}: "
                f"WAL failed with {type(wal_error).__name__}: {wal_error}; "
                f"DELETE failed with {type(exc).__name__}: {exc}"
            ) from exc
    return connection


def file_size_bytes(path: Path) -> int | None:
    try:
        return path.stat().st_size if path.exists() else None
    except OSError:
        return None


def file_mtime_timestamp(path: Path) -> str | None:
    try:
        if not path.exists():
            return None
        stat_result = path.stat()
        return datetime.fromtimestamp(
            stat_result.st_mtime_ns / 1_000_000_000,
            timezone.utc,
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except OSError:
        return None


def table_info(connection: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
    return connection.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()


def table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {row["name"] for row in table_info(connection, table_name)}


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def rename_table_if_needed(connection: sqlite3.Connection, old_name: str, new_name: str) -> bool:
    if not table_exists(connection, old_name) or table_exists(connection, new_name):
        return False
    connection.execute(
        f"ALTER TABLE {quote_identifier(old_name)} RENAME TO {quote_identifier(new_name)}"
    )
    return True


def rename_column_if_needed(
    connection: sqlite3.Connection,
    table_name: str,
    old_name: str,
    new_name: str,
) -> bool:
    columns = table_columns(connection, table_name)
    if old_name not in columns or new_name in columns:
        return False
    connection.execute(
        f"ALTER TABLE {quote_identifier(table_name)} "
        f"RENAME COLUMN {quote_identifier(old_name)} TO {quote_identifier(new_name)}"
    )
    return True


def backfill_legacy_column(
    connection: sqlite3.Connection,
    table_name: str,
    old_name: str,
    new_name: str,
    *,
    treat_blank_as_missing: bool = False,
) -> bool:
    columns = table_columns(connection, table_name)
    if old_name not in columns or new_name not in columns:
        return False
    before = connection.total_changes
    where_clause = f"{quote_identifier(new_name)} IS NULL"
    if treat_blank_as_missing:
        where_clause = (
            f"{quote_identifier(new_name)} IS NULL "
            f"OR TRIM({quote_identifier(new_name)}) = ''"
        )
    connection.execute(
        f"""
        UPDATE {quote_identifier(table_name)}
        SET {quote_identifier(new_name)} = {quote_identifier(old_name)}
        WHERE {quote_identifier(old_name)} IS NOT NULL
          AND ({where_clause})
        """
    )
    return connection.total_changes != before


def document_inventory_counts(connection: sqlite3.Connection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN parent_document_id IS NULL AND lifecycle_status != 'deleted' THEN 1 ELSE 0 END), 0) AS parent_documents,
          COALESCE(SUM(CASE WHEN parent_document_id IS NULL AND lifecycle_status = 'missing' THEN 1 ELSE 0 END), 0) AS missing_parent_documents,
          COALESCE(SUM(CASE WHEN parent_document_id IS NOT NULL AND lifecycle_status != 'deleted' THEN 1 ELSE 0 END), 0) AS attachment_children,
          COALESCE(SUM(CASE WHEN lifecycle_status != 'deleted' THEN 1 ELSE 0 END), 0) AS documents_total
        FROM documents
        """
    ).fetchone()
    return {
        "parent_documents": int(row["parent_documents"]),
        "missing_parent_documents": int(row["missing_parent_documents"]),
        "attachment_children": int(row["attachment_children"]),
        "documents_total": int(row["documents_total"]),
    }


def backfill_source_kinds(connection: sqlite3.Connection) -> int:
    if "source_kind" not in table_columns(connection, "documents"):
        return 0
    cursor = connection.execute(
        """
        UPDATE documents
        SET source_kind = CASE
          WHEN production_id IS NOT NULL THEN ?
          WHEN parent_document_id IS NOT NULL THEN ?
          ELSE ?
        END
        WHERE source_kind IS NULL OR TRIM(source_kind) = ''
        """,
        (PRODUCTION_SOURCE_KIND, EMAIL_ATTACHMENT_SOURCE_KIND, FILESYSTEM_SOURCE_KIND),
    )
    return int(cursor.rowcount or 0)


def ensure_column(connection: sqlite3.Connection, table_name: str, column_definition: str) -> None:
    column_name = column_definition.split()[0]
    if column_name in table_columns(connection, table_name):
        return
    connection.execute(f"ALTER TABLE {quote_identifier(table_name)} ADD COLUMN {column_definition}")


def normalize_string_list(raw_value: object) -> list[str]:
    if raw_value in (None, ""):
        return []
    if isinstance(raw_value, (list, tuple)):
        values = raw_value
    else:
        try:
            values = json.loads(raw_value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    if not isinstance(values, list):
        return []

    normalized: list[str] = []
    for item in values:
        if not isinstance(item, str):
            continue
        value = item.strip()
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_extension(path: Path) -> str:
    return path.suffix.lower().lstrip(".")


def format_control_number(
    batch_number: int,
    family_sequence: int,
    attachment_sequence: int | None = None,
) -> str:
    base = f"{CONTROL_NUMBER_PREFIX}{batch_number:0{CONTROL_NUMBER_BATCH_WIDTH}d}.{family_sequence:0{CONTROL_NUMBER_FAMILY_WIDTH}d}"
    if attachment_sequence is None:
        return base
    return f"{base}.{attachment_sequence:0{CONTROL_NUMBER_ATTACHMENT_WIDTH}d}"


def parse_control_number(control_number: object) -> tuple[int, int, int | None] | None:
    if not isinstance(control_number, str):
        return None
    match = re.fullmatch(
        rf"{CONTROL_NUMBER_PREFIX}(\d{{{CONTROL_NUMBER_BATCH_WIDTH}}})\.(\d{{{CONTROL_NUMBER_FAMILY_WIDTH}}})(?:\.(\d{{{CONTROL_NUMBER_ATTACHMENT_WIDTH}}}))?",
        control_number.strip(),
    )
    if not match:
        return None
    attachment_sequence = int(match.group(3)) if match.group(3) is not None else None
    return int(match.group(1)), int(match.group(2)), attachment_sequence


def parse_bates_identifier(value: object) -> dict[str, object] | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    match = re.fullmatch(r"(?P<prefix>.*?)(?P<number>\d+)$", normalized)
    if match is None:
        return None
    prefix = match.group("prefix")
    number_text = match.group("number")
    return {
        "raw": normalized,
        "prefix": prefix,
        "prefix_normalized": prefix.strip().upper(),
        "number": int(number_text),
        "width": len(number_text),
    }


def bates_series_key(parsed: dict[str, object] | None) -> tuple[str, int] | None:
    if not parsed:
        return None
    return (str(parsed["prefix_normalized"]), int(parsed["width"]))


def bates_range_compatible(left: dict[str, object] | None, right: dict[str, object] | None) -> bool:
    left_key = bates_series_key(left)
    right_key = bates_series_key(right)
    return left_key is not None and left_key == right_key


def bates_inclusive_contains(
    begin_value: object,
    end_value: object,
    query_value: object,
) -> bool:
    begin = parse_bates_identifier(begin_value)
    end = parse_bates_identifier(end_value)
    query = parse_bates_identifier(query_value)
    if not bates_range_compatible(begin, end) or not bates_range_compatible(begin, query):
        return False
    assert begin is not None and end is not None and query is not None
    return int(begin["number"]) <= int(query["number"]) <= int(end["number"])


def bates_ranges_overlap(
    begin_value: object,
    end_value: object,
    query_begin_value: object,
    query_end_value: object,
) -> bool:
    begin = parse_bates_identifier(begin_value)
    end = parse_bates_identifier(end_value)
    query_begin = parse_bates_identifier(query_begin_value)
    query_end = parse_bates_identifier(query_end_value)
    if not all((bates_range_compatible(begin, end), bates_range_compatible(query_begin, query_end), bates_range_compatible(begin, query_begin))):
        return False
    assert begin is not None and end is not None and query_begin is not None and query_end is not None
    return int(begin["number"]) <= int(query_end["number"]) and int(end["number"]) >= int(query_begin["number"])


def bates_sort_key(value: object) -> tuple[int, str, int, str]:
    parsed = parse_bates_identifier(value)
    if parsed is None:
        return (1, "", 0, str(value or ""))
    return (0, str(parsed["prefix_normalized"]), int(parsed["number"]), str(parsed["raw"]))


def parse_bates_query(query: str) -> tuple[str, str] | tuple[None, None]:
    stripped = query.strip()
    if not stripped:
        return None, None
    range_match = re.fullmatch(r"\s*(\S+)\s*[-–]\s*(\S+)\s*", stripped)
    if range_match:
        left = range_match.group(1)
        right = range_match.group(2)
        if parse_bates_identifier(left) and parse_bates_identifier(right):
            return left, right
    if " " not in stripped and parse_bates_identifier(stripped):
        return stripped, stripped
    return None, None


def normalize_internal_rel_path(path: Path) -> str:
    normalized = path.as_posix()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lstrip("/")


def is_internal_rel_path(rel_path: str | None) -> bool:
    if not rel_path:
        return False
    return normalize_internal_rel_path(Path(rel_path)).startswith(".retriever/")


def normalize_source_item_id(value: object) -> str:
    text = normalize_whitespace(str(value or ""))
    if not text:
        raise RetrieverError("Container-derived documents require a stable source item id.")
    return text


def encode_source_item_id_for_path(source_item_id: str) -> str:
    encoded = base64.urlsafe_b64encode(normalize_source_item_id(source_item_id).encode("utf-8")).decode("ascii")
    return encoded.rstrip("=") or "item"


def container_source_rel_path_from_message_rel_path(rel_path: str | None) -> str | None:
    if not rel_path:
        return None
    normalized = normalize_internal_rel_path(Path(rel_path))
    parts = Path(normalized).parts
    if len(parts) < 5 or parts[0] != ".retriever" or parts[1] != "sources":
        return None
    try:
        messages_index = parts.index("messages")
    except ValueError:
        return None
    if messages_index <= 2:
        return None
    return Path(*parts[2:messages_index]).as_posix()


def pst_message_rel_path(source_rel_path: str, source_item_id: str) -> str:
    encoded = encode_source_item_id_for_path(source_item_id)
    return (
        Path(".retriever")
        / "sources"
        / Path(source_rel_path)
        / "messages"
        / f"{encoded}.pstmsg"
    ).as_posix()


def pst_preview_file_name(source_item_id: str) -> str:
    return f"{encode_source_item_id_for_path(source_item_id)}.html"


def pst_message_file_name(source_item_id: str) -> str:
    return f"{encode_source_item_id_for_path(source_item_id)}.pstmsg"


def sanitize_storage_filename(file_name: str) -> str:
    sanitized = re.sub(r"[\\/:*?\"<>|\x00-\x1f]+", "_", file_name.strip())
    sanitized = sanitized.strip().strip(".")
    return sanitized or "attachment.bin"


def infer_content_type_from_extension(file_type: str) -> str | None:
    if not file_type:
        return None
    if file_type == "md":
        return "E-Doc"
    return CONTENT_TYPE_BY_EXTENSION.get(file_type)


def infer_registry_field_type(sqlite_type: str | None) -> str:
    type_name = (sqlite_type or "").upper()
    if "INT" in type_name:
        return "integer"
    if any(marker in type_name for marker in ("REAL", "FLOA", "DOUB")):
        return "real"
    return "text"


def sanitize_field_name(field_name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", field_name.strip()).strip("_").lower()
    if not sanitized:
        raise RetrieverError("Field name becomes empty after sanitization.")
    if sanitized[0].isdigit():
        sanitized = f"field_{sanitized}"
    if sanitized in BUILTIN_FIELD_TYPES:
        raise RetrieverError(f"Field name '{sanitized}' conflicts with a built-in document column.")
    return sanitized


def parse_pdf_date(value: object) -> str | None:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.startswith("D:"):
        raw = raw[2:]
    if not re.fullmatch(r"\d{4}(?:\d{2}){0,5}(?:Z|[+\-]\d{2}'?\d{2}'?)?", raw):
        return None
    match = re.match(
        r"^(?P<year>\d{4})(?P<month>\d{2})?(?P<day>\d{2})?(?P<hour>\d{2})?(?P<minute>\d{2})?(?P<second>\d{2})?",
        raw,
    )
    if not match:
        return None
    parts = match.groupdict(default=None)
    month = int(parts["month"] or "1")
    day = int(parts["day"] or "1")
    hour = int(parts["hour"] or "0")
    minute = int(parts["minute"] or "0")
    second = int(parts["second"] or "0")
    try:
        dt = datetime(int(parts["year"]), month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError:
        return None
    return dt.isoformat().replace("+00:00", "Z")


def parse_iso_datetime(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None
    candidate = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        try:
            parsed_date = date.fromisoformat(raw)
        except ValueError:
            return None
        dt = datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_datetime(value: object) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        parsed = parse_iso_datetime(value)
        if parsed is not None:
            return parsed
        parsed = parse_pdf_date(value)
        if parsed is not None:
            return parsed
        for fmt in (
            "%m/%d/%Y %I:%M:%S %p",
            "%m/%d/%Y %I:%M %p",
            "%m/%d/%y %I:%M:%S %p",
            "%m/%d/%y %I:%M %p",
        ):
            try:
                dt = datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except ValueError:
                pass
        try:
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            return value.strip() or None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return None


def decode_bytes(data: bytes, declared_encoding: str | None = None) -> tuple[str, str, str | None]:
    if declared_encoding:
        try:
            return data.decode(declared_encoding), "ok", declared_encoding
        except Exception:
            pass

    try:
        return data.decode("utf-8"), "ok", "utf-8"
    except UnicodeDecodeError:
        pass

    if charset_normalizer is not None:
        best = charset_normalizer.from_bytes(data).best()
        if best is not None:
            text = str(best)
            status = "partial" if "\ufffd" in text else "ok"
            return text, status, best.encoding

    text = data.decode("utf-8", errors="replace")
    status = "partial" if "\ufffd" in text else "ok"
    return text, status, None


def strip_html_tags(text: str) -> str:
    without_scripts = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    without_tags = re.sub(r"(?s)<[^>]+>", " ", without_scripts)
    return normalize_whitespace(html.unescape(without_tags))


def normalize_participant_token(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = normalize_whitespace(value)
    normalized = re.sub(r"\s*<\s*", " <", normalized)
    normalized = re.sub(r"\s*>\s*", ">", normalized)
    normalized = normalized.strip(" ,;")
    return normalized or None


def append_unique_participants(
    participants: list[str],
    seen: set[str],
    raw_values: list[str | None],
) -> None:
    for raw_value in raw_values:
        if not raw_value:
            continue
        normalized_candidate_text = normalize_participant_token(raw_value)
        if not normalized_candidate_text:
            continue

        if "@" not in normalized_candidate_text:
            for raw_part in re.split(r"\s*;\s*|\n+", normalized_candidate_text):
                rendered = normalize_participant_token(raw_part)
                if not rendered:
                    continue
                key = rendered.lower()
                if key not in seen:
                    seen.add(key)
                    participants.append(rendered)
            continue

        parsed_values = getaddresses([normalized_candidate_text.replace(";", ",")])
        for display_name, email_address in parsed_values:
            normalized_name = normalize_participant_token(display_name)
            normalized_email = normalize_participant_token(email_address.lower() if email_address else None)
            if normalized_email and "@" in normalized_email:
                rendered = f"{normalized_name} <{normalized_email}>" if normalized_name and normalized_name.lower() != normalized_email else normalized_email
            elif normalized_name and not normalized_email:
                rendered = normalized_name
            else:
                rendered = None
            if not rendered:
                continue
            key = rendered.lower()
            if key not in seen:
                seen.add(key)
                participants.append(rendered)


def email_headers_to_metadata(headers: dict[str, str]) -> dict[str, str | None]:
    recipients = ", ".join(headers[key] for key in ("to", "cc", "bcc") if headers.get(key)) or None
    subject = headers.get("subject") or None
    participants: list[str] = []
    seen: set[str] = set()
    append_unique_participants(
        participants,
        seen,
        [headers.get("from"), headers.get("to"), headers.get("cc"), headers.get("bcc")],
    )
    return {
        "author": headers.get("from") or None,
        "recipients": recipients,
        "participants": ", ".join(participants) or None,
        "date_created": normalize_datetime(headers.get("sent") or headers.get("date")),
        "subject": subject,
        "title": subject,
    }


def extract_email_header_blocks(text: str, max_lines: int | None = None) -> list[dict[str, str]]:
    if not text:
        return []

    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    recognized_keys = {"from", "to", "cc", "bcc", "sent", "date", "subject"}
    blocks: list[dict[str, str]] = []
    headers: dict[str, str] = {}
    current_key: str | None = None
    started = False

    def flush_headers() -> None:
        nonlocal headers, current_key, started
        if "from" in headers and any(key in headers for key in ("to", "cc", "bcc", "subject", "sent", "date")):
            blocks.append(dict(headers))
        headers = {}
        current_key = None
        started = False

    for raw_line in lines[: max_lines or len(lines)]:
        stripped = raw_line.strip()
        if not stripped:
            if started and len(headers) >= 2:
                flush_headers()
            continue

        match = re.match(r"^(From|To|Cc|Bcc|Sent|Date|Subject):\s*(.*)$", stripped, flags=re.IGNORECASE)
        if match:
            key = match.group(1).lower()
            if started and key == "from" and len(headers) >= 2:
                flush_headers()
            current_key = key
            headers[current_key] = normalize_whitespace(match.group(2))
            started = True
            continue

        if started and current_key and raw_line != raw_line.lstrip():
            headers[current_key] = normalize_whitespace(f"{headers.get(current_key, '')} {stripped}")
            continue

        if started and len(headers) >= 2:
            flush_headers()

    if started:
        flush_headers()

    return [block for block in blocks if set(block).issubset(recognized_keys)]


def extract_email_like_headers(text: str) -> dict[str, str | None]:
    blocks = extract_email_header_blocks(text, max_lines=60)
    if not blocks:
        return {}
    return email_headers_to_metadata(blocks[0])


def extract_email_chain_participants(
    text: str,
    initial_values: list[str | None] | None = None,
) -> str | None:
    participants: list[str] = []
    seen: set[str] = set()
    append_unique_participants(participants, seen, list(initial_values or []))
    for headers in extract_email_header_blocks(text):
        append_unique_participants(
            participants,
            seen,
            [headers.get("from"), headers.get("to"), headers.get("cc"), headers.get("bcc")],
        )
    return ", ".join(participants) or None


def extract_chat_participants(text: str) -> str | None:
    if not text:
        return None

    participants: list[str] = []
    seen: set[str] = set()
    speaker_counts: dict[str, int] = {}
    line_pattern_candidates = (
        r"^\[[^\]]{1,40}\]\s*([^:\n]{2,80}?):\s+\S",
        r"^(?:\d{1,2}[:.]\d{2}(?::\d{2})?\s*(?:AM|PM)?\s+)?([^:\n]{2,80}?):\s+\S",
        r"^([^-\n]{2,80}?)\s+-\s+\d{1,2}[:.]\d{2}(?::\d{2})?\s*(?:AM|PM)?\b",
    )
    blocked = {"from", "to", "cc", "bcc", "sent", "date", "subject"}

    for raw_line in text.splitlines()[:800]:
        stripped = raw_line.strip()
        if not stripped or len(stripped) > 240:
            continue
        for pattern in line_pattern_candidates:
            match = re.match(pattern, stripped, flags=re.IGNORECASE)
            if not match:
                continue
            candidate = normalize_participant_token(match.group(1))
            if not candidate:
                continue
            lowered = candidate.lower().strip("[]()")
            if lowered in blocked or len(candidate.split()) > 8:
                continue
            key = lowered
            speaker_counts[key] = speaker_counts.get(key, 0) + 1
            if key not in seen:
                seen.add(key)
                participants.append(candidate)
            break

    total_matches = sum(speaker_counts.values())
    if total_matches < 2:
        return None
    if len(participants) < 2 and total_matches < 3:
        return None
    return ", ".join(participants)


def infer_content_type_from_content(
    file_type: str,
    text_content: str,
    email_headers: dict[str, str | None] | None = None,
) -> str | None:
    if email_headers:
        return "Email"
    if not text_content:
        return None

    leading_text = text_content[:4000].upper()
    if "BEGIN:VCALENDAR" in leading_text or "BEGIN:VEVENT" in leading_text:
        return "Calendar"
    if file_type in {"xml"} and "<VCALENDAR" in leading_text:
        return "Calendar"
    return None


def determine_content_type(
    path: Path,
    text_content: str,
    email_headers: dict[str, str | None] | None = None,
    explicit_content_type: str | None = None,
) -> str | None:
    file_type = normalize_extension(path)
    return (
        infer_content_type_from_content(file_type, text_content, email_headers)
        or explicit_content_type
        or infer_content_type_from_extension(file_type)
    )


def dependency_guard(module: object | None, package_name: str, file_type: str) -> None:
    if module is None:
        raise RetrieverError(
            f"Missing dependency for .{file_type} parsing: install {package_name} before ingesting this file type."
        )


def build_html_preview(
    headers: dict[str, str],
    body_html: str | None = None,
    body_text: str | None = None,
    *,
    document_title: str = "Retriever Preview",
    head_html: str | None = None,
) -> str:
    header_html = "".join(
        f"<tr><th>{html.escape(key)}</th><td>{html.escape(value)}</td></tr>"
        for key, value in headers.items()
        if value
    )
    if body_html:
        body_section = body_html
    else:
        body_section = f"<pre>{html.escape(body_text or '')}</pre>"
    return (
        "<!DOCTYPE html>"
        "<html><head>"
        '<meta charset="utf-8"/>'
        f"<title>{html.escape(document_title)}</title>"
        f"{head_html or ''}"
        "</head><body>"
        "<h1>Retriever Preview</h1>"
        "<table>"
        f"{header_html}"
        "</table>"
        "<hr/>"
        f"{body_section}"
        "</body></html>"
    )


def parse_xml_document(data: bytes) -> ET.Element:
    return ET.fromstring(data)


def ooxml_relationship_part_name(part_name: str) -> str:
    directory, file_name = posixpath.split(part_name)
    return posixpath.join(directory, "_rels", f"{file_name}.rels")


def normalize_ooxml_target(base_part: str, target: str) -> str:
    return posixpath.normpath(posixpath.join(posixpath.dirname(base_part), target))


def read_ooxml_relationships(archive: zipfile.ZipFile, part_name: str) -> dict[str, dict[str, str]]:
    rels_part = ooxml_relationship_part_name(part_name)
    try:
        root = parse_xml_document(archive.read(rels_part))
    except KeyError:
        return {}
    relationships: dict[str, dict[str, str]] = {}
    for relationship in root.findall("rels:Relationship", OOXML_RELATIONSHIP_NS):
        rel_id = relationship.attrib.get("Id")
        target = relationship.attrib.get("Target")
        rel_type = relationship.attrib.get("Type")
        if rel_id and target and rel_type:
            relationships[rel_id] = {
                "target": normalize_ooxml_target(part_name, target),
                "type": rel_type,
            }
    return relationships


def xml_local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def pptx_shape_position(element: ET.Element) -> tuple[int, int]:
    for query in ("./p:spPr/a:xfrm/a:off", "./p:xfrm/a:off", "./p:grpSpPr/a:xfrm/a:off"):
        offset = element.find(query, PPTX_NAMESPACES)
        if offset is not None:
            x = int(offset.attrib.get("x", "0") or "0")
            y = int(offset.attrib.get("y", "0") or "0")
            return x, y
    return 0, 0


def pptx_shape_size(element: ET.Element) -> tuple[int, int]:
    for query in ("./p:spPr/a:xfrm/a:ext", "./p:xfrm/a:ext", "./p:grpSpPr/a:xfrm/a:ext"):
        extent = element.find(query, PPTX_NAMESPACES)
        if extent is not None:
            cx = int(extent.attrib.get("cx", "0") or "0")
            cy = int(extent.attrib.get("cy", "0") or "0")
            return cx, cy
    return 0, 0


def pptx_shape_placeholder_type(element: ET.Element) -> str | None:
    for query in (
        "./p:nvSpPr/p:nvPr/p:ph",
        "./p:nvGraphicFramePr/p:nvPr/p:ph",
        "./p:nvGrpSpPr/p:nvPr/p:ph",
    ):
        placeholder = element.find(query, PPTX_NAMESPACES)
        if placeholder is not None:
            return placeholder.attrib.get("type") or "body"
    return None


def pptx_paragraph_text(paragraph: ET.Element) -> str:
    parts = [text_node.text or "" for text_node in paragraph.findall(".//a:t", PPTX_NAMESPACES)]
    return normalize_whitespace("".join(parts))


def ooxml_image_mime_type(part_name: str) -> str | None:
    suffix = Path(part_name).suffix.lower()
    if suffix == ".png":
        return "image/png"
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".gif":
        return "image/gif"
    if suffix == ".bmp":
        return "image/bmp"
    if suffix == ".webp":
        return "image/webp"
    if suffix in {".tif", ".tiff"}:
        return "image/tiff"
    return None


def image_path_data_url(path: Path) -> str | None:
    mime_type, _ = mimetypes.guess_type(path.name)
    normalized_suffix = path.suffix.lower()
    if normalized_suffix in {".tif", ".tiff"}:
        if PilImage is None:
            return None
        with PilImage.open(path) as image:
            converted = image.convert("RGB")
            buffer = io.BytesIO()
            converted.save(buffer, format="PNG")
        return f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('ascii')}"
    if mime_type is None or not mime_type.startswith("image/"):
        return None
    return f"data:{mime_type};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def pptx_picture_entry(
    element: ET.Element,
    *,
    archive: zipfile.ZipFile,
    relationships: dict[str, dict[str, str]],
) -> dict[str, object] | None:
    blip = element.find(".//a:blip", PPTX_NAMESPACES)
    relationship_id = blip.attrib.get(f"{{{PPTX_NAMESPACES['r']}}}embed") if blip is not None else None
    if not relationship_id:
        return None
    relationship = relationships.get(relationship_id)
    if relationship is None:
        return None
    target = relationship["target"]
    mime_type = ooxml_image_mime_type(target)
    if mime_type is None:
        return None
    try:
        image_bytes = archive.read(target)
    except KeyError:
        return None
    c_nv_pr = element.find("./p:nvPicPr/p:cNvPr", PPTX_NAMESPACES)
    alt_text = None
    if c_nv_pr is not None:
        alt_text = normalize_whitespace(c_nv_pr.attrib.get("descr", "") or c_nv_pr.attrib.get("name", ""))
    width_emu, height_emu = pptx_shape_size(element)
    return {
        "kind": "image",
        "alt": alt_text or Path(target).name,
        "src": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
        "width_px": max(1, round(width_emu / EMU_PER_PIXEL)) if width_emu else None,
        "height_px": max(1, round(height_emu / EMU_PER_PIXEL)) if height_emu else None,
    }


def pptx_shape_text_blocks(element: ET.Element) -> list[str]:
    local_name = xml_local_name(element.tag)
    if local_name == "sp":
        paragraphs = [
            text
            for paragraph in element.findall("./p:txBody/a:p", PPTX_NAMESPACES)
            if (text := pptx_paragraph_text(paragraph))
        ]
        return ["\n".join(paragraphs)] if paragraphs else []
    if local_name == "graphicFrame":
        table = element.find(".//a:tbl", PPTX_NAMESPACES)
        if table is None:
            return []
        rows: list[str] = []
        for row in table.findall("./a:tr", PPTX_NAMESPACES):
            cells = [
                text
                for cell in row.findall("./a:tc", PPTX_NAMESPACES)
                if (text := normalize_whitespace(" ".join(filter(None, [node.text for node in cell.findall('.//a:t', PPTX_NAMESPACES)]))))
            ]
            if cells:
                rows.append(" | ".join(cells))
        return rows
    return []


def collect_pptx_shape_entries(
    container: ET.Element,
    *,
    archive: zipfile.ZipFile | None = None,
    relationships: dict[str, dict[str, str]] | None = None,
    group_offset: tuple[int, int] = (0, 0),
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    sequence = 0
    for child in list(container):
        local_name = xml_local_name(child.tag)
        if local_name in {"nvGrpSpPr", "grpSpPr"}:
            continue
        if local_name == "grpSp":
            child_x, child_y = pptx_shape_position(child)
            entries.extend(
                collect_pptx_shape_entries(
                    child,
                    archive=archive,
                    relationships=relationships,
                    group_offset=(group_offset[0] + child_x, group_offset[1] + child_y),
                )
            )
            sequence += 1
            continue
        if local_name == "pic" and archive is not None and relationships is not None:
            image_entry = pptx_picture_entry(child, archive=archive, relationships=relationships)
            if image_entry is not None:
                child_x, child_y = pptx_shape_position(child)
                entries.append(
                    {
                        **image_entry,
                        "placeholder_type": None,
                        "x": group_offset[0] + child_x,
                        "y": group_offset[1] + child_y,
                        "sequence": sequence,
                    }
                )
                sequence += 1
                continue
        text_blocks = pptx_shape_text_blocks(child)
        if not text_blocks:
            sequence += 1
            continue
        child_x, child_y = pptx_shape_position(child)
        placeholder_type = pptx_shape_placeholder_type(child)
        entries.append(
            {
                "kind": "text",
                "blocks": text_blocks,
                "placeholder_type": placeholder_type,
                "x": group_offset[0] + child_x,
                "y": group_offset[1] + child_y,
                "sequence": sequence,
            }
        )
        sequence += 1
    return entries


def sorted_pptx_content_entries(
    container: ET.Element,
    *,
    archive: zipfile.ZipFile | None = None,
    relationships: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, object]]:
    entries = collect_pptx_shape_entries(container, archive=archive, relationships=relationships)
    return sorted(
        entries,
        key=lambda item: (
            0 if item["placeholder_type"] in {"title", "ctrTitle", "subTitle"} else 1,
            int(item["y"]),
            int(item["x"]),
            int(item["sequence"]),
        ),
    )


def sorted_pptx_text_blocks(container: ET.Element) -> list[str]:
    ordered = sorted_pptx_content_entries(container)
    blocks: list[str] = []
    for entry in ordered:
        if entry.get("kind") == "text":
            blocks.extend(str(block) for block in entry["blocks"])
    return blocks


def render_html_text_blocks(blocks: list[str]) -> str:
    if not blocks:
        return "<p><em>No extractable text.</em></p>"
    paragraphs = []
    for block in blocks:
        escaped = html.escape(block).replace("\n", "<br/>")
        paragraphs.append(f"<p>{escaped}</p>")
    return "".join(paragraphs)


def render_pptx_content_entries(entries: list[dict[str, object]]) -> str:
    if not entries:
        return "<p><em>No extractable content.</em></p>"
    rendered: list[str] = []
    for entry in entries:
        if entry.get("kind") == "image":
            alt_text = str(entry.get("alt") or "Slide image")
            width_px = entry.get("width_px")
            height_px = entry.get("height_px")
            size_attrs = ""
            if isinstance(width_px, int) and width_px > 0:
                size_attrs += f' width="{width_px}"'
            if isinstance(height_px, int) and height_px > 0:
                size_attrs += f' height="{height_px}"'
            rendered.append(
                '<figure class="slide-image">'
                f'<img src="{html.escape(str(entry["src"]))}" alt="{html.escape(alt_text)}" loading="lazy"{size_attrs}/>'
                f"<figcaption>{html.escape(alt_text)}</figcaption>"
                "</figure>"
            )
            continue
        rendered.append(render_html_text_blocks([str(block) for block in entry.get("blocks", [])]))
    return "".join(rendered)


def extract_pptx_notes_blocks(archive: zipfile.ZipFile, slide_part_name: str) -> list[str]:
    relationships = read_ooxml_relationships(archive, slide_part_name)
    notes_part_name = None
    for relationship in relationships.values():
        if relationship["type"] == PPTX_NOTES_RELATIONSHIP_TYPE:
            notes_part_name = relationship["target"]
            break
    if not notes_part_name:
        return []
    try:
        notes_root = parse_xml_document(archive.read(notes_part_name))
    except KeyError:
        return []
    notes_tree = notes_root.find("./p:cSld/p:spTree", PPTX_NAMESPACES)
    if notes_tree is None:
        return []
    return sorted_pptx_text_blocks(notes_tree)


def build_pptx_preview_html(
    *,
    deck_title: str,
    author: str | None,
    date_created: str | None,
    date_modified: str | None,
    slides: list[dict[str, object]],
) -> str:
    slide_sections = []
    for slide in slides:
        slide_number = int(slide["slide_number"])
        notes_blocks = list(slide.get("notes_blocks", []))
        notes_section = ""
        if notes_blocks:
            notes_section = (
                '<div class="speaker-notes"><h3>Speaker Notes</h3>'
                f'{render_html_text_blocks([str(block) for block in notes_blocks])}</div>'
            )
        slide_sections.append(
            f'<section class="slide" id="slide-{slide_number}">'
            f"<h2>Slide {slide_number}</h2>"
            f'{render_pptx_content_entries([dict(entry) for entry in slide["content_entries"]])}'
            f"{notes_section}"
            "</section>"
        )
    metadata_rows = {
        "Title": deck_title,
        "Author": author or "",
        "Created": date_created or "",
        "Modified": date_modified or "",
    }
    return build_html_preview(
        metadata_rows,
        document_title=deck_title,
        head_html=(
            "<style>"
            "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.45; }"
            ".slide { border-top: 1px solid #ddd; margin-top: 1.5rem; padding-top: 1rem; }"
            ".slide-image { margin: 1rem 0; }"
            ".slide-image img { display: block; max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 6px; }"
            ".slide-image figcaption { color: #555; font-size: 0.9rem; margin-top: 0.35rem; }"
            ".speaker-notes { background: #f7f7f7; border-radius: 8px; margin-top: 0.75rem; padding: 0.75rem; }"
            ".speaker-notes h3 { margin-top: 0; }"
            "</style>"
        ),
        body_html=(
            "".join(slide_sections)
        ),
    )


def extract_pptx_file(path: Path) -> dict[str, object]:
    with zipfile.ZipFile(path) as archive:
        core_properties_root = None
        try:
            core_properties_root = parse_xml_document(archive.read("docProps/core.xml"))
        except KeyError:
            core_properties_root = None
        deck_title = None
        author = None
        subject = None
        date_created = None
        date_modified = None
        if core_properties_root is not None:
            deck_title = normalize_whitespace(core_properties_root.findtext("./dc:title", default="", namespaces=PPTX_NAMESPACES))
            author = normalize_whitespace(core_properties_root.findtext("./dc:creator", default="", namespaces=PPTX_NAMESPACES)) or None
            subject = normalize_whitespace(core_properties_root.findtext("./dc:subject", default="", namespaces=PPTX_NAMESPACES)) or None
            date_created = normalize_datetime(
                core_properties_root.findtext("./dcterms:created", default="", namespaces=PPTX_NAMESPACES)
            )
            date_modified = normalize_datetime(
                core_properties_root.findtext("./dcterms:modified", default="", namespaces=PPTX_NAMESPACES)
            )

        presentation_root = parse_xml_document(archive.read("ppt/presentation.xml"))
        presentation_relationships = read_ooxml_relationships(archive, "ppt/presentation.xml")
        slide_part_names: list[str] = []
        for slide_id in presentation_root.findall("./p:sldIdLst/p:sldId", PPTX_NAMESPACES):
            rel_id = slide_id.attrib.get(f"{{{PPTX_NAMESPACES['r']}}}id")
            if not rel_id:
                continue
            relationship = presentation_relationships.get(rel_id)
            if relationship is None:
                continue
            slide_part_names.append(relationship["target"])

        slides: list[dict[str, object]] = []
        text_sections: list[str] = []
        for index, slide_part_name in enumerate(slide_part_names, start=1):
            slide_root = parse_xml_document(archive.read(slide_part_name))
            slide_tree = slide_root.find("./p:cSld/p:spTree", PPTX_NAMESPACES)
            slide_relationships = read_ooxml_relationships(archive, slide_part_name)
            content_entries = (
                sorted_pptx_content_entries(slide_tree, archive=archive, relationships=slide_relationships)
                if slide_tree is not None
                else []
            )
            text_blocks = [str(block) for entry in content_entries if entry.get("kind") == "text" for block in entry["blocks"]]
            notes_blocks = extract_pptx_notes_blocks(archive, slide_part_name)
            slides.append(
                {
                    "slide_number": index,
                    "content_entries": content_entries,
                    "text_blocks": text_blocks,
                    "notes_blocks": notes_blocks,
                }
            )
            section_lines = [f"Slide {index}"]
            section_lines.extend(text_blocks)
            if notes_blocks:
                section_lines.append("Speaker notes")
                section_lines.extend(notes_blocks)
            text_sections.append("\n".join(line for line in section_lines if line))

        if deck_title and deck_title.strip().lower() in {"powerpoint presentation", "presentation"}:
            deck_title = None
        resolved_title = deck_title or path.stem
        preview = build_pptx_preview_html(
            deck_title=resolved_title,
            author=author,
            date_created=date_created,
            date_modified=date_modified,
            slides=slides,
        )
        text_content = normalize_whitespace("\n\n".join(section for section in text_sections if section))
        return {
            "page_count": len(slides),
            "author": author,
            "content_type": "Presentation",
            "date_created": date_created,
            "date_modified": date_modified,
            "participants": None,
            "title": resolved_title,
            "subject": subject,
            "recipients": None,
            "text_content": text_content,
            "text_status": "empty" if not text_content else "ok",
            "preview_artifacts": [
                {
                    "file_name": f"{path.name}.html",
                    "preview_type": "html",
                    "label": "deck",
                    "ordinal": 0,
                    "content": preview,
                }
            ],
        }


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "item"
