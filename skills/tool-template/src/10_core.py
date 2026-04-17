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
    CREATE TABLE IF NOT EXISTS datasets (
      id INTEGER PRIMARY KEY,
      source_kind TEXT NOT NULL,
      dataset_locator TEXT NOT NULL,
      dataset_name TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_sources (
      id INTEGER PRIMARY KEY,
      dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
      source_kind TEXT NOT NULL,
      source_locator TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_documents (
      id INTEGER PRIMARY KEY,
      dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      dataset_source_id INTEGER REFERENCES dataset_sources(id) ON DELETE CASCADE,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS documents (
      id INTEGER PRIMARY KEY,
      control_number TEXT UNIQUE,
      dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
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
      custodian TEXT,
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
      dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
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
      dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
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
      custodian,
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


def infer_source_custodian(
    *,
    source_kind: str | None,
    source_rel_path: str | None,
    parent_custodian: str | None = None,
) -> str | None:
    inherited = normalize_whitespace(str(parent_custodian or ""))
    if inherited:
        return inherited
    normalized_source_kind = normalize_whitespace(str(source_kind or "")).lower()
    normalized_source_rel_path = normalize_whitespace(str(source_rel_path or ""))
    if normalized_source_kind in {PST_SOURCE_KIND, MBOX_SOURCE_KIND} and normalized_source_rel_path:
        return normalize_whitespace(Path(normalized_source_rel_path).stem)
    return None


def filesystem_dataset_locator() -> str:
    return "."


def filesystem_dataset_name(root: Path | None = None) -> str:
    if root is not None:
        candidate = normalize_whitespace(root.resolve().name)
        if candidate:
            return candidate
    return "Workspace files"


def container_dataset_name(source_rel_path: str, fallback_label: str) -> str:
    candidate = normalize_whitespace(Path(source_rel_path).name)
    return candidate or normalize_whitespace(source_rel_path) or fallback_label


def pst_dataset_name(source_rel_path: str) -> str:
    return container_dataset_name(source_rel_path, "PST Dataset")


def mbox_dataset_name(source_rel_path: str) -> str:
    return container_dataset_name(source_rel_path, "MBOX Dataset")


def production_dataset_name(rel_root: str, production_name: str | None = None) -> str:
    preferred = normalize_whitespace(str(production_name or ""))
    if preferred:
        return preferred
    candidate = normalize_whitespace(Path(rel_root).name)
    return candidate or normalize_whitespace(rel_root) or "Production Dataset"


def manual_dataset_locator(dataset_name: str | None = None) -> str:
    seed = normalize_whitespace(str(dataset_name or "")) or "dataset"
    return f"manual:{sha256_text(f'{seed}:{utc_now()}')[:16]}"


def get_dataset_row(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    dataset_locator: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM datasets
        WHERE source_kind = ? AND dataset_locator = ?
        """,
        (source_kind, dataset_locator),
    ).fetchone()


def ensure_dataset_row(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    dataset_locator: str,
    dataset_name: str,
) -> int:
    now = utc_now()
    existing_row = get_dataset_row(
        connection,
        source_kind=source_kind,
        dataset_locator=dataset_locator,
    )
    if existing_row is None:
        connection.execute(
            """
            INSERT INTO datasets (
              source_kind, dataset_locator, dataset_name, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (source_kind, dataset_locator, dataset_name, now, now),
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    if str(existing_row["dataset_name"]) != dataset_name:
        connection.execute(
            """
            UPDATE datasets
            SET dataset_name = ?, updated_at = ?
            WHERE id = ?
            """,
            (dataset_name, now, existing_row["id"]),
        )
    return int(existing_row["id"])


def create_dataset_row(
    connection: sqlite3.Connection,
    dataset_name: str,
    *,
    source_kind: str | None = None,
    dataset_locator: str | None = None,
) -> int:
    normalized_name = normalize_whitespace(dataset_name) or "Dataset"
    normalized_source_kind = normalize_whitespace(str(source_kind or MANUAL_DATASET_SOURCE_KIND)).lower()
    normalized_locator = normalize_whitespace(str(dataset_locator or ""))
    if not normalized_locator:
        normalized_locator = manual_dataset_locator(normalized_name)
    return ensure_dataset_row(
        connection,
        source_kind=normalized_source_kind or MANUAL_DATASET_SOURCE_KIND,
        dataset_locator=normalized_locator,
        dataset_name=normalized_name,
    )


def get_dataset_source_row(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_locator: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM dataset_sources
        WHERE source_kind = ? AND source_locator = ?
        """,
        (source_kind, source_locator),
    ).fetchone()


def ensure_dataset_source_row(
    connection: sqlite3.Connection,
    *,
    dataset_id: int,
    source_kind: str,
    source_locator: str,
) -> int:
    normalized_source_kind = normalize_whitespace(source_kind).lower()
    normalized_source_locator = normalize_whitespace(source_locator)
    if not normalized_source_kind or not normalized_source_locator:
        raise RetrieverError("Dataset sources require non-empty source_kind and source_locator.")
    now = utc_now()
    existing_row = get_dataset_source_row(
        connection,
        source_kind=normalized_source_kind,
        source_locator=normalized_source_locator,
    )
    if existing_row is None:
        connection.execute(
            """
            INSERT INTO dataset_sources (
              dataset_id, source_kind, source_locator, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (dataset_id, normalized_source_kind, normalized_source_locator, now, now),
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    if int(existing_row["dataset_id"]) != dataset_id:
        raise RetrieverError(
            f"Source {normalized_source_kind}:{normalized_source_locator} is already bound to dataset {existing_row['dataset_id']}."
        )
    connection.execute(
        """
        UPDATE dataset_sources
        SET updated_at = ?
        WHERE id = ?
        """,
        (now, existing_row["id"]),
    )
    return int(existing_row["id"])


def ensure_source_backed_dataset(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_locator: str,
    dataset_name: str,
) -> tuple[int, int]:
    normalized_source_kind = normalize_whitespace(source_kind).lower()
    normalized_source_locator = normalize_whitespace(source_locator)
    existing_source = get_dataset_source_row(
        connection,
        source_kind=normalized_source_kind,
        source_locator=normalized_source_locator,
    )
    if existing_source is not None:
        return int(existing_source["dataset_id"]), int(existing_source["id"])
    dataset_id = ensure_dataset_row(
        connection,
        source_kind=normalized_source_kind,
        dataset_locator=normalized_source_locator,
        dataset_name=normalize_whitespace(dataset_name) or "Dataset",
    )
    dataset_source_id = ensure_dataset_source_row(
        connection,
        dataset_id=dataset_id,
        source_kind=normalized_source_kind,
        source_locator=normalized_source_locator,
    )
    return dataset_id, dataset_source_id


def ensure_dataset_document_membership(
    connection: sqlite3.Connection,
    *,
    dataset_id: int,
    document_id: int,
    dataset_source_id: int | None = None,
) -> int:
    now = utc_now()
    if dataset_source_id is None:
        existing_row = connection.execute(
            """
            SELECT id
            FROM dataset_documents
            WHERE dataset_id = ? AND document_id = ? AND dataset_source_id IS NULL
            """,
            (dataset_id, document_id),
        ).fetchone()
    else:
        existing_row = connection.execute(
            """
            SELECT id
            FROM dataset_documents
            WHERE dataset_id = ? AND document_id = ? AND dataset_source_id = ?
            """,
            (dataset_id, document_id, dataset_source_id),
        ).fetchone()
    if existing_row is None:
        connection.execute(
            """
            INSERT INTO dataset_documents (
              dataset_id, document_id, dataset_source_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (dataset_id, document_id, dataset_source_id, now, now),
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    return int(existing_row["id"])


def get_dataset_row_by_id(connection: sqlite3.Connection, dataset_id: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM datasets
        WHERE id = ?
        """,
        (dataset_id,),
    ).fetchone()


def find_dataset_rows_by_name(connection: sqlite3.Connection, dataset_name: str) -> list[sqlite3.Row]:
    normalized_name = normalize_whitespace(dataset_name)
    if not normalized_name:
        return []
    return connection.execute(
        """
        SELECT *
        FROM datasets
        WHERE dataset_name = ?
        ORDER BY id ASC
        """,
        (normalized_name,),
    ).fetchall()


def resolve_dataset_row(
    connection: sqlite3.Connection,
    *,
    dataset_id: int | None = None,
    dataset_name: str | None = None,
) -> sqlite3.Row:
    if dataset_id is None and dataset_name is None:
        raise RetrieverError("Specify either dataset_id or dataset_name.")
    if dataset_id is not None:
        row = get_dataset_row_by_id(connection, dataset_id)
        if row is None:
            raise RetrieverError(f"Unknown dataset id: {dataset_id}")
        if dataset_name is not None and normalize_whitespace(str(row["dataset_name"] or "")) != normalize_whitespace(dataset_name):
            raise RetrieverError(
                f"Dataset id {dataset_id} is named {row['dataset_name']!r}, not {normalize_whitespace(dataset_name)!r}."
            )
        return row
    matches = find_dataset_rows_by_name(connection, str(dataset_name or ""))
    if not matches:
        raise RetrieverError(f"Unknown dataset name: {dataset_name}")
    if len(matches) > 1:
        ids = ", ".join(str(row["id"]) for row in matches)
        raise RetrieverError(
            f"Dataset name {normalize_whitespace(str(dataset_name or ''))!r} is ambiguous; use --dataset-id. Matching ids: {ids}."
        )
    return matches[0]


def refresh_document_dataset_cache(connection: sqlite3.Connection, document_id: int) -> int | None:
    membership_rows = connection.execute(
        """
        SELECT DISTINCT dataset_id
        FROM dataset_documents
        WHERE document_id = ?
        ORDER BY dataset_id ASC
        """,
        (document_id,),
    ).fetchall()
    cached_dataset_id = int(membership_rows[0]["dataset_id"]) if len(membership_rows) == 1 else None
    connection.execute(
        """
        UPDATE documents
        SET dataset_id = ?
        WHERE id = ?
        """,
        (cached_dataset_id, document_id),
    )
    return cached_dataset_id


def list_dataset_summaries(connection: sqlite3.Connection) -> list[dict[str, object]]:
    dataset_rows = connection.execute(
        """
        SELECT *
        FROM datasets
        ORDER BY LOWER(dataset_name) ASC, id ASC
        """
    ).fetchall()
    if not dataset_rows:
        return []

    dataset_ids = [int(row["id"]) for row in dataset_rows]
    placeholders = ", ".join("?" for _ in dataset_ids)
    membership_rows = connection.execute(
        f"""
        SELECT
          dataset_id,
          COUNT(DISTINCT document_id) AS document_count,
          COUNT(DISTINCT CASE WHEN dataset_source_id IS NULL THEN document_id END) AS manual_document_count,
          COUNT(DISTINCT CASE WHEN dataset_source_id IS NOT NULL THEN document_id END) AS source_document_count
        FROM dataset_documents
        WHERE dataset_id IN ({placeholders})
        GROUP BY dataset_id
        """,
        dataset_ids,
    ).fetchall()
    membership_counts = {
        int(row["dataset_id"]): {
            "document_count": int(row["document_count"] or 0),
            "manual_document_count": int(row["manual_document_count"] or 0),
            "source_document_count": int(row["source_document_count"] or 0),
        }
        for row in membership_rows
    }
    source_rows = connection.execute(
        f"""
        SELECT *
        FROM dataset_sources
        WHERE dataset_id IN ({placeholders})
        ORDER BY dataset_id ASC, source_kind ASC, source_locator ASC, id ASC
        """,
        dataset_ids,
    ).fetchall()
    sources_by_dataset: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in source_rows:
        sources_by_dataset[int(row["dataset_id"])].append(
            {
                "id": int(row["id"]),
                "source_kind": row["source_kind"],
                "source_locator": row["source_locator"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )

    summaries: list[dict[str, object]] = []
    for row in dataset_rows:
        dataset_id = int(row["id"])
        counts = membership_counts.get(
            dataset_id,
            {"document_count": 0, "manual_document_count": 0, "source_document_count": 0},
        )
        source_bindings = sources_by_dataset.get(dataset_id, [])
        summaries.append(
            {
                "id": dataset_id,
                "dataset_name": row["dataset_name"],
                "source_kind": row["source_kind"],
                "dataset_locator": row["dataset_locator"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "document_count": counts["document_count"],
                "manual_document_count": counts["manual_document_count"],
                "source_document_count": counts["source_document_count"],
                "source_binding_count": len(source_bindings),
                "source_bindings": source_bindings,
            }
        )
    return summaries


def dataset_summary_by_id(connection: sqlite3.Connection, dataset_id: int) -> dict[str, object]:
    for summary in list_dataset_summaries(connection):
        if int(summary["id"]) == dataset_id:
            return summary
    raise RetrieverError(f"Unknown dataset id: {dataset_id}")


def add_documents_to_dataset(
    connection: sqlite3.Connection,
    *,
    dataset_id: int,
    document_ids: list[int],
) -> dict[str, list[int]]:
    if not document_ids:
        return {"added_document_ids": [], "already_present_document_ids": []}

    unique_document_ids = sorted(dict.fromkeys(int(document_id) for document_id in document_ids))
    placeholders = ", ".join("?" for _ in unique_document_ids)
    existing_rows = connection.execute(
        f"""
        SELECT id
        FROM documents
        WHERE id IN ({placeholders})
        """,
        unique_document_ids,
    ).fetchall()
    existing_ids = {int(row["id"]) for row in existing_rows}
    missing_ids = [document_id for document_id in unique_document_ids if document_id not in existing_ids]
    if missing_ids:
        raise RetrieverError(f"Unknown document ids: {', '.join(str(document_id) for document_id in missing_ids)}")

    current_rows = connection.execute(
        f"""
        SELECT DISTINCT document_id
        FROM dataset_documents
        WHERE dataset_id = ?
          AND document_id IN ({placeholders})
        """,
        [dataset_id, *unique_document_ids],
    ).fetchall()
    current_ids = {int(row["document_id"]) for row in current_rows}

    added_document_ids: list[int] = []
    already_present_document_ids: list[int] = []
    for document_id in unique_document_ids:
        if document_id in current_ids:
            already_present_document_ids.append(document_id)
            continue
        ensure_dataset_document_membership(
            connection,
            dataset_id=dataset_id,
            document_id=document_id,
            dataset_source_id=None,
        )
        refresh_document_dataset_cache(connection, document_id)
        added_document_ids.append(document_id)

    if added_document_ids:
        connection.execute(
            """
            UPDATE datasets
            SET updated_at = ?
            WHERE id = ?
            """,
            (utc_now(), dataset_id),
        )

    return {
        "added_document_ids": added_document_ids,
        "already_present_document_ids": already_present_document_ids,
    }


def remove_documents_from_dataset(
    connection: sqlite3.Connection,
    *,
    dataset_id: int,
    document_ids: list[int],
) -> dict[str, list[int]]:
    if not document_ids:
        return {
            "removed_document_ids": [],
            "not_present_document_ids": [],
            "documents_without_dataset_memberships": [],
        }

    unique_document_ids = sorted(dict.fromkeys(int(document_id) for document_id in document_ids))
    placeholders = ", ".join("?" for _ in unique_document_ids)
    existing_rows = connection.execute(
        f"""
        SELECT id
        FROM documents
        WHERE id IN ({placeholders})
        """,
        unique_document_ids,
    ).fetchall()
    existing_ids = {int(row["id"]) for row in existing_rows}
    missing_ids = [document_id for document_id in unique_document_ids if document_id not in existing_ids]
    if missing_ids:
        raise RetrieverError(f"Unknown document ids: {', '.join(str(document_id) for document_id in missing_ids)}")

    current_rows = connection.execute(
        f"""
        SELECT DISTINCT document_id
        FROM dataset_documents
        WHERE dataset_id = ?
          AND document_id IN ({placeholders})
        """,
        [dataset_id, *unique_document_ids],
    ).fetchall()
    current_ids = {int(row["document_id"]) for row in current_rows}

    removed_document_ids: list[int] = []
    not_present_document_ids: list[int] = []
    documents_without_dataset_memberships: list[int] = []
    for document_id in unique_document_ids:
        if document_id not in current_ids:
            not_present_document_ids.append(document_id)
            continue
        connection.execute(
            """
            DELETE FROM dataset_documents
            WHERE dataset_id = ? AND document_id = ?
            """,
            (dataset_id, document_id),
        )
        cached_dataset_id = refresh_document_dataset_cache(connection, document_id)
        if cached_dataset_id is None:
            remaining_membership = connection.execute(
                """
                SELECT 1
                FROM dataset_documents
                WHERE document_id = ?
                LIMIT 1
                """,
                (document_id,),
            ).fetchone()
            if remaining_membership is None:
                documents_without_dataset_memberships.append(document_id)
        removed_document_ids.append(document_id)

    if removed_document_ids:
        connection.execute(
            """
            UPDATE datasets
            SET updated_at = ?
            WHERE id = ?
            """,
            (utc_now(), dataset_id),
        )

    return {
        "removed_document_ids": removed_document_ids,
        "not_present_document_ids": not_present_document_ids,
        "documents_without_dataset_memberships": documents_without_dataset_memberships,
    }


def delete_dataset_row(connection: sqlite3.Connection, dataset_id: int) -> dict[str, object]:
    dataset_row = get_dataset_row_by_id(connection, dataset_id)
    if dataset_row is None:
        raise RetrieverError(f"Unknown dataset id: {dataset_id}")
    affected_rows = connection.execute(
        """
        SELECT DISTINCT document_id
        FROM dataset_documents
        WHERE dataset_id = ?
        ORDER BY document_id ASC
        """,
        (dataset_id,),
    ).fetchall()
    affected_document_ids = [int(row["document_id"]) for row in affected_rows]
    summary = dataset_summary_by_id(connection, dataset_id)
    connection.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))

    documents_without_dataset_memberships: list[int] = []
    for document_id in affected_document_ids:
        row = connection.execute("SELECT id FROM documents WHERE id = ?", (document_id,)).fetchone()
        if row is None:
            continue
        cached_dataset_id = refresh_document_dataset_cache(connection, document_id)
        if cached_dataset_id is None:
            remaining_membership = connection.execute(
                """
                SELECT 1
                FROM dataset_documents
                WHERE document_id = ?
                LIMIT 1
                """,
                (document_id,),
            ).fetchone()
            if remaining_membership is None:
                documents_without_dataset_memberships.append(document_id)

    return {
        "deleted_dataset": summary,
        "affected_document_ids": affected_document_ids,
        "documents_without_dataset_memberships": documents_without_dataset_memberships,
    }


def prune_unused_filesystem_dataset(connection: sqlite3.Connection) -> bool:
    dataset_source_row = get_dataset_source_row(
        connection,
        source_kind=FILESYSTEM_SOURCE_KIND,
        source_locator=filesystem_dataset_locator(),
    )
    if dataset_source_row is None:
        return False

    dataset_id = int(dataset_source_row["dataset_id"])
    membership_row = connection.execute(
        """
        SELECT 1
        FROM dataset_documents
        WHERE dataset_id = ?
        LIMIT 1
        """,
        (dataset_id,),
    ).fetchone()
    if membership_row is not None:
        return False

    filesystem_document_row = connection.execute(
        """
        SELECT 1
        FROM documents
        WHERE COALESCE(source_kind, ?) = ?
        LIMIT 1
        """,
        (FILESYSTEM_SOURCE_KIND, FILESYSTEM_SOURCE_KIND),
    ).fetchone()
    if filesystem_document_row is not None:
        return False

    connection.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))
    return True


def container_message_rel_path(source_rel_path: str, source_item_id: str, file_suffix: str) -> str:
    encoded = encode_source_item_id_for_path(source_item_id)
    return (
        Path(".retriever")
        / "sources"
        / Path(source_rel_path)
        / "messages"
        / f"{encoded}.{file_suffix}"
    ).as_posix()


def pst_message_rel_path(source_rel_path: str, source_item_id: str) -> str:
    return container_message_rel_path(source_rel_path, source_item_id, "pstmsg")


def mbox_message_rel_path(source_rel_path: str, source_item_id: str) -> str:
    return container_message_rel_path(source_rel_path, source_item_id, "mboxmsg")


def container_preview_file_name(source_item_id: str) -> str:
    return f"{encode_source_item_id_for_path(source_item_id)}.html"


def pst_preview_file_name(source_item_id: str) -> str:
    return container_preview_file_name(source_item_id)


def mbox_preview_file_name(source_item_id: str) -> str:
    return container_preview_file_name(source_item_id)


def container_message_file_name(source_item_id: str, file_suffix: str) -> str:
    return f"{encode_source_item_id_for_path(source_item_id)}.{file_suffix}"


def pst_message_file_name(source_item_id: str) -> str:
    return container_message_file_name(source_item_id, "pstmsg")


def mbox_message_file_name(source_item_id: str) -> str:
    return container_message_file_name(source_item_id, "mboxmsg")


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
    if "DATE" in type_name or "TIME" in type_name:
        return "date"
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


def normalize_date_field_value(value: object) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=timezone.utc)
        else:
            normalized = normalized.astimezone(timezone.utc)
        return normalized.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        try:
            return date.fromisoformat(raw).isoformat()
        except ValueError:
            return None
    return parse_iso_datetime(raw)


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
    with_breaks = re.sub(r"(?is)<br\s*/?>", "\n", without_scripts)
    with_breaks = re.sub(
        r"(?is)</(?:p|div|li|tr|td|th|h[1-6]|section|article|blockquote|pre|ul|ol|table)>",
        "\n",
        with_breaks,
    )
    without_tags = re.sub(r"(?s)<[^>]+>", " ", with_breaks)
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


CHAT_SPEAKER_BLOCKLIST = {
    "agenda",
    "answer",
    "bcc",
    "cc",
    "date",
    "description",
    "from",
    "message",
    "note",
    "notes",
    "owner",
    "priority",
    "question",
    "sent",
    "status",
    "subject",
    "summary",
    "task",
    "thread",
    "title",
    "to",
    "topic",
}
CHAT_TIMESTAMP_HINT_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4}|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b",
    re.IGNORECASE,
)
CHAT_ISO_DATETIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
CHAT_JSON_FIELD_PATTERN = re.compile(r'^"[^"\n]{1,80}"\s*:\s*')
CHAT_LINE_PATTERNS = (
    r"^\[(?P<timestamp>[^\]]{4,80})\]\s*(?P<speaker>[^:\n]{2,80}?):\s+(?P<body>\S.*)$",
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM))?(?:\s*(?:Z|UTC|[+\-]\d{2}:?\d{2}))?)\s*[-,]?\s*(?P<speaker>[^:\n]{2,80}?):\s+(?P<body>\S.*)$",
    r"^(?P<timestamp>\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?)\s*[-,]?\s*(?P<speaker>[^:\n]{2,80}?):\s+(?P<body>\S.*)$",
    r"^(?P<speaker>[^:\n]{2,80}?):\s+(?P<body>\S.*)$",
)


def normalize_chat_speaker(value: str | None) -> str | None:
    candidate = normalize_participant_token(value)
    if not candidate:
        return None
    lowered = candidate.lower().strip("[]()")
    if lowered in CHAT_SPEAKER_BLOCKLIST or len(candidate.split()) > 8:
        return None
    return candidate


def parse_chat_timestamp(value: str | None) -> str | None:
    raw = normalize_whitespace(str(value or "")).strip("[]()")
    if not raw or not CHAT_TIMESTAMP_HINT_PATTERN.search(raw):
        return None
    normalized = normalize_datetime(raw)
    if normalized and CHAT_ISO_DATETIME_PATTERN.fullmatch(normalized):
        return normalized
    return None


def format_chat_preview_timestamp(value: object) -> str | None:
    raw = normalize_whitespace(str(value or "")).strip("[]()")
    if not raw:
        return None
    parsed = parse_utc_timestamp(raw)
    if parsed is None:
        normalized = parse_chat_timestamp(raw)
        parsed = parse_utc_timestamp(normalized) if normalized else None
    if parsed is None:
        return raw
    return parsed.strftime("%b %d, %Y %I:%M %p UTC").replace(" 0", " ")


def chat_avatar_initials(value: str) -> str:
    letters = [part[0].upper() for part in re.split(r"\s+", value.strip()) if part and part[0].isalnum()]
    if not letters:
        return "?"
    if len(letters) == 1:
        return letters[0]
    return f"{letters[0]}{letters[-1]}"


CHAT_AVATAR_PALETTE = (
    ("#dbeafe", "#1d4ed8"),
    ("#dcfce7", "#166534"),
    ("#fef3c7", "#92400e"),
    ("#fce7f3", "#9d174d"),
    ("#ede9fe", "#6d28d9"),
    ("#cffafe", "#0f766e"),
    ("#fee2e2", "#b91c1c"),
    ("#e0e7ff", "#4338ca"),
)


def normalize_chat_avatar_color(value: object) -> str | None:
    candidate = normalize_whitespace(str(value or "")).lstrip("#")
    if re.fullmatch(r"[0-9a-fA-F]{6}", candidate or ""):
        return f"#{candidate.lower()}"
    return None


def chat_avatar_colors(seed: str, preferred_background: object = None) -> tuple[str, str]:
    background = normalize_chat_avatar_color(preferred_background)
    if background:
        red = int(background[1:3], 16)
        green = int(background[3:5], 16)
        blue = int(background[5:7], 16)
        luminance = (0.2126 * red) + (0.7152 * green) + (0.0722 * blue)
        return background, ("#ffffff" if luminance < 140 else "#111827")
    palette_index = int(hashlib.sha1(seed.encode("utf-8")).hexdigest(), 16) % len(CHAT_AVATAR_PALETTE)
    return CHAT_AVATAR_PALETTE[palette_index]


def build_chat_avatar_svg(label: str, background: str, foreground: str, alt_text: str) -> str:
    return (
        '<svg class="chat-avatar-svg" xmlns="http://www.w3.org/2000/svg" width="96" height="96" '
        'viewBox="0 0 96 96" role="img" '
        f'aria-label="{html.escape(alt_text, quote=True)}">'
        f'<circle cx="48" cy="48" r="48" fill="{html.escape(background, quote=True)}"/>'
        f'<text x="50%" y="55%" text-anchor="middle" dominant-baseline="middle" '
        f'font-family="Inter, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif" '
        f'font-size="30" font-weight="700" fill="{html.escape(foreground, quote=True)}">{html.escape(label)}</text>'
        "</svg>"
    )


def iter_chat_transcript_entries(text: str, max_lines: int = 800) -> list[dict[str, str | None]]:
    if not text:
        return []

    entries: list[dict[str, str | None]] = []
    for raw_line in text.splitlines()[:max_lines]:
        stripped = raw_line.strip()
        if not stripped or len(stripped) > 240:
            continue
        if CHAT_JSON_FIELD_PATTERN.match(stripped):
            continue
        for pattern in CHAT_LINE_PATTERNS:
            match = re.match(pattern, stripped, flags=re.IGNORECASE)
            if not match:
                continue
            speaker = normalize_chat_speaker(match.groupdict().get("speaker"))
            body = normalize_whitespace(match.groupdict().get("body") or "")
            if not speaker or not body:
                continue
            entries.append(
                {
                    "speaker": speaker,
                    "body": body,
                    "timestamp": parse_chat_timestamp(match.groupdict().get("timestamp")),
                }
            )
            break
    return entries


def extract_chat_participants(text: str) -> str | None:
    participants: list[str] = []
    seen: set[str] = set()
    speaker_counts: dict[str, int] = {}
    for entry in iter_chat_transcript_entries(text):
        candidate = str(entry["speaker"])
        key = candidate.lower().strip("[]()")
        speaker_counts[key] = speaker_counts.get(key, 0) + 1
        if key not in seen:
            seen.add(key)
            participants.append(candidate)

    total_matches = sum(speaker_counts.values())
    if total_matches < 2:
        return None
    if len(participants) < 2 and total_matches < 3:
        return None
    return ", ".join(participants)


def extract_chat_transcript_metadata(text: str) -> dict[str, object] | None:
    entries = iter_chat_transcript_entries(text, max_lines=1200)
    if not entries:
        return None

    participants: list[str] = []
    seen: set[str] = set()
    speaker_counts: dict[str, int] = {}
    first_speaker: str | None = None
    first_body: str | None = None
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    timestamped_matches = 0

    for entry in entries:
        speaker = str(entry["speaker"])
        key = speaker.lower().strip("[]()")
        speaker_counts[key] = speaker_counts.get(key, 0) + 1
        if key not in seen:
            seen.add(key)
            participants.append(speaker)
        if first_speaker is None:
            first_speaker = speaker
        if first_body is None:
            first_body = str(entry["body"])
        timestamp = entry.get("timestamp")
        if isinstance(timestamp, str):
            timestamped_matches += 1
            if first_timestamp is None:
                first_timestamp = timestamp
            last_timestamp = timestamp

    total_matches = sum(speaker_counts.values())
    repeated_speaker = any(count >= 2 for count in speaker_counts.values())
    if total_matches < 2:
        return None
    if timestamped_matches < 2:
        if len(participants) < 2 or total_matches < 3 or not repeated_speaker:
            return None
    elif len(participants) < 2 and total_matches < 3:
        return None

    return {
        "author": first_speaker,
        "participants": ", ".join(participants) or None,
        "date_created": first_timestamp,
        "date_modified": last_timestamp if last_timestamp and last_timestamp != first_timestamp else None,
        "title": (first_body[:200] if first_body else None),
        "message_count": total_matches,
        "timestamped_message_count": timestamped_matches,
    }


def infer_content_type_from_content(
    file_type: str,
    text_content: str,
    email_headers: dict[str, str | None] | None = None,
    chat_metadata: dict[str, object] | None = None,
) -> str | None:
    if email_headers:
        return "Email"
    if chat_metadata:
        return "Chat"
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
    chat_metadata: dict[str, object] | None = None,
    explicit_content_type: str | None = None,
) -> str | None:
    file_type = normalize_extension(path)
    return (
        infer_content_type_from_content(file_type, text_content, email_headers, chat_metadata)
        or explicit_content_type
        or infer_content_type_from_extension(file_type)
    )


def dependency_guard(module: object | None, package_name: str, file_type: str) -> None:
    if module is None:
        raise RetrieverError(
            f"Missing dependency for .{file_type} parsing: install {package_name} before ingesting this file type."
        )


CID_REFERENCE_PATTERN = re.compile(
    r"""(?i)(\b(?:src|background)\s*=\s*)(["'])cid:([^"']+)\2"""
)


def sniff_image_mime_type(payload: bytes) -> str | None:
    if not isinstance(payload, (bytes, bytearray)) or len(payload) < 4:
        return None
    data = bytes(payload)
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"
    if data.startswith(b"BM"):
        return "image/bmp"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"II*\x00") or data.startswith(b"MM\x00*"):
        return "image/tiff"
    return None


def normalize_content_id(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode("utf-8")
        except Exception:
            raw = bytes(raw).decode("utf-8", errors="replace")
    value = str(raw).strip()
    if not value:
        return None
    value = value.strip("<>").strip()
    return value or None


def build_cid_data_uri_map(attachments: list[dict[str, object]] | None) -> dict[str, str]:
    if not attachments:
        return {}
    mapping: dict[str, str] = {}
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        content_id = normalize_content_id(attachment.get("content_id"))
        if not content_id:
            continue
        payload = attachment.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            continue
        payload_bytes = bytes(payload)
        file_name = str(attachment.get("file_name") or "")
        mime_type = ooxml_image_mime_type(file_name) or sniff_image_mime_type(payload_bytes)
        if not mime_type:
            guessed, _ = mimetypes.guess_type(file_name)
            if guessed and guessed.startswith("image/"):
                mime_type = guessed
        if not mime_type:
            mime_type = "application/octet-stream"
        encoded = base64.b64encode(payload_bytes).decode("ascii")
        mapping[content_id.lower()] = f"data:{mime_type};base64,{encoded}"
    return mapping


def inline_cid_references_in_html(
    html_body: str | None,
    attachments: list[dict[str, object]] | None,
) -> str | None:
    if not html_body:
        return html_body
    cid_map = build_cid_data_uri_map(attachments)
    if not cid_map:
        return html_body

    def _replace(match: re.Match[str]) -> str:
        prefix = match.group(1)
        quote = match.group(2)
        cid = normalize_content_id(match.group(3))
        if not cid:
            return match.group(0)
        replacement = cid_map.get(cid.lower())
        if not replacement:
            return match.group(0)
        return f"{prefix}{quote}{replacement}{quote}"

    return CID_REFERENCE_PATTERN.sub(_replace, html_body)


def build_html_preview(
    headers: dict[str, str],
    body_html: str | None = None,
    body_text: str | None = None,
    *,
    document_title: str = "Retriever Preview",
    head_html: str | None = None,
    heading: str = "Retriever Preview",
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
        f"<h1>{html.escape(heading)}</h1>"
        "<table>"
        f"{header_html}"
        "</table>"
        "<hr/>"
        f"{body_section}"
        "</body></html>"
    )


def build_chat_preview_html(
    headers: dict[str, str],
    body_text: str,
    *,
    document_title: str = "Retriever Chat Preview",
    entries: list[dict[str, object]] | None = None,
) -> str:
    chat_entries = entries if entries is not None else iter_chat_transcript_entries(body_text, max_lines=4000)
    head_html = (
        "<style>"
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px; color: #1f2328; }"
        "h1 { font-size: 1.35rem; margin-bottom: 0.75rem; }"
        "table { border-collapse: collapse; margin-bottom: 1rem; }"
        "th { text-align: left; vertical-align: top; padding: 0.25rem 0.75rem 0.25rem 0; color: #57606a; }"
        "td { padding: 0.25rem 0; }"
        ".chat-transcript { display: grid; gap: 0.75rem; }"
        ".chat-message { display: flex; gap: 0.75rem; align-items: flex-start; border: 1px solid #d0d7de; border-radius: 12px; padding: 0.85rem 0.95rem; background: #f6f8fa; }"
        ".chat-avatar-svg { width: 2.5rem; height: 2.5rem; flex: 0 0 auto; display: block; }"
        ".chat-main { min-width: 0; flex: 1 1 auto; }"
        ".chat-meta { display: flex; gap: 0.55rem; align-items: baseline; margin-bottom: 0.25rem; flex-wrap: wrap; }"
        ".chat-speaker { font-weight: 600; color: #0969da; }"
        ".chat-time { color: #57606a; font-size: 0.9rem; }"
        ".chat-body { white-space: pre-wrap; line-height: 1.45; }"
        ".chat-raw { margin-top: 1rem; }"
        ".chat-raw summary { cursor: pointer; color: #57606a; }"
        "</style>"
    )
    if chat_entries:
        rendered_entries: list[str] = []
        for entry in chat_entries:
            speaker = normalize_whitespace(str(entry.get("speaker") or "")) or "Unknown"
            body = str(entry.get("body") or "").strip()
            if not body:
                continue
            timestamp_label = (
                normalize_whitespace(str(entry.get("timestamp_label") or ""))
                or format_chat_preview_timestamp(entry.get("timestamp"))
                or ""
            )
            timestamp_html = f'<span class="chat-time">[{html.escape(timestamp_label)}]</span>' if timestamp_label else ""
            avatar_label = normalize_whitespace(str(entry.get("avatar_label") or "")) or chat_avatar_initials(speaker)
            avatar_background, avatar_foreground = chat_avatar_colors(
                speaker,
                entry.get("avatar_color"),
            )
            avatar_html = build_chat_avatar_svg(avatar_label, avatar_background, avatar_foreground, speaker)
            rendered_entries.append(
                "<article class=\"chat-message\">"
                f"{avatar_html}"
                "<div class=\"chat-main\">"
                "<div class=\"chat-meta\">"
                f"<span class=\"chat-speaker\">{html.escape(speaker)}</span>"
                f"{timestamp_html}"
                "</div>"
                f"<div class=\"chat-body\">{html.escape(body)}</div>"
                "</div>"
                "</article>"
            )
        if rendered_entries:
            body_section = (
                "<div class=\"chat-transcript\">"
                f"{''.join(rendered_entries)}"
                "</div>"
                "<details class=\"chat-raw\">"
                "<summary>Full transcript</summary>"
                f"<pre>{html.escape(body_text or '')}</pre>"
                "</details>"
            )
        else:
            body_section = f"<pre>{html.escape(body_text or '')}</pre>"
    else:
        body_section = f"<pre>{html.escape(body_text or '')}</pre>"
    return build_html_preview(
        headers,
        body_html=body_section,
        document_title=document_title,
        head_html=head_html,
        heading="Retriever Chat Preview",
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
