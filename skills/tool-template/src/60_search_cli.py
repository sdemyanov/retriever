def parse_filter_args(raw_filters: list[list[str]] | None) -> list[dict[str, object]]:
    parsed: list[dict[str, object]] = []
    for item in raw_filters or []:
        if len(item) < 2:
            raise RetrieverError("Each --filter requires at least <field> <op>.")
        field_name = item[0]
        operator = item[1].lower()
        value = None if operator in {"is-null", "not-null"} else " ".join(item[2:]) if len(item) > 2 else None
        if operator not in {"eq", "neq", "gt", "gte", "lt", "lte", "contains", "is-null", "not-null"}:
            raise RetrieverError(f"Unsupported filter operator: {operator}")
        if operator not in {"is-null", "not-null"} and value in (None, ""):
            raise RetrieverError(f"Filter operator '{operator}' requires a value.")
        parsed.append({"field_name": field_name, "operator": operator, "value": value})
    return parsed


def build_filter_clause(alias: str, field_def: dict[str, str], operator: str, value: str | None) -> tuple[str, list[object]]:
    field_name = field_def["field_name"]
    field_type = field_def["field_type"]
    field_source = field_def.get("source")
    if field_source == "virtual":
        return build_virtual_filter_clause(alias, field_name, field_type, operator, value)
    column_expr = f"{alias}.{quote_identifier(field_name)}"

    if operator == "is-null":
        return f"{column_expr} IS NULL", []
    if operator == "not-null":
        return f"{column_expr} IS NOT NULL", []

    if operator == "contains":
        if field_type not in {"text", "date"}:
            raise RetrieverError(f"Operator 'contains' is not valid for field type '{field_type}'.")
        return f"LOWER(COALESCE({column_expr}, '')) LIKE LOWER(?)", [f"%{value}%"]

    typed_value = value_from_type(field_type if field_type in {"integer", "real", "boolean"} else "text", value)
    if field_type == "date":
        typed_value = normalize_date_field_value(str(value or ""))
        if typed_value is None:
            raise RetrieverError(f"Expected ISO date value, got {value!r}")

    comparators = {
        "eq": "=",
        "neq": "!=",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
    }
    comparator = comparators[operator]

    if operator in {"gt", "gte", "lt", "lte"} and field_type not in {"integer", "real", "date", "text"}:
        raise RetrieverError(f"Operator '{operator}' is not valid for field type '{field_type}'.")

    return f"{column_expr} {comparator} ?", [typed_value]


def build_virtual_filter_clause(
    alias: str,
    field_name: str,
    field_type: str,
    operator: str,
    value: str | None,
) -> tuple[str, list[object]]:
    if field_name in {"is_attachment", "has_attachments"}:
        if operator not in {"eq", "neq"}:
            raise RetrieverError(f"Virtual filter '{field_name}' only supports eq and neq.")
        typed_value = value_from_type("boolean", value)
        if field_name == "is_attachment":
            positive_clause = f"{alias}.parent_document_id IS NOT NULL"
        else:
            positive_clause = (
                "EXISTS ("
                "SELECT 1 FROM documents child "
                f"WHERE child.parent_document_id = {alias}.id "
                "AND child.lifecycle_status NOT IN ('missing', 'deleted')"
                ")"
            )
        positive = bool(typed_value)
        if operator == "neq":
            positive = not positive
        return (positive_clause if positive else f"NOT ({positive_clause})"), []

    if field_name == "production_name":
        column_expr = (
            "(SELECT p.production_name FROM productions p "
            f"WHERE p.id = {alias}.production_id)"
        )
        if operator == "is-null":
            return f"{column_expr} IS NULL", []
        if operator == "not-null":
            return f"{column_expr} IS NOT NULL", []
        if operator == "contains":
            return f"LOWER(COALESCE({column_expr}, '')) LIKE LOWER(?)", [f"%{value}%"]
        if operator in {"eq", "neq"}:
            comparator = "=" if operator == "eq" else "!="
            return f"COALESCE({column_expr}, '') {comparator} ?", [value or ""]
        raise RetrieverError(f"Virtual filter '{field_name}' does not support operator '{operator}'.")

    if field_name == "dataset_name":
        exists_expr = (
            "EXISTS ("
            "SELECT 1 "
            "FROM dataset_documents dd "
            "JOIN datasets ds ON ds.id = dd.dataset_id "
            f"WHERE dd.document_id = {alias}.id"
        )
        filtered_exists_expr = (
            "EXISTS ("
            "SELECT 1 "
            "FROM dataset_documents dd "
            "JOIN datasets ds ON ds.id = dd.dataset_id "
            f"WHERE dd.document_id = {alias}.id "
        )
        if operator == "is-null":
            return f"NOT {exists_expr})", []
        if operator == "not-null":
            return f"{exists_expr})", []
        if operator == "contains":
            return f"{filtered_exists_expr} AND LOWER(COALESCE(ds.dataset_name, '')) LIKE LOWER(?))", [f"%{value}%"]
        if operator in {"eq", "neq"}:
            positive_clause = f"{filtered_exists_expr} AND COALESCE(ds.dataset_name, '') = ?)"
            if operator == "eq":
                return positive_clause, [value or ""]
            return f"NOT ({positive_clause})", [value or ""]
        raise RetrieverError(f"Virtual filter '{field_name}' does not support operator '{operator}'.")

    raise RetrieverError(f"Unknown virtual filter: {field_name}")


def build_search_filters(
    connection: sqlite3.Connection, raw_filters: list[list[str]] | None
) -> tuple[list[dict[str, object]], list[str], list[object]]:
    parsed_filters = parse_filter_args(raw_filters)
    clauses = [
        "d.lifecycle_status NOT IN ('missing', 'deleted')",
        "EXISTS (SELECT 1 FROM dataset_documents dd WHERE dd.document_id = d.id)",
    ]
    params: list[object] = []
    normalized_filters: list[dict[str, object]] = []
    for raw_filter in parsed_filters:
        field_def = resolve_field_definition(connection, str(raw_filter["field_name"]))
        clause, clause_params = build_filter_clause(
            "d",
            field_def,
            str(raw_filter["operator"]),
            raw_filter["value"],  # type: ignore[arg-type]
        )
        clauses.append(clause)
        params.extend(clause_params)
        normalized_filters.append(
            {
                "field_name": field_def["field_name"],
                "field_type": field_def["field_type"],
                "operator": raw_filter["operator"],
                "value": raw_filter["value"],
            }
        )
    return normalized_filters, clauses, params


def metadata_snippet(row: sqlite3.Row) -> str:
    parts = [
        row["control_number"],
        row["begin_bates"],
        row["end_bates"],
        row["content_type"],
        row["custodian"],
        row["source_rel_path"],
        row["source_folder_path"],
        row["title"],
        row["subject"],
        row["author"],
        row["participants"],
        row["recipients"],
    ]
    text = normalize_whitespace(" | ".join(part for part in parts if part))
    return text[:220]


def query_terms(query: str) -> list[str]:
    return list(dict.fromkeys(re.findall(r"[A-Za-z0-9_]+", query.lower())))


def make_snippet(text: str, query: str) -> str:
    normalized = normalize_whitespace(text)
    if not normalized:
        return ""
    lower_text = normalized.lower()
    start = 0
    for term in query_terms(query):
        index = lower_text.find(term)
        if index != -1:
            start = max(0, index - 80)
            break
    end = min(len(normalized), start + 220)
    snippet = normalized[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(normalized):
        snippet = snippet + "..."
    return snippet


def fetch_documents_by_ids(connection: sqlite3.Connection, document_ids: list[int]) -> dict[int, sqlite3.Row]:
    if not document_ids:
        return {}
    placeholders = ", ".join("?" for _ in document_ids)
    rows = connection.execute(
        f"SELECT * FROM documents WHERE id IN ({placeholders})",
        document_ids,
    ).fetchall()
    return {int(row["id"]): row for row in rows}


def document_path_payload(
    paths: dict[str, Path],
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> dict[str, object]:
    preview_rel_path, preview_abs_path = default_preview_target(paths, row, connection)
    return {
        "rel_path": row["rel_path"],
        "abs_path": str(document_absolute_path(paths, row["rel_path"])),
        "preview_rel_path": preview_rel_path,
        "preview_abs_path": preview_abs_path,
        "preview_targets": collect_preview_targets(paths, int(row["id"]), row["rel_path"], connection),
    }


def fetch_attachment_summaries(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    parent_ids: list[int],
) -> dict[int, list[dict[str, object]]]:
    if not parent_ids:
        return {}
    placeholders = ", ".join("?" for _ in parent_ids)
    rows = connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE parent_document_id IN ({placeholders})
          AND lifecycle_status NOT IN ('missing', 'deleted')
        ORDER BY parent_document_id ASC, id ASC
        """,
        parent_ids,
    ).fetchall()
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["parent_document_id"])].append(
            {
                "id": int(row["id"]),
                "control_number": row["control_number"],
                "file_name": row["file_name"],
                "file_type": row["file_type"],
                **document_path_payload(paths, connection, row),
                "source_kind": row["source_kind"],
                "begin_bates": row["begin_bates"],
                "control_number_attachment_sequence": row["control_number_attachment_sequence"],
            }
        )
    for parent_id, items in grouped.items():
        grouped[parent_id] = sorted(
            items,
            key=lambda item: (
                0 if item.get("source_kind") == PRODUCTION_SOURCE_KIND else 1,
                bates_sort_key(item.get("begin_bates") or item.get("control_number"))
                if item.get("source_kind") == PRODUCTION_SOURCE_KIND
                else (0, "", int(item.get("control_number_attachment_sequence") or 0), ""),
                item["id"],
            ),
        )
        for item in grouped[parent_id]:
            item.pop("source_kind", None)
            item.pop("begin_bates", None)
            item.pop("control_number_attachment_sequence", None)
    return grouped


def fetch_parent_summaries(
    connection: sqlite3.Connection,
    child_rows: list[sqlite3.Row],
) -> dict[int, dict[str, object]]:
    parent_ids = sorted(
        {
            int(row["parent_document_id"])
            for row in child_rows
            if row["parent_document_id"] is not None
        }
    )
    if not parent_ids:
        return {}
    placeholders = ", ".join("?" for _ in parent_ids)
    rows = connection.execute(
        f"""
        SELECT id, control_number, rel_path, file_name, subject, author, date_created
        FROM documents
        WHERE id IN ({placeholders})
        """,
        parent_ids,
    ).fetchall()
    return {
        int(row["id"]): {
            "id": int(row["id"]),
            "control_number": row["control_number"],
            "rel_path": row["rel_path"],
            "file_name": row["file_name"],
            "subject": row["subject"],
            "author": row["author"],
            "date_created": row["date_created"],
        }
        for row in rows
    }


def fetch_production_names(connection: sqlite3.Connection, rows: list[sqlite3.Row]) -> dict[int, str]:
    production_ids = sorted({int(row["production_id"]) for row in rows if row["production_id"] is not None})
    if not production_ids:
        return {}
    placeholders = ", ".join("?" for _ in production_ids)
    result_rows = connection.execute(
        f"SELECT id, production_name FROM productions WHERE id IN ({placeholders})",
        production_ids,
    ).fetchall()
    return {int(row["id"]): str(row["production_name"]) for row in result_rows}


def fetch_document_dataset_memberships(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
) -> dict[int, dict[str, list[object]]]:
    document_ids = sorted({int(row["id"]) for row in rows})
    if not document_ids:
        return {}
    placeholders = ", ".join("?" for _ in document_ids)
    result_rows = connection.execute(
        f"""
        SELECT dd.document_id, ds.id AS dataset_id, ds.dataset_name
        FROM dataset_documents dd
        JOIN datasets ds ON ds.id = dd.dataset_id
        WHERE dd.document_id IN ({placeholders})
        ORDER BY dd.document_id ASC, LOWER(ds.dataset_name) ASC, ds.id ASC
        """,
        document_ids,
    ).fetchall()
    memberships: dict[int, dict[str, list[object]]] = defaultdict(lambda: {"ids": [], "names": []})
    for row in result_rows:
        payload = memberships[int(row["document_id"])]
        dataset_id = int(row["dataset_id"])
        dataset_name = str(row["dataset_name"])
        if dataset_id not in payload["ids"]:
            payload["ids"].append(dataset_id)
        if dataset_name not in payload["names"]:
            payload["names"].append(dataset_name)
    return memberships


def chunk_preview_text(text: object, *, max_chars: int = 220) -> str:
    normalized = normalize_whitespace(str(text or ""))
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3].rstrip() + "..."


COMPACT_METADATA_FIELDS = (
    "author",
    "content_type",
    "custodian",
    "date_created",
    "date_modified",
    "participants",
    "recipients",
    "subject",
    "title",
    "updated_at",
)


def payload_has_meaningful_value(value: object) -> bool:
    return value not in (None, "", [], {})


def compact_metadata_payload(metadata: object) -> dict[str, object]:
    if not isinstance(metadata, dict):
        return {}
    return {
        key: metadata[key]
        for key in COMPACT_METADATA_FIELDS
        if key in metadata and payload_has_meaningful_value(metadata[key])
    }


def compact_search_result_payload(item: dict[str, object]) -> dict[str, object]:
    compact: dict[str, object] = {"id": item["id"]}
    for key in ("control_number", "file_name", "file_type", "preview_rel_path", "preview_abs_path", "snippet", "rank"):
        if key in item and payload_has_meaningful_value(item[key]):
            compact[key] = item[key]

    metadata = compact_metadata_payload(item.get("metadata"))
    if metadata:
        compact["metadata"] = metadata
    if payload_has_meaningful_value(item.get("dataset_name")):
        compact["dataset_name"] = item["dataset_name"]
    elif payload_has_meaningful_value(item.get("dataset_names")):
        compact["dataset_names"] = item["dataset_names"]
    if payload_has_meaningful_value(item.get("production_name")):
        compact["production_name"] = item["production_name"]
    if int(item.get("attachment_count") or 0) > 0:
        compact["attachment_count"] = int(item["attachment_count"])
    if payload_has_meaningful_value(item.get("parent")):
        compact["parent"] = item["parent"]
    return compact


def compact_document_overview_payload(item: object) -> object:
    if not isinstance(item, dict):
        return item
    compact: dict[str, object] = {"document_id": item["document_id"]}
    for key in ("control_number", "file_name", "file_type", "preview_rel_path", "preview_abs_path"):
        if key in item and payload_has_meaningful_value(item[key]):
            compact[key] = item[key]

    metadata = compact_metadata_payload(item.get("metadata"))
    if metadata:
        compact["metadata"] = metadata
    if payload_has_meaningful_value(item.get("dataset_name")):
        compact["dataset_name"] = item["dataset_name"]
    elif payload_has_meaningful_value(item.get("dataset_names")):
        compact["dataset_names"] = item["dataset_names"]
    if payload_has_meaningful_value(item.get("production_name")):
        compact["production_name"] = item["production_name"]
    if int(item.get("attachment_count") or 0) > 0:
        compact["attachment_count"] = int(item["attachment_count"])
    if payload_has_meaningful_value(item.get("parent")):
        compact["parent"] = item["parent"]
    return compact


def compact_search_payload(payload: dict[str, object]) -> dict[str, object]:
    return {
        "query": payload["query"],
        "filters": payload["filters"],
        "sort": payload["sort"],
        "order": payload["order"],
        "page": payload["page"],
        "per_page": payload["per_page"],
        "total_hits": payload["total_hits"],
        "total_pages": payload["total_pages"],
        "results": [compact_search_result_payload(item) for item in payload["results"]],
    }


def compact_get_doc_payload(payload: dict[str, object]) -> dict[str, object]:
    compact = {
        "status": payload["status"],
        "document": compact_document_overview_payload(payload["document"]),
        "chunk_count": payload["chunk_count"],
        "include_text": payload["include_text"],
        "chunks": payload["chunks"],
    }
    if payload.get("text_summary") is not None:
        compact["text_summary"] = payload["text_summary"]
    return compact


def compact_search_chunk_result_payload(item: dict[str, object]) -> dict[str, object]:
    compact: dict[str, object] = {
        "document_id": item["document_id"],
        "chunk_index": item["chunk_index"],
        "char_start": item["char_start"],
        "char_end": item["char_end"],
        "citation": item["citation"],
    }
    for key in ("control_number", "file_name", "file_type", "token_estimate", "snippet", "rank"):
        if key in item and payload_has_meaningful_value(item[key]):
            compact[key] = item[key]

    metadata = compact_metadata_payload(item.get("metadata"))
    if metadata:
        compact["metadata"] = metadata
    if payload_has_meaningful_value(item.get("dataset_name")):
        compact["dataset_name"] = item["dataset_name"]
    elif payload_has_meaningful_value(item.get("dataset_names")):
        compact["dataset_names"] = item["dataset_names"]
    if payload_has_meaningful_value(item.get("production_name")):
        compact["production_name"] = item["production_name"]
    if payload_has_meaningful_value(item.get("parent")):
        compact["parent"] = item["parent"]
    return compact


def compact_search_chunks_payload(payload: dict[str, object]) -> dict[str, object]:
    if "results" not in payload:
        return payload
    return {
        "query": payload["query"],
        "filters": payload["filters"],
        "sort": payload["sort"],
        "order": payload["order"],
        "top_k": payload["top_k"],
        "per_doc_cap": payload["per_doc_cap"],
        "total_matches": payload["total_matches"],
        "results": [compact_search_chunk_result_payload(item) for item in payload["results"]],
    }


def prepare_cli_payload(command: str, payload: dict[str, object], *, verbose: bool = False) -> dict[str, object]:
    if verbose:
        return payload
    if command in {"search", "search-docs"}:
        return compact_search_payload(payload)
    if command == "get-doc":
        return compact_get_doc_payload(payload)
    if command == "search-chunks":
        return compact_search_chunks_payload(payload)
    return payload


def emit_cli_payload(command: str, payload: dict[str, object], *, verbose: bool = False) -> int:
    print(json.dumps(prepare_cli_payload(command, payload, verbose=verbose), indent=2, sort_keys=True))
    return 0


def document_chunk_rows(connection: sqlite3.Connection, document_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, document_id, chunk_index, char_start, char_end, token_estimate, text_content
        FROM document_chunks
        WHERE document_id = ?
        ORDER BY chunk_index ASC
        """,
        (document_id,),
    ).fetchall()


def reconstruct_document_text_prefix(chunk_rows: list[sqlite3.Row], max_chars: int) -> str | None:
    if not chunk_rows or max_chars <= 0:
        return None
    parts: list[str] = []
    current_end = 0
    total_length = 0
    for row in chunk_rows:
        chunk_text = str(row["text_content"] or "")
        chunk_start = int(row["char_start"])
        if not chunk_text:
            continue
        overlap = max(0, current_end - chunk_start)
        append_text = chunk_text[overlap:]
        if not append_text:
            continue
        remaining = max_chars - total_length
        if remaining <= 0:
            break
        parts.append(append_text[:remaining])
        total_length += len(parts[-1])
        current_end = max(current_end, chunk_start + len(chunk_text))
        if total_length >= max_chars:
            break
    combined = "".join(parts)
    return combined or None


def document_overview_payload(
    paths: dict[str, Path],
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    include_parent_context: bool = True,
    include_attachment_context: bool = True,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "document_id": int(row["id"]),
        "control_number": row["control_number"],
        "dataset_id": row["dataset_id"],
        "parent_document_id": row["parent_document_id"],
        "source_kind": row["source_kind"],
        "source_rel_path": row["source_rel_path"],
        "source_item_id": row["source_item_id"],
        "source_folder_path": row["source_folder_path"],
        "production_id": row["production_id"],
        **document_path_payload(paths, connection, row),
        "file_name": row["file_name"],
        "file_type": row["file_type"],
        "metadata": {
            "author": row["author"],
            "begin_attachment": row["begin_attachment"],
            "begin_bates": row["begin_bates"],
            "content_type": row["content_type"],
            "custodian": row["custodian"],
            "dataset_id": row["dataset_id"],
            "date_created": row["date_created"],
            "date_modified": row["date_modified"],
            "end_attachment": row["end_attachment"],
            "end_bates": row["end_bates"],
            "page_count": row["page_count"],
            "participants": row["participants"],
            "recipients": row["recipients"],
            "source_kind": row["source_kind"],
            "source_rel_path": row["source_rel_path"],
            "source_item_id": row["source_item_id"],
            "source_folder_path": row["source_folder_path"],
            "subject": row["subject"],
            "title": row["title"],
            "updated_at": row["updated_at"],
        },
        "manual_field_locks": normalize_string_list(row[MANUAL_FIELD_LOCKS_COLUMN]),
    }
    dataset_memberships = fetch_document_dataset_memberships(connection, [row]).get(int(row["id"]), {"ids": [], "names": []})
    dataset_ids = [int(dataset_id) for dataset_id in dataset_memberships["ids"]]
    dataset_names = [str(dataset_name) for dataset_name in dataset_memberships["names"]]
    payload["dataset_ids"] = dataset_ids
    payload["dataset_names"] = dataset_names
    if len(dataset_ids) == 1:
        payload["dataset_id"] = dataset_ids[0]
        payload["metadata"]["dataset_id"] = dataset_ids[0]
    else:
        payload["dataset_id"] = None
        payload["metadata"]["dataset_id"] = None
    if len(dataset_names) == 1:
        payload["dataset_name"] = dataset_names[0]
        payload["metadata"]["dataset_name"] = dataset_names[0]
    if row["production_id"] is not None:
        production_name = fetch_production_names(connection, [row]).get(int(row["production_id"]))
        payload["production_name"] = production_name
        payload["metadata"]["production_name"] = production_name
    if include_attachment_context and row["parent_document_id"] is None:
        attachments = fetch_attachment_summaries(connection, paths, [int(row["id"])]).get(int(row["id"]), [])
        payload["attachment_count"] = len(attachments)
        payload["attachments"] = attachments
    if include_parent_context and row["parent_document_id"] is not None:
        payload["parent"] = fetch_parent_summaries(connection, [row]).get(int(row["parent_document_id"]))
    return payload


def build_chunk_citation_payload(
    row: sqlite3.Row,
    *,
    preview_rel_path: str,
    preview_abs_path: str,
    chunk_index: int,
    char_start: int,
    char_end: int,
    snippet: str,
) -> dict[str, object]:
    return {
        "document_id": int(row["id"]),
        "control_number": row["control_number"],
        "file_name": row["file_name"],
        "chunk_index": chunk_index,
        "char_start": char_start,
        "char_end": char_end,
        "snippet": snippet,
        "preview_rel_path": preview_rel_path,
        "preview_abs_path": preview_abs_path,
    }


def filter_operators_for_field_type(field_type: str) -> list[str]:
    if field_type in {"integer", "real", "date"}:
        return ["eq", "neq", "gt", "gte", "lt", "lte", "is-null", "not-null"]
    if field_type == "boolean":
        return ["eq", "neq", "is-null", "not-null"]
    return ["eq", "neq", "contains", "is-null", "not-null"]


def catalog_description_for_field(field_name: str, *, source: str, instruction: object = None) -> str:
    if source == "builtin":
        return BUILTIN_FIELD_DESCRIPTIONS.get(field_name, normalize_whitespace(field_name.replace("_", " ")))
    if source == "virtual":
        return VIRTUAL_FIELD_DESCRIPTIONS.get(field_name, normalize_whitespace(field_name.replace("_", " ")))
    if isinstance(instruction, str) and normalize_whitespace(instruction):
        return normalize_whitespace(instruction)
    return normalize_whitespace(field_name.replace("_", " "))


def catalog_field_entry(
    field_name: str,
    field_type: str,
    *,
    source: str,
    instruction: object = None,
) -> dict[str, object]:
    return {
        "name": field_name,
        "type": field_type,
        "description": catalog_description_for_field(field_name, source=source, instruction=instruction),
        "filter_operators": filter_operators_for_field_type(field_type),
        "sortable": source != "virtual",
        "aggregatable": source != "virtual" or field_name in AGGREGATABLE_VIRTUAL_FIELDS,
        "date_granularities": ["year", "quarter", "month", "week"] if field_type == "date" else [],
    }


def search_bates(
    connection: sqlite3.Connection,
    query_begin: str,
    query_end: str,
    clauses: list[str],
    params: list[object],
) -> dict[int, dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT *
        FROM documents d
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchall()
    single_value = query_begin == query_end
    matches: dict[int, dict[str, object]] = {}
    for row in rows:
        row_begin = row["begin_bates"] or row["control_number"]
        row_end = row["end_bates"] or row["control_number"]
        rank: float | None = None
        if single_value:
            if row["control_number"] == query_begin or row["begin_bates"] == query_begin or row["end_bates"] == query_begin:
                rank = 0.0
            elif bates_inclusive_contains(row_begin, row_end, query_begin):
                rank = 1.0
        else:
            if bates_ranges_overlap(row_begin, row_end, query_begin, query_end):
                rank = 2.0
        if rank is None:
            continue
        document_id = int(row["id"])
        matches[document_id] = {
            "row": row,
            "rank": rank,
            "snippet": metadata_snippet(row),
            "bates_sort_key": bates_sort_key(row_begin),
        }
    return matches


def search_fts(
    connection: sqlite3.Connection,
    query: str,
    clauses: list[str],
    params: list[object],
) -> dict[int, dict[str, object]]:
    query_value = query.strip()
    if not query_value:
        return {}

    where_clause = " AND ".join(clauses)
    chunk_sql = f"""
        SELECT d.*, dc.text_content AS snippet_source, bm25(chunks_fts) AS rank
        FROM chunks_fts
        JOIN document_chunks dc ON dc.id = CAST(chunks_fts.chunk_id AS INTEGER)
        JOIN documents d ON d.id = dc.document_id
        WHERE chunks_fts MATCH ? AND {where_clause}
    """
    metadata_sql = f"""
        SELECT d.*, NULL AS snippet_source, bm25(documents_fts) AS rank
        FROM documents_fts
        JOIN documents d ON d.id = CAST(documents_fts.document_id AS INTEGER)
        WHERE documents_fts MATCH ? AND {where_clause}
    """

    matches: dict[int, dict[str, object]] = {}
    for sql, source in ((chunk_sql, "chunk"), (metadata_sql, "metadata")):
        try:
            rows = connection.execute(sql, [query_value, *params]).fetchall()
        except sqlite3.OperationalError:
            rows = connection.execute(sql, [f'"{query_value}"', *params]).fetchall()
        for row in rows:
            document_id = int(row["id"])
            rank = float(row["rank"])
            existing = matches.get(document_id)
            if existing is None or rank < float(existing["rank"]):
                matches[document_id] = {
                    "row": row,
                    "rank": rank,
                    "snippet": make_snippet(
                        row["snippet_source"] if source == "chunk" and row["snippet_source"] else metadata_snippet(row),
                        query_value,
                    ),
                }
    return matches


def search_browse(
    connection: sqlite3.Connection,
    clauses: list[str],
    params: list[object],
) -> dict[int, dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT *
        FROM documents d
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchall()
    return {
        int(row["id"]): {
            "row": row,
            "rank": None,
            "snippet": metadata_snippet(row),
        }
        for row in rows
    }


def coerce_sort_value(value: object) -> tuple[int, object]:
    if value is None:
        return (1, "")
    if isinstance(value, str):
        return (0, value.lower())
    return (0, value)


def sort_search_results(
    results: list[dict[str, object]],
    sort_field: str | None,
    order: str | None,
    query: str,
) -> list[dict[str, object]]:
    query_present = bool(query.strip())
    normalized_order = (order or ("asc" if (sort_field or "relevance") == "relevance" and query_present else "desc")).lower()
    stable_results = sorted(results, key=lambda item: item["id"])

    if query_present and (sort_field is None or sort_field == "relevance"):
        reverse = normalized_order == "desc"
        return sorted(stable_results, key=lambda item: (item["rank"] is None, item["rank"]), reverse=reverse)

    field_name = sort_field or "updated_at"
    reverse = normalized_order == "desc"
    return sorted(
        stable_results,
        key=lambda item: coerce_sort_value(item["row"][field_name]),
        reverse=reverse,
    )


def resolve_document_search(
    connection: sqlite3.Connection,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
) -> dict[str, object]:
    filter_summary, clauses, params = build_search_filters(connection, raw_filters)
    normalized_sort_field = sort_field
    bates_query_begin, bates_query_end = parse_bates_query(query)
    is_bates_query = bates_query_begin is not None and bates_query_end is not None
    if sort_field == "relevance" and not query.strip():
        raise RetrieverError("Sort 'relevance' requires a non-empty query.")
    if sort_field and sort_field != "relevance":
        sort_field_def = resolve_field_definition(connection, sort_field)
        if sort_field_def.get("source") == "virtual":
            raise RetrieverError(f"Cannot sort by virtual filter field: {sort_field}")
        normalized_sort_field = sort_field_def["field_name"]
    if is_bates_query:
        matches = search_bates(connection, str(bates_query_begin), str(bates_query_end), clauses, params)
    elif query.strip():
        matches = search_fts(connection, query, clauses, params)
    else:
        matches = search_browse(connection, clauses, params)

    results = [
        {
            "id": document_id,
            "rank": match["rank"],
            "snippet": match["snippet"],
            "bates_sort_key": match.get("bates_sort_key"),
            "row": match["row"],
        }
        for document_id, match in matches.items()
    ]
    if is_bates_query and normalized_sort_field is None and order is None:
        sorted_results = sorted(
            sorted(results, key=lambda item: item["id"]),
            key=lambda item: (
                item["rank"] is None,
                item["rank"],
                item.get("bates_sort_key") or (1, "", 0, ""),
            ),
        )
    else:
        sorted_results = sort_search_results(results, normalized_sort_field, order, query)
    return {
        "query": query,
        "filters": filter_summary,
        "sort": normalized_sort_field or ("bates" if is_bates_query and query.strip() else ("relevance" if query.strip() else "updated_at")),
        "order": (order or ("asc" if (is_bates_query or (query.strip() and (sort_field in (None, "relevance")))) else "desc")).lower(),
        "results": sorted_results,
    }


def search(
    root: Path,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
    page: int,
    per_page: int,
) -> dict[str, object]:
    if page < 1:
        raise RetrieverError("Page must be >= 1.")
    if per_page < 1:
        raise RetrieverError("per-page must be >= 1.")
    per_page = min(per_page, MAX_PAGE_SIZE)

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        selection = resolve_document_search(connection, query, raw_filters, sort_field, order)

        results: list[dict[str, object]] = []
        for match in selection["results"]:
            row = match["row"]
            results.append(
                {
                    "id": int(match["id"]),
                    "control_number": row["control_number"],
                    "dataset_id": row["dataset_id"],
                    "parent_document_id": row["parent_document_id"],
                    "source_kind": row["source_kind"],
                    "source_rel_path": row["source_rel_path"],
                    "source_item_id": row["source_item_id"],
                    "source_folder_path": row["source_folder_path"],
                    "production_id": row["production_id"],
                    **document_path_payload(paths, connection, row),
                    "file_name": row["file_name"],
                    "file_type": row["file_type"],
                    "snippet": str(match["snippet"]),
                    "rank": match["rank"],
                    "bates_sort_key": match.get("bates_sort_key"),
                    "metadata": {
                        "author": row["author"],
                        "begin_attachment": row["begin_attachment"],
                        "begin_bates": row["begin_bates"],
                        "content_type": row["content_type"],
                        "custodian": row["custodian"],
                        "dataset_id": row["dataset_id"],
                        "date_created": row["date_created"],
                        "date_modified": row["date_modified"],
                        "end_attachment": row["end_attachment"],
                        "end_bates": row["end_bates"],
                        "page_count": row["page_count"],
                        "participants": row["participants"],
                        "recipients": row["recipients"],
                        "source_kind": row["source_kind"],
                        "source_rel_path": row["source_rel_path"],
                        "source_item_id": row["source_item_id"],
                        "source_folder_path": row["source_folder_path"],
                        "subject": row["subject"],
                        "title": row["title"],
                        "updated_at": row["updated_at"],
                    },
                    "manual_field_locks": normalize_string_list(row[MANUAL_FIELD_LOCKS_COLUMN]),
                    "row": row,
                }
            )

        sorted_results = results
        total_hits = len(sorted_results)
        total_pages = max(1, (total_hits + per_page - 1) // per_page)
        start = (page - 1) * per_page
        end = start + per_page
        paged_results = sorted_results[start:end]
        paged_rows = [item["row"] for item in paged_results]
        production_names = fetch_production_names(connection, paged_rows)
        dataset_memberships = fetch_document_dataset_memberships(connection, paged_rows)
        attachment_summaries = fetch_attachment_summaries(
            connection,
            paths,
            [int(row["id"]) for row in paged_rows if row["parent_document_id"] is None],
        )
        parent_summaries = fetch_parent_summaries(
            connection,
            [row for row in paged_rows if row["parent_document_id"] is not None],
        )
        for item in paged_results:
            row = item["row"]
            memberships = dataset_memberships.get(int(row["id"]), {"ids": [], "names": []})
            dataset_ids = [int(dataset_id) for dataset_id in memberships["ids"]]
            dataset_names = [str(dataset_name) for dataset_name in memberships["names"]]
            item["dataset_ids"] = dataset_ids
            item["dataset_names"] = dataset_names
            item["metadata"]["dataset_ids"] = dataset_ids
            item["metadata"]["dataset_names"] = dataset_names
            if len(dataset_ids) == 1:
                item["dataset_id"] = dataset_ids[0]
                item["metadata"]["dataset_id"] = dataset_ids[0]
            else:
                item["dataset_id"] = None
                item["metadata"]["dataset_id"] = None
            if len(dataset_names) == 1:
                item["dataset_name"] = dataset_names[0]
                item["metadata"]["dataset_name"] = dataset_names[0]
            if row["production_id"] is not None:
                item["production_name"] = production_names.get(int(row["production_id"]))
                item["metadata"]["production_name"] = production_names.get(int(row["production_id"]))
            if row["parent_document_id"] is None:
                attachments = attachment_summaries.get(int(row["id"]), [])
                item["attachment_count"] = len(attachments)
                item["attachments"] = attachments
            else:
                item["parent"] = parent_summaries.get(int(row["parent_document_id"]))
            item.pop("bates_sort_key", None)
            item.pop("row", None)

        return {
            "query": selection["query"],
            "filters": selection["filters"],
            "sort": selection["sort"],
            "order": selection["order"],
            "page": page,
            "per_page": per_page,
            "total_hits": total_hits,
            "total_pages": total_pages,
            "results": paged_results,
        }
    finally:
        connection.close()


def search_docs(
    root: Path,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
    page: int,
    per_page: int,
) -> dict[str, object]:
    return search(root, query, raw_filters, sort_field, order, page, per_page)


def catalog(root: Path) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        reconcile_custom_fields_registry(connection, repair=True)
        intrinsic = [
            catalog_field_entry(field_name, field_type, source="builtin")
            for field_name, field_type in sorted(BUILTIN_FIELD_TYPES.items())
            if field_name not in CATALOG_EXCLUDED_BUILTIN_FIELDS
        ]
        custom_rows = connection.execute(
            """
            SELECT field_name, field_type, instruction
            FROM custom_fields_registry
            ORDER BY field_name ASC
            """
        ).fetchall()
        columns = table_columns(connection, "documents")
        custom = [
            catalog_field_entry(str(row["field_name"]), str(row["field_type"]), source="custom", instruction=row["instruction"])
            for row in custom_rows
            if row["field_name"] in columns and row["field_name"] not in CATALOG_EXCLUDED_CUSTOM_FIELDS
        ]
        virtual = [
            catalog_field_entry(field_name, field_type, source="virtual")
            for field_name, field_type in sorted(VIRTUAL_FILTER_FIELD_TYPES.items())
        ]
        return {
            "status": "ok",
            "intrinsic": intrinsic,
            "custom": custom,
            "virtual": virtual,
        }
    finally:
        connection.close()


def fetch_visible_document_rows_by_ids(
    connection: sqlite3.Connection,
    document_ids: list[int],
) -> list[sqlite3.Row]:
    normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in document_ids))
    if not normalized_document_ids:
        return []
    placeholders = ", ".join("?" for _ in normalized_document_ids)
    rows = connection.execute(
        f"""
        SELECT
          d.*,
          EXISTS (
            SELECT 1
            FROM dataset_documents dd
            WHERE dd.document_id = d.id
          ) AS has_dataset_membership
        FROM documents d
        WHERE id IN ({placeholders})
        """,
        normalized_document_ids,
    ).fetchall()
    rows_by_id = {int(row["id"]): row for row in rows}
    missing_ids: list[int] = []
    lifecycle_hidden: list[str] = []
    membership_hidden: list[int] = []
    visible_rows_by_id: dict[int, sqlite3.Row] = {}
    for document_id in normalized_document_ids:
        row = rows_by_id.get(document_id)
        if row is None:
            missing_ids.append(document_id)
            continue
        if row["lifecycle_status"] in {"missing", "deleted"}:
            lifecycle_hidden.append(f"{document_id} ({row['lifecycle_status']})")
            continue
        if not bool(row["has_dataset_membership"]):
            membership_hidden.append(document_id)
            continue
        visible_rows_by_id[document_id] = row

    errors: list[str] = []
    if missing_ids:
        errors.append(
            "Unknown document id" + ("" if len(missing_ids) == 1 else "s") + f": {', '.join(str(document_id) for document_id in missing_ids)}"
        )
    if lifecycle_hidden:
        errors.append(
            "Document id"
            + ("" if len(lifecycle_hidden) == 1 else "s")
            + " not visible due to lifecycle_status: "
            + ", ".join(lifecycle_hidden)
        )
    if membership_hidden:
        errors.append(
            "Document id"
            + ("" if len(membership_hidden) == 1 else "s")
            + " not visible because they have no dataset memberships: "
            + ", ".join(str(document_id) for document_id in membership_hidden)
        )
    if errors:
        raise RetrieverError(" ".join(errors))
    return [visible_rows_by_id[document_id] for document_id in normalized_document_ids]


def fetch_attachment_parent_ids(
    connection: sqlite3.Connection,
    parent_ids: list[int],
) -> set[int]:
    if not parent_ids:
        return set()
    placeholders = ", ".join("?" for _ in parent_ids)
    rows = connection.execute(
        f"""
        SELECT DISTINCT parent_document_id
        FROM documents
        WHERE parent_document_id IN ({placeholders})
          AND lifecycle_status NOT IN ('missing', 'deleted')
        """,
        parent_ids,
    ).fetchall()
    return {
        int(row["parent_document_id"])
        for row in rows
        if row["parent_document_id"] is not None
    }


def resolve_export_field_definitions(
    connection: sqlite3.Connection,
    raw_fields: list[str] | None,
) -> list[dict[str, str]]:
    if not raw_fields:
        raise RetrieverError("export-csv requires at least one --field.")
    resolved_fields: list[dict[str, str]] = []
    seen_fields: set[str] = set()
    for raw_field in raw_fields:
        field_def = resolve_field_definition(connection, raw_field)
        normalized_field_name = str(field_def["field_name"])
        if normalized_field_name in seen_fields:
            raise RetrieverError(f"Duplicate export field: {normalized_field_name}")
        seen_fields.add(normalized_field_name)
        resolved_fields.append(
            {
                "requested_name": raw_field,
                "field_name": normalized_field_name,
                "field_type": str(field_def["field_type"]),
                "source": str(field_def.get("source") or ""),
            }
        )
    return resolved_fields


def path_within(candidate: Path, parent: Path) -> bool:
    try:
        candidate.relative_to(parent)
        return True
    except ValueError:
        return False


def resolve_export_output_path(paths: dict[str, Path], raw_output_path: str) -> Path:
    normalized_output_path = raw_output_path.strip()
    if not normalized_output_path:
        raise RetrieverError("Output path cannot be empty.")
    exports_dir = (paths["state_dir"] / "exports").resolve()
    requested_path = Path(normalized_output_path).expanduser()
    if requested_path.is_absolute():
        resolved_path = requested_path.resolve()
    else:
        resolved_path = (exports_dir / requested_path).resolve()
        if not path_within(resolved_path, exports_dir):
            raise RetrieverError(f"Relative output paths must stay within {exports_dir}.")
    if resolved_path.exists() and resolved_path.is_dir():
        raise RetrieverError(f"Output path is a directory: {resolved_path}")
    workspace_root = paths["root"].resolve()
    if path_within(resolved_path, workspace_root) and not path_within(resolved_path, exports_dir):
        raise RetrieverError(
            f"Workspace-internal output paths must live under {exports_dir} to avoid re-ingesting exported artifacts."
        )
    return resolved_path


def build_export_context(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
    field_defs: list[dict[str, str]],
) -> dict[str, object]:
    requested_field_names = {field_def["field_name"] for field_def in field_defs}
    context: dict[str, object] = {
        "dataset_memberships": {},
        "production_names": {},
        "attachment_parent_ids": set(),
    }
    if "dataset_id" in requested_field_names or "dataset_name" in requested_field_names:
        context["dataset_memberships"] = fetch_document_dataset_memberships(connection, rows)
    if "production_name" in requested_field_names:
        context["production_names"] = fetch_production_names(connection, rows)
    if "has_attachments" in requested_field_names:
        context["attachment_parent_ids"] = fetch_attachment_parent_ids(
            connection,
            [int(row["id"]) for row in rows if row["parent_document_id"] is None],
        )
    return context


def export_field_value(
    row: sqlite3.Row,
    field_def: dict[str, str],
    context: dict[str, object],
) -> object:
    field_name = field_def["field_name"]
    document_id = int(row["id"])
    if field_name == "dataset_name":
        dataset_membership = context["dataset_memberships"].get(document_id, {"names": []})
        return "; ".join(str(dataset_name) for dataset_name in dataset_membership["names"])
    if field_name == "dataset_id":
        dataset_membership = context["dataset_memberships"].get(document_id, {"ids": []})
        membership_ids = [str(int(dataset_id)) for dataset_id in dataset_membership["ids"]]
        if membership_ids:
            return "; ".join(membership_ids)
        return row["dataset_id"]
    if field_name == "production_name":
        if row["production_id"] is None:
            return None
        return context["production_names"].get(int(row["production_id"]))
    if field_name == "is_attachment":
        return row["parent_document_id"] is not None
    if field_name == "has_attachments":
        return int(row["id"]) in context["attachment_parent_ids"]
    return row[field_name]


def serialize_export_cell_value(value: object, field_type: str) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(item) for item in value if item is not None)
    if field_type == "boolean":
        return "true" if bool(value) else "false"
    return str(value)


def export_csv(
    root: Path,
    raw_output_path: str,
    raw_fields: list[str] | None,
    document_ids: list[int] | None,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        field_defs = resolve_export_field_definitions(connection, raw_fields)
        output_path = resolve_export_output_path(paths, raw_output_path)

        normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in (document_ids or [])))
        if normalized_document_ids and (query.strip() or raw_filters or sort_field or order):
            raise RetrieverError("export-csv accepts either --doc-id selectors or query/filter selectors, not both.")

        if normalized_document_ids:
            rows = fetch_visible_document_rows_by_ids(connection, normalized_document_ids)
            selector: dict[str, object] = {
                "mode": "document_ids",
                "document_ids": normalized_document_ids,
            }
        else:
            selection = resolve_document_search(connection, query, raw_filters, sort_field, order)
            rows = [item["row"] for item in selection["results"]]
            selector = {
                "mode": "search",
                "query": selection["query"],
                "filters": selection["filters"],
                "sort": selection["sort"],
                "order": selection["order"],
            }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        overwrote_existing_file = output_path.exists()
        context = build_export_context(connection, rows, field_defs)
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([field_def["field_name"] for field_def in field_defs])
            for row in rows:
                writer.writerow(
                    [
                        serialize_export_cell_value(
                            export_field_value(row, field_def, context),
                            field_def["field_type"],
                        )
                        for field_def in field_defs
                    ]
                )

        output_rel_path = None
        try:
            output_rel_path = output_path.relative_to(root).as_posix()
        except ValueError:
            output_rel_path = None

        return {
            "status": "ok",
            "output_path": str(output_path),
            "output_rel_path": output_rel_path,
            "document_count": len(rows),
            "field_count": len(field_defs),
            "fields": field_defs,
            "selector": selector,
            "overwrote_existing_file": overwrote_existing_file,
            "file_size": file_size_bytes(output_path),
        }
    finally:
        connection.close()


def normalize_archive_member_path(raw_rel_path: str, *, label: str = "Archive member path") -> str:
    normalized = normalize_whitespace(raw_rel_path)
    if not normalized:
        raise RetrieverError(f"{label} cannot be empty.")
    candidate = Path(normalized)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise RetrieverError(f"{label} must stay within the archive root: {raw_rel_path!r}")
    return candidate.as_posix()


def add_archive_file_once(
    archive: zipfile.ZipFile,
    written_member_paths: set[str],
    source_path: Path,
    archive_rel_path: str,
) -> str:
    member_path = normalize_archive_member_path(archive_rel_path)
    if member_path in written_member_paths:
        return member_path
    archive.write(source_path, arcname=member_path)
    written_member_paths.add(member_path)
    return member_path


def add_archive_bytes_once(
    archive: zipfile.ZipFile,
    written_member_paths: set[str],
    payload_bytes: bytes,
    archive_rel_path: str,
) -> str:
    member_path = normalize_archive_member_path(archive_rel_path)
    if member_path in written_member_paths:
        return member_path
    archive.writestr(member_path, payload_bytes)
    written_member_paths.add(member_path)
    return member_path


def document_preview_rows(connection: sqlite3.Connection, document_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT rel_preview_path, preview_type, label, ordinal, created_at
        FROM document_previews
        WHERE document_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (document_id,),
    ).fetchall()


def document_source_part_rows(connection: sqlite3.Connection, document_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT part_kind, rel_source_path, ordinal, label, created_at
        FROM document_source_parts
        WHERE document_id = ?
        ORDER BY
          CASE part_kind WHEN 'native' THEN 0 WHEN 'image' THEN 1 ELSE 2 END ASC,
          ordinal ASC,
          id ASC
        """,
        (document_id,),
    ).fetchall()


def document_text_revision_rows(connection: sqlite3.Connection, document_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM text_revisions
        WHERE document_id = ?
        ORDER BY id ASC
        """,
        (document_id,),
    ).fetchall()


def document_source_text_body(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    row: sqlite3.Row,
) -> str | None:
    revision_ids: list[int] = []
    for field_name in ("source_text_revision_id", "active_search_text_revision_id"):
        if field_name in row.keys() and row[field_name] is not None:
            revision_id = int(row[field_name])
            if revision_id not in revision_ids:
                revision_ids.append(revision_id)
    for revision_id in revision_ids:
        revision_row = connection.execute(
            """
            SELECT storage_rel_path
            FROM text_revisions
            WHERE id = ?
            """,
            (revision_id,),
        ).fetchone()
        if revision_row is None:
            continue
        revision_body = read_text_revision_body(paths, revision_row["storage_rel_path"])
        if revision_body is not None:
            return revision_body
    chunk_rows = document_chunk_rows(connection, int(row["id"]))
    if not chunk_rows:
        return None
    return "\n\n".join(str(chunk_row["text_content"] or "") for chunk_row in chunk_rows)


def build_synthetic_document_export_payload(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    row: sqlite3.Row,
    *,
    preview_rows: list[sqlite3.Row],
    source_part_rows: list[sqlite3.Row],
) -> bytes:
    payload = {
        "document_id": int(row["id"]),
        "control_number": row["control_number"],
        "rel_path": row["rel_path"],
        "file_name": row["file_name"],
        "file_type": row["file_type"],
        "source_kind": row["source_kind"],
        "source_rel_path": row["source_rel_path"],
        "source_item_id": row["source_item_id"],
        "production_id": row["production_id"],
        "parent_document_id": row["parent_document_id"],
        "metadata": {
            "author": row["author"],
            "content_type": row["content_type"],
            "custodian": row["custodian"],
            "date_created": row["date_created"],
            "date_modified": row["date_modified"],
            "participants": row["participants"],
            "recipients": row["recipients"],
            "subject": row["subject"],
            "title": row["title"],
            "updated_at": row["updated_at"],
        },
        "preview_rel_paths": [
            str(Path(".retriever") / str(preview_row["rel_preview_path"]))
            for preview_row in preview_rows
        ],
        "source_part_rel_paths": [
            str(source_part_row["rel_source_path"])
            for source_part_row in source_part_rows
        ],
        "text_content": document_source_text_body(connection, paths, row),
    }
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def archive_document_files(
    archive: zipfile.ZipFile,
    written_member_paths: set[str],
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    row: sqlite3.Row,
) -> tuple[dict[str, object], list[str]]:
    document_id = int(row["id"])
    rel_path = normalize_archive_member_path(str(row["rel_path"]), label=f"Document {document_id} rel_path")
    preview_rows = document_preview_rows(connection, document_id)
    source_part_rows = document_source_part_rows(connection, document_id)
    warnings: list[str] = []
    exported_rel_paths: list[str] = []
    preview_rel_paths: list[str] = []
    source_part_entries: list[dict[str, object]] = []

    source_path = resolve_workspace_artifact_path(paths["root"], str(row["rel_path"]))
    if source_path is not None and source_path.exists():
        exported_rel_paths.append(add_archive_file_once(archive, written_member_paths, source_path, rel_path))
        document_entry_kind = "copied"
    else:
        descriptor_bytes = build_synthetic_document_export_payload(
            connection,
            paths,
            row,
            preview_rows=preview_rows,
            source_part_rows=source_part_rows,
        )
        exported_rel_paths.append(add_archive_bytes_once(archive, written_member_paths, descriptor_bytes, rel_path))
        document_entry_kind = "synthetic"
        if str(row["source_kind"] or "") not in {PRODUCTION_SOURCE_KIND, PST_SOURCE_KIND, MBOX_SOURCE_KIND}:
            warnings.append(
                f"Document {document_id} has no on-disk primary artifact at {row['rel_path']}; wrote a synthetic descriptor instead."
            )

    for preview_row in preview_rows:
        preview_source_path = paths["state_dir"] / str(preview_row["rel_preview_path"])
        archive_rel_path = normalize_archive_member_path(
            str(Path(".retriever") / str(preview_row["rel_preview_path"])),
            label=f"Preview path for document {document_id}",
        )
        if not preview_source_path.exists():
            warnings.append(
                f"Preview artifact is missing for document {document_id}: {archive_rel_path}"
            )
            continue
        preview_rel_paths.append(add_archive_file_once(archive, written_member_paths, preview_source_path, archive_rel_path))
        if archive_rel_path not in exported_rel_paths:
            exported_rel_paths.append(archive_rel_path)

    for source_part_row in source_part_rows:
        source_rel_path = normalize_archive_member_path(
            str(source_part_row["rel_source_path"]),
            label=f"Source part path for document {document_id}",
        )
        source_part_entry = {
            "part_kind": str(source_part_row["part_kind"]),
            "rel_path": source_rel_path,
            "ordinal": int(source_part_row["ordinal"] or 0),
            "label": source_part_row["label"],
        }
        source_part_path = resolve_workspace_artifact_path(paths["root"], str(source_part_row["rel_source_path"]))
        if source_part_path is None or not source_part_path.exists():
            source_part_entry["missing"] = True
            warnings.append(
                f"Source part is missing for document {document_id}: {source_rel_path}"
            )
            source_part_entries.append(source_part_entry)
            continue
        source_part_entry["archive_rel_path"] = add_archive_file_once(
            archive,
            written_member_paths,
            source_part_path,
            source_rel_path,
        )
        if source_part_entry["archive_rel_path"] not in exported_rel_paths:
            exported_rel_paths.append(str(source_part_entry["archive_rel_path"]))
        source_part_entries.append(source_part_entry)

    return (
        {
            "document_id": document_id,
            "control_number": row["control_number"],
            "file_name": row["file_name"],
            "file_type": row["file_type"],
            "rel_path": rel_path,
            "source_kind": row["source_kind"],
            "parent_document_id": row["parent_document_id"],
            "document_entry_kind": document_entry_kind,
            "preview_rel_paths": preview_rel_paths,
            "source_part_entries": source_part_entries,
            "exported_rel_paths": exported_rel_paths,
        },
        warnings,
    )


def archive_document_text_revisions(
    archive: zipfile.ZipFile,
    written_member_paths: set[str],
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    document_id: int,
) -> tuple[list[dict[str, object]], list[str]]:
    entries: list[dict[str, object]] = []
    warnings: list[str] = []
    for revision_row in document_text_revision_rows(connection, document_id):
        entry = {
            "id": int(revision_row["id"]),
            "revision_kind": revision_row["revision_kind"],
            "storage_rel_path": revision_row["storage_rel_path"],
        }
        storage_rel_path = normalize_whitespace(str(revision_row["storage_rel_path"] or ""))
        if not storage_rel_path:
            entries.append(entry)
            continue
        source_path = paths["state_dir"] / storage_rel_path
        archive_rel_path = normalize_archive_member_path(
            str(Path(".retriever") / storage_rel_path),
            label=f"Text revision path for document {document_id}",
        )
        if not source_path.exists():
            warnings.append(
                f"Text revision body is missing for document {document_id}: {archive_rel_path}"
            )
            entries.append(entry)
            continue
        entry["archive_rel_path"] = add_archive_file_once(
            archive,
            written_member_paths,
            source_path,
            archive_rel_path,
        )
        entries.append(entry)
    return entries, warnings


def sqlite_row_to_dict(row: sqlite3.Row) -> dict[str, object]:
    return {str(key): row[key] for key in row.keys()}


def table_column_names_in_order(connection: sqlite3.Connection, table_name: str) -> list[str]:
    return [str(row["name"]) for row in table_info(connection, table_name)]


def insert_row_dicts(
    connection: sqlite3.Connection,
    table_name: str,
    row_dicts: list[dict[str, object]],
    *,
    column_names: list[str] | None = None,
) -> None:
    if not row_dicts:
        return
    ordered_columns = column_names or table_column_names_in_order(connection, table_name)
    usable_columns = [column_name for column_name in ordered_columns if any(column_name in row_dict for row_dict in row_dicts)]
    if not usable_columns:
        return
    placeholders = ", ".join("?" for _ in usable_columns)
    connection.executemany(
        f"""
        INSERT INTO {quote_identifier(table_name)} ({', '.join(quote_identifier(column_name) for column_name in usable_columns)})
        VALUES ({placeholders})
        """,
        [[row_dict.get(column_name) for column_name in usable_columns] for row_dict in row_dicts],
    )


def portable_context_parent_rows(
    connection: sqlite3.Connection,
    selected_document_rows: list[sqlite3.Row],
) -> list[sqlite3.Row]:
    selected_document_ids = {int(row["id"]) for row in selected_document_rows}
    parent_ids = sorted(
        {
            int(row["parent_document_id"])
            for row in selected_document_rows
            if row["parent_document_id"] is not None and int(row["parent_document_id"]) not in selected_document_ids
        }
    )
    if not parent_ids:
        return []
    placeholders = ", ".join("?" for _ in parent_ids)
    return connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE id IN ({placeholders})
        ORDER BY id ASC
        """,
        parent_ids,
    ).fetchall()


def portable_document_row_dict(
    row: sqlite3.Row,
    *,
    preserve_text_revisions: bool,
) -> dict[str, object]:
    payload = sqlite_row_to_dict(row)
    if not preserve_text_revisions:
        for field_name in (
            "source_text_revision_id",
            "active_search_text_revision_id",
            "active_text_source_kind",
            "active_text_language",
            "active_text_quality_score",
        ):
            if field_name in payload:
                payload[field_name] = None
    return payload


def build_portable_workspace_db(
    source_connection: sqlite3.Connection,
    portable_root: Path,
    selected_document_rows: list[sqlite3.Row],
) -> dict[str, object]:
    portable_paths = workspace_paths(portable_root)
    ensure_layout(portable_paths)
    target_connection = sqlite3.connect(portable_paths["db_path"])
    target_connection.row_factory = sqlite3.Row
    target_connection.execute("PRAGMA busy_timeout = 5000")
    target_connection.execute("PRAGMA foreign_keys = ON")
    try:
        apply_schema(target_connection, portable_root)

        custom_field_rows = source_connection.execute(
            """
            SELECT *
            FROM custom_fields_registry
            ORDER BY id ASC
            """
        ).fetchall()
        for custom_field_row in custom_field_rows:
            field_name = str(custom_field_row["field_name"])
            sql_type = REGISTRY_FIELD_TYPES.get(str(custom_field_row["field_type"]))
            if sql_type is None:
                raise RetrieverError(f"Unsupported custom field type for portable export: {custom_field_row['field_type']!r}")
            target_connection.execute(
                f"ALTER TABLE documents ADD COLUMN {quote_identifier(field_name)} {sql_type}"
            )
        insert_row_dicts(
            target_connection,
            "custom_fields_registry",
            [sqlite_row_to_dict(row) for row in custom_field_rows],
        )

        selected_document_ids = sorted({int(row["id"]) for row in selected_document_rows})
        context_parent_rows = portable_context_parent_rows(source_connection, selected_document_rows)
        retained_row_by_id = {
            int(row["id"]): row
            for row in [*context_parent_rows, *selected_document_rows]
        }
        retained_rows = sorted(
            retained_row_by_id.values(),
            key=lambda row: (0 if row["parent_document_id"] is None else 1, int(row["id"])),
        )
        stub_document_ids = sorted(
            document_id for document_id in retained_row_by_id if document_id not in selected_document_ids
        )

        dataset_document_rows = source_connection.execute(
            f"""
            SELECT *
            FROM dataset_documents
            WHERE document_id IN ({', '.join('?' for _ in selected_document_ids)})
            ORDER BY id ASC
            """ if selected_document_ids else """
            SELECT *
            FROM dataset_documents
            WHERE 0
            """,
            selected_document_ids,
        ).fetchall()
        dataset_ids = sorted(
            {
                int(row["dataset_id"])
                for row in retained_rows
                if row["dataset_id"] is not None
            }
            | {
                int(row["dataset_id"])
                for row in dataset_document_rows
                if row["dataset_id"] is not None
            }
        )
        dataset_rows = source_connection.execute(
            f"""
            SELECT *
            FROM datasets
            WHERE id IN ({', '.join('?' for _ in dataset_ids)})
            ORDER BY id ASC
            """ if dataset_ids else """
            SELECT *
            FROM datasets
            WHERE 0
            """,
            dataset_ids,
        ).fetchall()
        dataset_source_rows = source_connection.execute(
            f"""
            SELECT *
            FROM dataset_sources
            WHERE dataset_id IN ({', '.join('?' for _ in dataset_ids)})
            ORDER BY id ASC
            """ if dataset_ids else """
            SELECT *
            FROM dataset_sources
            WHERE 0
            """,
            dataset_ids,
        ).fetchall()
        production_ids = sorted({int(row["production_id"]) for row in retained_rows if row["production_id"] is not None})
        production_rows = source_connection.execute(
            f"""
            SELECT *
            FROM productions
            WHERE id IN ({', '.join('?' for _ in production_ids)})
            ORDER BY id ASC
            """ if production_ids else """
            SELECT *
            FROM productions
            WHERE 0
            """,
            production_ids,
        ).fetchall()
        container_pairs = sorted(
            {
                (str(row["source_kind"]), str(row["source_rel_path"]))
                for row in retained_rows
                if normalize_whitespace(str(row["source_kind"] or "")).lower() in {PST_SOURCE_KIND, MBOX_SOURCE_KIND}
                and normalize_whitespace(str(row["source_rel_path"] or ""))
            }
        )
        container_source_rows: list[sqlite3.Row] = []
        for source_kind, source_rel_path in container_pairs:
            row = source_connection.execute(
                """
                SELECT *
                FROM container_sources
                WHERE source_kind = ? AND source_rel_path = ?
                """,
                (source_kind, source_rel_path),
            ).fetchone()
            if row is not None:
                container_source_rows.append(row)

        preview_rows = source_connection.execute(
            f"""
            SELECT *
            FROM document_previews
            WHERE document_id IN ({', '.join('?' for _ in selected_document_ids)})
            ORDER BY id ASC
            """ if selected_document_ids else """
            SELECT *
            FROM document_previews
            WHERE 0
            """,
            selected_document_ids,
        ).fetchall()
        source_part_rows = source_connection.execute(
            f"""
            SELECT *
            FROM document_source_parts
            WHERE document_id IN ({', '.join('?' for _ in selected_document_ids)})
            ORDER BY id ASC
            """ if selected_document_ids else """
            SELECT *
            FROM document_source_parts
            WHERE 0
            """,
            selected_document_ids,
        ).fetchall()
        chunk_rows = source_connection.execute(
            f"""
            SELECT *
            FROM document_chunks
            WHERE document_id IN ({', '.join('?' for _ in selected_document_ids)})
            ORDER BY id ASC
            """ if selected_document_ids else """
            SELECT *
            FROM document_chunks
            WHERE 0
            """,
            selected_document_ids,
        ).fetchall()
        text_revision_rows = source_connection.execute(
            f"""
            SELECT *
            FROM text_revisions
            WHERE document_id IN ({', '.join('?' for _ in selected_document_ids)})
            ORDER BY id ASC
            """ if selected_document_ids else """
            SELECT *
            FROM text_revisions
            WHERE 0
            """,
            selected_document_ids,
        ).fetchall()
        text_revision_ids = [int(row["id"]) for row in text_revision_rows]
        text_revision_segment_rows = source_connection.execute(
            f"""
            SELECT *
            FROM text_revision_segments
            WHERE revision_id IN ({', '.join('?' for _ in text_revision_ids)})
            ORDER BY id ASC
            """ if text_revision_ids else """
            SELECT *
            FROM text_revision_segments
            WHERE 0
            """,
            text_revision_ids,
        ).fetchall()

        target_connection.execute("PRAGMA foreign_keys = OFF")
        target_connection.execute("BEGIN")
        try:
            insert_row_dicts(target_connection, "datasets", [sqlite_row_to_dict(row) for row in dataset_rows])
            insert_row_dicts(target_connection, "dataset_sources", [sqlite_row_to_dict(row) for row in dataset_source_rows])
            insert_row_dicts(target_connection, "productions", [sqlite_row_to_dict(row) for row in production_rows])
            insert_row_dicts(target_connection, "container_sources", [sqlite_row_to_dict(row) for row in container_source_rows])
            insert_row_dicts(
                target_connection,
                "documents",
                [
                    portable_document_row_dict(
                        row,
                        preserve_text_revisions=int(row["id"]) not in set(stub_document_ids),
                    )
                    for row in retained_rows
                ],
                column_names=table_column_names_in_order(target_connection, "documents"),
            )
            insert_row_dicts(target_connection, "dataset_documents", [sqlite_row_to_dict(row) for row in dataset_document_rows])
            insert_row_dicts(target_connection, "document_previews", [sqlite_row_to_dict(row) for row in preview_rows])
            insert_row_dicts(target_connection, "document_source_parts", [sqlite_row_to_dict(row) for row in source_part_rows])
            insert_row_dicts(
                target_connection,
                "text_revisions",
                [
                    {
                        **sqlite_row_to_dict(row),
                        "created_by_job_version_id": None,
                    }
                    for row in text_revision_rows
                ],
            )
            insert_row_dicts(
                target_connection,
                "text_revision_segments",
                [sqlite_row_to_dict(row) for row in text_revision_segment_rows],
            )
            insert_row_dicts(target_connection, "document_chunks", [sqlite_row_to_dict(row) for row in chunk_rows])
            for document_id in selected_document_ids:
                refresh_documents_fts_row(target_connection, document_id)
            if chunk_rows:
                target_connection.executemany(
                    """
                    INSERT INTO chunks_fts (chunk_id, document_id, text_content)
                    VALUES (?, ?, ?)
                    """,
                    [
                        (row["id"], row["document_id"], row["text_content"])
                        for row in chunk_rows
                    ],
                )
            target_connection.commit()
        except Exception:
            target_connection.rollback()
            raise
        target_connection.execute("PRAGMA foreign_keys = ON")
        foreign_key_issues = target_connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_issues:
            raise RetrieverError(f"Portable workspace export failed foreign key validation: {foreign_key_issues[0]}")
        return {
            "db_path": portable_paths["db_path"],
            "selected_document_ids": selected_document_ids,
            "retained_document_ids": [int(row["id"]) for row in retained_rows],
            "stub_document_ids": stub_document_ids,
        }
    finally:
        target_connection.close()


def export_archive(
    root: Path,
    raw_output_path: str,
    *,
    dataset_ids: list[int] | None = None,
    dataset_names: list[str] | None = None,
    document_ids: list[int] | None = None,
    control_numbers: list[str] | None = None,
    query: str | None = None,
    raw_filters: list[list[str]] | None = None,
    from_run_id: int | None = None,
    exclude_dataset_ids: list[int] | None = None,
    exclude_dataset_names: list[str] | None = None,
    exclude_document_ids: list[int] | None = None,
    exclude_control_numbers: list[str] | None = None,
    exclude_query: str | None = None,
    exclude_filters: list[list[str]] | None = None,
    family_mode: str = "exact",
    seed_limit: int | None = None,
    portable_workspace: bool = False,
) -> dict[str, object]:
    normalized_family_mode = normalize_run_family_mode(family_mode)
    if seed_limit is not None and seed_limit < 1:
        raise RetrieverError("Archive limit must be >= 1.")

    selector = normalize_run_selector_spec(
        dataset_ids=dataset_ids,
        dataset_names=dataset_names,
        document_ids=document_ids,
        control_numbers=control_numbers,
        query=query,
        raw_filters=raw_filters,
        from_run_id=from_run_id,
    )
    exclude_selector = normalize_run_selector_spec(
        dataset_ids=exclude_dataset_ids,
        dataset_names=exclude_dataset_names,
        document_ids=exclude_document_ids,
        control_numbers=exclude_control_numbers,
        query=exclude_query,
        raw_filters=exclude_filters,
        from_run_id=None,
    )
    if not selector_has_inputs(selector):
        raise RetrieverError("Archive selector must include at least one inclusion input.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        output_path = resolve_export_output_path(paths, raw_output_path)
        selected_documents, _ = plan_selected_documents(
            connection,
            selector=selector,
            exclude_selector=exclude_selector,
            family_mode=normalized_family_mode,
            seed_limit=seed_limit,
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        overwrote_existing_file = output_path.exists()
        written_member_paths: set[str] = set()
        manifest_document_entries: list[dict[str, object]] = []
        warnings: list[str] = []
        created_at = utc_now()

        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for selected_document in selected_documents:
                document_row = selected_document["document_row"]
                manifest_entry, document_warnings = archive_document_files(
                    archive,
                    written_member_paths,
                    connection,
                    paths,
                    document_row,
                )
                manifest_entry["ordinal"] = int(selected_document["ordinal"])
                manifest_entry["inclusion_reason"] = selected_document["inclusion_reason"]
                if portable_workspace:
                    revision_entries, revision_warnings = archive_document_text_revisions(
                        archive,
                        written_member_paths,
                        connection,
                        paths,
                        int(document_row["id"]),
                    )
                    manifest_entry["text_revision_entries"] = revision_entries
                    warnings.extend(revision_warnings)
                manifest_document_entries.append(manifest_entry)
                warnings.extend(document_warnings)

            portable_workspace_payload = None
            if portable_workspace:
                with tempfile.TemporaryDirectory(prefix="retriever-portable-workspace-") as tempdir:
                    portable_root = Path(tempdir) / "workspace"
                    portable_workspace_payload = build_portable_workspace_db(
                        connection,
                        portable_root,
                        [selected_document["document_row"] for selected_document in selected_documents],
                    )
                    add_archive_file_once(
                        archive,
                        written_member_paths,
                        Path(portable_workspace_payload["db_path"]),
                        ".retriever/retriever.db",
                    )

            manifest_payload = {
                "status": "ok",
                "created_at": created_at,
                "selector": selector,
                "exclude_selector": exclude_selector,
                "family_mode": normalized_family_mode,
                "seed_limit": seed_limit,
                "document_count": len(selected_documents),
                "portable_workspace": portable_workspace,
                "portable_workspace_document_ids": (
                    portable_workspace_payload["selected_document_ids"]
                    if portable_workspace_payload is not None
                    else []
                ),
                "portable_workspace_stub_document_ids": (
                    portable_workspace_payload["stub_document_ids"]
                    if portable_workspace_payload is not None
                    else []
                ),
                "documents": manifest_document_entries,
                "warnings": warnings,
            }
            add_archive_bytes_once(
                archive,
                written_member_paths,
                (json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
                ".retriever/export-manifest.json",
            )

        output_rel_path = None
        try:
            output_rel_path = output_path.relative_to(root).as_posix()
        except ValueError:
            output_rel_path = None

        return {
            "status": "ok",
            "created_at": created_at,
            "output_path": str(output_path),
            "output_rel_path": output_rel_path,
            "document_count": len(selected_documents),
            "selector": selector,
            "exclude_selector": exclude_selector,
            "family_mode": normalized_family_mode,
            "seed_limit": seed_limit,
            "portable_workspace": portable_workspace,
            "manifest_rel_path": ".retriever/export-manifest.json",
            "archive_member_count": len(written_member_paths),
            "documents": manifest_document_entries,
            "warnings": warnings,
            "overwrote_existing_file": overwrote_existing_file,
            "file_size": file_size_bytes(output_path),
        }
    finally:
        connection.close()


def get_doc(
    root: Path,
    document_id: int,
    include_text: str,
    chunk_indexes: list[int] | None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        row = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE id = ? AND lifecycle_status NOT IN ('missing', 'deleted')
            """,
            (document_id,),
        ).fetchone()
        if row is None:
            raise RetrieverError(f"Unknown active document id: {document_id}")

        chunk_rows = document_chunk_rows(connection, document_id)
        requested_chunk_indexes = sorted(dict.fromkeys(int(chunk_index) for chunk_index in (chunk_indexes or [])))
        if len(requested_chunk_indexes) > MAX_GET_DOC_CHUNKS:
            raise RetrieverError(f"Requested too many chunks; max is {MAX_GET_DOC_CHUNKS}.")
        chunk_rows_by_index = {int(chunk_row["chunk_index"]): chunk_row for chunk_row in chunk_rows}
        missing_chunk_indexes = [chunk_index for chunk_index in requested_chunk_indexes if chunk_index not in chunk_rows_by_index]
        if missing_chunk_indexes:
            raise RetrieverError(
                f"Unknown chunk indexes for document {document_id}: {', '.join(str(chunk_index) for chunk_index in missing_chunk_indexes)}"
            )

        exact_chunks: list[dict[str, object]] = []
        total_text_chars = 0
        preview_rel_path, preview_abs_path = default_preview_target(paths, row, connection)
        for chunk_index in requested_chunk_indexes:
            chunk_row = chunk_rows_by_index[chunk_index]
            text_content = str(chunk_row["text_content"] or "")
            total_text_chars += len(text_content)
            if total_text_chars > MAX_GET_DOC_TEXT_CHARS:
                raise RetrieverError(f"Requested chunk text exceeds the {MAX_GET_DOC_TEXT_CHARS}-character limit.")
            snippet = chunk_preview_text(text_content)
            exact_chunks.append(
                {
                    "chunk_index": int(chunk_row["chunk_index"]),
                    "char_start": int(chunk_row["char_start"]),
                    "char_end": int(chunk_row["char_end"]),
                    "token_estimate": chunk_row["token_estimate"],
                    "text": text_content,
                    "snippet": snippet,
                    "citation": build_chunk_citation_payload(
                        row,
                        preview_rel_path=preview_rel_path,
                        preview_abs_path=preview_abs_path,
                        chunk_index=int(chunk_row["chunk_index"]),
                        char_start=int(chunk_row["char_start"]),
                        char_end=int(chunk_row["char_end"]),
                        snippet=snippet,
                    ),
                }
            )

        text_summary = None
        normalized_include_text = include_text.strip().lower()
        if normalized_include_text == "summary":
            text_summary = reconstruct_document_text_prefix(chunk_rows, GET_DOC_SUMMARY_CHARS)
        elif normalized_include_text != "none":
            raise RetrieverError(f"Unsupported include-text mode: {include_text}")

        return {
            "status": "ok",
            "document": document_overview_payload(paths, connection, row),
            "chunk_count": len(chunk_rows),
            "include_text": normalized_include_text,
            "text_summary": text_summary,
            "chunks": exact_chunks,
        }
    finally:
        connection.close()


def list_chunks(root: Path, document_id: int, page: int, per_page: int) -> dict[str, object]:
    if page < 1:
        raise RetrieverError("Page must be >= 1.")
    if per_page < 1:
        raise RetrieverError("per-page must be >= 1.")
    per_page = min(per_page, MAX_CHUNK_PAGE_SIZE)

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        row = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE id = ? AND lifecycle_status NOT IN ('missing', 'deleted')
            """,
            (document_id,),
        ).fetchone()
        if row is None:
            raise RetrieverError(f"Unknown active document id: {document_id}")

        chunk_rows = document_chunk_rows(connection, document_id)
        total_chunks = len(chunk_rows)
        total_pages = max(1, (total_chunks + per_page - 1) // per_page)
        start = (page - 1) * per_page
        end = start + per_page
        paged_rows = chunk_rows[start:end]
        return {
            "status": "ok",
            "document": {
                "document_id": int(row["id"]),
                "control_number": row["control_number"],
                "file_name": row["file_name"],
            },
            "page": page,
            "per_page": per_page,
            "total_chunks": total_chunks,
            "total_pages": total_pages,
            "chunks": [
                {
                    "chunk_index": int(chunk_row["chunk_index"]),
                    "char_start": int(chunk_row["char_start"]),
                    "char_end": int(chunk_row["char_end"]),
                    "token_estimate": chunk_row["token_estimate"],
                    "snippet": chunk_preview_text(chunk_row["text_content"]),
                }
                for chunk_row in paged_rows
            ],
        }
    finally:
        connection.close()


def search_chunk_rows(
    connection: sqlite3.Connection,
    query: str,
    clauses: list[str],
    params: list[object],
) -> list[sqlite3.Row]:
    query_value = query.strip()
    if not query_value:
        raise RetrieverError("search-chunks requires a non-empty query.")
    where_clause = " AND ".join(clauses)
    sql = f"""
        SELECT
          d.*,
          dc.id AS chunk_row_id,
          dc.chunk_index,
          dc.char_start,
          dc.char_end,
          dc.token_estimate,
          dc.text_content,
          bm25(chunks_fts) AS rank
        FROM chunks_fts
        JOIN document_chunks dc ON dc.id = CAST(chunks_fts.chunk_id AS INTEGER)
        JOIN documents d ON d.id = dc.document_id
        WHERE chunks_fts MATCH ? AND {where_clause}
    """
    try:
        return connection.execute(sql, [query_value, *params]).fetchall()
    except sqlite3.OperationalError:
        return connection.execute(sql, [f'"{query_value}"', *params]).fetchall()


def sort_chunk_match_rows(rows: list[sqlite3.Row], sort_field: str | None, order: str | None) -> list[sqlite3.Row]:
    normalized_sort_field = (sort_field or "relevance").lower()
    normalized_order = (order or ("asc" if normalized_sort_field == "relevance" else "desc")).lower()
    if normalized_sort_field not in {"relevance", "date_created", "date_modified"}:
        raise RetrieverError(f"Unsupported chunk sort field: {sort_field}")
    stable_rows = sorted(rows, key=lambda row: (int(row["id"]), int(row["chunk_index"])))
    if normalized_sort_field == "relevance":
        return sorted(stable_rows, key=lambda row: float(row["rank"]), reverse=normalized_order == "desc")
    ranked_rows = sorted(stable_rows, key=lambda row: float(row["rank"]))
    return sorted(ranked_rows, key=lambda row: coerce_sort_value(row[normalized_sort_field]), reverse=normalized_order == "desc")


def count_distinct_chunk_documents(
    connection: sqlite3.Connection,
    query: str,
    clauses: list[str],
    params: list[object],
) -> int:
    query_value = query.strip()
    where_clause = " AND ".join(clauses)
    sql = f"""
        SELECT COUNT(DISTINCT d.id) AS count
        FROM chunks_fts
        JOIN document_chunks dc ON dc.id = CAST(chunks_fts.chunk_id AS INTEGER)
        JOIN documents d ON d.id = dc.document_id
        WHERE chunks_fts MATCH ? AND {where_clause}
    """
    try:
        row = connection.execute(sql, [query_value, *params]).fetchone()
    except sqlite3.OperationalError:
        row = connection.execute(sql, [f'"{query_value}"', *params]).fetchone()
    return int(row["count"] or 0)


def count_filtered_documents(
    connection: sqlite3.Connection,
    clauses: list[str],
    params: list[object],
) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM documents d
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchone()
    return int(row["count"] or 0)


def search_chunks(
    root: Path,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
    top_k: int,
    per_doc_cap: int,
    *,
    count_only: bool = False,
    distinct_docs: bool = False,
) -> dict[str, object]:
    if top_k < 1:
        raise RetrieverError("top-k must be >= 1.")
    if per_doc_cap < 1:
        raise RetrieverError("per-doc-cap must be >= 1.")
    top_k = min(top_k, MAX_CHUNK_SEARCH_TOP_K)
    per_doc_cap = min(per_doc_cap, MAX_CHUNK_SEARCH_PER_DOC_CAP)
    if distinct_docs and not count_only:
        raise RetrieverError("--distinct-docs requires --count-only.")
    if count_only and not distinct_docs:
        raise RetrieverError("--count-only currently requires --distinct-docs.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        filter_summary, clauses, params = build_search_filters(connection, raw_filters)
        if count_only:
            return {
                "query": query,
                "filters": filter_summary,
                "documents_with_hits": count_distinct_chunk_documents(connection, query, clauses, params),
                "total_docs_filtered": count_filtered_documents(connection, clauses, params),
                "count_mode": "distinct-documents",
            }

        raw_rows = search_chunk_rows(connection, query, clauses, params)
        grouped_rows: dict[int, list[sqlite3.Row]] = defaultdict(list)
        for row in sorted(raw_rows, key=lambda item: (float(item["rank"]), int(item["chunk_index"]))):
            grouped_rows[int(row["id"])].append(row)
        selected_rows: list[sqlite3.Row] = []
        for rows in grouped_rows.values():
            selected_rows.extend(rows[:per_doc_cap])

        sorted_rows = sort_chunk_match_rows(selected_rows, sort_field, order)
        returned_rows: list[sqlite3.Row] = []
        total_text_chars = 0
        for row in sorted_rows:
            text_content = str(row["text_content"] or "")
            if len(returned_rows) >= top_k:
                break
            if returned_rows and total_text_chars + len(text_content) > MAX_CHUNK_SEARCH_TEXT_CHARS:
                break
            total_text_chars += len(text_content)
            returned_rows.append(row)

        results: list[dict[str, object]] = []
        dataset_memberships = fetch_document_dataset_memberships(connection, returned_rows)
        production_names = fetch_production_names(connection, returned_rows)
        parent_summaries = fetch_parent_summaries(
            connection,
            [row for row in returned_rows if row["parent_document_id"] is not None],
        )
        for row in returned_rows:
            preview_rel_path, preview_abs_path = default_preview_target(paths, row, connection)
            snippet = make_snippet(str(row["text_content"] or ""), query)
            result = {
                **document_path_payload(paths, connection, row),
                "document_id": int(row["id"]),
                "control_number": row["control_number"],
                "file_name": row["file_name"],
                "file_type": row["file_type"],
                "chunk_index": int(row["chunk_index"]),
                "char_start": int(row["char_start"]),
                "char_end": int(row["char_end"]),
                "token_estimate": row["token_estimate"],
                "text": str(row["text_content"] or ""),
                "snippet": snippet,
                "rank": float(row["rank"]),
                "metadata": {
                    "author": row["author"],
                    "content_type": row["content_type"],
                    "custodian": row["custodian"],
                    "date_created": row["date_created"],
                    "date_modified": row["date_modified"],
                    "participants": row["participants"],
                    "recipients": row["recipients"],
                    "subject": row["subject"],
                    "title": row["title"],
                    "updated_at": row["updated_at"],
                },
                "citation": build_chunk_citation_payload(
                    row,
                    preview_rel_path=preview_rel_path,
                    preview_abs_path=preview_abs_path,
                    chunk_index=int(row["chunk_index"]),
                    char_start=int(row["char_start"]),
                    char_end=int(row["char_end"]),
                    snippet=snippet,
                ),
            }
            membership = dataset_memberships.get(int(row["id"]), {"ids": [], "names": []})
            dataset_ids = [int(dataset_id) for dataset_id in membership["ids"]]
            dataset_names = [str(dataset_name) for dataset_name in membership["names"]]
            result["dataset_ids"] = dataset_ids
            result["dataset_names"] = dataset_names
            if len(dataset_ids) == 1:
                result["dataset_id"] = dataset_ids[0]
            if len(dataset_names) == 1:
                result["dataset_name"] = dataset_names[0]
            if row["production_id"] is not None:
                result["production_name"] = production_names.get(int(row["production_id"]))
            if row["parent_document_id"] is not None:
                result["parent"] = parent_summaries.get(int(row["parent_document_id"]))
            results.append(result)

        normalized_sort = (sort_field or "relevance").lower()
        normalized_order = (order or ("asc" if normalized_sort == "relevance" else "desc")).lower()
        return {
            "query": query,
            "filters": filter_summary,
            "sort": normalized_sort,
            "order": normalized_order,
            "top_k": top_k,
            "per_doc_cap": per_doc_cap,
            "total_matches": len(raw_rows),
            "results": results,
        }
    finally:
        connection.close()


def aggregate_output_name(base_name: str, used_names: set[str]) -> str:
    candidate = base_name
    if candidate not in used_names:
        used_names.add(candidate)
        return candidate
    suffix = 2
    while f"{base_name}_{suffix}" in used_names:
        suffix += 1
    candidate = f"{base_name}_{suffix}"
    used_names.add(candidate)
    return candidate


def aggregate_date_base_expr(field_name: str) -> str:
    return f"substr(COALESCE(d.{quote_identifier(field_name)}, ''), 1, 10)"


def resolve_aggregate_group(
    connection: sqlite3.Connection,
    raw_group: str,
    used_names: set[str],
) -> dict[str, object]:
    token = normalize_whitespace(raw_group)
    if not token:
        raise RetrieverError("Empty group-by expression.")
    temporal_match = re.fullmatch(r"(year|quarter|month|week):([A-Za-z0-9_]+)", token)
    if temporal_match:
        granularity = temporal_match.group(1)
        field_name = temporal_match.group(2)
        field_def = resolve_field_definition(connection, field_name)
        if field_def["field_type"] != "date":
            raise RetrieverError(f"Date bucket '{token}' requires a date-typed field.")
        if field_def.get("source") == "virtual":
            raise RetrieverError(f"Date bucket '{token}' cannot target a virtual field.")
        date_expr = aggregate_date_base_expr(field_def["field_name"])
        if granularity == "year":
            select_sql = f"NULLIF(substr({date_expr}, 1, 4), '')"
        elif granularity == "month":
            select_sql = f"NULLIF(substr({date_expr}, 1, 7), '')"
        elif granularity == "quarter":
            select_sql = (
                f"CASE WHEN length({date_expr}) >= 7 THEN "
                f"substr({date_expr}, 1, 4) || '-Q' || "
                f"CASE "
                f"WHEN CAST(substr({date_expr}, 6, 2) AS INTEGER) BETWEEN 1 AND 3 THEN '1' "
                f"WHEN CAST(substr({date_expr}, 6, 2) AS INTEGER) BETWEEN 4 AND 6 THEN '2' "
                f"WHEN CAST(substr({date_expr}, 6, 2) AS INTEGER) BETWEEN 7 AND 9 THEN '3' "
                f"WHEN CAST(substr({date_expr}, 6, 2) AS INTEGER) BETWEEN 10 AND 12 THEN '4' "
                f"ELSE NULL END "
                f"ELSE NULL END"
            )
        else:
            select_sql = f"CASE WHEN length({date_expr}) = 10 THEN strftime('%Y-W%W', {date_expr}) ELSE NULL END"
        output_name = aggregate_output_name(granularity, used_names)
        return {
            "token": token,
            "output_name": output_name,
            "select_sql": select_sql,
            "group_sql": select_sql,
            "join_dataset": False,
            "is_temporal": True,
        }

    field_def = resolve_field_definition(connection, token)
    if field_def.get("source") == "virtual":
        if field_def["field_name"] not in AGGREGATABLE_VIRTUAL_FIELDS:
            raise RetrieverError(f"Field '{token}' is not aggregatable.")
        if field_def["field_name"] == "dataset_name":
            output_name = aggregate_output_name("dataset_name", used_names)
            return {
                "token": token,
                "output_name": output_name,
                "select_sql": "ds.dataset_name",
                "group_sql": "ds.dataset_name",
                "join_dataset": True,
                "is_temporal": False,
            }
        raise RetrieverError(f"Unsupported aggregatable virtual field: {token}")

    column_expr = f"d.{quote_identifier(field_def['field_name'])}"
    output_name = aggregate_output_name(field_def["field_name"], used_names)
    return {
        "token": token,
        "output_name": output_name,
        "select_sql": column_expr,
        "group_sql": column_expr,
        "join_dataset": False,
        "is_temporal": False,
    }


def graph_metadata_for_buckets(group_defs: list[dict[str, object]], buckets: list[dict[str, object]]) -> dict[str, object]:
    graph_type = "bar"
    if any(bool(group_def["is_temporal"]) for group_def in group_defs):
        graph_type = "line"
    elif len(group_defs) == 1 and len(buckets) <= 6:
        graph_type = "pie"
    description = "Count by " + ", ".join(str(group_def["output_name"]).replace("_", " ") for group_def in group_defs)
    graph: dict[str, object] = {
        "type": graph_type,
        "x_axis": group_defs[0]["output_name"] if group_defs else None,
        "y_axis": "count",
        "description": description,
    }
    if len(group_defs) > 1:
        graph["series"] = group_defs[1]["output_name"]
    return graph


def aggregate(
    root: Path,
    raw_filters: list[list[str]] | None,
    raw_group_bys: list[str],
    metric: str,
    order_by: str | None,
    order: str | None,
    limit: int,
    explain: bool,
) -> dict[str, object]:
    if not raw_group_bys:
        raise RetrieverError("aggregate requires at least one --group-by.")
    if metric.strip().lower() != "count":
        raise RetrieverError("aggregate currently supports only metric=count.")
    if limit < 1:
        raise RetrieverError("limit must be >= 1.")
    limit = min(limit, MAX_AGGREGATE_LIMIT)

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        filter_summary, clauses, params = build_search_filters(connection, raw_filters)
        used_names: set[str] = set()
        group_defs = [resolve_aggregate_group(connection, raw_group, used_names) for raw_group in raw_group_bys]
        joins: list[str] = []
        if any(bool(group_def["join_dataset"]) for group_def in group_defs):
            joins.extend(
                [
                    "JOIN dataset_documents dd ON dd.document_id = d.id",
                    "JOIN datasets ds ON ds.id = dd.dataset_id",
                ]
            )
        select_parts = [f"{group_def['select_sql']} AS {quote_identifier(str(group_def['output_name']))}" for group_def in group_defs]
        select_parts.append(("COUNT(DISTINCT d.id)" if joins else "COUNT(*)") + " AS count")
        group_parts = [str(group_def["group_sql"]) for group_def in group_defs]
        sql = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM documents d {' '.join(joins)} "
            f"WHERE {' AND '.join(clauses)} "
            f"GROUP BY {', '.join(group_parts)}"
        )

        order_name_map = {
            "metric": "count",
            **{str(group_def["token"]): str(group_def["output_name"]) for group_def in group_defs},
            **{str(group_def["output_name"]): str(group_def["output_name"]) for group_def in group_defs},
        }
        resolved_order_by = order_name_map.get(order_by or "", None) if order_by else "count"
        if resolved_order_by is None:
            raise RetrieverError(f"Unsupported aggregate order-by: {order_by}")
        normalized_order = (order or ("desc" if resolved_order_by == "count" else "asc")).lower()
        sql += f" ORDER BY {quote_identifier(resolved_order_by)} {normalized_order.upper()}"
        if group_defs:
            for group_def in group_defs:
                if str(group_def["output_name"]) == resolved_order_by:
                    continue
                sql += f", {quote_identifier(str(group_def['output_name']))} ASC"
        sql += " LIMIT ?"

        rows = connection.execute(sql, [*params, limit]).fetchall()
        buckets = []
        for row in rows:
            bucket = {str(group_def["output_name"]): row[str(group_def["output_name"])] for group_def in group_defs}
            bucket["count"] = int(row["count"] or 0)
            buckets.append(bucket)
        payload = {
            "filters": filter_summary,
            "metric": "count",
            "group_by": [str(group_def["token"]) for group_def in group_defs],
            "buckets": buckets,
            "graph": graph_metadata_for_buckets(group_defs, buckets),
        }
        if explain:
            payload["sql"] = sql.replace(" ?", f" {limit}")
        return payload
    finally:
        connection.close()


def add_dataset_selector_arguments(parser: argparse.ArgumentParser) -> None:
    selector_group = parser.add_mutually_exclusive_group(required=True)
    selector_group.add_argument("--dataset-id", type=int, help="Dataset id")
    selector_group.add_argument("--dataset-name", help="Exact dataset name")


def add_search_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("workspace", help="Workspace root path")
    parser.add_argument("query", nargs="?", default="", help="Keyword query text")
    parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable filter in the form <field> <op> <value>",
    )
    parser.add_argument("--sort", "--sort-by", dest="sort", help="Sort field or 'relevance'")
    parser.add_argument("--order", "--sort-order", dest="order", choices=("asc", "desc"), help="Sort order")
    parser.add_argument("--page", type=int, default=1, help="1-based result page")
    parser.add_argument(
        "--per-page",
        "--limit",
        dest="per_page",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Results per page",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Return the full payload instead of the default compact JSON",
    )


def add_run_selector_arguments(parser: argparse.ArgumentParser, *, prefix: str = "") -> None:
    option_prefix = f"{prefix}-" if prefix else ""
    dest_prefix = f"{prefix.replace('-', '_')}_" if prefix else ""
    parser.add_argument(f"--{option_prefix}dataset-id", dest=f"{dest_prefix}dataset_ids", action="append", type=int, help="Dataset id (repeatable)")
    parser.add_argument(f"--{option_prefix}dataset-name", dest=f"{dest_prefix}dataset_names", action="append", help="Exact dataset name (repeatable)")
    parser.add_argument(f"--{option_prefix}doc-id", dest=f"{dest_prefix}document_ids", action="append", type=int, help="Document id (repeatable)")
    parser.add_argument(
        f"--{option_prefix}control-number",
        dest=f"{dest_prefix}control_numbers",
        action="append",
        help="Control number (repeatable)",
    )
    parser.add_argument(f"--{option_prefix}query", dest=f"{dest_prefix}query", help="Keyword query text")
    parser.add_argument(
        f"--{option_prefix}filter",
        dest=f"{dest_prefix}filters",
        action="append",
        nargs="+",
        help="Repeatable filter in the form <field> <op> <value>",
    )
    if not prefix:
        parser.add_argument("--from-run-id", type=int, help="Reuse the full frozen snapshot from a prior run")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Retriever workspace tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor_parser = subparsers.add_parser("doctor", help="Check runtime and workspace readiness")
    doctor_parser.add_argument("workspace", help="Workspace root path")
    doctor_parser.add_argument("--quick", action="store_true", help="Return the compact runtime payload")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Create workspace layout and schema")
    bootstrap_parser.add_argument("workspace", help="Workspace root path")

    ingest_parser = subparsers.add_parser("ingest", help="Index documents in the workspace")
    ingest_parser.add_argument("workspace", help="Workspace root path")
    ingest_parser.add_argument("--recursive", action="store_true", help="Scan directories recursively")
    ingest_parser.add_argument(
        "--file-types",
        help="Comma-separated file types to include, e.g. pdf,docx,eml",
    )

    ingest_production_parser = subparsers.add_parser("ingest-production", help="Ingest a processed production volume")
    ingest_production_parser.add_argument("workspace", help="Workspace root path")
    ingest_production_parser.add_argument("production_root", help="Production root directory inside the workspace")

    search_parser = subparsers.add_parser("search", help="Search indexed documents")
    add_search_arguments(search_parser)

    search_docs_parser = subparsers.add_parser("search-docs", help="Search indexed documents at the document level")
    add_search_arguments(search_docs_parser)

    catalog_parser = subparsers.add_parser("catalog", help="Describe searchable, filterable, and aggregatable fields")
    catalog_parser.add_argument("workspace", help="Workspace root path")

    export_parser = subparsers.add_parser("export-csv", help="Write selected documents and fields to a CSV on disk")
    export_parser.add_argument("workspace", help="Workspace root path")
    export_parser.add_argument("output_path", help="CSV file path; relative paths resolve under .retriever/exports")
    export_parser.add_argument("query", nargs="?", default="", help="Optional keyword query text for search-based export")
    export_parser.add_argument(
        "--field",
        dest="fields",
        action="append",
        required=True,
        help="Field to export (repeatable, preserves order)",
    )
    export_parser.add_argument(
        "--doc-id",
        dest="document_ids",
        action="append",
        type=int,
        help="Document id to export (repeatable, preserves input order)",
    )
    export_parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable filter in the form <field> <op> <value>",
    )
    export_parser.add_argument("--sort", "--sort-by", dest="sort", help="Sort field for search-based export or 'relevance'")
    export_parser.add_argument("--order", "--sort-order", dest="order", choices=("asc", "desc"), help="Sort order")

    export_archive_parser = subparsers.add_parser(
        "export-archive",
        help="Write selected documents, previews, and source artifacts to a zip archive",
    )
    export_archive_parser.add_argument("workspace", help="Workspace root path")
    export_archive_parser.add_argument(
        "output_path",
        help="Zip file path; relative paths resolve under .retriever/exports",
    )
    add_run_selector_arguments(export_archive_parser)
    add_run_selector_arguments(export_archive_parser, prefix="exclude")
    export_archive_parser.add_argument(
        "--family-mode",
        default="exact",
        choices=sorted(RUN_FAMILY_MODES),
        help="Whether to include only seed docs or their family members too",
    )
    export_archive_parser.add_argument("--limit", dest="seed_limit", type=int, help="Limit the directly matched seed set")
    export_archive_parser.add_argument(
        "--portable-workspace",
        action="store_true",
        help="Include a curated subset .retriever/retriever.db for the exported documents",
    )

    get_doc_parser = subparsers.add_parser("get-doc", help="Fetch one document with optional summary text or exact chunks")
    get_doc_parser.add_argument("workspace", help="Workspace root path")
    get_doc_parser.add_argument("--doc-id", dest="document_id", type=int, required=True, help="Document id")
    get_doc_parser.add_argument(
        "--include-text",
        choices=("none", "summary"),
        default="none",
        help="Include no extracted text or a deterministic summary prefix",
    )
    get_doc_parser.add_argument(
        "--chunk",
        dest="chunk_indexes",
        action="append",
        type=int,
        help="Exact chunk index to include (repeatable)",
    )
    get_doc_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Return the full document context instead of the default compact JSON",
    )

    list_chunks_parser = subparsers.add_parser("list-chunks", help="List chunk metadata for one document")
    list_chunks_parser.add_argument("workspace", help="Workspace root path")
    list_chunks_parser.add_argument("--doc-id", dest="document_id", type=int, required=True, help="Document id")
    list_chunks_parser.add_argument("--page", type=int, default=1, help="1-based chunk page")
    list_chunks_parser.add_argument(
        "--per-page",
        "--limit",
        dest="per_page",
        type=int,
        default=DEFAULT_CHUNK_PAGE_SIZE,
        help="Chunks per page",
    )

    search_chunks_parser = subparsers.add_parser("search-chunks", help="Search matching text chunks with citations")
    search_chunks_parser.add_argument("workspace", help="Workspace root path")
    search_chunks_parser.add_argument("query", help="Keyword query text")
    search_chunks_parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable filter in the form <field> <op> <value>",
    )
    search_chunks_parser.add_argument(
        "--sort",
        "--sort-by",
        dest="sort",
        choices=("relevance", "date_created", "date_modified"),
        help="Chunk result sort field",
    )
    search_chunks_parser.add_argument("--order", "--sort-order", dest="order", choices=("asc", "desc"), help="Sort order")
    search_chunks_parser.add_argument("--top-k", type=int, default=DEFAULT_CHUNK_SEARCH_TOP_K, help="Maximum chunks to return")
    search_chunks_parser.add_argument(
        "--per-doc-cap",
        type=int,
        default=DEFAULT_CHUNK_SEARCH_PER_DOC_CAP,
        help="Maximum chunks to keep from any one document",
    )
    search_chunks_parser.add_argument("--count-only", action="store_true", help="Return counts instead of chunk payloads")
    search_chunks_parser.add_argument(
        "--distinct-docs",
        action="store_true",
        help="Count distinct documents with matching chunks when used with --count-only",
    )
    search_chunks_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Return raw chunk text and the full document context instead of the default compact JSON",
    )

    aggregate_parser = subparsers.add_parser("aggregate", help="Run bounded metadata aggregations across documents")
    aggregate_parser.add_argument("workspace", help="Workspace root path")
    aggregate_parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable filter in the form <field> <op> <value>",
    )
    aggregate_parser.add_argument(
        "--group-by",
        dest="group_bys",
        action="append",
        required=True,
        help="Grouping expression, e.g. dataset_name or month:effective_date",
    )
    aggregate_parser.add_argument("--metric", default="count", help="Aggregation metric")
    aggregate_parser.add_argument("--order-by", help="Bucket field name or 'metric'")
    aggregate_parser.add_argument("--order", choices=("asc", "desc"), help="Sort order")
    aggregate_parser.add_argument("--limit", type=int, default=DEFAULT_AGGREGATE_LIMIT, help="Maximum buckets to return")
    aggregate_parser.add_argument("--explain", action="store_true", help="Include generated SQL in the response")

    list_datasets_parser = subparsers.add_parser("list-datasets", help="List datasets in the workspace")
    list_datasets_parser.add_argument("workspace", help="Workspace root path")

    create_dataset_parser = subparsers.add_parser("create-dataset", help="Create a manual dataset")
    create_dataset_parser.add_argument("workspace", help="Workspace root path")
    create_dataset_parser.add_argument("dataset_name", help="Dataset name")

    add_to_dataset_parser = subparsers.add_parser("add-to-dataset", help="Add documents to a dataset")
    add_to_dataset_parser.add_argument("workspace", help="Workspace root path")
    add_to_dataset_parser.add_argument("--doc-id", dest="document_ids", action="append", type=int, required=True, help="Document id to add (repeatable)")
    add_dataset_selector_arguments(add_to_dataset_parser)

    remove_from_dataset_parser = subparsers.add_parser("remove-from-dataset", help="Remove documents from a dataset")
    remove_from_dataset_parser.add_argument("workspace", help="Workspace root path")
    remove_from_dataset_parser.add_argument("--doc-id", dest="document_ids", action="append", type=int, required=True, help="Document id to remove (repeatable)")
    add_dataset_selector_arguments(remove_from_dataset_parser)

    delete_dataset_parser = subparsers.add_parser("delete-dataset", help="Delete a dataset")
    delete_dataset_parser.add_argument("workspace", help="Workspace root path")
    add_dataset_selector_arguments(delete_dataset_parser)

    list_runs_parser = subparsers.add_parser("list-runs", help="List planned processing runs")
    list_runs_parser.add_argument("workspace", help="Workspace root path")

    get_run_parser = subparsers.add_parser("get-run", help="Fetch one planned processing run")
    get_run_parser.add_argument("workspace", help="Workspace root path")
    get_run_parser.add_argument("--run-id", type=int, required=True, help="Run id")

    create_run_parser = subparsers.add_parser("create-run", help="Create a frozen processing run snapshot")
    create_run_parser.add_argument("workspace", help="Workspace root path")
    job_version_group = create_run_parser.add_mutually_exclusive_group(required=True)
    job_version_group.add_argument("--job-version-id", type=int, help="Explicit job version id")
    job_version_group.add_argument("--job-name", help="Job name")
    create_run_parser.add_argument("--job-version", dest="job_version_number", type=int, help="Job version number when selecting by job name")
    add_run_selector_arguments(create_run_parser)
    add_run_selector_arguments(create_run_parser, prefix="exclude")
    create_run_parser.add_argument(
        "--family-mode",
        default="exact",
        choices=sorted(RUN_FAMILY_MODES),
        help="Whether to include only seed docs or their family members too",
    )
    create_run_parser.add_argument("--limit", dest="seed_limit", type=int, help="Limit the directly matched seed set")

    list_text_revisions_parser = subparsers.add_parser("list-text-revisions", help="List stored text revisions for a document")
    list_text_revisions_parser.add_argument("workspace", help="Workspace root path")
    list_text_revisions_parser.add_argument("--doc-id", dest="document_id", type=int, required=True, help="Document id")

    activate_text_revision_parser = subparsers.add_parser("activate-text-revision", help="Promote a stored text revision to active indexed text")
    activate_text_revision_parser.add_argument("workspace", help="Workspace root path")
    activate_text_revision_parser.add_argument("--doc-id", dest="document_id", type=int, required=True, help="Document id")
    activate_text_revision_parser.add_argument("--text-revision-id", type=int, required=True, help="Stored text revision id")
    activate_text_revision_parser.add_argument(
        "--activation-policy",
        default="manual",
        choices=sorted(TEXT_REVISION_ACTIVATION_POLICIES),
        help="Audit label for why this revision is being promoted",
    )

    list_results_parser = subparsers.add_parser("list-results", help="List stored processing results")
    list_results_parser.add_argument("workspace", help="Workspace root path")
    list_results_parser.add_argument("--run-id", type=int, help="Filter results to one run")
    list_results_parser.add_argument("--doc-id", dest="document_id", type=int, help="Filter results to one document")

    execute_run_parser = subparsers.add_parser(
        "execute-run",
        help="Execute one planned processing run via the legacy direct executor",
    )
    execute_run_parser.add_argument("workspace", help="Workspace root path")
    execute_run_parser.add_argument("--run-id", type=int, required=True, help="Run id")

    claim_run_items_parser = subparsers.add_parser("claim-run-items", help="Atomically claim pending run items for one worker")
    claim_run_items_parser.add_argument("workspace", help="Workspace root path")
    claim_run_items_parser.add_argument("--run-id", type=int, required=True, help="Run id")
    claim_run_items_parser.add_argument("--claimed-by", required=True, help="Worker/session identifier claiming the items")
    claim_run_items_parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_RUN_ITEM_CLAIM_BATCH_SIZE,
        help="Maximum number of run items to claim",
    )
    claim_run_items_parser.add_argument(
        "--stale-seconds",
        type=int,
        default=DEFAULT_RUN_ITEM_CLAIM_STALE_SECONDS,
        help="Reclaim running items whose heartbeat is older than this many seconds",
    )
    claim_run_items_parser.add_argument(
        "--launch-mode",
        default="inline",
        choices=sorted(RUN_WORKER_MODES),
        help="Worker launch mode for supervision metadata",
    )
    claim_run_items_parser.add_argument("--worker-task-id", help="Optional background task identifier")
    claim_run_items_parser.add_argument(
        "--max-batches",
        type=int,
        help="Optional maximum number of batches this worker should prepare before handing off",
    )

    prepare_run_batch_parser = subparsers.add_parser(
        "prepare-run-batch",
        help="Claim one worker batch and return execution contexts plus worker hints",
    )
    prepare_run_batch_parser.add_argument("workspace", help="Workspace root path")
    prepare_run_batch_parser.add_argument("--run-id", type=int, required=True, help="Run id")
    prepare_run_batch_parser.add_argument("--claimed-by", required=True, help="Worker/session identifier claiming the batch")
    prepare_run_batch_parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of run items to claim; defaults to the worker recommendation",
    )
    prepare_run_batch_parser.add_argument(
        "--stale-seconds",
        type=int,
        default=DEFAULT_RUN_ITEM_CLAIM_STALE_SECONDS,
        help="Reclaim running items whose heartbeat is older than this many seconds",
    )
    prepare_run_batch_parser.add_argument(
        "--launch-mode",
        default="inline",
        choices=sorted(RUN_WORKER_MODES),
        help="Worker launch mode for supervision metadata",
    )
    prepare_run_batch_parser.add_argument("--worker-task-id", help="Optional background task identifier")
    prepare_run_batch_parser.add_argument(
        "--max-batches",
        type=int,
        help="Optional maximum number of batches this worker should prepare before handing off",
    )

    get_run_item_context_parser = subparsers.add_parser("get-run-item-context", help="Load the execution context for one run item")
    get_run_item_context_parser.add_argument("workspace", help="Workspace root path")
    get_run_item_context_parser.add_argument("--run-item-id", type=int, required=True, help="Run item id")

    heartbeat_run_items_parser = subparsers.add_parser("heartbeat-run-items", help="Refresh heartbeat timestamps for one worker's claimed items")
    heartbeat_run_items_parser.add_argument("workspace", help="Workspace root path")
    heartbeat_run_items_parser.add_argument("--run-id", type=int, required=True, help="Run id")
    heartbeat_run_items_parser.add_argument("--claimed-by", required=True, help="Worker/session identifier")

    finish_run_worker_parser = subparsers.add_parser(
        "finish-run-worker",
        help="Mark one worker as finished and persist its summary",
    )
    finish_run_worker_parser.add_argument("workspace", help="Workspace root path")
    finish_run_worker_parser.add_argument("--run-id", type=int, required=True, help="Run id")
    finish_run_worker_parser.add_argument("--claimed-by", required=True, help="Worker/session identifier")
    finish_run_worker_parser.add_argument(
        "--worker-status",
        required=True,
        choices=sorted(status for status in RUN_WORKER_STATUSES if status != "active"),
        help="Terminal worker status",
    )
    finish_run_worker_parser.add_argument("--summary-json", help="Optional worker summary JSON object")
    finish_run_worker_parser.add_argument("--error", dest="error_summary", help="Optional terminal error summary")

    complete_run_item_parser = subparsers.add_parser("complete-run-item", help="Mark one claimed run item completed and persist its result")
    complete_run_item_parser.add_argument("workspace", help="Workspace root path")
    complete_run_item_parser.add_argument("--run-item-id", type=int, required=True, help="Run item id")
    complete_run_item_parser.add_argument("--claimed-by", required=True, help="Worker/session identifier")
    complete_run_item_parser.add_argument(
        "--page-text",
        help="Plain-text completion for page-scoped visual run items (OCR or image description)",
    )
    complete_run_item_parser.add_argument("--raw-output-json", help="Optional raw output JSON")
    complete_run_item_parser.add_argument("--normalized-output-json", help="Optional normalized output JSON")
    complete_run_item_parser.add_argument("--output-values-json", help="Optional per-output values JSON object")
    complete_run_item_parser.add_argument("--created-text-revision-json", help="Optional derived text revision payload JSON object")
    complete_run_item_parser.add_argument("--provider-metadata-json", help="Optional provider metadata JSON object")
    complete_run_item_parser.add_argument("--provider-request-id", help="Optional provider request identifier")
    complete_run_item_parser.add_argument("--input-tokens", type=int, help="Optional input token count")
    complete_run_item_parser.add_argument("--output-tokens", type=int, help="Optional output token count")
    complete_run_item_parser.add_argument("--cost-cents", type=int, help="Optional provider cost in cents")
    complete_run_item_parser.add_argument("--latency-ms", type=int, help="Optional execution latency in milliseconds")

    fail_run_item_parser = subparsers.add_parser("fail-run-item", help="Mark one claimed run item failed")
    fail_run_item_parser.add_argument("workspace", help="Workspace root path")
    fail_run_item_parser.add_argument("--run-item-id", type=int, required=True, help="Run item id")
    fail_run_item_parser.add_argument("--claimed-by", required=True, help="Worker/session identifier")
    fail_run_item_parser.add_argument("--error", required=True, help="Failure summary")
    fail_run_item_parser.add_argument("--provider-metadata-json", help="Optional provider metadata JSON object")
    fail_run_item_parser.add_argument("--provider-request-id", help="Optional provider request identifier")
    fail_run_item_parser.add_argument("--input-tokens", type=int, help="Optional input token count")
    fail_run_item_parser.add_argument("--output-tokens", type=int, help="Optional output token count")
    fail_run_item_parser.add_argument("--cost-cents", type=int, help="Optional provider cost in cents")
    fail_run_item_parser.add_argument("--latency-ms", type=int, help="Optional execution latency in milliseconds")

    run_status_parser = subparsers.add_parser("run-status", help="Summarize run progress, claims, and recent failures")
    run_status_parser.add_argument("workspace", help="Workspace root path")
    run_status_parser.add_argument("--run-id", type=int, required=True, help="Run id")

    cancel_run_parser = subparsers.add_parser("cancel-run", help="Stop claiming new work for a run and skip its pending items")
    cancel_run_parser.add_argument("workspace", help="Workspace root path")
    cancel_run_parser.add_argument("--run-id", type=int, required=True, help="Run id")
    cancel_run_parser.add_argument(
        "--force",
        action="store_true",
        help="Request force-stop for background workers that expose task ids",
    )

    finalize_ocr_run_parser = subparsers.add_parser("finalize-ocr-run", help="Merge completed OCR page items into document-level OCR results")
    finalize_ocr_run_parser.add_argument("workspace", help="Workspace root path")
    finalize_ocr_run_parser.add_argument("--run-id", type=int, required=True, help="Run id")

    finalize_image_description_run_parser = subparsers.add_parser(
        "finalize-image-description-run",
        help="Merge completed image-description page items into document-level text revisions",
    )
    finalize_image_description_run_parser.add_argument("workspace", help="Workspace root path")
    finalize_image_description_run_parser.add_argument("--run-id", type=int, required=True, help="Run id")

    publish_run_results_parser = subparsers.add_parser(
        "publish-run-results",
        help="Publish bound result outputs from a run into custom fields",
    )
    publish_run_results_parser.add_argument("workspace", help="Workspace root path")
    publish_run_results_parser.add_argument("--run-id", type=int, required=True, help="Run id")
    publish_run_results_parser.add_argument(
        "--output-name",
        dest="output_names",
        action="append",
        help="Optional job output name to publish (repeatable)",
    )

    list_jobs_parser = subparsers.add_parser("list-jobs", help="List configured processing jobs")
    list_jobs_parser.add_argument("workspace", help="Workspace root path")

    create_job_parser = subparsers.add_parser("create-job", help="Create a processing job")
    create_job_parser.add_argument("workspace", help="Workspace root path")
    create_job_parser.add_argument("job_name", help="Job name")
    create_job_parser.add_argument("job_kind", choices=sorted(JOB_KINDS), help="Job kind")
    create_job_parser.add_argument("--description", help="Optional job description")

    add_job_output_parser = subparsers.add_parser("add-job-output", help="Create or update a job output")
    add_job_output_parser.add_argument("workspace", help="Workspace root path")
    add_job_output_parser.add_argument("job_name", help="Existing job name")
    add_job_output_parser.add_argument("output_name", help="Job output name")
    add_job_output_parser.add_argument(
        "--value-type",
        default="text",
        choices=sorted(JOB_OUTPUT_VALUE_TYPES),
        help="Logical output value type",
    )
    add_job_output_parser.add_argument("--bind-custom-field", dest="bound_custom_field", help="Optional custom field binding")
    add_job_output_parser.add_argument("--description", help="Optional output description")

    list_job_versions_parser = subparsers.add_parser("list-job-versions", help="List versions for one job")
    list_job_versions_parser.add_argument("workspace", help="Workspace root path")
    list_job_versions_parser.add_argument("job_name", help="Existing job name")

    create_job_version_parser = subparsers.add_parser("create-job-version", help="Create a new immutable job version")
    create_job_version_parser.add_argument("workspace", help="Workspace root path")
    create_job_version_parser.add_argument("job_name", help="Existing job name")
    create_job_version_parser.add_argument("--instruction", help="Optional job instruction text")
    create_job_version_parser.add_argument(
        "--capability",
        choices=sorted(JOB_CAPABILITIES),
        help="Optional Cowork execution capability; defaults from job kind when omitted",
    )
    create_job_version_parser.add_argument(
        "--provider",
        default="cowork_agent",
        help="Optional provider identifier (defaults to cowork_agent; external providers are future-facing)",
    )
    create_job_version_parser.add_argument("--model", help="Optional model name")
    create_job_version_parser.add_argument(
        "--input-basis",
        choices=sorted(JOB_INPUT_BASES),
        help="Primary input basis for this version; defaults from job kind when omitted",
    )
    create_job_version_parser.add_argument("--response-schema-json", help="Optional JSON schema payload")
    create_job_version_parser.add_argument("--parameters-json", help="Optional provider parameters as JSON object")
    create_job_version_parser.add_argument("--segment-profile", help="Optional segment profile name")
    create_job_version_parser.add_argument("--aggregation-strategy", help="Optional aggregation strategy")
    create_job_version_parser.add_argument("--display-name", help="Optional display name override")

    add_field_parser = subparsers.add_parser("add-field", help="Add a custom document field")
    add_field_parser.add_argument("workspace", help="Workspace root path")
    add_field_parser.add_argument("field_name", help="Field name")
    add_field_parser.add_argument("field_type", choices=sorted(REGISTRY_FIELD_TYPES), help="Field type")
    add_field_parser.add_argument("--instruction", help="Field extraction instruction")

    promote_field_parser = subparsers.add_parser("promote-field-type", help="Promote a custom field type in place")
    promote_field_parser.add_argument("workspace", help="Workspace root path")
    promote_field_parser.add_argument("field_name", help="Existing custom field name")
    promote_field_parser.add_argument("target_field_type", choices=("date",), help="Target field type")

    set_field_parser = subparsers.add_parser("set-field", help="Set a field value on one document")
    set_field_parser.add_argument("workspace", help="Workspace root path")
    set_field_parser.add_argument("--doc-id", type=int, required=True, help="Document id")
    set_field_parser.add_argument("--field", required=True, help="Field name")
    set_field_parser.add_argument("--value", help="Field value")

    subparsers.add_parser("schema-version", help="Print the schema version")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "schema-version":
            print(json.dumps({"schema_version": SCHEMA_VERSION, "tool_version": TOOL_VERSION}))
            return 0

        root = Path(args.workspace).expanduser().resolve()

        if args.command == "doctor":
            return emit_cli_payload("doctor", doctor(root, args.quick))

        if args.command == "bootstrap":
            return emit_cli_payload("bootstrap", bootstrap(root))

        if args.command == "ingest":
            return emit_cli_payload("ingest", ingest(root, args.recursive, args.file_types))

        if args.command == "ingest-production":
            return emit_cli_payload("ingest-production", ingest_production(root, args.production_root))

        if args.command == "search":
            return emit_cli_payload(
                "search",
                search(root, args.query, args.filters, args.sort, args.order, args.page, args.per_page),
                verbose=args.verbose,
            )

        if args.command == "search-docs":
            return emit_cli_payload(
                "search-docs",
                search_docs(root, args.query, args.filters, args.sort, args.order, args.page, args.per_page),
                verbose=args.verbose,
            )

        if args.command == "catalog":
            return emit_cli_payload("catalog", catalog(root))

        if args.command == "export-csv":
            print(
                json.dumps(
                    export_csv(
                        root,
                        args.output_path,
                        args.fields,
                        args.document_ids,
                        args.query,
                        args.filters,
                        args.sort,
                        args.order,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "export-archive":
            print(
                json.dumps(
                    export_archive(
                        root,
                        args.output_path,
                        dataset_ids=args.dataset_ids,
                        dataset_names=args.dataset_names,
                        document_ids=args.document_ids,
                        control_numbers=args.control_numbers,
                        query=args.query,
                        raw_filters=args.filters,
                        from_run_id=args.from_run_id,
                        exclude_dataset_ids=args.exclude_dataset_ids,
                        exclude_dataset_names=args.exclude_dataset_names,
                        exclude_document_ids=args.exclude_document_ids,
                        exclude_control_numbers=args.exclude_control_numbers,
                        exclude_query=args.exclude_query,
                        exclude_filters=args.exclude_filters,
                        family_mode=args.family_mode,
                        seed_limit=args.seed_limit,
                        portable_workspace=args.portable_workspace,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "get-doc":
            return emit_cli_payload(
                "get-doc",
                get_doc(root, args.document_id, args.include_text, args.chunk_indexes),
                verbose=args.verbose,
            )

        if args.command == "list-chunks":
            return emit_cli_payload("list-chunks", list_chunks(root, args.document_id, args.page, args.per_page))

        if args.command == "search-chunks":
            return emit_cli_payload(
                "search-chunks",
                search_chunks(
                    root,
                    args.query,
                    args.filters,
                    args.sort,
                    args.order,
                    args.top_k,
                    args.per_doc_cap,
                    count_only=args.count_only,
                    distinct_docs=args.distinct_docs,
                ),
                verbose=args.verbose,
            )

        if args.command == "aggregate":
            return emit_cli_payload(
                "aggregate",
                aggregate(
                    root,
                    args.filters,
                    args.group_bys,
                    args.metric,
                    args.order_by,
                    args.order,
                    args.limit,
                    args.explain,
                ),
            )

        if args.command == "list-datasets":
            return emit_cli_payload("list-datasets", list_datasets(root))

        if args.command == "create-dataset":
            return emit_cli_payload("create-dataset", create_dataset(root, args.dataset_name))

        if args.command == "add-to-dataset":
            return emit_cli_payload(
                "add-to-dataset",
                add_to_dataset(
                    root,
                    args.document_ids,
                    dataset_id=args.dataset_id,
                    dataset_name=args.dataset_name,
                ),
            )

        if args.command == "remove-from-dataset":
            return emit_cli_payload(
                "remove-from-dataset",
                remove_from_dataset(
                    root,
                    args.document_ids,
                    dataset_id=args.dataset_id,
                    dataset_name=args.dataset_name,
                ),
            )

        if args.command == "delete-dataset":
            return emit_cli_payload(
                "delete-dataset",
                delete_dataset(
                    root,
                    dataset_id=args.dataset_id,
                    dataset_name=args.dataset_name,
                ),
            )

        if args.command == "list-runs":
            print(json.dumps(list_runs(root), indent=2, sort_keys=True))
            return 0

        if args.command == "get-run":
            print(json.dumps(get_run(root, args.run_id), indent=2, sort_keys=True))
            return 0

        if args.command == "create-run":
            print(
                json.dumps(
                    create_run(
                        root,
                        job_version_id=args.job_version_id,
                        raw_job_name=args.job_name,
                        job_version_number=args.job_version_number,
                        dataset_ids=args.dataset_ids,
                        dataset_names=args.dataset_names,
                        document_ids=args.document_ids,
                        control_numbers=args.control_numbers,
                        query=args.query,
                        raw_filters=args.filters,
                        from_run_id=args.from_run_id,
                        exclude_dataset_ids=args.exclude_dataset_ids,
                        exclude_dataset_names=args.exclude_dataset_names,
                        exclude_document_ids=args.exclude_document_ids,
                        exclude_control_numbers=args.exclude_control_numbers,
                        exclude_query=args.exclude_query,
                        exclude_filters=args.exclude_filters,
                        family_mode=args.family_mode,
                        seed_limit=args.seed_limit,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "list-text-revisions":
            print(json.dumps(list_text_revisions(root, document_id=args.document_id), indent=2, sort_keys=True))
            return 0

        if args.command == "activate-text-revision":
            print(
                json.dumps(
                    activate_text_revision(
                        root,
                        document_id=args.document_id,
                        text_revision_id=args.text_revision_id,
                        activation_policy=args.activation_policy,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "list-results":
            print(
                json.dumps(
                    list_results(root, run_id=args.run_id, document_id=args.document_id),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "execute-run":
            print(json.dumps(execute_run(root, run_id=args.run_id), indent=2, sort_keys=True))
            return 0

        if args.command == "claim-run-items":
            print(
                json.dumps(
                    claim_run_items(
                        root,
                        run_id=args.run_id,
                        claimed_by=args.claimed_by,
                        limit=args.limit,
                        stale_after_seconds=args.stale_seconds,
                        launch_mode=args.launch_mode,
                        worker_task_id=args.worker_task_id,
                        max_batches=args.max_batches,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "prepare-run-batch":
            print(
                json.dumps(
                    prepare_run_batch(
                        root,
                        run_id=args.run_id,
                        claimed_by=args.claimed_by,
                        limit=args.limit,
                        stale_after_seconds=args.stale_seconds,
                        launch_mode=args.launch_mode,
                        worker_task_id=args.worker_task_id,
                        max_batches=args.max_batches,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "get-run-item-context":
            print(json.dumps(get_run_item_context(root, run_item_id=args.run_item_id), indent=2, sort_keys=True))
            return 0

        if args.command == "heartbeat-run-items":
            print(
                json.dumps(
                    heartbeat_run_items(root, run_id=args.run_id, claimed_by=args.claimed_by),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "finish-run-worker":
            print(
                json.dumps(
                    finish_run_worker(
                        root,
                        run_id=args.run_id,
                        claimed_by=args.claimed_by,
                        worker_status=args.worker_status,
                        summary_json=args.summary_json,
                        error_summary=args.error_summary,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "complete-run-item":
            print(
                json.dumps(
                    complete_run_item(
                        root,
                        run_item_id=args.run_item_id,
                        claimed_by=args.claimed_by,
                        page_text=args.page_text,
                        raw_output_json=args.raw_output_json,
                        normalized_output_json=args.normalized_output_json,
                        output_values_json=args.output_values_json,
                        created_text_revision_json=args.created_text_revision_json,
                        provider_metadata_json=args.provider_metadata_json,
                        provider_request_id=args.provider_request_id,
                        input_tokens=args.input_tokens,
                        output_tokens=args.output_tokens,
                        cost_cents=args.cost_cents,
                        latency_ms=args.latency_ms,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "fail-run-item":
            print(
                json.dumps(
                    fail_run_item(
                        root,
                        run_item_id=args.run_item_id,
                        claimed_by=args.claimed_by,
                        error_summary=args.error,
                        provider_metadata_json=args.provider_metadata_json,
                        provider_request_id=args.provider_request_id,
                        input_tokens=args.input_tokens,
                        output_tokens=args.output_tokens,
                        cost_cents=args.cost_cents,
                        latency_ms=args.latency_ms,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "run-status":
            print(json.dumps(run_status(root, run_id=args.run_id), indent=2, sort_keys=True))
            return 0

        if args.command == "cancel-run":
            print(json.dumps(cancel_run(root, run_id=args.run_id, force=args.force), indent=2, sort_keys=True))
            return 0

        if args.command == "finalize-ocr-run":
            print(json.dumps(finalize_ocr_run(root, run_id=args.run_id), indent=2, sort_keys=True))
            return 0

        if args.command == "finalize-image-description-run":
            print(json.dumps(finalize_image_description_run(root, run_id=args.run_id), indent=2, sort_keys=True))
            return 0

        if args.command == "publish-run-results":
            print(
                json.dumps(
                    publish_run_results(root, run_id=args.run_id, raw_output_names=args.output_names),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "list-jobs":
            print(json.dumps(list_jobs(root), indent=2, sort_keys=True))
            return 0

        if args.command == "create-job":
            print(
                json.dumps(
                    create_job(root, args.job_name, args.job_kind, args.description),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "add-job-output":
            print(
                json.dumps(
                    add_job_output(
                        root,
                        args.job_name,
                        args.output_name,
                        args.value_type,
                        bound_custom_field=args.bound_custom_field,
                        description=args.description,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "list-job-versions":
            print(json.dumps(list_job_versions(root, args.job_name), indent=2, sort_keys=True))
            return 0

        if args.command == "create-job-version":
            print(
                json.dumps(
                    create_job_version(
                        root,
                        args.job_name,
                        instruction=args.instruction,
                        capability=args.capability,
                        provider=args.provider,
                        model=args.model,
                        input_basis=args.input_basis,
                        response_schema_json=args.response_schema_json,
                        parameters_json=args.parameters_json,
                        segment_profile=args.segment_profile,
                        aggregation_strategy=args.aggregation_strategy,
                        display_name=args.display_name,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "add-field":
            return emit_cli_payload("add-field", add_field(root, args.field_name, args.field_type, args.instruction))

        if args.command == "promote-field-type":
            return emit_cli_payload("promote-field-type", promote_field_type(root, args.field_name, args.target_field_type))

        if args.command == "set-field":
            return emit_cli_payload("set-field", set_field(root, args.doc_id, args.field, args.value))

        parser.error(f"Unknown command: {args.command}")
        return 2
    except RetrieverError as exc:
        print(json.dumps({"error": str(exc), "tool_version": TOOL_VERSION}), file=sys.stderr)
        return 2
    except sqlite3.Error as exc:
        print(json.dumps({"error": f"SQLite error: {exc}", "tool_version": TOOL_VERSION}), file=sys.stderr)
        return 2
    except Exception as exc:
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}", "tool_version": TOOL_VERSION}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
