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
        typed_value = value

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
        "abs_path": str(paths["root"] / row["rel_path"]),
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

        results: list[dict[str, object]] = []
        for document_id, match in matches.items():
            row = match["row"]
            results.append(
                {
                    "id": document_id,
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
                    "snippet": match["snippet"],
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
            "query": query,
            "filters": filter_summary,
            "sort": normalized_sort_field or ("bates" if is_bates_query and query.strip() else ("relevance" if query.strip() else "updated_at")),
            "order": (order or ("asc" if (is_bates_query or (query.strip() and (sort_field in (None, "relevance")))) else "desc")).lower(),
            "page": page,
            "per_page": per_page,
            "total_hits": total_hits,
            "total_pages": total_pages,
            "results": paged_results,
        }
    finally:
        connection.close()


def add_dataset_selector_arguments(parser: argparse.ArgumentParser) -> None:
    selector_group = parser.add_mutually_exclusive_group(required=True)
    selector_group.add_argument("--dataset-id", type=int, help="Dataset id")
    selector_group.add_argument("--dataset-name", help="Exact dataset name")


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
    search_parser.add_argument("workspace", help="Workspace root path")
    search_parser.add_argument("query", nargs="?", default="", help="Keyword query text")
    search_parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable filter in the form <field> <op> <value>",
    )
    search_parser.add_argument("--sort", "--sort-by", dest="sort", help="Sort field or 'relevance'")
    search_parser.add_argument("--order", "--sort-order", dest="order", choices=("asc", "desc"), help="Sort order")
    search_parser.add_argument("--page", type=int, default=1, help="1-based result page")
    search_parser.add_argument("--per-page", "--limit", dest="per_page", type=int, default=DEFAULT_PAGE_SIZE, help="Results per page")

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

    add_field_parser = subparsers.add_parser("add-field", help="Add a custom document field")
    add_field_parser.add_argument("workspace", help="Workspace root path")
    add_field_parser.add_argument("field_name", help="Field name")
    add_field_parser.add_argument("field_type", choices=sorted(REGISTRY_FIELD_TYPES), help="Field type")
    add_field_parser.add_argument("--instruction", help="Field extraction instruction")

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
            print(json.dumps(doctor(root, args.quick), indent=2, sort_keys=True))
            return 0

        if args.command == "bootstrap":
            print(json.dumps(bootstrap(root), indent=2, sort_keys=True))
            return 0

        if args.command == "ingest":
            print(json.dumps(ingest(root, args.recursive, args.file_types), indent=2, sort_keys=True))
            return 0

        if args.command == "ingest-production":
            print(json.dumps(ingest_production(root, args.production_root), indent=2, sort_keys=True))
            return 0

        if args.command == "search":
            print(
                json.dumps(
                    search(root, args.query, args.filters, args.sort, args.order, args.page, args.per_page),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "list-datasets":
            print(json.dumps(list_datasets(root), indent=2, sort_keys=True))
            return 0

        if args.command == "create-dataset":
            print(json.dumps(create_dataset(root, args.dataset_name), indent=2, sort_keys=True))
            return 0

        if args.command == "add-to-dataset":
            print(
                json.dumps(
                    add_to_dataset(
                        root,
                        args.document_ids,
                        dataset_id=args.dataset_id,
                        dataset_name=args.dataset_name,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "remove-from-dataset":
            print(
                json.dumps(
                    remove_from_dataset(
                        root,
                        args.document_ids,
                        dataset_id=args.dataset_id,
                        dataset_name=args.dataset_name,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "delete-dataset":
            print(
                json.dumps(
                    delete_dataset(
                        root,
                        dataset_id=args.dataset_id,
                        dataset_name=args.dataset_name,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "add-field":
            print(json.dumps(add_field(root, args.field_name, args.field_type, args.instruction), indent=2, sort_keys=True))
            return 0

        if args.command == "set-field":
            print(json.dumps(set_field(root, args.doc_id, args.field, args.value), indent=2, sort_keys=True))
            return 0

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
