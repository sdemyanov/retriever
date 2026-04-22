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


OCCURRENCE_FILTER_FIELDS = {
    "begin_attachment",
    "begin_bates",
    "custodian",
    "end_attachment",
    "end_bates",
    "file_hash",
    "file_name",
    "file_size",
    "file_type",
    "ingested_at",
    "last_seen_at",
    "lifecycle_status",
    "production_id",
    "rel_path",
    "source_folder_path",
    "source_item_id",
    "source_kind",
    "source_rel_path",
    "text_status",
    "updated_at",
}


def build_scalar_filter_clause(
    column_expr: str,
    field_type: str,
    operator: str,
    value: str | None,
) -> tuple[str, list[object]]:
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


def build_filter_clause(alias: str, field_def: dict[str, str], operator: str, value: str | None) -> tuple[str, list[object]]:
    field_name = field_def["field_name"]
    field_type = field_def["field_type"]
    field_source = field_def.get("source")
    if field_source == "virtual":
        return build_virtual_filter_clause(alias, field_name, field_type, operator, value)
    if alias == "d" and field_name in OCCURRENCE_FILTER_FIELDS:
        occurrence_clause, occurrence_params = build_scalar_filter_clause(
            f"o.{quote_identifier(field_name)}",
            field_type,
            operator,
            value,
        )
        return (
            "EXISTS ("
            "SELECT 1 FROM document_occurrences o "
            f"WHERE o.document_id = {alias}.id "
            "AND o.lifecycle_status = 'active' "
            f"AND {occurrence_clause}"
            ")",
            occurrence_params,
        )
    return build_scalar_filter_clause(f"{alias}.{quote_identifier(field_name)}", field_type, operator, value)


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
            positive_clause = attachment_child_filter_sql(alias)
        else:
            positive_clause = (
                "EXISTS ("
                "SELECT 1 FROM documents child "
                f"WHERE child.parent_document_id = {alias}.id "
                f"AND COALESCE(child.child_document_kind, '{CHILD_DOCUMENT_KIND_ATTACHMENT}') = '{CHILD_DOCUMENT_KIND_ATTACHMENT}' "
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
    connection: sqlite3.Connection, raw_filters: object | None
) -> tuple[list[object], list[str], list[object]]:
    if uses_legacy_tuple_filters(raw_filters):
        return build_legacy_search_filters(connection, raw_filters)  # type: ignore[arg-type]
    return build_sql_like_search_filters(connection, raw_filters)


def uses_legacy_tuple_filters(raw_filters: object | None) -> bool:
    if not isinstance(raw_filters, list) or not raw_filters:
        return False
    legacy_operators = {"eq", "neq", "gt", "gte", "lt", "lte", "contains", "is-null", "not-null"}
    saw_item = False
    for item in raw_filters:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            return False
        operator = normalize_inline_whitespace(str(item[1] or "")).lower()
        if operator not in legacy_operators:
            return False
        saw_item = True
    return saw_item


def build_legacy_search_filters(
    connection: sqlite3.Connection,
    raw_filters: list[list[str]] | None,
) -> tuple[list[dict[str, object]], list[str], list[object]]:
    parsed_filters = parse_filter_args(raw_filters)
    clauses = base_document_search_clauses()
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


def base_document_search_clauses() -> list[str]:
    return [
        "d.lifecycle_status NOT IN ('missing', 'deleted')",
        "EXISTS (SELECT 1 FROM dataset_documents dd WHERE dd.document_id = d.id)",
    ]


def known_logical_field_names(connection: sqlite3.Connection) -> list[str]:
    names = set(BUILTIN_FIELD_TYPES) | set(VIRTUAL_FILTER_FIELD_TYPES)
    if table_exists(connection, "custom_fields_registry"):
        registry_rows = connection.execute(
            """
            SELECT field_name
            FROM custom_fields_registry
            ORDER BY field_name ASC
            """
        ).fetchall()
        document_columns = table_columns(connection, "documents")
        for row in registry_rows:
            field_name = str(row["field_name"])
            if field_name in document_columns:
                names.add(field_name)
    return sorted(names)


def field_name_suggestions(connection: sqlite3.Connection, field_name: str) -> list[str]:
    candidates = known_logical_field_names(connection)
    return difflib.get_close_matches(field_name, candidates, n=3, cutoff=0.45)


def sql_filter_operator_names_for_field_type(field_type: str) -> list[str]:
    if field_type == "boolean":
        return ["=", "!=", "IS NULL", "IS NOT NULL"]
    operators = ["=", "!=", "<", "<=", ">", ">=", "IS NULL", "IS NOT NULL", "IN", "BETWEEN"]
    if field_type in {"text", "date"}:
        operators.insert(6, "LIKE")
    return operators


def filter_error_excerpt(expression: str, position: int) -> tuple[str, str]:
    start = max(0, position - 60)
    end = min(len(expression), position + 60)
    excerpt = expression[start:end]
    caret = " " * max(0, position - start) + "^"
    return excerpt, caret


def raise_filter_syntax_error(expression: str, position: int, message: str) -> None:
    excerpt, caret = filter_error_excerpt(expression, position)
    raise RetrieverError(f"{message} at position {position + 1}.\n{excerpt}\n{caret}")


def tokenize_sql_filter_expression(expression: str) -> list[dict[str, object]]:
    if len(expression.encode("utf-8")) > MAX_FILTER_EXPRESSION_LENGTH:
        raise RetrieverError(
            f"Filter expressions are capped at {MAX_FILTER_EXPRESSION_LENGTH} bytes."
        )
    tokens: list[dict[str, object]] = []
    index = 0
    length = len(expression)
    keywords = {"AND", "BETWEEN", "FALSE", "IN", "IS", "LIKE", "NOT", "NULL", "OR", "TRUE"}

    while index < length:
        char = expression[index]
        if char.isspace():
            index += 1
            continue
        if char in "(),":
            token_kind = {"(": "lparen", ")": "rparen", ",": "comma"}[char]
            tokens.append({"kind": token_kind, "value": char, "start": index, "end": index + 1})
            index += 1
            continue
        if expression.startswith("<=", index) or expression.startswith(">=", index) or expression.startswith("!=", index) or expression.startswith("<>", index):
            tokens.append({"kind": "operator", "value": expression[index:index + 2], "start": index, "end": index + 2})
            index += 2
            continue
        if char in "=<>":
            tokens.append({"kind": "operator", "value": char, "start": index, "end": index + 1})
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            start = index
            index += 1
            value_chars: list[str] = []
            while index < length:
                current = expression[index]
                if current == "\\" and index + 1 < length:
                    value_chars.append(expression[index + 1])
                    index += 2
                    continue
                if current == quote:
                    if index + 1 < length and expression[index + 1] == quote:
                        value_chars.append(quote)
                        index += 2
                        continue
                    index += 1
                    break
                value_chars.append(current)
                index += 1
            else:
                raise_filter_syntax_error(expression, start, "Unterminated string literal")
            tokens.append(
                {
                    "kind": "literal",
                    "literal_kind": "string",
                    "value": "".join(value_chars),
                    "start": start,
                    "end": index,
                }
            )
            continue
        if char in "+-" and index + 1 < length and expression[index + 1].isdigit():
            start = index
            index += 1
            while index < length and expression[index].isdigit():
                index += 1
            if index < length and expression[index] == ".":
                index += 1
                while index < length and expression[index].isdigit():
                    index += 1
            tokens.append(
                {
                    "kind": "literal",
                    "literal_kind": "number",
                    "value": expression[start:index],
                    "start": start,
                    "end": index,
                }
            )
            continue
        if char.isdigit():
            start = index
            while index < length and expression[index].isdigit():
                index += 1
            if index < length and expression[index] == ".":
                index += 1
                while index < length and expression[index].isdigit():
                    index += 1
            tokens.append(
                {
                    "kind": "literal",
                    "literal_kind": "number",
                    "value": expression[start:index],
                    "start": start,
                    "end": index,
                }
            )
            continue
        if char.isalpha() or char == "_":
            start = index
            index += 1
            while index < length and (expression[index].isalnum() or expression[index] == "_"):
                index += 1
            raw_value = expression[start:index]
            upper_value = raw_value.upper()
            if upper_value in keywords:
                if upper_value in {"TRUE", "FALSE"}:
                    tokens.append(
                        {
                            "kind": "literal",
                            "literal_kind": "boolean",
                            "value": upper_value == "TRUE",
                            "start": start,
                            "end": index,
                        }
                    )
                elif upper_value == "NULL":
                    tokens.append(
                        {
                            "kind": "literal",
                            "literal_kind": "null",
                            "value": None,
                            "start": start,
                            "end": index,
                        }
                    )
                else:
                    tokens.append({"kind": "keyword", "value": upper_value, "start": start, "end": index})
            else:
                tokens.append({"kind": "identifier", "value": raw_value, "start": start, "end": index})
            continue
        raise_filter_syntax_error(expression, index, f"Unexpected character {char!r}")

    tokens.append({"kind": "eof", "value": "", "start": length, "end": length})
    return tokens


def peek_filter_token(state: dict[str, object]) -> dict[str, object]:
    tokens = state["tokens"]
    index = int(state["index"])
    return tokens[index]  # type: ignore[index]


def consume_filter_token(state: dict[str, object]) -> dict[str, object]:
    token = peek_filter_token(state)
    state["index"] = int(state["index"]) + 1
    return token


def match_filter_keyword(state: dict[str, object], keyword: str) -> bool:
    token = peek_filter_token(state)
    if token["kind"] == "keyword" and token["value"] == keyword:
        consume_filter_token(state)
        return True
    return False


def match_filter_token_kind(state: dict[str, object], kind: str, value: str | None = None) -> dict[str, object] | None:
    token = peek_filter_token(state)
    if token["kind"] != kind:
        return None
    if value is not None and token["value"] != value:
        return None
    consume_filter_token(state)
    return token


def expect_filter_token(state: dict[str, object], kind: str, message: str, value: str | None = None) -> dict[str, object]:
    token = match_filter_token_kind(state, kind, value=value)
    if token is not None:
        return token
    next_token = peek_filter_token(state)
    raise_filter_syntax_error(str(state["expression"]), int(next_token["start"]), message)


def resolve_sql_filter_field(connection: sqlite3.Connection, raw_field_name: str) -> dict[str, object]:
    try:
        return resolve_field_definition(connection, raw_field_name)
    except RetrieverError as exc:
        suggestions = field_name_suggestions(connection, raw_field_name)
        suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
        raise RetrieverError(f"Unknown field '{raw_field_name}'.{suggestion_text}") from exc


def literal_text_value(literal: dict[str, object]) -> str | None:
    literal_kind = literal["literal_kind"]
    if literal_kind == "null":
        return None
    if literal_kind == "boolean":
        return "true" if bool(literal["value"]) else "false"
    return str(literal["value"])


def coerce_sql_literal(field_type: str, literal: dict[str, object]) -> object:
    raw_value = literal_text_value(literal)
    if raw_value is None:
        raise RetrieverError("NULL is only valid with IS NULL / IS NOT NULL.")
    if literal["literal_kind"] == "boolean" and field_type not in {"boolean", "text"}:
        raise RetrieverError(f"Expected {field_type} value, got boolean literal.")
    if literal["literal_kind"] == "number" and field_type == "boolean":
        return value_from_type("boolean", raw_value)
    if field_type == "date":
        normalized = normalize_date_field_value(raw_value)
        if normalized is None:
            raise RetrieverError(f"Expected ISO date value, got {raw_value!r}")
        return normalized
    if field_type in {"integer", "real", "boolean"}:
        return value_from_type(field_type, raw_value)
    return raw_value


def ensure_sql_filter_operator_supported(field_def: dict[str, object], operator: str) -> None:
    supported = sql_filter_operator_names_for_field_type(str(field_def["field_type"]))
    if operator not in supported:
        field_name = str(field_def["field_name"])
        field_type = str(field_def["field_type"])
        raise RetrieverError(
            f"Field '{field_name}' is {field_type}; supported operators: {', '.join(supported)}."
        )


def build_scalar_sql_filter_clause(
    sql_expression: str,
    field_def: dict[str, object],
    operator: str,
    operand: object | None,
) -> tuple[str, list[object]]:
    ensure_sql_filter_operator_supported(field_def, operator)
    field_type = str(field_def["field_type"])
    if operator in {"IS NULL", "IS NOT NULL"}:
        return f"{sql_expression} {operator}", []

    if operator == "LIKE":
        if field_type not in {"text", "date"}:
            ensure_sql_filter_operator_supported(field_def, operator)
        assert isinstance(operand, dict)
        return f"COALESCE({sql_expression}, '') LIKE ?", [str(coerce_sql_literal("text", operand))]

    if operator == "IN":
        if not isinstance(operand, list) or not operand:
            raise RetrieverError("IN requires at least one value.")
        if len(operand) > MAX_FILTER_IN_LIST_ITEMS:
            raise RetrieverError(f"IN (...) is capped at {MAX_FILTER_IN_LIST_ITEMS} values.")
        typed_values = [coerce_sql_literal(field_type, literal) for literal in operand]
        placeholders = ", ".join("?" for _ in typed_values)
        return f"{sql_expression} IN ({placeholders})", typed_values

    if operator == "BETWEEN":
        if not isinstance(operand, tuple) or len(operand) != 2:
            raise RetrieverError("BETWEEN requires two values.")
        left_value = coerce_sql_literal(field_type, operand[0])
        right_value = coerce_sql_literal(field_type, operand[1])
        return f"{sql_expression} BETWEEN ? AND ?", [left_value, right_value]

    assert isinstance(operand, dict)
    comparator = "!=" if operator == "<>" else operator
    return f"{sql_expression} {comparator} ?", [coerce_sql_literal(field_type, operand)]


def build_dataset_name_sql_filter_clause(
    alias: str,
    field_def: dict[str, object],
    operator: str,
    operand: object | None,
) -> tuple[str, list[object]]:
    ensure_sql_filter_operator_supported(field_def, operator)
    exists_sql = (
        "SELECT 1 "
        "FROM dataset_documents dd "
        "JOIN datasets ds ON ds.id = dd.dataset_id "
        f"WHERE dd.document_id = {alias}.id"
    )
    if operator == "IS NULL":
        return f"NOT EXISTS ({exists_sql})", []
    if operator == "IS NOT NULL":
        return f"EXISTS ({exists_sql})", []
    if operator == "IN":
        assert isinstance(operand, list)
        if len(operand) > MAX_FILTER_IN_LIST_ITEMS:
            raise RetrieverError(f"IN (...) is capped at {MAX_FILTER_IN_LIST_ITEMS} values.")
        values = [str(coerce_sql_literal("text", literal)) for literal in operand]
        placeholders = ", ".join("?" for _ in values)
        return f"EXISTS ({exists_sql} AND ds.dataset_name IN ({placeholders}))", values
    if operator == "BETWEEN":
        assert isinstance(operand, tuple)
        values = [str(coerce_sql_literal("text", operand[0])), str(coerce_sql_literal("text", operand[1]))]
        return f"EXISTS ({exists_sql} AND ds.dataset_name BETWEEN ? AND ?)", values
    if operator == "LIKE":
        assert isinstance(operand, dict)
        value = str(coerce_sql_literal("text", operand))
        return f"EXISTS ({exists_sql} AND COALESCE(ds.dataset_name, '') LIKE ?)", [value]
    assert isinstance(operand, dict)
    comparator = "!=" if operator == "<>" else operator
    value = str(coerce_sql_literal("text", operand))
    if comparator == "!=":
        return f"NOT EXISTS ({exists_sql} AND COALESCE(ds.dataset_name, '') = ?)", [value]
    return f"EXISTS ({exists_sql} AND COALESCE(ds.dataset_name, '') {comparator} ?)", [value]


def virtual_field_sql_expression(alias: str, field_name: str) -> str:
    if field_name == "production_name":
        return (
            "(SELECT p.production_name FROM productions p "
            f"WHERE p.id = {alias}.production_id)"
        )
    if field_name == "is_attachment":
        return f"(CASE WHEN {alias}.parent_document_id IS NOT NULL THEN 1 ELSE 0 END)"
    if field_name == "has_attachments":
        return (
            "(CASE WHEN EXISTS ("
            "SELECT 1 FROM documents child "
            f"WHERE child.parent_document_id = {alias}.id "
            "AND child.lifecycle_status NOT IN ('missing', 'deleted')"
            ") THEN 1 ELSE 0 END)"
        )
    raise RetrieverError(f"Unknown virtual filter: {field_name}")


def build_sql_filter_clause(
    alias: str,
    field_def: dict[str, object],
    operator: str,
    operand: object | None,
) -> tuple[str, list[object]]:
    field_name = str(field_def["field_name"])
    if field_def.get("source") == "virtual":
        if field_name == "dataset_name":
            return build_dataset_name_sql_filter_clause(alias, field_def, operator, operand)
        return build_scalar_sql_filter_clause(virtual_field_sql_expression(alias, field_name), field_def, operator, operand)
    return build_scalar_sql_filter_clause(f"{alias}.{quote_identifier(field_name)}", field_def, operator, operand)


def parse_sql_filter_literal(state: dict[str, object]) -> dict[str, object]:
    token = peek_filter_token(state)
    if token["kind"] != "literal":
        raise_filter_syntax_error(str(state["expression"]), int(token["start"]), "Expected a literal value")
    return consume_filter_token(state)


def parse_sql_filter_predicate(state: dict[str, object]) -> tuple[str, list[object]]:
    identifier = expect_filter_token(state, "identifier", "Expected a field name")
    field_name = str(identifier["value"])
    field_def = resolve_sql_filter_field(state["connection"], field_name)

    if match_filter_keyword(state, "IS"):
        operator = "IS NOT NULL" if match_filter_keyword(state, "NOT") else "IS NULL"
        token = expect_filter_token(state, "literal", "Expected NULL after IS / IS NOT")
        if token["literal_kind"] != "null":
            raise_filter_syntax_error(str(state["expression"]), int(token["start"]), "Expected NULL after IS / IS NOT")
        return build_sql_filter_clause("d", field_def, operator, None)

    negated_operator = match_filter_keyword(state, "NOT")
    if match_filter_keyword(state, "IN"):
        expect_filter_token(state, "lparen", "Expected '(' after IN")
        values: list[dict[str, object]] = []
        while True:
            values.append(parse_sql_filter_literal(state))
            if match_filter_token_kind(state, "comma") is None:
                break
        expect_filter_token(state, "rparen", "Expected ')' to close IN list")
        clause, params = build_sql_filter_clause("d", field_def, "IN", values)
        return (f"NOT ({clause})", params) if negated_operator else (clause, params)

    if match_filter_keyword(state, "BETWEEN"):
        left_value = parse_sql_filter_literal(state)
        if not match_filter_keyword(state, "AND"):
            token = peek_filter_token(state)
            raise_filter_syntax_error(str(state["expression"]), int(token["start"]), "Expected AND in BETWEEN expression")
        right_value = parse_sql_filter_literal(state)
        clause, params = build_sql_filter_clause("d", field_def, "BETWEEN", (left_value, right_value))
        return (f"NOT ({clause})", params) if negated_operator else (clause, params)

    if match_filter_keyword(state, "LIKE"):
        value = parse_sql_filter_literal(state)
        clause, params = build_sql_filter_clause("d", field_def, "LIKE", value)
        return (f"NOT ({clause})", params) if negated_operator else (clause, params)

    if negated_operator:
        token = peek_filter_token(state)
        raise_filter_syntax_error(str(state["expression"]), int(token["start"]), "Expected IN, BETWEEN, or LIKE after NOT")

    operator_token = expect_filter_token(state, "operator", "Expected an operator after the field name")
    value = parse_sql_filter_literal(state)
    return build_sql_filter_clause("d", field_def, str(operator_token["value"]).upper(), value)


def parse_sql_filter_primary(state: dict[str, object]) -> tuple[str, list[object]]:
    if match_filter_token_kind(state, "lparen") is not None:
        clause, params = parse_sql_filter_or_expression(state)
        expect_filter_token(state, "rparen", "Expected ')' to close grouped expression")
        return clause, params
    return parse_sql_filter_predicate(state)


def parse_sql_filter_not_expression(state: dict[str, object]) -> tuple[str, list[object]]:
    if match_filter_keyword(state, "NOT"):
        clause, params = parse_sql_filter_not_expression(state)
        return f"NOT ({clause})", params
    return parse_sql_filter_primary(state)


def parse_sql_filter_and_expression(state: dict[str, object]) -> tuple[str, list[object]]:
    clause, params = parse_sql_filter_not_expression(state)
    while match_filter_keyword(state, "AND"):
        right_clause, right_params = parse_sql_filter_not_expression(state)
        clause = f"({clause}) AND ({right_clause})"
        params.extend(right_params)
    return clause, params


def parse_sql_filter_or_expression(state: dict[str, object]) -> tuple[str, list[object]]:
    clause, params = parse_sql_filter_and_expression(state)
    while match_filter_keyword(state, "OR"):
        right_clause, right_params = parse_sql_filter_and_expression(state)
        clause = f"({clause}) OR ({right_clause})"
        params.extend(right_params)
    return clause, params


def compile_sql_filter_expression(
    connection: sqlite3.Connection,
    expression: str,
) -> tuple[str, list[object]]:
    normalized_expression = expression.strip()
    if not normalized_expression:
        raise RetrieverError("Filter expression cannot be empty.")
    state: dict[str, object] = {
        "connection": connection,
        "expression": normalized_expression,
        "tokens": tokenize_sql_filter_expression(normalized_expression),
        "index": 0,
    }
    clause, params = parse_sql_filter_or_expression(state)
    trailing_token = peek_filter_token(state)
    if trailing_token["kind"] != "eof":
        raise_filter_syntax_error(
            normalized_expression,
            int(trailing_token["start"]),
            "Unexpected token after complete expression",
        )
    return clause, params


def normalize_sql_filter_expressions(raw_filters: object | None) -> list[str]:
    if raw_filters is None:
        return []
    if isinstance(raw_filters, str):
        return [raw_filters] if raw_filters.strip() else []
    if not isinstance(raw_filters, list):
        raise RetrieverError("Filters must be provided as strings or repeatable --filter arguments.")
    expressions: list[str] = []
    for item in raw_filters:
        if isinstance(item, str):
            if item.strip():
                expressions.append(item)
            continue
        if isinstance(item, (list, tuple)):
            expression = " ".join(str(part) for part in item if normalize_inline_whitespace(str(part or "")))
            if expression:
                expressions.append(expression)
            continue
        raise RetrieverError("Unsupported filter payload shape.")
    return expressions


def build_sql_like_search_filters(
    connection: sqlite3.Connection,
    raw_filters: object | None,
) -> tuple[list[str], list[str], list[object]]:
    expressions = normalize_sql_filter_expressions(raw_filters)
    clauses = base_document_search_clauses()
    params: list[object] = []
    for expression in expressions:
        clause, clause_params = compile_sql_filter_expression(connection, expression)
        clauses.append(f"({clause})")
        params.extend(clause_params)
    return expressions, clauses, params


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


def build_occurrence_scope_filters(
    connection: sqlite3.Connection,
    raw_filters: object | None,
) -> tuple[list[str], list[object]]:
    clauses = ["o.lifecycle_status = 'active'"]
    params: list[object] = []
    if not uses_legacy_tuple_filters(raw_filters):
        return clauses, params
    for raw_filter in parse_filter_args(raw_filters):
        field_def = resolve_field_definition(connection, str(raw_filter["field_name"]))
        if field_def.get("source") == "virtual":
            continue
        if field_def["field_name"] not in OCCURRENCE_FILTER_FIELDS:
            continue
        clause, clause_params = build_scalar_filter_clause(
            f"o.{quote_identifier(field_def['field_name'])}",
            field_def["field_type"],
            str(raw_filter["operator"]),
            raw_filter["value"],  # type: ignore[arg-type]
        )
        clauses.append(clause)
        params.extend(clause_params)
    return clauses, params


def preferred_occurrence_for_document(
    connection: sqlite3.Connection,
    document_id: int,
    occurrence_scope_clauses: list[str],
    occurrence_scope_params: list[object],
) -> sqlite3.Row | None:
    scoped_rows = connection.execute(
        f"""
        SELECT *
        FROM document_occurrences o
        WHERE o.document_id = ?
          AND {' AND '.join(occurrence_scope_clauses)}
        ORDER BY o.id ASC
        """,
        [document_id, *occurrence_scope_params],
    ).fetchall()
    preferred = select_preferred_occurrence(scoped_rows)
    if preferred is not None:
        return preferred
    return select_preferred_occurrence(active_occurrence_rows_for_document(connection, document_id))


def preferred_occurrences_by_document(
    connection: sqlite3.Connection,
    document_ids: list[int],
    occurrence_scope_clauses: list[str],
    occurrence_scope_params: list[object],
) -> dict[int, sqlite3.Row]:
    preferred: dict[int, sqlite3.Row] = {}
    for document_id in document_ids:
        occurrence_row = preferred_occurrence_for_document(
            connection,
            document_id,
            occurrence_scope_clauses,
            occurrence_scope_params,
        )
        if occurrence_row is not None:
            preferred[document_id] = occurrence_row
    return preferred


def document_path_payload(
    paths: dict[str, Path],
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    occurrence_row: sqlite3.Row | None = None,
    include_preview_targets: bool = True,
) -> dict[str, object]:
    preview_target = default_preview_target(paths, row, connection)
    effective_rel_path = str(occurrence_row["rel_path"]) if occurrence_row is not None else str(row["rel_path"])
    if (
        occurrence_row is not None
        and str(preview_target.get("preview_type") or "") == "native"
        and str(preview_target.get("rel_path") or "") == str(row["rel_path"])
    ):
        effective_abs_path = str(document_absolute_path(paths, effective_rel_path))
        preview_target = build_preview_target_payload(
            rel_path=effective_rel_path,
            abs_path=effective_abs_path,
            preview_type="native",
            label=None,
            ordinal=0,
        )
    payload = {
        "rel_path": effective_rel_path,
        "abs_path": str(document_absolute_path(paths, effective_rel_path)),
        "preview_rel_path": preview_target["rel_path"],
        "preview_abs_path": preview_target["abs_path"],
        "preview_file_rel_path": preview_target["file_rel_path"],
        "preview_file_abs_path": preview_target["file_abs_path"],
        "preview_target_fragment": preview_target["target_fragment"],
    }
    if include_preview_targets:
        payload["preview_targets"] = collect_preview_targets(paths, int(row["id"]), effective_rel_path, connection)
    return payload


def fetch_attachment_counts(
    connection: sqlite3.Connection,
    parent_ids: list[int],
) -> dict[int, int]:
    if not parent_ids:
        return {}
    placeholders = ", ".join("?" for _ in parent_ids)
    rows = connection.execute(
        f"""
        SELECT parent_document_id, COUNT(*) AS attachment_count
        FROM documents
        WHERE parent_document_id IN ({placeholders})
          AND COALESCE(child_document_kind, ?) = ?
          AND lifecycle_status NOT IN ('missing', 'deleted')
        GROUP BY parent_document_id
        """,
        [*parent_ids, CHILD_DOCUMENT_KIND_ATTACHMENT, CHILD_DOCUMENT_KIND_ATTACHMENT],
    ).fetchall()
    return {int(row["parent_document_id"]): int(row["attachment_count"]) for row in rows}


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
          AND COALESCE(child_document_kind, ?) = ?
          AND lifecycle_status NOT IN ('missing', 'deleted')
        ORDER BY parent_document_id ASC, id ASC
        """,
        [*parent_ids, CHILD_DOCUMENT_KIND_ATTACHMENT, CHILD_DOCUMENT_KIND_ATTACHMENT],
    ).fetchall()
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["parent_document_id"])].append(
            {
                "id": int(row["id"]),
                "control_number": row["control_number"],
                "file_name": row["file_name"],
                "file_type": row["file_type"],
                "content_type": row["content_type"],
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


def fetch_child_document_summaries(
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
          AND COALESCE(child_document_kind, ?) != ?
          AND lifecycle_status NOT IN ('missing', 'deleted')
        ORDER BY parent_document_id ASC, COALESCE(date_created, '') ASC, id ASC
        """,
        [*parent_ids, CHILD_DOCUMENT_KIND_ATTACHMENT, CHILD_DOCUMENT_KIND_ATTACHMENT],
    ).fetchall()
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["parent_document_id"])].append(
            {
                "id": int(row["id"]),
                "control_number": row["control_number"],
                "file_name": row["file_name"],
                "file_type": row["file_type"],
                "content_type": row["content_type"],
                **document_path_payload(paths, connection, row),
                "child_document_kind": row["child_document_kind"],
                "title": row["title"],
                "date_created": row["date_created"],
                "date_modified": row["date_modified"],
                "root_message_key": row["root_message_key"],
                "source_kind": row["source_kind"],
                "source_rel_path": row["source_rel_path"],
                "source_item_id": row["source_item_id"],
            }
        )
    return grouped


def fetch_child_document_counts(
    connection: sqlite3.Connection,
    parent_ids: list[int],
) -> dict[int, int]:
    if not parent_ids:
        return {}
    placeholders = ", ".join("?" for _ in parent_ids)
    rows = connection.execute(
        f"""
        SELECT parent_document_id, COUNT(*) AS child_document_count
        FROM documents
        WHERE parent_document_id IN ({placeholders})
          AND COALESCE(child_document_kind, ?) != ?
          AND lifecycle_status NOT IN ('missing', 'deleted')
        GROUP BY parent_document_id
        """,
        [*parent_ids, CHILD_DOCUMENT_KIND_ATTACHMENT, CHILD_DOCUMENT_KIND_ATTACHMENT],
    ).fetchall()
    return {int(row["parent_document_id"]): int(row["child_document_count"]) for row in rows}


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
    for key in ("control_number", "file_name", "file_type", "preview_abs_path", "snippet", "rank"):
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
    if int(item.get("child_document_count") or 0) > 0:
        compact["child_document_count"] = int(item["child_document_count"])
    if payload_has_meaningful_value(item.get("parent")):
        compact["parent"] = item["parent"]
    if "display_values" in item:
        compact["display_values"] = item["display_values"]
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
    if int(item.get("child_document_count") or 0) > 0:
        compact["child_document_count"] = int(item["child_document_count"])
    if payload_has_meaningful_value(item.get("parent")):
        compact["parent"] = item["parent"]
    return compact


def compact_search_payload(payload: dict[str, object]) -> dict[str, object]:
    compact_payload = {
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
    if payload_has_meaningful_value(payload.get("scope")):
        compact_payload["scope"] = payload["scope"]
    if payload_has_meaningful_value(payload.get("header")):
        compact_payload["header"] = payload["header"]
    if payload_has_meaningful_value(payload.get("display")):
        compact_payload["display"] = payload["display"]
    if payload_has_meaningful_value(payload.get("warnings")):
        compact_payload["warnings"] = payload["warnings"]
    if payload_has_meaningful_value(payload.get("rendered_markdown")):
        compact_payload["rendered_markdown"] = payload["rendered_markdown"]
    return compact_payload


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
    compact_payload = {
        "query": payload["query"],
        "filters": payload["filters"],
        "sort": payload["sort"],
        "order": payload["order"],
        "top_k": payload["top_k"],
        "per_doc_cap": payload["per_doc_cap"],
        "total_matches": payload["total_matches"],
        "results": [compact_search_chunk_result_payload(item) for item in payload["results"]],
    }
    if payload_has_meaningful_value(payload.get("scope")):
        compact_payload["scope"] = payload["scope"]
    if payload.get("selected_from_scope"):
        compact_payload["selected_from_scope"] = True
    return compact_payload


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
    benchmark_mark("prepare_payload_begin", command=command, verbose=verbose)
    prepared_payload = prepare_cli_payload(command, payload, verbose=verbose)
    benchmark_mark("prepare_payload_done")
    serialized = json.dumps(prepared_payload, indent=2, sort_keys=True)
    benchmark_mark("json_serialized", bytes=len(serialized.encode("utf-8")))
    sys.stdout.write(serialized + "\n")
    sys.stdout.flush()
    benchmark_mark("stdout_written")
    benchmark_emit(command=command, verbose=verbose)
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
    occurrence_row: sqlite3.Row | None = None,
    include_parent_context: bool = True,
    include_attachment_context: bool = True,
) -> dict[str, object]:
    source_row = occurrence_row or row
    payload: dict[str, object] = {
        "document_id": int(row["id"]),
        "control_number": row["control_number"],
        "conversation_id": row["conversation_id"],
        "dataset_id": row["dataset_id"],
        "parent_document_id": row["parent_document_id"],
        "child_document_kind": row["child_document_kind"],
        "root_message_key": row["root_message_key"],
        "source_kind": source_row["source_kind"],
        "source_rel_path": source_row["source_rel_path"],
        "source_item_id": source_row["source_item_id"],
        "source_folder_path": source_row["source_folder_path"],
        "production_id": source_row["production_id"],
        **document_path_payload(paths, connection, row, occurrence_row=occurrence_row),
        "file_name": source_row["file_name"],
        "file_type": source_row["file_type"],
        "metadata": {
            "author": row["author"],
            "begin_attachment": source_row["begin_attachment"],
            "begin_bates": source_row["begin_bates"],
            "child_document_kind": row["child_document_kind"],
            "content_type": row["content_type"],
            "conversation_id": row["conversation_id"],
            "custodian": source_row["custodian"],
            "dataset_id": row["dataset_id"],
            "date_created": row["date_created"],
            "date_modified": row["date_modified"],
            "end_attachment": source_row["end_attachment"],
            "end_bates": source_row["end_bates"],
            "page_count": row["page_count"],
            "participants": row["participants"],
            "recipients": row["recipients"],
            "root_message_key": row["root_message_key"],
            "source_kind": source_row["source_kind"],
            "source_rel_path": source_row["source_rel_path"],
            "source_item_id": source_row["source_item_id"],
            "source_folder_path": source_row["source_folder_path"],
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
        child_documents = fetch_child_document_summaries(connection, paths, [int(row["id"])]).get(int(row["id"]), [])
        payload["child_document_count"] = len(child_documents)
        payload["child_documents"] = child_documents
    if include_parent_context and row["parent_document_id"] is not None:
        payload["parent"] = fetch_parent_summaries(connection, [row]).get(int(row["parent_document_id"]))
    return payload


def build_chunk_citation_payload(
    row: sqlite3.Row,
    occurrence_row: sqlite3.Row | None = None,
    *,
    preview_rel_path: str,
    preview_abs_path: str,
    chunk_index: int,
    char_start: int,
    char_end: int,
    snippet: str,
) -> dict[str, object]:
    source_row = occurrence_row or row
    return {
        "document_id": int(row["id"]),
        "control_number": row["control_number"],
        "file_name": source_row["file_name"],
        "chunk_index": chunk_index,
        "char_start": char_start,
        "char_end": char_end,
        "snippet": snippet,
        "preview_rel_path": preview_rel_path,
        "preview_abs_path": preview_abs_path,
    }


def filter_operators_for_field_type(field_type: str) -> list[str]:
    return sql_filter_operator_names_for_field_type(field_type)


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
    displayable: bool | None = None,
) -> dict[str, object]:
    return {
        "name": field_name,
        "type": field_type,
        "description": catalog_description_for_field(field_name, source=source, instruction=instruction),
        "filter_operators": filter_operators_for_field_type(field_type),
        "sortable": source != "virtual",
        "aggregatable": source != "virtual" or field_name in AGGREGATABLE_VIRTUAL_FIELDS,
        "displayable": displayable if displayable is not None else (source != "virtual" or field_name in DISPLAYABLE_VIRTUAL_FIELDS),
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
    stable_results = sorted(results, key=lambda item: item["id"])

    if query_present and (sort_field is None or sort_field == "relevance"):
        stable_results = stable_sort_results_by_field(stable_results, "date_created", "desc")
        return sorted(stable_results, key=lambda item: (item["rank"] is None, item["rank"]))

    field_name = sort_field or "date_created"
    normalized_order = (order or "desc").lower()
    return stable_sort_results_by_field(stable_results, field_name, normalized_order)


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
        "sort": normalized_sort_field or ("bates" if is_bates_query and query.strip() else ("relevance" if query.strip() else "date_created")),
        "order": (order or ("asc" if (is_bates_query or (query.strip() and (sort_field in (None, "relevance")))) else "desc")).lower(),
        "sort_spec": f"{normalized_sort_field or ('bates' if is_bates_query and query.strip() else ('relevance' if query.strip() else 'date_created'))} "
        f"{(order or ('asc' if (is_bates_query or (query.strip() and (sort_field in (None, 'relevance')))) else 'desc')).lower()}",
        "results": sorted_results,
    }


def sql_sort_terms_for_field(
    connection: sqlite3.Connection,
    field_name: str,
    order: str,
    *,
    alias: str,
) -> list[str]:
    normalized_order = "DESC" if normalize_inline_whitespace(order).lower() == "desc" else "ASC"
    if field_name == "id":
        return [f"{alias}.id {normalized_order}"]

    field_def = resolve_field_definition(connection, field_name)
    if field_def.get("source") == "virtual":
        raise RetrieverError(f"Cannot sort by virtual filter field: {field_name}")

    canonical_name = str(field_def["field_name"])
    column_expr = f"{alias}.{quote_identifier(canonical_name)}"
    field_type = str(field_def["field_type"])
    if field_type == "date":
        normalized_expr = f"datetime({column_expr})"
        return [
            f"CASE WHEN {column_expr} IS NULL OR {normalized_expr} IS NULL THEN 1 ELSE 0 END ASC",
            f"{normalized_expr} {normalized_order}",
        ]
    if field_type == "text":
        return [
            f"CASE WHEN {column_expr} IS NULL THEN 1 ELSE 0 END ASC",
            f"LOWER({column_expr}) {normalized_order}",
        ]
    return [
        f"CASE WHEN {column_expr} IS NULL THEN 1 ELSE 0 END ASC",
        f"{column_expr} {normalized_order}",
    ]


def sql_order_by_for_sort_specs(
    connection: sqlite3.Connection,
    sort_specs: list[tuple[str, str]],
    *,
    alias: str,
) -> str:
    effective_specs = list(sort_specs)
    if not any(field_name == "id" for field_name, _ in effective_specs):
        effective_specs.append(("id", "asc"))
    terms: list[str] = []
    for field_name, direction in effective_specs:
        terms.extend(sql_sort_terms_for_field(connection, field_name, direction, alias=alias))
    return ", ".join(terms)


def sql_relevance_order_by(
    connection: sqlite3.Connection,
    *,
    row_alias: str,
    rank_expr: str,
) -> str:
    return ", ".join(
        [
            f"CASE WHEN {rank_expr} IS NULL THEN 1 ELSE 0 END ASC",
            f"{rank_expr} ASC",
            sql_order_by_for_sort_specs(connection, [("date_created", "desc")], alias=row_alias),
        ]
    )


def sql_bates_order_by(*, row_alias: str, prioritize_rank: bool) -> str:
    terms: list[str] = []
    if prioritize_rank:
        terms.extend(
            [
                f"CASE WHEN {row_alias}.rank IS NULL THEN 1 ELSE 0 END ASC",
                f"{row_alias}.rank ASC",
            ]
        )
    terms.extend(
        [
            f"CASE WHEN {row_alias}.bates_sort_value IS NULL THEN 1 ELSE 0 END ASC",
            f"{row_alias}.bates_sort_value ASC",
            f"{row_alias}.id ASC",
        ]
    )
    return ", ".join(terms)


def search_browse_page(
    connection: sqlite3.Connection,
    clauses: list[str],
    params: list[object],
    *,
    limit: int,
    offset: int,
    order_by_sql: str,
) -> dict[str, object]:
    where_clause = " AND ".join(clauses)
    count_row = connection.execute(
        f"""
        SELECT COUNT(*) AS total_hits
        FROM documents d
        WHERE {where_clause}
        """,
        params,
    ).fetchone()
    total_hits = int(count_row["total_hits"] or 0) if count_row is not None else 0
    rows = connection.execute(
        f"""
        SELECT d.*
        FROM documents d
        WHERE {where_clause}
        ORDER BY {order_by_sql}
        LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    return {
        "total_hits": total_hits,
        "results": [
            {
                "id": int(row["id"]),
                "rank": None,
                "snippet": metadata_snippet(row),
                "row": row,
            }
            for row in rows
        ],
    }


def search_bates_page(
    connection: sqlite3.Connection,
    query_begin: str,
    query_end: str,
    clauses: list[str],
    params: list[object],
    *,
    limit: int,
    offset: int,
    order_by_sql: str,
) -> dict[str, object]:
    where_clause = " AND ".join(clauses)
    range_begin_expr = "COALESCE(d.begin_bates, d.control_number)"
    range_end_expr = "COALESCE(d.end_bates, d.control_number)"
    single_value = query_begin == query_end
    if single_value:
        rank_sql = "CASE WHEN d.control_number = ? OR d.begin_bates = ? OR d.end_bates = ? THEN 0.0 ELSE 1.0 END"
        rank_params: list[object] = [query_begin, query_begin, query_begin]
        match_clause = (
            "d.control_number = ? OR d.begin_bates = ? OR d.end_bates = ? "
            f"OR ({range_begin_expr} <= ? AND {range_end_expr} >= ?)"
        )
        match_params: list[object] = [query_begin, query_begin, query_begin, query_begin, query_begin]
    else:
        rank_sql = "2.0"
        rank_params = []
        match_clause = f"{range_begin_expr} <= ? AND {range_end_expr} >= ?"
        match_params = [query_end, query_begin]

    cte_sql = f"""
        WITH bates_matches AS (
            SELECT d.*, {rank_sql} AS rank, {range_begin_expr} AS bates_sort_value
            FROM documents d
            WHERE {where_clause}
              AND ({match_clause})
        )
    """
    query_params = [*rank_params, *params, *match_params]
    count_row = connection.execute(
        f"""
        {cte_sql}
        SELECT COUNT(*) AS total_hits
        FROM bates_matches
        """,
        query_params,
    ).fetchone()
    total_hits = int(count_row["total_hits"] or 0) if count_row is not None else 0
    rows = connection.execute(
        f"""
        {cte_sql}
        SELECT *
        FROM bates_matches bm
        ORDER BY {order_by_sql}
        LIMIT ? OFFSET ?
        """,
        [*query_params, limit, offset],
    ).fetchall()
    return {
        "total_hits": total_hits,
        "results": [
            {
                "id": int(row["id"]),
                "rank": float(row["rank"]) if row["rank"] is not None else None,
                "snippet": metadata_snippet(row),
                "bates_sort_key": bates_sort_key(row["bates_sort_value"] or row["control_number"]),
                "row": row,
            }
            for row in rows
        ],
    }


def search_fts_page(
    connection: sqlite3.Connection,
    query: str,
    clauses: list[str],
    params: list[object],
    *,
    limit: int,
    offset: int,
    order_by_sql: str,
) -> dict[str, object]:
    query_value = query.strip()
    if not query_value:
        return {"total_hits": 0, "results": []}

    where_clause = " AND ".join(clauses)
    cte_sql = f"""
        WITH chunk_matches AS (
            SELECT d.id AS document_id, dc.text_content AS snippet_source, bm25(chunks_fts) AS rank, 0 AS source_priority
            FROM chunks_fts
            JOIN document_chunks dc ON dc.id = CAST(chunks_fts.chunk_id AS INTEGER)
            JOIN documents d ON d.id = dc.document_id
            WHERE chunks_fts MATCH ? AND {where_clause}
        ),
        metadata_matches AS (
            SELECT d.id AS document_id, NULL AS snippet_source, bm25(documents_fts) AS rank, 1 AS source_priority
            FROM documents_fts
            JOIN documents d ON d.id = CAST(documents_fts.document_id AS INTEGER)
            WHERE documents_fts MATCH ? AND {where_clause}
        ),
        all_matches AS (
            SELECT * FROM chunk_matches
            UNION ALL
            SELECT * FROM metadata_matches
        ),
        ranked_matches AS (
            SELECT
                document_id,
                snippet_source,
                rank,
                source_priority,
                ROW_NUMBER() OVER (
                    PARTITION BY document_id
                    ORDER BY rank ASC, source_priority ASC, document_id ASC
                ) AS row_number
            FROM all_matches
        ),
        best_matches AS (
            SELECT document_id, snippet_source, rank
            FROM ranked_matches
            WHERE row_number = 1
        )
    """

    def fts_params(fts_query: str) -> list[object]:
        return [fts_query, *params, fts_query, *params]

    count_sql = f"""
        {cte_sql}
        SELECT COUNT(*) AS total_hits
        FROM best_matches
    """
    effective_query = query_value
    try:
        count_row = connection.execute(count_sql, fts_params(effective_query)).fetchone()
    except sqlite3.OperationalError:
        effective_query = f'"{query_value}"'
        count_row = connection.execute(count_sql, fts_params(effective_query)).fetchone()
    total_hits = int(count_row["total_hits"] or 0) if count_row is not None else 0

    rows = connection.execute(
        f"""
        {cte_sql}
        SELECT d.*, bm.rank AS rank, bm.snippet_source AS snippet_source
        FROM best_matches bm
        JOIN documents d ON d.id = bm.document_id
        ORDER BY {order_by_sql}
        LIMIT ? OFFSET ?
        """,
        [*fts_params(effective_query), limit, offset],
    ).fetchall()
    results: list[dict[str, object]] = []
    for row in rows:
        snippet_source = row["snippet_source"] if row["snippet_source"] else metadata_snippet(row)
        results.append(
            {
                "id": int(row["id"]),
                "rank": float(row["rank"]) if row["rank"] is not None else None,
                "snippet": make_snippet(snippet_source, query_value),
                "row": row,
            }
        )
    return {"total_hits": total_hits, "results": results}


def resolve_paged_document_search(
    connection: sqlite3.Connection,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
    *,
    page: int,
    per_page: int,
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
        normalized_sort_field = str(sort_field_def["field_name"])

    offset = max(0, (page - 1) * per_page)
    if is_bates_query:
        if normalized_sort_field is None and order is None:
            selection_page = search_bates_page(
                connection,
                str(bates_query_begin),
                str(bates_query_end),
                clauses,
                params,
                limit=per_page,
                offset=offset,
                order_by_sql=sql_bates_order_by(row_alias="bm", prioritize_rank=True),
            )
        elif normalized_sort_field == "relevance":
            selection_page = search_bates_page(
                connection,
                str(bates_query_begin),
                str(bates_query_end),
                clauses,
                params,
                limit=per_page,
                offset=offset,
                order_by_sql=sql_relevance_order_by(connection, row_alias="bm", rank_expr="bm.rank"),
            )
        else:
            effective_field = normalized_sort_field or "date_created"
            effective_order = (order or "desc").lower()
            selection_page = search_bates_page(
                connection,
                str(bates_query_begin),
                str(bates_query_end),
                clauses,
                params,
                limit=per_page,
                offset=offset,
                order_by_sql=sql_order_by_for_sort_specs(connection, [(effective_field, effective_order)], alias="bm"),
            )
    elif query.strip():
        if normalized_sort_field is None or normalized_sort_field == "relevance":
            order_by_sql = sql_relevance_order_by(connection, row_alias="d", rank_expr="bm.rank")
        else:
            order_by_sql = sql_order_by_for_sort_specs(
                connection,
                [(normalized_sort_field, (order or "desc").lower())],
                alias="d",
            )
        selection_page = search_fts_page(
            connection,
            query,
            clauses,
            params,
            limit=per_page,
            offset=offset,
            order_by_sql=order_by_sql,
        )
    else:
        selection_page = search_browse_page(
            connection,
            clauses,
            params,
            limit=per_page,
            offset=offset,
            order_by_sql=sql_order_by_for_sort_specs(
                connection,
                [(normalized_sort_field or "date_created", (order or "desc").lower())],
                alias="d",
            ),
        )

    return {
        "query": query,
        "filters": filter_summary,
        "sort": normalized_sort_field or ("bates" if is_bates_query and query.strip() else ("relevance" if query.strip() else "date_created")),
        "order": (order or ("asc" if (is_bates_query or (query.strip() and (sort_field in (None, "relevance")))) else "desc")).lower(),
        "sort_spec": f"{normalized_sort_field or ('bates' if is_bates_query and query.strip() else ('relevance' if query.strip() else 'date_created'))} "
        f"{(order or ('asc' if (is_bates_query or (query.strip() and (sort_field in (None, 'relevance')))) else 'desc')).lower()}",
        "results": selection_page["results"],
        "total_hits": int(selection_page["total_hits"]),
    }


def search(
    root: Path,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
    page: int,
    per_page: int | None,
    raw_columns: str | None = None,
    mode: str = "compose",
    *,
    compact_mode: bool = False,
) -> dict[str, object]:
    if page < 1:
        raise RetrieverError("Page must be >= 1.")
    paths = workspace_paths(root)
    ensure_layout(paths)
    if per_page is None:
        per_page = session_page_size(read_session_state(paths))
    if per_page < 1:
        raise RetrieverError("per-page must be >= 1.")
    per_page = min(per_page, MAX_PAGE_SIZE)
    normalized_mode = normalize_search_mode(mode)
    compact_mode = bool(compact_mode) and normalized_mode == "compose"

    connection = connect_db(paths["db_path"])
    try:
        benchmark_mark("schema_begin")
        apply_schema(connection, root)
        benchmark_mark("schema_done")
        selection = resolve_paged_document_search(
            connection,
            query,
            raw_filters,
            sort_field,
            order,
            page=page,
            per_page=per_page,
        )
        benchmark_mark("query_done", total_hits=int(selection["total_hits"]))
        derived_scope = derive_search_scope(query, raw_filters)
        raw_column_list = parse_display_columns_argument(raw_columns) if raw_columns is not None else None
        display_column_defs, display_warnings, _ = resolve_display_column_definitions(
            connection,
            raw_column_list,
            drop_missing=False,
        )
        occurrence_scope_clauses, occurrence_scope_params = build_occurrence_scope_filters(connection, raw_filters)

        results: list[dict[str, object]] = []
        for match in selection["results"]:
            row = match["row"]
            if compact_mode:
                results.append(
                    {
                        "id": int(match["id"]),
                        "control_number": row["control_number"],
                        "dataset_id": row["dataset_id"],
                        "parent_document_id": row["parent_document_id"],
                        "production_id": row["production_id"],
                        **document_path_payload(paths, connection, row, include_preview_targets=False),
                        "file_name": row["file_name"],
                        "file_type": row["file_type"],
                        "snippet": str(match["snippet"]),
                        "rank": match["rank"],
                        "metadata": {
                            key: row[key]
                            for key in COMPACT_METADATA_FIELDS
                            if key in row.keys() and payload_has_meaningful_value(row[key])
                        },
                        "row": row,
                    }
                )
            else:
                results.append(
                    {
                        "id": int(match["id"]),
                        "control_number": row["control_number"],
                        "conversation_id": row["conversation_id"],
                        "dataset_id": row["dataset_id"],
                        "parent_document_id": row["parent_document_id"],
                        "child_document_kind": row["child_document_kind"],
                        "root_message_key": row["root_message_key"],
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
                            "child_document_kind": row["child_document_kind"],
                            "content_type": row["content_type"],
                            "conversation_id": row["conversation_id"],
                            "custodian": row["custodian"],
                            "dataset_id": row["dataset_id"],
                            "date_created": row["date_created"],
                            "date_modified": row["date_modified"],
                            "end_attachment": row["end_attachment"],
                            "end_bates": row["end_bates"],
                            "page_count": row["page_count"],
                            "participants": row["participants"],
                            "recipients": row["recipients"],
                            "root_message_key": row["root_message_key"],
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

        benchmark_mark("full_matchset_built", page_matches=len(results))
        paged_results = results
        total_hits = int(selection["total_hits"])
        total_pages = max(1, (total_hits + per_page - 1) // per_page)
        paged_rows = [item["row"] for item in paged_results]
        preferred_occurrences = preferred_occurrences_by_document(
            connection,
            [int(row["id"]) for row in paged_rows],
            occurrence_scope_clauses,
            occurrence_scope_params,
        )
        paged_parent_ids = [int(row["id"]) for row in paged_rows if row["parent_document_id"] is None]
        production_names = fetch_production_names(connection, paged_rows)
        dataset_memberships = fetch_document_dataset_memberships(connection, paged_rows)
        attachment_counts: dict[int, int] = {}
        child_document_counts: dict[int, int] = {}
        attachment_summaries: dict[int, list[dict[str, object]]] = {}
        child_document_summaries: dict[int, list[dict[str, object]]] = {}
        if compact_mode:
            attachment_counts = fetch_attachment_counts(connection, paged_parent_ids)
            child_document_counts = fetch_child_document_counts(connection, paged_parent_ids)
        else:
            attachment_summaries = fetch_attachment_summaries(
                connection,
                paths,
                paged_parent_ids,
            )
            child_document_summaries = fetch_child_document_summaries(
                connection,
                paths,
                paged_parent_ids,
            )
        parent_summaries = fetch_parent_summaries(
            connection,
            [row for row in paged_rows if row["parent_document_id"] is not None],
        )
        for item in paged_results:
            row = item["row"]
            occurrence_row = preferred_occurrences.get(int(row["id"]))
            source_row = occurrence_row or row
            path_payload = document_path_payload(
                paths,
                connection,
                row,
                occurrence_row=occurrence_row,
                include_preview_targets=not compact_mode,
            )
            item["rel_path"] = path_payload["rel_path"]
            item["abs_path"] = path_payload["abs_path"]
            item["preview_rel_path"] = path_payload["preview_rel_path"]
            item["preview_abs_path"] = path_payload["preview_abs_path"]
            item["preview_file_rel_path"] = path_payload["preview_file_rel_path"]
            item["preview_file_abs_path"] = path_payload["preview_file_abs_path"]
            item["preview_target_fragment"] = path_payload["preview_target_fragment"]
            if not compact_mode:
                item["preview_targets"] = path_payload["preview_targets"]
            item["file_name"] = source_row["file_name"]
            item["file_type"] = source_row["file_type"]
            if compact_mode:
                item["metadata"]["custodian"] = source_row["custodian"]
            else:
                item["source_kind"] = source_row["source_kind"]
                item["source_rel_path"] = source_row["source_rel_path"]
                item["source_item_id"] = source_row["source_item_id"]
                item["source_folder_path"] = source_row["source_folder_path"]
                item["production_id"] = source_row["production_id"]
                item["metadata"]["begin_attachment"] = source_row["begin_attachment"]
                item["metadata"]["begin_bates"] = source_row["begin_bates"]
                item["metadata"]["custodian"] = source_row["custodian"]
                item["metadata"]["end_attachment"] = source_row["end_attachment"]
                item["metadata"]["end_bates"] = source_row["end_bates"]
                item["metadata"]["source_kind"] = source_row["source_kind"]
                item["metadata"]["source_rel_path"] = source_row["source_rel_path"]
                item["metadata"]["source_item_id"] = source_row["source_item_id"]
                item["metadata"]["source_folder_path"] = source_row["source_folder_path"]
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
            if source_row["production_id"] is not None:
                item["production_name"] = production_names.get(int(source_row["production_id"]))
                item["metadata"]["production_name"] = production_names.get(int(source_row["production_id"]))
            if row["parent_document_id"] is None:
                if compact_mode:
                    attachment_count = attachment_counts.get(int(row["id"]), 0)
                    if attachment_count > 0:
                        item["attachment_count"] = attachment_count
                    child_document_count = child_document_counts.get(int(row["id"]), 0)
                    if child_document_count > 0:
                        item["child_document_count"] = child_document_count
                else:
                    attachments = attachment_summaries.get(int(row["id"]), [])
                    item["attachment_count"] = len(attachments)
                    item["attachments"] = attachments
                    child_documents = child_document_summaries.get(int(row["id"]), [])
                    item["child_document_count"] = len(child_documents)
                    item["child_documents"] = child_documents
            else:
                item["parent"] = parent_summaries.get(int(row["parent_document_id"]))
            item["display_values"] = build_search_result_display_values(row, item, display_column_defs)
            item.pop("bates_sort_key", None)
            item.pop("row", None)

        benchmark_mark("page_enriched", page_size=len(paged_results), total_hits=total_hits)
        payload = {
            "query": selection["query"],
            "filters": selection["filters"],
            "sort": selection["sort"],
            "order": selection["order"],
            "sort_spec": selection["sort_spec"],
            "page": page,
            "per_page": per_page,
            "total_hits": total_hits,
            "total_pages": total_pages,
            "results": paged_results,
            "scope": derived_scope,
            "display": build_display_payload(display_column_defs, per_page),
            "header": build_search_header_payload(derived_scope, {
                "sort": selection["sort"],
                "order": selection["order"],
                "sort_spec": selection["sort_spec"],
                "page": page,
                "per_page": per_page,
                "total_hits": total_hits,
                "total_pages": total_pages,
            }),
        }
        if display_warnings:
            payload["warnings"] = display_warnings
        payload["rendered_markdown"] = render_search_markdown(payload, display_column_defs)
        if normalized_mode == "view":
            explicit_sort_specs = None
            if sort_field:
                explicit_sort_specs = [(str(selection["sort"]), str(selection["order"]))]
            persist_direct_view_search_result(
                root,
                payload,
                display_column_defs,
                sort_specs=explicit_sort_specs,
            )
        return payload
    finally:
        connection.close()


def search_docs(
    root: Path,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
    page: int,
    per_page: int | None,
    raw_columns: str | None = None,
    mode: str = "compose",
    *,
    compact_mode: bool = False,
) -> dict[str, object]:
    return search(
        root,
        query,
        raw_filters,
        sort_field,
        order,
        page,
        per_page,
        raw_columns,
        mode,
        compact_mode=compact_mode,
    )


def format_scope_bates_value(bates_scope: object) -> str:
    if not isinstance(bates_scope, dict):
        return ""
    begin = normalize_inline_whitespace(str(bates_scope.get("begin") or ""))
    end = normalize_inline_whitespace(str(bates_scope.get("end") or ""))
    if not begin or not end:
        return ""
    return begin if begin == end else f"{begin}-{end}"


def truncate_scope_header_value(value: object, *, max_chars: int = 200) -> str:
    normalized = normalize_inline_whitespace(str(value or ""))
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 1] + "…"


def format_scope_header(scope: dict[str, object]) -> str:
    parts: list[str] = []
    if isinstance(scope.get("keyword"), str) and scope["keyword"].strip():
        parts.append(f"keyword={scope['keyword']!r}")
    bates_value = format_scope_bates_value(scope.get("bates"))
    if bates_value:
        parts.append(f"bates={bates_value}")
    if isinstance(scope.get("filter"), str) and scope["filter"].strip():
        parts.append(f"filter={truncate_scope_header_value(scope['filter'])}")
    dataset_entries = coerce_scope_dataset_entries(scope.get("dataset"))
    if dataset_entries:
        dataset_names = ", ".join(entry["name"] for entry in dataset_entries)
        parts.append(f"dataset={truncate_scope_header_value(dataset_names)}")
    if scope.get("from_run_id") is not None:
        parts.append(f"from_run_id={scope['from_run_id']}")
    return "Scope: (none)" if not parts else "Scope: " + ", ".join(parts)


def derive_search_scope(query: str, raw_filters: object | None) -> dict[str, object]:
    scope: dict[str, object] = {}
    bates_begin, bates_end = parse_bates_query(query)
    if bates_begin is not None and bates_end is not None:
        scope["bates"] = {"begin": bates_begin, "end": bates_end}
    elif query.strip():
        scope["keyword"] = query
    if raw_filters:
        if uses_legacy_tuple_filters(raw_filters):
            parsed_filters = parse_filter_args(raw_filters)  # type: ignore[arg-type]
            comparator_map = {
                "eq": "=",
                "neq": "!=",
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
                "contains": "LIKE",
                "is-null": "IS NULL",
                "not-null": "IS NOT NULL",
            }
            rendered_parts: list[str] = []
            for parsed_filter in parsed_filters:
                operator = str(parsed_filter["operator"])
                field_name = str(parsed_filter["field_name"])
                value = parsed_filter["value"]
                if operator in {"is-null", "not-null"}:
                    rendered_parts.append(f"{field_name} {comparator_map[operator]}")
                elif operator == "contains":
                    rendered_parts.append(f"{field_name} LIKE '%{value}%'")
                else:
                    rendered_parts.append(f"{field_name} {comparator_map[operator]} {value!r}")
            if rendered_parts:
                scope["filter"] = " AND ".join(rendered_parts)
        else:
            expressions = normalize_sql_filter_expressions(raw_filters)
            if expressions:
                scope["filter"] = " AND ".join(f"({expression})" for expression in expressions)
    return scope


def build_search_header_payload(scope: dict[str, object], payload: dict[str, object]) -> dict[str, str]:
    total_hits = int(payload.get("total_hits") or 0)
    page = int(payload.get("page") or 1)
    per_page = int(payload.get("per_page") or DEFAULT_PAGE_SIZE)
    start_index = 0 if total_hits == 0 else ((page - 1) * per_page) + 1
    end_index = 0 if total_hits == 0 else min(total_hits, page * per_page)
    sort_summary = str(payload.get("sort_spec") or f"{payload.get('sort')} {payload.get('order')}")
    return {
        "scope": format_scope_header(scope),
        "sort": f"Sort: {sort_summary}",
        "page": f"Page: {page} of {payload.get('total_pages')}  (docs {start_index}-{end_index} of {total_hits})",
    }


def default_display_columns() -> list[str]:
    return list(DEFAULT_DISPLAY_COLUMNS)


def normalize_search_mode(mode: object | None) -> str:
    normalized = normalize_inline_whitespace(str(mode or "compose")).lower()
    if not normalized:
        return "compose"
    if normalized not in {"compose", "view"}:
        raise RetrieverError(f"Unknown search mode: {mode!r}. Expected 'compose' or 'view'.")
    return normalized


def session_display_state(session_state: dict[str, object]) -> dict[str, object]:
    display = session_state.get("display")
    return display if isinstance(display, dict) else {}


def displayable_field_names(connection: sqlite3.Connection) -> list[str]:
    names: set[str] = set()
    for field_name in known_logical_field_names(connection):
        field_def = resolve_field_definition(connection, field_name)
        if str(field_def.get("displayable") or "").lower() == "true":
            names.add(str(field_def["field_name"]))
    return sorted(names)


def display_field_suggestions(connection: sqlite3.Connection, field_name: str) -> list[str]:
    return difflib.get_close_matches(field_name, displayable_field_names(connection), n=3, cutoff=0.45)


def displayable_field_examples(connection: sqlite3.Connection, *, limit: int = 12) -> str:
    return ", ".join(displayable_field_names(connection)[:limit])


def default_display_column_definitions(connection: sqlite3.Connection) -> list[dict[str, str]]:
    definitions: list[dict[str, str]] = []
    for column_name in default_display_columns():
        field_def = resolve_field_definition(connection, column_name)
        definitions.append(
            {
                "name": str(field_def["field_name"]),
                "type": str(field_def["field_type"]),
                "source": str(field_def["source"]),
            }
        )
    return definitions


def resolve_display_column_definitions(
    connection: sqlite3.Connection,
    raw_columns: object,
    *,
    drop_missing: bool,
) -> tuple[list[dict[str, str]], list[str], bool]:
    raw_values = raw_columns if isinstance(raw_columns, list) and raw_columns else default_display_columns()
    warnings: list[str] = []
    changed = False
    resolved_columns: list[dict[str, str]] = []
    seen_names: set[str] = set()
    for raw_value in raw_values:
        normalized_name = normalize_inline_whitespace(str(raw_value or ""))
        if not normalized_name:
            changed = True
            continue
        try:
            field_def = resolve_field_definition(connection, normalized_name)
        except RetrieverError as exc:
            if drop_missing:
                warnings.append(
                    f"Column '{normalized_name}' no longer exists and has been removed from your display preferences."
                )
                changed = True
                continue
            suggestions = display_field_suggestions(connection, normalized_name)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise RetrieverError(f"Unknown column: {normalized_name}.{suggestion_text}") from exc
        canonical_name = str(field_def["field_name"])
        if str(field_def.get("displayable") or "").lower() != "true":
            if drop_missing:
                warnings.append(
                    f"Column '{canonical_name}' is no longer displayable and has been removed from your display preferences."
                )
                changed = True
                continue
            raise RetrieverError(
                f"Field '{canonical_name}' is filter-only and cannot be displayed. "
                f"Displayable fields include: {displayable_field_examples(connection)}."
            )
        if canonical_name in seen_names:
            changed = True
            continue
        if canonical_name != normalized_name:
            changed = True
        seen_names.add(canonical_name)
        resolved_columns.append(
            {
                "name": canonical_name,
                "type": str(field_def["field_type"]),
                "source": str(field_def["source"]),
            }
        )
    if not resolved_columns:
        return default_display_column_definitions(connection), warnings, True
    return resolved_columns, warnings, changed


def display_column_names(column_defs: list[dict[str, str]]) -> list[str]:
    return [column_def["name"] for column_def in column_defs]


def build_display_payload(column_defs: list[dict[str, str]], page_size: int) -> dict[str, object]:
    return {
        "columns": display_column_names(column_defs),
        "page_size": page_size,
    }


def persist_display_columns(
    paths: dict[str, Path],
    session_state: dict[str, object],
    column_defs: list[dict[str, str]],
) -> dict[str, object]:
    display_state = session_display_state(session_state)
    column_names = display_column_names(column_defs)
    if column_names == default_display_columns():
        display_state.pop("columns", None)
    else:
        display_state["columns"] = column_names
    session_state["display"] = coerce_display_payload(display_state)
    return persist_session_state(paths, session_state)


def persist_display_preferences(
    paths: dict[str, Path],
    session_state: dict[str, object],
    column_defs: list[dict[str, str]],
    page_size: int,
) -> dict[str, object]:
    display_state = session_display_state(session_state)
    column_names = display_column_names(column_defs)
    if column_names == default_display_columns():
        display_state.pop("columns", None)
    else:
        display_state["columns"] = column_names
    if page_size == DEFAULT_PAGE_SIZE:
        display_state.pop("page_size", None)
    else:
        display_state["page_size"] = page_size
    session_state["display"] = coerce_display_payload(display_state)
    return persist_session_state(paths, session_state)


def resolve_session_display_columns(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    session_state: dict[str, object],
) -> tuple[list[dict[str, str]], list[str], dict[str, object]]:
    configured_columns = session_display_state(session_state).get("columns")
    column_defs, warnings, changed = resolve_display_column_definitions(
        connection,
        configured_columns,
        drop_missing=True,
    )
    if changed:
        session_state = persist_display_columns(paths, session_state, column_defs)
    return column_defs, warnings, session_state


def parse_display_columns_argument(raw_text: str) -> list[str]:
    column_names = split_quoted_comma_values(raw_text)
    if not column_names:
        raise RetrieverError("Column list cannot be empty.")
    return column_names


def parse_page_size_value(raw_value: str) -> int:
    try:
        page_size = int(normalize_inline_whitespace(raw_value))
    except ValueError as exc:
        raise RetrieverError("Usage: /page-size <N>") from exc
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise RetrieverError(f"Page size must be between 1 and {MAX_PAGE_SIZE}.")
    return page_size


def best_result_title(row: sqlite3.Row) -> str | None:
    for candidate in (row["title"], row["subject"], row["file_name"]):
        normalized = normalize_inline_whitespace(str(candidate or ""))
        if normalized:
            return normalized
    return None


def search_result_display_value(
    row: sqlite3.Row,
    item: dict[str, object],
    field_name: str,
    field_type: str,
) -> object:
    if field_name == "title":
        return best_result_title(row)
    if field_name == "dataset_name":
        dataset_names = item.get("dataset_names")
        if isinstance(dataset_names, list):
            normalized_names = [normalize_inline_whitespace(str(value)) for value in dataset_names if normalize_inline_whitespace(str(value))]
            return ", ".join(normalized_names) or None
        value = item.get("dataset_name")
        return normalize_inline_whitespace(str(value)) or None
    if field_name == "production_name":
        value = item.get("production_name")
        return normalize_inline_whitespace(str(value)) or None
    if field_name == "is_attachment":
        return "Yes" if row["parent_document_id"] is not None else "No"

    if field_name in item and field_name not in {"metadata", "display_values"}:
        value = item.get(field_name)
    elif field_name in row.keys():
        value = row[field_name]
    else:
        metadata = item.get("metadata")
        value = metadata.get(field_name) if isinstance(metadata, dict) else None

    if field_type == "boolean":
        if value in (None, ""):
            return None
        return "Yes" if bool(value) else "No"
    if isinstance(value, list):
        normalized_values = [normalize_inline_whitespace(str(entry)) for entry in value if normalize_inline_whitespace(str(entry))]
        return ", ".join(normalized_values) or None
    if isinstance(value, str):
        return value or None
    return value


def build_search_result_display_values(
    row: sqlite3.Row,
    item: dict[str, object],
    column_defs: list[dict[str, str]],
) -> dict[str, object]:
    return {
        column_def["name"]: search_result_display_value(row, item, column_def["name"], column_def["type"])
        for column_def in column_defs
    }


def best_summary_title(item: dict[str, object]) -> str | None:
    for candidate in (
        item.get("title"),
        item.get("subject"),
        item.get("file_name"),
        item.get("control_number"),
    ):
        normalized = normalize_inline_whitespace(str(candidate or ""))
        if normalized:
            return normalized
    return None


def summary_display_value(item: dict[str, object], field_name: str, field_type: str) -> object:
    if field_name == "title":
        return best_summary_title(item)
    if field_name == "dataset_name":
        dataset_names = item.get("dataset_names")
        if isinstance(dataset_names, list):
            normalized_names = [normalize_inline_whitespace(str(value)) for value in dataset_names if normalize_inline_whitespace(str(value))]
            return ", ".join(normalized_names) or None
        return normalize_inline_whitespace(str(item.get("dataset_name") or "")) or None
    if field_name == "production_name":
        return normalize_inline_whitespace(str(item.get("production_name") or "")) or None
    if field_name == "is_attachment":
        if item.get("parent_document_id") is not None:
            return "Yes"
        child_kind = normalize_inline_whitespace(str(item.get("child_document_kind") or ""))
        return "Yes" if child_kind == CHILD_DOCUMENT_KIND_ATTACHMENT else "No"
    if field_name in item:
        value = item.get(field_name)
    else:
        metadata = item.get("metadata")
        value = metadata.get(field_name) if isinstance(metadata, dict) else None
    if field_type == "boolean":
        if value in (None, ""):
            return None
        return "Yes" if bool(value) else "No"
    if isinstance(value, list):
        normalized_values = [normalize_inline_whitespace(str(entry)) for entry in value if normalize_inline_whitespace(str(entry))]
        return ", ".join(normalized_values) or None
    if isinstance(value, str):
        return value or None
    return value


def markdown_table_cell_text(value: object) -> str:
    normalized = normalize_inline_whitespace(str(value or ""))
    if not normalized:
        return ""
    return normalized.replace("\\", "\\\\").replace("|", "\\|")


def markdown_link_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def format_search_markdown_date(value: object) -> str:
    parsed = parse_utc_timestamp(value)
    if parsed is None:
        return markdown_table_cell_text(value)
    return parsed.strftime("%Y-%m-%d %H:%M")


def markdown_search_target(item: dict[str, object]) -> str | None:
    for key in ("preview_abs_path", "abs_path"):
        normalized = normalize_inline_whitespace(str(item.get(key) or ""))
        if not normalized:
            continue
        if normalized.startswith("computer://"):
            return normalized
        return f"computer://{normalized}"
    return None


def summarize_child_content_type(item: dict[str, object], *, attachment: bool) -> str | None:
    if attachment:
        return "Unrecognized"
    child_kind = normalize_inline_whitespace(str(item.get("child_document_kind") or ""))
    if child_kind:
        return child_kind.replace("_", " ").title()
    return None


def render_search_markdown_cell(
    item: dict[str, object],
    column_def: dict[str, str],
    *,
    child_prefix: str = "",
    child_content_type: str | None = None,
    standalone_child_parent: str | None = None,
) -> str:
    column_name = str(column_def["name"])
    field_type = str(column_def["type"])
    display_values = item.get("display_values")
    if isinstance(display_values, dict) and column_name in display_values and not child_prefix:
        value = display_values.get(column_name)
    else:
        value = summary_display_value(item, column_name, field_type)
    if column_name == "content_type" and child_content_type and not value:
        value = child_content_type
    if column_name in {"date_created", "date_modified"}:
        return format_search_markdown_date(value)
    if column_name == "title":
        title_text = normalize_inline_whitespace(str(value or best_summary_title(item) or "Untitled"))
        if child_prefix:
            title_text = f"{child_prefix}{title_text}"
        if standalone_child_parent:
            title_text = f"{title_text} ({standalone_child_parent})"
        target = markdown_search_target(item)
        if target:
            return f"[{markdown_link_text(title_text)}]({target})"
        return markdown_table_cell_text(title_text)
    return markdown_table_cell_text(value)


def render_search_markdown_row(
    item: dict[str, object],
    column_defs: list[dict[str, str]],
    *,
    child_prefix: str = "",
    child_content_type: str | None = None,
    standalone_child_parent: str | None = None,
) -> str:
    cells = [
        render_search_markdown_cell(
            item,
            column_def,
            child_prefix=child_prefix,
            child_content_type=child_content_type,
            standalone_child_parent=standalone_child_parent,
        )
        for column_def in column_defs
    ]
    return "| " + " | ".join(cells) + " |"


def render_search_markdown(payload: dict[str, object], column_defs: list[dict[str, str]]) -> str:
    lines: list[str] = []
    header = payload.get("header")
    if isinstance(header, dict):
        for key in ("scope", "sort", "page"):
            value = header.get(key)
            if payload_has_meaningful_value(value):
                lines.append(str(value))

    column_names = [str(column_def["name"]) for column_def in column_defs]
    lines.append("")
    lines.append("| " + " | ".join(column_names) + " |")
    lines.append("|" + "|".join("---" for _ in column_names) + "|")

    results = payload.get("results")
    if isinstance(results, list):
        for raw_item in results:
            if not isinstance(raw_item, dict):
                continue
            parent_context = None
            parent = raw_item.get("parent")
            if isinstance(parent, dict):
                parent_title = best_summary_title(parent)
                if parent_title:
                    parent_context = f"parent: {parent_title}"
            child_kind = normalize_inline_whitespace(str(raw_item.get("child_document_kind") or ""))
            lines.append(
                render_search_markdown_row(
                    raw_item,
                    column_defs,
                    child_prefix="↳ " if parent_context else "",
                    child_content_type=summarize_child_content_type(
                        raw_item,
                        attachment=child_kind == CHILD_DOCUMENT_KIND_ATTACHMENT,
                    ),
                    standalone_child_parent=parent_context,
                )
            )
            attachments = raw_item.get("attachments")
            if isinstance(attachments, list):
                for attachment in attachments:
                    if isinstance(attachment, dict):
                        lines.append(
                            render_search_markdown_row(
                                attachment,
                                column_defs,
                                child_prefix="↳ ",
                                child_content_type=summarize_child_content_type(attachment, attachment=True),
                            )
                        )
            child_documents = raw_item.get("child_documents")
            if isinstance(child_documents, list):
                for child in child_documents:
                    if isinstance(child, dict):
                        lines.append(
                            render_search_markdown_row(
                                child,
                                column_defs,
                                child_prefix="↳ ",
                                child_content_type=summarize_child_content_type(child, attachment=False),
                            )
                        )

    total_hits = int(payload.get("total_hits") or 0)
    page = int(payload.get("page") or 1)
    per_page = int(payload.get("per_page") or DEFAULT_PAGE_SIZE)
    start_index = 0 if total_hits == 0 else ((page - 1) * per_page) + 1
    end_index = 0 if total_hits == 0 else min(total_hits, page * per_page)
    footer = f"Documents {start_index}\u2013{end_index} of {total_hits}."
    if page < int(payload.get("total_pages") or 1):
        footer += " Ask for the next page to see more."
    lines.extend(["", footer])
    return "\n".join(lines)


def scope_dataset_name_suggestions(connection: sqlite3.Connection, dataset_name: str) -> list[str]:
    dataset_names = [summary["dataset_name"] for summary in list_dataset_summaries(connection)]
    return difflib.get_close_matches(normalize_inline_whitespace(dataset_name), dataset_names, n=3, cutoff=0.45)


def resolve_scope_dataset_entries(
    connection: sqlite3.Connection,
    dataset_entries: object,
) -> list[dict[str, object]]:
    normalized_entries = coerce_scope_dataset_entries(dataset_entries)
    if len(normalized_entries) > MAX_SCOPE_DATASETS:
        raise RetrieverError(f"Scope datasets are capped at {MAX_SCOPE_DATASETS} entries.")
    resolved_entries: list[dict[str, object]] = []
    seen_ids: set[int] = set()
    for entry in normalized_entries:
        dataset_id = int(entry["id"])
        row = get_dataset_row_by_id(connection, dataset_id)
        if row is None:
            raise RetrieverError(
                f"Dataset id {dataset_id} ({entry['name']!r}) no longer exists. Clear with /dataset clear or replace with /dataset <other-name>."
            )
        if dataset_id in seen_ids:
            continue
        seen_ids.add(dataset_id)
        resolved_entries.append({"id": dataset_id, "name": str(row["dataset_name"])})
    return resolved_entries


def resolve_scope_from_run_id(connection: sqlite3.Connection, from_run_id: object) -> int | None:
    if from_run_id in (None, ""):
        return None
    try:
        normalized_run_id = int(from_run_id)
    except (TypeError, ValueError) as exc:
        raise RetrieverError(f"Invalid run id: {from_run_id!r}") from exc
    row = connection.execute("SELECT id FROM runs WHERE id = ?", (normalized_run_id,)).fetchone()
    if row is None:
        raise RetrieverError(
            f"Run {normalized_run_id} referenced by scope.from_run_id no longer exists. Clear with /from-run clear."
        )
    return normalized_run_id


def build_scope_search_filters(
    connection: sqlite3.Connection,
    raw_scope: object,
) -> tuple[dict[str, object], list[str], list[object], list[object]]:
    scope = coerce_scope_payload(raw_scope)
    clauses = base_document_search_clauses()
    params: list[object] = []
    filter_summary: list[object] = []

    filter_expression = normalize_inline_whitespace(str(scope.get("filter") or ""))
    if filter_expression:
        clause, clause_params = compile_sql_filter_expression(connection, filter_expression)
        clauses.append(f"({clause})")
        params.extend(clause_params)
        filter_summary.append(filter_expression)

    dataset_entries = resolve_scope_dataset_entries(connection, scope.get("dataset"))
    if dataset_entries:
        placeholders = ", ".join("?" for _ in dataset_entries)
        clauses.append(
            "EXISTS (SELECT 1 FROM dataset_documents dd_scope "
            f"WHERE dd_scope.document_id = d.id AND dd_scope.dataset_id IN ({placeholders}))"
        )
        params.extend(int(entry["id"]) for entry in dataset_entries)
        scope["dataset"] = dataset_entries

    from_run_id = resolve_scope_from_run_id(connection, scope.get("from_run_id"))
    if from_run_id is not None:
        clauses.append(
            "EXISTS (SELECT 1 FROM run_snapshot_documents rsd "
            "WHERE rsd.document_id = d.id AND rsd.run_id = ?)"
        )
        params.append(from_run_id)
        scope["from_run_id"] = from_run_id

    return scope, clauses, params, filter_summary


def stable_sort_results_by_field(
    results: list[dict[str, object]],
    field_name: str,
    order: str,
) -> list[dict[str, object]]:
    reverse = order.lower() == "desc"
    field_type = BUILTIN_FIELD_TYPES.get(field_name)
    non_null_items: list[tuple[object, dict[str, object]]] = []
    null_items: list[dict[str, object]] = []
    for item in results:
        row = item["row"]
        raw_value = row[field_name]
        if raw_value is None:
            null_items.append(item)
            continue
        normalized_value: object = raw_value
        if field_type == "date":
            parsed_value = parse_utc_timestamp(raw_value)
            if parsed_value is None:
                null_items.append(item)
                continue
            normalized_value = parsed_value
        elif isinstance(raw_value, str):
            normalized_value = raw_value.lower()
        non_null_items.append((normalized_value, item))
    non_null_items.sort(key=lambda pair: pair[0], reverse=reverse)
    return [item for _, item in non_null_items] + null_items


def coerce_sort_specs(raw_value: object) -> list[tuple[str, str]]:
    if not isinstance(raw_value, list):
        return []
    normalized_specs: list[tuple[str, str]] = []
    for item in raw_value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        field_name = normalize_inline_whitespace(str(item[0] or ""))
        direction = normalize_inline_whitespace(str(item[1] or "")).lower()
        if not field_name or direction not in {"asc", "desc"}:
            continue
        normalized_specs.append((field_name, direction))
    return normalized_specs


def serialize_sort_specs(sort_specs: list[tuple[str, str]] | None) -> list[list[str]]:
    if not sort_specs:
        return []
    return [[field_name, direction] for field_name, direction in sort_specs]


def sort_specs_text(sort_specs: list[tuple[str, str]] | None) -> str | None:
    if not sort_specs:
        return None
    return ", ".join(f"{field_name} {direction}" for field_name, direction in sort_specs)


def resolve_sort_field_name(connection: sqlite3.Connection, raw_field_name: str) -> str:
    field_def = resolve_field_definition(connection, raw_field_name)
    if field_def.get("source") == "virtual":
        raise RetrieverError(f"Cannot sort by virtual filter field: {raw_field_name}")
    return str(field_def["field_name"])


def parse_slash_sort_specs(connection: sqlite3.Connection, raw_text: str) -> list[tuple[str, str]]:
    if not normalize_inline_whitespace(raw_text):
        raise RetrieverError("Usage: /sort <column asc|desc[, column asc|desc...]>")
    parts = split_quoted_comma_values(raw_text)
    if not parts:
        raise RetrieverError("Usage: /sort <column asc|desc[, column asc|desc...]>")
    sort_specs: list[tuple[str, str]] = []
    for part in parts:
        tokens = shlex_split_slash_tail(part)
        if len(tokens) != 2:
            raise RetrieverError("Each sort entry must be exactly '<column> <asc|desc>'.")
        field_name = resolve_sort_field_name(connection, tokens[0])
        direction = normalize_inline_whitespace(tokens[1]).lower()
        if direction not in {"asc", "desc"}:
            raise RetrieverError("Sort direction must be 'asc' or 'desc'.")
        sort_specs.append((field_name, direction))
    return sort_specs


def apply_sort_specs(
    results: list[dict[str, object]],
    sort_specs: list[tuple[str, str]] | None,
) -> list[dict[str, object]]:
    if not sort_specs:
        return results
    effective_specs = list(sort_specs)
    if not any(field_name == "id" for field_name, _ in effective_specs):
        effective_specs.append(("id", "asc"))
    sorted_results = list(results)
    for field_name, direction in reversed(effective_specs):
        if field_name == "id":
            sorted_results = sorted(
                sorted_results,
                key=lambda item: int(item["id"]),
                reverse=direction == "desc",
            )
            continue
        sorted_results = stable_sort_results_by_field(sorted_results, field_name, direction)
    return sorted_results


def resolve_scope_document_search(
    connection: sqlite3.Connection,
    raw_scope: object,
    *,
    sort_specs: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    scope, clauses, params, filter_summary = build_scope_search_filters(connection, raw_scope)
    keyword_query = normalize_inline_whitespace(str(scope.get("keyword") or ""))
    bates_scope = scope.get("bates")
    bates_query = format_scope_bates_value(bates_scope)
    bates_begin = normalize_inline_whitespace(str(bates_scope.get("begin") or "")) if isinstance(bates_scope, dict) else ""
    bates_end = normalize_inline_whitespace(str(bates_scope.get("end") or "")) if isinstance(bates_scope, dict) else ""

    bates_matches: dict[int, dict[str, object]] | None = None
    keyword_matches: dict[int, dict[str, object]] | None = None
    browse_matches: dict[int, dict[str, object]] | None = None

    if bates_query:
        bates_matches = search_bates(connection, bates_begin, bates_end, clauses, params)
    if keyword_query:
        keyword_matches = search_fts(connection, keyword_query, clauses, params)
    if bates_matches is None and keyword_matches is None:
        browse_matches = search_browse(connection, clauses, params)

    if bates_matches is not None and keyword_matches is not None:
        matches: dict[int, dict[str, object]] = {}
        for document_id, bates_match in bates_matches.items():
            keyword_match = keyword_matches.get(document_id)
            if keyword_match is None:
                continue
            matches[document_id] = {
                "row": bates_match["row"],
                "rank": keyword_match.get("rank"),
                "snippet": keyword_match.get("snippet") or bates_match["snippet"],
                "bates_sort_key": bates_match.get("bates_sort_key"),
            }
    else:
        matches = bates_matches or keyword_matches or browse_matches or {}

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
    stable_results = sorted(results, key=lambda item: int(item["id"]))
    if sort_specs:
        sorted_results = apply_sort_specs(stable_results, sort_specs)
        sort_name = sort_specs[0][0]
        order_name = sort_specs[0][1]
        sort_spec = sort_specs_text(sort_specs) or f"{sort_name} {order_name}"
    elif bates_query:
        sorted_results = sorted(
            stable_results,
            key=lambda item: item.get("bates_sort_key") or (1, "", 0, ""),
        )
        sort_name = "bates"
        order_name = "asc"
        sort_spec = "bates asc"
    elif keyword_query:
        sorted_results = stable_sort_results_by_field(stable_results, "date_created", "desc")
        sorted_results = sorted(
            sorted_results,
            key=lambda item: (item["rank"] is None, item["rank"]),
        )
        sort_name = "relevance"
        order_name = "asc"
        sort_spec = "relevance asc"
    else:
        sorted_results = stable_sort_results_by_field(stable_results, "date_created", "desc")
        sort_name = "date_created"
        order_name = "desc"
        sort_spec = "date_created desc"

    query_label = keyword_query or bates_query or ""
    return {
        "scope": scope,
        "query": query_label,
        "filters": filter_summary,
        "sort": sort_name,
        "order": order_name,
        "sort_spec": sort_spec,
        "results": sorted_results,
    }


def resolve_paged_scope_document_search(
    connection: sqlite3.Connection,
    raw_scope: object,
    *,
    sort_specs: list[tuple[str, str]] | None = None,
    offset: int,
    per_page: int,
) -> dict[str, object]:
    scope, clauses, params, filter_summary = build_scope_search_filters(connection, raw_scope)
    keyword_query = normalize_inline_whitespace(str(scope.get("keyword") or ""))
    bates_scope = scope.get("bates")
    bates_query = format_scope_bates_value(bates_scope)
    bates_begin = normalize_inline_whitespace(str(bates_scope.get("begin") or "")) if isinstance(bates_scope, dict) else ""
    bates_end = normalize_inline_whitespace(str(bates_scope.get("end") or "")) if isinstance(bates_scope, dict) else ""

    if bates_query and keyword_query:
        legacy_selection = resolve_scope_document_search(connection, scope, sort_specs=sort_specs)
        total_hits = len(legacy_selection["results"])
        paged_results = legacy_selection["results"][offset: offset + per_page]
        return {
            "scope": scope,
            "query": legacy_selection["query"],
            "filters": filter_summary,
            "sort": legacy_selection["sort"],
            "order": legacy_selection["order"],
            "sort_spec": legacy_selection["sort_spec"],
            "results": paged_results,
            "total_hits": total_hits,
        }

    if sort_specs:
        sort_name = sort_specs[0][0]
        order_name = sort_specs[0][1]
        sort_spec = sort_specs_text(sort_specs) or f"{sort_name} {order_name}"
    elif bates_query:
        sort_name = "bates"
        order_name = "asc"
        sort_spec = "bates asc"
    elif keyword_query:
        sort_name = "relevance"
        order_name = "asc"
        sort_spec = "relevance asc"
    else:
        sort_name = "date_created"
        order_name = "desc"
        sort_spec = "date_created desc"

    if bates_query:
        if sort_specs:
            selection_page = search_bates_page(
                connection,
                bates_begin,
                bates_end,
                clauses,
                params,
                limit=per_page,
                offset=offset,
                order_by_sql=sql_order_by_for_sort_specs(connection, sort_specs, alias="bm"),
            )
        else:
            selection_page = search_bates_page(
                connection,
                bates_begin,
                bates_end,
                clauses,
                params,
                limit=per_page,
                offset=offset,
                order_by_sql=sql_bates_order_by(row_alias="bm", prioritize_rank=False),
            )
    elif keyword_query:
        if sort_specs:
            order_by_sql = sql_order_by_for_sort_specs(connection, sort_specs, alias="d")
        else:
            order_by_sql = sql_relevance_order_by(connection, row_alias="d", rank_expr="bm.rank")
        selection_page = search_fts_page(
            connection,
            keyword_query,
            clauses,
            params,
            limit=per_page,
            offset=offset,
            order_by_sql=order_by_sql,
        )
    else:
        if sort_specs:
            order_by_sql = sql_order_by_for_sort_specs(connection, sort_specs, alias="d")
        else:
            order_by_sql = sql_order_by_for_sort_specs(connection, [("date_created", "desc")], alias="d")
        selection_page = search_browse_page(
            connection,
            clauses,
            params,
            limit=per_page,
            offset=offset,
            order_by_sql=order_by_sql,
        )

    query_label = keyword_query or bates_query or ""
    return {
        "scope": scope,
        "query": query_label,
        "filters": filter_summary,
        "sort": sort_name,
        "order": order_name,
        "sort_spec": sort_spec,
        "results": selection_page["results"],
        "total_hits": int(selection_page["total_hits"]),
    }


def search_with_scope(
    root: Path,
    raw_scope: object,
    *,
    page: int = 1,
    per_page: int = DEFAULT_PAGE_SIZE,
    offset: int | None = None,
    sort_specs: list[tuple[str, str]] | None = None,
    display_column_defs: list[dict[str, str]] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, object]:
    if page < 1:
        raise RetrieverError("Page must be >= 1.")
    if per_page < 1:
        raise RetrieverError("per-page must be >= 1.")
    per_page = min(per_page, MAX_PAGE_SIZE)
    if offset is not None and offset < 0:
        raise RetrieverError("Offset must be >= 0.")

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        requested_offset = (page - 1) * per_page if offset is None else offset
        selection = resolve_paged_scope_document_search(
            connection,
            raw_scope,
            sort_specs=sort_specs,
            offset=requested_offset,
            per_page=per_page,
        )
        effective_display_column_defs = display_column_defs or default_display_column_definitions(connection)

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

        total_hits = int(selection["total_hits"])
        total_pages = max(1, (total_hits + per_page - 1) // per_page)
        start = requested_offset
        if total_hits > 0 and start >= total_hits:
            start = (total_pages - 1) * per_page
            selection = resolve_paged_scope_document_search(
                connection,
                raw_scope,
                sort_specs=sort_specs,
                offset=start,
                per_page=per_page,
            )
            results = []
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
        start = max(0, start)
        page = (start // per_page) + 1
        paged_results = results
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
            item["display_values"] = build_search_result_display_values(row, item, effective_display_column_defs)
            item.pop("bates_sort_key", None)
            item.pop("row", None)

        payload = {
            "query": selection["query"],
            "filters": selection["filters"],
            "sort": selection["sort"],
            "order": selection["order"],
            "sort_spec": selection["sort_spec"],
            "page": page,
            "per_page": per_page,
            "offset": start,
            "total_hits": total_hits,
            "total_pages": total_pages,
            "results": paged_results,
            "scope": selection["scope"],
            "display": build_display_payload(effective_display_column_defs, per_page),
        }
        payload["header"] = build_search_header_payload(selection["scope"], payload)
        if warnings:
            payload["warnings"] = warnings
        payload["rendered_markdown"] = render_search_markdown(payload, effective_display_column_defs)
        return payload
    finally:
        connection.close()


def session_page_size(session_state: dict[str, object]) -> int:
    display = session_display_state(session_state)
    if not display:
        return DEFAULT_PAGE_SIZE
    page_size = display.get("page_size")
    if not isinstance(page_size, int) or page_size < 1:
        return DEFAULT_PAGE_SIZE
    return min(page_size, MAX_PAGE_SIZE)


def session_browsing_state(session_state: dict[str, object]) -> dict[str, object]:
    browsing = session_state.get("browsing")
    return browsing if isinstance(browsing, dict) else {}


def session_sort_specs(session_state: dict[str, object]) -> list[tuple[str, str]]:
    return coerce_sort_specs(session_browsing_state(session_state).get("sort"))


def saved_scope_summaries(paths: dict[str, Path]) -> list[dict[str, object]]:
    saved_scopes_state = read_saved_scopes_state(paths)
    scopes = saved_scopes_state.get("scopes")
    if not isinstance(scopes, dict):
        return []
    return [
        {
            "name": str(scope_name),
            "scope": coerce_saved_scope_payload(scope_payload),
        }
        for scope_name, scope_payload in sorted(
            scopes.items(),
            key=lambda item: (normalize_saved_scope_name(str(item[0])), str(item[0])),
        )
    ]


def sortable_field_entries(connection: sqlite3.Connection) -> list[dict[str, object]]:
    seen_names: set[str] = set()
    entries: list[dict[str, object]] = []
    for raw_field_name in known_logical_field_names(connection):
        field_def = resolve_field_definition(connection, raw_field_name)
        if field_def.get("source") == "virtual":
            continue
        canonical_name = str(field_def["field_name"])
        if canonical_name in seen_names:
            continue
        seen_names.add(canonical_name)
        entries.append(
            catalog_field_entry(
                canonical_name,
                str(field_def["field_type"]),
                source=str(field_def["source"]),
                instruction=field_def.get("instruction"),
                displayable=str(field_def.get("displayable") or "").lower() == "true",
            )
        )
    return sorted(entries, key=lambda item: (str(item["name"]).lower(), str(item["name"])))


def displayable_field_entries(connection: sqlite3.Connection) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for field_name in displayable_field_names(connection):
        field_def = resolve_field_definition(connection, field_name)
        entries.append(
            catalog_field_entry(
                str(field_def["field_name"]),
                str(field_def["field_type"]),
                source=str(field_def["source"]),
                instruction=field_def.get("instruction"),
                displayable=True,
            )
        )
    return entries


def active_sort_payload(scope: dict[str, object], session_state: dict[str, object]) -> dict[str, object]:
    sort_specs = session_sort_specs(session_state)
    if sort_specs:
        field_name, direction = sort_specs[0]
        return {
            "sort": field_name,
            "order": direction,
            "sort_spec": sort_specs_text(sort_specs) or f"{field_name} {direction}",
            "sort_source": "override",
            "sort_override": serialize_sort_specs(sort_specs),
        }

    if isinstance(scope.get("bates"), dict):
        return {
            "sort": "bates",
            "order": "asc",
            "sort_spec": "bates asc",
            "sort_source": "default",
        }

    if normalize_inline_whitespace(str(scope.get("keyword") or "")):
        return {
            "sort": "relevance",
            "order": "asc",
            "sort_spec": "relevance asc",
            "sort_source": "default",
        }

    return {
        "sort": "date_created",
        "order": "desc",
        "sort_spec": "date_created desc",
        "sort_source": "default",
    }


def active_page_payload(session_state: dict[str, object]) -> dict[str, int]:
    per_page = session_page_size(session_state)
    browsing = session_browsing_state(session_state)
    offset = int(browsing.get("offset") or 0)
    total_known = int(browsing.get("total_known") or 0)
    total_pages = max(1, (total_known + per_page - 1) // per_page)
    if total_known > 0 and offset >= total_known:
        offset = max(0, (total_pages - 1) * per_page)
    return {
        "page": (offset // per_page) + 1,
        "per_page": per_page,
        "offset": offset,
        "total_known": total_known,
        "total_pages": total_pages,
    }


def parse_slash_command_text(raw_command: str) -> tuple[str, str]:
    command_text = raw_command.strip()
    if not command_text:
        raise RetrieverError("Slash command cannot be empty.")
    if not command_text.startswith("/"):
        raise RetrieverError("Slash commands must begin with '/'.")
    command_body = command_text[1:]
    command_name, _, tail = command_body.partition(" ")
    return command_name, tail.lstrip()


def bates_scope_text(scope: dict[str, object]) -> str | None:
    bates = scope.get("bates")
    if not isinstance(bates, dict):
        return None
    begin = normalize_inline_whitespace(str(bates.get("begin") or ""))
    end = normalize_inline_whitespace(str(bates.get("end") or ""))
    if not begin or not end:
        return None
    return f"{begin}-{end}"


def summarize_scope_inline(scope: dict[str, object]) -> str:
    parts: list[str] = []
    keyword = normalize_inline_whitespace(str(scope.get("keyword") or ""))
    if keyword:
        parts.append(f"keyword={keyword}")
    bates_text = bates_scope_text(scope)
    if bates_text:
        parts.append(f"bates={bates_text}")
    filter_expression = normalize_inline_whitespace(str(scope.get("filter") or ""))
    if filter_expression:
        parts.append(f"filter={filter_expression}")
    dataset_entries = coerce_scope_dataset_entries(scope.get("dataset"))
    if dataset_entries:
        parts.append("dataset=" + ", ".join(str(entry["name"]) for entry in dataset_entries))
    from_run_id = scope.get("from_run_id")
    if from_run_id is not None:
        parts.append(f"from-run={from_run_id}")
    return "; ".join(parts) if parts else "(none)"


def render_slash_read_only_output(raw_command: str, payload: dict[str, object]) -> str | None:
    command_name, normalized_tail = parse_slash_command_text(raw_command)

    if command_name in {"next", "previous"}:
        rendered_markdown = payload.get("rendered_markdown")
        if payload_has_meaningful_value(rendered_markdown):
            return str(rendered_markdown)

    if command_name == "search" and not normalized_tail:
        keyword = normalize_inline_whitespace(str(payload.get("keyword") or ""))
        return f"Search: {keyword}" if keyword else "Search: (none)"

    if command_name == "filter" and not normalized_tail:
        filter_expression = normalize_inline_whitespace(str(payload.get("filter") or ""))
        return f"Filter: {filter_expression}" if filter_expression else "Filter: (none)"

    if command_name == "bates" and not normalized_tail:
        bates = payload.get("bates")
        if isinstance(bates, dict):
            begin = normalize_inline_whitespace(str(bates.get("begin") or ""))
            end = normalize_inline_whitespace(str(bates.get("end") or ""))
            if begin and end:
                return f"Bates: {begin}-{end}"
        return "Bates: (none)"

    if command_name == "from-run" and not normalized_tail:
        from_run_id = payload.get("from_run_id")
        return f"From run: {from_run_id}" if from_run_id is not None else "From run: (none)"

    if command_name == "scope":
        scope_args = shlex_split_slash_tail(normalized_tail) if normalized_tail else []
        if not scope_args:
            scope = coerce_scope_payload(payload.get("scope"))
            lines = ["Scope:"]
            if not scope:
                lines[0] = "Scope: (none)"
            else:
                keyword = normalize_inline_whitespace(str(scope.get("keyword") or ""))
                if keyword:
                    lines.append(f"- keyword: {keyword}")
                bates_text = bates_scope_text(scope)
                if bates_text:
                    lines.append(f"- bates: {bates_text}")
                filter_expression = normalize_inline_whitespace(str(scope.get("filter") or ""))
                if filter_expression:
                    lines.append(f"- filter: {filter_expression}")
                dataset_entries = coerce_scope_dataset_entries(scope.get("dataset"))
                if dataset_entries:
                    lines.append("- dataset: " + ", ".join(str(entry["name"]) for entry in dataset_entries))
                from_run_id = scope.get("from_run_id")
                if from_run_id is not None:
                    lines.append(f"- from-run: {from_run_id}")
            return "\n".join(lines)
        if scope_args == ["list"]:
            saved_scopes = payload.get("saved_scopes")
            if not isinstance(saved_scopes, list) or not saved_scopes:
                return "Saved scopes: (none)"
            lines = ["Saved scopes:"]
            for item in saved_scopes:
                if not isinstance(item, dict):
                    continue
                name = normalize_inline_whitespace(str(item.get("name") or ""))
                if not name:
                    continue
                scope = coerce_scope_payload(item.get("scope"))
                lines.append(f"- {name}: {summarize_scope_inline(scope)}")
            return "\n".join(lines)
        return None

    if command_name == "dataset":
        dataset_args = shlex_split_slash_tail(normalized_tail) if normalized_tail else []
        if not dataset_args:
            dataset_entries = coerce_scope_dataset_entries(payload.get("dataset"))
            if not dataset_entries:
                return "Dataset: (none)"
            return "Dataset: " + ", ".join(str(entry["name"]) for entry in dataset_entries)
        if dataset_args == ["list"]:
            datasets = payload.get("datasets")
            if not isinstance(datasets, list) or not datasets:
                return "Datasets: (none)"
            lines = ["Datasets:"]
            for item in datasets:
                if not isinstance(item, dict):
                    continue
                dataset_name = normalize_inline_whitespace(str(item.get("dataset_name") or ""))
                if not dataset_name:
                    continue
                document_count = int(item.get("document_count") or 0)
                manual_count = int(item.get("manual_document_count") or 0)
                source_count = int(item.get("source_document_count") or 0)
                lines.append(
                    f"- {dataset_name}: {document_count} docs (manual {manual_count}, source {source_count})"
                )
            return "\n".join(lines)
        return None

    if command_name == "sort":
        if not normalized_tail:
            sort_spec = normalize_inline_whitespace(str(payload.get("sort_spec") or ""))
            sort_source = normalize_inline_whitespace(str(payload.get("sort_source") or "")) or "default"
            return f"Sort: {sort_spec} ({sort_source})" if sort_spec else "Sort: (none)"
        if normalized_tail == "list":
            sortable_fields = payload.get("sortable_fields")
            if not isinstance(sortable_fields, list) or not sortable_fields:
                return "Sortable fields: (none)"
            lines = ["Sortable fields:"]
            for item in sortable_fields:
                if not isinstance(item, dict):
                    continue
                field_name = normalize_inline_whitespace(str(item.get("name") or ""))
                if field_name:
                    lines.append(f"- {field_name}")
            return "\n".join(lines)
        return None

    if command_name == "columns":
        if not normalized_tail:
            display = payload.get("display") if isinstance(payload.get("display"), dict) else {}
            columns = display.get("columns") if isinstance(display.get("columns"), list) else []
            page_size = display.get("page_size")
            lines = [
                "Columns: " + ", ".join(str(column) for column in columns)
                if columns
                else "Columns: (none)"
            ]
            if page_size is not None:
                lines.append(f"Page size: {page_size}")
            warnings = payload.get("warnings")
            if isinstance(warnings, list):
                for warning in warnings:
                    warning_text = normalize_inline_whitespace(str(warning or ""))
                    if warning_text:
                        lines.append(f"Warning: {warning_text}")
            return "\n".join(lines)
        if normalized_tail == "list":
            columns = payload.get("columns")
            if not isinstance(columns, list) or not columns:
                return "Displayable columns: (none)"
            lines = ["Displayable columns:"]
            for item in columns:
                if not isinstance(item, dict):
                    continue
                field_name = normalize_inline_whitespace(str(item.get("name") or ""))
                if field_name:
                    lines.append(f"- {field_name}")
            return "\n".join(lines)
        return None

    if command_name == "page-size" and not normalized_tail:
        return f"Page size: {int(payload.get('page_size') or 0)}"

    if command_name == "page" and not normalized_tail:
        page = int(payload.get("page") or 1)
        per_page = int(payload.get("per_page") or 0)
        offset = int(payload.get("offset") or 0)
        total_known = int(payload.get("total_known") or 0)
        total_pages = int(payload.get("total_pages") or 1)
        if total_known > 0:
            first_doc = offset + 1
            last_doc = min(offset + per_page, total_known)
        else:
            first_doc = 0
            last_doc = 0
        return f"Page: {page} of {total_pages} (docs {first_doc}-{last_doc} of {total_known})"

    return None


def clear_session_browsing(session_state: dict[str, object]) -> dict[str, object]:
    session_state["browsing"] = {}
    return session_state


def persist_session_state(paths: dict[str, Path], session_state: dict[str, object]) -> dict[str, object]:
    write_session_state(paths, session_state)
    return read_session_state(paths)


def persist_scope_to_session(
    paths: dict[str, Path],
    scope: dict[str, object],
    *,
    reset_browsing: bool = True,
) -> dict[str, object]:
    session_state = read_session_state(paths)
    normalized_scope = coerce_scope_payload(scope)
    if normalized_scope:
        normalized_scope["set_at"] = utc_now()
    session_state["scope"] = normalized_scope
    if reset_browsing:
        clear_session_browsing(session_state)
    return persist_session_state(paths, session_state)


def find_saved_scope_name(saved_scopes_state: dict[str, object], requested_name: str) -> str | None:
    scopes = saved_scopes_state.get("scopes")
    if not isinstance(scopes, dict):
        return None
    normalized_name = normalize_saved_scope_name(requested_name)
    for existing_name in scopes:
        if normalize_saved_scope_name(str(existing_name)) == normalized_name:
            return str(existing_name)
    return None


def save_named_scope(
    paths: dict[str, Path],
    scope_name: str,
    scope: dict[str, object],
) -> dict[str, object]:
    normalized_scope_name = normalize_inline_whitespace(scope_name)
    if not normalized_scope_name:
        raise RetrieverError("Saved scope name cannot be empty.")
    saved_scopes_state = read_saved_scopes_state(paths)
    scopes = saved_scopes_state.setdefault("scopes", {})
    assert isinstance(scopes, dict)
    existing_name = find_saved_scope_name(saved_scopes_state, normalized_scope_name)
    if existing_name is None and len(scopes) >= MAX_SAVED_SCOPES:
        raise RetrieverError(f"Saved scopes are capped at {MAX_SAVED_SCOPES} per workspace.")
    if existing_name is not None and existing_name != normalized_scope_name:
        scopes.pop(existing_name, None)
    payload = coerce_scope_payload(scope)
    payload.pop("set_at", None)
    if payload:
        payload["saved_at"] = utc_now()
    scopes[normalized_scope_name] = payload
    write_saved_scopes_state(paths, saved_scopes_state)
    return {"status": "ok", "name": normalized_scope_name, "scope": payload}


def parse_bates_scope_input(raw_value: str) -> dict[str, str]:
    begin, end = parse_bates_query(raw_value)
    if begin is None or end is None:
        raise RetrieverError("Expected a Bates token or Bates range.")
    begin_parsed = parse_bates_identifier(begin)
    end_parsed = parse_bates_identifier(end)
    if not bates_range_compatible(begin_parsed, end_parsed):
        raise RetrieverError("Mixed-prefix Bates ranges are not supported; use two separate queries.")
    return {"begin": begin, "end": end}


def intersect_bates_scopes(current_bates: object, incoming_bates: dict[str, str]) -> dict[str, str]:
    if not isinstance(current_bates, dict):
        return incoming_bates
    current_begin = parse_bates_identifier(current_bates.get("begin"))
    current_end = parse_bates_identifier(current_bates.get("end"))
    next_begin = parse_bates_identifier(incoming_bates.get("begin"))
    next_end = parse_bates_identifier(incoming_bates.get("end"))
    if not all((current_begin, current_end, next_begin, next_end)):
        return incoming_bates
    if not bates_range_compatible(current_begin, current_end) or not bates_range_compatible(next_begin, next_end):
        return incoming_bates
    if not bates_range_compatible(current_begin, next_begin):
        raise RetrieverError("Cannot intersect Bates ranges from different series.")
    overlap_begin = max(int(current_begin["number"]), int(next_begin["number"]))
    overlap_end = min(int(current_end["number"]), int(next_end["number"]))
    if overlap_begin > overlap_end:
        raise RetrieverError("The requested Bates range does not overlap the current Bates scope.")
    prefix = str(current_begin["prefix"])
    width = int(current_begin["width"])
    return {
        "begin": f"{prefix}{overlap_begin:0{width}d}",
        "end": f"{prefix}{overlap_end:0{width}d}",
    }


def refresh_dataset_name_refs_in_scope(scope: dict[str, object], dataset_id: int, dataset_name: str) -> dict[str, object]:
    dataset_entries = coerce_scope_dataset_entries(scope.get("dataset"))
    if not dataset_entries:
        return scope
    updated_entries: list[dict[str, object]] = []
    for entry in dataset_entries:
        if int(entry["id"]) == dataset_id:
            updated_entries.append({"id": dataset_id, "name": dataset_name})
        else:
            updated_entries.append(entry)
    scope["dataset"] = updated_entries
    return scope


def refresh_dataset_name_refs_in_saved_scopes(paths: dict[str, Path], dataset_id: int, dataset_name: str) -> None:
    saved_scopes_state = read_saved_scopes_state(paths)
    scopes = saved_scopes_state.get("scopes")
    if not isinstance(scopes, dict):
        return
    changed = False
    for scope_payload in scopes.values():
        if not isinstance(scope_payload, dict):
            continue
        before = json.dumps(scope_payload, ensure_ascii=True, sort_keys=True)
        refresh_dataset_name_refs_in_scope(scope_payload, dataset_id, dataset_name)
        after = json.dumps(scope_payload, ensure_ascii=True, sort_keys=True)
        if before != after:
            changed = True
    if changed:
        write_saved_scopes_state(paths, saved_scopes_state)


def split_quoted_comma_values(raw_text: str) -> list[str]:
    values: list[str] = []
    current: list[str] = []
    quote_char: str | None = None
    escaped = False
    for char in raw_text:
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if quote_char is not None:
            if char == quote_char:
                quote_char = None
            else:
                current.append(char)
            continue
        if char in {"'", '"'}:
            quote_char = char
            continue
        if char == ",":
            value = normalize_inline_whitespace("".join(current))
            if value:
                values.append(value)
            current = []
            continue
        current.append(char)
    if quote_char is not None:
        raise RetrieverError("Unterminated quote in slash command.")
    if escaped:
        current.append("\\")
    value = normalize_inline_whitespace("".join(current))
    if value:
        values.append(value)
    return values


def shlex_split_slash_tail(raw_tail: str) -> list[str]:
    try:
        return shlex.split(raw_tail, posix=True)
    except ValueError as exc:
        raise RetrieverError(f"Could not parse slash command arguments: {exc}") from exc


def resolve_scope_dataset_selection(connection: sqlite3.Connection, raw_values: list[str]) -> list[dict[str, object]]:
    if len(raw_values) > MAX_SCOPE_DATASETS:
        raise RetrieverError(f"Scope datasets are capped at {MAX_SCOPE_DATASETS} entries.")
    resolved_entries: list[dict[str, object]] = []
    seen_ids: set[int] = set()
    for raw_value in raw_values:
        matches = find_dataset_rows_by_name(connection, raw_value)
        if not matches:
            suggestions = scope_dataset_name_suggestions(connection, raw_value)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise RetrieverError(f"Unknown dataset name: {raw_value}.{suggestion_text}")
        row = matches[0]
        dataset_id = int(row["id"])
        if dataset_id in seen_ids:
            continue
        seen_ids.add(dataset_id)
        resolved_entries.append({"id": dataset_id, "name": str(row["dataset_name"])})
    return resolved_entries


def scope_selector_instances(raw_selector: object) -> list[dict[str, object]]:
    if isinstance(raw_selector, dict) and isinstance(raw_selector.get("all_of"), list):
        instances: list[dict[str, object]] = []
        for item in raw_selector["all_of"]:
            normalized = coerce_scope_payload(item)
            if normalized:
                instances.append(normalized)
        return instances
    normalized = coerce_scope_payload(raw_selector)
    return [normalized] if normalized else []


def compose_scope_selectors_and(*raw_selectors: object) -> dict[str, object]:
    instances: list[dict[str, object]] = []
    for raw_selector in raw_selectors:
        instances.extend(scope_selector_instances(raw_selector))
    if not instances:
        return {}
    if len(instances) == 1:
        return instances[0]
    return {"all_of": instances}


def preferred_scope_selector_from_run_id(raw_selector: object) -> int | None:
    preferred_run_id: int | None = None
    for scope in scope_selector_instances(raw_selector):
        if scope.get("from_run_id") is not None:
            preferred_run_id = int(scope["from_run_id"])
    return preferred_run_id


def and_compose_scope_text(existing_value: object, incoming_value: object) -> str:
    existing_text = normalize_inline_whitespace(str(existing_value or ""))
    incoming_text = normalize_inline_whitespace(str(incoming_value or ""))
    if not existing_text:
        return incoming_text
    if not incoming_text:
        return existing_text
    return f"({existing_text}) AND ({incoming_text})"


def merge_scope_with_search_inputs(
    raw_scope: object,
    query: str,
    raw_filters: list[list[str]] | None,
) -> dict[str, object]:
    merged_scope = coerce_scope_payload(raw_scope)
    incoming_scope = derive_search_scope(query, raw_filters)

    incoming_keyword = normalize_inline_whitespace(str(incoming_scope.get("keyword") or ""))
    if incoming_keyword:
        merged_scope["keyword"] = and_compose_scope_text(merged_scope.get("keyword"), incoming_keyword)

    incoming_bates = incoming_scope.get("bates")
    if isinstance(incoming_bates, dict):
        merged_scope["bates"] = intersect_bates_scopes(merged_scope.get("bates"), incoming_bates)

    incoming_filter = normalize_inline_whitespace(str(incoming_scope.get("filter") or ""))
    if incoming_filter:
        merged_scope["filter"] = and_compose_scope_text(merged_scope.get("filter"), incoming_filter)

    return merged_scope


def build_explicit_scope_selector(
    connection: sqlite3.Connection,
    *,
    query: str,
    raw_bates: str | None,
    raw_filters: list[list[str]] | None,
    dataset_names: list[str] | None,
    from_run_id: int | None,
) -> dict[str, object]:
    selector: dict[str, object] = {}
    normalized_query = query.strip()
    if normalized_query:
        selector["keyword"] = query

    normalized_bates = normalize_inline_whitespace(str(raw_bates or ""))
    if normalized_bates:
        selector["bates"] = parse_bates_scope_input(normalized_bates)

    expressions = normalize_sql_filter_expressions(raw_filters)
    if expressions:
        selector["filter"] = " AND ".join(f"({expression})" for expression in expressions)

    raw_dataset_names = [
        normalize_inline_whitespace(str(dataset_name or ""))
        for dataset_name in (dataset_names or [])
    ]
    normalized_dataset_names = [dataset_name for dataset_name in raw_dataset_names if dataset_name]
    if normalized_dataset_names:
        selector["dataset"] = resolve_scope_dataset_selection(connection, normalized_dataset_names)

    resolved_from_run_id = resolve_scope_from_run_id(connection, from_run_id)
    if resolved_from_run_id is not None:
        selector["from_run_id"] = resolved_from_run_id

    return coerce_scope_payload(selector)


def build_effective_scope_selector(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    query: str,
    raw_bates: str | None,
    raw_filters: list[list[str]] | None,
    dataset_names: list[str] | None,
    from_run_id: int | None,
    select_from_scope: bool,
) -> dict[str, object]:
    base_scope = {}
    if select_from_scope:
        base_scope = coerce_scope_payload(read_session_state(paths).get("scope"))
        base_scope.pop("set_at", None)
    explicit_selector = build_explicit_scope_selector(
        connection,
        query=query,
        raw_bates=raw_bates,
        raw_filters=raw_filters,
        dataset_names=dataset_names,
        from_run_id=from_run_id,
    )
    return compose_scope_selectors_and(base_scope, explicit_selector)


def resolve_scope_document_search_with_explicit_sort(
    connection: sqlite3.Connection,
    raw_scope: object,
    sort_field: str | None,
    order: str | None,
) -> dict[str, object]:
    normalized_sort_field = sort_field
    normalized_order = (order or "desc").lower()
    keyword_query = normalize_inline_whitespace(str(coerce_scope_payload(raw_scope).get("keyword") or ""))
    if sort_field == "relevance" and not keyword_query:
        raise RetrieverError("Sort 'relevance' requires a non-empty query.")
    if sort_field and sort_field != "relevance":
        normalized_sort_field = resolve_sort_field_name(connection, sort_field)
        selection = resolve_scope_document_search(
            connection,
            raw_scope,
            sort_specs=[(str(normalized_sort_field), normalized_order)],
        )
        selection["sort"] = str(normalized_sort_field)
        selection["order"] = normalized_order
        selection["sort_spec"] = f"{normalized_sort_field} {normalized_order}"
        return selection
    selection = resolve_scope_document_search(connection, raw_scope)
    if sort_field == "relevance":
        selection["sort"] = "relevance"
        selection["order"] = (order or "asc").lower()
        selection["sort_spec"] = f"relevance {selection['order']}"
    return selection


def persist_browsing_search_result(
    paths: dict[str, Path],
    session_state: dict[str, object],
    payload: dict[str, object],
    sort_specs: list[tuple[str, str]] | None,
) -> dict[str, object]:
    session_state["scope"] = coerce_scope_payload(payload.get("scope"))
    browsing_payload: dict[str, object] = {
        "offset": int(payload.get("offset") or 0),
        "total_known": int(payload.get("total_hits") or 0),
        "run_at": utc_now(),
    }
    if sort_specs:
        browsing_payload["sort"] = serialize_sort_specs(sort_specs)
    session_state["browsing"] = browsing_payload
    persist_session_state(paths, session_state)
    return payload


def persist_direct_view_search_result(
    root: Path,
    payload: dict[str, object],
    column_defs: list[dict[str, str]],
    *,
    sort_specs: list[tuple[str, str]] | None,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    session_state = read_session_state(paths)
    persisted_session_state = persist_display_preferences(
        paths,
        session_state,
        column_defs,
        int(payload.get("per_page") or DEFAULT_PAGE_SIZE),
    )
    persist_browsing_search_result(paths, persisted_session_state, payload, sort_specs)
    return payload


def run_browsing_search_from_session(
    root: Path,
    paths: dict[str, Path],
    session_state: dict[str, object] | None = None,
    *,
    offset: int | None = None,
    sort_specs: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    normalized_session_state = read_session_state(paths) if session_state is None else session_state
    effective_sort_specs = sort_specs if sort_specs is not None else session_sort_specs(normalized_session_state)
    current_offset = int(session_browsing_state(normalized_session_state).get("offset") or 0)
    connection = connect_db(paths["db_path"])
    try:
        display_column_defs, display_warnings, normalized_session_state = resolve_session_display_columns(
            connection,
            paths,
            normalized_session_state,
        )
    finally:
        connection.close()
    payload = search_with_scope(
        root,
        normalized_session_state.get("scope", {}),
        per_page=session_page_size(normalized_session_state),
        offset=current_offset if offset is None else offset,
        sort_specs=effective_sort_specs or None,
        display_column_defs=display_column_defs,
        warnings=display_warnings,
    )
    return persist_browsing_search_result(paths, normalized_session_state, payload, effective_sort_specs or None)


def run_scope_search_from_session(root: Path, paths: dict[str, Path], scope: dict[str, object]) -> dict[str, object]:
    session_state = persist_scope_to_session(paths, scope)
    return run_browsing_search_from_session(root, paths, session_state, offset=0)


def run_slash_command(root: Path, raw_command: str) -> dict[str, object]:
    command_name, normalized_tail = parse_slash_command_text(raw_command)

    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        session_state = read_session_state(paths)
        scope = coerce_scope_payload(session_state.get("scope"))

        if command_name == "scope":
            scope_args = shlex_split_slash_tail(normalized_tail) if normalized_tail else []
            if not scope_args:
                return {"status": "ok", "scope": scope}
            subcommand = scope_args[0]
            if subcommand == "list":
                if len(scope_args) != 1:
                    raise RetrieverError("Usage: /scope list")
                return {"status": "ok", "saved_scopes": saved_scope_summaries(paths)}
            if subcommand == "clear":
                return run_scope_search_from_session(root, paths, {})
            if subcommand == "save":
                if len(scope_args) != 2:
                    raise RetrieverError("Usage: /scope save <name>")
                return save_named_scope(paths, scope_args[1], scope)
            if subcommand == "load":
                if len(scope_args) != 2:
                    raise RetrieverError("Usage: /scope load <name>")
                saved_scopes_state = read_saved_scopes_state(paths)
                existing_name = find_saved_scope_name(saved_scopes_state, scope_args[1])
                if existing_name is None:
                    raise RetrieverError(f"Unknown saved scope: {scope_args[1]}")
                saved_scope = saved_scopes_state["scopes"][existing_name]
                loaded_scope = coerce_scope_payload(saved_scope)
                return run_scope_search_from_session(root, paths, loaded_scope)
            raise RetrieverError(f"Unknown /scope command: {subcommand}")

        if command_name == "page-size":
            if not normalized_tail:
                return {"status": "ok", "page_size": session_page_size(session_state)}
            updated_session_state = read_session_state(paths)
            display_state = session_display_state(updated_session_state)
            display_state["page_size"] = parse_page_size_value(normalized_tail)
            updated_session_state["display"] = coerce_display_payload(display_state)
            updated_session_state = persist_session_state(paths, updated_session_state)
            return run_browsing_search_from_session(root, paths, updated_session_state)

        if command_name == "columns":
            updated_session_state = read_session_state(paths)
            if not normalized_tail:
                column_defs, warnings, updated_session_state = resolve_session_display_columns(connection, paths, updated_session_state)
                payload: dict[str, object] = {
                    "status": "ok",
                    "display": build_display_payload(column_defs, session_page_size(updated_session_state)),
                }
                if warnings:
                    payload["warnings"] = warnings
                return payload

            if normalized_tail == "list":
                return {
                    "status": "ok",
                    "columns": displayable_field_entries(connection),
                }

            if normalized_tail == "default":
                display_state = session_display_state(updated_session_state)
                display_state.pop("columns", None)
                updated_session_state["display"] = coerce_display_payload(display_state)
                updated_session_state = persist_session_state(paths, updated_session_state)
                return run_browsing_search_from_session(root, paths, updated_session_state)

            subcommand, _, remainder = normalized_tail.partition(" ")
            if subcommand not in {"set", "add", "remove"}:
                raise RetrieverError("Usage: /columns, /columns set <list>, /columns add <col>, /columns remove <col>, or /columns default")
            if not remainder.strip():
                raise RetrieverError("Column selection cannot be empty.")

            current_column_defs, _, updated_session_state = resolve_session_display_columns(connection, paths, updated_session_state)
            current_columns = display_column_names(current_column_defs)

            if subcommand == "set":
                requested_columns = parse_display_columns_argument(remainder.strip())
                next_column_defs, _, _ = resolve_display_column_definitions(connection, requested_columns, drop_missing=False)
            elif subcommand == "add":
                field_name = normalize_inline_whitespace(remainder.strip())
                next_column_defs, _, _ = resolve_display_column_definitions(
                    connection,
                    current_columns + [field_name],
                    drop_missing=False,
                )
            else:
                field_name = normalize_inline_whitespace(remainder.strip())
                field_def = resolve_field_definition(connection, field_name)
                canonical_name = str(field_def["field_name"])
                if canonical_name not in current_columns:
                    raise RetrieverError(f"Column '{canonical_name}' is not in the current display set.")
                remaining_columns = [column_name for column_name in current_columns if column_name != canonical_name]
                if not remaining_columns:
                    raise RetrieverError("Display must include at least one column. Use /columns default to reset.")
                next_column_defs, _, _ = resolve_display_column_definitions(connection, remaining_columns, drop_missing=False)

            updated_session_state = persist_display_columns(paths, updated_session_state, next_column_defs)
            return run_browsing_search_from_session(root, paths, updated_session_state)

        if command_name == "sort":
            updated_session_state = read_session_state(paths)
            if not normalized_tail:
                return {"status": "ok", **active_sort_payload(scope, updated_session_state)}
            if normalized_tail == "list":
                return {"status": "ok", "sortable_fields": sortable_field_entries(connection)}
            if normalized_tail == "default":
                return run_browsing_search_from_session(root, paths, updated_session_state, offset=0, sort_specs=[])
            sort_specs = parse_slash_sort_specs(connection, normalized_tail)
            return run_browsing_search_from_session(root, paths, updated_session_state, offset=0, sort_specs=sort_specs)

        if command_name in {"next", "previous"}:
            updated_session_state = read_session_state(paths)
            per_page = session_page_size(updated_session_state)
            current_offset = int(session_browsing_state(updated_session_state).get("offset") or 0)
            target_offset = current_offset + per_page if command_name == "next" else max(0, current_offset - per_page)
            return run_browsing_search_from_session(root, paths, updated_session_state, offset=target_offset)

        if command_name == "page":
            updated_session_state = read_session_state(paths)
            if not normalized_tail:
                return {"status": "ok", **active_page_payload(updated_session_state)}
            per_page = session_page_size(updated_session_state)
            current_offset = int(session_browsing_state(updated_session_state).get("offset") or 0)
            page_token = normalize_inline_whitespace(normalized_tail).lower()
            if page_token == "first":
                target_offset = 0
            elif page_token == "last":
                target_offset = 10**12
            elif page_token == "next":
                target_offset = current_offset + per_page
            elif page_token == "previous":
                target_offset = max(0, current_offset - per_page)
            else:
                try:
                    page_number = int(page_token)
                except ValueError as exc:
                    raise RetrieverError("Usage: /page <N|first|last|next|previous>") from exc
                if page_number < 1:
                    raise RetrieverError("Page number must be >= 1.")
                target_offset = (page_number - 1) * per_page
            return run_browsing_search_from_session(root, paths, updated_session_state, offset=target_offset)

        if command_name == "search":
            if not normalized_tail:
                return {"status": "ok", "keyword": normalize_inline_whitespace(str(scope.get("keyword") or "")) or None}
            if normalized_tail == "clear":
                scope.pop("keyword", None)
                scope.pop("bates", None)
                return run_scope_search_from_session(root, paths, scope)
            force_fts = False
            within = False
            query_text = normalized_tail
            if normalized_tail.startswith("--within "):
                within = True
                query_text = normalized_tail[len("--within "):].lstrip()
            elif normalized_tail.startswith("--fts "):
                force_fts = True
                query_text = normalized_tail[len("--fts "):].lstrip()
            if not query_text:
                raise RetrieverError("Search text cannot be empty.")
            incoming_bates = None if force_fts else parse_bates_query(query_text)
            if incoming_bates[0] is not None and incoming_bates[1] is not None:
                parsed_bates = parse_bates_scope_input(query_text)
                if within:
                    if "bates" not in scope and "keyword" in scope:
                        raise RetrieverError("`/search --within` only composes within the current slot. Use `/search <bates>` to add Bates alongside a keyword scope.")
                    scope["bates"] = intersect_bates_scopes(scope.get("bates"), parsed_bates)
                else:
                    scope["bates"] = parsed_bates
                return run_scope_search_from_session(root, paths, scope)
            if within:
                if "keyword" not in scope and "bates" in scope:
                    raise RetrieverError("`/search --within` only composes within the current slot. Use `/search <text>` or `/filter <expr>` to add a keyword alongside Bates scope.")
                existing_keyword = normalize_inline_whitespace(str(scope.get("keyword") or ""))
                scope["keyword"] = query_text if not existing_keyword else f"({existing_keyword}) AND ({query_text})"
            else:
                scope["keyword"] = query_text
            return run_scope_search_from_session(root, paths, scope)

        if command_name == "bates":
            if not normalized_tail:
                return {"status": "ok", "bates": scope.get("bates")}
            if normalized_tail == "clear":
                scope.pop("bates", None)
                return run_scope_search_from_session(root, paths, scope)
            scope["bates"] = parse_bates_scope_input(normalized_tail)
            return run_scope_search_from_session(root, paths, scope)

        if command_name == "filter":
            if not normalized_tail:
                return {"status": "ok", "filter": normalize_inline_whitespace(str(scope.get("filter") or "")) or None}
            if normalized_tail == "clear":
                scope.pop("filter", None)
                return run_scope_search_from_session(root, paths, scope)
            compile_sql_filter_expression(connection, normalized_tail)
            existing_filter = normalize_inline_whitespace(str(scope.get("filter") or ""))
            scope["filter"] = normalized_tail if not existing_filter else f"({existing_filter}) AND ({normalized_tail})"
            return run_scope_search_from_session(root, paths, scope)

        if command_name == "dataset":
            if not normalized_tail:
                return {"status": "ok", "dataset": coerce_scope_dataset_entries(scope.get("dataset"))}
            dataset_args = shlex_split_slash_tail(normalized_tail)
            if dataset_args and dataset_args[0] == "list":
                if len(dataset_args) != 1:
                    raise RetrieverError("Usage: /dataset list")
                return {"status": "ok", "datasets": list_dataset_summaries(connection)}
            if dataset_args and dataset_args[0] == "clear":
                scope.pop("dataset", None)
                return run_scope_search_from_session(root, paths, scope)
            if dataset_args and dataset_args[0] == "rename":
                if len(dataset_args) != 3:
                    raise RetrieverError("Usage: /dataset rename <old-name> <new-name>")
                renamed_payload = rename_dataset(root, dataset_args[1], dataset_args[2])
                dataset_summary = renamed_payload["dataset"]
                dataset_id = int(dataset_summary["id"])
                dataset_name = str(dataset_summary["dataset_name"])
                refresh_dataset_name_refs_in_scope(scope, dataset_id, dataset_name)
                updated_session_state = read_session_state(paths)
                updated_session_state["scope"] = coerce_scope_payload(scope)
                persist_session_state(paths, updated_session_state)
                refresh_dataset_name_refs_in_saved_scopes(paths, dataset_id, dataset_name)
                return renamed_payload
            dataset_names = split_quoted_comma_values(normalized_tail)
            if not dataset_names:
                raise RetrieverError("Dataset selection cannot be empty.")
            scope["dataset"] = resolve_scope_dataset_selection(connection, dataset_names)
            return run_scope_search_from_session(root, paths, scope)

        if command_name == "from-run":
            if not normalized_tail:
                return {"status": "ok", "from_run_id": scope.get("from_run_id")}
            if normalized_tail == "clear":
                scope.pop("from_run_id", None)
                return run_scope_search_from_session(root, paths, scope)
            scope["from_run_id"] = resolve_scope_from_run_id(connection, normalized_tail)
            return run_scope_search_from_session(root, paths, scope)

        raise RetrieverError(f"Unknown slash command: /{command_name}")
    finally:
        connection.close()


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
          AND COALESCE(child_document_kind, ?) = ?
          AND lifecycle_status NOT IN ('missing', 'deleted')
        """,
        [*parent_ids, CHILD_DOCUMENT_KIND_ATTACHMENT, CHILD_DOCUMENT_KIND_ATTACHMENT],
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


def resolve_export_output_dir(paths: dict[str, Path], raw_output_path: str) -> Path:
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
    if resolved_path.exists() and not resolved_path.is_dir():
        raise RetrieverError(f"Output path is not a directory: {resolved_path}")
    workspace_root = paths["root"].resolve()
    if path_within(resolved_path, workspace_root) and not path_within(resolved_path, exports_dir):
        raise RetrieverError(
            f"Workspace-internal output paths must live under {exports_dir} to avoid re-ingesting exported exports."
        )
    return resolved_path


def relative_output_path_or_none(root: Path, output_path: Path) -> str | None:
    try:
        return output_path.relative_to(root).as_posix()
    except ValueError:
        return None


def export_preview_unit_file_name(unit: dict[str, object]) -> str:
    unit_kind = str(unit["unit_kind"])
    if unit_kind == "email_conversation":
        return f"conversation-{int(unit['conversation_id']):08d}.html"
    if unit_kind == "conversation_run":
        return (
            f"conversation-{int(unit['conversation_id']):08d}"
            f"-run-{int(unit['run_start_index']) + 1:04d}-{int(unit['run_end_index']) + 1:04d}.html"
        )
    return f"document-{int(unit['documents'][0]['id']):08d}.html"


def build_export_preview_unit_html(
    unit: dict[str, object],
    *,
    file_name: str,
) -> str:
    documents = list(unit["documents"])
    selected_document_ids = {int(document_id) for document_id in unit["selected_document_ids"]}
    if unit["unit_kind"] == "standalone":
        heading = conversation_preview_document_heading(documents[0])
        preview_type = "document"
    elif unit["unit_kind"] == "email_conversation":
        heading = str(unit["title"])
        preview_type = "email conversation"
    else:
        heading = str(unit["title"])
        preview_type = "conversation run"

    headers = {
        "Preview": heading,
        "Type": preview_type,
        "Documents": str(len(documents)),
        "Selected": str(len(selected_document_ids)),
    }
    if unit.get("conversation_type"):
        headers["Conversation type"] = str(unit["conversation_type"])

    doc_target_hrefs = {
        int(document["id"]): f"#{conversation_preview_anchor(int(document['id']))}"
        for document in documents
    }
    sections: list[str] = []
    for document in documents:
        section_html = render_conversation_document_section(
            document,
            current_segment_href=file_name,
            doc_target_hrefs=doc_target_hrefs,
        )
        if int(document["id"]) in selected_document_ids:
            section_html = section_html.replace(
                '<article class="conversation-document"',
                '<article class="conversation-document" data-selected="true"',
                1,
            )
        sections.append(section_html)
    nav_links = '<div class="conversation-nav-links"><a href="../index.html">Contents</a></div>'
    return build_html_preview(
        headers,
        body_html=(
            "<main>"
            "<div class=\"conversation-nav\">"
            f"{nav_links}"
            "</div>"
            f"{''.join(sections)}"
            "</main>"
        ),
        document_title=heading,
        head_html=build_conversation_preview_head_html(),
        heading=heading,
    )


def build_export_preview_index_html(
    *,
    units: list[dict[str, object]],
    selected_rows: list[sqlite3.Row],
    document_targets_by_id: dict[int, dict[str, object]],
) -> str:
    headers = {
        "Selected documents": str(len(selected_rows)),
        "Preview files": str(len(units)),
    }
    cards: list[str] = []
    for unit in units:
        output_rel_path = str(unit["output_rel_path"])
        selected_links = "".join(
            f"<li><a href=\"{html.escape(str(document_targets_by_id[int(document_id)]['href']))}\">{html.escape(str(document_targets_by_id[int(document_id)]['title']))}</a></li>"
            for document_id in unit["selected_document_ids"]
            if int(document_id) in document_targets_by_id
        )
        cards.append(
            "<section class=\"conversation-segment-card\">"
            f"<h2><a href=\"{html.escape(output_rel_path)}\">{html.escape(str(unit['title']))}</a></h2>"
            f"<p>{html.escape(str(unit['summary']))}</p>"
            f"{'<ul>' + selected_links + '</ul>' if selected_links else ''}"
            "</section>"
        )
    return build_html_preview(
        headers,
        body_html=f"<main><div class=\"conversation-segments\">{''.join(cards)}</div></main>",
        document_title="Export Preview Index",
        head_html=build_conversation_preview_head_html(),
        heading="Export Preview Index",
    )


def cleanup_previous_export_preview_outputs(output_dir: Path) -> None:
    manifest_path = output_dir / "manifest.json"
    if not manifest_path.exists():
        return
    try:
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return
    relative_paths: set[str] = set()
    for key in ("index_rel_path",):
        value = manifest_payload.get(key)
        if isinstance(value, str) and value:
            relative_paths.add(value)
    for unit in manifest_payload.get("units", []):
        if isinstance(unit, dict):
            value = unit.get("output_rel_path")
            if isinstance(value, str) and value:
                relative_paths.add(value)
    relative_paths.add("manifest.json")
    for relative_path in relative_paths:
        candidate = (output_dir / relative_path).resolve()
        if path_within(candidate, output_dir.resolve()) and candidate.exists() and candidate.is_file():
            candidate.unlink()


def build_export_preview_units(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    selected_rows: list[sqlite3.Row],
) -> list[dict[str, object]]:
    selected_rows_by_conversation: dict[int, list[sqlite3.Row]] = defaultdict(list)
    standalone_rows: list[sqlite3.Row] = []
    selection_order_by_document_id: dict[int, int] = {}
    for index, row in enumerate(selected_rows):
        document_id = int(row["id"])
        selection_order_by_document_id[document_id] = index
        child_kind = normalize_whitespace(str(row["child_document_kind"] or "")).lower()
        if row["conversation_id"] is None or child_kind == CHILD_DOCUMENT_KIND_ATTACHMENT:
            standalone_rows.append(row)
        else:
            selected_rows_by_conversation[int(row["conversation_id"])].append(row)

    units: list[dict[str, object]] = []
    for row in standalone_rows:
        document_id = int(row["id"])
        documents = load_preview_documents(
            connection,
            paths,
            document_ids=[document_id],
            include_attachment_children=True,
            require_dataset_membership=True,
        )
        if not documents:
            continue
        title = conversation_preview_document_heading(documents[0])
        units.append(
            {
                "unit_key": f"document:{document_id}",
                "unit_kind": "standalone",
                "title": title,
                "summary": "Standalone exported document preview",
                "documents": documents,
                "selected_document_ids": [document_id],
                "order_key": selection_order_by_document_id[document_id],
                "conversation_id": None,
                "conversation_type": None,
            }
        )

    for conversation_id, conversation_rows in selected_rows_by_conversation.items():
        conversation_row = connection.execute(
            "SELECT * FROM conversations WHERE id = ?",
            (conversation_id,),
        ).fetchone()
        if conversation_row is None:
            for row in conversation_rows:
                document_id = int(row["id"])
                documents = load_preview_documents(
                    connection,
                    paths,
                    document_ids=[document_id],
                    include_attachment_children=True,
                    require_dataset_membership=True,
                )
                if not documents:
                    continue
                title = conversation_preview_document_heading(documents[0])
                units.append(
                    {
                        "unit_key": f"document:{document_id}",
                        "unit_kind": "standalone",
                        "title": title,
                        "summary": "Standalone exported document preview",
                        "documents": documents,
                        "selected_document_ids": [document_id],
                        "order_key": selection_order_by_document_id[document_id],
                        "conversation_id": None,
                        "conversation_type": None,
                    }
                )
            continue
        documents = load_preview_documents(
            connection,
            paths,
            conversation_id=conversation_id,
            require_dataset_membership=True,
        )
        if not documents:
            continue
        conversation_type = normalize_whitespace(str(conversation_row["conversation_type"] or "")).lower()
        conversation_title = normalize_whitespace(str(conversation_row["display_name"] or "")) or f"Conversation {conversation_id}"
        if conversation_type == "email":
            selected_document_ids = [
                int(row["id"])
                for row in sorted(conversation_rows, key=lambda item: selection_order_by_document_id[int(item["id"])])
            ]
            units.append(
                {
                    "unit_key": f"conversation:{conversation_id}",
                    "unit_kind": "email_conversation",
                    "title": conversation_title,
                    "summary": f"Full email conversation export ({len(documents)} messages)",
                    "documents": documents,
                    "selected_document_ids": selected_document_ids,
                    "order_key": min(selection_order_by_document_id[int(document_id)] for document_id in selected_document_ids),
                    "conversation_id": conversation_id,
                    "conversation_type": conversation_type,
                }
            )
            continue

        position_by_document_id = {
            int(document["id"]): index
            for index, document in enumerate(documents)
        }
        selected_positions = sorted(
            position_by_document_id[int(row["id"])]
            for row in conversation_rows
            if int(row["id"]) in position_by_document_id
        )
        if not selected_positions:
            continue
        run_start = selected_positions[0]
        run_end = run_start
        for position in selected_positions[1:]:
            if position == run_end + 1:
                run_end = position
                continue
            run_documents = documents[run_start : run_end + 1]
            run_selected_document_ids = [
                int(document["id"])
                for document in run_documents
                if int(document["id"]) in selection_order_by_document_id
            ]
            units.append(
                {
                    "unit_key": f"conversation:{conversation_id}:run:{run_start}:{run_end}",
                    "unit_kind": "conversation_run",
                    "title": conversation_title,
                    "summary": f"Conversation run export ({len(run_documents)} documents)",
                    "documents": run_documents,
                    "selected_document_ids": run_selected_document_ids,
                    "order_key": min(selection_order_by_document_id[int(document_id)] for document_id in run_selected_document_ids),
                    "conversation_id": conversation_id,
                    "conversation_type": conversation_type,
                    "run_start_index": run_start,
                    "run_end_index": run_end,
                }
            )
            run_start = position
            run_end = position
        run_documents = documents[run_start : run_end + 1]
        run_selected_document_ids = [
            int(document["id"])
            for document in run_documents
            if int(document["id"]) in selection_order_by_document_id
        ]
        units.append(
            {
                "unit_key": f"conversation:{conversation_id}:run:{run_start}:{run_end}",
                "unit_kind": "conversation_run",
                "title": conversation_title,
                "summary": f"Conversation run export ({len(run_documents)} documents)",
                "documents": run_documents,
                "selected_document_ids": run_selected_document_ids,
                "order_key": min(selection_order_by_document_id[int(document_id)] for document_id in run_selected_document_ids),
                "conversation_id": conversation_id,
                "conversation_type": conversation_type,
                "run_start_index": run_start,
                "run_end_index": run_end,
            }
        )

    units.sort(key=lambda unit: (int(unit["order_key"]), str(unit["unit_key"])))
    return units


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
        return is_attachment_row(row)
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
    select_from_scope: bool = False,
) -> dict[str, object]:
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        field_defs = resolve_export_field_definitions(connection, raw_fields)
        output_path = resolve_export_output_path(paths, raw_output_path)

        normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in (document_ids or [])))
        if normalized_document_ids and (query.strip() or raw_filters or sort_field or order or select_from_scope):
            raise RetrieverError("export-csv accepts either --doc-id selectors or query/filter/scope selectors, not both.")

        if normalized_document_ids:
            rows = fetch_visible_document_rows_by_ids(connection, normalized_document_ids)
            selector: dict[str, object] = {
                "mode": "document_ids",
                "document_ids": normalized_document_ids,
            }
        else:
            if select_from_scope:
                session_state = read_session_state(paths)
                merged_scope = merge_scope_with_search_inputs(session_state.get("scope"), query, raw_filters)
                selection = resolve_scope_document_search_with_explicit_sort(connection, merged_scope, sort_field, order)
                selector = {
                    "mode": "scope_search",
                    "selected_from_scope": True,
                    "scope": selection["scope"],
                    "query": selection["query"],
                    "filters": selection["filters"],
                    "sort": selection["sort"],
                    "order": selection["order"],
                }
            else:
                selection = resolve_document_search(connection, query, raw_filters, sort_field, order)
                selector = {
                    "mode": "search",
                    "query": selection["query"],
                    "filters": selection["filters"],
                    "sort": selection["sort"],
                    "order": selection["order"],
                }
            rows = [item["row"] for item in selection["results"]]

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
        conversation_ids = sorted(
            {
                int(row["conversation_id"])
                for row in retained_rows
                if row["conversation_id"] is not None
            }
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
        conversation_rows = source_connection.execute(
            f"""
            SELECT *
            FROM conversations
            WHERE id IN ({', '.join('?' for _ in conversation_ids)})
            ORDER BY id ASC
            """ if conversation_ids else """
            SELECT *
            FROM conversations
            WHERE 0
            """,
            conversation_ids,
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
            insert_row_dicts(target_connection, "conversations", [sqlite_row_to_dict(row) for row in conversation_rows])
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
    dataset_names: list[str] | None = None,
    query: str = "",
    raw_bates: str | None = None,
    raw_filters: list[list[str]] | None = None,
    from_run_id: int | None = None,
    select_from_scope: bool = False,
    family_mode: str = "exact",
    seed_limit: int | None = None,
    portable_workspace: bool = False,
) -> dict[str, object]:
    normalized_family_mode = normalize_run_family_mode(family_mode)
    if seed_limit is not None and seed_limit < 1:
        raise RetrieverError("Archive limit must be >= 1.")
    paths = workspace_paths(root)
    ensure_layout(paths)
    connection = connect_db(paths["db_path"])
    try:
        apply_schema(connection, root)
        selector = build_effective_scope_selector(
            connection,
            paths,
            query=query,
            raw_bates=raw_bates,
            raw_filters=raw_filters,
            dataset_names=dataset_names,
            from_run_id=from_run_id,
            select_from_scope=select_from_scope,
        )
        if not scope_run_selector_has_inputs(selector):
            raise RetrieverError("Archive selector must include at least one inclusion input.")
        output_path = resolve_export_output_path(paths, raw_output_path)
        selected_documents, _ = plan_scope_selected_documents(
            connection,
            selector=selector,
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


def export_previews(
    root: Path,
    raw_output_path: str,
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
        output_dir = resolve_export_output_dir(paths, raw_output_path)

        normalized_document_ids = list(dict.fromkeys(int(document_id) for document_id in (document_ids or [])))
        if normalized_document_ids and (query.strip() or raw_filters or sort_field or order):
            raise RetrieverError("export-previews accepts either --doc-id selectors or query/filter selectors, not both.")

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

        output_dir.mkdir(parents=True, exist_ok=True)
        cleanup_previous_export_preview_outputs(output_dir)
        units_dir = output_dir / "units"
        units_dir.mkdir(parents=True, exist_ok=True)

        units = build_export_preview_units(connection, paths, rows)
        unit_payloads: list[dict[str, object]] = []
        document_targets_by_id: dict[int, dict[str, object]] = {}
        for unit in units:
            file_name = export_preview_unit_file_name(unit)
            unit_output_path = units_dir / file_name
            unit_output_path.write_text(
                build_export_preview_unit_html(unit, file_name=file_name),
                encoding="utf-8",
            )
            unit_output_rel_path = Path("units") / file_name
            unit_payload = {
                "unit_key": str(unit["unit_key"]),
                "unit_kind": str(unit["unit_kind"]),
                "title": str(unit["title"]),
                "summary": str(unit["summary"]),
                "conversation_id": int(unit["conversation_id"]) if unit.get("conversation_id") is not None else None,
                "conversation_type": unit.get("conversation_type"),
                "document_ids": [int(document["id"]) for document in unit["documents"]],
                "selected_document_ids": [int(document_id) for document_id in unit["selected_document_ids"]],
                "output_path": str(unit_output_path),
                "output_rel_path": unit_output_rel_path.as_posix(),
                "file_size": file_size_bytes(unit_output_path),
            }
            unit_payloads.append(unit_payload)
            for document in unit["documents"]:
                document_id = int(document["id"])
                if document_id not in unit_payload["selected_document_ids"]:
                    continue
                document_targets_by_id[document_id] = {
                    "document_id": document_id,
                    "title": conversation_preview_document_heading(document),
                    "output_path": str(unit_output_path),
                    "output_rel_path": unit_output_rel_path.as_posix(),
                    "file_output_path": str(unit_output_path),
                    "file_output_rel_path": unit_output_rel_path.as_posix(),
                    "target_fragment": conversation_preview_anchor(document_id),
                    "href": f"{unit_output_rel_path.as_posix()}#{conversation_preview_anchor(document_id)}",
                }

        index_path = output_dir / "index.html"
        index_path.write_text(
            build_export_preview_index_html(
                units=unit_payloads,
                selected_rows=rows,
                document_targets_by_id=document_targets_by_id,
            ),
            encoding="utf-8",
        )
        index_rel_path = "index.html"

        document_targets = [
            document_targets_by_id[int(row["id"])]
            for row in rows
            if int(row["id"]) in document_targets_by_id
        ]
        manifest_payload = {
            "status": "ok",
            "output_path": str(output_dir),
            "output_rel_path": relative_output_path_or_none(root, output_dir),
            "index_path": str(index_path),
            "index_rel_path": index_rel_path,
            "selected_document_count": len(rows),
            "unit_count": len(unit_payloads),
            "selector": selector,
            "units": unit_payloads,
            "document_targets": document_targets,
        }
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
        manifest_payload["manifest_path"] = str(manifest_path)
        manifest_payload["manifest_rel_path"] = "manifest.json"
        return manifest_payload
    finally:
        connection.close()


def resolve_workspace_input_path(root: Path, raw_input_path: str) -> Path:
    normalized_input = raw_input_path.strip()
    if not normalized_input:
        raise RetrieverError("Input path cannot be empty.")
    requested_path = Path(normalized_input).expanduser()
    if requested_path.is_absolute():
        return requested_path.resolve()
    return (root / requested_path).resolve()


def inspect_pst_properties(
    root: Path,
    raw_pst_path: str,
    *,
    source_item_ids: list[str] | None = None,
    message_kind: str = "all",
    limit: int = 20,
    max_record_entries: int = 128,
) -> dict[str, object]:
    normalized_message_kind = normalize_whitespace(message_kind).lower() or "all"
    if normalized_message_kind not in {"all", "chat", "email", "calendar", "skip"}:
        raise RetrieverError(f"Unsupported PST message-kind filter: {message_kind!r}")
    if limit < 1:
        raise RetrieverError("PST inspection limit must be >= 1.")
    if max_record_entries < 1:
        raise RetrieverError("PST inspection max-record-entries must be >= 1.")

    pst_path = resolve_workspace_input_path(root, raw_pst_path)
    if not pst_path.exists():
        raise RetrieverError(f"PST path not found: {pst_path}")
    if not pst_path.is_file():
        raise RetrieverError(f"PST path is not a file: {pst_path}")
    if normalize_extension(pst_path) != PST_SOURCE_KIND:
        raise RetrieverError(f"Expected a .pst file, got: {pst_path.name}")

    normalized_source_item_ids = [
        normalize_whitespace(str(source_item_id))
        for source_item_id in (source_item_ids or [])
        if normalize_whitespace(str(source_item_id))
    ]
    source_item_id_filter = set(normalized_source_item_ids)
    scanned = 0
    matched = 0
    messages: list[dict[str, object]] = []
    for payload in iter_pst_debug_messages(pst_path, max_record_entries=max_record_entries):
        scanned += 1
        if normalized_message_kind != "all" and str(payload["message_kind"]) != normalized_message_kind:
            continue
        if source_item_id_filter and str(payload["source_item_id"]) not in source_item_id_filter:
            continue
        matched += 1
        if len(messages) >= limit:
            continue
        messages.append(payload)

    return {
        "status": "ok",
        "pst_path": str(pst_path),
        "pst_rel_path": relative_output_path_or_none(root, pst_path),
        "message_kind": normalized_message_kind,
        "source_item_ids": normalized_source_item_ids,
        "limit": int(limit),
        "max_record_entries": int(max_record_entries),
        "scanned": int(scanned),
        "matched": int(matched),
        "returned": len(messages),
        "truncated": matched > len(messages),
        "messages": messages,
    }


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
        occurrence_row = preferred_occurrence_for_document(connection, document_id, ["o.lifecycle_status = 'active'"], [])

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
        preview_target = default_preview_target(paths, row, connection)
        if (
            occurrence_row is not None
            and str(preview_target.get("preview_type") or "") == "native"
            and str(preview_target.get("rel_path") or "") == str(row["rel_path"])
        ):
            preview_target = build_preview_target_payload(
                rel_path=str(occurrence_row["rel_path"]),
                abs_path=str(document_absolute_path(paths, str(occurrence_row["rel_path"]))),
                preview_type="native",
                label=None,
                ordinal=0,
            )
        preview_rel_path = str(preview_target["rel_path"])
        preview_abs_path = str(preview_target["abs_path"])
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
                        occurrence_row,
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
            "document": document_overview_payload(paths, connection, row, occurrence_row=occurrence_row),
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
        occurrence_row = preferred_occurrence_for_document(connection, document_id, ["o.lifecycle_status = 'active'"], [])

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
                "file_name": occurrence_row["file_name"] if occurrence_row is not None else row["file_name"],
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


def build_analysis_scope_filters(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    raw_filters: list[list[str]] | None,
    select_from_scope: bool,
) -> tuple[dict[str, object] | None, list[str], list[object], list[object]]:
    if not select_from_scope:
        filter_summary, clauses, params = build_search_filters(connection, raw_filters)
        return None, clauses, params, filter_summary

    merged_scope = merge_scope_with_search_inputs(read_session_state(paths).get("scope"), "", raw_filters)
    scope, clauses, params, filter_summary = build_scope_search_filters(connection, merged_scope)
    keyword_query = normalize_inline_whitespace(str(scope.get("keyword") or ""))
    bates_query = format_scope_bates_value(scope.get("bates"))
    if not keyword_query and not bates_query:
        return scope, clauses, params, filter_summary

    selection = resolve_scope_document_search(connection, scope)
    selected_document_ids = [int(item["id"]) for item in selection["results"]]
    if not selected_document_ids:
        return selection["scope"], ["0"], [], selection["filters"]

    connection.execute("DROP TABLE IF EXISTS temp_scope_selected_documents")
    connection.execute(
        """
        CREATE TEMP TABLE temp_scope_selected_documents (
          document_id INTEGER PRIMARY KEY
        )
        """
    )
    connection.executemany(
        "INSERT INTO temp_scope_selected_documents (document_id) VALUES (?)",
        [(document_id,) for document_id in selected_document_ids],
    )
    return (
        selection["scope"],
        ["EXISTS (SELECT 1 FROM temp_scope_selected_documents tsd WHERE tsd.document_id = d.id)"],
        [],
        selection["filters"],
    )


def search_chunks(
    root: Path,
    query: str,
    raw_filters: list[list[str]] | None,
    sort_field: str | None,
    order: str | None,
    top_k: int,
    per_doc_cap: int,
    *,
    select_from_scope: bool = False,
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
        effective_scope, clauses, params, filter_summary = build_analysis_scope_filters(
            connection,
            paths,
            raw_filters=raw_filters,
            select_from_scope=select_from_scope,
        )
        occurrence_scope_clauses, occurrence_scope_params = build_occurrence_scope_filters(connection, raw_filters)
        if count_only:
            payload = {
                "query": query,
                "filters": filter_summary,
                "documents_with_hits": count_distinct_chunk_documents(connection, query, clauses, params),
                "total_docs_filtered": count_filtered_documents(connection, clauses, params),
                "count_mode": "distinct-documents",
            }
            if effective_scope is not None:
                payload["selected_from_scope"] = True
                payload["scope"] = effective_scope
            return payload

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
        preferred_occurrences = preferred_occurrences_by_document(
            connection,
            [int(row["id"]) for row in returned_rows],
            occurrence_scope_clauses,
            occurrence_scope_params,
        )
        parent_summaries = fetch_parent_summaries(
            connection,
            [row for row in returned_rows if row["parent_document_id"] is not None],
        )
        for row in returned_rows:
            occurrence_row = preferred_occurrences.get(int(row["id"]))
            preview_target = default_preview_target(paths, row, connection)
            if (
                occurrence_row is not None
                and str(preview_target.get("preview_type") or "") == "native"
                and str(preview_target.get("rel_path") or "") == str(row["rel_path"])
            ):
                preview_target = build_preview_target_payload(
                    rel_path=str(occurrence_row["rel_path"]),
                    abs_path=str(document_absolute_path(paths, str(occurrence_row["rel_path"]))),
                    preview_type="native",
                    label=None,
                    ordinal=0,
                )
            preview_rel_path = str(preview_target["rel_path"])
            preview_abs_path = str(preview_target["abs_path"])
            snippet = make_snippet(str(row["text_content"] or ""), query)
            source_row = occurrence_row or row
            result = {
                **document_path_payload(paths, connection, row, occurrence_row=occurrence_row),
                "document_id": int(row["id"]),
                "control_number": row["control_number"],
                "file_name": source_row["file_name"],
                "file_type": source_row["file_type"],
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
                    "custodian": source_row["custodian"],
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
                    occurrence_row,
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
            if source_row["production_id"] is not None:
                result["production_name"] = production_names.get(int(source_row["production_id"]))
            if row["parent_document_id"] is not None:
                result["parent"] = parent_summaries.get(int(row["parent_document_id"]))
            results.append(result)

        normalized_sort = (sort_field or "relevance").lower()
        normalized_order = (order or ("asc" if normalized_sort == "relevance" else "desc")).lower()
        payload = {
            "query": query,
            "filters": filter_summary,
            "sort": normalized_sort,
            "order": normalized_order,
            "top_k": top_k,
            "per_doc_cap": per_doc_cap,
            "total_matches": len(raw_rows),
            "results": results,
        }
        if effective_scope is not None:
            payload["selected_from_scope"] = True
            payload["scope"] = effective_scope
        return payload
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
    *,
    select_from_scope: bool = False,
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
        effective_scope, clauses, params, filter_summary = build_analysis_scope_filters(
            connection,
            paths,
            raw_filters=raw_filters,
            select_from_scope=select_from_scope,
        )
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
        if effective_scope is not None:
            payload["selected_from_scope"] = True
            payload["scope"] = effective_scope
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
        help="Repeatable SQL-like filter expression",
    )
    parser.add_argument("--sort", "--sort-by", dest="sort", help="Sort field or 'relevance'")
    parser.add_argument("--order", "--sort-order", dest="order", choices=("asc", "desc"), help="Sort order")
    parser.add_argument("--page", type=int, default=1, help="1-based result page")
    parser.add_argument(
        "--per-page",
        "--limit",
        dest="per_page",
        type=int,
        default=None,
        help="Results per page (defaults to saved /page-size or 10)",
    )
    parser.add_argument("--columns", help="Comma-separated result columns")
    parser.add_argument("--mode", choices=("compose", "view"), default="compose", help="Search response mode")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Return the full payload instead of the default compact JSON",
    )


def add_scope_run_selector_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--keyword", dest="query", help="Keyword query text")
    parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable SQL-like filter expression",
    )
    parser.add_argument("--bates", help="Bates token or Bates range")
    parser.add_argument(
        "--dataset",
        dest="dataset_names",
        action="append",
        help="Exact dataset name (repeatable)",
    )
    parser.add_argument("--from-run-id", type=int, help="Restrict to documents already present in a prior run")
    parser.add_argument(
        "--select-from-scope",
        action="store_true",
        help="AND-narrow the selector with the persisted workspace scope",
    )
    parser.add_argument("query", nargs="?", default="", help="Optional keyword query text")


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

    inspect_pst_parser = subparsers.add_parser(
        "inspect-pst-properties",
        help="Inspect raw PST message fields and named-property candidates for debugging conversation scope ids",
    )
    inspect_pst_parser.add_argument("workspace", help="Workspace root path")
    inspect_pst_parser.add_argument("pst_path", help="PST file path; relative paths resolve from the workspace root")
    inspect_pst_parser.add_argument(
        "--source-item-id",
        dest="source_item_ids",
        action="append",
        help="Restrict to one PST message source_item_id (repeatable)",
    )
    inspect_pst_parser.add_argument(
        "--message-kind",
        default="all",
        choices=("all", "chat", "email", "calendar", "skip"),
        help="Filter to one normalized PST message kind",
    )
    inspect_pst_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of matching messages to return",
    )
    inspect_pst_parser.add_argument(
        "--max-record-entries",
        type=int,
        default=128,
        help="Maximum number of record entries to serialize per record set",
    )

    search_parser = subparsers.add_parser("search", help="Search indexed documents")
    add_search_arguments(search_parser)

    search_docs_parser = subparsers.add_parser("search-docs", help="Search indexed documents at the document level")
    add_search_arguments(search_docs_parser)

    slash_parser = subparsers.add_parser("slash", help="Execute a scope-aware slash command")
    slash_parser.add_argument("workspace", help="Workspace root path")
    slash_parser.add_argument("command_text", nargs=argparse.REMAINDER, help="Slash command text")

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
        help="Repeatable SQL-like filter expression",
    )
    export_parser.add_argument(
        "--select-from-scope",
        action="store_true",
        help="AND-narrow the export selector with the persisted workspace scope",
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
    add_scope_run_selector_arguments(export_archive_parser)
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

    export_previews_parser = subparsers.add_parser(
        "export-previews",
        help="Write HTML preview exports for selected documents under .retriever/exports",
    )
    export_previews_parser.add_argument("workspace", help="Workspace root path")
    export_previews_parser.add_argument("output_path", help="Output directory path; relative paths resolve under .retriever/exports")
    export_previews_parser.add_argument("query", nargs="?", default="", help="Optional keyword query text for search-based export")
    export_previews_parser.add_argument(
        "--doc-id",
        dest="document_ids",
        action="append",
        type=int,
        help="Document id to export (repeatable, preserves input order)",
    )
    export_previews_parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        nargs="+",
        help="Repeatable filter in the form <field> <op> <value>",
    )
    export_previews_parser.add_argument("--sort", "--sort-by", dest="sort", help="Sort field for search-based export or 'relevance'")
    export_previews_parser.add_argument("--order", "--sort-order", dest="order", choices=("asc", "desc"), help="Sort order")

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
        help="Repeatable SQL-like filter expression",
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
        "--select-from-scope",
        action="store_true",
        help="AND-narrow the chunk search to the persisted workspace scope",
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
        help="Repeatable SQL-like filter expression",
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
    aggregate_parser.add_argument(
        "--select-from-scope",
        action="store_true",
        help="AND-narrow the aggregation to the persisted workspace scope",
    )

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
    add_scope_run_selector_arguments(create_run_parser)
    create_run_parser.add_argument(
        "--doc-id",
        dest="document_ids",
        action="append",
        type=int,
        help="Document id to include in the run (repeatable)",
    )
    create_run_parser.add_argument(
        "--family-mode",
        default="exact",
        choices=sorted(RUN_FAMILY_MODES),
        help="Whether to include only seed docs or their family members too",
    )
    create_run_parser.add_argument(
        "--activation-policy",
        default="manual",
        choices=sorted(RUN_ACTIVATION_POLICIES),
        help="Whether revision-producing jobs should auto-promote created text revisions",
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

    reconcile_duplicates_parser = subparsers.add_parser(
        "reconcile-duplicates",
        help="Dry-run or apply post-ingest duplicate reconciliation",
    )
    reconcile_duplicates_parser.add_argument("workspace", help="Workspace root path")
    reconcile_duplicates_parser.add_argument(
        "--basis",
        choices=("content_hash",),
        default="content_hash",
        help="Reconciliation basis",
    )
    reconcile_mode_group = reconcile_duplicates_parser.add_mutually_exclusive_group()
    reconcile_mode_group.add_argument("--dry-run", action="store_true", help="Preview merge candidates without applying them")
    reconcile_mode_group.add_argument("--apply", action="store_true", help="Apply mergeable reconciliation candidates")

    merge_conversation_parser = subparsers.add_parser(
        "merge-into-conversation",
        help="Manually assign one document family into another document's conversation",
    )
    merge_conversation_parser.add_argument("workspace", help="Workspace root path")
    merge_conversation_parser.add_argument("--doc-id", type=int, required=True, help="Document id to move")
    merge_conversation_parser.add_argument("--target-doc-id", type=int, required=True, help="Target document id")

    split_conversation_parser = subparsers.add_parser(
        "split-from-conversation",
        help="Split one document family into its own manually pinned conversation",
    )
    split_conversation_parser.add_argument("workspace", help="Workspace root path")
    split_conversation_parser.add_argument("--doc-id", type=int, required=True, help="Document id to split")

    clear_conversation_parser = subparsers.add_parser(
        "clear-conversation-assignment",
        help="Clear a manual conversation pin and re-run automatic conversation assignment",
    )
    clear_conversation_parser.add_argument("workspace", help="Workspace root path")
    clear_conversation_parser.add_argument("--doc-id", type=int, required=True, help="Document id to clear")

    subparsers.add_parser("schema-version", help="Print the schema version")

    upgrade_workspace_parser = subparsers.add_parser(
        "upgrade-workspace",
        help="Replace the workspace's retriever_tools.py with the canonical plugin copy",
    )
    upgrade_workspace_parser.add_argument("workspace", help="Workspace root path")
    upgrade_workspace_parser.add_argument(
        "--from",
        dest="canonical_source",
        default=None,
        help="Path to the canonical retriever_tools.py (defaults to auto-discovery)",
    )
    upgrade_workspace_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the workspace tool even when it differs from runtime.json",
    )

    return parser


def _auto_upgrade_and_maybe_reexec(root: Path, command: str) -> None:
    """Auto-upgrade the workspace tool if it is cleanly stale.

    Writes one ``retriever-auto-upgrade: <json>`` line to stderr describing
    the outcome. When an upgrade actually happened, re-exec the freshly
    installed workspace tool so it handles the current command with its
    own code (not the currently-running, now-replaced image).
    """
    if command in AUTO_UPGRADE_EXEMPT_COMMANDS:
        return
    result = maybe_upgrade_workspace_tool(root)
    if not result:
        return
    try:
        print(
            "retriever-auto-upgrade: " + json.dumps(result, sort_keys=True),
            file=sys.stderr,
        )
    except Exception:  # pragma: no cover - never fail dispatch over logging
        pass

    if result.get("status") != "upgraded":
        return

    tool_path = result.get("tool_path")
    if not tool_path:
        return
    tool_path_obj = Path(str(tool_path))
    try:
        running = Path(__file__).resolve()
    except OSError:
        running = None
    try:
        upgraded = tool_path_obj.resolve()
    except OSError:
        upgraded = tool_path_obj
    if running is not None and upgraded == running:
        # We are already executing from the upgraded file (unusual, but
        # possible). Nothing to re-exec.
        return

    env = os.environ.copy()
    env["RETRIEVER_AUTO_UPGRADE_REEXEC"] = "1"
    try:
        os.execve(sys.executable, [sys.executable, str(tool_path_obj), *sys.argv[1:]], env)
    except OSError as exc:
        print(
            "retriever-auto-upgrade-reexec-failed: "
            + json.dumps({"error": f"{type(exc).__name__}: {exc}"}, sort_keys=True),
            file=sys.stderr,
        )


def main() -> int:
    benchmark_mark("main_entered")
    parser = build_parser()
    args = parser.parse_args()
    benchmark_mark("argparse_done", command=getattr(args, "command", None))

    try:
        if args.command == "schema-version":
            print(json.dumps({"schema_version": SCHEMA_VERSION, "tool_version": TOOL_VERSION}))
            return 0

        root = Path(args.workspace).expanduser().resolve()

        if args.command == "upgrade-workspace":
            canonical_source = getattr(args, "canonical_source", None)
            if canonical_source:
                canonical_path = Path(canonical_source).expanduser().resolve()
                if not canonical_path.is_file():
                    raise RetrieverError(
                        f"Canonical tool not found at --from path: {canonical_path}"
                    )
            else:
                located = locate_canonical_plugin_tool()
                if located is None:
                    # Fallback: the user invoked the canonical plugin tool
                    # directly (e.g. `python3 skills/tool-template/retriever_tools.py
                    # upgrade-workspace ...`). In that case __file__ *is* the
                    # canonical copy; ``locate_canonical_plugin_tool`` skips
                    # it to avoid self-upgrade loops, but here we want to
                    # honor it.
                    try:
                        self_path = Path(__file__).resolve()
                    except OSError:
                        self_path = None
                    if (
                        self_path is not None
                        and self_path.is_file()
                        and self_path.parent.name == "tool-template"
                        and self_path.parent.parent.name == "skills"
                    ):
                        located = self_path
                if located is None:
                    raise RetrieverError(
                        "Could not auto-discover the canonical retriever_tools.py. "
                        "Pass --from <path> or set RETRIEVER_CANONICAL_TOOL_PATH."
                    )
                canonical_path = located
            return emit_cli_payload(
                "upgrade-workspace",
                upgrade_workspace_tool(
                    root,
                    canonical_path,
                    force=bool(getattr(args, "force", False)),
                    reason="manual",
                ),
            )

        _auto_upgrade_and_maybe_reexec(root, args.command)

        if args.command == "doctor":
            return emit_cli_payload("doctor", doctor(root, args.quick))

        if args.command == "bootstrap":
            return emit_cli_payload("bootstrap", bootstrap(root))

        if args.command == "ingest":
            return emit_cli_payload("ingest", ingest(root, args.recursive, args.file_types))

        if args.command == "ingest-production":
            return emit_cli_payload("ingest-production", ingest_production(root, args.production_root))

        if args.command == "inspect-pst-properties":
            print(
                json.dumps(
                    inspect_pst_properties(
                        root,
                        args.pst_path,
                        source_item_ids=args.source_item_ids,
                        message_kind=args.message_kind,
                        limit=args.limit,
                        max_record_entries=args.max_record_entries,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "search":
            return emit_cli_payload(
                "search",
                search(
                    root,
                    args.query,
                    args.filters,
                    args.sort,
                    args.order,
                    args.page,
                    args.per_page,
                    args.columns,
                    args.mode,
                    compact_mode=(not args.verbose and args.mode == "compose"),
                ),
                verbose=args.verbose,
            )

        if args.command == "search-docs":
            return emit_cli_payload(
                "search-docs",
                search_docs(
                    root,
                    args.query,
                    args.filters,
                    args.sort,
                    args.order,
                    args.page,
                    args.per_page,
                    args.columns,
                    args.mode,
                    compact_mode=(not args.verbose and args.mode == "compose"),
                ),
                verbose=args.verbose,
            )

        if args.command == "slash":
            raw_command = " ".join(args.command_text).strip()
            payload = run_slash_command(root, raw_command)
            rendered_output = render_slash_read_only_output(raw_command, payload)
            if rendered_output is not None:
                sys.stdout.write(rendered_output + "\n")
                sys.stdout.flush()
            else:
                print(json.dumps(payload, indent=2, sort_keys=True))
            return 0

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
                    args.select_from_scope,
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
                        dataset_names=args.dataset_names,
                        query=args.query,
                        raw_bates=args.bates,
                        raw_filters=args.filters,
                        from_run_id=args.from_run_id,
                        select_from_scope=args.select_from_scope,
                        family_mode=args.family_mode,
                        seed_limit=args.seed_limit,
                        portable_workspace=args.portable_workspace,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "export-previews":
            print(
                json.dumps(
                    export_previews(
                        root,
                        args.output_path,
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
                    select_from_scope=args.select_from_scope,
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
                    select_from_scope=args.select_from_scope,
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
                        dataset_names=args.dataset_names,
                        document_ids=args.document_ids,
                        query=args.query,
                        raw_bates=args.bates,
                        raw_filters=args.filters,
                        from_run_id=args.from_run_id,
                        select_from_scope=args.select_from_scope,
                        activation_policy=args.activation_policy,
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

        if args.command == "reconcile-duplicates":
            return emit_cli_payload(
                "reconcile-duplicates",
                reconcile_duplicates(
                    root,
                    basis=args.basis,
                    apply_changes=bool(args.apply),
                ),
            )

        if args.command == "merge-into-conversation":
            return emit_cli_payload(
                "merge-into-conversation",
                merge_into_conversation(root, args.doc_id, args.target_doc_id),
            )

        if args.command == "split-from-conversation":
            return emit_cli_payload(
                "split-from-conversation",
                split_from_conversation(root, args.doc_id),
            )

        if args.command == "clear-conversation-assignment":
            return emit_cli_payload(
                "clear-conversation-assignment",
                clear_conversation_assignment(root, args.doc_id),
            )

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
