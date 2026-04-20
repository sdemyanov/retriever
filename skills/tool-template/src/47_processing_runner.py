def resolve_from_run_snapshot_rows(connection: sqlite3.Connection, from_run_id: int) -> list[sqlite3.Row]:
    run_row = connection.execute("SELECT id FROM runs WHERE id = ?", (from_run_id,)).fetchone()
    if run_row is None:
        raise RetrieverError(f"Unknown run id: {from_run_id}")
    return connection.execute(
        """
        SELECT *
        FROM run_snapshot_documents
        WHERE run_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (from_run_id,),
    ).fetchall()


def expand_seed_documents_with_family(
    connection: sqlite3.Connection,
    seed_document_ids: list[int],
    reasons_by_document_id: dict[int, dict[str, object]],
) -> list[int]:
    final_document_ids = list(seed_document_ids)
    seen_document_ids = set(seed_document_ids)
    if not seed_document_ids:
        return final_document_ids

    rows = connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE id IN ({', '.join('?' for _ in seed_document_ids)})
        """,
        seed_document_ids,
    ).fetchall()
    row_by_id = {int(row["id"]): row for row in rows}

    for seed_document_id in seed_document_ids:
        seed_row = row_by_id.get(seed_document_id)
        if seed_row is None:
            continue
        family_root_id = int(seed_row["parent_document_id"]) if seed_row["parent_document_id"] is not None else int(seed_row["id"])
        family_rows = connection.execute(
            """
            SELECT *
            FROM documents
            WHERE (id = ? OR parent_document_id = ?)
              AND lifecycle_status NOT IN ('missing', 'deleted')
            ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END ASC, id ASC
            """,
            (family_root_id, family_root_id, family_root_id),
        ).fetchall()
        for family_row in family_rows:
            family_document_id = int(family_row["id"])
            if family_document_id not in seen_document_ids:
                seen_document_ids.add(family_document_id)
                final_document_ids.append(family_document_id)
            reason_state = reasons_by_document_id.setdefault(
                family_document_id,
                {"direct_reasons": [], "family_seed_document_ids": []},
            )
            family_seed_ids = reason_state["family_seed_document_ids"]
            if seed_document_id not in family_seed_ids:
                family_seed_ids.append(seed_document_id)
    return final_document_ids


def compute_source_parts_content_hash(
    connection: sqlite3.Connection,
    *,
    root: Path,
    document_id: int,
) -> str | None:
    source_part_rows = connection.execute(
        """
        SELECT part_kind, rel_source_path, ordinal, label
        FROM document_source_parts
        WHERE document_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (document_id,),
    ).fetchall()
    if not source_part_rows:
        return None
    signature_parts: list[dict[str, object]] = []
    for row in source_part_rows:
        rel_source_path = normalize_whitespace(str(row["rel_source_path"] or ""))
        absolute_path = (root / rel_source_path).resolve() if rel_source_path else None
        file_hash = sha256_file(absolute_path) if absolute_path is not None and absolute_path.exists() else None
        signature_parts.append(
            {
                "part_kind": normalize_whitespace(str(row["part_kind"] or "")),
                "rel_source_path": rel_source_path,
                "ordinal": int(row["ordinal"] or 0),
                "label": normalize_whitespace(str(row["label"] or "")) or None,
                "file_hash": file_hash,
            }
        )
    return sha256_text(compact_json_text(signature_parts))


def compute_document_input_reference_for_job_version(
    connection: sqlite3.Connection,
    *,
    root: Path,
    document_row: sqlite3.Row,
    job_row: sqlite3.Row,
    job_version_row: sqlite3.Row,
    frozen_input_revision_id: int | None,
    frozen_content_hash: str | None,
) -> dict[str, object]:
    input_basis = str(job_version_row["input_basis"])
    job_kind = str(job_row["job_kind"])
    capability = normalize_whitespace(str(job_version_row["capability"] or ""))
    provider = normalize_whitespace(str(job_version_row["provider"] or ""))
    model = normalize_whitespace(str(job_version_row["model"] or ""))
    backend_id = capability or (f"{provider}:{model}" if model else provider)
    parameters = decode_json_text(job_version_row["parameters_json"], default={}) or {}
    if not isinstance(parameters, dict):
        parameters = {}

    if input_basis in {"active_search_text", "source_extract", "text_revision"}:
        if frozen_input_revision_id is not None:
            revision_id = int(frozen_input_revision_id)
        elif input_basis == "source_extract":
            revision_id = (
                int(document_row["source_text_revision_id"])
                if document_row["source_text_revision_id"] is not None
                else None
            )
        else:
            revision_id = (
                int(document_row["active_search_text_revision_id"])
                if document_row["active_search_text_revision_id"] is not None
                else None
            )
        if revision_id is None:
            raise RetrieverError(
                f"Document {document_row['id']} has no pinned text revision for input basis {input_basis!r}. Reingest the document first."
            )
        revision_row = connection.execute(
            """
            SELECT content_hash
            FROM text_revisions
            WHERE id = ? AND retracted_at IS NULL
            """,
            (revision_id,),
        ).fetchone()
        if revision_row is None:
            raise RetrieverError(f"Unknown active text revision id: {revision_id}")
        pinned_content_hash = frozen_content_hash or str(revision_row["content_hash"])
        if job_kind == "translation":
            target_language = (
                parameters.get("target_language")
                or parameters.get("target_lang")
                or parameters.get("language")
            )
            if not isinstance(target_language, str) or not target_language.strip():
                raise RetrieverError("Translation job versions require parameters_json.target_language.")
            input_identity = build_translation_input_identity(revision_id, target_language=target_language)
        else:
            input_identity = build_text_revision_input_identity(revision_id)
        return {
            "pinned_input_revision_id": revision_id,
            "pinned_input_identity": input_identity,
            "pinned_content_hash": pinned_content_hash,
        }

    if input_basis in {"source_file", "source_parts"}:
        source_hash = normalize_whitespace(str(frozen_content_hash or document_row["file_hash"] or ""))
        if not source_hash and input_basis == "source_parts":
            source_hash = normalize_whitespace(
                str(compute_source_parts_content_hash(connection, root=root, document_id=int(document_row["id"])) or "")
            )
        if not source_hash:
            raise RetrieverError(
                f"Document {document_row['id']} has no file hash for input basis {input_basis!r}."
            )
        if job_kind == "ocr":
            rendering_settings = parameters.get("rendering_settings")
            normalized_settings = rendering_settings if isinstance(rendering_settings, dict) else {}
            input_identity = build_ocr_input_identity(
                source_hash,
                rendering_settings=normalized_settings,
                backend_id=backend_id,
            )
        else:
            image_prep_settings = parameters.get("image_prep_settings")
            normalized_settings = image_prep_settings if isinstance(image_prep_settings, dict) else {}
            input_identity = build_image_source_input_identity(
                source_hash,
                image_prep_settings=normalized_settings,
                backend_id=backend_id,
            )
        return {
            "pinned_input_revision_id": None,
            "pinned_input_identity": input_identity,
            "pinned_content_hash": source_hash,
        }

    raise RetrieverError(f"Unsupported job input basis for run planning: {input_basis}")


def plan_scope_run_snapshot_rows(
    connection: sqlite3.Connection,
    *,
    root: Path,
    job_row: sqlite3.Row,
    job_version_row: sqlite3.Row,
    selector: dict[str, object],
    family_mode: str,
    seed_limit: int | None,
) -> list[dict[str, object]]:
    selected_documents, frozen_inputs_by_document_id = plan_scope_selected_documents(
        connection,
        selector=selector,
        family_mode=family_mode,
        seed_limit=seed_limit,
    )
    if not selected_documents:
        return []

    snapshot_rows: list[dict[str, object]] = []
    for selected_document in selected_documents:
        document_id = int(selected_document["document_id"])
        document_row = selected_document["document_row"]
        frozen_inputs = frozen_inputs_by_document_id.get(document_id, {})
        pinned_input = compute_document_input_reference_for_job_version(
            connection,
            root=root,
            document_row=document_row,
            job_row=job_row,
            job_version_row=job_version_row,
            frozen_input_revision_id=frozen_inputs.get("pinned_input_revision_id"),
            frozen_content_hash=frozen_inputs.get("pinned_content_hash"),
        )
        snapshot_rows.append(
            {
                "document_id": document_id,
                "ordinal": int(selected_document["ordinal"]),
                "inclusion_reason": selected_document["inclusion_reason"],
                "pinned_input_revision_id": pinned_input["pinned_input_revision_id"],
                "pinned_input_identity": pinned_input["pinned_input_identity"],
                "pinned_content_hash": pinned_input["pinned_content_hash"],
            }
        )
    return snapshot_rows


def scope_run_selector_has_inputs(selector: dict[str, object]) -> bool:
    return bool(scope_selector_instances(selector))


def scope_selector_reason_entries(scope: dict[str, object]) -> list[dict[str, object]]:
    reasons: list[dict[str, object]] = []
    keyword = normalize_inline_whitespace(str(scope.get("keyword") or ""))
    if keyword:
        reasons.append({"type": "keyword", "query": keyword})

    bates = scope.get("bates")
    if isinstance(bates, dict):
        begin = normalize_inline_whitespace(str(bates.get("begin") or ""))
        end = normalize_inline_whitespace(str(bates.get("end") or ""))
        if begin and end:
            reasons.append({"type": "bates", "begin": begin, "end": end})

    filter_expression = normalize_inline_whitespace(str(scope.get("filter") or ""))
    if filter_expression:
        reasons.append({"type": "filter", "expression": filter_expression})

    dataset_entries = coerce_scope_dataset_entries(scope.get("dataset"))
    if dataset_entries:
        reasons.append(
            {
                "type": "dataset",
                "dataset_ids": [int(entry["id"]) for entry in dataset_entries],
                "dataset_names": [str(entry["name"]) for entry in dataset_entries],
            }
        )

    if scope.get("from_run_id") is not None:
        reasons.append({"type": "from_run", "run_id": int(scope["from_run_id"])})
    return reasons


def resolve_seed_documents_for_scope_selector(
    connection: sqlite3.Connection,
    selector: dict[str, object],
) -> tuple[list[int], dict[int, dict[str, object]], dict[int, dict[str, object]]]:
    selector_instances = scope_selector_instances(selector)
    if not selector_instances:
        return [], {}, {}

    reasons_by_document_id: dict[int, dict[str, object]] = {}
    frozen_inputs_by_document_id: dict[int, dict[str, object]] = {}
    ordered_results: list[dict[str, object]] | None = None
    matching_document_ids: set[int] | None = None

    for selector_instance in selector_instances:
        selection = resolve_scope_document_search(connection, selector_instance)
        selection_scope = coerce_scope_payload(selection.get("scope"))
        selection_results = selection["results"]
        selection_document_ids = {int(item["id"]) for item in selection_results}
        if ordered_results is None:
            ordered_results = selection_results
            matching_document_ids = selection_document_ids
        else:
            assert matching_document_ids is not None
            matching_document_ids &= selection_document_ids

        direct_reasons = scope_selector_reason_entries(selection_scope)
        if direct_reasons:
            for item in selection_results:
                document_id = int(item["id"])
                reason_state = reasons_by_document_id.setdefault(
                    document_id,
                    {"direct_reasons": [], "family_seed_document_ids": []},
                )
                for reason in direct_reasons:
                    if reason not in reason_state["direct_reasons"]:
                        reason_state["direct_reasons"].append(reason)

        from_run_id = selection_scope.get("from_run_id")
        if from_run_id is not None:
            # Later selector clauses win so explicit --from-run-id narrows and pins over scope.
            for snapshot_row in resolve_from_run_snapshot_rows(connection, int(from_run_id)):
                document_id = int(snapshot_row["document_id"])
                frozen_inputs_by_document_id[document_id] = {
                    "pinned_input_revision_id": (
                        int(snapshot_row["pinned_input_revision_id"])
                        if snapshot_row["pinned_input_revision_id"] is not None
                        else None
                    ),
                    "pinned_content_hash": snapshot_row["pinned_content_hash"],
                }

    if ordered_results is None or matching_document_ids is None:
        return [], {}, {}

    ordered_matching_ids = [
        int(item["id"])
        for item in ordered_results
        if int(item["id"]) in matching_document_ids
    ]
    filtered_reasons = {
        document_id: reasons_by_document_id.get(
            document_id,
            {"direct_reasons": [], "family_seed_document_ids": []},
        )
        for document_id in ordered_matching_ids
    }
    filtered_frozen_inputs = {
        document_id: frozen_inputs
        for document_id, frozen_inputs in frozen_inputs_by_document_id.items()
        if document_id in matching_document_ids
    }
    return ordered_matching_ids, filtered_reasons, filtered_frozen_inputs


def plan_scope_selected_documents(
    connection: sqlite3.Connection,
    *,
    selector: dict[str, object],
    family_mode: str,
    seed_limit: int | None,
) -> tuple[list[dict[str, object]], dict[int, dict[str, object]]]:
    seed_document_ids, reasons_by_document_id, frozen_inputs_by_document_id = resolve_seed_documents_for_scope_selector(
        connection,
        selector,
    )
    if not seed_document_ids:
        return [], frozen_inputs_by_document_id

    if seed_limit is not None:
        seed_document_ids = seed_document_ids[:seed_limit]
    if family_mode == "with_family":
        final_document_ids = expand_seed_documents_with_family(connection, seed_document_ids, reasons_by_document_id)
    else:
        final_document_ids = list(seed_document_ids)
    if not final_document_ids:
        return [], frozen_inputs_by_document_id

    document_rows = connection.execute(
        f"""
        SELECT *
        FROM documents
        WHERE id IN ({', '.join('?' for _ in final_document_ids)})
        ORDER BY id ASC
        """,
        final_document_ids,
    ).fetchall()
    document_row_by_id = {int(row["id"]): row for row in document_rows}
    selected_documents: list[dict[str, object]] = []
    for ordinal, document_id in enumerate(final_document_ids):
        document_row = document_row_by_id.get(int(document_id))
        if document_row is None:
            continue
        selected_documents.append(
            {
                "document_id": int(document_id),
                "ordinal": ordinal,
                "document_row": document_row,
                "inclusion_reason": reasons_by_document_id.get(
                    int(document_id),
                    {"direct_reasons": [], "family_seed_document_ids": []},
                ),
            }
        )
    return selected_documents, frozen_inputs_by_document_id


def load_run_snapshot_rows(connection: sqlite3.Connection, run_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM run_snapshot_documents
        WHERE run_id = ?
        ORDER BY ordinal ASC, id ASC
        """,
        (run_id,),
    ).fetchall()


def load_text_input_for_snapshot_row(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    snapshot_row: sqlite3.Row,
) -> str | None:
    pinned_input_revision_id = snapshot_row["pinned_input_revision_id"]
    if pinned_input_revision_id is None:
        return None
    revision_row = require_text_revision_row_by_id(connection, int(pinned_input_revision_id))
    revision_body = read_text_revision_body(paths, revision_row["storage_rel_path"])
    if revision_body is None:
        raise RetrieverError(f"Text revision {pinned_input_revision_id} has no readable body on disk.")
    return revision_body


async def execute_run_async(
    connection: sqlite3.Connection,
    paths: dict[str, Path],
    *,
    run_id: int,
) -> dict[str, object]:
    run_row = connection.execute(
        """
        SELECT *
        FROM runs
        WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if run_row is None:
        raise RetrieverError(f"Unknown run id: {run_id}")
    job_version_row = require_job_version_row_by_id(connection, int(run_row["job_version_id"]))
    job_row = connection.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = ?
        """,
        (job_version_row["job_id"],),
    ).fetchone()
    assert job_row is not None
    job_kind = normalize_job_kind(str(job_row["job_kind"]))
    if job_kind not in {"structured_extraction", "translation"}:
        raise RetrieverError(
            "Run execution currently supports structured_extraction and translation jobs only."
        )
    snapshot_rows = load_run_snapshot_rows(connection, run_id)
    job_output_rows = connection.execute(
        """
        SELECT *
        FROM job_outputs
        WHERE job_id = ?
        ORDER BY ordinal ASC, output_name ASC, id ASC
        """,
        (job_row["id"],),
    ).fetchall()

    connection.execute(
        """
        UPDATE runs
        SET status = 'running',
            started_at = COALESCE(started_at, ?),
            completed_at = NULL,
            canceled_at = NULL
        WHERE id = ?
        """,
        (utc_now(), run_id),
    )
    connection.commit()

    for snapshot_row in snapshot_rows:
        run_item_row = ensure_run_item_row(
            connection,
            run_id=run_id,
            run_snapshot_document_id=int(snapshot_row["id"]),
            item_kind="document",
            document_id=int(snapshot_row["document_id"]),
            input_identity=str(snapshot_row["pinned_input_identity"]),
        )
        existing_result_row = find_active_result_row(
            connection,
            document_id=int(snapshot_row["document_id"]),
            job_version_id=int(job_version_row["id"]),
            input_identity=str(snapshot_row["pinned_input_identity"]),
        )
        if existing_result_row is not None:
            terminal_status = str(run_item_row["status"] or "")
            if terminal_status not in {"completed", "skipped"} or run_item_row["result_id"] != existing_result_row["id"]:
                update_run_item_row(
                    connection,
                    run_item_id=int(run_item_row["id"]),
                    status="skipped",
                    result_id=int(existing_result_row["id"]),
                    last_error=None,
                    completed_at=utc_now(),
                )
                connection.commit()
            continue

        started_at = utc_now()
        update_run_item_row(
            connection,
            run_item_id=int(run_item_row["id"]),
            status="running",
            result_id=None,
            last_error=None,
            started_at=(run_item_row["started_at"] or started_at),
            completed_at=None,
        )
        connection.commit()

        attempt_started = time.perf_counter()
        try:
            document_row = connection.execute(
                """
                SELECT *
                FROM documents
                WHERE id = ?
                """,
                (snapshot_row["document_id"],),
            ).fetchone()
            if document_row is None:
                raise RetrieverError(f"Unknown document id: {snapshot_row['document_id']}")
            text_input = load_text_input_for_snapshot_row(connection, paths, snapshot_row)
            provider_result = await execute_job_provider(
                job_row=job_row,
                job_version_row=job_version_row,
                job_output_rows=job_output_rows,
                document_row=document_row,
                text_input=text_input,
            )
            created_text_revision_id = None
            created_text_revision_payload = provider_result.get("created_text_revision")
            if isinstance(created_text_revision_payload, dict):
                created_text_content = str(created_text_revision_payload.get("text_content") or "")
                created_text_revision_id = create_text_revision_row(
                    connection,
                    paths,
                    document_id=int(snapshot_row["document_id"]),
                    revision_kind=str(created_text_revision_payload.get("revision_kind") or "translation"),
                    text_content=created_text_content,
                    language=(
                        str(created_text_revision_payload["language"])
                        if created_text_revision_payload.get("language")
                        else None
                    ),
                    parent_revision_id=(
                        int(snapshot_row["pinned_input_revision_id"])
                        if snapshot_row["pinned_input_revision_id"] is not None
                        else None
                    ),
                    created_by_job_version_id=int(job_version_row["id"]),
                    quality_score=None,
                    provider_metadata=provider_result.get("provider_metadata"),  # type: ignore[arg-type]
                )
            latency_ms = int((time.perf_counter() - attempt_started) * 1000)
            result_id, created = create_result_row(
                connection,
                run_id=run_id,
                document_id=int(snapshot_row["document_id"]),
                job_version_id=int(job_version_row["id"]),
                input_revision_id=(
                    int(snapshot_row["pinned_input_revision_id"])
                    if snapshot_row["pinned_input_revision_id"] is not None
                    else None
                ),
                input_identity=str(snapshot_row["pinned_input_identity"]),
                raw_output=provider_result["raw_output"],
                normalized_output=provider_result["normalized_output"],
                created_text_revision_id=created_text_revision_id,
                provider_metadata=provider_result["provider_metadata"],  # type: ignore[arg-type]
            )
            if created and job_output_rows:
                upsert_result_output_rows(
                    connection,
                    result_id=result_id,
                    job_output_rows=job_output_rows,
                    output_values_by_name=provider_result["output_values"],  # type: ignore[arg-type]
                )
            create_attempt_row(
                connection,
                run_item_id=int(run_item_row["id"]),
                provider_request_id=provider_result["provider_request_id"],  # type: ignore[arg-type]
                input_tokens=provider_result["input_tokens"],  # type: ignore[arg-type]
                output_tokens=provider_result["output_tokens"],  # type: ignore[arg-type]
                cost_cents=provider_result["cost_cents"],  # type: ignore[arg-type]
                latency_ms=latency_ms,
                provider_metadata=provider_result["provider_metadata"],  # type: ignore[arg-type]
                error_summary=None,
            )
            update_run_item_row(
                connection,
                run_item_id=int(run_item_row["id"]),
                status="completed",
                result_id=result_id,
                last_error=None,
                completed_at=utc_now(),
                increment_attempt_count=True,
            )
            connection.commit()
        except Exception as exc:
            latency_ms = int((time.perf_counter() - attempt_started) * 1000)
            error_summary = f"{type(exc).__name__}: {exc}"
            create_attempt_row(
                connection,
                run_item_id=int(run_item_row["id"]),
                provider_request_id=None,
                input_tokens=None,
                output_tokens=None,
                cost_cents=None,
                latency_ms=latency_ms,
                provider_metadata={"phase": "execute_run"},
                error_summary=error_summary,
            )
            update_run_item_row(
                connection,
                run_item_id=int(run_item_row["id"]),
                status="failed",
                result_id=None,
                last_error=error_summary,
                completed_at=utc_now(),
                increment_attempt_count=True,
            )
            connection.commit()

    refresh_run_progress(connection, run_id)
    connection.commit()
    return {
        "status": "ok",
        "run": run_summary_by_id(connection, run_id),
        "run_items": list_run_item_payloads_for_run(connection, run_id),
        "results": list_result_summaries(connection, run_id=run_id),
    }
