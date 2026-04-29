def build_content_type_by_extension() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for content_type, raw_extensions in CONTENT_TYPE_EXTENSION_GROUPS:
        for extension in raw_extensions.split():
            normalized_extension = extension.strip().lower().strip(",")
            if normalized_extension:
                mapping.setdefault(normalized_extension, content_type)
    return mapping


CONTENT_TYPE_BY_EXTENSION = build_content_type_by_extension()

ATTACHMENT_SUFFIX_PATTERN = re.compile(
    r"^(?P<title>.+?)\s+(?P<label>(?:and\s+(?:back\s*up|backup)\s+)?attachments?)\s*:\s*(?P<attachments>.+)$",
    re.IGNORECASE,
)
HTML_PREVIEW_ATTACHMENT_LINKS_PATTERN = re.compile(
    r"<!-- RETRIEVER_ATTACHMENT_LINKS_START -->.*?<!-- RETRIEVER_ATTACHMENT_LINKS_END -->",
    re.DOTALL,
)
HTML_PREVIEW_CALENDAR_INVITES_PATTERN = re.compile(
    r"<!-- RETRIEVER_CALENDAR_INVITES_START -->.*?<!-- RETRIEVER_CALENDAR_INVITES_END -->",
    re.DOTALL,
)

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
      dataset_name_normalized TEXT,
      allow_auto_merge INTEGER NOT NULL DEFAULT 1,
      email_auto_merge INTEGER NOT NULL DEFAULT 1,
      handle_auto_merge INTEGER NOT NULL DEFAULT 1,
      phone_auto_merge INTEGER NOT NULL DEFAULT 0,
      name_auto_merge INTEGER NOT NULL DEFAULT 0,
      external_id_auto_merge_names_json TEXT NOT NULL DEFAULT '[]',
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
    CREATE TABLE IF NOT EXISTS conversations (
      id INTEGER PRIMARY KEY,
      source_kind TEXT NOT NULL,
      source_locator TEXT NOT NULL,
      conversation_key TEXT NOT NULL,
      conversation_type TEXT NOT NULL,
      display_name TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS documents (
      id INTEGER PRIMARY KEY,
      control_number TEXT UNIQUE,
      canonical_kind TEXT NOT NULL DEFAULT 'unknown',
      canonical_status TEXT NOT NULL DEFAULT 'active',
      merged_into_document_id INTEGER REFERENCES documents(id) ON DELETE SET NULL,
      conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
      conversation_assignment_mode TEXT NOT NULL DEFAULT 'auto',
      dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
      parent_document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
      child_document_kind TEXT,
      source_kind TEXT,
      source_rel_path TEXT,
      source_item_id TEXT,
      root_message_key TEXT,
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
      custodians_json TEXT NOT NULL DEFAULT '[]',
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
    CREATE TABLE IF NOT EXISTS document_occurrences (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      dataset_source_id INTEGER REFERENCES dataset_sources(id) ON DELETE SET NULL,
      parent_occurrence_id INTEGER REFERENCES document_occurrences(id) ON DELETE SET NULL,
      occurrence_control_number TEXT,
      source_kind TEXT,
      source_rel_path TEXT,
      source_item_id TEXT,
      source_folder_path TEXT,
      production_id INTEGER REFERENCES productions(id) ON DELETE SET NULL,
      begin_bates TEXT,
      end_bates TEXT,
      begin_attachment TEXT,
      end_attachment TEXT,
      rel_path TEXT NOT NULL,
      file_name TEXT NOT NULL,
      file_type TEXT,
      mime_type TEXT,
      file_size INTEGER,
      file_hash TEXT,
      custodian TEXT,
      fs_created_at TEXT,
      fs_modified_at TEXT,
      extracted_author TEXT,
      extracted_title TEXT,
      extracted_subject TEXT,
      extracted_participants TEXT,
      extracted_recipients TEXT,
      extracted_doc_authored_at TEXT,
      extracted_doc_modified_at TEXT,
      extracted_content_type TEXT,
      extracted_kind TEXT,
      entity_hints_json TEXT NOT NULL DEFAULT '{}',
      text_status TEXT NOT NULL DEFAULT 'ok',
      lifecycle_status TEXT NOT NULL DEFAULT 'active',
      has_preview INTEGER NOT NULL DEFAULT 0,
      ingested_at TEXT,
      last_seen_at TEXT,
      updated_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entities (
      id INTEGER PRIMARY KEY,
      entity_type TEXT NOT NULL DEFAULT 'person',
      display_name TEXT,
      primary_email TEXT,
      primary_phone TEXT,
      sort_name TEXT,
      notes TEXT,
      display_name_source TEXT NOT NULL DEFAULT 'auto',
      entity_origin TEXT NOT NULL DEFAULT 'observed',
      canonical_status TEXT NOT NULL DEFAULT 'active',
      merged_into_entity_id INTEGER REFERENCES entities(id) ON DELETE SET NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      CHECK (entity_type IN ('person', 'organization', 'shared_mailbox', 'system_mailbox', 'unknown')),
      CHECK (display_name_source IN ('auto', 'manual')),
      CHECK (entity_origin IN ('observed', 'identified', 'manual')),
      CHECK (canonical_status IN ('active', 'merged', 'ignored')),
      CHECK (
        (canonical_status = 'merged' AND merged_into_entity_id IS NOT NULL)
        OR (canonical_status != 'merged' AND merged_into_entity_id IS NULL)
      )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entity_identifiers (
      id INTEGER PRIMARY KEY,
      entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
      identifier_type TEXT NOT NULL,
      display_value TEXT NOT NULL,
      normalized_value TEXT NOT NULL,
      provider TEXT,
      provider_scope TEXT,
      identifier_name TEXT,
      identifier_scope TEXT,
      parsed_name_json TEXT,
      parsed_phone_json TEXT,
      normalized_full_name TEXT,
      normalized_sort_name TEXT,
      is_primary INTEGER NOT NULL DEFAULT 0,
      is_verified INTEGER NOT NULL DEFAULT 0,
      source_kind TEXT NOT NULL DEFAULT 'auto',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      CHECK (identifier_type IN ('email', 'phone', 'name', 'handle', 'external_id'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entity_resolution_keys (
      id INTEGER PRIMARY KEY,
      entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
      identifier_id INTEGER REFERENCES entity_identifiers(id) ON DELETE CASCADE,
      key_type TEXT NOT NULL,
      provider TEXT,
      provider_scope TEXT,
      identifier_name TEXT,
      identifier_scope TEXT,
      normalized_value TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_entities (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
      role TEXT NOT NULL,
      ordinal INTEGER NOT NULL DEFAULT 0,
      assignment_mode TEXT NOT NULL DEFAULT 'auto',
      observed_title TEXT,
      evidence_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      CHECK (role IN ('author', 'participant', 'recipient', 'custodian')),
      CHECK (assignment_mode IN ('auto', 'manual'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entity_overrides (
      id INTEGER PRIMARY KEY,
      scope_type TEXT NOT NULL,
      scope_id INTEGER,
      role TEXT,
      source_entity_id INTEGER REFERENCES entities(id) ON DELETE SET NULL,
      normalized_candidate_key TEXT,
      replacement_entity_id INTEGER REFERENCES entities(id) ON DELETE SET NULL,
      override_effect TEXT NOT NULL,
      source_hint TEXT,
      reason TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      CHECK (scope_type IN ('document', 'global')),
      CHECK (override_effect IN ('replace', 'remove', 'ignore')),
      CHECK (
        (scope_type = 'document' AND scope_id IS NOT NULL AND override_effect IN ('replace', 'remove'))
        OR (scope_type = 'global' AND scope_id IS NULL AND override_effect = 'ignore')
      ),
      CHECK (
        (override_effect = 'replace' AND replacement_entity_id IS NOT NULL)
        OR (override_effect IN ('remove', 'ignore') AND replacement_entity_id IS NULL)
      ),
      CHECK (
        override_effect != 'ignore'
        OR source_entity_id IS NOT NULL
        OR normalized_candidate_key IS NOT NULL
      )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entity_merge_blocks (
      id INTEGER PRIMARY KEY,
      left_entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
      right_entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
      reason TEXT,
      created_at TEXT NOT NULL,
      CHECK (left_entity_id < right_entity_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_dedupe_keys (
      id INTEGER PRIMARY KEY,
      basis TEXT NOT NULL,
      key_value TEXT NOT NULL,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS canonical_metadata_conflicts (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      field_name TEXT NOT NULL,
      occurrence_id INTEGER NOT NULL REFERENCES document_occurrences(id) ON DELETE CASCADE,
      value TEXT,
      first_seen_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_control_number_aliases (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      occurrence_id INTEGER REFERENCES document_occurrences(id) ON DELETE CASCADE,
      alias_value TEXT NOT NULL,
      alias_type TEXT NOT NULL,
      active_flag INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_merge_events (
      id INTEGER PRIMARY KEY,
      survivor_document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      loser_document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      merge_basis TEXT NOT NULL,
      actor TEXT,
      schema_version INTEGER NOT NULL,
      pre_merge_survivor_json TEXT NOT NULL,
      pre_merge_loser_json TEXT NOT NULL,
      artifact_counts_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_field_conflicts (
      id INTEGER PRIMARY KEY,
      merge_event_id INTEGER REFERENCES document_merge_events(id) ON DELETE CASCADE,
      document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
      field_name TEXT NOT NULL,
      survivor_value TEXT,
      loser_value TEXT,
      resolution TEXT,
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_email_threading (
      document_id INTEGER PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
      message_id TEXT,
      in_reply_to TEXT,
      references_json TEXT NOT NULL DEFAULT '[]',
      conversation_index TEXT,
      conversation_topic TEXT,
      normalized_subject TEXT,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_chat_threading (
      document_id INTEGER PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
      thread_id TEXT,
      message_id TEXT,
      parent_message_id TEXT,
      thread_type TEXT,
      participants_json TEXT NOT NULL DEFAULT '[]',
      updated_at TEXT NOT NULL
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
      target_fragment TEXT,
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
    CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY,
      job_name TEXT NOT NULL UNIQUE,
      job_kind TEXT NOT NULL,
      description TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      archived_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS job_outputs (
      id INTEGER PRIMARY KEY,
      job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      output_name TEXT NOT NULL,
      value_type TEXT NOT NULL DEFAULT 'text',
      bound_custom_field TEXT,
      description TEXT,
      ordinal INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(job_id, output_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS job_versions (
      id INTEGER PRIMARY KEY,
      job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      version INTEGER NOT NULL,
      display_name TEXT NOT NULL,
      instruction_text TEXT NOT NULL DEFAULT '',
      instruction_hash TEXT NOT NULL,
      response_schema_json TEXT,
      capability TEXT NOT NULL,
      provider TEXT NOT NULL,
      model TEXT,
      parameters_json TEXT NOT NULL DEFAULT '{}',
      input_basis TEXT NOT NULL,
      segment_profile TEXT,
      aggregation_strategy TEXT,
      created_at TEXT NOT NULL,
      archived_at TEXT,
      UNIQUE(job_id, version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS text_revisions (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      revision_kind TEXT NOT NULL,
      language TEXT,
      parent_revision_id INTEGER REFERENCES text_revisions(id) ON DELETE SET NULL,
      created_by_job_version_id INTEGER REFERENCES job_versions(id) ON DELETE SET NULL,
      storage_rel_path TEXT,
      content_hash TEXT NOT NULL,
      char_count INTEGER,
      token_estimate INTEGER,
      quality_score REAL,
      provider_metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      retracted_at TEXT,
      retraction_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS text_revision_segments (
      id INTEGER PRIMARY KEY,
      revision_id INTEGER NOT NULL REFERENCES text_revisions(id) ON DELETE CASCADE,
      segment_profile TEXT NOT NULL,
      level INTEGER NOT NULL DEFAULT 0,
      parent_segment_id INTEGER REFERENCES text_revision_segments(id) ON DELETE CASCADE,
      ordinal INTEGER NOT NULL,
      char_start INTEGER NOT NULL,
      char_end INTEGER NOT NULL,
      token_estimate INTEGER,
      text_hash TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(revision_id, segment_profile, level, ordinal)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
      id INTEGER PRIMARY KEY,
      job_version_id INTEGER NOT NULL REFERENCES job_versions(id) ON DELETE CASCADE,
      from_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
      selector_json TEXT NOT NULL,
      exclude_selector_json TEXT NOT NULL DEFAULT '{}',
      activation_policy TEXT NOT NULL DEFAULT 'manual',
      family_mode TEXT NOT NULL DEFAULT 'exact',
      seed_limit INTEGER,
      status TEXT NOT NULL DEFAULT 'planned',
      planned_count INTEGER NOT NULL DEFAULT 0,
      completed_count INTEGER NOT NULL DEFAULT 0,
      failed_count INTEGER NOT NULL DEFAULT 0,
      skipped_count INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      canceled_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_snapshot_documents (
      id INTEGER PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      ordinal INTEGER NOT NULL,
      inclusion_reason_json TEXT NOT NULL DEFAULT '{}',
      pinned_input_revision_id INTEGER REFERENCES text_revisions(id) ON DELETE SET NULL,
      pinned_input_identity TEXT NOT NULL,
      pinned_content_hash TEXT,
      created_at TEXT NOT NULL,
      UNIQUE(run_id, document_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_items (
      id INTEGER PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
      run_snapshot_document_id INTEGER REFERENCES run_snapshot_documents(id) ON DELETE CASCADE,
      item_kind TEXT NOT NULL,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      page_number INTEGER,
      segment_id INTEGER REFERENCES text_revision_segments(id) ON DELETE CASCADE,
      input_artifact_rel_path TEXT,
      input_identity TEXT NOT NULL,
      result_id INTEGER REFERENCES results(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      claimed_by TEXT,
      claimed_at TEXT,
      last_heartbeat_at TEXT,
      attempt_count INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_workers (
      id INTEGER PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
      claimed_by TEXT NOT NULL,
      launch_mode TEXT NOT NULL DEFAULT 'inline',
      worker_task_id TEXT,
      status TEXT NOT NULL DEFAULT 'active',
      max_batches INTEGER,
      batches_prepared INTEGER NOT NULL DEFAULT 0,
      items_completed INTEGER NOT NULL DEFAULT 0,
      items_failed INTEGER NOT NULL DEFAULT 0,
      last_heartbeat_at TEXT,
      last_error TEXT,
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      cancel_requested_at TEXT,
      summary_json TEXT NOT NULL DEFAULT '{}',
      UNIQUE(run_id, claimed_by)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ocr_page_outputs (
      id INTEGER PRIMARY KEY,
      run_item_id INTEGER NOT NULL REFERENCES run_items(id) ON DELETE CASCADE,
      run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      page_number INTEGER NOT NULL,
      text_content TEXT NOT NULL,
      raw_output_json TEXT,
      normalized_output_json TEXT,
      provider_metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      UNIQUE(run_item_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS image_description_page_outputs (
      id INTEGER PRIMARY KEY,
      run_item_id INTEGER NOT NULL REFERENCES run_items(id) ON DELETE CASCADE,
      run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      page_number INTEGER NOT NULL,
      text_content TEXT NOT NULL,
      raw_output_json TEXT,
      normalized_output_json TEXT,
      provider_metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      UNIQUE(run_item_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS attempts (
      id INTEGER PRIMARY KEY,
      run_item_id INTEGER NOT NULL REFERENCES run_items(id) ON DELETE CASCADE,
      attempt_number INTEGER NOT NULL,
      provider_request_id TEXT,
      input_tokens INTEGER,
      output_tokens INTEGER,
      cost_cents INTEGER,
      latency_ms INTEGER,
      provider_metadata_json TEXT NOT NULL DEFAULT '{}',
      error_summary TEXT,
      created_at TEXT NOT NULL,
      UNIQUE(run_item_id, attempt_number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS results (
      id INTEGER PRIMARY KEY,
      run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      job_version_id INTEGER NOT NULL REFERENCES job_versions(id) ON DELETE CASCADE,
      input_revision_id INTEGER REFERENCES text_revisions(id) ON DELETE SET NULL,
      input_identity TEXT NOT NULL,
      raw_output_json TEXT,
      normalized_output_json TEXT,
      created_text_revision_id INTEGER REFERENCES text_revisions(id) ON DELETE SET NULL,
      provider_metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      retracted_at TEXT,
      retraction_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS result_outputs (
      id INTEGER PRIMARY KEY,
      result_id INTEGER NOT NULL REFERENCES results(id) ON DELETE CASCADE,
      job_output_id INTEGER NOT NULL REFERENCES job_outputs(id) ON DELETE CASCADE,
      output_value_json TEXT NOT NULL,
      display_value TEXT,
      score REAL,
      created_at TEXT NOT NULL,
      UNIQUE(result_id, job_output_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS embedding_vectors (
      id INTEGER PRIMARY KEY,
      job_version_id INTEGER NOT NULL REFERENCES job_versions(id) ON DELETE CASCADE,
      revision_id INTEGER REFERENCES text_revisions(id) ON DELETE CASCADE,
      segment_id INTEGER NOT NULL REFERENCES text_revision_segments(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      level INTEGER NOT NULL DEFAULT 0,
      vector_blob BLOB NOT NULL,
      encoding TEXT NOT NULL DEFAULT 'float32-le',
      dimensions INTEGER NOT NULL,
      distance_metric TEXT NOT NULL DEFAULT 'cosine',
      provider_metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      retracted_at TEXT,
      retraction_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS publications (
      id INTEGER PRIMARY KEY,
      result_output_id INTEGER NOT NULL REFERENCES result_outputs(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      job_output_id INTEGER NOT NULL REFERENCES job_outputs(id) ON DELETE CASCADE,
      custom_field_name TEXT NOT NULL,
      published_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS text_revision_activation_events (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      text_revision_id INTEGER NOT NULL REFERENCES text_revisions(id) ON DELETE CASCADE,
      activated_by_job_version_id INTEGER REFERENCES job_versions(id) ON DELETE SET NULL,
      source_result_id INTEGER REFERENCES results(id) ON DELETE SET NULL,
      activation_policy TEXT,
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
    """
    CREATE TABLE IF NOT EXISTS ingest_runs (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL UNIQUE,
      scope_json TEXT NOT NULL DEFAULT '{}',
      recursive INTEGER NOT NULL DEFAULT 0,
      raw_file_types TEXT,
      pipeline_schema_version INTEGER NOT NULL,
      phase TEXT NOT NULL DEFAULT 'planning',
      status TEXT NOT NULL DEFAULT 'planning',
      prepare_worker_soft_limit INTEGER NOT NULL DEFAULT 4,
      committer_lease_owner TEXT,
      committer_lease_expires_at TEXT,
      committer_heartbeat_at TEXT,
      entity_graph_stale INTEGER NOT NULL DEFAULT 0,
      entity_policy_snapshot_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      cancel_requested_at TEXT,
      last_heartbeat_at TEXT,
      error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_work_items (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
      unit_type TEXT NOT NULL,
      source_kind TEXT,
      source_key TEXT,
      rel_path TEXT,
      commit_order INTEGER,
      parent_order INTEGER,
      spawned_by_work_item_id INTEGER REFERENCES ingest_work_items(id) ON DELETE SET NULL,
      payload_json TEXT NOT NULL DEFAULT '{}',
      affected_document_ids_json TEXT NOT NULL DEFAULT '[]',
      affected_conversation_keys_json TEXT NOT NULL DEFAULT '[]',
      affected_entity_ids_json TEXT NOT NULL DEFAULT '[]',
      artifact_manifest_json TEXT NOT NULL DEFAULT '{}',
      status TEXT NOT NULL DEFAULT 'pending',
      lease_owner TEXT,
      lease_expires_at TEXT,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_prepared_items (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
      work_item_id INTEGER NOT NULL REFERENCES ingest_work_items(id) ON DELETE CASCADE,
      payload_kind TEXT NOT NULL,
      payload_json TEXT NOT NULL DEFAULT '{}',
      spill_rel_path TEXT,
      payload_bytes INTEGER NOT NULL DEFAULT 0,
      source_fingerprint_json TEXT NOT NULL DEFAULT '{}',
      prepared_at TEXT NOT NULL,
      error_json TEXT NOT NULL DEFAULT '{}',
      UNIQUE(work_item_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_rename_consumptions (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
      target_work_item_id INTEGER NOT NULL REFERENCES ingest_work_items(id) ON DELETE CASCADE,
      source_document_id INTEGER,
      source_occurrence_id INTEGER,
      file_hash TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(run_id, source_occurrence_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_phase_cursors (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
      phase TEXT NOT NULL,
      cursor_key TEXT NOT NULL,
      cursor_json TEXT NOT NULL DEFAULT '{}',
      status TEXT NOT NULL DEFAULT 'pending',
      updated_at TEXT NOT NULL,
      UNIQUE(run_id, phase, cursor_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_worker_events (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
      worker_id TEXT,
      event_type TEXT NOT NULL,
      work_item_id INTEGER REFERENCES ingest_work_items(id) ON DELETE SET NULL,
      phase TEXT,
      duration_ms REAL,
      details_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_artifact_sweeps (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
      work_item_id INTEGER REFERENCES ingest_work_items(id) ON DELETE SET NULL,
      artifact_kind TEXT NOT NULL,
      temp_rel_path TEXT,
      canonical_rel_path TEXT,
      content_hash TEXT,
      state TEXT NOT NULL DEFAULT 'staged',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entity_rebuild_runs (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL UNIQUE,
      mode TEXT NOT NULL DEFAULT 'full',
      phase TEXT NOT NULL DEFAULT 'resetting',
      status TEXT NOT NULL DEFAULT 'resetting',
      document_ids_json TEXT NOT NULL DEFAULT '[]',
      batch_size INTEGER NOT NULL DEFAULT 500,
      reset_stage TEXT NOT NULL DEFAULT 'document_entities',
      reset_counts_json TEXT NOT NULL DEFAULT '{}',
      cursor_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      cancel_requested_at TEXT,
      last_heartbeat_at TEXT,
      error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entity_rebuild_items (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES entity_rebuild_runs(run_id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      ordinal INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',
      lease_owner TEXT,
      lease_expires_at TEXT,
      attempts INTEGER NOT NULL DEFAULT 0,
      document_synced INTEGER NOT NULL DEFAULT 0,
      auto_links_created INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(run_id, document_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS export_runs (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL UNIQUE,
      export_kind TEXT NOT NULL,
      output_path TEXT NOT NULL,
      output_rel_path TEXT,
      selector_json TEXT NOT NULL DEFAULT '{}',
      config_json TEXT NOT NULL DEFAULT '{}',
      cursor_json TEXT NOT NULL DEFAULT '{}',
      phase TEXT NOT NULL DEFAULT 'exporting',
      status TEXT NOT NULL DEFAULT 'exporting',
      total_items INTEGER NOT NULL DEFAULT 0,
      completed_items INTEGER NOT NULL DEFAULT 0,
      failed_items INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      last_heartbeat_at TEXT,
      error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS export_work_items (
      id INTEGER PRIMARY KEY,
      run_id TEXT NOT NULL REFERENCES export_runs(run_id) ON DELETE CASCADE,
      unit_type TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      payload_json TEXT NOT NULL DEFAULT '{}',
      artifact_manifest_json TEXT NOT NULL DEFAULT '{}',
      status TEXT NOT NULL DEFAULT 'pending',
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(run_id, ordinal)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_documents_file_hash ON documents(file_hash)",
    "CREATE INDEX IF NOT EXISTS idx_documents_content_hash ON documents(content_hash)",
    "CREATE INDEX IF NOT EXISTS idx_documents_lifecycle_status ON documents(lifecycle_status)",
    "CREATE INDEX IF NOT EXISTS idx_document_source_parts_document_id ON document_source_parts(document_id, part_kind, ordinal)",
    "CREATE INDEX IF NOT EXISTS idx_previews_document_id ON document_previews(document_id, ordinal)",
    "CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON document_chunks(document_id, chunk_index)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_runs_status ON ingest_runs(status, phase)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_runs_cancel_requested ON ingest_runs(cancel_requested_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_work_items_run_status ON ingest_work_items(run_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_work_items_commit_order ON ingest_work_items(run_id, commit_order, id)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_work_items_lease_expires ON ingest_work_items(lease_expires_at)",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ingest_work_items_source_unique
    ON ingest_work_items(run_id, unit_type, COALESCE(source_key, ''), COALESCE(rel_path, ''), COALESCE(parent_order, -1))
    """,
    "CREATE INDEX IF NOT EXISTS idx_ingest_prepared_items_run_work ON ingest_prepared_items(run_id, work_item_id)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_phase_cursors_run_phase ON ingest_phase_cursors(run_id, phase, status)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_worker_events_run_created ON ingest_worker_events(run_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_artifact_sweeps_run_state ON ingest_artifact_sweeps(run_id, state)",
    "CREATE INDEX IF NOT EXISTS idx_entity_rebuild_runs_status ON entity_rebuild_runs(status, phase)",
    "CREATE INDEX IF NOT EXISTS idx_entity_rebuild_items_run_status ON entity_rebuild_items(run_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_entity_rebuild_items_lease ON entity_rebuild_items(lease_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_export_runs_kind_status ON export_runs(export_kind, status, phase)",
    "CREATE INDEX IF NOT EXISTS idx_export_work_items_run_status ON export_work_items(run_id, status, ordinal)",
]


class RetrieverError(RuntimeError):
    pass


class RetrieverStructuredError(RetrieverError):
    def __init__(self, message: str, payload: dict[str, object]):
        super().__init__(message)
        self.payload = payload


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
    tmp_dir = state_dir / "tmp"
    ingest_tmp_dir = tmp_dir / "ingest"
    locks_dir = state_dir / "locks"
    return {
        "root": root,
        "state_dir": state_dir,
        "db_path": state_dir / "retriever.db",
        "session_path": state_dir / "session.json",
        "saved_scopes_path": state_dir / "saved_scopes.json",
        "previews_dir": state_dir / "previews",
        "text_revisions_dir": state_dir / "text-revisions",
        "jobs_dir": state_dir / "jobs",
        "logs_dir": state_dir / "logs",
        "runtime_path": state_dir / "runtime.json",
        "tmp_dir": tmp_dir,
        "ingest_tmp_dir": ingest_tmp_dir,
        "locks_dir": locks_dir,
        "ingest_lock_path": locks_dir / "ingest.lock",
        "entity_rebuild_lock_path": locks_dir / "entity-rebuild.lock",
    }


def ensure_layout(paths: dict[str, Path]) -> None:
    paths["state_dir"].mkdir(parents=True, exist_ok=True)
    for key in (
        "previews_dir",
        "text_revisions_dir",
        "jobs_dir",
        "logs_dir",
        "tmp_dir",
        "ingest_tmp_dir",
        "locks_dir",
    ):
        paths[key].mkdir(parents=True, exist_ok=True)


def set_active_workspace_root(root: Path | None) -> None:
    global ACTIVE_WORKSPACE_ROOT
    if root is None:
        ACTIVE_WORKSPACE_ROOT = None
        return
    ACTIVE_WORKSPACE_ROOT = Path(root).expanduser().resolve()


def active_workspace_paths() -> dict[str, Path] | None:
    if ACTIVE_WORKSPACE_ROOT is None:
        return None
    return workspace_paths(ACTIVE_WORKSPACE_ROOT)


def _venv_python_rel_path() -> Path:
    return Path("Scripts/python.exe") if os.name == "nt" else Path("bin/python")


def _canonical_plugin_root_from_tool_path(path: Path) -> Path | None:
    if (
        path.is_file()
        and path.parent.name == "tool-template"
        and path.parent.parent.name == "skills"
    ):
        return path.parent.parent.parent
    return None


def resolve_plugin_root(root: Path | None = None, *, current_file: str | None = None) -> Path | None:
    env_root = normalize_whitespace(os.environ.get("RETRIEVER_PLUGIN_ROOT") or "")
    if env_root:
        try:
            return Path(env_root).expanduser().resolve()
        except OSError:
            return Path(env_root).expanduser()

    env_tool = normalize_whitespace(os.environ.get("RETRIEVER_CANONICAL_TOOL_PATH") or "")
    if env_tool:
        try:
            candidate_root = _canonical_plugin_root_from_tool_path(Path(env_tool).expanduser().resolve())
            if candidate_root is not None:
                return candidate_root
        except OSError:
            pass

    canonical_tool = locate_canonical_plugin_tool(current_file=current_file)
    if canonical_tool is not None:
        candidate_root = _canonical_plugin_root_from_tool_path(canonical_tool)
        if candidate_root is not None:
            return candidate_root

    candidate_file = current_file or __file__
    if candidate_file:
        try:
            current_path = Path(candidate_file).resolve()
        except OSError:
            current_path = None
        if current_path is not None:
            candidate_root = _canonical_plugin_root_from_tool_path(current_path)
            if candidate_root is not None:
                return candidate_root

    if root is not None:
        try:
            runtime = read_runtime(workspace_paths(Path(root).expanduser().resolve())["runtime_path"])
        except Exception:
            runtime = None
        if isinstance(runtime, dict):
            runtime_payload = runtime.get("plugin_runtime")
            if isinstance(runtime_payload, dict):
                plugin_root_value = normalize_whitespace(str(runtime_payload.get("plugin_root") or ""))
                if plugin_root_value:
                    try:
                        return Path(plugin_root_value).expanduser().resolve()
                    except OSError:
                        return Path(plugin_root_value).expanduser()
    return None


def plugin_runtime_environment_key() -> str:
    system = re.sub(r"[^a-z0-9]+", "-", platform.system().lower()).strip("-") or "unknown-system"
    machine = re.sub(r"[^a-z0-9]+", "-", platform.machine().lower()).strip("-") or "unknown-machine"
    return f"{system}-{machine}-py{sys.version_info.major}.{sys.version_info.minor}"


def plugin_runtime_paths(root: Path | None = None, *, current_file: str | None = None) -> dict[str, Path] | None:
    plugin_root = resolve_plugin_root(root, current_file=current_file)
    if plugin_root is None:
        return None
    runtime_root = plugin_root / ".retriever-plugin-runtime" / plugin_runtime_environment_key()
    locks_dir = runtime_root / "locks"
    venv_dir = runtime_root / "venv"
    return {
        "plugin_root": plugin_root,
        "runtime_root": runtime_root,
        "locks_dir": locks_dir,
        "venv_dir": venv_dir,
        "venv_python_path": venv_dir / _venv_python_rel_path(),
        "requirements_marker_path": runtime_root / ".requirements-version",
        "install_lock_path": locks_dir / "runtime-install.lock",
    }


def plugin_runtime_site_packages_candidates(paths: dict[str, Path]) -> list[Path]:
    if os.name == "nt":
        return [paths["venv_dir"] / "Lib" / "site-packages"]
    lib_dir = paths["venv_dir"] / "lib"
    default_path = lib_dir / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    candidates = [default_path]
    if lib_dir.exists():
        for candidate in sorted(lib_dir.glob("python*/site-packages")):
            if candidate not in candidates:
                candidates.append(candidate)
    return candidates


def first_existing_plugin_runtime_site_packages(paths: dict[str, Path]) -> Path | None:
    for candidate in plugin_runtime_site_packages_candidates(paths):
        if candidate.exists():
            return candidate
    return None


def remove_directory_tree(path: Path) -> bool:
    try:
        if not path.exists():
            return False
        shutil.rmtree(path)
        return True
    except FileNotFoundError:
        return False


def new_ingest_session_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{secrets.token_hex(4)}"


def sweep_stale_ingest_tmp_dirs(paths: dict[str, Path]) -> dict[str, object]:
    ingest_tmp_dir = paths["ingest_tmp_dir"]
    if not ingest_tmp_dir.exists():
        return {"removed": 0, "failures": []}
    removed = 0
    failures: list[dict[str, str]] = []
    try:
        children = sorted(ingest_tmp_dir.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        return {
            "removed": 0,
            "failures": [{"path": str(ingest_tmp_dir), "error": f"{type(exc).__name__}: {exc}"}],
        }
    for child in children:
        if not child.is_dir():
            continue
        try:
            if remove_directory_tree(child):
                removed += 1
        except OSError as exc:
            failures.append(
                {
                    "path": str(child),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    return {"removed": removed, "failures": failures}


def acquire_os_file_lock(handle) -> None:
    if os.name == "nt":
        if msvcrt is None:
            raise RetrieverError("Windows file locking support is unavailable in this runtime.")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    if fcntl is None:
        raise RetrieverError("POSIX file locking support is unavailable in this runtime.")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def release_os_file_lock(handle) -> None:
    if os.name == "nt":
        if msvcrt is None:
            return
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    if fcntl is None:
        return
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def acquire_workspace_ingest_lock(paths: dict[str, Path]):
    lock_path = paths["ingest_lock_path"]
    handle = lock_path.open("a+b")
    try:
        acquire_os_file_lock(handle)
    except RetrieverError:
        handle.close()
        raise
    except OSError as exc:
        handle.close()
        if exc.errno in {errno.EACCES, errno.EAGAIN} or isinstance(exc, PermissionError):
            raise RetrieverError("Another ingest is already running in this workspace. Wait for it to finish and retry.") from exc
        raise RetrieverError(
            f"Unable to acquire workspace ingest lock at {lock_path}: {type(exc).__name__}: {exc}"
        ) from exc
    return handle


def release_workspace_ingest_lock(handle) -> None:
    try:
        release_os_file_lock(handle)
    finally:
        handle.close()


def acquire_workspace_entity_rebuild_lock(paths: dict[str, Path]):
    lock_path = paths["entity_rebuild_lock_path"]
    handle = lock_path.open("a+b")
    try:
        acquire_os_file_lock(handle)
    except RetrieverError:
        handle.close()
        raise
    except OSError as exc:
        handle.close()
        if exc.errno in {errno.EACCES, errno.EAGAIN} or isinstance(exc, PermissionError):
            raise RetrieverError(
                "Another entity rebuild is already running in this workspace. Wait for it to finish and retry."
            ) from exc
        raise RetrieverError(
            f"Unable to acquire workspace entity rebuild lock at {lock_path}: {type(exc).__name__}: {exc}"
        ) from exc
    return handle


def release_workspace_entity_rebuild_lock(handle) -> None:
    try:
        release_os_file_lock(handle)
    finally:
        handle.close()


@contextmanager
def workspace_entity_rebuild_session(paths: dict[str, Path], *, command_name: str):
    ensure_layout(paths)
    ingest_lock_handle = acquire_workspace_ingest_lock(paths)
    entity_lock_handle = None
    benchmark_mark("workspace_ingest_lock_acquired", command=command_name)
    try:
        entity_lock_handle = acquire_workspace_entity_rebuild_lock(paths)
        benchmark_mark("workspace_entity_rebuild_lock_acquired", command=command_name)
        yield {"id": new_ingest_session_id()}
    finally:
        if entity_lock_handle is not None:
            release_workspace_entity_rebuild_lock(entity_lock_handle)
        release_workspace_ingest_lock(ingest_lock_handle)


def acquire_plugin_runtime_install_lock(paths: dict[str, Path]):
    paths["locks_dir"].mkdir(parents=True, exist_ok=True)
    lock_path = paths["install_lock_path"]
    handle = lock_path.open("a+b")
    try:
        acquire_os_file_lock(handle)
    except RetrieverError:
        handle.close()
        raise
    except OSError as exc:
        handle.close()
        if exc.errno in {errno.EACCES, errno.EAGAIN} or isinstance(exc, PermissionError):
            raise RetrieverError(
                "Another shared plugin runtime install is already running. Wait for it to finish and retry."
            ) from exc
        raise RetrieverError(
            f"Unable to acquire plugin runtime install lock at {lock_path}: {type(exc).__name__}: {exc}"
        ) from exc
    return handle


def release_plugin_runtime_install_lock(handle) -> None:
    try:
        release_os_file_lock(handle)
    finally:
        handle.close()


@contextmanager
def workspace_ingest_session(paths: dict[str, Path], *, command_name: str):
    ensure_layout(paths)
    lock_handle = acquire_workspace_ingest_lock(paths)
    benchmark_mark("workspace_ingest_lock_acquired", command=command_name)
    session_id = new_ingest_session_id()
    session_dir = paths["ingest_tmp_dir"] / session_id
    try:
        stale_tmp_sweep = sweep_stale_ingest_tmp_dirs(paths)
        stale_tmp_dirs_removed = int(stale_tmp_sweep["removed"])
        stale_tmp_dir_failures = list(stale_tmp_sweep.get("failures") or [])
        warnings = [
            f"Could not remove stale ingest tmp dir {failure['path']}: {failure['error']}"
            for failure in stale_tmp_dir_failures
        ]
        session_dir.mkdir(parents=True, exist_ok=True)
        benchmark_mark(
            "workspace_ingest_session_ready",
            command=command_name,
            session_id=session_id,
            stale_tmp_dirs_removed=stale_tmp_dirs_removed,
            stale_tmp_dirs_failed=len(stale_tmp_dir_failures),
        )
        yield {
            "id": session_id,
            "tmp_dir": session_dir,
            "stale_tmp_dirs_removed": stale_tmp_dirs_removed,
            "stale_tmp_dirs_failed": len(stale_tmp_dir_failures),
            "warnings": warnings,
        }
    finally:
        try:
            remove_directory_tree(session_dir)
        except OSError as exc:
            benchmark_mark(
                "workspace_ingest_session_cleanup_failed",
                command=command_name,
                session_id=session_id,
                error=f"{type(exc).__name__}: {exc}",
            )
        release_workspace_ingest_lock(lock_handle)


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
    columns = table_columns(connection, "documents")
    attachment_children_expr = (
        "CASE WHEN parent_document_id IS NOT NULL "
        "AND COALESCE(child_document_kind, 'attachment') = 'attachment' "
        "AND lifecycle_status != 'deleted' THEN 1 ELSE 0 END"
        if "child_document_kind" in columns
        else "CASE WHEN parent_document_id IS NOT NULL AND lifecycle_status != 'deleted' THEN 1 ELSE 0 END"
    )
    row = connection.execute(
        f"""
        SELECT
          COALESCE(SUM(CASE WHEN parent_document_id IS NULL AND lifecycle_status != 'deleted' THEN 1 ELSE 0 END), 0) AS parent_documents,
          COALESCE(SUM(CASE WHEN parent_document_id IS NULL AND lifecycle_status = 'missing' THEN 1 ELSE 0 END), 0) AS missing_parent_documents,
          COALESCE(SUM({attachment_children_expr}), 0) AS attachment_children,
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
    columns = table_columns(connection, "documents")
    if not {"source_kind", "production_id", "parent_document_id"}.issubset(columns):
        return 0
    attachment_clause = (
        "parent_document_id IS NOT NULL AND COALESCE(child_document_kind, 'attachment') = 'attachment'"
        if "child_document_kind" in columns
        else "parent_document_id IS NOT NULL"
    )
    cursor = connection.execute(
        f"""
        UPDATE documents
        SET source_kind = CASE
          WHEN production_id IS NOT NULL THEN ?
          WHEN {attachment_clause} THEN ?
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


def normalize_child_document_kind(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or "")).lower().replace("-", "_")
    if not normalized:
        return None
    if normalized not in ALLOWED_CHILD_DOCUMENT_KINDS:
        allowed = ", ".join(sorted(ALLOWED_CHILD_DOCUMENT_KINDS))
        raise RetrieverError(f"Unsupported child_document_kind: {value!r}. Expected one of: {allowed}.")
    return normalized


def normalize_conversation_assignment_mode(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or "")).lower().replace("-", "_")
    if not normalized:
        return None
    if normalized in {CONVERSATION_ASSIGNMENT_MODE_AUTO, CONVERSATION_ASSIGNMENT_MODE_MANUAL}:
        return normalized
    raise RetrieverError(
        "Unsupported conversation_assignment_mode: "
        f"{value!r}. Expected one of: {CONVERSATION_ASSIGNMENT_MODE_AUTO}, {CONVERSATION_ASSIGNMENT_MODE_MANUAL}."
    )


def effective_child_document_kind(
    *,
    parent_document_id: int | None,
    child_document_kind: object,
) -> str | None:
    normalized = normalize_child_document_kind(child_document_kind)
    if parent_document_id is None:
        return None
    return normalized or CHILD_DOCUMENT_KIND_ATTACHMENT


def effective_conversation_assignment_mode(conversation_assignment_mode: object) -> str:
    normalized = normalize_conversation_assignment_mode(conversation_assignment_mode)
    return normalized or CONVERSATION_ASSIGNMENT_MODE_AUTO


def attachment_child_filter_sql(alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"{prefix}parent_document_id IS NOT NULL "
        f"AND COALESCE({prefix}child_document_kind, '{CHILD_DOCUMENT_KIND_ATTACHMENT}') = '{CHILD_DOCUMENT_KIND_ATTACHMENT}'"
    )


def row_child_document_kind(row: sqlite3.Row | dict[str, object]) -> str | None:
    if isinstance(row, sqlite3.Row):
        parent_document_id = row["parent_document_id"]
        raw_kind = row["child_document_kind"] if "child_document_kind" in row.keys() else None
    else:
        parent_document_id = row.get("parent_document_id")
        raw_kind = row.get("child_document_kind")
    normalized = normalize_whitespace(str(raw_kind or "")).lower().replace("-", "_")
    if normalized:
        return normalized
    if parent_document_id is not None:
        return CHILD_DOCUMENT_KIND_ATTACHMENT
    return None


def is_attachment_row(row: sqlite3.Row | dict[str, object]) -> bool:
    if isinstance(row, sqlite3.Row):
        parent_document_id = row["parent_document_id"]
    else:
        parent_document_id = row.get("parent_document_id")
    return parent_document_id is not None and row_child_document_kind(row) == CHILD_DOCUMENT_KIND_ATTACHMENT


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_inline_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", str(text or "")).strip())


def normalize_dataset_name_for_compare(dataset_name: str) -> str:
    normalized = normalize_inline_whitespace(dataset_name)
    return normalized.casefold()


def normalize_saved_scope_name(scope_name: str) -> str:
    return normalize_dataset_name_for_compare(scope_name)


def normalize_browse_mode(raw_value: object | None) -> str:
    normalized = normalize_inline_whitespace(str(raw_value or DEFAULT_BROWSE_MODE)).lower()
    if normalized not in {BROWSE_MODE_DOCUMENTS, BROWSE_MODE_CONVERSATIONS, BROWSE_MODE_ENTITIES}:
        return DEFAULT_BROWSE_MODE
    return normalized


def default_session_state() -> dict[str, object]:
    return {
        "schema_version": SESSION_SCHEMA_VERSION,
        "scope": {},
        "browse_mode": DEFAULT_BROWSE_MODE,
        "browsing": {
            BROWSE_MODE_DOCUMENTS: {},
            BROWSE_MODE_CONVERSATIONS: {},
            BROWSE_MODE_ENTITIES: {},
        },
        "display": {
            BROWSE_MODE_DOCUMENTS: {},
            BROWSE_MODE_CONVERSATIONS: {},
            BROWSE_MODE_ENTITIES: {},
        },
    }


def default_saved_scopes_state() -> dict[str, object]:
    return {
        "schema_version": SESSION_SCHEMA_VERSION,
        "scopes": {},
    }


def coerce_scope_dataset_entries(raw_value: object) -> list[dict[str, object]]:
    if not isinstance(raw_value, list):
        return []
    normalized_entries: list[dict[str, object]] = []
    seen_ids: set[int] = set()
    for item in raw_value:
        if not isinstance(item, dict):
            continue
        try:
            dataset_id = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        dataset_name = normalize_inline_whitespace(str(item.get("name") or ""))
        if not dataset_name or dataset_id in seen_ids:
            continue
        seen_ids.add(dataset_id)
        normalized_entries.append({"id": dataset_id, "name": dataset_name})
    return normalized_entries


def coerce_scope_payload(raw_scope: object) -> dict[str, object]:
    if not isinstance(raw_scope, dict):
        return {}
    scope: dict[str, object] = {}
    keyword = raw_scope.get("keyword")
    if isinstance(keyword, str) and keyword.strip():
        scope["keyword"] = keyword
    bates = raw_scope.get("bates")
    if isinstance(bates, dict):
        begin = normalize_inline_whitespace(str(bates.get("begin") or ""))
        end = normalize_inline_whitespace(str(bates.get("end") or ""))
        if begin and end:
            scope["bates"] = {"begin": begin, "end": end}
    filter_expression = raw_scope.get("filter")
    if isinstance(filter_expression, str) and filter_expression.strip():
        scope["filter"] = filter_expression
    dataset_entries = coerce_scope_dataset_entries(raw_scope.get("dataset"))
    if dataset_entries:
        scope["dataset"] = dataset_entries
    from_run_id = raw_scope.get("from_run_id")
    if from_run_id is not None:
        try:
            scope["from_run_id"] = int(from_run_id)
        except (TypeError, ValueError):
            pass
    set_at = raw_scope.get("set_at")
    if isinstance(set_at, str) and set_at.strip():
        scope["set_at"] = set_at
    return scope


def coerce_saved_scope_payload(raw_scope: object) -> dict[str, object]:
    scope = coerce_scope_payload(raw_scope)
    saved_at = raw_scope.get("saved_at") if isinstance(raw_scope, dict) else None
    if isinstance(saved_at, str) and saved_at.strip():
        scope["saved_at"] = saved_at
    scope.pop("set_at", None)
    return scope


def coerce_browsing_sort_payload(raw_value: object) -> list[list[str]]:
    if not isinstance(raw_value, list):
        return []
    normalized_specs: list[list[str]] = []
    for item in raw_value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        field_name = normalize_inline_whitespace(str(item[0] or ""))
        direction = normalize_inline_whitespace(str(item[1] or "")).lower()
        if not field_name or direction not in {"asc", "desc"}:
            continue
        normalized_specs.append([field_name, direction])
    return normalized_specs


def coerce_browsing_payload(raw_browsing: object) -> dict[str, object]:
    if not isinstance(raw_browsing, dict):
        return {}
    browsing: dict[str, object] = {}
    sort_specs = coerce_browsing_sort_payload(raw_browsing.get("sort"))
    if sort_specs:
        browsing["sort"] = sort_specs
    offset = raw_browsing.get("offset")
    if isinstance(offset, int) and offset >= 0:
        browsing["offset"] = offset
    total_known = raw_browsing.get("total_known")
    if isinstance(total_known, int) and total_known >= 0:
        browsing["total_known"] = total_known
    query = raw_browsing.get("query")
    if isinstance(query, str) and query.strip():
        browsing["query"] = normalize_whitespace(query)
    include_ignored = raw_browsing.get("include_ignored")
    if isinstance(include_ignored, bool):
        browsing["include_ignored"] = include_ignored
    run_at = raw_browsing.get("run_at")
    if isinstance(run_at, str) and run_at.strip():
        browsing["run_at"] = run_at
    return browsing


def coerce_display_payload(raw_display: object) -> dict[str, object]:
    if not isinstance(raw_display, dict):
        return {}
    display: dict[str, object] = {}
    columns = raw_display.get("columns")
    if isinstance(columns, list):
        normalized_columns = [normalize_inline_whitespace(str(value)) for value in columns if normalize_inline_whitespace(str(value))]
        if normalized_columns:
            display["columns"] = normalized_columns
    page_size = raw_display.get("page_size")
    if isinstance(page_size, int) and page_size > 0:
        display["page_size"] = page_size
    return display


def coerce_mode_payloads(raw_value: object, payload_coercer) -> dict[str, object]:
    normalized_payloads = {
        BROWSE_MODE_DOCUMENTS: {},
        BROWSE_MODE_CONVERSATIONS: {},
        BROWSE_MODE_ENTITIES: {},
    }
    if not isinstance(raw_value, dict):
        return normalized_payloads
    if any(
        key in raw_value
        for key in ("columns", "page_size", "sort", "offset", "total_known", "query", "include_ignored", "run_at")
    ):
        normalized_payloads[BROWSE_MODE_DOCUMENTS] = payload_coercer(raw_value)
        return normalized_payloads
    for browse_mode in (BROWSE_MODE_DOCUMENTS, BROWSE_MODE_CONVERSATIONS, BROWSE_MODE_ENTITIES):
        normalized_payloads[browse_mode] = payload_coercer(raw_value.get(browse_mode))
    return normalized_payloads


def coerce_session_state(raw_value: object) -> dict[str, object]:
    session = default_session_state()
    if not isinstance(raw_value, dict):
        return session
    schema_version = raw_value.get("schema_version")
    if isinstance(schema_version, int):
        session["schema_version"] = schema_version
    session["scope"] = coerce_scope_payload(raw_value.get("scope"))
    session["browse_mode"] = normalize_browse_mode(raw_value.get("browse_mode"))
    session["browsing"] = coerce_mode_payloads(raw_value.get("browsing"), coerce_browsing_payload)
    session["display"] = coerce_mode_payloads(raw_value.get("display"), coerce_display_payload)
    return session


def coerce_saved_scopes_state(raw_value: object) -> dict[str, object]:
    saved_scopes = default_saved_scopes_state()
    if not isinstance(raw_value, dict):
        return saved_scopes
    schema_version = raw_value.get("schema_version")
    if isinstance(schema_version, int):
        saved_scopes["schema_version"] = schema_version
    scopes = raw_value.get("scopes")
    if not isinstance(scopes, dict):
        return saved_scopes
    normalized_scopes: dict[str, object] = {}
    for scope_name, scope_payload in scopes.items():
        if not isinstance(scope_name, str):
            continue
        normalized_name = normalize_saved_scope_name(scope_name)
        if not normalized_name:
            continue
        normalized_scopes[scope_name] = coerce_saved_scope_payload(scope_payload)
    saved_scopes["scopes"] = normalized_scopes
    return saved_scopes


def read_json_state(path: Path, default_factory) -> dict[str, object]:
    if not path.exists():
        return default_factory()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise RetrieverError(f"Could not read state file {path}: {exc}") from exc
    coerced = default_factory()
    if default_factory is default_session_state:
        coerced = coerce_session_state(payload)
    elif default_factory is default_saved_scopes_state:
        coerced = coerce_saved_scopes_state(payload)
    return coerced


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    temp_path = Path(file_handle.name)
    try:
        with file_handle:
            json.dump(payload, file_handle, indent=2, sort_keys=True)
            file_handle.write("\n")
            file_handle.flush()
        temp_path.replace(path)
    except Exception:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def read_session_state(paths: dict[str, Path]) -> dict[str, object]:
    return read_json_state(paths["session_path"], default_session_state)


def write_session_state(paths: dict[str, Path], payload: dict[str, object]) -> None:
    coerced = coerce_session_state(payload)
    coerced["schema_version"] = SESSION_SCHEMA_VERSION
    write_json_atomic(paths["session_path"], coerced)


def read_saved_scopes_state(paths: dict[str, Path]) -> dict[str, object]:
    return read_json_state(paths["saved_scopes_path"], default_saved_scopes_state)


def write_saved_scopes_state(paths: dict[str, Path], payload: dict[str, object]) -> None:
    coerced = coerce_saved_scopes_state(payload)
    coerced["schema_version"] = SESSION_SCHEMA_VERSION
    write_json_atomic(paths["saved_scopes_path"], coerced)


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


INTERNAL_REL_PATH_PREFIX = "_retriever"


def is_internal_rel_path(rel_path: str | None) -> bool:
    if not rel_path:
        return False
    return normalize_internal_rel_path(Path(rel_path)).startswith(f"{INTERNAL_REL_PATH_PREFIX}/")


def document_absolute_path(paths: dict[str, Path], rel_path: str | None) -> Path:
    """Resolve a documents.rel_path (possibly internal) to its absolute location.

    Internal rel_paths (those beginning with ``_retriever/``) address files that
    live under the workspace's ``.retriever`` state directory. Regular rel_paths
    are relative to the workspace root.
    """
    text = str(rel_path or "").strip()
    if not text:
        return paths["root"]
    path = Path(text)
    if path.parts and path.parts[0] == INTERNAL_REL_PATH_PREFIX:
        state_relative = Path(*path.parts[1:]) if len(path.parts) > 1 else Path()
        return paths["state_dir"] / state_relative
    return paths["root"] / text


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
    if len(parts) < 5 or parts[0] != INTERNAL_REL_PATH_PREFIX or parts[1] != "sources":
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
    if normalized_source_kind == MBOX_SOURCE_KIND and normalized_source_rel_path:
        return mbox_custodian_email_from_source_rel_path(normalized_source_rel_path)
    if normalized_source_kind == PST_SOURCE_KIND and normalized_source_rel_path:
        basename = normalize_whitespace(Path(normalized_source_rel_path).stem)
        if not basename:
            return None
        basename_email = normalize_entity_email(basename)
        if basename_email:
            entity_type = entity_type_from_candidate_parts(name_value=None, email_value=basename_email)
            if entity_type in {ENTITY_TYPE_SHARED_MAILBOX, ENTITY_TYPE_SYSTEM_MAILBOX}:
                return None
            return basename_email
        stripped = normalize_whitespace(strip_entity_container_words(basename))
        return stripped or None
    return None


GOOGLE_VAULT_MBOX_BASENAME_PATTERN = re.compile(
    r"^(?P<label>.+?)--(?P<email>[^-]+@[^-]+\.[^-]+)-(?P<random>[A-Za-z0-9_-]{4,})$"
)

GOOGLE_TAKEOUT_MBOX_BASENAMES = {
    "all mail",
    "all mail including spam and trash",
    "archive",
    "chats",
    "drafts",
    "important",
    "inbox",
    "sent",
    "spam",
    "starred",
    "trash",
}


def parse_google_vault_mbox_basename(basename: object) -> dict[str, str] | None:
    normalized = normalize_whitespace(str(basename or ""))
    if not normalized:
        return None
    match = GOOGLE_VAULT_MBOX_BASENAME_PATTERN.match(normalized)
    if match is None:
        return None
    email = normalize_entity_email(match.group("email"))
    if not email:
        return None
    return {
        "label": normalize_whitespace(match.group("label")),
        "email": email,
        "random": match.group("random"),
    }


def is_google_takeout_mbox_basename(basename: object) -> bool:
    normalized = normalize_entity_lookup_text(basename)
    return normalized in GOOGLE_TAKEOUT_MBOX_BASENAMES


def mbox_custodian_email_from_source_rel_path(source_rel_path: object) -> str | None:
    basename = normalize_whitespace(Path(normalize_whitespace(str(source_rel_path or ""))).stem)
    if not basename:
        return None
    vault_parts = parse_google_vault_mbox_basename(basename)
    if vault_parts is not None:
        return vault_parts["email"]
    if is_google_takeout_mbox_basename(basename):
        return None
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


def slack_export_dataset_name(source_rel_path: str) -> str:
    candidate = normalize_whitespace(source_rel_path)
    if candidate:
        return f"Slack Export: {candidate}"
    return "Slack Export"


def production_dataset_name(rel_root: str, production_name: str | None = None) -> str:
    preferred = normalize_whitespace(str(production_name or ""))
    if preferred:
        return preferred
    candidate = normalize_whitespace(Path(rel_root).name)
    return candidate or normalize_whitespace(rel_root) or "Production Dataset"


def dataset_source_absolute_path(root: Path, source_locator: str | None) -> Path | None:
    normalized_source_locator = normalize_whitespace(str(source_locator or ""))
    if not normalized_source_locator or normalized_source_locator == filesystem_dataset_locator():
        return None
    return root / normalized_source_locator


def manual_dataset_locator(dataset_name: str | None = None) -> str:
    seed = normalize_whitespace(str(dataset_name or "")) or "dataset"
    return f"manual:{sha256_text(f'{seed}:{utc_now()}')[:16]}"


def normalized_dataset_name_or_default(dataset_name: str | None, default: str = "Dataset") -> str:
    normalized = normalize_inline_whitespace(str(dataset_name or ""))
    return normalized or default


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
    normalized_dataset_name = normalized_dataset_name_or_default(dataset_name)
    normalized_compare_name = normalize_dataset_name_for_compare(normalized_dataset_name)
    existing_row = get_dataset_row(
        connection,
        source_kind=source_kind,
        dataset_locator=dataset_locator,
    )
    if existing_row is None:
        external_id_auto_merge_names = (
            ["slack_user_id"]
            if normalize_whitespace(str(source_kind or "")).lower() == SLACK_EXPORT_SOURCE_KIND
            else []
        )
        try:
            connection.execute(
                """
                INSERT INTO datasets (
                  source_kind, dataset_locator, dataset_name, dataset_name_normalized,
                  external_id_auto_merge_names_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_kind,
                    dataset_locator,
                    normalized_dataset_name,
                    normalized_compare_name,
                    json.dumps(external_id_auto_merge_names, ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            conflicting_row = connection.execute(
                """
                SELECT id, dataset_name
                FROM datasets
                WHERE dataset_name_normalized = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (normalized_compare_name,),
            ).fetchone()
            if conflicting_row is not None:
                raise RetrieverError(
                    f"Dataset name {normalized_dataset_name!r} is already used by dataset {conflicting_row['id']} ({conflicting_row['dataset_name']!r})."
                ) from exc
            raise
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    existing_name = normalized_dataset_name_or_default(str(existing_row["dataset_name"] or ""))
    existing_normalized = normalize_inline_whitespace(str(existing_row["dataset_name_normalized"] or ""))
    if existing_normalized != normalize_dataset_name_for_compare(existing_name):
        connection.execute(
            """
            UPDATE datasets
            SET dataset_name = ?, dataset_name_normalized = ?, updated_at = ?
            WHERE id = ?
            """,
            (existing_name, normalize_dataset_name_for_compare(existing_name), now, existing_row["id"]),
        )
    return int(existing_row["id"])


def create_dataset_row(
    connection: sqlite3.Connection,
    dataset_name: str,
    *,
    source_kind: str | None = None,
    dataset_locator: str | None = None,
) -> int:
    normalized_name = normalized_dataset_name_or_default(dataset_name)
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


def get_conversation_row(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_locator: str,
    conversation_key: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM conversations
        WHERE source_kind = ? AND source_locator = ? AND conversation_key = ?
        """,
        (
            normalize_whitespace(source_kind).lower(),
            normalize_whitespace(source_locator),
            normalize_whitespace(conversation_key),
        ),
    ).fetchone()


def upsert_conversation_row(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_locator: str,
    conversation_key: str,
    conversation_type: str,
    display_name: str,
) -> int:
    normalized_source_kind = normalize_whitespace(source_kind).lower()
    normalized_source_locator = normalize_whitespace(source_locator)
    normalized_conversation_key = normalize_whitespace(conversation_key)
    normalized_conversation_type = normalize_whitespace(conversation_type).lower()
    normalized_display_name = normalize_whitespace(display_name)
    if not all(
        (
            normalized_source_kind,
            normalized_source_locator,
            normalized_conversation_key,
            normalized_conversation_type,
            normalized_display_name,
        )
    ):
        raise RetrieverError("Conversations require non-empty source kind, source locator, key, type, and display name.")

    existing_row = get_conversation_row(
        connection,
        source_kind=normalized_source_kind,
        source_locator=normalized_source_locator,
        conversation_key=normalized_conversation_key,
    )
    now = utc_now()
    if existing_row is None:
        connection.execute(
            """
            INSERT INTO conversations (
              source_kind, source_locator, conversation_key, conversation_type, display_name, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_source_kind,
                normalized_source_locator,
                normalized_conversation_key,
                normalized_conversation_type,
                normalized_display_name,
                now,
                now,
            ),
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    connection.execute(
        """
        UPDATE conversations
        SET conversation_type = ?, display_name = ?, updated_at = ?
        WHERE id = ?
        """,
        (normalized_conversation_type, normalized_display_name, now, int(existing_row["id"])),
    )
    return int(existing_row["id"])


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
    normalized_name = normalize_dataset_name_for_compare(dataset_name)
    if not normalized_name:
        return []
    return connection.execute(
        """
        SELECT *
        FROM datasets
        WHERE dataset_name_normalized = ?
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
        if dataset_name is not None and normalize_dataset_name_for_compare(str(row["dataset_name"] or "")) != normalize_dataset_name_for_compare(dataset_name):
            raise RetrieverError(
                f"Dataset id {dataset_id} is named {row['dataset_name']!r}, not {normalize_inline_whitespace(dataset_name)!r}."
            )
        return row
    matches = find_dataset_rows_by_name(connection, str(dataset_name or ""))
    if not matches:
        raise RetrieverError(f"Unknown dataset name: {dataset_name}")
    return matches[0]


def rename_dataset_row(
    connection: sqlite3.Connection,
    dataset_id: int,
    new_dataset_name: str,
    root: Path | None = None,
) -> dict[str, object]:
    dataset_row = get_dataset_row_by_id(connection, dataset_id)
    if dataset_row is None:
        raise RetrieverError(f"Unknown dataset id: {dataset_id}")
    normalized_name = normalized_dataset_name_or_default(new_dataset_name)
    normalized_compare_name = normalize_dataset_name_for_compare(normalized_name)
    conflicting_row = connection.execute(
        """
        SELECT id, dataset_name
        FROM datasets
        WHERE dataset_name_normalized = ?
          AND id != ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (normalized_compare_name, dataset_id),
    ).fetchone()
    if conflicting_row is not None:
        raise RetrieverError(
            f"Dataset name {normalized_name!r} is already used by dataset {conflicting_row['id']} ({conflicting_row['dataset_name']!r})."
        )
    now = utc_now()
    connection.execute(
        """
        UPDATE datasets
        SET dataset_name = ?, dataset_name_normalized = ?, updated_at = ?
        WHERE id = ?
        """,
        (normalized_name, normalized_compare_name, now, dataset_id),
    )
    return dataset_summary_by_id(connection, dataset_id, root=root)


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


def dataset_container_size_bytes(
    root: Path | None,
    dataset_row: sqlite3.Row,
    source_bindings: list[dict[str, object]],
) -> int | None:
    if root is None:
        return None
    normalized_source_kind = normalize_whitespace(str(dataset_row["source_kind"] or "")).lower()
    if normalized_source_kind not in {MBOX_SOURCE_KIND, PST_SOURCE_KIND}:
        return None

    candidate_locators: list[str] = []
    dataset_locator = normalize_whitespace(str(dataset_row["dataset_locator"] or ""))
    if dataset_locator:
        candidate_locators.append(dataset_locator)
    for binding in source_bindings:
        source_locator = normalize_whitespace(str(binding.get("source_locator") or ""))
        if source_locator and source_locator not in candidate_locators:
            candidate_locators.append(source_locator)

    for source_locator in candidate_locators:
        source_path = dataset_source_absolute_path(root, source_locator)
        if source_path is None:
            continue
        size_bytes = file_size_bytes(source_path)
        if size_bytes is not None:
            return size_bytes
    return None


def list_dataset_summaries(connection: sqlite3.Connection, root: Path | None = None) -> list[dict[str, object]]:
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
    document_rows = connection.execute(
        f"""
        WITH distinct_dataset_documents AS (
            SELECT DISTINCT dataset_id, document_id
            FROM dataset_documents
            WHERE dataset_id IN ({placeholders})
        )
        SELECT
          distinct_dataset_documents.dataset_id,
          d.id AS document_id,
          d.file_size,
          d.content_type,
          d.custodians_json,
          d.date_created,
          d.date_modified
        FROM distinct_dataset_documents
        JOIN documents d ON d.id = distinct_dataset_documents.document_id
        ORDER BY distinct_dataset_documents.dataset_id ASC, d.id ASC
        """,
        dataset_ids,
    ).fetchall()
    stats_by_dataset: dict[int, dict[str, object]] = defaultdict(
        lambda: {
            "size_bytes": 0,
            "sized_document_count": 0,
            "content_type_counts": defaultdict(int),
            "custodians": set(),
            "time_range_start": None,
            "time_range_end": None,
        }
    )
    for row in document_rows:
        dataset_id = int(row["dataset_id"])
        stats = stats_by_dataset[dataset_id]

        file_size_value: int | None = None
        if row["file_size"] is not None:
            try:
                file_size_value = int(row["file_size"])
            except (TypeError, ValueError):
                file_size_value = None
        if file_size_value is not None and file_size_value >= 0:
            stats["size_bytes"] = int(stats["size_bytes"]) + file_size_value
            stats["sized_document_count"] = int(stats["sized_document_count"]) + 1

        content_type = normalize_whitespace(str(row["content_type"] or "")) or "Unknown"
        content_type_counts = stats["content_type_counts"]
        content_type_counts[content_type] += 1

        custodians = stats["custodians"]
        custodians.update(parse_document_custodians_json(row["custodians_json"]))

        start_candidate = normalize_datetime(row["date_created"]) or normalize_datetime(row["date_modified"])
        if start_candidate is not None:
            current_start = stats["time_range_start"]
            if current_start is None or start_candidate < current_start:
                stats["time_range_start"] = start_candidate

        end_candidate = normalize_datetime(row["date_modified"]) or normalize_datetime(row["date_created"])
        if end_candidate is not None:
            current_end = stats["time_range_end"]
            if current_end is None or end_candidate > current_end:
                stats["time_range_end"] = end_candidate

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
        dataset_stats = stats_by_dataset.get(dataset_id)
        source_bindings = sources_by_dataset.get(dataset_id, [])
        if dataset_stats is None:
            size_bytes = None
            sized_document_count = 0
            size_basis = None
            content_types: list[dict[str, object]] = []
            custodians: list[str] = []
            time_range_start = None
            time_range_end = None
        else:
            sized_document_count = int(dataset_stats["sized_document_count"] or 0)
            size_bytes = int(dataset_stats["size_bytes"] or 0) if sized_document_count else None
            size_basis = "documents" if size_bytes is not None else None
            content_types = [
                {"name": name, "count": count}
                for name, count in sorted(
                    dataset_stats["content_type_counts"].items(),
                    key=lambda item: (-int(item[1]), str(item[0]).lower(), str(item[0])),
                )
            ]
            custodians = sorted(
                [str(value) for value in dataset_stats["custodians"]],
                key=lambda value: (value.lower(), value),
            )
            time_range_start = dataset_stats["time_range_start"]
            time_range_end = dataset_stats["time_range_end"]
        container_size_bytes = dataset_container_size_bytes(root, row, source_bindings)
        if container_size_bytes is not None:
            size_bytes = container_size_bytes
            size_basis = "container"
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
                "size_bytes": size_bytes,
                "size_basis": size_basis,
                "sized_document_count": sized_document_count,
                "custodians": custodians,
                "content_types": content_types,
                "time_range_start": time_range_start,
                "time_range_end": time_range_end,
                "source_binding_count": len(source_bindings),
                "source_bindings": source_bindings,
                "merge_policy": dataset_merge_policy_payload_from_row(row),
            }
        )
    return summaries


def dataset_summary_by_id(connection: sqlite3.Connection, dataset_id: int, root: Path | None = None) -> dict[str, object]:
    for summary in list_dataset_summaries(connection, root=root):
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


def delete_dataset_row(connection: sqlite3.Connection, dataset_id: int, root: Path | None = None) -> dict[str, object]:
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
    summary = dataset_summary_by_id(connection, dataset_id, root=root)
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
        Path(INTERNAL_REL_PATH_PREFIX)
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


FILE_TYPE_ALIASES = {
    "htm": "html",
    "jpe": "jpg",
    "jpeg": "jpg",
    "tiff": "tif",
}
ATTACHMENT_FILE_TYPE_BY_MIME_TYPE = {
    "application/json": "json",
    "application/pdf": "pdf",
    "application/rtf": "rtf",
    "application/vnd.ms-excel": "xls",
    "application/vnd.ms-powerpoint": "ppt",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/xhtml+xml": "html",
    "application/xml": "xml",
    "application/zip": "zip",
    "application/x-zip-compressed": "zip",
    "text/calendar": "ics",
    "text/csv": "csv",
    "text/html": "html",
    "text/json": "json",
    "text/markdown": "md",
    "text/plain": "txt",
    "text/rtf": "rtf",
    "text/xml": "xml",
}
OLE_COMPOUND_FILE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def normalize_file_type_name(raw_value: object) -> str | None:
    normalized = normalize_whitespace(str(raw_value or "")).lower().lstrip(".")
    if not normalized:
        return None
    return FILE_TYPE_ALIASES.get(normalized, normalized)


def normalize_mime_type(raw_value: object) -> str | None:
    normalized = normalize_whitespace(str(raw_value or "")).lower()
    if not normalized:
        return None
    normalized = normalized.split(";", 1)[0].strip()
    return normalized or None


def attachment_file_type_from_mime_type(raw_value: object) -> str | None:
    mime_type = normalize_mime_type(raw_value)
    if mime_type is None or mime_type == "application/octet-stream":
        return None
    explicit = ATTACHMENT_FILE_TYPE_BY_MIME_TYPE.get(mime_type)
    if explicit:
        return explicit
    guessed = mimetypes.guess_extension(mime_type, strict=False)
    return normalize_file_type_name(guessed)


def infer_content_type_from_extension(file_type: str) -> str | None:
    if not file_type:
        return None
    if file_type == "md":
        return "E-Doc"
    return CONTENT_TYPE_BY_EXTENSION.get(file_type)


def canonical_kind_from_metadata(
    *,
    extracted_content_type: object = None,
    extracted_kind: object = None,
    file_type: object = None,
    source_kind: object = None,
) -> str:
    normalized_kind = normalize_whitespace(str(extracted_kind or "")).lower()
    if normalized_kind in CANONICAL_KIND_VALUES:
        return normalized_kind

    normalized_content_type = normalize_whitespace(str(extracted_content_type or "")).lower()
    normalized_file_type = normalize_whitespace(str(file_type or "")).lower()
    normalized_source_kind = normalize_whitespace(str(source_kind or "")).lower()

    if normalized_source_kind in {PST_SOURCE_KIND, MBOX_SOURCE_KIND} or normalized_content_type == "email":
        return "email"
    if "spreadsheet" in normalized_content_type or "table" in normalized_content_type:
        return "spreadsheet"
    if "presentation" in normalized_content_type:
        return "presentation"
    if normalized_content_type == "image":
        return "image"
    if normalized_content_type == "source code":
        return "code"
    if normalized_content_type == "database":
        return "data"
    if normalized_content_type == "container":
        return "binary"
    if normalized_content_type in {"calendar", "message"}:
        return "email"
    if normalized_content_type in {"chat", "e-doc", "web"}:
        return "document"

    if normalized_file_type in {"csv", "json", "xml", "yaml", "yml"}:
        return "data"
    if normalized_file_type in {"xls", "xlsx", "xlsm", "xlsb", "ods", "numbers"}:
        return "spreadsheet"
    if normalized_file_type in {"ppt", "pptx", "pptm", "odp", "key"}:
        return "presentation"
    if normalized_file_type in {"png", "jpg", "jpeg", "gif", "bmp", "tif", "tiff", "webp", "svg"}:
        return "image"
    if normalized_file_type in CURATED_TEXT_SOURCE_FILE_TYPES:
        return "code"
    if normalized_file_type in {"doc", "docx", "pdf", "txt", "rtf", "md", "html", "htm"}:
        return "document"
    return "unknown"


def canonical_kind_compatible(left: object, right: object) -> bool:
    left_kind = normalize_whitespace(str(left or "")).lower() or "unknown"
    right_kind = normalize_whitespace(str(right or "")).lower() or "unknown"
    return left_kind == "unknown" or right_kind == "unknown" or left_kind == right_kind


def occurrence_field_count(row: sqlite3.Row | dict[str, object]) -> int:
    fields = [
        "extracted_author",
        "extracted_title",
        "extracted_subject",
        "extracted_participants",
        "extracted_recipients",
        "extracted_doc_authored_at",
        "extracted_doc_modified_at",
        "extracted_content_type",
        "extracted_kind",
    ]
    return sum(1 for field_name in fields if normalize_whitespace(str(row[field_name] or "")))


def text_status_priority(status: object) -> int:
    normalized = normalize_whitespace(str(status or "")).lower()
    return TEXT_STATUS_PRIORITIES.get(normalized, max(TEXT_STATUS_PRIORITIES.values()) + 1)


def source_kind_priority(source_kind: object) -> int:
    normalized = normalize_whitespace(str(source_kind or "")).lower()
    return SOURCE_KIND_PREFERRED_ORDER.get(normalized, max(SOURCE_KIND_PREFERRED_ORDER.values()) + 1)


def active_occurrence_rows_for_document(
    connection: sqlite3.Connection,
    document_id: int,
    *,
    include_all_statuses: bool = False,
) -> list[sqlite3.Row]:
    if include_all_statuses:
        rows = connection.execute(
            """
            SELECT *
            FROM document_occurrences
            WHERE document_id = ?
            ORDER BY id ASC
            """,
            (document_id,),
        ).fetchall()
    else:
        rows = connection.execute(
            """
            SELECT *
            FROM document_occurrences
            WHERE document_id = ?
              AND lifecycle_status = ?
            ORDER BY id ASC
            """,
            (document_id, ACTIVE_OCCURRENCE_STATUS),
        ).fetchall()
    return rows


def select_preferred_occurrence(rows: list[sqlite3.Row]) -> sqlite3.Row | None:
    if not rows:
        return None
    ranked_rows = sorted(
        rows,
        key=lambda row: (
            0 if row["lifecycle_status"] == ACTIVE_OCCURRENCE_STATUS else 1,
            source_kind_priority(row["source_kind"]),
            text_status_priority(row["text_status"]),
            -occurrence_field_count(row),
            0 if int(row["has_preview"] or 0) else 1,
            parse_utc_timestamp(row["ingested_at"]) or datetime.max.replace(tzinfo=timezone.utc),
            int(row["id"]),
        ),
    )
    return ranked_rows[0]


def occurrence_field_value(
    preferred_row: sqlite3.Row | None,
    active_rows: list[sqlite3.Row],
    preferred_column: str,
) -> object:
    if preferred_row is not None:
        preferred_value = preferred_row[preferred_column]
        if preferred_value not in (None, ""):
            return preferred_value
    sorted_rows = sorted(
        active_rows,
        key=lambda row: (
            parse_utc_timestamp(row["ingested_at"]) or datetime.max.replace(tzinfo=timezone.utc),
            int(row["id"]),
        ),
    )
    for row in sorted_rows:
        value = row[preferred_column]
        if value not in (None, ""):
            return value
    return None


def normalize_custodian_values(values: list[object] | tuple[object, ...] | set[object]) -> list[str]:
    normalized_values: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        normalized = normalize_whitespace(str(raw_value or ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_values.append(normalized)
    return normalized_values


def occurrence_rows_in_preferred_order(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    return sorted(
        rows,
        key=lambda row: (
            0 if row["lifecycle_status"] == ACTIVE_OCCURRENCE_STATUS else 1,
            source_kind_priority(row["source_kind"]),
            text_status_priority(row["text_status"]),
            -occurrence_field_count(row),
            0 if int(row["has_preview"] or 0) else 1,
            parse_utc_timestamp(row["ingested_at"]) or datetime.max.replace(tzinfo=timezone.utc),
            int(row["id"]),
        ),
    )


def custodian_values_from_occurrence_rows(rows: list[sqlite3.Row]) -> list[str]:
    return normalize_custodian_values([row["custodian"] for row in occurrence_rows_in_preferred_order(rows)])


def parse_document_custodians_json(raw_value: object) -> list[str]:
    if isinstance(raw_value, list):
        return normalize_custodian_values(list(raw_value))
    if not raw_value:
        return []
    try:
        parsed = json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return normalize_custodian_values([raw_value])
    if isinstance(parsed, list):
        return normalize_custodian_values(parsed)
    return normalize_custodian_values([parsed])


def document_custodian_values_from_row(row: sqlite3.Row | dict[str, object] | None) -> list[str]:
    if row is None:
        return []
    if "custodians_json" in row.keys():  # type: ignore[attr-defined]
        return parse_document_custodians_json(row["custodians_json"])  # type: ignore[index]
    if "custodian" in row.keys():  # type: ignore[attr-defined]
        return normalize_custodian_values([row["custodian"]])  # type: ignore[index]
    return []


def document_custodian_display_text_from_row(row: sqlite3.Row | dict[str, object] | None) -> str | None:
    values = document_custodian_values_from_row(row)
    if not values:
        return None
    return ", ".join(values)


def normalize_entity_text(value: object) -> str:
    return normalize_whitespace(unicodedata.normalize("NFKC", str(value or "")))


def normalize_entity_lookup_text(value: object) -> str:
    text = normalize_entity_text(value).lower()
    text = re.sub(r"[^\w@.+#/-]+", " ", text, flags=re.UNICODE)
    return normalize_whitespace(text)


def normalize_entity_email(value: object) -> str | None:
    text = normalize_entity_text(value).strip("<>;,")
    if not text or "@" not in text:
        return None
    candidate = text.lower()
    candidate = re.sub(r"^mailto:", "", candidate)
    candidate = candidate.strip("<>;,")
    if not re.fullmatch(r"[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", candidate):
        return None
    if "." not in candidate.rsplit("@", 1)[1]:
        return None
    return candidate


def normalize_entity_identifier_name(value: object) -> str | None:
    text = normalize_entity_lookup_text(value)
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or None


def normalize_entity_handle(value: object) -> str | None:
    text = normalize_entity_text(value).strip()
    if not text:
        return None
    if text.startswith("@"):
        text = text[1:]
    text = text.strip()
    if not text:
        return None
    normalized = re.sub(r"\s+", "", text).lower()
    if not re.fullmatch(r"[a-z0-9._-]{2,128}", normalized):
        return None
    return normalized


def normalize_entity_phone(value: object) -> dict[str, object] | None:
    text = normalize_entity_text(value)
    if not text:
        return None
    extension = None
    extension_match = re.search(r"(?i)(?:ext\.?|extension|x)\s*([0-9]{1,10})\b", text)
    phone_text = text
    if extension_match:
        extension = extension_match.group(1)
        phone_text = (text[:extension_match.start()] + text[extension_match.end():]).strip()
    digits = re.sub(r"\D+", "", phone_text)
    if len(digits) < 7 or len(digits) > 15:
        return None
    has_phone_shape = bool(re.search(r"[()+\-\s.]", phone_text)) or phone_text.strip().startswith("+")
    if not has_phone_shape and len(digits) < 10:
        return None
    if phone_text.strip().startswith("+"):
        base_phone = f"+{digits}"
    elif len(digits) == 10:
        base_phone = f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        base_phone = f"+{digits}"
    else:
        base_phone = digits
    normalized = f"{base_phone}x{extension}" if extension else base_phone
    return {
        "display_value": text,
        "normalized_value": normalized,
        "parsed_phone": {
            "base_phone": base_phone,
            "extension": extension,
        },
    }


def strip_entity_container_words(value: str) -> str:
    text = normalize_entity_text(value)
    stripped = normalize_whitespace(re.sub(r"(?i)\b(?:mailbox|archive|export|pst|mbox)\b", " ", text))
    if len(entity_name_tokens(stripped)) >= 2:
        return stripped
    return text


def entity_name_tokens(value: object) -> list[str]:
    text = normalize_entity_text(value)
    text = re.sub(r"[\"'()<>]", " ", text)
    text = re.sub(r"\b(?:mr|mrs|ms|miss|dr|prof)\.?\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(?:jr|sr|ii|iii|iv|esq)\.?\b", " ", text, flags=re.IGNORECASE)
    return [
        token
        for token in re.split(r"[\s.]+", text)
        if token and re.search(r"[A-Za-z]", token)
    ]


def parse_entity_name(value: object) -> dict[str, object] | None:
    text = strip_entity_container_words(str(value or ""))
    if not text or "@" in text:
        return None
    compact = normalize_entity_lookup_text(text)
    if not compact:
        return None
    parts = [normalize_entity_text(part) for part in text.split(",", 1)]
    parsed: dict[str, object] = {}
    display_name = normalize_entity_text(text)
    if len(parts) == 2 and parts[0] and parts[1]:
        family_tokens = entity_name_tokens(parts[0])
        given_tokens = entity_name_tokens(parts[1])
        if family_tokens and given_tokens:
            tokens = [*given_tokens, *family_tokens]
            display_name = " ".join(tokens)
            parsed = {
                "given": given_tokens[0],
                "middle": " ".join(given_tokens[1:]) or None,
                "family": " ".join(family_tokens),
            }
    if not parsed:
        tokens = entity_name_tokens(text)
        if not tokens:
            return None
        if len(tokens) >= 2:
            parsed = {
                "given": tokens[0],
                "middle": " ".join(tokens[1:-1]) or None,
                "family": tokens[-1],
            }
            display_name = " ".join(tokens)
        else:
            parsed = {
                "given": tokens[0],
                "middle": None,
                "family": None,
            }
            display_name = tokens[0]
    display_tokens = entity_name_tokens(display_name)
    if not display_tokens:
        return None
    normalized_full_name = normalize_entity_lookup_text(" ".join(display_tokens))
    family = normalize_entity_lookup_text(str(parsed.get("family") or ""))
    given = normalize_entity_lookup_text(str(parsed.get("given") or ""))
    middle = normalize_entity_lookup_text(str(parsed.get("middle") or ""))
    sort_parts = [part for part in (family, given, middle) if part]
    normalized_sort_name = normalize_entity_lookup_text(" ".join(sort_parts)) if sort_parts else normalized_full_name
    return {
        "display_value": display_name,
        "normalized_value": normalized_full_name,
        "parsed_name": {key: value for key, value in parsed.items() if value},
        "normalized_full_name": normalized_full_name,
        "normalized_sort_name": normalized_sort_name,
        "is_full_name": len(display_tokens) >= 2 and bool(parsed.get("family")),
    }


def entity_name_identifier_looks_like_export_artifact(raw_value: object) -> bool:
    display_value = normalize_entity_text(raw_value)
    normalized = normalize_entity_lookup_text(display_value)
    if not normalized:
        return False
    compact = re.sub(r"\s+", "", normalized)
    if compact.startswith("-"):
        return True
    letters = re.sub(r"[^a-z]", "", compact.lower())
    if letters and not re.search(r"[aeiou]", letters):
        return True
    if re.fullmatch(r"[A-Za-z0-9_-]{4,8}", display_value):
        has_upper = bool(re.search(r"[A-Z]", display_value))
        has_lower = bool(re.search(r"[a-z]", display_value))
        has_digit_or_separator = bool(re.search(r"[0-9_-]", display_value))
        if has_upper and has_lower and has_digit_or_separator:
            return True
    return False


def entity_type_from_candidate_parts(
    *,
    name_value: str | None,
    email_value: str | None,
    name_is_full: bool = False,
) -> str:
    email = normalize_entity_email(email_value)
    if email:
        local_part = email.split("@", 1)[0]
        normalized_local = re.sub(r"[^a-z0-9]+", "", local_part.lower())
        if normalized_local in SYSTEM_MAILBOX_LOCAL_PARTS or any(
            token in normalized_local for token in SYSTEM_MAILBOX_LOCAL_CONTAINS
        ):
            return ENTITY_TYPE_SYSTEM_MAILBOX
        if normalized_local in SHARED_MAILBOX_LOCAL_PARTS:
            return ENTITY_TYPE_SHARED_MAILBOX
    name = normalize_entity_lookup_text(name_value or "")
    if email and any(re.search(rf"\b{re.escape(hint)}\b", name) for hint in SHARED_MAILBOX_NAME_HINTS):
        return ENTITY_TYPE_SHARED_MAILBOX
    if re.search(r"\b(inc|llc|llp|ltd|corp|corporation|company|co|plc|gmbh|sarl|partners|holdings)\b", name):
        return ENTITY_TYPE_ORGANIZATION
    if email or (name and name_is_full):
        return ENTITY_TYPE_PERSON
    if name:
        return ENTITY_TYPE_UNKNOWN
    return ENTITY_TYPE_UNKNOWN


def entity_candidate_identifier_key(identifier: dict[str, object]) -> str:
    identifier_type = str(identifier.get("identifier_type") or "")
    pieces = [identifier_type]
    for field_name in ("provider", "provider_scope", "identifier_name", "identifier_scope", "normalized_value"):
        value = normalize_entity_lookup_text(identifier.get(field_name) or "")
        if value:
            pieces.append(value)
    return ":".join(pieces)


def entity_candidate_key(role: str, identifiers: list[dict[str, object]], fallback_value: object) -> str:
    identifier_keys = sorted(entity_candidate_identifier_key(identifier) for identifier in identifiers)
    if identifier_keys:
        return "|".join([role, *identifier_keys])
    return "|".join([role, "raw", normalize_entity_lookup_text(fallback_value)])


def split_entity_like_values(raw_value: object, *, prefer_single_comma_name: bool = False) -> list[str]:
    text = normalize_entity_text(raw_value)
    if not text:
        return []
    semicolon_parts = [normalize_entity_text(part) for part in text.split(";")]
    if len([part for part in semicolon_parts if part]) > 1:
        return [part for part in semicolon_parts if part]
    if "@" in text or "<" in text:
        parsed_addresses = getaddresses([text])
        parts: list[str] = []
        for display_name, address in parsed_addresses:
            normalized_email = normalize_entity_email(address)
            display_name = normalize_entity_text(display_name)
            if normalized_email:
                parts.append(f"{display_name} <{normalized_email}>" if display_name else normalized_email)
            elif display_name:
                parts.append(display_name)
        if parts:
            return parts
    comma_count = text.count(",")
    if prefer_single_comma_name and comma_count == 1 and parse_entity_name(text):
        return [text]
    if comma_count:
        return [part for part in (normalize_entity_text(part) for part in text.split(",")) if part]
    return [text]


ENTITY_LABELED_EXTERNAL_ID_PATTERN = re.compile(
    r"\b([A-Za-z][A-Za-z0-9 _/-]{1,40})\s*[:=#]\s*([A-Za-z0-9][A-Za-z0-9_.:/-]{2,80})"
)


def parse_labeled_external_ids(value: object) -> list[dict[str, object]]:
    text = normalize_entity_text(value)
    identifiers: list[dict[str, object]] = []
    for match in ENTITY_LABELED_EXTERNAL_ID_PATTERN.finditer(text):
        identifier_name = normalize_entity_identifier_name(match.group(1))
        normalized_value = normalize_entity_lookup_text(match.group(2))
        if not identifier_name or not normalized_value:
            continue
        identifiers.append(
            {
                "identifier_type": "external_id",
                "display_value": match.group(2),
                "normalized_value": normalized_value,
                "identifier_name": identifier_name,
            }
        )
    return identifiers


def strip_labeled_external_ids(value: object) -> str:
    text = normalize_entity_text(value)
    if not text:
        return ""
    stripped = ENTITY_LABELED_EXTERNAL_ID_PATTERN.sub(" ", text)
    stripped = re.sub(r"\s*[\[\](){}]\s*", " ", stripped)
    return normalize_entity_text(stripped)


def parse_entity_candidate_text(
    raw_value: object,
    *,
    role: str,
    provider: str | None = None,
    provider_scope: str | None = None,
) -> dict[str, object] | None:
    text = normalize_entity_text(raw_value)
    if not text:
        return None
    emails: list[str] = []
    names: list[str] = []
    angle_address_match = re.match(r"^(?P<display>.*?)<(?P<address>[^>]+)>\s*$", text)
    if angle_address_match:
        email = normalize_entity_email(angle_address_match.group("address"))
        display_name = normalize_entity_text(angle_address_match.group("display"))
        if email:
            emails.append(email)
            if display_name:
                names.append(display_name.strip("\"' "))
    if not emails:
        parsed_addresses = getaddresses([text])
        for display_name, address in parsed_addresses:
            email = normalize_entity_email(address)
            if email and email not in emails:
                emails.append(email)
                if normalize_entity_text(display_name):
                    names.append(normalize_entity_text(display_name))
    if not emails:
        for email_match in re.finditer(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text):
            email = normalize_entity_email(email_match.group(0))
            if email and email not in emails:
                emails.append(email)
    name_source = names[0] if names else re.sub(r"<[^>]+>", " ", text)
    for email in emails:
        name_source = re.sub(re.escape(email), " ", name_source, flags=re.IGNORECASE)
    phone = normalize_entity_phone(text)
    if phone:
        name_source = normalize_whitespace(str(name_source).replace(str(phone["display_value"]), " "))
    name_source = strip_labeled_external_ids(name_source)
    identifiers: list[dict[str, object]] = []
    for email in emails:
        identifiers.append(
            {
                "identifier_type": "email",
                "display_value": email,
                "normalized_value": email,
                "is_verified": 1,
            }
        )
    parsed_name = parse_entity_name(name_source)
    if parsed_name is not None and entity_name_identifier_looks_like_export_artifact(parsed_name["display_value"]):
        parsed_name = None
    if parsed_name is not None:
        identifiers.append(
            {
                "identifier_type": "name",
                "display_value": parsed_name["display_value"],
                "normalized_value": parsed_name["normalized_value"],
                "parsed_name_json": json.dumps(parsed_name["parsed_name"], ensure_ascii=True, sort_keys=True),
                "normalized_full_name": parsed_name["normalized_full_name"],
                "normalized_sort_name": parsed_name["normalized_sort_name"],
                "is_primary": 1 if parsed_name["is_full_name"] else 0,
            }
        )
    if phone is not None:
        identifiers.append(
            {
                "identifier_type": "phone",
                "display_value": phone["display_value"],
                "normalized_value": phone["normalized_value"],
                "parsed_phone_json": json.dumps(phone["parsed_phone"], ensure_ascii=True, sort_keys=True),
            }
        )
    if provider and provider_scope:
        handle = normalize_entity_handle(text)
        if handle:
            identifiers.append(
                {
                    "identifier_type": "handle",
                    "display_value": text,
                    "normalized_value": handle,
                    "provider": normalize_entity_identifier_name(provider),
                    "provider_scope": normalize_entity_lookup_text(provider_scope),
                }
            )
    identifiers.extend(parse_labeled_external_ids(text))
    if not identifiers:
        return None
    display_basis = (
        str(parsed_name["display_value"])
        if parsed_name is not None
        else emails[0]
        if emails
        else str(phone["display_value"])
        if phone is not None
        else text
    )
    entity_type = entity_type_from_candidate_parts(
        name_value=str(parsed_name["display_value"]) if parsed_name is not None else None,
        email_value=emails[0] if emails else None,
        name_is_full=bool(parsed_name.get("is_full_name")) if parsed_name is not None else False,
    )
    return {
        "role": role,
        "raw_value": text,
        "display_value": display_basis,
        "entity_type": entity_type,
        "identifiers": identifiers,
        "normalized_candidate_key": entity_candidate_key(role, identifiers, text),
    }


def normalize_entity_hint_identifier(
    raw_identifier: object,
    *,
    default_source_kind: object = None,
) -> dict[str, object] | None:
    if not isinstance(raw_identifier, dict):
        return None
    identifier_type = normalize_entity_identifier_name(
        raw_identifier.get("identifier_type") or raw_identifier.get("type") or ""
    )
    if identifier_type not in {"email", "handle", "name", "external_id"}:
        return None
    display_value = normalize_entity_text(
        raw_identifier.get("display_value")
        or raw_identifier.get("value")
        or raw_identifier.get("normalized_value")
        or ""
    )
    is_primary = 1 if int(raw_identifier.get("is_primary") or 0) else 0
    is_verified = 1 if int(raw_identifier.get("is_verified") or 0) else 0
    source_kind = normalize_entity_identifier_name(raw_identifier.get("source_kind") or default_source_kind or "")
    identifier: dict[str, object]
    if identifier_type == "email":
        email = normalize_entity_email(raw_identifier.get("normalized_value") or display_value)
        if not email:
            return None
        identifier = {
            "identifier_type": "email",
            "display_value": email,
            "normalized_value": email,
            "is_primary": is_primary,
            "is_verified": is_verified,
        }
    elif identifier_type == "handle":
        provider = normalize_entity_identifier_name(raw_identifier.get("provider") or "")
        provider_scope = normalize_entity_lookup_text(raw_identifier.get("provider_scope") or raw_identifier.get("scope") or "")
        handle = normalize_entity_handle(raw_identifier.get("normalized_value") or display_value)
        if not provider or not provider_scope or not handle:
            return None
        identifier = {
            "identifier_type": "handle",
            "display_value": display_value or f"@{handle}",
            "normalized_value": handle,
            "provider": provider,
            "provider_scope": provider_scope,
            "is_primary": is_primary,
            "is_verified": is_verified,
        }
    elif identifier_type == "name":
        parsed_name = parse_entity_name(raw_identifier.get("normalized_value") or display_value)
        if parsed_name is None:
            return None
        identifier = {
            "identifier_type": "name",
            "display_value": parsed_name["display_value"],
            "normalized_value": parsed_name["normalized_value"],
            "parsed_name_json": json.dumps(parsed_name["parsed_name"], ensure_ascii=True, sort_keys=True),
            "normalized_full_name": parsed_name["normalized_full_name"],
            "normalized_sort_name": parsed_name["normalized_sort_name"],
            "is_primary": is_primary or (1 if parsed_name["is_full_name"] else 0),
            "is_verified": is_verified,
        }
    else:
        identifier_name = normalize_entity_identifier_name(
            raw_identifier.get("identifier_name") or raw_identifier.get("name") or ""
        )
        normalized_value = normalize_entity_lookup_text(raw_identifier.get("normalized_value") or display_value)
        if not identifier_name or not normalized_value:
            return None
        identifier = {
            "identifier_type": "external_id",
            "display_value": display_value or normalized_value,
            "normalized_value": normalized_value,
            "identifier_name": identifier_name,
            "is_primary": is_primary,
            "is_verified": is_verified,
        }
        identifier_scope = normalize_entity_lookup_text(
            raw_identifier.get("identifier_scope") or raw_identifier.get("scope") or ""
        )
        if identifier_scope:
            identifier["identifier_scope"] = identifier_scope
    if source_kind:
        identifier["source_kind"] = source_kind
    return identifier


def parse_entity_hint_candidates(
    raw_hints: object,
    *,
    role: str,
) -> list[dict[str, object]]:
    if not isinstance(raw_hints, list):
        return []
    candidates: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for raw_hint in raw_hints:
        if not isinstance(raw_hint, dict):
            continue
        display_value = normalize_entity_text(
            raw_hint.get("display_value")
            or raw_hint.get("name")
            or raw_hint.get("value")
            or ""
        )
        if not display_value:
            continue
        base_candidate = parse_entity_candidate_text(display_value, role=role)
        identifiers = list(base_candidate.get("identifiers") or []) if base_candidate is not None else []
        seen_identifier_keys = {
            entity_candidate_identifier_key(identifier)
            for identifier in identifiers
        }
        for raw_identifier in list(raw_hint.get("identifiers") or []):
            identifier = normalize_entity_hint_identifier(
                raw_identifier,
                default_source_kind=raw_hint.get("source_kind"),
            )
            if identifier is None:
                continue
            identifier_key = entity_candidate_identifier_key(identifier)
            if identifier_key in seen_identifier_keys:
                continue
            seen_identifier_keys.add(identifier_key)
            identifiers.append(identifier)
        if not identifiers:
            continue
        parsed_name_identifiers = [identifier for identifier in identifiers if identifier.get("identifier_type") == "name"]
        email_identifiers = [identifier for identifier in identifiers if identifier.get("identifier_type") == "email"]
        derived_entity_type = entity_type_from_candidate_parts(
            name_value=str(parsed_name_identifiers[0]["display_value"]) if parsed_name_identifiers else display_value,
            email_value=str(email_identifiers[0]["normalized_value"]) if email_identifiers else None,
            name_is_full=bool(parsed_name_identifiers and int(parsed_name_identifiers[0].get("is_primary") or 0)),
        )
        entity_type = str(base_candidate.get("entity_type")) if base_candidate is not None else derived_entity_type
        if entity_type == ENTITY_TYPE_UNKNOWN and derived_entity_type != ENTITY_TYPE_UNKNOWN:
            entity_type = derived_entity_type
        display_basis = str(base_candidate.get("display_value")) if base_candidate is not None else display_value
        candidate = {
            "role": role,
            "raw_value": display_value,
            "display_value": display_basis,
            "entity_type": entity_type,
            "identifiers": identifiers,
            "normalized_candidate_key": entity_candidate_key(role, identifiers, display_value),
        }
        key = str(candidate["normalized_candidate_key"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        candidates.append(candidate)
    return candidates


def entity_candidate_match_keys(candidate: dict[str, object]) -> set[str]:
    keys = {
        normalize_entity_lookup_text(candidate.get("display_value") or ""),
        normalize_entity_lookup_text(candidate.get("raw_value") or ""),
    }
    for identifier in list(candidate.get("identifiers") or []):
        if not isinstance(identifier, dict):
            continue
        if identifier.get("identifier_type") != "name":
            continue
        for field_name in ("display_value", "normalized_value", "normalized_full_name", "normalized_sort_name"):
            key = normalize_entity_lookup_text(identifier.get(field_name) or "")
            if key:
                keys.add(key)
    return {key for key in keys if key}


def entity_hints_for_role(raw_hints: object, role: str) -> list[dict[str, object]]:
    if isinstance(raw_hints, str):
        try:
            raw_hints = json.loads(raw_hints)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    if not isinstance(raw_hints, dict):
        return []
    role_keys = [role]
    if role == "participant":
        role_keys.append("participants")
    elif role == "recipient":
        role_keys.append("recipients")
    elif role == "custodian":
        role_keys.append("custodians")
    for role_key in role_keys:
        value = raw_hints.get(role_key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def parse_entity_candidates_with_hints(
    raw_value: object,
    *,
    role: str,
    raw_hints: object = None,
) -> list[dict[str, object]]:
    hint_candidates = parse_entity_hint_candidates(entity_hints_for_role(raw_hints, role), role=role)
    covered_keys: set[str] = set()
    for candidate in hint_candidates:
        covered_keys.update(entity_candidate_match_keys(candidate))
    candidates = list(hint_candidates)
    seen_keys = {str(candidate["normalized_candidate_key"]) for candidate in candidates}
    for candidate in parse_entity_candidates(raw_value, role=role):
        if entity_candidate_match_keys(candidate) & covered_keys:
            continue
        key = str(candidate["normalized_candidate_key"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        candidates.append(candidate)
    return candidates


def parse_entity_candidates(
    raw_value: object,
    *,
    role: str,
    provider: str | None = None,
    provider_scope: str | None = None,
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for part in split_entity_like_values(raw_value, prefer_single_comma_name=role in {"author", "custodian"}):
        candidate = parse_entity_candidate_text(part, role=role, provider=provider, provider_scope=provider_scope)
        if candidate is None:
            continue
        key = str(candidate["normalized_candidate_key"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        candidates.append(candidate)
    return candidates


def source_backed_dataset_policy_from_row(row: sqlite3.Row | None) -> dict[str, object]:
    if row is None or normalize_whitespace(str(row["source_kind"] or "")).lower() == MANUAL_DATASET_SOURCE_KIND:
        return {
            "dataset_id": None,
            "allow_auto_merge": False,
            "email_auto_merge": False,
            "handle_auto_merge": False,
            "phone_auto_merge": False,
            "name_auto_merge": False,
            "external_id_auto_merge_names": set(),
        }
    names = normalize_string_list(row["external_id_auto_merge_names_json"])
    return {
        "dataset_id": int(row["id"]),
        "allow_auto_merge": bool(int(row["allow_auto_merge"] or 0)),
        "email_auto_merge": bool(int(row["email_auto_merge"] or 0)),
        "handle_auto_merge": bool(int(row["handle_auto_merge"] or 0)),
        "phone_auto_merge": bool(int(row["phone_auto_merge"] or 0)),
        "name_auto_merge": bool(int(row["name_auto_merge"] or 0)),
        "external_id_auto_merge_names": {
            name
            for raw_name in names
            for name in [normalize_entity_identifier_name(raw_name)]
            if name
        },
    }


def dataset_merge_policy_payload_from_row(row: sqlite3.Row) -> dict[str, object]:
    source_kind = normalize_whitespace(str(row["source_kind"] or "")).lower()
    names = sorted(
        {
            name
            for raw_name in normalize_string_list(row["external_id_auto_merge_names_json"])
            for name in [normalize_entity_identifier_name(raw_name)]
            if name
        }
    )
    return {
        "dataset_id": int(row["id"]),
        "source_backed": source_kind != MANUAL_DATASET_SOURCE_KIND,
        "allow_auto_merge": bool(int(row["allow_auto_merge"] or 0)),
        "email_auto_merge": bool(int(row["email_auto_merge"] or 0)),
        "handle_auto_merge": bool(int(row["handle_auto_merge"] or 0)),
        "phone_auto_merge": bool(int(row["phone_auto_merge"] or 0)),
        "name_auto_merge": bool(int(row["name_auto_merge"] or 0)),
        "external_id_auto_merge_names": names,
    }


def source_backed_dataset_policy_for_source(
    connection: sqlite3.Connection,
    dataset_source_row: sqlite3.Row | None,
) -> dict[str, object]:
    if dataset_source_row is None:
        return source_backed_dataset_policy_from_row(None)
    dataset_row = get_dataset_row_by_id(connection, int(dataset_source_row["dataset_id"]))
    return source_backed_dataset_policy_from_row(dataset_row)


def identifier_auto_merge_enabled(identifier: dict[str, object], policy: dict[str, object]) -> bool:
    if not bool(policy.get("allow_auto_merge")):
        return False
    identifier_type = str(identifier.get("identifier_type") or "")
    if identifier_type == "email":
        return bool(policy.get("email_auto_merge"))
    if identifier_type == "handle":
        return bool(policy.get("handle_auto_merge")) and bool(identifier.get("provider")) and bool(identifier.get("provider_scope"))
    if identifier_type == "phone":
        return bool(policy.get("phone_auto_merge"))
    if identifier_type == "name":
        return bool(policy.get("name_auto_merge")) and bool(identifier.get("normalized_full_name")) and bool(identifier.get("normalized_sort_name"))
    if identifier_type == "external_id":
        enabled_names = policy.get("external_id_auto_merge_names")
        return isinstance(enabled_names, set) and str(identifier.get("identifier_name") or "") in enabled_names
    return False


def resolution_key_lookup_clause(identifier: dict[str, object]) -> tuple[str, list[object]]:
    identifier_type = str(identifier.get("identifier_type") or "")
    normalized_value = str(identifier.get("normalized_value") or "")
    if identifier_type == "handle":
        return (
            """
            key_type = ? AND provider = ? AND provider_scope = ? AND normalized_value = ?
            """,
            [
                identifier_type,
                identifier.get("provider"),
                identifier.get("provider_scope"),
                normalized_value,
            ],
        )
    if identifier_type == "external_id":
        return (
            """
            key_type = ? AND identifier_name = ? AND COALESCE(identifier_scope, '') = COALESCE(?, '') AND normalized_value = ?
            """,
            [
                identifier_type,
                identifier.get("identifier_name"),
                identifier.get("identifier_scope"),
                normalized_value,
            ],
        )
    return "key_type = ? AND normalized_value = ?", [identifier_type, normalized_value]


def canonicalize_entity_id(connection: sqlite3.Connection, entity_id: int) -> int | None:
    seen: set[int] = set()
    current_id = int(entity_id)
    while current_id not in seen:
        seen.add(current_id)
        row = connection.execute(
            """
            SELECT id, canonical_status, merged_into_entity_id
            FROM entities
            WHERE id = ?
            """,
            (current_id,),
        ).fetchone()
        if row is None:
            return None
        if row["canonical_status"] != ENTITY_STATUS_MERGED:
            return int(row["id"])
        if row["merged_into_entity_id"] is None:
            return None
        current_id = int(row["merged_into_entity_id"])
    return None


def entity_types_conflict(left_type: object, right_type: object) -> bool:
    left = normalize_entity_lookup_text(left_type)
    right = normalize_entity_lookup_text(right_type)
    if not left or not right or left == right:
        return False
    if ENTITY_TYPE_UNKNOWN in {left, right}:
        return False
    concrete = {ENTITY_TYPE_PERSON, ENTITY_TYPE_ORGANIZATION, ENTITY_TYPE_SHARED_MAILBOX, ENTITY_TYPE_SYSTEM_MAILBOX}
    return left in concrete and right in concrete


def active_entity_id_for_resolution_key(
    connection: sqlite3.Connection,
    identifier: dict[str, object],
) -> int | None:
    clause, params = resolution_key_lookup_clause(identifier)
    row = connection.execute(
        f"""
        SELECT entity_id
        FROM entity_resolution_keys
        WHERE {clause}
        ORDER BY id ASC
        LIMIT 1
        """,
        params,
    ).fetchone()
    if row is None:
        return None
    entity_id = canonicalize_entity_id(connection, int(row["entity_id"]))
    if entity_id is None:
        return None
    entity_row = connection.execute(
        """
        SELECT canonical_status
        FROM entities
        WHERE id = ?
        """,
        (entity_id,),
    ).fetchone()
    if entity_row is None or entity_row["canonical_status"] != ENTITY_STATUS_ACTIVE:
        return None
    return entity_id


def create_entity_for_candidate(connection: sqlite3.Connection, candidate: dict[str, object]) -> int:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO entities (
          entity_type, display_name, display_name_source, entity_origin,
          canonical_status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate.get("entity_type") or ENTITY_TYPE_UNKNOWN,
            None,
            ENTITY_DISPLAY_SOURCE_AUTO,
            ENTITY_ORIGIN_OBSERVED,
            ENTITY_STATUS_ACTIVE,
            now,
            now,
        ),
    )
    return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])


def ensure_entity_identifier(
    connection: sqlite3.Connection,
    *,
    entity_id: int,
    identifier: dict[str, object],
) -> int:
    identifier_type = str(identifier.get("identifier_type") or "")
    normalized_value = str(identifier.get("normalized_value") or "")
    if not identifier_type or not normalized_value:
        raise RetrieverError("Entity identifiers require type and normalized value.")
    provider = identifier.get("provider")
    provider_scope = identifier.get("provider_scope")
    identifier_name = identifier.get("identifier_name")
    identifier_scope = identifier.get("identifier_scope")
    existing_row = connection.execute(
        """
        SELECT id
        FROM entity_identifiers
        WHERE entity_id = ?
          AND identifier_type = ?
          AND normalized_value = ?
          AND COALESCE(provider, '') = COALESCE(?, '')
          AND COALESCE(provider_scope, '') = COALESCE(?, '')
          AND COALESCE(identifier_name, '') = COALESCE(?, '')
          AND COALESCE(identifier_scope, '') = COALESCE(?, '')
        ORDER BY id ASC
        LIMIT 1
        """,
        (entity_id, identifier_type, normalized_value, provider, provider_scope, identifier_name, identifier_scope),
    ).fetchone()
    now = utc_now()
    if existing_row is not None:
        connection.execute(
            """
            UPDATE entity_identifiers
            SET display_value = COALESCE(NULLIF(display_value, ''), ?),
                parsed_name_json = COALESCE(parsed_name_json, ?),
                parsed_phone_json = COALESCE(parsed_phone_json, ?),
                normalized_full_name = COALESCE(normalized_full_name, ?),
                normalized_sort_name = COALESCE(normalized_sort_name, ?),
                is_primary = CASE WHEN ? THEN 1 ELSE is_primary END,
                is_verified = CASE WHEN ? THEN 1 ELSE is_verified END,
                updated_at = ?
            WHERE id = ?
            """,
            (
                identifier.get("display_value") or normalized_value,
                identifier.get("parsed_name_json"),
                identifier.get("parsed_phone_json"),
                identifier.get("normalized_full_name"),
                identifier.get("normalized_sort_name"),
                1 if int(identifier.get("is_primary") or 0) else 0,
                1 if int(identifier.get("is_verified") or 0) else 0,
                now,
                int(existing_row["id"]),
            ),
        )
        return int(existing_row["id"])
    connection.execute(
        """
        INSERT INTO entity_identifiers (
          entity_id, identifier_type, display_value, normalized_value,
          provider, provider_scope, identifier_name, identifier_scope,
          parsed_name_json, parsed_phone_json, normalized_full_name, normalized_sort_name,
          is_primary, is_verified, source_kind, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entity_id,
            identifier_type,
            str(identifier.get("display_value") or normalized_value),
            normalized_value,
            provider,
            provider_scope,
            identifier_name,
            identifier_scope,
            identifier.get("parsed_name_json"),
            identifier.get("parsed_phone_json"),
            identifier.get("normalized_full_name"),
            identifier.get("normalized_sort_name"),
            1 if int(identifier.get("is_primary") or 0) else 0,
            1 if int(identifier.get("is_verified") or 0) else 0,
            str(identifier.get("source_kind") or "auto"),
            now,
            now,
        ),
    )
    return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])


def ensure_entity_resolution_key(
    connection: sqlite3.Connection,
    *,
    entity_id: int,
    identifier_id: int,
    identifier: dict[str, object],
) -> int | None:
    now = utc_now()
    try:
        connection.execute(
            """
            INSERT INTO entity_resolution_keys (
              entity_id, identifier_id, key_type, provider, provider_scope,
              identifier_name, identifier_scope, normalized_value, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_id,
                identifier_id,
                identifier.get("identifier_type"),
                identifier.get("provider"),
                identifier.get("provider_scope"),
                identifier.get("identifier_name"),
                identifier.get("identifier_scope"),
                identifier.get("normalized_value"),
                now,
                now,
            ),
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    except sqlite3.IntegrityError:
        existing_owner = active_entity_id_for_resolution_key(connection, identifier)
        if existing_owner == entity_id:
            return None
        return None


def recompute_entity_caches(connection: sqlite3.Connection, entity_id: int) -> None:
    row = connection.execute(
        """
        SELECT *
        FROM entities
        WHERE id = ?
        """,
        (entity_id,),
    ).fetchone()
    if row is None or row["canonical_status"] != ENTITY_STATUS_ACTIVE:
        return
    identifiers = connection.execute(
        """
        SELECT *
        FROM entity_identifiers
        WHERE entity_id = ?
        ORDER BY is_primary DESC, is_verified DESC, id ASC
        """,
        (entity_id,),
    ).fetchall()
    emails = [item for item in identifiers if item["identifier_type"] == "email"]
    phones = [item for item in identifiers if item["identifier_type"] == "phone"]
    names = [item for item in identifiers if item["identifier_type"] == "name"]
    primary_email = str(emails[0]["normalized_value"]) if emails else None
    primary_phone = str(phones[0]["normalized_value"]) if phones else None
    display_name = row["display_name"]
    sort_name = row["sort_name"]
    if row["display_name_source"] != ENTITY_DISPLAY_SOURCE_MANUAL:
        full_name_rows = [
            item
            for item in names
            if normalize_whitespace(str(item["normalized_full_name"] or ""))
            and len(str(item["normalized_full_name"]).split()) >= 2
        ]
        display_name_rows = full_name_rows
        if row["entity_type"] != ENTITY_TYPE_PERSON and not display_name_rows:
            display_name_rows = names
        if display_name_rows:
            display_name = str(display_name_rows[0]["display_value"])
            sort_name = str(display_name_rows[0]["normalized_sort_name"] or display_name_rows[0]["normalized_full_name"])
        else:
            display_name = None
            sort_name = None
    resolution_key_row = connection.execute(
        """
        SELECT 1
        FROM entity_resolution_keys
        WHERE entity_id = ?
        LIMIT 1
        """,
        (entity_id,),
    ).fetchone()
    entity_origin = row["entity_origin"]
    if entity_origin != ENTITY_ORIGIN_MANUAL:
        entity_origin = ENTITY_ORIGIN_IDENTIFIED if resolution_key_row is not None else ENTITY_ORIGIN_OBSERVED
    connection.execute(
        """
        UPDATE entities
        SET display_name = ?, primary_email = ?, primary_phone = ?,
            sort_name = ?, entity_origin = ?, updated_at = ?
        WHERE id = ?
        """,
        (display_name, primary_email, primary_phone, sort_name, entity_origin, utc_now(), entity_id),
    )


def resolve_entity_candidate(
    connection: sqlite3.Connection,
    candidate: dict[str, object],
    *,
    policy: dict[str, object],
) -> int:
    identifiers = list(candidate.get("identifiers") or [])
    enabled_identifiers = [
        identifier for identifier in identifiers if identifier_auto_merge_enabled(identifier, policy)
    ]
    matched_entity_ids: set[int] = set()
    for identifier in enabled_identifiers:
        matched_entity_id = active_entity_id_for_resolution_key(connection, identifier)
        if matched_entity_id is not None:
            matched_entity_ids.add(matched_entity_id)
    if len(matched_entity_ids) == 1:
        entity_id = next(iter(matched_entity_ids))
        entity_row = connection.execute(
            "SELECT entity_type FROM entities WHERE id = ?",
            (entity_id,),
        ).fetchone()
        if entity_row is not None and entity_types_conflict(entity_row["entity_type"], candidate.get("entity_type")):
            entity_id = create_entity_for_candidate(connection, candidate)
            enabled_identifiers = []
    else:
        entity_id = create_entity_for_candidate(connection, candidate)
        if len(matched_entity_ids) > 1:
            enabled_identifiers = []
    inserted_identifier_ids: dict[int, dict[str, object]] = {}
    for identifier in identifiers:
        identifier_id = ensure_entity_identifier(connection, entity_id=entity_id, identifier=identifier)
        inserted_identifier_ids[identifier_id] = identifier
    for identifier_id, identifier in inserted_identifier_ids.items():
        if identifier in enabled_identifiers:
            ensure_entity_resolution_key(
                connection,
                entity_id=entity_id,
                identifier_id=identifier_id,
                identifier=identifier,
            )
    recompute_entity_caches(connection, entity_id)
    return entity_id


def entity_display_label_from_row(row: sqlite3.Row | dict[str, object] | None) -> str:
    if row is None:
        return "Unknown Entity"
    display_name = normalize_entity_text(row["display_name"] if "display_name" in row.keys() else None)  # type: ignore[attr-defined,index]
    primary_email = normalize_entity_email(row["primary_email"] if "primary_email" in row.keys() else None)  # type: ignore[attr-defined,index]
    primary_phone = normalize_entity_text(row["primary_phone"] if "primary_phone" in row.keys() else None)  # type: ignore[attr-defined,index]
    entity_id = row["id"] if "id" in row.keys() else None  # type: ignore[attr-defined,index]
    if display_name and primary_email:
        return f"{display_name} <{primary_email}>"
    if display_name:
        return display_name
    if primary_email:
        return primary_email
    if primary_phone:
        return primary_phone
    return f"Unknown Entity {entity_id}" if entity_id is not None else "Unknown Entity"


def entity_display_label(connection: sqlite3.Connection, entity_id: int) -> str:
    row = connection.execute(
        """
        SELECT *
        FROM entities
        WHERE id = ?
        """,
        (entity_id,),
    ).fetchone()
    return entity_display_label_from_row(row)


def rebuild_document_entity_caches(connection: sqlite3.Connection, document_id: int) -> dict[str, object]:
    document_row = connection.execute(
        f"""
        SELECT id, {MANUAL_FIELD_LOCKS_COLUMN} AS locks_json
        FROM documents
        WHERE id = ?
        """,
        (document_id,),
    ).fetchone()
    if document_row is None:
        raise RetrieverError(f"Unknown document id: {document_id}")
    locked_fields = set(normalize_string_list(document_row["locks_json"]))
    rows = connection.execute(
        """
        SELECT de.role, de.ordinal, e.*
        FROM document_entities de
        JOIN entities e ON e.id = de.entity_id
        WHERE de.document_id = ?
          AND e.canonical_status = ?
        ORDER BY de.role ASC, de.ordinal ASC, de.id ASC
        """,
        (document_id, ENTITY_STATUS_ACTIVE),
    ).fetchall()
    labels_by_role: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        label = entity_display_label_from_row(row)
        if label not in labels_by_role[row["role"]]:
            labels_by_role[row["role"]].append(label)
    updates: dict[str, object] = {}
    if "author" not in locked_fields:
        updates["author"] = labels_by_role.get("author", [None])[0] if labels_by_role.get("author") else None
    if "participants" not in locked_fields:
        updates["participants"] = ", ".join(labels_by_role.get("participant", [])) or None
    if "recipients" not in locked_fields:
        updates["recipients"] = ", ".join(labels_by_role.get("recipient", [])) or None
    if "custodian" not in locked_fields:
        updates["custodians_json"] = json.dumps(labels_by_role.get("custodian", []), ensure_ascii=True)
    if updates:
        set_clause = ", ".join(f"{quote_identifier(column)} = ?" for column in updates)
        connection.execute(
            f"""
            UPDATE documents
            SET {set_clause}, updated_at = ?
            WHERE id = ?
            """,
            [*updates.values(), utc_now(), document_id],
        )
    return updates


def entity_candidate_is_globally_ignored(connection: sqlite3.Connection, candidate: dict[str, object]) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM entity_overrides
        WHERE scope_type = 'global'
          AND override_effect = 'ignore'
          AND (
            normalized_candidate_key = ?
            OR source_entity_id IS NULL AND normalized_candidate_key IS NULL AND source_hint = ?
          )
        LIMIT 1
        """,
        (candidate.get("normalized_candidate_key"), candidate.get("raw_value")),
    ).fetchone()
    return row is not None


def document_entity_override_for_candidate(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    role: str,
    source_entity_id: int,
    candidate: dict[str, object],
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM entity_overrides
        WHERE scope_type = 'document'
          AND scope_id = ?
          AND (role IS NULL OR role = ?)
          AND (
            source_entity_id = ?
            OR normalized_candidate_key = ?
            OR (
              source_entity_id IS NULL
              AND normalized_candidate_key IS NULL
              AND source_hint = ?
            )
          )
        ORDER BY id DESC
        LIMIT 1
        """,
        (
            int(document_id),
            role,
            int(source_entity_id),
            candidate.get("normalized_candidate_key"),
            candidate.get("raw_value"),
        ),
    ).fetchone()


def sync_document_entities(
    connection: sqlite3.Connection,
    document_id: int,
    *,
    refresh_fts: bool = True,
) -> dict[str, object]:
    if not all(
        table_exists(connection, table_name)
        for table_name in ("entities", "entity_identifiers", "entity_resolution_keys", "document_entities")
    ):
        return {"document_id": document_id, "synced": False, "reason": "entity schema unavailable"}
    active_rows = active_occurrence_rows_for_document(connection, document_id)
    auto_links: list[tuple[int, int, str, int, str, str | None, str, str, str]] = []
    seen_role_entities: set[tuple[str, int]] = set()
    ordinals_by_role: dict[str, int] = defaultdict(int)
    for occurrence_row in occurrence_rows_in_preferred_order(active_rows):
        dataset_source_row = dataset_source_row_for_occurrence(connection, occurrence_row)
        if dataset_source_row is None:
            dataset_source_row = dataset_source_row_for_document_membership(connection, document_id)
        if dataset_source_row is None:
            continue
        policy = source_backed_dataset_policy_for_source(connection, dataset_source_row)
        role_values = (
            ("author", occurrence_row["extracted_author"]),
            ("participant", occurrence_row["extracted_participants"]),
            ("recipient", occurrence_row["extracted_recipients"]),
            ("custodian", occurrence_row["custodian"]),
        )
        raw_entity_hints = occurrence_row["entity_hints_json"] if "entity_hints_json" in occurrence_row.keys() else None
        for role, raw_value in role_values:
            for candidate in parse_entity_candidates_with_hints(raw_value, role=role, raw_hints=raw_entity_hints):
                if entity_candidate_is_globally_ignored(connection, candidate):
                    continue
                entity_id = resolve_entity_candidate(connection, candidate, policy=policy)
                override_row = document_entity_override_for_candidate(
                    connection,
                    document_id=document_id,
                    role=role,
                    source_entity_id=entity_id,
                    candidate=candidate,
                )
                if override_row is not None:
                    if override_row["override_effect"] == "remove":
                        continue
                    if override_row["override_effect"] == "replace":
                        replacement_entity_id = override_row["replacement_entity_id"]
                        if replacement_entity_id is None:
                            continue
                        canonical_replacement_id = canonicalize_entity_id(connection, int(replacement_entity_id))
                        if canonical_replacement_id is None:
                            continue
                        replacement_row = connection.execute(
                            """
                            SELECT canonical_status
                            FROM entities
                            WHERE id = ?
                            """,
                            (canonical_replacement_id,),
                        ).fetchone()
                        if replacement_row is None or replacement_row["canonical_status"] != ENTITY_STATUS_ACTIVE:
                            continue
                        entity_id = canonical_replacement_id
                role_entity_key = (role, entity_id)
                if role_entity_key in seen_role_entities:
                    continue
                seen_role_entities.add(role_entity_key)
                ordinal = ordinals_by_role[role]
                ordinals_by_role[role] += 1
                evidence = {
                    "raw_value": candidate.get("raw_value"),
                    "occurrence_id": int(occurrence_row["id"]),
                    "dataset_source_id": int(dataset_source_row["id"]) if dataset_source_row is not None else None,
                    "normalized_candidate_key": candidate.get("normalized_candidate_key"),
                }
                auto_links.append(
                    (
                        document_id,
                        entity_id,
                        role,
                        ordinal,
                        "auto",
                        None,
                        json.dumps(evidence, ensure_ascii=True, sort_keys=True),
                        utc_now(),
                        utc_now(),
                    )
                )
    connection.execute(
        """
        DELETE FROM document_entities
        WHERE document_id = ?
          AND assignment_mode = 'auto'
        """,
        (document_id,),
    )
    if auto_links:
        connection.executemany(
            """
            INSERT OR IGNORE INTO document_entities (
              document_id, entity_id, role, ordinal, assignment_mode,
              observed_title, evidence_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            auto_links,
        )
    cache_updates = rebuild_document_entity_caches(connection, document_id)
    if refresh_fts:
        refresh_documents_fts_row(connection, document_id)
    return {
        "document_id": document_id,
        "synced": True,
        "auto_link_count": len(auto_links),
        "cache_updates": cache_updates,
    }


def refresh_document_control_number_aliases(connection: sqlite3.Connection, document_id: int) -> None:
    now = utc_now()
    connection.execute(
        "DELETE FROM document_control_number_aliases WHERE document_id = ?",
        (document_id,),
    )
    document_row = connection.execute(
        "SELECT control_number FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    alias_rows: list[tuple[int, int | None, str, str, int, str, str]] = []
    if document_row is not None and normalize_whitespace(str(document_row["control_number"] or "")):
        alias_rows.append(
            (
                document_id,
                None,
                str(document_row["control_number"]),
                "document_primary",
                1,
                now,
                now,
            )
        )
    occurrence_rows = connection.execute(
        """
        SELECT id, occurrence_control_number, lifecycle_status
        FROM document_occurrences
        WHERE document_id = ?
        ORDER BY id ASC
        """,
        (document_id,),
    ).fetchall()
    for occurrence_row in occurrence_rows:
        alias_value = normalize_whitespace(str(occurrence_row["occurrence_control_number"] or ""))
        if not alias_value:
            continue
        alias_rows.append(
            (
                document_id,
                int(occurrence_row["id"]),
                alias_value,
                "occurrence_control_number",
                1 if occurrence_row["lifecycle_status"] == ACTIVE_OCCURRENCE_STATUS else 0,
                now,
                now,
            )
        )
    if alias_rows:
        connection.executemany(
            """
            INSERT INTO document_control_number_aliases (
              document_id, occurrence_id, alias_value, alias_type, active_flag, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            alias_rows,
        )


def refresh_canonical_metadata_conflicts(
    connection: sqlite3.Connection,
    document_id: int,
    active_rows: list[sqlite3.Row],
) -> None:
    tracked_fields = {
        "author": "extracted_author",
        "title": "extracted_title",
        "subject": "extracted_subject",
        "participants": "extracted_participants",
        "recipients": "extracted_recipients",
        "date_created": "extracted_doc_authored_at",
        "date_modified": "extracted_doc_modified_at",
        "content_type": "extracted_content_type",
    }
    now = utc_now()
    connection.execute(
        "DELETE FROM canonical_metadata_conflicts WHERE document_id = ?",
        (document_id,),
    )
    rows_to_insert: list[tuple[int, str, int, str, str, str]] = []
    for field_name, occurrence_column in tracked_fields.items():
        distinct_values = {
            normalize_whitespace(str(row[occurrence_column] or "")): row
            for row in active_rows
            if normalize_whitespace(str(row[occurrence_column] or ""))
        }
        if len(distinct_values) <= 1:
            continue
        for value, row in distinct_values.items():
            rows_to_insert.append(
                (
                    document_id,
                    field_name,
                    int(row["id"]),
                    value,
                    now,
                    now,
                )
            )
    if rows_to_insert:
        connection.executemany(
            """
            INSERT INTO canonical_metadata_conflicts (
              document_id, field_name, occurrence_id, value, first_seen_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )


def dataset_source_row_for_occurrence_values(
    connection: sqlite3.Connection,
    *,
    source_kind: object,
    source_rel_path: object,
    production_id: object = None,
) -> sqlite3.Row | None:
    normalized_source_kind = normalize_whitespace(str(source_kind or "")).lower()
    normalized_source_rel_path = normalize_whitespace(str(source_rel_path or ""))
    if normalized_source_kind == FILESYSTEM_SOURCE_KIND:
        return get_dataset_source_row(
            connection,
            source_kind=FILESYSTEM_SOURCE_KIND,
            source_locator=filesystem_dataset_locator(),
        )
    if normalized_source_kind in {PST_SOURCE_KIND, MBOX_SOURCE_KIND} and normalized_source_rel_path:
        return get_dataset_source_row(
            connection,
            source_kind=normalized_source_kind,
            source_locator=normalized_source_rel_path,
        )
    if normalized_source_kind == PRODUCTION_SOURCE_KIND:
        if production_id is None:
            return None
        production_row = connection.execute(
            "SELECT rel_root FROM productions WHERE id = ?",
            (production_id,),
        ).fetchone()
        if production_row is None:
            return None
        return get_dataset_source_row(
            connection,
            source_kind=PRODUCTION_SOURCE_KIND,
            source_locator=str(production_row["rel_root"]),
        )
    if normalized_source_kind == SLACK_EXPORT_SOURCE_KIND and normalized_source_rel_path:
        return connection.execute(
            """
            SELECT *
            FROM dataset_sources
            WHERE source_kind = ?
              AND (? = source_locator OR ? LIKE source_locator || '/%')
            ORDER BY LENGTH(source_locator) DESC, id ASC
            LIMIT 1
            """,
            (SLACK_EXPORT_SOURCE_KIND, normalized_source_rel_path, normalized_source_rel_path),
        ).fetchone()
    return None


def dataset_source_row_for_occurrence(
    connection: sqlite3.Connection,
    occurrence_row: sqlite3.Row,
) -> sqlite3.Row | None:
    if "dataset_source_id" in occurrence_row.keys() and occurrence_row["dataset_source_id"] is not None:
        row = connection.execute(
            """
            SELECT *
            FROM dataset_sources
            WHERE id = ?
            """,
            (int(occurrence_row["dataset_source_id"]),),
        ).fetchone()
        if row is not None:
            return row
    return dataset_source_row_for_occurrence_values(
        connection,
        source_kind=occurrence_row["source_kind"],
        source_rel_path=occurrence_row["source_rel_path"],
        production_id=occurrence_row["production_id"],
    )


def dataset_source_row_for_document_membership(
    connection: sqlite3.Connection,
    document_id: int,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT ds.*
        FROM dataset_documents dd
        JOIN dataset_sources ds ON ds.id = dd.dataset_source_id
        WHERE dd.document_id = ?
          AND dd.dataset_source_id IS NOT NULL
        ORDER BY dd.dataset_id ASC, dd.dataset_source_id ASC
        LIMIT 1
        """,
        (int(document_id),),
    ).fetchone()


def refresh_source_backed_dataset_memberships_for_document(connection: sqlite3.Connection, document_id: int) -> None:
    active_rows = active_occurrence_rows_for_document(connection, document_id)
    document_row = connection.execute(
        "SELECT parent_document_id FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    connection.execute(
        """
        DELETE FROM dataset_documents
        WHERE document_id = ?
          AND dataset_source_id IS NOT NULL
        """,
        (document_id,),
    )
    if document_row is not None and document_row["parent_document_id"] is not None:
        parent_source_rows = connection.execute(
            """
            SELECT dataset_id, dataset_source_id
            FROM dataset_documents
            WHERE document_id = ?
              AND dataset_source_id IS NOT NULL
            ORDER BY dataset_id ASC, dataset_source_id ASC
            """,
            (document_row["parent_document_id"],),
        ).fetchall()
        for parent_source_row in parent_source_rows:
            ensure_dataset_document_membership(
                connection,
                dataset_id=int(parent_source_row["dataset_id"]),
                document_id=document_id,
                dataset_source_id=int(parent_source_row["dataset_source_id"]),
            )
        refresh_document_dataset_cache(connection, document_id)
        return
    for occurrence_row in active_rows:
        dataset_source_row = dataset_source_row_for_occurrence(connection, occurrence_row)
        if dataset_source_row is None:
            continue
        ensure_dataset_document_membership(
            connection,
            dataset_id=int(dataset_source_row["dataset_id"]),
            document_id=document_id,
            dataset_source_id=int(dataset_source_row["id"]),
        )
    refresh_document_dataset_cache(connection, document_id)


def refresh_document_from_occurrences(connection: sqlite3.Connection, document_id: int) -> dict[str, object]:
    document_row = connection.execute(
        "SELECT * FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    if document_row is None:
        raise RetrieverError(f"Unknown document id: {document_id}")

    active_rows = active_occurrence_rows_for_document(connection, document_id)
    if not active_rows:
        if document_row["canonical_status"] == CANONICAL_STATUS_MERGED:
            connection.execute(
                """
                UPDATE documents
                SET dataset_id = NULL, updated_at = ?
                WHERE id = ?
                """,
                (utc_now(), document_id),
            )
            refresh_document_control_number_aliases(connection, document_id)
            return {
                "document_id": document_id,
                "preferred_occurrence_id": None,
                "active_occurrence_count": 0,
                "canonical_status": CANONICAL_STATUS_MERGED,
            }
        connection.execute(
            """
            UPDATE documents
            SET canonical_status = ?, lifecycle_status = ?, dataset_id = NULL, updated_at = ?
            WHERE id = ?
            """,
            (CANONICAL_STATUS_DERELICT, "missing", utc_now(), document_id),
        )
        refresh_document_control_number_aliases(connection, document_id)
        refresh_documents_fts_row(connection, document_id)
        return {
            "document_id": document_id,
            "preferred_occurrence_id": None,
            "active_occurrence_count": 0,
            "canonical_status": CANONICAL_STATUS_DERELICT,
        }

    preferred_row = select_preferred_occurrence(active_rows)
    assert preferred_row is not None
    locked_fields = set(normalize_string_list(document_row[MANUAL_FIELD_LOCKS_COLUMN]))
    resolved_author = occurrence_field_value(preferred_row, active_rows, "extracted_author")
    resolved_content_type = occurrence_field_value(preferred_row, active_rows, "extracted_content_type")
    resolved_custodians = custodian_values_from_occurrence_rows(active_rows)
    resolved_date_created = occurrence_field_value(preferred_row, active_rows, "extracted_doc_authored_at")
    resolved_date_modified = occurrence_field_value(preferred_row, active_rows, "extracted_doc_modified_at")
    resolved_title = occurrence_field_value(preferred_row, active_rows, "extracted_title")
    resolved_subject = occurrence_field_value(preferred_row, active_rows, "extracted_subject")
    resolved_participants = occurrence_field_value(preferred_row, active_rows, "extracted_participants")
    resolved_recipients = occurrence_field_value(preferred_row, active_rows, "extracted_recipients")
    if "author" in locked_fields:
        resolved_author = document_row["author"]
    if "content_type" in locked_fields:
        resolved_content_type = document_row["content_type"]
    if "date_created" in locked_fields:
        resolved_date_created = document_row["date_created"]
    if "date_modified" in locked_fields:
        resolved_date_modified = document_row["date_modified"]
    if "title" in locked_fields:
        resolved_title = document_row["title"]
    if "subject" in locked_fields:
        resolved_subject = document_row["subject"]
    if "participants" in locked_fields:
        resolved_participants = document_row["participants"]
    if "recipients" in locked_fields:
        resolved_recipients = document_row["recipients"]
    canonical_control_number = None
    for row in sorted(
        active_rows,
        key=lambda candidate: (
            0 if int(candidate["id"]) == int(preferred_row["id"]) else 1,
            source_kind_priority(candidate["source_kind"]),
            text_status_priority(candidate["text_status"]),
            parse_utc_timestamp(candidate["ingested_at"]) or datetime.max.replace(tzinfo=timezone.utc),
            int(candidate["id"]),
        ),
    ):
        candidate_control_number = normalize_whitespace(str(row["occurrence_control_number"] or ""))
        if candidate_control_number:
            canonical_control_number = candidate_control_number
            break

    updated_values = {
        "control_number": canonical_control_number or document_row["control_number"],
        "canonical_kind": canonical_kind_from_metadata(
            extracted_content_type=occurrence_field_value(preferred_row, active_rows, "extracted_content_type"),
            extracted_kind=occurrence_field_value(preferred_row, active_rows, "extracted_kind"),
            file_type=preferred_row["file_type"],
            source_kind=preferred_row["source_kind"],
        ),
        "canonical_status": CANONICAL_STATUS_ACTIVE,
        "merged_into_document_id": None,
        "source_kind": preferred_row["source_kind"],
        "source_rel_path": preferred_row["source_rel_path"],
        "source_item_id": preferred_row["source_item_id"],
        "source_folder_path": preferred_row["source_folder_path"],
        "production_id": preferred_row["production_id"],
        "begin_bates": preferred_row["begin_bates"],
        "end_bates": preferred_row["end_bates"],
        "begin_attachment": preferred_row["begin_attachment"],
        "end_attachment": preferred_row["end_attachment"],
        "rel_path": preferred_row["rel_path"],
        "file_name": preferred_row["file_name"],
        "file_type": preferred_row["file_type"],
        "file_size": preferred_row["file_size"],
        "author": resolved_author,
        "content_type": resolved_content_type,
        "custodians_json": json.dumps(resolved_custodians),
        "date_created": resolved_date_created,
        "date_modified": resolved_date_modified,
        "title": resolved_title,
        "subject": resolved_subject,
        "participants": resolved_participants,
        "recipients": resolved_recipients,
        "file_hash": preferred_row["file_hash"],
        "text_status": min((row["text_status"] for row in active_rows), key=text_status_priority),
        "lifecycle_status": "active",
        "ingested_at": min(
            (parse_utc_timestamp(row["ingested_at"]) or datetime.max.replace(tzinfo=timezone.utc) for row in active_rows)
        ).isoformat().replace("+00:00", "Z"),
        "last_seen_at": max(
            (
                parse_utc_timestamp(row["last_seen_at"]) or datetime.min.replace(tzinfo=timezone.utc)
                for row in active_rows
            )
        ).isoformat().replace("+00:00", "Z"),
        "updated_at": utc_now(),
    }
    connection.execute(
        """
        UPDATE documents
        SET control_number = ?, canonical_kind = ?, canonical_status = ?, merged_into_document_id = ?,
            source_kind = ?, source_rel_path = ?, source_item_id = ?, source_folder_path = ?, production_id = ?,
            begin_bates = ?, end_bates = ?, begin_attachment = ?, end_attachment = ?, rel_path = ?, file_name = ?,
            file_type = ?, file_size = ?, author = ?, content_type = ?, custodians_json = ?, date_created = ?, date_modified = ?,
            title = ?, subject = ?, participants = ?, recipients = ?, file_hash = ?, text_status = ?,
            lifecycle_status = ?, ingested_at = ?, last_seen_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            updated_values["control_number"],
            updated_values["canonical_kind"],
            updated_values["canonical_status"],
            updated_values["merged_into_document_id"],
            updated_values["source_kind"],
            updated_values["source_rel_path"],
            updated_values["source_item_id"],
            updated_values["source_folder_path"],
            updated_values["production_id"],
            updated_values["begin_bates"],
            updated_values["end_bates"],
            updated_values["begin_attachment"],
            updated_values["end_attachment"],
            updated_values["rel_path"],
            updated_values["file_name"],
            updated_values["file_type"],
            updated_values["file_size"],
            updated_values["author"],
            updated_values["content_type"],
            updated_values["custodians_json"],
            updated_values["date_created"],
            updated_values["date_modified"],
            updated_values["title"],
            updated_values["subject"],
            updated_values["participants"],
            updated_values["recipients"],
            updated_values["file_hash"],
            updated_values["text_status"],
            updated_values["lifecycle_status"],
            updated_values["ingested_at"],
            updated_values["last_seen_at"],
            updated_values["updated_at"],
            document_id,
        ),
    )
    refresh_canonical_metadata_conflicts(connection, document_id, active_rows)
    refresh_document_control_number_aliases(connection, document_id)
    sync_document_entities(connection, document_id, refresh_fts=False)
    refresh_documents_fts_row(connection, document_id)
    return {
        "document_id": document_id,
        "preferred_occurrence_id": int(preferred_row["id"]),
        "active_occurrence_count": len(active_rows),
        "canonical_status": CANONICAL_STATUS_ACTIVE,
    }


def get_document_by_dedupe_key(
    connection: sqlite3.Connection,
    *,
    basis: str,
    key_value: str | None,
) -> sqlite3.Row | None:
    normalized_key = normalize_whitespace(str(key_value or ""))
    if not normalized_key:
        return None
    return connection.execute(
        """
        SELECT d.*
        FROM document_dedupe_keys dk
        JOIN documents d ON d.id = dk.document_id
        WHERE dk.basis = ? AND dk.key_value = ?
        """,
        (basis, normalized_key),
    ).fetchone()


def bind_document_dedupe_key(
    connection: sqlite3.Connection,
    *,
    basis: str,
    key_value: str | None,
    document_id: int,
) -> bool:
    normalized_key = normalize_whitespace(str(key_value or ""))
    if not normalized_key:
        return False
    now = utc_now()
    cursor = connection.execute(
        """
        INSERT INTO document_dedupe_keys (basis, key_value, document_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(basis, key_value) DO UPDATE SET
          document_id = excluded.document_id,
          updated_at = excluded.updated_at
        WHERE document_dedupe_keys.document_id != excluded.document_id
        """,
        (basis, normalized_key, document_id, now, now),
    )
    return int(cursor.rowcount or 0) > 0


def find_active_occurrence_by_source_identity(
    connection: sqlite3.Connection,
    *,
    source_kind: str | None,
    custodian: str | None,
    source_rel_path: str | None,
    source_item_id: str | None,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM document_occurrences
        WHERE source_kind = ?
          AND COALESCE(custodian, '') = COALESCE(?, '')
          AND COALESCE(source_rel_path, '') = COALESCE(?, '')
          AND COALESCE(source_item_id, '') = COALESCE(?, '')
          AND lifecycle_status = 'active'
        ORDER BY id ASC
        LIMIT 1
        """,
        (source_kind, custodian, source_rel_path, source_item_id),
    ).fetchone()


def container_root_occurrence_rows_for_source(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
    include_deleted: bool = False,
) -> list[sqlite3.Row]:
    normalized_source_kind = normalize_whitespace(str(source_kind or "")).lower()
    normalized_source_rel_path = normalize_whitespace(str(source_rel_path or ""))
    if not normalized_source_kind or not normalized_source_rel_path:
        return []
    clauses = [
        "parent_occurrence_id IS NULL",
        "source_kind = ?",
        "source_rel_path = ?",
    ]
    parameters: list[object] = [normalized_source_kind, normalized_source_rel_path]
    if not include_deleted:
        clauses.append("lifecycle_status != 'deleted'")
    return connection.execute(
        f"""
        SELECT *
        FROM document_occurrences
        WHERE {' AND '.join(clauses)}
        ORDER BY id ASC
        """,
        parameters,
    ).fetchall()


def container_document_ids_for_root_occurrence_ids(
    connection: sqlite3.Connection,
    root_occurrence_ids: list[int],
) -> set[int]:
    normalized_ids = sorted({int(occurrence_id) for occurrence_id in root_occurrence_ids})
    if not normalized_ids:
        return set()
    placeholders = ", ".join("?" for _ in normalized_ids)
    rows = connection.execute(
        f"""
        SELECT DISTINCT document_id
        FROM document_occurrences
        WHERE id IN ({placeholders}) OR parent_occurrence_id IN ({placeholders})
        ORDER BY document_id ASC
        """,
        [*normalized_ids, *normalized_ids],
    ).fetchall()
    return {int(row["document_id"]) for row in rows}


def container_root_document_ids_for_source(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
    include_deleted: bool = False,
) -> set[int]:
    return {
        int(row["document_id"])
        for row in container_root_occurrence_rows_for_source(
            connection,
            source_kind=source_kind,
            source_rel_path=source_rel_path,
            include_deleted=include_deleted,
        )
    }


def container_document_ids_for_source(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_rel_path: str,
    include_deleted: bool = False,
) -> set[int]:
    root_occurrence_ids = [
        int(row["id"])
        for row in container_root_occurrence_rows_for_source(
            connection,
            source_kind=source_kind,
            source_rel_path=source_rel_path,
            include_deleted=include_deleted,
        )
    ]
    return container_document_ids_for_root_occurrence_ids(connection, root_occurrence_ids)


def upsert_document_occurrence(
    connection: sqlite3.Connection,
    *,
    document_id: int,
    existing_occurrence_id: int | None,
    parent_occurrence_id: int | None,
    occurrence_control_number: str | None,
    source_kind: str | None,
    source_rel_path: str | None,
    source_item_id: str | None,
    source_folder_path: str | None,
    production_id: int | None,
    begin_bates: str | None,
    end_bates: str | None,
    begin_attachment: str | None,
    end_attachment: str | None,
    rel_path: str,
    file_name: str,
    file_type: str | None,
    mime_type: str | None,
    file_size: int | None,
    file_hash: str | None,
    custodian: str | None,
    fs_created_at: str | None,
    fs_modified_at: str | None,
    extracted: dict[str, object],
    has_preview: bool,
    text_status: str,
    ingested_at: str,
    last_seen_at: str,
    updated_at: str,
) -> int:
    dataset_source_row = dataset_source_row_for_occurrence_values(
        connection,
        source_kind=source_kind,
        source_rel_path=source_rel_path,
        production_id=production_id,
    )
    dataset_source_id = int(dataset_source_row["id"]) if dataset_source_row is not None else None
    raw_entity_hints = extracted.get("entity_hints")
    entity_hints_json = json.dumps(
        raw_entity_hints if isinstance(raw_entity_hints, dict) else {},
        ensure_ascii=True,
        sort_keys=True,
    )
    occurrence_values = (
        document_id,
        dataset_source_id,
        parent_occurrence_id,
        occurrence_control_number,
        source_kind,
        source_rel_path,
        source_item_id,
        source_folder_path,
        production_id,
        begin_bates,
        end_bates,
        begin_attachment,
        end_attachment,
        rel_path,
        file_name,
        file_type,
        mime_type,
        file_size,
        file_hash,
        custodian,
        fs_created_at,
        fs_modified_at,
        extracted.get("author"),
        extracted.get("title"),
        extracted.get("subject"),
        extracted.get("participants"),
        extracted.get("recipients"),
        extracted.get("date_created"),
        extracted.get("date_modified"),
        extracted.get("content_type"),
        canonical_kind_from_metadata(
            extracted_content_type=extracted.get("content_type"),
            file_type=file_type,
            source_kind=source_kind,
        ),
        entity_hints_json,
        text_status,
        ACTIVE_OCCURRENCE_STATUS,
        1 if has_preview else 0,
        ingested_at,
        last_seen_at,
        updated_at,
    )
    if existing_occurrence_id is None:
        connection.execute(
            """
            INSERT INTO document_occurrences (
              document_id, dataset_source_id, parent_occurrence_id, occurrence_control_number, source_kind, source_rel_path, source_item_id,
              source_folder_path, production_id, begin_bates, end_bates, begin_attachment, end_attachment,
              rel_path, file_name, file_type, mime_type, file_size, file_hash, custodian, fs_created_at, fs_modified_at,
              extracted_author, extracted_title, extracted_subject, extracted_participants, extracted_recipients,
              extracted_doc_authored_at, extracted_doc_modified_at, extracted_content_type, extracted_kind, entity_hints_json, text_status,
              lifecycle_status, has_preview, ingested_at, last_seen_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            occurrence_values,
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    connection.execute(
        """
        UPDATE document_occurrences
        SET document_id = ?, dataset_source_id = ?, parent_occurrence_id = ?, occurrence_control_number = ?, source_kind = ?, source_rel_path = ?,
            source_item_id = ?, source_folder_path = ?, production_id = ?, begin_bates = ?, end_bates = ?,
            begin_attachment = ?, end_attachment = ?, rel_path = ?, file_name = ?, file_type = ?, mime_type = ?,
            file_size = ?, file_hash = ?, custodian = ?, fs_created_at = ?, fs_modified_at = ?, extracted_author = ?,
            extracted_title = ?, extracted_subject = ?, extracted_participants = ?, extracted_recipients = ?,
            extracted_doc_authored_at = ?, extracted_doc_modified_at = ?, extracted_content_type = ?, extracted_kind = ?,
            entity_hints_json = ?, text_status = ?, lifecycle_status = ?, has_preview = ?, ingested_at = ?, last_seen_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (*occurrence_values, existing_occurrence_id),
    )
    return existing_occurrence_id


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
    if sanitized in INTERNAL_DOCUMENT_COLUMNS:
        raise RetrieverError(f"Field name '{sanitized}' conflicts with a system-managed document column.")
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
    def normalized_decoded_text(value: str) -> str:
        return value.lstrip("\ufeff")

    if declared_encoding:
        try:
            return normalized_decoded_text(data.decode(declared_encoding)), "ok", declared_encoding
        except Exception:
            pass

    try:
        return normalized_decoded_text(data.decode("utf-8-sig")), "ok", "utf-8"
    except UnicodeDecodeError:
        pass

    charset_normalizer_module = load_dependency("charset_normalizer")
    if charset_normalizer_module is not None:
        best = charset_normalizer_module.from_bytes(data).best()
        if best is not None:
            text = normalized_decoded_text(str(best))
            status = "partial" if "\ufffd" in text else "ok"
            return text, status, best.encoding

    text = normalized_decoded_text(data.decode("utf-8", errors="replace"))
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


def sorted_unique_display_names(raw_values: list[object]) -> list[str]:
    unique_names: dict[str, str] = {}
    for raw_value in raw_values:
        normalized = normalize_participant_token(raw_value)
        if not normalized:
            continue
        unique_names.setdefault(normalized.casefold(), normalized)
    return sorted(unique_names.values(), key=str.casefold)


def render_display_name_list(
    raw_values: list[object],
    *,
    max_names: int | None = None,
) -> str | None:
    names = sorted_unique_display_names(raw_values)
    if not names:
        return None
    if max_names is not None and max_names > 0 and len(names) > max_names:
        remaining = len(names) - max_names
        return ", ".join(names[:max_names]) + f" +{remaining} more"
    return ", ".join(names)


def render_display_name_title(
    raw_values: list[object],
    *,
    max_names: int | None = None,
) -> str | None:
    names = sorted_unique_display_names(raw_values)
    if not names:
        return None
    if max_names is not None and max_names > 0 and len(names) > max_names:
        remaining = len(names) - max_names
        return " / ".join(names[:max_names]) + f" +{remaining} more"
    return " / ".join(names)


def email_headers_to_metadata(headers: dict[str, str]) -> dict[str, str | None]:
    recipients = ", ".join(headers[key] for key in ("to", "cc", "bcc") if headers.get(key)) or None
    subject = normalize_generated_document_title(headers.get("subject"))
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

def attachment_list_looks_like_filenames(raw_value: str) -> bool:
    candidates = [
        normalize_whitespace(part).strip(" \t\r\n'\"()[]{}")
        for part in re.split(r"\s*[;,]\s*", raw_value)
        if normalize_whitespace(part)
    ]
    if not candidates:
        candidate = normalize_whitespace(raw_value).strip(" \t\r\n'\"()[]{}")
        if not candidate:
            return False
        candidates = [candidate]
    filename_like = 0
    for candidate in candidates:
        leaf = candidate.replace("\\", "/").rsplit("/", 1)[-1]
        if re.search(r"\.[A-Za-z0-9]{1,8}$", leaf):
            filename_like += 1
    return filename_like > 0


def normalize_generated_document_title(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or "")) or None
    if not normalized:
        return None
    match = ATTACHMENT_SUFFIX_PATTERN.match(normalized)
    if match is None or not attachment_list_looks_like_filenames(match.group("attachments")):
        return normalized
    trimmed_title = normalize_whitespace(match.group("title").rstrip(" -:;,"))
    return trimmed_title or normalized


EMAIL_MESSAGE_ID_PATTERN = re.compile(r"<\s*([^<>]+?)\s*>|([^\s<>;,]+@[^\s<>;,]+)")
EMAIL_THREAD_PREFIX_PATTERN = re.compile(r"^(?:(?:re|fw|fwd)\s*(?:\[\d+\])?\s*:\s*)+", flags=re.IGNORECASE)


def extract_email_message_ids(value: object) -> list[str]:
    normalized = normalize_whitespace(str(value or ""))
    if not normalized:
        return []
    message_ids: list[str] = []
    seen: set[str] = set()
    for match in EMAIL_MESSAGE_ID_PATTERN.finditer(normalized):
        raw = normalize_whitespace(match.group(1) or match.group(2) or "")
        if not raw:
            continue
        normalized_id = raw.strip("<>").strip().lower()
        if not normalized_id or normalized_id in seen:
            continue
        seen.add(normalized_id)
        message_ids.append(normalized_id)
    return message_ids


def normalize_email_message_id(value: object) -> str | None:
    message_ids = extract_email_message_ids(value)
    return message_ids[0] if message_ids else None


def normalize_email_thread_subject(value: object, *, preserve_case: bool = False) -> str | None:
    normalized = normalize_whitespace(str(value or ""))
    if not normalized:
        return None
    previous = None
    current = normalized
    while previous != current:
        previous = current
        current = EMAIL_THREAD_PREFIX_PATTERN.sub("", current).strip()
    current = normalize_whitespace(current)
    if not current:
        return None
    return current if preserve_case else current.lower()


def normalize_email_conversation_index_root(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or ""))
    if not normalized:
        return None
    compact = re.sub(r"\s+", "", normalized)
    if not compact:
        return None
    return compact[:44] if len(compact) > 44 else compact


def email_participant_keys(author: object, recipients: object) -> set[str]:
    participants: list[str] = []
    seen: set[str] = set()
    append_unique_participants(
        participants,
        seen,
        [
            normalize_whitespace(str(author or "")) or None,
            normalize_whitespace(str(recipients or "")) or None,
        ],
    )
    return {participant.lower() for participant in participants}


def email_heuristic_scope_key(source_kind: object, source_rel_path: object) -> str:
    normalized_source_kind = normalize_whitespace(str(source_kind or "")).lower()
    normalized_source_rel_path = normalize_whitespace(str(source_rel_path or ""))
    if normalized_source_kind == MBOX_SOURCE_KIND and normalized_source_rel_path:
        return f"{MBOX_SOURCE_KIND}:{normalized_source_rel_path}"
    if normalized_source_kind == PST_SOURCE_KIND and normalized_source_rel_path:
        return f"{PST_SOURCE_KIND}:{normalized_source_rel_path}"
    if normalized_source_kind == FILESYSTEM_SOURCE_KIND:
        return f"{FILESYSTEM_SOURCE_KIND}:{filesystem_dataset_locator()}"
    return f"{normalized_source_kind or FILESYSTEM_SOURCE_KIND}:{normalized_source_rel_path or filesystem_dataset_locator()}"


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
        normalized_headers = {
            key: normalize_whitespace(value)
            for key, value in headers.items()
            if normalize_whitespace(value)
        }
        if "from" in normalized_headers and any(
            key in normalized_headers
            for key in ("to", "cc", "bcc", "subject", "sent", "date")
        ):
            blocks.append(dict(normalized_headers))
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


ICALENDAR_FILE_TYPES = {"ics", "ifb", "vcal", "vcs"}
ICALENDAR_URL_PATTERN = re.compile(r"https?://[^\s<>'\"]+")
CALENDAR_INVITE_TEXT_BLOCK_START = "[[RETRIEVER_CALENDAR_INVITE]]"
CALENDAR_INVITE_TEXT_BLOCK_END = "[[/RETRIEVER_CALENDAR_INVITE]]"
CALENDAR_INVITE_TEXT_BLOCK_PATTERN = re.compile(
    re.escape(CALENDAR_INVITE_TEXT_BLOCK_START)
    + r"\n(.*?)"
    + re.escape(CALENDAR_INVITE_TEXT_BLOCK_END)
    + r"\s*",
    re.DOTALL,
)


def unfold_icalendar_lines(text: str) -> list[str]:
    unfolded: list[str] = []
    for raw_line in str(text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if raw_line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += raw_line[1:]
        else:
            unfolded.append(raw_line)
    return unfolded


def split_icalendar_property_line(line: str) -> tuple[str, str] | None:
    in_quotes = False
    escaped = False
    for index, char in enumerate(line):
        if char == '"' and not escaped:
            in_quotes = not in_quotes
        elif char == ":" and not in_quotes:
            return line[:index], line[index + 1 :]
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
    return None


def split_icalendar_parameter_values(raw_value: str) -> list[str]:
    values: list[str] = []
    current: list[str] = []
    in_quotes = False
    escaped = False
    for char in raw_value:
        if char == '"' and not escaped:
            in_quotes = not in_quotes
            current.append(char)
        elif char == "," and not in_quotes:
            values.append("".join(current))
            current = []
        else:
            current.append(char)
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
    values.append("".join(current))
    return values


def unescape_icalendar_text(value: object) -> str | None:
    normalized = str(value or "")
    if not normalized:
        return None
    return (
        normalized
        .replace("\\N", "\n")
        .replace("\\n", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
    ) or None


def parse_icalendar_parameters(raw_segments: list[str]) -> dict[str, list[str]]:
    parameters: dict[str, list[str]] = {}
    for raw_segment in raw_segments:
        if "=" not in raw_segment:
            continue
        raw_key, raw_value = raw_segment.split("=", 1)
        key = normalize_whitespace(raw_key).upper()
        if not key:
            continue
        values = [
            normalize_whitespace(str(unescape_icalendar_text(part.strip().strip('"')) or ""))
            for part in split_icalendar_parameter_values(raw_value)
        ]
        values = [value for value in values if value]
        if values:
            parameters[key] = values
    return parameters


def parse_icalendar_property_line(line: str) -> dict[str, object] | None:
    split_line = split_icalendar_property_line(line)
    if split_line is None:
        return None
    raw_head, raw_value = split_line
    head_parts = raw_head.split(";")
    name = normalize_whitespace(head_parts[0]).upper()
    if not name:
        return None
    return {
        "name": name,
        "params": parse_icalendar_parameters(head_parts[1:]),
        "value": raw_value,
    }


def parse_icalendar_datetime_value(
    value: object,
    *,
    tzid: object = None,
    value_type: object = None,
) -> dict[str, object]:
    raw = normalize_whitespace(str(value or ""))
    if not raw:
        return {}
    normalized_value_type = normalize_whitespace(str(value_type or "")).upper() or None
    normalized_tzid = normalize_whitespace(str(tzid or "")) or None
    if normalized_value_type == "DATE" or re.fullmatch(r"\d{8}", raw):
        try:
            parsed_date = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        except ValueError:
            return {"raw": raw}
        return {
            "raw": raw,
            "date": parsed_date,
            "all_day": True,
            "iso": parsed_date.isoformat(),
            "tz_label": normalized_tzid,
        }

    candidate = raw[:-1] if raw.endswith("Z") else raw
    parsed_dt = None
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            parsed_dt = datetime.strptime(candidate, fmt)
            break
        except ValueError:
            continue
    if parsed_dt is None:
        return {"raw": raw}

    tz_label = normalized_tzid
    if raw.endswith("Z"):
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
        tz_label = parsed_dt.tzname() or "UTC"
    elif normalized_tzid:
        try:
            parsed_dt = parsed_dt.replace(tzinfo=ZoneInfo(normalized_tzid))
            tz_label = parsed_dt.tzname() or normalized_tzid
        except Exception:
            tz_label = normalized_tzid

    return {
        "raw": raw,
        "datetime": parsed_dt,
        "all_day": False,
        "iso": (
            normalize_datetime(parsed_dt)
            if isinstance(parsed_dt, datetime) and parsed_dt.tzinfo is not None
            else parsed_dt.replace(microsecond=0).isoformat()
        ),
        "tz_label": tz_label,
        "tzid": normalized_tzid,
    }


def format_calendar_preview_date(value: date) -> str:
    return value.strftime("%b %d, %Y").replace(" 0", " ")


def format_calendar_preview_datetime(value: datetime) -> str:
    return value.strftime("%b %d, %Y %I:%M %p").replace(" 0", " ")


def format_calendar_preview_time(value: datetime) -> str:
    return value.strftime("%I:%M %p").lstrip("0")


def format_icalendar_event_range(
    start_info: dict[str, object] | None,
    end_info: dict[str, object] | None = None,
) -> str | None:
    if not start_info:
        return None
    if start_info.get("all_day"):
        start_date = start_info.get("date")
        if not isinstance(start_date, date):
            return normalize_whitespace(str(start_info.get("raw") or "")) or None
        end_date = end_info.get("date") if isinstance(end_info, dict) else None
        if isinstance(end_date, date) and end_date > start_date:
            inclusive_end = end_date - timedelta(days=1)
            if inclusive_end != start_date:
                return (
                    f"{format_calendar_preview_date(start_date)} - "
                    f"{format_calendar_preview_date(inclusive_end)} (all day)"
                )
        return f"{format_calendar_preview_date(start_date)} (all day)"

    start_dt = start_info.get("datetime")
    if not isinstance(start_dt, datetime):
        return normalize_whitespace(str(start_info.get("raw") or "")) or None

    end_dt = end_info.get("datetime") if isinstance(end_info, dict) else None
    if isinstance(end_dt, datetime) and start_dt.tzinfo is not None and end_dt.tzinfo is not None:
        end_dt = end_dt.astimezone(start_dt.tzinfo)
    label = format_calendar_preview_datetime(start_dt)
    if isinstance(end_dt, datetime):
        if start_dt.date() == end_dt.date():
            label = f"{label} - {format_calendar_preview_time(end_dt)}"
        else:
            label = f"{label} - {format_calendar_preview_datetime(end_dt)}"
    tz_label = normalize_whitespace(str(start_info.get("tz_label") or "")) or (
        start_dt.tzname() if start_dt.tzinfo is not None else None
    )
    if tz_label:
        label = f"{label} {tz_label}"
    return label


def humanize_icalendar_enum(value: object) -> str | None:
    normalized = normalize_whitespace(str(value or "")).upper()
    if not normalized:
        return None
    mapping = {
        "ACCEPTED": "Accepted",
        "CANCEL": "Canceled",
        "CANCELLED": "Canceled",
        "CANCELED": "Canceled",
        "CONFIRMED": "Confirmed",
        "COUNTER": "Counter",
        "DECLINED": "Declined",
        "DECLINECOUNTER": "Declined Counter",
        "NEEDS-ACTION": "Needs Action",
        "PUBLISH": "Published",
        "REQUEST": "Request",
        "TENTATIVE": "Tentative",
    }
    return mapping.get(normalized) or normalized.replace("-", " ").replace("_", " ").title()


def format_icalendar_participant(value: object, params: dict[str, list[str]] | None = None) -> str | None:
    normalized_value = normalize_participant_token(unescape_icalendar_text(value))
    if normalized_value and normalized_value.lower().startswith("mailto:"):
        normalized_value = normalize_participant_token(normalized_value[7:])
    if normalized_value and "@" in normalized_value:
        normalized_value = normalized_value.lower()
    cn_values = params.get("CN") if isinstance(params, dict) else None
    common_name = normalize_participant_token(unescape_icalendar_text(cn_values[0])) if cn_values else None
    if common_name and normalized_value and common_name.lower() != normalized_value.lower():
        return f"{common_name} <{normalized_value}>"
    return common_name or normalized_value


def first_icalendar_url(text: object) -> str | None:
    normalized = str(text or "")
    if not normalized:
        return None
    candidates = [
        match.group(0).rstrip(").,;")
        for match in ICALENDAR_URL_PATTERN.finditer(normalized)
    ]
    if not candidates:
        return None
    preferred_domains = ("meet.google.com", "zoom.us", "teams.microsoft.com", "webex.com")
    for candidate in candidates:
        if any(domain in candidate for domain in preferred_domains):
            return candidate
    return candidates[0]


def summarize_icalendar_invite_status(metadata: dict[str, object] | None) -> str | None:
    if not isinstance(metadata, dict):
        return None
    parts = [
        humanize_icalendar_enum(metadata.get("method")),
        humanize_icalendar_enum(metadata.get("status")),
    ]
    parts = [part for part in parts if part]
    return " · ".join(parts) if parts else None


def parse_icalendar_event_metadata(text: object) -> dict[str, object] | None:
    normalized_text = str(text or "")
    if not normalized_text:
        return None
    upper_text = normalized_text.upper()
    if "BEGIN:VCALENDAR" not in upper_text and "BEGIN:VEVENT" not in upper_text:
        return None

    calendar_properties: dict[str, list[dict[str, object]]] = defaultdict(list)
    event_properties: dict[str, list[dict[str, object]]] | None = None
    for raw_line in unfold_icalendar_lines(normalized_text):
        normalized_line = normalize_whitespace(raw_line)
        if not normalized_line:
            continue
        upper_line = normalized_line.upper()
        if upper_line == "BEGIN:VEVENT":
            if event_properties is None:
                event_properties = defaultdict(list)
            continue
        if upper_line == "END:VEVENT":
            break
        parsed = parse_icalendar_property_line(raw_line)
        if parsed is None:
            continue
        target = event_properties if event_properties is not None else calendar_properties
        target[str(parsed["name"])].append(parsed)
    if not event_properties:
        return None

    def _first_property(
        properties: dict[str, list[dict[str, object]]],
        name: str,
    ) -> dict[str, object] | None:
        values = properties.get(name) or []
        return values[0] if values else None

    def _first_text(
        properties: dict[str, list[dict[str, object]]],
        name: str,
    ) -> str | None:
        prop = _first_property(properties, name)
        if prop is None:
            return None
        return normalize_whitespace(str(unescape_icalendar_text(prop.get("value")) or "")) or None

    summary = normalize_generated_document_title(_first_text(event_properties, "SUMMARY"))
    description = _first_text(event_properties, "DESCRIPTION")
    organizer_prop = _first_property(event_properties, "ORGANIZER")
    organizer = (
        format_icalendar_participant(organizer_prop.get("value"), organizer_prop.get("params"))
        if organizer_prop is not None
        else None
    )
    attendees: list[str] = []
    seen_attendees: set[str] = set()
    for attendee_prop in event_properties.get("ATTENDEE") or []:
        formatted = format_icalendar_participant(attendee_prop.get("value"), attendee_prop.get("params"))
        if not formatted:
            continue
        key = formatted.lower()
        if key in seen_attendees:
            continue
        seen_attendees.add(key)
        attendees.append(formatted)
    start_prop = _first_property(event_properties, "DTSTART")
    end_prop = _first_property(event_properties, "DTEND")
    start_info = (
        parse_icalendar_datetime_value(
            start_prop.get("value"),
            tzid=(start_prop.get("params") or {}).get("TZID", [None])[0],
            value_type=(start_prop.get("params") or {}).get("VALUE", [None])[0],
        )
        if start_prop is not None
        else {}
    )
    end_info = (
        parse_icalendar_datetime_value(
            end_prop.get("value"),
            tzid=(end_prop.get("params") or {}).get("TZID", [None])[0],
            value_type=(end_prop.get("params") or {}).get("VALUE", [None])[0],
        )
        if end_prop is not None
        else {}
    )
    conference_url = (
        _first_text(event_properties, "X-GOOGLE-CONFERENCE")
        or _first_text(event_properties, "URL")
        or first_icalendar_url(description)
    )
    return {
        "summary": summary,
        "description": description,
        "organizer": organizer,
        "attendees": attendees,
        "attendees_display": ", ".join(attendees) or None,
        "location": _first_text(event_properties, "LOCATION"),
        "conference_url": conference_url,
        "start": start_info,
        "end": end_info,
        "start_iso": start_info.get("iso"),
        "end_iso": end_info.get("iso"),
        "when": format_icalendar_event_range(start_info, end_info),
        "method": _first_text(calendar_properties, "METHOD"),
        "status": _first_text(event_properties, "STATUS"),
        "uid": _first_text(event_properties, "UID"),
        "sequence": _first_text(event_properties, "SEQUENCE"),
    }


def build_calendar_invite_summary(
    metadata: dict[str, object] | None,
    *,
    file_name: object = None,
    href: object = None,
    detail: object = None,
) -> dict[str, str] | None:
    if not isinstance(metadata, dict):
        return None
    title = (
        normalize_generated_document_title(metadata.get("summary"))
        or normalize_generated_document_title(file_name)
        or "Calendar invite"
    )
    summary = {
        "kind": "calendar_invite",
        "label": normalize_whitespace(str(file_name or title or "Calendar invite")) or "Calendar invite",
        "title": title,
        "when": normalize_whitespace(str(metadata.get("when") or "")) or "",
        "organizer": normalize_whitespace(str(metadata.get("organizer") or "")) or "",
        "attendees": normalize_whitespace(str(metadata.get("attendees_display") or "")) or "",
        "location": normalize_whitespace(str(metadata.get("location") or "")) or "",
        "join_href": normalize_whitespace(str(metadata.get("conference_url") or "")) or "",
        "status": normalize_whitespace(str(summarize_icalendar_invite_status(metadata) or "")) or "",
        "uid": normalize_whitespace(str(metadata.get("uid") or "")) or "",
        "sequence": normalize_whitespace(str(metadata.get("sequence") or "")) or "",
        "href": normalize_whitespace(str(href or "")) or "",
        "detail": normalize_whitespace(str(detail or "")) or "",
        "file_name": normalize_whitespace(str(file_name or "")) or "",
    }
    if not any(summary.get(key) for key in ("title", "when", "organizer", "attendees", "location", "join_href", "status")):
        return None
    return summary


def extract_calendar_invite_summary_from_attachment(attachment: dict[str, object]) -> dict[str, str] | None:
    payload = attachment.get("payload")
    if not isinstance(payload, (bytes, bytearray)):
        return None
    file_name = normalize_whitespace(str(attachment.get("file_name") or "")) or None
    content_type = normalize_mime_type(attachment.get("content_type"))
    file_type = infer_attachment_file_type(
        file_name=file_name,
        payload=bytes(payload),
        content_type=content_type,
    )
    if content_type != "text/calendar" and file_type not in ICALENDAR_FILE_TYPES:
        return None
    decoded, _, _ = decode_bytes(bytes(payload))
    return build_calendar_invite_summary(
        parse_icalendar_event_metadata(decoded),
        file_name=file_name,
    )


def partition_calendar_invite_attachments(
    attachments: list[dict[str, object]] | None,
) -> tuple[list[dict[str, str]], list[dict[str, object]]]:
    invite_summaries: list[dict[str, str]] = []
    retained_attachments: list[dict[str, object]] = []
    for attachment in list(attachments or []):
        invite_summary = extract_calendar_invite_summary_from_attachment(attachment)
        if invite_summary is None:
            retained_attachments.append(attachment)
            continue
        invite_summaries.append(invite_summary)
    return invite_summaries, retained_attachments


def build_calendar_invite_search_text(invites: list[dict[str, str]] | None) -> str:
    if not invites:
        return ""
    blocks: list[str] = []
    for invite in invites:
        lines = [CALENDAR_INVITE_TEXT_BLOCK_START]
        for label, key in (
            ("Title", "title"),
            ("When", "when"),
            ("Organizer", "organizer"),
            ("Attendees", "attendees"),
            ("Location", "location"),
            ("Join", "join_href"),
            ("Status", "status"),
            ("UID", "uid"),
            ("Sequence", "sequence"),
            ("Attachment", "file_name"),
        ):
            value = normalize_whitespace(str(invite.get(key) or "")) or None
            if value:
                lines.append(f"{label}: {value}")
        lines.append(CALENDAR_INVITE_TEXT_BLOCK_END)
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def extract_calendar_invites_from_text_content(text: str) -> tuple[str, list[dict[str, str]]]:
    invites: list[dict[str, str]] = []
    normalized_text = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    label_map = {
        "title": "title",
        "when": "when",
        "organizer": "organizer",
        "attendees": "attendees",
        "location": "location",
        "join": "join_href",
        "status": "status",
        "uid": "uid",
        "sequence": "sequence",
        "attachment": "file_name",
    }

    def _replace(match: re.Match[str]) -> str:
        invite: dict[str, str] = {"kind": "calendar_invite"}
        for raw_line in match.group(1).splitlines():
            if ":" not in raw_line:
                continue
            raw_label, raw_value = raw_line.split(":", 1)
            key = label_map.get(normalize_whitespace(raw_label).lower())
            value = normalize_whitespace(raw_value)
            if key and value:
                invite[key] = value
        if invite.get("title") or invite.get("when") or invite.get("join_href"):
            invite.setdefault("label", invite.get("file_name") or invite.get("title") or "Calendar invite")
            invites.append(invite)
        return ""

    cleaned = CALENDAR_INVITE_TEXT_BLOCK_PATTERN.sub(_replace, normalized_text)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip("\n")
    return cleaned, invites


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
    timestamped_matches = 0
    for entry in iter_chat_transcript_entries(text):
        candidate = str(entry["speaker"])
        key = candidate.lower().strip("[]()")
        speaker_counts[key] = speaker_counts.get(key, 0) + 1
        if isinstance(entry.get("timestamp"), str):
            timestamped_matches += 1
        if key not in seen:
            seen.add(key)
            participants.append(candidate)

    total_matches = sum(speaker_counts.values())
    if total_matches < 2:
        return None
    if timestamped_matches < 2:
        repeated_speaker = any(count >= 2 for count in speaker_counts.values())
        if total_matches < 3 or not repeated_speaker:
            return None
    elif len(participants) < 2 and total_matches < 3:
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


LAZY_DEPENDENCY_IMPORT_TARGETS = {
    "charset_normalizer": ("charset_normalizer", None),
    "extract_msg": ("extract_msg", None),
    "openpyxl": ("openpyxl", None),
    "xlrd": ("xlrd", None),
    "pdfplumber": ("pdfplumber", None),
    "DocxDocument": ("docx", "Document"),
    "rtf_to_text": ("striprtf.striprtf", "rtf_to_text"),
    "PilImage": ("PIL.Image", None),
    "pypff": ("pypff", None),
}


def import_dependency_target(module_name: str, attribute_name: str | None) -> object:
    imported = importlib.import_module(module_name)
    return imported if attribute_name is None else getattr(imported, attribute_name)


def load_dependency(dependency_name: str, *, allow_auto_install: bool = True) -> object | None:
    current = globals().get(dependency_name, _UNLOADED_DEPENDENCY)
    if current is not _UNLOADED_DEPENDENCY and current is not None:
        return current
    import_target = LAZY_DEPENDENCY_IMPORT_TARGETS.get(dependency_name)
    if import_target is None:
        raise RetrieverError(f"Unknown dependency loader: {dependency_name}")
    module_name, attribute_name = import_target
    try:
        value = import_dependency_target(module_name, attribute_name)
    except Exception:
        value = None
    runtime_paths = plugin_runtime_paths(root=ACTIVE_WORKSPACE_ROOT)
    if value is None and runtime_paths is not None:
        try:
            if activate_plugin_site_packages(runtime_paths):
                value = import_dependency_target(module_name, attribute_name)
        except Exception:
            value = None
    if value is None and allow_auto_install and runtime_paths is not None:
        try:
            ensure_plugin_runtime(
                runtime_paths,
                install_requirements=True,
                force_requirements_install=True,
                reason=f"dependency:{dependency_name}",
            )
            activate_plugin_site_packages(runtime_paths)
            value = import_dependency_target(module_name, attribute_name)
        except Exception:
            value = None
    globals()[dependency_name] = value
    return value


def dependency_status(
    dependency_name: str,
    *,
    package_name: str,
    import_name: str | None = None,
    detail_label: str | None = None,
    probe_if_unloaded: bool = False,
    allow_auto_install: bool = False,
) -> dict[str, str]:
    current = globals().get(dependency_name, _UNLOADED_DEPENDENCY)
    if current is _UNLOADED_DEPENDENCY and probe_if_unloaded:
        current = load_dependency(dependency_name, allow_auto_install=allow_auto_install)
    detail_name = detail_label or import_name or dependency_name
    import_label = import_name or dependency_name
    if current is _UNLOADED_DEPENDENCY:
        return {
            "status": "deferred",
            "detail": f"{detail_name} will load on demand when a matching file type is used.",
        }
    if current is None:
        return {
            "status": "fail",
            "detail": f"Missing optional dependency import '{import_label}'. Install {package_name} before using the matching file type.",
        }
    return {"status": "pass", "detail": f"{detail_name} import succeeded"}


def dependency_guard(module: object | str | None, package_name: str, file_type: str) -> object:
    if isinstance(module, str):
        module = load_dependency(module, allow_auto_install=True)
    if module is None:
        raise RetrieverError(
            f"Missing dependency for .{file_type} parsing: install {package_name} before ingesting this file type."
        )
    if module is _UNLOADED_DEPENDENCY:
        raise RetrieverError(
            f"Dependency for .{file_type} parsing was not initialized correctly: {package_name}."
        )
    return module


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


def decode_attachment_text_sample(payload: bytes, *, max_bytes: int = 65536) -> str | None:
    if not isinstance(payload, (bytes, bytearray)):
        return None
    sample = bytes(payload[:max_bytes])
    if not sample or b"\x00" in sample:
        return None
    decoded, _, _ = decode_bytes(sample)
    if not decoded:
        return None
    replacement_count = decoded.count("\ufffd")
    if replacement_count > max(4, len(decoded) // 50):
        return None
    control_count = sum(
        1
        for character in decoded
        if ord(character) < 32 and character not in "\r\n\t\f\b"
    )
    if control_count > max(4, len(decoded) // 50):
        return None
    return decoded


def sniff_attachment_file_type(payload: bytes) -> str | None:
    if not isinstance(payload, (bytes, bytearray)) or not payload:
        return None
    data = bytes(payload)
    sample = data[:65536]
    trimmed_sample = sample.lstrip(b"\xef\xbb\xbf\r\n\t ")
    if trimmed_sample.startswith(b"%PDF-"):
        return "pdf"
    image_mime_type = sniff_image_mime_type(data)
    if image_mime_type:
        return attachment_file_type_from_mime_type(image_mime_type)
    if trimmed_sample.startswith(b"{\\rtf"):
        return "rtf"
    if zipfile.is_zipfile(io.BytesIO(data)):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                member_names = set(archive.namelist())
        except Exception:
            member_names = set()
        if "[Content_Types].xml" in member_names:
            if any(name.startswith("word/") for name in member_names):
                return "docx"
            if any(name.startswith("xl/") for name in member_names):
                return "xlsx"
            if any(name.startswith("ppt/") for name in member_names):
                return "pptx"
        return "zip"
    if sample.startswith(OLE_COMPOUND_FILE_MAGIC):
        if b"Workbook" in sample or b"Book" in sample:
            return "xls"
        if b"WordDocument" in sample:
            return "doc"
        if b"PowerPoint Document" in sample:
            return "ppt"
        return "ole"
    decoded_text = decode_attachment_text_sample(data)
    if not decoded_text:
        return None
    stripped_text = decoded_text.lstrip("\ufeff\r\n\t ")
    if not stripped_text:
        return None
    preview = stripped_text[:1024]
    preview_lower = preview.lower()
    if stripped_text.upper().startswith("BEGIN:VCALENDAR"):
        return "ics"
    if (
        preview_lower.startswith("<!doctype html")
        or preview_lower.startswith("<html")
        or preview_lower.startswith("<body")
        or "<html" in preview_lower
    ):
        return "html"
    if stripped_text.startswith("{") or stripped_text.startswith("["):
        try:
            parsed = json.loads(stripped_text)
        except Exception:
            parsed = None
        if isinstance(parsed, (dict, list)):
            return "json"
    if stripped_text.startswith("<?xml") or stripped_text.startswith("<"):
        try:
            ET.fromstring(stripped_text)
        except Exception:
            pass
        else:
            return "xml"
    return "txt"


def infer_attachment_file_type(
    *,
    file_name: str | None = None,
    payload: bytes | None = None,
    content_type: object = None,
    preferred_extension: object = None,
) -> str | None:
    normalized_extension = normalize_file_type_name(preferred_extension)
    if normalized_extension:
        return normalized_extension
    sniffed = sniff_attachment_file_type(payload) if isinstance(payload, (bytes, bytearray)) else None
    if sniffed:
        return sniffed
    declared = attachment_file_type_from_mime_type(content_type)
    if declared:
        return declared
    if file_name:
        return normalize_file_type_name(Path(file_name).suffix.lower().lstrip("."))
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


def attachment_image_mime_type(attachment: object) -> str | None:
    if not isinstance(attachment, dict):
        return None
    payload = attachment.get("payload")
    payload_bytes = bytes(payload) if isinstance(payload, (bytes, bytearray)) else None
    file_name = str(attachment.get("file_name") or "")
    mime_type = normalize_mime_type(attachment.get("content_type"))
    if mime_type is not None and mime_type.startswith("image/"):
        return mime_type
    ooxml_mime_type = ooxml_image_mime_type(file_name)
    if ooxml_mime_type:
        return ooxml_mime_type
    if payload_bytes is not None:
        sniffed = sniff_image_mime_type(payload_bytes)
        if sniffed:
            return sniffed
    guessed, _ = mimetypes.guess_type(file_name)
    if guessed and guessed.startswith("image/"):
        return guessed
    return None


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
        mime_type = attachment_image_mime_type(attachment)
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


def referenced_cids_in_html(html_body: str | None) -> set[str]:
    if not html_body:
        return set()
    referenced: set[str] = set()
    for match in CID_REFERENCE_PATTERN.finditer(html_body):
        cid = normalize_content_id(match.group(3))
        if cid:
            referenced.add(cid.lower())
    return referenced


def filter_html_preview_embedded_image_attachments(
    html_body: str | None,
    attachments: list[dict[str, object]] | None,
) -> list[dict[str, object]]:
    if not attachments:
        return []
    referenced_cids = referenced_cids_in_html(html_body)
    if not referenced_cids:
        return list(attachments)
    filtered: list[dict[str, object]] = []
    for attachment in attachments:
        content_id = normalize_content_id(attachment.get("content_id"))
        if content_id and content_id.lower() in referenced_cids and attachment_image_mime_type(attachment):
            continue
        filtered.append(attachment)
    return filtered


def render_html_preview_calendar_invite_cards(links: list[dict[str, str]]) -> str:
    if not links:
        return ""
    cards: list[str] = []
    for link in links:
        title = normalize_whitespace(str(link.get("title") or link.get("label") or "Calendar invite")) or "Calendar invite"
        href = normalize_whitespace(str(link.get("href") or "")) or None
        title_html = (
            f'<a href="{html.escape(href)}">{html.escape(title)}</a>'
            if href
            else html.escape(title)
        )
        metadata_items: list[str] = []
        for label, key in (
            ("When", "when"),
            ("Organizer", "organizer"),
            ("Attendees", "attendees"),
            ("Location", "location"),
            ("Status", "status"),
        ):
            value = normalize_whitespace(str(link.get(key) or ""))
            if not value:
                continue
            metadata_items.append(
                f"<div><dt>{html.escape(label)}</dt><dd>{html.escape(value)}</dd></div>"
            )
        join_href = normalize_whitespace(str(link.get("join_href") or "")) or None
        if join_href:
            metadata_items.append(
                "<div><dt>Join</dt>"
                f'<dd><a href="{html.escape(join_href)}">{html.escape(join_href)}</a></dd></div>'
            )
        detail = normalize_whitespace(str(link.get("detail") or ""))
        detail_html = (
            f'<p class="retriever-calendar-invite-detail">{html.escape(detail)}</p>'
            if detail
            else ""
        )
        cards.append(
            '<article class="retriever-calendar-invite">'
            '<div class="retriever-calendar-invite-header">'
            "<div>"
            '<p class="retriever-calendar-invite-kicker">Calendar invite</p>'
            f'<h3 class="retriever-calendar-invite-title">{title_html}</h3>'
            "</div>"
            f"{detail_html}"
            "</div>"
            + (
                f'<dl class="retriever-calendar-invite-meta">{"".join(metadata_items)}</dl>'
                if metadata_items
                else ""
            )
            + "</article>"
        )
    return (
        "<!-- RETRIEVER_CALENDAR_INVITES_START -->"
        + '<section class="retriever-calendar-invites">'
        + "".join(cards)
        + "</section>"
        + "<!-- RETRIEVER_CALENDAR_INVITES_END -->"
    )


def render_html_preview_attachment_links(links: list[dict[str, str]]) -> str:
    if not links:
        return ""
    calendar_links = [
        link
        for link in links
        if normalize_whitespace(str(link.get("kind") or "")).lower() == "calendar_invite"
    ]
    file_links = [
        link
        for link in links
        if normalize_whitespace(str(link.get("kind") or "")).lower() != "calendar_invite"
    ]
    sections: list[str] = []
    calendar_section = render_html_preview_calendar_invite_cards(calendar_links)
    if calendar_section:
        sections.append(calendar_section)
    items: list[str] = []
    for link in file_links:
        href = html.escape(str(link.get("href") or ""))
        label = html.escape(str(link.get("label") or "Attachment"))
        detail = normalize_whitespace(str(link.get("detail") or ""))
        detail_html = f' <span class="retriever-attachment-meta">({html.escape(detail)})</span>' if detail else ""
        items.append(f'<li><a href="{href}">{label}</a>{detail_html}</li>')
    if items:
        sections.append(
            '<section class="retriever-attachments"><h2>Attachments</h2><ul>'
            + "".join(items)
            + "</ul></section>"
        )
    if not sections:
        return ""
    return (
        "<!-- RETRIEVER_ATTACHMENT_LINKS_START -->"
        + "".join(sections)
        + "<!-- RETRIEVER_ATTACHMENT_LINKS_END -->"
    )


def inject_html_preview_attachment_links(html_text: str, links: list[dict[str, str]]) -> str:
    cleaned = HTML_PREVIEW_ATTACHMENT_LINKS_PATTERN.sub("", html_text)
    section = render_html_preview_attachment_links(links)
    if not section:
        return cleaned
    if "</h1>" in cleaned:
        return cleaned.replace("</h1>", f"</h1>{section}", 1)
    if "<body>" in cleaned:
        return cleaned.replace("<body>", f"<body>{section}", 1)
    return cleaned + section


def build_html_preview(
    headers: dict[str, str],
    body_html: str | None = None,
    body_text: str | None = None,
    *,
    document_title: str,
    head_html: str | None = None,
    heading: str | None = None,
) -> str:
    header_html = "".join(
        f"<tr><th>{html.escape(key)}</th><td>{html.escape(value)}</td></tr>"
        for key, value in headers.items()
        if value
    )
    resolved_heading = document_title if heading is None else heading
    heading_html = f"<h1>{html.escape(resolved_heading)}</h1>" if resolved_heading else ""
    header_section = f"<table>{header_html}</table><hr/>" if header_html else ""
    if body_html:
        body_section = body_html
    else:
        body_section = f"<pre>{html.escape(body_text or '')}</pre>"
    return (
        "<!DOCTYPE html>"
        "<html><head>"
        '<meta charset="utf-8"/>'
        '<meta name="viewport" content="width=device-width, initial-scale=1"/>'
        f"<title>{html.escape(document_title)}</title>"
        f"{head_html or ''}"
        "</head><body>"
        f"{heading_html}"
        f"{header_section}"
        f"{body_section}"
        "</body></html>"
    )


def build_chat_preview_html(
    headers: dict[str, str],
    body_text: str,
    *,
    document_title: str,
    entries: list[dict[str, object]] | None = None,
) -> str:
    chat_entries = entries if entries is not None else iter_chat_transcript_entries(body_text, max_lines=4000)
    head_html = (
        "<style>"
        "html { box-sizing: border-box; }"
        "*, *::before, *::after { box-sizing: inherit; }"
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: clamp(10px, 3vw, 24px); color: #1f2328; overflow-wrap: anywhere; }"
        "h1 { font-size: 1.35rem; line-height: 1.2; margin-bottom: 0.75rem; }"
        "table { border-collapse: collapse; table-layout: fixed; width: 100%; max-width: 100%; margin-bottom: 1rem; }"
        "th { width: min(11rem, 38%); text-align: left; vertical-align: top; padding: 0.25rem 0.75rem 0.25rem 0; color: #57606a; }"
        "td { padding: 0.25rem 0; }"
        "th, td { overflow-wrap: anywhere; }"
        "pre { white-space: pre-wrap; word-break: break-word; overflow-wrap: anywhere; }"
        ".chat-transcript { display: grid; gap: 0.75rem; min-width: 0; max-width: 100%; }"
        ".chat-message { display: flex; gap: 0.75rem; align-items: flex-start; min-width: 0; max-width: 100%; border: 1px solid #d0d7de; border-radius: 12px; padding: 0.85rem 0.95rem; background: #f6f8fa; }"
        ".chat-avatar-svg { width: 2.5rem; height: 2.5rem; flex: 0 0 auto; display: block; }"
        ".chat-main { min-width: 0; flex: 1 1 auto; }"
        ".chat-meta { display: flex; gap: 0.55rem; align-items: baseline; margin-bottom: 0.25rem; flex-wrap: wrap; }"
        ".chat-speaker { font-weight: 600; color: #0969da; }"
        ".chat-time { color: #57606a; font-size: 0.9rem; }"
        ".chat-body { white-space: pre-wrap; line-height: 1.45; word-break: break-word; overflow-wrap: anywhere; }"
        ".chat-raw { margin-top: 1rem; min-width: 0; max-width: 100%; }"
        ".chat-raw summary { cursor: pointer; color: #57606a; }"
        "@media (max-width: 520px) {"
        "body { margin: 10px; }"
        "table, tbody, tr, th, td { display: block; width: 100%; }"
        "th { padding-bottom: 0.05rem; }"
        "td { padding-top: 0.05rem; }"
        ".chat-message { gap: 0.55rem; padding: 0.7rem; }"
        ".chat-avatar-svg { width: 2rem; height: 2rem; }"
        "}"
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
    )


def conversation_preview_anchor(document_id: int) -> str:
    return f"doc-{int(document_id)}"


def conversation_preview_base_path(conversation_id: int) -> Path:
    return Path("previews") / "conversations" / f"conversation-{int(conversation_id):08d}"


def conversation_preview_full_rel_path(conversation_id: int) -> str:
    return (conversation_preview_base_path(conversation_id) / "conversation.html").as_posix()


def conversation_preview_toc_rel_path(conversation_id: int) -> str:
    return (conversation_preview_base_path(conversation_id) / "index.html").as_posix()


def conversation_preview_segment_rel_path(conversation_id: int, segment_token: str) -> str:
    normalized_token = re.sub(r"[^A-Za-z0-9._-]+", "-", normalize_whitespace(segment_token) or "segment").strip("-")
    return (conversation_preview_base_path(conversation_id) / f"segment-{normalized_token or 'segment'}.html").as_posix()


def conversation_preview_entry_rel_path(conversation_id: int, document_id: int) -> str:
    return (conversation_preview_base_path(conversation_id) / f"{conversation_preview_anchor(document_id)}.html").as_posix()


def is_conversation_preview_rel_path(rel_preview_path: object) -> bool:
    normalized = normalize_internal_rel_path(Path(str(rel_preview_path or "")))
    return normalized.startswith("previews/conversations/")


def append_preview_fragment(path: str, target_fragment: object) -> str:
    fragment = normalize_whitespace(str(target_fragment or ""))
    if not fragment:
        return path
    if "#" in path:
        return path
    return f"{path}#{fragment}"


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


def image_path_png_bytes(path: Path, *, max_dimension: int | None = None) -> bytes | None:
    resized_dimension = max(0, int(max_dimension or 0))
    pil_image_module = load_dependency("PilImage")
    if pil_image_module is None:
        return None
    with pil_image_module.open(path) as image:
        if resized_dimension:
            image.thumbnail((resized_dimension, resized_dimension))
        buffer = io.BytesIO()
        try:
            image.save(buffer, format="PNG", optimize=True)
        except (OSError, ValueError):
            image.convert("RGB").save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def image_path_data_url(path: Path, *, max_dimension: int | None = None) -> str | None:
    mime_type, _ = mimetypes.guess_type(path.name)
    normalized_suffix = path.suffix.lower()
    resized_dimension = max(0, int(max_dimension or 0))
    if normalized_suffix in {".tif", ".tiff"} or resized_dimension:
        png_bytes = image_path_png_bytes(path, max_dimension=resized_dimension)
        if png_bytes is None:
            if normalized_suffix not in {".tif", ".tiff"} and mime_type is not None and mime_type.startswith("image/"):
                return f"data:{mime_type};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"
            return None
        return f"data:image/png;base64,{base64.b64encode(png_bytes).decode('ascii')}"
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
        passive_field_label("title"): deck_title,
        passive_field_label("author"): author or "",
        passive_field_label("date_created"): date_created or "",
        passive_field_label("date_modified"): date_modified or "",
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
