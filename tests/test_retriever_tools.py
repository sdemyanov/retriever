from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import io
import mailbox
import os
import random
import re
import sqlite3
import tempfile
import types
import unittest
import zipfile
import base64
from contextlib import redirect_stderr, redirect_stdout
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "skills" / "tool-template" / "retriever_tools.py"
BUNDLER_PATH = REPO_ROOT / "skills" / "tool-template" / "bundle_retriever_tools.py"
TOOL_TEMPLATE_PATH = REPO_ROOT / "skills" / "tool-template" / "tool-template.md"
SOURCE_HEADER_PATH = REPO_ROOT / "skills" / "tool-template" / "src" / "00_header.py"
PLUGIN_MANIFEST_PATH = REPO_ROOT / ".claude-plugin" / "plugin.json"
PING_SKILL_PATH = REPO_ROOT / "skills" / "ping" / "SKILL.md"
REGRESSION_CORPUS_ROOT = REPO_ROOT / "phase0" / "regression_corpus"

retriever_tools = None
TOOL_BYTES = b""
RANDOMIZED_FILTER_TEST_SEED = 20260419
RANDOMIZED_FILTER_FIELD_TYPES = {
    "file_name": "text",
    "review_note": "text",
    "review_score": "integer",
    "review_date": "date",
    "is_hot": "boolean",
}
RANDOMIZED_FILTER_LITERAL_POOLS = {
    "file_name": [
        "alpha.txt",
        "beta.txt",
        "gamma.txt",
        "thread.eml",
        "notes.txt",
        "zeta.txt",
    ],
    "review_note": [
        "alpha memo",
        "beta's note",
        "needs review",
        "mail thread",
        "child's attachment",
        "missing note",
    ],
    "review_score": [-1, 1, 5, 7, 9, 12],
    "review_date": [
        "2026-04-01",
        "2026-04-02",
        "2026-04-03",
        "2026-04-05",
        "2026-04-09",
    ],
    "is_hot": [True, False],
}
RANDOMIZED_FILTER_LIKE_PATTERNS = {
    "file_name": ["alpha%", "%txt", "%notes%", "thread%", "%eml"],
    "review_note": ["%memo%", "%note%", "%review%", "%thread%", "%attachment%"],
    "review_date": ["2026-04-%", "%-03", "2026-04-0_", "%-09"],
}
RANDOMIZED_FILTER_OPERATOR_CHOICES = {
    "text": ["=", "!=", "<", "<=", ">", ">=", "LIKE", "NOT LIKE", "IN", "NOT IN", "BETWEEN", "NOT BETWEEN", "IS NULL", "IS NOT NULL"],
    "date": ["=", "!=", "<", "<=", ">", ">=", "LIKE", "NOT LIKE", "IN", "NOT IN", "BETWEEN", "NOT BETWEEN", "IS NULL", "IS NOT NULL"],
    "integer": ["=", "!=", "<", "<=", ">", ">=", "IN", "NOT IN", "BETWEEN", "NOT BETWEEN", "IS NULL", "IS NOT NULL"],
    "boolean": ["=", "!=", "IS NULL", "IS NOT NULL"],
}

def load_python_module(path: Path, module_name: str):
    module = types.ModuleType(module_name)
    module.__file__ = str(path)
    module.__package__ = ""
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), module.__dict__)
    return module


def assert_bundled_tooling_current() -> None:
    if not TOOL_PATH.exists():
        raise AssertionError(
            f"Missing generated bundle at {TOOL_PATH}. Run ./build.sh to regenerate the bundle and checksum before running tests."
        )

    bundler = load_python_module(BUNDLER_PATH, "retriever_bundler_under_test")
    expected_text = bundler.bundle_source(TOOL_PATH.parent / "src")
    current_text = TOOL_PATH.read_text(encoding="utf-8")
    if current_text != expected_text:
        raise AssertionError(
            "Generated retriever_tools.py is stale relative to skills/tool-template/src. "
            "Run ./build.sh to regenerate the bundle and checksum before running tests."
        )

    checksum_match = re.search(
        r"^- source checksum \(SHA256\): `([0-9a-f]{64})`$",
        TOOL_TEMPLATE_PATH.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if checksum_match is None:
        raise AssertionError(f"Could not find the canonical source checksum line in {TOOL_TEMPLATE_PATH}.")

    expected_sha = hashlib.sha256(expected_text.encode("utf-8")).hexdigest()
    if checksum_match.group(1) != expected_sha:
        raise AssertionError(
            "tool-template.md has a stale source checksum for the generated bundle. "
            "Run ./build.sh to regenerate the bundle and checksum before running tests."
        )


def assert_version_metadata_current() -> None:
    header_match = re.search(
        r'^TOOL_VERSION = "([^"]+)"$',
        SOURCE_HEADER_PATH.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if header_match is None:
        raise AssertionError(f"Could not determine TOOL_VERSION from {SOURCE_HEADER_PATH}.")
    expected_version = header_match.group(1)

    plugin_version = json.loads(PLUGIN_MANIFEST_PATH.read_text(encoding="utf-8")).get("version")
    if plugin_version != expected_version:
        raise AssertionError(
            ".claude-plugin/plugin.json has a stale version relative to TOOL_VERSION. "
            "Run ./build.sh to synchronize version metadata before running tests."
        )

    ping_text = PING_SKILL_PATH.read_text(encoding="utf-8")
    metadata_match = re.search(r'^\s*version:\s*"([^"]+)"\s*$', ping_text, re.MULTILINE)
    body_match = re.search(r"^Version:\s*(\S+)\s*$", ping_text, re.MULTILINE)
    if metadata_match is None or body_match is None:
        raise AssertionError(f"Could not find both ping skill version markers in {PING_SKILL_PATH}.")
    if metadata_match.group(1) != expected_version or body_match.group(1) != expected_version:
        raise AssertionError(
            "skills/ping/SKILL.md has stale version text relative to TOOL_VERSION. "
            "Run ./build.sh to synchronize version metadata before running tests."
        )


def setUpModule() -> None:
    global retriever_tools, TOOL_BYTES
    assert_bundled_tooling_current()
    assert_version_metadata_current()
    retriever_tools = load_python_module(TOOL_PATH, "retriever_tools_under_test")
    TOOL_BYTES = TOOL_PATH.read_bytes()


class RetrieverToolsRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="retriever-tests-")
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.paths = retriever_tools.workspace_paths(self.root)
        retriever_tools.ensure_layout(self.paths)
        self.materialize_workspace_tool()

    def materialize_workspace_tool(self) -> None:
        self.paths["tool_path"].write_bytes(TOOL_BYTES)

    def run_cli(self, *args: str) -> tuple[int, dict[str, object] | None, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            mock.patch.object(retriever_tools.sys, "argv", ["retriever_tools.py", *args]),
        ):
            exit_code = retriever_tools.main()
        stdout_text = stdout.getvalue().strip()
        stderr_text = stderr.getvalue().strip()
        payload = json.loads(stdout_text or stderr_text) if (stdout_text or stderr_text) else None
        return exit_code, payload, stdout_text, stderr_text

    def preview_target_file_path(self, target: dict[str, object]) -> Path:
        file_abs_path = str(target.get("file_abs_path") or target.get("abs_path") or "")
        return Path(file_abs_path.split("#", 1)[0])

    def create_legacy_documents_table(self, *, with_row: bool) -> None:
        connection = sqlite3.connect(self.paths["db_path"])
        try:
            connection.execute(
                """
                CREATE TABLE documents (
                  id INTEGER PRIMARY KEY,
                  rel_path TEXT NOT NULL UNIQUE,
                  file_name TEXT NOT NULL,
                  file_type TEXT,
                  file_size INTEGER,
                  page_count INTEGER,
                  author TEXT,
                  date_created TEXT,
                  date_modified TEXT,
                  title TEXT,
                  subject TEXT,
                  recipients TEXT,
                  manual_field_locks_json TEXT NOT NULL DEFAULT '[]',
                  file_hash TEXT,
                  content_hash TEXT,
                  text_status TEXT NOT NULL DEFAULT 'ok',
                  lifecycle_status TEXT NOT NULL DEFAULT 'active',
                  ingested_at TEXT,
                  last_seen_at TEXT,
                  updated_at TEXT
                )
                """
            )
            if with_row:
                connection.execute(
                    """
                    INSERT INTO documents (
                      id, rel_path, file_name, file_type, file_size, page_count, author,
                      date_created, date_modified, title, subject, recipients,
                      manual_field_locks_json, file_hash, content_hash, text_status,
                      lifecycle_status, ingested_at, last_seen_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,
                        "legacy.txt",
                        "legacy.txt",
                        "txt",
                        12,
                        1,
                        "Legacy Author",
                        None,
                        None,
                        "Legacy Title",
                        None,
                        None,
                        "[]",
                        "file-hash",
                        "content-hash",
                        "ok",
                        "active",
                        "2026-04-14T00:00:00Z",
                        "2026-04-14T00:00:00Z",
                        "2026-04-14T00:00:00Z",
                    ),
                )
            connection.commit()
        finally:
            connection.close()

    def create_v6_documents_table(self, *, with_row: bool) -> None:
        connection = sqlite3.connect(self.paths["db_path"])
        try:
            connection.execute(
                """
                CREATE TABLE documents (
                  id INTEGER PRIMARY KEY,
                  display_id TEXT UNIQUE,
                  parent_document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
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
                  display_batch INTEGER,
                  display_family_sequence INTEGER,
                  display_attachment_sequence INTEGER
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE display_id_batches (
                  batch_number INTEGER PRIMARY KEY,
                  next_family_sequence INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            if with_row:
                connection.execute(
                    """
                    INSERT INTO documents (
                      id, display_id, parent_document_id, rel_path, file_name, file_type, file_size,
                      page_count, author, content_type, date_created, date_modified, title, subject,
                      participants, recipients, manual_field_locks_json, file_hash, content_hash,
                      text_status, lifecycle_status, ingested_at, last_seen_at, updated_at,
                      display_batch, display_family_sequence, display_attachment_sequence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,
                        "DOC001.00000001",
                        None,
                        "legacy.txt",
                        "legacy.txt",
                        "txt",
                        12,
                        1,
                        "Legacy Author",
                        "E-Doc",
                        None,
                        None,
                        "Legacy Title",
                        None,
                        None,
                        None,
                        "[]",
                        "file-hash",
                        "content-hash",
                        "ok",
                        "active",
                        "2026-04-14T00:00:00Z",
                        "2026-04-14T00:00:00Z",
                        "2026-04-14T00:00:00Z",
                        1,
                        1,
                        None,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO display_id_batches (batch_number, next_family_sequence, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (1, 2, "2026-04-14T00:00:00Z", "2026-04-14T00:00:00Z"),
                )
            connection.commit()
        finally:
            connection.close()

    def fetch_document_row(self, rel_path: str) -> sqlite3.Row:
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            row = connection.execute(
                "SELECT * FROM documents WHERE rel_path = ?",
                (rel_path,),
            ).fetchone()
            self.assertIsNotNone(row)
            return row
        finally:
            connection.close()

    def fetch_document_by_id(self, document_id: int) -> sqlite3.Row:
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            row = connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
            self.assertIsNotNone(row)
            return row
        finally:
            connection.close()

    def fetch_dataset_row(self, dataset_id: int) -> sqlite3.Row:
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            row = connection.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
            self.assertIsNotNone(row)
            return row
        finally:
            connection.close()

    def fetch_child_rows(self, parent_document_id: int) -> list[sqlite3.Row]:
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            return connection.execute(
                """
                SELECT *
                FROM documents
                WHERE parent_document_id = ?
                ORDER BY id ASC
                """,
                (parent_document_id,),
            ).fetchall()
        finally:
            connection.close()

    def write_email_message(
        self,
        path: Path,
        *,
        subject: str,
        body_text: str,
        author: str = "Alice Example <alice@example.com>",
        recipients: str = "Bob Example <bob@example.com>",
        cc: str | None = "Carol Example <carol@example.com>",
        date_created: str = "Tue, 14 Apr 2026 10:00:00 +0000",
        message_id: str | None = None,
        in_reply_to: str | None = None,
        references: str | None = None,
        conversation_index: str | None = None,
        conversation_topic: str | None = None,
        attachment_name: str | None = None,
        attachment_text: str | None = None,
    ) -> None:
        message = EmailMessage()
        message["From"] = author
        message["To"] = recipients
        if cc is not None:
            message["Cc"] = cc
        message["Subject"] = subject
        message["Date"] = date_created
        if message_id is not None:
            message["Message-ID"] = message_id
        if in_reply_to is not None:
            message["In-Reply-To"] = in_reply_to
        if references is not None:
            message["References"] = references
        if conversation_index is not None:
            message["Conversation-Index"] = conversation_index
        if conversation_topic is not None:
            message["Conversation-Topic"] = conversation_topic
        message.set_content(body_text)
        if attachment_name is not None and attachment_text is not None:
            message.add_attachment(attachment_text, subtype="plain", filename=attachment_name)
        path.write_bytes(message.as_bytes(policy=policy.default))

    def create_structured_extraction_job_version(self, job_name: str) -> int:
        sanitized_job_name = re.sub(r"[^a-zA-Z0-9_]+", "_", job_name.strip()).strip("_").lower()
        if sanitized_job_name and sanitized_job_name[0].isdigit():
            sanitized_job_name = f"job_{sanitized_job_name}"
        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            job_name,
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            sanitized_job_name,
            "--instruction",
            "Extract a stable value.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        return int(create_version_payload["job_version"]["id"])

    def setup_randomized_sql_filter_corpus(self) -> list[dict[str, object]]:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("beta body\n", encoding="utf-8")
        (self.root / "gamma.txt").write_text("gamma body\n", encoding="utf-8")
        self.write_email_message(
            self.root / "thread.eml",
            subject="Filter thread",
            body_text="Parent email body text.",
            attachment_name="notes.txt",
            attachment_text="Attachment detail.",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 4)

        for field_name, field_type in (
            ("review_score", "integer"),
            ("review_date", "date"),
            ("is_hot", "boolean"),
            ("review_note", "text"),
        ):
            add_field_exit, _, _, _ = self.run_cli("add-field", str(self.root), field_name, field_type)
            self.assertEqual(add_field_exit, 0)

        alpha_row = self.fetch_document_row("alpha.txt")
        beta_row = self.fetch_document_row("beta.txt")
        gamma_row = self.fetch_document_row("gamma.txt")
        parent_row = self.fetch_document_row("thread.eml")
        child_row = self.fetch_child_rows(parent_row["id"])[0]
        updates = {
            int(alpha_row["id"]): (1, "2026-04-01", 1, "alpha memo"),
            int(beta_row["id"]): (5, "2026-04-02", 0, "beta's note"),
            int(gamma_row["id"]): (None, "2026-04-03", None, "needs review"),
            int(parent_row["id"]): (7, "2026-04-04", 1, "mail thread"),
            int(child_row["id"]): (9, "2026-04-05", 0, "child's attachment"),
        }

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            for document_id, (review_score, review_date, is_hot, review_note) in updates.items():
                connection.execute(
                    """
                    UPDATE documents
                    SET review_score = ?, review_date = ?, is_hot = ?, review_note = ?
                    WHERE id = ?
                    """,
                    (review_score, review_date, is_hot, review_note, document_id),
                )
            connection.commit()

            rows = connection.execute(
                f"""
                SELECT id AS doc_id, file_name, review_note, review_score, review_date, is_hot
                FROM documents
                WHERE id IN ({", ".join("?" for _ in updates)})
                ORDER BY id ASC
                """,
                tuple(updates),
            ).fetchall()
        finally:
            connection.close()

        return [dict(row) for row in rows]

    def reference_doc_ids_for_sql_filter(
        self,
        corpus_rows: list[dict[str, object]],
        expression: str,
    ) -> list[int]:
        reference_connection = sqlite3.connect(":memory:")
        try:
            reference_connection.execute(
                """
                CREATE TABLE docs (
                  doc_id INTEGER PRIMARY KEY,
                  file_name TEXT,
                  review_note TEXT,
                  review_score INTEGER,
                  review_date TEXT,
                  is_hot INTEGER
                )
                """
            )
            reference_connection.executemany(
                """
                INSERT INTO docs (doc_id, file_name, review_note, review_score, review_date, is_hot)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        int(row["doc_id"]),
                        row["file_name"],
                        row["review_note"],
                        row["review_score"],
                        row["review_date"],
                        row["is_hot"],
                    )
                    for row in corpus_rows
                ],
            )
            return [
                int(row[0])
                for row in reference_connection.execute(
                    f"SELECT doc_id FROM docs WHERE {expression} ORDER BY doc_id ASC"
                ).fetchall()
            ]
        finally:
            reference_connection.close()

    def render_sql_filter_literal(self, field_type: str, value: object) -> str:
        if field_type == "boolean":
            return "TRUE" if bool(value) else "FALSE"
        if field_type == "integer":
            return str(int(value))
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def build_random_sql_filter_predicate(self, rng: random.Random) -> dict[str, object]:
        field_name = rng.choice(sorted(RANDOMIZED_FILTER_FIELD_TYPES))
        field_type = RANDOMIZED_FILTER_FIELD_TYPES[field_name]
        operator = rng.choice(RANDOMIZED_FILTER_OPERATOR_CHOICES[field_type])
        predicate: dict[str, object] = {
            "kind": "predicate",
            "field_name": field_name,
            "field_type": field_type,
            "operator": operator,
        }
        if operator in {"IS NULL", "IS NOT NULL"}:
            return predicate
        if operator in {"LIKE", "NOT LIKE"}:
            predicate["operand"] = rng.choice(RANDOMIZED_FILTER_LIKE_PATTERNS[field_name])
            return predicate
        if operator in {"IN", "NOT IN"}:
            values = RANDOMIZED_FILTER_LITERAL_POOLS[field_name]
            predicate["operand"] = [rng.choice(values) for _ in range(rng.randint(1, 4))]
            return predicate
        if operator in {"BETWEEN", "NOT BETWEEN"}:
            values = RANDOMIZED_FILTER_LITERAL_POOLS[field_name]
            predicate["operand"] = (rng.choice(values), rng.choice(values))
            return predicate
        predicate["operand"] = rng.choice(RANDOMIZED_FILTER_LITERAL_POOLS[field_name])
        return predicate

    def build_random_sql_filter_node(self, rng: random.Random, *, depth: int = 0) -> dict[str, object]:
        if depth >= 3 or (depth > 0 and rng.random() < 0.45):
            node = self.build_random_sql_filter_predicate(rng)
        else:
            branch_kind = rng.choice(["and", "or", "not", "predicate"])
            if branch_kind == "predicate":
                node = self.build_random_sql_filter_predicate(rng)
            elif branch_kind == "not":
                node = {
                    "kind": "not",
                    "child": self.build_random_sql_filter_node(rng, depth=depth + 1),
                }
            else:
                node = {
                    "kind": branch_kind,
                    "left": self.build_random_sql_filter_node(rng, depth=depth + 1),
                    "right": self.build_random_sql_filter_node(rng, depth=depth + 1),
                }
        if depth > 0 and rng.random() < 0.2:
            return {"kind": "group", "child": node}
        return node

    def render_random_sql_filter_node(self, node: dict[str, object], *, parent_precedence: int = 0) -> str:
        kind = str(node["kind"])
        if kind == "predicate":
            field_name = str(node["field_name"])
            field_type = str(node["field_type"])
            operator = str(node["operator"])
            precedence = 4
            if operator in {"IS NULL", "IS NOT NULL"}:
                text = f"{field_name} {operator}"
            elif operator in {"LIKE", "NOT LIKE"}:
                operand = self.render_sql_filter_literal(field_type, node["operand"])
                text = f"{field_name} {operator} {operand}"
            elif operator in {"IN", "NOT IN"}:
                operands = ", ".join(
                    self.render_sql_filter_literal(field_type, value)
                    for value in node["operand"]
                )
                text = f"{field_name} {operator} ({operands})"
            elif operator in {"BETWEEN", "NOT BETWEEN"}:
                left_value, right_value = node["operand"]
                text = (
                    f"{field_name} {operator} "
                    f"{self.render_sql_filter_literal(field_type, left_value)} "
                    f"AND {self.render_sql_filter_literal(field_type, right_value)}"
                )
            else:
                operand = self.render_sql_filter_literal(field_type, node["operand"])
                text = f"{field_name} {operator} {operand}"
        elif kind == "group":
            return f"({self.render_random_sql_filter_node(node['child'])})"
        elif kind == "not":
            precedence = 3
            text = f"NOT {self.render_random_sql_filter_node(node['child'], parent_precedence=precedence)}"
        else:
            operator = "AND" if kind == "and" else "OR"
            precedence = 2 if operator == "AND" else 1
            left_text = self.render_random_sql_filter_node(node["left"], parent_precedence=precedence)
            right_text = self.render_random_sql_filter_node(node["right"], parent_precedence=precedence)
            text = f"{left_text} {operator} {right_text}"
        if precedence < parent_precedence:
            return f"({text})"
        return text

    def write_fake_pst_file(self, name: str = "mailbox.pst", content: bytes = b"pst-v1") -> Path:
        path = self.root / name
        path.write_bytes(content)
        return path

    def write_fake_mbox_file(self, messages: list[EmailMessage], name: str = "mailbox.mbox") -> Path:
        path = self.root / name
        if path.exists():
            path.unlink()
        archive = mailbox.mbox(str(path), create=True)
        try:
            for message in messages:
                archive.add(message)
            archive.flush()
        finally:
            archive.close()
        return path

    def build_fake_mbox_message(
        self,
        *,
        subject: str | None,
        body_text: str,
        message_id: str | None,
        author: str | None = "Alice Example <alice@example.com>",
        recipients: str | None = "Bob Example <bob@example.com>",
        date_created: str = "Tue, 14 Apr 2026 10:00:00 +0000",
        attachment_name: str | None = None,
        attachment_text: str | None = None,
    ) -> EmailMessage:
        message = EmailMessage(policy=policy.default)
        if author is not None:
            message["From"] = author
        if recipients is not None:
            message["To"] = recipients
        if subject is not None:
            message["Subject"] = subject
        message["Date"] = date_created
        if message_id is not None:
            message["Message-ID"] = message_id
        message.set_content(body_text)
        if attachment_name is not None and attachment_text is not None:
            message.add_attachment(attachment_text, subtype="plain", filename=attachment_name)
        return message

    def build_fake_pst_message(
        self,
        *,
        source_item_id: str,
        subject: str | None,
        body_text: str,
        folder_path: str = "Inbox",
        attachment_name: str | None = None,
        attachment_text: str | None = None,
        author: str | None = "Alice Example <alice@example.com>",
        recipients: str | None = "Bob Example <bob@example.com>, Carol Example <carol@example.com>",
        date_created: str | None = "2026-04-14T10:00:00Z",
        message_class: str | None = None,
        transport_headers: str | None = None,
        conversation_topic: str | None = None,
    ) -> dict[str, object]:
        attachments: list[dict[str, object]] = []
        if attachment_name is not None and attachment_text is not None:
            attachments.append(
                {
                    "file_name": attachment_name,
                    "payload": attachment_text.encode("utf-8"),
                }
            )
        return {
            "source_item_id": source_item_id,
            "folder_path": folder_path,
            "subject": subject,
            "author": author,
            "recipients": recipients,
            "date_created": date_created,
            "message_class": message_class,
            "transport_headers": transport_headers,
            "conversation_topic": conversation_topic,
            "text_body": body_text,
            "html_body": None,
            "attachments": attachments,
        }

    def write_xls_fixture(self, path: Path) -> None:
        try:
            import xlwt
        except Exception as exc:  # pragma: no cover - test helper dependency
            self.skipTest(f"xlwt unavailable for xls fixture generation: {exc}")
        workbook = xlwt.Workbook()
        sheet1 = workbook.add_sheet("Sheet1")
        sheet1.write(0, 0, "Name")
        sheet1.write(0, 1, "Value")
        sheet1.write(1, 0, "Alpha")
        sheet1.write(1, 1, 42)
        notes = workbook.add_sheet("Notes")
        notes.write(0, 0, "Memo")
        notes.write(1, 0, "Budget approved")
        workbook.save(str(path))

    def write_pptx_fixture(self, path: Path) -> None:
        image_bytes = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a9mQAAAAASUVORK5CYII="
        )
        entries = {
            "docProps/core.xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:dcmitype="http://purl.org/dc/dcmitype/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>Quarterly Strategy Deck</dc:title>
  <dc:subject>Board Update</dc:subject>
  <dc:creator>Rachel Green</dc:creator>
  <dcterms:created xsi:type="dcterms:W3CDTF">2026-04-15T10:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">2026-04-15T11:00:00Z</dcterms:modified>
</cp:coreProperties>
""",
            "ppt/presentation.xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:presentation xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
 xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <p:sldIdLst>
    <p:sldId id="256" r:id="rId1"/>
    <p:sldId id="257" r:id="rId2"/>
  </p:sldIdLst>
</p:presentation>
""",
            "ppt/_rels/presentation.xml.rels": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide2.xml"/>
</Relationships>
""",
            "ppt/slides/slide1.xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
 xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <p:cSld>
    <p:spTree>
      <p:nvGrpSpPr><p:cNvPr id="1" name=""/></p:nvGrpSpPr>
      <p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>
      <p:sp>
        <p:nvSpPr><p:cNvPr id="2" name="Title"/><p:cNvSpPr/><p:nvPr><p:ph type="title"/></p:nvPr></p:nvSpPr>
        <p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="4000" cy="800"/></a:xfrm></p:spPr>
        <p:txBody><a:bodyPr/><a:lstStyle/><a:p><a:r><a:t>Q3 Revenue Review</a:t></a:r></a:p></p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr><p:cNvPr id="3" name="Body 1"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>
        <p:spPr><a:xfrm><a:off x="100" y="900"/><a:ext cx="4000" cy="1200"/></a:xfrm></p:spPr>
        <p:txBody>
          <a:bodyPr/><a:lstStyle/>
          <a:p><a:r><a:t>Revenue up 15%</a:t></a:r></a:p>
          <a:p><a:r><a:t>Pipeline remains strong</a:t></a:r></a:p>
        </p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr><p:cNvPr id="4" name="Body 2"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>
        <p:spPr><a:xfrm><a:off x="200" y="2300"/><a:ext cx="4000" cy="800"/></a:xfrm></p:spPr>
        <p:txBody><a:bodyPr/><a:lstStyle/><a:p><a:r><a:t>Operating margin improved</a:t></a:r></a:p></p:txBody>
      </p:sp>
    </p:spTree>
  </p:cSld>
</p:sld>
""",
            "ppt/slides/_rels/slide1.xml.rels": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/notesSlide" Target="../notesSlides/notesSlide1.xml"/>
</Relationships>
""",
            "ppt/notesSlides/notesSlide1.xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:notes xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
 xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">
  <p:cSld>
    <p:spTree>
      <p:nvGrpSpPr><p:cNvPr id="1" name=""/></p:nvGrpSpPr>
      <p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>
      <p:sp>
        <p:nvSpPr><p:cNvPr id="2" name="Notes"/><p:cNvSpPr/><p:nvPr><p:ph type="body"/></p:nvPr></p:nvSpPr>
        <p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="4000" cy="800"/></a:xfrm></p:spPr>
        <p:txBody><a:bodyPr/><a:lstStyle/><a:p><a:r><a:t>Emphasize retained enterprise customers.</a:t></a:r></a:p></p:txBody>
      </p:sp>
    </p:spTree>
  </p:cSld>
</p:notes>
""",
            "ppt/slides/slide2.xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
 xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <p:cSld>
    <p:spTree>
      <p:nvGrpSpPr><p:cNvPr id="1" name=""/></p:nvGrpSpPr>
      <p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>
      <p:sp>
        <p:nvSpPr><p:cNvPr id="2" name="Title"/><p:cNvSpPr/><p:nvPr><p:ph type="title"/></p:nvPr></p:nvSpPr>
        <p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="4000" cy="800"/></a:xfrm></p:spPr>
        <p:txBody><a:bodyPr/><a:lstStyle/><a:p><a:r><a:t>Hiring Plan</a:t></a:r></a:p></p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr><p:cNvPr id="3" name="Body"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>
        <p:spPr><a:xfrm><a:off x="150" y="1000"/><a:ext cx="4000" cy="1000"/></a:xfrm></p:spPr>
        <p:txBody><a:bodyPr/><a:lstStyle/><a:p><a:r><a:t>Open 12 roles in engineering</a:t></a:r></a:p></p:txBody>
      </p:sp>
      <p:pic>
        <p:nvPicPr>
          <p:cNvPr id="4" name="Stark Therapeutics logo" descr="Stark Therapeutics logo"/>
          <p:cNvPicPr/>
          <p:nvPr/>
        </p:nvPicPr>
        <p:blipFill>
          <a:blip r:embed="rId1"/>
          <a:stretch><a:fillRect/></a:stretch>
        </p:blipFill>
        <p:spPr>
          <a:xfrm><a:off x="150" y="2200"/><a:ext cx="952500" cy="952500"/></a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
        </p:spPr>
      </p:pic>
    </p:spTree>
  </p:cSld>
</p:sld>
""",
            "ppt/slides/_rels/slide2.xml.rels": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="../media/image1.png"/>
</Relationships>
""",
        }
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in entries.items():
                archive.writestr(name, content)
            archive.writestr("ppt/media/image1.png", image_bytes)

    def write_tiff_fixture(self, path: Path, color: tuple[int, int, int]) -> None:
        try:
            from PIL import Image
        except Exception as exc:  # pragma: no cover - test helper dependency
            self.skipTest(f"Pillow unavailable for TIFF fixture generation: {exc}")
        image = Image.new("RGB", (8, 8), color)
        image.save(path, format="TIFF")

    def write_minimal_pdf(self, path: Path, title: str) -> None:
        path.write_bytes(
            (
                b"%PDF-1.1\n"
                b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n"
                b"2 0 obj<< /Type /Pages /Count 1 /Kids [3 0 R] >>endobj\n"
                b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] /Contents 4 0 R >>endobj\n"
                + f"4 0 obj<< /Length {len(title) + 30} >>stream\nBT /F1 12 Tf 20 100 Td ({title}) Tj ET\nendstream endobj\n".encode("latin-1")
                + b"trailer<< /Root 1 0 R >>\n%%EOF\n"
            )
        )

    def write_production_fixture(self, *, loadfile_volume_prefix: str | None = None) -> Path:
        production_root = self.root / "Synthetic_Production"
        data_dir = production_root / "DATA"
        text_dir = production_root / "TEXT" / "TEXT001"
        image_dir = production_root / "IMAGES" / "IMG001"
        native_dir = production_root / "NATIVES" / "NAT001"
        for directory in (data_dir, text_dir, image_dir, native_dir):
            directory.mkdir(parents=True, exist_ok=True)

        loadfile_root = (loadfile_volume_prefix or production_root.name).strip()

        def loadfile_path(*parts: str) -> str:
            return ".\\" + "\\".join([loadfile_root, *parts])

        (text_dir / "PDX000001.txt").write_text(
            (
                "From: Elena Steven <elena@example.com>\n"
                "To: Harry Montoro <harry@example.com>\n"
                "Date: Tue, 14 Apr 2026 10:32:00 +0000\n"
                "Subject: Attachment Handling\n\n"
                "Parent production memo\n"
                "Discuss attachment handling.\n"
            ),
            encoding="utf-8",
        )
        (text_dir / "PDX000003.txt").write_text(
            (
                "From: Review Team\n"
                "Sent: 04/14/2026 09:00 AM\n\n"
                "Case status update\n"
                "Contains follow-up details.\n"
            ),
            encoding="utf-8",
        )
        (text_dir / "PDX000004.txt").write_text("Native-backed production doc\nUse native preview first.\n", encoding="utf-8")

        self.write_tiff_fixture(image_dir / "PDX000001.tif", (255, 0, 0))
        self.write_tiff_fixture(image_dir / "PDX000002.tif", (0, 255, 0))
        self.write_tiff_fixture(image_dir / "PDX000003.tif", (0, 0, 255))
        self.write_tiff_fixture(image_dir / "PDX000005.tif", (128, 128, 0))
        self.write_tiff_fixture(image_dir / "PDX000006.tif", (0, 128, 128))
        self.write_minimal_pdf(native_dir / "PDX000004.pdf", "Native preview document")

        headers = ["Begin Bates", "End Bates", "Begin Attachment", "End Attachment", "Text Precedence", "FILE_PATH"]
        rows = [
            ["PDX000001", "PDX000002", "PDX000001", "PDX000003", loadfile_path("TEXT", "TEXT001", "PDX000001.txt"), ""],
            ["PDX000003", "PDX000003", "", "", loadfile_path("TEXT", "TEXT001", "PDX000003.txt"), ""],
            ["PDX000004", "PDX000004", "", "", loadfile_path("TEXT", "TEXT001", "PDX000004.txt"), loadfile_path("NATIVES", "NAT001", "PDX000004.pdf")],
            ["PDX000005", "PDX000006", "", "", "", ""],
        ]
        delimiter = b"\x14"
        quote = b"\xfe"

        def dat_line(fields: list[str]) -> bytes:
            return delimiter.join(quote + field.encode("latin-1") + quote for field in fields) + b"\r\n"

        (data_dir / "Synthetic_Production.dat").write_bytes(dat_line(headers) + b"".join(dat_line(row) for row in rows))

        opt_lines = [
            f"PDX000001,Synthetic_Production,{loadfile_path('IMAGES', 'IMG001', 'PDX000001.tif')},Y,,,2",
            f"PDX000002,Synthetic_Production,{loadfile_path('IMAGES', 'IMG001', 'PDX000002.tif')},,,,",
            f"PDX000003,Synthetic_Production,{loadfile_path('IMAGES', 'IMG001', 'PDX000003.tif')},Y,,,1",
            f"PDX000005,Synthetic_Production,{loadfile_path('IMAGES', 'IMG001', 'PDX000005.tif')},Y,,,2",
            f"PDX000006,Synthetic_Production,{loadfile_path('IMAGES', 'IMG001', 'PDX000006.tif')},,,,",
        ]
        (data_dir / "Synthetic_Production.opt").write_text("\n".join(opt_lines) + "\n", encoding="utf-8")
        return production_root

    def test_bootstrap_migrates_legacy_schema_and_backfills_content_type(self) -> None:
        self.create_legacy_documents_table(with_row=True)

        result = retriever_tools.bootstrap(self.root)

        self.assertEqual(result["schema_version"], retriever_tools.SCHEMA_VERSION)
        self.assertEqual(result["tool_version"], retriever_tools.TOOL_VERSION)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            columns = retriever_tools.table_columns(connection, "documents")
            preview_columns = retriever_tools.table_columns(connection, "document_previews")
            self.assertIn("content_type", columns)
            self.assertIn("custodian", columns)
            self.assertIn("participants", columns)
            self.assertIn("control_number", columns)
            self.assertIn("conversation_id", columns)
            self.assertIn("conversation_assignment_mode", columns)
            self.assertIn("dataset_id", columns)
            self.assertIn("parent_document_id", columns)
            self.assertIn("child_document_kind", columns)
            self.assertIn("root_message_key", columns)
            self.assertIn("target_fragment", preview_columns)
            row = connection.execute(
                """
                SELECT content_type, custodian, participants, control_number, conversation_id,
                       conversation_assignment_mode, dataset_id, parent_document_id,
                       child_document_kind, root_message_key
                FROM documents
                WHERE id = 1
                """
            ).fetchone()
            conversations_table = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = 'conversations'
                """
            ).fetchone()
            email_threading_table = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = 'document_email_threading'
                """
            ).fetchone()
            dataset_row = connection.execute(
                """
                SELECT *
                FROM datasets
                WHERE id = ?
                """,
                (row["dataset_id"],),
            ).fetchone()
            control_number_batch_row = connection.execute(
                """
                SELECT batch_number, next_family_sequence
                FROM control_number_batches
                WHERE batch_number = 1
                """
            ).fetchone()
        finally:
            connection.close()

        self.assertEqual(row["content_type"], "E-Doc")
        self.assertIsNone(row["custodian"])
        self.assertIsNone(row["participants"])
        self.assertEqual(row["control_number"], "DOC001.00000001")
        self.assertIsNone(row["conversation_id"])
        self.assertEqual(row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)
        self.assertIsNotNone(row["dataset_id"])
        self.assertIsNone(row["parent_document_id"])
        self.assertIsNone(row["child_document_kind"])
        self.assertIsNone(row["root_message_key"])
        self.assertIsNotNone(conversations_table)
        self.assertIsNotNone(email_threading_table)
        self.assertIsNotNone(dataset_row)
        self.assertEqual(dataset_row["source_kind"], retriever_tools.FILESYSTEM_SOURCE_KIND)
        self.assertEqual(dataset_row["dataset_locator"], ".")
        self.assertEqual(dataset_row["dataset_name"], self.root.name)
        self.assertIsNotNone(control_number_batch_row)
        self.assertEqual(control_number_batch_row["next_family_sequence"], 2)

        runtime = json.loads(self.paths["runtime_path"].read_text(encoding="utf-8"))
        self.assertEqual(runtime["tool_version"], retriever_tools.TOOL_VERSION)
        self.assertEqual(runtime["schema_version"], retriever_tools.SCHEMA_VERSION)
        self.assertEqual(runtime["template_sha256"], retriever_tools.sha256_file(self.paths["tool_path"]))

    def test_bootstrap_initializes_processing_schema_and_job_crud(self) -> None:
        result = retriever_tools.bootstrap(self.root)
        self.assertEqual(result["schema_version"], retriever_tools.SCHEMA_VERSION)

        exit_code, create_job_payload, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Contract Metadata",
            "structured_extraction",
            "--description",
            "Extract key contract facts",
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(create_job_payload)
        self.assertEqual(create_job_payload["job"]["job_name"], "contract_metadata")
        self.assertEqual(create_job_payload["job"]["job_kind"], "structured_extraction")

        exit_code, add_output_payload, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "contract_metadata",
            "Governing Law",
            "--value-type",
            "text",
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(add_output_payload)
        self.assertEqual(add_output_payload["job_output"]["output_name"], "governing_law")
        self.assertTrue(add_output_payload["created"])

        exit_code, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "contract_metadata",
            "--provider",
            "openai",
            "--model",
            "gpt-5.4",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Extract the governing law field.",
            "--response-schema-json",
            "{\"type\":\"object\",\"properties\":{\"governing_law\":{\"type\":\"string\"}}}",
            "--parameters-json",
            "{\"temperature\":0}",
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(create_version_payload)
        self.assertEqual(create_version_payload["job_version"]["version"], 1)
        self.assertEqual(create_version_payload["job_version"]["capability"], "text_structured")
        self.assertEqual(create_version_payload["job_version"]["provider"], "openai")
        self.assertEqual(create_version_payload["job_version"]["parameters"], {"temperature": 0})
        self.assertEqual(
            create_version_payload["job_version"]["response_schema"]["type"],
            "object",
        )

        exit_code, list_jobs_payload, _, _ = self.run_cli("list-jobs", str(self.root))
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(list_jobs_payload)
        self.assertEqual(len(list_jobs_payload["jobs"]), 1)
        self.assertEqual(list_jobs_payload["jobs"][0]["latest_job_version"]["version"], 1)
        self.assertEqual(list_jobs_payload["jobs"][0]["outputs"][0]["output_name"], "governing_law")

        exit_code, versions_payload, _, _ = self.run_cli("list-job-versions", str(self.root), "contract_metadata")
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(versions_payload)
        self.assertEqual(len(versions_payload["job_versions"]), 1)
        self.assertEqual(versions_payload["job_versions"][0]["display_name"], "contract_metadata v1")

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            document_columns = retriever_tools.table_columns(connection, "documents")
            self.assertIn("source_text_revision_id", document_columns)
            self.assertIn("active_search_text_revision_id", document_columns)
            self.assertIn("active_text_source_kind", document_columns)
            self.assertIn("active_text_language", document_columns)
            self.assertIn("active_text_quality_score", document_columns)
            self.assertIn("capability", retriever_tools.table_columns(connection, "job_versions"))
            run_item_columns = retriever_tools.table_columns(connection, "run_items")
            self.assertIn("claimed_by", run_item_columns)
            self.assertIn("claimed_at", run_item_columns)
            self.assertIn("last_heartbeat_at", run_item_columns)

            expected_tables = {
                "jobs",
                "job_outputs",
                "job_versions",
                "runs",
                "run_snapshot_documents",
                "run_items",
                "attempts",
                "results",
                "result_outputs",
                "text_revisions",
                "text_revision_segments",
                "embedding_vectors",
                "publications",
                "text_revision_activation_events",
            }
            actual_tables = {
                row["name"]
                for row in connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                    """
                ).fetchall()
            }
            self.assertTrue(expected_tables.issubset(actual_tables))

            job_row = connection.execute(
                "SELECT id FROM jobs WHERE job_name = ?",
                ("contract_metadata",),
            ).fetchone()
            self.assertIsNotNone(job_row)
            job_version_row = connection.execute(
                "SELECT id FROM job_versions WHERE job_id = ? AND version = 1",
                (job_row["id"],),
            ).fetchone()
            self.assertIsNotNone(job_version_row)

            document_cursor = connection.execute(
                """
                INSERT INTO documents (rel_path, file_name, updated_at)
                VALUES (?, ?, ?)
                """,
                ("sample.txt", "sample.txt", "2026-04-17T00:00:00Z"),
            )
            document_id = int(document_cursor.lastrowid)
            text_revision_cursor = connection.execute(
                """
                INSERT INTO text_revisions (
                  document_id,
                  revision_kind,
                  language,
                  parent_revision_id,
                  created_by_job_version_id,
                  storage_rel_path,
                  content_hash,
                  char_count,
                  token_estimate,
                  quality_score,
                  provider_metadata_json,
                  created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document_id,
                    "source_extract",
                    "en",
                    None,
                    None,
                    None,
                    retriever_tools.sha256_text("alpha"),
                    5,
                    1,
                    None,
                    "{}",
                    "2026-04-17T00:00:00Z",
                ),
            )
            input_revision_id = int(text_revision_cursor.lastrowid)
            input_identity = retriever_tools.build_text_revision_input_identity(input_revision_id)

            connection.execute(
                """
                INSERT INTO results (
                  run_id,
                  document_id,
                  job_version_id,
                  input_revision_id,
                  input_identity,
                  raw_output_json,
                  normalized_output_json,
                  created_text_revision_id,
                  provider_metadata_json,
                  created_at,
                  retracted_at,
                  retraction_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    None,
                    document_id,
                    int(job_version_row["id"]),
                    input_revision_id,
                    input_identity,
                    "{\"governing_law\":\"Delaware\"}",
                    "{\"governing_law\":\"Delaware\"}",
                    None,
                    "{}",
                    "2026-04-17T00:00:00Z",
                    None,
                    None,
                ),
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO results (
                      run_id,
                      document_id,
                      job_version_id,
                      input_revision_id,
                      input_identity,
                      raw_output_json,
                      normalized_output_json,
                      created_text_revision_id,
                      provider_metadata_json,
                      created_at,
                      retracted_at,
                      retraction_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        None,
                        document_id,
                        int(job_version_row["id"]),
                        input_revision_id,
                        input_identity,
                        "{\"governing_law\":\"Delaware\"}",
                        "{\"governing_law\":\"Delaware\"}",
                        None,
                        "{}",
                        "2026-04-17T00:00:01Z",
                        None,
                        None,
                    ),
                )

            connection.execute(
                """
                UPDATE results
                SET retracted_at = ?, retraction_reason = ?
                WHERE document_id = ? AND job_version_id = ? AND input_identity = ?
                """,
                (
                    "2026-04-17T00:00:02Z",
                    "Superseded during testing",
                    document_id,
                    int(job_version_row["id"]),
                    input_identity,
                ),
            )
            connection.execute(
                """
                INSERT INTO results (
                  run_id,
                  document_id,
                  job_version_id,
                  input_revision_id,
                  input_identity,
                  raw_output_json,
                  normalized_output_json,
                  created_text_revision_id,
                  provider_metadata_json,
                  created_at,
                  retracted_at,
                  retraction_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    None,
                    document_id,
                    int(job_version_row["id"]),
                    input_revision_id,
                    input_identity,
                    "{\"governing_law\":\"New York\"}",
                    "{\"governing_law\":\"New York\"}",
                    None,
                    "{}",
                    "2026-04-17T00:00:03Z",
                    None,
                    None,
                ),
            )
            connection.commit()

            active_count_row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM results
                WHERE document_id = ? AND job_version_id = ? AND input_identity = ? AND retracted_at IS NULL
                """,
                (document_id, int(job_version_row["id"]), input_identity),
            ).fetchone()
            self.assertEqual(active_count_row["count"], 1)
        finally:
            connection.close()

    def test_ingest_seeds_text_revisions_and_create_run_freezes_family_snapshot(self) -> None:
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Run planning",
            body_text="Parent body text for the email.",
            attachment_name="notes.txt",
            attachment_text="confidential attachment detail",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        parent_row = self.fetch_document_row("thread.eml")
        child_rows = self.fetch_child_rows(parent_row["id"])
        self.assertEqual(len(child_rows), 1)
        child_row = child_rows[0]

        self.assertIsNotNone(parent_row["source_text_revision_id"])
        self.assertEqual(parent_row["source_text_revision_id"], parent_row["active_search_text_revision_id"])
        self.assertIsNotNone(child_row["source_text_revision_id"])
        self.assertEqual(child_row["source_text_revision_id"], child_row["active_search_text_revision_id"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            parent_revision_row = connection.execute(
                "SELECT * FROM text_revisions WHERE id = ?",
                (parent_row["source_text_revision_id"],),
            ).fetchone()
            child_revision_row = connection.execute(
                "SELECT * FROM text_revisions WHERE id = ?",
                (child_row["source_text_revision_id"],),
            ).fetchone()
            self.assertIsNotNone(parent_revision_row)
            self.assertIsNotNone(child_revision_row)
            parent_revision_text = retriever_tools.read_text_revision_body(
                self.paths,
                parent_revision_row["storage_rel_path"],
            )
            child_revision_text = retriever_tools.read_text_revision_body(
                self.paths,
                child_revision_row["storage_rel_path"],
            )
        finally:
            connection.close()

        self.assertIn("Parent body text for the email.", parent_revision_text)
        self.assertIn("confidential attachment detail", child_revision_text)

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Issue Tags",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "issue_tags",
            "primary_issue",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        version_exit, version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "issue_tags",
            "--provider",
            "openai",
            "--model",
            "gpt-5.4",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Extract the main issue tag.",
            "--parameters-json",
            "{\"temperature\":0}",
        )
        self.assertEqual(version_exit, 0)
        self.assertIsNotNone(version_payload)
        self.assertEqual(version_payload["job_version"]["version"], 1)

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-name",
            "issue_tags",
            "--job-version",
            "1",
            "--keyword",
            "confidential",
            "--family-mode",
            "with_family",
            "--limit",
            "1",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_payload = create_run_payload["run"]
        self.assertEqual(run_payload["planned_count"], 2)
        self.assertEqual(len(run_payload["documents"]), 2)

        snapshot_by_document_id = {
            int(item["document_id"]): item
            for item in run_payload["documents"]
        }
        self.assertEqual(set(snapshot_by_document_id), {int(parent_row["id"]), int(child_row["id"])})
        self.assertEqual(
            snapshot_by_document_id[int(child_row["id"])]["pinned_input_revision_id"],
            int(child_row["source_text_revision_id"]),
        )
        self.assertEqual(
            snapshot_by_document_id[int(child_row["id"])]["pinned_input_identity"],
            retriever_tools.build_text_revision_input_identity(int(child_row["source_text_revision_id"])),
        )
        self.assertEqual(
            snapshot_by_document_id[int(child_row["id"])]["inclusion_reason"]["direct_reasons"][0]["type"],
            "keyword",
        )
        self.assertEqual(
            snapshot_by_document_id[int(parent_row["id"])]["inclusion_reason"]["family_seed_document_ids"],
            [int(child_row["id"])],
        )

        first_run_id = int(run_payload["id"])
        second_run_exit, second_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-name",
            "issue_tags",
            "--job-version",
            "1",
            "--from-run-id",
            str(first_run_id),
        )
        self.assertEqual(second_run_exit, 0)
        self.assertIsNotNone(second_run_payload)
        second_snapshot = {
            int(item["document_id"]): item
            for item in second_run_payload["run"]["documents"]
        }
        self.assertEqual(set(second_snapshot), set(snapshot_by_document_id))
        self.assertEqual(
            second_snapshot[int(parent_row["id"])]["pinned_input_revision_id"],
            snapshot_by_document_id[int(parent_row["id"])]["pinned_input_revision_id"],
        )
        self.assertEqual(
            second_snapshot[int(child_row["id"])]["pinned_input_identity"],
            snapshot_by_document_id[int(child_row["id"])]["pinned_input_identity"],
        )

    def test_activate_text_revision_rebuilds_search_chunks_and_records_event(self) -> None:
        note_path = self.root / "note.txt"
        note_path.write_text("sourcealphaonly original searchable text", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("note.txt")
        source_revision_id = int(document_row["source_text_revision_id"])
        replacement_text = "promotedbetaonly replacement searchable text"

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            connection.execute("BEGIN")
            try:
                promoted_revision_id = retriever_tools.create_text_revision_row(
                    connection,
                    self.paths,
                    document_id=int(document_row["id"]),
                    revision_kind="ocr",
                    text_content=replacement_text,
                    language="en",
                    parent_revision_id=source_revision_id,
                    created_by_job_version_id=None,
                    quality_score=0.95,
                    provider_metadata={"provider": "test"},
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        finally:
            connection.close()

        before_activation = retriever_tools.search(self.root, "promotedbetaonly", None, None, None, 1, 20)
        self.assertEqual(before_activation["total_hits"], 0)
        original_search = retriever_tools.search(self.root, "sourcealphaonly", None, None, None, 1, 20)
        self.assertEqual(original_search["total_hits"], 1)

        activate_exit, activate_payload, _, _ = self.run_cli(
            "activate-text-revision",
            str(self.root),
            "--doc-id",
            str(document_row["id"]),
            "--text-revision-id",
            str(promoted_revision_id),
        )
        self.assertEqual(activate_exit, 0)
        self.assertIsNotNone(activate_payload)
        self.assertEqual(activate_payload["activation_policy"], "manual")
        self.assertEqual(activate_payload["text_revision"]["id"], promoted_revision_id)
        self.assertTrue(activate_payload["text_revision"]["is_active_search_revision"])

        list_exit, list_payload, _, _ = self.run_cli(
            "list-text-revisions",
            str(self.root),
            "--doc-id",
            str(document_row["id"]),
        )
        self.assertEqual(list_exit, 0)
        self.assertIsNotNone(list_payload)
        listed_by_id = {int(item["id"]): item for item in list_payload["text_revisions"]}
        self.assertEqual(set(listed_by_id), {source_revision_id, int(promoted_revision_id)})
        self.assertTrue(listed_by_id[int(promoted_revision_id)]["is_active_search_revision"])
        self.assertTrue(listed_by_id[source_revision_id]["is_source_revision"])

        updated_row = self.fetch_document_by_id(int(document_row["id"]))
        self.assertEqual(updated_row["source_text_revision_id"], source_revision_id)
        self.assertEqual(updated_row["active_search_text_revision_id"], promoted_revision_id)
        self.assertEqual(updated_row["active_text_source_kind"], "ocr")
        self.assertEqual(updated_row["content_hash"], retriever_tools.sha256_text(replacement_text))

        promoted_search = retriever_tools.search(self.root, "promotedbetaonly", None, None, None, 1, 20)
        self.assertEqual(promoted_search["total_hits"], 1)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            chunk_texts = [
                row["text_content"]
                for row in connection.execute(
                    """
                    SELECT text_content
                    FROM document_chunks
                    WHERE document_id = ?
                    ORDER BY chunk_index ASC
                    """,
                    (document_row["id"],),
                ).fetchall()
            ]
            self.assertEqual(chunk_texts, [replacement_text])
            activation_row = connection.execute(
                """
                SELECT *
                FROM text_revision_activation_events
                WHERE document_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (document_row["id"],),
            ).fetchone()
            self.assertIsNotNone(activation_row)
            self.assertEqual(activation_row["text_revision_id"], promoted_revision_id)
            self.assertEqual(activation_row["activation_policy"], "manual")
        finally:
            connection.close()

    def test_execute_run_reuses_results_and_publishes_bound_outputs(self) -> None:
        note_path = self.root / "contract.txt"
        note_path.write_text("This contract mentions Delaware and an automatic renewal clause.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("contract.txt")

        add_field_exit, _, _, _ = self.run_cli("add-field", str(self.root), "governing_law", "text")
        self.assertEqual(add_field_exit, 0)
        add_bool_field_exit, _, _, _ = self.run_cli("add-field", str(self.root), "auto_renewal", "boolean")
        self.assertEqual(add_bool_field_exit, 0)

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Contract Metadata",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        add_governing_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "contract_metadata",
            "governing_law",
            "--value-type",
            "text",
            "--bind-custom-field",
            "governing_law",
        )
        self.assertEqual(add_governing_output_exit, 0)
        add_renewal_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "contract_metadata",
            "auto_renewal",
            "--value-type",
            "boolean",
            "--bind-custom-field",
            "auto_renewal",
        )
        self.assertEqual(add_renewal_output_exit, 0)

        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "contract_metadata",
            "--provider",
            "static_json",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Return contract metadata.",
            "--parameters-json",
            "{\"output_values\":{\"governing_law\":\"Delaware\",\"auto_renewal\":true}}",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        first_run_id = int(create_run_payload["run"]["id"])

        execute_run_exit, execute_run_payload, _, _ = self.run_cli(
            "execute-run",
            str(self.root),
            "--run-id",
            str(first_run_id),
        )
        self.assertEqual(execute_run_exit, 0)
        self.assertIsNotNone(execute_run_payload)
        self.assertEqual(execute_run_payload["run"]["status"], "completed")
        self.assertEqual(execute_run_payload["run"]["completed_count"], 1)
        self.assertEqual(execute_run_payload["run"]["skipped_count"], 0)
        self.assertEqual(len(execute_run_payload["run_items"]), 1)
        self.assertEqual(execute_run_payload["run_items"][0]["status"], "completed")
        self.assertEqual(len(execute_run_payload["results"]), 1)
        first_result = execute_run_payload["results"][0]
        outputs_by_name = {item["output_name"]: item for item in first_result["outputs"]}
        self.assertEqual(outputs_by_name["governing_law"]["output_value"], "Delaware")
        self.assertEqual(outputs_by_name["auto_renewal"]["output_value"], True)

        list_results_exit, list_results_payload, _, _ = self.run_cli(
            "list-results",
            str(self.root),
            "--run-id",
            str(first_run_id),
        )
        self.assertEqual(list_results_exit, 0)
        self.assertIsNotNone(list_results_payload)
        self.assertEqual(len(list_results_payload["results"]), 1)

        publish_exit, publish_payload, _, _ = self.run_cli(
            "publish-run-results",
            str(self.root),
            "--run-id",
            str(first_run_id),
        )
        self.assertEqual(publish_exit, 0)
        self.assertIsNotNone(publish_payload)
        self.assertEqual(publish_payload["published_count"], 2)
        updated_document_row = self.fetch_document_by_id(int(document_row["id"]))
        self.assertEqual(updated_document_row["governing_law"], "Delaware")
        self.assertEqual(updated_document_row["auto_renewal"], 1)

        second_run_exit, second_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--from-run-id",
            str(first_run_id),
        )
        self.assertEqual(second_run_exit, 0)
        self.assertIsNotNone(second_run_payload)
        second_run_id = int(second_run_payload["run"]["id"])

        second_execute_exit, second_execute_payload, _, _ = self.run_cli(
            "execute-run",
            str(self.root),
            "--run-id",
            str(second_run_id),
        )
        self.assertEqual(second_execute_exit, 0)
        self.assertIsNotNone(second_execute_payload)
        self.assertEqual(second_execute_payload["run"]["status"], "completed")
        self.assertEqual(second_execute_payload["run"]["completed_count"], 0)
        self.assertEqual(second_execute_payload["run"]["skipped_count"], 1)
        self.assertEqual(len(second_execute_payload["results"]), 1)
        self.assertEqual(second_execute_payload["results"][0]["id"], first_result["id"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            result_count = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM results
                WHERE document_id = ? AND job_version_id = ? AND retracted_at IS NULL
                """,
                (document_row["id"], job_version_id),
            ).fetchone()["count"]
            self.assertEqual(result_count, 1)
            publication_count = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM publications
                WHERE document_id = ?
                """,
                (document_row["id"],),
            ).fetchone()["count"]
            self.assertEqual(publication_count, 2)
            reused_run_item = connection.execute(
                """
                SELECT *
                FROM run_items
                WHERE run_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (second_run_id,),
            ).fetchone()
            self.assertIsNotNone(reused_run_item)
            self.assertEqual(reused_run_item["status"], "skipped")
            self.assertEqual(reused_run_item["result_id"], first_result["id"])
        finally:
            connection.close()

    def test_claim_complete_translation_run_creates_derived_text_revision(self) -> None:
        note_path = self.root / "memo.txt"
        note_path.write_text("Original English memo text.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("memo.txt")
        source_revision_id = int(document_row["source_text_revision_id"])

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Translate ES",
            "translation",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "translate_es",
            "--provider",
            "static_text",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Translate to Spanish.",
            "--parameters-json",
            "{\"target_language\":\"es\",\"translated_text\":\"ES::{text}\"}",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "translator-es",
            "--limit",
            "1",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        self.assertEqual(len(claim_payload["run_items"]), 1)
        run_item_id = int(claim_payload["run_items"][0]["id"])

        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        self.assertEqual(context_payload["context"]["job_version"]["capability"], "text_translation")
        self.assertEqual(
            context_payload["context"]["input"]["inline_text"],
            "Original English memo text.",
        )
        self.assertEqual(context_payload["context"]["execution"]["target_language"], "es")

        completion_template = context_payload["context"]["execution"]["completion_template"]
        raw_output = dict(completion_template["raw_output_json"])
        raw_output["translated_text"] = "ES::Original English memo text."
        normalized_output = dict(completion_template["normalized_output_json"])
        normalized_output["translated_text"] = "ES::Original English memo text."
        created_text_revision = dict(completion_template["created_text_revision_json"])
        created_text_revision["text_content"] = "ES::Original English memo text."

        complete_exit, complete_payload, _, _ = self.run_cli(
            "complete-run-item",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
            "--claimed-by",
            "translator-es",
            "--raw-output-json",
            json.dumps(raw_output),
            "--normalized-output-json",
            json.dumps(normalized_output),
            "--created-text-revision-json",
            json.dumps(created_text_revision),
        )
        self.assertEqual(complete_exit, 0)
        self.assertIsNotNone(complete_payload)
        self.assertFalse(complete_payload["idempotent"])
        self.assertEqual(complete_payload["run"]["status"], "completed")
        result_payload = complete_payload["result"]
        self.assertIsNotNone(result_payload["created_text_revision_id"])

        updated_row = self.fetch_document_by_id(int(document_row["id"]))
        self.assertEqual(updated_row["active_search_text_revision_id"], source_revision_id)
        self.assertEqual(updated_row["source_text_revision_id"], source_revision_id)

        revision_exit, revision_payload, _, _ = self.run_cli(
            "list-text-revisions",
            str(self.root),
            "--doc-id",
            str(document_row["id"]),
        )
        self.assertEqual(revision_exit, 0)
        self.assertIsNotNone(revision_payload)
        revisions_by_id = {int(item["id"]): item for item in revision_payload["text_revisions"]}
        translated_revision = revisions_by_id[int(result_payload["created_text_revision_id"])]
        self.assertEqual(translated_revision["revision_kind"], "translation")
        self.assertEqual(translated_revision["parent_revision_id"], source_revision_id)
        self.assertFalse(translated_revision["is_active_search_revision"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            translated_row = connection.execute(
                "SELECT * FROM text_revisions WHERE id = ?",
                (result_payload["created_text_revision_id"],),
            ).fetchone()
            self.assertIsNotNone(translated_row)
            translated_text = retriever_tools.read_text_revision_body(
                self.paths,
                translated_row["storage_rel_path"],
            )
            self.assertEqual(translated_text, "ES::Original English memo text.")
        finally:
            connection.close()

    def test_claim_complete_translation_run_with_always_activation_promotes_revision(self) -> None:
        note_path = self.root / "memo-activate.txt"
        note_path.write_text("Original English memo text.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("memo-activate.txt")
        source_revision_id = int(document_row["source_text_revision_id"])

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Translate RU",
            "translation",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "translate_ru",
            "--provider",
            "static_text",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Translate to Russian.",
            "--parameters-json",
            "{\"target_language\":\"ru\",\"translated_text\":\"RU::{text}\"}",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--doc-id",
            str(document_row["id"]),
            "--activation-policy",
            "always",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        self.assertEqual(create_run_payload["run"]["activation_policy"], "always")
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "translator-ru",
            "--limit",
            "1",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        run_item_id = int(claim_payload["run_items"][0]["id"])

        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        completion_template = context_payload["context"]["execution"]["completion_template"]
        raw_output = dict(completion_template["raw_output_json"])
        raw_output["translated_text"] = "RU::Original English memo text."
        normalized_output = dict(completion_template["normalized_output_json"])
        normalized_output["translated_text"] = "RU::Original English memo text."
        created_text_revision = dict(completion_template["created_text_revision_json"])
        created_text_revision["text_content"] = "RU::Original English memo text."

        complete_exit, complete_payload, _, _ = self.run_cli(
            "complete-run-item",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
            "--claimed-by",
            "translator-ru",
            "--raw-output-json",
            json.dumps(raw_output),
            "--normalized-output-json",
            json.dumps(normalized_output),
            "--created-text-revision-json",
            json.dumps(created_text_revision),
        )
        self.assertEqual(complete_exit, 0)
        self.assertIsNotNone(complete_payload)
        self.assertIn("activation", complete_payload)
        result_payload = complete_payload["result"]
        translated_revision_id = int(result_payload["created_text_revision_id"])
        self.assertEqual(complete_payload["activation"]["text_revision"]["id"], translated_revision_id)
        self.assertEqual(complete_payload["activation"]["activation_policy"], "always")

        updated_row = self.fetch_document_by_id(int(document_row["id"]))
        self.assertEqual(updated_row["source_text_revision_id"], source_revision_id)
        self.assertEqual(updated_row["active_search_text_revision_id"], translated_revision_id)
        self.assertEqual(updated_row["active_text_source_kind"], "translation")

        revision_exit, revision_payload, _, _ = self.run_cli(
            "list-text-revisions",
            str(self.root),
            "--doc-id",
            str(document_row["id"]),
        )
        self.assertEqual(revision_exit, 0)
        self.assertIsNotNone(revision_payload)
        revisions_by_id = {int(item["id"]): item for item in revision_payload["text_revisions"]}
        self.assertTrue(revisions_by_id[translated_revision_id]["is_active_search_revision"])

        repeat_complete_exit, repeat_complete_payload, _, _ = self.run_cli(
            "complete-run-item",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
            "--claimed-by",
            "translator-ru",
            "--raw-output-json",
            json.dumps(raw_output),
            "--normalized-output-json",
            json.dumps(normalized_output),
            "--created-text-revision-json",
            json.dumps(created_text_revision),
        )
        self.assertEqual(repeat_complete_exit, 0)
        self.assertIsNotNone(repeat_complete_payload)
        self.assertTrue(repeat_complete_payload["idempotent"])
        self.assertEqual(repeat_complete_payload["result"]["id"], result_payload["id"])

    def test_create_run_rejects_always_activation_for_structured_extraction(self) -> None:
        note_path = self.root / "contract-activation.txt"
        note_path.write_text("Contract text.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("contract-activation.txt")

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Extract Contract Metadata",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "extract_contract_metadata",
            "--instruction",
            "Extract metadata fields from the contract.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, stderr_text = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--doc-id",
            str(document_row["id"]),
            "--activation-policy",
            "always",
        )
        self.assertNotEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        self.assertIn("only supported", create_run_payload["error"])

    def test_translation_run_item_context_includes_execution_template(self) -> None:
        note_path = self.root / "translation.txt"
        note_path.write_text("Translate this sentence.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("translation.txt")

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Translate FR",
            "translation",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "translate_fr",
            "--instruction",
            "Translate to French and preserve meaning.",
            "--parameters-json",
            "{\"target_language\":\"fr\"}",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        self.assertEqual(create_version_payload["job_version"]["capability"], "text_translation")
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "translator-a",
            "--limit",
            "1",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        self.assertEqual(len(claim_payload["run_items"]), 1)
        run_item_id = int(claim_payload["run_items"][0]["id"])

        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        self.assertEqual(context_payload["context"]["job_version"]["capability"], "text_translation")
        self.assertEqual(context_payload["context"]["execution"]["capability"], "text_translation")
        self.assertEqual(context_payload["context"]["execution"]["target_language"], "fr")
        self.assertIn(
            "Translate the entire input text into fr.",
            context_payload["context"]["execution"]["task_prompt"],
        )
        self.assertEqual(
            context_payload["context"]["execution"]["completion_template"]["created_text_revision_json"]["language"],
            "fr",
        )
        self.assertEqual(
            context_payload["context"]["execution"]["completion_template"]["created_text_revision_json"]["revision_kind"],
            "translation",
        )

    def test_claim_complete_run_item_flow_is_idempotent(self) -> None:
        note_path = self.root / "contract.txt"
        note_path.write_text("Governing law is Delaware.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("contract.txt")

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Contract Metadata",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "contract_metadata",
            "governing_law",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "contract_metadata",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Extract the governing law field.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        self.assertEqual(create_version_payload["job_version"]["capability"], "text_structured")
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "worker-a",
            "--limit",
            "1",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        self.assertEqual(len(claim_payload["run_items"]), 1)
        run_item_id = int(claim_payload["run_items"][0]["id"])
        self.assertEqual(claim_payload["run_items"][0]["claimed_by"], "worker-a")

        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        self.assertEqual(context_payload["context"]["job_version"]["capability"], "text_structured")
        self.assertEqual(
            context_payload["context"]["input"]["inline_text"],
            "Governing law is Delaware.",
        )
        self.assertIn("governing_law", context_payload["context"]["response_schema"]["properties"])
        self.assertEqual(context_payload["context"]["execution"]["capability"], "text_structured")
        self.assertIn(
            "Return only a JSON object that matches response_schema exactly.",
            context_payload["context"]["execution"]["task_prompt"],
        )
        self.assertEqual(
            context_payload["context"]["execution"]["output_defaults"]["governing_law"],
            "",
        )
        self.assertEqual(
            context_payload["context"]["execution"]["completion_template"]["output_values_json"]["governing_law"],
            "<final governing_law value>",
        )

        completion_template = context_payload["context"]["execution"]["completion_template"]
        raw_output = dict(completion_template["raw_output_json"])
        raw_output["governing_law"] = "Delaware"
        normalized_output = dict(completion_template["normalized_output_json"])
        normalized_output["governing_law"] = "Delaware"
        output_values = dict(completion_template["output_values_json"])
        output_values["governing_law"] = "Delaware"

        complete_exit, complete_payload, _, _ = self.run_cli(
            "complete-run-item",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
            "--claimed-by",
            "worker-a",
            "--raw-output-json",
            json.dumps(raw_output),
            "--normalized-output-json",
            json.dumps(normalized_output),
            "--output-values-json",
            json.dumps(output_values),
        )
        self.assertEqual(complete_exit, 0)
        self.assertIsNotNone(complete_payload)
        self.assertFalse(complete_payload["idempotent"])
        self.assertEqual(complete_payload["run"]["status"], "completed")
        self.assertEqual(
            complete_payload["result"]["outputs"][0]["output_value"],
            "Delaware",
        )

        repeat_complete_exit, repeat_complete_payload, _, _ = self.run_cli(
            "complete-run-item",
            str(self.root),
            "--run-item-id",
            str(run_item_id),
            "--claimed-by",
            "worker-a",
            "--raw-output-json",
            json.dumps(raw_output),
            "--normalized-output-json",
            json.dumps(normalized_output),
            "--output-values-json",
            json.dumps(output_values),
        )
        self.assertEqual(repeat_complete_exit, 0)
        self.assertIsNotNone(repeat_complete_payload)
        self.assertTrue(repeat_complete_payload["idempotent"])
        self.assertEqual(repeat_complete_payload["result"]["id"], complete_payload["result"]["id"])

        status_exit, status_payload, _, _ = self.run_cli(
            "run-status",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(status_exit, 0)
        self.assertIsNotNone(status_payload)
        self.assertEqual(status_payload["run"]["status"], "completed")
        self.assertEqual(status_payload["run"]["run_item_counts"]["completed"], 1)

    def test_prepare_run_batch_returns_contexts_and_worker_hints(self) -> None:
        note_path = self.root / "batch-contract.txt"
        note_path.write_text("Governing law is New York.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("batch-contract.txt")

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Batch Contract Metadata",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "batch_contract_metadata",
            "governing_law",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "batch_contract_metadata",
            "--instruction",
            "Extract the governing law field.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "worker-loop",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)
        self.assertEqual(prepare_payload["worker"]["claimed_by"], "worker-loop")
        self.assertEqual(prepare_payload["worker"]["next_action"], "process_batch")
        self.assertEqual(prepare_payload["worker"]["recommended_execution_mode"], "inline")
        self.assertEqual(prepare_payload["worker"]["prepared_batch_size"], 1)
        self.assertEqual(len(prepare_payload["batch"]), 1)
        batch_entry = prepare_payload["batch"][0]
        self.assertEqual(batch_entry["run_item"]["claimed_by"], "worker-loop")
        self.assertEqual(batch_entry["context"]["job_version"]["capability"], "text_structured")
        self.assertEqual(
            batch_entry["context"]["input"]["inline_text"],
            "Governing law is New York.",
        )
        self.assertEqual(
            batch_entry["context"]["execution"]["completion_template"]["output_values_json"]["governing_law"],
            "<final governing_law value>",
        )

    def test_prepare_run_batch_registers_background_worker_and_tracks_task(self) -> None:
        note_path = self.root / "background-contract.txt"
        note_path.write_text("The governing law is Delaware.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("background-contract.txt")

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Background Contract Metadata",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "background_contract_metadata",
            "governing_law",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "background_contract_metadata",
            "--instruction",
            "Extract the governing law field.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "background-worker",
            "--launch-mode",
            "background",
            "--worker-task-id",
            "task-123",
            "--max-batches",
            "1",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)
        self.assertEqual(prepare_payload["worker"]["next_action"], "process_batch")
        self.assertEqual(prepare_payload["worker_record"]["launch_mode"], "background")
        self.assertEqual(prepare_payload["worker_record"]["worker_task_id"], "task-123")
        self.assertEqual(prepare_payload["worker_record"]["max_batches"], 1)
        self.assertEqual(prepare_payload["worker_record"]["batches_prepared"], 1)
        self.assertEqual(prepare_payload["worker_record"]["status"], "active")

        status_exit, status_payload, _, _ = self.run_cli(
            "run-status",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(status_exit, 0)
        self.assertIsNotNone(status_payload)
        self.assertEqual(status_payload["run"]["workers"][0]["claimed_by"], "background-worker")
        self.assertEqual(status_payload["run"]["workers"][0]["worker_task_id"], "task-123")
        self.assertEqual(status_payload["run"]["supervision"]["background_worker_count"], 1)

    def test_run_status_supervision_recommends_wakeup_and_bounded_worker_count(self) -> None:
        for index in range(12):
            (self.root / f"background-{index:02d}.txt").write_text(
                f"Document {index} governing law is Delaware.",
                encoding="utf-8",
            )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 12)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            document_rows = connection.execute("SELECT id FROM documents ORDER BY id ASC").fetchall()
        finally:
            connection.close()
        document_ids = [int(row["id"]) for row in document_rows]

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Long Background Contract Metadata",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "long_background_contract_metadata",
            "governing_law",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "long_background_contract_metadata",
            "--instruction",
            "Extract the governing law field.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_args = [
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id IN ({', '.join(str(document_id) for document_id in document_ids)})",
        ]
        create_run_exit, create_run_payload, _, _ = self.run_cli(*create_run_args)
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        status_exit, status_payload, _, _ = self.run_cli(
            "run-status",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(status_exit, 0)
        self.assertIsNotNone(status_payload)
        supervision = status_payload["run"]["supervision"]
        self.assertEqual(supervision["recommended_action"], "spawn_background_worker")
        self.assertTrue(supervision["continuation_needed"])
        self.assertTrue(supervision["should_schedule_wakeup"])
        self.assertEqual(supervision["wake_interval_seconds"], 60)
        self.assertEqual(supervision["wakeup_reason"], "pending_work")
        self.assertEqual(supervision["max_parallel_workers"], 4)
        self.assertEqual(supervision["suggested_worker_count"], 3)
        self.assertEqual(supervision["spawn_additional_worker_count"], 3)

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "background-supervisor-worker",
            "--launch-mode",
            "background",
            "--worker-task-id",
            "task-supervisor-1",
            "--max-batches",
            "1",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)

        after_status_exit, after_status_payload, _, _ = self.run_cli(
            "run-status",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(after_status_exit, 0)
        self.assertIsNotNone(after_status_payload)
        after_supervision = after_status_payload["run"]["supervision"]
        self.assertEqual(after_supervision["background_worker_count"], 1)
        self.assertTrue(after_supervision["should_schedule_wakeup"])
        self.assertEqual(after_supervision["wake_interval_seconds"], 60)
        self.assertEqual(after_supervision["wakeup_reason"], "workers_active")
        self.assertGreaterEqual(after_supervision["spawn_additional_worker_count"], 1)
        self.assertLessEqual(after_supervision["spawn_additional_worker_count"], 3)

    def test_prepare_run_batch_handoffs_after_worker_batch_budget(self) -> None:
        (self.root / "handoff-a.txt").write_text("Document A governing law is New York.", encoding="utf-8")
        (self.root / "handoff-b.txt").write_text("Document B governing law is California.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        first_doc = self.fetch_document_row("handoff-a.txt")
        second_doc = self.fetch_document_row("handoff-b.txt")

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Handoff Test", "structured_extraction")
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "handoff_test",
            "governing_law",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "handoff_test",
            "--instruction",
            "Extract the governing law field.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id IN ({first_doc['id']}, {second_doc['id']})",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "handoff-worker",
            "--launch-mode",
            "background",
            "--worker-task-id",
            "task-handoff",
            "--max-batches",
            "1",
            "--limit",
            "1",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)
        self.assertEqual(len(prepare_payload["batch"]), 1)

        batch_entry = prepare_payload["batch"][0]
        complete_exit, complete_payload, _, _ = self.run_cli(
            "complete-run-item",
            str(self.root),
            "--run-item-id",
            str(batch_entry["run_item"]["id"]),
            "--claimed-by",
            "handoff-worker",
            "--raw-output-json",
            json.dumps({"governing_law": "New York"}),
            "--normalized-output-json",
            json.dumps({"governing_law": "New York"}),
            "--output-values-json",
            json.dumps({"governing_law": "New York"}),
        )
        self.assertEqual(complete_exit, 0)
        self.assertIsNotNone(complete_payload)

        handoff_exit, handoff_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "handoff-worker",
            "--launch-mode",
            "background",
            "--worker-task-id",
            "task-handoff",
            "--max-batches",
            "1",
        )
        self.assertEqual(handoff_exit, 0)
        self.assertIsNotNone(handoff_payload)
        self.assertEqual(handoff_payload["batch"], [])
        self.assertEqual(handoff_payload["worker"]["next_action"], "handoff")
        self.assertEqual(handoff_payload["worker"]["stop_reason"], "max_batches_reached")
        self.assertTrue(handoff_payload["worker"]["should_exit_after_batch"])

        finish_exit, finish_payload, _, _ = self.run_cli(
            "finish-run-worker",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "handoff-worker",
            "--worker-status",
            "stopped",
            "--summary-json",
            json.dumps({"reason": "handoff", "processed": 1}),
        )
        self.assertEqual(finish_exit, 0)
        self.assertIsNotNone(finish_payload)
        self.assertEqual(finish_payload["worker"]["status"], "stopped")
        self.assertEqual(finish_payload["worker"]["summary"]["reason"], "handoff")

        status_exit, status_payload, _, _ = self.run_cli(
            "run-status",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(status_exit, 0)
        self.assertIsNotNone(status_payload)
        self.assertEqual(status_payload["run"]["supervision"]["active_worker_count"], 0)
        self.assertTrue(status_payload["run"]["supervision"]["continuation_needed"])

    def test_claim_run_items_reclaims_stale_running_items(self) -> None:
        note_path = self.root / "stale.txt"
        note_path.write_text("Counterparty is Acme.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("stale.txt")

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Counterparty", "structured_extraction")
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "counterparty",
            "counterparty_name",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "counterparty",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Extract the counterparty.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        first_claim_exit, first_claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "worker-a",
            "--launch-mode",
            "background",
            "--worker-task-id",
            "task-stale-a",
            "--limit",
            "1",
        )
        self.assertEqual(first_claim_exit, 0)
        self.assertIsNotNone(first_claim_payload)
        run_item_id = int(first_claim_payload["run_items"][0]["id"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            connection.execute(
                "UPDATE run_items SET last_heartbeat_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00Z", run_item_id),
            )
            connection.commit()
        finally:
            connection.close()

        second_claim_exit, second_claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "worker-b",
            "--launch-mode",
            "background",
            "--worker-task-id",
            "task-stale-b",
            "--limit",
            "1",
            "--stale-seconds",
            "60",
        )
        self.assertEqual(second_claim_exit, 0)
        self.assertIsNotNone(second_claim_payload)
        self.assertEqual(len(second_claim_payload["run_items"]), 1)
        self.assertEqual(int(second_claim_payload["run_items"][0]["id"]), run_item_id)
        self.assertEqual(second_claim_payload["run_items"][0]["claimed_by"], "worker-b")

        status_exit, status_payload, _, _ = self.run_cli(
            "run-status",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(status_exit, 0)
        self.assertIsNotNone(status_payload)
        workers_by_claimed_by = {
            worker_payload["claimed_by"]: worker_payload for worker_payload in status_payload["run"]["workers"]
        }
        self.assertEqual(workers_by_claimed_by["worker-a"]["status"], "orphaned")
        self.assertEqual(workers_by_claimed_by["worker-b"]["status"], "active")

    def test_cancel_run_skips_pending_items_and_blocks_new_claims(self) -> None:
        (self.root / "a.txt").write_text("Alpha text.", encoding="utf-8")
        (self.root / "b.txt").write_text("Beta text.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        first_doc = self.fetch_document_row("a.txt")
        second_doc = self.fetch_document_row("b.txt")

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Cancel Test", "structured_extraction")
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "cancel_test",
            "summary_text",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "cancel_test",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Summarize the document.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id IN ({first_doc['id']}, {second_doc['id']})",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "worker-a",
            "--limit",
            "1",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        self.assertEqual(len(claim_payload["run_items"]), 1)

        cancel_exit, cancel_payload, _, _ = self.run_cli(
            "cancel-run",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(cancel_exit, 0)
        self.assertIsNotNone(cancel_payload)
        self.assertEqual(cancel_payload["run"]["status"], "canceled")
        self.assertEqual(cancel_payload["canceled_pending_items"], 1)

        post_cancel_claim_exit, post_cancel_claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "worker-b",
            "--limit",
            "1",
        )
        self.assertEqual(post_cancel_claim_exit, 0)
        self.assertIsNotNone(post_cancel_claim_payload)
        self.assertEqual(post_cancel_claim_payload["run"]["status"], "canceled")
        self.assertEqual(post_cancel_claim_payload["run_items"], [])

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "worker-c",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)
        self.assertEqual(prepare_payload["batch"], [])
        self.assertEqual(prepare_payload["worker"]["next_action"], "stop")
        self.assertEqual(prepare_payload["worker"]["stop_reason"], "canceled")

    def test_cancel_run_force_returns_background_worker_task_ids(self) -> None:
        note_path = self.root / "force-cancel.txt"
        note_path.write_text("Summary this text.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("force-cancel.txt")

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Force Cancel", "structured_extraction")
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "force_cancel",
            "summary_text",
            "--value-type",
            "text",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "force_cancel",
            "--instruction",
            "Summarize the document.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "force-worker",
            "--launch-mode",
            "background",
            "--worker-task-id",
            "task-force-123",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)

        cancel_exit, cancel_payload, _, _ = self.run_cli(
            "cancel-run",
            str(self.root),
            "--run-id",
            str(run_id),
            "--force",
        )
        self.assertEqual(cancel_exit, 0)
        self.assertIsNotNone(cancel_payload)
        self.assertTrue(cancel_payload["force_stop_requested"])
        self.assertEqual(cancel_payload["force_stop_task_ids"], ["task-force-123"])
        self.assertEqual(cancel_payload["run"]["status"], "canceled")
        self.assertEqual(cancel_payload["run"]["workers"][0]["status"], "canceled")
        self.assertIsNotNone(cancel_payload["run"]["workers"][0]["cancel_requested_at"])
        self.assertEqual(cancel_payload["run"]["supervision"]["force_stop_task_ids"], ["task-force-123"])

    def test_ocr_page_run_items_finalize_into_document_result(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )
        self.assertEqual(image_only_row["text_status"], "empty")

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Production OCR", "ocr")
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "production_ocr",
            "--instruction",
            "OCR each page image and preserve reading order.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        self.assertEqual(create_version_payload["job_version"]["capability"], "vision_ocr")
        self.assertEqual(create_version_payload["job_version"]["input_basis"], "source_parts")
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {image_only_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "ocr-worker",
            "--limit",
            "10",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        claimed_items = claim_payload["run_items"]
        self.assertEqual(len(claimed_items), 2)
        self.assertTrue(all(item["item_kind"] == "page" for item in claimed_items))
        self.assertEqual([item["page_number"] for item in claimed_items], [1, 2])

        first_item_id = int(claimed_items[0]["id"])
        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(first_item_id),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        self.assertEqual(context_payload["context"]["input"]["kind"], "ocr_page_image")
        self.assertEqual(context_payload["context"]["input"]["page_number"], 1)
        self.assertTrue(context_payload["context"]["input"]["artifact_path"].endswith("PDX000005.tif"))

        for item in claimed_items:
            page_number = int(item["page_number"])
            complete_exit, complete_payload, _, _ = self.run_cli(
                "complete-run-item",
                str(self.root),
                "--run-item-id",
                str(item["id"]),
                "--claimed-by",
                "ocr-worker",
                "--page-text",
                f"OCR page {page_number}",
            )
            self.assertEqual(complete_exit, 0)
            self.assertIsNotNone(complete_payload)
            self.assertIn("ocr_page_output", complete_payload)
            self.assertEqual(complete_payload["ocr_page_output"]["page_number"], page_number)
            self.assertEqual(complete_payload["ocr_page_output"]["text_content"], f"OCR page {page_number}")

        finalize_exit, finalize_payload, _, _ = self.run_cli(
            "finalize-ocr-run",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(finalize_exit, 0)
        self.assertIsNotNone(finalize_payload)
        self.assertEqual(finalize_payload["run"]["status"], "completed")
        self.assertEqual(len(finalize_payload["results"]), 1)
        result_payload = finalize_payload["results"][0]
        self.assertIsNotNone(result_payload["created_text_revision_id"])

        revisions_exit, revisions_payload, _, _ = self.run_cli(
            "list-text-revisions",
            str(self.root),
            "--doc-id",
            str(image_only_row["id"]),
        )
        self.assertEqual(revisions_exit, 0)
        self.assertIsNotNone(revisions_payload)
        revisions_by_id = {int(item["id"]): item for item in revisions_payload["text_revisions"]}
        ocr_revision = revisions_by_id[int(result_payload["created_text_revision_id"])]
        self.assertEqual(ocr_revision["revision_kind"], "ocr")
        self.assertFalse(ocr_revision["is_active_search_revision"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            text_revision_row = connection.execute(
                "SELECT * FROM text_revisions WHERE id = ?",
                (result_payload["created_text_revision_id"],),
            ).fetchone()
            self.assertIsNotNone(text_revision_row)
            merged_text = retriever_tools.read_text_revision_body(
                self.paths,
                text_revision_row["storage_rel_path"],
            )
            self.assertEqual(merged_text, "OCR page 1\n\nOCR page 2")

            page_item_rows = connection.execute(
                """
                SELECT page_number, result_id, status
                FROM run_items
                WHERE run_id = ?
                ORDER BY page_number ASC, id ASC
                """,
                (run_id,),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual([int(row["page_number"]) for row in page_item_rows], [1, 2])
        self.assertTrue(all(str(row["status"]) == "completed" for row in page_item_rows))
        self.assertTrue(all(int(row["result_id"]) == int(result_payload["id"]) for row in page_item_rows))

    def test_prepare_run_batch_requests_ocr_finalization_after_completed_pages(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Queued OCR", "ocr")
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "queued_ocr",
            "--instruction",
            "OCR each page image.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {image_only_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "ocr-loop",
            "--limit",
            "10",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)
        self.assertEqual(prepare_payload["worker"]["next_action"], "process_batch")
        self.assertEqual(len(prepare_payload["batch"]), 2)

        for batch_entry in prepare_payload["batch"]:
            page_number = int(batch_entry["run_item"]["page_number"])
            complete_exit, complete_payload, _, _ = self.run_cli(
                "complete-run-item",
                str(self.root),
                "--run-item-id",
                str(batch_entry["run_item"]["id"]),
                "--claimed-by",
                "ocr-loop",
                "--page-text",
                f"OCR batch page {page_number}",
            )
            self.assertEqual(complete_exit, 0)
            self.assertIsNotNone(complete_payload)

        final_prepare_exit, final_prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "ocr-loop",
        )
        self.assertEqual(final_prepare_exit, 0)
        self.assertIsNotNone(final_prepare_payload)
        self.assertEqual(final_prepare_payload["batch"], [])
        self.assertTrue(final_prepare_payload["worker"]["needs_ocr_finalization"])
        self.assertEqual(final_prepare_payload["worker"]["next_action"], "finalize_ocr")

    def test_finalize_ocr_run_with_always_activation_promotes_revision(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Queued OCR Auto Activate", "ocr")
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "queued_ocr_auto_activate",
            "--instruction",
            "OCR each page image.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--doc-id",
            str(image_only_row["id"]),
            "--activation-policy",
            "always",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        self.assertEqual(create_run_payload["run"]["activation_policy"], "always")
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "ocr-activate-worker",
            "--limit",
            "10",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        for item in claim_payload["run_items"]:
            page_number = int(item["page_number"])
            complete_exit, complete_payload, _, _ = self.run_cli(
                "complete-run-item",
                str(self.root),
                "--run-item-id",
                str(item["id"]),
                "--claimed-by",
                "ocr-activate-worker",
                "--page-text",
                f"OCR auto page {page_number}",
            )
            self.assertEqual(complete_exit, 0)
            self.assertIsNotNone(complete_payload)

        finalize_exit, finalize_payload, _, _ = self.run_cli(
            "finalize-ocr-run",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(finalize_exit, 0)
        self.assertIsNotNone(finalize_payload)
        self.assertEqual(len(finalize_payload["activations"]), 1)
        result_payload = finalize_payload["results"][0]
        created_revision_id = int(result_payload["created_text_revision_id"])
        self.assertEqual(finalize_payload["activations"][0]["text_revision"]["id"], created_revision_id)
        self.assertEqual(finalize_payload["activations"][0]["activation_policy"], "always")

        updated_row = self.fetch_document_by_id(int(image_only_row["id"]))
        self.assertEqual(updated_row["active_search_text_revision_id"], created_revision_id)
        self.assertEqual(updated_row["active_text_source_kind"], "ocr")

    def test_image_description_page_run_items_finalize_into_document_result(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )
        self.assertEqual(image_only_row["text_status"], "empty")

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Production Image Description",
            "image_description",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "production_image_description",
            "--instruction",
            "Describe each page so image-only productions become searchable.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        self.assertEqual(create_version_payload["job_version"]["capability"], "vision_description")
        self.assertEqual(create_version_payload["job_version"]["input_basis"], "source_parts")
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {image_only_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "image-description-worker",
            "--limit",
            "10",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        claimed_items = claim_payload["run_items"]
        self.assertEqual(len(claimed_items), 2)
        self.assertTrue(all(item["item_kind"] == "page" for item in claimed_items))

        first_item_id = int(claimed_items[0]["id"])
        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(first_item_id),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        self.assertEqual(context_payload["context"]["input"]["kind"], "image_description_page_image")
        self.assertEqual(context_payload["context"]["execution"]["capability"], "vision_description")

        for item in claimed_items:
            page_number = int(item["page_number"])
            complete_exit, complete_payload, _, _ = self.run_cli(
                "complete-run-item",
                str(self.root),
                "--run-item-id",
                str(item["id"]),
                "--claimed-by",
                "image-description-worker",
                "--page-text",
                f"Image description page {page_number}",
            )
            self.assertEqual(complete_exit, 0)
            self.assertIsNotNone(complete_payload)
            self.assertIn("image_description_page_output", complete_payload)
            self.assertEqual(
                complete_payload["image_description_page_output"]["text_content"],
                f"Image description page {page_number}",
            )

        finalize_exit, finalize_payload, _, _ = self.run_cli(
            "finalize-image-description-run",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(finalize_exit, 0)
        self.assertIsNotNone(finalize_payload)
        self.assertEqual(finalize_payload["run"]["status"], "completed")
        self.assertEqual(len(finalize_payload["results"]), 1)
        result_payload = finalize_payload["results"][0]
        self.assertIsNotNone(result_payload["created_text_revision_id"])

        revisions_exit, revisions_payload, _, _ = self.run_cli(
            "list-text-revisions",
            str(self.root),
            "--doc-id",
            str(image_only_row["id"]),
        )
        self.assertEqual(revisions_exit, 0)
        self.assertIsNotNone(revisions_payload)
        revisions_by_id = {int(item["id"]): item for item in revisions_payload["text_revisions"]}
        description_revision = revisions_by_id[int(result_payload["created_text_revision_id"])]
        self.assertEqual(description_revision["revision_kind"], "image_description")
        self.assertFalse(description_revision["is_active_search_revision"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            text_revision_row = connection.execute(
                "SELECT * FROM text_revisions WHERE id = ?",
                (result_payload["created_text_revision_id"],),
            ).fetchone()
            self.assertIsNotNone(text_revision_row)
            merged_text = retriever_tools.read_text_revision_body(
                self.paths,
                text_revision_row["storage_rel_path"],
            )
            self.assertEqual(
                merged_text,
                "[IMAGE DESCRIPTION - PAGE 1]\nImage description page 1\n\n"
                "[IMAGE DESCRIPTION - PAGE 2]\nImage description page 2",
            )
        finally:
            connection.close()

    def test_finalize_image_description_run_with_always_activation_promotes_revision(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Queued Image Description Auto Activate",
            "image_description",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "queued_image_description_auto_activate",
            "--instruction",
            "Describe each page image in search-friendly prose.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--doc-id",
            str(image_only_row["id"]),
            "--activation-policy",
            "always",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        self.assertEqual(create_run_payload["run"]["activation_policy"], "always")
        run_id = int(create_run_payload["run"]["id"])

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "image-description-activate-worker",
            "--limit",
            "10",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        for item in claim_payload["run_items"]:
            page_number = int(item["page_number"])
            complete_exit, complete_payload, _, _ = self.run_cli(
                "complete-run-item",
                str(self.root),
                "--run-item-id",
                str(item["id"]),
                "--claimed-by",
                "image-description-activate-worker",
                "--page-text",
                f"Image description auto page {page_number}",
            )
            self.assertEqual(complete_exit, 0)
            self.assertIsNotNone(complete_payload)

        finalize_exit, finalize_payload, _, _ = self.run_cli(
            "finalize-image-description-run",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(finalize_exit, 0)
        self.assertIsNotNone(finalize_payload)
        self.assertEqual(len(finalize_payload["activations"]), 1)
        result_payload = finalize_payload["results"][0]
        created_revision_id = int(result_payload["created_text_revision_id"])
        self.assertEqual(finalize_payload["activations"][0]["text_revision"]["id"], created_revision_id)
        self.assertEqual(finalize_payload["activations"][0]["activation_policy"], "always")

        updated_row = self.fetch_document_by_id(int(image_only_row["id"]))
        self.assertEqual(updated_row["active_search_text_revision_id"], created_revision_id)
        self.assertEqual(updated_row["active_text_source_kind"], "image_description")

    def test_prepare_run_batch_requests_image_description_finalization_after_completed_pages(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )

        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Queued Image Description",
            "image_description",
        )
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "queued_image_description",
            "--instruction",
            "Describe each page image in search-friendly prose.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {image_only_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        prepare_exit, prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "image-description-loop",
            "--limit",
            "10",
        )
        self.assertEqual(prepare_exit, 0)
        self.assertIsNotNone(prepare_payload)
        self.assertEqual(prepare_payload["worker"]["next_action"], "process_batch")
        self.assertEqual(len(prepare_payload["batch"]), 2)

        for batch_entry in prepare_payload["batch"]:
            page_number = int(batch_entry["run_item"]["page_number"])
            complete_exit, complete_payload, _, _ = self.run_cli(
                "complete-run-item",
                str(self.root),
                "--run-item-id",
                str(batch_entry["run_item"]["id"]),
                "--claimed-by",
                "image-description-loop",
                "--page-text",
                f"Batch image description page {page_number}",
            )
            self.assertEqual(complete_exit, 0)
            self.assertIsNotNone(complete_payload)

        final_prepare_exit, final_prepare_payload, _, _ = self.run_cli(
            "prepare-run-batch",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "image-description-loop",
        )
        self.assertEqual(final_prepare_exit, 0)
        self.assertIsNotNone(final_prepare_payload)
        self.assertEqual(final_prepare_payload["batch"], [])
        self.assertTrue(final_prepare_payload["worker"]["needs_image_description_finalization"])
        self.assertEqual(final_prepare_payload["worker"]["next_action"], "finalize_image_description")

    def test_ocr_source_parts_uses_native_pdf_parts_for_production_docs(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        native_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000004.logical"
        )

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Native PDF OCR", "ocr")
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "native_pdf_ocr",
            "--instruction",
            "OCR the native production PDF.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        self.assertEqual(create_version_payload["job_version"]["input_basis"], "source_parts")
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {native_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])
        self.assertEqual(create_run_payload["run"]["planned_count"], 1)

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(run_id),
            "--claimed-by",
            "native-ocr-worker",
            "--limit",
            "10",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        self.assertEqual(len(claim_payload["run_items"]), 1)
        run_item = claim_payload["run_items"][0]
        self.assertEqual(run_item["item_kind"], "page")
        self.assertEqual(run_item["page_number"], 1)
        self.assertTrue(
            str(run_item["input_artifact_rel_path"]).startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/jobs/ocr/run-{run_id}/doc-{native_row['id']}/page-0001"
            )
        )
        self.assertTrue(str(run_item["input_artifact_rel_path"]).endswith(".png"))

        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(run_item["id"]),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        self.assertEqual(context_payload["context"]["input"]["kind"], "ocr_page_image")
        self.assertEqual(context_payload["context"]["input"]["page_number"], 1)
        self.assertTrue(str(context_payload["context"]["input"]["artifact_rel_path"]).endswith(".png"))
        self.assertTrue(str(context_payload["context"]["input"]["artifact_path"]).endswith(".png"))

    def test_ocr_run_freezes_artifacts_at_create_run_and_reuses_them_from_prior_run(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            source_part_rows = connection.execute(
                """
                SELECT rel_source_path
                FROM document_source_parts
                WHERE document_id = ?
                  AND part_kind = 'image'
                ORDER BY ordinal ASC, id ASC
                """,
                (image_only_row["id"],),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(len(source_part_rows), 2)
        source_part_paths = [str(row["rel_source_path"]) for row in source_part_rows]

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Frozen OCR", "ocr")
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "frozen_ocr",
            "--instruction",
            "OCR the document pages.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        self.assertEqual(create_version_payload["job_version"]["input_basis"], "source_parts")
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {image_only_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        first_run_id = int(create_run_payload["run"]["id"])
        self.assertEqual(create_run_payload["run"]["planned_count"], 2)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            first_run_item_rows = connection.execute(
                """
                SELECT page_number, input_artifact_rel_path
                FROM run_items
                WHERE run_id = ?
                ORDER BY page_number ASC, id ASC
                """,
                (first_run_id,),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual([int(row["page_number"]) for row in first_run_item_rows], [1, 2])
        first_run_artifact_paths = [str(row["input_artifact_rel_path"]) for row in first_run_item_rows]
        self.assertTrue(
            all(
                path.startswith(
                    f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/jobs/ocr/run-{first_run_id}/doc-{image_only_row['id']}/"
                )
                for path in first_run_artifact_paths
            )
        )
        self.assertTrue(all(path not in source_part_paths for path in first_run_artifact_paths))

        self.write_tiff_fixture(self.root / source_part_paths[0], (17, 34, 51))

        claim_exit, claim_payload, _, _ = self.run_cli(
            "claim-run-items",
            str(self.root),
            "--run-id",
            str(first_run_id),
            "--claimed-by",
            "freeze-worker",
            "--limit",
            "10",
        )
        self.assertEqual(claim_exit, 0)
        self.assertIsNotNone(claim_payload)
        self.assertEqual(len(claim_payload["run_items"]), 2)
        first_run_item_id = int(claim_payload["run_items"][0]["id"])

        context_exit, context_payload, _, _ = self.run_cli(
            "get-run-item-context",
            str(self.root),
            "--run-item-id",
            str(first_run_item_id),
        )
        self.assertEqual(context_exit, 0)
        self.assertIsNotNone(context_payload)
        self.assertEqual(
            context_payload["context"]["input"]["artifact_rel_path"],
            first_run_artifact_paths[0],
        )

        second_run_exit, second_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--from-run-id",
            str(first_run_id),
        )
        self.assertEqual(second_run_exit, 0)
        self.assertIsNotNone(second_run_payload)
        second_run_id = int(second_run_payload["run"]["id"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            second_run_item_rows = connection.execute(
                """
                SELECT page_number, input_artifact_rel_path
                FROM run_items
                WHERE run_id = ?
                ORDER BY page_number ASC, id ASC
                """,
                (second_run_id,),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual([int(row["page_number"]) for row in second_run_item_rows], [1, 2])
        self.assertEqual(
            [str(row["input_artifact_rel_path"]) for row in second_run_item_rows],
            first_run_artifact_paths,
        )

    def test_execute_openai_structured_extraction_run_uses_provider_api(self) -> None:
        note_path = self.root / "party.txt"
        note_path.write_text("Counterparty is Acme Corp.", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        document_row = self.fetch_document_row("party.txt")

        add_field_exit, _, _, _ = self.run_cli("add-field", str(self.root), "counterparty_name", "text")
        self.assertEqual(add_field_exit, 0)
        create_job_exit, _, _, _ = self.run_cli(
            "create-job",
            str(self.root),
            "Counterparty Extract",
            "structured_extraction",
        )
        self.assertEqual(create_job_exit, 0)
        add_output_exit, _, _, _ = self.run_cli(
            "add-job-output",
            str(self.root),
            "counterparty_extract",
            "counterparty_name",
            "--value-type",
            "text",
            "--bind-custom-field",
            "counterparty_name",
        )
        self.assertEqual(add_output_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "counterparty_extract",
            "--provider",
            "openai_responses",
            "--model",
            "gpt-5.4",
            "--input-basis",
            "active_search_text",
            "--instruction",
            "Extract the counterparty.",
            "--parameters-json",
            "{\"temperature\":0}",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--filter",
            f"id = {document_row['id']}",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_id = int(create_run_payload["run"]["id"])

        fake_response = {
            "id": "resp_test_123",
            "status": "completed",
            "output_text": "{\"counterparty_name\":\"Acme Corp\"}",
            "usage": {"input_tokens": 17, "output_tokens": 9},
        }
        with mock.patch.object(retriever_tools, "call_openai_responses_api", return_value=fake_response):
            execute_run_exit, execute_run_payload, _, _ = self.run_cli(
                "execute-run",
                str(self.root),
                "--run-id",
                str(run_id),
            )
        self.assertEqual(execute_run_exit, 0)
        self.assertIsNotNone(execute_run_payload)
        self.assertEqual(execute_run_payload["run"]["status"], "completed")
        self.assertEqual(len(execute_run_payload["results"]), 1)
        result_payload = execute_run_payload["results"][0]
        outputs_by_name = {item["output_name"]: item for item in result_payload["outputs"]}
        self.assertEqual(outputs_by_name["counterparty_name"]["output_value"], "Acme Corp")

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            attempt_row = connection.execute(
                """
                SELECT *
                FROM attempts
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(attempt_row)
            self.assertEqual(attempt_row["provider_request_id"], "resp_test_123")
            self.assertEqual(attempt_row["input_tokens"], 17)
            self.assertEqual(attempt_row["output_tokens"], 9)
        finally:
            connection.close()

        publish_exit, publish_payload, _, _ = self.run_cli(
            "publish-run-results",
            str(self.root),
            "--run-id",
            str(run_id),
        )
        self.assertEqual(publish_exit, 0)
        self.assertIsNotNone(publish_payload)
        updated_row = self.fetch_document_by_id(int(document_row["id"]))
        self.assertEqual(updated_row["counterparty_name"], "Acme Corp")

    def test_create_run_select_from_scope_snapshots_current_scope(self) -> None:
        (self.root / "alpha.txt").write_text("scopealpha only\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("scopebeta only\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        alpha_row = self.fetch_document_row("alpha.txt")

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Scope Snapshot", "structured_extraction")
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "scope_snapshot",
            "--instruction",
            "Extract a stable value.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        scope_exit, scope_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "scopealpha")
        self.assertEqual(scope_exit, 0)
        self.assertIsNotNone(scope_payload)
        self.assertEqual(scope_payload["total_hits"], 1)

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--select-from-scope",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_payload = create_run_payload["run"]
        self.assertEqual(run_payload["planned_count"], 1)
        self.assertEqual(run_payload["selector"]["keyword"], "scopealpha")
        self.assertEqual([item["document_id"] for item in run_payload["documents"]], [alpha_row["id"]])

        shifted_scope_exit, shifted_scope_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "scopebeta")
        self.assertEqual(shifted_scope_exit, 0)
        self.assertIsNotNone(shifted_scope_payload)
        self.assertEqual(shifted_scope_payload["total_hits"], 1)

        stored_run_exit, stored_run_payload, _, _ = self.run_cli(
            "get-run",
            str(self.root),
            "--run-id",
            str(run_payload["id"]),
        )
        self.assertEqual(stored_run_exit, 0)
        self.assertIsNotNone(stored_run_payload)
        self.assertEqual(stored_run_payload["run"]["selector"]["keyword"], "scopealpha")
        self.assertEqual([item["document_id"] for item in stored_run_payload["run"]["documents"]], [alpha_row["id"]])

    def test_create_run_select_from_scope_and_narrows_with_explicit_dataset(self) -> None:
        (self.root / "alpha.txt").write_text("shared dataset body\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("shared dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        alpha_row = self.fetch_document_row("alpha.txt")
        beta_row = self.fetch_document_row("beta.txt")

        create_scope_dataset_exit, _, _, _ = self.run_cli("create-dataset", str(self.root), "Scope Set")
        self.assertEqual(create_scope_dataset_exit, 0)
        create_narrow_dataset_exit, _, _, _ = self.run_cli("create-dataset", str(self.root), "Narrow Set")
        self.assertEqual(create_narrow_dataset_exit, 0)

        add_scope_set_exit, _, _, _ = self.run_cli(
            "add-to-dataset",
            str(self.root),
            "--dataset-name",
            "Scope Set",
            "--doc-id",
            str(alpha_row["id"]),
            "--doc-id",
            str(beta_row["id"]),
        )
        self.assertEqual(add_scope_set_exit, 0)
        add_narrow_set_exit, _, _, _ = self.run_cli(
            "add-to-dataset",
            str(self.root),
            "--dataset-name",
            "Narrow Set",
            "--doc-id",
            str(beta_row["id"]),
        )
        self.assertEqual(add_narrow_set_exit, 0)

        create_job_exit, _, _, _ = self.run_cli("create-job", str(self.root), "Scope Dataset Run", "structured_extraction")
        self.assertEqual(create_job_exit, 0)
        create_version_exit, create_version_payload, _, _ = self.run_cli(
            "create-job-version",
            str(self.root),
            "scope_dataset_run",
            "--instruction",
            "Extract a stable value.",
        )
        self.assertEqual(create_version_exit, 0)
        self.assertIsNotNone(create_version_payload)
        job_version_id = int(create_version_payload["job_version"]["id"])

        scope_exit, scope_payload, _, _ = self.run_cli("slash", str(self.root), "/dataset", "Scope Set")
        self.assertEqual(scope_exit, 0)
        self.assertIsNotNone(scope_payload)
        self.assertEqual(scope_payload["total_hits"], 2)

        create_run_exit, create_run_payload, _, _ = self.run_cli(
            "create-run",
            str(self.root),
            "--job-version-id",
            str(job_version_id),
            "--select-from-scope",
            "--dataset",
            "Narrow Set",
        )
        self.assertEqual(create_run_exit, 0)
        self.assertIsNotNone(create_run_payload)
        run_payload = create_run_payload["run"]
        self.assertEqual(run_payload["planned_count"], 1)
        self.assertEqual([item["document_id"] for item in run_payload["documents"]], [beta_row["id"]])
        self.assertIn("all_of", run_payload["selector"])
        self.assertEqual(len(run_payload["selector"]["all_of"]), 2)
        self.assertEqual(run_payload["selector"]["all_of"][0]["dataset"][0]["name"], "Scope Set")
        self.assertEqual(run_payload["selector"]["all_of"][1]["dataset"][0]["name"], "Narrow Set")

    def test_filter_errors_match_search_create_run_and_export_archive(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        job_version_id = self.create_structured_extraction_job_version("Filter Parity")

        for expression in ("authr = 'Smith'", "is_attachment > 5"):
            with self.subTest(expression=expression):
                search_exit, search_payload, _, _ = self.run_cli(
                    "search",
                    str(self.root),
                    "--filter",
                    expression,
                )
                self.assertEqual(search_exit, 2)
                self.assertIsNotNone(search_payload)

                create_run_exit, create_run_payload, _, _ = self.run_cli(
                    "create-run",
                    str(self.root),
                    "--job-version-id",
                    str(job_version_id),
                    "--filter",
                    expression,
                )
                self.assertEqual(create_run_exit, 2)
                self.assertIsNotNone(create_run_payload)
                self.assertEqual(create_run_payload["error"], search_payload["error"])

                archive_exit, archive_payload, _, _ = self.run_cli(
                    "export-archive",
                    str(self.root),
                    "filter-error-parity.zip",
                    "--filter",
                    expression,
                )
                self.assertEqual(archive_exit, 2)
                self.assertIsNotNone(archive_payload)
                self.assertEqual(archive_payload["error"], search_payload["error"])

    def test_filter_surface_parity_matches_search_create_run_and_export_archive(self) -> None:
        corpus_rows = self.setup_randomized_sql_filter_corpus()
        job_version_id = self.create_structured_extraction_job_version("Surface Filter Parity")
        expressions = [
            "review_score BETWEEN 1 AND 7 AND NOT is_hot = FALSE",
            "(file_name IN ('alpha.txt', 'notes.txt') OR review_note LIKE '%review%') AND review_date IS NOT NULL",
            "review_score IS NULL OR review_note = 'beta''s note'",
            "NOT (review_note LIKE '%thread%' OR review_score > 8)",
            "file_name NOT LIKE '%eml' AND (review_score >= 5 OR is_hot IS NULL)",
        ]

        for index, expression in enumerate(expressions, start=1):
            expected_doc_ids = self.reference_doc_ids_for_sql_filter(corpus_rows, expression)

            with self.subTest(expression=expression):
                search_exit, search_payload, _, _ = self.run_cli(
                    "search",
                    str(self.root),
                    "--filter",
                    expression,
                )
                self.assertEqual(search_exit, 0)
                self.assertIsNotNone(search_payload)
                search_doc_ids = sorted(int(item["id"]) for item in search_payload["results"])

                create_run_exit, create_run_payload, _, _ = self.run_cli(
                    "create-run",
                    str(self.root),
                    "--job-version-id",
                    str(job_version_id),
                    "--filter",
                    expression,
                )
                self.assertEqual(create_run_exit, 0)
                self.assertIsNotNone(create_run_payload)
                create_run_doc_ids = sorted(
                    int(item["document_id"]) for item in create_run_payload["run"]["documents"]
                )

                archive_exit, archive_payload, _, _ = self.run_cli(
                    "export-archive",
                    str(self.root),
                    f"filter-surface-parity-{index}.zip",
                    "--filter",
                    expression,
                )
                self.assertEqual(archive_exit, 0)
                self.assertIsNotNone(archive_payload)
                archive_doc_ids = sorted(
                    int(item["document_id"]) for item in archive_payload["documents"]
                )

                self.assertEqual(search_doc_ids, expected_doc_ids)
                self.assertEqual(create_run_doc_ids, expected_doc_ids)
                self.assertEqual(archive_doc_ids, expected_doc_ids)

    def test_connect_db_falls_back_to_delete_when_wal_fails(self) -> None:
        class FakeCursor:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        class FakeConnection:
            def __init__(self) -> None:
                self.row_factory = None
                self.commands: list[str] = []

            def execute(self, statement: str):
                self.commands.append(statement)
                if statement == "PRAGMA journal_mode = WAL":
                    raise sqlite3.OperationalError("wal unsupported")
                if statement == "PRAGMA journal_mode = DELETE":
                    return FakeCursor(["delete"])
                return FakeCursor(None)

            def close(self) -> None:
                return None

        fake_connection = FakeConnection()
        with mock.patch.object(retriever_tools.sqlite3, "connect", return_value=fake_connection):
            connection = retriever_tools.connect_db(self.paths["db_path"])

        self.assertIs(connection, fake_connection)
        self.assertIn("PRAGMA journal_mode = WAL", fake_connection.commands)
        self.assertIn("PRAGMA journal_mode = DELETE", fake_connection.commands)

    def test_bootstrap_recovers_zero_byte_sqlite_artifacts(self) -> None:
        self.paths["db_path"].touch()
        Path(f"{self.paths['db_path']}-journal").write_text("stale\n", encoding="utf-8")

        result = retriever_tools.bootstrap(self.root)

        self.assertEqual(result["schema_version"], retriever_tools.SCHEMA_VERSION)
        self.assertIn("recovered_sqlite_artifacts", result)
        self.assertIn(str(self.paths["db_path"]), result["recovered_sqlite_artifacts"])
        self.assertGreater(self.paths["db_path"].stat().st_size, 0)

    def test_bootstrap_then_ingest_creates_email_attachment_children_and_search_context(self) -> None:
        self.create_legacy_documents_table(with_row=False)
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Upgrade test",
            body_text="Hello team,\nThis is the email body.",
            attachment_name="notes.txt",
            attachment_text="confidential attachment detail",
        )

        retriever_tools.bootstrap(self.root)
        result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(result["new"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["scanned_files"], 1)
        self.assertEqual(result["workspace_parent_documents"], 1)
        self.assertEqual(result["workspace_attachment_children"], 1)
        self.assertEqual(result["workspace_documents_total"], 2)

        parent_row = self.fetch_document_row("thread.eml")
        self.assertEqual(parent_row["content_type"], "Email")
        self.assertEqual(parent_row["control_number"], "DOC001.00000001")
        self.assertIsNotNone(parent_row["participants"])
        self.assertIn("alice@example.com", parent_row["participants"])
        self.assertIn("bob@example.com", parent_row["participants"])
        self.assertIn("carol@example.com", parent_row["participants"])

        child_rows = self.fetch_child_rows(parent_row["id"])
        self.assertEqual(len(child_rows), 1)
        child_row = child_rows[0]
        self.assertEqual(child_row["file_name"], "notes.txt")
        self.assertEqual(child_row["control_number"], "DOC001.00000001.001")
        self.assertEqual(child_row["parent_document_id"], parent_row["id"])
        self.assertTrue(
            str(child_row["rel_path"]).startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/thread.eml/attachments/"
            )
        )
        self.assertEqual(child_row["content_type"], "E-Doc")

        parent_search = retriever_tools.search(self.root, "Upgrade test", None, None, None, 1, 20)
        parent_result = next(item for item in parent_search["results"] if item["id"] == parent_row["id"])
        self.assertEqual(parent_result["control_number"], parent_row["control_number"])
        self.assertEqual(parent_result["attachment_count"], 1)
        self.assertEqual(parent_result["attachments"][0]["control_number"], child_row["control_number"])
        self.assertEqual(parent_result["attachments"][0]["file_name"], "notes.txt")

        attachment_search = retriever_tools.search(self.root, "confidential attachment detail", None, None, None, 1, 20)
        attachment_result = next(item for item in attachment_search["results"] if item["id"] == child_row["id"])
        self.assertEqual(attachment_result["control_number"], child_row["control_number"])
        self.assertEqual(attachment_result["parent"]["control_number"], parent_row["control_number"])
        self.assertEqual(attachment_result["parent"]["file_name"], "thread.eml")

        attachments_only = retriever_tools.search(
            self.root,
            "",
            [["is_attachment", "eq", "true"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(attachments_only["total_hits"], 1)
        self.assertEqual(attachments_only["results"][0]["id"], child_row["id"])

        parents_with_attachments = retriever_tools.search(
            self.root,
            "",
            [["has_attachments", "eq", "true"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(parents_with_attachments["total_hits"], 1)
        self.assertEqual(parents_with_attachments["results"][0]["id"], parent_row["id"])

    def test_ingest_email_preview_moves_attachment_lists_out_of_titles_and_into_links(self) -> None:
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Energy Balance Revenue Summary and Back Up Attachments: notes.txt; revenue.txt",
            body_text="Hello team,\nThis is the email body.",
            attachment_name="notes.txt",
            attachment_text="confidential attachment detail",
        )

        retriever_tools.bootstrap(self.root)
        result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(result["new"], 1)
        self.assertEqual(result["failed"], 0)

        parent_row = self.fetch_document_row("thread.eml")
        child_row = self.fetch_child_rows(parent_row["id"])[0]
        self.assertEqual(parent_row["title"], "Energy Balance Revenue Summary")
        self.assertEqual(parent_row["subject"], "Energy Balance Revenue Summary")

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            parent_preview_target = retriever_tools.default_preview_target(self.paths, parent_row, connection)
            child_preview_target = retriever_tools.default_preview_target(self.paths, child_row, connection)
            parent_preview_abs_path = str(
                parent_preview_target.get("file_abs_path") or parent_preview_target.get("abs_path") or ""
            ).split("#", 1)[0]
            child_preview_abs_path = str(
                child_preview_target.get("file_abs_path") or child_preview_target.get("abs_path") or ""
            ).split("#", 1)[0]
        finally:
            connection.close()

        parent_preview_path = Path(parent_preview_abs_path)
        preview_html = parent_preview_path.read_text(encoding="utf-8")
        expected_href = retriever_tools.urllib_request.pathname2url(
            os.path.relpath(child_preview_abs_path, start=str(parent_preview_path.parent))
        )

        self.assertIn("<title>Energy Balance Revenue Summary - All messages</title>", preview_html)
        self.assertIn("<h1>All messages</h1>", preview_html)
        self.assertIn("<h2>Energy Balance Revenue Summary</h2>", preview_html)
        self.assertNotIn("Back Up Attachments:", preview_html)
        self.assertIn("<h2>Attachments</h2>", preview_html)
        self.assertIn(">notes.txt<", preview_html)
        self.assertIn(f'href="{expected_href}"', preview_html)

        with mock.patch.object(retriever_tools, "pypff", object()):
            doctor_result = retriever_tools.doctor(self.root, quick=False)
        self.assertEqual(doctor_result["workspace_inventory"]["parent_documents"], 1)
        self.assertEqual(doctor_result["workspace_inventory"]["attachment_children"], 1)
        self.assertEqual(doctor_result["workspace_inventory"]["documents_total"], 2)
        self.assertIn(doctor_result["sqlite_journal_mode"], {"wal", "delete"})

        with self.assertRaises(retriever_tools.RetrieverError) as context:
            retriever_tools.search(self.root, "", None, "is_attachment", None, 1, 20)
        self.assertIn("virtual filter field", str(context.exception))

    def test_non_attachment_child_documents_are_not_treated_as_attachments(self) -> None:
        parent_path = self.root / "parent.txt"
        parent_path.write_text("parent body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        parent_row = self.fetch_document_row("parent.txt")
        self.assertIsNotNone(parent_row["dataset_id"])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            child_control_number = retriever_tools.format_control_number(
                int(parent_row["control_number_batch"]),
                int(parent_row["control_number_family_sequence"]),
                retriever_tools.next_attachment_sequence(connection, int(parent_row["id"])),
            )
            child_id = retriever_tools.upsert_document_row(
                connection,
                "_retriever/logical/slack/general/threads/1700000000.000001.slackthread",
                None,
                None,
                {
                    "text_content": "Root message\nReply one",
                    "content_type": "Chat",
                    "title": "general thread",
                    "participants": "Alice Example; Bob Example",
                    "text_status": "ok",
                    "page_count": None,
                    "author": None,
                    "custodian": None,
                    "date_created": "2026-04-14T10:00:00Z",
                    "date_modified": "2026-04-14T11:00:00Z",
                    "subject": None,
                    "recipients": None,
                },
                file_name="1700000000.000001.slackthread",
                parent_document_id=int(parent_row["id"]),
                child_document_kind=retriever_tools.CHILD_DOCUMENT_KIND_REPLY_THREAD,
                control_number=child_control_number,
                dataset_id=int(parent_row["dataset_id"]),
                control_number_batch=int(parent_row["control_number_batch"]),
                control_number_family_sequence=int(parent_row["control_number_family_sequence"]),
                control_number_attachment_sequence=1,
                source_kind=retriever_tools.SLACK_EXPORT_SOURCE_KIND,
                source_rel_path="data/slack/general/2026-04-14.json",
                source_item_id="1700000000.000001",
                root_message_key="general:1700000000.000001",
            )
            retriever_tools.ensure_dataset_document_membership(
                connection,
                dataset_id=int(parent_row["dataset_id"]),
                document_id=child_id,
                dataset_source_id=None,
            )
            connection.commit()
            inventory = retriever_tools.document_inventory_counts(connection)
        finally:
            connection.close()

        child_row = self.fetch_document_by_id(child_id)
        self.assertEqual(child_row["child_document_kind"], retriever_tools.CHILD_DOCUMENT_KIND_REPLY_THREAD)
        self.assertEqual(child_row["source_kind"], retriever_tools.SLACK_EXPORT_SOURCE_KIND)
        self.assertEqual(inventory["parent_documents"], 1)
        self.assertEqual(inventory["attachment_children"], 0)
        self.assertEqual(inventory["documents_total"], 2)

        browse_results = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        parent_result = next(item for item in browse_results["results"] if item["id"] == parent_row["id"])
        child_result = next(item for item in browse_results["results"] if item["id"] == child_id)
        self.assertEqual(parent_result["attachment_count"], 0)
        self.assertEqual(child_result["child_document_kind"], retriever_tools.CHILD_DOCUMENT_KIND_REPLY_THREAD)
        self.assertEqual(child_result["parent"]["control_number"], parent_row["control_number"])

        attachments_only = retriever_tools.search(
            self.root,
            "",
            [["is_attachment", "eq", "true"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(attachments_only["total_hits"], 0)

        parents_with_attachments = retriever_tools.search(
            self.root,
            "",
            [["has_attachments", "eq", "true"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(parents_with_attachments["total_hits"], 0)

    def test_doctor_fails_when_required_pst_backend_is_missing(self) -> None:
        retriever_tools.bootstrap(self.root)

        with mock.patch.object(retriever_tools, "pypff", None):
            doctor_result = retriever_tools.doctor(self.root, quick=True)

        self.assertEqual(doctor_result["overall"], "fail")
        self.assertEqual(doctor_result["pst_backend"]["status"], "fail")
        self.assertIn("libpff-python", doctor_result["pst_backend"]["detail"])

    def test_ingest_supports_ics_calendar_files(self) -> None:
        calendar_path = self.root / "invite.ics"
        calendar_path.write_text(
            "\n".join(
                [
                    "BEGIN:VCALENDAR",
                    "VERSION:2.0",
                    "BEGIN:VEVENT",
                    "SUMMARY:Board Meeting",
                    "DESCRIPTION:Discuss launch plans",
                    "END:VEVENT",
                    "END:VCALENDAR",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("invite.ics")
        self.assertEqual(row["content_type"], "Calendar")
        self.assertEqual(row["text_status"], "ok")

        search_result = retriever_tools.search(self.root, "Board Meeting", None, None, None, 1, 20)
        self.assertEqual(search_result["results"][0]["file_name"], "invite.ics")
        self.assertEqual(search_result["results"][0]["preview_rel_path"], "invite.ics")

    def test_ingest_supports_png_native_preview_only(self) -> None:
        image_path = self.root / "sample.png"
        image_path.write_bytes(
            bytes.fromhex(
                "89504E470D0A1A0A0000000D49484452000000010000000108060000001F15C489"
                "0000000D49444154789C6360000000020001E221BC330000000049454E44AE426082"
            )
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("sample.png")
        self.assertEqual(row["content_type"], "Image")
        self.assertEqual(row["text_status"], "empty")

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        png_result = next(item for item in browse_result["results"] if item["file_name"] == "sample.png")
        self.assertEqual(png_result["preview_rel_path"], "sample.png")
        self.assertEqual(png_result["preview_targets"][0]["preview_type"], "native")

    def test_ingest_supports_additional_native_preview_images(self) -> None:
        image_files = {
            "sample.bmp": b"bmp bytes",
            "sample.gif": b"gif bytes",
            "sample.jpeg": b"jpeg bytes",
            "sample.jpg": b"jpg bytes",
            "sample.tif": b"tif bytes",
            "sample.tiff": b"tiff bytes",
            "sample.webp": b"webp bytes",
        }
        for file_name, payload in image_files.items():
            (self.root / file_name).write_bytes(payload)

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], len(image_files))
        self.assertEqual(ingest_result["failed"], 0)

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 50)
        for file_name in image_files:
            row = self.fetch_document_row(file_name)
            self.assertEqual(row["content_type"], "Image")
            self.assertEqual(row["text_status"], "empty")
            result = next(item for item in browse_result["results"] if item["file_name"] == file_name)
            self.assertEqual(result["preview_rel_path"], file_name)
            self.assertEqual(result["preview_targets"][0]["preview_type"], "native")

    def test_ingest_supports_rtf_text_extraction(self) -> None:
        rtf_path = self.root / "memo.rtf"
        rtf_path.write_text(
            r"{\rtf1\ansi\deff0 {\fonttbl {\f0 Arial;}}\f0 Contract Review Memorandum\par Budget approved for Q2.\par}",
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("memo.rtf")
        self.assertEqual(row["content_type"], "E-Doc")
        self.assertEqual(row["text_status"], "ok")
        self.assertEqual(row["title"], "Contract Review Memorandum")

        search_result = retriever_tools.search(self.root, "Budget approved", None, None, None, 1, 20)
        self.assertEqual(search_result["results"][0]["file_name"], "memo.rtf")
        self.assertTrue(search_result["results"][0]["preview_rel_path"].endswith(".html"))
        self.assertEqual(search_result["results"][0]["preview_targets"][0]["preview_type"], "html")
        preview_html = Path(search_result["results"][0]["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("<title>Contract Review Memorandum</title>", preview_html)
        self.assertIn("<h1>Contract Review Memorandum</h1>", preview_html)

    def test_ingest_prefers_native_preview_for_chat_like_rtf_files(self) -> None:
        rtf_path = self.root / "chat-thread.rtf"
        rtf_path.write_text(
            (
                r"{\rtf1\ansi\deff0 {\fonttbl {\f0 Arial;}}\f0 "
                r"[2026-04-15 09:00] Alice Example: Kickoff thread for launch planning.\par "
                r"[2026-04-15 09:05] Bob Example: I'll draft the update.\par "
                r"[2026-04-15 09:07] Alice Example: Great, thanks.\par}"
            ),
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("chat-thread.rtf")
        self.assertEqual(row["content_type"], "Chat")

        search_result = retriever_tools.search(self.root, "draft the update", None, None, None, 1, 20)
        result = search_result["results"][0]
        self.assertEqual(result["preview_rel_path"], "chat-thread.rtf")
        self.assertEqual(result["preview_targets"][0]["preview_type"], "native")
        self.assertGreaterEqual(len(result["preview_targets"]), 2)
        self.assertEqual(result["preview_targets"][1]["preview_type"], "html")
        preview_html = Path(result["preview_targets"][1]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("<title>Kickoff thread for launch planning.</title>", preview_html)
        self.assertIn("<h1>Kickoff thread for launch planning.</h1>", preview_html)
        self.assertIn("Alice Example", preview_html)
        self.assertIn("Bob Example", preview_html)

    def test_ingest_supports_chat_transcript_metadata_for_text_files(self) -> None:
        transcript_path = self.root / "chat-thread.txt"
        transcript_path.write_text(
            "\n".join(
                [
                    "[2026-04-15 09:00] Alice Example: Kickoff thread for launch planning.",
                    "[2026-04-15 09:05] Bob Example: I'll draft the update.",
                    "[2026-04-15 09:07] Alice Example: Great, thanks.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("chat-thread.txt")
        self.assertEqual(row["content_type"], "Chat")
        self.assertIsNone(row["author"])
        self.assertEqual(row["participants"], "Alice Example, Bob Example")
        self.assertEqual(row["date_created"], "2026-04-15T09:00:00Z")
        self.assertEqual(row["date_modified"], "2026-04-15T09:07:00Z")
        self.assertEqual(row["title"], "Kickoff thread for launch planning.")
        self.assertIsNone(row["subject"])
        self.assertIsNone(row["recipients"])

        search_result = retriever_tools.search(self.root, "draft the update", None, None, None, 1, 20)
        result = search_result["results"][0]
        self.assertEqual(result["file_name"], "chat-thread.txt")
        self.assertTrue(result["preview_rel_path"].endswith(".html"))
        self.assertEqual(result["preview_targets"][0]["preview_type"], "html")
        preview_html = Path(result["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("<title>Kickoff thread for launch planning.</title>", preview_html)
        self.assertIn("<h1>Kickoff thread for launch planning.</h1>", preview_html)
        self.assertIn("Alice Example", preview_html)
        self.assertIn("Bob Example", preview_html)
        self.assertIn("Kickoff thread for launch planning.", preview_html)
        self.assertIn("Full transcript", preview_html)

    def test_ingest_renders_slack_json_as_named_chat_messages(self) -> None:
        users_path = self.root / "users.json"
        users_path.write_text(
            json.dumps(
                [
                    {
                        "id": "U04SERGEY1",
                        "name": "sergey",
                        "profile": {
                            "real_name": "Sergey Demyanov",
                            "display_name": "Sergey",
                            "first_name": "Sergey",
                            "color": "9f69e7",
                        },
                    },
                    {
                        "id": "U04MAX0001",
                        "name": "maksim",
                        "profile": {
                            "real_name": "Maksim Faleev",
                            "display_name": "Maksim",
                            "first_name": "Maksim",
                            "color": "2eb67d",
                        },
                    },
                ],
                ensure_ascii=True,
            ),
            encoding="utf-8",
        )
        channel_dir = self.root / "general"
        channel_dir.mkdir()
        slack_day_path = channel_dir / "2022-12-16.json"
        slack_day_path.write_text(
            json.dumps(
                [
                    {
                        "type": "message",
                        "text": "<@U04MAX0001> can we sync?",
                        "user": "U04SERGEY1",
                        "ts": "1671235434.237949",
                    },
                    {
                        "type": "message",
                        "text": "Let's sync at 10:05 :partying_face::christmas_tree:",
                        "user": "U04MAX0001",
                        "ts": "1671235734.237949",
                    },
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("general/2022-12-16.json")
        self.assertEqual(row["content_type"], "Chat")
        self.assertIsNone(row["author"])
        self.assertEqual(row["participants"], "Sergey Demyanov, Maksim Faleev")
        self.assertEqual(row["date_created"], "2022-12-17T00:03:54Z")
        self.assertEqual(row["date_modified"], "2022-12-17T00:08:54Z")
        self.assertEqual(row["title"], "#general - Dec 16, 2022")

        search_result = retriever_tools.search(self.root, "sync", None, None, None, 1, 20)
        result = next(item for item in search_result["results"] if item["file_name"] == "2022-12-16.json")
        self.assertTrue(result["preview_rel_path"].endswith(".html"))
        self.assertEqual(result["preview_targets"][0]["preview_type"], "html")
        preview_html = Path(result["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("Sergey Demyanov", preview_html)
        self.assertIn("Maksim Faleev", preview_html)
        self.assertIn("@Maksim can we sync?", preview_html)
        self.assertIn("sync at 10:05 🥳🎄", preview_html)
        self.assertIn("chat-avatar-svg", preview_html)
        self.assertIn('aria-label="Sergey Demyanov"', preview_html)
        self.assertIn("#general - Dec 16, 2022", preview_html)
        self.assertIn("Dec 17, 2022 12:03 AM UTC", preview_html)
        self.assertIn("[Dec 17, 2022 12:03 AM UTC]", preview_html)
        self.assertNotIn("<th>Author</th>", preview_html)
        self.assertNotIn("&quot;type&quot;", preview_html)

    def test_ingest_routes_slack_export_root_into_dedicated_dataset(self) -> None:
        export_root = self.root / "data" / "slack"
        export_root.mkdir(parents=True)
        (export_root / "users.json").write_text(
            json.dumps(
                [
                    {
                        "id": "U04SERGEY1",
                        "name": "sergey",
                        "profile": {
                            "real_name": "Sergey Demyanov",
                            "display_name": "Sergey",
                        },
                    },
                    {
                        "id": "U04MAX0001",
                        "name": "maksim",
                        "profile": {
                            "real_name": "Maksim Faleev",
                            "display_name": "Maksim",
                        },
                    },
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (export_root / "channels.json").write_text(
            json.dumps(
                [
                    {
                        "id": "C04GENERAL1",
                        "name": "general",
                        "is_channel": True,
                    }
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (export_root / "canvases.json").write_text("[]\n", encoding="utf-8")
        channel_dir = export_root / "general"
        channel_dir.mkdir()
        (channel_dir / "2022-12-16.json").write_text(
            json.dumps(
                [
                    {
                        "type": "message",
                        "text": "<@U04MAX0001> can we sync?",
                        "user": "U04SERGEY1",
                        "ts": "1671235434.237949",
                    },
                    {
                        "type": "message",
                        "text": "Let's sync at 10:05",
                        "user": "U04MAX0001",
                        "ts": "1671235734.237949",
                    },
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (self.root / "alpha.txt").write_text("ordinary workspace file\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["slack_exports_detected"], 1)
        self.assertEqual(ingest_result["slack_day_documents_scanned"], 1)
        self.assertEqual(ingest_result["slack_documents_created"], 1)

        day_row = self.fetch_document_row("data/slack/general/2022-12-16.json")
        self.assertEqual(day_row["source_kind"], retriever_tools.SLACK_EXPORT_SOURCE_KIND)
        self.assertIsNotNone(day_row["conversation_id"])
        self.assertIsNotNone(day_row["dataset_id"])
        self.assertEqual(day_row["title"], "#general - Dec 16, 2022")

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            metadata_rows = connection.execute(
                """
                SELECT rel_path
                FROM documents
                WHERE rel_path IN (?, ?, ?)
                ORDER BY rel_path ASC
                """,
                (
                    "data/slack/users.json",
                    "data/slack/channels.json",
                    "data/slack/canvases.json",
                ),
            ).fetchall()
            conversation_row = connection.execute(
                """
                SELECT c.conversation_key, c.conversation_type, c.display_name
                FROM conversations c
                JOIN documents d ON d.conversation_id = c.id
                WHERE d.rel_path = ?
                """,
                ("data/slack/general/2022-12-16.json",),
            ).fetchone()
        finally:
            connection.close()

        self.assertEqual(metadata_rows, [])
        self.assertIsNotNone(conversation_row)
        self.assertEqual(conversation_row["conversation_key"], "C04GENERAL1")
        self.assertEqual(conversation_row["conversation_type"], "public_channel")
        self.assertEqual(conversation_row["display_name"], "#general")

        dataset_payload = retriever_tools.list_datasets(self.root)
        slack_dataset = next(
            item for item in dataset_payload["datasets"] if item["source_kind"] == retriever_tools.SLACK_EXPORT_SOURCE_KIND
        )
        filesystem_dataset = next(
            item for item in dataset_payload["datasets"] if item["source_kind"] == retriever_tools.FILESYSTEM_SOURCE_KIND
        )
        self.assertEqual(slack_dataset["dataset_locator"], "data/slack")
        self.assertEqual(slack_dataset["document_count"], 1)
        self.assertEqual(filesystem_dataset["document_count"], 1)

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_result["total_hits"], 2)
        day_result = next(item for item in browse_result["results"] if item["id"] == day_row["id"])
        self.assertEqual(day_result["dataset_name"], slack_dataset["dataset_name"])
        self.assertEqual(day_result["conversation_id"], day_row["conversation_id"])
        self.assertTrue(
            day_result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(day_result["preview_rel_path"].endswith(f"#doc-{day_row['id']}"))

        search_result = retriever_tools.search(self.root, "sync", None, None, None, 1, 20)
        self.assertEqual(search_result["results"][0]["id"], day_row["id"])

    def test_ingest_creates_slack_reply_thread_child_documents(self) -> None:
        export_root = self.root / "data" / "slack"
        export_root.mkdir(parents=True)
        (export_root / "users.json").write_text(
            json.dumps(
                [
                    {
                        "id": "U04SERGEY1",
                        "name": "sergey",
                        "profile": {
                            "real_name": "Sergey Demyanov",
                            "display_name": "Sergey",
                        },
                    },
                    {
                        "id": "U04MAX0001",
                        "name": "maksim",
                        "profile": {
                            "real_name": "Maksim Faleev",
                            "display_name": "Maksim",
                        },
                    },
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (export_root / "channels.json").write_text(
            json.dumps(
                [
                    {
                        "id": "C04GENERAL1",
                        "name": "general",
                        "is_channel": True,
                    }
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        channel_dir = export_root / "general"
        channel_dir.mkdir()
        thread_ts = "1671235434.237949"
        (channel_dir / "2022-12-16.json").write_text(
            json.dumps(
                [
                    {
                        "type": "message",
                        "text": "Kickoff thread",
                        "user": "U04SERGEY1",
                        "ts": thread_ts,
                        "thread_ts": thread_ts,
                        "reply_count": 1,
                    },
                    {
                        "type": "message",
                        "text": "Standalone channel update",
                        "user": "U04MAX0001",
                        "ts": "1671235834.237949",
                    },
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (channel_dir / "2022-12-17.json").write_text(
            json.dumps(
                [
                    {
                        "type": "message",
                        "text": "Following up on kickoff",
                        "user": "U04MAX0001",
                        "ts": "1671321834.237949",
                        "thread_ts": thread_ts,
                    }
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 3)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["slack_exports_detected"], 1)
        self.assertEqual(ingest_result["slack_day_documents_scanned"], 2)
        self.assertEqual(ingest_result["slack_documents_created"], 3)

        day_one_row = self.fetch_document_row("data/slack/general/2022-12-16.json")
        day_two_row = self.fetch_document_row("data/slack/general/2022-12-17.json")
        child_rel_path = retriever_tools.slack_reply_thread_rel_path("C04GENERAL1", thread_ts)
        child_row = self.fetch_document_row(child_rel_path)

        self.assertEqual(child_row["parent_document_id"], day_one_row["id"])
        self.assertEqual(child_row["child_document_kind"], retriever_tools.CHILD_DOCUMENT_KIND_REPLY_THREAD)
        self.assertEqual(child_row["source_kind"], retriever_tools.SLACK_EXPORT_SOURCE_KIND)
        self.assertEqual(child_row["source_rel_path"], "data/slack/general/2022-12-16.json")
        self.assertEqual(child_row["source_item_id"], thread_ts)
        self.assertEqual(child_row["root_message_key"], f"C04GENERAL1:{thread_ts}")
        self.assertEqual(child_row["conversation_id"], day_one_row["conversation_id"])
        self.assertEqual(day_two_row["conversation_id"], day_one_row["conversation_id"])
        self.assertEqual(day_two_row["text_status"], "empty")

        reply_search = retriever_tools.search(self.root, "Following up on kickoff", None, None, None, 1, 20)
        self.assertEqual(reply_search["total_hits"], 1)
        self.assertEqual(reply_search["results"][0]["id"], child_row["id"])
        self.assertTrue(
            reply_search["results"][0]["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(reply_search["results"][0]["preview_rel_path"].endswith(f"#doc-{child_row['id']}"))

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        day_one_result = next(item for item in browse_result["results"] if item["id"] == day_one_row["id"])
        day_two_result = next(item for item in browse_result["results"] if item["id"] == day_two_row["id"])
        self.assertEqual(day_one_result["attachment_count"], 0)
        self.assertEqual(day_one_result["child_document_count"], 1)
        self.assertEqual(day_one_result["child_documents"][0]["id"], child_row["id"])
        self.assertEqual(day_one_result["child_documents"][0]["child_document_kind"], retriever_tools.CHILD_DOCUMENT_KIND_REPLY_THREAD)
        self.assertEqual(day_two_result["child_document_count"], 0)
        self.assertTrue(
            day_one_result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(day_one_result["preview_rel_path"].endswith(f"#doc-{day_one_row['id']}"))
        self.assertEqual(
            self.preview_target_file_path(day_one_result["preview_targets"][0]),
            self.preview_target_file_path(reply_search["results"][0]["preview_targets"][0]),
        )
        slack_preview_html = self.preview_target_file_path(day_one_result["preview_targets"][0]).read_text(encoding="utf-8")
        self.assertIn("Kickoff thread", slack_preview_html)
        self.assertIn("Following up on kickoff", slack_preview_html)
        self.assertFalse(
            (
                self.paths["state_dir"]
                / retriever_tools.preview_base_path_for_rel_path("data/slack/general/2022-12-16.json")
                / "2022-12-16.html"
            ).exists()
        )
        self.assertFalse(
            (
                self.paths["state_dir"]
                / retriever_tools.preview_base_path_for_rel_path(child_rel_path)
                / f"{Path(child_rel_path).stem}.html"
            ).exists()
        )

        dataset_payload = retriever_tools.list_datasets(self.root)
        slack_dataset = next(
            item for item in dataset_payload["datasets"] if item["source_kind"] == retriever_tools.SLACK_EXPORT_SOURCE_KIND
        )
        self.assertEqual(slack_dataset["document_count"], 3)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            source_parts = connection.execute(
                """
                SELECT part_kind, rel_source_path, ordinal
                FROM document_source_parts
                WHERE document_id = ?
                ORDER BY ordinal ASC, id ASC
                """,
                (child_row["id"],),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(
            [(row["part_kind"], row["rel_source_path"]) for row in source_parts],
            [
                ("slack_thread_root_day", "data/slack/general/2022-12-16.json"),
                ("slack_thread_reply_day", "data/slack/general/2022-12-17.json"),
            ],
        )

    def test_ingest_groups_loose_eml_messages_into_one_conversation(self) -> None:
        root_path = self.root / "root.eml"
        reply_path = self.root / "reply.eml"
        self.write_email_message(
            root_path,
            subject="Status Update",
            body_text="Root message body",
            message_id="<root@example.com>",
            date_created="Tue, 14 Apr 2026 10:00:00 +0000",
        )
        self.write_email_message(
            reply_path,
            subject="Re: Status Update",
            body_text="Reply message body",
            message_id="<reply@example.com>",
            in_reply_to="<root@example.com>",
            references="<root@example.com>",
            date_created="Tue, 14 Apr 2026 11:00:00 +0000",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["email_conversations"], 1)

        root_row = self.fetch_document_row("root.eml")
        reply_row = self.fetch_document_row("reply.eml")
        self.assertIsNotNone(root_row["conversation_id"])
        self.assertEqual(root_row["conversation_id"], reply_row["conversation_id"])
        self.assertEqual(root_row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)
        self.assertEqual(reply_row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            signal_rows = connection.execute(
                """
                SELECT document_id, message_id, in_reply_to, references_json, normalized_subject
                FROM document_email_threading
                ORDER BY document_id ASC
                """
            ).fetchall()
            conversation_row = connection.execute(
                """
                SELECT source_kind, source_locator, conversation_type, display_name
                FROM conversations
                WHERE id = ?
                """,
                (root_row["conversation_id"],),
            ).fetchone()
        finally:
            connection.close()

        self.assertEqual(len(signal_rows), 2)
        signals_by_message_id = {row["message_id"]: row for row in signal_rows}
        self.assertEqual(set(signals_by_message_id), {"root@example.com", "reply@example.com"})
        self.assertEqual(signals_by_message_id["root@example.com"]["in_reply_to"], None)
        self.assertEqual(json.loads(signals_by_message_id["root@example.com"]["references_json"]), [])
        self.assertEqual(signals_by_message_id["root@example.com"]["normalized_subject"], "status update")
        self.assertEqual(signals_by_message_id["reply@example.com"]["in_reply_to"], "root@example.com")
        self.assertEqual(json.loads(signals_by_message_id["reply@example.com"]["references_json"]), ["root@example.com"])
        self.assertEqual(signals_by_message_id["reply@example.com"]["normalized_subject"], "status update")
        self.assertEqual(conversation_row["source_kind"], retriever_tools.EMAIL_CONVERSATION_SOURCE_KIND)
        self.assertEqual(conversation_row["source_locator"], ".")
        self.assertEqual(conversation_row["conversation_type"], "email")
        self.assertEqual(conversation_row["display_name"], "Status Update")

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        root_result = next(item for item in browse_result["results"] if item["id"] == root_row["id"])
        search_result = retriever_tools.search(self.root, "Reply message body", None, None, None, 1, 20)
        reply_result = next(item for item in search_result["results"] if item["id"] == reply_row["id"])
        self.assertTrue(
            reply_result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(reply_result["preview_rel_path"].endswith(f"#doc-{reply_row['id']}"))
        self.assertEqual(reply_result["preview_target_fragment"], f"doc-{reply_row['id']}")
        self.assertEqual(len(reply_result["preview_targets"]), 2)
        self.assertEqual(reply_result["preview_targets"][1]["label"], "contents")
        self.assertEqual(
            self.preview_target_file_path(root_result["preview_targets"][0]),
            self.preview_target_file_path(reply_result["preview_targets"][0]),
        )
        segment_html = self.preview_target_file_path(reply_result["preview_targets"][0]).read_text(encoding="utf-8")
        toc_html = self.preview_target_file_path(reply_result["preview_targets"][1]).read_text(encoding="utf-8")
        self.assertIn(f'id="doc-{root_row["id"]}"', segment_html)
        self.assertIn(f'id="doc-{reply_row["id"]}"', segment_html)
        self.assertIn("Root message body", segment_html)
        self.assertIn("Reply message body", segment_html)
        self.assertIn("Status Update", toc_html)
        self.assertIn(f"#doc-{reply_row['id']}", toc_html)
        self.assertFalse(
            (self.paths["state_dir"] / retriever_tools.preview_base_path_for_rel_path("root.eml") / "root.eml.html").exists()
        )
        self.assertFalse(
            (self.paths["state_dir"] / retriever_tools.preview_base_path_for_rel_path("reply.eml") / "reply.eml.html").exists()
        )

    def test_manual_conversation_assignment_mode_prevents_root_overwrite_and_retargets_auto_reply(self) -> None:
        root_path = self.root / "root.eml"
        reply_path = self.root / "reply.eml"
        self.write_email_message(
            root_path,
            subject="Deal Review",
            body_text="Root message body",
            message_id="<deal-root@example.com>",
            date_created="Tue, 14 Apr 2026 10:00:00 +0000",
        )
        self.write_email_message(
            reply_path,
            subject="Re: Deal Review",
            body_text="Reply message body",
            message_id="<deal-reply@example.com>",
            in_reply_to="<deal-root@example.com>",
            references="<deal-root@example.com>",
            date_created="Tue, 14 Apr 2026 11:00:00 +0000",
        )

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(first_ingest["failed"], 0)

        root_row = self.fetch_document_row("root.eml")
        reply_row = self.fetch_document_row("reply.eml")

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            connection.execute("BEGIN")
            manual_conversation_id = retriever_tools.upsert_conversation_row(
                connection,
                source_kind=retriever_tools.EMAIL_CONVERSATION_SOURCE_KIND,
                source_locator=".",
                conversation_key="manual:deal-review",
                conversation_type="email",
                display_name="Deal Review (Manual)",
            )
            connection.execute(
                """
                UPDATE documents
                SET conversation_id = ?, conversation_assignment_mode = ?
                WHERE id = ?
                """,
                (
                    manual_conversation_id,
                    retriever_tools.CONVERSATION_ASSIGNMENT_MODE_MANUAL,
                    root_row["id"],
                ),
            )
            connection.commit()
        finally:
            connection.close()

        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(second_ingest["failed"], 0)

        updated_root = self.fetch_document_row("root.eml")
        updated_reply = self.fetch_document_row("reply.eml")
        self.assertEqual(updated_root["conversation_id"], manual_conversation_id)
        self.assertEqual(
            updated_root["conversation_assignment_mode"],
            retriever_tools.CONVERSATION_ASSIGNMENT_MODE_MANUAL,
        )
        self.assertEqual(updated_reply["conversation_id"], manual_conversation_id)
        self.assertEqual(
            updated_reply["conversation_assignment_mode"],
            retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO,
        )

    def test_split_and_clear_conversation_assignment_cli_for_loose_eml(self) -> None:
        root_path = self.root / "root.eml"
        reply_path = self.root / "reply.eml"
        self.write_email_message(
            root_path,
            subject="Status Update",
            body_text="Root message body",
            message_id="<root@example.com>",
            date_created="Tue, 14 Apr 2026 10:00:00 +0000",
        )
        self.write_email_message(
            reply_path,
            subject="Re: Status Update",
            body_text="Reply message body",
            message_id="<reply@example.com>",
            in_reply_to="<root@example.com>",
            references="<root@example.com>",
            date_created="Tue, 14 Apr 2026 11:00:00 +0000",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["failed"], 0)

        root_row = self.fetch_document_row("root.eml")
        reply_row = self.fetch_document_row("reply.eml")
        self.assertEqual(root_row["conversation_id"], reply_row["conversation_id"])

        split_exit, split_payload, _, _ = self.run_cli(
            "split-from-conversation",
            str(self.root),
            "--doc-id",
            str(reply_row["id"]),
        )
        self.assertEqual(split_exit, 0)
        self.assertIsNotNone(split_payload)
        self.assertEqual(split_payload["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_MANUAL)

        split_root_row = self.fetch_document_row("root.eml")
        split_reply_row = self.fetch_document_row("reply.eml")
        self.assertNotEqual(split_root_row["conversation_id"], split_reply_row["conversation_id"])
        self.assertEqual(split_root_row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)
        self.assertEqual(split_reply_row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_MANUAL)

        clear_exit, clear_payload, _, _ = self.run_cli(
            "clear-conversation-assignment",
            str(self.root),
            "--doc-id",
            str(reply_row["id"]),
        )
        self.assertEqual(clear_exit, 0)
        self.assertIsNotNone(clear_payload)
        self.assertEqual(clear_payload["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)

        cleared_root_row = self.fetch_document_row("root.eml")
        cleared_reply_row = self.fetch_document_row("reply.eml")
        self.assertEqual(cleared_root_row["conversation_id"], cleared_reply_row["conversation_id"])
        self.assertEqual(cleared_reply_row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)

    def test_merge_into_conversation_cli_retargets_auto_email_replies(self) -> None:
        root_path = self.root / "root.eml"
        reply_path = self.root / "reply.eml"
        target_path = self.root / "target.eml"
        self.write_email_message(
            root_path,
            subject="Deal Review",
            body_text="Root message body",
            message_id="<deal-root@example.com>",
            date_created="Tue, 14 Apr 2026 10:00:00 +0000",
        )
        self.write_email_message(
            reply_path,
            subject="Re: Deal Review",
            body_text="Reply message body",
            message_id="<deal-reply@example.com>",
            in_reply_to="<deal-root@example.com>",
            references="<deal-root@example.com>",
            date_created="Tue, 14 Apr 2026 11:00:00 +0000",
        )
        self.write_email_message(
            target_path,
            subject="Separate Matter",
            body_text="Target message body",
            message_id="<target@example.com>",
            date_created="Tue, 14 Apr 2026 12:00:00 +0000",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["failed"], 0)

        root_row = self.fetch_document_row("root.eml")
        reply_row = self.fetch_document_row("reply.eml")
        target_row = self.fetch_document_row("target.eml")
        self.assertNotEqual(root_row["conversation_id"], target_row["conversation_id"])
        self.assertEqual(root_row["conversation_id"], reply_row["conversation_id"])

        merge_exit, merge_payload, _, _ = self.run_cli(
            "merge-into-conversation",
            str(self.root),
            "--doc-id",
            str(root_row["id"]),
            "--target-doc-id",
            str(target_row["id"]),
        )
        self.assertEqual(merge_exit, 0)
        self.assertIsNotNone(merge_payload)
        self.assertEqual(merge_payload["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_MANUAL)

        updated_root = self.fetch_document_row("root.eml")
        updated_reply = self.fetch_document_row("reply.eml")
        updated_target = self.fetch_document_row("target.eml")
        self.assertEqual(updated_root["conversation_id"], updated_target["conversation_id"])
        self.assertEqual(updated_reply["conversation_id"], updated_target["conversation_id"])
        self.assertEqual(updated_root["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_MANUAL)
        self.assertEqual(updated_reply["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)

    def test_ingest_mbox_groups_threaded_messages_into_one_conversation(self) -> None:
        root_message = self.build_fake_mbox_message(
            subject="Budget Review",
            body_text="Budget root body",
            message_id="<mbox-budget-root@example.com>",
            date_created="Tue, 14 Apr 2026 10:00:00 +0000",
        )
        reply_message = self.build_fake_mbox_message(
            subject="Re: Budget Review",
            body_text="Budget reply body",
            message_id="<mbox-budget-reply@example.com>",
            date_created="Tue, 14 Apr 2026 11:00:00 +0000",
        )
        reply_message["In-Reply-To"] = "<mbox-budget-root@example.com>"
        reply_message["References"] = "<mbox-budget-root@example.com>"
        self.write_fake_mbox_file([root_message, reply_message])

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["failed"], 0)

        root_row = self.fetch_document_row(
            retriever_tools.mbox_message_rel_path("mailbox.mbox", "<mbox-budget-root@example.com>")
        )
        reply_row = self.fetch_document_row(
            retriever_tools.mbox_message_rel_path("mailbox.mbox", "<mbox-budget-reply@example.com>")
        )
        self.assertIsNotNone(root_row["conversation_id"])
        self.assertEqual(root_row["conversation_id"], reply_row["conversation_id"])

    def test_ingest_pst_email_groups_threaded_messages_from_transport_headers(self) -> None:
        self.write_fake_pst_file()
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-email-root",
                subject="Witness Prep",
                body_text="PST root body",
                date_created="2026-04-14T10:00:00Z",
                transport_headers="\n".join(
                    [
                        "Message-ID: <pst-root@example.com>",
                        "Conversation-Topic: Witness Prep",
                        "",
                    ]
                ),
                conversation_topic="Witness Prep",
            ),
            self.build_fake_pst_message(
                source_item_id="pst-email-reply",
                subject="Re: Witness Prep",
                body_text="PST reply body",
                date_created="2026-04-14T11:00:00Z",
                transport_headers="\n".join(
                    [
                        "Message-ID: <pst-reply@example.com>",
                        "In-Reply-To: <pst-root@example.com>",
                        "References: <pst-root@example.com>",
                        "Conversation-Topic: Witness Prep",
                        "",
                    ]
                ),
                conversation_topic="Witness Prep",
            ),
        ]

        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            retriever_tools.bootstrap(self.root)
            ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["failed"], 0)
        root_row = self.fetch_document_row(retriever_tools.pst_message_rel_path("mailbox.pst", "pst-email-root"))
        reply_row = self.fetch_document_row(retriever_tools.pst_message_rel_path("mailbox.pst", "pst-email-reply"))
        self.assertIsNotNone(root_row["conversation_id"])
        self.assertEqual(root_row["conversation_id"], reply_row["conversation_id"])

    def test_search_parser_accepts_alias_flags_for_paging_and_sorting(self) -> None:
        parser = retriever_tools.build_parser()
        args = parser.parse_args(
            [
                "search",
                str(self.root),
                "",
                "--limit",
                "10",
                "--sort-by",
                "created_date",
                "--sort-order",
                "desc",
            ]
        )

        self.assertEqual(args.command, "search")
        self.assertEqual(args.per_page, 10)
        self.assertEqual(args.sort, "created_date")
        self.assertEqual(args.order, "desc")

    def test_search_accepts_created_date_sort_alias(self) -> None:
        first_path = self.root / "chat-older.txt"
        first_path.write_text(
            "\n".join(
                [
                    "[2026-04-14 09:00] Alice Example: Earlier thread.",
                    "[2026-04-14 09:05] Bob Example: Copy that.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        second_path = self.root / "chat-newer.txt"
        second_path.write_text(
            "\n".join(
                [
                    "[2026-04-15 09:00] Alice Example: Later thread.",
                    "[2026-04-15 09:05] Bob Example: Drafting now.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)
        self.assertEqual(ingest_result["failed"], 0)

        search_result = retriever_tools.search(self.root, "", None, "created_date", "desc", 1, 10)
        self.assertEqual(search_result["sort"], "date_created")
        self.assertEqual(search_result["order"], "desc")
        self.assertEqual(search_result["results"][0]["file_name"], "chat-newer.txt")
        self.assertEqual(search_result["results"][1]["file_name"], "chat-older.txt")

    def test_dataset_cli_commands_manage_manual_dataset_membership(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("sample dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")

        exit_code, create_payload, _, _ = self.run_cli("create-dataset", str(self.root), "Review Set")
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(create_payload)
        dataset_id = int(create_payload["dataset"]["id"])
        self.assertEqual(create_payload["dataset"]["dataset_name"], "Review Set")
        self.assertEqual(create_payload["dataset"]["source_kind"], retriever_tools.MANUAL_DATASET_SOURCE_KIND)

        exit_code, add_payload, _, _ = self.run_cli(
            "add-to-dataset",
            str(self.root),
            "--dataset-id",
            str(dataset_id),
            "--doc-id",
            str(row["id"]),
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(add_payload)
        self.assertEqual(add_payload["added_document_ids"], [row["id"]])
        self.assertEqual(add_payload["already_present_document_ids"], [])

        dataset_filtered = retriever_tools.search(
            self.root,
            "",
            [["dataset_name", "eq", "Review Set"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(dataset_filtered["total_hits"], 1)
        self.assertEqual(dataset_filtered["results"][0]["id"], row["id"])

        exit_code, list_payload, _, _ = self.run_cli("list-datasets", str(self.root))
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(list_payload)
        dataset_names = [item["dataset_name"] for item in list_payload["datasets"]]
        self.assertIn("Review Set", dataset_names)
        self.assertIn(self.root.name, dataset_names)

        exit_code, remove_payload, _, _ = self.run_cli(
            "remove-from-dataset",
            str(self.root),
            "--dataset-name",
            "Review Set",
            "--doc-id",
            str(row["id"]),
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(remove_payload)
        self.assertEqual(remove_payload["removed_document_ids"], [row["id"]])
        self.assertEqual(remove_payload["documents_without_dataset_memberships"], [])

        dataset_filtered = retriever_tools.search(
            self.root,
            "",
            [["dataset_name", "eq", "Review Set"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(dataset_filtered["total_hits"], 0)

        exit_code, delete_payload, _, _ = self.run_cli(
            "delete-dataset",
            str(self.root),
            "--dataset-id",
            str(dataset_id),
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(delete_payload)
        self.assertEqual(delete_payload["deleted_dataset"]["id"], dataset_id)

        exit_code, list_payload, _, _ = self.run_cli("list-datasets", str(self.root))
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(list_payload)
        dataset_names = [item["dataset_name"] for item in list_payload["datasets"]]
        self.assertNotIn("Review Set", dataset_names)

    def test_create_dataset_rejects_normalized_name_collision(self) -> None:
        retriever_tools.bootstrap(self.root)

        first_exit, first_payload, _, _ = self.run_cli("create-dataset", str(self.root), "Review Set")
        second_exit, second_payload, _, _ = self.run_cli("create-dataset", str(self.root), "  review   set  ")

        self.assertEqual(first_exit, 0)
        self.assertIsNotNone(first_payload)
        self.assertEqual(second_exit, 2)
        self.assertIsNotNone(second_payload)
        self.assertIn("already exists", second_payload["error"])

    def test_search_cli_accepts_sql_like_filter_boolean_groups(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("beta body\n", encoding="utf-8")
        (self.root / "gamma.md").write_text("gamma body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 3)

        exit_code, payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "--filter",
            "(file_name = 'alpha.txt' OR file_name = 'beta.txt') AND file_type = 'txt'",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["total_hits"], 2)
        self.assertEqual(sorted(item["file_name"] for item in payload["results"]), ["alpha.txt", "beta.txt"])

    def test_search_cli_sql_like_filter_errors_are_actionable(self) -> None:
        retriever_tools.bootstrap(self.root)

        unknown_exit, unknown_payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "--filter",
            "authr = 'Alice Example'",
        )
        mismatch_exit, mismatch_payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "--filter",
            "is_attachment > 5",
        )

        self.assertEqual(unknown_exit, 2)
        self.assertIsNotNone(unknown_payload)
        self.assertIn("Unknown field 'authr'", unknown_payload["error"])
        self.assertIn("author", unknown_payload["error"])

        self.assertEqual(mismatch_exit, 2)
        self.assertIsNotNone(mismatch_payload)
        self.assertIn("Field 'is_attachment' is boolean", mismatch_payload["error"])
        self.assertIn("IS NULL", mismatch_payload["error"])

    def test_sql_filter_randomized_semantics_match_sqlite_reference(self) -> None:
        corpus_rows = self.setup_randomized_sql_filter_corpus()
        rng = random.Random(RANDOMIZED_FILTER_TEST_SEED)

        for _ in range(40):
            expression = self.render_random_sql_filter_node(self.build_random_sql_filter_node(rng))
            expected_doc_ids = self.reference_doc_ids_for_sql_filter(corpus_rows, expression)

            with self.subTest(expression=expression):
                exit_code, payload, _, _ = self.run_cli(
                    "search",
                    str(self.root),
                    "--filter",
                    expression,
                )
                self.assertEqual(exit_code, 0)
                self.assertIsNotNone(payload)
                actual_doc_ids = sorted(int(item["id"]) for item in payload["results"])
                self.assertEqual(actual_doc_ids, expected_doc_ids)

    def test_sql_filter_in_list_boundary_is_enforced(self) -> None:
        retriever_tools.bootstrap(self.root)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            accepted_expression = "file_name IN (" + ", ".join("'a'" for _ in range(retriever_tools.MAX_FILTER_IN_LIST_ITEMS)) + ")"
            accepted_clause, accepted_params = retriever_tools.compile_sql_filter_expression(connection, accepted_expression)
            self.assertIn(" IN (", accepted_clause)
            self.assertEqual(len(accepted_params), retriever_tools.MAX_FILTER_IN_LIST_ITEMS)

            rejected_expression = "file_name IN (" + ", ".join("'a'" for _ in range(retriever_tools.MAX_FILTER_IN_LIST_ITEMS + 1)) + ")"
            with self.assertRaises(retriever_tools.RetrieverError) as excinfo:
                retriever_tools.compile_sql_filter_expression(connection, rejected_expression)
            self.assertIn(
                f"capped at {retriever_tools.MAX_FILTER_IN_LIST_ITEMS}",
                str(excinfo.exception),
            )
        finally:
            connection.close()

    def test_search_cli_columns_returns_display_values_and_rejects_filter_only_fields(self) -> None:
        (self.root / "sample.txt").write_text("sample body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        self.assertEqual(self.run_cli("add-field", str(self.root), "review_score", "integer")[0], 0)
        self.assertEqual(
            self.run_cli(
                "set-field",
                str(self.root),
                "--doc-id",
                str(row["id"]),
                "--field",
                "review_score",
                "--value",
                "7",
            )[0],
            0,
        )

        search_exit, search_payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "--columns",
            "title,review_score,control_number",
        )
        error_exit, error_payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "--columns",
            "has_attachments",
        )

        self.assertEqual(search_exit, 0)
        self.assertIsNotNone(search_payload)
        self.assertEqual(search_payload["display"]["columns"], ["title", "review_score", "control_number"])
        self.assertEqual(search_payload["results"][0]["display_values"]["review_score"], 7)
        self.assertIn("title", search_payload["results"][0]["display_values"])

        self.assertEqual(error_exit, 2)
        self.assertIsNotNone(error_payload)
        self.assertIn("filter-only", error_payload["error"])
        self.assertIn("has_attachments", error_payload["error"])

    def test_search_cli_view_mode_returns_rendered_markdown_with_current_compact_shape(self) -> None:
        (self.root / "sample.txt").write_text("sample body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        search_exit, search_payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "sample",
            "--mode",
            "view",
            "--columns",
            "title,control_number",
        )

        self.assertEqual(search_exit, 0)
        self.assertIsNotNone(search_payload)
        assert search_payload is not None
        self.assertIn("results", search_payload)
        self.assertNotIn("documents", search_payload)
        self.assertIn("rendered_markdown", search_payload)
        rendered = str(search_payload["rendered_markdown"])
        self.assertIn("Scope: keyword='sample'", rendered)
        self.assertIn("| title | control_number |", rendered)
        self.assertIn("](computer://", rendered)
        self.assertIn("Documents 1–1 of 1.", rendered)
        self.assertNotIn("| # |", rendered)

    def test_search_cli_view_mode_renders_attachment_rows_and_parent_context(self) -> None:
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Upgrade test",
            body_text="Hello team,\nThis is the email body.",
            attachment_name="notes.txt",
            attachment_text="confidential attachment detail",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        parent_exit, parent_payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "Upgrade test",
            "--mode",
            "view",
        )
        self.assertEqual(parent_exit, 0)
        self.assertIsNotNone(parent_payload)
        assert parent_payload is not None
        parent_rendered = str(parent_payload["rendered_markdown"])
        self.assertIn("[Upgrade test](computer://", parent_rendered)
        self.assertIn("[↳ notes.txt](computer://", parent_rendered)
        self.assertIn("| Attachment |", parent_rendered)

        attachment_exit, attachment_payload, _, _ = self.run_cli(
            "search",
            str(self.root),
            "confidential attachment detail",
            "--mode",
            "view",
        )
        self.assertEqual(attachment_exit, 0)
        self.assertIsNotNone(attachment_payload)
        assert attachment_payload is not None
        attachment_rendered = str(attachment_payload["rendered_markdown"])
        self.assertIn("[↳ notes.txt (parent: Upgrade test)](computer://", attachment_rendered)

    def test_slash_search_persists_scope_and_search_within_keyword(self) -> None:
        (self.root / "alpha.txt").write_text("alpha beta body\n", encoding="utf-8")
        (self.root / "second.txt").write_text("alpha only body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        first_exit, first_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")

        self.assertEqual(first_exit, 0)
        self.assertIsNotNone(first_payload)
        self.assertEqual(first_payload["scope"]["keyword"], "alpha")
        self.assertIn("keyword='alpha'", first_payload["header"]["scope"])

        session_payload = json.loads(self.paths["session_path"].read_text(encoding="utf-8"))
        self.assertEqual(session_payload["schema_version"], retriever_tools.SESSION_SCHEMA_VERSION)
        self.assertEqual(session_payload["scope"]["keyword"], "alpha")

        second_exit, second_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "--within", "beta")

        self.assertEqual(second_exit, 0)
        self.assertIsNotNone(second_payload)
        self.assertEqual(second_payload["scope"]["keyword"], "(alpha) AND (beta)")
        self.assertEqual(second_payload["total_hits"], 1)
        self.assertEqual(second_payload["results"][0]["file_name"], "alpha.txt")

    def test_slash_scope_save_load_and_dataset_rename_refresh_saved_scope_labels(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("sample dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        create_exit, create_payload, _, _ = self.run_cli("create-dataset", str(self.root), "Review Set")
        self.assertEqual(create_exit, 0)
        self.assertIsNotNone(create_payload)
        dataset_id = int(create_payload["dataset"]["id"])
        add_exit, _, _, _ = self.run_cli(
            "add-to-dataset",
            str(self.root),
            "--dataset-id",
            str(dataset_id),
            "--doc-id",
            str(row["id"]),
        )
        self.assertEqual(add_exit, 0)

        dataset_exit, dataset_payload, _, _ = self.run_cli("slash", str(self.root), "/dataset", "Review Set")
        save_exit, save_payload, _, _ = self.run_cli("slash", str(self.root), "/scope", "save", "review")
        rename_exit, rename_payload, _, _ = self.run_cli(
            "slash",
            str(self.root),
            '/dataset rename "Review Set" "Renamed Set"',
        )
        load_exit, load_payload, _, _ = self.run_cli("slash", str(self.root), "/scope", "load", "review")

        self.assertEqual(dataset_exit, 0)
        self.assertIsNotNone(dataset_payload)
        self.assertEqual(dataset_payload["scope"]["dataset"][0]["name"], "Review Set")

        self.assertEqual(save_exit, 0)
        self.assertIsNotNone(save_payload)
        self.assertEqual(save_payload["name"], "review")

        self.assertEqual(rename_exit, 0)
        self.assertIsNotNone(rename_payload)
        self.assertEqual(rename_payload["dataset"]["dataset_name"], "Renamed Set")

        self.assertEqual(load_exit, 0)
        self.assertIsNotNone(load_payload)
        self.assertEqual(load_payload["scope"]["dataset"][0]["id"], dataset_id)
        self.assertEqual(load_payload["scope"]["dataset"][0]["name"], "Renamed Set")
        self.assertEqual(load_payload["total_hits"], 1)

        saved_scopes_payload = json.loads(self.paths["saved_scopes_path"].read_text(encoding="utf-8"))
        self.assertEqual(saved_scopes_payload["schema_version"], retriever_tools.SESSION_SCHEMA_VERSION)
        self.assertEqual(saved_scopes_payload["scopes"]["review"]["dataset"][0]["id"], dataset_id)
        self.assertEqual(saved_scopes_payload["scopes"]["review"]["dataset"][0]["name"], "Renamed Set")

    def test_slash_scope_and_dataset_list_commands_show_current_and_available_state(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("alpha dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        create_exit, create_payload, _, _ = self.run_cli("create-dataset", str(self.root), "Review Set")
        self.assertEqual(create_exit, 0)
        self.assertIsNotNone(create_payload)
        dataset_id = int(create_payload["dataset"]["id"])
        add_exit, _, _, _ = self.run_cli(
            "add-to-dataset",
            str(self.root),
            "--dataset-id",
            str(dataset_id),
            "--doc-id",
            str(row["id"]),
        )
        self.assertEqual(add_exit, 0)

        self.assertEqual(self.run_cli("slash", str(self.root), "/search", "alpha")[0], 0)
        self.assertEqual(self.run_cli("slash", str(self.root), "/dataset", "Review Set")[0], 0)
        self.assertEqual(self.run_cli("slash", str(self.root), "/scope", "save", "review")[0], 0)

        search_show_exit, search_show_payload, _, _ = self.run_cli("slash", str(self.root), "/search")
        scope_show_exit, scope_show_payload, _, _ = self.run_cli("slash", str(self.root), "/scope")
        scope_list_exit, scope_list_payload, _, _ = self.run_cli("slash", str(self.root), "/scope", "list")
        dataset_show_exit, dataset_show_payload, _, _ = self.run_cli("slash", str(self.root), "/dataset")
        dataset_list_exit, dataset_list_payload, _, _ = self.run_cli("slash", str(self.root), "/dataset", "list")

        self.assertEqual(search_show_exit, 0)
        self.assertIsNotNone(search_show_payload)
        self.assertEqual(search_show_payload["keyword"], "alpha")

        self.assertEqual(scope_show_exit, 0)
        self.assertIsNotNone(scope_show_payload)
        self.assertEqual(scope_show_payload["scope"]["keyword"], "alpha")
        self.assertEqual(scope_show_payload["scope"]["dataset"][0]["name"], "Review Set")

        self.assertEqual(scope_list_exit, 0)
        self.assertIsNotNone(scope_list_payload)
        saved_scope_names = [item["name"] for item in scope_list_payload["saved_scopes"]]
        self.assertEqual(saved_scope_names, ["review"])
        self.assertEqual(scope_list_payload["saved_scopes"][0]["scope"]["keyword"], "alpha")
        self.assertEqual(scope_list_payload["saved_scopes"][0]["scope"]["dataset"][0]["name"], "Review Set")

        self.assertEqual(dataset_show_exit, 0)
        self.assertIsNotNone(dataset_show_payload)
        self.assertEqual(dataset_show_payload["dataset"][0]["name"], "Review Set")

        self.assertEqual(dataset_list_exit, 0)
        self.assertIsNotNone(dataset_list_payload)
        dataset_names = [item["dataset_name"] for item in dataset_list_payload["datasets"]]
        self.assertIn("Review Set", dataset_names)
        self.assertIn(self.root.name, dataset_names)

    def test_slash_bates_within_intersects_ranges_and_rejects_cross_slot_and_mixed_prefix(self) -> None:
        retriever_tools.bootstrap(self.root)

        first_exit, first_payload, _, _ = self.run_cli("slash", str(self.root), "/bates", "ABC0001-ABC0010")
        within_exit, within_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "--within", "ABC0005-ABC0007")
        cross_exit, cross_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "--within", "alpha")
        mixed_exit, mixed_payload, _, _ = self.run_cli("slash", str(self.root), "/bates", "ABC0001-XYZ0002")

        self.assertEqual(first_exit, 0)
        self.assertIsNotNone(first_payload)
        self.assertEqual(first_payload["scope"]["bates"], {"begin": "ABC0001", "end": "ABC0010"})

        self.assertEqual(within_exit, 0)
        self.assertIsNotNone(within_payload)
        self.assertEqual(within_payload["scope"]["bates"], {"begin": "ABC0005", "end": "ABC0007"})

        self.assertEqual(cross_exit, 2)
        self.assertIsNotNone(cross_payload)
        self.assertIn("only composes within the current slot", cross_payload["error"])

        self.assertEqual(mixed_exit, 2)
        self.assertIsNotNone(mixed_payload)
        self.assertIn("Mixed-prefix Bates ranges", mixed_payload["error"])

    def test_slash_search_errors_when_scope_dataset_reference_is_deleted(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("sample dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        create_exit, _, _, _ = self.run_cli("create-dataset", str(self.root), "Review Set")
        self.assertEqual(create_exit, 0)
        scope_exit, _, _, _ = self.run_cli("slash", str(self.root), "/dataset", "Review Set")
        self.assertEqual(scope_exit, 0)
        delete_exit, _, _, _ = self.run_cli("delete-dataset", str(self.root), "--dataset-name", "Review Set")
        self.assertEqual(delete_exit, 0)

        search_exit, search_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(search_exit, 2)
        self.assertIsNotNone(search_payload)
        self.assertIn("no longer exists", search_payload["error"])

    def test_slash_search_errors_when_scope_from_run_reference_is_missing(self) -> None:
        retriever_tools.bootstrap(self.root)
        self.paths["session_path"].write_text(
            json.dumps(
                {
                    "schema_version": retriever_tools.SESSION_SCHEMA_VERSION,
                    "scope": {"from_run_id": 999},
                    "browsing": {},
                    "display": {},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        search_exit, search_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(search_exit, 2)
        self.assertIsNotNone(search_payload)
        self.assertIn("scope.from_run_id no longer exists", search_payload["error"])

    def test_slash_search_filter_bates_and_from_run_show_current_scope_slots(self) -> None:
        retriever_tools.bootstrap(self.root)

        search_set_exit, _, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        filter_set_exit, _, _, _ = self.run_cli("slash", str(self.root), "/filter", "content_type = 'Email'")
        bates_set_exit, _, _, _ = self.run_cli("slash", str(self.root), "/bates", "ABC0001-ABC0010")
        self.assertEqual(search_set_exit, 0)
        self.assertEqual(filter_set_exit, 0)
        self.assertEqual(bates_set_exit, 0)

        self.paths["session_path"].write_text(
            json.dumps(
                {
                    "schema_version": retriever_tools.SESSION_SCHEMA_VERSION,
                    "scope": {
                        "keyword": "alpha",
                        "filter": "content_type = 'Email'",
                        "bates": {"begin": "ABC0001", "end": "ABC0010"},
                        "from_run_id": 42,
                    },
                    "browsing": {},
                    "display": {},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        search_show_exit, search_show_payload, _, _ = self.run_cli("slash", str(self.root), "/search")
        filter_show_exit, filter_show_payload, _, _ = self.run_cli("slash", str(self.root), "/filter")
        bates_show_exit, bates_show_payload, _, _ = self.run_cli("slash", str(self.root), "/bates")
        from_run_show_exit, from_run_show_payload, _, _ = self.run_cli("slash", str(self.root), "/from-run")

        self.assertEqual(search_show_exit, 0)
        self.assertIsNotNone(search_show_payload)
        self.assertEqual(search_show_payload["keyword"], "alpha")

        self.assertEqual(filter_show_exit, 0)
        self.assertIsNotNone(filter_show_payload)
        self.assertEqual(filter_show_payload["filter"], "content_type = 'Email'")

        self.assertEqual(bates_show_exit, 0)
        self.assertIsNotNone(bates_show_payload)
        self.assertEqual(bates_show_payload["bates"], {"begin": "ABC0001", "end": "ABC0010"})

        self.assertEqual(from_run_show_exit, 0)
        self.assertIsNotNone(from_run_show_payload)
        self.assertEqual(from_run_show_payload["from_run_id"], 42)

    def test_slash_sort_and_paging_persist_browsing_state(self) -> None:
        for index in range(25):
            (self.root / f"doc-{index:02d}.txt").write_text(f"document {index}\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 25)

        sort_exit, sort_payload, _, _ = self.run_cli("slash", str(self.root), "/sort", "file_name asc")

        self.assertEqual(sort_exit, 0)
        self.assertIsNotNone(sort_payload)
        self.assertEqual(sort_payload["results"][0]["file_name"], "doc-00.txt")
        self.assertEqual(sort_payload["header"]["sort"], "Sort: file_name asc")

        session_payload = json.loads(self.paths["session_path"].read_text(encoding="utf-8"))
        self.assertEqual(session_payload["browsing"]["sort"], [["file_name", "asc"]])
        self.assertEqual(session_payload["browsing"]["offset"], 0)

        next_exit, next_payload, _, _ = self.run_cli("slash", str(self.root), "/next")
        page_exit, page_payload, _, _ = self.run_cli("slash", str(self.root), "/page", "last")
        previous_exit, previous_payload, _, _ = self.run_cli("slash", str(self.root), "/previous")
        default_exit, default_payload, _, _ = self.run_cli("slash", str(self.root), "/sort", "default")

        self.assertEqual(next_exit, 0)
        self.assertIsNotNone(next_payload)
        self.assertEqual(next_payload["page"], 2)
        self.assertEqual(next_payload["offset"], 20)
        self.assertEqual(next_payload["results"][0]["file_name"], "doc-20.txt")

        self.assertEqual(page_exit, 0)
        self.assertIsNotNone(page_payload)
        self.assertEqual(page_payload["page"], 2)
        self.assertEqual(page_payload["offset"], 20)

        self.assertEqual(previous_exit, 0)
        self.assertIsNotNone(previous_payload)
        self.assertEqual(previous_payload["page"], 1)
        self.assertEqual(previous_payload["offset"], 0)

        self.assertEqual(default_exit, 0)
        self.assertIsNotNone(default_payload)
        self.assertEqual(default_payload["offset"], 0)

        final_session_payload = json.loads(self.paths["session_path"].read_text(encoding="utf-8"))
        self.assertNotIn("sort", final_session_payload["browsing"])
        self.assertEqual(final_session_payload["browsing"]["offset"], 0)

    def test_slash_sort_page_page_size_and_columns_show_current_and_available_state(self) -> None:
        for index in range(25):
            (self.root / f"doc-{index:02d}.txt").write_text(f"document {index}\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 25)

        sort_exit, _, _, _ = self.run_cli("slash", str(self.root), "/sort", "file_name asc")
        self.assertEqual(sort_exit, 0)

        sort_show_exit, sort_show_payload, _, _ = self.run_cli("slash", str(self.root), "/sort")
        sort_list_exit, sort_list_payload, _, _ = self.run_cli("slash", str(self.root), "/sort", "list")
        page_size_show_exit, page_size_show_payload, _, _ = self.run_cli("slash", str(self.root), "/page-size")
        self.assertEqual(self.run_cli("slash", str(self.root), "/page-size 5")[0], 0)
        self.assertEqual(self.run_cli("slash", str(self.root), "/next")[0], 0)
        page_show_exit, page_show_payload, _, _ = self.run_cli("slash", str(self.root), "/page")
        page_size_after_exit, page_size_after_payload, _, _ = self.run_cli("slash", str(self.root), "/page-size")
        columns_show_exit, columns_show_payload, _, _ = self.run_cli("slash", str(self.root), "/columns")
        columns_list_exit, columns_list_payload, _, _ = self.run_cli("slash", str(self.root), "/columns", "list")

        self.assertEqual(sort_show_exit, 0)
        self.assertIsNotNone(sort_show_payload)
        self.assertEqual(sort_show_payload["sort"], "file_name")
        self.assertEqual(sort_show_payload["order"], "asc")
        self.assertEqual(sort_show_payload["sort_spec"], "file_name asc")
        self.assertEqual(sort_show_payload["sort_source"], "override")

        self.assertEqual(sort_list_exit, 0)
        self.assertIsNotNone(sort_list_payload)
        sortable_names = [item["name"] for item in sort_list_payload["sortable_fields"]]
        self.assertIn("file_name", sortable_names)
        self.assertIn("title", sortable_names)
        self.assertNotIn("dataset_name", sortable_names)

        self.assertEqual(page_size_show_exit, 0)
        self.assertIsNotNone(page_size_show_payload)
        self.assertEqual(page_size_show_payload["page_size"], retriever_tools.DEFAULT_PAGE_SIZE)

        self.assertEqual(page_show_exit, 0)
        self.assertIsNotNone(page_show_payload)
        self.assertEqual(page_show_payload["page"], 2)
        self.assertEqual(page_show_payload["per_page"], 5)
        self.assertEqual(page_show_payload["offset"], 5)
        self.assertEqual(page_show_payload["total_known"], 25)
        self.assertEqual(page_show_payload["total_pages"], 5)

        self.assertEqual(page_size_after_exit, 0)
        self.assertIsNotNone(page_size_after_payload)
        self.assertEqual(page_size_after_payload["page_size"], 5)

        self.assertEqual(columns_show_exit, 0)
        self.assertIsNotNone(columns_show_payload)
        self.assertEqual(
            columns_show_payload["display"]["columns"],
            ["content_type", "title", "author", "date_created", "control_number"],
        )

        self.assertEqual(columns_list_exit, 0)
        self.assertIsNotNone(columns_list_payload)
        displayable_names = [item["name"] for item in columns_list_payload["columns"]]
        self.assertIn("title", displayable_names)
        self.assertIn("dataset_name", displayable_names)
        self.assertNotIn("has_attachments", displayable_names)

    def test_slash_columns_commands_persist_display_preferences_and_render_custom_fields(self) -> None:
        (self.root / "sample.txt").write_text("sample display body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        self.assertEqual(self.run_cli("add-field", str(self.root), "review_score", "integer")[0], 0)
        self.assertEqual(
            self.run_cli(
                "set-field",
                str(self.root),
                "--doc-id",
                str(row["id"]),
                "--field",
                "review_score",
                "--value",
                "7",
            )[0],
            0,
        )

        show_exit, show_payload, _, _ = self.run_cli("slash", str(self.root), "/columns")
        set_exit, set_payload, _, _ = self.run_cli(
            "slash",
            str(self.root),
            "/columns set title, review_score, control_number",
        )

        self.assertEqual(show_exit, 0)
        self.assertIsNotNone(show_payload)
        self.assertEqual(
            show_payload["display"]["columns"],
            ["content_type", "title", "author", "date_created", "control_number"],
        )

        self.assertEqual(set_exit, 0)
        self.assertIsNotNone(set_payload)
        self.assertEqual(set_payload["display"]["columns"], ["title", "review_score", "control_number"])
        self.assertEqual(set_payload["results"][0]["display_values"]["review_score"], 7)
        session_payload = json.loads(self.paths["session_path"].read_text(encoding="utf-8"))
        self.assertEqual(session_payload["display"]["columns"], ["title", "review_score", "control_number"])

        add_exit, add_payload, _, _ = self.run_cli("slash", str(self.root), "/columns add dataset_name")
        self.assertEqual(add_exit, 0)
        self.assertIsNotNone(add_payload)
        self.assertEqual(add_payload["display"]["columns"], ["title", "review_score", "control_number", "dataset_name"])
        self.assertEqual(add_payload["results"][0]["display_values"]["dataset_name"], self.root.name)

        remove_exit, remove_payload, _, _ = self.run_cli("slash", str(self.root), "/columns remove review_score")
        self.assertEqual(remove_exit, 0)
        self.assertIsNotNone(remove_payload)
        self.assertEqual(remove_payload["display"]["columns"], ["title", "control_number", "dataset_name"])

        default_exit, default_payload, _, _ = self.run_cli("slash", str(self.root), "/columns default")
        self.assertEqual(default_exit, 0)
        self.assertIsNotNone(default_payload)
        self.assertEqual(
            default_payload["display"]["columns"],
            ["content_type", "title", "author", "date_created", "control_number"],
        )
        final_session_payload = json.loads(self.paths["session_path"].read_text(encoding="utf-8"))
        self.assertNotIn("columns", final_session_payload["display"])

    def test_slash_page_size_persists_display_preferences(self) -> None:
        for index in range(12):
            (self.root / f"doc-{index:02d}.txt").write_text(f"document {index}\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 12)

        size_exit, size_payload, _, _ = self.run_cli("slash", str(self.root), "/page-size 5")
        next_exit, next_payload, _, _ = self.run_cli("slash", str(self.root), "/next")

        self.assertEqual(size_exit, 0)
        self.assertIsNotNone(size_payload)
        self.assertEqual(size_payload["per_page"], 5)
        self.assertEqual(size_payload["display"]["page_size"], 5)
        self.assertEqual(len(size_payload["results"]), 5)

        session_payload = json.loads(self.paths["session_path"].read_text(encoding="utf-8"))
        self.assertEqual(session_payload["display"]["page_size"], 5)

        self.assertEqual(next_exit, 0)
        self.assertIsNotNone(next_payload)
        self.assertEqual(next_payload["offset"], 5)
        self.assertEqual(next_payload["per_page"], 5)
        self.assertEqual(next_payload["page"], 2)

    def test_slash_search_drops_stale_display_columns_with_warning(self) -> None:
        (self.root / "sample.txt").write_text("sample body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        self.paths["session_path"].write_text(
            json.dumps(
                {
                    "schema_version": retriever_tools.SESSION_SCHEMA_VERSION,
                    "scope": {},
                    "browsing": {},
                    "display": {"columns": ["title", "missing_column", "has_attachments"]},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        search_exit, search_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "sample")

        self.assertEqual(search_exit, 0)
        self.assertIsNotNone(search_payload)
        self.assertEqual(search_payload["display"]["columns"], ["title"])
        self.assertIn("warnings", search_payload)
        self.assertIn("missing_column", search_payload["warnings"][0])
        persisted_session_payload = json.loads(self.paths["session_path"].read_text(encoding="utf-8"))
        self.assertEqual(persisted_session_payload["display"]["columns"], ["title"])

    def test_deleting_source_backed_dataset_hides_documents_until_reingest(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("sample dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        original_row = self.fetch_document_row("sample.txt")

        exit_code, delete_payload, _, _ = self.run_cli(
            "delete-dataset",
            str(self.root),
            "--dataset-name",
            self.root.name,
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(delete_payload)
        self.assertEqual(delete_payload["deleted_dataset"]["dataset_name"], self.root.name)
        self.assertEqual(delete_payload["documents_without_dataset_memberships"], [original_row["id"]])

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_result["total_hits"], 0)

        reingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(reingest_result["skipped"], 1)

        restored_row = self.fetch_document_row("sample.txt")
        self.assertEqual(restored_row["id"], original_row["id"])
        self.assertEqual(restored_row["control_number"], original_row["control_number"])

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_result["total_hits"], 1)
        self.assertEqual(browse_result["results"][0]["dataset_name"], self.root.name)

    def test_ingest_supports_xls_sheet_previews(self) -> None:
        xls_path = self.root / "ledger.xls"
        self.write_xls_fixture(xls_path)

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("ledger.xls")
        self.assertEqual(row["content_type"], "Spreadsheet / Table")
        self.assertEqual(row["page_count"], 2)

        search_result = retriever_tools.search(self.root, "Budget approved", None, None, None, 1, 20)
        self.assertEqual(search_result["results"][0]["file_name"], "ledger.xls")
        self.assertEqual(search_result["results"][0]["preview_targets"][0]["preview_type"], "csv")
        self.assertTrue(search_result["results"][0]["preview_rel_path"].endswith(".csv"))
        self.assertEqual(
            [target["label"] for target in search_result["results"][0]["preview_targets"]],
            ["Sheet1", "Notes"],
        )

    def test_ingest_supports_pptx_deck_preview_images_and_notes(self) -> None:
        pptx_path = self.root / "deck.pptx"
        self.write_pptx_fixture(pptx_path)

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)

        row = self.fetch_document_row("deck.pptx")
        self.assertEqual(row["content_type"], "Presentation")
        self.assertEqual(row["author"], "Rachel Green")
        self.assertEqual(row["title"], "Quarterly Strategy Deck")
        self.assertEqual(row["subject"], "Board Update")
        self.assertEqual(row["date_created"], "2026-04-15T10:00:00Z")
        self.assertEqual(row["date_modified"], "2026-04-15T11:00:00Z")
        self.assertEqual(row["page_count"], 2)
        self.assertEqual(row["text_status"], "ok")

        search_result = retriever_tools.search(self.root, "retained enterprise customers", None, None, None, 1, 20)
        result = search_result["results"][0]
        self.assertEqual(result["file_name"], "deck.pptx")
        self.assertTrue(result["preview_rel_path"].endswith(".html"))
        self.assertEqual(result["preview_targets"][0]["preview_type"], "html")

        preview_path = Path(result["preview_targets"][0]["abs_path"])
        preview_html = preview_path.read_text(encoding="utf-8")
        self.assertTrue(preview_html.startswith("<!DOCTYPE html>"))
        self.assertIn("<head>", preview_html)
        self.assertIn('<meta charset="utf-8"/>', preview_html)
        self.assertIn("<title>Quarterly Strategy Deck</title>", preview_html)
        self.assertIn("<h1>Quarterly Strategy Deck</h1>", preview_html)
        self.assertIn("Slide 1", preview_html)
        self.assertIn("Slide 2", preview_html)
        self.assertIn("Q3 Revenue Review", preview_html)
        self.assertIn("Hiring Plan", preview_html)
        self.assertIn("Speaker Notes", preview_html)
        self.assertIn("Emphasize retained enterprise customers.", preview_html)
        self.assertIn("data:image/png;base64,", preview_html)
        self.assertIn("Stark Therapeutics logo", preview_html)

        second_search = retriever_tools.search(self.root, "Open 12 roles in engineering", None, None, None, 1, 20)
        self.assertEqual(second_search["results"][0]["file_name"], "deck.pptx")

    def test_normalize_datetime_prefers_iso_before_pdf_date_parsing(self) -> None:
        self.assertEqual(
            retriever_tools.normalize_datetime("2023-01-18T09:00:00Z"),
            "2023-01-18T09:00:00Z",
        )
        self.assertEqual(
            retriever_tools.normalize_datetime("2023-01-18T09:00:00+00:00"),
            "2023-01-18T09:00:00Z",
        )
        self.assertEqual(
            retriever_tools.normalize_datetime("2023-01-18"),
            "2023-01-18T00:00:00Z",
        )
        self.assertEqual(
            retriever_tools.normalize_datetime("2023-01-18T09:00:00.123Z"),
            "2023-01-18T09:00:00Z",
        )
        self.assertEqual(
            retriever_tools.normalize_datetime("D:20230118090000"),
            "2023-01-18T09:00:00Z",
        )

    def test_ingest_supports_curated_source_and_config_text_files(self) -> None:
        text_files = {
            "main.py": "source marker python\n",
            "app.js": "source marker javascript\n",
            "types.ts": "source marker typescript\n",
            "widget.jsx": "source marker jsx\n",
            "view.tsx": "source marker tsx\n",
            "service.java": "source marker java\n",
            "worker.go": "source marker go\n",
            "script.rb": "source marker ruby\n",
            "index.php": "source marker php\n",
            "main.c": "source marker c\n",
            "header.h": "source marker h\n",
            "engine.cpp": "source marker cpp\n",
            "engine.hpp": "source marker hpp\n",
            "app.cs": "source marker csharp\n",
            "lib.rs": "source marker rust\n",
            "mobile.swift": "source marker swift\n",
            "service.kt": "source marker kotlin\n",
            "run.sh": "source marker sh\n",
            "login.bash": "source marker bash\n",
            "setup.zsh": "source marker zsh\n",
            "deploy.ps1": "source marker powershell\n",
            "app.yaml": "source marker yaml\n",
            "config.yml": "source marker yml\n",
            "settings.toml": "source marker toml\n",
            "local.ini": "source marker ini\n",
            "build.cfg": "source marker cfg\n",
            "service.conf": "source marker conf\n",
            "app.properties": "source marker properties\n",
            "schema.xml": "<config>source marker xml</config>\n",
            "query.sql": "select 'source marker sql';\n",
            "style.css": "/* source marker css */\n",
            "theme.scss": "/* source marker scss */\n",
            "theme.less": "/* source marker less */\n",
        }
        for file_name, content in text_files.items():
            (self.root / file_name).write_text(content, encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], len(text_files))
        self.assertEqual(ingest_result["failed"], 0)

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 100)
        for file_name in text_files:
            row = self.fetch_document_row(file_name)
            self.assertEqual(row["text_status"], "ok")
            result = next(item for item in browse_result["results"] if item["file_name"] == Path(file_name).name)
            self.assertEqual(result["preview_rel_path"], file_name)
            self.assertEqual(result["preview_targets"][0]["preview_type"], "native")

        xml_row = self.fetch_document_row("schema.xml")
        self.assertEqual(xml_row["content_type"], "Web")

        search_result = retriever_tools.search(self.root, "source marker", None, None, None, 1, 100)
        self.assertGreaterEqual(search_result["total_hits"], len(text_files))

    def test_v5_to_v7_migration_then_touch_reingest_keeps_backfilled_control_number(self) -> None:
        self.create_legacy_documents_table(with_row=True)
        legacy_path = self.root / "legacy.txt"
        legacy_path.write_text("updated legacy body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        migrated_row = self.fetch_document_row("legacy.txt")
        self.assertEqual(migrated_row["control_number"], "DOC001.00000001")
        self.assertEqual(migrated_row["control_number_batch"], 1)
        self.assertEqual(migrated_row["control_number_family_sequence"], 1)

        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["updated"], 1)

        updated_row = self.fetch_document_row("legacy.txt")
        self.assertEqual(updated_row["control_number"], migrated_row["control_number"])
        self.assertEqual(updated_row["control_number_batch"], 1)
        self.assertEqual(updated_row["control_number_family_sequence"], 1)

    def test_bootstrap_renames_v6_display_columns_to_control_number(self) -> None:
        self.create_v6_documents_table(with_row=True)
        legacy_path = self.root / "legacy.txt"
        legacy_path.write_text("updated legacy body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            columns = retriever_tools.table_columns(connection, "documents")
            self.assertIn("control_number", columns)
            self.assertNotIn("display_id", columns)
            row = connection.execute(
                """
                SELECT control_number, control_number_batch, control_number_family_sequence
                FROM documents
                WHERE rel_path = 'legacy.txt'
                """
            ).fetchone()
            table_names = {
                item["name"]
                for item in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
        finally:
            connection.close()

        self.assertEqual(row["control_number"], "DOC001.00000001")
        self.assertEqual(row["control_number_batch"], 1)
        self.assertEqual(row["control_number_family_sequence"], 1)
        self.assertIn("control_number_batches", table_names)
        self.assertNotIn("display_id_batches", table_names)

        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["updated"], 1)

        updated_row = self.fetch_document_row("legacy.txt")
        self.assertEqual(updated_row["control_number"], "DOC001.00000001")

    def test_bootstrap_repairs_partial_control_number_identity_fields(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("hello\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(first_ingest["new"], 1)

        row = self.fetch_document_row("sample.txt")
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            connection.execute(
                """
                UPDATE documents
                SET control_number_batch = NULL, control_number_family_sequence = NULL
                WHERE id = ?
                """,
                (row["id"],),
            )
            connection.commit()
        finally:
            connection.close()

        retriever_tools.bootstrap(self.root)
        repaired_row = self.fetch_document_row("sample.txt")
        self.assertEqual(repaired_row["control_number"], row["control_number"])
        self.assertEqual(repaired_row["control_number_batch"], 1)
        self.assertEqual(repaired_row["control_number_family_sequence"], 1)

    def test_reingest_preserves_attachment_control_number_and_manual_locks(self) -> None:
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Phase 3.5 test",
            body_text="Original parent body",
            attachment_name="notes.txt",
            attachment_text="stable attachment body",
        )

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(first_ingest["new"], 1)

        parent_row = self.fetch_document_row("thread.eml")
        child_row = self.fetch_child_rows(parent_row["id"])[0]
        retriever_tools.set_field(self.root, child_row["id"], "title", "Manual Attachment Title")

        self.write_email_message(
            email_path,
            subject="Phase 3.5 test updated",
            body_text="Updated parent body only",
            attachment_name="notes.txt",
            attachment_text="stable attachment body",
        )
        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(second_ingest["updated"], 1)
        self.assertEqual(second_ingest["failed"], 0)

        updated_parent = self.fetch_document_row("thread.eml")
        updated_child = self.fetch_child_rows(updated_parent["id"])[0]
        self.assertEqual(updated_parent["control_number"], parent_row["control_number"])
        self.assertEqual(updated_child["id"], child_row["id"])
        self.assertEqual(updated_child["control_number"], child_row["control_number"])
        self.assertEqual(updated_child["title"], "Manual Attachment Title")

        self.write_email_message(
            email_path,
            subject="Phase 3.5 test no attachment",
            body_text="Updated parent body without attachment",
        )
        third_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(third_ingest["updated"], 1)

        removed_child = self.fetch_document_by_id(child_row["id"])
        self.assertEqual(removed_child["lifecycle_status"], "deleted")

        parent_search = retriever_tools.search(self.root, "Phase 3.5 test no attachment", None, None, None, 1, 20)
        parent_result = next(item for item in parent_search["results"] if item["id"] == updated_parent["id"])
        self.assertEqual(parent_result["attachment_count"], 0)

    def test_rerun_ingest_after_attachment_extraction_keeps_document_total_stable(self) -> None:
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Attachment rerun guard",
            body_text="Parent body",
            attachment_name="notes.txt",
            attachment_text="attachment body",
        )

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(first_ingest["new"], 1)
        self.assertEqual(first_ingest["failed"], 0)

        parent_row = self.fetch_document_row("thread.eml")
        child_rows = self.fetch_child_rows(parent_row["id"])
        self.assertEqual(len(child_rows), 1)

        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(second_ingest["skipped"], 1)
        self.assertEqual(second_ingest["new"], 0)
        self.assertEqual(second_ingest["failed"], 0)

        browse_results = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_results["total_hits"], 2)
        attachments_only = retriever_tools.search(
            self.root,
            "",
            [["is_attachment", "eq", "true"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(attachments_only["total_hits"], 1)
        self.assertTrue(
            all(
                not str(item["rel_path"]).startswith(
                    f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/thread.eml/attachments/"
                )
                or item["parent_document_id"] is not None
                for item in browse_results["results"]
            )
        )

    def test_ingest_reuses_workspace_dataset_on_reingest(self) -> None:
        alpha_path = self.root / "alpha.txt"
        alpha_path.write_text("alpha dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(first_ingest["new"], 1)
        self.assertEqual(first_ingest["failed"], 0)

        alpha_row = self.fetch_document_row("alpha.txt")
        self.assertIsNotNone(alpha_row["dataset_id"])
        initial_dataset_id = int(alpha_row["dataset_id"])
        initial_dataset_row = self.fetch_dataset_row(initial_dataset_id)
        self.assertEqual(initial_dataset_row["source_kind"], retriever_tools.FILESYSTEM_SOURCE_KIND)
        self.assertEqual(initial_dataset_row["dataset_locator"], ".")
        self.assertEqual(initial_dataset_row["dataset_name"], self.root.name)

        beta_path = self.root / "beta.txt"
        beta_path.write_text("beta dataset body\n", encoding="utf-8")

        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(second_ingest["new"], 1)
        self.assertEqual(second_ingest["skipped"], 1)
        self.assertEqual(second_ingest["failed"], 0)

        updated_alpha_row = self.fetch_document_row("alpha.txt")
        beta_row = self.fetch_document_row("beta.txt")
        self.assertEqual(updated_alpha_row["dataset_id"], initial_dataset_id)
        self.assertEqual(beta_row["dataset_id"], initial_dataset_id)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            dataset_rows = connection.execute(
                """
                SELECT *
                FROM datasets
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(len(dataset_rows), 1)
        dataset_filtered = retriever_tools.search(
            self.root,
            "",
            [["dataset", "eq", self.root.name]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(dataset_filtered["total_hits"], 2)
        self.assertTrue(all(item["dataset_name"] == self.root.name for item in dataset_filtered["results"]))

    def test_document_can_belong_to_multiple_datasets_without_duplicate_search_hits(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("sample dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            review_dataset_id = retriever_tools.create_dataset_row(connection, "Review Set")
            retriever_tools.ensure_dataset_document_membership(
                connection,
                dataset_id=review_dataset_id,
                document_id=int(row["id"]),
                dataset_source_id=None,
            )
            connection.commit()
        finally:
            connection.close()

        dataset_filtered = retriever_tools.search(
            self.root,
            "",
            [["dataset_name", "eq", "Review Set"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(dataset_filtered["total_hits"], 1)
        result = dataset_filtered["results"][0]
        self.assertEqual(result["id"], row["id"])
        self.assertEqual(sorted(result["dataset_names"]), ["Review Set", self.root.name])
        self.assertIsNone(result["dataset_id"])
        self.assertNotIn("dataset_name", result)

    def test_search_omits_documents_without_dataset_memberships(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("sample dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            connection.execute("DELETE FROM dataset_documents WHERE document_id = ?", (row["id"],))
            connection.commit()
        finally:
            connection.close()

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_result["total_hits"], 0)

    def test_ingest_mbox_creates_message_rows_with_source_context_and_attachment_children(self) -> None:
        mbox_path = self.write_fake_mbox_file(
            [
                self.build_fake_mbox_message(
                    subject="MBOX Parent",
                    body_text="Parent message body",
                    message_id="<mbox-msg-001@example.com>",
                    attachment_name="notes.txt",
                    attachment_text="mbox attachment body",
                ),
                self.build_fake_mbox_message(
                    subject="MBOX Sibling",
                    body_text="Sibling body text",
                    message_id="<mbox-msg-002@example.com>",
                    date_created="Tue, 14 Apr 2026 10:05:00 +0000",
                ),
            ]
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["mbox_messages_created"], 2)
        self.assertEqual(ingest_result["workspace_parent_documents"], 2)
        self.assertEqual(ingest_result["workspace_attachment_children"], 1)
        self.assertEqual(ingest_result["workspace_documents_total"], 3)

        parent_rel_path = retriever_tools.mbox_message_rel_path("mailbox.mbox", "<mbox-msg-001@example.com>")
        sibling_rel_path = retriever_tools.mbox_message_rel_path("mailbox.mbox", "<mbox-msg-002@example.com>")
        parent_row = self.fetch_document_row(parent_rel_path)
        sibling_row = self.fetch_document_row(sibling_rel_path)
        child_row = self.fetch_child_rows(parent_row["id"])[0]
        dataset_row = self.fetch_dataset_row(int(parent_row["dataset_id"]))

        self.assertEqual(parent_row["source_kind"], retriever_tools.MBOX_SOURCE_KIND)
        self.assertEqual(parent_row["source_rel_path"], "mailbox.mbox")
        self.assertEqual(parent_row["source_item_id"], "<mbox-msg-001@example.com>")
        self.assertIsNone(parent_row["source_folder_path"])
        self.assertEqual(parent_row["file_type"], "mbox")
        self.assertIsNone(parent_row["file_size"])
        self.assertEqual(parent_row["content_type"], "Email")
        self.assertEqual(parent_row["custodian"], "mailbox")
        self.assertEqual(sibling_row["custodian"], "mailbox")
        self.assertEqual(child_row["custodian"], "mailbox")
        self.assertEqual(parent_row["dataset_id"], sibling_row["dataset_id"])
        self.assertEqual(parent_row["dataset_id"], child_row["dataset_id"])
        self.assertTrue(
            str(child_row["rel_path"]).startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/mailbox.mbox/attachments/"
            )
        )

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            container_row = connection.execute(
                "SELECT * FROM container_sources WHERE source_kind = ? AND source_rel_path = ?",
                (retriever_tools.MBOX_SOURCE_KIND, "mailbox.mbox"),
            ).fetchone()
            dataset_rows = connection.execute(
                """
                SELECT *
                FROM datasets
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            connection.close()

        self.assertIsNotNone(container_row)
        self.assertEqual(container_row["dataset_id"], parent_row["dataset_id"])
        self.assertEqual(container_row["message_count"], 2)
        self.assertEqual(container_row["file_size"], mbox_path.stat().st_size)
        self.assertIsNotNone(container_row["last_scan_completed_at"])
        self.assertEqual(dataset_row["source_kind"], retriever_tools.MBOX_SOURCE_KIND)
        self.assertEqual(dataset_row["dataset_locator"], "mailbox.mbox")
        self.assertEqual(dataset_row["dataset_name"], "mailbox.mbox")
        self.assertEqual(len(dataset_rows), 1)
        self.assertTrue(all(row["source_kind"] == retriever_tools.MBOX_SOURCE_KIND for row in dataset_rows))

        parent_search = retriever_tools.search(self.root, "MBOX Parent", None, None, None, 1, 20)
        parent_result = next(item for item in parent_search["results"] if item["id"] == parent_row["id"])
        self.assertEqual(parent_result["attachment_count"], 1)
        self.assertEqual(parent_result["source_rel_path"], "mailbox.mbox")
        self.assertEqual(parent_result["dataset_name"], "mailbox.mbox")
        self.assertTrue(
            parent_result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(parent_result["preview_rel_path"].endswith(f"#doc-{parent_row['id']}"))
        parent_preview_html = self.preview_target_file_path(parent_result["preview_targets"][0]).read_text(encoding="utf-8")
        self.assertIn("Apr 14, 2026 10:00 AM UTC", parent_preview_html)
        self.assertIn("MBOX Parent", parent_preview_html)

        mbox_only = retriever_tools.search(
            self.root,
            "",
            [["source_kind", "eq", retriever_tools.MBOX_SOURCE_KIND]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(mbox_only["total_hits"], 2)
        dataset_filtered = retriever_tools.search(
            self.root,
            "",
            [["dataset_name", "eq", "mailbox.mbox"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(dataset_filtered["total_hits"], 3)
        self.assertTrue(all(item["dataset_name"] == "mailbox.mbox" for item in dataset_filtered["results"]))
        attachment_search = retriever_tools.search(self.root, "mbox attachment body", None, None, None, 1, 20)
        attachment_result = next(item for item in attachment_search["results"] if item["id"] == child_row["id"])
        self.assertEqual(attachment_result["parent"]["control_number"], parent_row["control_number"])

    def test_ingest_mbox_fixture_file_is_searchable(self) -> None:
        fixture_source = REGRESSION_CORPUS_ROOT / "sample_utf8.mbox"
        fixture_target = self.root / fixture_source.name
        fixture_target.write_bytes(fixture_source.read_bytes())

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["mbox_messages_created"], 2)

        fixture_search = retriever_tools.search(self.root, "retriever regression fixture", None, None, None, 1, 20)
        self.assertEqual(fixture_search["total_hits"], 1)
        result = fixture_search["results"][0]
        self.assertEqual(result["source_kind"], retriever_tools.MBOX_SOURCE_KIND)
        self.assertEqual(result["dataset_name"], "sample_utf8.mbox")

    def test_ingest_mbox_without_message_id_uses_stable_fallback_source_item_id(self) -> None:
        first_messages = [
            self.build_fake_mbox_message(
                subject="Missing ID Parent",
                body_text="Fallback source id body",
                message_id=None,
            )
        ]
        self.write_fake_mbox_file(first_messages)

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(first_ingest["new"], 1)
        self.assertEqual(first_ingest["mbox_messages_created"], 1)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            first_row = connection.execute(
                """
                SELECT *
                FROM documents
                WHERE parent_document_id IS NULL
                  AND source_kind = ?
                  AND source_rel_path = ?
                ORDER BY id ASC
                """,
                (retriever_tools.MBOX_SOURCE_KIND, "mailbox.mbox"),
            ).fetchone()
        finally:
            connection.close()

        self.assertIsNotNone(first_row)
        fallback_source_item_id = str(first_row["source_item_id"])
        self.assertTrue(fallback_source_item_id.startswith("mbox-hash:"))
        original_control_number = str(first_row["control_number"])

        second_messages = [
            self.build_fake_mbox_message(
                subject="Missing ID Parent",
                body_text="Fallback source id body",
                message_id=None,
            ),
            self.build_fake_mbox_message(
                subject="Sibling",
                body_text="Sibling body text",
                message_id="<mbox-msg-002@example.com>",
                date_created="Tue, 14 Apr 2026 10:05:00 +0000",
            ),
        ]
        self.write_fake_mbox_file(second_messages)
        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(second_ingest["updated"], 1)
        self.assertEqual(second_ingest["mbox_messages_created"], 1)
        self.assertEqual(second_ingest["mbox_messages_updated"], 1)

        updated_row = self.fetch_document_row(
            retriever_tools.mbox_message_rel_path("mailbox.mbox", fallback_source_item_id)
        )
        self.assertEqual(updated_row["control_number"], original_control_number)

    def test_unchanged_mbox_source_skips_without_reparsing(self) -> None:
        self.write_fake_mbox_file(
            [
                self.build_fake_mbox_message(
                    subject="MBOX Parent",
                    body_text="Parent message body",
                    message_id="<mbox-msg-001@example.com>",
                )
            ]
        )

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(first_ingest["new"], 1)
        with mock.patch.object(
            retriever_tools,
            "iter_mbox_messages",
            side_effect=AssertionError("MBOX iterator should not run on unchanged source"),
        ):
            second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(second_ingest["skipped"], 1)
        self.assertEqual(second_ingest["mbox_sources_skipped"], 1)
        self.assertEqual(second_ingest["failed"], 0)
        browse_results = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_results["total_hits"], 1)

    def test_changed_mbox_reingest_preserves_control_numbers_and_retires_removed_messages(self) -> None:
        self.write_fake_mbox_file(
            [
                self.build_fake_mbox_message(
                    subject="Original MBOX Parent",
                    body_text="Parent v1",
                    message_id="<mbox-msg-001@example.com>",
                    attachment_name="notes.txt",
                    attachment_text="stable attachment body",
                ),
                self.build_fake_mbox_message(
                    subject="Removed later",
                    body_text="Remove me",
                    message_id="<mbox-msg-002@example.com>",
                    date_created="Tue, 14 Apr 2026 10:05:00 +0000",
                ),
            ]
        )

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(first_ingest["mbox_messages_created"], 2)
        parent_rel_path = retriever_tools.mbox_message_rel_path("mailbox.mbox", "<mbox-msg-001@example.com>")
        removed_rel_path = retriever_tools.mbox_message_rel_path("mailbox.mbox", "<mbox-msg-002@example.com>")
        parent_row = self.fetch_document_row(parent_rel_path)
        child_row = self.fetch_child_rows(parent_row["id"])[0]
        retriever_tools.set_field(self.root, parent_row["id"], "title", "Manual MBOX Title")

        self.write_fake_mbox_file(
            [
                self.build_fake_mbox_message(
                    subject="Updated MBOX Parent",
                    body_text="Parent v2",
                    message_id="<mbox-msg-001@example.com>",
                    attachment_name="notes.txt",
                    attachment_text="stable attachment body",
                )
            ]
        )
        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(second_ingest["updated"], 1)
        self.assertEqual(second_ingest["mbox_messages_updated"], 1)
        self.assertEqual(second_ingest["mbox_messages_deleted"], 1)

        updated_parent = self.fetch_document_row(parent_rel_path)
        updated_child = self.fetch_child_rows(updated_parent["id"])[0]
        retired_row = self.fetch_document_row(removed_rel_path)
        self.assertEqual(updated_parent["control_number"], parent_row["control_number"])
        self.assertEqual(updated_parent["title"], "Manual MBOX Title")
        self.assertEqual(updated_child["id"], child_row["id"])
        self.assertEqual(updated_child["control_number"], child_row["control_number"])
        self.assertEqual(retired_row["lifecycle_status"], "deleted")

    def test_missing_mbox_source_marks_messages_and_children_missing(self) -> None:
        mbox_path = self.write_fake_mbox_file(
            [
                self.build_fake_mbox_message(
                    subject="MBOX Parent",
                    body_text="Parent body",
                    message_id="<mbox-msg-001@example.com>",
                    attachment_name="notes.txt",
                    attachment_text="attachment body",
                )
            ]
        )

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(first_ingest["mbox_messages_created"], 1)
        parent_rel_path = retriever_tools.mbox_message_rel_path("mailbox.mbox", "<mbox-msg-001@example.com>")
        parent_row = self.fetch_document_row(parent_rel_path)
        child_row = self.fetch_child_rows(parent_row["id"])[0]

        mbox_path.unlink()
        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(second_ingest["missing"], 1)
        self.assertEqual(second_ingest["mbox_sources_missing"], 1)
        self.assertEqual(second_ingest["mbox_documents_missing"], 2)
        missing_parent = self.fetch_document_row(parent_rel_path)
        missing_child = self.fetch_document_by_id(child_row["id"])
        self.assertEqual(missing_parent["lifecycle_status"], "missing")
        self.assertEqual(missing_child["lifecycle_status"], "missing")

    def test_ingest_pst_creates_message_rows_with_source_context_and_attachment_children(self) -> None:
        pst_path = self.write_fake_pst_file()
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-msg-001",
                subject="PST Parent",
                body_text="Parent message body",
                folder_path="Inbox",
                attachment_name="notes.txt",
                attachment_text="pst attachment body",
            ),
            self.build_fake_pst_message(
                source_item_id="pst-msg-002",
                subject="PST Sibling",
                body_text="Sibling body text",
                folder_path="Sent Items",
            ),
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["pst_messages_created"], 2)
        self.assertEqual(ingest_result["workspace_parent_documents"], 2)
        self.assertEqual(ingest_result["workspace_attachment_children"], 1)
        self.assertEqual(ingest_result["workspace_documents_total"], 3)

        parent_rel_path = retriever_tools.pst_message_rel_path("mailbox.pst", "pst-msg-001")
        sibling_rel_path = retriever_tools.pst_message_rel_path("mailbox.pst", "pst-msg-002")
        parent_row = self.fetch_document_row(parent_rel_path)
        sibling_row = self.fetch_document_row(sibling_rel_path)
        child_row = self.fetch_child_rows(parent_row["id"])[0]

        self.assertEqual(parent_row["source_kind"], retriever_tools.PST_SOURCE_KIND)
        self.assertEqual(parent_row["source_rel_path"], "mailbox.pst")
        self.assertEqual(parent_row["source_item_id"], "pst-msg-001")
        self.assertEqual(parent_row["source_folder_path"], "Inbox")
        self.assertEqual(parent_row["file_type"], "pst")
        self.assertIsNone(parent_row["file_size"])
        self.assertEqual(parent_row["content_type"], "Email")
        self.assertEqual(parent_row["custodian"], "mailbox")
        self.assertEqual(sibling_row["source_folder_path"], "Sent Items")
        self.assertEqual(sibling_row["custodian"], "mailbox")
        self.assertEqual(child_row["custodian"], "mailbox")
        self.assertIsNotNone(parent_row["dataset_id"])
        self.assertEqual(parent_row["dataset_id"], sibling_row["dataset_id"])
        self.assertEqual(parent_row["dataset_id"], child_row["dataset_id"])
        self.assertTrue(
            str(child_row["rel_path"]).startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/mailbox.pst/attachments/"
            )
        )

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            container_row = connection.execute(
                "SELECT * FROM container_sources WHERE source_kind = ? AND source_rel_path = ?",
                (retriever_tools.PST_SOURCE_KIND, "mailbox.pst"),
            ).fetchone()
            dataset_row = connection.execute(
                "SELECT * FROM datasets WHERE id = ?",
                (parent_row["dataset_id"],),
            ).fetchone()
            dataset_rows = connection.execute(
                """
                SELECT *
                FROM datasets
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            connection.close()

        self.assertIsNotNone(container_row)
        self.assertEqual(container_row["dataset_id"], parent_row["dataset_id"])
        self.assertEqual(container_row["message_count"], 2)
        self.assertEqual(container_row["file_size"], pst_path.stat().st_size)
        self.assertIsNotNone(container_row["last_scan_completed_at"])
        self.assertIsNotNone(dataset_row)
        self.assertEqual(dataset_row["source_kind"], retriever_tools.PST_SOURCE_KIND)
        self.assertEqual(dataset_row["dataset_locator"], "mailbox.pst")
        self.assertEqual(dataset_row["dataset_name"], "mailbox.pst")
        self.assertEqual(len(dataset_rows), 1)
        self.assertTrue(all(row["source_kind"] == retriever_tools.PST_SOURCE_KIND for row in dataset_rows))

        parent_search = retriever_tools.search(self.root, "PST Parent", None, None, None, 1, 20)
        parent_result = next(item for item in parent_search["results"] if item["id"] == parent_row["id"])
        self.assertEqual(parent_result["attachment_count"], 1)
        self.assertEqual(parent_result["source_rel_path"], "mailbox.pst")
        self.assertEqual(parent_result["source_folder_path"], "Inbox")
        self.assertEqual(parent_result["dataset_name"], "mailbox.pst")
        self.assertTrue(
            parent_result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(parent_result["preview_rel_path"].endswith(f"#doc-{parent_row['id']}"))
        parent_preview_html = self.preview_target_file_path(parent_result["preview_targets"][0]).read_text(encoding="utf-8")
        self.assertIn("Apr 14, 2026 10:00 AM UTC", parent_preview_html)
        self.assertIn("PST Parent", parent_preview_html)

        pst_only = retriever_tools.search(
            self.root,
            "",
            [["source_kind", "eq", retriever_tools.PST_SOURCE_KIND]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(pst_only["total_hits"], 2)
        custodian_filtered = retriever_tools.search(
            self.root,
            "",
            [["custodian", "eq", "mailbox"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(custodian_filtered["total_hits"], 3)
        dataset_filtered = retriever_tools.search(
            self.root,
            "",
            [["dataset_name", "eq", "mailbox.pst"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(dataset_filtered["total_hits"], 3)
        self.assertTrue(all(item["dataset_name"] == "mailbox.pst" for item in dataset_filtered["results"]))
        custodian_query = retriever_tools.search(self.root, "mailbox", None, None, None, 1, 20)
        self.assertEqual(custodian_query["total_hits"], 3)

        attachment_search = retriever_tools.search(self.root, "pst attachment body", None, None, None, 1, 20)
        attachment_result = next(item for item in attachment_search["results"] if item["id"] == child_row["id"])
        self.assertEqual(attachment_result["parent"]["control_number"], parent_row["control_number"])

    def test_pst_only_ingest_prunes_stale_empty_filesystem_dataset(self) -> None:
        self.write_fake_pst_file()
        retriever_tools.bootstrap(self.root)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            stale_dataset_id, stale_dataset_source_id = retriever_tools.ensure_source_backed_dataset(
                connection,
                source_kind=retriever_tools.FILESYSTEM_SOURCE_KIND,
                source_locator=retriever_tools.filesystem_dataset_locator(),
                dataset_name=retriever_tools.filesystem_dataset_name(self.root),
            )
            connection.commit()
        finally:
            connection.close()

        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-msg-001",
                subject="PST Parent",
                body_text="Parent message body",
                folder_path="Inbox",
            )
        ]

        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["pruned_unused_filesystem_dataset"], 1)

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            dataset_rows = connection.execute(
                """
                SELECT *
                FROM datasets
                ORDER BY id ASC
                """
            ).fetchall()
            stale_dataset_row = connection.execute(
                "SELECT * FROM datasets WHERE id = ?",
                (stale_dataset_id,),
            ).fetchone()
            stale_dataset_source_row = connection.execute(
                "SELECT * FROM dataset_sources WHERE id = ?",
                (stale_dataset_source_id,),
            ).fetchone()
        finally:
            connection.close()

        self.assertIsNone(stale_dataset_row)
        self.assertIsNone(stale_dataset_source_row)
        self.assertEqual(len(dataset_rows), 1)
        self.assertEqual(dataset_rows[0]["source_kind"], retriever_tools.PST_SOURCE_KIND)

    def test_ingest_pst_chat_like_message_uses_chat_document_metadata(self) -> None:
        self.write_fake_pst_file()
        transcript = "\n".join(
            [
                "[2026-04-15 09:00] Alice Example: Kickoff thread for launch planning.",
                "[2026-04-15 09:05] Bob Example: I'll draft the update.",
                "[2026-04-15 09:07] Alice Example: Great, thanks.",
            ]
        )
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-chat-001",
                subject=None,
                body_text=transcript,
                folder_path="Conversation History",
                author="Conversation History",
                recipients=None,
                date_created="2026-04-16T00:00:00Z",
                message_class="IPM.Note.Microsoft.Conversation",
            )
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["pst_messages_created"], 1)
        self.assertEqual(ingest_result["pst_chat_conversations"], 1)

        row = self.fetch_document_row(retriever_tools.pst_message_rel_path("mailbox.pst", "pst-chat-001"))
        self.assertEqual(row["content_type"], "Chat")
        self.assertIsNone(row["author"])
        self.assertEqual(row["custodian"], "mailbox")
        self.assertEqual(row["participants"], "Alice Example, Bob Example")
        self.assertEqual(row["date_created"], "2026-04-15T09:00:00Z")
        self.assertEqual(row["date_modified"], "2026-04-15T09:07:00Z")
        self.assertEqual(row["title"], "Kickoff thread for launch planning.")
        self.assertIsNone(row["subject"])
        self.assertIsNone(row["recipients"])
        self.assertEqual(row["source_folder_path"], "Conversation History")
        self.assertIsNotNone(row["conversation_id"])

        search_result = retriever_tools.search(self.root, "draft the update", None, None, None, 1, 20)
        result = search_result["results"][0]
        self.assertTrue(
            result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(result["preview_rel_path"].endswith(f"#doc-{row['id']}"))
        self.assertEqual(result["preview_targets"][0]["preview_type"], "html")
        preview_html = self.preview_target_file_path(result["preview_targets"][0]).read_text(encoding="utf-8")
        self.assertIn("Alice Example", preview_html)
        self.assertIn("Bob Example", preview_html)
        self.assertIn("Kickoff thread for launch planning.", preview_html)
        self.assertIn(f'id="doc-{row["id"]}"', preview_html)

    def test_ingest_groups_pst_chat_messages_by_source_folder_path(self) -> None:
        self.write_fake_pst_file()
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-chat-001",
                subject=None,
                body_text="Kickoff thread for launch planning.",
                folder_path="Top of Personal Folders/user (Primary)/TeamsMessagesData",
                author="Alice Example",
                recipients=None,
                date_created="2026-04-15T09:00:00Z",
            ),
            self.build_fake_pst_message(
                source_item_id="pst-chat-002",
                subject=None,
                body_text="Follow-up from the same Teams space.",
                folder_path="Top of Personal Folders/user (Primary)/TeamsMessagesData",
                author="Bob Example",
                recipients=None,
                date_created="2026-04-16T09:00:00Z",
            ),
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["pst_messages_created"], 2)
        self.assertEqual(ingest_result["pst_chat_conversations"], 1)

        first_row = self.fetch_document_row(retriever_tools.pst_message_rel_path("mailbox.pst", "pst-chat-001"))
        second_row = self.fetch_document_row(retriever_tools.pst_message_rel_path("mailbox.pst", "pst-chat-002"))
        self.assertIsNotNone(first_row["conversation_id"])
        self.assertEqual(first_row["conversation_id"], second_row["conversation_id"])
        self.assertEqual(first_row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)
        self.assertEqual(second_row["conversation_assignment_mode"], retriever_tools.CONVERSATION_ASSIGNMENT_MODE_AUTO)

        search_result = retriever_tools.search(self.root, "Follow-up from the same Teams space", None, None, None, 1, 20)
        result = search_result["results"][0]
        self.assertTrue(
            result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        preview_html = self.preview_target_file_path(result["preview_targets"][0]).read_text(encoding="utf-8")
        self.assertIn(f'id="doc-{first_row["id"]}"', preview_html)
        self.assertIn(f'id="doc-{second_row["id"]}"', preview_html)
        self.assertIn("Kickoff thread for launch planning.", preview_html)
        self.assertIn("Follow-up from the same Teams space.", preview_html)

    def test_ingest_pst_routes_teams_messages_to_chat_and_skips_system_folders(self) -> None:
        self.write_fake_pst_file()
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-team-001",
                subject=None,
                body_text="hey there",
                folder_path="Top of Personal Folders/user (Primary)/TeamsMessagesData",
                author="Sergey Demyanov",
                recipients=None,
                date_created="2026-04-15T09:00:00Z",
            ),
            self.build_fake_pst_message(
                source_item_id="pst-spool-001",
                subject=None,
                body_text="",
                folder_path="Top of Personal Folders/user (Primary)/SubstrateFiles/SPOOLS",
                author=None,
                recipients=None,
                date_created="2026-04-15T09:01:00Z",
            ),
            self.build_fake_pst_message(
                source_item_id="pst-meeting-system-001",
                subject=None,
                body_text="<addmember><eventtime>1713916258178</eventtime></addmember>",
                folder_path="Top of Personal Folders/user (Primary)/SkypeSpacesData/TeamsMeetings",
                author="19:meeting@unq.gbl.spaces",
                recipients=None,
                date_created="2026-04-15T09:02:00Z",
            ),
            self.build_fake_pst_message(
                source_item_id="pst-email-001",
                subject="Ordinary inbox message",
                body_text="Parent message body",
                folder_path="Top of Information Store/Inbox",
                author="Alice Example <alice@example.com>",
                recipients="Bob Example <bob@example.com>",
                date_created="2026-04-15T09:03:00Z",
            ),
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["pst_messages_created"], 2)
        self.assertEqual(ingest_result["workspace_parent_documents"], 2)

        chat_rel_path = retriever_tools.pst_message_rel_path("mailbox.pst", "pst-team-001")
        email_rel_path = retriever_tools.pst_message_rel_path("mailbox.pst", "pst-email-001")
        chat_row = self.fetch_document_row(chat_rel_path)
        email_row = self.fetch_document_row(email_rel_path)

        self.assertEqual(chat_row["content_type"], "Chat")
        self.assertIsNone(chat_row["author"])
        self.assertEqual(chat_row["custodian"], "mailbox")
        self.assertEqual(chat_row["participants"], "Sergey Demyanov")
        self.assertEqual(chat_row["title"], "hey there")
        self.assertEqual(chat_row["source_folder_path"], "Top of Personal Folders/user (Primary)/TeamsMessagesData")
        self.assertIsNotNone(chat_row["conversation_id"])
        self.assertEqual(email_row["content_type"], "Email")
        self.assertEqual(email_row["custodian"], "mailbox")

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            skipped_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM documents
                    WHERE rel_path IN (?, ?)
                    """,
                    (
                        retriever_tools.pst_message_rel_path("mailbox.pst", "pst-spool-001"),
                        retriever_tools.pst_message_rel_path("mailbox.pst", "pst-meeting-system-001"),
                    ),
                ).fetchone()["count"]
            )
        finally:
            connection.close()
        self.assertEqual(skipped_count, 0)

        search_result = retriever_tools.search(self.root, "hey there", None, None, None, 1, 20)
        result = next(item for item in search_result["results"] if item["id"] == chat_row["id"])
        self.assertTrue(
            result["preview_rel_path"].startswith(
                f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/previews/conversations/"
            )
        )
        self.assertTrue(result["preview_rel_path"].endswith(f"#doc-{chat_row['id']}"))
        preview_html = self.preview_target_file_path(result["preview_targets"][0]).read_text(encoding="utf-8")
        self.assertIn("Conversation document", preview_html)
        self.assertIn("Sergey Demyanov", preview_html)
        self.assertIn("hey there", preview_html)

    def test_ingest_pst_calendar_folder_uses_calendar_content_type(self) -> None:
        self.write_fake_pst_file()
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-calendar-001",
                subject="Labor Day",
                body_text="",
                folder_path="Top of Information Store/Calendar/United States holidays",
                author="sergey",
                recipients=None,
                date_created="2026-09-07T00:00:00Z",
                message_class="IPM.Appointment",
            )
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["pst_messages_created"], 1)

        row = self.fetch_document_row(retriever_tools.pst_message_rel_path("mailbox.pst", "pst-calendar-001"))
        self.assertEqual(row["content_type"], "Calendar")
        self.assertEqual(row["custodian"], "mailbox")
        self.assertEqual(row["title"], "Labor Day")
        self.assertEqual(row["subject"], "Labor Day")
        self.assertEqual(row["author"], "sergey")

        search_result = retriever_tools.search(
            self.root,
            "",
            [["content_type", "eq", "Calendar"], ["source_kind", "eq", retriever_tools.PST_SOURCE_KIND]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(search_result["total_hits"], 1)
        preview_html = Path(search_result["results"][0]["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("<title>Labor Day</title>", preview_html)
        self.assertIn("<h1>Labor Day</h1>", preview_html)
        self.assertIn("Labor Day", preview_html)
        self.assertIn("Sep 7, 2026 12:00 AM UTC", preview_html)

    def test_unchanged_pst_source_skips_without_reparsing(self) -> None:
        self.write_fake_pst_file()
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-msg-001",
                subject="PST Parent",
                body_text="Parent message body",
            )
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(first_ingest["new"], 1)
        with mock.patch.object(retriever_tools, "iter_pst_messages", side_effect=AssertionError("PST iterator should not run on unchanged source")):
            second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(second_ingest["skipped"], 1)
        self.assertEqual(second_ingest["pst_sources_skipped"], 1)
        self.assertEqual(second_ingest["failed"], 0)
        browse_results = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_results["total_hits"], 1)

    def test_changed_pst_reingest_preserves_control_numbers_and_retires_removed_messages(self) -> None:
        pst_path = self.write_fake_pst_file(content=b"pst-v1")
        first_messages = [
            self.build_fake_pst_message(
                source_item_id="pst-msg-001",
                subject="Original PST Parent",
                body_text="Parent v1",
                attachment_name="notes.txt",
                attachment_text="stable attachment body",
            ),
            self.build_fake_pst_message(
                source_item_id="pst-msg-002",
                subject="Removed later",
                body_text="Remove me",
            ),
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(first_messages)):
            first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(first_ingest["pst_messages_created"], 2)
        parent_rel_path = retriever_tools.pst_message_rel_path("mailbox.pst", "pst-msg-001")
        removed_rel_path = retriever_tools.pst_message_rel_path("mailbox.pst", "pst-msg-002")
        parent_row = self.fetch_document_row(parent_rel_path)
        child_row = self.fetch_child_rows(parent_row["id"])[0]
        removed_row = self.fetch_document_row(removed_rel_path)
        retriever_tools.set_field(self.root, parent_row["id"], "title", "Manual PST Title")

        pst_path.write_bytes(b"pst-v2-expanded")
        second_messages = [
            self.build_fake_pst_message(
                source_item_id="pst-msg-001",
                subject="Updated PST Parent",
                body_text="Parent v2",
                attachment_name="notes.txt",
                attachment_text="stable attachment body",
            )
        ]
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(second_messages)):
            second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(second_ingest["updated"], 1)
        self.assertEqual(second_ingest["pst_messages_updated"], 1)
        self.assertEqual(second_ingest["pst_messages_deleted"], 1)

        updated_parent = self.fetch_document_row(parent_rel_path)
        updated_child = self.fetch_child_rows(updated_parent["id"])[0]
        retired_row = self.fetch_document_row(removed_rel_path)
        self.assertEqual(updated_parent["control_number"], parent_row["control_number"])
        self.assertEqual(updated_parent["title"], "Manual PST Title")
        self.assertEqual(updated_child["id"], child_row["id"])
        self.assertEqual(updated_child["control_number"], child_row["control_number"])
        self.assertEqual(retired_row["lifecycle_status"], "deleted")

    def test_missing_pst_source_marks_messages_and_children_missing(self) -> None:
        pst_path = self.write_fake_pst_file()
        messages = [
            self.build_fake_pst_message(
                source_item_id="pst-msg-001",
                subject="PST Parent",
                body_text="Parent body",
                attachment_name="notes.txt",
                attachment_text="attachment body",
            )
        ]

        retriever_tools.bootstrap(self.root)
        with mock.patch.object(retriever_tools, "iter_pst_messages", return_value=iter(messages)):
            first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(first_ingest["pst_messages_created"], 1)
        parent_rel_path = retriever_tools.pst_message_rel_path("mailbox.pst", "pst-msg-001")
        parent_row = self.fetch_document_row(parent_rel_path)
        child_row = self.fetch_child_rows(parent_row["id"])[0]

        pst_path.unlink()
        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(second_ingest["missing"], 1)
        self.assertEqual(second_ingest["pst_sources_missing"], 1)
        self.assertEqual(second_ingest["pst_documents_missing"], 2)
        missing_parent = self.fetch_document_row(parent_rel_path)
        missing_child = self.fetch_document_by_id(child_row["id"])
        self.assertEqual(missing_parent["lifecycle_status"], "missing")
        self.assertEqual(missing_child["lifecycle_status"], "missing")

    def test_plain_ingest_skips_detected_production_roots(self) -> None:
        self.write_production_fixture()
        loose_file = self.root / "notes.txt"
        loose_file.write_text("loose workspace note\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        self.assertEqual(ingest_result["new"], 1)
        self.assertEqual(ingest_result["failed"], 0)
        self.assertEqual(ingest_result["skipped_production_roots"], ["Synthetic_Production"])
        self.assertIn("use ingest-production", ingest_result["warnings"][0])

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_result["total_hits"], 1)
        self.assertEqual(browse_result["results"][0]["file_name"], "notes.txt")

    def test_ingest_production_creates_logical_documents_bates_lookup_and_preview_precedence(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)

        self.assertEqual(ingest_result["created"], 4)
        self.assertEqual(ingest_result["retired"], 0)
        self.assertEqual(ingest_result["docs_missing_linked_text"], 0)
        self.assertEqual(ingest_result["families_reconstructed"], 1)
        self.assertEqual(ingest_result["failures"], [])

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            production_row = connection.execute("SELECT * FROM productions WHERE rel_root = ?", ("Synthetic_Production",)).fetchone()
            source_parts = connection.execute(
                """
                SELECT part_kind, rel_source_path
                FROM document_source_parts
                WHERE document_id = (SELECT id FROM documents WHERE control_number = 'PDX000004')
                ORDER BY part_kind ASC, ordinal ASC
                """
            ).fetchall()
            dataset_row = connection.execute(
                """
                SELECT *
                FROM datasets
                WHERE id = (SELECT dataset_id FROM productions WHERE rel_root = ?)
                """,
                ("Synthetic_Production",),
            ).fetchone()
        finally:
            connection.close()

        self.assertIsNotNone(production_row)
        self.assertEqual(production_row["production_name"], "Synthetic_Production")
        self.assertIsNotNone(production_row["dataset_id"])
        self.assertIsNotNone(dataset_row)
        self.assertEqual(dataset_row["source_kind"], retriever_tools.PRODUCTION_SOURCE_KIND)
        self.assertEqual(dataset_row["dataset_locator"], "Synthetic_Production")
        self.assertEqual(dataset_row["dataset_name"], "Synthetic_Production")
        self.assertTrue(any(row["part_kind"] == "native" for row in source_parts))

        parent_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000001.logical"
        )
        child_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000003.logical"
        )
        native_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000004.logical"
        )
        image_only_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )

        self.assertEqual(parent_row["control_number"], "PDX000001")
        self.assertEqual(parent_row["begin_bates"], "PDX000001")
        self.assertEqual(parent_row["end_bates"], "PDX000002")
        self.assertEqual(parent_row["source_kind"], retriever_tools.PRODUCTION_SOURCE_KIND)
        self.assertEqual(parent_row["dataset_id"], production_row["dataset_id"])
        self.assertEqual(parent_row["content_type"], "Email")
        self.assertEqual(parent_row["author"], "Elena Steven <elena@example.com>")
        self.assertEqual(parent_row["date_created"], "2026-04-14T10:32:00Z")
        self.assertEqual(parent_row["title"], "Attachment Handling")
        self.assertEqual(parent_row["subject"], "Attachment Handling")
        self.assertEqual(parent_row["recipients"], "Harry Montoro <harry@example.com>")
        self.assertEqual(
            parent_row["participants"],
            "Elena Steven <elena@example.com>, Harry Montoro <harry@example.com>",
        )
        self.assertEqual(child_row["parent_document_id"], parent_row["id"])
        self.assertEqual(child_row["dataset_id"], production_row["dataset_id"])
        self.assertEqual(child_row["content_type"], "Email")
        self.assertEqual(child_row["author"], "Review Team")
        self.assertEqual(child_row["date_created"], "2026-04-14T09:00:00Z")
        self.assertEqual(child_row["title"], "Case status update")
        self.assertEqual(native_row["dataset_id"], production_row["dataset_id"])
        self.assertEqual(native_row["file_name"], "PDX000004.pdf")
        self.assertEqual(native_row["content_type"], "E-Doc")
        self.assertEqual(image_only_row["dataset_id"], production_row["dataset_id"])
        self.assertEqual(image_only_row["text_status"], "empty")
        self.assertEqual(image_only_row["content_type"], "Image")
        self.assertEqual(image_only_row["page_count"], 2)

        exact_search = retriever_tools.search(self.root, "PDX000001", None, None, None, 1, 20)
        self.assertEqual(exact_search["total_hits"], 1)
        self.assertEqual(exact_search["results"][0]["control_number"], "PDX000001")
        self.assertEqual(exact_search["results"][0]["attachment_count"], 1)
        self.assertEqual(exact_search["results"][0]["attachments"][0]["control_number"], "PDX000003")

        containing_search = retriever_tools.search(self.root, "PDX000002", None, None, None, 1, 20)
        self.assertEqual(containing_search["total_hits"], 1)
        self.assertEqual(containing_search["results"][0]["control_number"], "PDX000001")

        range_search = retriever_tools.search(self.root, "PDX000002-PDX000005", None, None, None, 1, 20)
        self.assertEqual(
            [item["control_number"] for item in range_search["results"]],
            ["PDX000001", "PDX000003", "PDX000004", "PDX000005"],
        )

        production_filtered = retriever_tools.search(
            self.root,
            "",
            [["source_kind", "eq", "production"], ["production_name", "contains", "Synthetic"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(production_filtered["total_hits"], 4)
        dataset_filtered = retriever_tools.search(
            self.root,
            "",
            [["dataset", "eq", "Synthetic_Production"]],
            None,
            None,
            1,
            20,
        )
        self.assertEqual(dataset_filtered["total_hits"], 4)
        self.assertTrue(all(item["dataset_name"] == "Synthetic_Production" for item in dataset_filtered["results"]))

        native_search = retriever_tools.search(self.root, "Native-backed production doc", None, None, None, 1, 20)
        native_result = next(item for item in native_search["results"] if item["control_number"] == "PDX000004")
        self.assertEqual(native_result["preview_rel_path"], "Synthetic_Production/NATIVES/NAT001/PDX000004.pdf")
        self.assertEqual(native_result["preview_targets"][0]["preview_type"], "native")
        self.assertEqual(native_result["production_name"], "Synthetic_Production")

        html_search = retriever_tools.search(self.root, "Discuss attachment handling", None, None, None, 1, 20)
        html_result = next(item for item in html_search["results"] if item["control_number"] == "PDX000001")
        self.assertTrue(html_result["preview_rel_path"].endswith(".html"))
        preview_html = Path(html_result["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("<title>Attachment Handling</title>", preview_html)
        self.assertIn("<h1>Attachment Handling</h1>", preview_html)
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            child_preview_target = retriever_tools.default_preview_target(self.paths, child_row, connection)
            child_preview_abs_path = str(
                child_preview_target.get("file_abs_path") or child_preview_target.get("abs_path") or ""
            ).split("#", 1)[0]
        finally:
            connection.close()
        expected_href = retriever_tools.urllib_request.pathname2url(
            os.path.relpath(child_preview_abs_path, start=str(Path(html_result["preview_targets"][0]["abs_path"]).parent))
        )
        self.assertIn("<h2>Attachments</h2>", preview_html)
        self.assertIn(f'href="{expected_href}"', preview_html)
        self.assertIn("PDX000001", preview_html)
        self.assertIn("Discuss attachment handling.", preview_html)
        self.assertIn("data:image/png;base64,", preview_html)

    def test_ingest_production_falls_back_when_loadfile_paths_include_missing_volume_prefix(self) -> None:
        production_root = self.write_production_fixture(loadfile_volume_prefix="Sunrise_Production_01")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)

        self.assertEqual(ingest_result["created"], 4)
        self.assertEqual(ingest_result["docs_missing_linked_text"], 0)
        self.assertEqual(ingest_result["docs_missing_linked_images"], 1)
        self.assertEqual(ingest_result["docs_missing_linked_natives"], 0)
        self.assertEqual(ingest_result["page_images_linked"], 5)
        self.assertEqual(ingest_result["failures"], [])

        parent_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000001.logical"
        )
        native_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000004.logical"
        )
        self.assertEqual(parent_row["content_type"], "Email")
        self.assertEqual(parent_row["author"], "Elena Steven <elena@example.com>")
        self.assertEqual(parent_row["date_created"], "2026-04-14T10:32:00Z")
        self.assertEqual(parent_row["title"], "Attachment Handling")
        self.assertEqual(parent_row["text_status"], "ok")
        self.assertEqual(parent_row["page_count"], 2)
        self.assertEqual(native_row["file_name"], "PDX000004.pdf")

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            parent_source_parts = connection.execute(
                """
                SELECT part_kind, rel_source_path
                FROM document_source_parts
                WHERE document_id = ?
                ORDER BY part_kind ASC, ordinal ASC
                """,
                (parent_row["id"],),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(
            [row["rel_source_path"] for row in parent_source_parts if row["part_kind"] == "text"],
            ["Synthetic_Production/TEXT/TEXT001/PDX000001.txt"],
        )
        self.assertEqual(
            [row["rel_source_path"] for row in parent_source_parts if row["part_kind"] == "image"],
            [
                "Synthetic_Production/IMAGES/IMG001/PDX000001.tif",
                "Synthetic_Production/IMAGES/IMG001/PDX000002.tif",
            ],
        )

    def test_ingest_production_rerun_retires_missing_rows(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        first_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(first_result["created"], 4)

        data_path = production_root / "DATA" / "Synthetic_Production.dat"
        lines = data_path.read_bytes().splitlines()
        filtered = [line for line in lines if b"PDX000005" not in line]
        data_path.write_bytes(b"\r\n".join(filtered) + b"\r\n")

        second_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(second_result["retired"], 1)

        retired_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000005.logical"
        )
        self.assertEqual(retired_row["lifecycle_status"], "deleted")

    def test_search_docs_cli_alias_matches_search(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("termination notice appears here\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        search_exit, search_payload, _, _ = self.run_cli("search", str(self.root), "termination")
        search_docs_exit, search_docs_payload, _, _ = self.run_cli("search-docs", str(self.root), "termination")

        self.assertEqual(search_exit, 0)
        self.assertEqual(search_docs_exit, 0)
        self.assertIsNotNone(search_payload)
        self.assertIsNotNone(search_docs_payload)
        self.assertEqual(search_docs_payload["query"], search_payload["query"])
        self.assertEqual(search_docs_payload["sort"], search_payload["sort"])
        self.assertEqual(search_docs_payload["total_hits"], search_payload["total_hits"])
        self.assertEqual(
            [item["id"] for item in search_docs_payload["results"]],
            [item["id"] for item in search_payload["results"]],
        )

    def test_search_cli_defaults_to_compact_payload_with_verbose_escape_hatch(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("termination notice appears here\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        compact_exit, compact_payload, _, _ = self.run_cli("search", str(self.root), "termination")
        verbose_exit, verbose_payload, _, _ = self.run_cli("search", str(self.root), "termination", "--verbose")

        self.assertEqual(compact_exit, 0)
        self.assertEqual(verbose_exit, 0)
        self.assertIsNotNone(compact_payload)
        self.assertIsNotNone(verbose_payload)

        compact_result = compact_payload["results"][0]
        verbose_result = verbose_payload["results"][0]

        self.assertNotIn("preview_targets", compact_result)
        self.assertNotIn("manual_field_locks", compact_result)
        self.assertIn("content_type", compact_result["metadata"])
        self.assertIn("updated_at", compact_result["metadata"])
        self.assertNotIn("page_count", compact_result["metadata"])
        self.assertIn("preview_abs_path", compact_result)
        self.assertIn("preview_targets", verbose_result)
        self.assertIn("manual_field_locks", verbose_result)
        self.assertIn("page_count", verbose_result["metadata"])

    def test_catalog_lists_dataset_name_and_date_granularities(self) -> None:
        retriever_tools.bootstrap(self.root)
        retriever_tools.add_field(self.root, "effective_date", "date", "Contract effective date")

        exit_code, payload, _, _ = self.run_cli("catalog", str(self.root))

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        intrinsic = {item["name"]: item for item in payload["intrinsic"]}
        custom = {item["name"]: item for item in payload["custom"]}
        virtual = {item["name"]: item for item in payload["virtual"]}

        self.assertIn("date_created", intrinsic)
        self.assertEqual(intrinsic["date_created"]["date_granularities"], ["year", "quarter", "month", "week"])
        self.assertEqual(custom["effective_date"]["type"], "date")
        self.assertEqual(custom["effective_date"]["date_granularities"], ["year", "quarter", "month", "week"])
        self.assertTrue(virtual["dataset_name"]["aggregatable"])
        self.assertEqual(virtual["dataset_name"]["description"], retriever_tools.VIRTUAL_FIELD_DESCRIPTIONS["dataset_name"])

    def test_export_csv_cli_writes_requested_fields_for_filtered_collection(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("beta body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        alpha_row = self.fetch_document_row("alpha.txt")
        self.assertEqual(self.run_cli("add-field", str(self.root), "effective_date", "date")[0], 0)
        self.assertEqual(
            self.run_cli(
                "set-field",
                str(self.root),
                "--doc-id",
                str(alpha_row["id"]),
                "--field",
                "effective_date",
                "--value",
                "2026-04-16",
            )[0],
            0,
        )

        export_path = self.root / ".retriever" / "exports" / "review.csv"
        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            "review.csv",
            "--field",
            "dataset_name",
            "--field",
            "control_number",
            "--field",
            "effective_date",
            "--field",
            "file_name",
            "--filter",
            "file_name",
            "eq",
            "alpha.txt",
            "--sort",
            "file_name",
            "--order",
            "asc",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 1)
        self.assertEqual(payload["output_rel_path"], ".retriever/exports/review.csv")
        self.assertEqual(payload["selector"]["mode"], "search")
        self.assertEqual([field["field_name"] for field in payload["fields"]], ["dataset_name", "control_number", "effective_date", "file_name"])

        with export_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["dataset_name", "control_number", "effective_date", "file_name"])
        self.assertEqual(rows[1], [self.root.name, alpha_row["control_number"], "2026-04-16", "alpha.txt"])
        self.assertEqual(len(rows), 2)

    def test_export_csv_select_from_scope_exports_current_scope(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("beta body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        scope_exit, scope_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(scope_exit, 0)
        self.assertIsNotNone(scope_payload)

        export_path = self.root / ".retriever" / "exports" / "scope.csv"
        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            "scope.csv",
            "--field",
            "file_name",
            "--select-from-scope",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 1)
        self.assertEqual(payload["selector"]["mode"], "scope_search")
        self.assertTrue(payload["selector"]["selected_from_scope"])
        self.assertEqual(payload["selector"]["scope"]["keyword"], "alpha")

        with export_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["file_name"])
        self.assertEqual(rows[1], ["alpha.txt"])
        self.assertEqual(len(rows), 2)

    def test_export_csv_select_from_scope_and_narrows_with_explicit_filter(self) -> None:
        (self.root / "alpha-one.txt").write_text("alpha body one\n", encoding="utf-8")
        (self.root / "alpha-two.txt").write_text("alpha body two\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        scope_exit, scope_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(scope_exit, 0)
        self.assertIsNotNone(scope_payload)
        self.assertEqual(scope_payload["total_hits"], 2)

        export_path = self.root / ".retriever" / "exports" / "scope-narrowed.csv"
        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            "scope-narrowed.csv",
            "--field",
            "file_name",
            "--select-from-scope",
            "--filter",
            "file_name = 'alpha-two.txt'",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 1)
        self.assertEqual(payload["selector"]["mode"], "scope_search")
        self.assertEqual(payload["selector"]["scope"]["keyword"], "alpha")
        self.assertIn("alpha-two.txt", payload["selector"]["scope"]["filter"])

        with export_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["file_name"])
        self.assertEqual(rows[1], ["alpha-two.txt"])
        self.assertEqual(len(rows), 2)

    def test_export_csv_rejects_combining_doc_ids_with_scope_selector(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("alpha.txt")
        scope_exit, _, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(scope_exit, 0)

        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            "scope-doc-id.csv",
            "--field",
            "file_name",
            "--select-from-scope",
            "--doc-id",
            str(row["id"]),
        )

        self.assertEqual(exit_code, 2)
        self.assertIsNotNone(payload)
        self.assertIn("query/filter/scope selectors", payload["error"])

    def test_export_csv_cli_preserves_explicit_document_order(self) -> None:
        (self.root / "first.txt").write_text("first body\n", encoding="utf-8")
        (self.root / "second.txt").write_text("second body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        first_row = self.fetch_document_row("first.txt")
        second_row = self.fetch_document_row("second.txt")

        export_path = self.root / ".retriever" / "exports" / "ordered.csv"
        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            "ordered.csv",
            "--field",
            "file_name",
            "--field",
            "control_number",
            "--doc-id",
            str(second_row["id"]),
            "--doc-id",
            str(first_row["id"]),
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 2)
        self.assertEqual(payload["selector"]["mode"], "document_ids")
        self.assertEqual(payload["selector"]["document_ids"], [second_row["id"], first_row["id"]])

        with export_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["file_name", "control_number"])
        self.assertEqual(rows[1], ["second.txt", second_row["control_number"]])
        self.assertEqual(rows[2], ["first.txt", first_row["control_number"]])

    def test_export_csv_relative_output_does_not_get_reingested(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("beta body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        first_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(first_ingest["new"], 2)

        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            "nested/review.csv",
            "--field",
            "file_name",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["output_rel_path"], ".retriever/exports/nested/review.csv")

        second_ingest = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(second_ingest["new"], 0)
        self.assertEqual(second_ingest["skipped"], 2)

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_result["total_hits"], 2)
        self.assertEqual(sorted(item["file_name"] for item in browse_result["results"]), ["alpha.txt", "beta.txt"])

    def test_export_csv_rejects_workspace_output_path_outside_retriever_state_dir(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        unsafe_path = self.root / "exports" / "review.csv"
        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            str(unsafe_path),
            "--field",
            "file_name",
        )

        self.assertEqual(exit_code, 2)
        self.assertIsNotNone(payload)
        self.assertIn("must live under", payload["error"])

    def test_export_csv_doc_id_mode_rejects_rows_without_dataset_membership(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("sample dataset body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        delete_exit, delete_payload, _, _ = self.run_cli(
            "delete-dataset",
            str(self.root),
            "--dataset-name",
            self.root.name,
        )
        self.assertEqual(delete_exit, 0)
        self.assertIsNotNone(delete_payload)
        self.assertEqual(delete_payload["documents_without_dataset_memberships"], [row["id"]])

        browse_result = retriever_tools.search(self.root, "", None, None, None, 1, 20)
        self.assertEqual(browse_result["total_hits"], 0)

        exit_code, payload, _, _ = self.run_cli(
            "export-csv",
            str(self.root),
            "doc-id.csv",
            "--field",
            "file_name",
            "--doc-id",
            str(row["id"]),
        )

        self.assertEqual(exit_code, 2)
        self.assertIsNotNone(payload)
        self.assertIn("not visible because they have no dataset memberships", payload["error"])

    def test_export_previews_expands_reply_selection_to_full_email_conversation(self) -> None:
        root_path = self.root / "root.eml"
        reply_path = self.root / "reply.eml"
        self.write_email_message(
            root_path,
            subject="Status Update",
            body_text="Root message body",
            message_id="<root@example.com>",
            date_created="Tue, 14 Apr 2026 10:00:00 +0000",
        )
        self.write_email_message(
            reply_path,
            subject="Re: Status Update",
            body_text="Reply message body",
            message_id="<reply@example.com>",
            in_reply_to="<root@example.com>",
            references="<root@example.com>",
            date_created="Tue, 14 Apr 2026 11:00:00 +0000",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["failed"], 0)

        root_row = self.fetch_document_row("root.eml")
        reply_row = self.fetch_document_row("reply.eml")

        exit_code, payload, _, _ = self.run_cli(
            "export-previews",
            str(self.root),
            "email-preview",
            "--doc-id",
            str(reply_row["id"]),
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["selected_document_count"], 1)
        self.assertEqual(payload["unit_count"], 1)
        self.assertEqual(payload["output_rel_path"], ".retriever/exports/email-preview")
        self.assertEqual(len(payload["document_targets"]), 1)
        self.assertEqual(payload["document_targets"][0]["document_id"], reply_row["id"])
        self.assertEqual(payload["document_targets"][0]["target_fragment"], f"doc-{reply_row['id']}")

        unit = payload["units"][0]
        self.assertEqual(unit["unit_kind"], "email_conversation")
        self.assertEqual(unit["selected_document_ids"], [reply_row["id"]])
        self.assertEqual(unit["document_ids"], [root_row["id"], reply_row["id"]])
        self.assertEqual(unit["output_rel_path"], payload["document_targets"][0]["output_rel_path"])

        unit_path = Path(unit["output_path"])
        index_path = Path(payload["index_path"])
        manifest_path = Path(payload["manifest_path"])
        self.assertTrue(unit_path.exists())
        self.assertTrue(index_path.exists())
        self.assertTrue(manifest_path.exists())

        unit_html = unit_path.read_text(encoding="utf-8")
        index_html = index_path.read_text(encoding="utf-8")
        self.assertIn("Root message body", unit_html)
        self.assertIn("Reply message body", unit_html)
        self.assertIn(f'id="doc-{root_row["id"]}"', unit_html)
        self.assertIn(f'id="doc-{reply_row["id"]}"', unit_html)
        self.assertIn("units/", index_html)
        self.assertIn(f"#doc-{reply_row['id']}", index_html)

        units_dir = unit_path.parent
        self.assertEqual(sorted(path.name for path in units_dir.glob("*.html")), [unit_path.name])

    def test_export_previews_merges_contiguous_slack_documents_and_splits_gaps(self) -> None:
        export_root = self.root / "data" / "slack"
        export_root.mkdir(parents=True)
        (export_root / "users.json").write_text(
            json.dumps(
                [
                    {
                        "id": "U04SERGEY1",
                        "name": "sergey",
                        "profile": {
                            "real_name": "Sergey Demyanov",
                            "display_name": "Sergey",
                        },
                    },
                    {
                        "id": "U04MAX0001",
                        "name": "maksim",
                        "profile": {
                            "real_name": "Maksim Faleev",
                            "display_name": "Maksim",
                        },
                    },
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (export_root / "channels.json").write_text(
            json.dumps(
                [
                    {
                        "id": "C04GENERAL1",
                        "name": "general",
                        "is_channel": True,
                    }
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        channel_dir = export_root / "general"
        channel_dir.mkdir()
        thread_ts = "1671235434.237949"
        (channel_dir / "2022-12-16.json").write_text(
            json.dumps(
                [
                    {
                        "type": "message",
                        "text": "Kickoff thread",
                        "user": "U04SERGEY1",
                        "ts": thread_ts,
                        "thread_ts": thread_ts,
                        "reply_count": 1,
                    },
                    {
                        "type": "message",
                        "text": "Standalone channel update",
                        "user": "U04MAX0001",
                        "ts": "1671235834.237949",
                    },
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (channel_dir / "2022-12-17.json").write_text(
            json.dumps(
                [
                    {
                        "type": "message",
                        "text": "Following up on kickoff",
                        "user": "U04MAX0001",
                        "ts": "1671321834.237949",
                        "thread_ts": thread_ts,
                    }
                ],
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["failed"], 0)

        day_one_row = self.fetch_document_row("data/slack/general/2022-12-16.json")
        day_two_row = self.fetch_document_row("data/slack/general/2022-12-17.json")
        child_row = self.fetch_document_row(
            retriever_tools.slack_reply_thread_rel_path("C04GENERAL1", thread_ts)
        )

        contiguous_exit, contiguous_payload, _, _ = self.run_cli(
            "export-previews",
            str(self.root),
            "slack-contiguous",
            "--doc-id",
            str(day_one_row["id"]),
            "--doc-id",
            str(child_row["id"]),
        )

        self.assertEqual(contiguous_exit, 0)
        self.assertIsNotNone(contiguous_payload)
        self.assertEqual(contiguous_payload["unit_count"], 1)
        self.assertEqual(len(contiguous_payload["document_targets"]), 2)
        contiguous_unit = contiguous_payload["units"][0]
        self.assertEqual(contiguous_unit["unit_kind"], "conversation_run")
        self.assertEqual(contiguous_unit["selected_document_ids"], [day_one_row["id"], child_row["id"]])
        self.assertEqual(contiguous_unit["document_ids"], [day_one_row["id"], child_row["id"]])
        self.assertEqual(
            {
                target["output_rel_path"]
                for target in contiguous_payload["document_targets"]
            },
            {contiguous_unit["output_rel_path"]},
        )
        contiguous_html = Path(contiguous_unit["output_path"]).read_text(encoding="utf-8")
        self.assertIn("Kickoff thread", contiguous_html)
        self.assertIn("Following up on kickoff", contiguous_html)
        self.assertIn(f'id="doc-{day_one_row["id"]}"', contiguous_html)
        self.assertIn(f'id="doc-{child_row["id"]}"', contiguous_html)
        self.assertNotIn(f'id="doc-{day_two_row["id"]}"', contiguous_html)

        split_exit, split_payload, _, _ = self.run_cli(
            "export-previews",
            str(self.root),
            "slack-split",
            "--doc-id",
            str(day_two_row["id"]),
            "--doc-id",
            str(child_row["id"]),
        )

        self.assertEqual(split_exit, 0)
        self.assertIsNotNone(split_payload)
        self.assertEqual(split_payload["unit_count"], 2)
        self.assertEqual(
            [unit["selected_document_ids"] for unit in split_payload["units"]],
            [[day_two_row["id"]], [child_row["id"]]],
        )
        self.assertEqual(
            [unit["document_ids"] for unit in split_payload["units"]],
            [[day_two_row["id"]], [child_row["id"]]],
        )
        self.assertEqual(
            len({target["output_rel_path"] for target in split_payload["document_targets"]}),
            2,
        )
        first_split_html = Path(split_payload["units"][0]["output_path"]).read_text(encoding="utf-8")
        second_split_html = Path(split_payload["units"][1]["output_path"]).read_text(encoding="utf-8")
        self.assertIn(f'id="doc-{day_two_row["id"]}"', first_split_html)
        self.assertNotIn(f'id="doc-{day_one_row["id"]}"', first_split_html)
        self.assertIn(f'id="doc-{child_row["id"]}"', second_split_html)
        self.assertNotIn(f'id="doc-{day_one_row["id"]}"', second_split_html)

    def test_export_archive_cli_includes_previews_and_attachment_family(self) -> None:
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Archive export",
            body_text="Parent email body text.",
            attachment_name="notes.txt",
            attachment_text="confidential attachment detail",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        parent_row = self.fetch_document_row("thread.eml")
        child_row = self.fetch_child_rows(parent_row["id"])[0]
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            parent_preview_rows = connection.execute(
                """
                SELECT rel_preview_path
                FROM document_previews
                WHERE document_id = ?
                ORDER BY ordinal ASC, id ASC
                """,
                (parent_row["id"],),
            ).fetchall()
        finally:
            connection.close()
        self.assertGreaterEqual(len(parent_preview_rows), 1)
        parent_preview_archive_path = str(Path(".retriever") / str(parent_preview_rows[0]["rel_preview_path"]))

        export_path = self.root / ".retriever" / "exports" / "family.zip"
        exit_code, payload, _, _ = self.run_cli(
            "export-archive",
            str(self.root),
            "family.zip",
            "--keyword",
            "confidential",
            "--family-mode",
            "with_family",
            "--limit",
            "1",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 2)
        self.assertEqual(payload["output_rel_path"], ".retriever/exports/family.zip")
        self.assertEqual(payload["family_mode"], "with_family")
        self.assertFalse(payload["portable_workspace"])
        self.assertEqual(payload["selector"]["keyword"], "confidential")

        with zipfile.ZipFile(export_path, "r") as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read(".retriever/export-manifest.json").decode("utf-8"))

        self.assertIn("thread.eml", names)
        self.assertIn(child_row["rel_path"], names)
        self.assertIn(parent_preview_archive_path, names)
        self.assertEqual(manifest["document_count"], 2)
        self.assertEqual(manifest["family_mode"], "with_family")
        self.assertEqual(manifest["warnings"], [])

        manifest_by_document_id = {
            int(item["document_id"]): item
            for item in manifest["documents"]
        }
        self.assertEqual(set(manifest_by_document_id), {int(parent_row["id"]), int(child_row["id"])})
        self.assertEqual(
            manifest_by_document_id[int(child_row["id"])]["inclusion_reason"]["direct_reasons"][0]["type"],
            "keyword",
        )
        self.assertEqual(
            manifest_by_document_id[int(parent_row["id"])]["inclusion_reason"]["family_seed_document_ids"],
            [int(child_row["id"])],
        )

    def test_export_archive_select_from_scope_exports_current_scope(self) -> None:
        (self.root / "alpha.txt").write_text("alpha body\n", encoding="utf-8")
        (self.root / "beta.txt").write_text("beta body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        scope_exit, scope_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(scope_exit, 0)
        self.assertIsNotNone(scope_payload)

        export_path = self.root / ".retriever" / "exports" / "scope.zip"
        exit_code, payload, _, _ = self.run_cli(
            "export-archive",
            str(self.root),
            "scope.zip",
            "--select-from-scope",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 1)
        self.assertEqual(payload["selector"]["keyword"], "alpha")

        with zipfile.ZipFile(export_path, "r") as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read(".retriever/export-manifest.json").decode("utf-8"))

        self.assertIn("alpha.txt", names)
        self.assertNotIn("beta.txt", names)
        self.assertEqual(manifest["document_count"], 1)
        self.assertEqual(manifest["selector"]["keyword"], "alpha")

    def test_export_archive_portable_workspace_supports_selected_child_with_parent_context_stub(self) -> None:
        email_path = self.root / "thread.eml"
        self.write_email_message(
            email_path,
            subject="Portable archive",
            body_text="Parent email body text.",
            attachment_name="notes.txt",
            attachment_text="confidential attachment detail",
        )

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        parent_row = self.fetch_document_row("thread.eml")
        child_row = self.fetch_child_rows(parent_row["id"])[0]

        export_path = self.root / ".retriever" / "exports" / "portable-child.zip"
        exit_code, payload, _, _ = self.run_cli(
            "export-archive",
            str(self.root),
            "portable-child.zip",
            "--filter",
            f"id = {child_row['id']}",
            "--portable-workspace",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 1)
        self.assertTrue(payload["portable_workspace"])
        self.assertEqual(payload["selector"]["filter"], f"(id = {child_row['id']})")

        with zipfile.ZipFile(export_path, "r") as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read(".retriever/export-manifest.json").decode("utf-8"))
            self.assertIn(".retriever/retriever.db", names)
            self.assertIn(child_row["rel_path"], names)

            with tempfile.TemporaryDirectory(prefix="retriever-portable-archive-") as extract_dir:
                extracted_root = Path(extract_dir) / "workspace"
                archive.extractall(extracted_root)

                portable_search = retriever_tools.search(extracted_root, "confidential", None, None, None, 1, 20)
                self.assertEqual(portable_search["total_hits"], 1)
                self.assertEqual(portable_search["results"][0]["id"], child_row["id"])
                self.assertEqual(portable_search["results"][0]["parent"]["id"], parent_row["id"])
                self.assertEqual(
                    portable_search["results"][0]["parent"]["control_number"],
                    parent_row["control_number"],
                )

                parent_search = retriever_tools.search(extracted_root, "Portable archive", None, None, None, 1, 20)
                self.assertEqual(parent_search["total_hits"], 0)

                portable_connection = retriever_tools.connect_db(extracted_root / ".retriever" / "retriever.db")
                try:
                    parent_stub_row = portable_connection.execute(
                        """
                        SELECT source_text_revision_id, active_search_text_revision_id
                        FROM documents
                        WHERE id = ?
                        """,
                        (parent_row["id"],),
                    ).fetchone()
                    self.assertIsNotNone(parent_stub_row)
                    self.assertIsNone(parent_stub_row["source_text_revision_id"])
                    self.assertIsNone(parent_stub_row["active_search_text_revision_id"])

                    dataset_document_count = portable_connection.execute(
                        "SELECT COUNT(*) AS count FROM dataset_documents"
                    ).fetchone()["count"]
                    self.assertEqual(dataset_document_count, 1)
                finally:
                    portable_connection.close()

        self.assertEqual(manifest["portable_workspace_document_ids"], [child_row["id"]])
        self.assertEqual(manifest["portable_workspace_stub_document_ids"], [parent_row["id"]])

    def test_export_archive_includes_production_source_parts_and_synthetic_logical_entry(self) -> None:
        production_root = self.write_production_fixture()

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest_production(self.root, production_root)
        self.assertEqual(ingest_result["created"], 4)

        native_row = self.fetch_document_row(
            f"{retriever_tools.INTERNAL_REL_PATH_PREFIX}/productions/Synthetic_Production/documents/PDX000004.logical"
        )
        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            source_part_rows = connection.execute(
                """
                SELECT part_kind, rel_source_path
                FROM document_source_parts
                WHERE document_id = ?
                ORDER BY part_kind ASC, ordinal ASC, id ASC
                """,
                (native_row["id"],),
            ).fetchall()
        finally:
            connection.close()
        self.assertTrue(any(row["part_kind"] == "native" for row in source_part_rows))

        export_path = self.root / ".retriever" / "exports" / "production.zip"
        exit_code, payload, _, _ = self.run_cli(
            "export-archive",
            str(self.root),
            "production.zip",
            "--filter",
            f"id = {native_row['id']}",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["document_count"], 1)
        self.assertEqual(payload["warnings"], [])
        self.assertEqual(payload["selector"]["filter"], f"(id = {native_row['id']})")

        with zipfile.ZipFile(export_path, "r") as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read(".retriever/export-manifest.json").decode("utf-8"))
            descriptor_payload = json.loads(archive.read(native_row["rel_path"]).decode("utf-8"))

        self.assertIn(native_row["rel_path"], names)
        self.assertTrue(all(row["rel_source_path"] in names for row in source_part_rows))
        self.assertEqual(manifest["warnings"], [])
        self.assertEqual(manifest["documents"][0]["document_entry_kind"], "synthetic")
        self.assertTrue(any(part["part_kind"] == "native" for part in manifest["documents"][0]["source_part_entries"]))
        self.assertEqual(descriptor_payload["document_id"], native_row["id"])
        self.assertEqual(descriptor_payload["control_number"], native_row["control_number"])

    def test_get_doc_and_list_chunks_return_summary_and_exact_chunk_text(self) -> None:
        paragraph = "Termination notice requires careful review and supporting detail. "
        body = "\n".join(f"Section {index}: {paragraph * 70}" for index in range(1, 8)) + "\n"
        document_path = self.root / "long.txt"
        document_path.write_text(body, encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("long.txt")
        expected_chunks = retriever_tools.chunk_text(body)
        self.assertGreater(len(expected_chunks), 2)

        exit_code, get_payload, _, _ = self.run_cli(
            "get-doc",
            str(self.root),
            "--doc-id",
            str(row["id"]),
            "--include-text",
            "summary",
            "--chunk",
            "0",
            "--chunk",
            "1",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(get_payload)
        self.assertEqual(get_payload["chunk_count"], len(expected_chunks))
        self.assertEqual(
            get_payload["text_summary"],
            retriever_tools.normalize_whitespace(body)[: retriever_tools.GET_DOC_SUMMARY_CHARS],
        )
        self.assertEqual(get_payload["chunks"][0]["chunk_index"], 0)
        self.assertEqual(get_payload["chunks"][0]["text"], expected_chunks[0]["text_content"])
        self.assertEqual(get_payload["chunks"][1]["chunk_index"], 1)
        self.assertEqual(get_payload["chunks"][1]["text"], expected_chunks[1]["text_content"])

        exit_code, list_payload, _, _ = self.run_cli(
            "list-chunks",
            str(self.root),
            "--doc-id",
            str(row["id"]),
            "--page",
            "1",
            "--per-page",
            "1",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(list_payload)
        self.assertEqual(list_payload["total_chunks"], len(expected_chunks))
        self.assertEqual(list_payload["total_pages"], len(expected_chunks))
        self.assertEqual(list_payload["chunks"][0]["chunk_index"], 0)
        self.assertEqual(
            list_payload["chunks"][0]["snippet"],
            retriever_tools.chunk_preview_text(expected_chunks[0]["text_content"]),
        )

    def test_get_doc_and_search_chunks_cli_default_to_compact_payloads(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("Termination notice appears here.\nSupporting detail follows.\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")

        get_exit, get_payload, _, _ = self.run_cli("get-doc", str(self.root), "--doc-id", str(row["id"]))
        get_verbose_exit, get_verbose_payload, _, _ = self.run_cli(
            "get-doc",
            str(self.root),
            "--doc-id",
            str(row["id"]),
            "--verbose",
        )

        self.assertEqual(get_exit, 0)
        self.assertEqual(get_verbose_exit, 0)
        self.assertIsNotNone(get_payload)
        self.assertIsNotNone(get_verbose_payload)
        self.assertNotIn("preview_targets", get_payload["document"])
        self.assertNotIn("manual_field_locks", get_payload["document"])
        self.assertIn("preview_targets", get_verbose_payload["document"])
        self.assertIn("manual_field_locks", get_verbose_payload["document"])

        chunk_exit, chunk_payload, _, _ = self.run_cli("search-chunks", str(self.root), "termination")
        chunk_verbose_exit, chunk_verbose_payload, _, _ = self.run_cli(
            "search-chunks",
            str(self.root),
            "termination",
            "--verbose",
        )

        self.assertEqual(chunk_exit, 0)
        self.assertEqual(chunk_verbose_exit, 0)
        self.assertIsNotNone(chunk_payload)
        self.assertIsNotNone(chunk_verbose_payload)

        compact_result = chunk_payload["results"][0]
        verbose_result = chunk_verbose_payload["results"][0]
        self.assertNotIn("text", compact_result)
        self.assertNotIn("preview_targets", compact_result)
        self.assertIn("citation", compact_result)
        self.assertIn("text", verbose_result)
        self.assertIn("preview_targets", verbose_result)

    def test_search_chunks_supports_citations_and_distinct_doc_count_mode(self) -> None:
        (self.root / "nda-one.txt").write_text("Termination notice must be delivered within thirty days.\n", encoding="utf-8")
        (self.root / "nda-two.txt").write_text("The agreement has a termination notice period of sixty days.\n", encoding="utf-8")
        (self.root / "other.txt").write_text("Unrelated payment processing clause.\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 3)

        exit_code, payload, _, _ = self.run_cli(
            "search-chunks",
            str(self.root),
            "termination",
            "--top-k",
            "5",
            "--per-doc-cap",
            "1",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(len(payload["results"]), 2)
        for item in payload["results"]:
            self.assertIn("citation", item)
            self.assertEqual(item["citation"]["document_id"], item["document_id"])
            self.assertEqual(item["citation"]["chunk_index"], item["chunk_index"])
            self.assertTrue(item["citation"]["snippet"])

        exit_code, count_payload, _, _ = self.run_cli(
            "search-chunks",
            str(self.root),
            "termination",
            "--count-only",
            "--distinct-docs",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(count_payload)
        self.assertEqual(count_payload["documents_with_hits"], 2)
        self.assertEqual(count_payload["total_docs_filtered"], 3)
        self.assertEqual(count_payload["count_mode"], "distinct-documents")

    def test_search_chunks_select_from_scope_narrows_to_scoped_documents(self) -> None:
        (self.root / "alpha-scope.txt").write_text("Alpha matter termination notice.\n", encoding="utf-8")
        (self.root / "beta-scope.txt").write_text("Beta project termination notice.\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        scope_exit, scope_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(scope_exit, 0)
        self.assertIsNotNone(scope_payload)
        self.assertEqual(scope_payload["total_hits"], 1)

        exit_code, payload, _, _ = self.run_cli(
            "search-chunks",
            str(self.root),
            "termination",
            "--select-from-scope",
            "--top-k",
            "10",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertTrue(payload["selected_from_scope"])
        self.assertEqual(payload["scope"]["keyword"], "alpha")
        self.assertEqual([item["file_name"] for item in payload["results"]], ["alpha-scope.txt"])

    def test_set_field_validates_custom_date_fields(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("hello\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")

        exit_code, payload, _, _ = self.run_cli("add-field", str(self.root), "effective_date", "date")
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)

        exit_code, payload, _, _ = self.run_cli(
            "set-field",
            str(self.root),
            "--doc-id",
            str(row["id"]),
            "--field",
            "effective_date",
            "--value",
            "2026-04-16",
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)

        updated_row = self.fetch_document_row("sample.txt")
        self.assertEqual(updated_row["effective_date"], "2026-04-16")

        exit_code, error_payload, _, _ = self.run_cli(
            "set-field",
            str(self.root),
            "--doc-id",
            str(row["id"]),
            "--field",
            "effective_date",
            "--value",
            "next Tuesday",
        )
        self.assertEqual(exit_code, 2)
        self.assertIsNotNone(error_payload)
        self.assertIn("Expected ISO date value", error_payload["error"])

    def test_promote_field_type_supports_week_aggregate_without_reingest(self) -> None:
        (self.root / "contract-a.txt").write_text("Contract A\n", encoding="utf-8")
        (self.root / "contract-b.txt").write_text("Contract B\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        first_row = self.fetch_document_row("contract-a.txt")
        second_row = self.fetch_document_row("contract-b.txt")

        self.assertEqual(self.run_cli("add-field", str(self.root), "effective_date", "text")[0], 0)
        self.assertEqual(
            self.run_cli(
                "set-field",
                str(self.root),
                "--doc-id",
                str(first_row["id"]),
                "--field",
                "effective_date",
                "--value",
                "2026-04-01",
            )[0],
            0,
        )
        self.assertEqual(
            self.run_cli(
                "set-field",
                str(self.root),
                "--doc-id",
                str(second_row["id"]),
                "--field",
                "effective_date",
                "--value",
                "2026-04-15",
            )[0],
            0,
        )

        exit_code, promote_payload, _, _ = self.run_cli(
            "promote-field-type",
            str(self.root),
            "effective_date",
            "date",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(promote_payload)
        self.assertEqual(promote_payload["status"], "ok")
        self.assertTrue(promote_payload["promotion_applied"])
        self.assertEqual(promote_payload["documents_with_values"], 2)

        aggregate_payload = retriever_tools.aggregate(
            self.root,
            None,
            ["week:effective_date"],
            "count",
            None,
            None,
            20,
            False,
        )
        self.assertEqual(aggregate_payload["graph"]["type"], "line")
        self.assertEqual(len(aggregate_payload["buckets"]), 2)
        self.assertTrue(all(str(bucket["week"]).startswith("2026-W") for bucket in aggregate_payload["buckets"]))

    def test_promote_field_type_blocks_invalid_existing_values(self) -> None:
        document_path = self.root / "contract.txt"
        document_path.write_text("Contract text\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("contract.txt")
        self.assertEqual(self.run_cli("add-field", str(self.root), "effective_date", "text")[0], 0)
        self.assertEqual(
            self.run_cli(
                "set-field",
                str(self.root),
                "--doc-id",
                str(row["id"]),
                "--field",
                "effective_date",
                "--value",
                "tomorrow",
            )[0],
            0,
        )

        exit_code, payload, _, _ = self.run_cli(
            "promote-field-type",
            str(self.root),
            "effective_date",
            "date",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["status"], "blocked")
        self.assertFalse(payload["promotion_applied"])
        self.assertEqual(payload["invalid_value_samples"][0]["value"], "tomorrow")

    def test_aggregate_groups_by_dataset_name_and_explain_flag(self) -> None:
        (self.root / "doc-one.txt").write_text("alpha\n", encoding="utf-8")
        (self.root / "doc-two.txt").write_text("beta\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        first_row = self.fetch_document_row("doc-one.txt")
        create_exit, create_payload, _, _ = self.run_cli("create-dataset", str(self.root), "Review Set")
        self.assertEqual(create_exit, 0)
        self.assertIsNotNone(create_payload)
        dataset_id = int(create_payload["dataset"]["id"])

        add_exit, _, _, _ = self.run_cli(
            "add-to-dataset",
            str(self.root),
            "--dataset-id",
            str(dataset_id),
            "--doc-id",
            str(first_row["id"]),
        )
        self.assertEqual(add_exit, 0)

        exit_code, payload, _, _ = self.run_cli(
            "aggregate",
            str(self.root),
            "--group-by",
            "dataset_name",
            "--explain",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        bucket_map = {item["dataset_name"]: item["count"] for item in payload["buckets"]}
        self.assertEqual(bucket_map[self.root.name], 2)
        self.assertEqual(bucket_map["Review Set"], 1)
        self.assertIn("sql", payload)
        self.assertIn("COUNT(DISTINCT d.id)", payload["sql"])
        self.assertIn("JOIN datasets ds", payload["sql"])

    def test_aggregate_select_from_scope_narrows_bucket_population(self) -> None:
        (self.root / "alpha-scope.txt").write_text("alpha body\n", encoding="utf-8")
        (self.root / "beta-scope.txt").write_text("beta body\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 2)

        scope_exit, scope_payload, _, _ = self.run_cli("slash", str(self.root), "/search", "alpha")
        self.assertEqual(scope_exit, 0)
        self.assertIsNotNone(scope_payload)
        self.assertEqual(scope_payload["total_hits"], 1)

        exit_code, payload, _, _ = self.run_cli(
            "aggregate",
            str(self.root),
            "--group-by",
            "file_name",
            "--select-from-scope",
        )

        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(payload)
        self.assertTrue(payload["selected_from_scope"])
        self.assertEqual(payload["scope"]["keyword"], "alpha")
        self.assertEqual(payload["buckets"], [{"file_name": "alpha-scope.txt", "count": 1}])

    def test_set_field_rejects_system_managed_fields(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("hello\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        blocked_fields = [
            "control_number",
            "dataset_id",
            "ingested_at",
            "last_seen_at",
            "parent_document_id",
            "updated_at",
            retriever_tools.MANUAL_FIELD_LOCKS_COLUMN,
        ]

        for field_name in blocked_fields:
            with self.subTest(field_name=field_name):
                with self.assertRaises(retriever_tools.RetrieverError) as context:
                    retriever_tools.set_field(self.root, row["id"], field_name, "override")
                self.assertIn("system-managed", str(context.exception))


class CidInliningTests(unittest.TestCase):
    PNG_PIXEL = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a9mQAAAAASUVORK5CYII="
    )

    def test_sniff_image_mime_type_detects_common_formats(self) -> None:
        self.assertEqual(retriever_tools.sniff_image_mime_type(self.PNG_PIXEL), "image/png")
        self.assertEqual(retriever_tools.sniff_image_mime_type(b"\xff\xd8\xff\xe0..."), "image/jpeg")
        self.assertEqual(retriever_tools.sniff_image_mime_type(b"GIF89a...."), "image/gif")
        self.assertIsNone(retriever_tools.sniff_image_mime_type(b"not-an-image"))

    def test_normalize_content_id_strips_brackets_and_whitespace(self) -> None:
        self.assertEqual(retriever_tools.normalize_content_id("  <foo@bar>  "), "foo@bar")
        self.assertEqual(retriever_tools.normalize_content_id("plain"), "plain")
        self.assertIsNone(retriever_tools.normalize_content_id(""))
        self.assertIsNone(retriever_tools.normalize_content_id(None))
        self.assertEqual(retriever_tools.normalize_content_id(b"<inline>"), "inline")

    def test_inline_cid_references_swaps_src_with_data_uri(self) -> None:
        html_body = (
            '<p>hi</p>'
            '<img src="cid:logo_icon">'
            "<img src='cid:LOGO_ICON'>"
            '<td background="cid:logo_icon">x</td>'
        )
        attachments = [
            {
                "file_name": "logo.png",
                "payload": self.PNG_PIXEL,
                "content_id": "<logo_icon>",
            },
        ]
        result = retriever_tools.inline_cid_references_in_html(html_body, attachments)
        self.assertIsNotNone(result)
        self.assertNotIn("cid:logo_icon", result)
        self.assertNotIn("cid:LOGO_ICON", result)
        self.assertEqual(result.count("data:image/png;base64,"), 3)

    def test_inline_cid_references_leaves_unknown_cids_intact(self) -> None:
        html_body = '<img src="cid:present"><img src="cid:missing">'
        attachments = [
            {
                "file_name": "present.png",
                "payload": self.PNG_PIXEL,
                "content_id": "present",
            },
        ]
        result = retriever_tools.inline_cid_references_in_html(html_body, attachments)
        self.assertIn("data:image/png;base64,", result)
        self.assertIn('src="cid:missing"', result)

    def test_inline_cid_references_noop_without_attachments_or_html(self) -> None:
        self.assertEqual(retriever_tools.inline_cid_references_in_html("<p>x</p>", []), "<p>x</p>")
        self.assertEqual(retriever_tools.inline_cid_references_in_html("<p>x</p>", None), "<p>x</p>")
        self.assertIsNone(retriever_tools.inline_cid_references_in_html(None, [{"content_id": "x", "payload": b""}]))
        html_body = '<img src="cid:logo">'
        attachments_missing_cid = [{"file_name": "logo.png", "payload": self.PNG_PIXEL}]
        self.assertEqual(
            retriever_tools.inline_cid_references_in_html(html_body, attachments_missing_cid),
            html_body,
        )

    def test_inline_cid_references_sniffs_mime_when_extension_missing(self) -> None:
        html_body = '<img src="cid:bareref">'
        attachments = [
            {
                "file_name": "",
                "payload": self.PNG_PIXEL,
                "content_id": "bareref",
            },
        ]
        result = retriever_tools.inline_cid_references_in_html(html_body, attachments)
        self.assertIn("data:image/png;base64,", result)

    def test_extract_eml_attachments_captures_content_id(self) -> None:
        boundary = "boundary-inline-test"
        encoded_png = base64.b64encode(self.PNG_PIXEL).decode("ascii")
        raw_message = (
            "From: sender@example.com\r\n"
            "To: recipient@example.com\r\n"
            "Subject: Inline icon test\r\n"
            "MIME-Version: 1.0\r\n"
            f'Content-Type: multipart/related; boundary="{boundary}"\r\n'
            "\r\n"
            f"--{boundary}\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "\r\n"
            '<html><body><img src="cid:logo-icon" alt="logo"/></body></html>\r\n'
            f"--{boundary}\r\n"
            "Content-Type: image/png\r\n"
            "Content-Transfer-Encoding: base64\r\n"
            "Content-ID: <logo-icon>\r\n"
            'Content-Disposition: inline; filename="logo.png"\r\n'
            "\r\n"
            f"{encoded_png}\r\n"
            f"--{boundary}--\r\n"
        ).encode("ascii")

        parsed = BytesParser(policy=policy.default).parsebytes(raw_message)
        attachments = retriever_tools.extract_eml_attachments(parsed)
        self.assertTrue(any(a.get("content_id") == "logo-icon" for a in attachments))
        self.assertTrue(any(a.get("is_inline") is True for a in attachments))

    def test_build_email_extracted_payload_inlines_cid_images_in_preview(self) -> None:
        html_body = '<html><body><img src="cid:icon"/></body></html>'
        attachments = [
            {
                "file_name": "icon.png",
                "ordinal": 1,
                "payload": self.PNG_PIXEL,
                "file_hash": "deadbeef",
                "content_id": "icon",
            }
        ]
        payload = retriever_tools.build_email_extracted_payload(
            subject="Test",
            author="a@example.com",
            recipients="b@example.com",
            date_created="2026-04-16T00:00:00Z",
            text_body="See icon.",
            html_body=html_body,
            attachments=attachments,
            preview_file_name="msg.html",
        )
        preview_content = payload["preview_artifacts"][0]["content"]
        self.assertIn("data:image/png;base64,", preview_content)
        self.assertNotIn('src="cid:icon"', preview_content)

    def test_build_email_extracted_payload_uses_subject_for_preview_title_and_heading(self) -> None:
        payload = retriever_tools.build_email_extracted_payload(
            subject="Legalweek 2023 Mobile App Now Available Attachments: agenda.pdf; deck.pdf",
            author="events@example.com",
            recipients="sergey@example.com",
            date_created="2026-04-17T15:31:00Z",
            text_body="Email body.",
            html_body=None,
            attachments=[],
            preview_file_name="msg.html",
        )

        preview_content = payload["preview_artifacts"][0]["content"]
        self.assertIn("<title>Legalweek 2023 Mobile App Now Available</title>", preview_content)
        self.assertIn("<h1>Legalweek 2023 Mobile App Now Available</h1>", preview_content)
        self.assertNotIn("Attachments: agenda.pdf", preview_content)


if __name__ == "__main__":
    unittest.main()
