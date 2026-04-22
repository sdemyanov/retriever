from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path


PHASE0_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PHASE0_ROOT.parent
DEFAULT_CORPUS = PHASE0_ROOT / "regression_corpus"
DEFAULT_TOOL = REPO_ROOT / "skills" / "tool-template" / "retriever_tools.py"
DOC_ID_PATTERN = re.compile(r"doc-\d+")
CONVERSATION_ID_PATTERN = re.compile(r"conversation-\d+")
PREVIEW_UPDATED_CELL_PATTERN = re.compile(r"(<th>Updated</th><td>).*?(</td>)", re.DOTALL)
SYNTHETIC_MTIME = 1_713_072_000
GMAIL_EXPORT_ARCHIVE_BROWSER_FILES = {"archive_browser.html", "Archive Browser.html"}
GMAIL_EXPORT_DRIVE_FOLDER_PATTERN = re.compile(r"_Drive_Link_Export(?:_\d+)?$", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run two retriever tool builds against the same corpus and diff normalized ingest state.",
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=DEFAULT_CORPUS,
        help="Directory to copy into each temporary workspace.",
    )
    parser.add_argument(
        "--baseline-tool",
        type=Path,
        help="Baseline retriever_tools.py path. Defaults to the candidate tool path.",
    )
    parser.add_argument(
        "--candidate-tool",
        type=Path,
        default=DEFAULT_TOOL,
        help="Candidate retriever_tools.py path.",
    )
    parser.add_argument(
        "--baseline-label",
        default="baseline",
        help="Artifact label for the baseline run.",
    )
    parser.add_argument(
        "--candidate-label",
        default="candidate",
        help="Artifact label for the candidate run.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Artifact directory. Defaults to a temporary directory.",
    )
    parser.add_argument(
        "--file-types",
        help="Optional comma-separated file types to pass to ingest.",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Run ingest without --recursive.",
    )
    parser.add_argument(
        "--no-benchmark",
        action="store_true",
        help="Do not enable RETRIEVER_BENCHMARK=1 during CLI runs.",
    )
    parser.add_argument(
        "--no-synthetic-extras",
        action="store_true",
        help="Do not add the built-in Slack and processed-production fixtures.",
    )
    parser.add_argument(
        "--allow-diff",
        action="store_true",
        help="Exit 0 even when semantic differences are found.",
    )
    return parser.parse_args()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str | None:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def sha256_json_value(value: object) -> str:
    return sha256_text(json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


def normalize_workspace_string(value: str, workspace_root: Path) -> str:
    text = value.replace(str(workspace_root.resolve()), "<workspace>")
    text = text.replace(str(workspace_root), "<workspace>")
    return text


def normalize_preview_string(value: str, workspace_root: Path) -> str:
    normalized = DOC_ID_PATTERN.sub("doc-<id>", normalize_workspace_string(value, workspace_root))
    normalized = CONVERSATION_ID_PATTERN.sub("conversation-<id>", normalized)
    return PREVIEW_UPDATED_CELL_PATTERN.sub(r"\1<updated>\2", normalized)


def normalize_object(value: object, workspace_root: Path) -> object:
    if isinstance(value, dict):
        return {str(key): normalize_object(item, workspace_root) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_object(item, workspace_root) for item in value]
    if isinstance(value, str):
        return normalize_workspace_string(value, workspace_root)
    return value


def decode_json_text(raw_value: object, default: object) -> object:
    if raw_value in (None, ""):
        return default
    try:
        return json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def escape_pdf_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_minimal_pdf(path: Path, text: str) -> None:
    content = f"BT /F1 12 Tf 72 72 Td ({escape_pdf_text(text)}) Tj ET".encode("latin-1")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n%b\nendstream" % (len(content), content),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    parts = [b"%PDF-1.4\n"]
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(sum(len(part) for part in parts))
        parts.append(f"{index} 0 obj\n".encode("ascii"))
        parts.append(obj)
        parts.append(b"\nendobj\n")
    xref_offset = sum(len(part) for part in parts)
    parts.append(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    parts.append(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        parts.append(f"{offset:010d} 00000 n \n".encode("ascii"))
    parts.append(
        (
            "trailer\n"
            f"<< /Root 1 0 R /Size {len(objects) + 1} >>\n"
            "startxref\n"
            f"{xref_offset}\n"
            "%%EOF\n"
        ).encode("ascii")
    )
    path.write_bytes(b"".join(parts))


def set_tree_mtime(root: Path, timestamp: int) -> None:
    for path in sorted(root.rglob("*")):
        os.utime(path, (timestamp, timestamp))
    os.utime(root, (timestamp, timestamp))


def loadfile_path(root_name: str, *parts: str) -> str:
    return ".\\" + "\\".join([root_name, *parts])


def write_synthetic_slack_export(workspace_root: Path) -> None:
    export_root = workspace_root / "data" / "slack"
    channel_dir = export_root / "general"
    channel_dir.mkdir(parents=True, exist_ok=True)
    (export_root / "users.json").write_text(
        json.dumps(
            [
                {
                    "id": "U04SERGEY1",
                    "name": "sergey",
                    "profile": {"real_name": "Sergey Demyanov", "display_name": "Sergey"},
                },
                {
                    "id": "U04MAX0001",
                    "name": "maksim",
                    "profile": {"real_name": "Maksim Faleev", "display_name": "Maksim"},
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
            [{"id": "C04GENERAL1", "name": "general", "is_channel": True}],
            ensure_ascii=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
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
    set_tree_mtime(export_root, SYNTHETIC_MTIME)


def write_synthetic_production(workspace_root: Path) -> None:
    production_name = "Synthetic_Production"
    control_prefix = "PDX"
    production_root = workspace_root / production_name
    data_dir = production_root / "DATA"
    text_dir = production_root / "TEXT" / "TEXT001"
    native_dir = production_root / "NATIVES" / "NAT001"
    for directory in (data_dir, text_dir, native_dir):
        directory.mkdir(parents=True, exist_ok=True)

    def bates(number: int) -> str:
        return f"{control_prefix}{number:06d}"

    (text_dir / f"{bates(1)}.txt").write_text(
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
    (text_dir / f"{bates(3)}.txt").write_text(
        (
            "From: Review Team\n"
            "Sent: 04/14/2026 09:00 AM\n\n"
            "Case status update\n"
            "Contains follow-up details.\n"
        ),
        encoding="utf-8",
    )
    (text_dir / f"{bates(4)}.txt").write_text(
        "Native-backed production doc\nUse native preview first.\n",
        encoding="utf-8",
    )
    build_minimal_pdf(native_dir / f"{bates(4)}.pdf", "Native preview document")

    headers = ["Begin Bates", "End Bates", "Begin Attachment", "End Attachment", "Text Precedence", "FILE_PATH"]
    rows = [
        [
            bates(1),
            bates(2),
            bates(1),
            bates(3),
            loadfile_path(production_root.name, "TEXT", "TEXT001", f"{bates(1)}.txt"),
            "",
        ],
        [
            bates(3),
            bates(3),
            "",
            "",
            loadfile_path(production_root.name, "TEXT", "TEXT001", f"{bates(3)}.txt"),
            "",
        ],
        [
            bates(4),
            bates(4),
            "",
            "",
            loadfile_path(production_root.name, "TEXT", "TEXT001", f"{bates(4)}.txt"),
            loadfile_path(production_root.name, "NATIVES", "NAT001", f"{bates(4)}.pdf"),
        ],
    ]
    delimiter = b"\x14"
    quote = b"\xfe"

    def dat_line(fields: list[str]) -> bytes:
        return delimiter.join(quote + field.encode("latin-1") + quote for field in fields) + b"\r\n"

    (data_dir / f"{production_name}.dat").write_bytes(dat_line(headers) + b"".join(dat_line(row) for row in rows))
    set_tree_mtime(production_root, SYNTHETIC_MTIME)


def copy_corpus_into_workspace(corpus_root: Path, workspace_root: Path, *, include_synthetic_extras: bool) -> None:
    shutil.copytree(
        corpus_root,
        workspace_root,
        ignore=shutil.ignore_patterns(".retriever"),
    )
    if include_synthetic_extras:
        write_synthetic_slack_export(workspace_root)
        write_synthetic_production(workspace_root)


def parse_benchmark_payload(stderr_text: str) -> dict[str, object] | None:
    for line in reversed(stderr_text.splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except (ValueError, json.JSONDecodeError):
            continue
        if isinstance(parsed, dict) and isinstance(parsed.get("_bench"), dict):
            return parsed["_bench"]
    return None


def run_tool_command(
    tool_path: Path,
    workspace_root: Path,
    command: str,
    extra_args: list[str],
    *,
    enable_benchmark: bool,
    artifact_dir: Path,
) -> dict[str, object]:
    env = os.environ.copy()
    if enable_benchmark:
        env["RETRIEVER_BENCHMARK"] = "1"
    command_line = [sys.executable, str(tool_path), command, str(workspace_root), *extra_args]
    completed = subprocess.run(
        command_line,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    stdout_text = completed.stdout
    stderr_text = completed.stderr
    stdout_path = artifact_dir / f"{command}.stdout.json"
    stderr_path = artifact_dir / f"{command}.stderr.txt"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_path.write_text(stdout_text, encoding="utf-8")
    stderr_path.write_text(stderr_text, encoding="utf-8")
    payload: dict[str, object] | None = None
    if stdout_text.strip():
        payload = json.loads(stdout_text)
    result = {
        "argv": command_line,
        "exit_code": completed.returncode,
        "payload": payload,
        "stderr_text": stderr_text,
        "benchmark": parse_benchmark_payload(stderr_text),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }
    if completed.returncode != 0:
        raise RuntimeError(
            f"{command} failed for {workspace_root}: exit={completed.returncode}; stderr saved to {stderr_path}"
        )
    return result


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def query_rows(connection: sqlite3.Connection, query: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
    return [{key: row[key] for key in row.keys()} for row in connection.execute(query, params).fetchall()]


def normalize_dataset_row(dataset_row: dict[str, object] | None) -> dict[str, object] | None:
    if dataset_row is None:
        return None
    return {
        "source_kind": dataset_row.get("source_kind"),
        "dataset_locator": dataset_row.get("dataset_locator"),
        "dataset_name": dataset_row.get("dataset_name"),
    }


def normalize_conversation_row(conversation_row: dict[str, object] | None) -> dict[str, object] | None:
    if conversation_row is None:
        return None
    return {
        "source_kind": conversation_row.get("source_kind"),
        "source_locator": conversation_row.get("source_locator"),
        "conversation_key": conversation_row.get("conversation_key"),
        "conversation_type": conversation_row.get("conversation_type"),
        "display_name": conversation_row.get("display_name"),
    }


def normalize_text_revision_row(
    text_revision_row: dict[str, object] | None,
    revision_by_id: dict[int, dict[str, object]],
) -> dict[str, object] | None:
    if text_revision_row is None:
        return None
    parent_row = None
    parent_revision_id = text_revision_row.get("parent_revision_id")
    if parent_revision_id is not None:
        parent_row = revision_by_id.get(int(parent_revision_id))
    return {
        "revision_kind": text_revision_row.get("revision_kind"),
        "content_hash": text_revision_row.get("content_hash"),
        "language": text_revision_row.get("language"),
        "quality_score": text_revision_row.get("quality_score"),
        "parent_content_hash": parent_row.get("content_hash") if parent_row is not None else None,
    }


def normalize_ingest_payload(payload: dict[str, object] | None, workspace_root: Path) -> dict[str, object]:
    if payload is None:
        return {}
    normalized: dict[str, object] = {}
    for key, value in payload.items():
        if key in {"tool_version", "schema_version", "workspace_root", "dataset_id", "dataset_source_id"}:
            continue
        if key == "failures":
            failures = [normalize_object(entry, workspace_root) for entry in list(value or [])]
            normalized[key] = sorted(
                failures,
                key=lambda entry: json.dumps(entry, sort_keys=True),
            )
            continue
        if key in {"warnings", "ingested_production_roots", "skipped_production_roots"}:
            normalized[key] = sorted(normalize_object(list(value or []), workspace_root))
            continue
        normalized[key] = normalize_object(value, workspace_root)
    return normalized


def iter_nonretriever_files(root: Path) -> list[Path]:
    files: list[Path] = []
    try:
        iterator = root.rglob("*")
    except OSError:
        return files
    for path in iterator:
        if ".retriever" in path.parts or path.is_dir():
            continue
        files.append(path)
    return files


def detect_gmail_export_root_for_source(workspace_root: Path, source_rel_path: str) -> Path | None:
    source_path = workspace_root / source_rel_path
    if source_path.suffix.lower() != ".mbox" or not source_path.exists():
        return None
    candidates: list[Path] = []
    current = source_path.parent
    while True:
        if current == workspace_root or workspace_root in current.parents:
            candidates.append(current)
        if current == workspace_root or current.parent == current:
            break
        current = current.parent
    for candidate_root in reversed(candidates):
        if any((candidate_root / file_name).exists() for file_name in GMAIL_EXPORT_ARCHIVE_BROWSER_FILES):
            return candidate_root
        try:
            if any(path.name.lower().endswith("-metadata.csv") for path in candidate_root.rglob("*.csv")):
                return candidate_root
            if any(path.name.lower().endswith("-drive-links.csv") for path in candidate_root.rglob("*.csv")):
                return candidate_root
            if any(path.is_dir() and GMAIL_EXPORT_DRIVE_FOLDER_PATTERN.search(path.name) for path in candidate_root.rglob("*")):
                return candidate_root
            if any("Drive_Link_Export" in path.name for path in candidate_root.rglob("*-metadata.xml")):
                return candidate_root
        except OSError:
            continue
    return None


def normalized_gmail_container_file_hash(workspace_root: Path, source_rel_path: str) -> str | None:
    source_path = workspace_root / source_rel_path
    candidate_root = detect_gmail_export_root_for_source(workspace_root, source_rel_path)
    if candidate_root is None or not source_path.exists():
        return None
    try:
        metadata_csv_paths = sorted(
            path
            for path in candidate_root.rglob("*.csv")
            if ".retriever" not in path.parts and path.name.lower().endswith("-metadata.csv")
        )
        drive_links_paths = sorted(
            path
            for path in candidate_root.rglob("*.csv")
            if ".retriever" not in path.parts and path.name.lower().endswith("-drive-links.csv")
        )
        drive_export_dirs = sorted(
            path
            for path in candidate_root.rglob("*")
            if path.is_dir() and ".retriever" not in path.parts and GMAIL_EXPORT_DRIVE_FOLDER_PATTERN.search(path.name)
        )
        drive_export_metadata_paths = sorted(
            path
            for path in candidate_root.rglob("*-metadata.xml")
            if ".retriever" not in path.parts and "Drive_Link_Export" in path.name
        )
        drive_export_error_paths = sorted(
            path
            for path in candidate_root.rglob("*-errors.csv")
            if ".retriever" not in path.parts and "Drive_Link_Export" in path.name
        )
    except OSError:
        return None
    drive_export_files: list[Path] = []
    for export_dir in drive_export_dirs:
        drive_export_files.extend(iter_nonretriever_files(export_dir))
    message_sidecar_paths = [
        *metadata_csv_paths,
        *drive_links_paths,
        *drive_export_metadata_paths,
        *drive_export_error_paths,
        *sorted(drive_export_files),
    ]
    message_sidecar_hash = sha256_json_value(
        {
            "files": {
                path.relative_to(candidate_root).as_posix(): sha256_file(path)
                for path in sorted(
                    dict.fromkeys(message_sidecar_paths),
                    key=lambda item: item.relative_to(candidate_root).as_posix(),
                )
            }
        }
    )
    return sha256_json_value(
        {
            "mbox_hash": sha256_file(source_path),
            "message_sidecar_hash": message_sidecar_hash,
            "source_rel_path": source_rel_path,
        }
    )


def normalize_container_source_file_hash(workspace_root: Path, row: dict[str, object]) -> str | None:
    source_kind = str(row.get("source_kind") or "")
    source_rel_path = str(row.get("source_rel_path") or "")
    if source_kind == "mbox":
        normalized_hash = normalized_gmail_container_file_hash(workspace_root, source_rel_path)
        if normalized_hash is not None:
            return normalized_hash
    raw_hash = row.get("file_hash")
    return str(raw_hash) if raw_hash is not None else None


def build_preview_file_snapshot(
    workspace_root: Path,
    rel_preview_paths: list[str],
) -> dict[str, dict[str, object]]:
    retriever_dir = workspace_root / ".retriever"
    snapshot: dict[str, dict[str, object]] = {}
    for rel_preview_path in sorted(set(rel_preview_paths)):
        normalized_rel_path = normalize_preview_string(rel_preview_path, workspace_root)
        abs_path = retriever_dir / rel_preview_path
        if not abs_path.exists():
            snapshot[normalized_rel_path] = {"exists": False}
            continue
        data = abs_path.read_bytes()
        if abs_path.suffix.lower() in {".html", ".txt", ".json"}:
            text = data.decode("utf-8", errors="replace")
            normalized_text = normalize_preview_string(text, workspace_root)
            snapshot[normalized_rel_path] = {
                "exists": True,
                "kind": "text",
                "sha256": sha256_text(normalized_text),
            }
            continue
        snapshot[normalized_rel_path] = {
            "exists": True,
            "kind": "binary",
            "sha256": sha256_bytes(data),
            "size": len(data),
        }
    return snapshot


def build_workspace_snapshot(workspace_root: Path, ingest_payload: dict[str, object] | None) -> dict[str, object]:
    db_path = workspace_root / ".retriever" / "retriever.db"
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        datasets = query_rows(connection, "SELECT * FROM datasets ORDER BY dataset_locator ASC, dataset_name ASC, id ASC") if table_exists(connection, "datasets") else []
        dataset_by_id = {int(row["id"]): row for row in datasets if row.get("id") is not None}
        dataset_sources = (
            query_rows(
                connection,
                "SELECT * FROM dataset_sources ORDER BY dataset_id ASC, source_kind ASC, source_locator ASC, id ASC",
            )
            if table_exists(connection, "dataset_sources")
            else []
        )
        dataset_sources_by_dataset_id: dict[int, list[dict[str, object]]] = defaultdict(list)
        for row in dataset_sources:
            dataset_sources_by_dataset_id[int(row["dataset_id"])].append(row)
        conversations = (
            query_rows(
                connection,
                """
                SELECT *
                FROM conversations
                ORDER BY source_kind ASC, source_locator ASC, conversation_key ASC, id ASC
                """,
            )
            if table_exists(connection, "conversations")
            else []
        )
        conversation_by_id = {int(row["id"]): row for row in conversations if row.get("id") is not None}
        productions = (
            query_rows(
                connection,
                """
                SELECT *
                FROM productions
                ORDER BY rel_root ASC, production_name ASC, id ASC
                """,
            )
            if table_exists(connection, "productions")
            else []
        )
        production_by_id = {int(row["id"]): row for row in productions if row.get("id") is not None}
        container_sources = (
            query_rows(
                connection,
                """
                SELECT *
                FROM container_sources
                ORDER BY source_kind ASC, source_rel_path ASC, id ASC
                """,
            )
            if table_exists(connection, "container_sources")
            else []
        )
        text_revisions = (
            query_rows(
                connection,
                """
                SELECT *
                FROM text_revisions
                ORDER BY document_id ASC, id ASC
                """,
            )
            if table_exists(connection, "text_revisions")
            else []
        )
        text_revision_by_id = {int(row["id"]): row for row in text_revisions if row.get("id") is not None}
        revision_count_by_document_id: dict[int, int] = defaultdict(int)
        for row in text_revisions:
            if row.get("document_id") is not None:
                revision_count_by_document_id[int(row["document_id"])] += 1
        documents = (
            query_rows(
                connection,
                """
                SELECT *
                FROM documents
                ORDER BY rel_path ASC, control_number ASC, id ASC
                """,
            )
            if table_exists(connection, "documents")
            else []
        )
        document_by_id = {int(row["id"]): row for row in documents if row.get("id") is not None}
        source_parts = (
            query_rows(
                connection,
                """
                SELECT *
                FROM document_source_parts
                ORDER BY document_id ASC, part_kind ASC, ordinal ASC, id ASC
                """,
            )
            if table_exists(connection, "document_source_parts")
            else []
        )
        source_parts_by_document_id: dict[int, list[dict[str, object]]] = defaultdict(list)
        for row in source_parts:
            source_parts_by_document_id[int(row["document_id"])].append(row)
        preview_rows = (
            query_rows(
                connection,
                """
                SELECT *
                FROM document_previews
                ORDER BY document_id ASC, rel_preview_path ASC, ordinal ASC, id ASC
                """,
            )
            if table_exists(connection, "document_previews")
            else []
        )
        preview_rows_by_document_id: dict[int, list[dict[str, object]]] = defaultdict(list)
        preview_paths: list[str] = []
        for row in preview_rows:
            preview_rows_by_document_id[int(row["document_id"])].append(row)
            preview_paths.append(str(row["rel_preview_path"]))
        chunk_rows = (
            query_rows(
                connection,
                """
                SELECT *
                FROM document_chunks
                ORDER BY document_id ASC, chunk_index ASC, id ASC
                """,
            )
            if table_exists(connection, "document_chunks")
            else []
        )
        chunks_by_document_id: dict[int, list[dict[str, object]]] = defaultdict(list)
        for row in chunk_rows:
            chunks_by_document_id[int(row["document_id"])].append(row)
        email_threading_rows = (
            query_rows(
                connection,
                """
                SELECT *
                FROM document_email_threading
                ORDER BY document_id ASC
                """,
            )
            if table_exists(connection, "document_email_threading")
            else []
        )
        email_threading_by_document_id = {int(row["document_id"]): row for row in email_threading_rows}
        chat_threading_rows = (
            query_rows(
                connection,
                """
                SELECT *
                FROM document_chat_threading
                ORDER BY document_id ASC
                """,
            )
            if table_exists(connection, "document_chat_threading")
            else []
        )
        chat_threading_by_document_id = {int(row["document_id"]): row for row in chat_threading_rows}
        dataset_document_rows = (
            query_rows(
                connection,
                """
                SELECT dd.document_id, d.dataset_locator, d.dataset_name, d.source_kind,
                       ds.source_kind AS dataset_source_kind, ds.source_locator AS dataset_source_locator
                FROM dataset_documents AS dd
                JOIN datasets AS d ON d.id = dd.dataset_id
                LEFT JOIN dataset_sources AS ds ON ds.id = dd.dataset_source_id
                ORDER BY dd.document_id ASC, d.dataset_locator ASC, ds.source_kind ASC, ds.source_locator ASC, dd.id ASC
                """,
            )
            if table_exists(connection, "dataset_documents") and table_exists(connection, "datasets")
            else []
        )
        dataset_memberships_by_document_id: dict[int, list[dict[str, object]]] = defaultdict(list)
        for row in dataset_document_rows:
            dataset_memberships_by_document_id[int(row["document_id"])].append(
                {
                    "dataset_name": row.get("dataset_name"),
                    "dataset_source_kind": row.get("source_kind"),
                    "dataset_source_locator": row.get("dataset_locator"),
                    "membership_source_kind": row.get("dataset_source_kind"),
                    "membership_source_locator": row.get("dataset_source_locator"),
                }
            )
        documents_snapshot: list[dict[str, object]] = []
        for row in documents:
            document_id = int(row["id"])
            parent_row = None
            if row.get("parent_document_id") is not None:
                parent_row = document_by_id.get(int(row["parent_document_id"]))
            conversation_row = None
            if row.get("conversation_id") is not None:
                conversation_row = conversation_by_id.get(int(row["conversation_id"]))
            dataset_row = None
            if row.get("dataset_id") is not None:
                dataset_row = dataset_by_id.get(int(row["dataset_id"]))
            production_row = None
            if row.get("production_id") is not None:
                production_row = production_by_id.get(int(row["production_id"]))
            source_text_revision_row = None
            if row.get("source_text_revision_id") is not None:
                source_text_revision_row = text_revision_by_id.get(int(row["source_text_revision_id"]))
            active_search_revision_row = None
            if row.get("active_search_text_revision_id") is not None:
                active_search_revision_row = text_revision_by_id.get(int(row["active_search_text_revision_id"]))
            documents_snapshot.append(
                {
                    "rel_path": row.get("rel_path"),
                    "file_name": row.get("file_name"),
                    "source_kind": row.get("source_kind"),
                    "source_rel_path": row.get("source_rel_path"),
                    "source_item_id": row.get("source_item_id"),
                    "source_folder_path": row.get("source_folder_path"),
                    "root_message_key": row.get("root_message_key"),
                    "control_number": row.get("control_number"),
                    "control_number_batch": row.get("control_number_batch"),
                    "control_number_family_sequence": row.get("control_number_family_sequence"),
                    "control_number_attachment_sequence": row.get("control_number_attachment_sequence"),
                    "conversation_assignment_mode": row.get("conversation_assignment_mode"),
                    "parent_rel_path": parent_row.get("rel_path") if parent_row is not None else None,
                    "parent_control_number": parent_row.get("control_number") if parent_row is not None else None,
                    "child_document_kind": row.get("child_document_kind"),
                    "production_rel_root": production_row.get("rel_root") if production_row is not None else None,
                    "begin_bates": row.get("begin_bates"),
                    "end_bates": row.get("end_bates"),
                    "begin_attachment": row.get("begin_attachment"),
                    "end_attachment": row.get("end_attachment"),
                    "file_type": row.get("file_type"),
                    "file_size": row.get("file_size"),
                    "file_hash": row.get("file_hash"),
                    "content_hash": row.get("content_hash"),
                    "page_count": row.get("page_count"),
                    "author": row.get("author"),
                    "content_type": row.get("content_type"),
                    "custodian": row.get("custodian"),
                    "date_created": row.get("date_created"),
                    "date_modified": row.get("date_modified"),
                    "title": row.get("title"),
                    "subject": row.get("subject"),
                    "participants": row.get("participants"),
                    "recipients": row.get("recipients"),
                    "text_status": row.get("text_status"),
                    "lifecycle_status": row.get("lifecycle_status"),
                    "manual_field_locks": decode_json_text(row.get("manual_field_locks_json"), default=[]),
                    "conversation": normalize_conversation_row(conversation_row),
                    "primary_dataset": normalize_dataset_row(dataset_row),
                    "source_text_revision": normalize_text_revision_row(source_text_revision_row, text_revision_by_id),
                    "active_search_text_revision": normalize_text_revision_row(active_search_revision_row, text_revision_by_id),
                    "text_revision_count": revision_count_by_document_id.get(document_id, 0),
                }
            )
        documents_snapshot.sort(key=lambda row: (str(row["rel_path"]), str(row["control_number"] or "")))

        source_parts_snapshot: dict[str, list[dict[str, object]]] = {}
        preview_rows_snapshot: dict[str, list[dict[str, object]]] = {}
        chunks_snapshot: dict[str, list[dict[str, object]]] = {}
        email_threading_snapshot: dict[str, dict[str, object]] = {}
        chat_threading_snapshot: dict[str, dict[str, object]] = {}
        dataset_membership_snapshot: dict[str, list[dict[str, object]]] = {}
        for row in documents:
            document_id = int(row["id"])
            rel_path = str(row["rel_path"])
            source_parts_snapshot[rel_path] = [
                {
                    "part_kind": part.get("part_kind"),
                    "rel_source_path": part.get("rel_source_path"),
                    "ordinal": part.get("ordinal"),
                    "label": part.get("label"),
                }
                for part in source_parts_by_document_id.get(document_id, [])
            ]
            preview_rows_snapshot[rel_path] = [
                {
                    "rel_preview_path": normalize_preview_string(str(preview_row.get("rel_preview_path") or ""), workspace_root),
                    "preview_type": preview_row.get("preview_type"),
                    "target_fragment": (
                        normalize_preview_string(str(preview_row.get("target_fragment")), workspace_root)
                        if preview_row.get("target_fragment") is not None
                        else None
                    ),
                    "label": preview_row.get("label"),
                    "ordinal": preview_row.get("ordinal"),
                }
                for preview_row in preview_rows_by_document_id.get(document_id, [])
            ]
            chunks_snapshot[rel_path] = [
                {
                    "chunk_index": chunk_row.get("chunk_index"),
                    "char_start": chunk_row.get("char_start"),
                    "char_end": chunk_row.get("char_end"),
                    "token_estimate": chunk_row.get("token_estimate"),
                    "text_hash": sha256_text(str(chunk_row.get("text_content") or "")),
                    "text_preview": str(chunk_row.get("text_content") or "")[:120],
                }
                for chunk_row in chunks_by_document_id.get(document_id, [])
            ]
            email_row = email_threading_by_document_id.get(document_id)
            if email_row is not None:
                email_threading_snapshot[rel_path] = {
                    "message_id": email_row.get("message_id"),
                    "in_reply_to": email_row.get("in_reply_to"),
                    "references": decode_json_text(email_row.get("references_json"), default=[]),
                    "conversation_index": email_row.get("conversation_index"),
                    "conversation_topic": email_row.get("conversation_topic"),
                    "normalized_subject": email_row.get("normalized_subject"),
                }
            chat_row = chat_threading_by_document_id.get(document_id)
            if chat_row is not None:
                chat_threading_snapshot[rel_path] = {
                    "thread_id": chat_row.get("thread_id"),
                    "message_id": chat_row.get("message_id"),
                    "parent_message_id": chat_row.get("parent_message_id"),
                    "thread_type": chat_row.get("thread_type"),
                    "participants": decode_json_text(chat_row.get("participants_json"), default=[]),
                }
            dataset_membership_snapshot[rel_path] = sorted(
                dataset_memberships_by_document_id.get(document_id, []),
                key=lambda item: (
                    str(item.get("dataset_source_locator") or ""),
                    str(item.get("membership_source_locator") or ""),
                    str(item.get("membership_source_kind") or ""),
                ),
            )

        conversations_snapshot: list[dict[str, object]] = []
        for row in conversations:
            conversation_id = int(row["id"])
            conversations_snapshot.append(
                {
                    "source_kind": row.get("source_kind"),
                    "source_locator": row.get("source_locator"),
                    "conversation_key": row.get("conversation_key"),
                    "conversation_type": row.get("conversation_type"),
                    "display_name": row.get("display_name"),
                    "documents": sorted(
                        str(document.get("rel_path"))
                        for document in documents
                        if document.get("conversation_id") == conversation_id
                    ),
                }
            )
        productions_snapshot = [
            {
                "rel_root": row.get("rel_root"),
                "production_name": row.get("production_name"),
                "metadata_load_rel_path": row.get("metadata_load_rel_path"),
                "image_load_rel_path": row.get("image_load_rel_path"),
                "source_type": row.get("source_type"),
                "dataset": normalize_dataset_row(dataset_by_id.get(int(row["dataset_id"]))) if row.get("dataset_id") is not None else None,
            }
            for row in productions
        ]
        container_sources_snapshot = [
            {
                "source_kind": row.get("source_kind"),
                "source_rel_path": row.get("source_rel_path"),
                "file_size": row.get("file_size"),
                "file_hash": normalize_container_source_file_hash(workspace_root, row),
                "message_count": row.get("message_count"),
            }
            for row in container_sources
        ]
        preview_files_snapshot = build_preview_file_snapshot(workspace_root, preview_paths)
        fts_documents_snapshot: dict[str, dict[str, object]] = {}
        if table_exists(connection, "documents_fts") and table_exists(connection, "documents"):
            try:
                for row in query_rows(
                    connection,
                    """
                    SELECT d.rel_path, f.file_name, f.title, f.subject, f.author, f.custodian, f.participants, f.recipients
                    FROM documents_fts AS f
                    JOIN documents AS d ON d.id = f.document_id
                    ORDER BY d.rel_path ASC
                    """,
                ):
                    fts_documents_snapshot[str(row["rel_path"])] = {
                        "file_name": row.get("file_name"),
                        "title": row.get("title"),
                        "subject": row.get("subject"),
                        "author": row.get("author"),
                        "custodian": row.get("custodian"),
                        "participants": row.get("participants"),
                        "recipients": row.get("recipients"),
                    }
            except sqlite3.Error:
                fts_documents_snapshot = {}
        fts_chunks_snapshot: dict[str, list[dict[str, object]]] = {}
        if table_exists(connection, "chunks_fts") and table_exists(connection, "document_chunks"):
            try:
                for row in query_rows(
                    connection,
                    """
                    SELECT d.rel_path, c.chunk_index, f.text_content
                    FROM chunks_fts AS f
                    JOIN document_chunks AS c ON c.id = f.chunk_id
                    JOIN documents AS d ON d.id = c.document_id
                    ORDER BY d.rel_path ASC, c.chunk_index ASC
                    """,
                ):
                    fts_chunks_snapshot.setdefault(str(row["rel_path"]), []).append(
                        {
                            "chunk_index": row.get("chunk_index"),
                            "text_hash": sha256_text(str(row.get("text_content") or "")),
                            "text_preview": str(row.get("text_content") or "")[:120],
                        }
                    )
            except sqlite3.Error:
                fts_chunks_snapshot = {}

        snapshot = {
            "counts": {
                "documents": len(documents_snapshot),
                "conversations": len(conversations_snapshot),
                "productions": len(productions_snapshot),
                "container_sources": len(container_sources_snapshot),
            },
            "ingest_result": normalize_ingest_payload(ingest_payload, workspace_root),
            "documents": documents_snapshot,
            "dataset_memberships": dataset_membership_snapshot,
            "document_source_parts": source_parts_snapshot,
            "document_previews": preview_rows_snapshot,
            "preview_files": preview_files_snapshot,
            "document_chunks": chunks_snapshot,
            "document_email_threading": email_threading_snapshot,
            "document_chat_threading": chat_threading_snapshot,
            "conversations": conversations_snapshot,
            "productions": productions_snapshot,
            "container_sources": container_sources_snapshot,
            "documents_fts": fts_documents_snapshot,
            "chunks_fts": fts_chunks_snapshot,
        }
        return normalize_object(snapshot, workspace_root)
    finally:
        connection.close()


def run_side(
    *,
    label: str,
    tool_path: Path,
    corpus_root: Path,
    output_dir: Path,
    recursive: bool,
    file_types: str | None,
    enable_benchmark: bool,
    include_synthetic_extras: bool,
) -> dict[str, object]:
    side_dir = output_dir / label
    workspace_root = side_dir / "workspace"
    commands_dir = side_dir / "commands"
    copy_corpus_into_workspace(
        corpus_root,
        workspace_root,
        include_synthetic_extras=include_synthetic_extras,
    )
    bootstrap_result = run_tool_command(
        tool_path,
        workspace_root,
        "bootstrap",
        [],
        enable_benchmark=enable_benchmark,
        artifact_dir=commands_dir,
    )
    ingest_args: list[str] = []
    if recursive:
        ingest_args.append("--recursive")
    if file_types:
        ingest_args.extend(["--file-types", file_types])
    ingest_result = run_tool_command(
        tool_path,
        workspace_root,
        "ingest",
        ingest_args,
        enable_benchmark=enable_benchmark,
        artifact_dir=commands_dir,
    )
    snapshot = build_workspace_snapshot(workspace_root, ingest_result["payload"])
    snapshot_path = side_dir / "snapshot.json"
    write_json(snapshot_path, snapshot)
    raw_payloads_path = side_dir / "command_results.json"
    write_json(
        raw_payloads_path,
        {
            "bootstrap": {
                "exit_code": bootstrap_result["exit_code"],
                "payload": normalize_object(bootstrap_result["payload"], workspace_root),
                "benchmark": bootstrap_result["benchmark"],
            },
            "ingest": {
                "exit_code": ingest_result["exit_code"],
                "payload": normalize_object(ingest_result["payload"], workspace_root),
                "benchmark": ingest_result["benchmark"],
            },
        },
    )
    return {
        "label": label,
        "tool_path": str(tool_path),
        "workspace_root": str(workspace_root),
        "snapshot": snapshot,
        "snapshot_path": str(snapshot_path),
        "command_results_path": str(raw_payloads_path),
        "bootstrap": bootstrap_result,
        "ingest": ingest_result,
    }


def build_diff(baseline_snapshot: dict[str, object], candidate_snapshot: dict[str, object]) -> str:
    baseline_text = json.dumps(baseline_snapshot, indent=2, sort_keys=True).splitlines()
    candidate_text = json.dumps(candidate_snapshot, indent=2, sort_keys=True).splitlines()
    return "\n".join(
        difflib.unified_diff(
            baseline_text,
            candidate_text,
            fromfile="baseline_snapshot.json",
            tofile="candidate_snapshot.json",
            lineterm="",
        )
    )


def benchmark_summary(benchmark_payload: dict[str, object] | None) -> dict[str, object] | None:
    if benchmark_payload is None:
        return None
    events = list(benchmark_payload.get("events") or [])
    deltas = list(benchmark_payload.get("deltas") or [])
    return {
        "event_count": len(events),
        "delta_count": len(deltas),
        "first_event": events[0]["name"] if events else None,
        "last_event": events[-1]["name"] if events else None,
        "total_delta_ms": round(sum(float(delta.get("delta_ms") or 0.0) for delta in deltas), 3),
    }


def main() -> int:
    args = parse_args()
    candidate_tool = args.candidate_tool.expanduser().resolve()
    baseline_tool = (
        args.baseline_tool.expanduser().resolve()
        if args.baseline_tool is not None
        else candidate_tool
    )
    corpus_root = args.corpus.expanduser().resolve()
    if not corpus_root.is_dir():
        raise SystemExit(f"Corpus directory does not exist: {corpus_root}")
    for tool_path in (baseline_tool, candidate_tool):
        if not tool_path.exists():
            raise SystemExit(f"Tool path does not exist: {tool_path}")
    output_dir = (
        args.output_dir.expanduser().resolve()
        if args.output_dir is not None
        else Path(tempfile.mkdtemp(prefix="retriever-ingest-corpus-diff-"))
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    recursive = not args.no_recursive
    include_synthetic_extras = not args.no_synthetic_extras
    enable_benchmark = not args.no_benchmark

    baseline_run = run_side(
        label=args.baseline_label,
        tool_path=baseline_tool,
        corpus_root=corpus_root,
        output_dir=output_dir,
        recursive=recursive,
        file_types=args.file_types,
        enable_benchmark=enable_benchmark,
        include_synthetic_extras=include_synthetic_extras,
    )
    candidate_run = run_side(
        label=args.candidate_label,
        tool_path=candidate_tool,
        corpus_root=corpus_root,
        output_dir=output_dir,
        recursive=recursive,
        file_types=args.file_types,
        enable_benchmark=enable_benchmark,
        include_synthetic_extras=include_synthetic_extras,
    )
    semantic_match = baseline_run["snapshot"] == candidate_run["snapshot"]
    diff_text = build_diff(baseline_run["snapshot"], candidate_run["snapshot"])
    diff_path = output_dir / "semantic_diff.patch"
    diff_path.write_text(diff_text + ("\n" if diff_text else ""), encoding="utf-8")
    changed_sections = sorted(
        key
        for key in set(baseline_run["snapshot"]) | set(candidate_run["snapshot"])
        if baseline_run["snapshot"].get(key) != candidate_run["snapshot"].get(key)
    )
    summary = {
        "semantic_match": semantic_match,
        "changed_sections": changed_sections,
        "artifacts_dir": str(output_dir),
        "corpus_root": str(corpus_root),
        "synthetic_extras": include_synthetic_extras,
        "recursive": recursive,
        "file_types": args.file_types,
        "baseline": {
            "label": baseline_run["label"],
            "tool_path": baseline_run["tool_path"],
            "snapshot_path": baseline_run["snapshot_path"],
            "ingest_benchmark": benchmark_summary(baseline_run["ingest"]["benchmark"]),
        },
        "candidate": {
            "label": candidate_run["label"],
            "tool_path": candidate_run["tool_path"],
            "snapshot_path": candidate_run["snapshot_path"],
            "ingest_benchmark": benchmark_summary(candidate_run["ingest"]["benchmark"]),
        },
        "diff_path": str(diff_path),
    }
    summary_path = output_dir / "summary.json"
    write_json(summary_path, summary)

    sys.stdout.write(
        "\n".join(
            [
                f"Artifacts: {output_dir}",
                f"Baseline tool: {baseline_tool}",
                f"Candidate tool: {candidate_tool}",
                f"Synthetic extras: {'on' if include_synthetic_extras else 'off'}",
                f"Semantic match: {'yes' if semantic_match else 'no'}",
                f"Changed sections: {', '.join(changed_sections) if changed_sections else '(none)'}",
                f"Summary: {summary_path}",
                f"Diff: {diff_path}",
            ]
        )
        + "\n"
    )
    if semantic_match or args.allow_diff:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
