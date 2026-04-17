from __future__ import annotations

import hashlib
import importlib.util
import json
import io
import mailbox
import re
import sqlite3
import tempfile
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
REGRESSION_CORPUS_ROOT = REPO_ROOT / "phase0" / "regression_corpus"

retriever_tools = None
TOOL_BYTES = b""

def load_python_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
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


def setUpModule() -> None:
    global retriever_tools, TOOL_BYTES
    assert_bundled_tooling_current()
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
        attachment_name: str | None = None,
        attachment_text: str | None = None,
    ) -> None:
        message = EmailMessage()
        message["From"] = "Alice Example <alice@example.com>"
        message["To"] = "Bob Example <bob@example.com>"
        message["Cc"] = "Carol Example <carol@example.com>"
        message["Subject"] = subject
        message["Date"] = "Tue, 14 Apr 2026 10:00:00 +0000"
        message.set_content(body_text)
        if attachment_name is not None and attachment_text is not None:
            message.add_attachment(attachment_text, subtype="plain", filename=attachment_name)
        path.write_bytes(message.as_bytes(policy=policy.default))

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
            self.assertIn("content_type", columns)
            self.assertIn("custodian", columns)
            self.assertIn("participants", columns)
            self.assertIn("control_number", columns)
            self.assertIn("dataset_id", columns)
            self.assertIn("parent_document_id", columns)
            row = connection.execute(
                """
                SELECT content_type, custodian, participants, control_number, dataset_id, parent_document_id
                FROM documents
                WHERE id = 1
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
        self.assertIsNotNone(row["dataset_id"])
        self.assertIsNone(row["parent_document_id"])
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
        self.assertTrue(str(child_row["rel_path"]).startswith(".retriever/previews/thread.eml/attachments/"))
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

        with mock.patch.object(retriever_tools, "pypff", object()):
            doctor_result = retriever_tools.doctor(self.root, quick=False)
        self.assertEqual(doctor_result["workspace_inventory"]["parent_documents"], 1)
        self.assertEqual(doctor_result["workspace_inventory"]["attachment_children"], 1)
        self.assertEqual(doctor_result["workspace_inventory"]["documents_total"], 2)
        self.assertIn(doctor_result["sqlite_journal_mode"], {"wal", "delete"})

        with self.assertRaises(retriever_tools.RetrieverError) as context:
            retriever_tools.search(self.root, "", None, "is_attachment", None, 1, 20)
        self.assertIn("virtual filter field", str(context.exception))

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

        search_result = retriever_tools.search(self.root, "Budget approved", None, None, None, 1, 20)
        self.assertEqual(search_result["results"][0]["file_name"], "memo.rtf")
        self.assertTrue(search_result["results"][0]["preview_rel_path"].endswith(".html"))
        self.assertEqual(search_result["results"][0]["preview_targets"][0]["preview_type"], "html")

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
        self.assertIn("Retriever Chat Preview", preview_html)
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
        self.assertIn("Retriever Chat Preview", preview_html)
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
            all(not str(item["rel_path"]).startswith(".retriever/previews/thread.eml/attachments/") or item["parent_document_id"] is not None
                for item in browse_results["results"])
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
        self.assertTrue(str(child_row["rel_path"]).startswith(".retriever/previews/mailbox.mbox/attachments/"))

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
        self.assertTrue(parent_result["preview_rel_path"].startswith(".retriever/previews/mailbox.mbox/messages/"))
        parent_preview_html = Path(parent_result["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("Apr 14, 2026 10:00 AM UTC", parent_preview_html)

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
        self.assertTrue(str(child_row["rel_path"]).startswith(".retriever/previews/mailbox.pst/attachments/"))

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
        self.assertTrue(parent_result["preview_rel_path"].startswith(".retriever/previews/mailbox.pst/messages/"))
        parent_preview_html = Path(parent_result["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("Apr 14, 2026 10:00 AM UTC", parent_preview_html)

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

        search_result = retriever_tools.search(self.root, "draft the update", None, None, None, 1, 20)
        result = search_result["results"][0]
        self.assertTrue(result["preview_rel_path"].startswith(".retriever/previews/mailbox.pst/messages/"))
        self.assertEqual(result["preview_targets"][0]["preview_type"], "html")
        preview_html = Path(result["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("Retriever Chat Preview", preview_html)
        self.assertIn("Alice Example", preview_html)
        self.assertIn("Bob Example", preview_html)
        self.assertIn("Kickoff thread for launch planning.", preview_html)

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
        preview_html = Path(result["preview_targets"][0]["abs_path"]).read_text(encoding="utf-8")
        self.assertIn("Retriever Chat Preview", preview_html)
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
        self.assertIn("Retriever Calendar Preview", preview_html)
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

        parent_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000001.logical")
        child_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000003.logical")
        native_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000004.logical")
        image_only_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000005.logical")

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

        parent_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000001.logical")
        native_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000004.logical")
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

        retired_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000005.logical")
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


class FilesystemIngestRevisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="retriever-fs-rev-")
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.paths = retriever_tools.workspace_paths(self.root)

    def _sample_eml(self) -> Path:
        path = self.root / "sample.eml"
        message = EmailMessage()
        message["From"] = "Sender <sender@example.com>"
        message["To"] = "Receiver <receiver@example.com>"
        message["Subject"] = "Salt fingerprint test"
        message["Date"] = "Tue, 14 Apr 2026 10:00:00 +0000"
        message.set_content("Body.")
        path.write_bytes(message.as_bytes(policy=policy.default))
        return path

    def test_fingerprint_is_salted_and_differs_from_raw_sha(self) -> None:
        path = self._sample_eml()
        raw = retriever_tools.sha256_file(path)
        salted = retriever_tools.filesystem_ingest_fingerprint(path)
        self.assertIsNotNone(salted)
        self.assertNotEqual(salted, raw)
        expected = retriever_tools.sha256_text(
            f"{retriever_tools.FILESYSTEM_INGEST_REVISION}:{raw}"
        )
        self.assertEqual(salted, expected)

    def test_ingest_stores_salted_fingerprint_in_file_hash(self) -> None:
        path = self._sample_eml()
        retriever_tools.bootstrap(self.root)
        retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        connection = sqlite3.connect(str(self.paths["db_path"]))
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                "SELECT file_hash FROM documents WHERE rel_path = ?", ("sample.eml",)
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row["file_hash"], retriever_tools.filesystem_ingest_fingerprint(path))

    def test_revision_bump_forces_reingest_of_unchanged_files(self) -> None:
        path = self._sample_eml()
        retriever_tools.bootstrap(self.root)
        retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)

        connection = sqlite3.connect(str(self.paths["db_path"]))
        connection.row_factory = sqlite3.Row
        try:
            original_hash = connection.execute(
                "SELECT file_hash FROM documents WHERE rel_path = ?", ("sample.eml",)
            ).fetchone()["file_hash"]
            connection.execute(
                "UPDATE documents SET file_hash = ? WHERE rel_path = ?",
                ("sha256-from-an-older-revision-" + "0" * 40, "sample.eml"),
            )
            connection.commit()
        finally:
            connection.close()

        result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["skipped"], 0)

        connection = sqlite3.connect(str(self.paths["db_path"]))
        connection.row_factory = sqlite3.Row
        try:
            refreshed_hash = connection.execute(
                "SELECT file_hash FROM documents WHERE rel_path = ?", ("sample.eml",)
            ).fetchone()["file_hash"]
        finally:
            connection.close()
        self.assertEqual(refreshed_hash, original_hash)

        third_run = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(third_run["updated"], 0)
        self.assertEqual(third_run["skipped"], 1)


if __name__ == "__main__":
    unittest.main()
