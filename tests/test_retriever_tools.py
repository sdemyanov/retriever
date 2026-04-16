from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sqlite3
import tempfile
import unittest
import zipfile
import base64
from email import policy
from email.message import EmailMessage
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "skills" / "tool-template" / "retriever_tools.py"
BUNDLER_PATH = REPO_ROOT / "skills" / "tool-template" / "bundle_retriever_tools.py"
TOOL_TEMPLATE_PATH = REPO_ROOT / "skills" / "tool-template" / "tool-template.md"

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

    def build_fake_pst_message(
        self,
        *,
        source_item_id: str,
        subject: str,
        body_text: str,
        folder_path: str = "Inbox",
        attachment_name: str | None = None,
        attachment_text: str | None = None,
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
            "author": "Alice Example <alice@example.com>",
            "recipients": "Bob Example <bob@example.com>, Carol Example <carol@example.com>",
            "date_created": "2026-04-14T10:00:00Z",
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
            self.assertIn("participants", columns)
            self.assertIn("control_number", columns)
            self.assertIn("parent_document_id", columns)
            row = connection.execute(
                "SELECT content_type, participants, control_number, parent_document_id FROM documents WHERE id = 1"
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
        self.assertIsNone(row["participants"])
        self.assertEqual(row["control_number"], "DOC001.00000001")
        self.assertIsNone(row["parent_document_id"])
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
        self.assertEqual(sibling_row["source_folder_path"], "Sent Items")
        self.assertTrue(str(child_row["rel_path"]).startswith(".retriever/previews/mailbox.pst/attachments/"))

        connection = retriever_tools.connect_db(self.paths["db_path"])
        try:
            container_row = connection.execute(
                "SELECT * FROM container_sources WHERE source_kind = ? AND source_rel_path = ?",
                (retriever_tools.PST_SOURCE_KIND, "mailbox.pst"),
            ).fetchone()
        finally:
            connection.close()

        self.assertIsNotNone(container_row)
        self.assertEqual(container_row["message_count"], 2)
        self.assertEqual(container_row["file_size"], pst_path.stat().st_size)
        self.assertIsNotNone(container_row["last_scan_completed_at"])

        parent_search = retriever_tools.search(self.root, "PST Parent", None, None, None, 1, 20)
        parent_result = next(item for item in parent_search["results"] if item["id"] == parent_row["id"])
        self.assertEqual(parent_result["attachment_count"], 1)
        self.assertEqual(parent_result["source_rel_path"], "mailbox.pst")
        self.assertEqual(parent_result["source_folder_path"], "Inbox")
        self.assertTrue(parent_result["preview_rel_path"].startswith(".retriever/previews/mailbox.pst/messages/"))

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

        attachment_search = retriever_tools.search(self.root, "pst attachment body", None, None, None, 1, 20)
        attachment_result = next(item for item in attachment_search["results"] if item["id"] == child_row["id"])
        self.assertEqual(attachment_result["parent"]["control_number"], parent_row["control_number"])

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
        finally:
            connection.close()

        self.assertIsNotNone(production_row)
        self.assertEqual(production_row["production_name"], "Synthetic_Production")
        self.assertTrue(any(row["part_kind"] == "native" for row in source_parts))

        parent_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000001.logical")
        child_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000003.logical")
        native_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000004.logical")
        image_only_row = self.fetch_document_row(".retriever/productions/Synthetic_Production/documents/PDX000005.logical")

        self.assertEqual(parent_row["control_number"], "PDX000001")
        self.assertEqual(parent_row["begin_bates"], "PDX000001")
        self.assertEqual(parent_row["end_bates"], "PDX000002")
        self.assertEqual(parent_row["source_kind"], retriever_tools.PRODUCTION_SOURCE_KIND)
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
        self.assertEqual(child_row["content_type"], "Email")
        self.assertEqual(child_row["author"], "Review Team")
        self.assertEqual(child_row["date_created"], "2026-04-14T09:00:00Z")
        self.assertEqual(child_row["title"], "Case status update")
        self.assertEqual(native_row["file_name"], "PDX000004.pdf")
        self.assertEqual(native_row["content_type"], "E-Doc")
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

    def test_set_field_rejects_system_managed_fields(self) -> None:
        document_path = self.root / "sample.txt"
        document_path.write_text("hello\n", encoding="utf-8")

        retriever_tools.bootstrap(self.root)
        ingest_result = retriever_tools.ingest(self.root, recursive=True, raw_file_types=None)
        self.assertEqual(ingest_result["new"], 1)

        row = self.fetch_document_row("sample.txt")
        blocked_fields = [
            "control_number",
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


if __name__ == "__main__":
    unittest.main()
