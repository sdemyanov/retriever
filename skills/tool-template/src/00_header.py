#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import argparse
import base64
import csv
import difflib
import errno
import hashlib
import html
import importlib
import io
import json
import mailbox
import mimetypes
import os
import pickle
import posixpath
import platform
import re
import secrets
import shlex
import shutil
import site
import sqlite3
import subprocess
import sys
import tempfile
import time
import unicodedata
import venv
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterator
from urllib import error as urllib_error
from urllib import request as urllib_request
from zoneinfo import ZoneInfo

_UNLOADED_DEPENDENCY = object()
ACTIVE_WORKSPACE_ROOT: Path | None = None
ACTIVATED_PLUGIN_SITE_PACKAGES: set[str] = set()

# Third-party parsing dependencies load on demand so ordinary commands do not
# depend on every parser being importable up front.
charset_normalizer = _UNLOADED_DEPENDENCY
extract_msg = _UNLOADED_DEPENDENCY
openpyxl = _UNLOADED_DEPENDENCY
xlrd = _UNLOADED_DEPENDENCY
pdfplumber = _UNLOADED_DEPENDENCY
DocxDocument = _UNLOADED_DEPENDENCY
rtf_to_text = _UNLOADED_DEPENDENCY
PilImage = _UNLOADED_DEPENDENCY
pypff = _UNLOADED_DEPENDENCY

try:
    import fcntl
except Exception:  # pragma: no cover - platform-specific locking
    fcntl = None

try:
    import msvcrt
except Exception:  # pragma: no cover - platform-specific locking
    msvcrt = None


TOOL_VERSION = "1.0.3"
SCHEMA_VERSION = 23
SESSION_SCHEMA_VERSION = 2
REQUIREMENTS_VERSION = "2026-04-21-phase11-document-deduplication"
TEMPLATE_SOURCE = "skills/tool-template/tools.py"
PINNED_RUNTIME_REQUIREMENTS = (
    "pdfplumber==0.11.9",
    "python-docx==1.2.0",
    "openpyxl==3.1.5",
    "xlrd==2.0.1",
    "extract-msg==0.55.0",
    "libpff-python==20231205",
    "striprtf==0.0.26",
    "Pillow==10.3.0",
    "charset-normalizer==3.4.7",
)
MANUAL_FIELD_LOCKS_COLUMN = "manual_field_locks_json"
LEGACY_METADATA_LOCKS_COLUMN = "locked_metadata_fields_json"
CHUNK_TARGET_CHARS = 3200
CHUNK_OVERLAP_CHARS = 250
CONVERSATION_PREVIEW_MAX_CHARS = 180000
DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 100
BROWSE_MODE_DOCUMENTS = "documents"
BROWSE_MODE_CONVERSATIONS = "conversations"
DEFAULT_BROWSE_MODE = BROWSE_MODE_DOCUMENTS
DEFAULT_DOCUMENT_DISPLAY_COLUMNS = (
    "content_type",
    "title",
    "author",
    "date_created",
    "control_number",
)
DEFAULT_DISPLAY_COLUMNS = DEFAULT_DOCUMENT_DISPLAY_COLUMNS
DEFAULT_CONVERSATION_DISPLAY_COLUMNS = (
    "conversation_type",
    "title",
    "participants",
    "last_activity",
    "document_count",
)
MAX_SCOPE_DATASETS = 999
MAX_FILTER_EXPRESSION_LENGTH = 8192
MAX_FILTER_IN_LIST_ITEMS = 999
MAX_SAVED_SCOPES = 256
DEFAULT_CHUNK_PAGE_SIZE = 50
MAX_CHUNK_PAGE_SIZE = 200
GET_DOC_SUMMARY_CHARS = 1200
MAX_GET_DOC_CHUNKS = 10
MAX_GET_DOC_TEXT_CHARS = 30000
DEFAULT_CHUNK_SEARCH_TOP_K = 12
MAX_CHUNK_SEARCH_TOP_K = 50
DEFAULT_CHUNK_SEARCH_PER_DOC_CAP = 3
MAX_CHUNK_SEARCH_PER_DOC_CAP = 10
MAX_CHUNK_SEARCH_TEXT_CHARS = 100000
DEFAULT_AGGREGATE_LIMIT = 20
MAX_AGGREGATE_LIMIT = 200
CONTROL_NUMBER_PREFIX = "DOC"
CONTROL_NUMBER_BATCH_WIDTH = 3
CONTROL_NUMBER_FAMILY_WIDTH = 8

BENCHMARK_ENABLED = os.environ.get("RETRIEVER_BENCHMARK") == "1"
BENCHMARK_EVENTS: list[dict[str, object]] = []
if BENCHMARK_ENABLED:
    BENCHMARK_EVENTS.append({"name": "module_import_start", "ts": time.perf_counter()})


def benchmark_mark(name: str, **fields: object) -> None:
    if not BENCHMARK_ENABLED:
        return
    event: dict[str, object] = {"name": name, "ts": time.perf_counter()}
    if fields:
        event.update(fields)
    BENCHMARK_EVENTS.append(event)


def benchmark_payload(**fields: object) -> dict[str, object]:
    events: list[dict[str, object]] = []
    deltas: list[dict[str, object]] = []
    for event in BENCHMARK_EVENTS:
        events.append({key: value for key, value in event.items() if key != "ts"})
    for previous, current in zip(BENCHMARK_EVENTS, BENCHMARK_EVENTS[1:]):
        deltas.append(
            {
                "from": previous["name"],
                "to": current["name"],
                "delta_ms": round((float(current["ts"]) - float(previous["ts"])) * 1000.0, 3),
            }
        )
    payload: dict[str, object] = {
        "enabled": BENCHMARK_ENABLED,
        "events": events,
        "deltas": deltas,
    }
    if fields:
        payload.update(fields)
    return payload


def benchmark_emit(**fields: object) -> None:
    if not BENCHMARK_ENABLED:
        return
    sys.stderr.write(json.dumps({"_bench": benchmark_payload(**fields)}) + "\n")
    sys.stderr.flush()
CONTROL_NUMBER_ATTACHMENT_WIDTH = 3
EMU_PER_PIXEL = 9525
IMAGE_NATIVE_PREVIEW_FILE_TYPES = {
    "bmp",
    "gif",
    "jpeg",
    "jpg",
    "png",
    "tif",
    "tiff",
    "webp",
}
CURATED_TEXT_SOURCE_FILE_TYPES = {
    "bash",
    "c",
    "cfg",
    "conf",
    "cpp",
    "cs",
    "css",
    "go",
    "h",
    "hpp",
    "ini",
    "java",
    "js",
    "jsx",
    "kt",
    "less",
    "php",
    "properties",
    "ps1",
    "py",
    "rb",
    "rs",
    "scss",
    "sh",
    "sql",
    "swift",
    "toml",
    "ts",
    "tsx",
    "xml",
    "yaml",
    "yml",
    "zsh",
}
SUPPORTED_FILE_TYPES = {
    "bash",
    "bmp",
    "c",
    "cfg",
    "conf",
    "cpp",
    "csv",
    "cs",
    "css",
    "docx",
    "eml",
    "gif",
    "go",
    "h",
    "htm",
    "html",
    "hpp",
    "ics",
    "ini",
    "java",
    "jpeg",
    "jpg",
    "json",
    "js",
    "jsx",
    "kt",
    "less",
    "mbox",
    "md",
    "msg",
    "pdf",
    "php",
    "pst",
    "png",
    "pptx",
    "properties",
    "ps1",
    "py",
    "rb",
    "rs",
    "rtf",
    "scss",
    "sh",
    "sql",
    "swift",
    "tif",
    "tiff",
    "toml",
    "txt",
    "ts",
    "tsx",
    "webp",
    "xls",
    "xml",
    "xlsx",
    "yaml",
    "yml",
    "zsh",
}
NATIVE_PREVIEW_FILE_TYPES = {
    "bash",
    "bmp",
    "c",
    "cfg",
    "conf",
    "cpp",
    "csv",
    "cs",
    "css",
    "docx",
    "gif",
    "go",
    "h",
    "htm",
    "html",
    "hpp",
    "ics",
    "ini",
    "java",
    "jpeg",
    "jpg",
    "json",
    "js",
    "jsx",
    "kt",
    "less",
    "md",
    "pdf",
    "php",
    "png",
    "properties",
    "ps1",
    "py",
    "rb",
    "rs",
    "scss",
    "sh",
    "sql",
    "swift",
    "tif",
    "tiff",
    "toml",
    "txt",
    "ts",
    "tsx",
    "webp",
    "xml",
    "yaml",
    "yml",
    "zsh",
}
TEXT_FILE_TYPES = {"csv", "htm", "html", "ics", "json", "md", "txt", *CURATED_TEXT_SOURCE_FILE_TYPES}
EDITABLE_BUILTIN_FIELDS = {
    "author",
    "content_type",
    "date_created",
    "date_modified",
    "page_count",
    "participants",
    "recipients",
    "subject",
    "title",
}
CONVERSATION_ASSIGNMENT_MODE_AUTO = "auto"
CONVERSATION_ASSIGNMENT_MODE_MANUAL = "manual"
SYSTEM_MANAGED_FIELDS = {
    "active_search_text_revision_id",
    "active_text_language",
    "active_text_quality_score",
    "active_text_source_kind",
    "canonical_kind",
    "canonical_status",
    "content_hash",
    "control_number_attachment_sequence",
    "control_number_batch",
    "control_number_family_sequence",
    "control_number",
    "conversation_id",
    "conversation_assignment_mode",
    "dataset_id",
    "file_hash",
    "file_name",
    "file_size",
    "file_type",
    "id",
    "ingested_at",
    "last_seen_at",
    "lifecycle_status",
    MANUAL_FIELD_LOCKS_COLUMN,
    LEGACY_METADATA_LOCKS_COLUMN,
    "merged_into_document_id",
    "begin_attachment",
    "begin_bates",
    "end_attachment",
    "end_bates",
    "child_document_kind",
    "parent_document_id",
    "production_id",
    "rel_path",
    "root_message_key",
    "source_folder_path",
    "source_item_id",
    "source_rel_path",
    "source_kind",
    "source_text_revision_id",
    "text_status",
    "updated_at",
}
BUILTIN_FIELD_TYPES = {
    "id": "integer",
    "canonical_kind": "text",
    "canonical_status": "text",
    "control_number": "text",
    "conversation_id": "integer",
    "conversation_assignment_mode": "text",
    "dataset_id": "integer",
    "merged_into_document_id": "integer",
    "parent_document_id": "integer",
    "child_document_kind": "text",
    "source_kind": "text",
    "source_rel_path": "text",
    "source_item_id": "text",
    "root_message_key": "text",
    "source_folder_path": "text",
    "production_id": "integer",
    "begin_bates": "text",
    "end_bates": "text",
    "begin_attachment": "text",
    "end_attachment": "text",
    "rel_path": "text",
    "file_name": "text",
    "file_type": "text",
    "file_size": "integer",
    "page_count": "integer",
    "author": "text",
    "content_type": "text",
    "date_created": "date",
    "date_modified": "date",
    "title": "text",
    "subject": "text",
    "participants": "text",
    "recipients": "text",
    MANUAL_FIELD_LOCKS_COLUMN: "text",
    "file_hash": "text",
    "content_hash": "text",
    "source_text_revision_id": "integer",
    "active_search_text_revision_id": "integer",
    "active_text_source_kind": "text",
    "active_text_language": "text",
    "active_text_quality_score": "real",
    "text_status": "text",
    "lifecycle_status": "text",
    "ingested_at": "date",
    "last_seen_at": "date",
    "updated_at": "date",
    "control_number_batch": "integer",
    "control_number_family_sequence": "integer",
    "control_number_attachment_sequence": "integer",
}
FIELD_NAME_ALIASES = {
    "dataset": "dataset_name",
    "dataset_label": "dataset_name",
    "collected_from": "custodian",
    "created_date": "date_created",
    "modified_date": "date_modified",
}
PASSIVE_FIELD_LABELS = {
    "active_search_text_revision_id": "Active Search Revision ID",
    "active_text_language": "Active Text Language",
    "active_text_quality_score": "Active Text Quality Score",
    "active_text_source_kind": "Active Text Source",
    "author": "Author",
    "begin_attachment": "Begin Attachment",
    "begin_bates": "Begin Bates",
    "canonical_kind": "Family",
    "canonical_status": "Record Status",
    "child_document_kind": "Document Role",
    "content_hash": "Content Hash",
    "content_type": "Type",
    "control_number": "Control #",
    "control_number_attachment_sequence": "Attachment Seq.",
    "control_number_batch": "Batch",
    "control_number_family_sequence": "Family Seq.",
    "conversation_assignment_mode": "Assignment Mode",
    "conversation_id": "Conversation ID",
    "conversation_type": "Type",
    "custodian": "Custodian",
    "dataset_id": "Dataset ID",
    "dataset_name": "Dataset",
    "date_created": "Created",
    "date_modified": "Modified",
    "document_count": "Documents",
    "end_attachment": "End Attachment",
    "end_bates": "End Bates",
    "file_hash": "File Hash",
    "file_name": "File",
    "file_size": "Size",
    "file_type": "File Type",
    "first_activity": "Started",
    "has_attachments": "Has Attachments",
    "id": "ID",
    "ingested_at": "Ingested",
    "is_attachment": "Attachment",
    "last_activity": "Last Activity",
    "last_seen_at": "Last Seen",
    "lifecycle_status": "File Status",
    "matching_document_count": "Matches",
    "merged_into_document_id": "Merged Into ID",
    "page_count": "Pages",
    "parent_document_id": "Parent ID",
    "participants": "Participants",
    "production_id": "Production ID",
    "production_name": "Production",
    "recipients": "Recipients",
    "rel_path": "Path",
    "root_message_key": "Root Message Key",
    "source_folder_path": "Source Folder",
    "source_item_id": "Source Item ID",
    "source_kind": "Source",
    "source_rel_path": "Source Path",
    "source_text_revision_id": "Source Text Revision ID",
    "subject": "Subject",
    "text_status": "Text Status",
    "title": "Title",
    "updated_at": "Updated",
}
PASSIVE_MIXED_CONTEXT_FIELD_LABELS = {
    "content_type": "Document Type",
    "conversation_type": "Conversation Type",
}
PASSIVE_FIELD_LABEL_UPPERCASE_TOKENS = {
    "api",
    "csv",
    "eml",
    "fts",
    "html",
    "id",
    "json",
    "mbox",
    "msg",
    "ocr",
    "pdf",
    "pst",
    "sql",
    "tsv",
    "uri",
    "url",
    "utc",
    "xml",
}


def passive_field_label(field_name: object, *, mixed_context: bool = False) -> str:
    normalized_name = str(field_name or "").strip()
    if not normalized_name:
        return ""
    canonical_name = FIELD_NAME_ALIASES.get(normalized_name, normalized_name)
    if mixed_context:
        mixed_context_label = PASSIVE_MIXED_CONTEXT_FIELD_LABELS.get(canonical_name)
        if mixed_context_label:
            return mixed_context_label
    label = PASSIVE_FIELD_LABELS.get(canonical_name)
    if label:
        return label
    words: list[str] = []
    for token in re.split(r"[_\s]+", canonical_name):
        if not token:
            continue
        lower_token = token.lower()
        if lower_token in PASSIVE_FIELD_LABEL_UPPERCASE_TOKENS:
            words.append(lower_token.upper())
        elif token.isupper():
            words.append(token)
        else:
            words.append(token[:1].upper() + token[1:])
    return " ".join(words)


REGISTRY_FIELD_TYPES = {
    "boolean": "INTEGER",
    "date": "TEXT",
    "integer": "INTEGER",
    "real": "REAL",
    "text": "TEXT",
}
VIRTUAL_FILTER_FIELD_TYPES = {
    "custodian": "text",
    "dataset_name": "text",
    "is_attachment": "boolean",
    "has_attachments": "boolean",
    "production_name": "text",
}
DISPLAYABLE_VIRTUAL_FIELDS = {
    "custodian",
    "dataset_name",
    "is_attachment",
    "production_name",
}
CATALOG_EXCLUDED_BUILTIN_FIELDS = {
    "active_search_text_revision_id",
    "active_text_language",
    "active_text_quality_score",
    "active_text_source_kind",
    "content_hash",
    "conversation_assignment_mode",
    "dataset_id",
    "file_hash",
    "id",
    MANUAL_FIELD_LOCKS_COLUMN,
    LEGACY_METADATA_LOCKS_COLUMN,
    "production_id",
    "root_message_key",
    "source_text_revision_id",
}
CATALOG_EXCLUDED_CUSTOM_FIELDS = {
    MANUAL_FIELD_LOCKS_COLUMN,
    LEGACY_METADATA_LOCKS_COLUMN,
}
AGGREGATABLE_VIRTUAL_FIELDS = {"dataset_name"}
BUILTIN_FIELD_DESCRIPTIONS = {
    "author": "Document author from file metadata",
    "begin_bates": "Beginning Bates label for a production document",
    "canonical_kind": "Normalized logical content family used for dedupe safety checks",
    "canonical_status": "Logical document lifecycle state such as active, derelict, or merged",
    "content_type": "Normalized content category such as Email, E-Doc, or Chat",
    "control_number": "Stable document label used for review and export",
    "conversation_id": "Internal conversation grouping id shared by related documents",
    "conversation_assignment_mode": "Whether conversation grouping is automatic or manually pinned",
    "custodian": "Custodian or mailbox owner associated with the document",
    "date_created": "ISO date the document was created",
    "date_modified": "ISO date the document was last modified",
    "end_bates": "Ending Bates label for a production document",
    "file_name": "Document file name",
    "file_size": "File size in bytes",
    "file_type": "Normalized file extension such as pdf, docx, or eml",
    "merged_into_document_id": "Canonical survivor id when this document has been merged",
    "page_count": "Page or sheet count when available",
    "child_document_kind": "Contained-child semantics such as attachment or reply_thread",
    "participants": "Participants extracted from chat or email-style content",
    "recipients": "Recipients extracted from message metadata",
    "rel_path": "Workspace-relative document path",
    "source_folder_path": "Container-relative source folder path for container-derived items",
    "source_kind": "Origin kind such as filesystem, production, email_attachment, pst, or mbox",
    "source_rel_path": "Source-relative path used to derive the document",
    "subject": "Email or message subject line",
    "title": "Document title from metadata or content",
    "updated_at": "ISO timestamp when Retriever last updated the document row",
}
VIRTUAL_FIELD_DESCRIPTIONS = {
    "custodian": "Custodian or mailbox owner across the document's active collected copies",
    "dataset_name": "Logical dataset label for PSTs, MBOXes, productions, and manual document sets",
    "has_attachments": "Whether the document has one or more attachment child documents",
    "is_attachment": "Whether the document is an attachment child document",
    "production_name": "Friendly production name for production-derived documents",
}
INTERNAL_DOCUMENT_COLUMNS = {
    "custodians_json",
}
CONTENT_TYPE_EXTENSION_GROUPS = [
    ("Email", "dbx eml emlx mbox msg nsf ost p7m p7s pst tnef vcf"),
    (
        "Spreadsheet / Table",
        "123 csv dat dif gsheet numbers ods ots qpw slk sxc tsv uxdc wk1 wk3 wk4 wks wq1 xla xlam xlm xls xlsb xlsm xlsx xlt xltm xltx xlw",
    ),
    ("Presentation", "gslides key odp pez pot potm potx pps ppsm ppsx ppt pptm pptx prz sdd show shw sldm sldx sti"),
    ("Image", "bmp cut dds emf emz exf exif fax gif hpg hpgl ico iff jng jpeg jpg koala lbm pbm pcd pcx pgm plo plt png ppm prn psd ras sgi snp svg svgz targa tga tif tiff wbmp wdp webp wmf wmz xbm"),
    ("Audio", "3ga aac ac3 aif aifc aiff amr ape au awb dss dvf flac gsm m3u m4a m4p m4r mid midi mmf mp3 msv ogg opus pcm ra ram raw voc wav wma wv"),
    ("Video", "3g2 3gp amv asf avi divx drc flv gifv m2ts m2v m4v mkv mng mov mp4 mpeg mpg mxf nsv ogv qt rm rmvb roq ts vob webm wmv yuv"),
    ("Database", "accdb bak bson dbf dmp exp frm json ldf mdb mdf myd myi ndf odb ora pdb rdb sql"),
    ("Web", "htm html mht mhtml xhtm xhtml xml"),
    ("E-Doc", "doc docm docx dot dotm dotx eps fm one pdf ps pub rtf txt vdx vsd vsdm vsdx vss vst vsw vsx wpd wps"),
    (
        "Source Code",
        "asm bash c cfg class coffee conf cpp cs css dart ear egg egg-info elm ex exs f90 fs go gradle groovy h hs ini ipynb jar java javafx jl jmod jnlp js jsh jsp jspx jsx jws kt less lisp lua m ml mm pas perl pex php pipfile pl pom properties ps1 pth py pyc pyd pyi pyo pyw pyz pyzw r rb requirementstxt rs sass scala scss sh sol swift toml ts tsx vb vue war whl yaml yml zsh",
    ),
    ("Container", "7z alzip bz2 cab e01 ex01 gz l01 lx01 rar tar z zip"),
    ("Calendar", "calendar ical icalendar ics ifb invite vcal vcs"),
    ("CAD", "3dxml asmdot drwdot dwg dxf easm easmx edrw edrwx eprt eprtx prtdot sldasm slddrw sldprt stl"),
    ("Message", "rsmf"),
]

OOXML_RELATIONSHIP_NS = {"rels": "http://schemas.openxmlformats.org/package/2006/relationships"}
PPTX_NAMESPACES = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
PPTX_NOTES_RELATIONSHIP_TYPE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/notesSlide"
PRODUCTION_SOURCE_KIND = "production"
EMAIL_ATTACHMENT_SOURCE_KIND = "email_attachment"
EMAIL_CONVERSATION_SOURCE_KIND = "email_conversation"
FILESYSTEM_SOURCE_KIND = "filesystem"
MBOX_SOURCE_KIND = "mbox"
PST_SOURCE_KIND = "pst"
SLACK_EXPORT_SOURCE_KIND = "slack_export"
MANUAL_DATASET_SOURCE_KIND = "manual"
ACTIVE_OCCURRENCE_STATUS = "active"
OCCURRENCE_LIFECYCLE_STATUSES = {"active", "superseded", "missing", "deleted"}
CANONICAL_STATUS_ACTIVE = "active"
CANONICAL_STATUS_DERELICT = "derelict"
CANONICAL_STATUS_MERGED = "merged"
ENTITY_TYPE_PERSON = "person"
ENTITY_TYPE_ORGANIZATION = "organization"
ENTITY_TYPE_SHARED_MAILBOX = "shared_mailbox"
ENTITY_TYPE_SYSTEM_MAILBOX = "system_mailbox"
ENTITY_TYPE_UNKNOWN = "unknown"
ENTITY_TYPES = {
    ENTITY_TYPE_PERSON,
    ENTITY_TYPE_ORGANIZATION,
    ENTITY_TYPE_SHARED_MAILBOX,
    ENTITY_TYPE_SYSTEM_MAILBOX,
    ENTITY_TYPE_UNKNOWN,
}
ENTITY_ORIGIN_OBSERVED = "observed"
ENTITY_ORIGIN_IDENTIFIED = "identified"
ENTITY_ORIGIN_MANUAL = "manual"
ENTITY_ORIGINS = {
    ENTITY_ORIGIN_OBSERVED,
    ENTITY_ORIGIN_IDENTIFIED,
    ENTITY_ORIGIN_MANUAL,
}
ENTITY_STATUS_ACTIVE = "active"
ENTITY_STATUS_MERGED = "merged"
ENTITY_STATUS_IGNORED = "ignored"
ENTITY_STATUSES = {
    ENTITY_STATUS_ACTIVE,
    ENTITY_STATUS_MERGED,
    ENTITY_STATUS_IGNORED,
}
ENTITY_DISPLAY_SOURCE_AUTO = "auto"
ENTITY_DISPLAY_SOURCE_MANUAL = "manual"
ENTITY_DISPLAY_SOURCES = {
    ENTITY_DISPLAY_SOURCE_AUTO,
    ENTITY_DISPLAY_SOURCE_MANUAL,
}
ENTITY_IDENTIFIER_TYPES = {
    "email",
    "phone",
    "name",
    "handle",
    "external_id",
}
DOCUMENT_ENTITY_ROLES = {
    "author",
    "participant",
    "recipient",
    "custodian",
}
CANONICAL_KIND_VALUES = {
    "email",
    "document",
    "spreadsheet",
    "presentation",
    "image",
    "code",
    "data",
    "binary",
    "unknown",
}
TEXT_STATUS_PRIORITIES = {
    "ok": 0,
    "partial": 1,
    "empty": 2,
    "failed": 3,
    "error": 4,
}
SOURCE_KIND_PREFERRED_ORDER = {
    PRODUCTION_SOURCE_KIND: 0,
    FILESYSTEM_SOURCE_KIND: 1,
    PST_SOURCE_KIND: 2,
    MBOX_SOURCE_KIND: 3,
    SLACK_EXPORT_SOURCE_KIND: 4,
    EMAIL_ATTACHMENT_SOURCE_KIND: 5,
}
CHILD_DOCUMENT_KIND_ATTACHMENT = "attachment"
CHILD_DOCUMENT_KIND_REPLY_THREAD = "reply_thread"
ALLOWED_CHILD_DOCUMENT_KINDS = {
    CHILD_DOCUMENT_KIND_ATTACHMENT,
    CHILD_DOCUMENT_KIND_REPLY_THREAD,
}
PRODUCTION_DAT_HEADER_ALIASES = {
    "begbates": "begin_bates",
    "beginbates": "begin_bates",
    "begin bates": "begin_bates",
    "endbates": "end_bates",
    "end bates": "end_bates",
    "begattach": "begin_attachment",
    "beginattach": "begin_attachment",
    "begin attachment": "begin_attachment",
    "endattach": "end_attachment",
    "endattachment": "end_attachment",
    "end attachment": "end_attachment",
    "filepath": "native_path",
    "file path": "native_path",
    "nativefile": "native_path",
    "native file": "native_path",
    "native path": "native_path",
    "textpath": "text_path",
    "text path": "text_path",
    "textprecedence": "text_path",
    "text precedence": "text_path",
}
