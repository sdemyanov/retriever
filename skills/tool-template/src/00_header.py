#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import html
import io
import json
import mimetypes
import posixpath
import platform
import re
import sqlite3
import subprocess
import sys
import tempfile
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from pathlib import Path

try:
    import charset_normalizer
except Exception:  # pragma: no cover - dependency probe
    charset_normalizer = None

try:
    import extract_msg
except Exception:  # pragma: no cover - dependency probe
    extract_msg = None

try:
    import openpyxl
except Exception:  # pragma: no cover - dependency probe
    openpyxl = None

try:
    import xlrd
except Exception:  # pragma: no cover - dependency probe
    xlrd = None

try:
    import pdfplumber
except Exception:  # pragma: no cover - dependency probe
    pdfplumber = None

try:
    from docx import Document as DocxDocument
except Exception:  # pragma: no cover - dependency probe
    DocxDocument = None

try:
    from striprtf.striprtf import rtf_to_text
except Exception:  # pragma: no cover - dependency probe
    rtf_to_text = None

try:
    from PIL import Image as PilImage
except Exception:  # pragma: no cover - dependency probe
    PilImage = None

try:
    import pypff
except Exception:  # pragma: no cover - required PST backend probe
    pypff = None


TOOL_VERSION = "0.9.4"
SCHEMA_VERSION = 12
REQUIREMENTS_VERSION = "2026-04-16-phase4-pst"
TEMPLATE_SOURCE = "skills/tool-template/retriever_tools.py"
MANUAL_FIELD_LOCKS_COLUMN = "manual_field_locks_json"
LEGACY_METADATA_LOCKS_COLUMN = "locked_metadata_fields_json"
CHUNK_TARGET_CHARS = 3200
CHUNK_OVERLAP_CHARS = 250
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100
CONTROL_NUMBER_PREFIX = "DOC"
CONTROL_NUMBER_BATCH_WIDTH = 3
CONTROL_NUMBER_FAMILY_WIDTH = 8
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
    "custodian",
    "date_created",
    "date_modified",
    "page_count",
    "participants",
    "recipients",
    "subject",
    "title",
}
SYSTEM_MANAGED_FIELDS = {
    "content_hash",
    "control_number_attachment_sequence",
    "control_number_batch",
    "control_number_family_sequence",
    "control_number",
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
    "begin_attachment",
    "begin_bates",
    "end_attachment",
    "end_bates",
    "parent_document_id",
    "production_id",
    "rel_path",
    "source_folder_path",
    "source_item_id",
    "source_rel_path",
    "source_kind",
    "text_status",
    "updated_at",
}
BUILTIN_FIELD_TYPES = {
    "id": "integer",
    "control_number": "text",
    "dataset_id": "integer",
    "parent_document_id": "integer",
    "source_kind": "text",
    "source_rel_path": "text",
    "source_item_id": "text",
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
    "custodian": "text",
    "date_created": "date",
    "date_modified": "date",
    "title": "text",
    "subject": "text",
    "participants": "text",
    "recipients": "text",
    MANUAL_FIELD_LOCKS_COLUMN: "text",
    "file_hash": "text",
    "content_hash": "text",
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
REGISTRY_FIELD_TYPES = {
    "boolean": "INTEGER",
    "integer": "INTEGER",
    "real": "REAL",
    "text": "TEXT",
}
VIRTUAL_FILTER_FIELD_TYPES = {
    "dataset_name": "text",
    "is_attachment": "boolean",
    "has_attachments": "boolean",
    "production_name": "text",
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
FILESYSTEM_SOURCE_KIND = "filesystem"
PST_SOURCE_KIND = "pst"
MANUAL_DATASET_SOURCE_KIND = "manual"
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
