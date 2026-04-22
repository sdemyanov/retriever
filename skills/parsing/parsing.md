# Parsing Contract

## Supported MVP file types

- `pdf`: parse text and metadata with `pdfplumber`
- `docx`: parse paragraphs and core properties with `python-docx`
- `txt`, `md`, `csv`, `json`, `html`, `htm`, `ics`, and curated code/config text types such as `py`, `js`, `ts`, `tsx`, `java`, `go`, `rb`, `php`, `c`, `cpp`, `cs`, `rs`, `swift`, `kt`, `sh`, `ps1`, `yaml`, `yml`, `toml`, `ini`, `cfg`, `conf`, `properties`, `xml`, `sql`, `css`, `scss`, and `less`: decode bytes, normalize text, strip HTML tags when needed
- `eml`: parse headers and body with stdlib `email`, generate HTML preview, and extract one level of attachment payloads as child documents
- `msg`: parse with `extract-msg`, generate HTML preview, and extract one level of attachment payloads as child documents
- `pst`: regular `ingest` treats PST as a container source and emits one logical message document per PST message, with HTML preview and one level of attachment child documents
- `png`, `jpg`, `jpeg`, `gif`, `bmp`, `webp`, `tif`, `tiff`: keep as native preview-only documents with no OCR or text extraction
- `rtf`: extract text with `striprtf` and generate a simple HTML preview
- `xls`: read sheets with `xlrd`, generate one CSV preview per sheet
- `xlsx`: read sheets with `openpyxl`, generate one CSV preview per sheet
- `pptx`: extract deck-level slide text and speaker notes from OOXML, render embedded slide images when present, and generate one HTML preview for the whole deck
- archives such as `zip` remain unsupported; Retriever does not inspect archive contents
- processed productions are supported through `ingest-production`, and plain `ingest` without `--file-types` auto-routes detected production roots into that same production pipeline

## Encoding behavior

- Try the declared encoding first when one exists.
- Fall back to UTF-8.
- If UTF-8 fails, use `charset-normalizer`.
- Store normalized UTF-8 text in SQLite.
- Mark `text_status` as:
  - `ok` for clean decode
  - `partial` when replacement characters or uncertain recovery were needed
  - `empty` when no usable text was extracted
  - `failed` only when extraction itself fails and the file is recorded as a structured ingest failure

## Content type classification

- Populate built-in `documents.content_type` during ingest.
- Start from extension-to-type mapping.
- Allow content inspection to override the extension default when the content is more trustworthy.
- Current override rules include:
  - email-style header blocks near the top of a PDF, DOCX, HTML, or text document -> `Email`
  - calendar markers such as `BEGIN:VCALENDAR` or `BEGIN:VEVENT` -> `Calendar`
- Example:
  - `.pdf` defaults to `E-Doc`
  - a PDF with leading `From:`, `To:`, `Sent:`, `Subject:` headers should be classified as `Email`

## Participants extraction

- Populate built-in `documents.participants` during ingest.
- For email documents, collect the union of all senders and all recipients found across the email chain in the extracted text.
- For chat-like documents, collect the union of sender names that appear as message speakers in the transcript.
- Current sources include:
  - top-level EML and MSG headers
  - repeated `From:`, `To:`, `Cc:`, `Bcc:` header blocks inside email bodies or rendered email exports
  - chat-style speaker lines in TXT, HTML, PDF, and DOCX transcripts
- Existing workspaces should reindex after upgrading to populate `participants` on already ingested documents.

## Preview behavior

- Native preview types: `pdf`, `docx`, `txt`, `md`, `csv`, `json`, `html`, `htm`, `ics`, `png`, `jpg`, `jpeg`, `gif`, `bmp`, `webp`, `tif`, `tiff`, and the supported code/config text formats
- Generated preview types:
  - `eml` -> one `.html` preview preserving headers and body
  - `msg` -> one `.html` preview preserving headers and body
  - `pst` -> one `.html` preview per extracted PST message
  - `rtf` -> one `.html` preview from extracted text
  - `xls` -> one `.csv` preview per sheet
  - `xlsx` -> one `.csv` preview per sheet
  - `pptx` -> one `.html` preview containing all slides in deck order, with speaker notes labeled per slide and embedded slide images shown inline when present
  - production documents -> one `.html` preview combining Bates metadata, linked text, and page images when no preferred native preview exists
- Store preview files under `.retriever/previews/`
- Store preview record paths relative to `.retriever/`
- If an archive such as `.zip` is separately extracted into the workspace, the extracted files are ingested as normal top-level files; the archive itself still remains unsupported.
- For processed productions, prefer a real produced per-document PDF first, then a linked native file that Retriever can preview directly, and only then fall back to synthesized HTML.
- Production page images may be TIFF; when the preview pane needs browser-friendly images, Retriever converts them for the generated production HTML preview.

## Attachment family behavior

- EML and MSG parents may emit one level of child attachment documents.
- CID-backed images that are rendered inline inside the generated HTML preview are kept as preview assets and are not materialized as child attachment documents.
- Store extracted attachment payloads under `.retriever/previews/<parent>/attachments/`.
- Child attachment rows are derived documents linked back to the parent email; they are not top-level scanned workspace files.
- Re-ingesting a parent email must reconcile its child attachments inside the same parent transaction.
- When an attachment payload is unchanged, keep the existing child row and preserve its stable `control_number`, locked built-in values, and custom-field values.

## PST behavior

- PST support is first-class in the pinned runtime and requires the `pypff` backend to be importable at runtime.
- Use regular `ingest` for `.pst` sources; do not route PST through `ingest-production`.
- Retriever treats a `.pst` file as a container source rather than as one flat document.
- One logical parent document row is created per PST message.
- One level of PST attachment payloads may be materialized as child attachment documents, just like EML/MSG attachment families.
- `doctor` reports PST backend readiness under `pst_backend` and should fail until it is available.
- Unchanged PST sources skip reparsing and refresh seen timestamps instead.
- Changed PST sources update matching message rows in place and retire removed messages.

## Production behavior

- Processed productions are ingested through the production pipeline.
- Plain `ingest` without `--file-types` should detect likely production roots and auto-route them through production ingest.
- Plain `ingest` with `--file-types` should still detect likely production roots but skip them with a warning instead of triggering full production ingest.
- Phase 4 targets Concordance-style `DAT` + `OPT` with `TEXT/`, `IMAGES/`, and optional `NATIVES/`.
- Retriever creates one logical document row per load-file row, not one row per page image or text file.
- Produced `Begin Bates` becomes the document `control_number`.
- `Begin Attachment` / `End Attachment` spans may create parent/child family links through `parent_document_id`.
- Searchable text comes from linked production text when present.
- Phase 4 does not OCR production page images; image-only production documents are valid logical docs with empty text and page-image-driven preview.
- Linked `TEXT/`, `IMAGES/`, and `NATIVES/` files remain source parts, not top-level scanned workspace documents.

## PowerPoint behavior

- PPTX is indexed as one deck-level document row, not one child row per slide.
- Generated preview is a single HTML file for the whole deck.
- The preview renders one section per slide in deck order.
- Embedded slide images are shown inline in the generated HTML preview when they are stored directly in the PPTX package.
- Speaker notes are included under a labeled notes section for each slide when present.
- Slide-master/layout text, charts, SmartArt, animations, transitions, and non-image embedded media are ignored in MVP.
- Embedded PPTX media is not extracted as child attachment documents in MVP.

## Failure isolation

- Ingest runs in a per-file transaction.
- If one parser raises, roll back that file only.
- Record a structured failure entry with `rel_path` and the exception summary.
- Continue scanning the remaining files.

## Deferred from MVP

- OCR for scanned PDFs
- OCR / text extraction for supported image files
- embedded image extraction
- PowerPoint chart rendering or media extraction
- archive inspection for `.zip` and similar containers
- OCR for production page images
- MBOX support
- custom-field full-text indexing
