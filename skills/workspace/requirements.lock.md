# Requirements Lock

## Lock metadata

- lock version: `2026-04-16-phase4-pst`
- Python target: `>=3.10`
- scope: direct runtime dependencies for the current Phase 4 runtime

## Install command

Use this exact command when bootstrapping or verifying the pinned dependency set:

```bash
python3 -m pip install \
  pdfplumber==0.11.9 \
  python-docx==1.2.0 \
  openpyxl==3.1.5 \
  xlrd==2.0.1 \
  extract-msg==0.55.0 \
  libpff-python==20231205 \
  striprtf==0.0.26 \
  Pillow==10.3.0 \
  charset-normalizer==3.4.7
```

## Pinned direct dependencies

| Package | Version | Purpose |
|---|---|---|
| `pdfplumber` | `0.11.9` | PDF text extraction baseline |
| `python-docx` | `1.2.0` | DOCX parsing |
| `openpyxl` | `3.1.5` | XLSX parsing |
| `xlrd` | `2.0.1` | legacy XLS parsing |
| `extract-msg` | `0.55.0` | Outlook `.msg` parsing |
| `libpff-python` | `20231205` | PST ingest backend (imports as `pypff`) |
| `striprtf` | `0.0.26` | RTF text extraction |
| `Pillow` | `10.3.0` | browser-friendly image conversion for TIFF-backed previews |
| `charset-normalizer` | `3.4.7` | fallback text decoding and normalization |

## Verification

After installation, confirm the environment can import the direct packages:

```bash
python3 -c "import pdfplumber, docx, openpyxl, xlrd, extract_msg, charset_normalizer, pypff; from PIL import Image; from striprtf.striprtf import rtf_to_text; print('imports ok')"
```

## Notes

- This lock file intentionally pins direct dependencies only.
- Exact transitive dependency hashes are deferred until packaging needs become stricter.
- If pip is present but install is blocked by network or policy, stop and report that clearly.
- PST ingest is part of the pinned runtime through `libpff-python` (import name `pypff`).
- If the PST backend cannot be installed or imported on the target platform, report that clearly when PST ingest is attempted.
- OCR and semantic-search dependencies are not part of the current pinned runtime.
