# Phase 0 Support Matrix

| Capability | Status | Evidence | Notes |
|---|---|---|---|
| Python runtime available | Verified in Cowork | Python 3.10.12 in Cowork VM | Also verified locally on macOS |
| SQLite available | Verified in Cowork | SQLite 3.37.2 in Cowork VM | Also verified locally on macOS |
| FTS5 support | Verified in Cowork | `doctor` skill reported FTS5 works | Required for search plan |
| Pinned dependency install flow | Partial | pip 25.3 is available in Cowork; local virtualenv install succeeded | Still need an actual pinned install test in Cowork |
| PDF parsing | Verified locally | Valid PDF parsed, corrupt PDF failed cleanly | Good signal for per-file ingest isolation |
| DOCX parsing | Verified locally | Sample DOCX parsed with `python-docx` | Good |
| XLSX parsing | Verified locally | Multi-sheet workbook parsed with `openpyxl` | Good |
| EML parsing | Verified locally | UTF-8 and ISO-8859-1 samples parsed correctly | Prefer declared MIME charset over heuristics |
| MSG parsing | Partial | `extract-msg` installs and invalid fixture fails cleanly | Positive-path parse of a real `.msg` still missing |
| TXT encoding recovery | Partial | `charset-normalizer` recovered readable text | Detector label may not match original encoding family |
| CSV encoding recovery | Partial | `charset-normalizer` recovered readable text | Same caveat as TXT |
| HTML preview fixture | Fixture ready | Sample HTML generated | Cowork preview pane not validated here |
| PDF preview fixture | Fixture ready | Sample PDF generated | Cowork preview pane not validated here |
| DOCX preview fixture | Fixture ready | Sample DOCX generated | Cowork preview pane not validated here |
| CSV preview fixture | Fixture ready | Sample CSV generated | Cowork preview pane not validated here |
| Workspace persistence across processes | Partial | Marker file written and read back | Cowork reopen persistence still unverified |
| Workspace persistence across Cowork sessions | Unverified | Not yet tested in Cowork after reopen | Must be tested in product |

## Current Read

Phase 1 is ready to start.

The remaining unknowns are product-environment questions, not fundamental architecture blockers:

- actual Cowork preview rendering
- valid `.msg` coverage
- reopened-session persistence
- full pinned dependency install inside Cowork
