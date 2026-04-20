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
| Slack export root ingest and dataset routing | Verified locally | Synthetic Slack export regressions in `tests/test_retriever_tools.py` | One dedicated `slack_export` dataset per export root; `users.json` / `channels.json` / `canvases.json` stay adapter-owned |
| Conversation grouping for email / Slack / PST chat | Verified locally | Synthetic conversation regressions in `tests/test_retriever_tools.py` | Email uses conversation chains, Slack uses shared conversation plus `reply_thread` children, PST chat uses `source_folder_path` as the v1 grouping key |
| Shared conversation preview browsing | Verified locally | Email, Slack, and PST preview-routing regressions in `tests/test_retriever_tools.py` | Ordinary browsing resolves to shared TOC + segment HTML with stable `#doc-<id>` anchors |
| Export preview materialization | Verified locally | `export-previews` regressions in `tests/test_retriever_tools.py` | Email exports expand to the full chain; chat exports merge contiguous selected docs into one HTML unit |
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
- manual scorecard generation against adjudicated conversation truth
