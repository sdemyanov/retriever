# Phase 0 Green-Light Memo

## Scope

Phase 0 now includes two layers of validation:

1. local development proof on macOS
2. in-product runtime check inside the actual Cowork VM

### Local development proof

- platform: macOS 15.6.1 on arm64
- Python: 3.11.5
- SQLite: 3.42.0 with FTS5 enabled

### Cowork VM runtime check

- platform: Ubuntu 22.04.5 LTS (Jammy), aarch64, kernel 6.8.0-106
- Python: 3.10.12
- pip: 25.3
- SQLite: 3.37.2 with FTS5 enabled

Important limitation:

- the Cowork runtime gate is now confirmed
- preview behavior, reopened-session persistence, and positive-path MSG coverage still need separate validation

## Recommendation

Proceed to Phase 1.

Why:

- the core local runtime assumptions look viable
- the actual Cowork VM matches the expected Ubuntu runtime closely enough for the planned architecture
- Python, pip, SQLite, and FTS5 are all available in Cowork
- the parser stack installs and imports successfully
- PDF, DOCX, XLSX, EML, TXT, and CSV probes behaved well enough to justify moving forward
- corrupt-input behavior looks manageable if ingest is implemented with per-file transactions

What still must be validated in-product before public release:

- actual Cowork preview rendering for PDF, DOCX, HTML, and CSV
- reopened-project persistence across Cowork sessions
- positive-path parsing of a real `.msg` sample

## What Was Verified

### Runtime baseline

- Python 3.11.5 is available
- SQLite 3.42.0 is available
- FTS5 virtual tables can be created successfully

### Cowork VM runtime baseline

- Python 3.10.12 is available in Cowork
- pip 25.3 is available in Cowork
- SQLite 3.37.2 is available in Cowork
- FTS5 works in Cowork
- platform reported by Cowork: Ubuntu 22.04.5 LTS (Jammy), aarch64, kernel 6.8.0-106

### Dependency install flow

- a clean virtualenv was created at `.phase0-venv`
- the following packages installed successfully after allowing network access:
  - `pdfplumber==0.11.9`
  - `python-docx==1.2.0`
  - `openpyxl==3.1.5`
  - `extract-msg==0.55.0`
  - `charset-normalizer==3.4.7`

### Parser probes

- valid PDF created and parsed successfully with `pdfplumber`
- intentionally corrupt PDF raised a clean parser exception
- DOCX created and parsed successfully with `python-docx`
- XLSX with multiple sheets created and parsed successfully with `openpyxl`
- UTF-8 and ISO-8859-1 EML samples parsed successfully with the stdlib email parser
- invalid `.msg` fixture failed cleanly through `extract-msg`

### Encoding probes

- non-UTF-8 TXT and CSV samples were recovered to correct readable text through `charset-normalizer`
- declared charsets in EML behaved better than blind detection, which reinforces the plan to prefer MIME metadata first

### Persistence probe

- local filesystem writes persisted across separate processes
- true Cowork cross-session persistence remains unverified from this environment

### Cowork runtime outcome

The in-product `doctor` skill check returned:

- Overall: pass
- Python: 3.10.12, usable
- pip: 25.3, available
- SQLite/FTS5: 3.37.2, FTS5 works
- Platform: Ubuntu 22.04.5 LTS (Jammy), aarch64, kernel 6.8.0-106

This clears the core runtime gate for Phase 1.

## Notable Findings

### Encoding labels are fuzzy

`charset-normalizer` recovered the text content correctly, but labeled simple Latin-1 style samples as `cp1250`.

Implication:

- we should rely on the detector primarily for text recovery and normalization, not as a source of truth for the original encoding label
- when an explicit charset is available from MIME headers or metadata, prefer that over heuristics

### MSG support is only partially validated

We verified:

- the `extract-msg` package installs
- invalid `.msg` input fails cleanly

We did not yet verify:

- successful parsing of a real Outlook `.msg` file

This should stay marked as partial until a valid fixture is added to the regression corpus and tested in Cowork.

### Preview support is still a manual gate

We generated previewable sample files for:

- PDF
- DOCX
- HTML
- CSV

But we could not validate the Cowork preview pane itself from this environment. That remains a manual test gate.

## Artifacts Created

- runtime probe script: [runtime_probe.py](/Users/sergey/Projects/retriever-plugin/phase0/runtime_probe.py)
- runtime proof output: [runtime_proof.json](/Users/sergey/Projects/retriever-plugin/phase0/runtime_proof.json)
- support matrix: [SUPPORTED_MATRIX.md](/Users/sergey/Projects/retriever-plugin/phase0/SUPPORTED_MATRIX.md)
- regression corpus: [regression_corpus](/Users/sergey/Projects/retriever-plugin/phase0/regression_corpus)

## Required Follow-Up Before Release

1. Manually open the generated PDF, DOCX, HTML, and CSV fixtures in Cowork preview.
2. Add one valid `.msg` sample and verify positive-path parsing.
3. Reopen a Cowork project and confirm `.retriever/` state persists across sessions.
4. Verify dependency installation in Cowork with the pinned package set, not just pip availability.
