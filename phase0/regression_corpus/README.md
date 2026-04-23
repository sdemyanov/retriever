# Regression Corpus

This directory contains Phase 0 sample files used by [runtime_probe.py](../runtime_probe.py).

## Files

- `sample.pdf`: minimal valid PDF for text extraction and preview checks
- `corrupt.pdf`: intentionally broken PDF for ingest failure-path testing
- `sample.docx`: valid DOCX for parsing and preview checks
- `sample.xlsx`: valid XLSX with multiple sheets
- `sample.html`: preview-target HTML fixture
- `sample_utf8.txt`: UTF-8 plain text sample
- `sample_latin1.txt`: non-UTF-8 text sample for fallback decoding
- `sample_utf8.csv`: UTF-8 CSV sample
- `sample_latin1.csv`: non-UTF-8 CSV sample for fallback decoding
- `sample_utf8.eml`: UTF-8 email sample
- `sample_latin1.eml`: ISO-8859-1 email sample
- `sample_utf8.mbox`: UTF-8 MBOX sample with two messages
- `sample_invalid.msg`: intentionally invalid Outlook MSG fixture for failure-path testing

## Conversation Coverage

Not every multi-document conversation case is stored as a static fixture in this directory.

The current conversation and preview regressions generate their own tiny synthetic inputs inside
`tests/test_retriever_tools.py`, including:

- loose email chains with `Message-ID` / `In-Reply-To` linkage
- extracted Slack export roots with `users.json`, `channels.json`, per-day channel files, and cross-day reply threads
- PST email and PST chat-like items
- `export-previews` outputs under `.retriever/exports/`

That keeps the repo corpus small while still exercising cross-document grouping and preview behavior.

## Current Gap

This corpus does not yet include a valid positive-path `.msg` sample.

That means:

- negative-path behavior is covered for `.msg`
- successful `.msg` extraction is still unverified

Add a real `.msg` sample before calling MSG support fully validated.

Additional conversation-fixture gaps still open:

- one malformed or incomplete Slack export fixture stored on disk
- one ZIP-wrapped Slack export fixture stored on disk

## Corpus Diff

Use [ingest_corpus_diff.py](../ingest_corpus_diff.py) to run two `tools.py` builds against the same fixed corpus and compare normalized ingest state.

Example:

```bash
python3 phase0/ingest_corpus_diff.py \
  --baseline-tool /path/to/baseline/skills/tool-template/tools.py \
  --candidate-tool /path/to/candidate/skills/tool-template/tools.py
```

By default the script:

- copies this corpus into two fresh `workspace/` roots
- adds tiny synthetic Slack export and processed-production fixtures
- runs `bootstrap` and recursive `ingest` on both tools
- writes normalized snapshots, a unified diff, and a summary under a temporary artifacts directory
- exits `1` if a semantic diff is found

Pass `--output-dir <path>` to keep artifacts in a stable location, `--no-synthetic-extras` to diff only the static corpus, or `--allow-diff` to exit `0` even when semantic differences are found.
