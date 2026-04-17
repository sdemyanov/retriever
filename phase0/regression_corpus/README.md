# Regression Corpus

This directory contains Phase 0 sample files used by [runtime_probe.py](/Users/sergey/Projects/retriever-plugin/phase0/runtime_probe.py).

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

## Current Gap

This corpus does not yet include a valid positive-path `.msg` sample.

That means:

- negative-path behavior is covered for `.msg`
- successful `.msg` extraction is still unverified

Add a real `.msg` sample before calling MSG support fully validated.
