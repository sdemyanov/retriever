# Spreadsheet QA Eval

This directory holds a small manual/semi-automated eval set for spreadsheet
question answering.

The goal is to test the full workflow:

1. find the right spreadsheet via Retriever
2. inspect the spreadsheet data
3. answer a specific question grounded in the sheet contents

This is intentionally separate from unit tests in `tests/test_retriever_tools.py`.
Those tests verify parsing and search behavior. The cases here are meant for
agent-level QA over real spreadsheets.

## Dataset

The cases were derived from the spreadsheets under:

`~/Projects/retriever-plugin/data/raw/dataset`

The manifest stores the expected workbook basename and, when applicable, the
expected sheet label.

## Files

- `spreadsheet_qa_eval_cases.json`
  - query set plus expected retrieval target and answer checks
- `evaluate_spreadsheet_qa.py`
  - lightweight scorer for saved responses

## Suggested Workflow

1. Ingest the dataset into a scratch workspace.
2. Ask the agent each query from `spreadsheet_qa_eval_cases.json`.
3. Save responses in a JSON file using this shape:

```json
{
  "contamination_total_impact": {
    "answer": "The contamination workbook shows a total financial impact of $3,075,000.",
    "selected_document": "task_0006_Contamination_Impact_Assessment_and_Remediation_Timeline_2023-03-05.xlsx",
    "selected_sheet": "Batch Impact Analysis"
  }
}
```

`selected_document` and `selected_sheet` are optional but recommended. If they
are missing, the scorer evaluates only the answer text.

4. Run the scorer:

```bash
python3 tests/bench/evaluate_spreadsheet_qa.py \
  --responses /path/to/spreadsheet_qa_responses.json
```

## Notes

- The scorer uses case-insensitive substring matching with multiple acceptable
  variants for dates, currency, and labels.
- This is a pragmatic regression tool, not a semantic grader. Near-miss wording
  may still need manual review.
- The cases deliberately favor questions that require actual spreadsheet data,
  not just structural labels.
