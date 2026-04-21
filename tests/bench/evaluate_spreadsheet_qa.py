#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


DEFAULT_CASES_PATH = Path(__file__).with_name("spreadsheet_qa_eval_cases.json")


def normalize(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip().casefold()


def load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def evaluate_case(case: dict[str, object], response_entry: dict[str, object] | None) -> dict[str, object]:
    result: dict[str, object] = {
        "id": case["id"],
        "query": case["query"],
        "status": "pass",
        "issues": [],
    }
    if response_entry is None:
        result["status"] = "missing"
        result["issues"] = ["missing response"]
        return result

    answer_text = normalize(response_entry.get("answer"))
    selected_document = response_entry.get("selected_document")
    selected_sheet = response_entry.get("selected_sheet")

    issues: list[str] = []

    expected_document = case.get("expected_document")
    if selected_document is not None and expected_document is not None:
        if normalize(selected_document) != normalize(expected_document):
            issues.append(
                f"selected_document={selected_document!r} did not match expected_document={expected_document!r}"
            )

    expected_sheet = case.get("expected_sheet")
    if selected_sheet is not None and expected_sheet not in (None, ""):
        if normalize(selected_sheet) != normalize(expected_sheet):
            issues.append(f"selected_sheet={selected_sheet!r} did not match expected_sheet={expected_sheet!r}")

    for index, options in enumerate(case.get("answer_must_include_any", []), start=1):
        if not isinstance(options, list):
            continue
        if not any(normalize(option) in answer_text for option in options):
            issues.append(f"answer missing required fact group #{index}: {options}")

    for forbidden in case.get("answer_must_not_include", []):
        if normalize(forbidden) in answer_text:
            issues.append(f"answer contained forbidden text: {forbidden!r}")

    if issues:
        result["status"] = "fail"
        result["issues"] = issues
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate saved responses against spreadsheet QA cases.")
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES_PATH, help="Path to the eval case manifest.")
    parser.add_argument("--responses", type=Path, required=True, help="Path to the saved responses JSON.")
    parser.add_argument("--case-id", dest="case_ids", action="append", default=[], help="Evaluate only the given case id.")
    args = parser.parse_args()

    case_rows = load_json(args.cases)
    if not isinstance(case_rows, list):
        raise SystemExit(f"Expected a JSON array in {args.cases}")

    responses = load_json(args.responses)
    if not isinstance(responses, dict):
        raise SystemExit(f"Expected a JSON object in {args.responses}")

    requested_ids = {normalize(case_id) for case_id in args.case_ids}
    filtered_cases = [
        case
        for case in case_rows
        if isinstance(case, dict) and (not requested_ids or normalize(case.get("id")) in requested_ids)
    ]

    results = [evaluate_case(case, responses.get(str(case["id"]))) for case in filtered_cases]

    failed = 0
    missing = 0
    passed = 0
    for result in results:
        status = str(result["status"])
        if status == "pass":
            passed += 1
            print(f"PASS  {result['id']}")
            continue
        if status == "missing":
            missing += 1
            print(f"MISS  {result['id']}: missing response")
            continue
        failed += 1
        print(f"FAIL  {result['id']}")
        for issue in result["issues"]:
            print(f"  - {issue}")

    print()
    print(f"Summary: {passed} passed, {failed} failed, {missing} missing, {len(results)} total")
    return 0 if failed == 0 and missing == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
