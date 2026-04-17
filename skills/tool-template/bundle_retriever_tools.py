#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


FRAGMENT_FILENAMES = [
    "00_header.py",
    "10_core.py",
    "20_extractors.py",
    "30_productions.py",
    "40_schema_runtime.py",
    "45_processing_core.py",
    "46_processing_store.py",
    "50_ingest_mutation.py",
    "60_search_cli.py",
]


def bundle_source(fragment_dir: Path) -> str:
    chunks: list[str] = []
    for name in FRAGMENT_FILENAMES:
        path = fragment_dir / name
        if not path.exists():
            raise SystemExit(f"Missing Retriever source fragment: {path}")
        chunks.append(path.read_text(encoding="utf-8"))
    return "".join(chunks)


def main() -> int:
    tool_dir = Path(__file__).parent
    fragment_dir = tool_dir / "src"
    target_path = tool_dir / "retriever_tools.py"
    bundled = bundle_source(fragment_dir)
    current = target_path.read_text(encoding="utf-8") if target_path.exists() else None
    if bundled != current:
        target_path.write_text(bundled, encoding="utf-8")
        print(f"Wrote {target_path}")
    else:
        print(f"{target_path} already up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
