from __future__ import annotations

import importlib.util
import os
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parent.parent
TOOL_PATH = REPO_ROOT / "skills" / "tool-template" / "tools.py"
MODULE_NAME = "retriever_tools_compat"


def configure_environment() -> None:
    os.environ.setdefault("RETRIEVER_PLUGIN_ROOT", str(REPO_ROOT))
    os.environ.setdefault("RETRIEVER_CANONICAL_TOOL_PATH", str(TOOL_PATH))


@lru_cache(maxsize=1)
def load_tools_module() -> ModuleType:
    configure_environment()
    if not TOOL_PATH.exists():
        raise RuntimeError(f"Retriever compatibility bundle not found at {TOOL_PATH}")
    spec = importlib.util.spec_from_file_location(MODULE_NAME, TOOL_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load Retriever compatibility bundle from {TOOL_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


def invoke_tools_main(argv: Sequence[str] | None = None) -> int:
    tools = load_tools_module()
    previous_argv = sys.argv[:]
    forwarded_argv = list(argv) if argv is not None else sys.argv[1:]
    if (
        "--human" not in forwarded_argv
        and "--output" not in forwarded_argv
        and not any(argument.startswith("--output=") for argument in forwarded_argv)
    ):
        forwarded_argv = ["--human", *forwarded_argv]
    sys.argv = ["retriever", *forwarded_argv]
    try:
        return int(tools.main())
    finally:
        sys.argv = previous_argv
