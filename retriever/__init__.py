"""Retriever package entrypoint support for Claude Code installs.

The authored backend still lives in ``skills/tool-template/tools.py`` for now.
This package provides a stable ``python -m retriever`` surface so Claude-facing
commands no longer call the generated bundle directly.
"""

from ._compat import REPO_ROOT, TOOL_PATH, invoke_tools_main, load_tools_module

__all__ = [
    "REPO_ROOT",
    "TOOL_PATH",
    "invoke_tools_main",
    "load_tools_module",
]
