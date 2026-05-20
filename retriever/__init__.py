"""Retriever package entrypoint support for Claude Code installs.

The authored backend still lives in ``skills/tool-template/src`` and its
generated compatibility bundle, but the package now owns first-class native
command orchestration for Claude-facing processing flows.
"""

from ._compat import REPO_ROOT, TOOL_PATH, invoke_tools_main, load_tools_module
from .native import NATIVE_COMMAND_NAMES, handles_native_command

__all__ = [
    "NATIVE_COMMAND_NAMES",
    "REPO_ROOT",
    "TOOL_PATH",
    "handles_native_command",
    "invoke_tools_main",
    "load_tools_module",
]
