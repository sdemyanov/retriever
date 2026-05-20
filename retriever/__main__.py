from __future__ import annotations

from ._compat import invoke_tools_main, load_tools_module
from .native import handles_native_command, main as native_main, render_native_help_section


if __name__ == "__main__":
    import sys

    argv = sys.argv[1:]
    if not argv or argv == ["--help"] or argv == ["-h"]:
        tools = load_tools_module()
        help_text = tools.build_parser().format_help().replace("__main__.py", "python -m retriever")
        sys.stdout.write(help_text)
        sys.stdout.write(render_native_help_section())
        sys.stdout.flush()
        raise SystemExit(0)
    if handles_native_command(argv):
        raise SystemExit(native_main(argv))
    raise SystemExit(invoke_tools_main(argv))
