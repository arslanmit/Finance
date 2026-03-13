"""CLI entrypoint facade for the Finance CLI."""

from __future__ import annotations

import sys
from typing import Sequence

from . import cli_handlers as _cli_handlers
from . import cli_parser as _cli_parser
from .errors import FinanceCliError
from . import matrix as _matrix
from . import run_workflow as _run_workflow
from . import wizard as _wizard

build_matrix_jobs = _matrix.build_matrix_jobs
build_matrix_output_path = _matrix.build_matrix_output_path
build_parser = _cli_parser.build_parser
build_wizard_menu_items = _wizard.build_wizard_menu_items
dispatch_command = _cli_handlers.dispatch_command
execute_analysis = _run_workflow.execute_analysis
handle_matrix_command = _cli_handlers.handle_matrix_command
handle_run_command = _cli_handlers.handle_run_command
run_wizard = _wizard.run_wizard
slugify_rule = _matrix.slugify_rule


def main(argv: Sequence[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()

    try:
        if not args_list:
            run_wizard()
            return 0

        try:
            args = parser.parse_args(args_list)
        except SystemExit as exc:
            return exc.code if isinstance(exc.code, int) else 1
        return dispatch_command(args)
    except FinanceCliError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


__all__ = [
    "build_matrix_jobs",
    "build_matrix_output_path",
    "build_parser",
    "build_wizard_menu_items",
    "dispatch_command",
    "execute_analysis",
    "handle_matrix_command",
    "handle_run_command",
    "main",
    "run_wizard",
    "slugify_rule",
]
