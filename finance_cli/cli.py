"""CLI entrypoint facade for the Finance CLI."""

from __future__ import annotations

import sys
from typing import Sequence

from .cli_handlers import dispatch_command, handle_matrix_command, handle_run_command
from .cli_parser import build_parser
from .errors import FinanceCliError
from .matrix import build_matrix_jobs, build_matrix_output_path, slugify_rule
from .run_workflow import execute_analysis
from .wizard import build_wizard_menu_items, run_wizard


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
