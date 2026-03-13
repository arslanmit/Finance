import finance_cli.analysis as analysis
import finance_cli.cli as cli


def test_cli_facade_declares_explicit_exports() -> None:
    expected = {
        "build_matrix_jobs",
        "build_matrix_output_path",
        "build_parser",
        "build_wizard_menu_items",
        "dispatch_command",
        "handle_matrix_command",
        "handle_run_command",
        "main",
        "run_wizard",
        "slugify_rule",
    }

    assert expected.issubset(set(cli.__all__))


def test_analysis_facade_declares_explicit_exports() -> None:
    expected = {
        "AnalysisConfig",
        "IndicatorCalculator",
        "IndicatorRegistry",
        "PRIMARY_GAP_COLUMN",
        "SCREENING_RULE_COLUMN",
        "SECONDARY_GAP_COLUMN",
        "analyze_dataframe",
        "analyze_dataframe_with_config",
        "build_default_output_path",
        "calculate_ema",
        "calculate_sma",
        "calculate_wma",
        "evaluate_rule",
        "format_indicator_column_name",
        "format_rule",
        "get_indicator_registry",
        "get_trailing_derived_columns",
        "ordered_output_columns",
        "parse_rule",
        "prepare_dataframe",
        "render_filtered_rows",
        "save_dataframe",
        "validate_indicator_result",
    }

    assert expected.issubset(set(analysis.__all__))
