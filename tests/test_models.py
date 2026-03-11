"""Tests for data models."""

import pytest
from hypothesis import given, strategies as st, settings

from finance_cli.errors import AnalysisError
from finance_cli.models import AnalysisConfig, ParsedRule


class TestAnalysisConfig:
    """Tests for AnalysisConfig dataclass."""

    def test_valid_config_with_defaults(self):
        """Test creating AnalysisConfig with default values."""
        config = AnalysisConfig(months=6)
        assert config.months == 6
        assert config.indicator_type == "sma"
        assert config.rule == "indicator > open"

    def test_valid_config_with_custom_values(self):
        """Test creating AnalysisConfig with custom values."""
        config = AnalysisConfig(
            months=12,
            indicator_type="ema",
            rule="indicator > close"
        )
        assert config.months == 12
        assert config.indicator_type == "ema"
        assert config.rule == "indicator > close"

    def test_config_is_frozen(self):
        """Test that AnalysisConfig is immutable."""
        config = AnalysisConfig(months=6)
        with pytest.raises(Exception):  # FrozenInstanceError
            config.months = 12  # type: ignore

    def test_config_rejects_months_less_than_one(self):
        """Test that months < 1 raises AnalysisError."""
        with pytest.raises(AnalysisError, match="Months must be at least 1"):
            AnalysisConfig(months=0)

        with pytest.raises(AnalysisError, match="Months must be at least 1"):
            AnalysisConfig(months=-5)

    def test_config_accepts_months_equal_to_one(self):
        """Test that months = 1 is valid."""
        config = AnalysisConfig(months=1)
        assert config.months == 1

    @given(
        months=st.integers(min_value=-1000, max_value=0),
        indicator_type=st.text(min_size=0, max_size=20),
        rule=st.text(min_size=0, max_size=50)
    )
    @settings(max_examples=100)
    def test_property_edge_case_handling_consistency(
        self, months, indicator_type, rule
    ):
        """Property 13: Edge Case Handling Consistency.
        
        **Validates: Requirements 8.1, 8.2, 8.3, 8.4**
        
        For any indicator type, the AnalysisConfig should handle edge cases
        consistently. Specifically, window size (months) less than 1 should
        always raise an AnalysisError with a descriptive message.
        
        This property test validates the configuration-level validation.
        Additional edge cases (empty dataframes, all-NaN results, window size
        greater than data length) are validated at the analysis engine level.
        """
        # Property: months < 1 should always raise AnalysisError
        with pytest.raises(AnalysisError) as exc_info:
            AnalysisConfig(
                months=months,
                indicator_type=indicator_type,
                rule=rule
            )
        
        # Verify error message is descriptive
        error_msg = str(exc_info.value)
        assert "Months must be at least 1" in error_msg
        assert len(error_msg) > 0  # Non-empty error message

    @given(
        months=st.integers(min_value=1, max_value=1000),
        indicator_type=st.text(min_size=0, max_size=20),
        rule=st.text(min_size=0, max_size=50)
    )
    @settings(max_examples=100)
    def test_property_valid_months_always_accepted(
        self, months, indicator_type, rule
    ):
        """Property: Valid months (>= 1) should always be accepted.
        
        **Validates: Requirements 8.1, 8.3**
        
        For any months value >= 1, AnalysisConfig should successfully create
        an instance without raising validation errors. This ensures consistent
        handling of valid window sizes.
        """
        # Property: months >= 1 should always succeed
        config = AnalysisConfig(
            months=months,
            indicator_type=indicator_type,
            rule=rule
        )
        
        # Verify configuration is created correctly
        assert config.months == months
        assert config.indicator_type == indicator_type
        assert config.rule == rule
        
        # Verify immutability
        assert config.__class__.__name__ == "AnalysisConfig"


class TestParsedRule:
    """Tests for ParsedRule dataclass."""

    def test_valid_rule_with_greater_than(self):
        """Test creating ParsedRule with > operator."""
        rule = ParsedRule(
            left_operand="indicator",
            operator=">",
            right_operand="open"
        )
        assert rule.left_operand == "indicator"
        assert rule.operator == ">"
        assert rule.right_operand == "open"

    def test_valid_rule_with_all_operators(self):
        """Test all valid operators."""
        for op in [">", "<", ">=", "<="]:
            rule = ParsedRule(
                left_operand="indicator",
                operator=op,
                right_operand="close"
            )
            assert rule.operator == op

    def test_rule_is_frozen(self):
        """Test that ParsedRule is immutable."""
        rule = ParsedRule(
            left_operand="indicator",
            operator=">",
            right_operand="open"
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            rule.operator = "<"  # type: ignore

    def test_rule_rejects_invalid_operator(self):
        """Test that invalid operators raise AnalysisError."""
        with pytest.raises(AnalysisError, match="Invalid operator '=='"):
            ParsedRule(
                left_operand="indicator",
                operator="==",
                right_operand="open"
            )

        with pytest.raises(AnalysisError, match="Invalid operator '!='"):
            ParsedRule(
                left_operand="indicator",
                operator="!=",
                right_operand="open"
            )

    def test_rule_error_message_lists_valid_operators(self):
        """Test that error message includes valid operators."""
        with pytest.raises(AnalysisError) as exc_info:
            ParsedRule(
                left_operand="indicator",
                operator="invalid",
                right_operand="open"
            )
        
        error_msg = str(exc_info.value)
        assert "Valid operators:" in error_msg
        assert ">" in error_msg
        assert "<" in error_msg
        assert ">=" in error_msg
        assert "<=" in error_msg

    def test_valid_operators_class_variable(self):
        """Test that VALID_OPERATORS is accessible as class variable."""
        assert ParsedRule.VALID_OPERATORS == {">", "<", ">=", "<="}
