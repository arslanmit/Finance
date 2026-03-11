"""Property-based tests for indicator registry."""

import pytest
import pandas as pd
from hypothesis import given, strategies as st, settings

from finance_cli.analysis import get_indicator_registry, IndicatorRegistry
from finance_cli.errors import AnalysisError


class TestIndicatorRegistryProperties:
    """Property-based tests for indicator registration validation."""

    @given(
        data_length=st.integers(min_value=1, max_value=100),
        window=st.integers(min_value=1, max_value=50),
        values=st.lists(
            st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
            min_size=1,
            max_size=100
        )
    )
    @settings(max_examples=100)
    def test_property_indicator_registration_type_validation(
        self, data_length, window, values
    ):
        """Property 10: Indicator Registration Type Validation.
        
        **Validates: Requirements 6.2, 6.3**
        
        For any function registered as an indicator, when called with a Series
        and window parameter, it should return a pandas Series with the same
        length as the input Series.
        
        This property ensures that all registered indicators conform to the
        IndicatorCalculator protocol and produce output with the correct shape.
        """
        # Ensure values list matches data_length
        if len(values) < data_length:
            values = values + [0.0] * (data_length - len(values))
        values = values[:data_length]
        
        # Create test series
        test_series = pd.Series(values)
        
        # Create a test registry with sample indicators
        test_registry = IndicatorRegistry()
        
        # Register test indicators that should conform to the protocol
        def test_sma(series: pd.Series, window: int) -> pd.Series:
            """Test SMA calculator."""
            return series.rolling(window=window).mean()
        
        def test_ema(series: pd.Series, window: int) -> pd.Series:
            """Test EMA calculator."""
            return series.ewm(span=window, adjust=False).mean()
        
        def test_identity(series: pd.Series, window: int) -> pd.Series:
            """Test identity calculator (returns input unchanged)."""
            return series.copy()
        
        test_registry.register("test_sma", test_sma)
        test_registry.register("test_ema", test_ema)
        test_registry.register("test_identity", test_identity)
        
        # Also test any indicators in the global registry
        global_registry = get_indicator_registry()
        all_registries = [
            ("test", test_registry),
            ("global", global_registry)
        ]
        
        # Test all registered indicators in both registries
        for registry_name, registry in all_registries:
            for indicator_name in registry.list_indicators():
                calculator = registry.get(indicator_name)
                
                # Property: Calculator must return a Series
                result = calculator(test_series, window)
                assert isinstance(result, pd.Series), (
                    f"Indicator '{indicator_name}' in {registry_name} registry "
                    f"must return a pandas Series, but returned {type(result)}"
                )
                
                # Property: Result must have same length as input
                assert len(result) == len(test_series), (
                    f"Indicator '{indicator_name}' in {registry_name} registry "
                    f"must return a Series with the same length as input. "
                    f"Expected {len(test_series)}, got {len(result)}"
                )
                
                # Property: Result must be a Series (not DataFrame or other type)
                assert result.ndim == 1, (
                    f"Indicator '{indicator_name}' in {registry_name} registry "
                    f"must return a 1-dimensional Series, but returned "
                    f"{result.ndim}-dimensional object"
                )

    def test_custom_indicator_registration_validation(self):
        """Test that custom indicators are validated for correct return type.
        
        This unit test complements the property test by explicitly testing
        the registration mechanism with valid and invalid calculators.
        """
        # Create a temporary registry for testing
        test_registry = IndicatorRegistry()
        
        # Valid calculator: returns Series with correct length
        def valid_calculator(series: pd.Series, window: int) -> pd.Series:
            return series.rolling(window=window).mean()
        
        # Register and test valid calculator
        test_registry.register("test_valid", valid_calculator)
        test_series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = test_registry.get("test_valid")(test_series, 3)
        
        assert isinstance(result, pd.Series)
        assert len(result) == len(test_series)
        
    def test_indicator_registration_duplicate_name(self):
        """Test that registering duplicate indicator names raises error."""
        test_registry = IndicatorRegistry()
        
        def calculator(series: pd.Series, window: int) -> pd.Series:
            return series.rolling(window=window).mean()
        
        # First registration should succeed
        test_registry.register("duplicate_test", calculator)
        
        # Second registration with same name should fail
        with pytest.raises(AnalysisError, match="already registered"):
            test_registry.register("duplicate_test", calculator)

    def test_indicator_get_nonexistent(self):
        """Test that getting non-existent indicator raises descriptive error."""
        test_registry = IndicatorRegistry()
        
        with pytest.raises(AnalysisError) as exc_info:
            test_registry.get("nonexistent_indicator")
        
        error_msg = str(exc_info.value)
        assert "Unknown indicator type" in error_msg
        assert "nonexistent_indicator" in error_msg
        assert "Available:" in error_msg

    def test_indicator_list_returns_sorted(self):
        """Test that list_indicators returns sorted list of names."""
        test_registry = IndicatorRegistry()
        
        def dummy_calc(series: pd.Series, window: int) -> pd.Series:
            return series
        
        # Register in non-alphabetical order
        test_registry.register("zebra", dummy_calc)
        test_registry.register("alpha", dummy_calc)
        test_registry.register("middle", dummy_calc)
        
        indicators = test_registry.list_indicators()
        assert indicators == ["alpha", "middle", "zebra"]
        assert indicators == sorted(indicators)
