"""Unit tests for indicator calculator functions (SMA, EMA, WMA)."""

import pandas as pd
import pytest

from finance_cli.analysis import (
    calculate_sma,
    calculate_ema,
    calculate_wma,
    get_indicator_registry,
)


class TestSMACalculator:
    """Tests for Simple Moving Average calculator."""

    def test_calculators_are_available_from_indicator_module(self) -> None:
        from finance_cli.analysis_indicators import (
            calculate_ema as calculate_ema_module,
            calculate_sma as calculate_sma_module,
            calculate_wma as calculate_wma_module,
        )

        series = pd.Series([1.0, 2.0, 3.0])

        assert len(calculate_sma_module(series, 2)) == len(series)
        assert len(calculate_ema_module(series, 2)) == len(series)
        assert len(calculate_wma_module(series, 2)) == len(series)

    def test_sma_with_known_values(self) -> None:
        """Test SMA calculation with known input/output values."""
        series = pd.Series([2.0, 4.0, 6.0, 8.0, 10.0])
        window = 3
        
        result = calculate_sma(series, window)
        
        # First two values should be NaN (window-1)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        # Remaining values should be the rolling mean
        assert result.iloc[2] == pytest.approx(4.0)  # (2+4+6)/3
        assert result.iloc[3] == pytest.approx(6.0)  # (4+6+8)/3
        assert result.iloc[4] == pytest.approx(8.0)  # (6+8+10)/3

    def test_sma_window_2(self) -> None:
        """Test SMA with window size 2."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0])
        window = 2
        
        result = calculate_sma(series, window)
        
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(15.0)  # (10+20)/2
        assert result.iloc[2] == pytest.approx(25.0)  # (20+30)/2
        assert result.iloc[3] == pytest.approx(35.0)  # (30+40)/2

    def test_sma_window_equals_1(self) -> None:
        """Test SMA with window=1 returns original series."""
        series = pd.Series([5.0, 10.0, 15.0, 20.0])
        window = 1
        
        result = calculate_sma(series, window)
        
        # Window of 1 should return the original values
        pd.testing.assert_series_equal(result, series, check_names=False)

    def test_sma_window_equals_data_length(self) -> None:
        """Test SMA when window equals data length."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0])
        window = 4
        
        result = calculate_sma(series, window)
        
        # First 3 values should be NaN
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])
        # Last value should be the mean of all values
        assert result.iloc[3] == pytest.approx(25.0)  # (10+20+30+40)/4

    def test_sma_window_greater_than_data_length(self) -> None:
        """Test SMA when window is greater than data length."""
        series = pd.Series([10.0, 20.0, 30.0])
        window = 5
        
        result = calculate_sma(series, window)
        
        # All values should be NaN when window > data length
        assert result.isna().all()

    def test_sma_returns_same_length_as_input(self) -> None:
        """Test that SMA returns a series with the same length as input."""
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        window = 3
        
        result = calculate_sma(series, window)
        
        assert len(result) == len(series)

    def test_sma_with_single_value(self) -> None:
        """Test SMA with a single data point."""
        series = pd.Series([42.0])
        window = 1
        
        result = calculate_sma(series, window)
        
        assert len(result) == 1
        assert result.iloc[0] == pytest.approx(42.0)

    def test_sma_with_empty_series(self) -> None:
        """Test SMA with empty series."""
        series = pd.Series([], dtype=float)
        window = 3
        
        result = calculate_sma(series, window)
        
        assert len(result) == 0


class TestEMACalculator:
    """Tests for Exponential Moving Average calculator."""

    def test_ema_with_known_values(self) -> None:
        """Test EMA calculation with known input/output values."""
        series = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0])
        window = 3
        
        result = calculate_ema(series, window)
        
        # EMA should have no NaN values (starts from first value)
        assert not result.isna().any()
        # First value should equal the first input
        assert result.iloc[0] == pytest.approx(10.0)
        # Subsequent values should be exponentially weighted
        # Using span=3, alpha = 2/(span+1) = 0.5
        assert result.iloc[1] == pytest.approx(10.5)  # 10 + 0.5*(11-10)
        assert result.iloc[2] == pytest.approx(11.25)  # 10.5 + 0.5*(12-10.5)
        assert result.iloc[3] == pytest.approx(12.125)  # 11.25 + 0.5*(13-11.25)
        assert result.iloc[4] == pytest.approx(13.0625)  # 12.125 + 0.5*(14-12.125)

    def test_ema_window_2(self) -> None:
        """Test EMA with window size 2."""
        series = pd.Series([20.0, 22.0, 24.0, 26.0])
        window = 2
        
        result = calculate_ema(series, window)
        
        # With span=2, alpha = 2/(2+1) = 2/3
        assert result.iloc[0] == pytest.approx(20.0)
        assert result.iloc[1] == pytest.approx(21.333333, abs=1e-5)  # 20 + (2/3)*(22-20)
        assert result.iloc[2] == pytest.approx(23.111111, abs=1e-5)  # 21.333 + (2/3)*(24-21.333)
        assert result.iloc[3] == pytest.approx(25.037037, abs=1e-5)  # 23.111 + (2/3)*(26-23.111)

    def test_ema_window_equals_1(self) -> None:
        """Test EMA with window=1."""
        series = pd.Series([5.0, 10.0, 15.0, 20.0])
        window = 1
        
        result = calculate_ema(series, window)
        
        # With span=1, alpha = 2/(1+1) = 1.0, so EMA equals the series
        pd.testing.assert_series_equal(result, series, check_names=False)

    def test_ema_window_equals_data_length(self) -> None:
        """Test EMA when window equals data length."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0])
        window = 4
        
        result = calculate_ema(series, window)
        
        # EMA should still calculate for all values
        assert not result.isna().any()
        assert result.iloc[0] == pytest.approx(10.0)
        # With span=4, alpha = 2/5 = 0.4
        assert result.iloc[1] == pytest.approx(14.0)  # 10 + 0.4*(20-10)

    def test_ema_returns_same_length_as_input(self) -> None:
        """Test that EMA returns a series with the same length as input."""
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        window = 3
        
        result = calculate_ema(series, window)
        
        assert len(result) == len(series)

    def test_ema_with_single_value(self) -> None:
        """Test EMA with a single data point."""
        series = pd.Series([42.0])
        window = 3
        
        result = calculate_ema(series, window)
        
        assert len(result) == 1
        assert result.iloc[0] == pytest.approx(42.0)

    def test_ema_with_empty_series(self) -> None:
        """Test EMA with empty series."""
        series = pd.Series([], dtype=float)
        window = 3
        
        result = calculate_ema(series, window)
        
        assert len(result) == 0


class TestWMACalculator:
    """Tests for Weighted Moving Average calculator."""

    def test_wma_with_known_values(self) -> None:
        """Test WMA calculation with known input/output values."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        window = 3
        
        result = calculate_wma(series, window)
        
        # First two values should be NaN (window-1)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        # WMA with weights [1, 2, 3]
        # (10*1 + 20*2 + 30*3) / (1+2+3) = 140/6 = 23.333...
        assert result.iloc[2] == pytest.approx(23.333333, abs=1e-5)
        # (20*1 + 30*2 + 40*3) / 6 = 200/6 = 33.333...
        assert result.iloc[3] == pytest.approx(33.333333, abs=1e-5)
        # (30*1 + 40*2 + 50*3) / 6 = 260/6 = 43.333...
        assert result.iloc[4] == pytest.approx(43.333333, abs=1e-5)

    def test_wma_window_2(self) -> None:
        """Test WMA with window size 2."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0])
        window = 2
        
        result = calculate_wma(series, window)
        
        assert pd.isna(result.iloc[0])
        # Weights [1, 2], sum = 3
        # (10*1 + 20*2) / 3 = 50/3 = 16.666...
        assert result.iloc[1] == pytest.approx(16.666667, abs=1e-5)
        # (20*1 + 30*2) / 3 = 80/3 = 26.666...
        assert result.iloc[2] == pytest.approx(26.666667, abs=1e-5)
        # (30*1 + 40*2) / 3 = 110/3 = 36.666...
        assert result.iloc[3] == pytest.approx(36.666667, abs=1e-5)

    def test_wma_window_equals_1(self) -> None:
        """Test WMA with window=1 returns original series."""
        series = pd.Series([5.0, 10.0, 15.0, 20.0])
        window = 1
        
        result = calculate_wma(series, window)
        
        # Window of 1 with weight [1] should return original values
        pd.testing.assert_series_equal(result, series, check_names=False)

    def test_wma_window_equals_data_length(self) -> None:
        """Test WMA when window equals data length."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0])
        window = 4
        
        result = calculate_wma(series, window)
        
        # First 3 values should be NaN
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])
        # Weights [1, 2, 3, 4], sum = 10
        # (10*1 + 20*2 + 30*3 + 40*4) / 10 = 300/10 = 30.0
        assert result.iloc[3] == pytest.approx(30.0)

    def test_wma_window_greater_than_data_length(self) -> None:
        """Test WMA when window is greater than data length."""
        series = pd.Series([10.0, 20.0, 30.0])
        window = 5
        
        result = calculate_wma(series, window)
        
        # All values should be NaN when window > data length
        assert result.isna().all()

    def test_wma_returns_same_length_as_input(self) -> None:
        """Test that WMA returns a series with the same length as input."""
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        window = 3
        
        result = calculate_wma(series, window)
        
        assert len(result) == len(series)

    def test_wma_with_single_value(self) -> None:
        """Test WMA with a single data point."""
        series = pd.Series([42.0])
        window = 1
        
        result = calculate_wma(series, window)
        
        assert len(result) == 1
        assert result.iloc[0] == pytest.approx(42.0)

    def test_wma_with_empty_series(self) -> None:
        """Test WMA with empty series."""
        series = pd.Series([], dtype=float)
        window = 3
        
        result = calculate_wma(series, window)
        
        assert len(result) == 0

    def test_wma_weights_increase_linearly(self) -> None:
        """Test that WMA gives more weight to recent values."""
        # Create a series where recent values are higher
        series = pd.Series([10.0, 10.0, 10.0, 100.0])
        window = 4
        
        result = calculate_wma(series, window)
        
        # Weights [1, 2, 3, 4], sum = 10
        # (10*1 + 10*2 + 10*3 + 100*4) / 10 = 460/10 = 46.0
        # This should be higher than SMA which would be (10+10+10+100)/4 = 32.5
        assert result.iloc[3] == pytest.approx(46.0)
        assert result.iloc[3] > 32.5  # Verify it's higher than SMA


class TestIndicatorComparison:
    """Tests comparing behavior across different indicators."""

    def test_all_indicators_return_same_length(self) -> None:
        """Test that all indicators return series with same length as input."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        window = 3
        
        sma_result = calculate_sma(series, window)
        ema_result = calculate_ema(series, window)
        wma_result = calculate_wma(series, window)
        
        assert len(sma_result) == len(series)
        assert len(ema_result) == len(series)
        assert len(wma_result) == len(series)

    def test_sma_and_wma_have_same_nan_pattern(self) -> None:
        """Test that SMA and WMA have NaN values in the same positions."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        window = 3
        
        sma_result = calculate_sma(series, window)
        wma_result = calculate_wma(series, window)
        
        # Both should have NaN in first (window-1) positions
        assert sma_result.isna().equals(wma_result.isna())

    def test_ema_has_no_nan_values(self) -> None:
        """Test that EMA produces no NaN values for valid input."""
        series = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        window = 3
        
        ema_result = calculate_ema(series, window)
        
        # EMA should have no NaN values
        assert not ema_result.isna().any()

    def test_indicators_with_constant_series(self) -> None:
        """Test all indicators with a constant series."""
        series = pd.Series([25.0, 25.0, 25.0, 25.0, 25.0])
        window = 3
        
        sma_result = calculate_sma(series, window)
        ema_result = calculate_ema(series, window)
        wma_result = calculate_wma(series, window)
        
        # For constant series, all non-NaN values should equal the constant
        assert sma_result.dropna().eq(25.0).all()
        assert ema_result.eq(25.0).all()
        assert wma_result.dropna().eq(25.0).all()


# Property-Based Tests

from hypothesis import given, strategies as st, settings


class TestWindowParameterConsistency:
    """Property-based tests for window parameter consistency across indicators.
    
    **Validates: Requirements 1.6**
    """

    @settings(max_examples=100)
    @given(
        indicator_type=st.sampled_from(["sma", "ema", "wma"]),
        window=st.integers(min_value=2, max_value=10),
        data_length=st.integers(min_value=10, max_value=50)
    )
    def test_window_parameter_consistency(
        self,
        indicator_type: str,
        window: int,
        data_length: int
    ) -> None:
        """Property 2: Window Parameter Consistency
        
        For any registered indicator type and valid window size, the indicator
        calculation should use the specified window parameter correctly.
        
        This test verifies that:
        1. The indicator returns a series of the same length as input
        2. For rolling indicators (SMA, WMA), the first (window-1) values are NaN
        3. For EMA, all values are non-NaN (starts from first value)
        4. The window parameter affects the calculation correctly
        """
        # Generate test data with monotonically increasing values
        # This makes it easier to verify window behavior
        series = pd.Series(range(1, data_length + 1), dtype=float)
        
        # Get the calculator from the registry
        registry = get_indicator_registry()
        calculator = registry.get(indicator_type)
        
        # Calculate indicator
        result = calculator(series, window)
        
        # Property 1: Result should have same length as input
        assert len(result) == len(series), (
            f"Indicator {indicator_type} returned wrong length: "
            f"expected {len(series)}, got {len(result)}"
        )
        
        # Property 2: For rolling indicators (SMA, WMA), verify NaN pattern
        if indicator_type in ["sma", "wma"]:
            # First (window-1) values should be NaN
            nan_count = result.iloc[:window-1].isna().sum()
            assert nan_count == window - 1, (
                f"Indicator {indicator_type} with window={window} should have "
                f"{window-1} NaN values at start, but has {nan_count}"
            )
            
            # Values after window should be non-NaN (for sufficient data)
            if data_length >= window:
                non_nan_count = result.iloc[window-1:].notna().sum()
                expected_non_nan = data_length - window + 1
                assert non_nan_count == expected_non_nan, (
                    f"Indicator {indicator_type} with window={window} should have "
                    f"{expected_non_nan} non-NaN values, but has {non_nan_count}"
                )
        
        # Property 3: For EMA, all values should be non-NaN
        elif indicator_type == "ema":
            assert result.notna().all(), (
                f"EMA should have no NaN values, but found "
                f"{result.isna().sum()} NaN values"
            )
        
        # Property 4: Verify window affects calculation
        # For monotonically increasing data, the indicator value at position
        # window should be related to the window size
        if data_length >= window:
            if indicator_type == "sma":
                # SMA at position window-1 should be the mean of first window values
                expected_sma = sum(range(1, window + 1)) / window
                assert result.iloc[window - 1] == pytest.approx(expected_sma), (
                    f"SMA at position {window-1} should be {expected_sma}, "
                    f"got {result.iloc[window - 1]}"
                )
            elif indicator_type == "wma":
                # WMA at position window-1 should use weighted average
                weights = list(range(1, window + 1))
                values = list(range(1, window + 1))
                expected_wma = sum(v * w for v, w in zip(values, weights)) / sum(weights)
                assert result.iloc[window - 1] == pytest.approx(expected_wma, abs=1e-5), (
                    f"WMA at position {window-1} should be approximately {expected_wma}, "
                    f"got {result.iloc[window - 1]}"
                )

    @settings(max_examples=100)
    @given(
        indicator_type=st.sampled_from(["sma", "ema", "wma"]),
        window=st.integers(min_value=1, max_value=5),
        data_length=st.integers(min_value=5, max_value=20)
    )
    def test_window_parameter_affects_smoothing(
        self,
        indicator_type: str,
        window: int,
        data_length: int
    ) -> None:
        """Property 2 (Extended): Window Parameter Affects Smoothing
        
        For any indicator, larger window sizes should produce smoother results
        (less variation between consecutive values) for the same input data.
        """
        # Generate test data with some variation
        series = pd.Series([float(i + (i % 3)) for i in range(data_length)])
        
        # Get the calculator
        registry = get_indicator_registry()
        calculator = registry.get(indicator_type)
        
        # Calculate with current window
        result = calculator(series, window)
        
        # If we have a larger window available, compare smoothness
        if window < data_length - 1:
            larger_window = min(window + 2, data_length)
            result_larger = calculator(series, larger_window)
            
            # Calculate variation (sum of absolute differences between consecutive values)
            # Skip NaN values for comparison
            variation_current = result.dropna().diff().abs().sum()
            variation_larger = result_larger.dropna().diff().abs().sum()
            
            # Larger window should generally produce less variation (smoother)
            # Note: This is a general property but may not hold for all edge cases
            # especially with EMA which has different behavior
            if indicator_type in ["sma", "wma"] and len(result.dropna()) > 2 and len(result_larger.dropna()) > 2:
                # For rolling averages, larger windows should smooth more
                # Allow some tolerance for edge cases
                assert variation_larger <= variation_current * 1.5, (
                    f"Larger window should produce smoother results for {indicator_type}, "
                    f"but variation increased: {variation_current} -> {variation_larger}"
                )

    @settings(max_examples=100)
    @given(
        window=st.integers(min_value=1, max_value=10),
        data_length=st.integers(min_value=10, max_value=30)
    )
    def test_all_indicators_respect_window_parameter(
        self,
        window: int,
        data_length: int
    ) -> None:
        """Property 2 (Cross-Indicator): All Indicators Respect Window Parameter
        
        For any window size, all registered indicators should accept and use
        the window parameter without errors.
        """
        # Generate test data
        series = pd.Series(range(1, data_length + 1), dtype=float)
        
        # Get all registered indicators
        registry = get_indicator_registry()
        indicators = registry.list_indicators()
        
        # Test each indicator
        for indicator_name in indicators:
            calculator = registry.get(indicator_name)
            
            # Should not raise an error
            result = calculator(series, window)
            
            # Should return correct length
            assert len(result) == len(series), (
                f"Indicator {indicator_name} with window={window} returned "
                f"wrong length: expected {len(series)}, got {len(result)}"
            )
            
            # Should return a pandas Series
            assert isinstance(result, pd.Series), (
                f"Indicator {indicator_name} should return pd.Series, "
                f"got {type(result)}"
            )
