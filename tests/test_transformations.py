from transform_function import kelvin_to_fahrenheit
import pytest

def test_kelvin_to_fahrenheit_valid_cases():
    """Test valid Kelvin values."""
    assert kelvin_to_fahrenheit(0) == -459.67, "Conversion for 0K failed"
    assert kelvin_to_fahrenheit(273.15) == 32.0, "Conversion for 273.15K (freezing point) failed"
    assert kelvin_to_fahrenheit(373.15) == 212.0, "Conversion for 373.15K (boiling point) failed"

def test_kelvin_to_fahrenheit_edge_cases():
    """Test edge cases for Kelvin to Fahrenheit conversion."""
    assert kelvin_to_fahrenheit(None) is None, "Conversion for None should return None"
    
    with pytest.raises(TypeError):
        kelvin_to_fahrenheit("invalid")  # Non-numeric input
    
    with pytest.raises(TypeError):
        kelvin_to_fahrenheit([300])  # List as input

def test_kelvin_to_fahrenheit_negative():
    """Test negative Kelvin values."""
    # In physics, negative Kelvin is invalid, but the function should handle it.
    result = kelvin_to_fahrenheit(-100)
    assert result < -459.67, f"Expected a temperature below -459.67F for -100K, got {result}"
