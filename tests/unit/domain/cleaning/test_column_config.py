import pytest

from cidacsrl.domain.cleaning.column import ColumnConfig

pytestmark = pytest.mark.unit


def test_cleaned_name_defaults_to_name():
    config = ColumnConfig(name="nome")
    assert config.cleaned_name == "nome"


def test_cleaned_name_explicit():
    config = ColumnConfig(name="nome", cleaned_name="nome_limpo")
    assert config.cleaned_name == "nome_limpo"


def test_invalid_values_defaults_empty():
    config = ColumnConfig(name="col")
    assert config.invalid_values == []


def test_invalid_values_accepts_list():
    config = ColumnConfig(name="col", invalid_values=["99", "NÃO INFORMADO"])
    assert config.invalid_values == ["99", "NÃO INFORMADO"]


def test_standardize_case_upper():
    config = ColumnConfig(name="col", standardize_case="upper")
    assert config.standardize_case == "upper"


def test_standardize_case_lower():
    config = ColumnConfig(name="col", standardize_case="lower")
    assert config.standardize_case == "lower"


def test_standardize_case_title():
    config = ColumnConfig(name="col", standardize_case="title")
    assert config.standardize_case == "title"


def test_standardize_case_invalid_raises():
    with pytest.raises(ValueError, match="standardize_case"):
        ColumnConfig(name="col", standardize_case="UPPER")


def test_truncate_length_positive():
    config = ColumnConfig(name="col", truncate_length=6)
    assert config.truncate_length == 6


def test_truncate_length_zero_raises():
    with pytest.raises(ValueError, match="truncate_length"):
        ColumnConfig(name="col", truncate_length=0)


def test_truncate_length_negative_raises():
    with pytest.raises(ValueError, match="truncate_length"):
        ColumnConfig(name="col", truncate_length=-1)


def test_trim_whitespace_default_false():
    config = ColumnConfig(name="col")
    assert config.trim_whitespace is False


def test_replace_empty_with_null_default_false():
    config = ColumnConfig(name="col")
    assert config.replace_empty_with_null is False


def test_normalize_chars_default_false():
    config = ColumnConfig(name="col")
    assert config.normalize_chars is False


def test_all_defaults():
    config = ColumnConfig(name="campo")
    assert config.cleaned_name == "campo"
    assert config.invalid_values == []
    assert config.standardize_case is None
    assert config.replace_empty_with_null is False
    assert config.trim_whitespace is False
    assert config.cast_to is None
    assert config.chars_to_remove is None
    assert config.normalize_chars is False
    assert config.truncate_length is None
