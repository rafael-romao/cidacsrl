import pytest

from cidacsrl.adapters.outbound.spark.cleaning_adapter import SparkCleaningAdapter
from cidacsrl.domain.cleaning.column import ColumnConfig

pytestmark = pytest.mark.unit


@pytest.fixture
def adapter():
    return SparkCleaningAdapter()


def _df(spark, rows: list[dict]):
    return spark.createDataFrame(rows)


def _val(df, col: str):
    return df.collect()[0][col]


class TestTrimWhitespace:
    def test_trims_leading_and_trailing(self, spark, adapter):
        df = _df(spark, [{"nome": "  João  "}])
        result = adapter.apply_column(df, ColumnConfig(name="nome", trim_whitespace=True))
        assert _val(result, "nome") == "João"

    def test_no_trim_by_default(self, spark, adapter):
        df = _df(spark, [{"nome": " abc "}])
        result = adapter.apply_column(df, ColumnConfig(name="nome"))
        assert _val(result, "nome") == " abc "


class TestInvalidValues:
    def test_replaces_single_invalid_with_null(self, spark, adapter):
        df = _df(spark, [{"raca": "99"}])
        result = adapter.apply_column(df, ColumnConfig(name="raca", invalid_values=["99"]))
        assert _val(result, "raca") is None

    def test_replaces_multiple_invalid_values(self, spark, adapter):
        rows = [{"raca": "99"}, {"raca": "NÃO INFORMADO"}, {"raca": "parda"}]
        df = _df(spark, rows)
        result = adapter.apply_column(df, ColumnConfig(name="raca", invalid_values=["99", "NÃO INFORMADO"]))
        values = [r["raca"] for r in result.collect()]
        assert values == [None, None, "parda"]

    def test_keeps_valid_values(self, spark, adapter):
        df = _df(spark, [{"raca": "branca"}])
        result = adapter.apply_column(df, ColumnConfig(name="raca", invalid_values=["99"]))
        assert _val(result, "raca") == "branca"


class TestReplaceEmptyWithNull:
    def test_replaces_empty_string(self, spark, adapter):
        df = _df(spark, [{"campo": ""}])
        result = adapter.apply_column(df, ColumnConfig(name="campo", replace_empty_with_null=True))
        assert _val(result, "campo") is None

    def test_keeps_non_empty(self, spark, adapter):
        df = _df(spark, [{"campo": "valor"}])
        result = adapter.apply_column(df, ColumnConfig(name="campo", replace_empty_with_null=True))
        assert _val(result, "campo") == "valor"

    def test_trim_then_empty_becomes_null(self, spark, adapter):
        df = _df(spark, [{"campo": "   "}])
        result = adapter.apply_column(
            df, ColumnConfig(name="campo", trim_whitespace=True, replace_empty_with_null=True)
        )
        assert _val(result, "campo") is None


class TestStandardizeCase:
    def test_upper(self, spark, adapter):
        df = _df(spark, [{"nome": "joão silva"}])
        result = adapter.apply_column(df, ColumnConfig(name="nome", standardize_case="upper"))
        assert _val(result, "nome") == "JOÃO SILVA"

    def test_lower(self, spark, adapter):
        df = _df(spark, [{"nome": "JOÃO SILVA"}])
        result = adapter.apply_column(df, ColumnConfig(name="nome", standardize_case="lower"))
        assert _val(result, "nome") == "joão silva"

    def test_title(self, spark, adapter):
        df = _df(spark, [{"nome": "JOÃO SILVA"}])
        result = adapter.apply_column(df, ColumnConfig(name="nome", standardize_case="title"))
        assert _val(result, "nome") == "João Silva"


class TestNormalizeChars:
    def test_removes_accents(self, spark, adapter):
        df = _df(spark, [{"nome": "ção"}])
        result = adapter.apply_column(df, ColumnConfig(name="nome", normalize_chars=True))
        assert _val(result, "nome") == "cao"

    def test_handles_null(self, spark, adapter):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField("nome", StringType(), nullable=True)])
        df = spark.createDataFrame([(None,)], schema)
        result = adapter.apply_column(df, ColumnConfig(name="nome", normalize_chars=True))
        assert _val(result, "nome") is None


class TestCharsToRemove:
    def test_removes_specified_chars(self, spark, adapter):
        df = _df(spark, [{"cpf": "123.456.789-00"}])
        result = adapter.apply_column(df, ColumnConfig(name="cpf", chars_to_remove=".-"))
        assert _val(result, "cpf") == "12345678900"

    def test_removes_special_regex_chars(self, spark, adapter):
        df = _df(spark, [{"campo": "ab[cd]ef"}])
        result = adapter.apply_column(df, ColumnConfig(name="campo", chars_to_remove="[]"))
        assert _val(result, "campo") == "abcdef"


class TestTruncate:
    def test_truncates_to_length(self, spark, adapter):
        df = _df(spark, [{"codigo": "1234567"}])
        result = adapter.apply_column(df, ColumnConfig(name="codigo", truncate_length=6))
        assert _val(result, "codigo") == "123456"


class TestCastTo:
    def test_casts_to_integer(self, spark, adapter):
        df = _df(spark, [{"idade": "30"}])
        result = adapter.apply_column(df, ColumnConfig(name="idade", cast_to="integer"))
        assert _val(result, "idade") == 30


class TestRename:
    def test_renames_column(self, spark, adapter):
        df = _df(spark, [{"nome_original": "valor"}])
        result = adapter.apply_column(
            df, ColumnConfig(name="nome_original", cleaned_name="nome_limpo")
        )
        assert "nome_limpo" in result.columns
        assert "nome_original" not in result.columns


class TestApplyMultiple:
    def test_applies_all_configs_in_order(self, spark, adapter):
        df = _df(spark, [{"nome": " joão  ", "municipio": "SÃO PAULO"}])
        configs = [
            ColumnConfig(name="nome", trim_whitespace=True, standardize_case="upper"),
            ColumnConfig(name="municipio", normalize_chars=True, standardize_case="upper"),
        ]
        result = adapter.apply(df, configs)
        row = result.collect()[0]
        assert row["nome"] == "JOÃO"
        assert row["municipio"] == "SAO PAULO"
