import re
import unicodedata
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from cidacsrl.domain.cleaning.column import ColumnConfig


def _normalize(text: str) -> str:
    if text is None:
        return None
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


_normalize_udf = F.udf(_normalize, StringType())


def _chars_to_regex_class(chars: str) -> str:
    # Escapa apenas os metacaracteres especiais dentro de uma classe de caracteres regex
    escaped = re.sub(r"([\]\[\\^-])", r"\\\1", chars)
    return f"[{escaped}]"


class SparkCleaningAdapter:
    """Aplica as transformações declaradas em ColumnConfig a DataFrames Spark."""

    def apply_column(self, df: DataFrame, config: ColumnConfig) -> DataFrame:
        col = config.name

        if config.trim_whitespace:
            df = df.withColumn(col, F.trim(F.col(col)))

        if config.invalid_values:
            df = df.withColumn(
                col,
                F.when(F.col(col).isin(config.invalid_values), F.lit(None)).otherwise(F.col(col)),
            )

        if config.replace_empty_with_null:
            df = df.withColumn(
                col,
                F.when(F.col(col) == "", F.lit(None)).otherwise(F.col(col)),
            )

        if config.chars_to_remove:
            pattern = _chars_to_regex_class(config.chars_to_remove)
            df = df.withColumn(col, F.regexp_replace(F.col(col), pattern, ""))

        if config.normalize_chars:
            df = df.withColumn(col, _normalize_udf(F.col(col)))

        if config.standardize_case == "upper":
            df = df.withColumn(col, F.upper(F.col(col)))
        elif config.standardize_case == "lower":
            df = df.withColumn(col, F.lower(F.col(col)))
        elif config.standardize_case == "title":
            df = df.withColumn(col, F.initcap(F.col(col)))

        if config.truncate_length:
            df = df.withColumn(col, F.substring(F.col(col), 1, config.truncate_length))

        if config.cast_to:
            df = df.withColumn(col, F.col(col).cast(config.cast_to))

        if config.cleaned_name != config.name:
            df = df.withColumnRenamed(config.name, config.cleaned_name)

        return df

    def apply(self, df: DataFrame, configs: List[ColumnConfig]) -> DataFrame:
        for config in configs:
            df = self.apply_column(df, config)
        return df
