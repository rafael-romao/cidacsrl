import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class PartitionedDataReader:
    """
    Adapter Spark para leitura de dados particionados ou não particionados.

    Centraliza três responsabilidades que estão hoje espalhadas em
    `sequential_linkage_workflow.get_available_partitions` e
    `io_manager.read_source_data` / `ler_particao`:

    - Descoberta de partições disponíveis no filesystem
    - Leitura de uma partição específica ou do dataset completo
    - Aplicação de amostragem reproduzível
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def get_partitions(
        self,
        source_path: Union[str, Path],
        partition_config: Dict[str, Any],
    ) -> List[str]:
        """
        Retorna os valores de partição disponíveis no path de origem.

        Se partition_config não tiver a chave 'partition', retorna ["*"]
        indicando que o dataset deve ser lido por inteiro.

        Args:
            source_path: Caminho para o diretório de dados.
            partition_config: Dict com 'partition' (nome da coluna de partição)
                              e opcionalmente 'filter_partitions' (lista de
                              valores permitidos).

        Returns:
            Lista de valores de partição, ou ["*"] se não particionado.
        """
        partition_col = partition_config.get("partition") if partition_config else None

        if not partition_col:
            return ["*"]

        logger.info(
            f"Discovering partitions for column '{partition_col}' in: {source_path}"
        )

        partitions = [
            row[0]
            for row in (
                self._spark.read.parquet(str(source_path))
                .select(partition_col)
                .distinct()
                .collect()
            )
        ]

        filter_values = partition_config.get("filter_partitions")
        if filter_values:
            partitions = [p for p in partitions if p in filter_values]

        logger.info(f"{len(partitions)} partition(s) found: {partitions}")
        return partitions

    def read(
        self,
        source_path: Union[str, Path],
        partition_col: Optional[str] = None,
        partition: str = "*",
        sample_fraction: Optional[float] = None,
        sample_seed: Optional[int] = None,
    ) -> DataFrame:
        """
        Lê dados do source_path para uma partição específica ou o dataset completo.

        Estratégia de leitura quando partition != "*":
        1. Se existir subdiretório `{partition_col}={partition}` (Hive-style):
           lê o subdiretório e adiciona a coluna de partição como literal.
        2. Caso contrário: lê o dataset completo e filtra em memória.

        Args:
            source_path: Caminho para o diretório de dados.
            partition_col: Nome da coluna de partição. Obrigatório quando
                           partition != "*".
            partition: Valor da partição. Use "*" para leitura completa.
            sample_fraction: Fração de amostragem [0.0, 1.0]. None desativa.
            sample_seed: Semente para reproducibilidade da amostragem.

        Returns:
            DataFrame com os dados lidos, opcionalmente amostrado.

        Raises:
            FileNotFoundError: Se source_path não existir no filesystem.
            ValueError: Se partition_col não for fornecido quando partition != "*".
        """
        source_path = Path(source_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source data path not found: {source_path}")

        df = self._read_partition(source_path, partition_col, partition)

        if sample_fraction and 0.0 < sample_fraction <= 1.0:
            df = df.sample(
                withReplacement=False,
                fraction=sample_fraction,
                seed=sample_seed,
            )
            logger.info(
                f"Sampling applied: fraction={sample_fraction}, seed={sample_seed}"
            )

        return df

    def _read_partition(
        self,
        source_path: Path,
        partition_col: Optional[str],
        partition: str,
    ) -> DataFrame:
        if partition == "*":
            logger.info(f"Reading full dataset from: {source_path}")
            return self._spark.read.parquet(str(source_path))

        if not partition_col:
            raise ValueError(
                f"partition_col must be provided when reading a specific partition "
                f"(got partition='{partition}')."
            )

        # Hive-style partition directory: <path>/<col>=<value>
        partitioned_path = source_path / f"{partition_col}={partition}"

        if partitioned_path.exists():
            logger.info(f"Reading partition directory: {partitioned_path}")
            return (
                self._spark.read.parquet(str(partitioned_path))
                .withColumn(partition_col, F.lit(partition))
            )

        # Fallback: dados não foram particionados na escrita
        logger.info(
            f"Partition directory not found. Reading full dataset and filtering "
            f"on {partition_col}='{partition}'"
        )
        return (
            self._spark.read.parquet(str(source_path))
            .filter(F.col(partition_col) == partition)
        )