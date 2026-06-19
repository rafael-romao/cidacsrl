from dataclasses import dataclass, field
from typing import Any, Dict

from cidacsrl.config.models.storage_config import SourceStorageConfig, OutputStorageConfig
from cidacsrl.domain.deduplication.deduplication_specification import DeduplicationSpecification


@dataclass(frozen=True)
class DeduplicateWorkflowConfig:
    """Config de ambiente para o workflow de deduplicação.

    Estrutura YAML esperada::

        app_name: "CIDACS-RL Deduplication"

        storage:
          source_path: "data/linked_pairs.parquet"
          source_format: "parquet"
          output_path: "data/deduplicated.parquet"
          output_format: "parquet"

        spark:
          spark_configs:
            spark.master: "local[*]"
            spark.sql.shuffle.partitions: "4"
            spark.ui.enabled: "false"

        deduplication:
          id_source_column: "id_table"
          id_target_column: "candidate_id_table"
          output_group_id_column: "cidacs_cluster_id"
    """
    source_storage: SourceStorageConfig
    output_storage: OutputStorageConfig
    deduplication_spec: DeduplicationSpecification
    app_name: str = "CIDACS-RL Deduplication"
    spark_configs: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeduplicateWorkflowConfig":
        storage_data = data.get("storage")
        if not storage_data:
            raise ValueError("O bloco 'storage' é obrigatório.")

        dedup_data = data.get("deduplication")
        if not dedup_data:
            raise ValueError(
                "O bloco 'deduplication' é obrigatório e deve conter "
                "'id_source_column' e 'id_target_column'."
            )

        spark_configs = data.get("spark", {}).get("spark_configs", {})

        return cls(
            source_storage=SourceStorageConfig.from_dict(storage_data),
            output_storage=OutputStorageConfig.from_dict(storage_data),
            deduplication_spec=DeduplicationSpecification.from_dict(dedup_data),
            app_name=data.get("app_name", "CIDACS-RL Deduplication"),
            spark_configs=spark_configs,
        )
