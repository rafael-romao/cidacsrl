import dataclasses
import json
from dataclasses import dataclass, field
from typing import Any, Dict

from cidacsrl.config.models.storage_config import (
    OutputStorageConfig,
    SourceStorageConfig,
)
from cidacsrl.domain.deduplication.deduplication_specification import (
    DeduplicationSpecification,
)


@dataclass(frozen=True)
class DeduplicateWorkflowConfig:
    """Configuração completa do workflow de deduplicação.

    Attributes:
        source_storage: Configuração de leitura dos pares linkados de entrada.
        output_storage: Configuração de escrita do resultado deduplicado.
        deduplication_spec: Mapeamento das colunas de ID e da coluna de cluster de saída.
        app_name: Nome da SparkSession. Defaults to "CIDACS-RL Deduplication".
        spark_configs: Parâmetros adicionais da SparkSession.
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

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)
