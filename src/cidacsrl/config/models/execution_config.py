import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass(frozen=True)
class DataPartitioningConfig:
    """Estratégia de particionamento lógico das work units.

    Attributes:
        partition_column: Coluna usada para dividir os dados em work units; None para execução global.
        filter_partitions: Subconjunto de partições a processar; vazio significa todas.
    """

    partition_column: Optional[str] = None
    filter_partitions: List[str] = field(default_factory=list)

    @property
    def has_filters(self) -> bool:
        return bool(self.partition_column and self.filter_partitions)

@dataclass(frozen=True)
class ExecutionConfig:
    """Contexto de execução de um job de linkage.

    Attributes:
        job_id: Identificador único do job; gerado automaticamente se não informado.
        partitioning: Configuração de particionamento lógico das work units.
        sample_fraction: Fração da tabela a amostrar (0.0–1.0); None para usar todos os dados.
        sample_seed: Semente para reprodutibilidade da amostragem. Defaults to 42.
        audit_log_path: Caminho base para escrita de telemetria JSONL e checkpoints.
    """

    job_id: Optional[str] = None
    partitioning: DataPartitioningConfig = field(default_factory=DataPartitioningConfig)
    sample_fraction: Optional[float] = None
    sample_seed: int = 42
    audit_log_path: Optional[str] = None

    def __post_init__(self):
        if not self.job_id:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            object.__setattr__(self, "job_id", f"job_{timestamp_str}")

    def with_discovered_partitions(self, partitions: List[str]) -> "ExecutionConfig":
        """Retorna nova instância com as partições descobertas substituindo filter_partitions.

        Args:
            partitions: Lista de valores de partição descobertos dinamicamente.

        Returns:
            Nova ExecutionConfig com partitioning.filter_partitions atualizado.
        """
        return dataclasses.replace(
            self,
            partitioning=dataclasses.replace(self.partitioning, filter_partitions=partitions)
        )