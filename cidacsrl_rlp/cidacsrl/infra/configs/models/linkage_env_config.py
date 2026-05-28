from dataclasses import dataclass, field
from typing import Optional, List

@dataclass(frozen=True)
class DataPartitioningConfig:
    """Estrutura interna para governar o particionamento físico dos dados fonte."""
    partition_column: Optional[str] = None      # Ex: "uf"
    filter_partitions: List[str] = field(default_factory=list)  # Ex: ["BA", "SP"]

    @property
    def has_filters(self) -> bool:
        return bool(self.partition_column and self.filter_partitions)


@dataclass(frozen=True)
class LinkageEnvironmentConfig:
    """Configuração de ambiente e infraestrutura para o workflow de linkage sequencial."""
    
    # Caminhos de fiação para outros subsistemas/YAMLs
    linkage_specification_path: str
    es_config_path: str
    spark_config_path: str
    
    # Mapeamento físico de I/O do FileSystem (HDFS, Local ou Object Storage)
    source_data_path: str
    output_data_path: str
    source_data_format: str
    output_data_format: str = "parquet"
    
    # Governança e Otimização de dados no Spark
    partitioning: DataPartitioningConfig = field(default_factory=DataPartitioningConfig)
    
    # Amostragem estatística controlada para homologação de volumes
    sample_fraction: Optional[float] = None
    sample_seed: int = 42
    
    # Trilha de auditoria dos eventos de cruzamento
    audit_log_path: Optional[str] = None