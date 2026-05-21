
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class LinkageWorkflowConfig:
    """Configuração do workflow de linkage sequencial."""
    linkage_config_path: str
    es_config_path: str
    spark_config_path: str
    output_data_path: str
    source_data_path: str
    source_data_format: str
    partition_by: dict = field(default_factory=dict) # Dicionario com a estrutura das partições dos dados. Exemplo em dict: {partition_by: {partition: "uf"}, filter_partitions: ["BA", "SP"]}
    log_linkage_file: Optional[str] = None # Path onde serão salvos os logs de eventos, no estilo CDC, do linkage.
    sample_fraction: Optional[float] = None
    sample_seed: int = 42