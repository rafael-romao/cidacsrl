from dataclasses import dataclass, field
from typing import Optional, List

@dataclass(frozen=True)
class DataPartitioningConfig:    
    partition_column: Optional[str] = None      # Ex: "uf"
    filter_partitions: List[str] = field(default_factory=list)  # Ex: ["BA", "SP"]

    @property
    def has_filters(self) -> bool:
        return bool(self.partition_column and self.filter_partitions)

@dataclass(frozen=True)
class ExecutionConfig:
    """Responsável por COMO o pipeline deve se comportar durante a execução."""
    partitioning: DataPartitioningConfig = field(default_factory=DataPartitioningConfig)
    sample_fraction: Optional[float] = None
    sample_seed: int = 42
    audit_log_path: Optional[str] = None