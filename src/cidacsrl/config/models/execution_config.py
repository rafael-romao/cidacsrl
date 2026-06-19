import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass(frozen=True)
class DataPartitioningConfig:
    partition_column: Optional[str] = None
    filter_partitions: List[str] = field(default_factory=list)

    @property
    def has_filters(self) -> bool:
        return bool(self.partition_column and self.filter_partitions)

@dataclass(frozen=True)
class ExecutionConfig:
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
        return dataclasses.replace(
            self,
            partitioning=dataclasses.replace(self.partitioning, filter_partitions=partitions)
        )