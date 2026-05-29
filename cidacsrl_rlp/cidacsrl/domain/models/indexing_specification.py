from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

@dataclass
class IndexSettingsConfig:
    name: str
    number_of_shards: int = 1
    number_of_replicas: int = 0
    refresh_interval: str = "1s"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IndexSettingsConfig":
        return cls(
            name=data["name"],
            number_of_shards=data.get("number_of_shards", 1),
            number_of_replicas=data.get("number_of_replicas", 0),
            refresh_interval=data.get("refresh_interval", "1s")
        )

@dataclass
class IndexColumnConfig:
    name: str
    type: str
    index_as: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IndexColumnConfig":
        return cls(
            name=data["name"],
            type=data["type"],
            index_as=data.get("index_as")
        )

@dataclass
class DatasetIndexingSpecification:
    index_config: IndexSettingsConfig
    columns: List[IndexColumnConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetIndexingSpecification":
        return cls(
            index_config=IndexSettingsConfig.from_dict(data["index_config"]),
            columns=[IndexColumnConfig.from_dict(col) for col in data.get("columns", [])]
        )