from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class SourceConfig:
    source_table: str
    id_field: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceConfig":
        return cls(
            source_table=data["source_table"],
            id_field=data["id_field"]
        )

@dataclass
class IndexSettingsConfig:
    name: str
    id_from_source: Optional[bool] = False
    number_of_shards: int = 1
    number_of_replicas: int = 0
    refresh_interval: str = "1s"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IndexSettingsConfig":
        return cls(
            name=data["name"],            
            id_from_source=data.get("id_from_source", False),
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
    source_config: SourceConfig
    index_config: IndexSettingsConfig
    index_columns: List[IndexColumnConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetIndexingSpecification":
        return cls(
            source_config=SourceConfig.from_dict(data["source_config"]),
            index_config=IndexSettingsConfig.from_dict(data["index_config"]),
            index_columns=[IndexColumnConfig.from_dict(col) for col in data.get("index_columns", [])]
        )