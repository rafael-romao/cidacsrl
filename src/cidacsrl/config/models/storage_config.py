import dataclasses
import json
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class SourceStorageConfig:
    """Contrato estrito exclusivo para leitura de dados."""
    source_path: str
    source_format: str = "parquet"

    def __post_init__(self) -> None:
        if not self.source_path:
            raise ValueError("'source_path' é obrigatório no bloco de storage.")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceStorageConfig":
        data = data or {}
        return cls(
            source_path=data.get("source_path", ""),
            source_format=data.get("source_format", "parquet"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)


@dataclass(frozen=True)
class OutputStorageConfig:
    """Contrato estrito exclusivo para persistência de dados."""
    output_path: str
    output_format: str = "parquet"

    def __post_init__(self) -> None:
        if not self.output_path:
            raise ValueError("'output_path' é obrigatório no bloco de storage.")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OutputStorageConfig":
        data = data or {}
        return cls(
            output_path=data.get("output_path", ""),
            output_format=data.get("output_format", "parquet"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)
