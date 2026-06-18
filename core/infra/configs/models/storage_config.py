from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class SourceStorageConfig:
    """Contrato estrito exclusivo para leitura de dados."""
    source_path: str
    source_format: str = "parquet"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceStorageConfig":
        if not data or not data.get("source_path"):
            raise ValueError("'source_path' é obrigatório no bloco de storage.")
        return cls(
            source_path=data["source_path"],
            source_format=data.get("source_format", "parquet"),
        )


@dataclass(frozen=True)
class OutputStorageConfig:
    """Contrato estrito exclusivo para persistência de dados."""
    output_path: str
    output_format: str = "parquet"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OutputStorageConfig":
        if not data or not data.get("output_path"):
            raise ValueError("'output_path' é obrigatório no bloco de storage.")
        return cls(
            output_path=data["output_path"],
            output_format=data.get("output_format", "parquet"),
        )
