from dataclasses import dataclass

@dataclass(frozen=True)
class SourceStorageConfig:
    """Contrato estrito exclusivo para leitura de dados."""
    source_data_path: str
    source_data_format: str = "parquet"

@dataclass(frozen=True)
class OutputStorageConfig:
    """Contrato estrito exclusivo para persistência de dados."""
    output_data_path: str
    output_data_format: str = "parquet"