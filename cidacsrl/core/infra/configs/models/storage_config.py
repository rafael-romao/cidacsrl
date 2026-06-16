from dataclasses import dataclass

@dataclass(frozen=True)
class SourceStorageConfig:
    """Contrato estrito exclusivo para leitura de dados."""
    source_path: str
    source_format: str

@dataclass(frozen=True)
class OutputStorageConfig:
    """Contrato estrito exclusivo para persistência de dados."""
    output_path: str
    output_format: str