from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame


class WorkUnitStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass(frozen=True)
class WorkUnitMetadata:
    """Representação lógica de uma fatia de dados planejada no Domínio."""
    unit_id: str
    filters: Dict[str, Any]

@dataclass(frozen=True)
class WorkUnitExecutionRecord:
    """Representa o registro real de execução de uma Work Unit dentro do arquivo de Tracking."""
    unit_id: str
    filters: Dict[str, Any] = field(default_factory=dict)
    status: WorkUnitStatus = WorkUnitStatus.PENDING
    records_processed: int = 0
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serializa o registro para gravação direta no arquivo JSON."""
        return {
            "filters": self.filters,
            "status": self.status.value,
            "records_processed": self.records_processed,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error_message": self.error_message
        }

    @classmethod
    def from_dict(cls, unit_id: str, data: Dict[str, Any]) -> "WorkUnitExecutionRecord":
        """Reconstrói o registro a partir de um bloco lido do JSON de auditoria."""
        return cls(
            unit_id=unit_id,
            filters=data["filters"],
            status=WorkUnitStatus(data["status"]),
            records_processed=data.get("records_processed", 0),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            error_message=data.get("error_message")
        )

@dataclass(frozen=True)
class WorkUnitPayload:
    """Contêiner que transporta a fatia física de dados (DataFrame) acoplada ao ID."""
    unit_id: str
    dataframe: DataFrame