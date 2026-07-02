from abc import ABC, abstractmethod
from typing import List

from cidacsrl.domain.linkage.tracking.work_unit import (
    WorkUnitExecutionRecord,
    WorkUnitStatus,
)


class CheckpointPort(ABC):
    """Contrato para persistência e recuperação do estado de execução de work units."""

    @abstractmethod
    def initialize_job_state(self, job_id: str, work_units: List[WorkUnitExecutionRecord]) -> None:
        """Persiste o estado inicial de todas as work units de um job como PENDING.

        Args:
            job_id: Identificador único do job.
            work_units: Lista de registros de execução a serem inicializados.
        """
        pass

    @abstractmethod
    def get_pending_work_units(self, job_id: str) -> List[WorkUnitExecutionRecord]:
        """Retorna apenas as work units ainda não concluídas de um job.

        Args:
            job_id: Identificador único do job.

        Returns:
            Lista de work units com status PENDING ou PROCESSING.
        """
        pass

    @abstractmethod
    def get_all_work_units(self, job_id: str) -> List[WorkUnitExecutionRecord]:
        """Retorna todas as work units de um job, independente do status.

        Args:
            job_id: Identificador único do job.

        Returns:
            Lista completa de work units registradas para o job.
        """
        pass

    @abstractmethod
    def update_work_unit_status(
        self,
        job_id: str,
        unit_id: str,
        status: WorkUnitStatus,
        records_processed: int = 0,
        error_message: str = None,
    ) -> None:
        """Atualiza o status de uma work unit no checkpoint.

        Args:
            job_id: Identificador único do job.
            unit_id: Identificador da work unit a atualizar.
            status: Novo status (PROCESSING, COMPLETED ou FAILED).
            records_processed: Número de registros processados. Defaults to 0.
            error_message: Mensagem de erro, se status for FAILED. Defaults to None.
        """
        pass
