import logging
from typing import Iterable, List

from cidacsrl.config.models.execution_config import ExecutionConfig
from cidacsrl.domain.linkage.tracking.work_unit import (
    WorkUnitExecutionRecord,
    WorkUnitPayload,
    WorkUnitStatus,
)
from cidacsrl.domain.linkage.tracking.work_unit_factory import WorkUnitFactory
from cidacsrl.ports.linkage.checkpoint_port import CheckpointPort
from cidacsrl.ports.linkage.data_ingestion_port import DataIngestionPort

logger = logging.getLogger("Service: Orchestrator")

class WorkUnitOrchestrator:
    """
    Orquestrador de Aplicação encarregado de isolar o controle de fluxo,
    descoberta de partições e o gerenciamento de estados transicionais do Job.
    """
    def __init__(self, ingestion_port: DataIngestionPort, checkpoint_port: CheckpointPort):
        self.ingestion = ingestion_port
        self.checkpoint = checkpoint_port

    def prepare(self, table_name: str, execution_config: ExecutionConfig) -> ExecutionConfig:
        """
        Resolve o escopo de partições — via descoberta dinâmica se filter_partitions estiver
        vazio — e devolve um ExecutionConfig enriquecido com os valores concretos.
        """
        partition_col = execution_config.partitioning.partition_column
        partitions = list(execution_config.partitioning.filter_partitions)
        job_id = execution_config.job_id

        if partition_col and not partitions:
            logger.info(
                f"[{job_id}] 'filter_partitions' vazio. "
                f"Iniciando varredura automatizada na coluna '{partition_col}' via Ingestão..."
            )
            partitions = self.ingestion.discover_partitions(table_name, partition_col)
            logger.info(f"[{job_id}] Partições físicas detectadas em disco: {partitions}")

        return execution_config.with_discovered_partitions(partitions)

    def prepare_and_route(self, table_name: str, execution_config: ExecutionConfig) -> Iterable[WorkUnitPayload]:
        enriched_config = self.prepare(table_name, execution_config)
        return self.route(table_name, enriched_config)

    def route(self, table_name: str, execution_config: ExecutionConfig) -> Iterable[WorkUnitPayload]:
        """
        Transforma o escopo lógico em fatias físicas de dados e as despacha
        preguiçosamente para processamento. A inicialização do checkpoint é executada
        de forma eager antes de retornar o stream, garantindo visibilidade imediata do estado.
        """
        partition_col = execution_config.partitioning.partition_column
        partitions = list(execution_config.partitioning.filter_partitions)
        job_id = execution_config.job_id

        records_to_init = WorkUnitFactory.create_execution_scope(partition_col, partitions)

        self.checkpoint.initialize_job_state(job_id, records_to_init)

        pending_records = self.checkpoint.get_pending_work_units(job_id)
        logger.info(f"[{job_id}] Unidades para processamento: {len(pending_records)}")

        return self._stream(job_id, table_name, pending_records)

    def _stream(self, job_id: str, table_name: str, pending_records: List[WorkUnitExecutionRecord]) -> Iterable[WorkUnitPayload]:
        for record in pending_records:
            self.checkpoint.update_work_unit_status(job_id, record.unit_id, WorkUnitStatus.PROCESSING)

            try:
                dataframe_slice = self.ingestion.read_slice(table_name, record.filters)
                yield WorkUnitPayload(unit_id=record.unit_id, dataframe=dataframe_slice)
            except Exception as e:
                logger.error(f"[{job_id}] Erro crítico ao montar os dados da unidade '{record.unit_id}': {e}")
                self.checkpoint.update_work_unit_status(
                    job_id=job_id,
                    unit_id=record.unit_id,
                    status=WorkUnitStatus.FAILED,
                    error_message=str(e)
                )
                raise e
