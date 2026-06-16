import logging
from typing import Iterable, List

from core.application.ports.outbound.data_ingestion_port import DataIngestionPort
from core.application.ports.outbound.execution_tracking_port import ExecutionTrackingPort
from core.domain.models.tracking.work_unit_factory import WorkUnitFactory
from core.domain.models.tracking.work_unit import WorkUnitExecutionRecord, WorkUnitPayload, WorkUnitStatus
from core.infra.configs.models.execution_config import ExecutionConfig

logger = logging.getLogger("Service: Orchestrator")

class WorkUnitOrchestrator:
    """
    Orquestrador de Aplicação encarregado de isolar o controle de fluxo,
    descoberta de partições e o gerenciamento de estados transicionais do Job.
    """
    def __init__(self, ingestion_port: DataIngestionPort, tracking_port: ExecutionTrackingPort):
        self.ingestion = ingestion_port
        self.tracking = tracking_port

    def prepare_and_route(self, table_name: str, execution_config: ExecutionConfig) -> Iterable[WorkUnitPayload]:
        """
        Orquestra de ponta a ponta a preparação do escopo lógico e despacha 
        preguiçosamente as fatias físicas de dados prontas para processamento.
        """
        partition_col = execution_config.partitioning.partition_column
        partitions = list(execution_config.partitioning.filter_partitions)
        job_id = execution_config.job_id

        # 1. Caso o usuário queira quebrar por coluna mas omitiu os valores, ativa Descoberta Dinâmica
        if partition_col and not partitions:
            logger.info(
                f"[{job_id}] 'filter_partitions' vazio. "
                f"Iniciando varredura automatizada na coluna '{partition_col}' via Ingestão..."
            )
            partitions = self.ingestion.discover_partitions(table_name, partition_col)
            logger.info(f"[{job_id}] Partições físicas detectadas em disco: {partitions}")

        # 2. Invoca a Fábrica de Domínio Puro para planejar o escopo lógico (WorkUnitMetadata)
        logical_scope = WorkUnitFactory.create_execution_scope(partition_col, partitions)

        # 3. Transforma o escopo lógico em registros persistíveis de auditoria (WorkUnitExecutionRecord)
        records_to_init: List[WorkUnitExecutionRecord] = [
            WorkUnitExecutionRecord(unit_id=metadata.unit_id, filters=metadata.filters)
            for metadata in logical_scope
        ]

        # 4. Inicializa o arquivo JSON de auditoria (Preserva o arquivo caso seja um Restart)
        self.tracking.initialize_job_state(job_id, records_to_init)

        # 5. Recupera as fatias pendentes e as consome sob demanda (Lazy Loading)
        pending_records = self.tracking.get_pending_work_units(job_id)
        logger.info(f"[{job_id}] Fatias pendentes de processamento: {len(pending_records)}")

        for record in pending_records:
            # Altera transicionalmente para PROCESSING imediatamente antes de ler os dados
            self.tracking.update_work_unit_status(job_id, record.unit_id, WorkUnitStatus.PROCESSING)
            
            try:
                # Carrega o DataFrame fisicamente filtrado pelo Spark
                dataframe_slice = self.ingestion.read_slice(table_name, record.filters)
                
                yield WorkUnitPayload(
                    unit_id=record.unit_id,
                    dataframe=dataframe_slice
                )
            except Exception as e:
                logger.error(f"[{job_id}] Erro crítico ao montar os dados da unidade '{record.unit_id}': {e}")
                self.tracking.update_work_unit_status(
                    job_id=job_id,
                    unit_id=record.unit_id,
                    status=WorkUnitStatus.FAILED,
                    error_message=str(e)
                )
                raise e