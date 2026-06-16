import pytest
from unittest.mock import Mock, ANY

from core.cidacsrl.application.services.work_unit_orchestrator import WorkUnitOrchestrator
from core.cidacsrl.domain.models.tracking.work_unit import WorkUnitExecutionRecord, WorkUnitStatus
from core.cidacsrl.infra.configs.models.execution_config import ExecutionConfig, DataPartitioningConfig


def test_prepare_and_route_uses_configured_partitions_without_scanning_disk():
    # Setup de Mocks das Portas de Saída (Outbound Ports)
    mock_ingestion = Mock()
    mock_tracking = Mock()

    # Criação de uma configuração com partições fixas explícitas
    config = ExecutionConfig(
        job_id="job_fixed_partitions_test",
        partitioning=DataPartitioningConfig(
            partition_column="uf_internacao",
            filter_partitions=["BA", "SP"]
        ),
        audit_log_path="/tmp/audit"
    )

    # Simula registros pendentes sendo retornados pela máquina de estados do JSON
    mock_record_ba = WorkUnitExecutionRecord(unit_id="uf_internacao_BA", filters={"uf_internacao": "BA"})
    mock_record_sp = WorkUnitExecutionRecord(unit_id="uf_internacao_SP", filters={"uf_internacao": "SP"})
    mock_tracking.get_pending_work_units.return_value = [mock_record_ba, mock_record_sp]

    # Instanciação do componente sob teste
    orchestrator = WorkUnitOrchestrator(ingestion_port=mock_ingestion, tracking_port=mock_tracking)
    
    # Consome o gerador (work stream)
    payloads = list(orchestrator.prepare_and_route(table_name="internacao_example", execution_config=config))

    # Asserções de Fluxo e Isolamento
    assert len(payloads) == 2
    
    # Garante que o método de varredura/descoberta dinâmica NÃO foi chamado
    mock_ingestion.discover_partitions.assert_not_called()

    # Valida a inicialização síncrona do arquivo de estados em disco
    mock_tracking.initialize_job_state.assert_called_once_with(
        "job_fixed_partitions_test", 
        ANY  # Lista contendo os WorkUnitExecutionRecord mapeados
    )

    # Verifica se os status transicionais mudaram para PROCESSING imediatamente antes da leitura física
    assert mock_tracking.update_work_unit_status.call_count == 2
    mock_tracking.update_work_unit_status.assert_any_call(
        "job_fixed_partitions_test", "uf_internacao_BA", WorkUnitStatus.PROCESSING
    )
    mock_tracking.update_work_unit_status.assert_any_call(
        "job_fixed_partitions_test", "uf_internacao_SP", WorkUnitStatus.PROCESSING
    )

    # Verifica se o carregador físico do Spark (read_slice) foi acionado com os filtros limpos
    assert mock_ingestion.read_slice.call_count == 2
    mock_ingestion.read_slice.assert_any_call("internacao_example", {"uf_internacao": "BA"})
    mock_ingestion.read_slice.assert_any_call("internacao_example", {"uf_internacao": "SP"})


def test_prepare_and_route_triggers_dynamic_discovery_when_partitions_list_is_empty():
    mock_ingestion = Mock()
    mock_tracking = Mock()

    # Lista de partições vazia força a descoberta dinâmica por varredura
    config = ExecutionConfig(
        job_id="job_dynamic_discovery_test",
        partitioning=DataPartitioningConfig(
            partition_column="uf_internacao",
            filter_partitions=[]  # Vazio
        ),
        audit_log_path="/tmp/audit"
    )

    # Simula a infraestrutura Spark descobrindo os valores físicos no storage Parquet
    mock_ingestion.discover_partitions.return_value = ["BA", "SP", "RJ"]

    mock_record_ba = WorkUnitExecutionRecord(unit_id="uf_internacao_BA", filters={"uf_internacao": "BA"})
    mock_record_sp = WorkUnitExecutionRecord(unit_id="uf_internacao_SP", filters={"uf_internacao": "SP"})
    mock_record_rj = WorkUnitExecutionRecord(unit_id="uf_internacao_RJ", filters={"uf_internacao": "RJ"})
    mock_tracking.get_pending_work_units.return_value = [mock_record_ba, mock_record_sp, mock_record_rj]

    orchestrator = WorkUnitOrchestrator(ingestion_port=mock_ingestion, tracking_port=mock_tracking)
    
    payloads = list(orchestrator.prepare_and_route(table_name="internacao_example", execution_config=config))

    # Asserções de Descoberta Dinâmica
    assert len(payloads) == 3

    # Verifica se o orquestrador delegou com sucesso o scan para a porta de ingestão
    mock_ingestion.discover_partitions.assert_called_once_with("internacao_example", "uf_internacao")

    # Confirma se o estado síncrono em disco foi populado com as 3 partições localizadas
    called_records = mock_tracking.initialize_job_state.call_args[0][1]
    assert len(called_records) == 3
    assert called_records[0].unit_id == "uf_internacao_BA"
    assert called_records[1].unit_id == "uf_internacao_SP"
    assert called_records[2].unit_id == "uf_internacao_RJ"

    # Confirma que os fatiamentos físicos do Spark foram executados para todas as UFs descobertas
    assert mock_ingestion.read_slice.call_count == 3
    mock_ingestion.read_slice.assert_any_call("internacao_example", {"uf_internacao": "RJ"})