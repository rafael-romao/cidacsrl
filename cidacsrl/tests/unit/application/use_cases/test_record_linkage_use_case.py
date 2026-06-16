import pytest
from unittest.mock import MagicMock, ANY
from pyspark.sql import DataFrame

from cidacsrl.cidacsrl.application.use_cases.record_linkage_use_case import RecordLinkageUseCase
from cidacsrl.cidacsrl.application.services.work_unit_orchestrator import WorkUnitOrchestrator
from cidacsrl.cidacsrl.application.ports.outbound.data_persistence_port import DataPersistencePort
from cidacsrl.cidacsrl.application.ports.outbound.data_transformation_port import DataTransformationPort
from cidacsrl.cidacsrl.application.ports.outbound.get_candidates_port import GetCandidatesPort
from cidacsrl.cidacsrl.application.ports.outbound.scoring_port import ScoringPort
from cidacsrl.cidacsrl.application.ports.outbound.execution_tracking_port import ExecutionTrackingPort
from cidacsrl.cidacsrl.domain.models.linkage_specification import SequentialLinkageSpecification, BlockingPhaseContext
from cidacsrl.cidacsrl.domain.models.tracking.work_unit import WorkUnitPayload, WorkUnitStatus


@pytest.fixture
def mock_dependencies():
    return {
        "orchestrator": MagicMock(spec=WorkUnitOrchestrator),
        "persistence": MagicMock(spec=DataPersistencePort),
        "transformation": MagicMock(spec=DataTransformationPort),
        "get_candidates": MagicMock(spec=GetCandidatesPort),
        "scoring": MagicMock(spec=ScoringPort),
        "tracking": MagicMock(spec=ExecutionTrackingPort),
    }


@pytest.fixture
def mock_specification():
    spec = MagicMock(spec=SequentialLinkageSpecification)
    spec.source_table = "internacao_table"
    
    # Cria uma fase de bloqueio simulada e habilitada
    phase = MagicMock(spec=BlockingPhaseContext)
    phase.enabled = True
    phase.phase_name = "fase_teste_unitario"
    phase.strong_match_score_threshold = 0.92
    phase.rules = ["regra_1"]
    
    spec.blocking_phases = [phase]
    return spec


def test_execute_processes_all_work_units_successfully(mock_dependencies, mock_specification):
    orchestrator = mock_dependencies["orchestrator"]
    persistence = mock_dependencies["persistence"]
    transformation = mock_dependencies["transformation"]
    get_candidates = mock_dependencies["get_candidates"]
    scoring = mock_dependencies["scoring"]
    tracking = mock_dependencies["tracking"]

    # DataFrames simulados que trafegam entre os componentes de infraestrutura
    df_raw = MagicMock(spec=DataFrame)
    df_candidates = MagicMock(spec=DataFrame)
    df_scored = MagicMock(spec=DataFrame)
    df_filtered = MagicMock(spec=DataFrame)
    df_marked = MagicMock(spec=DataFrame)

    # Configuração dos retornos encadeados dos mocks de portas e serviços
    payload = WorkUnitPayload(unit_id="uf_BA", dataframe=df_raw)
    orchestrator.prepare_and_route.return_value = [payload]  # Simula stream com 1 unidade
    
    get_candidates.get_candidates.return_value = df_candidates
    scoring.calculate_score.return_value = df_scored
    transformation.filter_matches_by_threshold.return_value = df_filtered
    transformation.add_phase_marker.return_value = df_marked
    persistence.save_linkage_output.return_value = 150  # 150 pares salvos

    # Instanciação do Caso de Uso aplicando a nova assinatura baseada em Orchestrator
    use_case = RecordLinkageUseCase(
        orchestrator=orchestrator,
        persistence_port=persistence,
        transformation_port=transformation,
        get_candidates_port=get_candidates,
        scoring_port=scoring,
        tracking_port=tracking
    )

    execution_config_mock = MagicMock()
    use_case.execute(
        specification=mock_specification,
        job_id="job_unit_test_001",
        execution_config=execution_config_mock
    )

    # 1. Valida que o orquestrador planejou e roteou os dados
    orchestrator.prepare_and_route.assert_called_once_with(
        table_name="internacao_table",
        execution_config=execution_config_mock
    )

    # 2. Valida o fluxo interno do loop da fase de bloqueio ativa
    phase_mock = mock_specification.blocking_phases[0]
    get_candidates.get_candidates.assert_called_once_with(df_raw, phase_mock)
    scoring.calculate_score.assert_called_once_with(df_candidates, phase_mock)
    
    # 3. Valida que a filtragem por nota de corte (threshold) aconteceu no Transformation Adapter
    transformation.filter_matches_by_threshold.assert_called_once_with(
        dataset=df_scored,
        threshold=0.92
    )
    transformation.add_phase_marker.assert_called_once_with(df_filtered, "fase_teste_unitario")

    # 4. Valida a persistência final e o avanço síncrono da máquina de estados do tracking
    persistence.save_linkage_output.assert_called_once_with(
        phase_outputs=[df_marked],
        unit_id="uf_BA"
    )
    tracking.update_work_unit_status.assert_called_once_with(
        job_id="job_unit_test_001",
        unit_id="uf_BA",
        status=WorkUnitStatus.COMPLETED,
        records_processed=150
    )


def test_execute_updates_status_to_failed_when_exception_occurs(mock_dependencies, mock_specification):
    orchestrator = mock_dependencies["orchestrator"]
    tracking = mock_dependencies["tracking"]

    # Simula o orquestrador arremessando um erro crítico de I/O em tempo de execução
    orchestrator.prepare_and_route.side_effect = RuntimeError("Erro simulado de leitura no cluster distributed storage")

    use_case = RecordLinkageUseCase(
        orchestrator=orchestrator,
        persistence_port=mock_dependencies["persistence"],
        transformation_port=mock_dependencies["transformation"],
        get_candidates_port=mock_dependencies["get_candidates"],
        scoring_port=mock_dependencies["scoring"],
        tracking_port=tracking
    )

    # O erro deve ser propagado para cima para que o CLI/Bootstrapper tome providências
    with pytest.raises(RuntimeError) as exc_info:
        use_case.execute(
            specification=mock_specification,
            job_id="job_unit_test_failed",
            execution_config=MagicMock()
        )

    assert "Erro simulado de leitura" in str(exc_info.value)
    
    mock_dependencies["persistence"].save_linkage_output.assert_not_called()