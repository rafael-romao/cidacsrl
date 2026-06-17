import pytest
from unittest.mock import MagicMock, ANY
from pyspark.sql import DataFrame

from core.application.use_cases.record_linkage_use_case import RecordLinkageUseCase
from core.application.services.work_unit_orchestrator import WorkUnitOrchestrator
from core.application.ports.outbound.data_persistence_port import DataPersistencePort
from core.application.ports.outbound.data_transformation_port import DataTransformationPort
from core.application.ports.outbound.get_candidates_port import GetCandidatesPort
from core.application.ports.outbound.scoring_port import ScoringPort
from core.domain.models.tracking.work_unit import WorkUnitPayload, WorkUnitStatus
from core.application.ports.outbound.execution_tracking_port import ExecutionTrackingPort
from core.domain.models.linkage_specification import SequentialLinkageSpecification, BlockingPhaseContext


@pytest.fixture
def mock_dependencies():
    return {
        "orchestrator": MagicMock(spec=WorkUnitOrchestrator),
        "persistence": MagicMock(spec=DataPersistencePort),
        "transformation": MagicMock(),  # sem spec: port não declara exclude_records ainda
        "get_candidates": MagicMock(spec=GetCandidatesPort),
        "scoring": MagicMock(spec=ScoringPort),
        "tracking": MagicMock(spec=ExecutionTrackingPort),
    }


@pytest.fixture
def mock_specification():
    spec = MagicMock(spec=SequentialLinkageSpecification)
    spec.source_table = "internacao_table"
    spec.target_es_index = "nascimentos_index"
    spec.linkage_project_name = "test_linkage"
    spec.id_source_table = "codigo_internacao"

    phase = MagicMock(spec=BlockingPhaseContext)
    phase.enabled = True
    phase.phase_name = "fase_teste_unitario"
    phase.strong_match_score_threshold = 0.92
    phase.rules = ["regra_1"]

    spec.build_blocking_phase_contexts.return_value = [phase]
    return spec


def test_execute_processes_all_work_units_successfully(mock_dependencies, mock_specification):
    orchestrator = mock_dependencies["orchestrator"]
    persistence = mock_dependencies["persistence"]
    transformation = mock_dependencies["transformation"]
    get_candidates = mock_dependencies["get_candidates"]
    scoring = mock_dependencies["scoring"]
    tracking = mock_dependencies["tracking"]

    df_raw = MagicMock(spec=DataFrame)
    df_raw.count.return_value = 10
    df_candidates = MagicMock(spec=DataFrame)
    df_scored = MagicMock(spec=DataFrame)
    df_filtered = MagicMock(spec=DataFrame)
    df_marked = MagicMock(spec=DataFrame)
    df_remaining_after_exclude = MagicMock(spec=DataFrame)

    payload = WorkUnitPayload(unit_id="uf_BA", dataframe=df_raw)
    orchestrator.prepare_and_route.return_value = [payload]
    tracking.get_all_work_units.return_value = [MagicMock(status=WorkUnitStatus.PENDING)]

    get_candidates.get_candidates.return_value = df_candidates
    scoring.calculate_score.return_value = df_scored
    transformation.filter_matches_by_threshold.return_value = df_filtered
    transformation.add_phase_marker.return_value = df_marked
    transformation.exclude_records.return_value = df_remaining_after_exclude
    persistence.save_phase_output.return_value = 150

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

    # 1. Orquestrador roteou com os parâmetros corretos
    orchestrator.prepare_and_route.assert_called_once_with(
        table_name="internacao_table",
        execution_config=execution_config_mock
    )

    # 2. Fluxo da fase de bloqueio ativa
    phase_mock = mock_specification.build_blocking_phase_contexts.return_value[0]
    get_candidates.get_candidates.assert_called_once_with(df_raw, phase_mock)
    scoring.calculate_score.assert_called_once_with(df_candidates, phase_mock)

    # 3. Filtragem por threshold e marcação de fase
    transformation.filter_matches_by_threshold.assert_called_once_with(
        dataset=df_scored,
        threshold=0.92
    )
    transformation.add_phase_marker.assert_called_once_with(df_filtered, "fase_teste_unitario")

    # 4. Persistência com nova assinatura
    persistence.save_phase_output.assert_called_once_with(
        df=df_marked,
        project_name="test_linkage",
        job_id="job_unit_test_001",
        unit_id="uf_BA",
        phase_name="fase_teste_unitario"
    )

    # 5. Tracking final
    tracking.update_work_unit_status.assert_called_once_with(
        job_id="job_unit_test_001",
        unit_id="uf_BA",
        status=WorkUnitStatus.COMPLETED,
        records_processed=150
    )
