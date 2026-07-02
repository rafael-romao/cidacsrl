import pytest
from unittest.mock import MagicMock, ANY
from pyspark.sql import DataFrame

from cidacsrl.application.linkage.record_linkage_use_case import RecordLinkageUseCase
from cidacsrl.application.linkage.work_unit_orchestrator import WorkUnitOrchestrator
from cidacsrl.ports.linkage.data_persistence_port import DataPersistencePort
from cidacsrl.ports.linkage.data_transformation_port import DataTransformationPort
from cidacsrl.ports.linkage.get_candidates_port import GetCandidatesPort
from cidacsrl.ports.linkage.scoring_port import ScoringPort
from cidacsrl.ports.linkage.checkpoint_port import CheckpointPort
from cidacsrl.ports.linkage.telemetry_port import TelemetryPort
from cidacsrl.domain.linkage.tracking.work_unit import WorkUnitPayload, WorkUnitStatus
from cidacsrl.domain.linkage.linkage_specification import SequentialLinkageSpecification, BlockingPhaseContext


@pytest.fixture
def mock_dependencies():
    return {
        "orchestrator": MagicMock(spec=WorkUnitOrchestrator),
        "persistence": MagicMock(spec=DataPersistencePort),
        "transformation": MagicMock(),
        "get_candidates": MagicMock(spec=GetCandidatesPort),
        "scoring": MagicMock(spec=ScoringPort),
        "checkpoint": MagicMock(spec=CheckpointPort),
        "telemetry": MagicMock(spec=TelemetryPort),
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
    checkpoint = mock_dependencies["checkpoint"]
    telemetry = mock_dependencies["telemetry"]

    df_raw = MagicMock(spec=DataFrame)
    df_raw.count.return_value = 10
    df_candidates = MagicMock(spec=DataFrame)
    df_scored = MagicMock(spec=DataFrame)
    df_filtered = MagicMock(spec=DataFrame)
    df_marked = MagicMock(spec=DataFrame)
    df_remaining_after_exclude = MagicMock(spec=DataFrame)

    payload = WorkUnitPayload(unit_id="uf_BA", dataframe=df_raw)
    orchestrator.route.return_value = [payload]
    checkpoint.get_all_work_units.return_value = [MagicMock(status=WorkUnitStatus.PENDING)]

    get_candidates.get_candidates.return_value = df_candidates
    scoring.calculate_score.return_value = df_scored
    transformation.filter_matches_by_threshold.return_value = df_filtered
    transformation.add_phase_marker.return_value = df_marked
    transformation.exclude_records.return_value = df_remaining_after_exclude
    persistence.save_phase_output.return_value = 150

    execution_config_mock = MagicMock()
    execution_config_mock.partitioning.partition_column = "uf"

    use_case = RecordLinkageUseCase(
        orchestrator=orchestrator,
        persistence_port=persistence,
        transformation_port=transformation,
        get_candidates_port=get_candidates,
        scoring_port=scoring,
        checkpoint_port=checkpoint,
        telemetry_port=telemetry,
    )

    use_case.execute(
        specification=mock_specification,
        job_id="job_unit_test_001",
        execution_config=execution_config_mock
    )

    # 1. Orquestrador roteou com os parâmetros corretos
    orchestrator.route.assert_called_once_with(
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

    # 4. Persistência com particionamento Hive
    persistence.save_phase_output.assert_called_once_with(
        df=df_marked,
        project_name="test_linkage",
        phase_name="fase_teste_unitario",
        partition_column="uf",
    )

    # 5. Checkpoint de conclusão
    checkpoint.update_work_unit_status.assert_called_once_with(
        job_id="job_unit_test_001",
        unit_id="uf_BA",
        status=WorkUnitStatus.COMPLETED,
        records_processed=150
    )

    # 6. Telemetria emitida em todas as etapas esperadas
    telemetry.log_job_start.assert_called_once_with("job_unit_test_001", "test_linkage", ANY)
    telemetry.log_work_unit_start.assert_called_once_with("job_unit_test_001", "uf_BA", ANY)
    telemetry.log_phase_telemetry.assert_called_once_with(
        job_id="job_unit_test_001",
        unit_id="uf_BA",
        phase_index=1,
        phase_name="fase_teste_unitario",
        records_in=10,
        candidates_found=ANY,
        records_out=150,
        duration=ANY,
        search_duration=ANY,
        persist_duration=ANY,
    )
    telemetry.log_work_unit_completion.assert_called_once_with(
        job_id="job_unit_test_001",
        unit_id="uf_BA",
        total_links=150,
        remaining=ANY,
        duration=ANY,
    )
    telemetry.log_job_completion.assert_called_once_with("job_unit_test_001", ANY, ANY)


def test_execute_emits_failure_telemetry_and_reraises_on_exception(mock_dependencies, mock_specification):
    orchestrator = mock_dependencies["orchestrator"]
    persistence = mock_dependencies["persistence"]
    transformation = mock_dependencies["transformation"]
    get_candidates = mock_dependencies["get_candidates"]
    scoring = mock_dependencies["scoring"]
    checkpoint = mock_dependencies["checkpoint"]
    telemetry = mock_dependencies["telemetry"]

    df_raw = MagicMock(spec=DataFrame)
    df_raw.count.return_value = 10

    payload = WorkUnitPayload(unit_id="uf_RJ", dataframe=df_raw)
    orchestrator.route.return_value = [payload]
    checkpoint.get_all_work_units.return_value = [MagicMock(status=WorkUnitStatus.PENDING)]

    get_candidates.get_candidates.side_effect = RuntimeError("ES unreachable")

    execution_config_mock = MagicMock()
    execution_config_mock.partitioning.partition_column = None

    use_case = RecordLinkageUseCase(
        orchestrator=orchestrator,
        persistence_port=persistence,
        transformation_port=transformation,
        get_candidates_port=get_candidates,
        scoring_port=scoring,
        checkpoint_port=checkpoint,
        telemetry_port=telemetry,
    )

    with pytest.raises(RuntimeError, match="ES unreachable"):
        use_case.execute(
            specification=mock_specification,
            job_id="job_failure_test",
            execution_config=execution_config_mock
        )

    # Checkpoint marcado como FAILED com a mensagem de erro
    checkpoint.update_work_unit_status.assert_called_once_with(
        job_id="job_failure_test",
        unit_id="uf_RJ",
        status=WorkUnitStatus.FAILED,
        error_message="ES unreachable",
    )

    # Telemetria de falha emitida
    telemetry.log_work_unit_failure.assert_called_once_with(
        job_id="job_failure_test",
        unit_id="uf_RJ",
        error_message="ES unreachable",
        duration=ANY,
    )

    # Telemetria de conclusão NÃO deve ser emitida em caso de falha
    telemetry.log_work_unit_completion.assert_not_called()
