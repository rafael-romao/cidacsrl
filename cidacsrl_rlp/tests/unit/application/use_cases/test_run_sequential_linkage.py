import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock
from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage import RunSequentialLinkageUseCase

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("SparkScoringAdapterTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_use_case_orchestration_loop(spark):
    # 1. DataFrames Falsos
    df_source_mock = spark.createDataFrame([{"id": 1, "nome": "Joao"}])
    df_candidates_mock = spark.createDataFrame([{"source_record": {"nome": "Joao"}, "candidate_record": {"nome": "Joao"}}])
    df_scored_mock = spark.createDataFrame([{"match_score": 1.0, "source_nome": "Joao"}])

    # 2. Mock dos Adaptadores (Ports)
    mock_ingestion = MagicMock()
    mock_ingestion.read_source_data.return_value = df_source_mock

    mock_persistence = MagicMock()
    mock_transformation = MagicMock()

    mock_search = MagicMock()
    mock_search.get_candidates.return_value = df_candidates_mock

    mock_scoring = MagicMock()
    mock_scoring.calculate_score.return_value = df_scored_mock

    # 3. Mock da Configuração (Workflow)
    mock_config = MagicMock()
    mock_config.source_table = "source_table"
    mock_config.id_source_table = "id"
    mock_phase_1 = MagicMock(enabled=True, phase_name="Fase1")
    mock_phase_2 = MagicMock(enabled=False, phase_name="Fase2") # Fase desabilitada para testar o fluxo de controle
    mock_config.build_blocking_phase_contexts.return_value = [mock_phase_1, mock_phase_2]

    # 4. Executa o Use Case
    use_case = RunSequentialLinkageUseCase(
        ingestion_port=mock_ingestion,
        persistence_port=mock_persistence,
        transformation_port=mock_transformation,
        get_candidates_port=mock_search,
        scoring_port=mock_scoring
    )
    final_df = use_case.execute(mock_config)

    # 5. Validações da Orquestração
    mock_ingestion.read_source_data.assert_called_once_with(table_name="source_table")
    mock_search.get_candidates.assert_called_once_with(df_source_mock, mock_phase_1)
    mock_scoring.calculate_score.assert_called_once_with(df_candidates_mock, mock_phase_1)
    mock_persistence.write_data.assert_called_once()
    assert final_df == df_scored_mock