import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession

from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage import RunSequentialLinkageUseCase
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import SequentialLinkageSpecification

def _build_real_spec() -> SequentialLinkageSpecification:
    return SequentialLinkageSpecification.from_dict(
        {
            "source_table": "nascimentos_integracao",
            "id_source_table": "source_id_cidadao",
            "target_es_index": "indice_nacional_integracao",
            "id_target_table": "candidate_id_es",
            "blocking_phases": [
                {
                    "phase_name": "phase_1_fase_teste_integrado",
                    "enabled": True,
                    "candidate_limit": 5,
                    "strong_match_score_threshold": 0.9,
                    "rules": [
                        {
                            "source_column": "nome",
                            "target_column": "nome",
                            "similarity": "exact",
                            "weight": 1.0,
                        }
                    ],
                },
                {
                    "phase_name": "phase_2_fase_relaxada",
                    "enabled": True,
                    "candidate_limit": 5,
                    "strong_match_score_threshold": 0.8,
                    "rules": [
                        {
                            "source_column": "nome",
                            "target_column": "nome",
                            "similarity": "jaro_winkler",
                            "weight": 1.0,
                        }
                    ],
                }
            ],
        }
    )

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("UnitTest-SequentialLinkage").getOrCreate()
    yield spark
    spark.stop()

def test_run_sequential_linkage_with_real_implementation(spark_session):
    spec_config = _build_real_spec()

    # Mocks das portas (Ports)
    ingestion_port = MagicMock()
    persistence_port = MagicMock()
    transformation_port = MagicMock()
    get_candidates_port = MagicMock()
    scoring_port = MagicMock()

    # Mocks dos DataFrames
    df_source = MagicMock(name="df_source")
    df_candidates = MagicMock(name="df_candidates")
    df_scored_pairs = MagicMock(name="df_scored_pairs")
    df_matches_to_exclude = MagicMock(name="df_matches_to_exclude")

    # Configuração dos retornos dos Mocks
    ingestion_port.read_source_data.return_value = df_source
    get_candidates_port.get_candidates.return_value = df_candidates
    df_candidates.withColumn.return_value = df_candidates
    scoring_port.calculate_score.return_value = df_scored_pairs
    df_scored_pairs.count.return_value = 1
    
    transformation_port.filter_matches_by_threshold.return_value = df_matches_to_exclude
    transformation_port.exclude_records.return_value = df_source

    use_case = RunSequentialLinkageUseCase(
        ingestion_port=ingestion_port,
        persistence_port=persistence_port,
        transformation_port=transformation_port,
        get_candidates_port=get_candidates_port,
        scoring_port=scoring_port,
    )

    result_df = use_case.execute(spec_config)

    # Verificações (Asserts)
    assert result_df is df_scored_pairs
    ingestion_port.read_source_data.assert_called_once_with(table_name="nascimentos_integracao")
    
    # O get_candidates e o scoring devem ser chamados 2 vezes (uma para cada fase)
    assert get_candidates_port.get_candidates.call_count == 2
    assert scoring_port.calculate_score.call_count == 2
    
    # Verifica se a persistência foi chamada para as duas fases
    assert persistence_port.write_data.call_count == 2

    # Verifica se a filtragem pelo threshold ocorreu em ambas as fases
    assert transformation_port.filter_matches_by_threshold.call_count == 2
    
    # Verifica se a exclusão de registros foi chamada na Fase 2 com o dataframe correto filtrado
    transformation_port.exclude_records.assert_called_once_with(
        primary_dataset=df_source,
        records_to_exclude=df_matches_to_exclude,
        join_key="source_id_cidadao"
    )