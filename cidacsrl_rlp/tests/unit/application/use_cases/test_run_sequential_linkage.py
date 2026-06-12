import pytest
from pathlib import Path
from pyspark.sql import SparkSession

# Importação do Caso de Uso
from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage import RunSequentialLinkageUseCase

# Importação dos Adaptadores Reais
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter

# Importação dos loaders de configuração
from cidacsrl_rlp.cidacsrl.infra.configs.loader import (
    load_linkage_env_config,
    load_sequential_linkage_specification,
    load_es_config,
)
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import SourceStorageConfig, OutputStorageConfig
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

    ingestion_port = MagicMock()
    persistence_port = MagicMock()
    transformation_port = MagicMock()
    get_candidates_port = MagicMock()
    scoring_port = MagicMock()

    df_source = MagicMock(name="df_source")
    df_candidates = MagicMock(name="df_candidates")
    df_scored_pairs = MagicMock(name="df_scored_pairs")

    ingestion_port.read_source_data.return_value = df_source
    get_candidates_port.get_candidates.return_value = df_candidates
    df_candidates.withColumn.return_value = df_candidates
    scoring_port.calculate_score.return_value = df_scored_pairs
    df_scored_pairs.count.return_value = 1

    use_case = RunSequentialLinkageUseCase(
        ingestion_port=ingestion_port,
        persistence_port=persistence_port,
        transformation_port=transformation_port,
        get_candidates_port=get_candidates_port,
        scoring_port=scoring_port,
    )

    result_df = use_case.execute(spec_config)

    assert result_df is df_scored_pairs
    ingestion_port.read_source_data.assert_called_once_with(table_name="nascimentos_integracao")
    get_candidates_port.get_candidates.assert_called_once()
    scoring_port.calculate_score.assert_called_once_with(df_candidates, spec_config.build_blocking_phase_contexts()[0])
    persistence_port.write_data.assert_called_once_with(
        data=df_scored_pairs,
        output_folder="phase_1_phase_1_fase_teste_integrado",
    )