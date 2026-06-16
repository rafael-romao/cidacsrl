import pytest
from unittest.mock import patch, Mock, ANY
from cidacsrl.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage
from cidacsrl.cidacsrl.domain.models.linkage_specification import SequentialLinkageSpecification

@pytest.fixture
def mock_storage_config_data():
    return {
        "source_path": "tests/data/input",
        "source_format": "parquet",
        "output_path": "tests/data/output",
        "output_format": "parquet"
    }

@pytest.fixture
def mock_execution_config_data():
    return {
        "job_id": "job_simulated_orchestration_123",
        "sample_fraction": 0.1,
        "sample_seed": 42,
        "audit_log_path": "tests/data/audit_logs",
        "partitioning": {
            "partition_column": "uf_internacao",
            "filter_partitions": ["BA", "SP"]
        }
    }

@pytest.fixture
def mock_linkage_spec_data():
    return {
        "source_table": "internacao_example",
        "target_es_index": "nascimentos_example_index",
        "id_source_table": "codigo_internacao",
        "id_target_table": "codigo_nascimento",
        "extra_target_fields": [
            "uf_nascimento",
            "municipio_nascimento"
        ],
        "blocking_phases": [
            {
                "phase_name": "fase_e2e_nome",
                "enabled": True,
                "candidate_limit": 100,
                "strong_match_score_threshold": 0.99,
                "rules": [
                    {
                        "source_column": "nome_completo",
                        "target_column": "nome_completo",
                        "es_clause_type": "must",
                        "similarity": "exact",
                        "weight": 0.5
                    },
                    {
                        "source_column": "nome_mae",
                        "target_column": "nome_mae",
                        "es_clause_type": "must",
                        "similarity": "exact",
                        "weight": 0.3
                    }
                ]
            }
        ]
    }

@pytest.fixture
def mock_es_config_data():
    return {
        "host": "localhost",
        "port": 9200,
        "es_connection_url": "http://localhost:9200",
        "wan_only": True,
        "search_strategy": "multisearch"
    }

@pytest.fixture
def mock_spark_config_data():
    return {
        "spark_configs": {
            "spark.master": "local[*]",
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false"
        }
    }

@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.validate_elasticsearch_schema")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.create_spark_session")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.JSONExecutionTrackingAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataIngestionAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataPersistenceAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataTransformationAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkESSearchAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkScoringAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.WorkUnitOrchestrator")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.RecordLinkageUseCase")
def test_bootstrap_sequential_linkage_wires_and_executes_from_simulated_content(
    mock_use_case_cls,
    mock_orchestrator_cls,
    mock_scoring_cls,
    mock_search_cls,
    mock_transformation_cls,
    mock_persistence_cls,
    mock_ingestion_cls,
    mock_tracking_cls,
    mock_create_spark,
    mock_validate_schema,
    mock_get_es_client,
    mock_storage_config_data,
    mock_execution_config_data,
    mock_linkage_spec_data,
    mock_es_config_data,
    mock_spark_config_data
):
    # Setup de mocks de infraestrutura básicos
    mock_spark = Mock()
    mock_create_spark.return_value = mock_spark

    mock_ingestion = Mock()
    mock_ingestion.check_health.return_value = []
    mock_ingestion_cls.return_value = mock_ingestion

    mock_use_case = Mock()
    mock_use_case_cls.return_value = mock_use_case

    # Disparo do inicializador central do pipeline
    bootstrap_sequential_linkage(
        storage_config_data=mock_storage_config_data,
        execution_config_data=mock_execution_config_data,
        linkage_spec_data=mock_linkage_spec_data,
        es_config_data=mock_es_config_data,
        spark_config_data=mock_spark_config_data
    )

    # 1. Validação de Schema do Elasticsearch acionada corretamente
    mock_validate_schema.assert_called_once()
    assert mock_validate_schema.call_args[1]["index_name"] == "nascimentos_example_index"
    assert "nome_completo" in mock_validate_schema.call_args[1]["required_columns"]

    # 2. Inicialização correta dos adaptadores primitivos de I/O
    mock_tracking_cls.assert_called_once_with(tracking_directory="tests/data/audit_logs")
    mock_ingestion_cls.assert_called_once_with(
        spark_session=mock_spark,
        storage_config=ANY
    )
    mock_persistence_cls.assert_called_once_with(
                output_config=ANY
            )
    
    mock_use_case_cls.assert_called_once_with(
            orchestrator=mock_orchestrator_cls.return_value,
            persistence_port=mock_persistence_cls.return_value,
            transformation_port=mock_transformation_cls.return_value,
            get_candidates_port=mock_search_cls.return_value,
            scoring_port=mock_scoring_cls.return_value,
            tracking_port=mock_tracking_cls.return_value
        )

    
    mock_use_case.execute.assert_called_once_with(
            specification=ANY,
            job_id=mock_execution_config_data["job_id"],
            execution_config=ANY
        )

    # 3. Validação do Orquestrador de Aplicação (Recebe portas primitivas)
    mock_orchestrator_cls.assert_called_once_with(
        ingestion_port=mock_ingestion,
        tracking_port=mock_tracking_cls.return_value
    )

    # 4. Validação da montagem do Caso de Uso Principal (Recebe o Orquestrador)
    mock_use_case_cls.assert_called_once_with(
        orchestrator=mock_orchestrator_cls.return_value,
        persistence_port=mock_persistence_cls.return_value,
        transformation_port=mock_transformation_cls.return_value,
        get_candidates_port=mock_search_cls.return_value,
        scoring_port=mock_scoring_cls.return_value,
        tracking_port=mock_tracking_cls.return_value
    )

    # 5. Execução do Caso de Uso acionada passando a especificação tipada do domínio
    mock_use_case.execute.assert_called_once()
    call_kwargs = mock_use_case.execute.call_args[1]
    assert call_kwargs["job_id"] == "job_simulated_orchestration_123"
    assert isinstance(call_kwargs["specification"], SequentialLinkageSpecification)
    assert call_kwargs["execution_config"].job_id == "job_simulated_orchestration_123"

    # 6. Ciclo de vida encerrado fechando a SparkSession de forma limpa
    mock_spark.stop.assert_called_once()