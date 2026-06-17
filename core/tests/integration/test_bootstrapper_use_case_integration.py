import os
import pytest
from unittest.mock import patch, MagicMock
from core.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage

@pytest.fixture
def setup_integration_payloads(tmp_path):
    """Gera o cenário físico de arquivos YAML e diretórios de dados Parquet para o teste, retornando os dicionários."""
    configs_dir = tmp_path / "configs"
    data_input_dir = tmp_path / "data" / "input"
    data_output_dir = tmp_path / "data" / "output"
    
    os.makedirs(configs_dir, exist_ok=True)
    os.makedirs(data_input_dir, exist_ok=True)
    os.makedirs(data_output_dir, exist_ok=True)

    # Dicionário de Storage (Ambiente)
    storage_payload = {
        "source_path": str(data_input_dir),
        "output_path": str(data_output_dir),
        "source_format": "parquet",
        "output_format": "parquet"
    }

    # Dicionário da Especificação de Linkage
    linkage_spec_payload = {
        "source_table": "nascimentos_integracao",
        "target_es_index": "indice_nacional_integracao",
        "id_source_table": "source_id_cidadao",
        "id_target_table": "candidate_id_es",
        "blocking_phases": [
            {
                "phase_name": "phase_1_fase_teste_integrado",
                "enabled": True,
                "candidate_limit": 5,
                "strong_match_score_threshold": 0.90,
                "rules": [] 
            }
        ]
    }

    # Dicionários de Elasticsearch e Spark Local
    es_payload = {
        "host": "localhost",
        "port": 9200,
        "es_connection_url": "http://localhost:9200",
        "wan_only": True
    }
    
    spark_payload = {
        "spark.sql.shuffle.partitions": "1",
        "spark.ui.enabled": "false",
        "spark.master": "local[1]"
    }

    execution_payload = {
        "job_id": "job_test_integration",
        "audit_log_path": str(tmp_path / "audit")
    }

    # Preparar dados estáticos de origem
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("DataPrep").getOrCreate()
    dummy_data = spark.createDataFrame([("A1", "Teste")], ["source_id_cidadao", "nome"])
    dummy_data.write.parquet(os.path.join(data_input_dir, "nascimentos_integracao"))
    spark.stop()

    return {
        "storage_config_data": storage_payload,
        "execution_config_data": execution_payload,
        "linkage_spec_data": linkage_spec_payload,
        "es_config_data": es_payload,
        "spark_config_data": spark_payload,
        "output_dir": str(data_output_dir)
    }

def test_bootstrap_sequential_linkage_execution(setup_integration_payloads):
    payloads = setup_integration_payloads    
    
    with patch("core.infra.bootstrappers.linkage_bootstrapper.get_es_client", return_value=MagicMock()), \
         patch("core.infra.bootstrappers.linkage_bootstrapper.validate_elasticsearch_schema"), \
         patch("core.infra.bootstrappers.linkage_bootstrapper.RecordLinkageUseCase") as mock_uc_class, \
         patch("core.infra.adapters.outbound.spark_data_ingestion_adapter.SparkDataIngestionAdapter.check_health", return_value=[]):
        
        try:
            bootstrap_sequential_linkage(
                storage_config_data=payloads["storage_config_data"],
                execution_config_data=payloads["execution_config_data"],
                linkage_spec_data=payloads["linkage_spec_data"],
                es_config_data=payloads["es_config_data"],
                spark_config_data=payloads["spark_config_data"]
            )
        except Exception as e:
            pytest.fail(f"O bootstrapper falhou ao ser executado com os payloads segregados: {e}")

    mock_uc_class.return_value.execute.assert_called_once()
    call_kwargs = mock_uc_class.return_value.execute.call_args.kwargs
    assert call_kwargs["job_id"] == "job_test_integration"
    assert call_kwargs["specification"].source_table == "nascimentos_integracao"