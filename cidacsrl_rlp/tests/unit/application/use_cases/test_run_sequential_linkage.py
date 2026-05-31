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

@pytest.fixture(scope="module")
def spark_integration():
    spark = SparkSession.builder \
        .appName("IntegrationTest-SequentialLinkage") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .config("spark.es.nodes.wan.only", "true") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_run_sequential_linkage_with_real_infrastructure(spark_integration, test_paths):
    """
    Testa a orquestração do Use Case consumindo configurações dinâmicas de 
    ambiente (env_local.yml) e injetando as classes adequadas de StorageConfig nos Adapters.
    """
    tests_root = test_paths["configs"].parent
    project_root = tests_root.parent

    # =========================================================================
    # 1. Carregar Configurações de Ambiente (env_local.yml) e Especificação (linkage_spec_local.yml)
    # =========================================================================
    env_yaml_path = tests_root / "e2e" / "configs" / "env_local.yml"
    if not env_yaml_path.exists():
        env_yaml_path = Path.cwd() / "cidacsrl_rlp" / "tests" / "e2e" / "configs" / "env_local.yml"
    assert env_yaml_path.exists(), f"Arquivo de ambiente não encontrado: {env_yaml_path}"

    spec_yaml_path = tests_root / "e2e" / "configs" / "linkage_spec_local.yml"
    if not spec_yaml_path.exists():
        spec_yaml_path = Path.cwd() / "cidacsrl_rlp" / "tests" / "e2e" / "configs" / "linkage_spec_local.yml"
    assert spec_yaml_path.exists(), f"Arquivo de especificação não encontrado: {spec_yaml_path}"

    # Usando os loaders para obter os modelos validados
    env_config = load_linkage_env_config(env_yaml_path)
    spec_config = load_sequential_linkage_specification(spec_yaml_path)
    es_config = load_es_config(env_config.es_config_path)

    # =========================================================================
    # 2. Instanciar Modelos de Configuração do Domínio (StorageConfigs)
    # =========================================================================
    source_config = SourceStorageConfig(
        source_data_path=env_config.source_data_path,
        source_data_format=env_config.source_data_format
    )
    output_config = OutputStorageConfig(
        output_data_path=env_config.output_data_path,
        output_data_format=env_config.output_data_format
    )

    # =========================================================================
    # 3. Instanciar Adaptadores Reais injetando os StorageConfigs
    # =========================================================================
    ingestion_port = SparkDataIngestionAdapter(
        spark_session=spark_integration, 
        config=source_config
    )
    persistence_port = SparkDataPersistenceAdapter(
        spark_session=spark_integration, 
        config=output_config
    )
    transformation_port = SparkDataTransformationAdapter()
    get_candidates_port = SparkESSearchAdapter(
        index_name=spec_config.target_es_index, 
        es_config=es_config
    )
    scoring_port = SparkScoringAdapter()

    use_case = RunSequentialLinkageUseCase(
        ingestion_port=ingestion_port,
        persistence_port=persistence_port,
        transformation_port=transformation_port,
        get_candidates_port=get_candidates_port,
        scoring_port=scoring_port
    )

    # =========================================================================
    # 4. Executar e Validar
    # =========================================================================
    try:
        result_df = use_case.execute(spec_config)
        total_links = result_df.count()
        assert result_df is not None, "O DataFrame retornado não deveria ser nulo."
        assert total_links >= 0, "O processamento finalizou com sucesso."

        # Validar as pastas físicas na pasta extraída da config
        for index, fase in enumerate(spec_config.blocking_phases):
            if fase.enabled:
                esperado_folder_name = f"phase_{index + 1}_{fase.phase_name}"
                fase_path = Path(env_config.output_data_path) / esperado_folder_name
                assert fase_path.exists(), (
                    f"Falha de Persistência: A pasta esperada '{esperado_folder_name}' "
                    f"não foi encontrada no disco em: {fase_path}"
                )
    except Exception as e:
        pytest.fail(f"O teste de integração falhou durante a execução: {e}")