import logging
from typing import Any

from cidacsrl_rlp.cidacsrl.infra.configs.loader import (
    load_linkage_env_config,
    load_es_config,
    load_dataset_indexing_specification,
)
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_repository_adapter import SparkDataRepositoryAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter import SparkESIndexingAdapter
from cidacsrl_rlp.cidacsrl.application.use_cases.index_dataset_use_case import IndexDatasetUseCase

logger = logging.getLogger(__name__)

def bootstrap_elasticsearch_indexing(config_path: str, indexing_spec_path: str, spark_session: Any) -> None:
    """
    Ponto de entrada isolado para orquestrar a fiação de dependências
    e disparar o Caso de Uso de indexação em massa (Bulk) no Elasticsearch.
    """
    logger.info("Initializing Hexagonal Indexing Bootstrapper...")
    
    # 1. Carrega as configurações utilizando os loaders resolvidos do loader.py
    env_config = load_linkage_env_config(config_path)
    es_config = load_es_config(env_config.es_config_path)
    indexing_spec = load_dataset_indexing_specification(indexing_spec_path)

    # 2. Instancia os adaptadores de infraestrutura outbound
    spark_adapter = SparkDataRepositoryAdapter(spark_session=spark_session, env_config=env_config)
    indexing_adapter = SparkESIndexingAdapter(es_config=es_config)

    # 3. Injeta as portas no Caso de Uso de Domínio
    use_case = IndexDatasetUseCase(
        ingestion_port=spark_adapter, 
        indexing_port=indexing_adapter
    )
    
    # 4. Executa o pipeline delegando os parâmetros da especificação abstrata
    use_case.execute(
        source_table=indexing_spec.index_config.name,
        spec=indexing_spec,
        id_field="codigo_nascimento"
    )
    
    logger.info("Elasticsearch indexing pipeline completed successfully.")