import argparse
import logging
import sys

from core.infra.configs.loader import load_yaml
from core.infra.bootstrappers.indexing_bootstrapper import bootstrap_elasticsearch_indexing
from core.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("CLI")

def main():
    parser = argparse.ArgumentParser(description="CIDACS-RL Engine - Interface de Linha de Comando")
    parser.add_argument("use_case", choices=["cleaning", "indexing", "linkage", "deduplication"])
    parser.add_argument("--env-config", required=True, help="YAML de configuracao do ambiente local/deploy")
    parser.add_argument("--spec-config", required=False, help="YAML com a especificacao do pipeline")
    
    args = parser.parse_args()
    logger.info(f"Pipeline de execução: {args.use_case}")

    # Configurações de ambiente e infraestrutura
    env_data = load_yaml(args.env_config)


    es_data = env_data.get("elasticsearch", {})
    spark_data = env_data.get("spark", {})
    storage_data = env_data.get("storage", {})
    execution_data = env_data.get("execution", {})

    if args.use_case == "indexing":
        spec_path = args.spec_config or env_data.get("specification", {}).get("indexing_path")
        indexing_spec_data = load_yaml(spec_path)
        bootstrap_elasticsearch_indexing(
            storage_config_data=storage_data,
            indexing_spec_data=indexing_spec_data,
            es_config_data=es_data,
            spark_config_data=spark_data
        )

    elif args.use_case == "linkage":
        spec_path = args.spec_config or env_data.get("specification", {}).get("linkage_path")
        linkage_spec_data = load_yaml(spec_path)
        bootstrap_sequential_linkage(
            storage_config_data=storage_data,
            execution_config_data=execution_data,
            linkage_spec_data=linkage_spec_data,
            es_config_data=es_data,
            spark_config_data=spark_data
        )
        
    elif args.use_case in ["cleaning", "deduplication"]:
        logger.info(f"Pipeline {args.use_case} em desenvolvimento")

if __name__ == "__main__":
    main()