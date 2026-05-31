# cli.py
import argparse
import logging
import sys

from cidacsrl_rlp.cidacsrl.infra.configs.loader import load_yaml
from cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper import bootstrap_elasticsearch_indexing
from cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("cidacsrl_cli")

def main():
    parser = argparse.ArgumentParser(description="CIDACS-RL Engine - Interface de Linha de Comando")
    parser.add_argument("use_case", choices=["cleaning", "indexing", "linkage", "deduplication"])
    parser.add_argument("--env-config", required=True, help="YAML de configuracao do ambiente local/deploy")
    parser.add_argument("--spec-config", required=False, help="YAML com a especificacao abstrata de dominio do Caso de Uso")
    
    args = parser.parse_args()
    logger.info(f"Iniciando o Inbound Adapter CLI para o Caso de Uso: {args.use_case}")

    
    env_data = load_yaml(args.env_config)    
    es_data = load_yaml(env_data.get("es_config_path"))
    spark_data = load_yaml(env_data.get("spark_config_path"))

    if args.use_case == "indexing":
        indexing_spec_data = load_yaml(args.spec_config)        
        bootstrap_elasticsearch_indexing(
            storage_config_data=env_data,
            indexing_spec_data=indexing_spec_data,
            es_config_data=es_data,
            spark_config_data=spark_data
        )

    elif args.use_case == "linkage":
        linkage_spec_data = load_yaml(args.spec_config)        
        bootstrap_sequential_linkage(
            storage_config_data=env_data,
            linkage_spec_data=linkage_spec_data,
            es_config_data=es_data,
            spark_config_data=spark_data
        )
        
    elif args.use_case in ["cleaning", "deduplication"]:
        logger.info(f"O Caso de Uso {args.use_case} está mapeado, mas aguarda a fiação do bootstrapper correspondente.")

if __name__ == "__main__":
    main()