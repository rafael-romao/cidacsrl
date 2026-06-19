import argparse
import logging
import sys

from cidacsrl.config.loader import load_yaml
from cidacsrl.config.logging import configure_logging
from cidacsrl.bootstrap.linkage_bootstrap import build_linkage_use_case
from cidacsrl.bootstrap.indexing_bootstrap import build_indexing_use_case

configure_logging()
logger = logging.getLogger("CLI")

def main():
    parser = argparse.ArgumentParser(description="CIDACS-RL Engine - Interface de Linha de Comando")
    parser.add_argument("use_case", choices=["cleaning", "indexing", "linkage", "deduplication"])
    parser.add_argument("--env-config", required=True, help="YAML de configuracao do ambiente local/deploy")
    parser.add_argument("--spec-config", required=False, help="YAML com a especificacao do pipeline")

    args = parser.parse_args()
    logger.info(f"Pipeline de execução: {args.use_case}")

    env_data = load_yaml(args.env_config)
    es_data = env_data.get("elasticsearch", {})
    spark_data = env_data.get("spark", {})
    storage_data = env_data.get("storage", {})
    execution_data = env_data.get("execution", {})

    if args.use_case == "indexing":
        spec_path = args.spec_config or env_data.get("specification", {}).get("indexing_path")
        indexing_spec_data = load_yaml(spec_path)
        use_case, spec, spark = build_indexing_use_case(
            storage_config_data=storage_data,
            indexing_spec_data=indexing_spec_data,
            es_config_data=es_data,
            spark_config_data=spark_data,
        )
        try:
            use_case.execute(spec=spec)
            logger.info("Indexing Use Case executed successfully.")
        except Exception as e:
            logger.critical(f"Erro crítico na execução de indexing: {e}", exc_info=True)
            sys.exit(1)
        finally:
            spark.stop()

    elif args.use_case == "linkage":
        spec_path = args.spec_config or env_data.get("specification", {}).get("linkage_path")
        linkage_spec_data = load_yaml(spec_path)
        use_case, spec, enriched_config, spark = build_linkage_use_case(
            storage_config_data=storage_data,
            execution_config_data=execution_data,
            linkage_spec_data=linkage_spec_data,
            es_config_data=es_data,
            spark_config_data=spark_data,
        )
        try:
            use_case.execute(
                specification=spec,
                job_id=enriched_config.job_id,
                execution_config=enriched_config,
            )
            logger.info("Linkage Execution finished successfully.")
        except Exception as e:
            logger.critical(f"Erro crítico na execução de linkage: {e}", exc_info=True)
            sys.exit(1)
        finally:
            spark.stop()

    elif args.use_case in ["cleaning", "deduplication"]:
        logger.info(f"Pipeline {args.use_case} em desenvolvimento")

if __name__ == "__main__":
    main()
