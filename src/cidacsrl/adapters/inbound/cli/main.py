import argparse
import logging
import sys

from cidacsrl.bootstrap.deduplication_bootstrap import (
    build_deduplication_use_case,
)
from cidacsrl.bootstrap.indexing_bootstrap import build_indexing_use_case
from cidacsrl.bootstrap.linkage_bootstrap import build_linkage_use_case
from cidacsrl.config.dedup_loader import load_deduplicate_workflow_config
from cidacsrl.config.loader import load_yaml
from cidacsrl.config.logging import configure_logging

logger = logging.getLogger("cidacsrl.cli")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cidacsrl",
        description="CIDACS-RL Record Linkage Engine — Interface de Linha de Comando",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Nível de logging (padrão: INFO).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    linkage = subparsers.add_parser("linkage", help="Executa o pipeline de record linkage.")
    linkage.add_argument("--env-config", required=True, help="YAML de configuração do ambiente.")
    linkage.add_argument("--spec-config", help="YAML com a especificação do linkage.")

    indexing = subparsers.add_parser("indexing", help="Executa a indexação de datasets no Elasticsearch.")
    indexing.add_argument("--env-config", required=True, help="YAML de configuração do ambiente.")
    indexing.add_argument("--spec-config", help="YAML com a especificação do dataset.")

    dedup = subparsers.add_parser("deduplication", help="Executa o workflow de deduplicação.")
    dedup.add_argument("--config-path", required=True, metavar="PATH", help="YAML de configuração do workflow.")

    return parser


def main() -> None:
    configure_logging()
    parser = _build_parser()
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    logger.info(f"Iniciando pipeline: {args.command}")

    if args.command == "linkage":
        env_data = load_yaml(args.env_config)
        spec_path = args.spec_config or env_data.get("specification", {}).get("linkage_path")
        linkage_spec_data = load_yaml(spec_path)

        use_case, spec, enriched_config, spark = build_linkage_use_case(
            storage_config_data=env_data.get("storage", {}),
            execution_config_data=env_data.get("execution", {}),
            linkage_spec_data=linkage_spec_data,
            es_config_data=env_data.get("elasticsearch", {}),
            spark_config_data=env_data.get("spark", {}),
        )
        try:
            use_case.execute(
                specification=spec,
                job_id=enriched_config.job_id,
                execution_config=enriched_config,
            )
            logger.info("Linkage concluído com sucesso.")
        except Exception as e:
            logger.critical(f"Erro crítico no linkage: {e}", exc_info=True)
            sys.exit(1)
        finally:
            spark.stop()

    elif args.command == "indexing":
        env_data = load_yaml(args.env_config)
        spec_path = args.spec_config or env_data.get("specification", {}).get("indexing_path")
        indexing_spec_data = load_yaml(spec_path)

        use_case, spec, spark = build_indexing_use_case(
            storage_config_data=env_data.get("storage", {}),
            execution_config_data=env_data.get("execution", {}),
            indexing_spec_data=indexing_spec_data,
            es_config_data=env_data.get("elasticsearch", {}),
            spark_config_data=env_data.get("spark", {}),
        )
        try:
            use_case.execute(spec=spec)
            logger.info("Indexação concluída com sucesso.")
        except Exception as e:
            logger.critical(f"Erro crítico na indexação: {e}", exc_info=True)
            sys.exit(1)
        finally:
            spark.stop()

    elif args.command == "deduplication":
        try:
            config = load_deduplicate_workflow_config(args.config_path)
        except (FileNotFoundError, ValueError, IOError) as e:
            logger.error(f"Falha ao carregar configuração: {e}")
            sys.exit(1)

        use_case, spark = build_deduplication_use_case(config)
        try:
            use_case.execute(spec=config.deduplication_spec)
            logger.info("Deduplicação concluída com sucesso.")
        except Exception as e:
            logger.critical(f"Erro crítico na deduplicação: {e}", exc_info=True)
            sys.exit(1)
        finally:
            spark.stop()
