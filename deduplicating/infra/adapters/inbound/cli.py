import sys
import argparse
import logging

from cidacsrl.config.logging import configure_logging
from cidacsrl.config.dedup_loader import load_deduplicate_workflow_config
from cidacsrl.bootstrap.deduplication_bootstrap import build_deduplication_use_case

logger = logging.getLogger("CLI: Deduplication")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cidacsrl-dedup",
        description="CIDACS-RL — Workflow de deduplicação via componentes conectados.",
    )
    parser.add_argument(
        "--config-path",
        required=True,
        metavar="PATH",
        help="Caminho para o arquivo YAML de configuração do workflow.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        metavar="LEVEL",
        help="Nível de logging (padrão: INFO).",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    configure_logging()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

    logger.info(f"Iniciando deduplicação com config: '{args.config_path}'.")

    try:
        config = load_deduplicate_workflow_config(args.config_path)
    except (FileNotFoundError, ValueError, IOError) as e:
        logger.error(f"Falha ao carregar configuração: {e}")
        sys.exit(1)

    use_case, spark = build_deduplication_use_case(config)
    try:
        use_case.execute(spec=config.deduplication_spec)
        logger.info("Deduplication workflow finalizado com sucesso.")
    except Exception as e:
        logger.critical(f"Erro crítico na execução: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
