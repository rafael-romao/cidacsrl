"""
CIDACS-RL — E2E Runner: Deduplicação

Executa o pipeline completo de deduplicação usando dados locais de teste
e valida a saída produzida.

Uso:
    poetry run python deduplicating/tests/e2e/run_e2e_deduplication.py
    poetry run python deduplicating/tests/e2e/run_e2e_deduplication.py \\
        --config-path deduplicating/tests/configs/deduplicate_acidentes_obitos_env.yml
"""

import argparse
import logging
import subprocess
import sys
import yaml
from pathlib import Path

_PYTHON = sys.executable

from cidacsrl.config.logging import configure_logging

configure_logging()
logger = logging.getLogger("E2E Deduplication Runner")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "configs" / "deduplicate_acidentes_obitos_env.yml"


def run_deduplication_step(config_path: Path, project_root: Path) -> None:
    logger.info(f"Passo 1/2: Disparando CLI de deduplicação com config '{config_path.name}'...")
    result = subprocess.run(
        [
            _PYTHON, "-m",
            "deduplicating.infra.adapters.inbound.cli",
            "--config-path", str(config_path),
        ],
        cwd=str(project_root),
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"CLI de deduplicação reportou erro (exit {result.returncode})."
        )


def verify_deduplication_results(
    output_path: Path,
    id_source_column: str,
    output_group_id_column: str,
) -> None:
    logger.info("Passo 2/2: Validando resultados da deduplicação...")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("E2E-Dedup-Validation")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        assert output_path.exists(), f"Diretório de saída não encontrado: {output_path}"

        df = spark.read.parquet(str(output_path))
        total = df.count()

        assert total > 0, "O pipeline de deduplicação gerou saída vazia."
        assert output_group_id_column in df.columns, (
            f"Coluna de cluster '{output_group_id_column}' ausente na saída. "
            f"Colunas disponíveis: {df.columns}"
        )

        n_clusters = df.select(output_group_id_column).distinct().count()
        assert n_clusters > 0, "Nenhum cluster conectado foi gerado."

        logger.info(f"Auditoria: {total:,} registros agrupados em {n_clusters:,} clusters.")

        logger.info("Amostra dos clusters gerados:")
        rows = df.select(id_source_column, output_group_id_column).limit(5).collect()
        for i, row in enumerate(rows):
            logger.info(
                f"  [#{i + 1}] {id_source_column}={row[id_source_column]} "
                f"→ {output_group_id_column}={row[output_group_id_column]}"
            )

    finally:
        spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CIDACS-RL — E2E Runner de Deduplicação"
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Caminho para o YAML de configuração (padrão: configs/deduplicate_acidentes_obitos_env.yml).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    config_path = Path(args.config_path) if args.config_path else _default_config_path()
    if not config_path.is_absolute():
        config_path = _project_root() / config_path

    if not config_path.exists():
        logger.error(f"Config não encontrado: {config_path}")
        sys.exit(1)

    project_root = _project_root()

    try:
        logger.info("=" * 72)
        logger.info(f"       INICIANDO E2E DE DEDUPLICAÇÃO: {config_path.name}       ")
        logger.info("=" * 72)

        run_deduplication_step(config_path=config_path, project_root=project_root)

        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        raw_output_path = cfg["storage"]["output_path"]
        output_path = Path(raw_output_path)
        if not output_path.is_absolute():
            output_path = project_root / output_path

        deduplication_cfg = cfg.get("deduplication", {})
        id_source_column = deduplication_cfg["id_source_column"]
        output_group_id_column = deduplication_cfg.get("output_group_id_column", "cidacs_cluster_id")

        verify_deduplication_results(
            output_path=output_path,
            id_source_column=id_source_column,
            output_group_id_column=output_group_id_column,
        )

        logger.info("=" * 72)
        logger.info("     STATUS FINAL: SUCESSO")
        logger.info("=" * 72)
        sys.exit(0)

    except Exception as e:
        logger.error(f"Falha no pipeline de deduplicação: {e}", exc_info=True)
        sys.exit(1)
