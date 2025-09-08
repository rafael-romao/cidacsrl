import argparse
import logging
import time
import re
import os
from pathlib import Path

from typing import Optional, Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

# Project-specific imports
from cidacsrl_rlp.src.linkage.cidacsrl import cidacsrl
from cidacsrl_rlp.src.linkage.models import (
    SequentialBlockingWorkflow,
    BlockingPhase
)
from cidacsrl_rlp.src.config.loader import load_sequential_blocking_workflow_config, load_service_config
from cidacsrl_rlp.src.utils.logging_config import setup_logging
from cidacsrl_rlp.src.utils.utils import sanitize_string
from cidacsrl_rlp.src.utils.spark_utils import create_spark_session

# Logger for this module
logger = logging.getLogger(__name__)

def ler_particao(path, partition_by, partition, spark):
    if partition == "*":
        # Serão consideradas todas as partições (sem filtro)
        read_path = str(path)
        logger.info(f"> Lendo dados em: {path}")
        return spark.read.parquet(read_path)
    else:
        read_path = f"{re.sub("\/$", "", str(path))}/{partition_by}={partition}"
        # Verifica se os dados de origem foram particionados na escrita
        if os.path.exists(read_path):
            logger.info(f"> Lendo dados em: {read_path}")
            return spark.read.parquet(read_path).withColumn(partition_by, F.lit(partition))
        else:
            # Os dados fonte não foram particionados na escrita então serão lidos e filtrados de acordo com o valor da partição
            read_path = "/".join(read_path.split("/")[:-1])
            logger.info(f"Lendo dados em {read_path} e filtrando partição: {partition_by}={partition}")
            return spark.read.parquet(read_path).filter(f"{partition_by}={partition}")


def main():
    """Ponto de entrada principal para o fluxo de trabalho de linkage sequencial.

    Este script orquestra um processo de linkage de dados em múltiplas fases,
    conhecido como "Sequential Blocking". Ele é projetado para ser executado
    a partir da linha de comando e realiza as seguintes operações:

    1.  Carrega as configurações do fluxo de trabalho, do Spark e do Elasticsearch
        a partir de arquivos YAML.
    2.  Inicializa uma sessão Spark com as configurações fornecidas.
    3.  Carrega os dados da fonte (source data) a partir de um caminho especificado
        (ex: diretório Parquet).
    4.  Opcionalmente, aplica amostragem aos dados da fonte para testes ou depuração.
    5.  Itera através de uma série de "fases de bloqueio" (blocking phases)
        definidas no arquivo de configuração do fluxo de trabalho.
    6.  Em cada fase, executa a lógica de linkage para encontrar registros
        candidatos no índice do Elasticsearch e calcular scores de similaridade.
    7.  Salva os resultados (pares encontrados) de cada fase em um diretório de saída
        intermediário.
    8.  Registros da fonte que encontram uma correspondência forte em uma fase são
        removidos do conjunto de dados para as fases subsequentes, otimizando o processo.
    9.  Gerencia o particionamento de dados para processamento distribuído e
        consultas ao Elasticsearch.

    Args:
        --workflow-config-path (str): Caminho para o arquivo de configuração YAML
            do fluxo de trabalho de linkage sequencial.
        --es-config-path (str): Caminho para o arquivo de configuração YAML da
            conexão com o Elasticsearch.
        --spark-config-path (str): Caminho para o arquivo de configuração YAML do Spark.
        --output-data-dir (str): Diretório base para salvar todos os resultados
            das fases de linkage.
        --source-data-path (str): Caminho para os dados da fonte (ex: diretório Parquet).
        --current-partition-value (str, opcional): Valor da partição atual sendo
            processada (ex: 'SP'). Usado para filtrar dados no Elasticsearch e,
            potencialmente, no caminho dos dados da fonte.
        --log-level (str): Nível de logging (padrão: "INFO").
        --sample-fraction (float, opcional): Fração dos dados da fonte para
            amostragem (entre 0.0 e 1.0).
        --sample-seed (int, opcional): Semente para a amostragem (padrão: 42).
        --spark-checkpoint-base-dir (str, opcional): Diretório base para os
            checkpoints do Spark, para otimizar a performance em fluxos longos.

    Example:
        Para executar o fluxo de trabalho a partir do terminal:

        .. code-block:: bash

            python -m src.workflows.sequential_linkage_workflow \\
                --workflow-config-path /path/to/your_workflow.yaml \\
                --es-config-path /path/to/your_es_config.yaml \\
                --spark-config-path /path/to/your_spark_config.yaml \\
                --output-data-dir /path/to/output/data \\
                --source-data-path /path/to/source_data.parquet \\
                --current-partition-value SP \\
                --log-level DEBUG
    """
    parser = argparse.ArgumentParser(description="Executes a Sequential Blocking Linkage Workflow using Elasticsearch and Spark.")
    parser.add_argument("--workflow-config-path", required=True, help="Path to the Sequential Blocking Workflow YAML configuration file.")
    parser.add_argument("--es-config-path", required=True, help="Path to the Elasticsearch connection configuration YAML file.")
    parser.add_argument("--spark-config-path", required=True, help="Path to the Spark configuration YAML file.")
    parser.add_argument("--output-data-dir", required=True, help="Base directory for saving all linkage phase outputs and final results.")
    parser.add_argument("--source-data-path", required=True, help="Path to the source data (e.g., Parquet directory).")
    parser.add_argument("--current-partition-value", type=str, default=None,
                        help="Value of the current partition being processed (e.g., 'SP' for a UF partition). "
                             "Used with 'target_es_partition_filter_field' from workflow config for ES queries, "
                             "and potentially for filtering source data path if it's partitioned.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level for the application (default: INFO).")
    parser.add_argument("--sample-fraction", type=float, default=None,
                        help="Fraction of source data to sample for processing (0.0 to 1.0). For testing/debugging.")
    parser.add_argument("--sample-seed", type=int, default=42,
                        help="Seed for sampling if --sample-fraction is used (default: 42).")
    parser.add_argument("--spark-checkpoint-base-dir", type=str, default=None,
                        help="Base directory for Spark checkpoints. If provided, some DataFrames might be checkpointed. "
                             "A subdirectory specific to this job will be created here.")

    args = parser.parse_args()
    setup_logging(level=getattr(logging, args.log_level.upper()))
    logger.info(f"Starting CIDACS-RL Engine Sequential Linkage Workflow with workflow config: {args.workflow_config_path}")
    workflow_start_time = time.time()

    # Load configurations
    workflow_config = load_sequential_blocking_workflow_config(args.workflow_config_path)
    es_settings = load_service_config(args.es_config_path, service_name="elasticsearch")
    spark_settings = load_service_config(args.spark_config_path, service_name="spark")

    safe_source_name = sanitize_string(workflow_config.source_table)
    safe_target_name = sanitize_string(workflow_config.target_es_index) # Uses target_es_index

    # Construct a descriptive application name for Spark UI
    app_name_parts = [workflow_config.workflow_name or f"linkage-{safe_source_name}_vs_{safe_target_name}"]
    if args.current_partition_value:
        # FIXME: podemos remover esse argumento das configurações uma vez que o melhorei com o `filter_partitions``
        app_name_parts.append(f"partition-{sanitize_string(args.current_partition_value)}")
    app_name = "_".join(app_name_parts)

    spark = create_spark_session(
        app_name=app_name,
        spark_config_path=args.spark_config_path,
    )

    try:
        output_data_dir_path = Path(args.output_data_dir)
        workflow_prefix_for_dir = ""
        if workflow_config.workflow_name:
            workflow_prefix_for_dir = sanitize_string(workflow_config.workflow_name) + "_"
        # Base name for directory holding all intermediate phase results for this workflow
        intermediate_base_dir_name = f"{workflow_prefix_for_dir}{safe_source_name}_vs_{safe_target_name}_intermediate_phases"
        intermediate_results_base_path = output_data_dir_path / intermediate_base_dir_name
        intermediate_results_base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Intermediate results from each linkage phase will be saved under: {intermediate_results_base_path}")

        # Load source data
        source_data_path_obj = Path(args.source_data_path)
        source_data_path_to_load = source_data_path_obj

        
        if not source_data_path_obj.exists() or not source_data_path_obj.is_dir():
            logger.error(f"Source data path does not exist or is not a directory: {source_data_path_obj}")
            raise FileNotFoundError(f"Source data path not found: {source_data_path_obj}")

        # Se existir o parâmetro que indica que os dados foram particionados
        partitions = None
        if workflow_config.partition_by['partition']:
            partitions = [x[0] for x in spark.read.parquet(str(source_data_path_to_load)).select(workflow_config.partition_by['partition']).distinct().collect()]
            if workflow_config.partition_by['filter_partitions']:
                partitions = [x for x in partitions if x in workflow_config.partition_by['filter_partitions']]
        else:
            partitions = ["*"]

        # Percorre todas as partições do DataFrame caso os dados estejam particionados ou lê o DataFrame completo caso contrário.
        for partition in partitions:
            try:
                # df_source_for_processing = spark.read.parquet(str(source_data_path_to_load))
                df_source_for_processing = ler_particao(
                    path=str(source_data_path_to_load),
                    partition_by=workflow_config.partition_by['partition'],
                    partition=partition,
                    spark=spark,
                )

                # Remove duplicates from source based on its ID to avoid redundant processing
                df_source_for_processing = df_source_for_processing.dropDuplicates([workflow_config.id_source_table])
                logger.info(f"Source DataFrame loaded successfully from '{source_data_path_to_load}'.")
                logger.info(f"Source DataFrame initially has {df_source_for_processing.rdd.getNumPartitions()} partitions.")
                # Consider making repartitioning configurable via spark_settings or workflow_config
                num_shuffle_partitions = 50
                df_source_for_processing = df_source_for_processing.repartition(num_shuffle_partitions)
                logger.info(f"Source DataFrame repartitioned to {df_source_for_processing.rdd.getNumPartitions()} partitions.")
            except Exception as e_read_source:
                logger.error(f"Failed to read source data from '{source_data_path_to_load}'. "
                            f"Verify path and format. Error: {e_read_source}", exc_info=True)
                raise

            # Apply sampling if configured
            if args.sample_fraction and 0.0 < args.sample_fraction <= 1.0:
                df_source_for_processing = df_source_for_processing.sample(
                    withReplacement=False, fraction=args.sample_fraction, seed=args.sample_seed
                )
                logger.info(f"Sampled source DataFrame with fraction {args.sample_fraction} and seed {args.sample_seed}.")

            initial_source_count = df_source_for_processing.count()
            logger.info(f"{initial_source_count:,} unique source records submitted for linkage after initial load and deduplication.")

            if initial_source_count == 0:
                logger.info("No source records found to process after loading (and sampling, if any). Linkage workflow terminating.")
                return

            #################################################### Cidacs - Record Linkage ############################################

            # Executa o linkage
            cidacsrl(
                df=df_source_for_processing,
                linkage_name=f"linkage-{safe_source_name}_vs_{safe_target_name}{'-' + str(partition) if partition != '*' else ''}",
                workflow_config=workflow_config,
                spark=spark,
                es_settings=es_settings,
                intermediate_results_base_path=intermediate_results_base_path,
                partition=partition,
                log_file=f"{output_data_dir_path}/metadata_linkages.csv",
                logger=logger,
            )


        logger.info("All linkage phases have been executed.")


    except Exception as e:
        logger.critical(f"Critical error in linkage workflow: {e}", exc_info=True)
        exit(1) # Exit with error code
    finally:
        workflow_duration_total = time.time() - workflow_start_time
        mins, secs = divmod(workflow_duration_total, 60)
        logger.info(f"CIDACS-RL Engine Linkage Workflow completed in {workflow_duration_total:.2f} seconds "
                    f"(approximately {int(mins):02d}m:{int(secs):02d}s).")


if __name__ == "__main__":
    main()