import argparse
import logging
import time
from pathlib import Path

from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from cidacsrl_rlp.src.linkage.cidacsrl import cidacsrl
from cidacsrl_rlp.src.linkage.models import (
    SequentialBlockingWorkflow,
    BlockingPhase
)
from cidacsrl_rlp.src.config.loader import load_sequential_blocking_workflow_config, load_service_config, load_linkage_workflow_config
from cidacsrl_rlp.src.linkage.rdd_processing import process_partition_for_phase
from cidacsrl_rlp.src.utils.schema_helpers import define_phase_output_schema, define_workflow_output_schema
from cidacsrl_rlp.src.utils.logging_config import setup_logging
from cidacsrl_rlp.src.utils.utils import sanitize_string
from cidacsrl_rlp.src.utils.spark_utils import create_spark_session
from cidacsrl_rlp.src.utils.io_manager import read_source_data, write_phase_results

# Logger for this module
logger = logging.getLogger(__name__)

def get_available_partitions(source_path, partition_by, spark):
    # Verifica se os dados foram particionados
    partitions = None
    if partition_by and partition_by.get('partition'):
        partitions = [x[0] for x in spark.read(source_path).select(partition_by['partition']).distinct().collect()]
        if partition_by.get('filter_partitions'):
            partitions = [x for x in partitions if x in partition_by['filter_partitions']]
    else:
        partitions = ["*"]
    return partitions

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
        --config-path (str): Caminho para o arquivo de configuração YAML principal
            do fluxo de trabalho que contém todos os parâmetros necessários.
        --log-level (str): Nível de logging (padrão: "INFO").

    Example:
        Para executar o fluxo de trabalho a partir do terminal:

        .. code-block:: bash

            python -m cidacsrl_rlp.src.workflows.sequential_linkage_workflow \\
                --config-path /path/to/workflow_config.yaml \\
                --log-level DEBUG

        Exemplo do arquivo de configuração YAML `workflow_config.yaml`:

        .. code-block:: yaml

            linkage_config_path: "/path/to/linkage_workflow.yaml"
            es_config_path: "/path/to/elasticsearch_config.yaml"
            spark_config_path: "/path/to/spark_config.yaml"
            output_base_path: "/path/to/output/data"
            source_data_path: "/path/to/source_data"
            sample_fraction: 0.1  # opcional - para teste/debug
            sample_seed: 42       # opcional
    """
    parser = argparse.ArgumentParser(description="Executes a Sequential Blocking Linkage Workflow using Elasticsearch and Spark.")
    parser.add_argument("--config-path", required=True, 
                        help="Path to the main workflow configuration YAML file containing all parameters.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level for the application (default: INFO).")

    args = parser.parse_args()
    
    # Setup logging
    setup_logging(level=getattr(logging, args.log_level.upper()))
    
    # Load main workflow configuration
    try:
        logger.info("Loading workflow configuration...")
        workflow_config = load_linkage_workflow_config(args.config_path)
        logger.info(f"Workflow configuration loaded successfully for linkage config: {workflow_config.linkage_config_path}")
    except (FileNotFoundError, ValueError, IOError) as e:
        logger.error(f"Failed to load workflow configuration: {e}")
        exit(1)
    
    logger.info(f"Starting CIDACS-RL Workflow")
    workflow_start_time = time.time()

    # Load configurations
    try:
        logger.info("Loading individual configurations...")
        linkage_config = load_sequential_blocking_workflow_config(workflow_config.linkage_config_path)
        es_settings = load_service_config(workflow_config.es_config_path, service_name="elasticsearch")
        spark_settings = load_service_config(workflow_config.spark_config_path, service_name="spark")
        logger.info("All configurations loaded successfully.")
    except (FileNotFoundError, ValueError, IOError) as e:
        logger.error(f"Failed to load configurations: {e}")
        exit(1)

    source_name = sanitize_string(linkage_config.source_table)
    target_name = sanitize_string(linkage_config.target_es_index) 

    # Construct a descriptive application name for Spark UI
    app_name_parts = [f"linkage-{source_name}_vs_{target_name}"]
    if workflow_config.sample_fraction and 0 < workflow_config.sample_fraction < 1.0:
        app_name_parts.append(f"sample{int(workflow_config.sample_fraction * 100)}pct")
    if workflow_config.sample_seed is not None:
        app_name_parts.append(f"seed{workflow_config.sample_seed}")
    app_name = "_".join(app_name_parts)

    spark = create_spark_session(
        app_name=app_name,
        spark_config_path=spark_settings,
    )

    try:
        # Obtém as partições dos dados
        partitions = get_available_partitions(
            workflow_config.source_data_path,
            workflow_config.partition_by,
            spark
        )
        
        # Percorre todas as partições do DataFrame caso os dados estejam particionados ou lê o DataFrame completo caso contrário.
        for i, partition in enumerate(partitions):
            # debug
            logger.info(f"> [#{i + 1}/{len(partitions)}] {workflow_config.partition_by.get('partition', 'ALL')}={partition}")
            
            df_source = read_source_data(
                spark=spark,
                source_path=workflow_config.source_data_path,
                sample_fraction=workflow_config.sample_fraction,
                sample_seed=workflow_config.sample_seed,
                partition_info={
                    'partition_by': workflow_config.partition_by.get('partition'),
                    'partition': partition
                },
            )
            
            source_count = df_source.count()
            if source_count == 0:
                logger.info("No source records found to process. Linkage workflow terminating.")
                raise ValueError("No source records found")
            else:
                logger.info(f"{source_count:,} source records submitted for linkage")
            
            # Define o path onde o linkage será salvo
            write_path = Path(workflow_config.output_base_path) / f"linkage_{source_name}_vs_{target_name}"

            # Define o path de escrita da partição (caso os dados estejam particionados)
            if partition != "*":
                write_path = f"{write_path}/{workflow_config.partition_by.get('partition')}={partition}"

            # Executa o linkage
            cidacsrl(
                df=df_source,
                linkage_config=linkage_config,
                spark=spark,
                es_settings=es_settings,
                write_path=write_path,
                partition_column=workflow_config.partition_by.get('partition'),
                log_linkage_file=linkage_config.log_linkage_file, # f"/tmp/metadata_linkages.csv",
            )

        logger.info("All linkage phases have been executed.")

    except Exception as e:
        # TODO: adicionar um evento de erro no log (similar ao evento de END porém indicando ERROR)
        logger.critical(f"Critical error in linkage workflow: {e}", exc_info=True)
        exit(1)
    finally:
        workflow_duration_total = time.time() - workflow_start_time
        mins, secs = divmod(workflow_duration_total, 60)
        logger.info(f"linkage_{source_name}_vs_{target_name} workflow completed in {workflow_duration_total:.2f} seconds "
                    f"(approximately {int(mins):02d}m:{int(secs):02d}s).")


if __name__ == "__main__":
    main()