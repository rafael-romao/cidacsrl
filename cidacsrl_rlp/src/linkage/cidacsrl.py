from cidacsrl_rlp.src.linkage.rdd_processing import process_partition_for_phase
from cidacsrl_rlp.src.utils.io_manager import write_phase_results
from cidacsrl_rlp.src.utils.schema_helpers import define_phase_output_schema, define_workflow_output_schema
from cidacsrl_rlp.src.utils.utils import sanitize_string
from cidacsrl_rlp.src.utils.log import trace_execution
from typing import Dict, Any

# Project-specific imports
from cidacsrl_rlp.src.linkage.models import (
    SequentialBlockingWorkflow,
    BlockingPhase,
    LinkageWorkflowConfig
)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import logging
import time

class CidacsRL:
    def __init__(
            self,
            spark: SparkSession,
            df: DataFrame,
            linkage_config: SequentialBlockingWorkflow,
            es_settings: Dict[str, Any],
            workflow_config: LinkageWorkflowConfig,
            logger = None,
            debug: bool = True
        ):
        """Args:
            * `spark` (SparkSession): Instância do Spark.
            * `df` (DataFrame): PySpark DataFrame que será submetido ao Linkage.
            * `linkage_config` (SequentialBlockingWorkflow): Configurações gerais do linkage.
            * `es_settings` (Dict[str, Any]): Configurações de conexão com o Elasticsearch.
            * `workflow_config` (LinkageWorkflowConfig): Configurações gerais do Workflow.
            * `logger` (logging, Optional): Objeto logger para exibição dos logs do linkage. Caso não informado será feita uma instância nova.
            * `debug`: (bool, Optional): Flag booleana para indicar se devem ser exibidos prints de debug ou não.
        """
        # Cria um ID para a execução atual (pode ser utilizado nos logs)
        self.execution_id = datetime.now().strftime("%Y%m%d%H%M")
        self.__spark = spark
        self.df = df
        self.linkage_config = linkage_config
        self.es_settings = es_settings
        self.write_path = workflow_config.output_data_path
        self.partition_column = workflow_config.partition_by.get('partition')
        self.log_linkage_file = workflow_config.log_linkage_file
        self.__logger = logging.getLogger(__name__) if not logger else logger
        self.__debug = debug

    def __execute_linkage_phase(
        self,
        df_source_this_phase: DataFrame,
        phase: BlockingPhase,
    ) -> DataFrame:
        """
        Executa uma única fase de linkage (blocking phase).

        Esta função pega o DataFrame da fonte para a fase atual, aplica as regras de blocking
        e similaridade definidas na `phase` para encontrar e pontuar candidatos
        do Elasticsearch.

        Args:
            spark (SparkSession): A sessão Spark ativa.
            df_source_this_phase (DataFrame): DataFrame contendo os registros da fonte a serem processados nesta fase.
            workflow_config (SequentialBlockingWorkflow): Configuração geral do workflow.
            phase (BlockingPhase): Configuração específica para esta fase de linkage.
            es_settings (Dict[str, Any]): Configurações de conexão com o Elasticsearch.

        Returns:
            DataFrame: Um DataFrame contendo todos os pares fonte-candidato encontrados e
                    pontuados por esta fase. O DataFrame de saída conterá o `id_source_table`
                    da fonte, dados do candidato (prefixados) e os scores calculados.
                    Retorna um DataFrame vazio com o schema esperado se não houver dados de entrada
                    ou se nenhum candidato for encontrado.
        """
        source_df_schema_for_phase = df_source_this_phase.schema

        if df_source_this_phase.rdd.isEmpty():
            self.__logger.warning(f"Source DataFrame for phase '{phase.phase_name}' is empty. Returning an empty DataFrame with defined schema.")
            # Define schema based on the (empty) source DataFrame's schema and phase config
            raw_output_schema = define_phase_output_schema(source_df_schema_for_phase, self.linkage_config, phase)
            return self.__spark.createDataFrame([], schema=raw_output_schema)

        # Define the output schema for the results of this phase
        raw_output_schema = define_phase_output_schema(source_df_schema_for_phase, self.linkage_config, phase)

        # Prepare configurations for broadcasting (must be dicts)
        linkage_config_dict = vars(self.linkage_config).copy() # Convert dataclass to dict
        phase_config_dict = vars(phase).copy()       # Convert dataclass to dict
        # Convert nested ComparisonRule objects to dicts as well
        phase_config_dict['rules'] = [vars(rule) for rule in phase.rules]

        # Broadcast variables to all Spark executors
        workflow_config_dict_bcast = self.__spark.sparkContext.broadcast(linkage_config_dict)
        phase_config_dict_bcast = self.__spark.sparkContext.broadcast(phase_config_dict)
        es_config_dict_bcast = self.__spark.sparkContext.broadcast(self.es_settings)
        # source_schema_bcast = self.__spark.sparkContext.broadcast(source_df_schema_for_phase) # Broadcast source schema

        # process_partition_for_phase is designed to accept these broadcasted dict configurations
        scored_candidates_rdd = df_source_this_phase.rdd.mapPartitions(
            lambda partition_iter: process_partition_for_phase(
                partition_iter,
                workflow_config_dict_bcast,
                phase_config_dict_bcast,
                es_config_dict_bcast,
                # source_schema_bcast
            )
        )

        if scored_candidates_rdd.isEmpty():
            self.__logger.warning(f"Scored candidates RDD for phase '{phase.phase_name}' is empty. Returning an empty DataFrame.")
            return self.__spark.createDataFrame([], schema=raw_output_schema)
        else:
            self.__logger.info(f"RDD with scored candidates for phase '{phase.phase_name}' created successfully.")

        # Create DataFrame from the RDD of scored candidates
        df_phase_scored_candidates = self.__spark.createDataFrame(scored_candidates_rdd, schema=raw_output_schema)

        return df_phase_scored_candidates

    def __process_phase(
        self,
        df_phase: DataFrame,
        phase: BlockingPhase,
    ) -> DataFrame:
        """
        Processa uma fase do workflow de linkage.

        Args:
            spark (SparkSession): A sessão Spark ativa.
            phase (BlockingPhase): Configuração específica para esta fase de linkage.
            df_phase (DataFrame): DataFrame contendo os registros da fonte a serem processados.

        Returns:
            DataFrame: Um DataFrame contendo todos os pares fonte-candidato encontrados e
                    pontuados por esta fase. O DataFrame de saída conterá o `id_source_table`
                    da fonte, dados do candidato (prefixados) e os scores calculados.
                    Retorna um DataFrame vazio com o schema esperado se não houver dados de entrada
                    ou se nenhum candidato for encontrado.
        """
        phase_name = sanitize_string(phase.phase_name)
        self.__logger.info(f"Phase '{phase_name}': starting with {df_phase.count():,} records")

        phase_execution_start_time = time.time()

        df_matches = self.__execute_linkage_phase(df_phase, phase)
        
        phase_execution_duration = time.time() - phase_execution_start_time
        self.__logger.info(f"Phase '{phase_name}': execution completed in {phase_execution_duration:.2f}s.")
        self.__logger.info(f"Phase '{phase_name}': {df_matches.count():,} matches found")

        return df_matches

    def execute_linkage(self):
        """Função para consolidar as chamadas e execuções do fluxo do Cidacs-RL e executar o linkage.
        """
        # Backup do schema original das colunas
        original_source_schema = self.df.schema

        # Cria um nome para o linkage para ser salvo nos logs
        linkage_name = f"linkage{self.write_path.split("linkage")[1:]}" if "linkage" in self.write_path else self.write_path

        # Verifica se é para registrar logs do processo de linkage
        if self.log_linkage_file:
            # Registra o início do linkage nos logs
            trace_execution(process_name=linkage_name, operation="START", caminho_csv=self.log_linkage_file, execution_id=self.execution_id)

        # Faz uma cópia dos dados fonte originais. Essa cópia será atualizada em cada iteração das fases
        df_source = self.df

        try:
            # Main loop through blocking phases
            for i, phase in enumerate(self.linkage_config.blocking_phases):
                phase_loop_start_time = time.time()
                phase_name = phase.phase_name           
                phase_threshold = phase.strong_match_score_threshold
                phase_output_path = f"{self.write_path}/linkage_phase_name={phase_name}"
                
                phase_results_schema = define_workflow_output_schema(
                    original_source_schema,
                    self.linkage_config,
                    phase,
                    include_phase_name=True
                )

                if not phase.enabled:
                    self.__logger.info(f"Skipping disabled phase: '{phase_name}'")
                    continue
                self.__logger.info(f"[#{i + 1}/{len(self.linkage_config.blocking_phases)}] Executing phase: '{phase.phase_name}'")

                if df_source.isEmpty():
                    self.__logger.info(f"No source records remaining. Stopping workflow before phase '{phase_name}'.")
                    break

                df_matches = self.__process_phase(df_phase=df_source, phase=phase)

                if df_matches.isEmpty():
                    self.__logger.info(f"No matches found in phase '{phase_name}'.")
                    continue
                else:
                    phase_result_columns = [field.name for field in phase_results_schema.fields]
                    df_matches = df_matches.select(phase_result_columns)

                    # Colunas de auditoria
                    df_matches = df_matches.withColumns({
                        "_LINKED_FROM": F.lit(phase_name),
                        "_DT_LINKAGE": F.from_utc_timestamp(F.current_timestamp(), "America/Sao_Paulo"),
                    })

                    if self.partition_column:
                        df_matches = df_matches.drop(self.partition_column)

                    self.__logger.info(f"[Phase '{phase_name}']: Escrevendo dados em `{phase_output_path}`...")
                    write_phase_results(self.__spark, df_matches, phase_output_path, mode="overwrite")
                    # df_matches.write.format("parquet").mode("overwrite").save(phase_output_path)
                    self.__logger.info(f"Candidates from phase '{phase_name}' written successfully to: {phase_output_path}")

                    # Identify unique source IDs that found a strong match in this phase
                    source_matched = self.__spark.read.parquet(phase_output_path).select(self.linkage_config.id_source_table).distinct()

                    if self.__debug:
                        self.__logger.info(f"[Phase '{phase_name}']: {source_matched.count()} source found matches above {phase_threshold} and writed in `{phase_output_path}`")
                    else:
                        self.__logger.info(f"[Phase '{phase_name}']: results written to `{phase_output_path}`")

                    # Remove matched source IDs from the pool for subsequent phases
                    df_source = df_source.join(
                        source_matched,
                        on=self.linkage_config.id_source_table,
                        how="left_anti"
                    )

                    # Checkpoint aqui do df_source?

                    count_remaining_source = df_source.count()

                    if count_remaining_source == 0 and phase != self.linkage_config.blocking_phases[-1]:
                        self.__logger.info(f"No source records remaining after phase '{phase.phase_name}'.")
                        self.__logger.info("All source records have been matched. Linkage workflow terminating.")
                        break
                    else:
                        self.__logger.info(f"{count_remaining_source:,} source records remaining for next phase.")

                phase_loop_total_duration = time.time() - phase_loop_start_time
                self.__logger.info(f"Phase '{phase.phase_name}': completed in {phase_loop_total_duration:.2f}s")

            self.__logger.info(f"Registrando o término do Linkage em `{self.log_linkage_file}`")
            if self.log_linkage_file:
                # Registra o término do linkage
                trace_execution(process_name=linkage_name, operation="END", caminho_csv=self.log_linkage_file, execution_id=self.execution_id)
        except Exception as exc:
            if self.log_linkage_file:
                # Registra o evento de erro do processo de linkage nos logs desse linkage
                trace_execution(process_name=linkage_name, operation="ERROR", caminho_csv=self.log_linkage_file, execution_id=self.execution_id)
            # Lança a exceção para o fluxo que chamou essa função 
            raise Exception(exc)

