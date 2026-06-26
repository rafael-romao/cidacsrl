import logging
from typing import Any, Dict, Tuple

from pyspark.sql import SparkSession

from cidacsrl.adapters.outbound.checkpoint.json_checkpoint_adapter import (
    JSONCheckpointAdapter,
)
from cidacsrl.adapters.outbound.elasticsearch.client import (
    get_es_client,
    validate_elasticsearch_schema,
)
from cidacsrl.adapters.outbound.elasticsearch.executors import (
    MultiSearchExecutor,
    SingleSearchExecutor,
)
from cidacsrl.adapters.outbound.elasticsearch.service_config import (
    ElasticsearchConfig,
)
from cidacsrl.adapters.outbound.elasticsearch.spark_es_search_adapter import (
    SparkESSearchAdapter,
)
from cidacsrl.adapters.outbound.spark.data_ingestion_adapter import (
    SparkDataIngestionAdapter,
)
from cidacsrl.adapters.outbound.spark.data_persistence_adapter import (
    SparkDataPersistenceAdapter,
)
from cidacsrl.adapters.outbound.spark.data_transformation_adapter import (
    SparkDataTransformationAdapter,
)
from cidacsrl.adapters.outbound.spark.scoring_adapter import (
    SparkScoringAdapter,
)
from cidacsrl.adapters.outbound.spark.spark_factory import create_spark_session
from cidacsrl.adapters.outbound.telemetry.composite_linkage_telemetry_adapter import (
    CompositeLinkageTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.formatted_log_telemetry_adapter import (
    FormattedLogTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.jsonl_telemetry_adapter import (
    JsonlLinkageTelemetryAdapter,
)
from cidacsrl.application.linkage.record_linkage_use_case import (
    RecordLinkageUseCase,
)
from cidacsrl.application.linkage.work_unit_orchestrator import (
    WorkUnitOrchestrator,
)
from cidacsrl.config.loader import (
    parse_es_config,
    parse_execution_config,
    parse_output_storage_config,
    parse_sequential_linkage_specification,
    parse_source_storage_config,
)
from cidacsrl.config.models.execution_config import ExecutionConfig
from cidacsrl.domain.linkage.linkage_specification import (
    SequentialLinkageSpecification,
)

logger = logging.getLogger("Bootstrap: Record Linkage")


def _run_preflight_validations(
    linkage_spec: SequentialLinkageSpecification,
    execution_config: ExecutionConfig,
    es_config: ElasticsearchConfig,
    ingestion_adapter: SparkDataIngestionAdapter,
) -> None:
    """Valida infraestrutura e schemas antes de iniciar o pipeline de linkage.

    Verifica acessibilidade do storage de origem, presença das colunas requeridas
    na tabela fonte (incluindo coluna de partição, se configurada) e consistência
    do mapeamento do índice Elasticsearch com as colunas esperadas.

    Args:
        linkage_spec: Especificação do projeto com tabela fonte e índice alvo.
        execution_config: Configuração com coluna de partição e paths de auditoria.
        es_config: Configuração de conexão com o Elasticsearch.
        ingestion_adapter: Adapter de ingestão já instanciado para validações de schema.

    Raises:
        ValueError: Se o storage estiver inacessível ou alguma coluna requerida estiver ausente.
    """
    logger.info("Iniciando validações de pré-execução...")

    errors = ingestion_adapter.check_health(linkage_spec.source_table)
    if errors:
        raise ValueError(f"Falha de Infraestrutura no storage de origem: {errors}")

    required_source_cols = linkage_spec.get_required_source_columns()
    partition_col = execution_config.partitioning.partition_column
    if partition_col:
        required_source_cols = required_source_cols | {partition_col}
    ingestion_adapter.validate_source_schema(linkage_spec.source_table, required_source_cols)

    es_client = get_es_client(es_config, use_cache=False)
    validate_elasticsearch_schema(
        es_client=es_client,
        index_name=linkage_spec.target_es_index,
        required_columns=linkage_spec.get_required_target_columns(),
    )

    logger.info("Validações de pré-execução concluídas com sucesso.")


def build_linkage_use_case(
    storage_config_data: Dict[str, Any],
    execution_config_data: Dict[str, Any],
    linkage_spec_data: Dict[str, Any],
    es_config_data: Dict[str, Any],
    spark_config_data: Dict[str, Any],
) -> Tuple[RecordLinkageUseCase, SequentialLinkageSpecification, ExecutionConfig, SparkSession]:
    """Constrói e retorna o use case de linkage com todas as dependências injetadas.

    Realiza parsing das configurações, instancia todos os adapters (Spark, ES, telemetria,
    checkpoint), executa validações de pré-execução e inicializa o orquestrador de work units.

    Args:
        storage_config_data: Configuração bruta de storage (origem e saída).
        execution_config_data: Configuração de execução (job ID, particionamento, auditoria).
        linkage_spec_data: Especificação bruta do projeto de linkage (fases e regras).
        es_config_data: Configuração de conexão e estratégia de busca no Elasticsearch.
        spark_config_data: Configurações da SparkSession.

    Returns:
        Tupla com (use_case, linkage_spec, execution_config, spark_session).

    Raises:
        ValueError: Se a estratégia de busca ES for inválida ou as validações falharem.
    """
    logger.info("Building Sequential Linkage use case...")

    linkage_spec = parse_sequential_linkage_specification(linkage_spec_data)
    source_storage_config = parse_source_storage_config(storage_config_data)
    output_storage_config = parse_output_storage_config(storage_config_data)
    execution_config = parse_execution_config(execution_config_data)
    es_config = parse_es_config(es_config_data)

    job_id = execution_config.job_id
    logger.info(f"Contexto operacional resolvido. Job ID: '{job_id}'")

    strategy_name = es_config.get("search_strategy", "multisearch").lower()
    if strategy_name == "single":
        search_executor = SingleSearchExecutor()
    elif strategy_name == "multisearch":
        search_executor = MultiSearchExecutor()
    else:
        raise ValueError(f"Estratégia de busca Elasticsearch desconhecida: '{strategy_name}'.")

    spark = create_spark_session(
        app_name=f"CIDACS-RL Linkage - {linkage_spec.source_table}_{linkage_spec.target_es_index}",
        spark_config=spark_config_data.get("spark_configs", {}),
    )

    ingestion_adapter = SparkDataIngestionAdapter(
        spark_session=spark,
        storage_config=source_storage_config,
    )
    persistence_adapter = SparkDataPersistenceAdapter(output_config=output_storage_config)
    transformation_adapter = SparkDataTransformationAdapter()
    search_adapter = SparkESSearchAdapter(
        index_name=linkage_spec.target_es_index,
        es_config=es_config,
        search_executor=search_executor,
    )
    scoring_adapter = SparkScoringAdapter()
    checkpoint_adapter = JSONCheckpointAdapter(
        tracking_directory=execution_config.audit_log_path,
        project_name=linkage_spec.linkage_project_name,
    )

    telemetry_adapters = [FormattedLogTelemetryAdapter()]
    if execution_config.audit_log_path:
        job_dir = (
            f"{execution_config.audit_log_path}"
            f"/{linkage_spec.linkage_project_name}"
            f"/{job_id}"
        )
        telemetry_adapters.append(
            JsonlLinkageTelemetryAdapter(
                phases_path=f"{job_dir}/phases.jsonl",
                units_path=f"{job_dir}/units.jsonl",
                job_path=f"{job_dir}/job.jsonl",
            )
        )
    telemetry_adapter = CompositeLinkageTelemetryAdapter(telemetry_adapters)

    _run_preflight_validations(linkage_spec, execution_config, es_config, ingestion_adapter)

    orchestrator = WorkUnitOrchestrator(
        ingestion_port=ingestion_adapter,
        checkpoint_port=checkpoint_adapter,
    )
    enriched_config = orchestrator.prepare(
        table_name=linkage_spec.source_table,
        execution_config=execution_config,
    )

    use_case = RecordLinkageUseCase(
        orchestrator=orchestrator,
        persistence_port=persistence_adapter,
        transformation_port=transformation_adapter,
        get_candidates_port=search_adapter,
        scoring_port=scoring_adapter,
        checkpoint_port=checkpoint_adapter,
        telemetry_port=telemetry_adapter,
    )

    return use_case, linkage_spec, enriched_config, spark
