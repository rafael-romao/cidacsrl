import logging
from typing import Any, Dict, Set

from core.infra.spark.spark_factory import spark_session_context
from cidacsrl.config.loader import (
    parse_sequential_linkage_specification,
    parse_es_config,
    parse_source_storage_config,
    parse_output_storage_config,
    parse_execution_config
)

from core.infra.adapters.outbound.elasticsearch.client import get_es_client, validate_elasticsearch_schema
from core.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from core.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from core.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter
from core.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from core.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from core.infra.adapters.outbound.elasticsearch.executors import SingleSearchExecutor, MultiSearchExecutor
from core.infra.adapters.outbound.json_checkpoint_adapter import JSONCheckpointAdapter
from core.infra.adapters.outbound.formatted_log_telemetry_adapter import FormattedLogTelemetryAdapter
from core.infra.adapters.outbound.jsonl_telemetry_adapter import JsonlLinkageTelemetryAdapter
from core.infra.adapters.outbound.composite_telemetry_adapter import CompositeLinkageTelemetryAdapter

from cidacsrl.application.linkage.work_unit_orchestrator import WorkUnitOrchestrator
from cidacsrl.application.linkage.record_linkage_use_case import RecordLinkageUseCase
from cidacsrl.domain.linkage.linkage_specification import SequentialLinkageSpecification
from core.infra.elasticsearch.models.service_config import ElasticsearchConfig
from cidacsrl.config.models.execution_config import ExecutionConfig

logger = logging.getLogger("Bootstrapper: Record Linkage")


def _run_preflight_validations(
    linkage_spec: SequentialLinkageSpecification,
    execution_config: ExecutionConfig,
    es_config: ElasticsearchConfig,
    ingestion_adapter: SparkDataIngestionAdapter,
) -> None:
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
        required_columns=linkage_spec.get_required_target_columns()
    )

    logger.info("Validações de pré-execução concluídas com sucesso.")


def bootstrap_sequential_linkage(
    storage_config_data: Dict[str, Any],
    execution_config_data: Dict[str, Any],
    linkage_spec_data: Dict[str, Any],
    es_config_data: Dict[str, Any],
    spark_config_data: Dict[str, Any],
):
    logger.info("Bootstrapping Sequential Linkage...")

    linkage_spec = parse_sequential_linkage_specification(linkage_spec_data)
    source_storage_config = parse_source_storage_config(storage_config_data)
    output_storage_config = parse_output_storage_config(storage_config_data)
    execution_config = parse_execution_config(execution_config_data)
    es_config = parse_es_config(es_config_data)

    job_id = execution_config.job_id
    logger.info(f"Contexto operacional resolvido com sucesso. Job ID ativo: '{job_id}'")

    strategy_name = es_config.get("search_strategy", "multisearch").lower()
    if strategy_name == "single":
        search_executor = SingleSearchExecutor()
    elif strategy_name == "multisearch":
        search_executor = MultiSearchExecutor()
    else:
        raise ValueError(f"Estratégia de busca Elasticsearch desconhecida: '{strategy_name}'.")

    with spark_session_context(
        app_name=f"CIDACS-RL Linkage - {linkage_spec.source_table}_{linkage_spec.target_es_index}",
        spark_config=spark_config_data.get("spark_configs", {})
    ) as spark_session:

        ingestion_adapter = SparkDataIngestionAdapter(
            spark_session=spark_session,
            storage_config=source_storage_config
        )
        persistence_adapter = SparkDataPersistenceAdapter(output_config=output_storage_config)
        transformation_adapter = SparkDataTransformationAdapter()
        search_adapter = SparkESSearchAdapter(
            index_name=linkage_spec.target_es_index,
            es_config=es_config,
            search_executor=search_executor
        )
        scoring_adapter = SparkScoringAdapter()
        checkpoint_adapter = JSONCheckpointAdapter(
            tracking_directory=execution_config.audit_log_path,
            project_name=linkage_spec.linkage_project_name
        )
        telemetry_adapters = [FormattedLogTelemetryAdapter()]
        if execution_config.audit_log_path:
            job_dir = (
                f"{execution_config.audit_log_path}"
                f"/{linkage_spec.linkage_project_name}"
                f"/{job_id}"
            )
            telemetry_adapters.append(JsonlLinkageTelemetryAdapter(
                phases_path=f"{job_dir}/phases.jsonl",
                units_path=f"{job_dir}/units.jsonl",
                job_path=f"{job_dir}/job.jsonl",
            ))
        telemetry_adapter = CompositeLinkageTelemetryAdapter(telemetry_adapters)

        _run_preflight_validations(linkage_spec, execution_config, es_config, ingestion_adapter)

        orchestrator = WorkUnitOrchestrator(
            ingestion_port=ingestion_adapter,
            checkpoint_port=checkpoint_adapter
        )
        enriched_config = orchestrator.prepare(
            table_name=linkage_spec.source_table,
            execution_config=execution_config
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

        use_case.execute(
            specification=linkage_spec,
            job_id=job_id,
            execution_config=enriched_config
        )

        logger.info("Linkage Execution finished successfully.")