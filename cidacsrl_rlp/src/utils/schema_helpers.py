# src/utils/schema_helpers.py

import logging
from typing import List, Dict, Any

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DataType,
)
from pyspark.sql import DataFrame

# Import necessary models for type hinting and access to configuration
from geo_cidacsrl.src.linkage.models import (
    SequentialBlockingWorkflow,
    BlockingPhase
    # ComparisonRule is not directly type-hinted here, accessed via phase_config.rules
)

# --- Constants for special field names ---
CANDIDATE_ES_DOC_ID_FIELD = "_candidate_elasticsearch_document_id" # Prefixed with underscore for "internal" feel
ES_HIT_SCORE_FIELD = "_elasticsearch_hit_score" # Prefixed with underscore
COMPOSITE_SCORE_FIELD = "match_score" # More generic term for final score
LINKAGE_PHASE_NAME_FIELD = "match_phase_name" # More generic term

logger = logging.getLogger(__name__)

def define_phase_output_schema(
    source_df_schema: StructType,
    workflow_config: SequentialBlockingWorkflow,
    phase_config: BlockingPhase,
) -> StructType:
    """
    Define o schema Spark para o DataFrame resultante de uma única fase de linkage (blocking phase).
    Este schema é para o DataFrame que contém todos os pares fonte-candidato pontuados,
    produzido pela função `process_partition_for_phase`.

    Args:
        source_df_schema (StructType): O schema do DataFrame da fonte original.
        workflow_config (SequentialBlockingWorkflow): A configuração do workflow de linkage.
        phase_config (BlockingPhase): A configuração da fase de blocking específica.

    Returns:
        StructType: O schema Spark definido para a saída da fase.
    """
    logger.debug(f"Defining output schema for linkage phase: '{phase_config.phase_name}'")

    output_fields: List[StructField] = []

    # 1. Add ONLY the ID column from the original source table (id_source_table)
    id_source_field_name = workflow_config.id_source_table
    id_source_field = next(
        (field for field in source_df_schema.fields if field.name == id_source_field_name), None
    )

    if id_source_field:
        output_fields.append(id_source_field)
        logger.debug(f"  Source ID field ('{id_source_field_name}') added to phase output schema.")
    else:
        # This should ideally not happen if configurations are correct and source DataFrame is valid.
        logger.error(
            f"Source ID field '{id_source_field_name}' not found in source DataFrame schema! "
            f"Adding as StringType by default, but this could cause issues downstream. "
            f"Source schema fields: {[f.name for f in source_df_schema.fields]}"
        )
        output_fields.append(StructField(id_source_field_name, StringType(), True))

    # Create a map for type inference of candidate fields, based on source fields
    source_column_type_map: Dict[str, DataType] = {
        field.name: field.dataType for field in source_df_schema.fields
    }

    # 2. Add the application-level ID of the candidate (prefixed)
    candidate_app_id_df_col_name = workflow_config.prefixed_id_target_table
    # Infer type from the original target ID column in the source (if analogous) or default to StringType
    candidate_app_id_col_type = source_column_type_map.get(
        workflow_config.id_target_table, StringType()
    )
    output_fields.append(
        StructField(candidate_app_id_df_col_name, candidate_app_id_col_type, True)
    )
    logger.debug(
        f"  Candidate application ID field added: '{candidate_app_id_df_col_name}' (Type: {candidate_app_id_col_type})"
    )

    # Add the internal Elasticsearch document ID of the candidate
    output_fields.append(StructField(CANDIDATE_ES_DOC_ID_FIELD, StringType(), True))
    logger.debug(
        f"  Candidate Elasticsearch document ID field added: '{CANDIDATE_ES_DOC_ID_FIELD}'"
    )

    # 3. Add other candidate data fields (prefixed) and individual similarity scores
    # Keep track of already added prefixed candidate fields to avoid duplicates if ID field is also in rules
    added_prefixed_candidate_fields = {candidate_app_id_df_col_name, CANDIDATE_ES_DOC_ID_FIELD}

    candidate_prefix = workflow_config._candidate_prefix

    for rule in phase_config.rules:
        prefixed_candidate_data_field_name = f"{candidate_prefix}{rule.target_column}"

        if prefixed_candidate_data_field_name not in added_prefixed_candidate_fields:
            # Infer type for the candidate's data field from the corresponding source column in the rule
            source_col_for_type_inference = rule.source_column
            candidate_field_type = source_column_type_map.get(
                source_col_for_type_inference, StringType() # Default to StringType if not found
            )
            output_fields.append(
                StructField(
                    prefixed_candidate_data_field_name, candidate_field_type, True
                )
            )
            added_prefixed_candidate_fields.add(prefixed_candidate_data_field_name)
            logger.debug(
                f"  Candidate data field added: '{prefixed_candidate_data_field_name}' "
                f"(Type: {candidate_field_type}, inferred from source field '{source_col_for_type_inference}')"
            )

        # Add similarity score field for the current rule
        # Naming convention: sim_ + source_column_name (as it represents similarity with that source column)
        sim_score_field_name = f"sim_{rule.source_column}" # Or consider f"sim_{rule.source_column}_vs_{rule.target_column}" for more clarity
        output_fields.append(StructField(sim_score_field_name, DoubleType(), True))
        logger.debug(
            f"  Similarity score field added: '{sim_score_field_name}' for rule on '{rule.source_column}'"
        )

    # 4. Add field for the raw Elasticsearch hit score
    output_fields.append(StructField(ES_HIT_SCORE_FIELD, DoubleType(), True))
    logger.debug(
        f"  Raw Elasticsearch hit score field ('_score') added as: '{ES_HIT_SCORE_FIELD}'"
    )

    # 5. Add field for the final composite linkage score for this phase
    output_fields.append(StructField(COMPOSITE_SCORE_FIELD, DoubleType(), True))
    logger.debug(
        f"  Final composite score field for the phase added: '{COMPOSITE_SCORE_FIELD}'"
    )

    # 6. Add field for the name of the linkage phase
    output_fields.append(StructField(LINKAGE_PHASE_NAME_FIELD, StringType(), False)) # Usually not nullable
    logger.debug(
        f"  Linkage phase name field added: '{LINKAGE_PHASE_NAME_FIELD}'"
    )

    final_schema = StructType(output_fields)
    logger.info(
        f"Schema defined for phase '{phase_config.phase_name}' with {len(final_schema.fields)} fields."
    )
    return final_schema

def define_workflow_output_schema(
    source_df_schema: StructType,
    workflow_config: SequentialBlockingWorkflow,
    phase_config: BlockingPhase, # Schema is usually defined per phase
    include_phase_name: bool = True # Kept for compatibility, but phase name is now part of phase_output_schema
) -> StructType:
    """
    Gera o schema completo para a saída de uma fase do workflow, destinado à escrita.
    Este schema é normalmente gerado por fase. Se um schema "final" consolidado do workflow
    for necessário (após todas as fases), ele pode requerer lógica adicional para combinar
    colunas de múltiplas fases ou selecionar colunas específicas.

    Args:
        source_df_schema (StructType): O schema do DataFrame da fonte original.
        workflow_config (SequentialBlockingWorkflow): A configuração do workflow de linkage.
        phase_config (BlockingPhase): A configuração da fase de blocking específica.
        include_phase_name (bool): Se True (padrão), garante que o campo `LINKAGE_PHASE_NAME_FIELD` está presente.
                                   (Este campo já é incluído por `define_phase_output_schema`).

    Returns:
        StructType: O schema Spark completo para a saída da fase.
    """
    # define_phase_output_schema already includes the phase name field.
    base_schema = define_phase_output_schema(source_df_schema, workflow_config, phase_config)

    if include_phase_name: # Double check, though it should already be there
        field_names = [f.name for f in base_schema.fields]
        if LINKAGE_PHASE_NAME_FIELD not in field_names:
            # This case should be rare if define_phase_output_schema is correct
            logger.warning(f"Field '{LINKAGE_PHASE_NAME_FIELD}' was missing from base phase schema; adding it now.")
            extended_fields = list(base_schema.fields)
            extended_fields.append(StructField(LINKAGE_PHASE_NAME_FIELD, StringType(), False))
            return StructType(extended_fields)

    return base_schema

def get_score_fields(phase_config: BlockingPhase) -> List[str]:
    """
    Retorna uma lista dos nomes dos campos de score para uma dada fase de blocking.
    Inclui scores de similaridade individuais por regra, o score do hit do Elasticsearch,
    e o score composto final.

    Args:
        phase_config (BlockingPhase): A configuração da fase de blocking.

    Returns:
        List[str]: Uma lista de nomes de campos de score.
    """
    sim_score_fields = [f"sim_{rule.source_column}" for rule in phase_config.rules]
    # These are the standard score fields generated for each pair
    return sim_score_fields + [ES_HIT_SCORE_FIELD, COMPOSITE_SCORE_FIELD]

def validate_schema(df: DataFrame, expected_schema: StructType, strict_order: bool = False) -> bool:
    """
    Valida se o schema de um DataFrame corresponde a um schema esperado.

    Args:
        df (DataFrame): O DataFrame a ser validado.
        expected_schema (StructType): O schema Spark esperado.
        strict_order (bool): Se True, a ordem dos campos também deve corresponder.
                             Se False (padrão), apenas a presença, nome e tipo dos campos são verificados.

    Returns:
        bool: True se o schema do DataFrame corresponder ao esperado, False caso contrário.
    """
    actual_schema = df.schema
    if len(actual_schema.fields) != len(expected_schema.fields):
        logger.warning(f"Schema validation failed: Mismatch in number of fields. "
                       f"Actual: {len(actual_schema.fields)}, Expected: {len(expected_schema.fields)}")
        return False

    expected_fields_dict = {field.name: field for field in expected_schema.fields}
    actual_fields_dict = {field.name: field for field in actual_schema.fields}

    if set(actual_fields_dict.keys()) != set(expected_fields_dict.keys()):
        logger.warning(f"Schema validation failed: Mismatch in field names. "
                       f"Actual names: {set(actual_fields_dict.keys())}, "
                       f"Expected names: {set(expected_fields_dict.keys())}")
        return False

    for i, actual_field in enumerate(actual_schema.fields):
        expected_field_instance = expected_fields_dict.get(actual_field.name)
        if not expected_field_instance:
            # This case should be caught by the key set comparison above, but as a safeguard:
            logger.warning(f"Schema validation failed: Field '{actual_field.name}' present in actual schema but not in expected.")
            return False

        if strict_order:
            expected_field_at_pos = expected_schema.fields[i]
            if actual_field.name != expected_field_at_pos.name:
                logger.warning(f"Schema validation failed (strict order): Field name mismatch at position {i}. "
                               f"Actual: '{actual_field.name}', Expected: '{expected_field_at_pos.name}'.")
                return False
            if actual_field.dataType != expected_field_at_pos.dataType:
                logger.warning(f"Schema validation failed (strict order): DataType mismatch for field '{actual_field.name}' at position {i}. "
                               f"Actual: {actual_field.dataType}, Expected: {expected_field_at_pos.dataType}.")
                return False
            if actual_field.nullable != expected_field_at_pos.nullable:
                 logger.warning(f"Schema validation failed (strict order): Nullability mismatch for field '{actual_field.name}' at position {i}. "
                               f"Actual: {actual_field.nullable}, Expected: {expected_field_at_pos.nullable}.")
                 return False

        else: # Not strict order, compare by name
            if actual_field.dataType != expected_field_instance.dataType:
                logger.warning(f"Schema validation failed: DataType mismatch for field '{actual_field.name}'. "
                               f"Actual: {actual_field.dataType}, Expected: {expected_field_instance.dataType}.")
                return False
            if actual_field.nullable != expected_field_instance.nullable:
                 logger.warning(f"Schema validation failed: Nullability mismatch for field '{actual_field.name}'. "
                               f"Actual: {actual_field.nullable}, Expected: {expected_field_instance.nullable}.")
                 return False
    return True

def get_id_fields(workflow_config: SequentialBlockingWorkflow) -> List[str]:
    """
    Retorna os nomes dos campos de ID relevantes para o workflow:
    o ID da tabela fonte e o ID (prefixado) da tabela candidata.

    Args:
        workflow_config (SequentialBlockingWorkflow): A configuração do workflow.

    Returns:
        List[str]: Uma lista contendo o nome do campo de ID da fonte e o nome do campo de ID do candidato prefixado.
    """
    return [workflow_config.id_source_table, workflow_config.prefixed_id_target_table]

# Example usage (can be kept as comments for developers or used in unit tests):
# source_schema = ... # Spark StructType of the source DataFrame
# workflow_cfg = ...  # Instance of SequentialBlockingWorkflow
# phase_cfg = ...     # Instance of BlockingPhase
#
# phase_output_schema = define_phase_output_schema(source_schema, workflow_cfg, phase_cfg)
# logger.info(f"Phase output schema: {phase_output_schema.simpleString()}")
#
# workflow_out_schema = define_workflow_output_schema(source_schema, workflow_cfg, phase_cfg)
# logger.info(f"Workflow output schema for writing: {workflow_out_schema.simpleString()}")
#
# scores = get_score_fields(phase_cfg)
# ids = get_id_fields(workflow_cfg)
#
# df_to_validate = ... # A DataFrame that should match one of the defined schemas
# if not validate_schema(df_to_validate, phase_output_schema):
#     logger.error("DataFrame does not match the expected phase output schema!")