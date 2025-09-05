import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)

@dataclass
class WorkflowConfig:
    """Configuração do workflow de linkage sequencial."""
    linkage_config_path: str
    es_config_path: str
    spark_config_path: str
    output_data_dir: str
    source_data_path: str
    sample_fraction: Optional[float] = None
    sample_seed: int = 42
    spark_checkpoint_base_dir: Optional[str] = None
    current_partition_value: Optional[str] = None



@dataclass
class ComparisonRule:
    """
    Define uma regra de comparação entre uma coluna da fonte e uma coluna do alvo.

    Atributos:
        source_column (str): Nome da coluna na tabela de origem.
        target_column (str): Nome da coluna na tabela de destino (ex: campo no Elasticsearch).
        es_clause_type (str): Tipo de cláusula Elasticsearch a ser usada (ex: 'must', 'should', 'filter').
        query_type (str): Tipo de query Elasticsearch (ex: 'match', 'term', 'prefix').
        similarity (str): Chave da função de similaridade a ser usada para pontuação (ex: 'jaro_winkler', 'exact').
        weight (float): Peso desta regra no cálculo do score composto.
        penalty (float): Penalidade a ser aplicada se uma das colunas for nula (default: 0.0).
        is_fuzzy (bool): Indica se a query `match` deve usar `fuzziness` (default: False).
        boost (Optional[float]): Fator de boost para a cláusula na query Elasticsearch (default: 1.0).
    """
    source_column: str
    target_column: str
    es_clause_type: str
    query_type: str
    similarity: str
    weight: float
    penalty: float = 0.0
    is_fuzzy: bool = False
    boost: Optional[float] = 1.0

@dataclass
class BlockingPhase:
    """
    Configura uma fase (ou estratégia) de blocking dentro do workflow de linkage.
    Cada fase define um conjunto de regras para encontrar candidatos e calcular scores.

    Atributos:
        phase_name (str): Nome único para a fase de blocking.
        phase_description (Optional[str]): Descrição opcional da fase.
        enabled (bool): Se esta fase está habilitada para execução (default: True).
        candidate_limit (int): Número máximo de candidatos a serem recuperados do Elasticsearch por registro fonte (default: 10).
        strong_match_score_threshold (float): Limiar de score (entre 0 e 1) para considerar um par como "strong match" nesta fase (default: 0.9).
        rules (List[ComparisonRule]): Lista de `ComparisonRule` para esta fase.
    """
    phase_name: str
    phase_description: Optional[str] = None
    enabled: bool = True
    candidate_limit: int = 10
    strong_match_score_threshold: float = 0.9
    rules: List[ComparisonRule] = field(default_factory=list)

    def __post_init__(self):
        if not self.rules:
            logger.warning(f"BlockingPhase '{self.phase_name}' has no ComparisonRules defined.")
        if not 0 <= self.strong_match_score_threshold <= 1:
            # This validation ensures the threshold is a valid probability/score.
            raise ValueError("strong_match_score_threshold must be between 0 and 1.")

@dataclass
class SequentialBlockingWorkflow:
    """
    Configuração de nível superior para um workflow de linkage sequencial,
    baseado em múltiplas fases de blocking.
    """
    workflow_name: str
    source_table: str  # Identifier for the source dataset
    id_source_table: str  # Name of the unique ID column in the source table
    target_es_index: str  # Name of the target Elasticsearch index
    id_target_table: str  # Name of the unique ID field in the target Elasticsearch documents

    workflow_description: Optional[str] = None
    source_es_index_name: Optional[str] = None # Optional: if source is also an ES index for some operations
    source_es_partition_filter_field: Optional[str] = None # Field used for partitioning source data if applicable
    target_es_partition_filter_field: Optional[str] = None # Field in target ES index for partition filtering

    blocking_phases: List[BlockingPhase] = field(default_factory=list)

    output_base_path: Optional[str] = None # Base path for saving linkage results
    final_output_filename: Optional[str] = "final_linked_pairs.parquet" # Filename for the consolidated output
    intermediate_results_enabled: bool = True # Whether to save results from each phase

    # Columns to include in the final output from source and target respectively
    final_output_columns_source: Optional[List[str]] = field(default_factory=list)
    final_output_columns_target: Optional[List[str]] = field(default_factory=list)

    # Internal field: prefix for candidate columns in the output DataFrame
    _candidate_prefix: str = field(default="candidate_", init=False, repr=False)
    # Internal field: prefixed name for the target ID column
    prefixed_id_target_table: str = field(init=False)


    def __post_init__(self):
        if not self.blocking_phases:
            raise ValueError("At least one BlockingPhase must be defined in the workflow.")

        self.prefixed_id_target_table = f"{self._candidate_prefix}{self.id_target_table}"

        if self.id_source_table == self.prefixed_id_target_table:
            raise ValueError(f"id_source_table ('{self.id_source_table}') cannot be the same as "
                             f"prefixed_id_target_table ('{self.prefixed_id_target_table}'). "
                             f"Consider changing id_target_table or _candidate_prefix.")

        logger.debug(f"SequentialBlockingWorkflow '{self.workflow_name}' initialized.")
        logger.debug(f"  Prefixed ID target table: {self.prefixed_id_target_table}")
        logger.debug(f"  Source table name: {self.source_table}")
        logger.debug(f"  Target ES Index: {self.target_es_index}")


def _create_comparison_rules(rules_data: List[Dict[str, Any]]) -> List[ComparisonRule]:
    """Internal helper to create a list of ComparisonRule objects from dictionaries."""
    return [ComparisonRule(**rule) for rule in rules_data]

def _create_blocking_phases(phases_data: List[Dict[str, Any]]) -> List[BlockingPhase]:
    """Internal helper to create a list of BlockingPhase objects from dictionaries."""
    phase_list = []
    for phase_config_dict in phases_data:
        rules_data = phase_config_dict.pop("rules", [])
        # Expects phase_config_dict to already contain phase_name, phase_description, etc.
        # Any mapping from old names (e.g., strategy_name) to new names (phase_name)
        # should have happened before this point or is assumed to be handled by the caller
        # if this function is used in a more generic context.
        # For load_workflow_from_dict, the YAML is expected to use the new names.
        phase = BlockingPhase(rules=_create_comparison_rules(rules_data), **phase_config_dict)
        phase_list.append(phase)
    return phase_list

def load_workflow_from_dict(config: Dict[str, Any]) -> SequentialBlockingWorkflow:
    """
    Carrega a configuração do workflow a partir de um dicionário (ex: YAML parseado).
    Assume que o dicionário de entrada já utiliza os nomes de campo corretos
    conforme definido nos dataclasses `SequentialBlockingWorkflow`, `BlockingPhase` e `ComparisonRule`.

    Args:
        config: Dicionário contendo a configuração completa do workflow.

    Returns:
        Uma instância de `SequentialBlockingWorkflow`.

    Raises:
        ValueError: Se campos obrigatórios estiverem ausentes ou se ocorrerem erros de tipo
                    durante a instanciação dos dataclasses.
        TypeError: Se chaves inesperadas forem passadas para os construtores dos dataclasses.
    """
    logger.debug(f"Loading SequentialBlockingWorkflow from dictionary: {config.get('workflow_name', 'N/A')}")

    # The config dictionary is expected to have a "blocking_phases" key
    # containing a list of phase configurations.
    # Old keys like "strategies" are no longer mapped here; the input 'config' dict
    # must use "blocking_phases".
    if "blocking_phases" in config and isinstance(config["blocking_phases"], list):
         config["blocking_phases"] = _create_blocking_phases(config["blocking_phases"])
    elif "blocking_phases" not in config:
        logger.warning("'blocking_phases' key not found in workflow configuration. Assuming no phases if allowed by model.")
        # The SequentialBlockingWorkflow model itself will raise an error if blocking_phases is empty and required.
    else: # 'blocking_phases' exists but is not a list
        raise ValueError("'blocking_phases' must be a list of phase configurations.")


    try:
        workflow = SequentialBlockingWorkflow(**config)
        logger.info(f"SequentialBlockingWorkflow configuration '{workflow.workflow_name}' parsed successfully.")
        return workflow
    except TypeError as e: # Handles errors like unexpected keyword arguments
        logger.error(f"Type error creating SequentialBlockingWorkflow: {e}. Check for unexpected or misspelled keys in the configuration.", exc_info=True)
        logger.error(f"Provided configuration dictionary keys: {list(config.keys())}")

        expected_fields = SequentialBlockingWorkflow.__dataclass_fields__.keys()
        logger.error(f"Expected fields for SequentialBlockingWorkflow: {list(expected_fields)}")

        extra_fields = [k for k in config if k not in expected_fields and k != "blocking_phases"] # blocking_phases is handled separately
        if "blocking_phases" in config and not isinstance(config["blocking_phases"], List): # If it was popped and processed.
            if not all(isinstance(p, BlockingPhase) for p in config["blocking_phases"]): # type: ignore
                 extra_fields.append("blocking_phases (due to incorrect type after processing)")


        if extra_fields:
            logger.error(f"Potential unexpected or unprocessed extra fields provided: {extra_fields}")
        raise # Re-raise the TypeError with more context
    except ValueError as e: # Handles errors from __post_init__ or other value issues
        logger.error(f"Value error creating SequentialBlockingWorkflow: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unknown error loading workflow configuration: {e}", exc_info=True)
        raise