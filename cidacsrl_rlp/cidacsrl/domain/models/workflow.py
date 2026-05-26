import logging
from cidacsrl_rlp.cidacsrl.domain.models.rules import BlockingPhase
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)

@dataclass
class SequentialBlockingWorkflow:
    """
    Configuração de nível superior para um workflow de linkage sequencial,
    baseado em múltiplas fases de blocking.
    """
    source_table: str
    id_source_table: str
    target_es_index: str
    id_target_table: str
    indexed_dataset_filter: Optional[List[Dict[str, Any]]] = None
    workflow_name: Optional[str] = None
    workflow_description: Optional[str] = None
    source_es_index_name: Optional[str] = None
    source_es_partition_filter_field: Optional[str] = None
    target_es_partition_filter_field: Optional[str] = None
    blocking_phases: List[BlockingPhase] = field(default_factory=list)
    output_base_path: Optional[str] = None
    final_output_filename: Optional[str] = "final_linked_pairs.parquet" 
    intermediate_results_enabled: bool = True    
    final_output_columns_source: Optional[List[str]] = field(default_factory=list)
    final_output_columns_target: Optional[List[str]] = field(default_factory=list)    
    _candidate_prefix: str = field(default="candidate_", init=False, repr=False)    
    prefixed_id_target_table: str = field(init=False)


    def __post_init__(self):
        if not self.blocking_phases:
            raise ValueError("At least one BlockingPhase must be defined in the workflow.")

        self.prefixed_id_target_table = f"{self._candidate_prefix}{self.id_target_table}"

        if self.id_source_table == self.prefixed_id_target_table:
            raise ValueError(f"id_source_table ('{self.id_source_table}') cannot be the same as "
                             f"prefixed_id_target_table ('{self.prefixed_id_target_table}'). "
                             f"Consider changing id_target_table or _candidate_prefix.")

        
        logger.debug(f"Prefixed ID target table: {self.prefixed_id_target_table}")
        logger.debug(f"Source table name: {self.source_table}")
        logger.debug(f"Target ES Index: {self.target_es_index}")
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'SequentialBlockingWorkflow':
        config_dict = config_dict.copy()
        blocking_phases_data = config_dict.pop("blocking_phases", None)
        if blocking_phases_data is None:
            raise ValueError("'blocking_phases' is required in workflow configuration.")
        if not isinstance(blocking_phases_data, list):
            raise ValueError("'blocking_phases' must be a list of phase configurations.")

        blocking_phases: List[BlockingPhase] = []
        for i, phase_data in enumerate(blocking_phases_data):
            if not isinstance(phase_data, dict):
                raise ValueError(
                    f"Each item in 'blocking_phases' must be a dictionary. "
                    f"Invalid item at index {i}: {type(phase_data).__name__}."
                )
            try:
                blocking_phases.append(BlockingPhase.from_dict(phase_data))
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"Invalid blocking phase at index {i}: {e}"
                ) from e

        return cls(blocking_phases=blocking_phases, **config_dict)