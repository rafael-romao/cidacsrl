import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from cidacsrl_rlp.cidacsrl.domain.models.rules import BlockingPhase, ComparisonRule

logger = logging.getLogger(__name__)


def _dedupe_fields(fields: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(field_name for field_name in fields if field_name))


@dataclass
class BlockingPhaseTargetFields:
    comparison_fields: List[str] = field(default_factory=list)
    required_fields: List[str] = field(default_factory=list)
    extra_fields: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.comparison_fields = _dedupe_fields(self.comparison_fields)
        self.required_fields = _dedupe_fields(self.required_fields)
        self.extra_fields = _dedupe_fields(self.extra_fields)

    @property
    def fetch_fields(self) -> List[str]:
        return _dedupe_fields(
            [
                *self.required_fields,
                *self.comparison_fields,
                *self.extra_fields,
            ]
        )

    @property
    def result_fields(self) -> List[str]:
        return _dedupe_fields([
            *self.required_fields, 
            *self.comparison_fields,
            *self.extra_fields       
        ])


@dataclass
class BlockingPhaseContext:
    phase_name: str
    phase_description: Optional[str] = None
    enabled: bool = True
    candidate_limit: int = 10
    strong_match_score_threshold: float = 0.9
    rules: List[ComparisonRule] = field(default_factory=list)
    target_fields: BlockingPhaseTargetFields = field(
        default_factory=BlockingPhaseTargetFields
    )
    source_output_fields: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.source_output_fields = _dedupe_fields(self.source_output_fields)

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
    extra_target_fields: Optional[List[str]] = field(default_factory=list)


    def __post_init__(self):
        if not self.blocking_phases:
            raise ValueError("At least one BlockingPhase must be defined in the workflow.")
        
        logger.debug(f"Source table name: {self.source_table}")
        logger.debug(f"Target ES Index: {self.target_es_index}")

    def build_blocking_phase_context(self, phase: BlockingPhase) -> BlockingPhaseContext:
        return BlockingPhaseContext(
            phase_name=phase.phase_name,
            phase_description=phase.phase_description,
            enabled=phase.enabled,
            candidate_limit=phase.candidate_limit,
            strong_match_score_threshold=phase.strong_match_score_threshold,
            rules=phase.rules,
            target_fields=BlockingPhaseTargetFields(
                comparison_fields=phase.comparison_target_fields,
                required_fields=[self.id_target_table],
                extra_fields=list(self.extra_target_fields or []),
            )
        )

    def build_blocking_phase_contexts(self) -> List[BlockingPhaseContext]:
        return [
            self.build_blocking_phase_context(phase)
            for phase in self.blocking_phases
        ]
    
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