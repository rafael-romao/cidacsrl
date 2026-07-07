import dataclasses
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from cidacsrl.config.models.indexed_dataset_filter import (
    IndexedDatasetFilterItem,
)
from cidacsrl.domain.linkage.matching_rules import (
    BlockingPhase,
    ComparisonRule,
)

logger = logging.getLogger("Entity: LinkageSpecification")


def _dedupe_fields(fields: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(field_name for field_name in fields if field_name))


@dataclass
class BlockingPhaseTargetFields:
    """Campos do índice Elasticsearch requeridos por uma fase de blocagem.

    Attributes:
        comparison_fields: Campos usados diretamente nas regras de comparação.
        required_fields: Campos obrigatórios no resultado (ex.: ID do alvo).
        extra_fields: Campos adicionais a retornar além dos requeridos e de comparação.
    """

    comparison_fields: List[str] = field(default_factory=list)
    required_fields: List[str] = field(default_factory=list)
    extra_fields: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.comparison_fields = _dedupe_fields(self.comparison_fields)
        self.required_fields = _dedupe_fields(self.required_fields)
        self.extra_fields = _dedupe_fields(self.extra_fields)

    @property
    def fetch_fields(self) -> List[str]:
        """Todos os campos a buscar no ES, sem duplicatas, na ordem (required, comparison, extra)."""
        return _dedupe_fields(
            [
                *self.required_fields,
                *self.comparison_fields,
                *self.extra_fields,
            ]
        )

    @property
    def result_fields(self) -> List[str]:
        """Todos os campos a projetar no resultado final, sem duplicatas."""
        return _dedupe_fields([
            *self.required_fields,
            *self.comparison_fields,
            *self.extra_fields
        ])

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)


@dataclass
class BlockingPhaseContext:
    """Contexto de execução de uma fase de blocagem, resolvido a partir da especificação.

    Attributes:
        phase_name: Nome único da fase.
        phase_description: Descrição opcional da fase.
        enabled: Se False, a fase é pulada no pipeline. Defaults to True.
        candidate_limit: Número máximo de candidatos por registro de origem. Defaults to 10.
        strong_match_score_threshold: Score mínimo para considerar um par como match forte. Defaults to 0.9.
        rules: Regras de comparação com pesos e funções de similaridade.
        target_fields: Campos do índice ES requeridos pela fase.
        indexed_dataset_filter: Filtros estáticos aplicados a todas as queries ES da fase
            (já resolvidos: filtros do workflow + filtros da fase, combinados).
        source_output_fields: Campos da tabela fonte a incluir no output da fase.
    """

    phase_name: str
    phase_description: Optional[str] = None
    enabled: bool = True
    candidate_limit: int = 10
    strong_match_score_threshold: float = 0.9
    rules: List[ComparisonRule] = field(default_factory=list)
    target_fields: BlockingPhaseTargetFields = field(
        default_factory=BlockingPhaseTargetFields
    )
    indexed_dataset_filter: Optional[List[IndexedDatasetFilterItem]] = None
    source_output_fields: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.source_output_fields = _dedupe_fields(self.source_output_fields)

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)

@dataclass
class SequentialLinkageSpecification:
    """
    Configuração de nível superior para um workflow de linkage sequencial,
    baseado em múltiplas fases de blocking.

    Attributes:
        indexed_dataset_filter: Filtros estáticos aplicados por padrão a todas as fases.
            Combinado (não substituído) com o `indexed_dataset_filter` de cada
            `BlockingPhase`, se houver — ver `build_blocking_phase_context`.
    """
    source_table: str
    id_source_table: str
    target_es_index: str
    id_target_table: str
    indexed_dataset_filter: Optional[List[Dict[str, Any]]] = None
    workflow_name: Optional[str] = None
    workflow_description: Optional[str] = None
    source_es_index_name: Optional[str] = None
    blocking_phases: List[BlockingPhase] = field(default_factory=list)
    extra_target_fields: Optional[List[str]] = field(default_factory=list)


    def __post_init__(self):
        if not self.blocking_phases:
            raise ValueError("At least one BlockingPhase must be defined in the workflow.")
        
        logger.debug(f"Source table name: {self.source_table}")
        logger.debug(f"Target ES Index: {self.target_es_index}")
    
    @property
    def linkage_project_name(self) -> str:
        return f"linkage_{self.source_table}_{self.target_es_index}"

    def build_blocking_phase_context(self, phase: BlockingPhase) -> BlockingPhaseContext:
        """Constrói o contexto de execução de uma fase de blocagem.

        Combina os filtros do nível global com os da fase (merge aditivo, não
        substituição) e agrega os campos alvo com o ID do índice e os campos
        extras da especificação.

        Args:
            phase: Configuração da fase a converter em contexto de execução.

        Returns:
            BlockingPhaseContext pronto para uso pelo pipeline.
        """
        workflow_filters = self.indexed_dataset_filter or []
        phase_filters = getattr(phase, "indexed_dataset_filter", None) or []
        raw_filters = workflow_filters + phase_filters

        parsed_filters = None
        if raw_filters:
            parsed_filters = [IndexedDatasetFilterItem.from_dict(f) for f in raw_filters]

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
            ),
            indexed_dataset_filter=parsed_filters
        )

    def build_blocking_phase_contexts(self) -> List[BlockingPhaseContext]:
        """Constrói os contextos de execução de todas as fases da especificação.

        Returns:
            Lista de BlockingPhaseContext na mesma ordem das fases configuradas.
        """
        return [
            self.build_blocking_phase_context(phase)
            for phase in self.blocking_phases
        ]
    
    def get_required_target_columns(self) -> set[str]:
        """Coleta todas as colunas requeridas do índice Elasticsearch.

        Agrega os fetch_fields de todas as fases e inclui o ID do alvo.

        Returns:
            Conjunto de nomes de colunas que devem existir no índice ES.
        """
        required_fields = set()
        for phase_context in self.build_blocking_phase_contexts():
            for field in phase_context.target_fields.fetch_fields:
                required_fields.add(field)
        required_fields.add(self.id_target_table)
        return required_fields

    def get_required_source_columns(self) -> set[str]:
        """Coleta todas as colunas requeridas da tabela de origem.

        Inclui o ID da fonte e as source_columns de todas as regras de todas as fases.

        Returns:
            Conjunto de nomes de colunas que devem existir na tabela fonte.
        """
        required_fields = {self.id_source_table}
        for phase in self.blocking_phases:
            for rule in phase.rules:
                required_fields.add(rule.source_column)
        return required_fields


    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'SequentialLinkageSpecification':
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

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)