import dataclasses
import json
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class DeduplicationSpecification:
    """Encapsula os mapeamentos de colunas para o processo de deduplicação via grafo."""
    id_source_column: str
    id_target_column: str
    output_group_id_column: str = "cidacs_cluster_id"

    def __post_init__(self):
        if not self.id_source_column:
            raise ValueError("'id_source_column' não pode ser vazio.")
        if not self.id_target_column:
            raise ValueError("'id_target_column' não pode ser vazio.")
        if self.id_source_column == self.id_target_column:
            raise ValueError(
                f"'id_source_column' e 'id_target_column' não podem ser iguais: '{self.id_source_column}'."
            )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeduplicationSpecification":
        return cls(
            id_source_column=data["id_source_column"],
            id_target_column=data["id_target_column"],
            output_group_id_column=data.get("output_group_id_column", "cidacs_cluster_id"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)