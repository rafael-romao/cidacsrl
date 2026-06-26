from dataclasses import dataclass, field
from typing import List, Optional

_VALID_CASES = frozenset({"upper", "lower", "title"})


@dataclass
class ColumnConfig:
    """Define as operações de limpeza para uma única coluna."""
    name: str
    cleaned_name: Optional[str] = None
    invalid_values: List[str] = field(default_factory=list)
    standardize_case: Optional[str] = None  # upper, lower, title
    replace_empty_with_null: bool = False
    trim_whitespace: bool = False
    cast_to: Optional[str] = None
    chars_to_remove: Optional[str] = None
    normalize_chars: bool = False
    truncate_length: Optional[int] = None

    def __post_init__(self):
        if self.cleaned_name is None:
            self.cleaned_name = self.name
        if self.standardize_case is not None and self.standardize_case not in _VALID_CASES:
            raise ValueError(
                f"standardize_case deve ser um de {sorted(_VALID_CASES)}, recebeu '{self.standardize_case}'"
            )
        if self.truncate_length is not None and self.truncate_length <= 0:
            raise ValueError(
                f"truncate_length deve ser um inteiro positivo, recebeu {self.truncate_length}"
            )
