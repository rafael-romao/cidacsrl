from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class IndexedDatasetFilterItem:
    """Represents one valid filter item used to build ES filter clauses."""

    query: Optional[List[Dict[str, Any]]] = None
    column: Optional[str] = None
    term: Optional[Dict[str, Any]] = None
    range: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        provided = [
            self.query is not None,
            self.column is not None,
            self.term is not None,
            self.range is not None,
        ]

        if sum(provided) != 1:
            raise ValueError(
                "Each indexed_dataset_filter item must contain exactly one of: "
                "'query', 'column', 'term', or 'range'."
            )

        if self.query is not None:
            if not isinstance(self.query, list):
                raise ValueError("'query' must be a list of query clauses.")
            for i, clause in enumerate(self.query):
                if not isinstance(clause, dict):
                    raise ValueError(
                        f"Each item in 'query' must be a dictionary. Invalid item at index {i}."
                    )

        if self.column is not None and not isinstance(self.column, str):
            raise ValueError("'column' must be a string.")

        if self.term is not None and not isinstance(self.term, dict):
            raise ValueError("'term' must be a dictionary.")

        if self.range is not None and not isinstance(self.range, dict):
            raise ValueError("'range' must be a dictionary.")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IndexedDatasetFilterItem":
        if not isinstance(data, dict):
            raise ValueError("Each indexed_dataset_filter item must be a dictionary.")
        return cls(
            query=data.get("query"),
            column=data.get("column"),
            term=data.get("term"),
            range=data.get("range"),
        )

    def to_dict(self) -> Dict[str, Any]:
        if self.query is not None:
            return {"query": self.query}
        if self.column is not None:
            return {"column": self.column}
        if self.term is not None:
            return {"term": self.term}
        return {"range": self.range}


def parse_indexed_dataset_filter(data: Any) -> Optional[List[Dict[str, Any]]]:
    """
    Validates indexed_dataset_filter structure and returns a normalized list of dicts.
    """
    if data is None:
        return None

    if not isinstance(data, list):
        raise ValueError("'indexed_dataset_filter' must be a list when provided.")

    parsed_items: List[IndexedDatasetFilterItem] = []
    for i, item in enumerate(data):
        try:
            parsed_items.append(IndexedDatasetFilterItem.from_dict(item))
        except ValueError as e:
            raise ValueError(
                f"Invalid item in 'indexed_dataset_filter' at index {i}: {e}"
            ) from e

    return [item.to_dict() for item in parsed_items]