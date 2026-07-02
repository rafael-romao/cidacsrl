from abc import ABC, abstractmethod
from typing import Any, List


class DataTransformationPort(ABC):
    """Contrato para operações de transformação de DataFrames entre fases do pipeline."""

    @abstractmethod
    def add_phase_marker(self, df: Any, phase_name: str) -> Any:
        """Adiciona uma coluna identificando a fase que produziu cada registro.

        Args:
            df: DataFrame a marcar.
            phase_name: Nome da fase a registrar na coluna de marcação.

        Returns:
            DataFrame com a coluna de fase adicionada.
        """
        pass

    @abstractmethod
    def filter_matches_by_threshold(self, dataset: Any, threshold: float) -> Any:
        """Filtra pares que atingiram o score mínimo de pareamento.

        Args:
            dataset: DataFrame com pares pontuados.
            threshold: Score mínimo para considerar um par como match.

        Returns:
            DataFrame contendo apenas pares com score >= threshold.
        """
        pass

    @abstractmethod
    def union_results(self, phase_outputs: List[Any]) -> Any:
        """Consolida resultados de múltiplas fases em um único DataFrame.

        Args:
            phase_outputs: Lista de DataFrames de saída das fases.

        Returns:
            DataFrame único com todos os pares de todas as fases.
        """
        pass