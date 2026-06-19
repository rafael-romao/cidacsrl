from abc import ABC, abstractmethod
from typing import Any


class GraphProcessingPort(ABC):
    """Porta de processamento de grafo para identificação de clusters de duplicatas.

    Abstrai o algoritmo de componentes conectados, isolando a dependência de
    GraphFrames da camada de aplicação. Retorna um DataFrame com duas colunas:
    o ID do registro e o ID do cluster ao qual ele pertence.
    """

    @abstractmethod
    def find_clusters(
        self,
        df_pairs: Any,
        id_source_column: str,
        id_target_column: str,
    ) -> Any:
        """Executa componentes conectados e retorna DataFrame com (id, cluster_id).

        Args:
            df_pairs: DataFrame com os pares de registros linkados.
            id_source_column: Coluna do ID de origem nos pares.
            id_target_column: Coluna do ID de destino nos pares.

        Returns:
            DataFrame com colunas ('id', 'cluster_id'), onde cada linha mapeia
            um ID de registro ao ID do cluster ao qual pertence.
        """
        pass
