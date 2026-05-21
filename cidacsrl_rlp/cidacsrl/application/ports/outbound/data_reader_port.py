from abc import ABC, abstractmethod
from typing import Any, Dict

class DataReaderPort(ABC):
    @abstractmethod
    def read_data(self) -> Any:
        """
        Lê dados de uma fonte especificada e retorna em um formato adequado para processamento.

        Args:
            source (str): Identificador da fonte de dados (ex: caminho do arquivo, nome da tabela, etc.).
            options (Dict[str, Any]): Opções adicionais para leitura (ex: formato do arquivo, credenciais, etc.).

        Returns:
            Any: Os dados lidos, no formato apropriado para o processamento subsequente.
        """
        pass