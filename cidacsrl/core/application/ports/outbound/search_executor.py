from abc import ABC, abstractmethod
from typing import List, Dict


class SearchExecutor(ABC):
    @abstractmethod
    def execute(self, es_client, index: str, queries: List[Dict]) -> List[Dict]:
        pass