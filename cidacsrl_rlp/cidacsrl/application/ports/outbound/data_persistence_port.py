from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class DataPersistencePort(ABC):

    @abstractmethod
    def save_phase_output(
        self, 
        df: DataFrame, 
        project_name: str, 
        job_id: str, 
        unit_id: str, 
        phase_name: str
    ) -> int:
        pass