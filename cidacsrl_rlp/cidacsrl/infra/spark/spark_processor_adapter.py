from cidacsrl.application.ports.processador_dados_port import ProcessadorDadosPort
from cidacsrl.domain.models.workflow import BlockingPhase
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class SparkProcessadorAdapter(ProcessadorDadosPort):
    def __init__(self, spark: SparkSession, df_source: DataFrame):
        self.spark = spark
        self.df_source = df_source

    def processar_fase(self, phase: BlockingPhase):
        df_matches = self._execute_linkage_spark_logic(phase)
        return df_matches